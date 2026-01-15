/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "axiom/runner/LocalRunner.h"
#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/connectors/hive/HiveConnectorMetadata.h"
#include "velox/common/time/Timer.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/PlanNodeStats.h"

#include <set>

namespace facebook::axiom::runner {
namespace {

/// Testing proxy for a split source managed by a system with full metadata
/// access.
class SimpleSplitSource : public connector::SplitSource {
 public:
  explicit SimpleSplitSource(
      std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> splits)
      : splits_(std::move(splits)) {}

  std::vector<SplitAndGroup> getSplits(uint64_t /* targetBytes */) override {
    if (splitIdx_ >= splits_.size()) {
      return {{nullptr, 0}};
    }
    return {SplitAndGroup{std::move(splits_[splitIdx_++]), 0}};
  }

 private:
  std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> splits_;
  int32_t splitIdx_{0};
};
} // namespace

std::shared_ptr<connector::SplitSource>
SimpleSplitSourceFactory::splitSourceForScan(
    const connector::ConnectorSessionPtr& /* session */,
    const velox::core::TableScanNode& scan) {
  auto it = nodeSplitMap_.find(scan.id());
  if (it == nodeSplitMap_.end()) {
    VELOX_FAIL("Splits are not provided for scan {}", scan.id());
  }
  return std::make_shared<SimpleSplitSource>(it->second);
}

std::shared_ptr<connector::SplitSource>
ConnectorSplitSourceFactory::splitSourceForScan(
    const connector::ConnectorSessionPtr& session,
    const velox::core::TableScanNode& scan) {
  const auto& handle = scan.tableHandle();
  auto metadata = connector::ConnectorMetadata::metadata(handle->connectorId());
  auto splitManager = metadata->splitManager();

  auto partitions = splitManager->listPartitions(session, handle);
  return splitManager->getSplitSource(session, handle, partitions, options_);
}

namespace {

std::shared_ptr<velox::exec::RemoteConnectorSplit> remoteSplit(
    const std::string& taskId) {
  return std::make_shared<velox::exec::RemoteConnectorSplit>(taskId);
}

std::vector<velox::exec::Split> listAllSplits(
    const std::shared_ptr<connector::SplitSource>& source) {
  std::vector<velox::exec::Split> result;
  for (;;) {
    auto splits = source->getSplits(std::numeric_limits<uint64_t>::max());
    VELOX_CHECK(!splits.empty());
    for (auto& split : splits) {
      if (split.split == nullptr) {
        return result;
      }
      result.push_back(velox::exec::Split(std::move(split.split)));
    }
  }
  VELOX_UNREACHABLE();
}

void getTopologicalOrder(
    const std::vector<ExecutableFragment>& fragments,
    int32_t index,
    const folly::F14FastMap<std::string, int32_t>& taskPrefixToIndex,
    std::vector<bool>& visited,
    std::stack<int32_t>& indices) {
  visited[index] = true;
  for (const auto& input : fragments.at(index).inputStages) {
    if (!visited[taskPrefixToIndex.at(input.producerTaskPrefix)]) {
      getTopologicalOrder(
          fragments,
          taskPrefixToIndex.at(input.producerTaskPrefix),
          taskPrefixToIndex,
          visited,
          indices);
    }
  }
  indices.push(index);
}

std::vector<ExecutableFragment> topologicalSort(
    const std::vector<ExecutableFragment>& fragments) {
  folly::F14FastMap<std::string, int32_t> taskPrefixToIndex;
  for (auto i = 0; i < fragments.size(); ++i) {
    taskPrefixToIndex[fragments[i].taskPrefix] = i;
  }

  std::stack<int32_t> indices;
  std::vector<bool> visited(fragments.size(), false);
  for (auto i = 0; i < fragments.size(); ++i) {
    if (!visited[i]) {
      getTopologicalOrder(fragments, i, taskPrefixToIndex, visited, indices);
    }
  }

  auto size = indices.size();
  VELOX_CHECK_EQ(size, fragments.size());
  std::vector<ExecutableFragment> result(size);
  auto i = size - 1;
  while (!indices.empty()) {
    result[i--] = fragments[indices.top()];
    indices.pop();
  }
  VELOX_CHECK_EQ(result.size(), fragments.size());
  return result;
}
} // namespace

LocalRunner::LocalRunner(
    MultiFragmentPlanPtr plan,
    FinishWrite finishWrite,
    std::shared_ptr<velox::core::QueryCtx> queryCtx,
    std::shared_ptr<SplitSourceFactory> splitSourceFactory,
    std::shared_ptr<velox::memory::MemoryPool> outputPool)
    : plan_{std::move(plan)},
      fragments_{topologicalSort(plan_->fragments())},
      finishWrite_{std::move(finishWrite)},
      splitSourceFactory_{std::move(splitSourceFactory)} {
  params_.queryCtx = std::move(queryCtx);
  params_.outputPool = std::move(outputPool);

  VELOX_CHECK_NOT_NULL(splitSourceFactory_);
  VELOX_CHECK(!finishWrite_ || params_.outputPool != nullptr);

  setupBuckets();
}

velox::RowVectorPtr LocalRunner::next() {
  if (finishWrite_) {
    return nextWrite();
  }

  if (!cursor_) {
    start();
  }

  if (!cursor_->moveNext()) {
    state_ = State::kFinished;
    return nullptr;
  }

  return cursor_->current();
}

int64_t LocalRunner::runWrite() {
  std::vector<velox::RowVectorPtr> result;
  auto finishWrite = std::move(finishWrite_);
  auto state = State::kError;
  SCOPE_EXIT {
    state_ = state;
  };
  try {
    start();
    while (cursor_->moveNext()) {
      result.push_back(cursor_->current());
    }
  } catch (const std::exception&) {
    try {
      waitForCompletion(1'000'000);
    } catch (const std::exception& e) {
      LOG(ERROR) << e.what()
                 << " while waiting for completion after error in write query";
      throw;
    }
    std::move(finishWrite).abort().get();
    throw;
  }

  auto rows = std::move(finishWrite).commit(result).get();
  state = State::kFinished;
  return rows;
}

velox::RowVectorPtr LocalRunner::nextWrite() {
  VELOX_DCHECK(finishWrite_);

  const int64_t rows = runWrite();

  auto child = velox::BaseVector::create<velox::FlatVector<int64_t>>(
      velox::BIGINT(), /*size=*/1, params_.outputPool.get());
  child->set(0, rows);

  return std::make_shared<velox::RowVector>(
      params_.outputPool.get(),
      velox::ROW("rows", velox::BIGINT()),
      /*nulls=*/nullptr,
      /*length=*/1,
      std::vector<velox::VectorPtr>{std::move(child)});
}

void LocalRunner::start() {
  VELOX_CHECK_EQ(state_, State::kInitialized);

  params_.maxDrivers = plan_->options().numDrivers;
  params_.planNode = fragments_.back().fragment.planNode;

  VELOX_CHECK_LE(fragments_.back().width, 1);

  auto cursor = velox::exec::TaskCursor::create(params_);
  makeStages(cursor->task());

  {
    std::lock_guard<std::mutex> l(mutex_);
    if (!error_) {
      cursor_ = std::move(cursor);
      state_ = State::kRunning;
    }
  }

  if (!cursor_) {
    // The cursor was not set because previous fragments had an error.
    abort();
    std::rethrow_exception(error_);
  }
}

std::shared_ptr<connector::SplitSource> LocalRunner::splitSourceForScan(
    const connector::ConnectorSessionPtr& session,
    const velox::core::TableScanNode& scan) {
  return splitSourceFactory_->splitSourceForScan(session, scan);
}

void LocalRunner::abort() {
  // If called without previous error, we set the error to be cancellation.
  if (!error_) {
    try {
      state_ = State::kCancelled;
      VELOX_FAIL("Query cancelled");
    } catch (const std::exception&) {
      error_ = std::current_exception();
    }
  }
  VELOX_CHECK(state_ != State::kInitialized);
  // Setting errors is thread safe. The stages do not change after
  // initialization.
  for (auto& stage : stages_) {
    for (auto& task : stage) {
      task->setError(error_);
    }
  }
  if (cursor_) {
    cursor_->setError(error_);
  }
}

void LocalRunner::waitForCompletion(int32_t maxWaitMicros) {
  VELOX_CHECK_NE(state_, State::kInitialized);
  std::vector<velox::ContinueFuture> futures;
  {
    std::lock_guard<std::mutex> l(mutex_);
    for (auto& stage : stages_) {
      for (auto& task : stage) {
        futures.push_back(task->taskDeletionFuture());
      }
      stage.clear();
    }
  }

  const auto startTime = velox::getCurrentTimeMicro();
  for (auto& future : futures) {
    const auto elapsedTime = velox::getCurrentTimeMicro() - startTime;
    VELOX_CHECK_LT(
        elapsedTime,
        maxWaitMicros,
        "LocalRunner did not finish within {} us",
        maxWaitMicros);

    auto& executor = folly::QueuedImmediateExecutor::instance();
    std::move(future)
        .within(std::chrono::microseconds(maxWaitMicros - elapsedTime))
        .via(&executor)
        .wait();
  }
}

namespace {
bool isBroadcast(const velox::core::PlanFragment& fragment) {
  if (auto partitionedOutputNode =
          std::dynamic_pointer_cast<const velox::core::PartitionedOutputNode>(
              fragment.planNode)) {
    return partitionedOutputNode->kind() ==
        velox::core::PartitionedOutputNode::Kind::kBroadcast;
  }

  return false;
}

void gatherScans(
    const velox::core::PlanNodePtr& plan,
    std::vector<velox::core::TableScanNodePtr>& scans) {
  if (auto scan =
          std::dynamic_pointer_cast<const velox::core::TableScanNode>(plan)) {
    scans.push_back(scan);
    return;
  }
  for (const auto& source : plan->sources()) {
    gatherScans(source, scans);
  }
}

/// Gathers all TableScanNodes that are direct children of MergeJoinNodes.
void gatherMergeJoinScans(
    const velox::core::PlanNodePtr& plan,
    std::vector<velox::core::TableScanNodePtr>& scans) {
  if (auto mergeJoin =
          std::dynamic_pointer_cast<const velox::core::MergeJoinNode>(plan)) {
    // Check if left child is a TableScanNode
    if (auto leftScan =
            std::dynamic_pointer_cast<const velox::core::TableScanNode>(
                mergeJoin->sources()[0])) {
      scans.push_back(leftScan);
    }
    // Check if right child is a TableScanNode
    if (auto rightScan =
            std::dynamic_pointer_cast<const velox::core::TableScanNode>(
                mergeJoin->sources()[1])) {
      scans.push_back(rightScan);
    }
  }
  // Continue searching in children
  for (const auto& source : plan->sources()) {
    gatherMergeJoinScans(source, scans);
  }
}
} // namespace

void LocalRunner::makeStages(
    const std::shared_ptr<velox::exec::Task>& lastStageTask) {
  auto sharedRunner = shared_from_this();
  auto onError = [self = sharedRunner, this](std::exception_ptr error) {
    {
      std::lock_guard<std::mutex> l(mutex_);
      if (error_) {
        return;
      }
      state_ = State::kError;
      error_ = std::move(error);
    }
    if (cursor_) {
      abort();
    }
  };

  // Mapping from task prefix to the stage index and whether it is a broadcast.
  folly::F14FastMap<std::string, std::pair<int32_t, bool>> stageMap;
  for (auto fragmentIndex = 0; fragmentIndex < fragments_.size() - 1;
       ++fragmentIndex) {
    const auto& fragment = fragments_[fragmentIndex];
    stageMap[fragment.taskPrefix] = {
        stages_.size(), isBroadcast(fragment.fragment)};
    stages_.emplace_back();

    for (auto i = 0; i < fragment.width; ++i) {
      auto task = velox::exec::Task::create(
          fmt::format(
              "local://{}/{}.{}",
              params_.queryCtx->queryId(),
              fragment.taskPrefix,
              i),
          fragment.fragment,
          i,
          params_.queryCtx,
          velox::exec::Task::ExecutionMode::kParallel,
          velox::exec::ConsumerSupplier{},
          /*memoryArbitrationPriority=*/0,
          /*spillDiskOpts=*/std::nullopt,
          onError);
      stages_.back().push_back(task);

      task->start(plan_->options().numDrivers);
    }
  }

  stages_.push_back({lastStageTask});

  try {
    for (auto fragmentIndex = 0; fragmentIndex < fragments_.size();
         ++fragmentIndex) {
      const auto& fragment = fragments_[fragmentIndex];
      const auto& stage = stages_[fragmentIndex];

      // Check if this stage has a merge join group
      if (fragmentIndex < mergeJoinGroups_.size() &&
          !mergeJoinGroups_[fragmentIndex].scans.empty()) {
        distributeMergeJoinSplits(
            fragmentIndex, mergeJoinGroups_[fragmentIndex], stage);
      } else {
        std::vector<velox::core::TableScanNodePtr> scans;
        gatherScans(fragment.fragment.planNode, scans);

        for (const auto& scan : scans) {
          auto source = splitSourceForScan(/*session=*/nullptr, *scan);

          std::vector<connector::SplitSource::SplitAndGroup> splits;
          int32_t splitIdx = 0;
          auto getNextSplit = [&]() {
            if (splitIdx < splits.size()) {
              return velox::exec::Split(std::move(splits[splitIdx++].split));
            }
            splits = source->getSplits(std::numeric_limits<int64_t>::max());
            splitIdx = 1;
            return velox::exec::Split(std::move(splits[0].split));
          };

          // Check if we have a bucket map for this stage
          const bool hasBucketMap = !stageBucketMap_.empty() &&
              fragmentIndex < stageBucketMap_.size() &&
              !stageBucketMap_[fragmentIndex].empty();

          // Distribute splits across tasks.
          // For bucketed tables with a bucket map, route splits to specific
          // workers. Otherwise, use round-robin distribution.
          bool allDone = false;
          do {
            auto split = getNextSplit();
            if (!split.hasConnectorSplit()) {
              allDone = true;
              break;
            }

            // Determine which task should receive this split
            int32_t targetTaskIndex = -1;

            if (hasBucketMap) {
              // Try to extract bucket number from the split
              auto* connectorSplit = split.connectorSplit.get();
              auto* hiveSplit =
                  dynamic_cast<velox::connector::hive::HiveConnectorSplit*>(
                      connectorSplit);

              if (hiveSplit && hiveSplit->tableBucketNumber.has_value()) {
                // Split has a bucket number - use the bucket map
                const int32_t bucketNumber =
                    hiveSplit->tableBucketNumber.value();
                const int32_t numBuckets =
                    stageBucketMap_[fragmentIndex].size();
                const int32_t mappedBucket = bucketNumber % numBuckets;
                targetTaskIndex = stageBucketMap_[fragmentIndex][mappedBucket];
              }
            }

            if (targetTaskIndex < 0 || targetTaskIndex >= stage.size()) {
              // No bucket number or bucket map - use round-robin
              // Find next task in round-robin fashion
              static thread_local int32_t roundRobinIndex = 0;
              targetTaskIndex = roundRobinIndex % stage.size();
              roundRobinIndex++;
            }

            stage[targetTaskIndex]->addSplit(scan->id(), std::move(split));
          } while (!allDone);

          for (auto& task : stage) {
            task->noMoreSplits(scan->id());
          }
        }
      }

      for (const auto& input : fragment.inputStages) {
        const auto [sourceStage, broadcast] =
            stageMap[input.producerTaskPrefix];

        std::vector<std::shared_ptr<velox::exec::RemoteConnectorSplit>>
            sourceSplits;
        for (const auto& task : stages_[sourceStage]) {
          sourceSplits.push_back(remoteSplit(task->taskId()));

          if (broadcast) {
            task->updateOutputBuffers(fragment.width, true);
          }
        }

        for (auto& task : stage) {
          for (const auto& remote : sourceSplits) {
            task->addSplit(input.consumerNodeId, velox::exec::Split(remote));
          }
        }
      }

      for (const auto& input : fragment.inputStages) {
        for (auto& task : stage) {
          task->noMoreSplits(input.consumerNodeId);
        }
      }
    }
  } catch (const std::exception&) {
    onError(std::current_exception());
  }
}

void LocalRunner::setupBuckets() {
  // Only setup buckets if we have multiple workers
  if (plan_->options().numWorkers <= 1) {
    return;
  }

  stageBucketMap_.resize(fragments_.size());

  // Process each fragment
  for (size_t fragmentIndex = 0; fragmentIndex < fragments_.size();
       ++fragmentIndex) {
    const auto& fragment = fragments_[fragmentIndex];

    // Gather all table scans in this fragment
    std::vector<velox::core::TableScanNodePtr> scans;
    gatherScans(fragment.fragment.planNode, scans);

    // Find scans with partition columns
    std::vector<velox::core::TableScanNodePtr> partitionedScans;
    for (const auto& scan : scans) {
      const auto& handle = scan->tableHandle();

      connector::ConnectorMetadata* metadata = nullptr;
      try {
        metadata =
            connector::ConnectorMetadata::metadata(handle->connectorId());
      } catch (const std::exception&) {
        // Metadata not registered, skip bucket setup
        return;
      }

      // Get the table
      auto table = metadata->findTable(handle->name());
      if (!table) {
        continue;
      }

      // Get the first layout (assuming single layout per table for now)
      const auto& layouts = table->layouts();
      if (layouts.empty()) {
        continue;
      }

      const auto* layout = layouts[0];

      // Check if this table has partition columns
      if (!layout->partitionColumns().empty()) {
        partitionedScans.push_back(scan);
      }
    }

    // If no partitioned scans in this fragment, continue
    if (partitionedScans.empty()) {
      continue;
    }

    // Validate that all partitioned scans have the same number of partition
    // columns and matching types
    const auto& firstHandle = partitionedScans[0]->tableHandle();

    connector::ConnectorMetadata* firstMetadata = nullptr;
    try {
      firstMetadata =
          connector::ConnectorMetadata::metadata(firstHandle->connectorId());
    } catch (const std::exception&) {
      // Metadata not registered, skip bucket setup
      return;
    }

    auto firstTable = firstMetadata->findTable(firstHandle->name());
    VELOX_CHECK_NOT_NULL(
        firstTable,
        "Table {} not found for scan {} in fragment {}",
        firstHandle->name(),
        partitionedScans[0]->id(),
        fragmentIndex);
    VELOX_CHECK(
        !firstTable->layouts().empty(),
        "Table {} has no layouts for scan {} in fragment {}",
        firstHandle->name(),
        partitionedScans[0]->id(),
        fragmentIndex);
    const auto* firstLayout = firstTable->layouts()[0];

    const auto& firstPartitionColumns = firstLayout->partitionColumns();
    const size_t numPartitionColumns = firstPartitionColumns.size();

    // Build a vector of partition column types for the first scan
    std::vector<velox::TypePtr> firstPartitionTypes;
    for (const auto* col : firstPartitionColumns) {
      firstPartitionTypes.push_back(col->type());
    }

    // Check all other partitioned scans
    for (size_t i = 1; i < partitionedScans.size(); ++i) {
      const auto& handle = partitionedScans[i]->tableHandle();

      connector::ConnectorMetadata* metadata = nullptr;
      try {
        metadata =
            connector::ConnectorMetadata::metadata(handle->connectorId());
      } catch (const std::exception&) {
        // Metadata not registered, skip bucket setup
        return;
      }

      auto table = metadata->findTable(handle->name());
      VELOX_CHECK_NOT_NULL(
          table,
          "Table {} not found for scan {} in fragment {}",
          handle->name(),
          partitionedScans[i]->id(),
          fragmentIndex);
      VELOX_CHECK(
          !table->layouts().empty(),
          "Table {} has no layouts for scan {} in fragment {}",
          handle->name(),
          partitionedScans[i]->id(),
          fragmentIndex);
      const auto* layout = table->layouts()[0];

      const auto& partitionColumns = layout->partitionColumns();

      if (partitionColumns.size() != numPartitionColumns) {
        VELOX_FAIL(
            "All table scans in fragment {} with partition columns must have the same number of partition columns. "
            "Expected {}, but scan {} has {}",
            fragmentIndex,
            numPartitionColumns,
            partitionedScans[i]->id(),
            partitionColumns.size());
      }

      // Check that partition column types match pairwise
      for (size_t colIdx = 0; colIdx < numPartitionColumns; ++colIdx) {
        if (!partitionColumns[colIdx]->type()->equivalent(
                *firstPartitionTypes[colIdx])) {
          VELOX_FAIL(
              "Partition column types must match pairwise. "
              "In fragment {}, scan {} has partition column {} with type {}, "
              "but expected type {}",
              fragmentIndex,
              partitionedScans[i]->id(),
              colIdx,
              partitionColumns[colIdx]->type()->toString(),
              firstPartitionTypes[colIdx]->toString());
        }
      }
    }

    // Find the table with the smallest number of partitions
    const connector::hive::HivePartitionType* minPartitionType = nullptr;
    int32_t minNumPartitions = std::numeric_limits<int32_t>::max();

    for (const auto& scan : partitionedScans) {
      const auto& handle = scan->tableHandle();

      connector::ConnectorMetadata* metadata = nullptr;
      try {
        metadata =
            connector::ConnectorMetadata::metadata(handle->connectorId());
      } catch (const std::exception&) {
        // Metadata not registered, skip bucket setup
        return;
      }

      auto table = metadata->findTable(handle->name());
      VELOX_CHECK_NOT_NULL(
          table,
          "Table {} not found for scan {} in fragment {}",
          handle->name(),
          scan->id(),
          fragmentIndex);
      VELOX_CHECK(
          !table->layouts().empty(),
          "Table {} has no layouts for scan {} in fragment {}",
          handle->name(),
          scan->id(),
          fragmentIndex);
      const auto* layout = table->layouts()[0];

      const auto* partitionType = layout->partitionType();
      if (!partitionType) {
        VELOX_FAIL(
            "Table scan {} in fragment {} has partition columns but no partition type",
            scan->id(),
            fragmentIndex);
      }

      const auto* hivePartitionType =
          dynamic_cast<const connector::hive::HivePartitionType*>(
              partitionType);

      if (!hivePartitionType) {
        VELOX_FAIL(
            "Partition type for scan {} in fragment {} is not HivePartitionType",
            scan->id(),
            fragmentIndex);
      }

      if (hivePartitionType->numPartitions() < minNumPartitions) {
        minNumPartitions = hivePartitionType->numPartitions();
        minPartitionType = hivePartitionType;
      }
    }

    // Create the bucket map for this stage
    const int32_t numWorkers = plan_->options().numWorkers;
    const int32_t numBuckets = minNumPartitions;

    stageBucketMap_[fragmentIndex].resize(numBuckets);
    for (int32_t bucket = 0; bucket < numBuckets; ++bucket) {
      stageBucketMap_[fragmentIndex][bucket] = bucket % numWorkers;
    }
  }

  detectMergeJoinGroups();
}

void LocalRunner::detectMergeJoinGroups() {
  mergeJoinGroups_.resize(fragments_.size());

  for (size_t fragmentIndex = 0; fragmentIndex < fragments_.size();
       ++fragmentIndex) {
    const auto& fragment = fragments_[fragmentIndex];

    // Gather all TableScanNodes that are children of MergeJoinNodes
    std::vector<velox::core::TableScanNodePtr> mergeJoinScans;
    gatherMergeJoinScans(fragment.fragment.planNode, mergeJoinScans);

    // If we found any, add them to the MergeJoinGroup for this stage
    if (!mergeJoinScans.empty()) {
      MergeJoinGroup group;
      for (const auto& scan : mergeJoinScans) {
        group.scans.push_back(MergeSplitSpec{scan->id()});
      }
      mergeJoinGroups_[fragmentIndex] = std::move(group);
    }
  }
}

void LocalRunner::distributeMergeJoinSplits(
    size_t fragmentIndex,
    const MergeJoinGroup& group,
    const std::vector<std::shared_ptr<velox::exec::Task>>& tasks) {
  const size_t numWorkers = tasks.size();
  const size_t numScans = group.scans.size();

  // List all splits for each scan.
  // allSplits[scanIndex] = vector of splits for that scan
  std::vector<std::vector<std::shared_ptr<velox::connector::ConnectorSplit>>>
      allSplits(numScans);

  for (size_t scanIndex = 0; scanIndex < numScans; ++scanIndex) {
    const auto& scanId = group.scans[scanIndex].scanId;

    // Find the TableScanNode with this ID
    std::vector<velox::core::TableScanNodePtr> scans;
    gatherScans(fragments_[fragmentIndex].fragment.planNode, scans);

    for (const auto& scan : scans) {
      if (scan->id() == scanId) {
        // Get splits with whole file option
        auto source = splitSourceForScan(/*session=*/nullptr, *scan);

        for (;;) {
          auto splits = source->getSplits(std::numeric_limits<int64_t>::max());
          if (splits.empty() || splits[0].split == nullptr) {
            break;
          }
          for (auto& splitAndGroup : splits) {
            if (splitAndGroup.split) {
              allSplits[scanIndex].push_back(std::move(splitAndGroup.split));
            }
          }
        }
        break;
      }
    }
  }

  // Organize splits by worker.
  // mergeGroups[worker][scanIndex] = vector of splits for this worker and scan
  std::vector<std::vector<
      std::vector<std::shared_ptr<velox::connector::ConnectorSplit>>>>
      mergeGroups(numWorkers);
  for (size_t worker = 0; worker < numWorkers; ++worker) {
    mergeGroups[worker].resize(numScans);
  }

  // Check if we have a bucket map for this stage
  const bool hasBucketMap = !stageBucketMap_.empty() &&
      fragmentIndex < stageBucketMap_.size() &&
      !stageBucketMap_[fragmentIndex].empty();

  // Distribute splits to workers based on bucket number
  for (size_t scanIndex = 0; scanIndex < numScans; ++scanIndex) {
    for (auto& split : allSplits[scanIndex]) {
      int32_t targetWorker = 0;

      if (hasBucketMap) {
        auto* hiveSplit =
            dynamic_cast<velox::connector::hive::HiveConnectorSplit*>(
                split.get());
        if (hiveSplit && hiveSplit->tableBucketNumber.has_value()) {
          const int32_t bucketNumber = hiveSplit->tableBucketNumber.value();
          const int32_t numBuckets = stageBucketMap_[fragmentIndex].size();
          const int32_t mappedBucket = bucketNumber % numBuckets;
          targetWorker = stageBucketMap_[fragmentIndex][mappedBucket];
        }
      }

      if (targetWorker < 0 || targetWorker >= numWorkers) {
        targetWorker = 0;
      }

      mergeGroups[targetWorker][scanIndex].push_back(std::move(split));
    }
  }

  // For each worker, collect distinct bucket numbers and create split groups
  for (size_t worker = 0; worker < numWorkers; ++worker) {
    auto& task = tasks[worker];

    // Collect all distinct bucket numbers from all scans for this worker
    std::set<int32_t> distinctBuckets;
    for (size_t scanIndex = 0; scanIndex < numScans; ++scanIndex) {
      for (const auto& split : mergeGroups[worker][scanIndex]) {
        auto* hiveSplit =
            dynamic_cast<velox::connector::hive::HiveConnectorSplit*>(
                split.get());
        if (hiveSplit && hiveSplit->tableBucketNumber.has_value()) {
          distinctBuckets.insert(hiveSplit->tableBucketNumber.value());
        }
      }
    }

    // For each distinct bucket, create a split group
    int32_t groupId = 1;
    for (int32_t bucket : distinctBuckets) {
      // For each scan, find the split with this bucket and add it
      for (size_t scanIndex = 0; scanIndex < numScans; ++scanIndex) {
        const auto& scanId = group.scans[scanIndex].scanId;

        for (auto& split : mergeGroups[worker][scanIndex]) {
          auto* hiveSplit =
              dynamic_cast<velox::connector::hive::HiveConnectorSplit*>(
                  split.get());
          if (hiveSplit && hiveSplit->tableBucketNumber.has_value() &&
              hiveSplit->tableBucketNumber.value() == bucket) {
            // Copy the shared_ptr, then move it into Split constructor
            auto splitCopy = split;
            task->addSplit(
                scanId, velox::exec::Split(std::move(splitCopy), groupId));
          }
        }
      }

      // Signal no more splits for this group for all scans
      for (size_t scanIndex = 0; scanIndex < numScans; ++scanIndex) {
        const auto& scanId = group.scans[scanIndex].scanId;
        task->noMoreSplitsForGroup(scanId, groupId);
      }

      ++groupId;
    }

    // Signal no more splits for all scans
    for (size_t scanIndex = 0; scanIndex < numScans; ++scanIndex) {
      const auto& scanId = group.scans[scanIndex].scanId;
      task->noMoreSplits(scanId);
    }
  }
}

std::vector<velox::exec::TaskStats> LocalRunner::stats() const {
  std::vector<velox::exec::TaskStats> result;
  std::lock_guard<std::mutex> l(mutex_);
  for (const auto& tasks : stages_) {
    VELOX_CHECK(!tasks.empty());

    auto stats = tasks[0]->taskStats();
    for (auto i = 1; i < tasks.size(); ++i) {
      const auto moreStats = tasks[i]->taskStats();
      for (auto pipeline = 0; pipeline < stats.pipelineStats.size();
           ++pipeline) {
        auto& pipelineStats = stats.pipelineStats[pipeline];
        for (auto op = 0; op < pipelineStats.operatorStats.size(); ++op) {
          pipelineStats.operatorStats[op].add(
              moreStats.pipelineStats[pipeline].operatorStats[op]);
        }
      }
    }
    result.push_back(std::move(stats));
  }
  return result;
}

namespace {
void printCustomStats(
    const std::unordered_map<std::string, velox::RuntimeMetric>& stats,
    std::string_view indentation,
    std::ostream& stream) {
  int width = 0;
  for (const auto& entry : stats) {
    if (width < entry.first.size()) {
      width = entry.first.size();
    }
  }
  width += 3;

  // Copy to a map to get a deterministic output.
  std::map<std::string_view, velox::RuntimeMetric> orderedStats;
  for (const auto& [name, metric] : stats) {
    orderedStats[name] = metric;
  }

  for (const auto& [name, metric] : orderedStats) {
    stream << indentation << std::left << std::setw(width) << name;
    metric.printMetric(stream);
    stream << std::endl;
  }
}
} // namespace

std::string LocalRunner::printPlanWithStats(
    bool includeCustomStats,
    const std::function<void(
        const velox::core::PlanNodeId& nodeId,
        std::string_view indentation,
        std::ostream& out)>& addContext) const {
  folly::F14FastSet<velox::core::PlanNodeId> leafNodeIds;
  for (const auto& fragment : fragments_) {
    for (const auto& nodeId : fragment.fragment.planNode->leafPlanNodeIds()) {
      leafNodeIds.insert(nodeId);
    }
  }

  const auto taskStats = stats();

  folly::F14FastMap<velox::core::PlanNodeId, velox::exec::PlanNodeStats>
      planNodeStats;
  for (const auto& stats : taskStats) {
    auto planStats = velox::exec::toPlanStats(stats);
    for (auto& [id, nodeStats] : planStats) {
      bool ok = planNodeStats.emplace(id, std::move(nodeStats)).second;
      VELOX_CHECK(
          ok,
          "Plan node IDs must be unique across fragments. "
          "Found duplicate ID: {}",
          id);
    }
  }

  return plan_->toString(
      true, [&](const auto& planNodeId, const auto& indentation, auto& out) {
        if (addContext != nullptr) {
          addContext(planNodeId, indentation, out);
        }

        auto statsIt = planNodeStats.find(planNodeId);
        if (statsIt != planNodeStats.end()) {
          out << indentation
              << statsIt->second.toString(leafNodeIds.contains(planNodeId))
              << std::endl;
          if (includeCustomStats) {
            printCustomStats(statsIt->second.customStats, indentation, out);
          }
        }
      });
}

} // namespace facebook::axiom::runner
