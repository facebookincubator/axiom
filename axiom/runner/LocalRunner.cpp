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
#include <folly/coro/AsyncScope.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Task.h>
#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "velox/common/base/SpillConfig.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/time/Timer.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/PlanNodeStats.h"

namespace facebook::axiom::runner {
namespace {

/// Testing proxy for a split source managed by a system with full metadata
/// access.
class SimpleSplitSource : public connector::SplitSource {
 public:
  explicit SimpleSplitSource(
      std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> splits)
      : splits_(std::move(splits)) {}

  folly::coro::Task<connector::SplitBatch> co_getSplits(
      uint32_t maxSplitCount,
      int32_t /*bucket*/) override {
    connector::SplitBatch batch;
    auto end = std::min(
        nextIndex_ + static_cast<size_t>(maxSplitCount), splits_.size());
    for (auto i = nextIndex_; i < end; ++i) {
      batch.splits.push_back(std::move(splits_[i]));
    }
    nextIndex_ = end;
    batch.noMoreSplits = (nextIndex_ >= splits_.size());
    co_return batch;
  }

 private:
  std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> splits_;
  size_t nextIndex_{0};
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
  auto metadata =
      connector::ConnectorMetadataRegistry::get(handle->connectorId());
  auto splitManager = metadata->splitManager();

  auto partitions = folly::coro::blockingWait(
      splitManager->co_listPartitions(session, handle));
  return splitManager->getSplitSource(session, handle, partitions, options_);
}

namespace {

std::shared_ptr<velox::exec::RemoteConnectorSplit> remoteSplit(
    const std::string& taskId) {
  return std::make_shared<velox::exec::RemoteConnectorSplit>(taskId);
}

// Builds per-task SpillDiskOptions under the base spill directory. Each task
// gets a unique subdirectory created lazily on first spill.
std::optional<velox::common::SpillDiskOptions> makeSpillDiskOptions(
    const std::string& baseSpillDirectory,
    const std::string& taskId) {
  if (baseSpillDirectory.empty()) {
    return std::nullopt;
  }
  velox::common::SpillDiskOptions options;
  options.spillDirPath = fmt::format("{}/{}", baseSpillDirectory, taskId);
  options.spillDirCreated = false;
  options.spillDirCreateCb = [path = options.spillDirPath]() {
    auto fileSystem = velox::filesystems::getFileSystem(path, nullptr);
    fileSystem->mkdir(path);
    return path;
  };
  return options;
}

// Streams splits from the source and distributes them round-robin across tasks.
folly::coro::Task<void> co_generateAndDistributeSplits(
    std::shared_ptr<connector::SplitSource> source,
    velox::core::PlanNodeId scanId,
    std::vector<std::shared_ptr<velox::exec::Task>> tasks,
    std::function<void(std::exception_ptr)> onError) {
  try {
    VELOX_CHECK(!tasks.empty(), "tasks must not be empty");

    size_t taskIdx = 0;
    for (;;) {
      auto batch = co_await source->co_getSplits(1);
      for (auto& split : batch.splits) {
        tasks[taskIdx]->addSplit(scanId, velox::exec::Split(std::move(split)));
        taskIdx = (taskIdx + 1) % tasks.size();
      }
      if (batch.noMoreSplits) {
        break;
      }
    }
    for (auto& task : tasks) {
      task->noMoreSplits(scanId);
    }
  } catch (...) {
    onError(std::current_exception());
    throw;
  }
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

void generateSplits(
    SplitSourceFactory& splitSourceFactory,
    const velox::core::PlanNodePtr& planNode,
    const std::vector<std::shared_ptr<velox::exec::Task>>& tasks) {
  std::vector<velox::core::TableScanNodePtr> scans;
  gatherScans(planNode, scans);

  for (const auto& scan : scans) {
    auto source =
        splitSourceFactory.splitSourceForScan(/*session=*/nullptr, *scan);
    size_t taskIdx = 0;
    for (;;) {
      auto batch = folly::coro::blockingWait(source->co_getSplits(1));
      for (auto& split : batch.splits) {
        tasks[taskIdx]->addSplit(
            scan->id(), velox::exec::Split(std::move(split)));
        taskIdx = (taskIdx + 1) % tasks.size();
      }
      if (batch.noMoreSplits) {
        break;
      }
    }

    for (auto& task : tasks) {
      task->noMoreSplits(scan->id());
    }
  }
}

void getTopologicalOrder(
    const std::vector<optimizer::ExecutableFragment>& fragments,
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

std::vector<optimizer::ExecutableFragment> topologicalSort(
    const std::vector<optimizer::ExecutableFragment>& fragments) {
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

  VELOX_CHECK_EQ(indices.size(), fragments.size());
  std::vector<optimizer::ExecutableFragment> result;
  result.reserve(indices.size());
  while (!indices.empty()) {
    result.push_back(fragments[indices.top()]);
    indices.pop();
  }
  std::reverse(result.begin(), result.end());
  return result;
}
} // namespace

LocalRunner::LocalRunner(
    optimizer::MultiFragmentPlanPtr plan,
    optimizer::FinishWrite finishWrite,
    std::shared_ptr<velox::core::QueryCtx> queryCtx,
    std::shared_ptr<SplitSourceFactory> splitSourceFactory,
    std::shared_ptr<velox::memory::MemoryPool> outputPool,
    std::string baseSpillDirectory,
    velox::exec::Task::ExecutionMode mode)
    : plan_{std::move(plan)},
      fragments_{topologicalSort(plan_->fragments())},
      finishWrite_{std::move(finishWrite)},
      splitSourceFactory_{std::move(splitSourceFactory)},
      mode_{mode},
      baseSpillDirectory_{std::move(baseSpillDirectory)} {
  params_.queryCtx = std::move(queryCtx);
  params_.outputPool = std::move(outputPool);
  if (!baseSpillDirectory_.empty()) {
    params_.spillDirectory = baseSpillDirectory_;
  }
  params_.serialExecution =
      (mode_ == velox::exec::Task::ExecutionMode::kSerial);

  VELOX_CHECK_NOT_NULL(splitSourceFactory_);
  VELOX_CHECK(!finishWrite_ || params_.outputPool != nullptr);
  VELOX_CHECK_EQ(
      params_.serialExecution,
      params_.queryCtx->executor() == nullptr,
      "Serial mode requires no executor; parallel mode requires one");
}

LocalRunner::~LocalRunner() {
  if (!splitScopeJoined_) {
    folly::coro::blockingWait(splitScope_.joinAsync());
  }
}

velox::RowVectorPtr LocalRunner::next() {
  if (finishWrite_) {
    return nextWrite();
  }

  try {
    if (!cursor_) {
      start();
    }

    if (!cursor_->moveNext()) {
      state_ = State::kFinished;
      return nullptr;
    }

    return cursor_->current();
  } catch (...) {
    setErrorIfNone(std::current_exception());
    throw;
  }
}

void LocalRunner::setErrorIfNone(std::exception_ptr error) {
  std::lock_guard<std::mutex> l(mutex_);
  if (!error_) {
    state_ = State::kError;
    error_ = std::move(error);
  }
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

  VELOX_CHECK_LE(
      fragments_.back().width.value_or(1),
      1,
      "Last fragment must be single-task");
  if (mode_ == velox::exec::Task::ExecutionMode::kSerial) {
    VELOX_CHECK_EQ(
        fragments_.size(),
        1,
        "Serial execution requires a single-fragment plan");
  }

  auto cursor = velox::exec::TaskCursor::create(params_);

  switch (mode_) {
    case velox::exec::Task::ExecutionMode::kSerial:
      generateSplits(*splitSourceFactory_, params_.planNode, {cursor->task()});
      break;
    case velox::exec::Task::ExecutionMode::kParallel:
      makeStages(cursor->task());
      break;
  }

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

  // Take a local copy of tasks under the mutex to prevent use-after-free if
  // waitForCompletion() clears stages_ concurrently.
  std::vector<std::shared_ptr<velox::exec::Task>> tasks;
  {
    std::lock_guard<std::mutex> l(mutex_);
    for (auto& stage : stages_) {
      for (auto& task : stage) {
        tasks.push_back(task);
      }
    }
  }

  for (auto& task : tasks) {
    task->setError(error_);
  }
  if (cursor_) {
    cursor_->setError(error_);
  }
}

bool LocalRunner::waitForCompletion(int32_t maxWaitMicros) {
  VELOX_CHECK_NE(state_, State::kInitialized);

  if (mode_ == velox::exec::Task::ExecutionMode::kSerial) {
    return true;
  }

  if (!splitScopeJoined_) {
    folly::coro::blockingWait(splitScope_.joinAsync());
    splitScopeJoined_ = true;
  }

  std::vector<velox::ContinueFuture> futures;
  {
    std::lock_guard<std::mutex> l(mutex_);
    cursor_.reset();
    for (auto& stage : stages_) {
      for (auto& task : stage) {
        futures.push_back(task->taskDeletionFuture());
      }
      stage.clear();
    }
  }

  auto& executor = folly::QueuedImmediateExecutor::instance();
  auto result = folly::collectAll(std::move(futures))
                    .via(&executor)
                    .within(std::chrono::microseconds(maxWaitMicros))
                    .wait();

  return !result.hasException();
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

} // namespace

void LocalRunner::makeStages(
    const std::shared_ptr<velox::exec::Task>& lastStageTask) {
  auto sharedRunner = shared_from_this();
  // Use weak_ptr to avoid a reference cycle:
  // Task -> onError -> shared_ptr<LocalRunner> -> stages_ -> shared_ptr<Task>.
  auto onError = [weak = std::weak_ptr<LocalRunner>(sharedRunner)](
                     std::exception_ptr error) {
    auto self = weak.lock();
    if (!self) {
      return;
    }
    {
      std::lock_guard<std::mutex> l(self->mutex_);
      if (self->error_) {
        return;
      }
      self->state_ = State::kError;
      self->error_ = std::move(error);
    }
    if (self->cursor_) {
      self->abort();
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

    auto numTasks = fragment.width.value_or(
        fragment.type == optimizer::FragmentType::kSource
            ? plan_->options().numWorkers
            : 1);
    for (auto i = 0; i < numTasks; ++i) {
      auto taskId = fmt::format(
          "local://{}/{}.{}",
          params_.queryCtx->queryId(),
          fragment.taskPrefix,
          i);
      auto task = velox::exec::Task::create(
          taskId,
          fragment.fragment,
          i,
          params_.queryCtx,
          velox::exec::Task::ExecutionMode::kParallel,
          velox::exec::ConsumerSupplier{},
          /*memoryArbitrationPriority=*/0,
          makeSpillDiskOptions(baseSpillDirectory_, taskId),
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

      std::vector<velox::core::TableScanNodePtr> scans;
      gatherScans(fragment.fragment.planNode, scans);

      for (const auto& scan : scans) {
        auto source = splitSourceForScan(/*session=*/nullptr, *scan);
        splitScope_.add(
            folly::coro::co_withExecutor(
                params_.queryCtx->executor(),
                co_generateAndDistributeSplits(
                    source, scan->id(), stage, onError)));
      }

      for (const auto& input : fragment.inputStages) {
        const auto [sourceStage, broadcast] =
            stageMap[input.producerTaskPrefix];

        std::vector<std::shared_ptr<velox::exec::RemoteConnectorSplit>>
            sourceSplits;
        for (const auto& task : stages_[sourceStage]) {
          sourceSplits.push_back(remoteSplit(task->taskId()));

          if (broadcast) {
            task->updateOutputBuffers(static_cast<int>(stage.size()), true);
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
