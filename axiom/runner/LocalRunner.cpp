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
    const velox::core::TableScanNode& scan) {
  auto it = nodeSplitMap_.find(scan.id());
  if (it == nodeSplitMap_.end()) {
    VELOX_FAIL("Splits are not provided for scan {}", scan.id());
  }
  return std::make_shared<SimpleSplitSource>(it->second);
}

std::shared_ptr<connector::SplitSource>
ConnectorSplitSourceFactory::splitSourceForScan(
    const velox::core::TableScanNode& scan) {
  const auto& handle = scan.tableHandle();
  auto metadata = connector::ConnectorMetadata::metadata(handle->connectorId());
  auto splitManager = metadata->splitManager();

  auto partitions = splitManager->listPartitions(handle);
  return splitManager->getSplitSource(handle, partitions, options_);
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
    const MultiFragmentPlanPtr& plan,
    std::shared_ptr<velox::core::QueryCtx> queryCtx,
    std::shared_ptr<SplitSourceFactory> splitSourceFactory,
    std::shared_ptr<velox::memory::MemoryPool> outputPool)
    : plan_{plan},
      fragments_(topologicalSort(plan->fragments())),
      splitSourceFactory_(std::move(splitSourceFactory)) {
  params_.queryCtx = std::move(queryCtx);
  params_.outputPool = std::move(outputPool);
}

velox::RowVectorPtr LocalRunner::next() {
  if (!cursor_) {
    start();
  }

  if (!cursor_->moveNext()) {
    state_ = State::kFinished;
    return nullptr;
  }
  return cursor_->current();
}

void LocalRunner::start() {
  VELOX_CHECK_EQ(state_, State::kInitialized);

  params_.maxDrivers = plan_->options().numDrivers;
  params_.planNode = fragments_.back().fragment.planNode;

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
    const velox::core::TableScanNode& scan) {
  return splitSourceFactory_->splitSourceForScan(scan);
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
      velox::exec::Consumer consumer = nullptr;
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
          consumer,
          0,
          onError);
      stages_.back().push_back(task);

      task->start(plan_->options().numDrivers);
    }
  }

  stages_.push_back({lastStageTask});

  for (auto fragmentIndex = 0; fragmentIndex < fragments_.size();
       ++fragmentIndex) {
    const auto& fragment = fragments_[fragmentIndex];
    const auto& stage = stages_[fragmentIndex];

    for (const auto& scan : fragment.scans) {
      auto source = splitSourceForScan(*scan);

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

      // Distribute splits across tasks using round-robin.
      bool allDone = false;
      do {
        for (auto& task : stage) {
          auto split = getNextSplit();
          if (!split.hasConnectorSplit()) {
            allDone = true;
            break;
          }
          task->addSplit(scan->id(), std::move(split));
        }
      } while (!allDone);

      for (auto& task : stage) {
        task->noMoreSplits(scan->id());
      }
    }

    for (const auto& input : fragment.inputStages) {
      const auto [sourceStage, broadcast] = stageMap[input.producerTaskPrefix];

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
        task->noMoreSplits(input.consumerNodeId);
      }
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

std::string LocalRunner::printPlanWithStats(
    const std::function<void(
        const velox::core::PlanNodeId& nodeId,
        const std::string& indentation,
        std::ostream& out)>& addContext) const {
  folly::F14FastSet<velox::core::PlanNodeId> leafNodeIds;
  for (const auto& fragment : fragments_) {
    for (const auto& nodeId : fragment.fragment.planNode->leafPlanNodeIds()) {
      leafNodeIds.insert(nodeId);
    }
  }

  const auto taskStats = stats();
  folly::F14FastMap<velox::core::PlanNodeId, std::string> planNodeStats;
  for (const auto& stats : taskStats) {
    auto planStats = velox::exec::toPlanStats(stats);
    for (const auto& [id, nodeStats] : planStats) {
      planNodeStats[id] = nodeStats.toString(leafNodeIds.contains(id));
    }
  }

  return plan_->toString(
      true, [&](const auto& planNodeId, const auto& indentation, auto& out) {
        addContext(planNodeId, indentation, out);

        auto statsIt = planNodeStats.find(planNodeId);
        if (statsIt != planNodeStats.end()) {
          out << indentation << statsIt->second << std::endl;
        }
      });
}

} // namespace facebook::axiom::runner
