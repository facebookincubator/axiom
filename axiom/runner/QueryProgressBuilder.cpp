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

#include "axiom/runner/QueryProgressBuilder.h"

#include <folly/container/F14Map.h>

#include "axiom/optimizer/MultiFragmentPlan.h"
#include "velox/common/base/Exceptions.h"
#include "velox/exec/OperatorType.h"
#include "velox/exec/TaskStats.h"

namespace velox = facebook::velox;
namespace optimizer = facebook::axiom::optimizer;

namespace facebook::axiom::runner {
namespace {

// Normalizes a finished frame so it is internally consistent -- nothing left
// queued or running.
void markFinished(ExecutionStats& stats) {
  stats.state = ExecutionState::kFinished;
  stats.completedSplits = stats.totalSplits;
  stats.runningSplits = 0;
  stats.queuedSplits = 0;
}

} // namespace

std::vector<QueryProgressBuilder::StageTopology>
QueryProgressBuilder::toStageTopology(
    const std::vector<optimizer::ExecutableFragment>& fragments) {
  // Map each fragment's task prefix to its index so the exchange inputs (named
  // by producer task prefix) resolve to stage indexes.
  folly::F14FastMap<std::string, int32_t> indexByPrefix;
  indexByPrefix.reserve(fragments.size());
  for (size_t i = 0; i < fragments.size(); ++i) {
    indexByPrefix.emplace(fragments[i].taskPrefix, static_cast<int32_t>(i));
  }

  std::vector<StageTopology> stages;
  stages.reserve(fragments.size());
  for (size_t i = 0; i < fragments.size(); ++i) {
    StageTopology stage;
    for (const auto& input : fragments[i].inputStages) {
      const auto it = indexByPrefix.find(input.producerTaskPrefix);
      // Every exchange input must resolve to a known producer fragment.
      VELOX_CHECK(
          it != indexByPrefix.end(),
          "Exchange input names an unknown producer task prefix. stage: {}, prefix: {}",
          i,
          input.producerTaskPrefix);
      stage.producers.push_back(it->second);
    }
    stages.push_back(std::move(stage));
  }
  return stages;
}

QueryProgress QueryProgressBuilder::computeProgress(
    const std::vector<StageTopology>& stages,
    const std::vector<velox::exec::TaskStats>& taskStats,
    std::string_view queryId,
    int64_t wallMicros,
    bool finished) {
  // The topology is positionally 1:1 with the snapshot by fragment order.
  VELOX_CHECK_EQ(
      stages.size(),
      taskStats.size(),
      "Stage topology and task-stats snapshot disagree in size. topology: {}, snapshot: {}",
      stages.size(),
      taskStats.size());

  ExecutionStats queryStats;
  // Driver counts distinguish a started/in-flight scan-less query (no splits)
  // from one that has not begun. Typed to match TaskStats' unsigned counts;
  // only their > 0 state is read.
  uint64_t completedDrivers{0};
  uint64_t runningDrivers{0};
  // CPU is summed in nanoseconds and converted once; a per-operator /1'000
  // would truncate sub-microsecond remainders.
  int64_t queryCpuNanos{0};

  std::vector<StageProgress> stageProgress;
  stageProgress.reserve(taskStats.size());
  for (size_t i = 0; i < taskStats.size(); ++i) {
    const auto& task = taskStats[i];

    ExecutionStats stats;
    stats.totalSplits = task.numTotalSplits;
    stats.queuedSplits = task.numQueuedSplits;
    stats.runningSplits = task.numRunningSplits;
    stats.completedSplits = task.numFinishedSplits;

    int64_t stageCpuNanos{0};
    for (const auto& pipeline : task.pipelineStats) {
      for (const auto& op : pipeline.operatorStats) {
        if (op.operatorType == velox::exec::OperatorType::kTableScan) {
          stats.scanRows += op.rawInputPositions;
          stats.scanBytes += op.rawInputBytes;
        } else if (
            op.operatorType == velox::exec::OperatorType::kExchange ||
            op.operatorType == velox::exec::OperatorType::kMergeExchange) {
          stats.shuffleRows += op.rawInputPositions;
          stats.shuffleBytes += op.rawInputBytes;
        } else {
          // Only scan and exchange sources contribute raw input; any other
          // source reporting raw input would be dropped from the totals.
          VELOX_CHECK(
              op.rawInputPositions == 0 && op.rawInputBytes == 0,
              "Unclassified source operator reports input. operator: {}, rows: {}, bytes: {}",
              op.operatorType,
              op.rawInputPositions,
              op.rawInputBytes);
        }
        const auto it =
            op.runtimeStats.find(velox::exec::OperatorStats::kDriverCpuTime);
        if (it != op.runtimeStats.end()) {
          stageCpuNanos += it->second.sum;
        }
      }
    }
    stats.cpuTimeMicros = stageCpuNanos / 1'000;

    if (task.numTotalDrivers > 0 &&
        task.numCompletedDrivers == task.numTotalDrivers) {
      stats.state = ExecutionState::kFinished;
    } else if (task.numRunningDrivers > 0 || task.numCompletedDrivers > 0) {
      // Any started driver makes the stage kRunning, matching the query-level
      // rule; a stage with some but not all drivers done is not kPlanned.
      stats.state = ExecutionState::kRunning;
    }

    queryStats.scanRows += stats.scanRows;
    queryStats.scanBytes += stats.scanBytes;
    queryStats.shuffleRows += stats.shuffleRows;
    queryStats.shuffleBytes += stats.shuffleBytes;
    queryStats.totalSplits += stats.totalSplits;
    queryStats.queuedSplits += stats.queuedSplits;
    queryStats.runningSplits += stats.runningSplits;
    queryStats.completedSplits += stats.completedSplits;
    queryCpuNanos += stageCpuNanos;
    completedDrivers += task.numCompletedDrivers;
    runningDrivers += task.numRunningDrivers;

    StageProgress stage;
    stage.producers = stages[i].producers;
    stage.stats = stats;
    stageProgress.push_back(std::move(stage));
  }
  queryStats.cpuTimeMicros = queryCpuNanos / 1'000;

  // kFinished comes only from the runner's finished flag; otherwise the query
  // is kRunning once any split or driver has started, else kPlanned.
  if (finished) {
    queryStats.state = ExecutionState::kFinished;
  } else if (
      queryStats.runningSplits > 0 || queryStats.completedSplits > 0 ||
      runningDrivers > 0 || completedDrivers > 0) {
    queryStats.state = ExecutionState::kRunning;
  }

  // On a clean finish, make the frame internally consistent so no per-stage row
  // shows running/queued splits in a query reported as finished.
  if (finished) {
    markFinished(queryStats);
    for (auto& stage : stageProgress) {
      markFinished(stage.stats);
    }
  }

  QueryProgress progress;
  progress.queryId = std::string(queryId);
  progress.stats = queryStats;
  progress.wallTimeMicros = wallMicros;
  progress.stages = std::move(stageProgress);
  return progress;
}

} // namespace facebook::axiom::runner
