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
#include <gtest/gtest.h>
#include <cstdint>
#include <string>
#include <vector>
#include "axiom/optimizer/MultiFragmentPlan.h"
#include "axiom/runner/QueryProgress.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/TaskStats.h"

namespace facebook::axiom::runner {
namespace {

namespace optimizer = facebook::axiom::optimizer;
namespace velox = facebook::velox;

using StageTopology = QueryProgressBuilder::StageTopology;

optimizer::ExecutableFragment fragment(
    const std::string& taskPrefix,
    const std::vector<std::string>& producerTaskPrefixes) {
  std::vector<optimizer::InputStage> inputStages;
  inputStages.reserve(producerTaskPrefixes.size());
  for (size_t i = 0; i < producerTaskPrefixes.size(); ++i) {
    // Each producer feeds its own exchange node, so give every input a distinct
    // consumerNodeId rather than sharing one.
    inputStages.push_back(
        {.consumerNodeId = taskPrefix + "_exchange_" + std::to_string(i),
         .producerTaskPrefix = producerTaskPrefixes[i]});
  }
  return optimizer::ExecutableFragment{
      .taskPrefix = taskPrefix, .inputStages = std::move(inputStages)};
}

velox::exec::OperatorStats op(
    const std::string& operatorType,
    int64_t rows,
    int64_t bytes,
    int64_t cpuNanos) {
  velox::exec::OperatorStats stats;
  stats.operatorType = operatorType;
  stats.rawInputPositions = rows;
  stats.rawInputBytes = bytes;
  if (cpuNanos != 0) {
    velox::RuntimeMetric metric;
    metric.sum = cpuNanos;
    metric.count = 1;
    stats.runtimeStats[velox::exec::OperatorStats::kDriverCpuTime] = metric;
  }
  return stats;
}

// Wires `operators` into a single pipeline on a designated-initialized
// TaskStats. PipelineStats is not an aggregate, so the pipeline is built here
// while the split and driver counts are set inline at the call site.
velox::exec::TaskStats task(
    velox::exec::TaskStats stats,
    std::vector<velox::exec::OperatorStats> operators) {
  velox::exec::PipelineStats pipeline(
      /*inputPipeline=*/true, /*outputPipeline=*/true);
  pipeline.operatorStats = std::move(operators);
  stats.pipelineStats.push_back(std::move(pipeline));
  return stats;
}

TEST(QueryProgressBuilderTest, toStageTopologyLinearChain) {
  const auto stages = QueryProgressBuilder::toStageTopology(
      {fragment("leaf", {}),
       fragment("mid", {"leaf"}),
       fragment("root", {"mid"})});

  ASSERT_EQ(stages.size(), 3);
  EXPECT_TRUE(stages[0].producers.empty());
  EXPECT_EQ(stages[1].producers, std::vector<int32_t>{0});
  EXPECT_EQ(stages[2].producers, std::vector<int32_t>{1});
}

TEST(QueryProgressBuilderTest, toStageTopologyThrowsOnUnknownProducer) {
  // An exchange input naming a fragment that does not exist is a malformed
  // plan; the builder fails loudly rather than dropping the edge.
  VELOX_ASSERT_THROW(
      QueryProgressBuilder::toStageTopology(
          {fragment("a", {}), fragment("b", {"a", "ghost"})}),
      "Exchange input names an unknown producer task prefix. stage: 1, prefix: ghost");
}

TEST(QueryProgressBuilderTest, splitsScanAndShuffleBySourceOperator) {
  const std::vector<StageTopology> stages = {StageTopology{}};
  const std::vector<velox::exec::TaskStats> taskStats = {task(
      {.numTotalSplits = 4,
       .numFinishedSplits = 1,
       .numRunningSplits = 2,
       .numQueuedSplits = 1,
       .numTotalDrivers = 4,
       .numRunningDrivers = 2},
      {op("TableScan", 100, 1'000, 5'000),
       op("Exchange", 40, 400, 3'000),
       op("FilterProject", 0, 0, 2'000)})};

  const auto progress = QueryProgressBuilder::computeProgress(
      stages,
      taskStats,
      "q",
      /*wallMicros=*/0,
      /*finished=*/false);

  // Scan counters come from TableScan, shuffle from Exchange; the non-source
  // FilterProject reports no raw input and contributes only CPU.
  EXPECT_EQ(progress.stats.scanRows, 100);
  EXPECT_EQ(progress.stats.scanBytes, 1'000);
  EXPECT_EQ(progress.stats.shuffleRows, 40);
  EXPECT_EQ(progress.stats.shuffleBytes, 400);
  // CPU is summed across every operator: (5'000 + 3'000 + 2'000) / 1'000.
  EXPECT_EQ(progress.stats.cpuTimeMicros, 10);
  EXPECT_EQ(progress.stats.totalSplits, 4);
  EXPECT_EQ(progress.stats.completedSplits, 1);
  EXPECT_EQ(progress.stats.runningSplits, 2);
  EXPECT_EQ(progress.stats.queuedSplits, 1);
  EXPECT_EQ(progress.stats.state, ExecutionState::kRunning);

  ASSERT_EQ(progress.stages.size(), 1);
  EXPECT_TRUE(progress.stages[0].producers.empty());
  EXPECT_EQ(progress.stages[0].stats.scanRows, 100);
  EXPECT_EQ(progress.stages[0].stats.shuffleRows, 40);
}

TEST(QueryProgressBuilderTest, cpuConvertedOncePerQueryNotPerStage) {
  const std::vector<StageTopology> stages = {StageTopology{}, StageTopology{}};
  const std::vector<velox::exec::TaskStats> taskStats = {
      task(
          {.numTotalDrivers = 1, .numRunningDrivers = 1},
          {op("TableScan", 0, 0, 1'500)}),
      task(
          {.numTotalDrivers = 1, .numRunningDrivers = 1},
          {op("TableScan", 0, 0, 1'500)})};

  const auto progress =
      QueryProgressBuilder::computeProgress(stages, taskStats, "q", 0, false);

  // Each stage truncates 1'500ns to 1us, but the query sums nanoseconds first:
  // (1'500 + 1'500) / 1'000 = 3us, not 1 + 1 = 2us.
  EXPECT_EQ(progress.stages[0].stats.cpuTimeMicros, 1);
  EXPECT_EQ(progress.stages[1].stats.cpuTimeMicros, 1);
  EXPECT_EQ(progress.stats.cpuTimeMicros, 3);
}

TEST(QueryProgressBuilderTest, plannedWhenNothingStarted) {
  const std::vector<StageTopology> stages = {StageTopology{}};
  const std::vector<velox::exec::TaskStats> taskStats = {task(
      {.numTotalSplits = 4, .numQueuedSplits = 4, .numTotalDrivers = 4},
      {op("TableScan", 0, 0, 0)})};

  const auto progress =
      QueryProgressBuilder::computeProgress(stages, taskStats, "q", 0, false);

  EXPECT_EQ(progress.stats.state, ExecutionState::kPlanned);
  EXPECT_EQ(progress.stages[0].stats.state, ExecutionState::kPlanned);
}

TEST(QueryProgressBuilderTest, runningWhenDriversRunning) {
  const std::vector<StageTopology> stages = {StageTopology{}};
  const std::vector<velox::exec::TaskStats> taskStats = {task(
      {.numTotalDrivers = 4, .numRunningDrivers = 2}, {op("Values", 0, 0, 0)})};

  const auto progress =
      QueryProgressBuilder::computeProgress(stages, taskStats, "q", 0, false);

  EXPECT_EQ(progress.stats.state, ExecutionState::kRunning);
  EXPECT_EQ(progress.stages[0].stats.state, ExecutionState::kRunning);
}

TEST(QueryProgressBuilderTest, splitsCompleteWithoutFinishFlagStayRunning) {
  // Snapshot completion (all known splits done) does not infer kFinished -- the
  // runner adds splits over time, so completion comes only from its finished
  // flag.
  const std::vector<StageTopology> stages = {StageTopology{}};
  const std::vector<velox::exec::TaskStats> taskStats = {task(
      {.numTotalSplits = 10, .numFinishedSplits = 10},
      {op("TableScan", 100, 1'000, 0)})};

  const auto progress = QueryProgressBuilder::computeProgress(
      stages, taskStats, "q", 0, /*finished=*/false);

  EXPECT_EQ(progress.stats.state, ExecutionState::kRunning);
}

TEST(QueryProgressBuilderTest, driversCompleteWithoutFinishFlagStayRunning) {
  // A scan-less query whose drivers are all done still reports kRunning until
  // the runner's finished flag is set, though a single stage may be finished.
  const std::vector<StageTopology> stages = {StageTopology{}};
  const std::vector<velox::exec::TaskStats> taskStats = {task(
      {.numTotalDrivers = 4, .numCompletedDrivers = 4},
      {op("Values", 0, 0, 0)})};

  const auto progress = QueryProgressBuilder::computeProgress(
      stages, taskStats, "q", 0, /*finished=*/false);

  EXPECT_EQ(progress.stats.totalSplits, 0);
  EXPECT_EQ(progress.stats.state, ExecutionState::kRunning);
  EXPECT_EQ(progress.stages[0].stats.state, ExecutionState::kFinished);
}

TEST(QueryProgressBuilderTest, stageWithSomeDriversDoneIsRunning) {
  // Some drivers finished, none running, not all done: the stage is kRunning,
  // not kPlanned -- mirrors the query-level rule that any started work runs.
  const std::vector<StageTopology> stages = {StageTopology{}};
  const std::vector<velox::exec::TaskStats> taskStats = {task(
      {.numTotalDrivers = 4, .numCompletedDrivers = 2, .numRunningDrivers = 0},
      {op("Values", 0, 0, 0)})};

  const auto progress = QueryProgressBuilder::computeProgress(
      stages, taskStats, "q", 0, /*finished=*/false);

  EXPECT_EQ(progress.stages[0].stats.state, ExecutionState::kRunning);
}

TEST(QueryProgressBuilderTest, finishedFlagOverridesRunningCounts) {
  const std::vector<StageTopology> stages = {StageTopology{}};
  const std::vector<velox::exec::TaskStats> taskStats = {task(
      {.numTotalSplits = 4,
       .numRunningSplits = 2,
       .numTotalDrivers = 4,
       .numRunningDrivers = 2},
      {op("TableScan", 50, 500, 0)})};

  const auto progress = QueryProgressBuilder::computeProgress(
      stages, taskStats, "q", 0, /*finished=*/true);

  // The finish flag wins over the running counts, and the frame is normalized
  // so neither the query nor its stages show running/queued splits.
  EXPECT_EQ(progress.stats.state, ExecutionState::kFinished);
  EXPECT_EQ(progress.stats.completedSplits, 4);
  EXPECT_EQ(progress.stats.runningSplits, 0);
  EXPECT_EQ(progress.stats.queuedSplits, 0);
  ASSERT_EQ(progress.stages.size(), 1);
  EXPECT_EQ(progress.stages[0].stats.state, ExecutionState::kFinished);
  EXPECT_EQ(progress.stages[0].stats.completedSplits, 4);
  EXPECT_EQ(progress.stages[0].stats.runningSplits, 0);
}

TEST(QueryProgressBuilderTest, computeProgressThrowsOnTopologySizeMismatch) {
  // Non-empty topology must be positionally 1:1 with the snapshot; a size
  // mismatch is a bug, not a recoverable state to paper over.
  const std::vector<StageTopology> stages = {
      StageTopology{}, StageTopology{.producers = {0}}};
  const std::vector<velox::exec::TaskStats> taskStats = {task(
      {.numTotalSplits = 2, .numFinishedSplits = 1},
      {op("Exchange", 7, 70, 0)})};

  VELOX_ASSERT_THROW(
      QueryProgressBuilder::computeProgress(stages, taskStats, "q", 0, false),
      "Stage topology and task-stats snapshot disagree in size. topology: 2, snapshot: 1");
}

TEST(QueryProgressBuilderTest, computeProgressThrowsOnUnclassifiedSource) {
  // A source operator outside the scan/exchange buckets that reports raw input
  // would silently drop rows/bytes from the totals; the builder fails loudly.
  const std::vector<StageTopology> stages = {StageTopology{}};
  const std::vector<velox::exec::TaskStats> taskStats = {task(
      {.numTotalDrivers = 1, .numRunningDrivers = 1},
      {op("SomeNewSource", 5, 50, 0)})};

  VELOX_ASSERT_THROW(
      QueryProgressBuilder::computeProgress(stages, taskStats, "q", 0, false),
      "Unclassified source operator reports input. operator: SomeNewSource, rows: 5, bytes: 50");
}

TEST(QueryProgressBuilderTest, multiStageSumsWithResolvedTopology) {
  const auto stages = QueryProgressBuilder::toStageTopology(
      {fragment("leaf", {}), fragment("root", {"leaf"})});
  const std::vector<velox::exec::TaskStats> taskStats = {
      task(
          {.numTotalSplits = 8,
           .numFinishedSplits = 8,
           .numTotalDrivers = 2,
           .numCompletedDrivers = 2},
          {op("TableScan", 1'000, 8'000, 4'000)}),
      task(
          {.numTotalDrivers = 1, .numCompletedDrivers = 1},
          {op("Exchange", 1'000, 8'000, 2'000)})};

  const auto progress = QueryProgressBuilder::computeProgress(
      stages, taskStats, "q", /*wallMicros=*/777, /*finished=*/false);

  EXPECT_EQ(progress.wallTimeMicros, 777);
  EXPECT_EQ(progress.stats.scanRows, 1'000);
  EXPECT_EQ(progress.stats.shuffleRows, 1'000);
  EXPECT_EQ(progress.stats.cpuTimeMicros, 6); // (4'000 + 2'000) / 1'000.
  EXPECT_EQ(progress.stats.totalSplits, 8);
  EXPECT_EQ(progress.stats.state, ExecutionState::kRunning);

  ASSERT_EQ(progress.stages.size(), 2);
  EXPECT_TRUE(progress.stages[0].producers.empty());
  ASSERT_EQ(progress.stages[1].producers.size(), 1);
  EXPECT_EQ(progress.stages[1].producers[0], 0);
  EXPECT_EQ(progress.stages[0].stats.scanRows, 1'000);
  EXPECT_EQ(progress.stages[1].stats.shuffleRows, 1'000);
}

} // namespace
} // namespace facebook::axiom::runner
