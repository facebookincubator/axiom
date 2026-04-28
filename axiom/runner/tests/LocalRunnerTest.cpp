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
#include "axiom/runner/tests/DistributedPlanBuilder.h"
#include "axiom/runner/tests/LocalRunnerTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::runner {
namespace {

using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

class LocalRunnerTest : public test::LocalRunnerTestBase {
 public:
  static constexpr int32_t kNumFiles = 5;
  static constexpr int32_t kNumVectors = 5;
  static constexpr int32_t kRowsPerVector = 10000;
  static constexpr int32_t kNumRows = kNumFiles * kNumVectors * kRowsPerVector;

  static void makeAscending(const velox::RowVectorPtr& rows, int32_t& counter) {
    auto ints = rows->childAt(0)->as<velox::FlatVector<int64_t>>();
    for (auto i = 0; i < ints->size(); ++i) {
      ints->set(i, counter + i);
    }
    counter += ints->size();
  }

  static void makeDescending(
      const velox::RowVectorPtr& rows,
      int32_t& counter) {
    auto ints = rows->childAt(0)->as<velox::FlatVector<int64_t>>();
    for (auto i = 0; i < ints->size(); ++i) {
      ints->set(i, counter - i);
    }
    counter -= ints->size();
  }

  void SetUp() override {
    rowType_ = velox::ROW({"c0"}, {velox::BIGINT()});

    int32_t counter1 = 0;
    int32_t counter2 = kNumRows - 1;

    // makeTables() is a no-op after the first call.
    makeTables(
        {test::TableSpec{
             .name = "t",
             .columns = rowType_,
             .rowsPerVector = kRowsPerVector,
             .numVectorsPerFile = kNumVectors,
             .numFiles = kNumFiles,
             .customizeData =
                 [&counter1](const velox::RowVectorPtr& rows) {
                   makeAscending(rows, counter1);
                 }},
         test::TableSpec{
             .name = "u",
             .columns = rowType_,
             .rowsPerVector = kRowsPerVector,
             .numVectorsPerFile = kNumVectors,
             .numFiles = kNumFiles,
             .customizeData = [&counter2](const velox::RowVectorPtr& rows) {
               makeDescending(rows, counter2);
             }}});

    LocalRunnerTestBase::SetUp();
  }

  // Returns a plan with a table scan. This is a single stage if 'numWorkers' is
  // 1, otherwise this is a scan stage plus shuffle to a stage that gathers the
  // scan results.
  optimizer::MultiFragmentPlanPtr makeScanPlan(int32_t numWorkers) {
    optimizer::MultiFragmentPlan::Options options = {
        .queryId = makeQueryId(), .numWorkers = numWorkers, .numDrivers = 2};

    test::DistributedPlanBuilder builder(options, idGenerator_, pool_.get());
    builder.tableScan("t", rowType_);
    if (numWorkers > 1) {
      builder.shufflePartitioned({}, 1, false);
    }
    return builder.build();
  }

  optimizer::MultiFragmentPlanPtr makeJoinPlan(
      std::string project = "c0",
      bool broadcastBuild = false) {
    optimizer::MultiFragmentPlan::Options options = {
        .queryId = makeQueryId(), .numWorkers = 4, .numDrivers = 2};
    const int32_t width = 3;

    test::DistributedPlanBuilder rootBuilder(
        options, idGenerator_, pool_.get());
    rootBuilder.tableScan("t", rowType_)
        .project({project})
        .shufflePartitioned({"c0"}, 3, false)
        .hashJoin(
            {"c0"},
            {"b0"},
            broadcastBuild
                ? test::DistributedPlanBuilder(rootBuilder)
                      .tableScan("u", rowType_)
                      .project({"c0 as b0"})
                      .shuffleBroadcastResult()
                : test::DistributedPlanBuilder(rootBuilder)
                      .tableScan("u", rowType_)
                      .project({"c0 as b0"})
                      .shufflePartitionedResult({"b0"}, width, false),
            "",
            {"c0", "b0"})
        .shufflePartitioned({}, 1, false)
        .localPartition({})
        .finalAggregation({}, {"count(1)"}, {{velox::BIGINT()}});

    return rootBuilder.build();
  }

  std::string makeQueryId() {
    return fmt::format("q{}", queryCounter_++);
  }

  std::shared_ptr<LocalRunner> makeRunner(
      optimizer::MultiFragmentPlanPtr plan) {
    const auto queryId = plan->options().queryId;

    return std::make_shared<LocalRunner>(
        std::move(plan), optimizer::FinishWrite{}, makeQueryCtx(queryId));
  }

  std::shared_ptr<LocalRunner> makeSerialRunner(
      optimizer::MultiFragmentPlanPtr plan) {
    const auto queryId = plan->options().queryId;

    auto queryCtx = velox::core::QueryCtx::create(
        /*executor=*/nullptr,
        velox::core::QueryConfig({}),
        {},
        velox::cache::AsyncDataCache::getInstance(),
        /*pool=*/nullptr,
        /*spillExecutor=*/nullptr,
        queryId);

    return std::make_shared<LocalRunner>(
        std::move(plan),
        optimizer::FinishWrite{},
        std::move(queryCtx),
        std::make_shared<runner::ConnectorSplitSourceFactory>(),
        /*outputPool=*/nullptr,
        /*baseSpillDirectory=*/"",
        velox::exec::Task::ExecutionMode::kSerial);
  }

  // Fetches all remaining data from the runner.
  static std::vector<velox::RowVectorPtr> readCursor(
      const std::shared_ptr<LocalRunner>& runner) {
    std::vector<velox::RowVectorPtr> result;
    while (auto rowVector = runner->next()) {
      result.push_back(rowVector);
    }
    return result;
  }

  std::shared_ptr<velox::core::PlanNodeIdGenerator> idGenerator_{
      std::make_shared<velox::core::PlanNodeIdGenerator>()};

  int32_t queryCounter_{0};

  velox::RowTypePtr rowType_;
};

int64_t extractSingleInt64(const std::vector<velox::RowVectorPtr>& vectors) {
  return vectors.at(0)->childAt(0)->as<velox::FlatVector<int64_t>>()->valueAt(
      0);
}

constexpr int32_t kWaitTimeoutUs = 500'000;

TEST_F(LocalRunnerTest, count) {
  auto join = makeJoinPlan();
  auto localRunner = makeRunner(join);

  auto results = readCursor(localRunner);
  auto stats = localRunner->stats();
  EXPECT_EQ(1, results.size());
  EXPECT_EQ(1, results[0]->size());
  EXPECT_EQ(kNumRows, extractSingleInt64(results));
  results.clear();
  EXPECT_EQ(Runner::State::kFinished, localRunner->state());
  ASSERT_TRUE(localRunner->waitForCompletion(kWaitTimeoutUs));
}

TEST_F(LocalRunnerTest, error) {
  auto join = makeJoinPlan("if (c0 = 111, c0 / 0, c0 + 1) as c0");
  auto localRunner = makeRunner(join);

  VELOX_ASSERT_THROW(readCursor(localRunner), "division by zero");
  EXPECT_EQ(Runner::State::kError, localRunner->state());
  ASSERT_TRUE(localRunner->waitForCompletion(kWaitTimeoutUs));
}

TEST_F(LocalRunnerTest, scan) {
  auto checkScanCount = [&](int32_t numWorkers) {
    auto scan = makeScanPlan(numWorkers);
    auto localRunner = makeRunner(scan);

    {
      auto results = readCursor(localRunner);

      int32_t count = 0;
      for (auto& rows : results) {
        count += rows->size();
      }
      EXPECT_EQ(kNumRows, count);
    }

    ASSERT_TRUE(localRunner->waitForCompletion(kWaitTimeoutUs));
  };

  checkScanCount(1);
  checkScanCount(3);
}

TEST_F(LocalRunnerTest, broadcast) {
  auto join = makeJoinPlan("c0", true);
  auto localRunner = makeRunner(join);

  auto results = readCursor(localRunner);
  auto stats = localRunner->stats();
  EXPECT_EQ(1, results.size());
  EXPECT_EQ(1, results[0]->size());
  EXPECT_EQ(kNumRows, extractSingleInt64(results));
  results.clear();
  EXPECT_EQ(Runner::State::kFinished, localRunner->state());
  ASSERT_TRUE(localRunner->waitForCompletion(kWaitTimeoutUs));
}

TEST_F(LocalRunnerTest, lastStageWithMultipleInputs) {
  optimizer::MultiFragmentPlan::Options options = {
      .queryId = "test.", .numWorkers = 1, .numDrivers = 1};

  test::DistributedPlanBuilder rootBuilder(options, idGenerator_, pool_.get());
  auto probe = test::DistributedPlanBuilder(rootBuilder)
                   .tableScan("t", rowType_)
                   .project({"c0"})
                   .shuffleBroadcastResult();
  auto build = test::DistributedPlanBuilder(rootBuilder)
                   .tableScan("u", rowType_)
                   .project({"c0 as b0"})
                   .shuffleBroadcastResult();
  rootBuilder.addNode([&](auto, auto) { return probe; })
      .hashJoin({"c0"}, {"b0"}, build, "", {"c0", "b0"});

  auto plan = rootBuilder.build();

  auto localRunner = makeRunner(plan);
  auto results = readCursor(localRunner);
  auto stats = localRunner->stats();

  size_t numRows = 0;
  for (const auto& result : results) {
    numRows += result->size();
  }

  EXPECT_EQ(kNumRows, numRows);

  results.clear();
  EXPECT_EQ(Runner::State::kFinished, localRunner->state());
  ASSERT_TRUE(localRunner->waitForCompletion(kWaitTimeoutUs));
}

TEST_F(LocalRunnerTest, spillDirectoryWiring) {
  auto spillDir = velox::common::testutil::TempDirectoryPath::create();

  auto join = makeJoinPlan();
  const auto queryId = join->options().queryId;
  auto queryCtx = makeQueryCtx(queryId);

  auto localRunner = std::make_shared<LocalRunner>(
      std::move(join),
      optimizer::FinishWrite{},
      std::move(queryCtx),
      std::make_shared<ConnectorSplitSourceFactory>(),
      /*outputPool=*/nullptr,
      spillDir->getPath());

  auto results = readCursor(localRunner);
  EXPECT_EQ(1, results.size());
  EXPECT_EQ(kNumRows, extractSingleInt64(results));
  results.clear();
  EXPECT_EQ(Runner::State::kFinished, localRunner->state());
  ASSERT_TRUE(localRunner->waitForCompletion(kWaitTimeoutUs));
}

TEST_F(LocalRunnerTest, serialScan) {
  auto scan = makeScanPlan(1);
  auto localRunner = makeSerialRunner(scan);

  auto results = readCursor(localRunner);

  int32_t count = 0;
  for (auto& rows : results) {
    count += rows->size();
  }
  EXPECT_EQ(kNumRows, count);
  EXPECT_EQ(Runner::State::kFinished, localRunner->state());
  ASSERT_TRUE(localRunner->waitForCompletion(kWaitTimeoutUs));
}

TEST_F(LocalRunnerTest, serialError) {
  optimizer::MultiFragmentPlan::Options options = {
      .queryId = makeQueryId(), .numWorkers = 1, .numDrivers = 1};
  test::DistributedPlanBuilder builder(options, idGenerator_, pool_.get());
  builder.tableScan("t", rowType_)
      .project({"if (c0 = 111, c0 / 0, c0 + 1) as c0"});

  auto localRunner = makeSerialRunner(builder.build());

  VELOX_ASSERT_THROW(readCursor(localRunner), "division by zero");
  EXPECT_EQ(Runner::State::kError, localRunner->state());
  ASSERT_TRUE(localRunner->waitForCompletion(kWaitTimeoutUs));
}

} // namespace
} // namespace facebook::axiom::runner
