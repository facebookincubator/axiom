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
#include <folly/CancellationToken.h>
#include <folly/OperationCancelled.h>
#include <folly/coro/AsyncGenerator.h>
#include <folly/coro/Baton.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Task.h>
#include <folly/coro/WithCancellation.h>
#include <folly/synchronization/Baton.h>
#include <thread>
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
      std::string_view project = "c0",
      bool broadcastBuild = false) {
    optimizer::MultiFragmentPlan::Options options = {
        .queryId = makeQueryId(), .numWorkers = 4, .numDrivers = 2};
    const int32_t width = 3;

    test::DistributedPlanBuilder rootBuilder(
        options, idGenerator_, pool_.get());
    rootBuilder.tableScan("t", rowType_)
        .project({std::string{project}})
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

  static axiom::runner::RunnerSessionPtr makeRunnerSession(
      std::string_view queryId) {
    return std::make_shared<axiom::runner::RunnerSession>(
        std::string(queryId),
        "test",
        axiom::runner::Properties{},
        axiom::connector::ConnectorProperties{});
  }

  template <typename RunnerT = LocalRunner>
  std::shared_ptr<RunnerT> makeRunner(optimizer::MultiFragmentPlanPtr plan) {
    const auto queryId = plan->options().queryId;
    return std::make_shared<RunnerT>(
        makeRunnerSession(queryId),
        std::move(plan),
        optimizer::FinishWrite{},
        makeQueryCtx(queryId),
        std::make_shared<ConnectorSplitSourceFactory>(runtimeStats_),
        /*outputPool=*/nullptr,
        /*baseSpillDirectory=*/"",
        runtimeStats_);
  }

  std::shared_ptr<velox::core::PlanNodeIdGenerator> idGenerator_{
      std::make_shared<velox::core::PlanNodeIdGenerator>()};

  int32_t queryCounter_{0};
  QueryRuntimeStats runtimeStats_;

  velox::RowTypePtr rowType_;
};

int64_t extractSingleInt64(const std::vector<velox::RowVectorPtr>& vectors) {
  return vectors.at(0)->childAt(0)->as<velox::FlatVector<int64_t>>()->valueAt(
      0);
}

// A LocalRunner whose co_close() throws after the real reap, to exercise
// drain()'s policy that a reap failure must not mask the original stop reason.
class ReapFailingLocalRunner : public LocalRunner {
 public:
  using LocalRunner::LocalRunner;

  folly::coro::Task<void> co_close() override {
    co_await LocalRunner::co_close();
    VELOX_FAIL("injected reap failure");
  }
};

// A Runner whose execute() yields no batches and blocks until the awaiting
// scope is cancelled, so drain()'s deadline is the only thing that ends the
// run. Exercises the timeout deterministically, without depending on how long a
// real query happens to take.
class BlockingRunner : public Runner {
 public:
  folly::coro::AsyncGenerator<velox::RowVectorPtr> execute() override {
    const auto token = co_await folly::coro::co_current_cancellation_token;
    folly::coro::Baton baton;
    folly::CancellationCallback unblock{token, [&baton] { baton.post(); }};
    co_await baton;
    // Surface the deadline the way LocalRunner surfaces a cancel, so drain()'s
    // folly::coro::timeout classifies it as a timeout.
    throw folly::OperationCancelled{};
  }

  folly::coro::Task<void> co_close() override {
    co_return;
  }

  // Only execute() and co_close() are exercised by drain(); the rest of the
  // Runner interface is required but unused here.
  std::vector<velox::exec::TaskStats> stats() const override {
    VELOX_UNREACHABLE();
  }

  const std::vector<optimizer::ExecutableFragment>& fragments() const override {
    VELOX_UNREACHABLE();
  }

  State state() const override {
    VELOX_UNREACHABLE();
  }
};

TEST_F(LocalRunnerTest, count) {
  auto join = makeJoinPlan();
  auto localRunner = makeRunner(join);

  std::vector<velox::RowVectorPtr> results;
  localRunner->drain(
      [&](velox::RowVectorPtr batch) { results.push_back(std::move(batch)); });
  auto stats = localRunner->stats();
  EXPECT_EQ(1, results.size());
  EXPECT_EQ(1, results[0]->size());
  EXPECT_EQ(kNumRows, extractSingleInt64(results));
  results.clear();
  EXPECT_EQ(Runner::State::kFinished, localRunner->state());
}

// execute() yields all result batches and reports kFinished on completion.
TEST_F(LocalRunnerTest, execute) {
  auto join = makeJoinPlan();
  auto localRunner = makeRunner(join);

  std::vector<velox::RowVectorPtr> results;
  folly::coro::blockingWait([&]() -> folly::coro::Task<void> {
    auto generator = localRunner->execute();
    while (auto rows = co_await generator.next()) {
      results.push_back(std::move(*rows));
    }
  }());
  // Mandatory owner-scoped wind-down before the runner is destroyed.
  folly::coro::blockingWait(localRunner->co_close());

  EXPECT_EQ(1, results.size());
  EXPECT_EQ(kNumRows, extractSingleInt64(results));
  results.clear();
  EXPECT_EQ(Runner::State::kFinished, localRunner->state());
}

// Cancelling the awaiting scope interrupts execute() and surfaces the error;
// the runner still terminates cleanly. A pre-cancelled token makes the abort
// deterministic.
TEST_F(LocalRunnerTest, executeCancellation) {
  auto scan = makeScanPlan(/*numWorkers=*/3);
  auto localRunner = makeRunner(scan);

  folly::CancellationSource source;
  source.requestCancellation();

  auto drainLoop = [&]() -> folly::coro::Task<void> {
    auto generator = localRunner->execute();
    while (co_await generator.next()) {
    }
  };
  // Cancellation surfaces as cooperative cancellation
  // (folly::OperationCancelled), not a VeloxRuntimeError shaped like a genuine
  // failure.
  EXPECT_THROW(
      folly::coro::blockingWait(
          folly::coro::co_withCancellation(source.getToken(), drainLoop())),
      folly::OperationCancelled);
  folly::coro::blockingWait(localRunner->co_close());

  EXPECT_EQ(Runner::State::kCancelled, localRunner->state());
}

// The cancellation callback may fire on a thread other than the one awaiting
// execute(). Here a separate thread requests cancellation (which runs the
// registered cancelTasks() synchronously on that thread) while the drain is
// parked between batches; the next pull must surface the cross-thread
// cancellation, and teardown stays clean.
TEST_F(LocalRunnerTest, executeCancellationFromAnotherThread) {
  auto scan = makeScanPlan(/*numWorkers=*/1);
  auto localRunner = makeRunner(scan);

  folly::CancellationSource source;
  folly::Baton<> gotBatch;
  folly::Baton<> cancelled;

  std::thread canceller([&] {
    gotBatch.wait();
    // requestCancellation() invokes the registered callback (cancelTasks())
    // inline on this thread, so the cancellation genuinely races the awaiting
    // thread.
    source.requestCancellation();
    cancelled.post();
  });

  auto drainLoop = [&]() -> folly::coro::Task<void> {
    auto generator = localRunner->execute();
    auto first = co_await generator.next();
    EXPECT_TRUE(first.has_value());
    gotBatch.post();
    cancelled.wait();
    while (co_await generator.next()) {
    }
  };
  EXPECT_THROW(
      folly::coro::blockingWait(
          folly::coro::co_withCancellation(source.getToken(), drainLoop())),
      folly::OperationCancelled);
  canceller.join();
  folly::coro::blockingWait(localRunner->co_close());

  EXPECT_EQ(Runner::State::kCancelled, localRunner->state());
}

// Early stop is "stop pulling, then co_close()": after one batch the caller
// drops the generator (which no longer reaps on its own) and awaits co_close(),
// which cancels the still-running work and reaps without hanging. The runner
// reaches a terminal kCancelled state.
TEST_F(LocalRunnerTest, earlyStopClosesAndReaps) {
  auto scan = makeScanPlan(/*numWorkers=*/3);
  auto localRunner = makeRunner(scan);

  {
    auto gen = localRunner->execute();
    auto first = folly::coro::blockingWait(gen.next());
    EXPECT_TRUE(first.has_value());
    // Stop pulling; 'gen' goes out of scope. Dropping it does not reap.
  }
  // The still-running run is wound down and reaped here.
  folly::coro::blockingWait(localRunner->co_close());

  EXPECT_EQ(Runner::State::kCancelled, localRunner->state());
}

TEST_F(LocalRunnerTest, error) {
  auto join = makeJoinPlan("if (c0 = 111, c0 / 0, c0 + 1) as c0");
  auto localRunner = makeRunner(join);

  std::vector<velox::RowVectorPtr> results;
  VELOX_ASSERT_THROW(
      localRunner->drain([&](velox::RowVectorPtr batch) {
        results.push_back(std::move(batch));
      }),
      "division by zero");
  EXPECT_EQ(Runner::State::kError, localRunner->state());
}

// drain(..., timeoutMicros) fails with a user error when execution overruns the
// cooperative deadline. BlockingRunner blocks until the deadline cancels it, so
// the timeout is the sole exit and the test is deterministic regardless of
// platform or query speed.
TEST_F(LocalRunnerTest, drainTimeout) {
  BlockingRunner runner;
  VELOX_ASSERT_THROW(
      runner.drain([](velox::RowVectorPtr) {}, /*timeoutMicros=*/1'000),
      "exceeded maximum time limit");
}

// A reap (co_close) failure must not mask the original stop reason: the genuine
// execution error surfaces from drain() while the secondary reap failure is
// logged, not thrown.
TEST_F(LocalRunnerTest, drainReapFailureDoesNotMaskError) {
  auto join = makeJoinPlan("if (c0 = 111, c0 / 0, c0 + 1) as c0");
  auto localRunner = makeRunner<ReapFailingLocalRunner>(join);

  VELOX_ASSERT_THROW(
      localRunner->drain([](velox::RowVectorPtr) {}), "division by zero");
  EXPECT_EQ(Runner::State::kError, localRunner->state());
}

TEST_F(LocalRunnerTest, scan) {
  auto checkScanCount = [&](int32_t numWorkers) {
    auto scan = makeScanPlan(numWorkers);
    auto localRunner = makeRunner(scan);

    {
      int32_t count = 0;
      auto generator = localRunner->execute();
      while (auto rows = folly::coro::blockingWait(generator.next())) {
        count += (*rows)->size();
      }
      folly::coro::blockingWait(localRunner->co_close());
      EXPECT_EQ(kNumRows, count);
    }
  };

  checkScanCount(1);
  checkScanCount(3);
}

TEST_F(LocalRunnerTest, broadcast) {
  auto join = makeJoinPlan("c0", true);
  auto localRunner = makeRunner(join);

  std::vector<velox::RowVectorPtr> results;
  localRunner->drain(
      [&](velox::RowVectorPtr batch) { results.push_back(std::move(batch)); });
  auto stats = localRunner->stats();
  EXPECT_EQ(1, results.size());
  EXPECT_EQ(1, results[0]->size());
  EXPECT_EQ(kNumRows, extractSingleInt64(results));
  results.clear();
  EXPECT_EQ(Runner::State::kFinished, localRunner->state());
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
  rootBuilder.addNode([&](const auto&, auto) { return probe; })
      .hashJoin({"c0"}, {"b0"}, build, "", {"c0", "b0"});

  auto plan = rootBuilder.build();

  auto localRunner = makeRunner(plan);

  size_t numRows = 0;
  auto generator = localRunner->execute();
  while (auto rows = folly::coro::blockingWait(generator.next())) {
    numRows += (*rows)->size();
  }
  folly::coro::blockingWait(localRunner->co_close());

  EXPECT_EQ(kNumRows, numRows);
  EXPECT_EQ(Runner::State::kFinished, localRunner->state());
}

TEST_F(LocalRunnerTest, spillDirectoryWiring) {
  auto spillDir = velox::common::testutil::TempDirectoryPath::create();

  auto join = makeJoinPlan();
  const auto queryId = join->options().queryId;
  auto queryCtx = makeQueryCtx(queryId);

  auto localRunner = std::make_shared<LocalRunner>(
      makeRunnerSession(queryId),
      std::move(join),
      optimizer::FinishWrite{},
      std::move(queryCtx),
      std::make_shared<ConnectorSplitSourceFactory>(runtimeStats_),
      /*outputPool=*/nullptr,
      spillDir->getPath(),
      runtimeStats_);

  std::vector<velox::RowVectorPtr> results;
  localRunner->drain(
      [&](velox::RowVectorPtr batch) { results.push_back(std::move(batch)); });
  EXPECT_EQ(1, results.size());
  EXPECT_EQ(kNumRows, extractSingleInt64(results));
  EXPECT_EQ(Runner::State::kFinished, localRunner->state());
}

} // namespace
} // namespace facebook::axiom::runner
