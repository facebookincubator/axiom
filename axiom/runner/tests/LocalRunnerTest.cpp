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
#include <folly/coro/Cleanup.h>
#include <folly/coro/Collect.h>
#include <folly/coro/GtestHelpers.h>
#include <folly/coro/ScopeExit.h>
#include <folly/coro/Task.h>
#include <folly/coro/Timeout.h>
#include <folly/coro/WithCancellation.h>
#include <folly/synchronization/Baton.h>
#include <thread>
#include "axiom/connectors/tests/TestConnectorContext.h"
#include "axiom/runner/tests/DistributedPlanBuilder.h"
#include "axiom/runner/tests/LocalRunnerTestBase.h"
#include "velox/common/base/ConcurrentRuntimeStatWriter.h"
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
        .queryId = makeQueryId(),
        .maxRemotePartitions = numWorkers,
        .maxLocalPartitions = 2};

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
        .queryId = makeQueryId(),
        .maxRemotePartitions = 4,
        .maxLocalPartitions = 2};
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

  axiom::runner::RunnerSessionPtr makeRunnerSession(std::string_view queryId) {
    // The runner and its connectors record into separate writers.
    auto context = axiom::connector::ConnectorContext::createProcessWide(
        std::string(queryId),
        "test",
        axiom::connector::ConnectorProperties{},
        [this](
            std::string_view) -> std::shared_ptr<velox::BaseRuntimeStatWriter> {
          return std::shared_ptr<velox::BaseRuntimeStatWriter>(
              &connectorWriter_, [](auto*) {});
        });
    return std::make_shared<axiom::runner::RunnerSession>(
        std::move(context),
        std::shared_ptr<velox::BaseRuntimeStatWriter>(
            &runnerWriter_, [](auto*) {}),
        axiom::runner::Properties{});
  }

  template <typename RunnerT = LocalRunner>
  std::shared_ptr<RunnerT> makeRunner(optimizer::MultiFragmentPlanPtr plan) {
    const auto queryId = plan->options().queryId;
    return std::make_shared<RunnerT>(
        makeRunnerSession(queryId),
        std::move(plan),
        optimizer::FinishWrite{},
        makeQueryCtx(queryId),
        std::make_shared<ConnectorSplitSourceFactory>(),
        /*outputPool=*/nullptr,
        /*baseSpillDirectory=*/"");
  }

  std::shared_ptr<velox::core::PlanNodeIdGenerator> idGenerator_{
      std::make_shared<velox::core::PlanNodeIdGenerator>()};

  int32_t queryCounter_{0};
  // The runner's own bucket.
  velox::ConcurrentRuntimeStatWriter runnerWriter_;
  // Backs the sessions spawned for scanned connectors.
  velox::ConcurrentRuntimeStatWriter connectorWriter_;

  velox::RowTypePtr rowType_;
};

int64_t extractSingleInt64(const std::vector<velox::RowVectorPtr>& vectors) {
  return vectors.at(0)->childAt(0)->as<velox::FlatVector<int64_t>>()->valueAt(
      0);
}

// A LocalRunner whose cleanup throws after the real reap, to exercise the
// policy that a reap failure must not mask the original stop reason.
class ReapFailingLocalRunner : public LocalRunner {
 public:
  using LocalRunner::LocalRunner;

  folly::coro::Task<void> co_cleanupImpl() override {
    co_await LocalRunner::co_cleanupImpl();
    VELOX_FAIL("injected reap failure");
  }
};

// Exposes when a real LocalRunner has finished its asynchronous cleanup.
class CleanupTrackingLocalRunner : public LocalRunner {
 public:
  using LocalRunner::LocalRunner;

  bool cleanupComplete() const {
    return cleanupComplete_;
  }

 protected:
  folly::coro::Task<void> co_cleanupImpl() override {
    co_await LocalRunner::co_cleanupImpl();
    cleanupComplete_ = true;
  }

 private:
  // Tracks completion of the cleanup contract.
  bool cleanupComplete_{false};
};

// A Runner whose result stream raises an implementation-specific error when
// its execution token is cancelled.
class BlockingRunner : public Runner {
 public:
  BlockingRunner() = default;

  explicit BlockingRunner(folly::coro::Baton& pending) : pending_{&pending} {}

  folly::coro::AsyncGenerator<velox::RowVectorPtr> executeImpl() override {
    state_ = State::kRunning;
    const auto token = co_await folly::coro::co_current_cancellation_token;
    folly::coro::Baton baton;
    folly::CancellationCallback unblock{token, [&baton] { baton.post(); }};
    if (pending_) {
      pending_->post();
    }
    co_await baton;
    state_ = State::kCancelled;
    VELOX_FAIL("injected cancellation error");
  }

  folly::coro::Task<void> co_cleanupImpl() override {
    co_return;
  }

  // Only execute() and co_cleanupImpl() are exercised by drain(); the rest of
  // the Runner interface is required but unused here.
  std::vector<velox::exec::TaskStats> stats() const override {
    VELOX_UNREACHABLE();
  }

  const std::vector<optimizer::ExecutableFragment>& fragments() const override {
    VELOX_UNREACHABLE();
  }

  State state() const override {
    return state_;
  }

 private:
  // Signals when the result pull starts waiting for cancellation.
  folly::coro::Baton* pending_{nullptr};
  // Tracks execution while the result pull waits for cancellation.
  std::atomic<State> state_{State::kInitialized};
};

// Holds a terminal runner state until a test releases the result pull.
class ControlledTerminalRunner : public Runner {
 public:
  enum class Result { kFinished, kError };

  explicit ControlledTerminalRunner(Result result) : result_{result} {}

  folly::coro::AsyncGenerator<velox::RowVectorPtr> executeImpl() override {
    state_ = State::kRunning;
    const auto token = co_await folly::coro::co_current_cancellation_token;
    folly::coro::Baton cancelled;
    folly::CancellationCallback cancellation{
        token, [&cancelled] { cancelled.post(); }};
    switch (result_) {
      case Result::kFinished:
        state_ = State::kFinished;
        break;
      case Result::kError:
        state_ = State::kError;
        break;
    }
    co_await cancelled;
    if (result_ == Result::kError) {
      VELOX_FAIL("injected execution error");
    }
  }

  folly::coro::Task<void> co_cleanupImpl() override {
    co_return;
  }

  std::vector<velox::exec::TaskStats> stats() const override {
    VELOX_UNREACHABLE();
  }

  const std::vector<optimizer::ExecutableFragment>& fragments() const override {
    VELOX_UNREACHABLE();
  }

  State state() const override {
    return state_;
  }

 private:
  // Selects the terminal outcome produced by executeImpl().
  const Result result_;
  // Exposes the terminal transition through Runner::state().
  std::atomic<State> state_{State::kInitialized};
};

// Blocks the first split request until the test releases it or split
// enumeration is cancelled, placing the result pull in a pending state.
class PendingSplitSource : public connector::SplitSource {
 public:
  PendingSplitSource(
      std::shared_ptr<connector::SplitSource> inner,
      std::shared_ptr<folly::coro::Baton> gate,
      std::shared_ptr<folly::coro::Baton> waiting)
      : inner_{std::move(inner)},
        gate_{std::move(gate)},
        waiting_{std::move(waiting)} {}

  folly::coro::Task<connector::SplitBatch> co_getSplits(
      uint32_t maxSplitCount) override {
    waiting_->post();
    const auto token = co_await folly::coro::co_current_cancellation_token;
    folly::CancellationCallback unblock{
        token, [gate = gate_] { gate->post(); }};
    co_await *gate_;
    co_return co_await inner_->co_getSplits(maxSplitCount);
  }

 protected:
  folly::coro::Task<void> co_closeImpl() noexcept override {
    co_await inner_->co_close();
  }

 private:
  // Provides split batches after the pending gate opens.
  std::shared_ptr<connector::SplitSource> inner_;
  // Holds the first split request until the test or cancellation releases it.
  std::shared_ptr<folly::coro::Baton> gate_;
  // Signals that the first split request is waiting on the gate.
  std::shared_ptr<folly::coro::Baton> waiting_;
};

// Wraps scan split sources with a deterministic pending gate.
class PendingSplitSourceFactory : public SplitSourceFactory {
 public:
  PendingSplitSourceFactory(
      std::shared_ptr<folly::coro::Baton> gate,
      std::shared_ptr<folly::coro::Baton> waiting)
      : gate_{std::move(gate)}, waiting_{std::move(waiting)} {}

  std::shared_ptr<connector::SplitSource> splitSourceForScan(
      const RunnerSessionPtr& session,
      const velox::core::TableScanNode& scan,
      const std::shared_ptr<connector::PartitionType>& partitionType,
      std::optional<double> samplePercentage) override {
    return std::make_shared<PendingSplitSource>(
        inner_.splitSourceForScan(
            session, scan, partitionType, samplePercentage),
        gate_,
        waiting_);
  }

 private:
  // Creates connector split sources before wrapping them with the gate.
  ConnectorSplitSourceFactory inner_;
  // Shares the pending gate with each wrapped split source.
  std::shared_ptr<folly::coro::Baton> gate_;
  // Receives the first-wait notification from each wrapped split source.
  std::shared_ptr<folly::coro::Baton> waiting_;
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

TEST_F(LocalRunnerTest, fixedPoint) {
  optimizer::MultiFragmentPlan::Options options = {
      .queryId = makeQueryId(),
      .maxRemotePartitions = 1,
      .maxLocalPartitions = 2,
  };
  auto schema = velox::ROW("n", velox::BIGINT());
  auto initialPlan =
      PlanBuilder(idGenerator_, pool_.get())
          .values({makeRowVector({"n"}, {makeFlatVector<int64_t>({1})})})
          .planNode();
  auto step = PlanBuilder(idGenerator_, pool_.get())
                  .stateSource("r", schema)
                  .filter("n < 5")
                  .project({"n + 1 AS n"});

  test::DistributedPlanBuilder builder(options, idGenerator_, pool_.get());
  builder
      .fixedPoint(
          velox::core::VectorState("r", schema, /*append=*/true)
              .initial(initialPlan),
          step,
          velox::core::ConvergenceConfig::whenDeltaEmpty(/*maxIterations=*/10))
      .orderBy({"n"}, /*isPartial=*/false);
  auto localRunner = makeRunner(builder.build());

  std::vector<int64_t> values;
  localRunner->drain([&](const velox::RowVectorPtr& batch) {
    const auto* vector = batch->childAt(0)->as<velox::SimpleVector<int64_t>>();
    for (velox::vector_size_t i = 0; i < batch->size(); ++i) {
      values.push_back(vector->valueAt(i));
    }
  });
  EXPECT_EQ(values, std::vector<int64_t>({1, 2, 3, 4, 5}));
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
    co_await std::move(generator).cleanup();
  }());

  EXPECT_EQ(1, results.size());
  EXPECT_EQ(kNumRows, extractSingleInt64(results));
  results.clear();
  EXPECT_EQ(Runner::State::kFinished, localRunner->state());
}

// A terminal pull reports its result only after runner cleanup completes.
CO_TEST_F(LocalRunnerTest, terminalCleanup) {
  enum class Outcome { kFinished, kError, kCancelled };
  struct TestCase {
    std::string_view name;
    Outcome outcome;
  };
  const TestCase testCases[] = {
      {"finished", Outcome::kFinished},
      {"error", Outcome::kError},
      {"cancelled", Outcome::kCancelled},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.name);
    auto plan = testCase.outcome == Outcome::kCancelled
        ? makeScanPlan(/*numWorkers=*/1)
        : makeJoinPlan(
              testCase.outcome == Outcome::kError
                  ? "if (c0 = 111, c0 / 0, c0 + 1) as c0"
                  : "c0");
    auto localRunner = makeRunner<CleanupTrackingLocalRunner>(std::move(plan));
    auto&& [generator] = co_await folly::coro::co_scope_exit(
        folly::coro::co_cleanup, localRunner->execute());
    folly::CancellationSource cancellation;

    auto result = co_await folly::coro::co_awaitTry(
        folly::coro::co_withCancellation(
            cancellation.getToken(),
            folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
              while (co_await generator.next()) {
                if (testCase.outcome == Outcome::kCancelled) {
                  cancellation.requestCancellation();
                }
              }
            })));

    switch (testCase.outcome) {
      case Outcome::kFinished:
        EXPECT_TRUE(result.hasValue());
        break;
      case Outcome::kError:
        EXPECT_TRUE(result.hasException());
        break;
      case Outcome::kCancelled:
        EXPECT_NE(
            result.tryGetExceptionObject<folly::OperationCancelled>(), nullptr);
        break;
    }
    EXPECT_TRUE(localRunner->cleanupComplete());
    co_await std::move(generator).cleanup();
  }
}

// The result stream completes when each pull runs under its own timeout.
TEST_F(LocalRunnerTest, perPullTimeout) {
  auto scan = makeScanPlan(/*numWorkers=*/1);
  auto localRunner = makeRunner(scan);

  int64_t numRows{0};
  EXPECT_NO_THROW(folly::coro::blockingWait([&]() -> folly::coro::Task<void> {
    auto&& [generator] = co_await folly::coro::co_scope_exit(
        folly::coro::co_cleanup, localRunner->execute());
    while (auto rows = co_await folly::coro::timeout(
               generator.next(), std::chrono::seconds(10))) {
      numRows += (*rows)->size();
    }
  }()));
  EXPECT_EQ(kNumRows, numRows);
  EXPECT_EQ(Runner::State::kFinished, localRunner->state());
}

// The runner's terminal state decides the outcome when a deadline races with
// completion or an execution error.
CO_TEST_F(LocalRunnerTest, deadlineRace) {
  struct TestCase {
    std::string_view name;
    ControlledTerminalRunner::Result result;
  };
  const TestCase testCases[] = {
      {"finished", ControlledTerminalRunner::Result::kFinished},
      {"error", ControlledTerminalRunner::Result::kError},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.name);
    ControlledTerminalRunner runner{testCase.result};
    auto generator = runner.execute(/*timeoutMicros=*/10'000);

    auto pullResult = co_await folly::coro::co_awaitTry(
        folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
          while (co_await generator.next()) {
          }
        }));

    co_await std::move(generator).cleanup();

    switch (testCase.result) {
      case ControlledTerminalRunner::Result::kFinished:
        EXPECT_TRUE(pullResult.hasValue());
        break;
      case ControlledTerminalRunner::Result::kError: {
        auto* error =
            pullResult.tryGetExceptionObject<velox::VeloxRuntimeError>();
        CO_ASSERT_NE(error, nullptr);
        EXPECT_EQ(error->message(), "injected execution error");
        break;
      }
    }
  }
}

// Caller cancellation has one public exception type across Runner
// implementations.
CO_TEST_F(LocalRunnerTest, cancellationContract) {
  folly::coro::Baton pending;
  BlockingRunner runner{pending};
  auto generator = runner.execute();
  folly::CancellationSource cancellation;

  auto [pullResult, cancellationResult] = co_await folly::coro::collectAllTry(
      folly::coro::co_withCancellation(
          cancellation.getToken(),
          folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
            while (co_await generator.next()) {
            }
          })),
      [&]() -> folly::coro::Task<void> {
        co_await pending;
        cancellation.requestCancellation();
      }());

  co_await folly::coro::co_cleanup(std::move(generator));

  CO_ASSERT_TRUE(cancellationResult.hasValue());
  EXPECT_NE(
      pullResult.tryGetExceptionObject<folly::OperationCancelled>(), nullptr);
}

// Each blockingWait() pull returns its batch while a deadline is armed.
TEST_F(LocalRunnerTest, blockingWaitPerPull) {
  auto localRunner = makeRunner(makeScanPlan(/*numWorkers=*/1));
  auto generator = localRunner->execute(/*timeoutMicros=*/3'600'000'000);

  int64_t numRows{0};
  while (auto batch = folly::coro::blockingWait(generator.next())) {
    numRows += (*batch)->size();
  }
  folly::coro::blockingWait(std::move(generator).cleanup());

  EXPECT_EQ(kNumRows, numRows);
  EXPECT_EQ(Runner::State::kFinished, localRunner->state());
}

// A pending result pull reports whether its deadline or caller cancellation
// stops execution.
CO_TEST_F(LocalRunnerTest, timeoutWhilePending) {
  struct TestCase {
    std::string_view name;
    bool callerCancels;
    int64_t timeoutMicros;
  };
  const TestCase testCases[] = {
      {"deadline", false, 10'000},
      {"caller", true, 3'600'000'000},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.name);
    auto gate = std::make_shared<folly::coro::Baton>();
    auto waiting = std::make_shared<folly::coro::Baton>();
    auto scan = makeScanPlan(/*numWorkers=*/1);
    const auto queryId = scan->options().queryId;
    auto localRunner = std::make_shared<LocalRunner>(
        makeRunnerSession(queryId),
        std::move(scan),
        optimizer::FinishWrite{},
        makeQueryCtx(queryId),
        std::make_shared<PendingSplitSourceFactory>(gate, waiting),
        /*outputPool=*/nullptr,
        /*baseSpillDirectory=*/"");
    auto generator = localRunner->execute(testCase.timeoutMicros);
    folly::CancellationSource callerCancellation;

    auto [pullResult, controlResult] = co_await folly::coro::collectAllTry(
        folly::coro::co_withCancellation(
            callerCancellation.getToken(),
            folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
              while (co_await generator.next()) {
              }
            })),
        [&]() -> folly::coro::Task<void> {
          if (testCase.callerCancels) {
            co_await *waiting;
            callerCancellation.requestCancellation();
          }
        }());

    co_await std::move(generator).cleanup();

    CO_ASSERT_TRUE(controlResult.hasValue());
    if (testCase.callerCancels) {
      EXPECT_NE(
          pullResult.tryGetExceptionObject<folly::OperationCancelled>(),
          nullptr);
    } else {
      auto* error = pullResult.tryGetExceptionObject<velox::VeloxUserError>();
      CO_ASSERT_NE(error, nullptr);
      EXPECT_EQ(error->message(), "Query exceeded maximum time limit of 0.01s");
    }
    EXPECT_EQ(Runner::State::kCancelled, localRunner->state());
  }
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
    auto&& [generator] = co_await folly::coro::co_scope_exit(
        folly::coro::co_cleanup, localRunner->execute());
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
    auto&& [generator] = co_await folly::coro::co_scope_exit(
        folly::coro::co_cleanup, localRunner->execute());
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

  EXPECT_EQ(Runner::State::kCancelled, localRunner->state());
}

// Awaiting cleanup after one batch cancels and reaps the still-running work,
// with or without an active execution deadline.
TEST_F(LocalRunnerTest, earlyStop) {
  for (const int64_t timeoutMicros : {int64_t{0}, int64_t{3'600'000'000}}) {
    SCOPED_TRACE(timeoutMicros);
    auto localRunner = makeRunner(makeScanPlan(/*numWorkers=*/3));

    folly::coro::blockingWait([&]() -> folly::coro::Task<void> {
      auto generator = localRunner->execute(timeoutMicros);
      auto first = co_await generator.next();
      EXPECT_TRUE(first.has_value());
      co_await std::move(generator).cleanup();
    }());
    EXPECT_EQ(Runner::State::kCancelled, localRunner->state());
  }
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

// A callback failure stops and reaps an execution with an active deadline
// before the callback error is rethrown.
TEST_F(LocalRunnerTest, drainCallbackError) {
  auto localRunner = makeRunner(makeScanPlan(/*numWorkers=*/1));

  VELOX_ASSERT_THROW(
      localRunner->drain(
          [](velox::RowVectorPtr) { VELOX_FAIL("injected callback failure"); },
          /*timeoutMicros=*/3'600'000'000),
      "injected callback failure");
  EXPECT_EQ(Runner::State::kCancelled, localRunner->state());
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

// A reap failure must not mask the original stop reason: the genuine execution
// error surfaces from drain() while the secondary reap failure is logged.
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
      folly::coro::blockingWait(std::move(generator).cleanup());
      EXPECT_EQ(kNumRows, count);
    }
  };

  checkScanCount(1);
  checkScanCount(3);
}

// Both halves of split enumeration are the runner's work, so both land in the
// runner's bucket rather than the scanned connector's.
TEST_F(LocalRunnerTest, splitEnumerationStatsAreRunnerScoped) {
  auto localRunner = makeRunner(makeScanPlan(/*numWorkers=*/1));
  auto generator = localRunner->execute();
  while (folly::coro::blockingWait(generator.next())) {
  }
  folly::coro::blockingWait(std::move(generator).cleanup());

  const auto stats = runnerWriter_.runtimeStats();
  auto partitions = stats.find(std::string(LocalRunner::kListPartitionsCount));
  ASSERT_NE(partitions, stats.end());
  EXPECT_GT(partitions->second.sum, 0);
  // One scan node, drained in one enumeration loop, so one sample of each.
  EXPECT_EQ(partitions->second.count, 1);
  auto splits = stats.find(std::string(LocalRunner::kGetSplitsCount));
  ASSERT_NE(splits, stats.end());
  EXPECT_GT(splits->second.sum, 0);
  EXPECT_EQ(splits->second.count, 1);

  const auto connectorStats = connectorWriter_.runtimeStats();
  EXPECT_FALSE(
      connectorStats.contains(std::string(LocalRunner::kListPartitionsCount)));
  EXPECT_FALSE(
      connectorStats.contains(std::string(LocalRunner::kGetSplitsCount)));
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
      .queryId = "test.", .maxRemotePartitions = 1, .maxLocalPartitions = 1};

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
  folly::coro::blockingWait(std::move(generator).cleanup());

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
      std::make_shared<ConnectorSplitSourceFactory>(),
      /*outputPool=*/nullptr,
      spillDir->getPath());

  std::vector<velox::RowVectorPtr> results;
  localRunner->drain(
      [&](velox::RowVectorPtr batch) { results.push_back(std::move(batch)); });
  EXPECT_EQ(1, results.size());
  EXPECT_EQ(kNumRows, extractSingleInt64(results));
  EXPECT_EQ(Runner::State::kFinished, localRunner->state());
}

} // namespace
} // namespace facebook::axiom::runner
