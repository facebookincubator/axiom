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

#include "axiom/runner/ProgressReporter.h"

#include <folly/coro/Baton.h>
#include <folly/coro/BlockingWait.h>
#include <folly/executors/FunctionScheduler.h>
#include <gtest/gtest.h>
#include <condition_variable>
#include <mutex>
#include <vector>

#include "axiom/connectors/ConnectorSplitManager.h"
#include "axiom/runner/LocalRunner.h"
#include "axiom/runner/tests/DistributedPlanBuilder.h"
#include "axiom/runner/tests/LocalRunnerTestBase.h"

namespace facebook::axiom::runner {
namespace {

namespace velox = facebook::velox;

// Releases scan splits one at a time. Each co_getSplits awaits 'gate', so the
// runner blocks between splits until the test posts it -- letting the test
// observe one split's worth of progress per release.
class GatedSplitSource : public connector::SplitSource {
 public:
  GatedSplitSource(
      std::shared_ptr<connector::SplitSource> inner,
      std::shared_ptr<folly::coro::Baton> gate)
      : inner_{std::move(inner)}, gate_{std::move(gate)} {}

  folly::coro::Task<connector::SplitBatch> co_getSplits(
      uint32_t /*maxSplitCount*/) override {
    co_await *gate_;
    gate_->reset();
    co_return co_await inner_->co_getSplits(1);
  }

 protected:
  folly::coro::Task<void> co_closeImpl() noexcept override {
    co_await inner_->co_close();
  }

 private:
  std::shared_ptr<connector::SplitSource> inner_;
  std::shared_ptr<folly::coro::Baton> gate_;
};

// Wraps ConnectorSplitSourceFactory so the scan's splits flow through a gate.
class GatedSplitSourceFactory : public SplitSourceFactory {
 public:
  GatedSplitSourceFactory(
      QueryRuntimeStats& runtimeStats,
      std::shared_ptr<folly::coro::Baton> gate)
      : inner_{runtimeStats}, gate_{std::move(gate)} {}

  std::shared_ptr<connector::SplitSource> splitSourceForScan(
      const connector::ConnectorSessionPtr& session,
      const velox::core::TableScanNode& scan,
      const std::shared_ptr<connector::PartitionType>& partitionType,
      std::optional<double> samplePercentage) override {
    return std::make_shared<GatedSplitSource>(
        inner_.splitSourceForScan(
            session, scan, partitionType, samplePercentage),
        gate_);
  }

 private:
  ConnectorSplitSourceFactory inner_;
  std::shared_ptr<folly::coro::Baton> gate_;
};

class ProgressReporterTest : public test::LocalRunnerTestBase {
 protected:
  static constexpr int32_t kNumSplits = 4;
  static constexpr int32_t kRowsPerSplit = 100;

  void SetUp() override {
    rowType_ = velox::ROW("c0", velox::BIGINT());
    makeTables({test::TableSpec{
        .name = "t",
        .columns = rowType_,
        .rowsPerVector = kRowsPerSplit,
        .numVectorsPerFile = 1,
        .numFiles = kNumSplits}});
    LocalRunnerTestBase::SetUp();
  }

  optimizer::MultiFragmentPlanPtr makeScanPlan() {
    optimizer::MultiFragmentPlan::Options options = {
        .queryId = "progress-q", .numWorkers = 1, .numDrivers = 1};
    test::DistributedPlanBuilder builder(options, idGenerator_, pool_.get());
    builder.tableScan("t", rowType_);
    return builder.build();
  }

  static RunnerSessionPtr makeRunnerSession(std::string_view queryId) {
    return std::make_shared<RunnerSession>(
        std::string(queryId),
        "test",
        Properties{},
        connector::ConnectorProperties{});
  }

  velox::RowTypePtr rowType_;
  std::shared_ptr<velox::core::PlanNodeIdGenerator> idGenerator_{
      std::make_shared<velox::core::PlanNodeIdGenerator>()};
  QueryRuntimeStats runtimeStats_;
};

// Releases scan splits one at a time and asserts the scheduler's periodic poll
// observes each one through the callback. With a single worker and driver the
// runner blocks between splits, so each poll lands on a stable, advanced
// snapshot -- no sleeps, no flaky timing.
TEST_F(ProgressReporterTest, reportsProgressPerSplit) {
  auto gate = std::make_shared<folly::coro::Baton>();
  auto plan = makeScanPlan();
  const auto queryId = plan->options().queryId;
  auto runner = std::make_shared<LocalRunner>(
      makeRunnerSession(queryId),
      std::move(plan),
      optimizer::FinishWrite{},
      makeQueryCtx(queryId),
      std::make_shared<GatedSplitSourceFactory>(runtimeStats_, gate),
      /*outputPool=*/nullptr,
      /*baseSpillDirectory=*/"",
      runtimeStats_);

  std::mutex mutex;
  std::condition_variable cv;
  std::vector<QueryProgress> reports;

  folly::FunctionScheduler scheduler;
  scheduler.start();

  auto waitForCompletedPast = [&](int32_t prev) {
    std::unique_lock<std::mutex> lock(mutex);
    const bool ok = cv.wait_for(lock, std::chrono::seconds(10), [&] {
      return !reports.empty() && reports.back().stats.completedSplits > prev;
    });
    EXPECT_TRUE(ok) << "no report past " << prev;
    // On timeout the predicate never held, so reports may be empty; return a
    // default rather than dereferencing back() so a hang fails cleanly.
    return reports.empty() ? QueryProgress{} : reports.back();
  };

  {
    ProgressReporter reporter(
        *runner,
        scheduler,
        "progress-q",
        [&](const QueryProgress& report) {
          {
            std::lock_guard<std::mutex> lock(mutex);
            reports.push_back(report);
          }
          cv.notify_all();
        },
        ProgressReporter::kMinProgressReportIntervalMs);

    // Splits are released and enumerated lazily, so after the i-th release
    // exactly i splits are known and all i are completed (the runner is blocked
    // requesting the next), with i splits' worth of rows scanned.
    int32_t completedSplits{0};
    int64_t outputRows{0};
    gate->post();
    runner->drain([&](velox::RowVectorPtr batch) {
      outputRows += batch->size();
      ++completedSplits;
      const auto report = waitForCompletedPast(completedSplits - 1);
      EXPECT_EQ(report.queryId, "progress-q");
      EXPECT_EQ(report.stats.state, ExecutionState::kRunning);
      EXPECT_EQ(report.stats.completedSplits, completedSplits);
      EXPECT_EQ(report.stats.totalSplits, completedSplits);
      EXPECT_EQ(report.stats.scanRows, completedSplits * kRowsPerSplit);
      gate->post();
    });
    EXPECT_EQ(completedSplits, kNumSplits);
    EXPECT_EQ(outputRows, kNumSplits * kRowsPerSplit);
  } // ~reporter delivers the final report synchronously.

  // The teardown report is the last one: all splits completed, finished.
  const auto& teardown = reports.back();
  EXPECT_EQ(teardown.stats.state, ExecutionState::kFinished);
  EXPECT_EQ(teardown.stats.completedSplits, kNumSplits);
  EXPECT_EQ(teardown.stats.totalSplits, kNumSplits);
  EXPECT_EQ(teardown.stats.scanRows, kNumSplits * kRowsPerSplit);
  ASSERT_EQ(teardown.stages.size(), 1);
  EXPECT_TRUE(teardown.stages[0].producers.empty());
  EXPECT_EQ(teardown.stages[0].stats.completedSplits, kNumSplits);
  EXPECT_EQ(teardown.stages[0].stats.scanRows, kNumSplits * kRowsPerSplit);
}

} // namespace
} // namespace facebook::axiom::runner
