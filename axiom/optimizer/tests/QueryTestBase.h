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

#pragma once

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <gflags/gflags.h>
#include "axiom/optimizer/VeloxHistory.h"
#include "axiom/runner/LocalRunner.h"
#include "axiom/runner/tests/LocalRunnerTestBase.h"

DECLARE_string(history_save_path);

namespace facebook::axiom::optimizer::test {

struct TestResult {
  /// Runner that produced the results. Owns results.
  std::shared_ptr<runner::LocalRunner> runner;

  /// Results. Declare after runner because results are from a pool in the
  /// runner's cursor, so runner must destruct last.
  std::vector<velox::RowVectorPtr> results;

  /// Runtime stats retrieved from Velox tasks. One entry per fragment. See
  /// LocalRunner::stats() for details.
  std::vector<velox::exec::TaskStats> stats;
};

class QueryTestBase : public runner::test::LocalRunnerTestBase {
 protected:
  void SetUp() override;

  void TearDown() override;

  optimizer::PlanAndStats planVelox(
      const logical_plan::LogicalPlanNodePtr& plan,
      const runner::MultiFragmentPlan::Options& options =
          {
              .numWorkers = 4,
              .numDrivers = 4,
          },
      std::string* planString = nullptr);

  TestResult runVelox(
      const logical_plan::LogicalPlanNodePtr& plan,
      const runner::MultiFragmentPlan::Options& options = {
          .numWorkers = 4,
          .numDrivers = 4,
      });

  TestResult runFragmentedPlan(const optimizer::PlanAndStats& plan);

  /// Runs the given single-stage Velox plan single-threaded.
  TestResult runVelox(const velox::core::PlanNodePtr& plan);

  /// Checks that 'reference' and 'experiment' produce the same result.
  /// Runs 'reference' plan single-threaded.
  /// @return 'reference' result.
  TestResult checkSame(
      const optimizer::PlanAndStats& experiment,
      const velox::core::PlanNodePtr& reference);

  /// Checks that 'reference' and 'velox' produce the same result.
  /// Runs 'referencePlan' single-threaded. Runs 'planNode' multiple times using
  /// different parallelism settings. Runs single-node-single-threaded,
  /// single-node-multi-threaded, multi-node-single-threaded, and
  /// multi-node-multi-threaded. Uses options.numWorkers for multi-node runs and
  /// options.numDrivers for multi-threaded runs. Doesn't run with higher
  /// parallelism than specified in 'options'. E.g. if options = {.numWorkers =
  /// 1, .numDrivers = 1}, then runs only once (single-node-single-threaded).
  /// All runs are expected to produce the same result that matches result of
  /// 'referencePlan'.
  void checkSame(
      const logical_plan::LogicalPlanNodePtr& planNode,
      const velox::core::PlanNodePtr& referencePlan,
      const axiom::runner::MultiFragmentPlan::Options& options = {
          .numWorkers = 4,
          .numDrivers = 4,
      });

  velox::core::PlanNodePtr toSingleNodePlan(
      const logical_plan::LogicalPlanNodePtr& logicalPlan,
      int32_t numDrivers = 1);

  void checkSameSingleNode(
      const logical_plan::LogicalPlanNodePtr& planNode,
      const velox::core::PlanNodePtr& referencePlan,
      int32_t numDrivers = 1) {
    checkSame(
        planNode, referencePlan, {.numWorkers = 1, .numDrivers = numDrivers});
  }

  std::shared_ptr<velox::core::QueryCtx>& getQueryCtx();

  static VeloxHistory& suiteHistory() {
    return *gSuiteHistory;
  }

  OptimizerOptions optimizerOptions_;

 private:
  std::shared_ptr<velox::memory::MemoryPool> optimizerPool_;

  // A QueryCtx created for each compiled query.
  std::shared_ptr<velox::core::QueryCtx> queryCtx_;
  std::unique_ptr<optimizer::VeloxHistory> history_;

  inline static int32_t gQueryCounter{0};
  inline static std::unique_ptr<VeloxHistory> gSuiteHistory;
};
} // namespace facebook::axiom::optimizer::test
