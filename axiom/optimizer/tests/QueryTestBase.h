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
#include "axiom/optimizer/SchemaResolver.h"
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

  /// Human readable Velox plan.
  std::string veloxString;

  /// Human readable Verax output.
  std::string planString;

  std::vector<velox::exec::TaskStats> stats;
};

class QueryTestBase : public runner::test::LocalRunnerTestBase {
 protected:
  void SetUp() override;

  void TearDown() override;

  /// Reads the data directory and picks up new tables.
  void tablesCreated();

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

  TestResult runVelox(
      const velox::core::PlanNodePtr& plan,
      runner::MultiFragmentPlan::Options options = {
          .numWorkers = 1,
          .numDrivers = 4,
      });

  /// Checks that 'reference' and 'experiment' produce the same result.
  /// @return 'reference' result.
  TestResult assertSame(
      const velox::core::PlanNodePtr& reference,
      const optimizer::PlanAndStats& experiment,
      const runner::MultiFragmentPlan::Options& options = {
          .numWorkers = 1,
          .numDrivers = 4,
      });

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

  std::shared_ptr<velox::core::QueryCtx> getQueryCtx();

  std::string veloxString(const runner::MultiFragmentPlanPtr& plan);

  static VeloxHistory& suiteHistory() {
    return *gSuiteHistory;
  }

  OptimizerOptions optimizerOptions_;
  std::shared_ptr<optimizer::SchemaResolver> schema_;

 private:
  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<velox::memory::MemoryPool> optimizerPool_;

  // A QueryCtx created for each compiled query.
  std::shared_ptr<velox::core::QueryCtx> queryCtx_;
  std::shared_ptr<velox::connector::Connector> connector_;
  std::unique_ptr<optimizer::VeloxHistory> history_;

  inline static int32_t gQueryCounter{0};
  inline static std::unique_ptr<VeloxHistory> gSuiteHistory;
};
} // namespace facebook::axiom::optimizer::test
