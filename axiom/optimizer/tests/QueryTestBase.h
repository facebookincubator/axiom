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

namespace facebook::velox::optimizer::test {

struct TestResult {
  /// Runner that produced the results. Owns results.
  std::shared_ptr<axiom::runner::LocalRunner> runner;

  /// Results. Declare after runner because results are from a pool in the
  /// runner's cursor, so runner must destruct last.
  std::vector<RowVectorPtr> results;

  /// Human readable Velox plan.
  std::string veloxString;

  /// Human readable Verax output.
  std::string planString;

  std::vector<exec::TaskStats> stats;
};

class QueryTestBase : public axiom::runner::test::LocalRunnerTestBase {
 protected:
  void SetUp() override;

  void TearDown() override;

  /// Reads the data directory and picks up new tables.
  void tablesCreated();

  optimizer::PlanAndStats planVelox(
      const logical_plan::LogicalPlanNodePtr& plan,
      std::string* planString = nullptr);

  optimizer::PlanAndStats planVelox(
      const logical_plan::LogicalPlanNodePtr& plan,
      const axiom::runner::MultiFragmentPlan::Options& options,
      std::string* planString = nullptr);

  TestResult runVelox(const logical_plan::LogicalPlanNodePtr& plan);

  TestResult runVelox(
      const logical_plan::LogicalPlanNodePtr& plan,
      const axiom::runner::MultiFragmentPlan::Options& options);

  TestResult runFragmentedPlan(const optimizer::PlanAndStats& plan);

  TestResult runVelox(const core::PlanNodePtr& plan);

  /// Checks that 'reference' and 'experiment' produce the same result.
  /// @return 'reference' result.
  TestResult assertSame(
      const core::PlanNodePtr& reference,
      const optimizer::PlanAndStats& experiment);

  std::shared_ptr<core::QueryCtx> getQueryCtx();

  std::string veloxString(const axiom::runner::MultiFragmentPlanPtr& plan);

  static VeloxHistory& suiteHistory() {
    return *suiteHistory_;
  }

  OptimizerOptions optimizerOptions_;
  std::shared_ptr<optimizer::SchemaResolver> schema_;

 private:
  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> optimizerPool_;

  // A QueryCtx created for each compiled query.
  std::shared_ptr<core::QueryCtx> queryCtx_;
  std::shared_ptr<connector::Connector> connector_;
  std::unique_ptr<velox::optimizer::VeloxHistory> history_;

  inline static int32_t queryCounter_{0};
  inline static std::unique_ptr<VeloxHistory> suiteHistory_;
};
} // namespace facebook::velox::optimizer::test
