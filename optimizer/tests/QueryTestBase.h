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

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <gflags/gflags.h>
#include "optimizer/SchemaResolver.h" //@manual
#include "optimizer/VeloxHistory.h" //@manual
#include "optimizer/connectors/hive/LocalHiveConnectorMetadata.h" //@manual
#include "optimizer/tests/ParquetTpchTest.h" //@manual
#include "velox/exec/tests/utils/LocalRunnerTestBase.h"
#include "velox/parse/QueryPlanner.h"
#include "velox/runner/LocalRunner.h"

namespace facebook::velox::optimizer::test {

struct TestResult {
  /// Runner that produced the results. Owns results.
  std::shared_ptr<runner::LocalRunner> runner;

  /// Results. Declare after runner because results are from a pool in the
  /// runner's cursor, so runner must destruct last.
  std::vector<RowVectorPtr> results;

  /// Human readable Velox plan.
  std::string veloxString;

  /// Human readable Verax  output.
  std::string planString;

  /// Error message.
  std::string errorString;

  std::vector<exec::TaskStats> stats;
};

class QueryTestBase : public exec::test::LocalRunnerTestBase {
 protected:
  void SetUp() override;

  void TearDown() override;

  // reads the data directory and picks up new tables.
  void tablesCreated();

  core::PlanNodePtr toTableScan(
      const std::string& id,
      const std::string& name,
      const RowTypePtr& rowType,
      const std::vector<std::string>& columnNames);

  TestResult runSql(const std::string& sql);

  TestResult runVelox(const core::PlanNodePtr& plan);

  TestResult runFragmentedPlan(optimizer::PlanAndStats& plan);

  /// Checks that 'reference' and 'experiment' produce the same result.
  void assertSame(
      const core::PlanNodePtr& reference,
      optimizer::PlanAndStats& experiment,
      TestResult* referenceReturn = nullptr);

  optimizer::PlanAndStats planSql(
      const std::string& sql,
      std::string* planString = nullptr,
      std::string* errorString = nullptr);

  optimizer::PlanAndStats planVelox(
      const core::PlanNodePtr& plan,
      std::string* planString = nullptr,
      std::string* errorString = nullptr);

  std::string veloxString(const std::string& sql);

  std::string veloxString(const runner::MultiFragmentPlanPtr& plan);

  void
  expectRegexp(std::string& text, const std::string regexp, bool expect = true);

  void waitForCompletion(const std::shared_ptr<runner::LocalRunner>& runner);

  OptimizerOptions optimizerOptions_;
  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> optimizerPool_;
  std::shared_ptr<memory::MemoryPool> schemaPool_;
  std::shared_ptr<memory::MemoryPool> schemaRootPool_;
  std::shared_ptr<core::QueryCtx> schemaQueryCtx_;

  // A QueryCtx created for each compiled query.
  std::shared_ptr<core::QueryCtx> queryCtx_;
  std::shared_ptr<connector::ConnectorQueryCtx> connectorQueryCtx_;
  std::shared_ptr<connector::Connector> connector_;
  std::shared_ptr<optimizer::SchemaResolver> schema_;
  std::unique_ptr<facebook::velox::optimizer::VeloxHistory> history_;
  std::unique_ptr<core::DuckDbQueryPlanner> planner_;
  inline static int32_t queryCounter_{0};
};
} // namespace facebook::velox::optimizer::test
