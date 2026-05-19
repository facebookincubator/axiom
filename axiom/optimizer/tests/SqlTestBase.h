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
#include <functional>
#include <optional>
#include "axiom/common/QueryRuntimeStats.h"
#include "axiom/connectors/tests/TestConnector.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"

namespace facebook::velox::cache {
class AsyncDataCache;
} // namespace facebook::velox::cache

namespace facebook::axiom::logical_plan {
class LogicalPlanNode;
using LogicalPlanNodePtr = std::shared_ptr<const LogicalPlanNode>;
} // namespace facebook::axiom::logical_plan

namespace facebook::axiom::runner {
class LocalRunner;
} // namespace facebook::axiom::runner

namespace facebook::axiom::optimizer::test {

/// End-to-end SQL testing framework for Axiom. Runs a SQL query through the
/// full Axiom pipeline (parse -> optimize -> execute via LocalRunner), runs
/// the same query through DuckDB, and compares the results.
///
/// Usage:
///   class MyTest : public SqlTestBase {
///    protected:
///     void SetUp() override {
///       SqlTestBase::SetUp();
///       createTable("t", {makeRowVector(...)});
///     }
///   };
///
///   TEST_F(MyTest, simple) {
///     assertResults("SELECT a, sum(b) FROM t GROUP BY 1");
///   }
class SqlTestBase : public velox::exec::test::OperatorTestBase {
 public:
  static constexpr const char* kTestConnectorId = "test";

  /// Registers Presto scalar, aggregate, and window functions.
  static void SetUpTestCase();

  /// Creates a TestConnector and initializes memory pools.
  void SetUp() override;

  /// Unregisters the connector and resets pools.
  void TearDown() override;

  /// Registers a table with the given name and data in both the Axiom
  /// TestConnector and DuckDB. May be called multiple times to register
  /// multiple tables. Each RowVector in 'data' becomes a separate split.
  void createTable(
      const std::string& name,
      const std::vector<velox::RowVectorPtr>& data);

  /// Executes a single setup DDL statement against the Axiom TestConnector
  /// and DuckDB. Supports:
  ///   - CREATE TABLE name (col TYPE, ...) — registers an empty table.
  ///   - INSERT INTO name VALUES (..., ...), (..., ...), ... — appends one
  ///     RowVector (one TestConnector split) and runs the same statement
  ///     in DuckDB.
  /// Each INSERT to the same table produces a separate split, mirroring
  /// the multi-RowVector createTable shape.
  void runSetupStatement(const std::string& sql);

  /// Static variant of runSetupStatement. The caller supplies the
  /// connector, the DuckDB runner, and a factory that builds a LocalRunner
  /// for an INSERT / CTAS plan. Suite-scoped fixtures use this in
  /// SetUpTestSuite to install reference tables once into a standalone
  /// connector backed by their own MemoryManager / executor / cache.
  static void runSetupStatement(
      const std::string& sql,
      connector::TestConnector& connector,
      velox::exec::test::DuckDbQueryRunner& duckDb,
      const std::function<std::shared_ptr<runner::LocalRunner>(
          const logical_plan::LogicalPlanNodePtr&)>& runnerFactory);

  /// Builds a LocalRunner for 'logicalPlan' using caller-supplied
  /// infrastructure. Suite-scoped fixtures use this to bind their static
  /// MemoryManager / executor / AsyncDataCache into a runnerFactory for
  /// runSetupStatement.
  static std::shared_ptr<runner::LocalRunner> makeLocalRunner(
      const logical_plan::LogicalPlanNodePtr& logicalPlan,
      folly::Executor* executor,
      velox::cache::AsyncDataCache* asyncDataCache,
      const std::shared_ptr<velox::memory::MemoryPool>& rootPool,
      const std::shared_ptr<velox::memory::MemoryPool>& optimizerPool,
      int32_t numWorkers = 4,
      int32_t numDrivers = 4);

  /// Runs SQL through Axiom and DuckDB, asserts unordered results match.
  /// @param sql SQL query to run through both engines.
  /// @param checkColumnNames If true, also asserts output column names match.
  /// @param duckDbSql Optional different SQL for DuckDB (e.g. different
  /// function names). Uses 'sql' for DuckDB if not set.
  void assertResults(
      std::string_view sql,
      bool checkColumnNames = false,
      std::optional<std::string> duckDbSql = std::nullopt);

  /// Runs SQL through both engines, asserts ordered results match.
  void assertOrderedResults(
      std::string_view sql,
      bool checkColumnNames = false,
      std::optional<std::string> duckDbSql = std::nullopt);

  /// Runs SQL through Axiom, asserts the result has exactly 'expectedCount'
  /// rows.
  void assertResultCount(std::string_view sql, uint64_t expectedCount);

  /// Runs SQL through Axiom, asserts it throws with a message containing
  /// 'expectedMessage'.
  void assertFailure(std::string_view sql, std::string_view expectedMessage);

 protected:
  // Execution parallelism settings.
  int32_t numWorkers_{4};
  int32_t numDrivers_{4};

  // Override to false in subclasses that manage the TestConnector lifecycle
  // at suite scope (i.e., create and register the connector once per
  // fixture class in SetUpTestSuite, not once per test in SetUp).
  virtual bool createsConnectorPerTest() const {
    return true;
  }

  // Returns the DuckDbQueryRunner used for table installation and result
  // comparison. The default returns 'duckDbQueryRunner_' (per-test,
  // inherited from OperatorTestBase). Subclasses that want suite-scoped
  // DuckDB state override to return their own static runner.
  virtual velox::exec::test::DuckDbQueryRunner& duckDbRunner() {
    return duckDbQueryRunner_;
  }

  std::shared_ptr<connector::TestConnector> connector_;

 private:
  // Runs SQL through Axiom's full pipeline without storing results. Returns
  // the total number of rows.
  uint64_t run(std::string_view sql);

  // Runs SQL through Axiom's full pipeline and returns all result batches.
  std::vector<velox::RowVectorPtr> runAndCollect(std::string_view sql);

  // Runs SQL through Axiom's full pipeline (parse, optimize, execute) and
  // returns a LocalRunner to iterate over.
  std::shared_ptr<runner::LocalRunner> makeRunner(std::string_view sql);

  // Builds a LocalRunner directly from a logical plan. Used by
  // runSetupStatement to execute INSERT / CREATE TABLE AS SELECT plans
  // without re-parsing.
  std::shared_ptr<runner::LocalRunner> makeRunner(
      const logical_plan::LogicalPlanNodePtr& logicalPlan);

  std::shared_ptr<velox::memory::MemoryPool> optimizerPool_;
  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
  QueryRuntimeStats runtimeStats_;
};

} // namespace facebook::axiom::optimizer::test
