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
#include "axiom/connectors/tests/TestConnector.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"

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

  /// Runs SQL through Axiom and DuckDB, asserts unordered results match.
  /// @param sql SQL query to run through both engines.
  /// @param duckDbSql Optional different SQL for DuckDB (e.g. different
  /// function names). Uses 'sql' for DuckDB if empty.
  void assertResults(std::string_view sql, std::string_view duckDbSql = {});

  /// Runs SQL through both engines, asserts ordered results match.
  void assertOrderedResults(
      std::string_view sql,
      std::string_view duckDbSql = {});

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

 private:
  // Runs SQL through Axiom's full pipeline without storing results. Returns
  // the total number of rows.
  uint64_t run(std::string_view sql);

  // Runs SQL through Axiom's full pipeline and returns all result batches.
  std::vector<velox::RowVectorPtr> runAndCollect(std::string_view sql);

  // Runs SQL through Axiom's full pipeline (parse, optimize, execute) and
  // returns a LocalRunner to iterate over.
  std::shared_ptr<runner::LocalRunner> makeRunner(std::string_view sql);

  std::shared_ptr<connector::TestConnector> connector_;
  std::shared_ptr<velox::memory::MemoryPool> optimizerPool_;
  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
};

} // namespace facebook::axiom::optimizer::test
