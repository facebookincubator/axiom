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

#include <folly/executors/IOThreadPoolExecutor.h>
#include "axiom/connectors/SchemaResolver.h"
#include "axiom/optimizer/DerivedTable.h"
#include "axiom/optimizer/VeloxHistory.h"
#include "axiom/runner/LocalRunner.h"
#include "axiom/sql/presto/PrestoParser.h"

namespace axiom::sql {

class SqlQueryRunner {
 public:
  /// @param initializeConnectors Lambda to call to initialize connectors and
  /// return a pair of default {connector ID, schema}. Takes a reference to the
  /// history to allow for loading from persistent storage.
  void initialize(
      const std::function<std::pair<std::string, std::optional<std::string>>(
          facebook::axiom::optimizer::VeloxHistory& history)>&
          initializeConnectors);

  /// Results of running a query. SELECT queries return a vector of results.
  /// Other queries return a message. SELECT query that returns no rows returns
  /// std::nullopt message and empty vector of results.
  struct SqlResult {
    std::optional<std::string> message;
    std::vector<facebook::velox::RowVectorPtr> results;
  };

  struct RunOptions {
    int32_t numWorkers{4};
    int32_t numDrivers{4};
    uint64_t splitTargetBytes{16 << 20};
    uint32_t optimizerTraceFlags{0};

    /// If true, EXPLAIN ANALYZE output includes custom operator stats.
    bool debugMode{false};
  };

  /// Runs a single SQL statement and returns the result.
  SqlResult run(std::string_view sql, const RunOptions& options);

  /// Runs a single parsed SQL statement and returns the result.
  SqlResult run(
      const presto::SqlStatement& statement,
      const RunOptions& options);

  /// Parses SQL text containing one or more semicolon-separated statements.
  /// @param sql SQL text to parse.
  /// @return Vector of parsed statements.
  std::vector<presto::SqlStatementPtr> parseMultiple(
      std::string_view sql,
      const RunOptions& options);

  /// Generates DOT representation of the query graph for a single SELECT
  /// statement. The output can be rendered using Graphviz:
  ///   dot -Tsvg output.dot -o output.svg
  /// @param sql A single SELECT or EXPLAIN SELECT statement.
  std::string toQueryGraphDot(std::string_view sql);

  /// Generates DOT representation of the logical plan for a single SELECT
  /// statement. The output can be rendered using Graphviz:
  ///   dot -Tsvg output.dot -o output.svg
  /// @param sql A single SELECT or EXPLAIN SELECT statement.
  std::string toLogicalPlanDot(std::string_view sql);

  std::unordered_map<std::string, std::string>& sessionConfig() {
    return config_;
  }

  void saveHistory(const std::string& path) {
    history_->saveToFile(path);
  }

  void clearHistory() {
    history_ = std::make_unique<facebook::axiom::optimizer::VeloxHistory>();
  }

 private:
  std::shared_ptr<facebook::velox::core::QueryCtx> newQuery(
      const RunOptions& options);

  facebook::axiom::connector::TablePtr createTable(
      const presto::CreateTableAsSelectStatement& statement);

  std::string dropTable(const presto::DropTableStatement& statement);

  std::string runExplain(
      const facebook::axiom::logical_plan::LogicalPlanNodePtr& logicalPlan,
      presto::ExplainStatement::Type type,
      const RunOptions& options);

  std::string runExplainAnalyze(
      const facebook::axiom::logical_plan::LogicalPlanNodePtr& logicalPlan,
      const RunOptions& options);

  // Parses SQL and returns the logical plan.
  facebook::axiom::logical_plan::LogicalPlanNodePtr toLogicalPlan(
      std::string_view sql);

  // Optimizes provided logical plan.
  // @param checkDerivedTable Optional lambda to call after to-graph stage of
  // optimization. If returns 'false', the optimization stops and returns an
  // empty result.
  // @param checkBestPlan Optional lambda to call towards the end of
  // optimization after best plan is found. If returns 'false', the optimization
  // stops and returns an empty result.
  facebook::axiom::optimizer::PlanAndStats optimize(
      const facebook::axiom::logical_plan::LogicalPlanNodePtr& logicalPlan,
      const std::shared_ptr<facebook::velox::core::QueryCtx>& queryCtx,
      const RunOptions& options,
      const std::function<bool(
          const facebook::axiom::optimizer::DerivedTable&)>& checkDerivedTable =
          nullptr,
      const std::function<bool(const facebook::axiom::optimizer::RelationOp&)>&
          checkBestPlan = nullptr);

  std::shared_ptr<facebook::axiom::runner::LocalRunner> makeLocalRunner(
      facebook::axiom::optimizer::PlanAndStats& planAndStats,
      const std::shared_ptr<facebook::velox::core::QueryCtx>& queryCtx,
      const RunOptions& options);

  /// Runs a query and returns the result as a single vector in *resultVector,
  /// the plan text in *planString and the error message in *errorString.
  /// *errorString is not set if no error. Any of these may be nullptr.
  std::vector<facebook::velox::RowVectorPtr> runSql(
      const facebook::axiom::logical_plan::LogicalPlanNodePtr& logicalPlan,
      const RunOptions& options);

  static void waitForCompletion(
      const std::shared_ptr<facebook::axiom::runner::LocalRunner>& runner) {
    if (runner) {
      try {
        runner->waitForCompletion(500000);
      } catch (const std::exception&) {
      }
    }
  }

  std::shared_ptr<facebook::velox::cache::AsyncDataCache> cache_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> optimizerPool_;
  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<folly::IOThreadPoolExecutor> spillExecutor_;
  std::shared_ptr<facebook::axiom::connector::SchemaResolver> schema_;
  std::unordered_map<std::string, std::string> config_;
  std::unique_ptr<facebook::axiom::optimizer::VeloxHistory> history_;
  std::unique_ptr<presto::PrestoParser> prestoParser_;
  int32_t queryCounter_{0};
};

} // namespace axiom::sql
