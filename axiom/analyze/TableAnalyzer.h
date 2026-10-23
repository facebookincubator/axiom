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

#include <folly/coro/Task.h>
#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/logical_plan/PlanBuilder.h"

namespace facebook::axiom::optimizer {
class FunctionRegistry;
} // namespace facebook::axiom::optimizer

namespace facebook::axiom::analyze {

/// Runs one ANALYZE statement. It asks the connector what to collect, builds
/// the query that computes the statistics, and gives the results back to the
/// connector. The frontend creates one for both ANALYZE and EXPLAIN, but calls
/// co_run only for ANALYZE. The frontend runs the query and checks
/// permissions.
///
///   TableAnalyzer analyzer(
///       metadata, session, table, options, std::move(context), functions);
///   explainPlan(analyzer.plan()); // EXPLAIN: nothing else runs.
///   auto rows = co_await analyzer.co_run(execute, pool); // ANALYZE.
///
/// The query scans the table through the table's own connector, keeps only
/// the requested groups if there are any, and runs one aggregation. The
/// aggregation computes $row_count and one aggregate per statistic. A
/// statistic whose aggregate can't take the column's type is skipped, as
/// writes skip it.
///
/// The constructor checks that:
/// - Every column the user chose exists and is listed once.
/// - The connector's request reads the table being analyzed, and only the
///   chosen columns.
/// - The grouping columns exist and are all different, and each group has
///   one value for each grouping column.
/// - Each sketch statistic's aggregate returns binary data.
class TableAnalyzer {
 public:
  /// Runs a logical plan and returns all of its output batches.
  using Execute =
      std::function<folly::coro::Task<std::vector<velox::RowVectorPtr>>(
          const logical_plan::LogicalPlanNodePtr&)>;

  /// Gets the connector's request for 'table' and builds the statistics query
  /// in 'context'. It changes nothing, so EXPLAIN can use it. 'functions'
  /// tells which aggregate computes each statistic.
  TableAnalyzer(
      std::shared_ptr<connector::ConnectorMetadata> metadata,
      connector::ConnectorSessionPtr session,
      connector::TablePtr table,
      connector::AnalyzeOptions options,
      logical_plan::PlanBuilder::Context context,
      const optimizer::FunctionRegistry& functions);

  const logical_plan::LogicalPlanNodePtr& plan() const {
    return plan_;
  }

  /// Runs the query, stores the results through the connector, and returns
  /// the number of rows read. Can be called only once.
  folly::coro::Task<int64_t> co_run(
      Execute execute,
      velox::memory::MemoryPool* pool);

 private:
  std::shared_ptr<connector::ConnectorMetadata> metadata_;
  connector::ConnectorSessionPtr session_;
  connector::TablePtr table_;
  // The connector's request. It goes back to the connector with the results.
  std::shared_ptr<const connector::StatisticsRequest> request_;
  // request_ plus the statistics that other statistics need. plan_ computes
  // these.
  connector::StatisticsRequest effectiveRequest_;
  // For each statistic in effectiveRequest_, whether plan_ computes it.
  std::vector<bool> collected_;
  // Keeps the memory for the plan's literal values alive while the plan is in
  // use.
  logical_plan::PlanBuilder::Context context_;
  logical_plan::LogicalPlanNodePtr plan_;
  bool started_{false};
};
} // namespace facebook::axiom::analyze
