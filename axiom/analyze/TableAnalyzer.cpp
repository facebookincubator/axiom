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
#include "axiom/analyze/TableAnalyzer.h"
#include <unordered_set>
#include "axiom/optimizer/FunctionRegistry.h"

namespace facebook::axiom::analyze {
namespace lp = logical_plan;
using connector::ColumnStatisticKind;

TableAnalyzer::TableAnalyzer(
    std::shared_ptr<connector::ConnectorMetadata> metadata,
    connector::ConnectorSessionPtr session,
    connector::TablePtr table,
    connector::AnalyzeOptions options,
    lp::PlanBuilder::Context context,
    const optimizer::FunctionRegistry& functions)
    : metadata_{std::move(metadata)},
      session_{std::move(session)},
      table_{std::move(table)},
      context_{std::move(context)} {
  VELOX_USER_CHECK(
      functions.count().has_value() && functions.statsAggregates().has_value(),
      "Statistics aggregates are not registered");
  std::unordered_set<std::string> selected;
  if (options.columns.has_value()) {
    for (const auto& name : *options.columns) {
      VELOX_USER_CHECK(
          table_->type()->containsChild(name),
          "Unknown ANALYZE column: {}",
          name);
      VELOX_USER_CHECK(
          selected.insert(name).second, "Duplicate ANALYZE column: {}", name);
    }
  }
  request_ = metadata_->getStatisticsRequest(session_, table_, options);
  VELOX_CHECK_NOT_NULL(request_);
  effectiveRequest_ = *request_;
  // A connector may not add columns the user did not choose.
  for (const auto& stat : effectiveRequest_.statistics) {
    VELOX_USER_CHECK(
        !options.columns || selected.contains(stat.column),
        "ANALYZE request includes unselected column: {}",
        stat.column);
  }
  effectiveRequest_.addDependencies();
  const auto& request = effectiveRequest_;
  VELOX_USER_CHECK(
      request.table == table_->name(),
      "ANALYZE request must read its target table");
  lp::PlanBuilder builder(context_);
  builder.tableScan(
      table_->layouts().front()->connectorId(),
      request.table.schema,
      request.table.table);
  const auto scanType = builder.build()->outputType();
  std::vector<lp::ExprApi> keys;
  std::unordered_set<std::string> seen;
  for (const auto& name : request.groupingColumns) {
    VELOX_USER_CHECK(
        scanType->containsChild(name),
        "Unknown ANALYZE grouping column: {}",
        name);
    VELOX_USER_CHECK(
        seen.insert(name).second,
        "Duplicate ANALYZE grouping column: {}",
        name);
    keys.push_back(lp::Col(name));
  }
  if (request.groups.has_value()) {
    lp::ExprApi filter = lp::Lit(false);
    for (const auto& group : *request.groups) {
      VELOX_USER_CHECK_EQ(
          group.size(), keys.size(), "ANALYZE group width mismatch");
      lp::ExprApi match = lp::Lit(true);
      for (size_t i = 0; i < keys.size(); ++i) {
        // A null group value matches rows where the column is null, as in
        // GROUP BY.
        if (group[i].isNull()) {
          VELOX_USER_CHECK(
              functions.isNull().has_value(),
              "ANALYZE of a NULL group needs a registered is_null function");
          match = match && lp::Call(*functions.isNull(), keys[i]);
        } else {
          match = match &&
              keys[i] ==
                  lp::Lit(
                      group[i],
                      scanType->findChild(request.groupingColumns[i]));
        }
      }
      filter = filter || match;
    }
    builder.filter(filter);
  }
  std::vector<lp::ExprApi> aggregates{
      lp::Call(*functions.count()).as("$row_count")};
  collected_.reserve(request.statistics.size());
  for (const auto& stat : request.statistics) {
    VELOX_USER_CHECK(
        scanType->containsChild(stat.column),
        "Unknown ANALYZE column: {}",
        stat.column);
    const auto aggregate =
        functions.statisticAggregate(stat, scanType->findChild(stat.column));
    collected_.push_back(aggregate.has_value());
    if (!aggregate.has_value()) {
      continue;
    }
    if (stat.kind == ColumnStatisticKind::kDistinctSketch ||
        stat.kind == ColumnStatisticKind::kAggregate) {
      VELOX_USER_CHECK(
          aggregate->resultType->isVarbinary(),
          "Sketch aggregates must return binary data: {}",
          aggregate->name);
    }
    std::vector<lp::ExprApi> args{lp::Col(stat.column)};
    for (const auto& arg : stat.arguments) {
      args.push_back(lp::Lit(arg));
    }
    aggregates.push_back(
        lp::Call(aggregate->name, args)
            .as(fmt::format("$stat_{}", aggregates.size())));
  }
  plan_ = builder.aggregate(keys, aggregates).build();
}

folly::coro::Task<int64_t> TableAnalyzer::co_run(
    Execute execute,
    velox::memory::MemoryPool* pool) {
  VELOX_CHECK(!started_, "ANALYZE request has already been executed");
  started_ = true;
  auto batches = co_await execute(plan_);
  // A grouped query over no rows can return no batches. Add an empty one so
  // the results still have the right columns.
  if (batches.empty()) {
    batches.push_back(
        velox::BaseVector::create<velox::RowVector>(
            plan_->outputType(), 0, pool));
  }
  auto results = connector::StatisticsResults::fromAggregates(
      effectiveRequest_, collected_, batches, pool, /*firstChannel=*/0);
  int64_t rows = 0;
  const auto& counts =
      results.groups->childAt(results.groups->childrenSize() - 1);
  for (auto i = 0; i < counts->size(); ++i) {
    rows += counts->variantAt(i).value<int64_t>();
  }
  co_await metadata_->co_finishAnalyze(
      session_, table_, *request_, std::move(results));
  co_return rows;
}
} // namespace facebook::axiom::analyze
