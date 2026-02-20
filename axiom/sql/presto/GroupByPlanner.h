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

#include <unordered_map>
#include "axiom/logical_plan/ExprApi.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/sql/presto/ExpressionPlanner.h"
#include "axiom/sql/presto/ast/AstNodesAll.h"
#include "folly/container/F14Map.h"

namespace axiom::sql::presto {

namespace lp = facebook::axiom::logical_plan;

/// Plans GROUP BY, ROLLUP, CUBE, and GROUPING SETS clauses. Constructed
/// per-query to hold intermediate state during aggregation planning.
class GroupByPlanner {
 public:
  GroupByPlanner(
      std::shared_ptr<lp::PlanBuilder>& builder,
      ExpressionPlanner& exprPlanner)
      : builder_(builder), exprPlanner_(exprPlanner) {}

  /// Plans a GROUP BY clause with optional HAVING and ORDER BY.
  void plan(
      const std::vector<SelectItemPtr>& selectItems,
      const std::vector<GroupingElementPtr>& groupingElements,
      const ExpressionPtr& having,
      const OrderByPtr& orderBy) &&;

  /// Detects implicit global aggregation (e.g. SELECT count(*) FROM t)
  /// and plans it. Returns true if aggregation was added.
  bool tryPlanGlobalAgg(
      const std::vector<SelectItemPtr>& selectItems,
      const ExpressionPtr& having) &&;

 private:
  using AggregateOptionsMap = std::unordered_map<
      const facebook::velox::core::IExpr*,
      lp::PlanBuilder::AggregateOptions>;

  std::vector<std::vector<lp::ExprApi>> expandGroupingSets(
      const std::vector<GroupingElementPtr>& groupingElements,
      const std::vector<SelectItemPtr>& selectItems);
  void deduplicateGroupingKeys();
  AggregateOptionsMap collectAggregates(
      const std::vector<SelectItemPtr>& selectItems,
      const ExpressionPtr& having,
      const OrderByPtr& orderBy);
  void addAggregate(
      const AggregateOptionsMap& aggregateOptionsMap,
      bool useGroupingSets);
  void rewritePostAggregateExprs();
  std::vector<size_t> resolveSortOrdinals(const OrderByPtr& orderBy);
  bool isIdentityProjection() const;
  void addSort(
      const std::vector<SelectItemPtr>& selectItems,
      const std::vector<size_t>& sortingKeyOrdinals);

  lp::ExprApi resolveGroupingExpression(
      const ExpressionPtr& expr,
      const std::vector<SelectItemPtr>& selectItems);
  lp::ExprApi resolveWithCache(
      const ExpressionPtr& expr,
      const std::vector<SelectItemPtr>& selectItems);
  std::vector<lp::ExprApi> resolveWithCache(
      const std::vector<ExpressionPtr>& exprs,
      const std::vector<SelectItemPtr>& selectItems);

  // Injected dependencies.
  std::shared_ptr<lp::PlanBuilder>& builder_;
  ExpressionPlanner& exprPlanner_;

  // Shared state across phases.
  folly::F14FastMap<const Expression*, lp::ExprApi> exprCache_;
  std::vector<lp::ExprApi> flatInputs_;
  std::vector<std::vector<lp::ExprApi>> groupingSets_;
  std::vector<lp::ExprApi> groupingKeys_;
  std::vector<std::vector<int32_t>> groupingSetsIndices_;
  std::vector<lp::ExprApi> projections_;
  std::vector<lp::ExprApi> aggregates_;
  std::optional<lp::ExprApi> filter_;
  std::vector<lp::SortKey> sortingKeys_;
  std::vector<std::string> outputNames_;
};

} // namespace axiom::sql::presto
