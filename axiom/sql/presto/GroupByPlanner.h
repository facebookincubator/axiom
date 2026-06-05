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
#include "velox/parse/IExpr.h"

namespace axiom::sql::presto {

namespace lp = facebook::axiom::logical_plan;

/// Plans GROUP BY, ROLLUP, CUBE, and GROUPING SETS clauses. Constructed
/// per-query to hold intermediate state during aggregation planning.
class GroupByPlanner {
 public:
  /// Marker function name emitted by ExpressionPlanner for GROUPING() AST
  /// nodes.
  static constexpr std::string_view kGroupingFunctionName = "grouping";

  GroupByPlanner(
      std::shared_ptr<lp::PlanBuilder>& builder,
      ExpressionPlanner& exprPlanner)
      : builder_(builder), exprPlanner_(exprPlanner) {}

  /// Plans a GROUP BY clause with optional HAVING and ORDER BY.
  /// When 'distinct' is true (GROUP BY DISTINCT), duplicate grouping sets
  /// are removed after expansion. Accepts a flat list of pre-resolved
  /// ExprApi items (AllColumns / SelectColumns must be expanded by the
  /// caller).
  void plan(
      const std::vector<GroupingElementPtr>& groupingElements,
      bool distinct,
      const std::vector<lp::ExprApi>& selectExprs,
      const ExpressionPtr& having,
      const OrderByPtr& orderBy) &&;

  /// Detects implicit global aggregation (e.g. SELECT count(*) FROM t)
  /// and plans it. Returns true if aggregation was added. Accepts raw AST
  /// select items and resolves them internally.
  bool tryPlanGlobalAgg(
      const std::vector<SelectItemPtr>& selectItems,
      const ExpressionPtr& having) &&;

 private:
  std::vector<std::vector<lp::ExprApi>> expandGroupingSets(
      const std::vector<GroupingElementPtr>& groupingElements,
      const std::vector<lp::ExprApi>& selectExprs);
  void deduplicateGroupingKeys();
  void collectAggregates(
      const std::vector<lp::ExprApi>& selectExprs,
      const ExpressionPtr& having,
      const OrderByPtr& orderBy);
  void addAggregate(bool useGroupingSets);
  void rewritePostAggregateExprs();

  // Projects nested window functions (kWindow nodes inside non-window
  // expressions) into a separate plan node via builder_->with(). Records
  // replacement mappings in 'keyInputs' for the caller's projection rewrite.
  void projectNestedWindows(
      const std::function<facebook::velox::core::ExprPtr(
          const facebook::velox::core::ExprPtr&)>& rewriteIExpr,
      folly::F14FastMap<
          facebook::velox::core::ExprPtr,
          facebook::velox::core::ExprPtr,
          facebook::velox::core::IExprHash,
          facebook::velox::core::IExprEqual>& keyInputs);

  std::vector<size_t> resolveSortOrdinals(const OrderByPtr& orderBy);
  bool isIdentityProjection() const;

  lp::ExprApi resolveGroupingExpression(
      const ExpressionPtr& expr,
      const std::vector<lp::ExprApi>& selectExprs);
  lp::ExprApi resolveWithCache(
      const ExpressionPtr& expr,
      const std::vector<lp::ExprApi>& selectExprs);
  std::vector<lp::ExprApi> resolveWithCache(
      const std::vector<ExpressionPtr>& exprs,
      const std::vector<lp::ExprApi>& selectExprs);

  // Replaces `grouping(...)` marker calls in 'exprs' with
  // `element_at(array[bitmasks...], $grouping_set_id + 1)`.
  void resolveGroupingCalls(std::vector<lp::ExprApi>& exprs);

  // Replaces a single `grouping(...)` marker in 'expr' (recursively) with
  // the bitmask lookup expression. Returns the original expr if unchanged.
  facebook::velox::core::ExprPtr rewriteGroupingMarker(
      const facebook::velox::core::ExprPtr& expr);

  // Injected dependencies.
  std::shared_ptr<lp::PlanBuilder>& builder_;
  ExpressionPlanner& exprPlanner_;

  // Shared state across phases.
  folly::F14FastMap<const Expression*, lp::ExprApi> exprCache_;
  std::vector<lp::ExprApi> flatInputs_;
  std::vector<std::vector<lp::ExprApi>> groupingSets_;
  std::vector<lp::ExprApi> groupingKeys_;
  std::vector<std::vector<int32_t>> groupingSetsIndices_;
  // Maps grouping key column name → index in groupingKeys_ for GROUPING()
  // resolution.
  folly::F14FastMap<std::string, int32_t> groupingColumnToIndex_;
  std::vector<lp::ExprApi> projections_;

  // Deduplicated aggregate expressions. Aggregate options are embedded in
  // AggregateCallExpr nodes within the IExpr tree.
  std::vector<lp::ExprApi> aggregates_;

  std::optional<lp::ExprApi> filter_;
  std::vector<lp::ExprApi> sortingKeyExprs_;
  std::vector<std::string> outputNames_;
};

} // namespace axiom::sql::presto
