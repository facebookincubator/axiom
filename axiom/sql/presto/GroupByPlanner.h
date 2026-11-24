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
#include "folly/container/F14Set.h"
#include "velox/parse/IExpr.h"

namespace axiom::sql::presto {

class SelectProjection;

namespace lp = facebook::axiom::logical_plan;

/// Plans GROUP BY, ROLLUP, CUBE, and GROUPING SETS clauses. Constructed
/// per-query to hold intermediate state during aggregation planning.
class GroupByPlanner {
 public:
  /// Marker function name emitted by ExpressionPlanner for GROUPING() AST
  /// nodes.
  static constexpr std::string_view kGroupingFunctionName = "grouping";

  /// 'selectProjection' holds the references made to SELECT items through
  /// their output aliases, and computes what the final projection reads.
  GroupByPlanner(
      std::shared_ptr<lp::PlanBuilder>& builder,
      ExpressionPlanner& exprPlanner,
      SelectProjection& selectProjection)
      : builder_(builder),
        exprPlanner_(exprPlanner),
        selectProjection_(selectProjection) {}

  /// Plans a GROUP BY clause with optional HAVING and ORDER BY.
  /// When 'distinct' is true (GROUP BY DISTINCT), duplicate grouping sets
  /// are removed after expansion. Accepts a flat list of pre-resolved
  /// ExprApi items (AllColumns / SelectColumns must be expanded by the
  /// caller). Window-definition expressions participate in semantic
  /// validation but are not projected.
  void plan(
      const std::vector<GroupingElementPtr>& groupingElements,
      bool distinct,
      const std::vector<lp::ExprApi>& selectExprs,
      const ExpressionPtr& having,
      const OrderByPtr& orderBy,
      std::vector<lp::ExprApi> windowDefinitionExprs) &&;

  /// Returns whether the query block contains an aggregate call and rejects
  /// nested aggregate calls.
  static bool containsAggregate(
      const std::vector<SelectItemPtr>& selectItems,
      const ExpressionPtr& having,
      const OrderByPtr& orderBy,
      const std::vector<WindowDefinitionPtr>& windowDefinitions);

  /// Plans implicit global aggregation (e.g. SELECT count(*) FROM t),
  /// including any ORDER BY over the aggregates. Returns true if aggregation
  /// was added. AllColumns / SelectColumns must be expanded by the caller.
  bool tryPlanGlobalAgg(
      const std::vector<lp::ExprApi>& selectExprs,
      const ExpressionPtr& having,
      const OrderByPtr& orderBy,
      const std::vector<lp::ExprApi>& windowDefinitionExprs) &&;

 private:
  std::vector<std::vector<lp::ExprApi>> expandGroupingSets(
      const std::vector<GroupingElementPtr>& groupingElements,
      const std::vector<lp::ExprApi>& selectExprs);
  void deduplicateGroupingKeys();
  void collectAggregates(
      const std::vector<lp::ExprApi>& selectExprs,
      const ExpressionPtr& having,
      const OrderByPtr& orderBy,
      const std::vector<lp::ExprApi>& windowDefinitionExprs);

  // Adds the Aggregate node and plans expressions evaluated above it.
  void buildAggregationPlan(
      const std::vector<lp::ExprApi>& selectExprs,
      const OrderByPtr& orderBy,
      std::vector<lp::ExprApi> windowDefinitionExprs);

  void addAggregate(bool useGroupingSets);
  void rewritePostAggregateExprs(
      const std::vector<lp::ExprApi>& windowDefinitionExprs);

  // Replaces the references to SELECT items in the grouping keys and
  // aggregates, which are evaluated below the aggregate.
  void lowerBelowAggregate();

  std::vector<size_t> resolveSortOrdinals();
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
  SelectProjection& selectProjection_;

  // Shared state across phases.
  folly::F14FastMap<const Expression*, lp::ExprApi> exprCache_;
  std::vector<lp::ExprApi> flatInputs_;
  std::vector<std::vector<lp::ExprApi>> groupingSets_;
  std::vector<lp::ExprApi> groupingKeys_;
  std::vector<std::vector<int32_t>> groupingSetsIndices_;
  // Maps each grouping-key expression to its index in groupingKeys_.
  facebook::velox::core::ExprMap<int32_t> groupingKeyToIndex_;
  std::vector<lp::ExprApi> projections_;

  // Deduplicated aggregate expressions. Aggregate options are embedded in
  // AggregateCallExpr nodes within the IExpr tree.
  std::vector<lp::ExprApi> aggregates_;

  // 'aggregates_' with the references to SELECT items replaced.
  std::vector<lp::ExprApi> loweredAggregates_;

  // What the grouping keys and aggregates read for each SELECT item; nullptr
  // for an item they do not read. See SelectProjection::lowerBelowAggregate.
  std::vector<facebook::velox::core::ExprPtr> itemValues_;

  // SELECT items a GROUP BY position names.
  folly::F14FastSet<size_t> groupedItems_;

  std::optional<lp::ExprApi> filter_;
  std::vector<lp::ExprApi> sortingKeyExprs_;
  // 1-based SELECT item each sort key names by position or by output alias;
  // 0 for a key that is an expression. Parallel to 'sortingKeyExprs_'.
  std::vector<size_t> sortKeyItems_;
  // Names of the aggregate's output columns, in output order. A column whose
  // bare name is ambiguous is reachable only through its relation alias.
  std::vector<lp::PlanBuilder::OutputColumnName> outputColumns_;
};

} // namespace axiom::sql::presto
