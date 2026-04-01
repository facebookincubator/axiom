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

#include <folly/container/F14Map.h>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include "axiom/logical_plan/ExprApi.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/sql/presto/ast/AstNodesAll.h"

namespace axiom::sql::presto {

namespace lp = facebook::axiom::logical_plan;

/// Canonicalizes a name to lowercase.
std::string canonicalizeName(const std::string& name);

/// Canonicalizes an AST identifier to lowercase.
std::string canonicalizeIdentifier(const Identifier& identifier);

/// Parses a TypeSignature AST node into a Velox type.
facebook::velox::TypePtr parseType(const TypeSignaturePtr& type);

/// Translates Presto SQL AST expression nodes into logical plan ExprApi
/// objects. Handles all expression types (literals, comparisons, arithmetic,
/// function calls, casts, subqueries, etc.).
///
/// Can be used standalone for translating simple expressions (e.g. in
/// parseSqlExpression) by passing nullptr for both callbacks. When used
/// within RelationPlanner to translate full queries, callbacks must be
/// provided to handle subqueries and ordinal sort keys in aggregates.
class ExpressionPlanner {
 public:
  /// Callback to plan a subquery. Takes the Query AST node, returns the built
  /// logical plan. Required when the expression may contain subquery
  /// expressions (e.g. IN (SELECT ...), scalar subqueries). Can be nullptr
  /// if subqueries are not expected.
  using SubqueryPlanner = std::function<lp::LogicalPlanNodePtr(Query* query)>;

  /// Callback to resolve ordinal sort keys (e.g. ORDER BY 1 inside aggregate
  /// functions). Required when the expression may contain aggregate function
  /// calls with ORDER BY clauses. Can be nullptr if aggregates with ORDER BY
  /// are not expected.
  using SortingKeyResolver =
      std::function<lp::ExprApi(const ExpressionPtr& expr)>;

  ExpressionPlanner(
      SubqueryPlanner subqueryPlanner,
      SortingKeyResolver sortingKeyResolver)
      : subqueryPlanner_(std::move(subqueryPlanner)),
        sortingKeyResolver_(std::move(sortingKeyResolver)) {}

  /// Translates an AST expression into an ExprApi. Optionally collects
  /// sideband data for aggregate and window function calls:
  /// - aggregateOptions: collects DISTINCT, FILTER, ORDER BY for aggregates.
  /// - windowOptions: collects WindowSpec for window function calls. When
  ///   provided, window function calls are returned as plain function calls
  ///   (without .over()) and the caller is responsible for extracting them
  ///   and applying the window spec.
  lp::ExprApi toExpr(
      const ExpressionPtr& node,
      std::unordered_map<
          const facebook::velox::core::IExpr*,
          lp::PlanBuilder::AggregateOptions>* aggregateOptions = nullptr,
      folly::F14FastMap<const facebook::velox::core::IExpr*, lp::WindowSpec>*
          windowOptions = nullptr);

  /// Sets alias-to-expression mappings for lateral column alias resolution.
  /// When set, Identifier nodes matching an alias key are resolved to the
  /// alias's expression instead of being treated as column references.
  /// Column names take priority over aliases — if a name exists in both
  /// 'columnNames' and 'aliasExprs', it is treated as a column reference.
  void setLateralAliases(
      const std::unordered_map<std::string, facebook::velox::core::ExprPtr>*
          aliasExprs,
      const std::unordered_set<std::string>* columnNames) {
    aliasExprs_ = aliasExprs;
    columnNames_ = columnNames;
  }

  /// Clears lateral column alias mappings.
  void clearLateralAliases() {
    aliasExprs_ = nullptr;
    columnNames_ = nullptr;
  }

 private:
  // Converts a Window AST node into a WindowSpec.
  lp::WindowSpec convertWindow(
      const std::shared_ptr<Window>& window,
      std::unordered_map<
          const facebook::velox::core::IExpr*,
          lp::PlanBuilder::AggregateOptions>* aggregateOptions);

  SubqueryPlanner subqueryPlanner_;
  SortingKeyResolver sortingKeyResolver_;

  // Lateral column alias mappings. When non-null, Identifier nodes matching
  // a key are resolved to the corresponding expression, unless the name also
  // appears in columnNames_ (column names take priority).
  const std::unordered_map<std::string, facebook::velox::core::ExprPtr>*
      aliasExprs_{nullptr};
  const std::unordered_set<std::string>* columnNames_{nullptr};
};

} // namespace axiom::sql::presto
