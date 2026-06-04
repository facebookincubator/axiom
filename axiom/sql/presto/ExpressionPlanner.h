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

#include <folly/container/F14Map.h>

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

/// Returns true if `query` is a scalar subquery whose aggregate(s)
/// bind to an outer scope per SQL's innermost-binding rule, making
/// the body a candidate for outer-scope-aggregate lift. AST-only and
/// conservative — unqualified column refs in a body with a FROM
/// clause are treated as inner-scope; complex FROM shapes (joins,
/// subqueries) are rejected.
bool isOuterScopeAggregateLiftCandidate(Query* query);

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
  /// Carries a planned subquery's logical plan together with a flag marking
  /// whether the subquery is correlated. Correlated subqueries must not be
  /// cached because their planned output binds physical column names from a
  /// specific outer scope; reusing the plan under a different outer scope
  /// would leave dangling input references.
  struct SubqueryPlanResult {
    /// Built logical plan for the subquery.
    lp::LogicalPlanNodePtr plan;

    /// True if planning resolved at least one column reference against the
    /// outer scope (the subquery is correlated).
    bool touchedOuterScope{false};
  };

  /// Callback to plan a subquery. Takes the Query AST node, returns the built
  /// logical plan and a flag indicating whether the subquery is correlated.
  /// Required when the expression may contain subquery expressions (e.g. IN
  /// (SELECT ...), scalar subqueries). Can be nullptr if subqueries are not
  /// expected.
  using SubqueryPlanner = std::function<SubqueryPlanResult(Query* query)>;

  /// Callback to resolve ordinal sort keys (e.g. ORDER BY 1 inside aggregate
  /// functions). Required when the expression may contain aggregate function
  /// calls with ORDER BY clauses. Can be nullptr if aggregates with ORDER BY
  /// are not expected.
  using SortingKeyResolver =
      std::function<lp::ExprApi(const ExpressionPtr& expr)>;

  /// Returns true if the table qualifier should be dropped from a column
  /// reference (e.g. t.col -> col).
  using ShouldDropQualifier = std::function<
      bool(const std::string& qualifier, const std::string& name)>;

  /// Checks if a dotted name prefix resolves to a column or table alias in the
  /// current scope. Returns true if 'first' is a column name OR if 'first' is
  /// a table alias qualifying 'second' as a column. Used for column-first enum
  /// literal resolution. Can be nullptr when no column scope is available.
  using ColumnResolver =
      std::function<bool(const std::string& first, const std::string& second)>;

  ExpressionPlanner(
      SubqueryPlanner subqueryPlanner,
      SortingKeyResolver sortingKeyResolver,
      ShouldDropQualifier shouldDropQualifier = nullptr,
      ColumnResolver columnResolver = nullptr)
      : subqueryPlanner_(std::move(subqueryPlanner)),
        sortingKeyResolver_(std::move(sortingKeyResolver)),
        shouldDropQualifier_(std::move(shouldDropQualifier)),
        columnResolver_(std::move(columnResolver)) {}

  /// Finds WindowCallExpr nodes nested inside non-window expressions.
  /// Skips top-level window projections (where the ExprApi itself is a
  /// WindowCallExpr). Populates 'windowExprs' with pointers to the found
  /// WindowCallExpr nodes (raw pointer -> shared pointer) and
  /// 'traversalOrder' with raw pointers in left-to-right order for
  /// deterministic plan generation.
  static void findNestedWindowExprs(
      const std::vector<lp::ExprApi>& exprs,
      std::unordered_map<
          const facebook::velox::core::IExpr*,
          facebook::velox::core::ExprPtr>& windowExprs,
      std::vector<const facebook::velox::core::IExpr*>& traversalOrder);

  /// Translates an AST expression into an ExprApi. Aggregate options
  /// (DISTINCT, FILTER, ORDER BY) are embedded in the returned ExprApi via
  /// AggregateCallExpr. Window functions are embedded via WindowCallExpr.
  lp::ExprApi toExpr(const ExpressionPtr& node);

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
  // Walks an IExpr tree collecting WindowCallExpr nodes. Stops recursion at
  // kWindow nodes (does not descend into window function arguments).
  static void findWindowExprs(
      const facebook::velox::core::ExprPtr& expr,
      std::unordered_map<
          const facebook::velox::core::IExpr*,
          facebook::velox::core::ExprPtr>& windowExprs,
      std::vector<const facebook::velox::core::IExpr*>& traversalOrder);

  // Converts a Window AST node into a WindowSpec.
  lp::WindowSpec convertWindow(const std::shared_ptr<Window>& window);

  // Translates each AST argument via `toExpr`.
  std::vector<lp::ExprApi> translateArgs(
      const std::vector<ExpressionPtr>& arguments);

  // Builds an AggregateCallExpr from a FunctionCall AST node that has
  // DISTINCT, FILTER, or ORDER BY.
  lp::ExprApi toAggregateCallExpr(
      const FunctionCall* call,
      const std::string& funcName,
      const std::vector<lp::ExprApi>& args);

  // Plans `subquery` via `subqueryPlanner_` and wraps the resulting
  // plan as an `lp::Subquery` expression, caching by AST identity.
  // When `scalar` is true, applies the outer-scope-aggregate lift
  // (`tryLiftPureOuterAggregate`), rejects unhandled outer-scope-
  // aggregate shapes, and enforces the SQL rule that a subquery used
  // as a scalar expression must return exactly one column. EXISTS
  // (which only checks row presence) passes false to skip those
  // checks.
  lp::ExprApi planSubquery(const SubqueryExpression* subquery, bool scalar);

  // SQL binds an aggregate to the innermost query block containing all
  // its column references. A scalar subquery body whose aggregates all
  // take outer-scope-only arguments is therefore an outer-scope
  // aggregation. Detects that shape (AST shape + LP-level check that
  // each aggregate's args reference no inner-source column) and
  // rewrites the subquery so each aggregate becomes
  //
  //   agg(outer_args) FILTER (WHERE EXISTS(SELECT 1 FROM <body's FROM>
  //                                        [WHERE <body's WHERE>]))
  //
  // with any surrounding expression (arithmetic, multiple aggregates
  // in one SELECT item) preserved around the lifted aggregates.
  // `bodyResult` must be the result of planning `query` via
  // `subqueryPlanner_`. Returns std::nullopt when the shape doesn't
  // match; the caller falls through to normal subquery planning.
  std::optional<lp::ExprApi> tryLiftPureOuterAggregate(
      Query* query,
      const SubqueryPlanResult& bodyResult);

  // Resolves a type signature, trying built-in Velox types first, then
  // connector-based resolution for dotted names (e.g., "catalog.schema.Type").
  facebook::velox::TypePtr resolveType(const TypeSignaturePtr& type);

  // Dedups subquery expressions across textually-identical scalar subquery
  // occurrences (e.g. same subquery in SELECT and GROUP BY of the same
  // query). Keyed by AST structural equality on the inner Query, so two
  // distinct AST nodes with equal Query bodies share one ExprApi (and
  // therefore one inner LogicalPlanNode). Downstream ToGraph dedups
  // subqueries by inner-plan pointer, so sharing the plan collapses them
  // to a single result column. Holding StatementPtr (not a raw pointer)
  // keeps the cached AST alive for the cache's lifetime.
  struct SubqueryAstHash {
    size_t operator()(const StatementPtr& query) const {
      return query->hash();
    }
  };

  struct SubqueryAstEqual {
    bool operator()(const StatementPtr& lhs, const StatementPtr& rhs) const {
      return *lhs == *rhs;
    }
  };

  SubqueryPlanner subqueryPlanner_;
  SortingKeyResolver sortingKeyResolver_;
  ShouldDropQualifier shouldDropQualifier_;
  ColumnResolver columnResolver_;

  // Per-query cache for user-defined type lookups. Entries with nullptr values
  // represent negatively cached (not-found) types.
  folly::F14FastMap<std::string, facebook::velox::TypePtr> typeCache_;

  folly::
      F14FastMap<StatementPtr, lp::ExprApi, SubqueryAstHash, SubqueryAstEqual>
          subqueryCache_;

  // Lateral column alias mappings. When non-null, Identifier nodes matching
  // a key are resolved to the corresponding expression, unless the name also
  // appears in columnNames_ (column names take priority).
  const std::unordered_map<std::string, facebook::velox::core::ExprPtr>*
      aliasExprs_{nullptr};
  const std::unordered_set<std::string>* columnNames_{nullptr};

  // Lambda parameters currently in scope. Entries are appended on entering a
  // lambda body and dropped on exit, so the vector grows from outermost
  // (front) to innermost (back); lookups search back-to-front to match SQL
  // lexical scoping. Used to prevent 'shouldDropQualifier_' from stripping
  // the qualifier of 'x.field' when 'x' names a lambda parameter rather than
  // an outer table alias.
  std::vector<std::string> lambdaParamScope_;
};

} // namespace axiom::sql::presto
