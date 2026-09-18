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

#include <span>

#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>
#include <functional>
#include <unordered_map>
#include <unordered_set>

#include "axiom/logical_plan/ExprApi.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/sql/presto/ParserOptions.h"
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

/// Options for context-specific validation during expression translation.
struct ExprOptions {
  /// Whether GROUPING() expressions are allowed. Defaults to false so that
  /// GROUPING() is rejected unless explicitly enabled by the GROUP BY path.
  bool allowGrouping{false};

  /// Whether subqueries are left unplanned, as SubqueryMarkerExpr markers, so
  /// that they can be planned in the scope their clause is evaluated in. Set
  /// for what an aggregating block evaluates above its AggregateNode; an
  /// aggregate call's arguments clear it, the node evaluating them itself.
  bool deferSubqueries{false};
};

// Defined in the .cpp: callers reach markers only through
// 'planIfMarker'.
class SubqueryMarkerExpr;

/// Translates Presto SQL AST expression nodes into logical plan ExprApi
/// objects. Handles all expression types (literals, comparisons, arithmetic,
/// function calls, casts, subqueries, etc.).
///
/// Can be used standalone for translating simple expressions (e.g. in
/// resolveSqlExpression) by passing nullptr for both callbacks. When used
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
  using SortingKeyResolver = std::function<
      lp::ExprApi(const ExpressionPtr& expr, ExprOptions options)>;

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

  /// Returns the qualifier a column reference should use when 'parts' names a
  /// relation in scope by a suffix of its qualified name (`schema.table` or
  /// `catalog.schema.table`); nullopt when none matches. Lets
  /// `schema.table.column` resolve without accepting a qualifier that names no
  /// relation. Can be nullptr when no relation scope is available.
  using RelationQualifierResolver = std::function<std::optional<std::string>(
      std::span<const std::string> parts)>;

  ExpressionPlanner(
      std::string user,
      SubqueryPlanner subqueryPlanner,
      SortingKeyResolver sortingKeyResolver,
      const ParserOptions& options,
      ShouldDropQualifier shouldDropQualifier = nullptr,
      ColumnResolver columnResolver = nullptr,
      RelationQualifierResolver relationQualifierResolver = nullptr)
      : user_{std::move(user)},
        subqueryPlanner_(std::move(subqueryPlanner)),
        sortingKeyResolver_(std::move(sortingKeyResolver)),
        shouldDropQualifier_(std::move(shouldDropQualifier)),
        columnResolver_(std::move(columnResolver)),
        relationQualifierResolver_(std::move(relationQualifierResolver)),
        options_{options} {}

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
  /// Rejects expressions nested deeper than the configured limit.
  lp::ExprApi toExpr(const ExpressionPtr& node, ExprOptions options = {});

  /// The output aliases in effect for one clause's translation. Held as one
  /// value so saving and restoring cannot miss a field.
  struct OutputAliases {
    /// Alias name to the expression the SELECT item projects.
    const folly::F14FastMap<std::string, facebook::velox::core::ExprPtr>* exprs{
        nullptr};

    /// Names that stay column references even when they name an alias.
    /// Lateral column aliases pass it; ORDER BY, where an output alias
    /// outranks a column of the same name, leaves it null.
    const folly::F14FastSet<std::string>* columnNames{nullptr};

    /// Alias names several output columns carry with different values. An
    /// Identifier naming one fails rather than picking a column.
    const folly::F14FastSet<std::string>* ambiguous{nullptr};
  };

  /// Sets the aliases an Identifier resolves against. An Identifier naming one
  /// resolves to the alias's expression rather than to a column. A qualified
  /// reference is a dereference, never an Identifier, so it always names a
  /// column of an input relation.
  void setOutputAliases(const OutputAliases& aliases) {
    outputAliases_ = aliases;
  }

  /// Clears output alias mappings.
  void clearOutputAliases() {
    outputAliases_ = {};
  }

  /// Plans 'expr' against the current scope if it is a marker, and returns
  /// the planned subquery; returns nullopt if it is not a marker. The result
  /// carries the name lp::Subquery derives from the subquery's output column.
  /// The caller decides when to call, and so which scope the subquery's
  /// correlated references bind to.
  ///
  /// Not memoized per marker: one marker can stand for occurrences on both
  /// sides of an AggregateNode, which need plans bound to different scopes.
  /// Callers substituting several occurrences in one position memoize.
  std::optional<lp::ExprApi> planIfMarker(
      const facebook::velox::core::ExprPtr& expr);

  /// Bounds the lifetime of the markers a query block hands out. Markers are
  /// shared within one of these and never across, so a block shares none with
  /// a sibling already planned, and a subquery body none with the block
  /// around it. Construct one per query block.
  class [[nodiscard]] MarkerScope {
   public:
    explicit MarkerScope(ExpressionPlanner& planner) : planner_{planner} {
      planner_.pushMarkerScope();
    }

    ~MarkerScope() {
      planner_.popMarkerScope();
    }

    MarkerScope(const MarkerScope&) = delete;
    MarkerScope& operator=(const MarkerScope&) = delete;

   private:
    ExpressionPlanner& planner_;
  };

  /// Makes named windows visible while translating one query specification.
  /// Example: `WindowScope scope{planner, query->windows(), options};`.
  ///
  /// Invariants:
  /// - Restores the enclosing query block's definitions on destruction.
  /// - Resolves and translates definitions in declaration order.
  class [[nodiscard]] WindowScope {
   public:
    WindowScope(
        ExpressionPlanner& planner,
        const std::vector<WindowDefinitionPtr>& definitions,
        ExprOptions options);

    ~WindowScope();

    const std::vector<lp::ExprApi>& definitionExprs() const {
      return definitionExprs_;
    }

    WindowScope(const WindowScope&) = delete;
    WindowScope& operator=(const WindowScope&) = delete;
    WindowScope(WindowScope&&) = delete;
    WindowScope& operator=(WindowScope&&) = delete;

   private:
    // Planner whose query-local window state this scope owns.
    ExpressionPlanner& planner_;

    // Enclosing query block's definitions, restored on destruction.
    folly::F14FastMap<std::string, lp::WindowSpec> saved_;

    // Translated definition parts used for aggregate discovery and grouping
    // validation.
    std::vector<lp::ExprApi> definitionExprs_;
  };

  /// Installs the SELECT output aliases for the duration of the scope, so an
  /// ORDER BY key naming one resolves to that item's expression rather than to
  /// a column of an input relation. See 'setOutputAliases'.
  class [[nodiscard]] OutputAliasScope {
   public:
    OutputAliasScope(
        ExpressionPlanner& planner,
        const std::vector<lp::ExprApi>& projections);

    ~OutputAliasScope() {
      planner_.setOutputAliases(saved_);
    }

    OutputAliasScope(const OutputAliasScope&) = delete;
    OutputAliasScope& operator=(const OutputAliasScope&) = delete;

   private:
    ExpressionPlanner& planner_;
    OutputAliases saved_;
    folly::F14FastMap<std::string, facebook::velox::core::ExprPtr> aliasExprs_;
    folly::F14FastSet<std::string> ambiguousAliases_;
  };

 private:
  // Start and end of one MarkerScope.
  void pushMarkerScope();
  void popMarkerScope();

  // Walks an IExpr tree collecting WindowCallExpr nodes. Stops recursion at
  // kWindow nodes (does not descend into window function arguments).
  static void findWindowExprs(
      const facebook::velox::core::ExprPtr& expr,
      std::unordered_map<
          const facebook::velox::core::IExpr*,
          facebook::velox::core::ExprPtr>& windowExprs,
      std::vector<const facebook::velox::core::IExpr*>& traversalOrder);

  // Converts a Window AST node into a WindowSpec.
  lp::WindowSpec convertWindow(
      const std::shared_ptr<Window>& window,
      ExprOptions options);

  // Translates the parts written in 'window' and adds them to 'spec'.
  lp::WindowSpec convertWindowParts(
      const std::shared_ptr<Window>& window,
      ExprOptions options,
      lp::WindowSpec spec);

  // Translates each AST argument via `toExpr`.
  std::vector<lp::ExprApi> translateArgs(
      const std::vector<ExpressionPtr>& arguments,
      ExprOptions options = {});

  // Builds an AggregateCallExpr from a FunctionCall AST node that has
  // DISTINCT, FILTER, or ORDER BY. FILTER and ORDER BY are evaluated by the
  // AggregateNode, so they translate in the aggregate's own scope.
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
  // Returns a SubqueryMarkerExpr instead when 'options' defers subqueries
  // and the body does not produce a lifted outer-scope aggregate.
  lp::ExprApi planSubquery(
      const SubqueryExpression* subquery,
      bool scalar,
      ExprOptions options);

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

  // Session user used to constant-fold CURRENT_USER at planning time. Empty
  // when no session context is available; CURRENT_USER translation then fails.
  const std::string user_;

  SubqueryPlanner subqueryPlanner_;
  SortingKeyResolver sortingKeyResolver_;
  ShouldDropQualifier shouldDropQualifier_;
  ColumnResolver columnResolver_;
  RelationQualifierResolver relationQualifierResolver_;

  ParserOptions options_;

  // Per-query cache for user-defined type lookups. Entries with nullptr values
  // represent negatively cached (not-found) types.
  folly::F14FastMap<std::string, facebook::velox::TypePtr> typeCache_;

  folly::
      F14FastMap<StatementPtr, lp::ExprApi, SubqueryAstHash, SubqueryAstEqual>
          subqueryCache_;

  // The markers handed out for one subquery text. A scalar use and an EXISTS
  // use are validated differently, so each gets its own.
  struct QueryMarkers {
    std::shared_ptr<const SubqueryMarkerExpr> scalar;
    std::shared_ptr<const SubqueryMarkerExpr> predicate;
  };

  // Markers handed out while resolving one block's clauses, so that textually
  // identical subqueries share one marker and stay one expression for
  // grouping-key matching. Sharing is safe because a marker binds no names;
  // each planning of it binds to the scope in place at that point, which is
  // how one marker can serve occurrences on both sides of an aggregation.
  // Swapped out by MarkerScope so a nested block starts with none.
  folly::
      F14FastMap<StatementPtr, QueryMarkers, SubqueryAstHash, SubqueryAstEqual>
          markersByQuery_;

  // Markers of the blocks enclosing the one being planned, restored as each
  // MarkerScope goes out of scope.
  std::vector<decltype(markersByQuery_)> enclosingMarkers_;

  // Output aliases the clause being translated resolves names against.
  OutputAliases outputAliases_;

  // Named windows in the current query block.
  folly::F14FastMap<std::string, lp::WindowSpec> windowDefinitions_;

  // Lambda parameters currently in scope. Entries are appended on entering a
  // lambda body and dropped on exit, so the vector grows from outermost
  // (front) to innermost (back); lookups search back-to-front to match SQL
  // lexical scoping. Used to prevent 'shouldDropQualifier_' from stripping
  // the qualifier of 'x.field' when 'x' names a lambda parameter rather than
  // an outer table alias.
  std::vector<std::string> lambdaParamScope_;

  // Current toExpr recursion depth, bounded by options_.maxExpressionDepth.
  uint32_t exprDepth_{0};
};

} // namespace axiom::sql::presto
