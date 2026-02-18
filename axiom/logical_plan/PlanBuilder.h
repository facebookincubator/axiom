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

#include "axiom/logical_plan/ExprApi.h"
#include "axiom/logical_plan/ExprResolver.h"
#include "axiom/logical_plan/LogicalPlanNode.h"
#include "axiom/logical_plan/NameAllocator.h"
#include "velox/core/QueryCtx.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/PlanNodeIdGenerator.h"

namespace facebook::axiom::logical_plan {

class NameMappings;

/// Placeholder SQL parser that always throws. Used as a fallback when SQL
/// parsing is not available.
class ThrowingSqlExpressionsParser : public velox::parse::SqlExpressionsParser {
 public:
  velox::core::ExprPtr parseExpr(const std::string& expr) override {
    VELOX_USER_FAIL("SQL parsing is not supported");
  }

  std::vector<velox::core::ExprPtr> parseExprs(
      const std::string& expr) override {
    VELOX_USER_FAIL("SQL parsing is not supported");
  }

  velox::parse::OrderByClause parseOrderByExpr(
      const std::string& expr) override {
    VELOX_USER_FAIL("SQL parsing is not supported");
  }
};

/// Builds logical plan trees using a fluent API. Supports table scans,
/// filters, projections, aggregations, joins, set operations, sorts, limits,
/// unnest, and table writes.
///
/// Most methods come in two flavors:
///
/// - **SQL-based** — accept SQL expression strings, e.g. "a + b AS c",
///   "sum(x) filter (where y > 0)". Parsed using DuckDB SQL dialect by
///   default. Callers can provide a custom SQL parser via Context.
/// - **ExprApi-based** — accept ExprApi objects built programmatically with
///   Col(), Lit(), Sql(), Subquery(), etc. Required for subqueries.
///
/// Example using SQL strings:
///
///   PlanBuilder(ctx)
///     .tableScan("orders")
///     .filter("o_totalprice > 100")
///     .aggregate({"o_custkey"}, {"sum(o_totalprice)"})
///     .build();
///
/// Equivalent using ExprApi:
///
///   PlanBuilder(ctx)
///     .tableScan("orders")
///     .filter(Col("o_totalprice") > 100)
///     .aggregate({Col("o_custkey")}, {Sql("sum(o_totalprice)")})
///     .build();
///
/// Share a Context across multiple PlanBuilder instances when building queries
/// with two or more table scans to ensure globally unique plan-node IDs and
/// column names. Set the default connector ID in Context to omit it from
/// individual tableScan calls:
///
///   PlanBuilder::Context ctx(connectorId);
///   auto plan = PlanBuilder(ctx)
///     .tableScan("orders")
///     .join(PlanBuilder(ctx).tableScan("customers"), "o_custkey = c_custkey",
///           JoinType::kInner)
///     .build();
///
/// Set 'enableCoercions' to true to automatically insert implicit CASTs when
/// expression types don't match (e.g. INTEGER + BIGINT). Set Context.queryCtx
/// to enable constant folding, i.e. evaluating constant expressions at
/// plan-build time (e.g. 1 + 2 becomes 3).
class PlanBuilder {
 public:
  /// Shared state across PlanBuilder instances. Pass the same Context to
  /// multiple builders to keep plan-node IDs and column names globally unique.
  struct Context {
    /// Identifies the connector to use when tableScan omits the catalog prefix.
    std::optional<std::string> defaultConnectorId;

    /// Parses SQL expression strings. Defaults to DuckDB dialect.
    std::shared_ptr<velox::parse::SqlExpressionsParser> sqlParser;

    /// Generates unique plan-node IDs across all builders sharing this Context.
    std::shared_ptr<velox::core::PlanNodeIdGenerator> planNodeIdGenerator;

    /// Allocates unique internal column names across all builders sharing this
    /// Context.
    std::shared_ptr<NameAllocator> nameAllocator;

    /// Enables constant folding when set. Also provides a memory pool for
    /// evaluating constant expressions.
    std::shared_ptr<velox::core::QueryCtx> queryCtx;

    /// Rewrites function calls during expression resolution, e.g. maps
    /// SQL function names to Velox runtime functions.
    ExprResolver::FunctionRewriteHook hook;

    /// Provides memory for allocating literal values during constant folding.
    /// Automatically derived from queryCtx.
    std::shared_ptr<velox::memory::MemoryPool> pool;

    explicit Context(
        const std::optional<std::string>& defaultConnectorId = std::nullopt,
        std::shared_ptr<velox::core::QueryCtx> queryCtxPtr = nullptr,
        ExprResolver::FunctionRewriteHook hook = nullptr,
        std::shared_ptr<velox::parse::SqlExpressionsParser> sqlParser =
            std::make_shared<velox::parse::DuckSqlExpressionsParser>(
                velox::parse::ParseOptions{.parseInListAsArray = false}))
        : defaultConnectorId{defaultConnectorId},
          sqlParser{std::move(sqlParser)},
          planNodeIdGenerator{
              std::make_shared<velox::core::PlanNodeIdGenerator>()},
          nameAllocator{std::make_shared<NameAllocator>()},
          queryCtx{std::move(queryCtxPtr)},
          hook{std::move(hook)},
          pool{
              queryCtx && queryCtx->pool()
                  ? queryCtx->pool()->addLeafChild("literals")
                  : nullptr} {}
  };

  /// Resolves a column reference from an outer query scope. Used to support
  /// correlated subqueries where an inner query references columns from the
  /// outer query.
  using Scope = std::function<ExprPtr(
      const std::optional<std::string>& alias,
      const std::string& name)>;

  explicit PlanBuilder(bool enableCoercions = false, Scope outerScope = nullptr)
      : PlanBuilder{Context{}, enableCoercions, std::move(outerScope)} {}

  explicit PlanBuilder(
      const Context& context,
      bool enableCoercions = false,
      Scope outerScope = nullptr)
      : defaultConnectorId_{context.defaultConnectorId},
        planNodeIdGenerator_{context.planNodeIdGenerator},
        nameAllocator_{context.nameAllocator},
        outerScope_{std::move(outerScope)},
        sqlParser_{context.sqlParser},
        enableCoercions_{enableCoercions},
        resolver_{
            context.queryCtx,
            enableCoercions,
            context.hook,
            context.pool,
            context.planNodeIdGenerator} {
    VELOX_CHECK_NOT_NULL(planNodeIdGenerator_);
    VELOX_CHECK_NOT_NULL(nameAllocator_);
  }

  /// Adds a leaf Values node producing literal rows. Must be the first node
  /// in the plan.
  PlanBuilder& values(
      const velox::RowTypePtr& rowType,
      std::vector<velox::Variant> rows);

  /// @overload Takes pre-built RowVectors.
  PlanBuilder& values(const std::vector<velox::RowVectorPtr>& values);

  /// @overload Takes column names and per-row expressions.
  PlanBuilder& values(
      const std::vector<std::string>& names,
      const std::vector<std::vector<ExprApi>>& values);

  /// @overload Takes column names and per-row SQL strings.
  PlanBuilder& values(
      const std::vector<std::string>& names,
      const std::vector<std::vector<std::string>>& values) {
    std::vector<std::vector<ExprApi>> exprs;
    exprs.reserve(values.size());
    for (const auto& expr : values) {
      exprs.emplace_back(parse(expr));
    }
    return this->values(names, exprs);
  }

  /// Equivalent to SELECT col1, col2,.. FROM <tableName>.
  /// Adds a leaf TableScan node reading specified columns from a table.
  PlanBuilder& tableScan(
      const std::string& connectorId,
      const std::string& tableName,
      const std::vector<std::string>& columnNames);

  PlanBuilder& tableScan(
      const std::string& connectorId,
      const char* tableName,
      std::initializer_list<const char*> columnNames) {
    return tableScan(
        connectorId,
        tableName,
        std::vector<std::string>{columnNames.begin(), columnNames.end()});
  }

  /// @overload Uses the default connector ID from Context.
  PlanBuilder& tableScan(
      const std::string& tableName,
      const std::vector<std::string>& columnNames);

  PlanBuilder& tableScan(
      const char* tableName,
      std::initializer_list<const char*> columnNames) {
    return tableScan(
        tableName,
        std::vector<std::string>{columnNames.begin(), columnNames.end()});
  }

  /// Equivalent to SELECT * FROM <tableName>.
  PlanBuilder& tableScan(
      const std::string& connectorId,
      const std::string& tableName,
      bool includeHiddenColumns = false);

  PlanBuilder& tableScan(
      const std::string& connectorId,
      const char* tableName,
      bool includeHiddenColumns = false) {
    return tableScan(connectorId, std::string{tableName}, includeHiddenColumns);
  }

  /// @overload Uses the default connector ID from Context.
  PlanBuilder& tableScan(
      const std::string& tableName,
      bool includeHiddenColumns = false);

  /// Removes hidden columns (e.g. $row_id) from the output.
  PlanBuilder& dropHiddenColumns();

  /// Equivalent to SELECT * FROM t1, t2, t3...
  ///
  /// Shortcut for
  ///
  ///   PlanBuilder(context)
  ///     .tableScan(t1)
  ///     .crossJoin(PlanBuilder(context).tableScan(t2))
  ///     .crossJoin(PlanBuilder(context).tableScan(t3))
  ///     ...
  ///     .build();
  PlanBuilder& from(const std::vector<std::string>& tableNames);

  /// Adds a Filter node that keeps only rows matching the predicate.
  PlanBuilder& filter(const std::string& predicate);

  /// @overload Takes an ExprApi predicate.
  PlanBuilder& filter(const ExprApi& predicate);

  /// Adds a Project node that computes new columns, replacing the current
  /// output. Use 'with' to append columns instead.
  PlanBuilder& project(const std::vector<std::string>& projections) {
    return project(parse(projections));
  }

  PlanBuilder& project(std::initializer_list<std::string> projections) {
    return project(std::vector<std::string>{projections});
  }

  PlanBuilder& project(const std::vector<ExprApi>& projections);

  PlanBuilder& project(std::initializer_list<ExprApi> projections) {
    return project(std::vector<ExprApi>{projections});
  }

  /// An alias for 'project'.
  PlanBuilder& map(const std::vector<std::string>& projections) {
    return project(projections);
  }

  PlanBuilder& map(std::initializer_list<std::string> projections) {
    return map(std::vector<std::string>{projections});
  }

  PlanBuilder& map(const std::vector<ExprApi>& projections) {
    return project(projections);
  }

  PlanBuilder& map(std::initializer_list<ExprApi> projections) {
    return map(std::vector<ExprApi>{projections});
  }

  /// Similar to 'project', but appends 'projections' to the existing columns.
  PlanBuilder& with(const std::vector<std::string>& projections) {
    return with(parse(projections));
  }

  PlanBuilder& with(std::initializer_list<std::string> projections) {
    return with(std::vector<std::string>{projections});
  }

  PlanBuilder& with(const std::vector<ExprApi>& projections);

  PlanBuilder& with(std::initializer_list<ExprApi> projections) {
    return with(std::vector<ExprApi>{projections});
  }

  /// Adds an Aggregate node with the specified grouping keys and aggregate
  /// functions.
  PlanBuilder& aggregate(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates);

  struct AggregateOptions {
    AggregateOptions(
        velox::core::ExprPtr filter,
        std::vector<SortKey> orderBy,
        bool distinct)
        : filter(std::move(filter)),
          orderBy(std::move(orderBy)),
          distinct(distinct) {}

    AggregateOptions() = default;

    velox::core::ExprPtr filter;
    std::vector<SortKey> orderBy;
    bool distinct{false};
  };

  PlanBuilder& aggregate(
      const std::vector<ExprApi>& groupingKeys,
      const std::vector<ExprApi>& aggregates,
      const std::vector<AggregateOptions>& options);

  /// Builds an aggregate node with SQL GROUPING SETS semantics.
  ///
  /// This is the most flexible overload, allowing explicit control over
  /// grouping key ordering and index-based grouping set specification. Similar
  /// to SQL's GROUPING SETS with ordinal references: GROUPING SETS ((1, 2), (1,
  /// 2, 3)).
  ///
  /// @param groupingKeys All grouping key expressions. Output column order
  /// matches the order of keys in this vector.
  /// @param groupingSets Vector of grouping sets, where each set is a vector
  /// of indices into groupingKeys. For example, with groupingKeys [a, b, c]:
  ///   - ROLLUP(a,b,c) would have sets: [[0,1,2], [0,1], [0], []]
  ///   - CUBE(a,b) would have sets: [[0,1], [0], [1], []]
  /// @param aggregates The aggregate expressions to compute.
  /// @param options Per-aggregate options (filter, orderBy, distinct).
  /// @param groupingSetIndexName Name of the output column that identifies
  /// which grouping set each row belongs to.
  PlanBuilder& aggregate(
      const std::vector<ExprApi>& groupingKeys,
      const std::vector<std::vector<int32_t>>& groupingSets,
      const std::vector<ExprApi>& aggregates,
      const std::vector<AggregateOptions>& options,
      const std::string& groupingSetIndexName);

  /// Aggregate with grouping sets support for ROLLUP, CUBE, and GROUPING SETS.
  ///
  /// Convenience overload that extracts unique grouping keys from the sets.
  /// Grouping keys appear in output in first-occurrence order across all sets.
  /// For explicit key ordering, use the overload with separate groupingKeys and
  /// index-based sets.
  ///
  /// @param groupingSets Vector of grouping sets, where each set contains
  /// the grouping key expressions for that set.
  /// Example: {{"a", "b"}, {"a"}, {}} for ROLLUP(a, b)
  /// @param aggregates The aggregate expressions to compute.
  /// @param options Per-aggregate options (filter, orderBy, distinct).
  /// @param groupingSetIndexName Name of the output column that identifies
  /// which grouping set each row belongs to.
  PlanBuilder& aggregate(
      const std::vector<std::vector<ExprApi>>& groupingSets,
      const std::vector<ExprApi>& aggregates,
      const std::vector<AggregateOptions>& options,
      const std::string& groupingSetIndexName);

  /// Convenience overload that parses SQL strings for grouping keys and
  /// aggregates.
  PlanBuilder& aggregate(
      const std::vector<std::vector<std::string>>& groupingSets,
      const std::vector<std::string>& aggregates,
      const std::string& groupingSetIndexName);

  /// Convenience overload that parses SQL strings for grouping keys and
  /// aggregates. This version takes explicit grouping keys and index-based
  /// grouping sets.
  PlanBuilder& aggregate(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::vector<int32_t>>& groupingSets,
      const std::vector<std::string>& aggregates,
      const std::vector<AggregateOptions>& options,
      const std::string& groupingSetIndexName);

  /// ROLLUP convenience API. Expands ROLLUP(a, b, c) to grouping sets:
  /// [[0,1,2], [0,1], [0], []]
  PlanBuilder& rollup(
      const std::vector<ExprApi>& groupingKeys,
      const std::vector<ExprApi>& aggregates,
      const std::vector<AggregateOptions>& options,
      const std::string& groupingSetIndexName);

  PlanBuilder& rollup(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::string& groupingSetIndexName);

  /// CUBE convenience API. Expands CUBE(a, b) to grouping sets:
  /// [[0,1], [0], [1], []]
  /// Supports at most 30 grouping keys (2^30 grouping sets).
  PlanBuilder& cube(
      const std::vector<ExprApi>& groupingKeys,
      const std::vector<ExprApi>& aggregates,
      const std::vector<AggregateOptions>& options,
      const std::string& groupingSetIndexName);

  PlanBuilder& cube(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::string& groupingSetIndexName);

  /// Adds an Aggregate node that deduplicates rows on all output columns.
  PlanBuilder& distinct();

  /// Starts or continues the plan with an Unnest node. Uses auto-generated
  /// names for unnested columns. Use the version of 'unnest' API that takes
  /// ExprApi together with ExprApi::unnestAs to provide aliases for unnested
  /// columns.
  ///
  /// Example:
  ///
  ///     PlanBuilder()
  ///       .unnest({Lit(Variant::array({1, 2, 3})).unnestAs("x")})
  ///       .build();
  ///
  /// @param unnestExprs A list of constant expressions to unnest.
  PlanBuilder& unnest(
      const std::vector<std::string>& unnestExprs,
      bool withOrdinality = false) {
    return unnest(parse(unnestExprs), withOrdinality);
  }

  PlanBuilder& unnest(
      const std::vector<ExprApi>& unnestExprs,
      bool withOrdinality = false) {
    return unnest(unnestExprs, withOrdinality, std::nullopt, {});
  }

  /// An alternative way to specify aliases for unnested columns. A preferred
  /// way is by using ExprApi::unnestAs.
  ///
  /// @param alias Optional alias for the relation produced by unnest.
  /// @param columnAliases An optional list of aliases for columns produced by
  /// unnest. The list can be empty or must have a non-empty alias for each
  /// column.
  PlanBuilder& unnest(
      const std::vector<ExprApi>& unnestExprs,
      bool withOrdinality,
      const std::optional<std::string>& alias,
      const std::vector<std::string>& unnestAliases);

  /// Joins the current plan (left) with the 'right' plan on the given
  /// condition. Pass a separate PlanBuilder for the right side.
  PlanBuilder& join(
      const PlanBuilder& right,
      const std::string& condition,
      JoinType joinType);

  /// @overload Takes an optional ExprApi condition. Pass std::nullopt for
  /// cross joins.
  PlanBuilder& join(
      const PlanBuilder& right,
      const std::optional<ExprApi>& condition,
      JoinType joinType);

  /// Adds a cross join (cartesian product) with the 'right' plan.
  PlanBuilder& crossJoin(const PlanBuilder& right) {
    return join(right, /* condition */ std::nullopt, JoinType::kInner);
  }

  /// Adds a UNION ALL set operation combining this plan with 'other'.
  PlanBuilder& unionAll(const PlanBuilder& other);

  /// Adds an INTERSECT set operation.
  PlanBuilder& intersect(const PlanBuilder& other);

  /// Adds an EXCEPT set operation.
  PlanBuilder& except(const PlanBuilder& other);

  /// Adds a set operation of the given type with a single other input.
  PlanBuilder& setOperation(SetOperation op, const PlanBuilder& other);

  /// @overload Combines multiple inputs.
  PlanBuilder& setOperation(
      SetOperation op,
      const std::vector<PlanBuilder>& inputs);

  /// Adds a Sort node ordering rows by the given keys.
  PlanBuilder& sort(const std::vector<std::string>& sortingKeys);

  /// @overload Takes typed SortKey objects for explicit sort direction.
  PlanBuilder& sort(const std::vector<SortKey>& sortingKeys);

  /// An alias for 'sort'.
  PlanBuilder& orderBy(const std::vector<std::string>& sortingKeys) {
    return sort(sortingKeys);
  }

  /// Adds a Limit node returning at most 'count' rows.
  PlanBuilder& limit(int32_t count) {
    return limit(0, count);
  }

  /// @overload Skips 'offset' rows, then returns at most 'count' rows.
  PlanBuilder& limit(int64_t offset, int64_t count);

  /// Adds an Offset node that skips the first 'offset' rows.
  PlanBuilder& offset(int64_t offset);

  /// Adds a TableWrite node that writes the current plan's output into the
  /// specified table.
  PlanBuilder& tableWrite(
      std::string connectorId,
      std::string tableName,
      WriteKind kind,
      std::vector<std::string> columnNames,
      const std::vector<ExprApi>& columnExprs,
      folly::F14FastMap<std::string, std::string> options = {});

  // A convenience method taking std::initializer_list<std::string> for
  // 'columnExprs'.
  PlanBuilder& tableWrite(
      std::string connectorId,
      std::string tableName,
      WriteKind kind,
      std::vector<std::string> columnNames,
      std::initializer_list<std::string> columnExprs,
      folly::F14FastMap<std::string, std::string> options = {}) {
    return tableWrite(
        std::move(connectorId),
        std::move(tableName),
        kind,
        std::move(columnNames),
        std::vector<std::string>{columnExprs},
        std::move(options));
  }

  // A convenience method taking std::vector<std::string> for 'columnExprs'.
  PlanBuilder& tableWrite(
      std::string connectorId,
      std::string tableName,
      WriteKind kind,
      std::vector<std::string> columnNames,
      const std::vector<std::string>& columnExprs,
      folly::F14FastMap<std::string, std::string> options = {}) {
    return tableWrite(
        std::move(connectorId),
        std::move(tableName),
        kind,
        std::move(columnNames),
        parse(columnExprs),
        std::move(options));
  }

  // A shortcut for calling tableWrite with the default connector ID.
  PlanBuilder& tableWrite(
      std::string tableName,
      WriteKind kind,
      std::vector<std::string> columnNames,
      const std::initializer_list<std::string>& columnExprs,
      folly::F14FastMap<std::string, std::string> options = {}) {
    VELOX_USER_CHECK(defaultConnectorId_.has_value());
    return tableWrite(
        defaultConnectorId_.value(),
        std::move(tableName),
        kind,
        std::move(columnNames),
        std::vector<std::string>{columnExprs},
        std::move(options));
  }

  /// A shortcut for calling tableWrite with the default connector ID and
  /// 'columnExprs' that are simple references to the input columns. The number
  /// of 'columnNames' must match the number of input columns.
  PlanBuilder& tableWrite(
      std::string tableName,
      WriteKind kind,
      std::vector<std::string> columnNames,
      folly::F14FastMap<std::string, std::string> options = {});

  /// Adds a Sample node that passes each row with the given probability.
  PlanBuilder& sample(double percentage, SampleNode::SampleMethod sampleMethod);

  /// @overload Takes a dynamic percentage expression.
  PlanBuilder& sample(
      const ExprApi& percentage,
      SampleNode::SampleMethod sampleMethod);

  /// Assigns a relation alias, enabling qualified column references like
  /// "alias.column" in subsequent operations.
  PlanBuilder& as(const std::string& alias);

  /// Captures the current name-resolution scope into 'scope' so it can be
  /// passed to an inner PlanBuilder for correlated subqueries.
  ///
  /// Example:
  ///
  ///   PlanBuilder::Scope scope;
  ///   auto plan = PlanBuilder(ctx)
  ///     .tableScan("orders")
  ///     .as("o")
  ///     .captureScope(scope)
  ///     .filter(Col("totalprice") >
  ///       Subquery(PlanBuilder(ctx, false, scope)
  ///         .tableScan("thresholds")
  ///         .as("t")
  ///         .filter("o.custkey = t.custkey")
  ///         .aggregate({}, {"max(threshold)"})
  ///         .build()))
  ///     .build();
  PlanBuilder& captureScope(Scope& scope) {
    scope = [this](const auto& alias, const auto& name) {
      return resolveInputName(alias, name);
    };

    return *this;
  }

  /// Returns the number of output columns.
  size_t numOutput() const;

  /// Returns the names of the output columns. Returns std::nullopt for
  /// anonymous columns.
  std::vector<std::optional<std::string>> outputNames() const;

  /// Returns the types of the output columns. 1:1 with outputNames().
  std::vector<velox::TypePtr> outputTypes() const;

  /// Returns the names of the output columns. If some colums are anonymous,
  /// assigns them unique names before returning.
  /// @param includeHiddenColumns Boolean indicating whether to include hidden
  /// columns.
  /// @param alias Optional alias to filter output columns. If specified,
  /// returns a subset of columns accessible with the specified alias.
  std::vector<std::string> findOrAssignOutputNames(
      bool includeHiddenColumns = false,
      const std::optional<std::string>& alias = std::nullopt) const;

  /// Returns the name of the output column at the given index. If the column
  /// is anonymous, assigns unique name before returning.
  std::string findOrAssignOutputNameAt(size_t index) const;

  /// @param useIds Boolean indicating whether to use user-specified names or
  /// use auto-generated IDs for the output column names.
  LogicalPlanNodePtr build(bool useIds = false);

 private:
  std::string nextId() {
    return planNodeIdGenerator_->next();
  }

  std::string newName(const std::string& hint);

  ExprPtr resolveInputName(
      const std::optional<std::string>& alias,
      const std::string& name) const;

  ExprPtr resolveScalarTypes(const velox::core::ExprPtr& expr) const;

  AggregateExprPtr resolveAggregateTypes(
      const velox::core::ExprPtr& expr,
      const ExprPtr& filter,
      const std::vector<SortingField>& ordering,
      bool distinct) const;

  std::vector<ExprApi> parse(const std::vector<std::string>& exprs);

  void resolveProjections(
      const std::vector<ExprApi>& projections,
      std::vector<std::string>& outputNames,
      std::vector<ExprPtr>& exprs,
      NameMappings& mappings);

  void resolveAggregates(
      const std::vector<ExprApi>& aggregates,
      const std::vector<AggregateOptions>& options,
      std::vector<std::string>& outputNames,
      std::vector<AggregateExprPtr>& exprs,
      NameMappings& mappings);

  // Connector ID used when table names omit the catalog prefix.
  const std::optional<std::string> defaultConnectorId_;

  // Generates unique plan-node IDs (shared across builders via Context).
  const std::shared_ptr<velox::core::PlanNodeIdGenerator> planNodeIdGenerator_;

  // Allocates unique internal column names (shared across builders via
  // Context).
  const std::shared_ptr<NameAllocator> nameAllocator_;

  // Resolves column references from the enclosing query for correlated
  // subqueries. Null for top-level queries.
  const Scope outerScope_;

  // Parses SQL expression strings into Velox expression trees.
  const std::shared_ptr<velox::parse::SqlExpressionsParser> sqlParser_;

  // When true, inserts implicit CAST nodes to coerce mismatched types.
  const bool enableCoercions_;

  // Root of the plan tree built so far. Null before the first leaf node
  // (values or tableScan) is added.
  LogicalPlanNodePtr node_;

  // Maps user-visible column names to auto-generated internal IDs.
  std::shared_ptr<NameMappings> outputMapping_;

  // Resolves and type-checks scalar and aggregate expressions.
  ExprResolver resolver_;
};

} // namespace facebook::axiom::logical_plan
