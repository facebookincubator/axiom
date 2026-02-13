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

// Make sure to specify Context.queryCtx to enable constand folding.
class PlanBuilder {
 public:
  struct Context {
    std::optional<std::string> defaultConnectorId;
    std::shared_ptr<velox::parse::SqlExpressionsParser> sqlParser;
    std::shared_ptr<velox::core::PlanNodeIdGenerator> planNodeIdGenerator;
    std::shared_ptr<NameAllocator> nameAllocator;
    std::shared_ptr<velox::core::QueryCtx> queryCtx;
    ExprResolver::FunctionRewriteHook hook;
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

  PlanBuilder& values(
      const velox::RowTypePtr& rowType,
      std::vector<velox::Variant> rows);

  PlanBuilder& values(const std::vector<velox::RowVectorPtr>& values);

  PlanBuilder& values(
      const std::vector<std::string>& names,
      const std::vector<std::vector<ExprApi>>& values);

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

  PlanBuilder& tableScan(
      const std::string& tableName,
      bool includeHiddenColumns = false);

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

  PlanBuilder& filter(const std::string& predicate);

  PlanBuilder& filter(const ExprApi& predicate);

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

  PlanBuilder& join(
      const PlanBuilder& right,
      const std::string& condition,
      JoinType joinType);

  PlanBuilder& join(
      const PlanBuilder& right,
      const std::optional<ExprApi>& condition,
      JoinType joinType);

  PlanBuilder& crossJoin(const PlanBuilder& right) {
    return join(right, /* condition */ std::nullopt, JoinType::kInner);
  }

  PlanBuilder& unionAll(const PlanBuilder& other);

  PlanBuilder& intersect(const PlanBuilder& other);

  PlanBuilder& except(const PlanBuilder& other);

  PlanBuilder& setOperation(SetOperation op, const PlanBuilder& other);

  PlanBuilder& setOperation(
      SetOperation op,
      const std::vector<PlanBuilder>& inputs);

  PlanBuilder& sort(const std::vector<std::string>& sortingKeys);

  PlanBuilder& sort(const std::vector<SortKey>& sortingKeys);

  /// An alias for 'sort'.
  PlanBuilder& orderBy(const std::vector<std::string>& sortingKeys) {
    return sort(sortingKeys);
  }

  PlanBuilder& limit(int32_t count) {
    return limit(0, count);
  }

  PlanBuilder& limit(int64_t offset, int64_t count);

  PlanBuilder& offset(int64_t offset);

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

  PlanBuilder& sample(double percentage, SampleNode::SampleMethod sampleMethod);

  PlanBuilder& sample(
      const ExprApi& percentage,
      SampleNode::SampleMethod sampleMethod);

  PlanBuilder& as(const std::string& alias);

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

  const std::optional<std::string> defaultConnectorId_;
  const std::shared_ptr<velox::core::PlanNodeIdGenerator> planNodeIdGenerator_;
  const std::shared_ptr<NameAllocator> nameAllocator_;
  const Scope outerScope_;
  const std::shared_ptr<velox::parse::SqlExpressionsParser> sqlParser_;
  const bool enableCoercions_;

  LogicalPlanNodePtr node_;

  // Mapping from user-provided to auto-generated output column names.
  std::shared_ptr<NameMappings> outputMapping_;

  ExprResolver resolver_;
};

} // namespace facebook::axiom::logical_plan
