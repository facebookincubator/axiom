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
#include "axiom/logical_plan/LogicalPlanNode.h"
#include "axiom/logical_plan/NameAllocator.h"
#include "velox/core/ITypedExpr.h"
#include "velox/core/QueryCtx.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/PlanNodeIdGenerator.h"

namespace facebook::axiom::logical_plan {

class NameMappings;

/// Class encapsulating functions for type inference and constant folding. Use
/// with SQL and PlanBuilder.
class ExprResolver {
 public:
  using InputNameResolver = std::function<ExprPtr(
      const std::optional<std::string>& alias,
      const std::string& fieldName)>;

  /// Maps from an untyped call and  resolved arguments to a resolved function
  /// call. Use only for anamolous functions where the type depends on constant
  /// arguments, e.g. Koski make_row_from_map().
  using FunctionRewriteHook = std::function<
      ExprPtr(const std::string& name, const std::vector<ExprPtr>& args)>;

  ExprResolver(
      std::shared_ptr<velox::core::QueryCtx> queryCtx,
      bool enableCoersions,
      FunctionRewriteHook hook = nullptr,
      std::shared_ptr<velox::memory::MemoryPool> pool = nullptr)
      : queryCtx_(std::move(queryCtx)),
        enableCoersions_{enableCoersions},
        hook_(std::move(hook)),
        pool_(std::move(pool)) {}

  ExprPtr resolveScalarTypes(
      const velox::core::ExprPtr& expr,
      const InputNameResolver& inputNameResolver) const;

  AggregateExprPtr resolveAggregateTypes(
      const velox::core::ExprPtr& expr,
      const InputNameResolver& inputNameResolver,
      const ExprPtr& filter,
      const std::vector<SortingField>& ordering,
      bool distinct) const;

 private:
  ExprPtr resolveLambdaExpr(
      const velox::core::LambdaExpr* lambdaExpr,
      const std::vector<velox::TypePtr>& lambdaInputTypes,
      const InputNameResolver& inputNameResolver) const;

  ExprPtr tryResolveCallWithLambdas(
      const std::shared_ptr<const velox::core::CallExpr>& callExpr,
      const InputNameResolver& inputNameResolver) const;

  ExprPtr tryFoldCall(
      const velox::TypePtr& type,
      const std::string& name,
      const std::vector<ExprPtr>& inputs) const;

  ExprPtr tryFoldCast(const velox::TypePtr& type, const ExprPtr& input) const;

  velox::core::TypedExprPtr makeConstantTypedExpr(const ExprPtr& expr) const;

  ExprPtr makeConstant(const velox::VectorPtr& vector) const;

  ExprPtr tryFoldCall(const velox::TypePtr& type, ExprPtr input) const;

  ExprPtr tryFoldSpecialForm(
      const std::string& name,
      const std::vector<ExprPtr>& inputs) const;

  std::shared_ptr<velox::core::QueryCtx> queryCtx_;
  const bool enableCoersions_;
  FunctionRewriteHook hook_;
  std::shared_ptr<velox::memory::MemoryPool> pool_;
};

// Make sure to specify Context.queryCtx to enable constand folding.
class PlanBuilder {
 public:
  struct Context {
    std::optional<std::string> defaultConnectorId;
    std::shared_ptr<velox::core::PlanNodeIdGenerator> planNodeIdGenerator;
    std::shared_ptr<NameAllocator> nameAllocator;
    std::shared_ptr<velox::core::QueryCtx> queryCtx;
    ExprResolver::FunctionRewriteHook hook;
    std::shared_ptr<velox::memory::MemoryPool> pool;

    explicit Context(
        const std::optional<std::string>& defaultConnectorId = std::nullopt,
        std::shared_ptr<velox::core::QueryCtx> queryCtxPtr = nullptr,
        ExprResolver::FunctionRewriteHook hook = nullptr)
        : defaultConnectorId{defaultConnectorId},
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

  explicit PlanBuilder(bool enableCoersions = false, Scope outerScope = nullptr)
      : PlanBuilder{Context{}, enableCoersions, std::move(outerScope)} {}

  explicit PlanBuilder(
      const Context& context,
      bool enableCoersions = false,
      Scope outerScope = nullptr)
      : defaultConnectorId_{context.defaultConnectorId},
        planNodeIdGenerator_{context.planNodeIdGenerator},
        nameAllocator_{context.nameAllocator},
        outerScope_{std::move(outerScope)},
        parseOptions_{.parseInListAsArray = false},
        resolver_{
            context.queryCtx,
            enableCoersions,
            context.hook,
            context.pool} {
    VELOX_CHECK_NOT_NULL(planNodeIdGenerator_);
    VELOX_CHECK_NOT_NULL(nameAllocator_);
  }

  PlanBuilder& values(
      const velox::RowTypePtr& rowType,
      std::vector<velox::Variant> rows);

  PlanBuilder& values(const std::vector<velox::RowVectorPtr>& values);

  /// Equivalent to SELECT col1, col2,.. FROM <tableName>.
  PlanBuilder& tableScan(
      const std::string& connectorId,
      const std::string& tableName,
      const std::vector<std::string>& columnNames);

  PlanBuilder& tableScan(
      const std::string& tableName,
      const std::vector<std::string>& columnNames);

  /// Equivalent to SELECT * FROM <tableName>.
  PlanBuilder& tableScan(
      const std::string& connectorId,
      const std::string& tableName);

  PlanBuilder& tableScan(const std::string& tableName);

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
        ExprPtr filters,
        std::vector<SortingField> orderings,
        bool distinct)
        : filters(std::move(filters)),
          orderings(std::move(orderings)),
          distinct(distinct) {}

    AggregateOptions() = default;

    ExprPtr filters;
    std::vector<SortingField> orderings;
    bool distinct{false};
  };
  PlanBuilder& aggregate(
      const std::vector<ExprApi>& groupingKeys,
      const std::vector<ExprApi>& aggregates,
      const std::vector<AggregateOptions>& options);

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
    return join(right, /* condition */ "", JoinType::kInner);
  }

  PlanBuilder& unionAll(const PlanBuilder& other);

  PlanBuilder& intersect(const PlanBuilder& other);

  PlanBuilder& except(const PlanBuilder& other);

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

  PlanBuilder& tableWrite(
      std::string connectorId,
      std::string tableName,
      WriteKind kind,
      std::vector<std::string> columnNames,
      const std::vector<ExprApi>& columnExprs,
      folly::F14FastMap<std::string, std::string> options = {});

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
  std::vector<std::string> findOrAssignOutputNames() const;

  /// Returns the name of the output column at the given index. If the column is
  /// anonymous, assigns unique name before returning.
  std::string findOrAssignOutputNameAt(size_t index) const;

  LogicalPlanNodePtr build();

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

  const std::optional<std::string> defaultConnectorId_;
  const std::shared_ptr<velox::core::PlanNodeIdGenerator> planNodeIdGenerator_;
  const std::shared_ptr<NameAllocator> nameAllocator_;
  const Scope outerScope_;
  const velox::parse::ParseOptions parseOptions_;

  LogicalPlanNodePtr node_;

  // Mapping from user-provided to auto-generated output column names.
  std::shared_ptr<NameMappings> outputMapping_;

  ExprResolver resolver_;
};

} // namespace facebook::axiom::logical_plan
