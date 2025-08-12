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
#include "velox/core/Expressions.h"
#include "velox/core/QueryCtx.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/PlanNodeIdGenerator.h"

namespace facebook::velox::logical_plan {

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
      std::shared_ptr<core::QueryCtx> queryCtx,
      FunctionRewriteHook hook = nullptr)
      : queryCtx_(std::move(queryCtx)),
        hook_(hook),
        pool_(
            queryCtx_ ? queryCtx_->pool()->addLeafChild(
                            fmt::format("literals{}", ++literalsCounter_))
                      : nullptr) {}

  ExprPtr resolveScalarTypes(
      const core::ExprPtr& expr,
      const InputNameResolver& inputNameResolver) const;

  AggregateExprPtr resolveAggregateTypes(
      const core::ExprPtr& expr,
      const InputNameResolver& inputNameResolver) const;

 private:
  ExprPtr resolveLambdaExpr(
      const core::LambdaExpr* lambdaExpr,
      const std::vector<TypePtr>& lambdaInputTypes,
      const InputNameResolver& inputNameResolver) const;

  ExprPtr tryResolveCallWithLambdas(
      const std::shared_ptr<const core::CallExpr>& callExpr,
      const InputNameResolver& inputNameResolver) const;

  ExprPtr tryFoldCall(
      const TypePtr& type,
      const std::string& name,
      const std::vector<ExprPtr>& inputs) const;

  ExprPtr tryFoldCast(const TypePtr& type, const ExprPtr& input) const;

  core::TypedExprPtr makeConstantTypedExpr(const ExprPtr& expr) const;

  ExprPtr makeConstant(const VectorPtr& vector) const;

  ExprPtr tryFoldCall(const TypePtr& type, ExprPtr input) const;

  std::shared_ptr<core::QueryCtx> queryCtx_;
  FunctionRewriteHook hook_;
  std::shared_ptr<memory::MemoryPool> pool_;
  static inline int32_t literalsCounter_{0};
};

class PlanBuilder {
 public:
  struct Context {
    std::optional<std::string> defaultConnectorId;
    std::shared_ptr<core::PlanNodeIdGenerator> planNodeIdGenerator;
    std::shared_ptr<NameAllocator> nameAllocator;
    std::shared_ptr<core::QueryCtx> queryCtx;
    ExprResolver::FunctionRewriteHook hook;

    Context(
        const std::optional<std::string>& defaultConnectorId = std::nullopt,
        std::shared_ptr<core::QueryCtx> queryCtx = nullptr,
        ExprResolver::FunctionRewriteHook hook = nullptr)
        : defaultConnectorId{defaultConnectorId},
          planNodeIdGenerator{std::make_shared<core::PlanNodeIdGenerator>()},
          nameAllocator{std::make_shared<NameAllocator>()},
          queryCtx(std::move(queryCtx)),
          hook(std::move(hook)) {}
  };

  using Scope = std::function<ExprPtr(
      const std::optional<std::string>& alias,
      const std::string& name)>;

  PlanBuilder(Scope outerScope = nullptr)
      : planNodeIdGenerator_(std::make_shared<core::PlanNodeIdGenerator>()),
        nameAllocator_(std::make_shared<NameAllocator>()),
        outerScope_{std::move(outerScope)},
        resolver_(nullptr, nullptr) {}

  explicit PlanBuilder(const Context& context, Scope outerScope = nullptr)
      : defaultConnectorId_(context.defaultConnectorId),
        planNodeIdGenerator_{context.planNodeIdGenerator},
        nameAllocator_{context.nameAllocator},
        outerScope_{std::move(outerScope)},
        resolver_(context.queryCtx, context.hook) {
    VELOX_CHECK_NOT_NULL(planNodeIdGenerator_);
    VELOX_CHECK_NOT_NULL(nameAllocator_);
  }

  PlanBuilder& values(const RowTypePtr& rowType, std::vector<Variant> rows);

  PlanBuilder& values(const std::vector<RowVectorPtr>& values);

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

  PlanBuilder& project(const std::vector<std::string>& projections);

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

  PlanBuilder& join(
      const PlanBuilder& right,
      const std::string& condition,
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

  /// An alias for 'sort'.
  PlanBuilder& orderBy(const std::vector<std::string>& sortingKeys) {
    return sort(sortingKeys);
  }

  PlanBuilder& limit(int32_t count) {
    return limit(0, count);
  }

  PlanBuilder& limit(int64_t offset, int64_t count);

  PlanBuilder& offset(int64_t offset);

  PlanBuilder& as(const std::string& alias);

  PlanBuilder& captureScope(Scope& scope) {
    scope = [this](const auto& alias, const auto& name) {
      return resolveInputName(alias, name);
    };

    return *this;
  }

  LogicalPlanNodePtr build();

 private:
  std::string nextId() {
    return planNodeIdGenerator_->next();
  }

  std::string newName(const std::string& hint);

  ExprPtr resolveInputName(
      const std::optional<std::string>& alias,
      const std::string& name) const;

  ExprPtr resolveScalarTypes(const core::ExprPtr& expr) const;

  AggregateExprPtr resolveAggregateTypes(const core::ExprPtr& expr) const;

  std::vector<ExprApi> parse(const std::vector<std::string>& exprs);

  void resolveProjections(
      const std::vector<ExprApi>& projections,
      std::vector<std::string>& outputNames,
      std::vector<ExprPtr>& exprs,
      NameMappings& mappings);

  const std::optional<std::string> defaultConnectorId_;
  const std::shared_ptr<core::PlanNodeIdGenerator> planNodeIdGenerator_;
  const std::shared_ptr<NameAllocator> nameAllocator_;
  const Scope outerScope_;
  const parse::ParseOptions parseOptions_;

  LogicalPlanNodePtr node_;

  // Mapping from user-provided to auto-generated output column names.
  std::shared_ptr<NameMappings> outputMapping_;

  ExprResolver resolver_;
};

} // namespace facebook::velox::logical_plan
