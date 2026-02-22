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

#include "axiom/logical_plan/Expr.h"
#include "axiom/logical_plan/ExprApi.h"
#include "velox/core/ITypedExpr.h"
#include "velox/core/QueryCtx.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/PlanNodeIdGenerator.h"

namespace facebook::axiom::logical_plan {

/// Resolves untyped expressions (velox::core::IExpr) into typed expressions
/// (Expr). Performs type inference, signature matching, implicit coercions, and
/// constant folding.
class ExprResolver {
 public:
  /// Resolves an unqualified or qualified column name to a typed input
  /// reference. Called during expression resolution to look up column types
  /// from the current plan node's input schema.
  ///
  /// @param alias Optional table alias (e.g. "t" in "t.col").
  /// @param fieldName Column name.
  using InputNameResolver = std::function<ExprPtr(
      const std::optional<std::string>& alias,
      const std::string& fieldName)>;

  /// Maps from an untyped call and resolved arguments to a resolved function
  /// call. Use only for anomalous functions where the type depends on constant
  /// arguments, e.g. Koski's make_row_from_map().
  using FunctionRewriteHook = std::function<
      ExprPtr(const std::string& name, const std::vector<ExprPtr>& args)>;

  /// @param queryCtx Query context for constant folding.
  /// @param enableCoercions Whether to apply implicit type coercions when
  /// resolving function signatures.
  /// @param hook Optional rewrite hook for special functions.
  /// @param pool Memory pool for constant folding evaluation.
  /// @param planNodeIdGenerator Plan node ID generator for subquery
  /// expressions.
  ExprResolver(
      std::shared_ptr<velox::core::QueryCtx> queryCtx,
      bool enableCoercions,
      FunctionRewriteHook hook = nullptr,
      std::shared_ptr<velox::memory::MemoryPool> pool = nullptr,
      std::shared_ptr<velox::core::PlanNodeIdGenerator> planNodeIdGenerator =
          nullptr)
      : queryCtx_(std::move(queryCtx)),
        enableCoercions_{enableCoercions},
        hook_(std::move(hook)),
        pool_(std::move(pool)),
        planNodeIdGenerator_{std::move(planNodeIdGenerator)} {}

  /// Resolves an untyped scalar expression into a typed expression. Handles
  /// column references, function calls, casts, literals, lambdas, and
  /// subqueries. Attempts constant folding when all inputs are constants.
  ExprPtr resolveScalarTypes(
      const velox::core::ExprPtr& expr,
      const InputNameResolver& inputNameResolver) const;

  /// Resolves an aggregate function call. Resolves argument types, looks up
  /// the aggregate function signature, resolves the filter and ordering
  /// expressions, and produces an AggregateExpr.
  AggregateExprPtr resolveAggregateTypes(
      const velox::core::ExprPtr& expr,
      const InputNameResolver& inputNameResolver,
      const velox::core::ExprPtr& filter,
      const std::vector<SortKey>& ordering,
      bool distinct) const;

  /// Resolves a window function call. Resolves argument types, partition keys,
  /// ordering, and frame bounds, then looks up the window function signature
  /// and produces a WindowExpr.
  WindowExprPtr resolveWindowTypes(
      const velox::core::ExprPtr& expr,
      const WindowSpec& windowSpec,
      const InputNameResolver& inputNameResolver) const;

 private:
  ExprPtr resolveLambdaExpr(
      const velox::core::LambdaExpr& lambdaExpr,
      const std::vector<velox::TypePtr>& lambdaInputTypes,
      const InputNameResolver& inputNameResolver) const;

  ExprPtr tryResolveCallWithLambdas(
      const std::shared_ptr<const velox::core::CallExpr>& callExpr,
      const InputNameResolver& inputNameResolver) const;

  // Resolves lambda arguments using already resolved non-lambda arguments.
  // Populates 'resolvedInputs' entries that correspond to lambda arguments.
  //
  // @param inputs Function arguments.
  // @param signature Function signature.
  // @param resolvedInputs Partially resolved function arguments. 1:1 with
  // 'inputs'. Non-lambda arguments are resolved and therefore not null. Lambda
  // arguments may be null.
  // @return True if all arguments were resolved successfully and
  // 'resolvedInputs' was updated. False otherwise.
  bool resolveLambdaArguments(
      const std::vector<velox::core::ExprPtr>& inputs,
      const velox::exec::FunctionSignature& signature,
      std::vector<ExprPtr>& resolvedInputs,
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
  const bool enableCoercions_;
  FunctionRewriteHook hook_;
  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::shared_ptr<velox::core::PlanNodeIdGenerator> planNodeIdGenerator_;
};
} // namespace facebook::axiom::logical_plan
