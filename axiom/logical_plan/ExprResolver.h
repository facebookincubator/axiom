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

#include <optional>

#include <folly/container/F14Set.h>

#include "axiom/logical_plan/Expr.h"
#include "axiom/logical_plan/ExprApi.h"
#include "velox/core/ITypedExpr.h"
#include "velox/core/QueryCtx.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/PlanNodeIdGenerator.h"
#include "velox/type/TypeCoercer.h"

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

  /// Wraps a resolved SQL-function body so a null in any argument yields null
  /// without evaluating the body. Receives the resolved body (already coerced
  /// to the result type) and the coerced argument expressions; returns the
  /// wrapped body. The frontend supplies it so the dialect-specific null test
  /// (e.g. is_null) stays out of this layer.
  using NullWrapper = std::function<
      ExprPtr(const ExprPtr& body, const std::vector<ExprPtr>& args)>;

  /// A SQL-invoked function selected for a call, resolved into the untyped body
  /// to inline plus the metadata needed to embed it. Produced by the frontend
  /// that owns a SQL parser for the defining connector's dialect; consumed by
  /// resolveScalarTypes, which coerces and binds the call's arguments and
  /// resolves the body.
  struct ResolvedSqlFunction {
    /// Formal argument names, positionally aligned with 'argumentTypes'.
    std::vector<std::string> argumentNames;

    /// Formal argument types. The call's arguments are coerced to these before
    /// binding, so the body sees the declared types (e.g. int -> bigint).
    std::vector<velox::TypePtr> argumentTypes;

    /// Untyped scalar body expression over 'argumentNames', parsed from the
    /// connector's SQL by the frontend. Must not contain subqueries.
    velox::core::ExprPtr body;

    /// Declared result type; the resolved body is implicitly coerced to it.
    velox::TypePtr returnType;

    /// Wraps the resolved body for RETURNS NULL ON NULL INPUT, or null when the
    /// function is called on null input (no wrapping). Applied after argument
    /// binding and return-type coercion.
    NullWrapper nullWrapper;
  };

  /// Resolves a call to a SQL-invoked (inlined) function by name and argument
  /// types, or returns nullopt if 'name' is not a SQL-invoked function.
  /// Argument types select among overloads. If 'name' is a SQL-invoked function
  /// but no overload accepts 'argTypes', the resolver fails with a user error
  /// rather than returning nullopt (which would surface as a misleading
  /// function-not-found error). Installed by the frontend, which resolves the
  /// function against connectors and parses its body in the appropriate
  /// dialect.
  using SqlFunctionResolver = std::function<std::optional<ResolvedSqlFunction>(
      const std::string& name,
      const std::vector<velox::TypePtr>& argTypes)>;

  /// @param queryCtx Query context for constant folding.
  /// @param coercer Coercion rule set used for implicit type conversions,
  /// or nullptr to disable implicit coercions.
  /// @param hook Optional rewrite hook for special functions.
  /// @param pool Memory pool for constant folding evaluation.
  /// @param planNodeIdGenerator Plan node ID generator for subquery
  /// expressions.
  /// @param sqlFunctionResolver Optional resolver for SQL-invoked functions
  /// inlined from connectors. Requires a non-null 'coercer', since inlining
  /// coerces the arguments and body to their declared types.
  ExprResolver(
      std::shared_ptr<velox::core::QueryCtx> queryCtx,
      const velox::TypeCoercer* coercer,
      FunctionRewriteHook hook = nullptr,
      std::shared_ptr<velox::memory::MemoryPool> pool = nullptr,
      std::shared_ptr<velox::core::PlanNodeIdGenerator> planNodeIdGenerator =
          nullptr,
      SqlFunctionResolver sqlFunctionResolver = nullptr)
      : queryCtx_(std::move(queryCtx)),
        coercer_{coercer},
        hook_(std::move(hook)),
        pool_(std::move(pool)),
        planNodeIdGenerator_{std::move(planNodeIdGenerator)},
        sqlFunctionResolver_{std::move(sqlFunctionResolver)} {}

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

  /// Resolves an aggregate call, extracting DISTINCT / FILTER / ORDER BY from
  /// 'expr' itself (an AggregateCallExpr or SpecialFormAggCallExpr). Used for
  /// nested aggregates such as a special aggregate's fallback.
  AggregateExprPtr resolveAggregateTypes(
      const velox::core::ExprPtr& expr,
      const InputNameResolver& inputNameResolver) const;

  /// Resolves a window function call. Resolves argument types, partition keys,
  /// ordering, and frame bounds, then looks up the window function signature
  /// and produces a WindowExpr.
  WindowExprPtr resolveWindowTypes(
      const velox::core::WindowCallExpr& windowCall,
      const InputNameResolver& inputNameResolver) const;

 private:
  using ResolveFunc = velox::TypePtr (*)(
      const std::string&,
      const std::vector<velox::TypePtr>&);

  using ResolveWithCoercionsFunc = velox::TypePtr (*)(
      const std::string&,
      const std::vector<velox::TypePtr>&,
      std::vector<velox::TypePtr>&,
      const velox::TypeCoercer&);

  struct ResolvedCall {
    std::string name;
    std::vector<ExprPtr> inputs;
    velox::TypePtr type;
  };

  // Resolves a list of untyped scalar arguments to typed expressions.
  std::vector<ExprPtr> resolveScalarInputs(
      const std::vector<velox::core::ExprPtr>& inputs,
      const InputNameResolver& inputNameResolver) const;

  // Resolves a SpecialFormAggCallExpr into a SpecialFormAggExpr. These
  // aggregates reject the DISTINCT and FILTER modifiers ('filter' / 'distinct'
  // here) and ignore ORDER BY (order-insensitive).
  AggregateExprPtr resolveSpecialFormAgg(
      const velox::core::SpecialFormAggCallExpr& expr,
      const InputNameResolver& inputNameResolver,
      const velox::core::ExprPtr& filter,
      bool distinct) const;

  // Resolves function call arguments (including lambdas) and determines the
  // return type using the provided resolve functions. Used by both
  // resolveAggregateTypes and resolveWindowTypes.
  ResolvedCall resolveCallTypes(
      const velox::core::ExprPtr& expr,
      const InputNameResolver& inputNameResolver,
      const char* label,
      ResolveFunc resolveFunc,
      ResolveWithCoercionsFunc resolveWithCoercionsFunc) const;

  ExprPtr resolveLambdaExpr(
      const velox::core::LambdaExpr& lambdaExpr,
      const std::vector<velox::TypePtr>& lambdaInputTypes,
      const InputNameResolver& inputNameResolver) const;

  ExprPtr tryResolveCallWithLambdas(
      const std::shared_ptr<const velox::core::CallExpr>& callExpr,
      const InputNameResolver& inputNameResolver) const;

  // Resolves 'name'(inputs) as a SQL-invoked function inlined from a connector,
  // or returns nullptr if 'name' is not such a function. Binds the argument
  // names to 'inputs', resolves the body, and coerces it to the declared return
  // type.
  ExprPtr resolveSqlFunction(
      const std::string& name,
      const std::vector<ExprPtr>& inputs) const;

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
  // Nullable: nullptr means implicit coercions are disabled.
  const velox::TypeCoercer* coercer_;
  FunctionRewriteHook hook_;
  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::shared_ptr<velox::core::PlanNodeIdGenerator> planNodeIdGenerator_;
  SqlFunctionResolver sqlFunctionResolver_;

  // Names of SQL-invoked functions currently being inlined on the active
  // resolveScalarTypes recursion, used to reject recursive definitions. This
  // guard only catches recursion routed back through this same ExprResolver: it
  // relies on sqlFunctionResolver_ returning an unresolved body that this
  // instance resolves (nested SQL functions must not be resolved by a different
  // ExprResolver). Resolution through a single ExprResolver is single-threaded.
  mutable folly::F14FastSet<std::string> inliningFunctions_;
};
} // namespace facebook::axiom::logical_plan
