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

#include "axiom/optimizer/v2/ExprEmitter.h"

#include <folly/container/F14Map.h>
#include "axiom/optimizer/FunctionRegistry.h"
#include "velox/expression/ExprConstants.h"
#include "velox/vector/BaseVector.h"

namespace facebook::axiom::optimizer::v2 {

namespace {

const folly::F14FastMap<ColumnNaming, std::string_view>& columnNamingNames() {
  static const folly::F14FastMap<ColumnNaming, std::string_view> kNames = {
      {ColumnNaming::kOutputName, "OutputName"},
      {ColumnNaming::kSchemaName, "SchemaName"},
  };
  return kNames;
}

velox::core::TypedExprPtr columnToTypedExpr(
    ColumnCP column,
    ColumnNaming naming) {
  const std::string name = naming == ColumnNaming::kSchemaName
      ? std::string{column->name()}
      : column->outputName();
  return std::make_shared<velox::core::FieldAccessTypedExpr>(
      toTypePtr(column->value().type), name);
}

velox::core::TypedExprPtr literalToTypedExpr(LiteralCP literal) {
  return std::make_shared<velox::core::ConstantTypedExpr>(
      toTypePtr(literal->value().type), literal->literal());
}

// Emits a DEREFERENCE call (resolved at translate time to a numeric field
// index) as Velox's `DereferenceTypedExpr`.
velox::core::TypedExprPtr dereferenceToTypedExpr(
    const Call* call,
    const std::vector<velox::core::TypedExprPtr>& args) {
  VELOX_CHECK_EQ(call->args().size(), 2);
  const auto* index = call->args()[1]->as<Literal>();
  VELOX_CHECK_NOT_NULL(index);
  VELOX_CHECK_EQ(index->literal().kind(), velox::TypeKind::INTEGER);
  return std::make_shared<velox::core::DereferenceTypedExpr>(
      toTypePtr(call->value().type),
      args[0],
      static_cast<uint32_t>(
          index->literal().value<velox::TypeKind::INTEGER>()));
}

// Folds an all-literal IN list into `in(col, ARRAY[...])` with one array
// constant, so Velox binds the set/bitmask-based InPredicate instead of a
// per-row variadic compare. Returns nullptr when any element is non-literal,
// leaving the variadic call. Translate folds every single-element IN to
// equality, so an IN reaching here has >= 2 elements. `column` is the
// already-lowered first argument.
velox::core::TypedExprPtr tryLowerConstantIn(
    const Call* call,
    const velox::core::TypedExprPtr& column,
    velox::memory::MemoryPool* pool) {
  const auto& args = call->args();
  VELOX_CHECK_GE(args.size(), 3);
  for (size_t i = 1; i < args.size(); ++i) {
    if (!args[i]->is(PlanType::kLiteralExpr)) {
      return nullptr;
    }
  }

  const auto elementType = toTypePtr(args[0]->value().type);
  std::vector<velox::Variant> elements;
  elements.reserve(args.size() - 1);
  for (size_t i = 1; i < args.size(); ++i) {
    elements.push_back(args[i]->as<Literal>()->literal());
  }

  // Materialize the array through a constant vector, which supports complex
  // element types.
  return std::make_shared<velox::core::CallTypedExpr>(
      velox::BOOLEAN(),
      specialForm(logical_plan::SpecialForm::kIn),
      column,
      std::make_shared<velox::core::ConstantTypedExpr>(
          velox::BaseVector::createConstant(
              velox::ARRAY(elementType),
              velox::Variant::array(elements),
              1,
              pool)));
}

velox::core::TypedExprPtr lambdaToTypedExpr(
    const Lambda* lambda,
    const velox::core::TypedExprPtr& body) {
  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;
  names.reserve(lambda->args().size());
  types.reserve(lambda->args().size());
  for (ColumnCP arg : lambda->args()) {
    names.push_back(arg->outputName());
    types.push_back(toTypePtr(arg->value().type));
  }
  return std::make_shared<velox::core::LambdaTypedExpr>(
      velox::ROW(std::move(names), std::move(types)), body);
}

} // namespace

AXIOM_DEFINE_ENUM_NAME(ColumnNaming, columnNamingNames);

velox::core::TypedExprPtr ExprEmitter::callToTypedExpr(
    const Call* call,
    std::vector<velox::core::TypedExprPtr> args) {
  auto resultType = toTypePtr(call->value().type);
  if (auto form = SpecialFormCallNames::tryFromCallName(call->name())) {
    if (form == logical_plan::SpecialForm::kCast) {
      return std::make_shared<velox::core::CastTypedExpr>(
          resultType, std::move(args), /*nullOnFailure=*/false);
    }

    if (form == logical_plan::SpecialForm::kTryCast) {
      return std::make_shared<velox::core::CastTypedExpr>(
          resultType, std::move(args), /*nullOnFailure=*/true);
    }

    if (form == logical_plan::SpecialForm::kDereference) {
      return dereferenceToTypedExpr(call, args);
    }

    if (form == logical_plan::SpecialForm::kNullIf) {
      // args[2] is a null literal whose type is the common comparison
      // supertype.
      VELOX_CHECK_EQ(args.size(), 3);
      return std::make_shared<velox::core::NullIfTypedExpr>(
          args[0], args[1], args[2]->type());
    }

    if (form == logical_plan::SpecialForm::kIn) {
      // An all-literal IN list lowers to `in(col, ARRAY[...])`; a list with any
      // non-literal element stays a variadic call.
      if (auto lowered = tryLowerConstantIn(call, args[0], pool_)) {
        return lowered;
      }
    }

    return std::make_shared<velox::core::CallTypedExpr>(
        resultType, std::move(args), specialForm(*form));
  }

  return std::make_shared<velox::core::CallTypedExpr>(
      resultType, std::move(args), std::string{call->name()});
}

velox::core::TypedExprPtr ExprEmitter::toTypedExpr(
    ExprCP expr,
    ColumnNaming naming) {
  switch (expr->type()) {
    case PlanType::kColumnExpr:
      return columnToTypedExpr(expr->as<Column>(), naming);
    case PlanType::kLiteralExpr:
      return literalToTypedExpr(expr->as<Literal>());
    case PlanType::kCallExpr: {
      const auto* call = expr->as<Call>();
      std::vector<velox::core::TypedExprPtr> args;
      args.reserve(call->args().size());
      for (ExprCP arg : call->args()) {
        args.push_back(toTypedExpr(arg, naming));
      }
      return callToTypedExpr(call, std::move(args));
    }
    case PlanType::kLambdaExpr: {
      const auto* lambda = expr->as<Lambda>();
      return lambdaToTypedExpr(lambda, toTypedExpr(lambda->body(), naming));
    }
    default:
      VELOX_NYI("Unsupported expression type: {}", expr->typeName());
  }
}

std::vector<velox::core::TypedExprPtr> ExprEmitter::toTypedExprs(
    const ExprVector& exprs,
    ColumnNaming naming) {
  std::vector<velox::core::TypedExprPtr> result;
  result.reserve(exprs.size());
  for (ExprCP expr : exprs) {
    result.push_back(toTypedExpr(expr, naming));
  }
  return result;
}

velox::core::TypedExprPtr ExprEmitter::makeAnd(
    const ExprVector& predicates,
    ColumnNaming naming) {
  VELOX_DCHECK(!predicates.empty());
  if (predicates.size() == 1) {
    return toTypedExpr(predicates[0], naming);
  }
  return std::make_shared<velox::core::CallTypedExpr>(
      velox::BOOLEAN(),
      toTypedExprs(predicates, naming),
      velox::expression::kAnd);
}

} // namespace facebook::axiom::optimizer::v2
