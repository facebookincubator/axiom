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

#include "axiom/optimizer/v2/ExprFactory.h"

namespace facebook::axiom::optimizer::v2 {

ExprCP ExprFactory::makeBooleanCall(
    Name name,
    ExprVector arguments,
    bool specialForm) {
  const Value value(toType(velox::BOOLEAN()), 2);
  const FunctionSet functions =
      Call::unionArgFunctions(functionBits(name, specialForm), arguments);
  return builder_.makeCall(name, value, std::move(arguments), functions);
}

ExprCP ExprFactory::makeAnd(ExprCP lhs, ExprCP rhs) {
  return makeBooleanCall(
      SpecialFormCallNames::kAnd, {lhs, rhs}, /*specialForm=*/true);
}

namespace {

// Recursively appends conjuncts/disjuncts of `expr` to `out` by
// descending through calls named `targetName`.
void flattenInto(ExprCP expr, Name targetName, ExprVector& out) {
  if (expr->is(PlanType::kCallExpr) && expr->as<Call>()->name() == targetName) {
    for (ExprCP arg : expr->as<Call>()->args()) {
      flattenInto(arg, targetName, out);
    }
    return;
  }
  out.push_back(expr);
}

} // namespace

ExprVector ExprFactory::flattenAnd(ExprCP expr) {
  ExprVector conjuncts;
  flattenInto(expr, SpecialFormCallNames::kAnd, conjuncts);
  return conjuncts;
}

void ExprFactory::flattenAnd(ExprCP expr, ExprVector& conjuncts) {
  flattenInto(expr, SpecialFormCallNames::kAnd, conjuncts);
}

ExprCP ExprFactory::makeOr(ExprCP lhs, ExprCP rhs) {
  return makeBooleanCall(
      SpecialFormCallNames::kOr, {lhs, rhs}, /*specialForm=*/true);
}

ExprVector ExprFactory::flattenOr(ExprCP expr) {
  ExprVector disjuncts;
  flattenInto(expr, SpecialFormCallNames::kOr, disjuncts);
  return disjuncts;
}

void ExprFactory::flattenOr(ExprCP expr, ExprVector& disjuncts) {
  flattenInto(expr, SpecialFormCallNames::kOr, disjuncts);
}

ExprCP ExprFactory::makeNot(ExprCP arg) {
  const Name name = builder_.functionNames().negation;
  VELOX_USER_CHECK_NOT_NULL(
      name,
      "ExprFactory::makeNot requires not registered via "
      "FunctionRegistry::registerNegation; the active dialect did not "
      "register it");
  return makeBooleanCall(name, {arg}, /*specialForm=*/false);
}

ExprCP ExprFactory::makeEq(ExprCP lhs, ExprCP rhs) {
  return makeBooleanCall(
      builder_.functionNames().equality, {lhs, rhs}, /*specialForm=*/false);
}

ExprCP ExprFactory::makeLessThanOrEqual(ExprCP lhs, ExprCP rhs) {
  const Name name = builder_.functionNames().lte;
  VELOX_USER_CHECK_NOT_NULL(
      name,
      "ExprFactory::makeLessThanOrEqual requires lessThanOrEqual registered "
      "via FunctionRegistry::registerLessThanOrEqual; the active dialect did "
      "not register it");
  return makeBooleanCall(name, {lhs, rhs}, /*specialForm=*/false);
}

ExprCP ExprFactory::makeIsNull(ExprCP arg) {
  const Name name = builder_.functionNames().isNull;
  VELOX_USER_CHECK_NOT_NULL(
      name,
      "ExprFactory::makeIsNull requires is_null registered via "
      "FunctionRegistry::registerIsNull; the active dialect did not "
      "register it");
  return makeBooleanCall(name, {arg}, /*specialForm=*/false);
}

ExprCP ExprFactory::makeCoalesce(ExprCP value, ExprCP fallback) {
  ExprVector arguments{value, fallback};
  const Name coalesceName = SpecialFormCallNames::kCoalesce;
  const Value resultValue = value->value();
  const FunctionSet functions = Call::unionArgFunctions(
      functionBits(coalesceName, /*specialForm=*/true), arguments);
  return builder_.makeCall(
      coalesceName, resultValue, std::move(arguments), functions);
}

ExprCP ExprFactory::makeIf(ExprCP condition, ExprCP thenExpr, ExprCP elseExpr) {
  ExprVector arguments{condition, thenExpr, elseExpr};
  const Name ifName = SpecialFormCallNames::kIf;
  const Value resultValue = thenExpr->value();
  const FunctionSet functions = Call::unionArgFunctions(
      functionBits(ifName, /*specialForm=*/true), arguments);
  return builder_.makeCall(
      ifName, resultValue, std::move(arguments), functions);
}

namespace {

// Rebuilds `call` with each argument substituted, sharing the original
// when no argument changed. Substituted calls go through `Builder::makeCall`
// so hash-consing stays canonical.
ExprCP substituteCall(
    ExprFactory& factory,
    Builder& builder,
    const Call* call,
    const ColumnVector& sources,
    const ExprVector& targets) {
  ExprVector newArgs;
  newArgs.reserve(call->args().size());

  bool anyChange = false;
  for (ExprCP arg : call->args()) {
    ExprCP newArg = factory.substitute(arg, sources, targets);
    anyChange |= newArg != arg;
    newArgs.push_back(newArg);
  }

  if (!anyChange) {
    return call;
  }

  const bool specialForm = SpecialFormCallNames::isSpecialForm(call->name());
  const FunctionSet functions =
      Call::unionArgFunctions(functionBits(call->name(), specialForm), newArgs);
  return builder.makeCall(
      call->name(), call->value(), std::move(newArgs), functions);
}

// Rebuilds `field` with its base substituted. Field is arena-allocated
// (not hash-consed in Builder), so it goes through `make`,
// which is safe in v2-only contexts.
ExprCP substituteField(
    ExprFactory& factory,
    const Field* field,
    const ColumnVector& sources,
    const ExprVector& targets) {
  ExprCP newBase = factory.substitute(field->base(), sources, targets);
  if (newBase == field->base()) {
    return field;
  }
  if (field->field() != nullptr) {
    return make<Field>(field->value().type, newBase, field->field());
  }
  return make<Field>(field->value().type, newBase, field->index());
}

// Rebuilds `lambda` with its body substituted. The lambda's bound args
// shadow any same-named outer column, so they are removed from the
// source mapping before recursing — a caller's substitution set can't
// rebind them.
ExprCP substituteLambda(
    ExprFactory& factory,
    const Lambda* lambda,
    const ColumnVector& sources,
    const ExprVector& targets) {
  ColumnVector filteredSources;
  ExprVector filteredTargets;
  filteredSources.reserve(sources.size());
  filteredTargets.reserve(targets.size());
  const auto& boundArgs = lambda->args();
  for (size_t i = 0; i < sources.size(); ++i) {
    bool isBound = false;
    for (ColumnCP boundArg : boundArgs) {
      if (boundArg == sources[i]) {
        isBound = true;
        break;
      }
    }
    if (!isBound) {
      filteredSources.push_back(sources[i]);
      filteredTargets.push_back(targets[i]);
    }
  }
  ExprCP newBody =
      factory.substitute(lambda->body(), filteredSources, filteredTargets);
  if (newBody == lambda->body()) {
    return lambda;
  }
  return make<Lambda>(lambda->args(), lambda->value().type, newBody);
}

} // namespace

ExprCP ExprFactory::substitute(
    ExprCP expr,
    const ColumnVector& sources,
    const ExprVector& targets) {
  if (expr == nullptr) {
    return nullptr;
  }
  switch (expr->type()) {
    case PlanType::kColumnExpr: {
      for (size_t i = 0; i < sources.size(); ++i) {
        if (sources[i] == expr) {
          return targets[i];
        }
      }
      return expr;
    }
    case PlanType::kLiteralExpr:
      return expr;
    case PlanType::kCallExpr:
      return substituteCall(
          *this, builder_, expr->as<Call>(), sources, targets);
    case PlanType::kFieldExpr:
      return substituteField(*this, expr->as<Field>(), sources, targets);
    case PlanType::kLambdaExpr:
      return substituteLambda(*this, expr->as<Lambda>(), sources, targets);
    default:
      VELOX_NYI(
          "ExprFactory::substitute: unsupported expression type {}",
          expr->typeName());
  }
}

ExprVector ExprFactory::substitute(
    const ExprVector& exprs,
    const ColumnVector& sources,
    const ExprVector& targets) {
  ExprVector substituted;
  substituted.reserve(exprs.size());
  for (ExprCP expr : exprs) {
    substituted.push_back(substitute(expr, sources, targets));
  }
  return substituted;
}

} // namespace facebook::axiom::optimizer::v2
