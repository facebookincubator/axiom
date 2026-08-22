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
  if (lhs == rhs && !lhs->containsNonDeterministic()) {
    return lhs;
  }
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

ExprCP ExprFactory::makeSamplePredicate(double fraction) {
  const Name randName = builder_.functionNames().random;
  VELOX_USER_CHECK_NOT_NULL(
      randName,
      "BERNOULLI sampling requires a 'rand' function registered via "
      "FunctionRegistry::registerRandom; the active dialect did not register "
      "it");
  const Name ltName = builder_.functionNames().lt;
  VELOX_USER_CHECK_NOT_NULL(
      ltName,
      "BERNOULLI sampling requires lessThan registered via "
      "FunctionRegistry::registerLessThan; the active dialect did not register "
      "it");

  ExprCP randCall = builder_.makeCall(
      randName,
      Value(toType(velox::DOUBLE())),
      /*args=*/{},
      functionBits(randName, /*specialForm=*/false));
  ExprCP threshold =
      builder_.makeLiteral(velox::Variant(fraction), toType(velox::DOUBLE()));
  return makeBooleanCall(ltName, {randCall, threshold}, /*specialForm=*/false);
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

// Rebuilds `call` with each argument replaced, sharing the original
// when no argument changed.
ExprCP replaceInCall(
    ExprFactory& factory,
    const Call* call,
    const ExprFactory::ExprSubstitution& mapping) {
  ExprVector newArgs;
  newArgs.reserve(call->args().size());

  bool anyChange = false;
  for (ExprCP arg : call->args()) {
    ExprCP newArg = factory.replace(arg, mapping);
    anyChange |= newArg != arg;
    newArgs.push_back(newArg);
  }

  if (!anyChange) {
    return call;
  }

  return factory.rebuildCall(call, std::move(newArgs));
}

// Rebuilds `field` with its base replaced, sharing the original when the
// base did not change.
ExprCP replaceInField(
    ExprFactory& factory,
    const Field* field,
    const ExprFactory::ExprSubstitution& mapping) {
  ExprCP newBase = factory.replace(field->base(), mapping);
  if (newBase == field->base()) {
    return field;
  }
  return factory.rebuildField(field, newBase);
}

// Rebuilds `lambda` with its body replaced. The lambda's bound args shadow
// anything outside, so they are dropped from the mapping before recursing.
ExprCP replaceInLambda(
    ExprFactory& factory,
    const Lambda* lambda,
    const ExprFactory::ExprSubstitution& mapping) {
  ExprFactory::ExprSubstitution visible;
  visible.reserve(mapping.size());
  for (const auto& [source, target] : mapping) {
    bool isBound = false;
    for (ColumnCP boundArg : lambda->args()) {
      if (boundArg == source) {
        isBound = true;
        break;
      }
    }
    if (!isBound) {
      visible.emplace(source, target);
    }
  }
  ExprCP newBody = factory.replace(lambda->body(), visible);
  if (newBody == lambda->body()) {
    return lambda;
  }
  return make<Lambda>(lambda->args(), lambda->value().type, newBody);
}

ExprFactory::ExprSubstitution toSubstitution(
    const ColumnVector& sources,
    const ExprVector& targets) {
  VELOX_CHECK_EQ(sources.size(), targets.size());
  ExprFactory::ExprSubstitution mapping;
  mapping.reserve(sources.size());
  for (size_t i = 0; i < sources.size(); ++i) {
    const bool inserted = mapping.emplace(sources[i], targets[i]).second;
    VELOX_CHECK(
        inserted, "Duplicate substitution source: {}", sources[i]->toString());
  }
  return mapping;
}

} // namespace

ExprCP ExprFactory::replace(ExprCP expr, const ExprSubstitution& mapping) {
  if (expr == nullptr) {
    return nullptr;
  }
  if (auto it = mapping.find(expr); it != mapping.end()) {
    return it->second;
  }
  switch (expr->type()) {
    case PlanType::kColumnExpr:
    case PlanType::kLiteralExpr:
      return expr;
    case PlanType::kCallExpr:
      return replaceInCall(*this, expr->as<Call>(), mapping);
    case PlanType::kFieldExpr:
      return replaceInField(*this, expr->as<Field>(), mapping);
    case PlanType::kLambdaExpr:
      return replaceInLambda(*this, expr->as<Lambda>(), mapping);
    default:
      VELOX_NYI(
          "ExprFactory::replace: unsupported expression type {}",
          expr->typeName());
  }
}

ExprVector ExprFactory::replace(
    const ExprVector& exprs,
    const ExprSubstitution& mapping) {
  ExprVector result;
  result.reserve(exprs.size());
  for (ExprCP expr : exprs) {
    result.push_back(replace(expr, mapping));
  }
  return result;
}

ExprCP ExprFactory::substitute(
    ExprCP expr,
    const ColumnVector& sources,
    const ExprVector& targets) {
  return replace(expr, toSubstitution(sources, targets));
}

ExprVector ExprFactory::substitute(
    const ExprVector& exprs,
    const ColumnVector& sources,
    const ExprVector& targets) {
  return replace(exprs, toSubstitution(sources, targets));
}

ExprCP ExprFactory::rebuildField(const Field* field, ExprCP base) {
  if (field->field() != nullptr) {
    return make<Field>(field->value().type, base, field->field());
  }
  return make<Field>(field->value().type, base, field->index());
}

ExprCP ExprFactory::rebuildCall(const Call* call, ExprVector args) {
  const bool specialForm = SpecialFormCallNames::isSpecialForm(call->name());
  const FunctionSet functions =
      Call::unionArgFunctions(functionBits(call->name(), specialForm), args);
  return builder_.makeCall(
      call->name(), call->value(), std::move(args), functions);
}

} // namespace facebook::axiom::optimizer::v2
