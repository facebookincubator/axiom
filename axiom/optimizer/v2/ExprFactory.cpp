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

#include <algorithm>

#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>

#include "axiom/optimizer/FunctionRegistry.h"

namespace facebook::axiom::optimizer::v2 {

ExprCP ExprFactory::makeCall(
    Name name,
    Value value,
    ExprVector arguments,
    bool specialForm) {
  const FunctionSet functions =
      Call::unionArgFunctions(functionBits(name, specialForm), arguments);
  return builder_.makeCall(name, value, std::move(arguments), functions);
}

ExprCP ExprFactory::makeBooleanCall(
    Name name,
    ExprVector arguments,
    bool specialForm) {
  return makeCall(
      name,
      Value(toType(velox::BOOLEAN()), 2),
      std::move(arguments),
      specialForm);
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

ExprCP ExprFactory::makeNotDistinctFrom(ExprCP lhs, ExprCP rhs) {
  const Name name = builder_.functionNames().distinctFrom;
  VELOX_USER_CHECK_NOT_NULL(
      name,
      "ExprFactory::makeNotDistinctFrom requires distinct-from registered via "
      "FunctionRegistry::registerDistinctFrom; the active dialect did not "
      "register it");
  return makeNot(makeBooleanCall(name, {lhs, rhs}, /*specialForm=*/false));
}

std::optional<std::pair<ExprCP, ExprCP>> ExprFactory::literalEquality(
    ExprCP expr) const {
  if (expr->isNot(PlanType::kCallExpr)) {
    return std::nullopt;
  }
  const auto* call = expr->as<Call>();
  if (call->name() != builder_.functionNames().equality ||
      call->args()[1]->isNot(PlanType::kLiteralExpr)) {
    return std::nullopt;
  }
  return std::pair{call->args()[0], call->args()[1]};
}

ExprCP ExprFactory::makeLessThan(ExprCP lhs, ExprCP rhs) {
  return makeBooleanCall(
      builder_.functionNames().lt, {lhs, rhs}, /*specialForm=*/false);
}

ExprCP ExprFactory::makeBigintAdd(ExprCP lhs, ExprCP rhs) {
  VELOX_CHECK_EQ(lhs->value().type->kind(), velox::TypeKind::BIGINT);
  VELOX_CHECK_EQ(rhs->value().type->kind(), velox::TypeKind::BIGINT);
  return makeInternalCall(
      builder_.internalFunctionNames().bigintAdd,
      "makeBigintAdd",
      Value(lhs->value().type),
      {lhs, rhs});
}

ExprCP ExprFactory::makeArrayAppend(ExprCP array, ExprCP element) {
  return makeInternalCall(
      builder_.internalFunctionNames().arrayAppend,
      "makeArrayAppend",
      Value(array->value().type),
      {array, element});
}

ExprCP ExprFactory::makeArrayPrepend(ExprCP element, ExprCP array) {
  return makeInternalCall(
      builder_.internalFunctionNames().arrayPrepend,
      "makeArrayPrepend",
      Value(array->value().type),
      {element, array});
}

ExprCP ExprFactory::makeInternalCall(
    Name name,
    std::string_view caller,
    Value value,
    ExprVector arguments) {
  VELOX_USER_CHECK_NOT_NULL(
      name,
      "ExprFactory::{} requires the internal function prefix: "
      "FunctionRegistry::registerInternalPrefix",
      caller);
  return makeCall(
      name, std::move(value), std::move(arguments), /*specialForm=*/false);
}

ExprCP ExprFactory::makeIn(ExprCP value, std::vector<velox::Variant> values) {
  VELOX_CHECK(!values.empty());
  folly::F14FastSet<velox::Variant, velox::Variant::Hasher> seen;
  seen.reserve(values.size());
  std::erase_if(values, [&](const auto& element) {
    return !seen.insert(element).second;
  });

  const TypeCP type = value->value().type;
  if (values.size() > 1) {
    ExprCP list = makeLiteral(
        velox::Variant::array(std::move(values)),
        toType(velox::ARRAY(toTypePtr(type))));
    return makeBooleanCall(
        SpecialFormCallNames::kIn, {value, list}, /*specialForm=*/true);
  }

  return makeEq(value, makeLiteral(std::move(values.front()), type));
}

ExprCP ExprFactory::makeLiteral(velox::Variant value, TypeCP type) {
  return builder_.makeLiteral(std::move(value), type);
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

ExprCP ExprFactory::makeGreaterThan(ExprCP lhs, ExprCP rhs) {
  return makeBooleanCall(
      builder_.functionNames().gt, {lhs, rhs}, /*specialForm=*/false);
}

ExprCP ExprFactory::makeGreaterThanOrEqual(ExprCP lhs, ExprCP rhs) {
  return makeBooleanCall(
      builder_.functionNames().gte, {lhs, rhs}, /*specialForm=*/false);
}

ExprCP ExprFactory::makeBetween(ExprCP value, ExprCP lower, ExprCP upper) {
  return makeBooleanCall(
      builder_.functionNames().between,
      {value, lower, upper},
      /*specialForm=*/false);
}

ExprCP ExprFactory::makeCardinality(ExprCP value) {
  const Name name = builder_.functionNames().cardinality;
  VELOX_USER_CHECK_NOT_NULL(
      name,
      "ExprFactory::makeCardinality requires cardinality registered via "
      "FunctionRegistry::registerCardinality; the active dialect did not "
      "register it");
  return makeCall(
      name, Value(toType(velox::BIGINT())), {value}, /*specialForm=*/false);
}

ExprCP ExprFactory::makeRowNumber() {
  const Name name = builder_.functionNames().rowNumber;
  VELOX_USER_CHECK_NOT_NULL(
      name,
      "ExprFactory::makeRowNumber requires row_number registered via "
      "FunctionRegistry::registerRowNumber; the active dialect did not "
      "register it");
  // Window functions are non-deterministic with non-default null behavior.
  return builder_.makeCall(
      name,
      Value(toType(velox::BIGINT())),
      {},
      FunctionSet{} | FunctionSet::kNonDeterministic |
          FunctionSet::kNonDefaultNullBehavior);
}

ExprCP ExprFactory::makeRowConstructor(ExprVector arguments, TypeCP rowType) {
  return makeCall(
      SpecialFormCallNames::kRowConstructor,
      Value(rowType),
      std::move(arguments),
      /*specialForm=*/true);
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
  return makeCall(
      SpecialFormCallNames::kCoalesce,
      value->value(),
      {value, fallback},
      /*specialForm=*/true);
}

ExprCP ExprFactory::makeIf(ExprCP condition, ExprCP thenExpr, ExprCP elseExpr) {
  return makeCall(
      SpecialFormCallNames::kIf,
      thenExpr->value(),
      {condition, thenExpr, elseExpr},
      /*specialForm=*/true);
}

ExprCP ExprFactory::makeSwitch(
    const std::vector<std::pair<ExprCP, ExprCP>>& when,
    ExprCP elseExpr) {
  VELOX_CHECK(!when.empty(), "makeSwitch requires at least one condition");

  ExprVector arguments;
  arguments.reserve(when.size() * 2 + 1);
  for (const auto& [condition, result] : when) {
    arguments.push_back(condition);
    arguments.push_back(result);
  }
  arguments.push_back(elseExpr);

  return makeCall(
      SpecialFormCallNames::kSwitch,
      when.front().second->value(),
      std::move(arguments),
      /*specialForm=*/true);
}

namespace {

// Maps each call, field and lambda reached under one mapping to its rewrite.
using ReplaceMemo = folly::F14FastMap<ExprCP, ExprCP>;

// Rewrites `expr` under `mapping`. `memo` holds the rewrites already made under
// the same mapping and records the new ones.
ExprCP replaceExpr(
    ExprFactory& factory,
    ExprCP expr,
    const ExprFactory::ExprSubstitution& mapping,
    ReplaceMemo& memo);

// Rebuilds `call` with each argument replaced, sharing the original
// when no argument changed.
ExprCP replaceInCall(
    ExprFactory& factory,
    const Call* call,
    const ExprFactory::ExprSubstitution& mapping,
    ReplaceMemo& memo) {
  ExprVector newArgs;
  newArgs.reserve(call->args().size());

  bool anyChange = false;
  for (ExprCP arg : call->args()) {
    ExprCP newArg = replaceExpr(factory, arg, mapping, memo);
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
    const ExprFactory::ExprSubstitution& mapping,
    ReplaceMemo& memo) {
  ExprCP newBase = replaceExpr(factory, field->base(), mapping, memo);
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
    const ExprFactory::ExprSubstitution& mapping,
    ReplaceMemo& memo) {
  const bool shadowsMapping = std::ranges::any_of(
      lambda->args(),
      [&](ColumnCP boundArg) { return mapping.contains(boundArg); });
  ExprCP newBody{nullptr};
  if (!shadowsMapping) {
    // The body can capture subexpressions that the enclosing expression also
    // reads, so it shares the memo.
    newBody = replaceExpr(factory, lambda->body(), mapping, memo);
  } else {
    // The body sees a narrower mapping, so it gets a memo of its own.
    ExprFactory::ExprSubstitution visible{mapping};
    for (ColumnCP boundArg : lambda->args()) {
      visible.erase(boundArg);
    }
    ReplaceMemo bodyMemo;
    newBody = replaceExpr(factory, lambda->body(), visible, bodyMemo);
  }
  if (newBody == lambda->body()) {
    return lambda;
  }
  return factory.rebuildLambda(lambda, newBody);
}

ExprFactory::ExprSubstitution toSubstitution(
    const ColumnVector& sources,
    const ExprVector& targets) {
  VELOX_CHECK_EQ(sources.size(), targets.size());
  ExprFactory::ExprSubstitution mapping;
  mapping.reserve(sources.size());
  for (size_t i = 0; i < sources.size(); ++i) {
    const auto [it, inserted] = mapping.emplace(sources[i], targets[i]);
    VELOX_CHECK(
        inserted || it->second == targets[i],
        "Conflicting substitution targets for source: {}",
        sources[i]->toString());
  }
  return mapping;
}

ExprCP replaceExpr(
    ExprFactory& factory,
    ExprCP expr,
    const ExprFactory::ExprSubstitution& mapping,
    ReplaceMemo& memo) {
  if (expr == nullptr) {
    return nullptr;
  }
  if (auto it = mapping.find(expr); it != mapping.end()) {
    return it->second;
  }
  if (auto it = memo.find(expr); it != memo.end()) {
    return it->second;
  }
  ExprCP result{nullptr};
  switch (expr->type()) {
    case PlanType::kColumnExpr:
    case PlanType::kLiteralExpr:
      return expr;
    case PlanType::kCallExpr:
      result = replaceInCall(factory, expr->as<Call>(), mapping, memo);
      break;
    case PlanType::kFieldExpr:
      result = replaceInField(factory, expr->as<Field>(), mapping, memo);
      break;
    case PlanType::kLambdaExpr:
      result = replaceInLambda(factory, expr->as<Lambda>(), mapping, memo);
      break;
    default:
      VELOX_NYI(
          "ExprFactory::replace: unsupported expression type {}",
          expr->typeName());
  }
  memo.emplace(expr, result);
  return result;
}

} // namespace

ExprCP ExprFactory::replace(ExprCP expr, const ExprSubstitution& mapping) {
  ReplaceMemo memo;
  return replaceExpr(*this, expr, mapping, memo);
}

ExprVector ExprFactory::replace(
    const ExprVector& exprs,
    const ExprSubstitution& mapping) {
  ReplaceMemo memo;
  ExprVector result;
  result.reserve(exprs.size());
  for (ExprCP expr : exprs) {
    result.push_back(replaceExpr(*this, expr, mapping, memo));
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

ExprCP ExprFactory::rebuildLambda(const Lambda* lambda, ExprCP body) {
  return make<Lambda>(lambda->args(), lambda->value().type, body);
}

ExprCP ExprFactory::rebuildCall(const Call* call, ExprVector args) {
  const FunctionSet functions =
      Call::unionArgFunctions(call->functions(), args);
  return builder_.makeCall(
      call->name(), call->value(), std::move(args), functions);
}

} // namespace facebook::axiom::optimizer::v2
