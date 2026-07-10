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

#include "axiom/optimizer/v2/Builder.h"

#include "axiom/optimizer/FunctionRegistry.h"

namespace facebook::axiom::optimizer::v2 {

Builder::Builder() : functionNames_{queryCtx()->functionNames()} {
  const auto& reversibles = FunctionRegistry::instance()->reversibleFunctions();
  reversibleFunctions_.reserve(reversibles.size() * 2);
  for (const auto& [name, reverseName] : reversibles) {
    reversibleFunctions_.emplace(toName(name), toName(reverseName));
    reversibleFunctions_.emplace(toName(reverseName), toName(name));
  }
}

namespace {

// Returns true if `args` should be swapped to put a literal on the
// right, or — when neither side is a literal — to put the lower-id
// expression on the left.
bool shouldInvert(ExprCP left, ExprCP right) {
  const bool leftIsLiteral = left->is(PlanType::kLiteralExpr);
  const bool rightIsLiteral = right->is(PlanType::kLiteralExpr);
  if (leftIsLiteral && !rightIsLiteral) {
    return true;
  }
  if (!leftIsLiteral && !rightIsLiteral && left->id() > right->id()) {
    return true;
  }
  return false;
}

} // namespace

void Builder::canonicalizeCall(Name& name, ExprVector& args) const {
  if (args.size() != 2) {
    return;
  }
  auto it = reversibleFunctions_.find(name);
  if (it == reversibleFunctions_.end()) {
    return;
  }
  if (!shouldInvert(args[0], args[1])) {
    return;
  }
  std::swap(args[0], args[1]);
  name = it->second;
}

const Literal*
Builder::makeLiteral(velox::Variant&& variant, TypeCP type, float cardinality) {
  const Value value(type, cardinality);
  // Look up by the caller's Variant first, so a cache hit registers nothing.
  Literal::KeyView view{type, &variant};
  if (auto it = literals_.find(view); it != literals_.end()) {
    return *it;
  }
  auto* registered = queryCtx()->registerVariant(
      std::make_unique<velox::Variant>(std::move(variant)));
  auto* literal = optimizer::make<Literal>(value, registered);
  literals_.insert(literal);
  return literal;
}

const Literal* Builder::makeLiteral(
    const Value& value,
    const velox::Variant* variant) {
  Literal::KeyView view{value.type, variant};
  if (auto it = literals_.find(view); it != literals_.end()) {
    return *it;
  }
  auto* literal = optimizer::make<Literal>(value, variant);
  literals_.insert(literal);
  return literal;
}

const Call* Builder::makeCall(
    Name name,
    const Value& value,
    ExprVector args,
    FunctionSet functions) {
  if (functions.contains(FunctionSet::kNonDeterministic)) {
    // Skip dedup: each evaluation of a non-deterministic call produces an
    // independent value, so identical calls must stay distinct objects.
    return optimizer::make<Call>(name, value, std::move(args), functions);
  }
  canonicalizeCall(name, args);
  Call::KeyView view{name, value.type, args};
  if (auto it = calls_.find(view); it != calls_.end()) {
    return *it;
  }
  auto* call = optimizer::make<Call>(name, value, std::move(args), functions);
  calls_.insert(call);
  return call;
}

const optimizer::Aggregate* Builder::makeAggregate(
    Name name,
    const Value& value,
    ExprVector args,
    FunctionSet functions,
    bool isDistinct,
    ExprCP condition,
    TypeCP intermediateType,
    ExprVector orderKeys,
    OrderTypeVector orderTypes) {
  optimizer::Aggregate::KeyView view{
      name, args, isDistinct, condition, orderKeys, orderTypes};
  if (auto it = aggregateCalls_.find(view); it != aggregateCalls_.end()) {
    return *it;
  }
  auto* aggregate = optimizer::make<optimizer::Aggregate>(
      name,
      value,
      std::move(args),
      functions,
      isDistinct,
      condition,
      intermediateType,
      std::move(orderKeys),
      std::move(orderTypes));
  aggregateCalls_.insert(aggregate);
  return aggregate;
}

void Builder::addEquivalences(const Join::Key& key) {
  if (key.joinType != velox::core::JoinType::kInner) {
    return;
  }
  for (size_t i = 0; i < key.leftKeys.size(); ++i) {
    ExprCP leftKey = key.leftKeys[i];
    ExprCP rightKey = key.rightKeys[i];
    if (leftKey->isColumn() && rightKey->isColumn()) {
      leftKey->as<Column>()->equals(rightKey->as<Column>());
    }
  }
}

} // namespace facebook::axiom::optimizer::v2
