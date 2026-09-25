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
#include "axiom/optimizer/v2/ExprFactory.h"

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
  // Only reorder operands of the same type.
  if (left->value().type != right->value().type) {
    return false;
  }
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

ExprCP Builder::canonicalizeCoalesce(ExprCP a, ExprCP b) {
  if (shouldInvert(a, b)) {
    std::swap(a, b);
  }
  return ExprFactory(*this).makeCoalesce(a, b);
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
    OrderTypeVector orderTypes,
    std::optional<logical_plan::SpecialAggregateKind> specialKind,
    const optimizer::Aggregate* fallback) {
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
      std::move(orderTypes),
      specialKind,
      fallback);
  aggregateCalls_.insert(aggregate);
  return aggregate;
}

namespace {

// Drops key pairs equal to an earlier pair. `a = b AND a = b` is one equality.
void dropRepeatedKeyPairs(Join::Key& key) {
  const size_t numKeys = key.leftKeys.size();

  // Interned expressions compare by pointer. A pair repeating one that was
  // itself dropped also repeats the pair that one was dropped for, so
  // comparing against all earlier positions needs no separate kept set.
  const auto repeatsEarlier = [&](size_t index) {
    for (size_t earlier = 0; earlier < index; ++earlier) {
      if (key.leftKeys[earlier] == key.leftKeys[index] &&
          key.rightKeys[earlier] == key.rightKeys[index]) {
        return true;
      }
    }
    return false;
  };

  size_t firstRepeat = 0;
  while (firstRepeat < numKeys && !repeatsEarlier(firstRepeat)) {
    ++firstRepeat;
  }
  if (firstRepeat == numKeys) {
    return;
  }

  ExprVector leftKeys{key.leftKeys.begin(), key.leftKeys.begin() + firstRepeat};
  ExprVector rightKeys{
      key.rightKeys.begin(), key.rightKeys.begin() + firstRepeat};
  for (size_t i = firstRepeat + 1; i < numKeys; ++i) {
    if (!repeatsEarlier(i)) {
      leftKeys.push_back(key.leftKeys[i]);
      rightKeys.push_back(key.rightKeys[i]);
    }
  }

  key.leftKeys = std::move(leftKeys);
  key.rightKeys = std::move(rightKeys);
}

// Drops predicates equal to an earlier one. A predicate stated twice selects
// the same rows as one stated once, unless evaluating it twice can give two
// answers, so a non-deterministic one is kept.
void dropRepeatedPredicates(Filter::Key& key) {
  PlanObjectSet seen;
  ExprVector predicates;
  predicates.reserve(key.predicates.size());
  for (ExprCP predicate : key.predicates) {
    if (predicate->containsNonDeterministic()) {
      predicates.push_back(predicate);
      continue;
    }
    if (!seen.contains(predicate)) {
      seen.add(predicate);
      predicates.push_back(predicate);
    }
  }
  if (predicates.size() < key.predicates.size()) {
    key.predicates = std::move(predicates);
  }
}

// Columns a filter has made equal, each mapped to the one they were equated
// to. Two columns with the same anchor hold one value.
using ColumnAnchors = folly::F14FastMap<ColumnCP, ColumnCP>;

// Whether `anchors` records `left` and `right` as filter-enforced equal.
bool isEnforcedEqual(const ColumnAnchors& anchors, ExprCP left, ExprCP right) {
  if (left == right) {
    return true;
  }
  if (!left->isColumn() || !right->isColumn()) {
    return false;
  }
  const auto leftAnchor = anchors.find(left->as<Column>());
  const auto rightAnchor = anchors.find(right->as<Column>());
  return leftAnchor != anchors.end() && rightAnchor != anchors.end() &&
      leftAnchor->second == rightAnchor->second;
}

// Drops key pairs that repeat an earlier pair modulo equalities the derived
// filters enforce: two columns with the same anchor hold one value.
void dropEnforcedKeyPairs(
    Join::Key& key,
    const ColumnAnchors& leftEnforced,
    const ColumnAnchors& rightEnforced) {
  if (leftEnforced.empty() && rightEnforced.empty()) {
    // Only pairs equal to an earlier one could be redundant, and
    // `dropRepeatedKeyPairs` has already removed those.
    return;
  }

  ExprVector leftKeys;
  ExprVector rightKeys;
  for (size_t i = 0; i < key.leftKeys.size(); ++i) {
    bool redundant = false;
    for (size_t kept = 0; kept < leftKeys.size(); ++kept) {
      if (isEnforcedEqual(leftEnforced, leftKeys[kept], key.leftKeys[i]) &&
          isEnforcedEqual(rightEnforced, rightKeys[kept], key.rightKeys[i])) {
        redundant = true;
        break;
      }
    }
    if (!redundant) {
      leftKeys.push_back(key.leftKeys[i]);
      rightKeys.push_back(key.rightKeys[i]);
    }
  }
  key.leftKeys = std::move(leftKeys);
  key.rightKeys = std::move(rightKeys);
}

// Returns `input` filtered by those of `conjuncts` that the chain of Filters
// topping `input` does not already apply, or `input` itself when it applies
// them all.
NodeCP withNewConjuncts(Builder& builder, NodeCP input, ExprVector conjuncts) {
  // Exprs are interned, so a conjunct this input already applies is the same
  // pointer. Skipping it keeps a second join deriving the same equality from
  // stacking a duplicate Filter.
  PlanObjectSet applied;
  for (NodeCP node = input; node->is(NodeType::kFilter);
       node = node->as<Filter>()->input()) {
    for (ExprCP predicate : node->as<Filter>()->predicates()) {
      applied.add(predicate);
    }
  }

  ExprVector fresh;
  for (ExprCP conjunct : conjuncts) {
    if (!applied.contains(conjunct)) {
      fresh.push_back(conjunct);
    }
  }
  if (fresh.empty()) {
    return input;
  }
  return builder.make<Filter>({input, std::move(fresh)});
}

// Returns `input` filtered by an equality between the first of each group in
// `groups` and every other member, recording each group's members against
// that first one in `enforced`.
template <typename Key>
NodeCP withGroupEqualities(
    Builder& builder,
    NodeCP input,
    const std::vector<std::pair<Key, ColumnVector>>& groups,
    ColumnAnchors& enforced) {
  ExprVector conjuncts;
  ExprFactory factory{builder};
  for (const auto& [key, columns] : groups) {
    for (size_t i = 1; i < columns.size(); ++i) {
      conjuncts.push_back(factory.makeEq(columns[0], columns[i]));
      enforced[columns[0]] = columns[0];
      enforced[columns[i]] = columns[0];
    }
  }
  return withNewConjuncts(builder, input, std::move(conjuncts));
}

// Returns `input` with a filter enforcing the equalities it holds implicitly:
// two of its columns in one class of `classes` are equal only where every join
// of that class is applied. Making that explicit keeps it enforced when a
// later step treats the two as interchangeable, and lets pushdown carry it to
// the source. Records the columns it made equal in `enforced`.
NodeCP withImpliedEqualities(
    Builder& builder,
    NodeCP input,
    const folly::F14FastSet<EquivalenceP>& classes,
    ColumnAnchors& enforced) {
  // Insertion-ordered: the conjuncts below are part of the Filter's identity,
  // so their order must not depend on pointer values.
  std::vector<std::pair<EquivalenceP, ColumnVector>> byClass;
  for (ColumnCP column : input->outputColumns()) {
    const EquivalenceP equivalence = column->equivalence();
    if (equivalence == nullptr || !classes.contains(equivalence)) {
      continue;
    }
    auto it = std::ranges::find_if(
        byClass, [&](const auto& entry) { return entry.first == equivalence; });
    if (it == byClass.end()) {
      byClass.emplace_back(equivalence, ColumnVector{});
      it = byClass.end() - 1;
    }
    it->second.push_back(column);
  }

  return withGroupEqualities(builder, input, byClass, enforced);
}

// Returns `input` filtered by the equalities `keys` imply: two keys paired
// with one and the same key on the other side are equal on every matched row.
// `pairedKeys` aligns 1:1 with `keys`. Records the columns it made equal in
// `enforced`.
NodeCP withKeyImpliedEqualities(
    Builder& builder,
    NodeCP input,
    const ExprVector& pairedKeys,
    const ExprVector& keys,
    ColumnAnchors& enforced) {
  std::vector<std::pair<ColumnCP, ColumnVector>> byPairedKey;
  for (size_t i = 0; i < keys.size(); ++i) {
    if (!keys[i]->isColumn() || !pairedKeys[i]->isColumn()) {
      continue;
    }
    ColumnCP paired = pairedKeys[i]->as<Column>();
    auto it = std::ranges::find_if(
        byPairedKey, [&](const auto& entry) { return entry.first == paired; });
    if (it == byPairedKey.end()) {
      byPairedKey.emplace_back(paired, ColumnVector{});
      it = byPairedKey.end() - 1;
    }
    it->second.push_back(keys[i]->as<Column>());
  }

  return withGroupEqualities(builder, input, byPairedKey, enforced);
}

// These join types never emit a right row that found no match, so dropping a
// right row that cannot match is unobservable.
bool rightEmittedOnlyOnMatch(velox::core::JoinType joinType) {
  using JoinType = velox::core::JoinType;
  return joinType == JoinType::kInner || joinType == JoinType::kLeft ||
      joinType == JoinType::kLeftSemiFilter ||
      joinType == JoinType::kLeftSemiProject || joinType == JoinType::kAnti;
}

// The same for the left side. A mark-producing right semi join qualifies: a
// left row that cannot match sets no mark.
bool leftEmittedOnlyOnMatch(velox::core::JoinType joinType) {
  using JoinType = velox::core::JoinType;
  return joinType == JoinType::kInner || joinType == JoinType::kRight ||
      joinType == JoinType::kRightSemiFilter ||
      joinType == JoinType::kRightSemiProject;
}

} // namespace

void Builder::normalizeKey(Filter::Key& key) {
  dropRepeatedPredicates(key);
}

void Builder::normalizeKey(Join::Key& key) {
  dropRepeatedKeyPairs(key);

  // A derived equality is plain `=`, so it drops a row holding a NULL key.
  // Where such a row matches, deriving one would change the result.
  const bool nullKeysMatch = key.nullAsValue || key.nullAware;

  if (key.joinType != velox::core::JoinType::kInner) {
    // No class: these columns are not equal in the output. A side whose
    // unmatched rows are not emitted can still be filtered, since a row
    // failing an equality the keys imply never matches.
    if (nullKeysMatch) {
      return;
    }
    ColumnAnchors leftEnforced;
    ColumnAnchors rightEnforced;
    if (rightEmittedOnlyOnMatch(key.joinType)) {
      key.right = withKeyImpliedEqualities(
          *this, key.right, key.leftKeys, key.rightKeys, rightEnforced);
    }
    if (leftEmittedOnlyOnMatch(key.joinType)) {
      key.left = withKeyImpliedEqualities(
          *this, key.left, key.rightKeys, key.leftKeys, leftEnforced);
    }
    dropEnforcedKeyPairs(key, leftEnforced, rightEnforced);
    return;
  }

  // Classes this join's keys newly merged. A chain of joins sharing one class
  // must derive its equality once, at the join that proves it.
  folly::F14FastSet<EquivalenceP> merged;
  for (size_t i = 0; i < key.leftKeys.size(); ++i) {
    ExprCP leftKey = key.leftKeys[i];
    ExprCP rightKey = key.rightKeys[i];
    if (leftKey->isColumn() && rightKey->isColumn()) {
      ColumnCP leftColumn = leftKey->as<Column>();
      if (leftColumn->equals(rightKey->as<Column>())) {
        merged.insert(leftColumn->equivalence());
      }
    }
  }
  if (merged.empty() || nullKeysMatch) {
    return;
  }

  ColumnAnchors leftEnforced;
  ColumnAnchors rightEnforced;
  key.left = withImpliedEqualities(*this, key.left, merged, leftEnforced);
  key.right = withImpliedEqualities(*this, key.right, merged, rightEnforced);
  dropEnforcedKeyPairs(key, leftEnforced, rightEnforced);
}

} // namespace facebook::axiom::optimizer::v2
