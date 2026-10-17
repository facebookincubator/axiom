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

#include "axiom/optimizer/v2/JoinPredicatePlacement.h"

#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/v2/AppendAll.h"
#include "axiom/optimizer/v2/ImpliedFilters.h"
#include "axiom/optimizer/v2/JoinCondition.h"

namespace facebook::axiom::optimizer::v2 {
namespace {

// An output predicate can move to an input only when the join preserves that
// input's rows.
bool canPushLeft(velox::core::JoinType joinType, bool leftOnly) {
  return leftOnly && Join::preservedSides(joinType).left;
}

bool canPushRight(velox::core::JoinType joinType, bool rightOnly) {
  return rightOnly && Join::preservedSides(joinType).right;
}

enum class FilterTarget { kKeep, kLeft, kRight };

// A join-condition predicate can move to the input whose rows are not
// independently preserved. Semi and anti joins use their existence-check
// input.
FilterTarget
filterTarget(velox::core::JoinType joinType, bool leftOnly, bool rightOnly) {
  switch (joinType) {
    case velox::core::JoinType::kInner:
      if (leftOnly) {
        return FilterTarget::kLeft;
      }
      return rightOnly ? FilterTarget::kRight : FilterTarget::kKeep;
    case velox::core::JoinType::kLeft:
      return rightOnly ? FilterTarget::kRight : FilterTarget::kKeep;
    case velox::core::JoinType::kRight:
      return leftOnly ? FilterTarget::kLeft : FilterTarget::kKeep;
    case velox::core::JoinType::kFull:
      return FilterTarget::kKeep;
    case velox::core::JoinType::kLeftSemiFilter:
    case velox::core::JoinType::kCountingLeftSemiFilter:
    case velox::core::JoinType::kLeftSemiProject:
    case velox::core::JoinType::kAnti:
    case velox::core::JoinType::kCountingAnti:
      return rightOnly ? FilterTarget::kRight : FilterTarget::kKeep;
    case velox::core::JoinType::kRightSemiFilter:
    case velox::core::JoinType::kRightSemiProject:
    case velox::core::JoinType::kRightAnti:
      return leftOnly ? FilterTarget::kLeft : FilterTarget::kKeep;
    case velox::core::JoinType::kNumJoinTypes:
      break;
  }
  VELOX_UNREACHABLE();
}

// Places the equalities the keys imply and drops the key pairs they make
// redundant.
void placeKeyEqualities(
    velox::core::JoinType joinType,
    JoinPredicatePlacement::Routed& routed,
    Builder& builder) {
  ExprFactory factory(builder);
  const auto equalities = ImpliedFilters::deriveForJoinKeys(
      routed.leftKeys, routed.rightKeys, factory);
  appendAll(
      filterTarget(joinType, /*leftOnly=*/true, /*rightOnly=*/false) ==
              FilterTarget::kLeft
          ? routed.leftInputPredicates
          : routed.joinPredicates,
      equalities.left);
  appendAll(
      filterTarget(joinType, /*leftOnly=*/false, /*rightOnly=*/true) ==
              FilterTarget::kRight
          ? routed.rightInputPredicates
          : routed.joinPredicates,
      equalities.right);

  ExprVector leftKeys;
  ExprVector rightKeys;
  for (size_t i = 0; i < routed.leftKeys.size(); ++i) {
    if (!equalities.redundant[i]) {
      leftKeys.push_back(routed.leftKeys[i]);
      rightKeys.push_back(routed.rightKeys[i]);
    }
  }
  routed.leftKeys = std::move(leftKeys);
  routed.rightKeys = std::move(rightKeys);
}

} // namespace

JoinPredicatePlacement::FilterPropagation
JoinPredicatePlacement::filterPropagation(
    velox::core::JoinType joinType,
    bool nullAware) {
  using JoinType = velox::core::JoinType;
  switch (joinType) {
    case JoinType::kInner:
    case JoinType::kLeftSemiFilter:
    case JoinType::kCountingLeftSemiFilter:
    case JoinType::kRightSemiFilter:
      return {
          .leftToRight = true,
          .rightToLeft = true,
          .emitsOnlyMatchedRows = true,
      };
    case JoinType::kLeft:
      return {.leftToRight = true};
    case JoinType::kRight:
      return {.rightToLeft = true};
    // A NULL on the matching input affects the projected mark or anti result.
    case JoinType::kLeftSemiProject:
      return {.leftToRight = !nullAware};
    case JoinType::kRightSemiProject:
      return {.rightToLeft = !nullAware};
    case JoinType::kAnti:
      return {.leftToRight = !nullAware};
    case JoinType::kCountingAnti:
      return {.leftToRight = true};
    case JoinType::kRightAnti:
      return {.rightToLeft = !nullAware};
    case JoinType::kFull:
      return {};
    case JoinType::kNumJoinTypes:
      break;
  }
  VELOX_UNREACHABLE();
}

std::pair<ColumnVector, ColumnVector> JoinPredicatePlacement::equiColumnPairs(
    JoinCP join,
    const PlanObjectSet& leftColumns,
    const PlanObjectSet& rightColumns,
    const ExprVector& extraConjuncts) {
  ColumnVector leftKeys;
  ColumnVector rightKeys;
  const auto addPair = [&](ColumnCP left, ColumnCP right) {
    for (size_t i = 0; i < leftKeys.size(); ++i) {
      if (leftKeys[i] == left && rightKeys[i] == right) {
        return;
      }
    }
    leftKeys.push_back(left);
    rightKeys.push_back(right);
  };

  for (size_t i = 0; i < join->leftKeys().size(); ++i) {
    ExprCP leftKey = join->leftKeys()[i];
    ExprCP rightKey = join->rightKeys()[i];
    if (leftKey->is(PlanType::kColumnExpr) &&
        rightKey->is(PlanType::kColumnExpr)) {
      addPair(leftKey->as<Column>(), rightKey->as<Column>());
    }
  }

  const Name equalityName = toName(queryCtx()->functionRegistry().equality());
  const auto addEqualityPair = [&](ExprCP conjunct) {
    if (!conjunct->is(PlanType::kCallExpr)) {
      return;
    }
    const Call* call = conjunct->as<Call>();
    if (call->name() != equalityName) {
      return;
    }
    ExprCP first = call->args()[0];
    ExprCP second = call->args()[1];
    if (!first->is(PlanType::kColumnExpr) ||
        !second->is(PlanType::kColumnExpr)) {
      return;
    }
    ColumnCP firstColumn = first->as<Column>();
    ColumnCP secondColumn = second->as<Column>();
    if (leftColumns.contains(firstColumn) &&
        rightColumns.contains(secondColumn)) {
      addPair(firstColumn, secondColumn);
    } else if (
        leftColumns.contains(secondColumn) &&
        rightColumns.contains(firstColumn)) {
      addPair(secondColumn, firstColumn);
    }
  };
  for (ExprCP conjunct : join->filter()) {
    addEqualityPair(conjunct);
  }
  for (ExprCP conjunct : extraConjuncts) {
    addEqualityPair(conjunct);
  }
  return {std::move(leftKeys), std::move(rightKeys)};
}

JoinPredicatePlacement::Routed JoinPredicatePlacement::route(
    const Join* join,
    velox::core::JoinType joinType,
    bool nullAware,
    const PlanObjectSet& leftColumns,
    const PlanObjectSet& rightColumns,
    const ExprVector& pending,
    ExprVector leftInputPredicates,
    ExprVector rightInputPredicates,
    Builder& builder) {
  Routed result{
      .leftInputPredicates = std::move(leftInputPredicates),
      .rightInputPredicates = std::move(rightInputPredicates),
      .leftKeys = join->leftKeys(),
      .rightKeys = join->rightKeys(),
  };
  ExprVector joinCandidates;
  for (ExprCP conjunct : pending) {
    const auto& columns = conjunct->columns();
    // A constant belongs to neither input and remains at the join boundary.
    const bool leftOnly = !columns.empty() && columns.isSubset(leftColumns);
    const bool rightOnly = !columns.empty() && columns.isSubset(rightColumns);
    // Preserve one evaluation per join output.
    if (conjunct->containsNonDeterministic() && (leftOnly || rightOnly)) {
      result.aboveJoinPredicates.push_back(conjunct);
    } else if (canPushLeft(joinType, leftOnly)) {
      result.leftInputPredicates.push_back(conjunct);
    } else if (canPushRight(joinType, rightOnly)) {
      result.rightInputPredicates.push_back(conjunct);
    } else if (joinType == velox::core::JoinType::kInner) {
      joinCandidates.push_back(conjunct);
    } else {
      result.aboveJoinPredicates.push_back(conjunct);
    }
  }

  result.joinPredicates.reserve(join->filter().size());
  for (ExprCP conjunct : join->filter()) {
    // A conjunct can remain on the join regardless of which inputs it reads.
    if (conjunct->containsNonDeterministic()) {
      result.joinPredicates.push_back(conjunct);
      continue;
    }
    const auto& columns = conjunct->columns();
    const bool leftOnly = !columns.empty() && columns.isSubset(leftColumns);
    const bool rightOnly = !columns.empty() && columns.isSubset(rightColumns);
    switch (filterTarget(joinType, leftOnly, rightOnly)) {
      case FilterTarget::kLeft:
        result.leftInputPredicates.push_back(conjunct);
        break;
      case FilterTarget::kRight:
        result.rightInputPredicates.push_back(conjunct);
        break;
      case FilterTarget::kKeep:
        result.joinPredicates.push_back(conjunct);
        break;
    }
  }

  // Implied predicates can filter an outer join's non-preserved input.
  const bool pushLeft = joinType == velox::core::JoinType::kInner ||
      joinType == velox::core::JoinType::kRight;
  const bool pushRight = joinType == velox::core::JoinType::kInner ||
      joinType == velox::core::JoinType::kLeft;
  if (pushLeft || pushRight) {
    ExprVector filters = result.joinPredicates;
    appendAll(filters, joinCandidates);
    ExprFactory factory(builder);
    auto [leftFilters, rightFilters] = ImpliedFilters::deriveForJoinInputs(
        filters, leftColumns, rightColumns, factory);
    if (pushLeft) {
      appendAll(result.leftInputPredicates, leftFilters);
    }
    if (pushRight) {
      appendAll(result.rightInputPredicates, rightFilters);
    }
  }

  if (!joinCandidates.empty()) {
    JoinCondition::Split split =
        JoinCondition::splitEquiKeys(joinCandidates, leftColumns, rightColumns);
    appendAll(result.leftKeys, split.leftKeys);
    appendAll(result.rightKeys, split.rightKeys);
    appendAll(result.joinPredicates, split.residual);
  }

  // A counting join counts its matches per key.
  if (!Join::nullKeysMatch(nullAware, join->nullAsValue()) &&
      !velox::core::isCountingJoin(joinType)) {
    placeKeyEqualities(joinType, result, builder);
  }
  return result;
}

} // namespace facebook::axiom::optimizer::v2
