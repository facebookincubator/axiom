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

#include "axiom/optimizer/JoinConstraints.h"

#include <algorithm>
#include <cmath>
#include <cstddef>

namespace facebook::axiom::optimizer {

double expectedNumDistincts(double numRows, double numDistinct) {
  if (numDistinct <= 0 || numRows <= 0) {
    return 1.0;
  }

  // Using the coupon collector formula.
  // Expected distinct values = d * (1 - (1 - 1/d)^n)
  // where d is total distinct values and n is number of samples.

  // For numerical stability, use the identity:
  //   (1 - 1/d)^n = exp(n * log(1 - 1/d))
  // When d is large, log(1 - 1/d) ≈ -1/d (first-order Taylor expansion),
  // so (1 - 1/d)^n ≈ exp(-n/d).
  //
  // We use exp(-n/d) directly when d is large enough (d >= 1e6) to avoid
  // precision loss from computing log(1 - 1/d) with floating point.
  // For smaller d, we use std::log1p(-1/d) which is numerically stable
  // for small arguments.
  double exponent;
  if (numDistinct >= 1e6) {
    // Large d: use exp(-n/d) approximation directly.
    exponent = -numRows / numDistinct;
  } else {
    // std::log1p(x) computes log(1+x) accurately for small x.
    exponent = numRows * std::log1p(-1.0 / numDistinct);
  }

  // Clamp exponent to avoid underflow. exp(-746) ≈ 0 in double precision.
  // For very large negative exponents, (1-1/d)^n → 0, so result → d.
  if (exponent < -700) {
    return numDistinct;
  }

  // Use -expm1(x) instead of (1 - exp(x)) to avoid catastrophic cancellation
  // when exponent ≈ 0. The naive form can land a few ULPs above
  // min(numRows, numDistinct), breaking downstream invariants.
  return numDistinct * -std::expm1(exponent);
}

namespace {

// Updates key constraints for inner join (neither side optional).
// Sets nullable=false, nullFraction=0, and applies
// columnComparisonSelectivity to both sides.
void updateKeyInner(
    ExprCP left,
    ExprCP right,
    bool leftInOutput,
    bool rightInOutput,
    Value leftValue,
    Value rightValue,
    ConstraintMap& constraints) {
  leftValue.nullable = false;
  leftValue.nullFraction = 0.0f;
  rightValue.nullable = false;
  rightValue.nullFraction = 0.0f;

  ConstraintMap tempConstraints;
  columnComparisonSelectivity(
      left,
      right,
      leftValue,
      rightValue,
      queryCtx()->functionNames().equality,
      true,
      tempConstraints);

  if (leftInOutput && tempConstraints.contains(left->id())) {
    constraints.insert_or_assign(left->id(), tempConstraints.at(left->id()));
  }
  if (rightInOutput && tempConstraints.contains(right->id())) {
    constraints.insert_or_assign(right->id(), tempConstraints.at(right->id()));
  }
}

// Updates key constraints for outer join (one or both sides optional).
// Computes null fraction for optional-side keys based on the ratio of
// distinct values. Non-optional side constraints are not modified.
void updateKeyOuter(
    ExprCP left,
    ExprCP right,
    bool leftOptional,
    bool rightOptional,
    bool leftInOutput,
    bool rightInOutput,
    Value leftValue,
    Value rightValue,
    ConstraintMap& constraints) {
  // Compute the fraction of rows on the non-optional side that don't find
  // a match on the optional side.
  //
  // For LEFT JOIN: nullFraction = fraction of left rows that don't match
  // any right row = max(0, 1 - ndv(right) / ndv(left)).
  //
  // For RIGHT JOIN: same logic with sides reversed.
  // Null fraction is unknown when either side's NDV is unknown; otherwise it
  // defaults to 0 and is refined from the NDV ratio below.
  const bool cardinalitiesKnown =
      leftValue.cardinality.has_value() && rightValue.cardinality.has_value();
  std::optional<float> leftNullFraction =
      cardinalitiesKnown ? std::optional<float>(0.0f) : std::nullopt;
  std::optional<float> rightNullFraction =
      cardinalitiesKnown ? std::optional<float>(0.0f) : std::nullopt;

  if (leftOptional && cardinalitiesKnown && *rightValue.cardinality > 0) {
    leftNullFraction =
        std::max(0.0f, 1.0f - *leftValue.cardinality / *rightValue.cardinality);
  }

  if (rightOptional && cardinalitiesKnown && *leftValue.cardinality > 0) {
    rightNullFraction =
        std::max(0.0f, 1.0f - *rightValue.cardinality / *leftValue.cardinality);
  }

  if (leftOptional) {
    leftValue.nullable = true;
    leftValue.nullFraction = leftNullFraction;
  }

  if (rightOptional) {
    rightValue.nullable = true;
    rightValue.nullFraction = rightNullFraction;
  }

  // Compute constraints into tempConstraints, then selectively apply only
  // to the optional side(s). We use tempConstraints because
  // columnComparisonSelectivity adds constraints for both sides, but for
  // outer joins we only want constraints on the optional side(s). We also
  // need to preserve the nullable and nullFraction we computed above.
  ConstraintMap tempConstraints;
  columnComparisonSelectivity(
      left,
      right,
      leftValue,
      rightValue,
      queryCtx()->functionNames().equality,
      true,
      tempConstraints);

  if (leftOptional && leftInOutput && tempConstraints.contains(left->id())) {
    Value constraint = tempConstraints.at(left->id());
    constraint.nullable = leftValue.nullable;
    constraint.nullFraction = leftValue.nullFraction;
    constraints.insert_or_assign(left->id(), constraint);
  }
  if (rightOptional && rightInOutput && tempConstraints.contains(right->id())) {
    Value constraint = tempConstraints.at(right->id());
    constraint.nullable = rightValue.nullable;
    constraint.nullFraction = rightValue.nullFraction;
    constraints.insert_or_assign(right->id(), constraint);
  }
}

// Updates constraints for a single join key pair. Dispatches to
// updateKeyInner or updateKeyOuter based on optionality. constraints must
// already be populated with output columns.
void updateKey(
    ExprCP left,
    ExprCP right,
    bool leftOptional,
    bool rightOptional,
    ConstraintMap& constraints) {
  bool leftInOutput = constraints.contains(left->id());
  bool rightInOutput = constraints.contains(right->id());
  if (!leftInOutput && !rightInOutput) {
    return;
  }

  Value leftValue = value(constraints, left);
  Value rightValue = value(constraints, right);

  if (!leftOptional && !rightOptional) {
    updateKeyInner(
        left,
        right,
        leftInOutput,
        rightInOutput,
        leftValue,
        rightValue,
        constraints);
  } else {
    updateKeyOuter(
        left,
        right,
        leftOptional,
        rightOptional,
        leftInOutput,
        rightInOutput,
        leftValue,
        rightValue,
        constraints);
  }
}

} // namespace

std::pair<bool, bool> JoinConstraints::optionality(
    velox::core::JoinType joinType) {
  bool leftOptional =
      (joinType == velox::core::JoinType::kRight ||
       joinType == velox::core::JoinType::kFull);
  bool rightOptional =
      (joinType == velox::core::JoinType::kLeft ||
       joinType == velox::core::JoinType::kFull);
  return {leftOptional, rightOptional};
}

void JoinConstraints::updateKeys(
    const ExprVector& left,
    const ExprVector& right,
    bool leftOptional,
    bool rightOptional,
    ConstraintMap& constraints) {
  VELOX_CHECK_EQ(
      left.size(), right.size(), "Join key vectors must have same size");

  for (size_t i = 0; i < left.size(); ++i) {
    updateKey(left[i], right[i], leftOptional, rightOptional, constraints);
  }
}

void JoinConstraints::updatePayload(
    const ColumnVector& columns,
    const ExprVector& keys,
    std::optional<float> nullFraction,
    ConstraintMap& constraints) {
  auto keyIds = PlanObjectSet::fromObjects(keys);

  for (auto* column : columns) {
    if (keyIds.contains(column)) {
      continue;
    }
    auto it = constraints.find(column->id());
    if (it != constraints.end()) {
      it->second.nullable = true;
      it->second.nullFraction = nullFraction;
    }
  }
}

std::optional<float> JoinConstraints::computeNullFraction(
    const ExprVector& optionalKeys,
    const ExprVector& nonOptionalKeys,
    const ConstraintMap& constraints) {
  float nullFraction = 0.0f;
  for (size_t i = 0; i < optionalKeys.size(); ++i) {
    const auto& optionalValue = value(constraints, optionalKeys[i]);
    const auto& nonOptionalValue = value(constraints, nonOptionalKeys[i]);
    // An unknown NDV on either side makes the null fraction unknown.
    if (!optionalValue.cardinality.has_value() ||
        !nonOptionalValue.cardinality.has_value()) {
      return std::nullopt;
    }
    if (*nonOptionalValue.cardinality > 0) {
      nullFraction = std::max(
          nullFraction,
          std::max(
              0.0f,
              1.0f -
                  *optionalValue.cardinality / *nonOptionalValue.cardinality));
    }
  }
  return nullFraction;
}

void JoinConstraints::addMark(
    const ColumnVector& columns,
    std::optional<float> fanout,
    std::optional<float> filterSelectivity,
    ConstraintMap& constraints) {
  VELOX_DCHECK(!columns.empty(), "LeftSemiProject must have columns");

  auto* markColumn = columns.back();
  VELOX_DCHECK(
      markColumn->value().type->isBoolean(), "Mark column must be boolean");

  Value markValue = markColumn->value();
  // An unknown fanout or filter selectivity makes the mark's true fraction
  // unknown.
  markValue.trueFraction = mul(minOf(fanout, 1.0f), filterSelectivity);
  markValue.nullable = false;
  markValue.nullFraction = 0;

  constraints.emplace(markColumn->id(), markValue);
}

void JoinConstraints::scalePayloadCardinality(
    const ColumnVector& columns,
    const ExprVector& keys,
    float selectivity,
    std::optional<float> numRows,
    ConstraintMap& constraints) {
  // Without a known input row count the scaled NDV cannot be estimated;
  // leave payload cardinalities unchanged.
  if (selectivity >= 1.0f || !numRows.has_value()) {
    return;
  }
  auto keyIds = PlanObjectSet::fromObjects(keys);
  for (auto* column : columns) {
    if (keyIds.contains(column)) {
      continue;
    }
    auto it = constraints.find(column->id());
    if (it != constraints.end() && it->second.cardinality.has_value()) {
      it->second.cardinality = std::max(
          1.0f,
          (float)expectedNumDistincts(
              *numRows * selectivity, *it->second.cardinality));
    }
  }
}

void JoinConstraints::updateAntiKeys(
    const ExprVector& leftKeys,
    const ExprVector& rightKeys,
    float antiSelectivity,
    ConstraintMap& constraints) {
  for (size_t i = 0; i < leftKeys.size(); ++i) {
    auto it = constraints.find(leftKeys[i]->id());
    if (it != constraints.end()) {
      auto& value = it->second;
      const auto rightNdv = rightKeys[i]->value().cardinality;
      // Refine NDV only when both the left and right key NDVs are known.
      if (value.cardinality.has_value() && rightNdv.has_value()) {
        const float minNdv = std::min(*value.cardinality, *rightNdv);
        value.cardinality = std::max(1.0f, *value.cardinality - minNdv);
      }

      // Refine the surviving null fraction only when it is known.
      if (value.nullFraction.has_value()) {
        const float survivingFraction = *value.nullFraction +
            (1.0f - *value.nullFraction) * antiSelectivity;
        value.nullFraction = survivingFraction > 0
            ? *value.nullFraction / survivingFraction
            : 0.0f;
      }
    }
  }
}

} // namespace facebook::axiom::optimizer
