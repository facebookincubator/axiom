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

#include "axiom/optimizer/Filters.h"

namespace facebook::axiom::optimizer {

/// Predicts the number of distinct values expected after sampling 'numRows'
/// items from a population with 'numDistinct' distinct values, using the
/// coupon collector formula. Returns a value in [1, min(numRows,
/// numDistinct)].
double expectedNumDistincts(double numRows, double numDistinct);

/// Propagates column constraints across a join into a constraint map. Every
/// method takes a 'constraints' map keyed by `expr->id()` that must already
/// hold the join's output columns; the methods refine those entries in place
/// per join semantics. Typical use mirrors `Join::initConstraints`: seed the
/// map with output columns, then call `optionality`, `updateKeys`, and the
/// payload/mark/anti helpers as the join type requires. Estimates that depend
/// on an unknown NDV or cardinality propagate as `std::nullopt`.
struct JoinConstraints {
  /// Returns {leftOptional, rightOptional}: leftOptional is true for RIGHT and
  /// FULL joins, rightOptional for LEFT and FULL joins.
  static std::pair<bool, bool> optionality(velox::core::JoinType joinType);

  /// Refines constraints for the join key columns given each side's
  /// optionality. Inner keys lose nullability and gain comparison selectivity;
  /// optional-side keys gain a null fraction derived from the NDV ratio.
  static void updateKeys(
      const ExprVector& left,
      const ExprVector& right,
      bool leftOptional,
      bool rightOptional,
      ConstraintMap& constraints);

  /// Marks optional-side payload (non-key) columns nullable with the given
  /// null fraction.
  static void updatePayload(
      const ColumnVector& columns,
      const ExprVector& keys,
      std::optional<float> nullFraction,
      ConstraintMap& constraints);

  /// Returns the fraction of non-optional-side rows with no match: the max
  /// over key pairs of max(0, 1 - ndv(optional) / ndv(nonOptional)). Returns
  /// nullopt when either side's NDV is unknown.
  static std::optional<float> computeNullFraction(
      const ExprVector& optionalKeys,
      const ExprVector& nonOptionalKeys,
      const ConstraintMap& constraints);

  /// Adds the boolean mark column constraint for a left-semi-project (mark)
  /// join, with trueFraction derived from raw fanout and filter selectivity.
  /// An unknown fanout or filter selectivity leaves trueFraction unknown.
  static void addMark(
      const ColumnVector& columns,
      std::optional<float> fanout,
      std::optional<float> filterSelectivity,
      ConstraintMap& constraints);

  /// Scales non-key payload column NDV by the coupon collector formula when
  /// the join removes rows (selectivity < 1). Leaves cardinalities unchanged
  /// when 'numRows' is unknown.
  static void scalePayloadCardinality(
      const ColumnVector& columns,
      const ExprVector& keys,
      float selectivity,
      std::optional<float> numRows,
      ConstraintMap& constraints);

  /// Refines key constraints for an anti join (NOT EXISTS / NOT IN): removes
  /// the overlapping NDV and recomputes the surviving null fraction.
  static void updateAntiKeys(
      const ExprVector& leftKeys,
      const ExprVector& rightKeys,
      float antiSelectivity,
      ConstraintMap& constraints);
};

} // namespace facebook::axiom::optimizer
