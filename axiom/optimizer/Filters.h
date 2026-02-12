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

#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/QueryGraphContext.h"

namespace facebook::axiom::optimizer {

/// Map from expression ID to constraint Value.
using ConstraintMap = QGF14FastMap<int32_t, Value>;

/// Represents the estimated selectivity of a filter expression.
///
/// Selectivity captures the estimated fraction of input rows that will pass
/// a filter condition (trueFraction) and the fraction that will produce NULL
/// (nullFraction). The remaining fraction (falseFraction) represents rows that
/// fail the filter.
///
/// Invariant: trueFraction + nullFraction + falseFraction = 1.0
struct Selectivity {
  /// Fraction of rows that satisfy the filter (0.0 to 1.0).
  double trueFraction;

  /// Fraction of rows that produce NULL when evaluating the filter (0.0 to
  /// 1.0).
  double nullFraction;

  /// Default selectivity for near-zero estimates when zero is not certain.
  static constexpr double kLikelyZero = 0.01;

  /// Default selectivity for range comparisons (lt, lte, gt, gte) between two
  /// columns when range information is unavailable.
  static constexpr double kUnknown = 0.5;

  /// Default selectivity for functions and expressions not handled by the
  /// optimizer.
  static constexpr double kLikelyTrue = 0.8;

  /// Default selectivity for types without range semantics or columns without
  /// min/max statistics.
  static constexpr double kNoRange = 0.1;

  /// Checks that trueFraction and nullFraction are in [0, 1] and their sum
  /// does not exceed 1.
  void checkConsistency() const;

  /// Returns the fraction of rows that fail the filter.
  double falseFraction() const {
    return 1.0 - trueFraction - nullFraction;
  }

  /// Returns a Selectivity with zero true fraction (no rows match). Used for
  /// contradictory conditions and comparisons with NULL.
  static Selectivity zero(double nullFraction) {
    VELOX_DCHECK_GE(nullFraction, 0.0);
    VELOX_DCHECK_LE(nullFraction, 1.0);
    return {0.0, nullFraction};
  }

  /// Returns a Selectivity with near-zero true fraction. Used when estimation
  /// suggests zero selectivity but it is not provably zero.
  static Selectivity likelyZero(double nullFraction) {
    return make(kLikelyZero, nullFraction);
  }

  /// Returns a default selectivity when precise estimation is not possible.
  static Selectivity unknown(double nullFraction) {
    return make(kUnknown, nullFraction);
  }

  /// Returns a default selectivity for unhandled functions and expressions.
  /// Assumes most rows pass (80%) and no NULLs are produced.
  static Selectivity likelyTrue() {
    return {kLikelyTrue, 0.0};
  }

  /// Returns a default selectivity for types without meaningful range
  /// semantics (e.g., ARRAY, MAP, VARBINARY) or columns without min/max
  /// statistics.
  static Selectivity noRange(double nullFraction) {
    return make(kNoRange, nullFraction);
  }

 private:
  // Constructs a Selectivity by scaling baseSelectivity by (1 - nullFraction).
  // Floors trueFraction at kLikelyZero to handle nullFraction ≈ 1.0, and caps
  // nullFraction to maintain the invariant trueFraction + nullFraction <= 1.
  static Selectivity make(double baseSelectivity, double nullFraction) {
    VELOX_DCHECK_GE(nullFraction, 0.0);
    VELOX_DCHECK_LE(nullFraction, 1.0);
    double trueFraction =
        std::max(baseSelectivity * (1.0 - nullFraction), kLikelyZero);
    return {trueFraction, std::min(nullFraction, 1.0 - trueFraction)};
  }
};

/// Returns the Value for an expression from constraints if present,
/// otherwise returns expr->value().
const Value& value(const ConstraintMap& constraints, ExprCP expr);

/// Derives constraints for an expression based on its type and arguments.
/// Records the result in constraints keyed on expr->id().
/// If update is true, recomputes constraints for non-leaf expressions even if
/// already cached, and stores the updated constraint.
Value exprConstraint(
    ExprCP expr,
    ConstraintMap& constraints,
    bool update = false);

/// Clamps Value's cardinality to type-specific limits.
/// BOOLEAN: max 2, TINYINT: max 256, SMALLINT: max 65536.
Value clampCardinality(const Value& value);

/// Computes selectivity for a conjunction of expressions.
Selectivity conjunctsSelectivity(
    ConstraintMap& constraints,
    std::span<const ExprCP> conjuncts,
    bool updateConstraints);

/// Computes selectivity for a single expression.
///
/// Estimates the fraction of rows that will satisfy the expression
/// (trueFraction) and the fraction that will produce NULL (nullFraction).
///
/// Handles the following expression types:
/// - NOT: Inverts trueFraction, preserves nullFraction
/// - AND: Delegates to conjunctsSelectivity
/// - OR: Combines disjunct selectivities
/// - ISNULL: trueFraction = argument's nullFraction
/// - IN: Uses range selectivity with the IN list
/// - Comparisons (eq, lt, lte, gt, gte): Uses range or column comparison
/// selectivity
/// - Boolean columns: Uses trueFraction from Value if known, else 0.8
/// - Other columns: trueFraction = 1 - nullFraction
/// - NULL literal: {0.0, 1.0}
/// - FALSE literal: {0.0, 0.0}
/// - Other literals: {1.0, 0.0}
/// - Unknown functions: Default selectivity {0.8, 0.0}
///
/// @param constraints The constraint map containing refined value statistics.
/// @param expr The expression to compute selectivity for.
/// @param updateConstraints If true, updates constraints with refined
///        value ranges based on the expression semantics.
/// @return Selectivity with trueFraction and nullFraction.
Selectivity exprSelectivity(
    ConstraintMap& constraints,
    ExprCP expr,
    bool updateConstraints);

/// Computes selectivity for comparisons between two columns/expressions
/// and optionally updates constraints.
///
/// Takes both expressions and their values as separate parameters because:
/// 1. The expressions are needed to get IDs for updating constraints.
/// 2. The values may differ from expr->value() - callers may pass modified
///    values (e.g., with adjusted nullable/nullFraction for outer joins) or
///    values from constraints that reflect prior constraint propagation.
///
/// @param left Left expression (may be nullptr if updateConstraints is false).
/// @param right Right expression (may be nullptr if updateConstraints is
/// false).
/// @param leftValue Statistics for left expression.
/// @param rightValue Statistics for right expression.
/// @param funcName Comparison operator: "eq", "lt", "lte", "gt", "gte".
/// @param updateConstraints If true, adds refined constraints to the map.
/// @param constraints Map to store constraints keyed by expression ID.
Selectivity columnComparisonSelectivity(
    ExprCP left,
    ExprCP right,
    const Value& leftValue,
    const Value& rightValue,
    Name funcName,
    bool updateConstraints,
    ConstraintMap& constraints);

// ---------------------------------------------------------------------------
// Functions below are exported for testing. They are internal implementation
// details and should not be called directly from production code outside of
// Filters.cpp.
// ---------------------------------------------------------------------------

Selectivity combineConjuncts(std::span<const Selectivity> selectivities);

Selectivity combineDisjuncts(std::span<const Selectivity> selectivities);

} // namespace facebook::axiom::optimizer
