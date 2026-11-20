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

#include <array>
#include <concepts>
#include <span>
#include <vector>

#include "axiom/optimizer/Filters.h"

namespace facebook::velox::common {
class Filter;
} // namespace facebook::velox::common

/// Templated filter-selectivity engine.
///
/// The navigation over an expression tree (classifying a node, reading its
/// operator, arguments, literal value, and statistics) is factored behind a
/// compile-time Policy (the `SelectivityPolicy` concept below) so the same
/// selectivity logic runs over two expression representations: the optimizer's
/// `ExprCP` graph (ExprSelectivityPolicy, in Filters.cpp) and velox `TypedExpr`
/// filters pushed into a connector (TypedExprPolicy, in
/// StatsFilterSelectivityEstimator.cpp).
///
/// The value/formula layer (Selectivity combination, range math, cardinality
/// math) is representation-agnostic and shared verbatim; see the `detail`
/// namespace at the bottom of this file.
namespace facebook::axiom::optimizer {

namespace detail {
// Defined at the bottom of this file; referenced by the engine's private method
// signatures.
struct RangeConstraints;
struct EffectiveBounds;
} // namespace detail

/// Estimates a single-column velox common::Filter over a column with statistics
/// 'value'. Returns selectivity in [0, 1] and writes the refined column
/// statistics (for rows passing the filter) to 'refined'. Filter kinds outside
/// the modeled set (multi-range, bloom, hugeint, timestamp, bool) return a
/// default selectivity. This is the common::Filter counterpart of the templated
/// Expr engine and shares its range/IN/refinement math via the detail::
/// primitives.
std::optional<Selectivity> commonFilterSelectivity(
    const velox::common::Filter& filter,
    const Value& value,
    Value& refined);

/// Adapts a concrete expression representation to SelectivityEngine.
///
/// A Policy classifies a node (isCall/isColumn/isLiteral), exposes its operator
/// name interned to the optimizer's canonical Names -- so the engine compares
/// against queryCtx()->functionNames() and SpecialFormCallNames -- its
/// arguments, its literal value, and its per-node statistics (valueOf). It also
/// yields a hashable ColumnKey identifying the column a predicate constrains,
/// so predicates on the same column are grouped for combined range analysis.
///
/// valueOf() must return a self-contained Value (no reference into per-call
/// scratch that outlives the return).
///
/// When kUpdatesConstraints is true, the Policy must additionally provide
///   void seed(Expr) const;                // derive and cache node constraints
///   void refine(Expr, const Value&) const;// record a refined column Value
/// These are referenced only under `if constexpr
/// (Policy::kUpdatesConstraints)`, so a read-only Policy omits them.
template <typename P>
concept SelectivityPolicy = requires(const P policy, typename P::Expr expr) {
  typename P::Expr;
  typename P::ColumnKey;
  { P::kUpdatesConstraints } -> std::convertible_to<bool>;
  { policy.valueOf(expr) } -> std::convertible_to<Value>;
  { policy.isCall(expr) } -> std::same_as<bool>;
  { policy.isColumn(expr) } -> std::same_as<bool>;
  { policy.isLiteral(expr) } -> std::same_as<bool>;
  { policy.name(expr) } -> std::same_as<Name>;
  {
    policy.args(expr)
  } -> std::convertible_to<std::span<const typename P::Expr>>;
  { policy.literal(expr) } -> std::convertible_to<const velox::Variant&>;
  { policy.columnKey(expr) } -> std::convertible_to<typename P::ColumnKey>;
};

/// Estimates the selectivity of filter expressions from column statistics.
///
/// The std::optional result distinguishes "unknown" from a known selectivity: a
/// std::nullopt means selectivity could not be estimated because a required
/// statistic was unavailable (e.g. a column's NDV needed for an equality or IN
/// estimate), and it propagates -- a single unknown conjunct makes the whole
/// conjunction unknown. This is deliberately not collapsed to a default so the
/// caller can decide how to treat missing statistics; a known-but-tiny
/// selectivity (e.g. Selectivity::likelyZero) is a value, not nullopt.
template <SelectivityPolicy Policy>
class SelectivityEngine {
 public:
  using Expr = typename Policy::Expr;

  explicit SelectivityEngine(const Policy& policy) : policy_{policy} {}

  /// Computes selectivity for a conjunction of filter expressions.
  std::optional<Selectivity> conjunctsSelectivity(
      std::span<const Expr> conjuncts,
      bool updateConstraints);

  /// Computes selectivity for a single expression.
  std::optional<Selectivity> exprSelectivity(Expr expr, bool updateConstraints);

  /// Computes selectivity for a comparison between two columns/expressions with
  /// caller-supplied statistics (which may differ from valueOf(), e.g. adjusted
  /// nullFraction for outer joins). Writes refined constraints for both
  /// operands when the Policy maintains them.
  std::optional<Selectivity> columnComparisonSelectivity(
      Expr left,
      Expr right,
      const Value& leftValue,
      const Value& rightValue,
      Name funcName,
      bool updateConstraints);

 private:
  Selectivity literalSelectivity(Expr expr);

  std::optional<Selectivity> columnSelectivity(Expr expr);

  std::optional<Selectivity> callSelectivity(Expr expr, bool updateConstraints);

  // True when evaluating 'expr' always fails the query: a call to `fail`, or a
  // call that always evaluates an argument that always fails.
  bool alwaysFails(Expr expr) const;

  // Extracts the IN list from an IN call as a registered array Variant,
  // handling both the constant-array in(column, ARRAY[...]) and variadic
  // in(column, v1, ...) encodings. Returns nullptr if any element is not a
  // literal.
  VariantCP getInListFromCall(Expr expr);

  std::optional<Selectivity> processInClause(
      Expr expr,
      detail::RangeConstraints& rangeConstraints,
      double nullFraction);

  std::optional<Selectivity> processEqualityClause(
      Expr expr,
      const Value& exprValue,
      detail::RangeConstraints& rangeConstraints,
      double nullFraction);

  std::optional<Selectivity> processRangeBound(
      Expr expr,
      Name funcName,
      detail::RangeConstraints& rangeConstraints,
      double nullFraction);

  // Adds the inclusive bounds of a BETWEEN expression.
  std::optional<Selectivity> processBetweenClause(
      Expr expr,
      detail::RangeConstraints& rangeConstraints,
      double nullFraction);

  // Creates a refined constraint Value for a column by tightening bounds and
  // adjusting cardinality. Only used when the Policy maintains constraints.
  Value makeConstraint(
      Expr expr,
      VariantCP lower,
      VariantCP upper,
      float cardinality);

  // Computes selectivity for an expression falling within [lower, upper].
  std::optional<Selectivity>
  rangeSelectivity(Expr expr, VariantCP lower, VariantCP upper);

  std::optional<Selectivity> computeEqualitySelectivity(
      Expr leftSide,
      const detail::RangeConstraints& rangeConstraints,
      double nullFraction,
      bool updateConstraints);

  std::optional<Selectivity> computeInListSelectivity(
      Expr leftSide,
      const detail::EffectiveBounds& bounds,
      const Value& exprValue,
      double nullFraction,
      bool updateConstraints);

  std::optional<Selectivity> computeBoundsSelectivity(
      Expr leftSide,
      const detail::RangeConstraints& rangeConstraints,
      const Value& exprValue,
      double nullFraction,
      bool updateConstraints);

  // Computes selectivity for combined range predicates on a single column.
  std::optional<Selectivity> rangeSelectivity(
      std::span<const Expr> exprs,
      bool updateConstraints);

  // Column-vs-column comparison entry: reads statistics via valueOf and
  // dispatches to range- or cardinality-based estimation.
  std::optional<Selectivity> comparisonSelectivity(
      Expr expr,
      bool updateConstraints);

  // Comparison selectivity from cardinality only (no usable range on one or
  // both sides).
  std::optional<Selectivity> cardinalityBasedSelectivity(
      Expr left,
      Expr right,
      const Value& leftValue,
      const Value& rightValue,
      Name funcName,
      bool updateConstraints);

  // Range-overlap comparison selectivity for two columns with known ranges and
  // cardinalities.
  template <velox::TypeKind KIND>
  std::optional<Selectivity> comparisonSelectivityImpl(
      Expr left,
      Expr right,
      const Value& leftValue,
      const Value& rightValue,
      Name funcName,
      bool updateConstraints);

  const Policy& policy_;
};

namespace detail {

// Returns true if the function name is a range bound operator (lt, lte, gt,
// gte).
inline bool isRangeBoundOperator(Name funcName) {
  const auto& fn = queryCtx()->functionNames();
  return funcName == fn.lt || funcName == fn.lte || funcName == fn.gt ||
      funcName == fn.gte;
}

// Returns true if the function name is a comparison operator (eq, lt, lte, gt,
// gte).
inline bool isComparisonOperator(Name funcName) {
  return funcName == queryCtx()->functionNames().equality ||
      isRangeBoundOperator(funcName);
}

// Computes P(A OR B) = P(A) + P(B) - P(A) * P(B) for two null fractions.
inline double combinedNullFraction(
    double leftNullFraction,
    double rightNullFraction) {
  return leftNullFraction + rightNullFraction -
      (leftNullFraction * rightNullFraction);
}

// Computes the size of a range, returning at least 1.0 to avoid division by
// zero.
inline double rangeSize(double low, double high) {
  return std::max(1.0, high - low);
}

// Holds pre-computed overlap information for two ranges.
struct RangeOverlap {
  double low{0};
  double high{0};
  double size{0};

  // Computes the overlap of two ranges [leftLow, leftHigh] and
  // [rightLow, rightHigh].
  static RangeOverlap
  compute(double leftLow, double leftHigh, double rightLow, double rightHigh) {
    double low = std::max(leftLow, rightLow);
    double high = std::min(leftHigh, rightHigh);
    double size = std::max(0.0, high - low);
    return {low, high, size};
  }
};

// Returns the Variant with the larger value.
template <velox::TypeKind KIND>
VariantCP variantMax(VariantCP lhs, VariantCP rhs) {
  return lhs->value<KIND>() >= rhs->value<KIND>() ? lhs : rhs;
}

// Returns the Variant with the smaller value.
template <velox::TypeKind KIND>
VariantCP variantMin(VariantCP lhs, VariantCP rhs) {
  return lhs->value<KIND>() <= rhs->value<KIND>() ? lhs : rhs;
}

// Intersects two array variants and returns a new registered variant
// containing only elements present in both arrays.
inline VariantCP intersectArrayVariants(VariantCP lhs, VariantCP rhs) {
  if (lhs == nullptr || rhs == nullptr) {
    return nullptr;
  }

  const auto& lhsArray = lhs->array();
  const auto& rhsArray = rhs->array();

  std::vector<velox::Variant> result;
  for (const auto& lhsElement : lhsArray) {
    for (const auto& rhsElement : rhsArray) {
      if (lhsElement.equals(rhsElement)) {
        result.push_back(lhsElement);
        break;
      }
    }
  }

  return registerVariant(velox::Variant::array(result));
}

// Returns the tighter lower bound (the higher of the two values).
inline VariantCP tightenLowerBound(VariantCP existing, VariantCP candidate) {
  if (existing == nullptr) {
    return candidate;
  }
  if (candidate == nullptr) {
    return existing;
  }
  return *existing < *candidate ? candidate : existing;
}

// Returns the tighter upper bound (the lower of the two values).
inline VariantCP tightenUpperBound(VariantCP existing, VariantCP candidate) {
  if (existing == nullptr) {
    return candidate;
  }
  if (candidate == nullptr) {
    return existing;
  }
  return *candidate < *existing ? candidate : existing;
}

// Finds the minimum and maximum values in an array of Variants.
// Returns {nullptr, nullptr} if the array is empty.
inline std::pair<VariantCP, VariantCP> findArrayMinMax(
    const std::vector<velox::Variant>& array) {
  if (array.empty()) {
    return {nullptr, nullptr};
  }

  VariantCP minVal = &array[0];
  VariantCP maxVal = &array[0];

  for (size_t i = 1; i < array.size(); ++i) {
    if (array[i] < *minVal) {
      minVal = &array[i];
    }
    if (*maxVal < array[i]) {
      maxVal = &array[i];
    }
  }

  return {minVal, maxVal};
}

// Returns true if the given TypeKind represents an integer type.
inline bool isIntegerKind(velox::TypeKind kind) {
  switch (kind) {
    case velox::TypeKind::TINYINT:
    case velox::TypeKind::SMALLINT:
    case velox::TypeKind::INTEGER:
    case velox::TypeKind::BIGINT:
    case velox::TypeKind::HUGEINT:
      return true;
    default:
      return false;
  }
}

// Checks if a value is present in an array variant.
inline bool isValueInArray(const velox::Variant& value, VariantCP array) {
  if (array == nullptr) {
    return false;
  }

  const auto& arrayElements = array->array();
  for (const auto& elem : arrayElements) {
    if (elem.equals(value)) {
      return true;
    }
  }
  return false;
}

// Prunes elements from an IN list based on lower and upper bounds.
// Returns a new registered variant with the filtered list.
inline VariantCP
pruneInList(VariantCP inList, VariantCP lower, VariantCP upper) {
  if (inList == nullptr) {
    return nullptr;
  }

  std::vector<velox::Variant> filteredList;
  const auto& array = inList->array();

  for (const auto& elem : array) {
    bool keep = true;
    if (upper != nullptr && *upper < elem) {
      keep = false;
    }
    if (lower != nullptr && elem < *lower) {
      keep = false;
    }
    if (keep) {
      filteredList.push_back(elem);
    }
  }

  if (filteredList.size() != array.size()) {
    return registerVariant(velox::Variant::array(filteredList));
  }
  return inList;
}

// Computes selectivity as ratio of intersection to total range.
// For discrete types (integers, VARCHAR), uses +1 to count distinct values.
// For continuous types (REAL, DOUBLE), uses the range difference.
// Returns selectivity clamped to [0, 1].
inline float computeRangeSelectivity(
    double exprMin,
    double exprMax,
    double effectiveLower,
    double effectiveUpper,
    bool discrete) {
  if (effectiveLower > effectiveUpper) {
    return 0.0f;
  }

  double offset = discrete ? 1.0 : 0.0;
  double intersectionRange = effectiveUpper - effectiveLower + offset;
  double exprRange = exprMax - exprMin + offset;

  if (exprRange < 1.0) {
    exprRange = 1.0;
  }

  return std::clamp<float>(intersectionRange / exprRange, 0.0f, 1.0f);
}

template <velox::TypeKind KIND>
float rangeSelectivityImpl(
    const Value& exprValue,
    VariantCP lower,
    VariantCP upper) {
  using T = typename velox::TypeTraits<KIND>::NativeType;

  // VARBINARY and TIMESTAMP are handled by the caller (rangeSelectivity).
  // This branch exists only because VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH
  // instantiates the template for all scalar types.
  if constexpr (
      KIND == velox::TypeKind::VARBINARY ||
      KIND == velox::TypeKind::TIMESTAMP) {
    VELOX_UNREACHABLE();
  } else {
    T exprMin = exprValue.min->value<KIND>();
    T exprMax = exprValue.max->value<KIND>();

    T effectiveLower = exprMin;
    if (lower != nullptr) {
      effectiveLower = std::max(exprMin, lower->value<KIND>());
    }

    T effectiveUpper = exprMax;
    if (upper != nullptr) {
      effectiveUpper = std::min(exprMax, upper->value<KIND>());
    }

    return computeRangeSelectivity(
        exprMin,
        exprMax,
        effectiveLower,
        effectiveUpper,
        /*discrete=*/!std::is_floating_point_v<T>);
  }
}

// Template specialization for VARCHAR.
template <>
inline float rangeSelectivityImpl<velox::TypeKind::VARCHAR>(
    const Value& exprValue,
    VariantCP lower,
    VariantCP upper) {
  // Returns the ASCII value of the first character, or 0 for empty or null
  // strings.
  auto getFirstCharValue = [](VariantCP var) -> int32_t {
    if (var == nullptr) {
      return 0;
    }
    const auto& str = var->value<velox::TypeKind::VARCHAR>();
    if (str.empty()) {
      return 0;
    }
    return static_cast<unsigned char>(str[0]);
  };

  int32_t exprMin = getFirstCharValue(exprValue.min);
  int32_t exprMax = getFirstCharValue(exprValue.max);

  int32_t effectiveLower = exprMin;
  if (lower != nullptr) {
    effectiveLower = std::max(exprMin, getFirstCharValue(lower));
  }

  int32_t effectiveUpper = exprMax;
  if (upper != nullptr) {
    effectiveUpper = std::min(exprMax, getFirstCharValue(upper));
  }

  return computeRangeSelectivity(
      exprMin, exprMax, effectiveLower, effectiveUpper, /*discrete=*/true);
}

// For integer types, adjusts a strict inequality bound to the nearest inclusive
// value. For example, > 2 becomes >= 3 (lower + 1), and < 22 becomes <= 21
// (upper - 1). Returns nullptr if the adjusted value would overflow.
template <velox::TypeKind KIND>
VariantCP adjustStrictBound(VariantCP bound, bool isLower) {
  using T = typename velox::TypeTraits<KIND>::NativeType;
  auto value = bound->value<KIND>();
  if (isLower) {
    if (value == std::numeric_limits<T>::max()) {
      return nullptr;
    }
    return registerVariant(velox::Variant::create<T>(value + 1));
  }
  if (value == std::numeric_limits<T>::min()) {
    return nullptr;
  }
  return registerVariant(velox::Variant::create<T>(value - 1));
}

// Adjusts a strict inequality bound for integer types. For non-integer types,
// returns the bound unchanged.
inline VariantCP
adjustStrictIntegerBound(VariantCP bound, velox::TypeKind kind, bool isLower) {
  switch (kind) {
    case velox::TypeKind::TINYINT:
      return adjustStrictBound<velox::TypeKind::TINYINT>(bound, isLower);
    case velox::TypeKind::SMALLINT:
      return adjustStrictBound<velox::TypeKind::SMALLINT>(bound, isLower);
    case velox::TypeKind::INTEGER:
      return adjustStrictBound<velox::TypeKind::INTEGER>(bound, isLower);
    case velox::TypeKind::BIGINT:
      return adjustStrictBound<velox::TypeKind::BIGINT>(bound, isLower);
    case velox::TypeKind::HUGEINT:
      return adjustStrictBound<velox::TypeKind::HUGEINT>(bound, isLower);
    default:
      return bound;
  }
}

// Holds accumulated constraints from processing range expressions.
struct RangeConstraints {
  VariantCP lower = nullptr;
  VariantCP upper = nullptr;
  std::optional<double> eqSelectivity;
  VariantCP eqValue = nullptr;
  VariantCP inList = nullptr;
  bool empty = false;
};

// Holds effective bounds after combining query constraints with column stats.
struct EffectiveBounds {
  VariantCP lower{nullptr};
  VariantCP upper{nullptr};
  VariantCP inList{nullptr};
};

// Computes effective bounds by combining query constraints with column
// min/max.
inline EffectiveBounds computeEffectiveBounds(
    const Value& exprValue,
    const RangeConstraints& rangeConstraints) {
  VariantCP effectiveLower = rangeConstraints.lower;
  VariantCP effectiveUpper = rangeConstraints.upper;

  if (exprValue.min != nullptr) {
    if (effectiveLower == nullptr || *effectiveLower < *exprValue.min) {
      effectiveLower = exprValue.min;
    }
  }

  if (exprValue.max != nullptr) {
    if (effectiveUpper == nullptr || *exprValue.max < *effectiveUpper) {
      effectiveUpper = exprValue.max;
    }
  }

  VariantCP prunedInList = rangeConstraints.inList;
  if (prunedInList != nullptr) {
    prunedInList = pruneInList(prunedInList, effectiveLower, effectiveUpper);
  }

  return {effectiveLower, effectiveUpper, prunedInList};
}

// Detects contradictory constraints between bounds, equality, and IN list.
inline bool hasContradiction(
    const RangeConstraints& rangeConstraints,
    const EffectiveBounds& bounds) {
  if (rangeConstraints.empty) {
    return true;
  }

  if (bounds.inList != nullptr && bounds.inList->array().empty()) {
    return true;
  }

  if (bounds.inList != nullptr && rangeConstraints.eqValue != nullptr) {
    if (!isValueInArray(*rangeConstraints.eqValue, bounds.inList)) {
      return true;
    }
  }

  if (rangeConstraints.eqValue != nullptr && bounds.lower != nullptr) {
    if (*rangeConstraints.eqValue < *bounds.lower) {
      return true;
    }
  }

  if (rangeConstraints.eqValue != nullptr && bounds.upper != nullptr) {
    if (*bounds.upper < *rangeConstraints.eqValue) {
      return true;
    }
  }

  if (bounds.lower != nullptr && bounds.upper != nullptr) {
    if (*bounds.upper < *bounds.lower) {
      return true;
    }
  }

  return false;
}

// Selectivity of a column value falling within [lower, upper] (either bound
// nullptr for unbounded), given the column's statistics. Single-column
// primitive shared by the engine's Expr path and the connector's common::Filter
// path.
inline std::optional<Selectivity>
rangeSelectivity(const Value& exprValue, VariantCP lower, VariantCP upper) {
  const auto kind = exprValue.type->kind();
  double nullFraction = exprValue.nullFraction.value_or(0);

  switch (kind) {
    case velox::TypeKind::ARRAY:
    case velox::TypeKind::MAP:
    case velox::TypeKind::ROW:
    case velox::TypeKind::UNKNOWN:
    case velox::TypeKind::FUNCTION:
    case velox::TypeKind::OPAQUE:
    case velox::TypeKind::INVALID:
    case velox::TypeKind::VARBINARY:
    case velox::TypeKind::TIMESTAMP:
      return Selectivity::noRange(nullFraction);
    default:
      break;
  }

  if (!exprValue.min || !exprValue.max) {
    return Selectivity::noRange(nullFraction);
  }

  float baseTrueFraction = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      rangeSelectivityImpl, kind, exprValue, lower, upper);

  return Selectivity{baseTrueFraction * (1.0 - nullFraction), nullFraction};
}

// Selectivity of an IN list of 'listSize' distinct values against a column.
// Single-column primitive shared by both paths.
inline std::optional<Selectivity> inListSelectivity(
    const Value& exprValue,
    double listSize) {
  double nullFraction = exprValue.nullFraction.value_or(0);
  if (!exprValue.cardinality.has_value()) {
    return Selectivity::unknown(nullFraction);
  }
  double trueFraction =
      std::clamp(listSize / *exprValue.cardinality, 0.0, 1.0) *
      (1.0 - nullFraction);
  return Selectivity{trueFraction, nullFraction};
}

// Refined column Value after intersecting its range with [lower, upper] and
// setting the distinct count to 'cardinality'. Rows passing the filter have no
// nulls. Shared by both paths.
inline Value refineRange(
    const Value& oldValue,
    VariantCP lower,
    VariantCP upper,
    float cardinality) {
  VariantCP minPtr = tightenLowerBound(oldValue.min, lower);
  VariantCP maxPtr = tightenUpperBound(oldValue.max, upper);

  float finalCardinality = std::max(1.0f, cardinality);
  if (const auto rangeCard = rangeCardinality(oldValue.type, minPtr, maxPtr)) {
    finalCardinality = std::min(finalCardinality, *rangeCard);
  }

  Value result(oldValue.type, finalCardinality);
  result.min = minPtr;
  result.max = maxPtr;
  result.nullFraction = 0;
  result.nullable = false;
  return result;
}

} // namespace detail

template <SelectivityPolicy Policy>
std::optional<Selectivity> SelectivityEngine<Policy>::conjunctsSelectivity(
    std::span<const Expr> conjuncts,
    bool updateConstraints) {
  // Derive constraints for all expressions in the conjuncts. Only meaningful
  // for a Policy that maintains a constraint map; the estimator reads
  // statistics directly in valueOf().
  if constexpr (Policy::kUpdatesConstraints) {
    for (const auto& conjunct : conjuncts) {
      policy_.seed(conjunct);
    }
  }

  std::vector<std::optional<Selectivity>> selectivities;
  selectivities.reserve(conjuncts.size());

  // Map from left-hand side column to list of comparison expressions.
  folly::F14FastMap<typename Policy::ColumnKey, std::vector<Expr>>
      rangeConditions;
  std::vector<Expr> otherConditions;
  const auto& functionNames = queryCtx()->functionNames();

  for (const auto& arg : conjuncts) {
    if (policy_.isCall(arg)) {
      auto argFuncName = policy_.name(arg);
      auto argArgs = policy_.args(arg);

      if (detail::isComparisonOperator(argFuncName) &&
          policy_.isLiteral(argArgs[1])) {
        rangeConditions[policy_.columnKey(argArgs[0])].push_back(arg);
      } else if (
          argFuncName == functionNames.between && argArgs.size() == 3 &&
          policy_.isLiteral(argArgs[1]) && policy_.isLiteral(argArgs[2])) {
        rangeConditions[policy_.columnKey(argArgs[0])].push_back(arg);
      } else if (argFuncName == SpecialFormCallNames::kIn) {
        rangeConditions[policy_.columnKey(argArgs[0])].push_back(arg);
      } else {
        otherConditions.push_back(arg);
      }
    } else {
      otherConditions.push_back(arg);
    }
  }

  for (const auto& [leftSide, conditions] : rangeConditions) {
    selectivities.push_back(rangeSelectivity(conditions, updateConstraints));
  }

  for (const auto& arg : otherConditions) {
    selectivities.push_back(exprSelectivity(arg, updateConstraints));
  }

  return combineConjuncts(selectivities);
}

template <SelectivityPolicy Policy>
std::optional<Selectivity> SelectivityEngine<Policy>::exprSelectivity(
    Expr expr,
    bool updateConstraints) {
  if (policy_.isCall(expr)) {
    return callSelectivity(expr, updateConstraints);
  }
  if (policy_.isColumn(expr)) {
    return columnSelectivity(expr);
  }
  if (policy_.isLiteral(expr)) {
    return literalSelectivity(expr);
  }
  return Selectivity::likelyTrue();
}

template <SelectivityPolicy Policy>
std::optional<Selectivity>
SelectivityEngine<Policy>::columnComparisonSelectivity(
    Expr left,
    Expr right,
    const Value& leftValue,
    const Value& rightValue,
    Name funcName,
    bool updateConstraints) {
  bool canUseRange =
      (leftValue.min && leftValue.max && rightValue.min && rightValue.max);

  if (!canUseRange) {
    return cardinalityBasedSelectivity(
        left, right, leftValue, rightValue, funcName, updateConstraints);
  }

  switch (leftValue.type->kind()) {
#define NUMERIC_CASE(KIND)                                   \
  case velox::TypeKind::KIND:                                \
    return comparisonSelectivityImpl<velox::TypeKind::KIND>( \
        left, right, leftValue, rightValue, funcName, updateConstraints);

    NUMERIC_CASE(TINYINT)
    NUMERIC_CASE(SMALLINT)
    NUMERIC_CASE(INTEGER)
    NUMERIC_CASE(BIGINT)
    NUMERIC_CASE(HUGEINT)
    NUMERIC_CASE(REAL)
    NUMERIC_CASE(DOUBLE)
#undef NUMERIC_CASE

    default:
      return cardinalityBasedSelectivity(
          left, right, leftValue, rightValue, funcName, updateConstraints);
  }
}

template <SelectivityPolicy Policy>
Selectivity SelectivityEngine<Policy>::literalSelectivity(Expr expr) {
  const auto& literalValue = policy_.literal(expr);

  if (literalValue.isNull()) {
    return {0.0, 1.0};
  }

  if (policy_.valueOf(expr).type->isBoolean()) {
    if (literalValue.kind() == velox::TypeKind::BOOLEAN &&
        !literalValue.template value<bool>()) {
      return {0.0, 0.0};
    }
  }

  return {1.0, 0.0};
}

template <SelectivityPolicy Policy>
std::optional<Selectivity> SelectivityEngine<Policy>::columnSelectivity(
    Expr expr) {
  const Value exprValue = policy_.valueOf(expr);
  const float nullFraction = exprValue.nullFraction.value_or(0);
  if (exprValue.type->isBoolean()) {
    return {
        {exprValue.trueFraction.value_or(Selectivity::kUnknown), nullFraction}};
  }
  return {{1.0 - nullFraction, nullFraction}};
}

template <SelectivityPolicy Policy>
std::optional<Selectivity> SelectivityEngine<Policy>::callSelectivity(
    Expr expr,
    bool updateConstraints) {
  auto funcName = policy_.name(expr);
  auto args = policy_.args(expr);
  const auto& fn = queryCtx()->functionNames();

  // NOT: null fraction stays the same, true becomes false and vice versa.
  if (funcName == fn.negation) {
    VELOX_CHECK_EQ(args.size(), 1, "NOT must have exactly 1 argument");
    auto innerSel = exprSelectivity(args[0], false);
    if (!innerSel.has_value()) {
      return std::nullopt;
    }
    return Selectivity{innerSel->falseFraction(), innerSel->nullFraction};
  }

  if (funcName == SpecialFormCallNames::kAnd) {
    return conjunctsSelectivity(args, updateConstraints);
  }

  if (funcName == SpecialFormCallNames::kOr) {
    // An assertion, such as `x > 0 OR fail('...') IS NULL`: a row the other
    // disjuncts leave false or null fails the query, so the filter keeps every
    // row and the other disjuncts' OR holds on all of them. A single one left
    // narrows the constraints as a filter on it would.
    std::vector<Expr> others;
    for (const auto& arg : args) {
      if (!alwaysFails(arg)) {
        others.push_back(arg);
      }
    }
    if (others.size() < args.size()) {
      if (updateConstraints && others.size() == 1) {
        exprSelectivity(others.front(), true);
      }
      return Selectivity{1.0, 0.0};
    }
    std::vector<std::optional<Selectivity>> disjuncts;
    disjuncts.reserve(args.size());
    for (const auto& arg : args) {
      disjuncts.push_back(exprSelectivity(arg, false));
    }
    return combineDisjuncts(disjuncts);
  }

  // ISNULL: trueFraction = argument's nullFraction, nullFraction = 0.
  if (funcName == fn.isNull) {
    VELOX_CHECK_EQ(args.size(), 1, "isnull must have exactly 1 argument");
    return Selectivity{policy_.valueOf(args[0]).nullFraction.value_or(0), 0.0};
  }

  if (funcName == SpecialFormCallNames::kIn) {
    std::array<Expr, 1> singleExpr = {expr};
    return rangeSelectivity(singleExpr, updateConstraints);
  }

  if (funcName == fn.between) {
    VELOX_CHECK_EQ(args.size(), 3, "BETWEEN must have exactly 3 arguments");
    if (policy_.isLiteral(args[1]) && policy_.isLiteral(args[2])) {
      std::array<Expr, 1> singleExpr = {expr};
      return rangeSelectivity(singleExpr, updateConstraints);
    }
  }

  if (detail::isComparisonOperator(funcName)) {
    // Column-vs-literal comparison: reuse the range machinery.
    if (args.size() >= 2 && policy_.isLiteral(args[1])) {
      std::array<Expr, 1> singleExpr = {expr};
      return rangeSelectivity(singleExpr, updateConstraints);
    }
    // Column-vs-column comparison (e.g. a = b).
    return comparisonSelectivity(expr, updateConstraints);
  }

  // Other (unmodelable) function - heuristic default, stats not the issue.
  return Selectivity::likelyTrue();
}

template <SelectivityPolicy Policy>
bool SelectivityEngine<Policy>::alwaysFails(Expr expr) const {
  if (!policy_.isCall(expr)) {
    return false;
  }
  const auto name = policy_.name(expr);
  const auto& fail = queryCtx()->functionNames().fail;
  if (fail != nullptr && name == fail) {
    return true;
  }
  // A function evaluates its first argument on every row it reads, and so
  // does CAST. Other special forms may not: TRY catches the failure, AND and OR
  // may decide on another argument first.
  const auto args = policy_.args(expr);
  return !args.empty() &&
      (!SpecialFormCallNames::isSpecialForm(name) ||
       name == SpecialFormCallNames::kCast) &&
      alwaysFails(args[0]);
}

template <SelectivityPolicy Policy>
VariantCP SelectivityEngine<Policy>::getInListFromCall(Expr expr) {
  auto args = policy_.args(expr);
  VELOX_CHECK_GT(args.size(), 1, "IN must have at least 2 arguments");

  // Constant-array encoding: the single argument after the column is an array
  // literal whose elements are the IN list.
  if (args.size() == 2 && policy_.isLiteral(args[1])) {
    const auto& litValue = policy_.literal(args[1]);
    if (!litValue.isNull() && litValue.kind() == velox::TypeKind::ARRAY) {
      return registerVariant(litValue);
    }
  }

  // Variadic encoding.
  std::vector<velox::Variant> inListValues;
  inListValues.reserve(args.size() - 1);

  for (size_t i = 1; i < args.size(); ++i) {
    if (!policy_.isLiteral(args[i])) {
      return nullptr;
    }
    inListValues.push_back(policy_.literal(args[i]));
  }

  return registerVariant(velox::Variant::array(inListValues));
}

template <SelectivityPolicy Policy>
std::optional<Selectivity> SelectivityEngine<Policy>::processInClause(
    Expr expr,
    detail::RangeConstraints& rangeConstraints,
    double nullFraction) {
  VariantCP currentInList = getInListFromCall(expr);

  if (currentInList == nullptr) {
    return Selectivity::unknown(nullFraction);
  }

  for (const auto& elem : currentInList->array()) {
    if (elem.isNull()) {
      return Selectivity::zero(nullFraction);
    }
  }

  if (rangeConstraints.inList == nullptr) {
    rangeConstraints.inList = currentInList;
  } else {
    rangeConstraints.inList =
        detail::intersectArrayVariants(rangeConstraints.inList, currentInList);
  }

  return std::nullopt;
}

template <SelectivityPolicy Policy>
std::optional<Selectivity> SelectivityEngine<Policy>::processEqualityClause(
    Expr expr,
    const Value& exprValue,
    detail::RangeConstraints& rangeConstraints,
    double nullFraction) {
  auto args = policy_.args(expr);
  VELOX_CHECK_EQ(args.size(), 2, "eq must have exactly 2 arguments");

  if (!policy_.isLiteral(args[1])) {
    return std::nullopt;
  }

  const auto& litValue = policy_.literal(args[1]);

  if (litValue.isNull()) {
    return Selectivity::zero(nullFraction);
  }

  if (!rangeConstraints.eqSelectivity.has_value()) {
    rangeConstraints.eqSelectivity =
        *exprValue.cardinality > 0 ? 1.0 / *exprValue.cardinality : 1.0;
    rangeConstraints.eqValue = &litValue;
  } else {
    if (rangeConstraints.eqValue != nullptr &&
        !(rangeConstraints.eqValue->equals(litValue))) {
      rangeConstraints.empty = true;
    }
  }

  return std::nullopt;
}

template <SelectivityPolicy Policy>
std::optional<Selectivity> SelectivityEngine<Policy>::processRangeBound(
    Expr expr,
    Name funcName,
    detail::RangeConstraints& rangeConstraints,
    double nullFraction) {
  const auto& fn = queryCtx()->functionNames();
  auto args = policy_.args(expr);
  VELOX_CHECK_EQ(args.size(), 2, "Comparison must have exactly 2 arguments");

  if (!policy_.isLiteral(args[1])) {
    return std::nullopt;
  }

  const auto& litValue = policy_.literal(args[1]);

  if (litValue.isNull()) {
    return Selectivity::zero(nullFraction);
  }

  auto kind = policy_.valueOf(args[0]).type->kind();

  if (funcName == fn.lt || funcName == fn.lte) {
    VariantCP bound = &litValue;
    if (funcName == fn.lt && detail::isIntegerKind(kind)) {
      bound = detail::adjustStrictIntegerBound(bound, kind, /*isLower=*/false);
    }
    if (bound != nullptr &&
        (rangeConstraints.upper == nullptr ||
         *bound < *rangeConstraints.upper)) {
      rangeConstraints.upper = bound;
    }
  } else if (funcName == fn.gt || funcName == fn.gte) {
    VariantCP bound = &litValue;
    if (funcName == fn.gt && detail::isIntegerKind(kind)) {
      bound = detail::adjustStrictIntegerBound(bound, kind, /*isLower=*/true);
    }
    if (bound != nullptr &&
        (rangeConstraints.lower == nullptr ||
         *rangeConstraints.lower < *bound)) {
      rangeConstraints.lower = bound;
    }
  }

  return std::nullopt;
}

template <SelectivityPolicy Policy>
std::optional<Selectivity> SelectivityEngine<Policy>::processBetweenClause(
    Expr expr,
    detail::RangeConstraints& rangeConstraints,
    double nullFraction) {
  auto args = policy_.args(expr);
  VELOX_CHECK_EQ(args.size(), 3, "BETWEEN must have exactly 3 arguments");
  VELOX_DCHECK(policy_.isLiteral(args[1]));
  VELOX_DCHECK(policy_.isLiteral(args[2]));

  const auto& lower = policy_.literal(args[1]);
  const auto& upper = policy_.literal(args[2]);
  if (lower.isNull() || upper.isNull()) {
    return Selectivity::zero(nullFraction);
  }

  if (rangeConstraints.lower == nullptr || *rangeConstraints.lower < lower) {
    rangeConstraints.lower = &lower;
  }
  if (rangeConstraints.upper == nullptr || upper < *rangeConstraints.upper) {
    rangeConstraints.upper = &upper;
  }

  return std::nullopt;
}

template <SelectivityPolicy Policy>
Value SelectivityEngine<Policy>::makeConstraint(
    Expr expr,
    VariantCP lower,
    VariantCP upper,
    float cardinality) {
  const Value oldValue = policy_.valueOf(expr);

  VariantCP minPtr = detail::tightenLowerBound(oldValue.min, lower);
  VariantCP maxPtr = detail::tightenUpperBound(oldValue.max, upper);

  float finalCardinality = std::max(1.0f, cardinality);
  if (const auto rangeCard = rangeCardinality(oldValue.type, minPtr, maxPtr)) {
    finalCardinality = std::min(finalCardinality, *rangeCard);
  }

  Value result(oldValue.type, finalCardinality);
  result.min = minPtr;
  result.max = maxPtr;
  result.nullFraction = 0;
  result.nullable = false;
  return result;
}

template <SelectivityPolicy Policy>
std::optional<Selectivity> SelectivityEngine<Policy>::rangeSelectivity(
    Expr expr,
    VariantCP lower,
    VariantCP upper) {
  const Value exprValue = policy_.valueOf(expr);
  const auto kind = exprValue.type->kind();

  double nullFraction = exprValue.nullFraction.value_or(0);

  switch (kind) {
    case velox::TypeKind::ARRAY:
    case velox::TypeKind::MAP:
    case velox::TypeKind::ROW:
    case velox::TypeKind::UNKNOWN:
    case velox::TypeKind::FUNCTION:
    case velox::TypeKind::OPAQUE:
    case velox::TypeKind::INVALID:
    case velox::TypeKind::VARBINARY:
    case velox::TypeKind::TIMESTAMP:
      return Selectivity::noRange(nullFraction);
    default:
      break;
  }

  if (!exprValue.min || !exprValue.max) {
    return Selectivity::noRange(nullFraction);
  }

  float baseTrueFraction = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      detail::rangeSelectivityImpl, kind, exprValue, lower, upper);

  double trueFraction = baseTrueFraction * (1.0 - nullFraction);

  return Selectivity{trueFraction, nullFraction};
}

template <SelectivityPolicy Policy>
std::optional<Selectivity>
SelectivityEngine<Policy>::computeEqualitySelectivity(
    Expr leftSide,
    const detail::RangeConstraints& rangeConstraints,
    double nullFraction,
    bool updateConstraints) {
  if constexpr (Policy::kUpdatesConstraints) {
    if (updateConstraints) {
      policy_.refine(
          leftSide,
          makeConstraint(
              leftSide,
              rangeConstraints.eqValue,
              rangeConstraints.eqValue,
              1.0f));
    }
  }
  return Selectivity{
      rangeConstraints.eqSelectivity.value() * (1.0 - nullFraction),
      nullFraction};
}

template <SelectivityPolicy Policy>
std::optional<Selectivity> SelectivityEngine<Policy>::computeInListSelectivity(
    Expr leftSide,
    const detail::EffectiveBounds& bounds,
    const Value& exprValue,
    double nullFraction,
    bool updateConstraints) {
  const auto& array = bounds.inList->array();

  double inListSize = array.size();
  double trueFraction =
      std::clamp(inListSize / *exprValue.cardinality, 0.0, 1.0) *
      (1.0 - nullFraction);

  if constexpr (Policy::kUpdatesConstraints) {
    if (updateConstraints) {
      auto [minVal, maxVal] = detail::findArrayMinMax(array);
      policy_.refine(
          leftSide, makeConstraint(leftSide, minVal, maxVal, inListSize));
    }
  }

  return Selectivity{trueFraction, nullFraction};
}

template <SelectivityPolicy Policy>
std::optional<Selectivity> SelectivityEngine<Policy>::computeBoundsSelectivity(
    Expr leftSide,
    const detail::RangeConstraints& rangeConstraints,
    const Value& exprValue,
    double nullFraction,
    bool updateConstraints) {
  auto selectivity = rangeSelectivity(
      leftSide, rangeConstraints.lower, rangeConstraints.upper);
  if (!selectivity.has_value()) {
    return std::nullopt;
  }

  // Use baseTrueFraction (before null scaling) for cardinality estimation.
  double baseTrueFraction = (nullFraction < 1.0)
      ? selectivity->trueFraction / (1.0 - nullFraction)
      : selectivity->trueFraction;

  if constexpr (Policy::kUpdatesConstraints) {
    if (updateConstraints) {
      policy_.refine(
          leftSide,
          makeConstraint(
              leftSide,
              rangeConstraints.lower,
              rangeConstraints.upper,
              *exprValue.cardinality * baseTrueFraction));
    }
  }

  return selectivity;
}

template <SelectivityPolicy Policy>
std::optional<Selectivity> SelectivityEngine<Policy>::rangeSelectivity(
    std::span<const Expr> exprs,
    bool updateConstraints) {
  VELOX_CHECK_GE(exprs.size(), 1, "exprs must have at least one element");
  VELOX_CHECK(policy_.isCall(exprs[0]), "All elements must be Call");

  auto firstArgs = policy_.args(exprs[0]);
  VELOX_CHECK_GE(firstArgs.size(), 1, "Call must have at least one argument");
  Expr leftSide = firstArgs[0];

  const Value exprValue = policy_.valueOf(leftSide);
  if (!exprValue.cardinality.has_value()) {
    return std::nullopt;
  }
  double nullFraction = exprValue.nullFraction.value_or(0);

  detail::RangeConstraints rangeConstraints;
  const auto& fn = queryCtx()->functionNames();

  for (const auto& expr : exprs) {
    VELOX_CHECK(policy_.isCall(expr), "All elements must be calls");
    auto funcName = policy_.name(expr);

    std::optional<Selectivity> earlyReturn;

    if (funcName == SpecialFormCallNames::kIn) {
      earlyReturn = processInClause(expr, rangeConstraints, nullFraction);
    } else if (funcName == fn.equality) {
      earlyReturn = processEqualityClause(
          expr, exprValue, rangeConstraints, nullFraction);
    } else if (detail::isRangeBoundOperator(funcName)) {
      earlyReturn =
          processRangeBound(expr, funcName, rangeConstraints, nullFraction);
    } else if (funcName == fn.between) {
      earlyReturn = processBetweenClause(expr, rangeConstraints, nullFraction);
    }

    if (earlyReturn.has_value()) {
      return earlyReturn.value();
    }
  }

  auto bounds = detail::computeEffectiveBounds(exprValue, rangeConstraints);

  if (detail::hasContradiction(rangeConstraints, bounds)) {
    if constexpr (Policy::kUpdatesConstraints) {
      if (updateConstraints) {
        Value emptyConstraint(exprValue.type, 0);
        emptyConstraint.nullFraction = nullFraction;
        policy_.refine(leftSide, emptyConstraint);
      }
    }
    return Selectivity::likelyZero(nullFraction);
  }

  if (rangeConstraints.eqValue != nullptr) {
    return computeEqualitySelectivity(
        leftSide, rangeConstraints, nullFraction, updateConstraints);
  }

  if (bounds.inList != nullptr) {
    return computeInListSelectivity(
        leftSide, bounds, exprValue, nullFraction, updateConstraints);
  }

  if (rangeConstraints.lower != nullptr || rangeConstraints.upper != nullptr) {
    return computeBoundsSelectivity(
        leftSide, rangeConstraints, exprValue, nullFraction, updateConstraints);
  }

  return Selectivity::unknown(nullFraction);
}

template <SelectivityPolicy Policy>
std::optional<Selectivity> SelectivityEngine<Policy>::comparisonSelectivity(
    Expr expr,
    bool updateConstraints) {
  auto args = policy_.args(expr);
  auto funcName = policy_.name(expr);
  VELOX_DCHECK_EQ(
      args.size(), 2, "Comparison operators must have exactly 2 arguments");

  Expr leftExpr = args[0];
  Expr rightExpr = args[1];
  const Value leftValue = policy_.valueOf(leftExpr);
  const Value rightValue = policy_.valueOf(rightExpr);

  if (leftValue.min && leftValue.max && rightValue.min && rightValue.max) {
    return columnComparisonSelectivity(
        leftExpr,
        rightExpr,
        leftValue,
        rightValue,
        funcName,
        updateConstraints);
  }

  return cardinalityBasedSelectivity(
      leftExpr, rightExpr, leftValue, rightValue, funcName, updateConstraints);
}

template <SelectivityPolicy Policy>
std::optional<Selectivity>
SelectivityEngine<Policy>::cardinalityBasedSelectivity(
    [[maybe_unused]] Expr left,
    [[maybe_unused]] Expr right,
    const Value& leftValue,
    const Value& rightValue,
    Name funcName,
    [[maybe_unused]] bool updateConstraints) {
  double nullFraction = detail::combinedNullFraction(
      leftValue.nullFraction.value_or(0), rightValue.nullFraction.value_or(0));

  const auto& fn = queryCtx()->functionNames();

  if (funcName == fn.equality) {
    if (!leftValue.cardinality.has_value() ||
        !rightValue.cardinality.has_value()) {
      return std::nullopt;
    }
    double leftNdv = std::max<double>(1.0, *leftValue.cardinality);
    double rightNdv = std::max<double>(1.0, *rightValue.cardinality);
    double matchingNdv = std::min(leftNdv, rightNdv);
    double probTrue = matchingNdv / (leftNdv * rightNdv);

    if constexpr (Policy::kUpdatesConstraints) {
      if (updateConstraints) {
        Value newConstraint(leftValue.type, matchingNdv);
        newConstraint.nullFraction = 0.0f;
        policy_.refine(left, newConstraint);
        policy_.refine(right, newConstraint);
      }
    }

    double trueFraction = probTrue * (1.0 - nullFraction);
    return Selectivity{trueFraction, nullFraction};
  }

  if (detail::isComparisonOperator(funcName)) {
    return Selectivity::unknown(nullFraction);
  }

  return Selectivity::likelyZero(nullFraction);
}

template <SelectivityPolicy Policy>
template <velox::TypeKind KIND>
std::optional<Selectivity> SelectivityEngine<Policy>::comparisonSelectivityImpl(
    [[maybe_unused]] Expr left,
    [[maybe_unused]] Expr right,
    const Value& leftValue,
    const Value& rightValue,
    Name funcName,
    [[maybe_unused]] bool updateConstraints) {
  if (!leftValue.cardinality.has_value() ||
      !rightValue.cardinality.has_value()) {
    return std::nullopt;
  }
  double nullFraction = detail::combinedNullFraction(
      leftValue.nullFraction.value_or(0), rightValue.nullFraction.value_or(0));

  double leftMin = leftValue.min->value<KIND>();
  double leftMax = leftValue.max->value<KIND>();
  double rightMin = rightValue.min->value<KIND>();
  double rightMax = rightValue.max->value<KIND>();

  double leftNdv = std::max<double>(1.0, *leftValue.cardinality);
  double rightNdv = std::max<double>(1.0, *rightValue.cardinality);

  double leftRange = detail::rangeSize(leftMin, leftMax);
  double rightRange = detail::rangeSize(rightMin, rightMax);

  auto overlap =
      detail::RangeOverlap::compute(leftMin, leftMax, rightMin, rightMax);

  const auto& fn = queryCtx()->functionNames();
  double probTrue = 0.0;

  if (funcName == fn.equality) {
    if (overlap.size > 0) {
      double leftMatches = leftNdv * (overlap.size / leftRange);
      double rightMatches = rightNdv * (overlap.size / rightRange);
      double matchingNdv = std::min(leftMatches, rightMatches);
      probTrue = matchingNdv / (leftNdv * rightNdv);

      if constexpr (Policy::kUpdatesConstraints) {
        if (updateConstraints) {
          Value newConstraint(leftValue.type, matchingNdv);
          newConstraint.min =
              detail::variantMax<KIND>(leftValue.min, rightValue.min);
          newConstraint.max =
              detail::variantMin<KIND>(leftValue.max, rightValue.max);
          newConstraint.nullFraction = 0.0f;
          policy_.refine(left, newConstraint);
          policy_.refine(right, newConstraint);
        }
      }
    }
  } else if (funcName == fn.lt || funcName == fn.lte) {
    if (leftMax < rightMin) {
      probTrue = 1.0;
    } else if (leftMin > rightMax) {
      probTrue = 0.0;
    } else {
      double belowRight = std::max(0.0, std::min(leftMax, rightMin) - leftMin);
      double overlapIntegral = 0.0;
      if (overlap.size > 0) {
        overlapIntegral = overlap.size *
            (2.0 * rightMax - overlap.high - overlap.low) / (2.0 * rightRange);
      }
      probTrue = (belowRight + overlapIntegral) / leftRange;
    }
  } else if (funcName == fn.gt || funcName == fn.gte) {
    if (leftMin > rightMax) {
      probTrue = 1.0;
    } else if (leftMax < rightMin) {
      probTrue = 0.0;
    } else {
      double belowLeft = std::max(0.0, std::min(rightMax, leftMin) - rightMin);
      double overlapIntegral = 0.0;
      if (overlap.size > 0) {
        overlapIntegral = overlap.size *
            (2.0 * leftMax - overlap.high - overlap.low) / (2.0 * leftRange);
      }
      probTrue = (belowLeft + overlapIntegral) / rightRange;
    }
  }

  probTrue = std::clamp(probTrue, 0.0, 1.0);
  double trueFraction = probTrue * (1.0 - nullFraction);

  return Selectivity{trueFraction, nullFraction};
}

} // namespace facebook::axiom::optimizer
