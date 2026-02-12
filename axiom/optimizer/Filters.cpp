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

#include <algorithm>

#include "axiom/optimizer/Filters.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/QueryGraphContext.h"

namespace facebook::axiom::optimizer {

void Selectivity::checkConsistency() const {
  [[maybe_unused]] static constexpr double kTolerance = 1e-9;

  VELOX_DCHECK_GE(trueFraction, -kTolerance, "trueFraction must be >= 0");
  VELOX_DCHECK_LE(trueFraction, 1.0 + kTolerance, "trueFraction must be <= 1");

  VELOX_DCHECK_GE(nullFraction, -kTolerance, "nullFraction must be >= 0");
  VELOX_DCHECK_LE(nullFraction, 1.0 + kTolerance, "nullFraction must be <= 1");

  VELOX_DCHECK_LE(
      trueFraction + nullFraction,
      1.0 + kTolerance,
      "trueFraction + nullFraction must be <= 1");
}

namespace {

// Returns true if the function name is a range bound operator (lt, lte, gt,
// gte). Excludes equality which is handled separately in some contexts.
bool isRangeBoundOperator(Name funcName) {
  const auto& fn = queryCtx()->functionNames();
  return funcName == fn.lt || funcName == fn.lte || funcName == fn.gt ||
      funcName == fn.gte;
}

// Returns true if the function name is a comparison operator (eq, lt, lte, gt,
// gte).
bool isComparisonOperator(Name funcName) {
  return funcName == queryCtx()->functionNames().equality ||
      isRangeBoundOperator(funcName);
}

// Computes P(A OR B) = P(A) + P(B) - P(A) * P(B) for two null fractions.
// Used to calculate the probability that at least one of two values is null.
double combinedNullFraction(double leftNullFraction, double rightNullFraction) {
  return leftNullFraction + rightNullFraction -
      (leftNullFraction * rightNullFraction);
}

// Computes the size of a range, returning at least 1.0 to avoid division by
// zero.
double rangeSize(double low, double high) {
  return std::max(1.0, high - low);
}

// Holds pre-computed overlap information for two ranges.
struct RangeOverlap {
  double low;
  double high;
  double size;

  // Computes the overlap of two ranges [al, ah] and [bl, bh].
  static RangeOverlap compute(double al, double ah, double bl, double bh) {
    double low = std::max(al, bl);
    double high = std::min(ah, bh);
    double size = std::max(0.0, high - low);
    return {low, high, size};
  }
};

// Forward declarations for internal functions.

Selectivity rangeSelectivity(
    ConstraintMap& constraints,
    std::span<const ExprCP> exprs,
    bool updateConstraints);

Selectivity cardinalityBasedSelectivity(
    ExprCP left,
    ExprCP right,
    const Value& leftValue,
    const Value& rightValue,
    Name funcName,
    bool updateConstraints,
    ConstraintMap& constraints);

} // namespace

Selectivity combineConjuncts(std::span<const Selectivity> selectivities) {
  if (selectivities.empty()) {
    return {1.0, 0.0};
  }

  // For AND: result is TRUE only if all are TRUE.
  // Result is NULL if any is NULL and none is FALSE.
  // Result is FALSE otherwise.
  //
  // P(TRUE) = product of all trueFractions
  // P(NULL) = (p1 + n1)(p2 + n2)...(pN + nN) - P(TRUE)
  //         = product of (trueFraction + nullFraction) - P(TRUE)
  // P(FALSE) = 1 - P(TRUE) - P(NULL)

  double trueProduct = 1.0;
  double trueOrNullProduct = 1.0;

  for (const auto& selectivity : selectivities) {
    trueProduct *= selectivity.trueFraction;
    trueOrNullProduct *= (selectivity.trueFraction + selectivity.nullFraction);
  }

  // Clamp to handle floating-point rounding.
  double resultNull = std::max(0.0, trueOrNullProduct - trueProduct);
  Selectivity result{trueProduct, resultNull};
  result.checkConsistency();
  return result;
}

Selectivity combineDisjuncts(std::span<const Selectivity> selectivities) {
  if (selectivities.empty()) {
    return {0.0, 0.0};
  }

  // For OR: result is TRUE if any is TRUE.
  // Result is NULL if any is NULL and none is TRUE.
  // Result is FALSE only if all are FALSE.
  //
  // P(TRUE) = 1 - product of all (1 - trueFraction)
  // P(FALSE) = product of all falseFractions
  //          = product of (1 - trueFraction - nullFraction)
  // P(NULL) = 1 - P(TRUE) - P(FALSE)

  double notTrueProduct = 1.0;
  double falseProduct = 1.0;

  for (const auto& selectivity : selectivities) {
    notTrueProduct *= (1.0 - selectivity.trueFraction);
    falseProduct *= (1.0 - selectivity.trueFraction - selectivity.nullFraction);
  }

  double resultTrue = 1.0 - notTrueProduct;
  // Clamp to handle floating-point rounding.
  double resultNull = std::max(0.0, notTrueProduct - falseProduct);

  Selectivity result{resultTrue, resultNull};
  result.checkConsistency();
  return result;
}

const Value& value(const ConstraintMap& constraints, ExprCP expr) {
  auto it = constraints.find(expr->id());
  if (it != constraints.end()) {
    return it->second;
  }
  return expr->value();
}

Value exprConstraint(ExprCP expr, ConstraintMap& constraints, bool update) {
  // For leaf expressions (Literal and Column), check if already computed
  // For non-leaf expressions, if update=true, skip the cache lookup and
  // recompute
  bool isLeaf =
      expr->is(PlanType::kLiteralExpr) || expr->is(PlanType::kColumnExpr);

  if (!update || isLeaf) {
    auto it = constraints.find(expr->id());
    if (it != constraints.end()) {
      return it->second;
    }
  }

  Value result = expr->value();

  if (expr->is(PlanType::kFieldExpr)) {
    // For Field, get value from first arg with cardinality from value(first
    // arg, constraints)
    auto* field = expr->as<Field>();
    Value baseValue = exprConstraint(field->base(), constraints, update);
    result = expr->value();
    result.cardinality = baseValue.cardinality;
  } else if (expr->is(PlanType::kCallExpr)) {
    auto* call = expr->as<Call>();

    VELOX_CHECK(
        !call->containsFunction(FunctionSet::kAggregate),
        "Predicate cannot contain an aggregate function call: {}",
        call->toString());

    // No functionConstraint: get max cardinality from args.
    float maxCardinality = 1.0f;
    for (auto* arg : call->args()) {
      Value argValue = exprConstraint(arg, constraints, update);
      maxCardinality = std::max(maxCardinality, argValue.cardinality);
    }
    result = expr->value();
    result.cardinality = maxCardinality;
  }

  // Record the result in constraints
  constraints.insert_or_assign(expr->id(), clampCardinality(result));

  return result;
}

// Clamps Value's cardinality to type-specific limits.
// BOOLEAN: max 2, TINYINT: max 256, SMALLINT: max 65536.
Value clampCardinality(const Value& value) {
  Value result = value;
  auto typeKind = result.type->kind();

  if (typeKind == velox::TypeKind::BOOLEAN) {
    result.cardinality = std::min(result.cardinality, 2.0f);
  } else if (typeKind == velox::TypeKind::TINYINT) {
    result.cardinality = std::min(result.cardinality, 256.0f);
  } else if (typeKind == velox::TypeKind::SMALLINT) {
    result.cardinality = std::min(result.cardinality, 65536.0f);
  }
  return result;
}

Selectivity comparisonSelectivity(
    ConstraintMap& constraints,
    ExprCP expr,
    bool updateConstraints) {
  VELOX_DCHECK(expr->is(PlanType::kCallExpr));

  auto call = expr->as<Call>();
  auto funcName = call->name();

  VELOX_DCHECK_EQ(
      call->args().size(),
      2,
      "Comparison operators must have exactly 2 arguments");

  ExprCP leftExpr = call->args()[0];
  ExprCP rightExpr = call->args()[1];

  const auto& leftValue = value(constraints, leftExpr);
  const auto& rightValue = value(constraints, rightExpr);

  // Only call columnComparisonSelectivity if both sides have min and max.
  if (leftValue.min && leftValue.max && rightValue.min && rightValue.max) {
    return columnComparisonSelectivity(
        leftExpr,
        rightExpr,
        leftValue,
        rightValue,
        funcName,
        updateConstraints,
        constraints);
  }

  // Otherwise, use cardinality-based calculation.
  return cardinalityBasedSelectivity(
      leftExpr,
      rightExpr,
      leftValue,
      rightValue,
      funcName,
      updateConstraints,
      constraints);
}

Selectivity conjunctsSelectivity(
    ConstraintMap& constraints,
    std::span<const ExprCP> conjuncts,
    bool updateConstraints) {
  // Update constraints for each conjunct before processing.
  for (auto* conjunct : conjuncts) {
    exprConstraint(conjunct, constraints, true);
  }

  std::vector<Selectivity> selectivities;
  selectivities.reserve(conjuncts.size());

  // Map from left-hand side expression to list of comparison expressions.
  folly::F14FastMap<ExprCP, std::vector<ExprCP>> rangeConditions;
  std::vector<ExprCP> otherConditions;

  // Classify arguments as range conditions or other conditions.
  for (auto* arg : conjuncts) {
    if (arg->is(PlanType::kCallExpr)) {
      auto argCall = arg->as<Call>();
      auto argFuncName = argCall->name();

      if (isComparisonOperator(argFuncName) &&
          argCall->args()[1]->is(PlanType::kLiteralExpr)) {
        // Group literal-bound comparisons by left-hand side for range
        // analysis. Column-vs-column comparisons (e.g., a = b) go through
        // exprSelectivity which handles cardinality-based estimation.
        ExprCP leftSide = argCall->args()[0];
        rangeConditions[leftSide].push_back(arg);

      } else if (argFuncName == SpecialFormCallNames::kIn) {
        // Handle IN predicates as range conditions
        ExprCP leftSide = argCall->args()[0];
        rangeConditions[leftSide].push_back(arg);

      } else {
        otherConditions.push_back(arg);
      }
    } else {
      otherConditions.push_back(arg);
    }
  }

  // Process range conditions grouped by left-hand side.
  for (const auto& [leftSide, conditions] : rangeConditions) {
    selectivities.push_back(
        rangeSelectivity(constraints, conditions, updateConstraints));
  }

  // Process other conditions.
  for (auto* arg : otherConditions) {
    selectivities.push_back(
        exprSelectivity(constraints, arg, updateConstraints));
  }

  return combineConjuncts(selectivities);
}

// Computes selectivity for literal expressions.
Selectivity literalSelectivity(ExprCP expr) {
  auto literal = expr->as<Literal>();
  const auto& literalValue = literal->literal();

  // Returns null selectivity for NULL literals.
  if (literalValue.isNull()) {
    return {0.0, 1.0};
  }

  // Returns false selectivity for boolean FALSE.
  if (literal->value().type->isBoolean()) {
    if (literalValue.kind() == velox::TypeKind::BOOLEAN &&
        !literalValue.value<bool>()) {
      return {0.0, 0.0};
    }
  }

  // Returns true selectivity for all other constants.
  return {1.0, 0.0};
}

// Computes selectivity for column expressions.
Selectivity columnSelectivity(ConstraintMap& constraints, ExprCP expr) {
  const auto& exprValue = value(constraints, expr);
  if (exprValue.type->isBoolean()) {
    if (exprValue.trueFraction != Value::kUnknown) {
      return {exprValue.trueFraction, exprValue.nullFraction};
    }
    return Selectivity::likelyTrue();
  }
  // For non-boolean columns, selectivity is 1 - nullFraction.
  return {1.0 - exprValue.nullFraction, exprValue.nullFraction};
}

// Computes selectivity for call expressions (function calls).
Selectivity callSelectivity(
    ConstraintMap& constraints,
    ExprCP expr,
    bool updateConstraints) {
  auto call = expr->as<Call>();
  auto funcName = call->name();
  const auto& fn = queryCtx()->functionNames();

  // NOT: null fraction stays the same, true becomes false and vice versa.
  if (funcName == fn.negation) {
    VELOX_CHECK_EQ(call->args().size(), 1, "NOT must have exactly 1 argument");
    auto innerSel = exprSelectivity(constraints, call->args()[0], false);
    return {innerSel.falseFraction(), innerSel.nullFraction};
  }

  // AND
  if (funcName == SpecialFormCallNames::kAnd) {
    return conjunctsSelectivity(constraints, call->args(), updateConstraints);
  }

  // OR
  if (funcName == SpecialFormCallNames::kOr) {
    const auto& args = call->args();
    std::vector<Selectivity> disjuncts;
    disjuncts.reserve(args.size());
    for (auto* arg : args) {
      disjuncts.push_back(exprSelectivity(constraints, arg, false));
    }
    return combineDisjuncts(disjuncts);
  }

  // ISNULL: trueFraction = argument's nullFraction, nullFraction = 0.
  if (funcName == fn.isNull) {
    VELOX_CHECK_EQ(
        call->args().size(), 1, "isnull must have exactly 1 argument");
    const auto& argValue = value(constraints, call->args()[0]);
    return {argValue.nullFraction, 0.0};
  }

  // IN operator.
  if (funcName == SpecialFormCallNames::kIn) {
    std::array<ExprCP, 1> singleExpr = {expr};
    return rangeSelectivity(constraints, singleExpr, updateConstraints);
  }

  // Comparison operators.
  if (isComparisonOperator(funcName)) {
    // If has a second argument that is a literal, use rangeSelectivity.
    if (call->args().size() >= 2 &&
        call->args()[1]->is(PlanType::kLiteralExpr)) {
      std::array<ExprCP, 1> singleExpr = {expr};
      return rangeSelectivity(constraints, singleExpr, updateConstraints);
    }
    // Otherwise use comparisonSelectivity.
    return comparisonSelectivity(constraints, expr, updateConstraints);
  }

  // Other function - default selectivity.
  return Selectivity::likelyTrue();
}

Selectivity exprSelectivity(
    ConstraintMap& constraints,
    ExprCP expr,
    bool updateConstraints) {
  if (expr->is(PlanType::kCallExpr)) {
    return callSelectivity(constraints, expr, updateConstraints);
  }

  if (expr->is(PlanType::kColumnExpr)) {
    return columnSelectivity(constraints, expr);
  }

  if (expr->is(PlanType::kLiteralExpr)) {
    return literalSelectivity(expr);
  }

  return Selectivity::likelyTrue();
}

namespace {

using VariantCP = const velox::Variant*;

// Returns the Variant with the larger value.
template <velox::TypeKind KIND>
VariantCP variantMax(VariantCP a, VariantCP b) {
  return a->value<KIND>() >= b->value<KIND>() ? a : b;
}

// Returns the Variant with the smaller value.
template <velox::TypeKind KIND>
VariantCP variantMin(VariantCP a, VariantCP b) {
  return a->value<KIND>() <= b->value<KIND>() ? a : b;
}

// Intersects two array variants and returns a new registered variant
// containing only elements present in both arrays.
VariantCP intersectArrayVariants(VariantCP arr1, VariantCP arr2) {
  if (arr1 == nullptr || arr2 == nullptr) {
    return nullptr;
  }

  const auto& array1 = arr1->array();
  const auto& array2 = arr2->array();

  std::vector<velox::Variant> result;
  for (const auto& elem1 : array1) {
    for (const auto& elem2 : array2) {
      if (elem1.equals(elem2)) {
        result.push_back(elem1);
        break;
      }
    }
  }

  return registerVariant(velox::Variant::array(result));
}

// Returns the tighter lower bound (the higher of the two values).
// If either is nullptr, returns the other.
VariantCP tightenLowerBound(VariantCP existing, VariantCP candidate) {
  if (existing == nullptr) {
    return candidate;
  }
  if (candidate == nullptr) {
    return existing;
  }
  return *existing < *candidate ? candidate : existing;
}

// Returns the tighter upper bound (the lower of the two values).
// If either is nullptr, returns the other.
VariantCP tightenUpperBound(VariantCP existing, VariantCP candidate) {
  if (existing == nullptr) {
    return candidate;
  }
  if (candidate == nullptr) {
    return existing;
  }
  return *candidate < *existing ? candidate : existing;
}

// Finds the minimum and maximum values in an array of Variants.
// Returns a pair of pointers to the min and max elements.
// Returns {nullptr, nullptr} if the array is empty.
std::pair<VariantCP, VariantCP> findArrayMinMax(
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

// Computes the maximum cardinality for an integer range: 1 + (max - min).
template <velox::TypeKind KIND>
float rangeCardinality(VariantCP minPtr, VariantCP maxPtr) {
  auto upperVal = maxPtr->value<KIND>();
  auto lowerVal = minPtr->value<KIND>();
  return 1.0f + static_cast<float>(upperVal - lowerVal);
}

// Returns true if the given TypeKind represents an integer type
bool isIntegerKind(velox::TypeKind kind) {
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

// Extracts the IN list from an IN call expression.
// The IN list is constructed from all non-first arguments of the call.
// Returns nullptr if any argument is not a literal, indicating that precise
// selectivity cannot be computed and a default should be used.
VariantCP getInListFromCall(const Call* call) {
  VELOX_CHECK(
      call->name() == SpecialFormCallNames::kIn, "Expected IN call expression");
  VELOX_CHECK_GT(call->args().size(), 1, "IN must have at least 2 arguments");

  std::vector<velox::Variant> inListValues;
  inListValues.reserve(call->args().size() - 1);

  // Skip first argument (the left-hand side), collect rest as IN list.
  for (size_t i = 1; i < call->args().size(); ++i) {
    ExprCP arg = call->args()[i];
    if (!arg->is(PlanType::kLiteralExpr)) {
      // Return nullptr if any argument is not a literal.
      return nullptr;
    }
    auto literal = arg->as<Literal>();
    inListValues.push_back(literal->literal());
  }

  return registerVariant(velox::Variant::array(inListValues));
}

// Checks if a value is present in an array variant.
bool isValueInArray(const velox::Variant& value, VariantCP array) {
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
VariantCP pruneInList(VariantCP inList, VariantCP lower, VariantCP upper) {
  if (inList == nullptr) {
    return nullptr;
  }

  std::vector<velox::Variant> filteredList;
  const auto& array = inList->array();

  for (const auto& elem : array) {
    bool keep = true;

    // Check against upper bound
    if (upper != nullptr && *upper < elem) {
      keep = false;
    }

    // Check against lower bound
    if (lower != nullptr && elem < *lower) {
      keep = false;
    }

    if (keep) {
      filteredList.push_back(elem);
    }
  }

  // Register and return the new filtered IN list.
  if (filteredList.size() != array.size()) {
    return registerVariant(velox::Variant::array(filteredList));
  }
  return inList;
}

// Computes selectivity as ratio of intersection to total range.
// For discrete types (integers, VARCHAR), uses +1 to count distinct values.
// For continuous types (REAL, DOUBLE), uses the range difference.
// Returns selectivity clamped to [0, 1].
float computeRangeSelectivity(
    double exprMin,
    double exprMax,
    double effectiveLower,
    double effectiveUpper,
    bool discrete) {
  // Check if intersection is empty.
  if (effectiveLower > effectiveUpper) {
    return 0.0f;
  }

  double offset = discrete ? 1.0 : 0.0;
  double intersectionRange = effectiveUpper - effectiveLower + offset;
  double exprRange = exprMax - exprMin + offset;

  // Ensure divisor is at least 1.
  if (exprRange < 1.0) {
    exprRange = 1.0;
  }

  float selectivity = static_cast<float>(intersectionRange / exprRange);
  return std::clamp(selectivity, 0.0f, 1.0f);
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
        static_cast<double>(exprMin),
        static_cast<double>(exprMax),
        static_cast<double>(effectiveLower),
        static_cast<double>(effectiveUpper),
        /*discrete=*/!std::is_floating_point_v<T>);
  }
}

// Template specialization for VARCHAR.
template <>
float rangeSelectivityImpl<velox::TypeKind::VARCHAR>(
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
    return static_cast<int32_t>(static_cast<unsigned char>(str[0]));
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
      static_cast<double>(exprMin),
      static_cast<double>(exprMax),
      static_cast<double>(effectiveLower),
      static_cast<double>(effectiveUpper),
      /*discrete=*/true);
}

// Computes selectivity for comparisons using only cardinality (no range info).
// Used when min/max bounds are not available for one or both sides.
Selectivity cardinalityBasedSelectivity(
    ExprCP left,
    ExprCP right,
    const Value& leftValue,
    const Value& rightValue,
    Name funcName,
    bool updateConstraints,
    ConstraintMap& constraints) {
  double nullFraction =
      combinedNullFraction(leftValue.nullFraction, rightValue.nullFraction);

  const auto& fn = queryCtx()->functionNames();

  if (funcName == fn.equality) {
    double ac = std::max(1.0, static_cast<double>(leftValue.cardinality));
    double bc = std::max(1.0, static_cast<double>(rightValue.cardinality));
    double minCard = std::min(ac, bc);
    double probTrue = minCard / (ac * bc);

    if (updateConstraints && left && right) {
      Value newConstraint(leftValue.type, minCard);
      newConstraint.nullFraction = 0.0f;
      constraints.insert_or_assign(left->id(), newConstraint);
      constraints.insert_or_assign(right->id(), newConstraint);
    }

    double trueFraction = probTrue * (1.0 - nullFraction);
    return {trueFraction, nullFraction};
  }

  if (isComparisonOperator(funcName)) {
    return Selectivity::unknown(nullFraction);
  }

  return Selectivity::likelyZero(nullFraction);
}

// Comparison selectivity for columns with known ranges and cardinalities.
//
// Given two columns a and b with:
// - Column a: range [al, ah], ac distinct values, nullFraction na
// - Column b: range [bl, bh], bc distinct values, nullFraction nb
//
// We assume values are evenly distributed within each range, and that
// in the intersection of the ranges, the distinct values align.
//
// For example, if a ranges from [1000, 2000] with 100 distinct values,
// and b ranges from [1500, 2500] with 100 distinct values:
// - The intersection [1500, 2000] covers 50% of each range
// - We assume 50 values from a and 50 values from b fall in the intersection
// - These 50 values align (same positions), so they can match
//
// NULL handling:
// - Comparisons with NULL yield NULL (not TRUE or FALSE)
// - P(either null) = na + nb - na×nb
// - All comparison probabilities are scaled by (1 - P(either null))
template <velox::TypeKind KIND>
Selectivity comparisonSelectivityImpl(
    ExprCP left,
    ExprCP right,
    const Value& leftValue,
    const Value& rightValue,
    Name funcName,
    bool updateConstraints,
    ConstraintMap& constraints) {
  double nullFraction =
      combinedNullFraction(leftValue.nullFraction, rightValue.nullFraction);

  // Extract min/max as doubles for arithmetic (caller guarantees non-null).
  double al = leftValue.min->value<KIND>();
  double ah = leftValue.max->value<KIND>();
  double bl = rightValue.min->value<KIND>();
  double bh = rightValue.max->value<KIND>();

  double ac = std::max(1.0, static_cast<double>(leftValue.cardinality));
  double bc = std::max(1.0, static_cast<double>(rightValue.cardinality));

  // Calculate ranges (avoiding zero).
  double rangeA = rangeSize(al, ah);
  double rangeB = rangeSize(bl, bh);

  // Calculate overlap.
  auto overlap = RangeOverlap::compute(al, ah, bl, bh);

  const auto& fn = queryCtx()->functionNames();
  double probTrue = 0.0;

  if (funcName == fn.equality) {
    // P(a = b): distinct values in intersection that match
    if (overlap.size > 0) {
      double nA = ac * (overlap.size / rangeA);
      double nB = bc * (overlap.size / rangeB);
      double matching = std::min(nA, nB);
      probTrue = matching / (ac * bc);

      // Update constraints if requested.
      if (updateConstraints && left && right) {
        // Create a new constraint with the intersection range.
        Value newConstraint(leftValue.type, matching);
        newConstraint.min = variantMax<KIND>(leftValue.min, rightValue.min);
        newConstraint.max = variantMin<KIND>(leftValue.max, rightValue.max);
        newConstraint.nullFraction = 0.0f;
        constraints.insert_or_assign(left->id(), newConstraint);
        constraints.insert_or_assign(right->id(), newConstraint);
      }
    }
  } else if (funcName == fn.lt || funcName == fn.lte) {
    // P(a < b)
    if (ah < bl) {
      // No overlap, a entirely below b
      probTrue = 1.0;
    } else if (al > bh) {
      // No overlap, a entirely above b
      probTrue = 0.0;
    } else {
      // Ranges overlap - use continuous approximation.
      // Fraction of a's range below b's minimum.
      double belowB = std::max(0.0, std::min(ah, bl) - al);

      // For values in overlap, integrate probability that b is greater.
      // Average position of values in overlap from b's perspective.
      double overlapIntegral = 0.0;
      if (overlap.size > 0) {
        // For uniform distribution, P(b > x) for x in overlap
        // = (bh - x) / rangeB, averaged over overlap
        // = overlapSize × (2×bh - overlapHigh - overlapLow) / (2 × rangeB)
        overlapIntegral = overlap.size *
            (2.0 * bh - overlap.high - overlap.low) / (2.0 * rangeB);
      }

      probTrue = (belowB + overlapIntegral) / rangeA;
    }
  } else if (funcName == fn.gt || funcName == fn.gte) {
    // P(a > b) - swap and compute as P(b < a)
    if (al > bh) {
      // No overlap, a entirely above b
      probTrue = 1.0;
    } else if (ah < bl) {
      // No overlap, a entirely below b
      probTrue = 0.0;
    } else {
      // Ranges overlap.
      double belowA = std::max(0.0, std::min(bh, al) - bl);

      double overlapIntegral = 0.0;
      if (overlap.size > 0) {
        overlapIntegral = overlap.size *
            (2.0 * ah - overlap.high - overlap.low) / (2.0 * rangeA);
      }

      probTrue = (belowA + overlapIntegral) / rangeB;
    }
  }

  // Clamp probability and scale by (1 - nullFraction)
  probTrue = std::clamp(probTrue, 0.0, 1.0);
  double trueFraction = probTrue * (1.0 - nullFraction);

  return {trueFraction, nullFraction};
}

} // namespace

Selectivity columnComparisonSelectivity(
    ExprCP left,
    ExprCP right,
    const Value& leftValue,
    const Value& rightValue,
    Name funcName,
    bool updateConstraints,
    ConstraintMap& constraints) {
  // Check if this is a numeric type and both sides have min/max
  bool canUseRange =
      (leftValue.min && leftValue.max && rightValue.min && rightValue.max);

  if (!canUseRange) {
    return cardinalityBasedSelectivity(
        left,
        right,
        leftValue,
        rightValue,
        funcName,
        updateConstraints,
        constraints);
  }

  // Use range-based calculation for numeric types
  switch (leftValue.type->kind()) {
#define NUMERIC_CASE(KIND)                                   \
  case velox::TypeKind::KIND:                                \
    return comparisonSelectivityImpl<velox::TypeKind::KIND>( \
        left,                                                \
        right,                                               \
        leftValue,                                           \
        rightValue,                                          \
        funcName,                                            \
        updateConstraints,                                   \
        constraints);

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
          left,
          right,
          leftValue,
          rightValue,
          funcName,
          updateConstraints,
          constraints);
  }
}

// Computes selectivity for an expression within a value range.
//
// Estimates the fraction of rows where expr falls within [lower, upper].
// Uses min/max/cardinality statistics from the expression's Value to compute
// an accurate selectivity estimate.
//
// For discrete types (integers, VARCHAR), uses +1 to count distinct values:
//   selectivity = (min(upper, max) - max(lower, min) + 1) / (max - min + 1)
// For continuous types (REAL, DOUBLE), uses the range difference:
//   selectivity = (min(upper, max) - max(lower, min)) / (max - min)
// where min/max are the expression's known bounds.
//
// For non-scalar types (ARRAY, MAP, ROW, UNKNOWN, FUNCTION, OPAQUE, INVALID),
// returns a default selectivity of 0.1.
//
// The result is scaled by (1 - nullFraction) since NULLs cannot satisfy
// the range condition.
//
// @param constraints The constraint map containing refined value statistics.
// @param expr The expression to compute range selectivity for.
// @param lower Lower bound of the range (nullptr means unbounded).
// @param upper Upper bound of the range (nullptr means unbounded).
// @return Selectivity with trueFraction and nullFraction.
Selectivity rangeSelectivity(
    const ConstraintMap& constraints,
    ExprCP expr,
    VariantCP lower,
    VariantCP upper) {
  const auto& exprValue = value(constraints, expr);
  const auto kind = exprValue.type->kind();

  // Retrieve null fraction from the expression.
  double nullFraction = exprValue.nullFraction;

  // Types without meaningful range semantics.
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

  // No min/max statistics available.
  if (!exprValue.min || !exprValue.max) {
    return Selectivity::noRange(nullFraction);
  }

  // Dispatch to type-specific implementation using the Velox macro.
  float baseTrueFraction = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      rangeSelectivityImpl, kind, exprValue, lower, upper);

  // Scale true fraction by (1 - nullFraction) since NULLs cannot match.
  double trueFraction = baseTrueFraction * (1.0 - nullFraction);

  return {trueFraction, nullFraction};
}

// Creates a new constraint Value for an expression by tightening bounds and
// adjusting cardinality. Takes the intersection of existing and new bounds.
// For integer types, cardinality is capped at the range size. Sets nullFraction
// to 0 because rows passing a filter condition cannot be NULL (comparisons with
// NULL return NULL/unknown, which is treated as FALSE in WHERE clauses).
Value makeConstraint(
    const ConstraintMap& constraints,
    ExprCP expr,
    VariantCP lower,
    VariantCP upper,
    float cardinality) {
  const auto& oldValue = value(constraints, expr);

  // Tighten bounds: take intersection of old and new constraints.
  VariantCP minPtr = tightenLowerBound(oldValue.min, lower);
  VariantCP maxPtr = tightenUpperBound(oldValue.max, upper);

  // cardinality is at least 1 distinct value. If the type is an integer type,
  // then the cardinality is not more than 1 + (max - min).
  float finalCardinality = std::max(1.0f, cardinality);
  auto kind = oldValue.type->kind();

  // If the type is an integer type and both upper and lower are set (after
  // defaulting), adjust cardinality based on range.
  if (isIntegerKind(kind) && minPtr != nullptr && maxPtr != nullptr) {
#define INTEGER_CASE(KIND)                                        \
  case velox::TypeKind::KIND:                                     \
    finalCardinality = std::min(                                  \
        finalCardinality,                                         \
        rangeCardinality<velox::TypeKind::KIND>(minPtr, maxPtr)); \
    break

    switch (kind) {
      INTEGER_CASE(TINYINT);
      INTEGER_CASE(SMALLINT);
      INTEGER_CASE(INTEGER);
      INTEGER_CASE(BIGINT);
      INTEGER_CASE(HUGEINT);
      default:
        break;
    }
#undef INTEGER_CASE
  }

  Value result(oldValue.type, finalCardinality);
  result.min = minPtr;
  result.max = maxPtr;
  result.nullFraction = 0;
  result.nullable = false;
  return result;
}

namespace {

// Computes selectivity for multiple range conditions on the same column.
//
// Analyzes a set of comparison expressions (eq, lt, lte, gt, gte, IN) on the
// same left-hand side column and computes combined selectivity by:
// - Accumulating upper/lower bounds from lt/lte/gt/gte comparisons
// - Tracking equality constraints from eq
// - Intersecting IN lists when multiple IN conditions exist
// - Detecting contradictory constraints (e.g., x = 5 AND x = 6)
//
// The function detects and returns zero selectivity for contradictory
// conditions:
// - Multiple eq with different values
// - eq value outside effective range bounds
// - eq value not in IN list
// - Empty IN list after pruning to effective range
// - Effective upper < effective lower
// - Any comparison with NULL literal
//
// When updateConstraints is true, records refined constraints based on:
// - For eq: min=max=eqValue, cardinality=1
// - For IN: min/max from list bounds, cardinality=list size
// - For range: lower/upper bounds, cardinality scaled by selectivity
//
// @param state The plan state containing constraints and statistics.
// @param exprs The comparison expressions to analyze (must be non-empty).
// @param updateConstraints If true, updates constraints with refined
//        value ranges.
// @return Selectivity with trueFraction and nullFraction.

// Holds accumulated constraints from processing range expressions.
struct RangeConstraints {
  VariantCP lower = nullptr;
  VariantCP upper = nullptr;
  std::optional<double> eqSelectivity;
  VariantCP eqValue = nullptr;
  VariantCP inList = nullptr;
  bool empty = false;
};

// Processes an IN clause expression, updating the accumulated inList.
// Returns Selectivity if early return is needed (e.g., NULL in list),
// std::nullopt to continue processing.
std::optional<Selectivity> processInClause(
    const Call* call,
    RangeConstraints& rangeConstraints,
    double nullFraction) {
  VariantCP currentInList = getInListFromCall(call);

  // If IN list contains non-literal arguments, use default selectivity.
  if (currentInList == nullptr) {
    return Selectivity::unknown(nullFraction);
  }

  // Return zero selectivity if the IN list contains any NULL literals.
  const auto& array = currentInList->array();
  for (const auto& elem : array) {
    if (elem.isNull()) {
      return Selectivity::zero(nullFraction);
    }
  }

  if (rangeConstraints.inList == nullptr) {
    rangeConstraints.inList = currentInList;
  } else {
    rangeConstraints.inList =
        intersectArrayVariants(rangeConstraints.inList, currentInList);
  }

  return std::nullopt;
}

// Processes an equality clause expression, updating eqSelectivity and eqValue.
// Returns Selectivity if early return is needed (e.g., NULL comparison),
// std::nullopt to continue processing.
std::optional<Selectivity> processEqualityClause(
    const Call* call,
    const Value& exprValue,
    RangeConstraints& rangeConstraints,
    double nullFraction) {
  VELOX_CHECK_EQ(call->args().size(), 2, "eq must have exactly 2 arguments");

  ExprCP rhs = call->args()[1];
  if (!rhs->is(PlanType::kLiteralExpr)) {
    return std::nullopt;
  }

  const auto& litValue = rhs->as<Literal>()->literal();

  // Return zero selectivity if comparing with a NULL literal.
  if (litValue.isNull()) {
    return Selectivity::zero(nullFraction);
  }

  if (!rangeConstraints.eqSelectivity.has_value()) {
    rangeConstraints.eqSelectivity = 1.0 / exprValue.cardinality;
    rangeConstraints.eqValue = &litValue;
  } else {
    if (rangeConstraints.eqValue != nullptr &&
        !(rangeConstraints.eqValue->equals(litValue))) {
      rangeConstraints.empty = true;
    }
  }

  return std::nullopt;
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
VariantCP
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

// Processes a range bound expression (lt, lte, gt, gte), updating lower/upper.
// For integer types, strict inequalities (gt, lt) are converted to inclusive
// bounds (e.g., > 2 becomes >= 3). Returns Selectivity if early return is
// needed (e.g., NULL comparison), std::nullopt to continue processing.
std::optional<Selectivity> processRangeBound(
    const Call* call,
    Name funcName,
    RangeConstraints& rangeConstraints,
    double nullFraction) {
  const auto& fn = queryCtx()->functionNames();
  VELOX_CHECK_EQ(
      call->args().size(), 2, "Comparison must have exactly 2 arguments");

  ExprCP rhs = call->args()[1];
  if (!rhs->is(PlanType::kLiteralExpr)) {
    return std::nullopt;
  }

  const auto& litValue = rhs->as<Literal>()->literal();

  // Return zero selectivity if comparing with a NULL literal.
  if (litValue.isNull()) {
    return Selectivity::zero(nullFraction);
  }

  auto kind = call->args()[0]->value().type->kind();

  if (funcName == fn.lt || funcName == fn.lte) {
    VariantCP bound = &litValue;
    if (funcName == fn.lt && isIntegerKind(kind)) {
      bound = adjustStrictIntegerBound(bound, kind, /*isLower=*/false);
    }
    if (bound != nullptr &&
        (rangeConstraints.upper == nullptr ||
         *bound < *rangeConstraints.upper)) {
      rangeConstraints.upper = bound;
    }
  } else if (funcName == fn.gt || funcName == fn.gte) {
    VariantCP bound = &litValue;
    if (funcName == fn.gt && isIntegerKind(kind)) {
      bound = adjustStrictIntegerBound(bound, kind, /*isLower=*/true);
    }
    if (bound != nullptr &&
        (rangeConstraints.lower == nullptr ||
         *rangeConstraints.lower < *bound)) {
      rangeConstraints.lower = bound;
    }
  }

  return std::nullopt;
}

// Holds effective bounds after combining query constraints with column stats.
struct EffectiveBounds {
  VariantCP lower;
  VariantCP upper;
  VariantCP inList;
};

// Computes effective bounds by combining query constraints with column
// min/max.
EffectiveBounds computeEffectiveBounds(
    const Value& exprValue,
    const RangeConstraints& rangeConstraints) {
  VariantCP effectiveLower = rangeConstraints.lower;
  VariantCP effectiveUpper = rangeConstraints.upper;

  // The effective lower bound is the maximum of query lower and value min.
  if (exprValue.min != nullptr) {
    if (effectiveLower == nullptr || *effectiveLower < *exprValue.min) {
      effectiveLower = exprValue.min;
    }
  }

  // The effective upper bound is the minimum of query upper and value max.
  if (exprValue.max != nullptr) {
    if (effectiveUpper == nullptr || *exprValue.max < *effectiveUpper) {
      effectiveUpper = exprValue.max;
    }
  }

  // Prune the IN list to only include values within the effective range.
  VariantCP prunedInList = rangeConstraints.inList;
  if (prunedInList != nullptr) {
    prunedInList = pruneInList(prunedInList, effectiveLower, effectiveUpper);
  }

  return {effectiveLower, effectiveUpper, prunedInList};
}

// Detects contradictory constraints between bounds, equality, and IN list.
// Returns true if constraints are contradictory.
bool hasContradiction(
    const RangeConstraints& rangeConstraints,
    const EffectiveBounds& bounds) {
  if (rangeConstraints.empty) {
    return true;
  }

  // Empty IN list after pruning.
  if (bounds.inList != nullptr && bounds.inList->array().empty()) {
    return true;
  }

  // Equality value not in IN list.
  if (bounds.inList != nullptr && rangeConstraints.eqValue != nullptr) {
    if (!isValueInArray(*rangeConstraints.eqValue, bounds.inList)) {
      return true;
    }
  }

  // Equality value outside bounds.
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

  // Contradictory range bounds (e.g., lower=5 > upper=4).
  if (bounds.lower != nullptr && bounds.upper != nullptr) {
    if (*bounds.upper < *bounds.lower) {
      return true;
    }
  }

  return false;
}

// Computes selectivity for an equality constraint.
Selectivity computeEqualitySelectivity(
    ConstraintMap& constraints,
    ExprCP leftSide,
    const RangeConstraints& rangeConstraints,
    double nullFraction,
    bool updateConstraints) {
  if (updateConstraints) {
    constraints.insert_or_assign(
        leftSide->id(),
        makeConstraint(
            constraints,
            leftSide,
            rangeConstraints.eqValue,
            rangeConstraints.eqValue,
            1.0f));
  }
  // Scale by (1 - nullFraction) since NULLs cannot match equality.
  return {
      rangeConstraints.eqSelectivity.value() * (1.0 - nullFraction),
      nullFraction};
}

// Computes selectivity for an IN list constraint.
Selectivity computeInListSelectivity(
    ConstraintMap& constraints,
    ExprCP leftSide,
    const EffectiveBounds& bounds,
    const Value& exprValue,
    double nullFraction,
    bool updateConstraints) {
  const auto& array = bounds.inList->array();

  double inListSize = static_cast<double>(array.size());
  double trueFraction =
      std::clamp(inListSize / exprValue.cardinality, 0.0, 1.0) *
      (1.0 - nullFraction);

  if (updateConstraints) {
    auto [minVal, maxVal] = findArrayMinMax(array);
    constraints.insert_or_assign(
        leftSide->id(),
        makeConstraint(constraints, leftSide, minVal, maxVal, inListSize));
  }

  return {trueFraction, nullFraction};
}

// Computes selectivity for range bounds (lower/upper).
Selectivity computeBoundsSelectivity(
    ConstraintMap& constraints,
    ExprCP leftSide,
    const RangeConstraints& rangeConstraints,
    const EffectiveBounds& bounds,
    const Value& exprValue,
    double nullFraction,
    bool updateConstraints) {
  Selectivity selectivity = rangeSelectivity(
      constraints, leftSide, rangeConstraints.lower, rangeConstraints.upper);

  // Use baseTrueFraction (before null scaling) for cardinality estimation.
  // Nulls reduce the fraction of rows passing the filter, but don't reduce
  // the number of distinct values in the matching range.
  double baseTrueFraction = (exprValue.nullFraction < 1.0)
      ? selectivity.trueFraction / (1.0 - exprValue.nullFraction)
      : selectivity.trueFraction;

  if (updateConstraints) {
    constraints.insert_or_assign(
        leftSide->id(),
        makeConstraint(
            constraints,
            leftSide,
            rangeConstraints.lower,
            rangeConstraints.upper,
            exprValue.cardinality * baseTrueFraction));
  }

  return selectivity;
}

// Computes selectivity for range predicates on a single column.
//
// All expressions in exprs are assumed to operate on the same left-hand side
// column/expression. This function is designed for combined range predicates
// on a single column (e.g., col > 5 AND col < 10 AND col IN (6, 7, 8)).
//
// @param constraints The constraint map containing refined value statistics.
// @param exprs The comparison expressions to analyze (must be non-empty).
// @param updateConstraints If true, updates constraints with refined
//        value ranges.
// @return Selectivity with trueFraction and nullFraction.
Selectivity rangeSelectivity(
    ConstraintMap& constraints,
    std::span<const ExprCP> exprs,
    bool updateConstraints) {
  VELOX_CHECK_GE(exprs.size(), 1, "exprs must have at least one element");
  VELOX_CHECK(
      exprs[0]->is(PlanType::kCallExpr),
      "All elements must be Call expressions");

  auto firstCall = exprs[0]->as<Call>();
  VELOX_CHECK_GE(
      firstCall->args().size(), 1, "Call must have at least one argument");
  ExprCP leftSide = firstCall->args()[0];

  // Retrieve null fraction and other statistics from the left-hand side.
  const auto& exprValue = value(constraints, leftSide);
  double nullFraction = exprValue.nullFraction;

  RangeConstraints rangeConstraints;
  const auto& fn = queryCtx()->functionNames();

  // Process each comparison expression to accumulate bounds and constraints.
  for (auto* expr : exprs) {
    VELOX_CHECK(expr->is(PlanType::kCallExpr), "All elements must be calls");

    auto call = expr->as<Call>();
    auto funcName = call->name();

    std::optional<Selectivity> earlyReturn;

    if (funcName == SpecialFormCallNames::kIn) {
      earlyReturn = processInClause(call, rangeConstraints, nullFraction);
    } else if (funcName == fn.equality) {
      earlyReturn = processEqualityClause(
          call, exprValue, rangeConstraints, nullFraction);
    } else if (isRangeBoundOperator(funcName)) {
      earlyReturn =
          processRangeBound(call, funcName, rangeConstraints, nullFraction);
    }

    if (earlyReturn.has_value()) {
      return earlyReturn.value();
    }
  }

  // Compute effective bounds considering column min/max.
  auto bounds = computeEffectiveBounds(exprValue, rangeConstraints);

  // Detect contradictions.
  if (hasContradiction(rangeConstraints, bounds)) {
    if (updateConstraints) {
      Value emptyConstraint(exprValue.type, 0);
      emptyConstraint.nullFraction = nullFraction;
      constraints.insert_or_assign(leftSide->id(), emptyConstraint);
    }
    return Selectivity::likelyZero(nullFraction);
  }

  // Compute selectivity based on constraint type.
  if (rangeConstraints.eqValue != nullptr) {
    return computeEqualitySelectivity(
        constraints,
        leftSide,
        rangeConstraints,
        nullFraction,
        updateConstraints);
  }

  if (bounds.inList != nullptr) {
    return computeInListSelectivity(
        constraints,
        leftSide,
        bounds,
        exprValue,
        nullFraction,
        updateConstraints);
  }

  if (rangeConstraints.lower != nullptr || rangeConstraints.upper != nullptr) {
    return computeBoundsSelectivity(
        constraints,
        leftSide,
        rangeConstraints,
        bounds,
        exprValue,
        nullFraction,
        updateConstraints);
  }

  // Return default selectivity of 0.5 when no specific constraints apply.
  return Selectivity::unknown(nullFraction);
}

} // namespace

// Declared in namespace to allow calling from debugger.
std::string constraintsString(const ConstraintMap& constraints) {
  std::stringstream out;
  for (const auto& pair : constraints) {
    out << pair.first;
    if (queryCtx() != nullptr) {
      auto* expr = queryCtx()->objectAt(pair.first);
      if (expr != nullptr) {
        out << " (" << expr->toString() << ")";
      }
    }
    out << " = " << pair.second.toString() << "\n";
  }
  return out.str();
}

} // namespace facebook::axiom::optimizer
