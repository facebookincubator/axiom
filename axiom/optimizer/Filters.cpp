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
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/PlanUtils.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/QueryGraphContext.h"
#include "axiom/optimizer/RelationOpPrinter.h"
#include "axiom/optimizer/RelationOpVisitor.h"
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/expression/ScopedVarSetter.h"

namespace facebook::axiom::optimizer {

bool isName(Name name, const char* functionName) {
  return name == toName(functionName);
}

void validateSelectivity(const Selectivity& sel) {
  VELOX_CHECK_GE(sel.trueFraction, -1e-9, "trueFraction must be >= 0");
  VELOX_CHECK_LE(sel.trueFraction, 1.0 + 1e-9, "trueFraction must be <= 1");
  VELOX_CHECK_GE(sel.nullFraction, -1e-9, "nullFraction must be >= 0");
  VELOX_CHECK_LE(sel.nullFraction, 1.0 + 1e-9, "nullFraction must be <= 1");
  VELOX_CHECK_LE(
      sel.trueFraction + sel.nullFraction,
      1.0 + 1e-9,
      "trueFraction + nullFraction must be <= 1");
}

Selectivity combineConjuncts(std::span<const Selectivity> selectivities) {
  if (selectivities.empty()) {
    return {1.0, 0.0};
  }

  // For AND: result is TRUE only if all are TRUE
  // result is NULL if any is NULL and none is FALSE
  // result is FALSE otherwise
  //
  // P(TRUE) = product of all trueFractions
  // P(NULL) = (p1 + n1)(p2 + n2)...(pN + nN) - P(TRUE)
  //         = product of (trueFraction + nullFraction) - P(TRUE)
  // P(FALSE) = 1 - P(TRUE) - P(NULL)

  double trueProduct = 1.0;
  double trueOrNullProduct = 1.0;

  for (const auto& sel : selectivities) {
    validateSelectivity(sel);
    trueProduct *= sel.trueFraction;
    trueOrNullProduct *= (sel.trueFraction + sel.nullFraction);
  }

  double resultTrue = trueProduct;
  double resultNull = trueOrNullProduct - trueProduct;

  Selectivity result{resultTrue, resultNull};
  validateSelectivity(result);
  return result;
}

Selectivity combineDisjuncts(std::span<const Selectivity> selectivities) {
  if (selectivities.empty()) {
    return {0.0, 0.0};
  }

  // For OR: result is TRUE if any is TRUE
  // result is NULL if any is NULL and none is TRUE
  // result is FALSE only if all are FALSE
  //
  // P(TRUE) = 1 - product of all (1 - trueFraction)
  // P(FALSE) = product of all falseFractions
  //          = product of (1 - trueFraction - nullFraction)
  // P(NULL) = 1 - P(TRUE) - P(FALSE)

  double notTrueProduct = 1.0;
  double falseProduct = 1.0;

  for (const auto& sel : selectivities) {
    validateSelectivity(sel);
    notTrueProduct *= (1.0 - sel.trueFraction);
    falseProduct *= (1.0 - sel.trueFraction - sel.nullFraction);
  }

  double resultTrue = 1.0 - notTrueProduct;
  double resultFalse = falseProduct;
  double resultNull = 1.0 - resultTrue - resultFalse;

  Selectivity result{resultTrue, resultNull};
  validateSelectivity(result);
  return result;
}

float combineSelectivities(
    float minLimit,
    std::span<const float> selectivities) {
  float product = 1.0f;
  for (float c : selectivities) {
    product *= c;
  }

  // Ensures result never goes below minLimit
  // But transitions smoothly from multiplication to the limit
  if (product >= minLimit * 10) {
    return product; // Normal multiplication when far from limit
  }

  // Smooth interpolation: minLimit + dampened product
  return minLimit + (product - minLimit) * (product / (minLimit + product));
}

const Value& value(const PlanState& state, ExprCP expr) {
  auto it = state.constraints().find(expr->id());
  if (it != state.constraints().end()) {
    return it->second;
  }
  return expr->value();
}

void addConstraint(int32_t exprId, Value value, ConstraintMap& constraints) {
  // Limit cardinality based on type kind
  auto typeKind = value.type->kind();

  if (typeKind == velox::TypeKind::BOOLEAN) {
    // Boolean can have at most 2 distinct values (true/false)
    value.cardinality = std::min(value.cardinality, 2.0f);
  } else if (typeKind == velox::TypeKind::TINYINT) {
    // TINYINT (int8) can have at most 256 distinct values (-128 to 127)
    value.cardinality = std::min(value.cardinality, 256.0f);
  } else if (typeKind == velox::TypeKind::SMALLINT) {
    // SMALLINT (int16) can have at most 65536 distinct values
    value.cardinality = std::min(value.cardinality, 65536.0f);
  }
  // Other types (INTEGER, BIGINT, VARCHAR, etc.) have no practical limit

  constraints.insert_or_assign(exprId, value);
}

Selectivity comparisonSelectivity(
    const PlanState& state,
    ExprCP expr,
    bool updateConstraints,
    ConstraintMap& newConstraints) {
  // Extract the comparison operator
  if (!expr->is(PlanType::kCallExpr)) {
    return {0.3, 0.0};
  }

  auto call = expr->as<Call>();
  auto funcName = call->name();

  // Check if this is a comparison operator
  if (!isName(funcName, "eq") && !isName(funcName, "lt") &&
      !isName(funcName, "lte") && !isName(funcName, "gt") &&
      !isName(funcName, "gte")) {
    return {0.3, 0.0};
  }

  VELOX_CHECK_EQ(
      call->args().size(),
      2,
      "Comparison operators must have exactly 2 arguments");

  ExprCP leftExpr = call->args()[0];
  ExprCP rightExpr = call->args()[1];

  // Get values for both sides
  const auto& leftValue = value(state, leftExpr);
  const auto& rightValue = value(state, rightExpr);

  // Only call columnComparisonSelectivity if both sides have min and max
  if (leftValue.min && leftValue.max && rightValue.min && rightValue.max) {
    return columnComparisonSelectivity(
        leftExpr,
        rightExpr,
        leftValue,
        rightValue,
        funcName,
        updateConstraints,
        newConstraints);
  }

  // Otherwise, use cardinality-based calculation
  double nullFrac = leftValue.nullFraction + rightValue.nullFraction -
      (leftValue.nullFraction * rightValue.nullFraction);

  double ac = std::max(1.0, static_cast<double>(leftValue.cardinality));
  double bc = std::max(1.0, static_cast<double>(rightValue.cardinality));

  double probTrue = 0.0;
  if (isName(funcName, "eq")) {
    double minCard = std::min(ac, bc);
    probTrue = minCard / (ac * bc);

    // Update constraints if requested
    if (updateConstraints) {
      Value newConstraint(leftValue.type, minCard);
      newConstraint.nullFraction = 0.0f;

      int32_t leftId = leftExpr->id();
      int32_t rightId = rightExpr->id();
      newConstraints.erase(leftId);
      newConstraints.emplace(leftId, newConstraint);
      newConstraints.erase(rightId);
      newConstraints.emplace(rightId, newConstraint);
    }
  } else {
    probTrue = 0.5;
  }

  double trueFrac = probTrue * (1.0 - nullFrac);
  return {trueFrac, nullFrac};
}

Selectivity functionSelectivity(
    const PlanState& state,
    ExprCP expr,
    bool updateConstraints,
    ConstraintMap& newConstraints) {
  // Placeholder implementation
  return {0.8, 0.0};
}

Selectivity conjunctsSelectivity(
    PlanState& state,
    std::span<const ExprCP> conjuncts,
    bool updateConstraints,
    ConstraintMap& newConstraints) {
  // Update constraints for each conjunct before processing
  for (auto* conjunct : conjuncts) {
    exprConstraint(conjunct, state, true);
  }

  std::vector<Selectivity> selectivities;
  selectivities.reserve(conjuncts.size());

  // Map from left-hand side expression to list of comparison expressions
  folly::F14FastMap<ExprCP, std::vector<ExprCP>> rangeConditions;
  std::vector<ExprCP> otherConditions;

  // Classify arguments as range conditions or other conditions
  for (auto* arg : conjuncts) {
    if (arg->is(PlanType::kCallExpr)) {
      auto argCall = arg->as<Call>();
      auto argFuncName = argCall->name();

      // Check if this is a comparison operator
      if ((isName(argFuncName, "eq") || isName(argFuncName, "lt") ||
           isName(argFuncName, "lte") || isName(argFuncName, "gt") ||
           isName(argFuncName, "gte")) &&
          argCall->args().size() == 2) {
        // Group by left-hand side
        ExprCP leftSide = argCall->args()[0];
        rangeConditions[leftSide].push_back(arg);
      } else if (
          argFuncName == SpecialFormCallNames::kIn &&
          argCall->args().size() > 1) {
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

  // Process range conditions grouped by left-hand side
  for (const auto& [leftSide, conditions] : rangeConditions) {
    selectivities.push_back(
        rangeSelectivity(state, conditions, updateConstraints, newConstraints));
  }

  // Process other conditions
  for (auto* arg : otherConditions) {
    selectivities.push_back(
        exprSelectivity(state, arg, updateConstraints, newConstraints));
  }

  return combineConjuncts(selectivities);
}

Selectivity exprSelectivity(
    PlanState& state,
    ExprCP expr,
    bool updateConstraints,
    ConstraintMap& newConstraints) {
  // Handle function calls
  if (expr->is(PlanType::kCallExpr)) {
    auto call = expr->as<Call>();
    auto funcName = call->name();
    auto* registry = FunctionRegistry::instance();

    // Check for 'not' function
    if (isName(funcName, registry->negation().c_str())) {
      VELOX_CHECK_EQ(
          call->args().size(), 1, "NOT must have exactly 1 argument");
      auto innerSel =
          exprSelectivity(state, call->args()[0], false, newConstraints);
      // NOT: null fraction stays the same, true fraction inverted
      return {1.0 - innerSel.trueFraction, innerSel.nullFraction};
    }

    // Check for 'and' function
    if (funcName == SpecialFormCallNames::kAnd) {
      return conjunctsSelectivity(
          state, call->args(), updateConstraints, newConstraints);
    }

    // Check for 'or' function
    if (funcName == SpecialFormCallNames::kOr) {
      const auto& args = call->args();
      std::vector<Selectivity> disjuncts;
      disjuncts.reserve(args.size());
      for (auto* arg : args) {
        disjuncts.push_back(exprSelectivity(state, arg, false, newConstraints));
      }
      return combineDisjuncts(disjuncts);
    }

    // Check for 'isnull' function
    if (isName(funcName, "isnull")) {
      VELOX_CHECK_EQ(
          call->args().size(), 1, "isnull must have exactly 1 argument");
      const auto& argValue = value(state, call->args()[0]);
      // ISNULL: trueFraction = argument's nullFraction, nullFraction = 0
      return {argValue.nullFraction, 0.0};
    }

    // Check for IN operator
    if (funcName == SpecialFormCallNames::kIn) {
      std::array<ExprCP, 1> singleExpr = {expr};
      return rangeSelectivity(
          state, singleExpr, updateConstraints, newConstraints);
    }

    // Check for comparison operators
    if (isName(funcName, "eq") || isName(funcName, "lt") ||
        isName(funcName, "lte") || isName(funcName, "gt") ||
        isName(funcName, "gte")) {
      // If has a second argument that is a literal, use rangeSelectivity
      if (call->args().size() >= 2 &&
          call->args()[1]->is(PlanType::kLiteralExpr)) {
        std::array<ExprCP, 1> singleExpr = {expr};
        return rangeSelectivity(
            state, singleExpr, updateConstraints, newConstraints);
      }
      // Otherwise use comparisonSelectivity
      return comparisonSelectivity(
          state, expr, updateConstraints, newConstraints);
    }

    // Other function
    return functionSelectivity(state, expr, updateConstraints, newConstraints);
  }

  // Handle columns
  if (expr->is(PlanType::kColumnExpr)) {
    const auto& val = value(state, expr);
    if (val.type->isBoolean()) {
      if (val.trueFraction != Value::kUnknown) {
        return {val.trueFraction, val.nullFraction};
      }
      return {0.8, 0.0};
    }
    // For other columns, selectivity is 1 - nullFraction
    return {1.0 - val.nullFraction, val.nullFraction};
  }

  // Handle literals
  if (expr->is(PlanType::kLiteralExpr)) {
    auto literal = expr->as<Literal>();
    const auto& lit = literal->literal();

    // null returns null selectivity
    if (lit.isNull()) {
      return {0.0, 1.0};
    }

    // boolean false returns false selectivity
    if (literal->value().type->isBoolean()) {
      if (lit.kind() == velox::TypeKind::BOOLEAN && !lit.value<bool>()) {
        return {0.0, 0.0};
      }
    }

    // Any other constant returns true selectivity
    return {1.0, 0.0};
  }

  // Default case
  return {0.8, 0.0};
}

namespace {

// Intersects two array variants and returns a new registered variant
// containing only elements present in both arrays.
const velox::Variant* intersectArrayVariants(
    const velox::Variant* arr1,
    const velox::Variant* arr2) {
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
// All non-first arguments are expected to be literals.
const velox::Variant* getInListFromCall(const Call* call) {
  VELOX_CHECK(
      call->name() == SpecialFormCallNames::kIn, "Expected IN call expression");
  VELOX_CHECK_GT(call->args().size(), 1, "IN must have at least 2 arguments");

  std::vector<velox::Variant> inListValues;
  // Skip first argument (the left-hand side), collect rest as IN list
  for (size_t i = 1; i < call->args().size(); ++i) {
    ExprCP arg = call->args()[i];
    VELOX_CHECK(
        arg->is(PlanType::kLiteralExpr),
        "All IN list arguments must be literals");
    auto literal = arg->as<Literal>();
    inListValues.push_back(literal->literal());
  }

  return registerVariant(velox::Variant::array(inListValues));
}

// Checks if a value is present in an array variant.
bool isValueInArray(const velox::Variant& value, const velox::Variant* array) {
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
const velox::Variant* pruneInList(
    const velox::Variant* inList,
    const velox::Variant* lower,
    const velox::Variant* upper) {
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

  // Register and return the new filtered IN list
  if (filteredList.size() != array.size()) {
    return registerVariant(velox::Variant::array(filteredList));
  }
  return inList;
}

// Template function for computing range selectivity for scalar types
template <velox::TypeKind KIND>
float rangeSelectivityImpl(
    const Value& exprValue,
    const velox::Variant* lower,
    const velox::Variant* upper) {
  using T = typename velox::TypeTraits<KIND>::NativeType;

  // Non-numeric types (VARBINARY, TIMESTAMP, etc.) return 0.1
  if constexpr (
      KIND == velox::TypeKind::VARBINARY ||
      KIND == velox::TypeKind::TIMESTAMP) {
    return 0.1f;
  } else {
    // Numeric types: compute intersection
    // Check if expr has min/max set
    if (!exprValue.min || !exprValue.max) {
      return 0.1f; // No min/max available
    }

    T exprMin = exprValue.min->value<KIND>();
    T exprMax = exprValue.max->value<KIND>();

    // Determine effective lower bound
    T effectiveLower = exprMin;
    if (lower != nullptr) {
      T lowerVal = lower->value<KIND>();
      effectiveLower = std::max(exprMin, lowerVal);
    }

    // Determine effective upper bound
    T effectiveUpper = exprMax;
    if (upper != nullptr) {
      T upperVal = upper->value<KIND>();
      effectiveUpper = std::min(exprMax, upperVal);
    }

    // Check if intersection is empty
    if (effectiveLower > effectiveUpper) {
      return 0.0f;
    }

    // Compute selectivity as ratio of intersection to expr range
    // Convert to double for computation to avoid overflow/underflow
    double intersectionRange = static_cast<double>(effectiveUpper) -
        static_cast<double>(effectiveLower);
    double exprRange =
        static_cast<double>(exprMax) - static_cast<double>(exprMin);

    // Ensure divisor is at least 1
    if (exprRange < 1.0) {
      exprRange = 1.0;
    }

    float selectivity = static_cast<float>(intersectionRange / exprRange);

    // Clamp to [0, 1]
    return std::clamp(selectivity, 0.0f, 1.0f);
  }
}

// Template specialization for VARCHAR
template <>
float rangeSelectivityImpl<velox::TypeKind::VARCHAR>(
    const Value& exprValue,
    const velox::Variant* lower,
    const velox::Variant* upper) {
  // Helper to get ASCII value of first character, treating empty string as 0
  auto getFirstCharValue = [](const velox::Variant* var) -> int32_t {
    if (var == nullptr) {
      return 0;
    }
    const auto& str = var->value<velox::TypeKind::VARCHAR>();
    if (str.empty()) {
      return 0;
    }
    return static_cast<int32_t>(static_cast<unsigned char>(str[0]));
  };

  // Check if expr has min/max set
  if (!exprValue.min || !exprValue.max) {
    return 0.1f; // No min/max available
  }

  int32_t exprMin = getFirstCharValue(exprValue.min);
  int32_t exprMax = getFirstCharValue(exprValue.max);

  // Determine effective lower bound
  int32_t effectiveLower = exprMin;
  if (lower != nullptr) {
    int32_t lowerVal = getFirstCharValue(lower);
    effectiveLower = std::max(exprMin, lowerVal);
  }

  // Determine effective upper bound
  int32_t effectiveUpper = exprMax;
  if (upper != nullptr) {
    int32_t upperVal = getFirstCharValue(upper);
    effectiveUpper = std::min(exprMax, upperVal);
  }

  // Check if intersection is empty
  if (effectiveLower > effectiveUpper) {
    return 0.0f;
  }

  // Compute selectivity as ratio of intersection to expr range
  double intersectionRange =
      static_cast<double>(effectiveUpper) - static_cast<double>(effectiveLower);
  double exprRange =
      static_cast<double>(exprMax) - static_cast<double>(exprMin);

  // Ensure divisor is at least 1
  if (exprRange < 1.0) {
    exprRange = 1.0;
  }

  float selectivity = static_cast<float>(intersectionRange / exprRange);

  // Clamp to [0, 1]
  return std::clamp(selectivity, 0.0f, 1.0f);
}

} // namespace

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
  using T = typename velox::TypeTraits<KIND>::NativeType;

  // Calculate null fraction: P(left null OR right null)
  double nullFrac = leftValue.nullFraction + rightValue.nullFraction -
      (leftValue.nullFraction * rightValue.nullFraction);

  // Extract min/max (caller guarantees these are non-null)
  T al = leftValue.min->value<KIND>();
  T ah = leftValue.max->value<KIND>();
  T bl = rightValue.min->value<KIND>();
  T bh = rightValue.max->value<KIND>();

  double ac = leftValue.cardinality;
  double bc = rightValue.cardinality;

  // Ensure cardinality is at least 1
  ac = std::max(1.0, ac);
  bc = std::max(1.0, bc);

  // Calculate ranges (avoiding zero)
  double range_a =
      std::max(1.0, static_cast<double>(ah) - static_cast<double>(al));
  double range_b =
      std::max(1.0, static_cast<double>(bh) - static_cast<double>(bl));

  // Calculate overlap
  double overlap_low =
      std::max(static_cast<double>(al), static_cast<double>(bl));
  double overlap_high =
      std::min(static_cast<double>(ah), static_cast<double>(bh));
  double overlap_size = std::max(0.0, overlap_high - overlap_low);

  double probTrue = 0.0;

  if (isName(funcName, "eq")) {
    // P(a = b): distinct values in intersection that match
    if (overlap_size > 0) {
      double n_a = ac * (overlap_size / range_a);
      double n_b = bc * (overlap_size / range_b);
      double matching = std::min(n_a, n_b);
      probTrue = matching / (ac * bc);

      // Update constraints if requested
      if (updateConstraints && left && right) {
        // Create a new constraint with the intersection range
        Value newConstraint(leftValue.type, matching);
        newConstraint.min = registerVariant(
            velox::Variant::create<T>(static_cast<T>(overlap_low)));
        newConstraint.max = registerVariant(
            velox::Variant::create<T>(static_cast<T>(overlap_high)));
        newConstraint.nullFraction = 0.0f;

        // Store constraint for both left and right using pointer as key
        int32_t leftId = left->id();
        int32_t rightId = right->id();
        constraints.erase(leftId);
        constraints.emplace(leftId, newConstraint);
        constraints.erase(rightId);
        constraints.emplace(rightId, newConstraint);
      }
    }
  } else if (isName(funcName, "lt") || isName(funcName, "lte")) {
    // P(a < b)
    if (ah < bl) {
      // No overlap, a entirely below b
      probTrue = 1.0;
    } else if (al > bh) {
      // No overlap, a entirely above b
      probTrue = 0.0;
    } else {
      // Ranges overlap - use continuous approximation
      // Fraction of a's range below b's minimum
      double below_b = std::max(
          0.0,
          std::min(static_cast<double>(ah), static_cast<double>(bl)) -
              static_cast<double>(al));

      // For values in overlap, integrate probability that b is greater
      // Average position of values in overlap from b's perspective
      double overlap_integral = 0.0;
      if (overlap_size > 0) {
        // For uniform distribution, P(b > x) for x in overlap
        // = (bh - x) / range_b, averaged over overlap
        // = overlap_size × (2×bh - overlap_high - overlap_low) / (2 × range_b)
        overlap_integral = overlap_size *
            (2.0 * static_cast<double>(bh) - overlap_high - overlap_low) /
            (2.0 * range_b);
      }

      probTrue = (below_b + overlap_integral) / range_a;
    }
  } else if (isName(funcName, "gt") || isName(funcName, "gte")) {
    // P(a > b) - swap and compute as P(b < a)
    if (al > bh) {
      // No overlap, a entirely above b
      probTrue = 1.0;
    } else if (ah < bl) {
      // No overlap, a entirely below b
      probTrue = 0.0;
    } else {
      // Ranges overlap
      double below_a = std::max(
          0.0,
          std::min(static_cast<double>(bh), static_cast<double>(al)) -
              static_cast<double>(bl));

      double overlap_integral = 0.0;
      if (overlap_size > 0) {
        overlap_integral = overlap_size *
            (2.0 * static_cast<double>(ah) - overlap_high - overlap_low) /
            (2.0 * range_a);
      }

      probTrue = (below_a + overlap_integral) / range_b;
    }
  }

  // Clamp probability and scale by (1 - nullFrac)
  probTrue = std::clamp(probTrue, 0.0, 1.0);
  double trueFrac = probTrue * (1.0 - nullFrac);

  return {trueFrac, nullFrac};
}

Selectivity columnComparisonSelectivity(
    ExprCP left,
    ExprCP right,
    const Value& leftValue,
    const Value& rightValue,
    Name funcName,
    bool updateConstraints,
    ConstraintMap& constraints) {
  // Calculate null fraction first
  double nullFrac = leftValue.nullFraction + rightValue.nullFraction -
      (leftValue.nullFraction * rightValue.nullFraction);

  const auto kind = leftValue.type->kind();

  // Check if this is a numeric type and both sides have min/max
  bool canUseRange =
      (leftValue.min && leftValue.max && rightValue.min && rightValue.max);

  if (!canUseRange) {
    // Use cardinality-based calculation
    double ac = std::max(1.0, static_cast<double>(leftValue.cardinality));
    double bc = std::max(1.0, static_cast<double>(rightValue.cardinality));

    double probTrue = 0.0;
    if (isName(funcName, "eq")) {
      // Probability of equality: (1/a.cardinality) * (1/b.cardinality)
      // But we assume values are aligned, so we need min cardinality
      double minCard = std::min(ac, bc);
      probTrue = minCard / (ac * bc);

      // Update constraints if requested
      if (updateConstraints && left && right) {
        Value newConstraint(leftValue.type, minCard);
        newConstraint.nullFraction = 0.0f;

        int32_t leftId = left->id();
        int32_t rightId = right->id();
        constraints.erase(leftId);
        constraints.emplace(leftId, newConstraint);
        constraints.erase(rightId);
        constraints.emplace(rightId, newConstraint);
      }
    } else if (
        isName(funcName, "lt") || isName(funcName, "lte") ||
        isName(funcName, "gt") || isName(funcName, "gte")) {
      // For range comparisons, assume range is [0, max(ac, bc)]
      // Values are aligned and uniformly distributed
      // P(a < b) ≈ 0.5 for symmetric case
      probTrue = 0.5;
    }

    double trueFrac = probTrue * (1.0 - nullFrac);
    return {trueFrac, nullFrac};
  }

  // Use range-based calculation for numeric types
  switch (kind) {
    case velox::TypeKind::TINYINT:
      return comparisonSelectivityImpl<velox::TypeKind::TINYINT>(
          left,
          right,
          leftValue,
          rightValue,
          funcName,
          updateConstraints,
          constraints);
    case velox::TypeKind::SMALLINT:
      return comparisonSelectivityImpl<velox::TypeKind::SMALLINT>(
          left,
          right,
          leftValue,
          rightValue,
          funcName,
          updateConstraints,
          constraints);
    case velox::TypeKind::INTEGER:
      return comparisonSelectivityImpl<velox::TypeKind::INTEGER>(
          left,
          right,
          leftValue,
          rightValue,
          funcName,
          updateConstraints,
          constraints);
    case velox::TypeKind::BIGINT:
      return comparisonSelectivityImpl<velox::TypeKind::BIGINT>(
          left,
          right,
          leftValue,
          rightValue,
          funcName,
          updateConstraints,
          constraints);
    case velox::TypeKind::HUGEINT:
      return comparisonSelectivityImpl<velox::TypeKind::HUGEINT>(
          left,
          right,
          leftValue,
          rightValue,
          funcName,
          updateConstraints,
          constraints);
    case velox::TypeKind::REAL:
      return comparisonSelectivityImpl<velox::TypeKind::REAL>(
          left,
          right,
          leftValue,
          rightValue,
          funcName,
          updateConstraints,
          constraints);
    case velox::TypeKind::DOUBLE:
      return comparisonSelectivityImpl<velox::TypeKind::DOUBLE>(
          left,
          right,
          leftValue,
          rightValue,
          funcName,
          updateConstraints,
          constraints);
    default:
      // Non-numeric types: use cardinality-based calculation
      double ac = std::max(1.0, static_cast<double>(leftValue.cardinality));
      double bc = std::max(1.0, static_cast<double>(rightValue.cardinality));

      double probTrue = 0.0;
      if (isName(funcName, "eq")) {
        double minCard = std::min(ac, bc);
        probTrue = minCard / (ac * bc);

        // Update constraints if requested
        if (updateConstraints && left && right) {
          Value newConstraint(leftValue.type, minCard);
          newConstraint.nullFraction = 0.0f;

          int32_t leftId = left->id();
          int32_t rightId = right->id();
          constraints.erase(leftId);
          constraints.emplace(leftId, newConstraint);
          constraints.erase(rightId);
          constraints.emplace(rightId, newConstraint);
        }
      } else {
        probTrue = 0.5;
      }

      double trueFrac = probTrue * (1.0 - nullFrac);
      return {trueFrac, nullFrac};
  }
}

Selectivity rangeSelectivity(
    const PlanState& state,
    ExprCP expr,
    const velox::Variant* lower,
    const velox::Variant* upper) {
  const auto& exprValue = value(state, expr);
  const auto kind = exprValue.type->kind();

  // Get null fraction from the expression
  double nullFrac = exprValue.nullFraction;

  // Handle non-scalar types explicitly
  float baseTrueFraction;
  switch (kind) {
    case velox::TypeKind::ARRAY:
    case velox::TypeKind::MAP:
    case velox::TypeKind::ROW:
    case velox::TypeKind::UNKNOWN:
    case velox::TypeKind::FUNCTION:
    case velox::TypeKind::OPAQUE:
    case velox::TypeKind::INVALID:
      baseTrueFraction = 0.1f;
      break;
    default:
      // Dispatch on scalar TypeKind using Velox macro
      baseTrueFraction = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          rangeSelectivityImpl, kind, exprValue, lower, upper);
      break;
  }

  // Scale true fraction by (1 - nullFraction)
  double trueFrac = baseTrueFraction * (1.0 - nullFrac);

  return {trueFrac, nullFrac};
}

void newConstraint(
    const PlanState& state,
    ExprCP expr,
    const velox::Variant* lower,
    const velox::Variant* upper,
    float cardinality,
    ConstraintMap& constraints) {
  const auto& oldValue = value(state, expr);

  // Determine min and max pointers
  const velox::Variant* minPtr = oldValue.min;
  const velox::Variant* maxPtr = oldValue.max;

  // Tighten bounds: take intersection of old and new constraints
  // For lower bound: max(old_min, new_lower) - highest lower bound wins
  if (lower != nullptr && oldValue.min != nullptr) {
    if (*oldValue.min < *lower) {
      minPtr = lower;
    } else {
      minPtr = oldValue.min;
    }
  } else if (lower != nullptr) {
    minPtr = lower;
  }

  // For upper bound: min(old_max, new_upper) - lowest upper bound wins
  if (upper != nullptr && oldValue.max != nullptr) {
    if (*upper < *oldValue.max) {
      maxPtr = upper;
    } else {
      maxPtr = oldValue.max;
    }
  } else if (upper != nullptr) {
    maxPtr = upper;
  }

  // cardinality is at least 1 distinct value. If the type is an integer type,
  // then the cardinality is not more than 1 + (max - min).
  float finalCardinality = std::max(1.0f, cardinality);
  auto kind = oldValue.type->kind();

  // If the type is an integer type and both upper and lower are set (after
  // defaulting), adjust cardinality based on range
  if (isIntegerKind(kind) && minPtr != nullptr && maxPtr != nullptr) {
    // Calculate range-based cardinality bound: 1 + (upper - lower)
    float rangeCardinality = 0.0f;
    switch (kind) {
      case velox::TypeKind::TINYINT: {
        auto upperVal = maxPtr->value<velox::TypeKind::TINYINT>();
        auto lowerVal = minPtr->value<velox::TypeKind::TINYINT>();
        rangeCardinality = 1.0f + static_cast<float>(upperVal - lowerVal);
        break;
      }
      case velox::TypeKind::SMALLINT: {
        auto upperVal = maxPtr->value<velox::TypeKind::SMALLINT>();
        auto lowerVal = minPtr->value<velox::TypeKind::SMALLINT>();
        rangeCardinality = 1.0f + static_cast<float>(upperVal - lowerVal);
        break;
      }
      case velox::TypeKind::INTEGER: {
        auto upperVal = maxPtr->value<velox::TypeKind::INTEGER>();
        auto lowerVal = minPtr->value<velox::TypeKind::INTEGER>();
        rangeCardinality = 1.0f + static_cast<float>(upperVal - lowerVal);
        break;
      }
      case velox::TypeKind::BIGINT: {
        auto upperVal = maxPtr->value<velox::TypeKind::BIGINT>();
        auto lowerVal = minPtr->value<velox::TypeKind::BIGINT>();
        rangeCardinality = 1.0f + static_cast<float>(upperVal - lowerVal);
        break;
      }
      case velox::TypeKind::HUGEINT: {
        auto upperVal = maxPtr->value<velox::TypeKind::HUGEINT>();
        auto lowerVal = minPtr->value<velox::TypeKind::HUGEINT>();
        rangeCardinality = 1.0f + static_cast<float>(upperVal - lowerVal);
        break;
      }
      default:
        rangeCardinality = cardinality;
        break;
    }

    // Set cardinality to the minimum of the provided cardinality and range
    // cardinality
    finalCardinality = std::min(finalCardinality, rangeCardinality);
  }

  // Create a new Value with updated fields
  Value newValue(oldValue.type, finalCardinality);
  newValue.min = minPtr;
  newValue.max = maxPtr;
  newValue.trueFraction = oldValue.trueFraction;
  newValue.nullFraction = 0;
  newValue.nullable = false;

  // Add to constraints using expr->id()
  // Erase existing entry if present, then emplace new one (Value has const
  // fields)
  constraints.erase(expr->id());
  constraints.emplace(expr->id(), newValue);
}

Selectivity rangeSelectivity(
    const PlanState& state,
    std::span<const ExprCP> exprs,
    bool updateConstraints,
    ConstraintMap& newConstraints) {
  // Validate that exprs is at least size 1
  VELOX_CHECK_GE(exprs.size(), 1, "exprs must have at least one element");

  // Validate that the first element is a call
  VELOX_CHECK(
      exprs[0]->is(PlanType::kCallExpr), "First element must be a call");

  // Set leftSide to be the first argument of the first call
  auto firstCall = exprs[0]->as<Call>();
  VELOX_CHECK_GE(
      firstCall->args().size(), 1, "Call must have at least one argument");
  ExprCP leftSide = firstCall->args()[0];

  // Get null fraction from left side
  const auto& val = value(state, leftSide);
  double nullFrac = val.nullFraction;

  bool empty = false;
  const velox::Variant* lower = nullptr;
  const velox::Variant* upper = nullptr;
  std::optional<double> eqSelectivity;
  const velox::Variant* eqValue = nullptr;
  const velox::Variant* inList = nullptr;

  // Loop over the expressions
  for (auto* expr : exprs) {
    // Check that all elements are calls
    VELOX_CHECK(expr->is(PlanType::kCallExpr), "All elements must be calls");

    auto call = expr->as<Call>();
    auto funcName = call->name();

    // Handle IN function
    if (funcName == SpecialFormCallNames::kIn) {
      // Extract IN list using helper function
      const velox::Variant* currentInList = getInListFromCall(call);

      // Check for null literals in the IN list
      const auto& array = currentInList->array();
      for (const auto& elem : array) {
        if (elem.isNull()) {
          return {0.0, nullFrac};
        }
      }

      if (inList == nullptr) {
        inList = currentInList;
      } else {
        // Intersect with existing IN list
        inList = intersectArrayVariants(inList, currentInList);
      }
    }
    // Handle eq function
    else if (isName(funcName, "eq")) {
      VELOX_CHECK_EQ(
          call->args().size(), 2, "eq must have exactly 2 arguments");
      ExprCP rhs = call->args()[1];

      if (rhs->is(PlanType::kLiteralExpr)) {
        auto literal = rhs->as<Literal>();
        const auto& litValue = literal->literal();

        // Check for null literal
        if (litValue.isNull()) {
          return {0.0, nullFrac};
        }

        if (!eqSelectivity.has_value()) {
          eqSelectivity = 1.0 / val.cardinality;
          eqValue = &litValue;
        } else {
          // Check if the literal is different
          if (eqValue != nullptr && !(eqValue->equals(litValue))) {
            empty = true;
          }
        }
      }
    }
    // Handle lt or lte functions
    else if (isName(funcName, "lt") || isName(funcName, "lte")) {
      VELOX_CHECK_EQ(
          call->args().size(), 2, "lt/lte must have exactly 2 arguments");
      ExprCP rhs = call->args()[1];

      if (rhs->is(PlanType::kLiteralExpr)) {
        auto literal = rhs->as<Literal>();
        const auto& litValue = literal->literal();

        // Check for null literal
        if (litValue.isNull()) {
          return {0.0, nullFrac};
        }

        if (upper == nullptr || litValue < *upper) {
          upper = &litValue;
        }
      }
    }
    // Handle gt or gte functions
    else if (isName(funcName, "gt") || isName(funcName, "gte")) {
      VELOX_CHECK_EQ(
          call->args().size(), 2, "gt/gte must have exactly 2 arguments");
      ExprCP rhs = call->args()[1];

      if (rhs->is(PlanType::kLiteralExpr)) {
        auto literal = rhs->as<Literal>();
        const auto& litValue = literal->literal();

        // Check for null literal
        if (litValue.isNull()) {
          return {0.0, nullFrac};
        }

        if (lower == nullptr || *lower < litValue) {
          lower = &litValue;
        }
      }
    }
  }

  // Calculate effective upper and lower bounds
  // These are the more restrictive of query conditions and value min/max
  const velox::Variant* effectiveLower = lower;
  const velox::Variant* effectiveUpper = upper;

  // Effective lower is the maximum of query lower and value min
  if (val.min != nullptr) {
    if (effectiveLower == nullptr || *effectiveLower < *val.min) {
      effectiveLower = val.min;
    }
  }

  // Effective upper is the minimum of query upper and value max
  if (val.max != nullptr) {
    if (effectiveUpper == nullptr || *val.max < *effectiveUpper) {
      effectiveUpper = val.max;
    }
  }

  // Apply constraints to the IN list using helper function
  if (inList != nullptr) {
    inList = pruneInList(inList, effectiveLower, effectiveUpper);

    // Check if IN list is empty after pruning
    if (inList->array().empty()) {
      empty = true;
    }
  }

  // Check for contradictory constraints
  if (inList != nullptr && eqValue != nullptr && !empty) {
    // Check if eq is in the in list using helper function
    if (!isValueInArray(*eqValue, inList)) {
      empty = true;
    }
  }

  if (eqValue != nullptr && effectiveLower != nullptr && !empty) {
    if (*eqValue < *effectiveLower) {
      empty = true;
    }
  }

  if (eqValue != nullptr && effectiveUpper != nullptr && !empty) {
    if (*effectiveUpper < *eqValue) {
      empty = true;
    }
  }

  // Calculate selectivity and update constraints
  if (empty) {
    return {0.0, nullFrac};
  }

  // After looping over the exprs
  if (eqValue != nullptr) {
    if (updateConstraints) {
      newConstraint(state, leftSide, eqValue, eqValue, 1.0f, newConstraints);
    }
    // Scale by (1 - nullFraction) since NULLs won't match
    return {eqSelectivity.value() * (1.0 - nullFrac), nullFrac};
  }

  if (inList != nullptr) {
    const auto& array = inList->array();

    double inListSize = static_cast<double>(array.size());
    double trueFrac =
        std::clamp(inListSize / val.cardinality, 0.0, 1.0) * (1.0 - nullFrac);

    if (updateConstraints) {
      // Find min and max from IN list
      const velox::Variant* minVal = nullptr;
      const velox::Variant* maxVal = nullptr;

      if (!array.empty()) {
        minVal = &array[0];
        maxVal = &array[0];

        for (size_t i = 1; i < array.size(); ++i) {
          if (array[i] < *minVal) {
            minVal = &array[i];
          }
          if (*maxVal < array[i]) {
            maxVal = &array[i];
          }
        }
      }

      newConstraint(
          state, leftSide, minVal, maxVal, inListSize, newConstraints);
    }

    return {trueFrac, nullFrac};
  }

  if (upper != nullptr || lower != nullptr) {
    // Check if effective bounds are contradictory
    if (effectiveLower != nullptr && effectiveUpper != nullptr) {
      if (*effectiveUpper < *effectiveLower) {
        return {0.0, nullFrac};
      }
    }

    Selectivity selectivity = rangeSelectivity(state, leftSide, lower, upper);

    if (updateConstraints) {
      newConstraint(
          state,
          leftSide,
          lower,
          upper,
          val.cardinality * selectivity.trueFraction,
          newConstraints);
    }

    return selectivity;
  }

  // Default case
  return {0.5 * (1.0 - nullFrac), nullFrac};
}

/// Declared in namespace to allow calling from debugger.
std::string constraintsString(ConstraintMap& constraints) {
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
