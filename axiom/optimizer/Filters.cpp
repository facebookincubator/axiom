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
#include <limits>

#include "axiom/optimizer/EstimateMath.h"
#include "axiom/optimizer/Filters.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/QueryGraphContext.h"
#include "axiom/optimizer/SelectivityEngine.h"

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

std::optional<Selectivity> combineConjuncts(
    std::span<const std::optional<Selectivity>> selectivities) {
  if (selectivities.empty()) {
    return Selectivity{1.0, 0.0};
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
    // An unknown conjunct makes the combined selectivity unknown.
    if (!selectivity.has_value()) {
      return std::nullopt;
    }
    trueProduct *= selectivity->trueFraction;
    trueOrNullProduct *=
        (selectivity->trueFraction + selectivity->nullFraction);
  }

  // Clamp to handle floating-point rounding.
  double resultNull = std::max(0.0, trueOrNullProduct - trueProduct);
  Selectivity result{trueProduct, resultNull};
  result.checkConsistency();
  return result;
}

std::optional<Selectivity> combineDisjuncts(
    std::span<const std::optional<Selectivity>> selectivities) {
  if (selectivities.empty()) {
    return Selectivity{0.0, 0.0};
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
    // An unknown disjunct makes the combined selectivity unknown.
    if (!selectivity.has_value()) {
      return std::nullopt;
    }
    notTrueProduct *= (1.0 - selectivity->trueFraction);
    falseProduct *=
        (1.0 - selectivity->trueFraction - selectivity->nullFraction);
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

    // No functionConstraint: get max cardinality from args. Unknown if any
    // arg's cardinality is unknown.
    std::optional<float> maxCardinality = 1.0f;
    for (auto* arg : call->args()) {
      Value argValue = exprConstraint(arg, constraints, update);
      maxCardinality = maxOf(maxCardinality, argValue.cardinality);
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

  // Clamp only when the cardinality is known; unknown stays unknown.
  if (typeKind == velox::TypeKind::BOOLEAN) {
    result.cardinality = minOf(result.cardinality, 2.0f);
  } else if (typeKind == velox::TypeKind::TINYINT) {
    result.cardinality = minOf(result.cardinality, 256.0f);
  } else if (typeKind == velox::TypeKind::SMALLINT) {
    result.cardinality = minOf(result.cardinality, 65536.0f);
  }
  return result;
}

namespace {

// Computes the maximum cardinality for an integer range: 1 + (max - min), or 0
// when max < min.
template <velox::TypeKind KIND>
float rangeCardinality(VariantCP minPtr, VariantCP maxPtr) {
  const auto upper = maxPtr->value<KIND>();
  const auto lower = minPtr->value<KIND>();
  if (upper < lower) {
    return 0;
  }
  // Subtracting in unsigned 128-bit arithmetic is exact for every integer
  // type. Converted to double first, bounds above 2^53 would round.
  const auto width = static_cast<velox::uint128_t>(upper) -
      static_cast<velox::uint128_t>(lower);
  return static_cast<float>(std::min(
      1.0 + static_cast<double>(width),
      static_cast<double>(std::numeric_limits<float>::max())));
}

} // namespace

std::optional<float>
rangeCardinality(TypeCP type, VariantCP min, VariantCP max) {
  const auto kind = type->kind();
  if (!detail::isIntegerKind(kind) || min == nullptr || max == nullptr) {
    return std::nullopt;
  }
  switch (kind) {
    case velox::TypeKind::TINYINT:
      return rangeCardinality<velox::TypeKind::TINYINT>(min, max);
    case velox::TypeKind::SMALLINT:
      return rangeCardinality<velox::TypeKind::SMALLINT>(min, max);
    case velox::TypeKind::INTEGER:
      return rangeCardinality<velox::TypeKind::INTEGER>(min, max);
    case velox::TypeKind::BIGINT:
      return rangeCardinality<velox::TypeKind::BIGINT>(min, max);
    case velox::TypeKind::HUGEINT:
      return rangeCardinality<velox::TypeKind::HUGEINT>(min, max);
    default:
      return std::nullopt;
  }
}

namespace {

// Navigation policy over the optimizer's ExprCP graph. Maintains a
// ConstraintMap so refined column statistics propagate to downstream operators.
struct ExprSelectivityPolicy {
  using Expr = ExprCP;
  using ColumnKey = ExprCP;
  static constexpr bool kUpdatesConstraints = true;

  ConstraintMap& constraints;

  Value valueOf(ExprCP expr) const {
    return value(constraints, expr);
  }

  bool isCall(ExprCP expr) const {
    return expr->is(PlanType::kCallExpr);
  }

  bool isColumn(ExprCP expr) const {
    return expr->is(PlanType::kColumnExpr);
  }

  bool isLiteral(ExprCP expr) const {
    return expr->is(PlanType::kLiteralExpr);
  }

  Name name(ExprCP expr) const {
    return expr->as<Call>()->name();
  }

  std::span<const ExprCP> args(ExprCP expr) const {
    return expr->as<Call>()->args();
  }

  const velox::Variant& literal(ExprCP expr) const {
    return expr->as<Literal>()->literal();
  }

  ColumnKey columnKey(ExprCP expr) const {
    return expr;
  }

  void seed(ExprCP conjunct) const {
    exprConstraint(conjunct, constraints, /*update=*/true);
  }

  void refine(ExprCP expr, const Value& value) const {
    constraints.insert_or_assign(expr->id(), value);
  }
};

} // namespace

std::optional<Selectivity> conjunctsSelectivity(
    ConstraintMap& constraints,
    std::span<const ExprCP> conjuncts,
    bool updateConstraints) {
  ExprSelectivityPolicy policy{constraints};
  SelectivityEngine<ExprSelectivityPolicy> engine{policy};
  return engine.conjunctsSelectivity(conjuncts, updateConstraints);
}

std::optional<Selectivity> exprSelectivity(
    ConstraintMap& constraints,
    ExprCP expr,
    bool updateConstraints) {
  ExprSelectivityPolicy policy{constraints};
  SelectivityEngine<ExprSelectivityPolicy> engine{policy};
  return engine.exprSelectivity(expr, updateConstraints);
}

std::optional<Selectivity> columnComparisonSelectivity(
    ExprCP left,
    ExprCP right,
    const Value& leftValue,
    const Value& rightValue,
    Name funcName,
    bool updateConstraints,
    ConstraintMap& constraints) {
  ExprSelectivityPolicy policy{constraints};
  SelectivityEngine<ExprSelectivityPolicy> engine{policy};
  return engine.columnComparisonSelectivity(
      left, right, leftValue, rightValue, funcName, updateConstraints);
}

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
