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

#include "axiom/optimizer/Domain.h"

#include <algorithm>

#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/QueryGraphContext.h"

namespace facebook::axiom::optimizer {

namespace {

// Returns -1, 0, or 1 comparing two Bound values.
int compareBoundValues(const velox::Variant& lhs, const velox::Variant& rhs) {
  if (lhs < rhs) {
    return -1;
  }
  if (rhs < lhs) {
    return 1;
  }
  return 0;
}

// Compares two low bounds. Unbounded (nullopt) sorts before any value.
// For equal values, inclusive sorts before exclusive.
int compareLowBounds(
    const std::optional<Bound>& lhs,
    const std::optional<Bound>& rhs) {
  if (!lhs && !rhs) {
    return 0;
  }
  if (!lhs) {
    return -1;
  }
  if (!rhs) {
    return 1;
  }
  int compareResult = compareBoundValues(lhs->value, rhs->value);
  if (compareResult != 0) {
    return compareResult;
  }
  // Both have equal values. Inclusive < exclusive for low bounds.
  if (lhs->inclusive == rhs->inclusive) {
    return 0;
  }
  return lhs->inclusive ? -1 : 1;
}

// Compares two high bounds. Unbounded (nullopt) sorts after any value.
// For equal values, inclusive sorts after exclusive.
int compareHighBounds(
    const std::optional<Bound>& lhs,
    const std::optional<Bound>& rhs) {
  if (!lhs && !rhs) {
    return 0;
  }
  if (!lhs) {
    return 1;
  }
  if (!rhs) {
    return -1;
  }
  int compareResult = compareBoundValues(lhs->value, rhs->value);
  if (compareResult != 0) {
    return compareResult;
  }
  // Both have equal values. Inclusive > exclusive for high bounds.
  if (lhs->inclusive == rhs->inclusive) {
    return 0;
  }
  return lhs->inclusive ? 1 : -1;
}

// Returns the max of two low bounds.
const std::optional<Bound>& maxLow(
    const std::optional<Bound>& lhs,
    const std::optional<Bound>& rhs) {
  return compareLowBounds(lhs, rhs) >= 0 ? lhs : rhs;
}

// Returns the min of two low bounds.
const std::optional<Bound>& minLow(
    const std::optional<Bound>& lhs,
    const std::optional<Bound>& rhs) {
  return compareLowBounds(lhs, rhs) <= 0 ? lhs : rhs;
}

// Returns the max of two high bounds.
const std::optional<Bound>& maxHigh(
    const std::optional<Bound>& lhs,
    const std::optional<Bound>& rhs) {
  return compareHighBounds(lhs, rhs) >= 0 ? lhs : rhs;
}

// Returns the min of two high bounds.
const std::optional<Bound>& minHigh(
    const std::optional<Bound>& lhs,
    const std::optional<Bound>& rhs) {
  return compareHighBounds(lhs, rhs) <= 0 ? lhs : rhs;
}

// Returns true if low <= high (i.e. the range is non-empty).
bool lowNotAboveHigh(
    const std::optional<Bound>& low,
    const std::optional<Bound>& high) {
  if (!low || !high) {
    return true;
  }
  int compareResult = compareBoundValues(low->value, high->value);
  if (compareResult < 0) {
    return true;
  }
  if (compareResult > 0) {
    return false;
  }
  // Equal values: both must be inclusive for the range to be non-empty.
  return low->inclusive && high->inclusive;
}

} // namespace

// static
Range Range::singleValue(velox::Variant value) {
  Bound bound{value, true};
  return Range(std::move(bound), Bound{std::move(value), true});
}

bool Range::isSingleValue() const {
  return low_.has_value() && high_.has_value() && low_->inclusive &&
      high_->inclusive && low_->value == high_->value;
}

bool Range::isEmpty() const {
  return !lowNotAboveHigh(low_, high_);
}

std::optional<Range> Range::intersect(const Range& other) const {
  const auto& newLow = maxLow(low_, other.low_);
  const auto& newHigh = minHigh(high_, other.high_);
  if (!lowNotAboveHigh(newLow, newHigh)) {
    return std::nullopt;
  }
  return Range(newLow, newHigh);
}

bool Range::overlapsOrAdjacent(const Range& other) const {
  // Two ranges overlap or are adjacent if the higher low bound does not exceed
  // the lower high bound. For adjacency with exclusive bounds, we check value
  // equality since (_, x) and [x, _) are adjacent.
  const auto& higherLow = maxLow(low_, other.low_);
  const auto& lowerHigh = minHigh(high_, other.high_);

  if (!higherLow || !lowerHigh) {
    return true;
  }

  int compareResult = compareBoundValues(higherLow->value, lowerHigh->value);
  if (compareResult < 0) {
    return true;
  }
  if (compareResult > 0) {
    return false;
  }
  // Equal values: overlapping if at least one bound is inclusive on each side.
  return higherLow->inclusive || lowerHigh->inclusive;
}

Range Range::merge(const Range& other) const {
  return Range(minLow(low_, other.low_), maxHigh(high_, other.high_));
}

// static
Domain Domain::singleValue(velox::Variant value) {
  return Domain(false, {Range::singleValue(std::move(value))});
}

// static
Domain Domain::greaterThan(velox::Variant value) {
  return Domain(false, {Range(Bound{std::move(value), false}, std::nullopt)});
}

// static
Domain Domain::greaterThanOrEqual(velox::Variant value) {
  return Domain(false, {Range(Bound{std::move(value), true}, std::nullopt)});
}

// static
Domain Domain::lessThan(velox::Variant value) {
  return Domain(false, {Range(std::nullopt, Bound{std::move(value), false})});
}

// static
Domain Domain::lessThanOrEqual(velox::Variant value) {
  return Domain(false, {Range(std::nullopt, Bound{std::move(value), true})});
}

// static
Domain Domain::in(std::vector<velox::Variant> values) {
  std::vector<Range> ranges;
  ranges.reserve(values.size());
  for (auto& value : values) {
    ranges.push_back(Range::singleValue(std::move(value)));
  }
  return Domain(false, normalize(std::move(ranges)));
}

Domain Domain::intersect(const Domain& other) const {
  bool newNullsAllowed = nullsAllowed_ && other.nullsAllowed_;

  // Two-pointer merge: for each pair of ranges, compute intersection.
  std::vector<Range> result;
  size_t left = 0;
  size_t right = 0;
  while (left < ranges_.size() && right < other.ranges_.size()) {
    auto intersection = ranges_[left].intersect(other.ranges_[right]);
    if (intersection.has_value()) {
      result.push_back(std::move(*intersection));
    }

    // Advance the pointer whose high bound is smaller.
    if (compareHighBounds(ranges_[left].high(), other.ranges_[right].high()) <=
        0) {
      ++left;
    } else {
      ++right;
    }
  }

  return Domain(newNullsAllowed, std::move(result));
}

Domain Domain::unite(const Domain& other) const {
  bool newNullsAllowed = nullsAllowed_ || other.nullsAllowed_;

  std::vector<Range> combined;
  combined.reserve(ranges_.size() + other.ranges_.size());
  combined.insert(combined.end(), ranges_.begin(), ranges_.end());
  combined.insert(combined.end(), other.ranges_.begin(), other.ranges_.end());

  return Domain(newNullsAllowed, normalize(std::move(combined)));
}

bool Domain::isAll() const {
  return ranges_.size() == 1 && !ranges_[0].low() && !ranges_[0].high();
}

bool Domain::isNone() const {
  return !nullsAllowed_ && ranges_.empty();
}

// static
std::vector<Range> Domain::normalize(std::vector<Range> ranges) {
  if (ranges.empty()) {
    return ranges;
  }

  // Sort by low bound.
  std::sort(
      ranges.begin(), ranges.end(), [](const Range& lhs, const Range& rhs) {
        return compareLowBounds(lhs.low(), rhs.low()) < 0;
      });

  std::vector<Range> result;
  result.push_back(std::move(ranges[0]));
  for (size_t i = 1; i < ranges.size(); ++i) {
    if (result.back().overlapsOrAdjacent(ranges[i])) {
      result.back() = result.back().merge(ranges[i]);
    } else {
      result.push_back(std::move(ranges[i]));
    }
  }

  return result;
}

std::optional<Domain> exprToDomain(ExprCP expr) {
  if (!expr->is(PlanType::kCallExpr)) {
    return std::nullopt;
  }

  auto* call = expr->as<Call>();
  auto funcName = call->name();

  // AND: intersect all children.
  if (funcName == SpecialFormCallNames::kAnd) {
    Domain result = Domain::all();
    for (auto* arg : call->args()) {
      if (auto argDomain = exprToDomain(arg)) {
        result = result.intersect(*argDomain);
      } else {
        return std::nullopt;
      }
    }
    return result;
  }

  // OR: unite all children.
  if (funcName == SpecialFormCallNames::kOr) {
    Domain result = Domain::none();
    for (auto* arg : call->args()) {
      if (auto argDomain = exprToDomain(arg)) {
        result = result.unite(*argDomain);
      } else {
        return std::nullopt;
      }
    }
    return result;
  }

  const auto& functionNames = queryCtx()->functionNames();

  // IS NULL.
  if (funcName == functionNames.isNull) {
    return Domain::onlyNull();
  }

  // IN(col, val1, val2, ...).
  if (funcName == SpecialFormCallNames::kIn) {
    std::vector<velox::Variant> values;
    for (size_t i = 1; i < call->args().size(); ++i) {
      if (call->args()[i]->is(PlanType::kLiteralExpr)) {
        values.push_back(call->args()[i]->as<Literal>()->literal());
      } else {
        return std::nullopt;
      }
    }
    return Domain::in(std::move(values));
  }

  // BETWEEN(col, low, high) maps to the closed range [low, high].
  if (funcName == functionNames.between && call->args().size() == 3 &&
      call->args()[0]->is(PlanType::kColumnExpr) &&
      call->args()[1]->is(PlanType::kLiteralExpr) &&
      call->args()[2]->is(PlanType::kLiteralExpr)) {
    const auto& low = call->args()[1]->as<Literal>()->literal();
    const auto& high = call->args()[2]->as<Literal>()->literal();
    return Domain::greaterThanOrEqual(low).intersect(
        Domain::lessThanOrEqual(high));
  }

  // Comparison of a column with a literal: eq, lt, lte, gt, gte.
  if (call->args().size() == 2 && call->args()[0]->is(PlanType::kColumnExpr) &&
      call->args()[1]->is(PlanType::kLiteralExpr)) {
    const auto& literalValue = call->args()[1]->as<Literal>()->literal();
    if (funcName == functionNames.equality) {
      return Domain::singleValue(literalValue);
    }
    if (funcName == functionNames.lt) {
      return Domain::lessThan(literalValue);
    }
    if (funcName == functionNames.lte) {
      return Domain::lessThanOrEqual(literalValue);
    }
    if (funcName == functionNames.gt) {
      return Domain::greaterThan(literalValue);
    }
    if (funcName == functionNames.gte) {
      return Domain::greaterThanOrEqual(literalValue);
    }
  }

  // Unrecognized expression.
  return std::nullopt;
}

} // namespace facebook::axiom::optimizer
