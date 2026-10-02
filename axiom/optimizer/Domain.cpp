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
#include <limits>

#include "axiom/optimizer/PlanUtils.h"
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

Domain Domain::subtract(const Domain& other) const {
  return intersect(other.complement());
}

Domain Domain::complement() const {
  std::vector<Range> result;
  std::optional<Bound> low;
  for (const auto& range : ranges_) {
    if (range.low().has_value()) {
      result.emplace_back(
          low, Bound{range.low()->value, !range.low()->inclusive});
    }
    if (!range.high().has_value()) {
      return Domain(!nullsAllowed_, std::move(result));
    }
    low = Bound{range.high()->value, !range.high()->inclusive};
  }
  result.emplace_back(std::move(low), std::nullopt);
  return Domain(!nullsAllowed_, std::move(result));
}

bool Domain::operator==(const Domain& other) const {
  if (nullsAllowed_ != other.nullsAllowed_ ||
      ranges_.size() != other.ranges_.size()) {
    return false;
  }
  for (size_t i = 0; i < ranges_.size(); ++i) {
    const auto sameBound = [](const std::optional<Bound>& lhs,
                              const std::optional<Bound>& rhs) {
      if (lhs.has_value() != rhs.has_value()) {
        return false;
      }
      return !lhs.has_value() ||
          (lhs->inclusive == rhs->inclusive && lhs->value == rhs->value);
    };
    if (!sameBound(ranges_[i].low(), other.ranges_[i].low()) ||
        !sameBound(ranges_[i].high(), other.ranges_[i].high())) {
      return false;
    }
  }
  return true;
}

bool Domain::isAll() const {
  return nullsAllowed_ && ranges_.size() == 1 && !ranges_[0].low() &&
      !ranges_[0].high();
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

std::optional<Domain> integralTypeDomain(const velox::Type& type) {
  if (type.isDate() || type.isIntervalDayTime() || type.isIntervalYearMonth() ||
      type.isDecimal() || type.isTime()) {
    return std::nullopt;
  }

  int64_t minimum;
  int64_t maximum;
  switch (type.kind()) {
    case velox::TypeKind::TINYINT:
      minimum = std::numeric_limits<int8_t>::min();
      maximum = std::numeric_limits<int8_t>::max();
      break;
    case velox::TypeKind::SMALLINT:
      minimum = std::numeric_limits<int16_t>::min();
      maximum = std::numeric_limits<int16_t>::max();
      break;
    case velox::TypeKind::INTEGER:
      minimum = std::numeric_limits<int32_t>::min();
      maximum = std::numeric_limits<int32_t>::max();
      break;
    case velox::TypeKind::BIGINT:
      minimum = std::numeric_limits<int64_t>::min();
      maximum = std::numeric_limits<int64_t>::max();
      break;
    default:
      return std::nullopt;
  }
  return Domain::greaterThanOrEqual(velox::Variant(minimum))
      .intersect(Domain::lessThanOrEqual(velox::Variant(maximum)));
}

namespace {

enum class PredicateResult { kTrue, kFalse };

PredicateResult opposite(PredicateResult result) {
  return result == PredicateResult::kTrue ? PredicateResult::kFalse
                                          : PredicateResult::kTrue;
}

ColumnCP domainColumn(ExprCP expr) {
  if (expr->is(PlanType::kColumnExpr)) {
    return expr->as<Column>();
  }
  if (!expr->is(PlanType::kCallExpr)) {
    return nullptr;
  }

  const auto* call = expr->as<Call>();
  if ((call->name() != SpecialFormCallNames::kCast &&
       call->name() != SpecialFormCallNames::kTryCast) ||
      call->args().size() != 1) {
    return nullptr;
  }

  const auto* column = domainColumn(call->args().front());
  if (column == nullptr ||
      !integralTypeDomain(*column->value().type).has_value() ||
      !integralTypeDomain(*expr->value().type).has_value() ||
      column->value().type->cppSizeInBytes() >
          expr->value().type->cppSizeInBytes()) {
    return nullptr;
  }
  return column;
}

Domain nonNullDomain(ColumnCP column) {
  auto domain = integralTypeDomain(*column->value().type);
  return domain.has_value() ? std::move(*domain) : Domain::notNull();
}

std::optional<velox::Variant> normalizeLiteral(
    ColumnCP column,
    const velox::Variant& literal) {
  if (literal.isNull() ||
      !integralTypeDomain(*column->value().type).has_value()) {
    return literal;
  }
  switch (literal.kind()) {
    case velox::TypeKind::TINYINT:
    case velox::TypeKind::SMALLINT:
    case velox::TypeKind::INTEGER:
    case velox::TypeKind::BIGINT:
      return velox::Variant(integerValue(&literal));
    default:
      return std::nullopt;
  }
}

std::optional<Domain> comparisonDomain(
    ColumnCP column,
    Name functionName,
    const velox::Variant& literal,
    PredicateResult result) {
  const auto& functionNames = queryCtx()->functionNames();
  if (functionName == functionNames.distinctFrom) {
    auto value = normalizeLiteral(column, literal);
    if (!value.has_value()) {
      return std::nullopt;
    }
    const auto nonNullValues = nonNullDomain(column);
    auto equalValues = value->isNull()
        ? Domain::onlyNull()
        : Domain::singleValue(std::move(*value)).intersect(nonNullValues);
    const auto allValues = nonNullValues.unite(Domain::onlyNull());
    return result == PredicateResult::kTrue ? allValues.subtract(equalValues)
                                            : std::move(equalValues);
  }

  Domain (*makeTrueValues)(velox::Variant){nullptr};
  if (functionName == functionNames.equality) {
    makeTrueValues = &Domain::singleValue;
  } else if (functionName == functionNames.lt) {
    makeTrueValues = &Domain::lessThan;
  } else if (functionName == functionNames.lte) {
    makeTrueValues = &Domain::lessThanOrEqual;
  } else if (functionName == functionNames.gt) {
    makeTrueValues = &Domain::greaterThan;
  } else if (functionName == functionNames.gte) {
    makeTrueValues = &Domain::greaterThanOrEqual;
  } else {
    return std::nullopt;
  }

  auto value = normalizeLiteral(column, literal);
  if (!value.has_value()) {
    return std::nullopt;
  }
  // eq, lt, lte, gt and gte return NULL for a NULL literal, so no row makes
  // them TRUE or FALSE.
  if (value->isNull()) {
    return Domain::none();
  }

  const auto validValues = nonNullDomain(column);
  auto trueValues = makeTrueValues(std::move(*value)).intersect(validValues);
  return result == PredicateResult::kTrue ? std::move(trueValues)
                                          : validValues.subtract(trueValues);
}

// Returns the exclusive upper bound for all strings that start with 'prefix':
// increments the last byte below 0xFF and drops the bytes after it. Returns
// std::nullopt when every byte is 0xFF, in which case there is no finite upper
// bound. Comparison is byte-wise, matching VARCHAR ordering.
std::optional<std::string> prefixUpperBound(std::string_view prefix) {
  std::string upper{prefix};
  while (!upper.empty()) {
    if (static_cast<unsigned char>(upper.back()) != 0xFF) {
      ++upper.back();
      return upper;
    }
    upper.pop_back();
  }
  return std::nullopt;
}

// Converts a LIKE pattern to a Domain. A pattern with a fixed literal prefix
// before the first wildcard ('%' or '_') maps to the half-open range
// [prefix, prefixUpperBound(prefix)); a wildcard-free pattern is exact
// equality. Returns std::nullopt (broader is safe) when the pattern has no
// leading literal, i.e. it starts with a wildcard.
std::optional<Domain> likePatternToDomain(const velox::Variant& pattern) {
  if (pattern.isNull() || pattern.kind() != velox::TypeKind::VARCHAR) {
    return std::nullopt;
  }
  const auto& text = pattern.value<velox::TypeKind::VARCHAR>();
  const auto wildcard = text.find_first_of("%_");
  if (wildcard == std::string::npos) {
    return Domain::singleValue(pattern);
  }
  if (wildcard == 0) {
    return std::nullopt;
  }
  std::string prefix = text.substr(0, wildcard);
  auto upper = prefixUpperBound(prefix);
  Domain domain = Domain::greaterThanOrEqual(velox::Variant(std::move(prefix)));
  if (upper) {
    domain =
        domain.intersect(Domain::lessThan(velox::Variant(std::move(*upper))));
  }
  return domain;
}

std::optional<Domain>
exprToDomain(ExprCP expr, ColumnCP column, PredicateResult result) {
  if (!expr->is(PlanType::kCallExpr)) {
    return std::nullopt;
  }

  const auto* call = expr->as<Call>();
  const auto& args = call->args();
  const auto name = call->name();
  const auto& functionNames = queryCtx()->functionNames();

  if (name == functionNames.negation && args.size() == 1) {
    return exprToDomain(args.front(), column, opposite(result));
  }

  if (name == SpecialFormCallNames::kAnd || name == SpecialFormCallNames::kOr) {
    if (args.empty()) {
      return std::nullopt;
    }
    const bool intersect = (name == SpecialFormCallNames::kAnd) ==
        (result == PredicateResult::kTrue);
    auto domain = exprToDomain(args.front(), column, result);
    if (!domain.has_value()) {
      return std::nullopt;
    }
    for (size_t i = 1; i < args.size(); ++i) {
      auto next = exprToDomain(args[i], column, result);
      if (!next.has_value()) {
        return std::nullopt;
      }
      domain = intersect ? domain->intersect(*next) : domain->unite(*next);
    }
    return domain;
  }

  if (name == functionNames.isNull && args.size() == 1 &&
      domainColumn(args.front()) == column) {
    return result == PredicateResult::kTrue ? Domain::onlyNull()
                                            : nonNullDomain(column);
  }

  if (name == SpecialFormCallNames::kIn && args.size() >= 2 &&
      domainColumn(args.front()) == column) {
    std::vector<velox::Variant> values;
    bool hasNull{false};
    for (size_t i = 1; i < args.size(); ++i) {
      if (!args[i]->is(PlanType::kLiteralExpr)) {
        return std::nullopt;
      }
      auto value = normalizeLiteral(column, args[i]->as<Literal>()->literal());
      if (!value.has_value()) {
        return std::nullopt;
      }
      if (value->isNull()) {
        hasNull = true;
      } else {
        values.push_back(std::move(*value));
      }
    }
    auto trueValues =
        Domain::in(std::move(values)).intersect(nonNullDomain(column));
    if (result == PredicateResult::kTrue) {
      return trueValues;
    }
    return hasNull ? Domain::none()
                   : nonNullDomain(column).subtract(trueValues);
  }

  if (name == functionNames.between && args.size() == 3 &&
      domainColumn(args[0]) == column && args[1]->is(PlanType::kLiteralExpr) &&
      args[2]->is(PlanType::kLiteralExpr)) {
    auto lower = comparisonDomain(
        column, functionNames.gte, args[1]->as<Literal>()->literal(), result);
    auto upper = comparisonDomain(
        column, functionNames.lte, args[2]->as<Literal>()->literal(), result);
    if (!lower.has_value() || !upper.has_value()) {
      return std::nullopt;
    }
    return result == PredicateResult::kTrue ? lower->intersect(*upper)
                                            : lower->unite(*upper);
  }

  if (name == functionNames.like && result == PredicateResult::kTrue &&
      args.size() == 2 && domainColumn(args[0]) == column &&
      args[1]->is(PlanType::kLiteralExpr)) {
    return likePatternToDomain(args[1]->as<Literal>()->literal());
  }

  if (args.size() != 2) {
    return std::nullopt;
  }
  if (domainColumn(args[0]) == column && args[1]->is(PlanType::kLiteralExpr)) {
    return comparisonDomain(
        column, name, args[1]->as<Literal>()->literal(), result);
  }
  if (domainColumn(args[1]) == column && args[0]->is(PlanType::kLiteralExpr)) {
    Name reversed = name;
    if (reversed == functionNames.lt) {
      reversed = functionNames.gt;
    } else if (reversed == functionNames.lte) {
      reversed = functionNames.gte;
    } else if (reversed == functionNames.gt) {
      reversed = functionNames.lt;
    } else if (reversed == functionNames.gte) {
      reversed = functionNames.lte;
    }
    return comparisonDomain(
        column, reversed, args[0]->as<Literal>()->literal(), result);
  }
  return std::nullopt;
}

} // namespace

std::optional<Domain> exprToDomain(ExprCP expr) {
  if (expr->columns().size() != 1) {
    return std::nullopt;
  }
  return exprToDomain(
      expr, expr->columns().onlyObject<Column>(), PredicateResult::kTrue);
}

} // namespace facebook::axiom::optimizer
