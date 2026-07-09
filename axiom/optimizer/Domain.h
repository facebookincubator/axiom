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

#include <optional>
#include <vector>

#include "velox/type/Variant.h"

namespace facebook::axiom::optimizer {

/// Represents a bound of a range (low or high).
struct Bound {
  velox::Variant value;
  bool inclusive;
};

/// A contiguous range of values with inclusive/exclusive bounds. Either bound
/// can be absent (unbounded).
class Range {
 public:
  /// Creates a single-value range: [value, value].
  static Range singleValue(velox::Variant value);

  /// Creates an unbounded range: (-inf, +inf).
  static Range unbounded() {
    return Range(std::nullopt, std::nullopt);
  }

  Range(std::optional<Bound> low, std::optional<Bound> high)
      : low_(std::move(low)), high_(std::move(high)) {}

  const std::optional<Bound>& low() const {
    return low_;
  }

  const std::optional<Bound>& high() const {
    return high_;
  }

  bool lowInclusive() const {
    return low_.has_value() && low_->inclusive;
  }

  bool highInclusive() const {
    return high_.has_value() && high_->inclusive;
  }

  /// Returns true if both bounds are present, equal, and inclusive.
  bool isSingleValue() const;

  /// Returns true if the range contains no values.
  bool isEmpty() const;

  /// Returns the intersection of two ranges, or std::nullopt if disjoint.
  std::optional<Range> intersect(const Range& other) const;

  /// Returns true if this range overlaps or is adjacent to 'other'.
  bool overlapsOrAdjacent(const Range& other) const;

  /// Merges two overlapping or adjacent ranges into one.
  Range merge(const Range& other) const;

 private:
  std::optional<Bound> low_;
  std::optional<Bound> high_;
};

/// Represents a domain: a set of values expressed as a union of sorted,
/// non-overlapping ranges plus a nullsAllowed flag.
///
/// Supports intersect (AND) and unite (OR) operations for composing
/// constraints from nested boolean expressions.
///
/// Distinct from optimizer::Value (Schema.h) which is a statistical estimate
/// that may be inaccurate. Domain values are guaranteed to be correct: the
/// actual constraint is always equal to or narrower than the Domain.
class Domain {
 public:
  /// Unconstrained domain: all values including null.
  static Domain all() {
    return Domain(true, {Range::unbounded()});
  }

  /// Empty domain: no values match.
  static Domain none() {
    return Domain(false, {});
  }

  /// Only null values match.
  static Domain onlyNull() {
    return Domain(true, {});
  }

  /// All non-null values match (unbounded range, nulls not allowed).
  static Domain notNull() {
    return Domain(false, {Range::unbounded()});
  }

  /// Exactly one value, not null.
  static Domain singleValue(velox::Variant value);

  /// Open lower bound: (value, +inf), not null.
  static Domain greaterThan(velox::Variant value);

  /// Closed lower bound: [value, +inf), not null.
  static Domain greaterThanOrEqual(velox::Variant value);

  /// Open upper bound: (-inf, value), not null.
  static Domain lessThan(velox::Variant value);

  /// Closed upper bound: (-inf, value], not null.
  static Domain lessThanOrEqual(velox::Variant value);

  /// Multiple discrete values (for IN predicate), not null.
  static Domain in(std::vector<velox::Variant> values);

  /// AND: intersection of two domains.
  Domain intersect(const Domain& other) const;

  /// OR: union of two domains.
  Domain unite(const Domain& other) const;

  /// Returns true if this domain is unconstrained (all non-null values match).
  /// Ignores nullsAllowed since a null-only constraint is not useful for IO.
  bool isAll() const;

  /// Returns true if no values match this domain.
  bool isNone() const;

  bool nullsAllowed() const {
    return nullsAllowed_;
  }

  const std::vector<Range>& ranges() const {
    return ranges_;
  }

 private:
  Domain(bool nullsAllowed, std::vector<Range> ranges)
      : nullsAllowed_(nullsAllowed), ranges_(std::move(ranges)) {}

  // Sorts ranges by low bound and merges overlapping/adjacent ranges.
  static std::vector<Range> normalize(std::vector<Range> ranges);

  bool nullsAllowed_;
  std::vector<Range> ranges_;
};

class Expr;
using ExprCP = const Expr*;

/// Converts a filter expression to a Domain. Returns std::nullopt if any
/// sub-expression cannot be converted.
///
/// Recognized expressions:
///   - Comparisons with literals: eq, lt, lte, gt, gte(column, literal)
///   - BETWEEN(column, literal, literal) — closed range [low, high]
///   - LIKE(column, literal) — prefix range, or equality if wildcard-free
///   - IN(column, literal, literal, ...)
///   - IS NULL(column)
///   - AND(expr, expr, ...) — intersects children
///   - OR(expr, expr, ...) — unites children
std::optional<Domain> exprToDomain(ExprCP expr);

} // namespace facebook::axiom::optimizer
