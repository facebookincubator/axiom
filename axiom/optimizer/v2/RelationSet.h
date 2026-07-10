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

#include <bit>
#include <cstddef>
#include <cstdint>
#include <functional>

#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::optimizer::v2 {

/// Fixed-capacity set of small integer ids backed by a single
/// 64-bit word. Used to identify subsets of a join cluster's
/// relations during cost-based join enumeration.
///
/// Invariant: every id passed to `add`, `erase`, or `contains`
/// must be in `[0, kMaxRelations)`. Violations are checked under
/// `VELOX_DCHECK`.
class RelationSet {
 public:
  /// Upper bound on representable ids (exclusive).
  static constexpr int32_t kMaxRelations = 64;

  RelationSet() = default;

  /// Constructs a set from a raw word; bit `i` set means id `i` is
  /// a member.
  explicit constexpr RelationSet(uint64_t bits) : bits_{bits} {}

  /// Returns a set containing only `id`.
  static RelationSet singleton(int32_t id) {
    RelationSet result;
    result.add(id);
    return result;
  }

  /// Returns the set of all ids up to and including `id`:
  /// `{0, 1, ..., id}`.
  static RelationSet prefixThrough(int32_t id) {
    VELOX_DCHECK_GE(id, 0);
    VELOX_DCHECK_LT(id, kMaxRelations);
    if (id == kMaxRelations - 1) {
      return RelationSet{~uint64_t{0}};
    }
    return RelationSet{(uint64_t{1} << (id + 1)) - 1};
  }

  bool contains(int32_t id) const {
    return (bits_ & mask(id)) != 0;
  }

  void add(int32_t id) {
    bits_ |= mask(id);
  }

  void erase(int32_t id) {
    bits_ &= ~mask(id);
  }

  bool operator==(const RelationSet& other) const = default;

  size_t hash() const {
    return std::hash<uint64_t>{}(bits_);
  }

  bool empty() const {
    return bits_ == 0;
  }

  void clear() {
    bits_ = 0;
  }

  /// True if 'this' ⊆ 'super'.
  bool isSubset(const RelationSet& super) const {
    return (bits_ & ~super.bits_) == 0;
  }

  /// True if 'this' and 'other' share at least one id.
  bool hasIntersection(const RelationSet& other) const {
    return (bits_ & other.bits_) != 0;
  }

  /// Erases all ids not in 'other'.
  void intersect(const RelationSet& other) {
    bits_ &= other.bits_;
  }

  /// Adds all ids in 'other'.
  void unionSet(const RelationSet& other) {
    bits_ |= other.bits_;
  }

  /// Erases all ids present in 'other'.
  void except(const RelationSet& other) {
    bits_ &= ~other.bits_;
  }

  size_t size() const {
    return std::popcount(bits_);
  }

  /// Returns the smallest id in the set. The set must be non-empty.
  int32_t min() const {
    VELOX_DCHECK(!empty());
    return std::countr_zero(bits_);
  }

  /// Calls 'f(id)' for each set bit, in ascending id order.
  template <typename Func>
  void forEach(Func f) const {
    uint64_t remaining{bits_};
    while (remaining != 0) {
      const int32_t id = std::countr_zero(remaining);
      f(id);
      remaining &= remaining - 1;
    }
  }

  /// True if 'predicate(id)' returns true for any set bit.
  /// Short-circuits at the first match.
  template <typename Predicate>
  bool anyOf(Predicate predicate) const {
    uint64_t remaining{bits_};
    while (remaining != 0) {
      const int32_t id = std::countr_zero(remaining);
      if (predicate(id)) {
        return true;
      }
      remaining &= remaining - 1;
    }
    return false;
  }

  /// Raw 64-bit representation. Bit `i` set ⇔ id `i` is a member.
  constexpr uint64_t bits() const {
    return bits_;
  }

 private:
  static uint64_t mask(int32_t id) {
    VELOX_DCHECK_GE(id, 0);
    VELOX_DCHECK_LT(id, kMaxRelations);
    return uint64_t{1} << id;
  }

  uint64_t bits_{0};
};

} // namespace facebook::axiom::optimizer::v2

namespace std {
template <>
struct hash<facebook::axiom::optimizer::v2::RelationSet> {
  size_t operator()(
      const facebook::axiom::optimizer::v2::RelationSet& set) const {
    return set.hash();
  }
};
} // namespace std
