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

#include "axiom/optimizer/QueryGraphContext.h"

namespace facebook::axiom::optimizer {

class BitSet {
 public:
  bool contains(int32_t id) const {
    return id < bits_.size() * 64 && velox::bits::isBitSet(bits_.data(), id);
  }

  void add(int32_t id) {
    ensureSize(id);
    velox::bits::setBit(bits_.data(), id);
  }

  void erase(int32_t id) {
    if (id < bits_.size() * 64) {
      velox::bits::clearBit(bits_.data(), id);
    }
  }

  bool operator==(const BitSet& other) const;

  size_t hash() const;

  // True if no members.
  bool empty() const {
    return std::ranges::all_of(bits_, [](auto word) { return word == 0; });
  }

  /// Returns true if 'this' is a subset of 'super'.
  bool isSubset(const BitSet& super) const;

  /// Returns true if 'this' and 'other' share at least one id.
  bool hasIntersection(const BitSet& other) const;

  /// Erases all ids not in 'other'.
  void intersect(const BitSet& other);

  /// Adds all ids in 'other'.
  void unionSet(const BitSet& other);

  void except(const BitSet& other) {
    velox::bits::forEachSetBit(
        other.bits_.data(),
        0,
        static_cast<int32_t>(other.bits_.size() * 64),
        [&](auto id) {
          if (id < bits_.size() * 64) {
            velox::bits::clearBit(bits_.data(), id);
          }
        });
  }

  size_t size() const {
    return velox::bits::countBits(
        bits_.data(), 0, static_cast<int32_t>(bits_.size() * 64));
  }

  template <typename Func>
  void forEach(Func f) const {
    velox::bits::forEachSetBit(bits_.data(), 0, bits_.size() * 64, f);
  }

 protected:
  void ensureSize(size_t id) {
    ensureWords(velox::bits::nwords(id + 1));
  }

  void ensureWords(size_t size) {
    if (bits_.size() < size) {
      bits_.resize(size);
    }
  }

  // A one bit corresponds to the id of each member.
  QGVector<uint64_t> bits_;
};

} // namespace facebook::axiom::optimizer
