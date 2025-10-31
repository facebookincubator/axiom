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

#include "axiom/optimizer/BitSet.h"

namespace facebook::axiom::optimizer {

namespace {
template <typename V>
bool isZero(const V& bits, size_t begin, size_t end) {
  for (size_t i = begin; i < end; ++i) {
    if (bits[i]) {
      return false;
    }
  }
  return true;
}
} // namespace

bool BitSet::operator==(const BitSet& other) const {
  // The sets are equal if they have the same bits set. Trailing words of zeros
  // do not count.
  auto l1 = bits_.size();
  auto l2 = other.bits_.size();
  for (unsigned i = 0; i < l1 && i < l2; ++i) {
    if (bits_[i] != other.bits_[i]) {
      return false;
    }
  }
  if (l1 < l2) {
    return isZero(other.bits_, l1, l2);
  }
  if (l2 < l1) {
    return isZero(bits_, l2, l1);
  }
  return true;
}

bool BitSet::isSubset(const BitSet& super) const {
  auto l1 = bits_.size();
  auto l2 = super.bits_.size();
  if (!velox::bits::isSubset(
          bits_.data(), super.bits_.data(), 0, std::min(l1, l2) * 64)) {
    return false;
  }
  if (l2 < l1) {
    return isZero(bits_, l2, l1);
  }
  return true;
}

bool BitSet::hasIntersection(const BitSet& other) const {
  auto l1 = bits_.size();
  auto l2 = other.bits_.size();
  return velox::bits::hasIntersection(
      bits_.data(), other.bits_.data(), 0, std::min(l1, l2) * 64);
}

size_t BitSet::hash() const {
  // The hash is a mix of the hashes of all non-zero words.
  size_t hash = 123;
  for (auto word : bits_) {
    hash = velox::simd::crc32U64(hash, word);
  }
  return hash * hash;
}

void BitSet::unionSet(const BitSet& other) {
  ensureWords(other.bits_.size());
  for (size_t i = 0; i < other.bits_.size(); ++i) {
    bits_[i] |= other.bits_[i];
  }
}

void BitSet::intersect(const BitSet& other) {
  bits_.resize(std::min(bits_.size(), other.bits_.size()));
  for (size_t i = 0; i < bits_.size(); ++i) {
    VELOX_DCHECK(!other.bits_.empty());
    bits_[i] &= other.bits_[i];
  }
}

} // namespace facebook::axiom::optimizer
