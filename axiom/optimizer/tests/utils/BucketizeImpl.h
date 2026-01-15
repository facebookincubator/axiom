// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <stddef.h>
#include <array>
#include <limits>
#include <vector>

#if defined(__x86_64__)
#if !defined(__AVX2__) || !defined(__FMA__) || !defined(__F16C__)
#error "This file requires AVX2, FMA and F16C"
#endif
#include <immintrin.h>
#elif defined(__aarch64__)
#if !defined(__ARM_FEATURE_SVE)
#error "This file requires SVE"
#endif
#include "common/aarch64/compat.h" // @manual=//common/aarch64:intrinsics_dep
#endif

namespace facebook::axiom::df4ai::detail {

namespace {

constexpr size_t getIndexShift(size_t fanout) {
  return __builtin_ctzll(fanout);
}

constexpr size_t getEntryCount(size_t fanout, size_t count) {
  return (count + fanout - 1) >> getIndexShift(fanout);
}

constexpr size_t getLevelCount(size_t fanout, size_t count) {
  auto shift = getIndexShift(fanout);
  return (63 - __builtin_clzll(count - 1) + shift) / shift;
}

template <size_t FANOUT>
struct IndexEntry {
  size_t offset{0};
  std::array<float, FANOUT> values;
};

} // namespace

template <size_t FANOUT, size_t LEVEL>
class BordersIndex {
  static constexpr size_t kIndexShift = getIndexShift(FANOUT);
  static constexpr size_t kIndexMask = FANOUT - 1;

 public:
  struct Traits {
    static constexpr size_t kMinCount =
        (LEVEL == 0 ? 0 : 1ULL << (kIndexShift * (LEVEL - 1)));
    static constexpr size_t kMaxCount = kMinCount << kIndexShift;
  };

  explicit BordersIndex(const std::vector<float>& borders)
      : count_{borders.size()} {
    // To make it easier for find(), always append +INF, hence +1;
    auto count = borders.size() + 1;
    entries_.reserve(getLevelCount(FANOUT, count));

    auto entries = &entries_.emplace_back();
    entries->reserve(getEntryCount(FANOUT, count));
    IndexEntry<FANOUT>* entry = nullptr;
    size_t index = FANOUT;

    // Fill the leaves
    for (auto i = 0; i < borders.size(); ++i) {
      if (index == FANOUT) {
        entry = &entries->emplace_back();
        entry->offset = i;
        index = 0;
      }

      entry->values[index++] = borders[i];
    }

    // Make sure we have enough space for the +INF, then further pad to create
    // the 64 bytes alignment.
    if (index == FANOUT) {
      entry = &entries->emplace_back();
      entry->offset = borders.size();
      index = 0;
    }

    while (index < FANOUT) {
      entry->values[index++] = std::numeric_limits<float>::infinity();
    }

    // Fill intermediate levels until we hit the root
    while (entries->size() > 1) {
      entries = &entries_.emplace_back();
      auto& last = entries_[entries_.size() - 2];
      entries->reserve(getEntryCount(FANOUT, last.size()));
      index = FANOUT;

      for (auto i = 0; i < last.size(); ++i) {
        if (index == FANOUT) {
          entry = &entries->emplace_back();
          entry->offset = i;
          index = 0;
        }

        entry->values[index++] = last[i].values[kIndexMask];
      }

      while (index < FANOUT) {
        entry->values[index++] = std::numeric_limits<float>::infinity();
      }
    }
  }

  size_t count() const {
    return count_;
  }

  float operator[](size_t index) const {
    return entries_[0][index >> kIndexShift].values[index & kIndexMask];
  }

 protected:
  size_t count_;
  // Index entries are organized as a tree upside down. entries_[0] contains the
  // leaves, while entries_[entries_.size() - 1] always contain 1 root entry.
  std::vector<std::vector<IndexEntry<FANOUT>>> entries_;
};

// SIMD Implementation
template <size_t LEVEL = 0>
class BordersIndexAvx2 : public BordersIndex<8, LEVEL> {
 public:
  explicit BordersIndexAvx2(const std::vector<float>& borders)
      : BordersIndex<8, LEVEL>{borders} {}

  size_t find(float val) const;
};

namespace {

#ifdef __aarch64__
inline size_t findImpl(const IndexEntry<8>& entry, const float32x4_t& values) {
  size_t mask;
  asm volatile(

      "mov v22.16b, %[values].16b\t\n"

      "ldp q20, q21, [%[entryValuesPtr]]\t\n"

      "ptrue p0.s\t\n"

      "fcmge p1.s, p0/z, z20.s, z22.s\t\n"
      "fcmuo p3.s, p0/z, z20.s, z22.s\t\n"
      "fcmge p2.s, p0/z, z21.s, z22.s\t\n"
      "fcmuo p4.s, p0/z, z21.s, z22.s\t\n"

      "mov z20.b, #0\t\n"
      "mov z21.b, #0\t\n"
      "orr p1.b, p0/z, p1.b, p3.b\t\n"
      "orr p2.b, p0/z, p2.b, p4.b\t\n"
      "not z20.s, p1/m, z20.s\t\n"
      "not z21.s, p2/m, z21.s\t\n"
      "xtn v20.4h, v20.4s\t\n"
      "xtn2 v20.8h, v21.4s\t\n"
      "xtn v20.8b, v20.8h\t\n"

      "umov %[mask], v20.d[0]\t\n"

      "rbit %[mask], %[mask]\t\n"

      "clz %[mask], %[mask]\t\n"

      "lsr %[mask], %[mask], #3\t\n"

      : [mask] "=r"(mask)
      : [entryValuesPtr] "r"(&entry.values), [values] "w"(values)
      : "memory", "cc", "v20", "v21", "v22", "p0", "p1", "p2", "p3", "p4");
  return mask + entry.offset;
}
#else
inline size_t findImpl(const IndexEntry<8>& entry, const __m256& values) {
  auto result = _mm256_cmp_ps(
      _mm256_loadu_ps(reinterpret_cast<const float*>(&entry.values)),
      values,
      _CMP_NLT_UQ);
  auto mask = _mm256_movemask_ps(result);
  return __builtin_ctzll(mask) + entry.offset;
}
#endif
} // namespace

template <size_t LEVEL>
size_t BordersIndexAvx2<LEVEL>::find(float val) const {
#ifdef __aarch64__
  auto values = vdupq_n_f32(val);
#else
  auto values = _mm256_set1_ps(val);
#endif

  // Special case for a few levels. We do that for 4 levels, which supports up
  // to 4095 buckets.
  if constexpr (LEVEL == 1) {
    return findImpl(this->entries_[0][0], values);
  }

  if constexpr (LEVEL == 2) {
    auto pos = findImpl(this->entries_[1][0], values);
    return findImpl(this->entries_[0][pos], values);
  }

  if constexpr (LEVEL == 3) {
    auto pos = findImpl(this->entries_[2][0], values);
    pos = findImpl(this->entries_[1][pos], values);
    return findImpl(this->entries_[0][pos], values);
  }

  if constexpr (LEVEL == 4) {
    auto pos = findImpl(this->entries_[3][0], values);
    pos = findImpl(this->entries_[2][pos], values);
    pos = findImpl(this->entries_[1][pos], values);
    return findImpl(this->entries_[0][pos], values);
  }

  size_t pos = 0;
  int32_t level = this->entries_.size() - 1;
  while (level >= 0) {
    pos = findImpl(this->entries_[level--][pos], values);
  }
  return pos;
}

} // namespace facebook::axiom::df4ai::detail
