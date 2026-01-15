// Copyright 2004-present Facebook. All Rights Reserved.

#pragma once

#include <folly/Hash.h>
#include <velox/common/base/Exceptions.h>
#include <velox/common/base/SimdUtil.h>

namespace facebook::axiom::velox_udf {

template <typename Input, typename Output>
struct ComputeSigridHash {
  struct Parameters {
    size_t rows;
    const Input* input;
    Output* output;
    int64_t salt;
    int64_t maxValue;
    uint64_t multiplier;
    int shift;
  };

  template <typename Arch>
  void operator()(const Arch&, const Parameters&);

  static void scalar(const Parameters&);
};

template <typename A>
struct ComputeSigridHashImpl {
  // Thomas Wang 64 bit mix hash function.
  static xsimd::batch<uint64_t, A> twangMix64(xsimd::batch<uint64_t, A> key) {
    key = (~key) + (key << 21); // key *= (1 << 21) - 1; key -= 1;
    key = key ^ (key >> 24);
    key = key + (key << 3) + (key << 8); // key *= 1 + (1 << 3) + (1 << 8)
    key = key ^ (key >> 14);
    key = key + (key << 2) + (key << 4); // key *= 1 + (1 << 2) + (1 << 4)
    key = key ^ (key >> 28);
    key = key + (key << 31); // key *= 1 + (1 << 31)
    return key;
  }

  // Performs element-by-element multiplication between elements in x and y and
  // return the high 64 bits of each result.
  static xsimd::batch<int64_t, A> mulhi(
      xsimd::batch<int64_t, A> x,
      xsimd::batch<int64_t, A> y) {
    // Swap the low and high bits for each int64.  0xB1 is 0b10110001.
    auto xh = shuffle<0xB1>(x.data);
    auto yh = shuffle<0xB1>(y.data);

    auto w0 = mul(x.data, y.data); // x0l*y0l, x1l*y1l
    auto w1 = mul(x.data, yh); // x0l*y0h, x1l*y1h
    auto w2 = mul(xh, y.data); // x0h*y0l, x1h*y0l
    auto w3 = mul(xh, yh); // x0h*y0h, x1h*y1h

    auto w0h = w0 >> 32;

    auto s1 = w1 + w0h;
    auto s1l = s1 & xsimd::broadcast<uint64_t, A>(0xFFFFFFFF);
    auto s1h = s1 >> 32;

    auto s2 = w2 + s1l;
    auto s2h = s2 >> 32;

    auto hi1 = w3 + s1h;
    return xsimd::bitwise_cast<xsimd::batch<int64_t, A>>(hi1 + s2h);
  }

 private:
  template <uint8_t kIndices>
  static xsimd::batch<uint32_t, A> shuffle(xsimd::batch<uint32_t, A>);

  static xsimd::batch<uint64_t, A> mul(
      xsimd::batch<uint32_t, A>,
      xsimd::batch<uint32_t, A>);
};

template <typename Input, typename Output>
void ComputeSigridHash<Input, Output>::scalar(const Parameters& params) {
  auto [rows, input, output, salt, maxValue, multiplier, shift] = params;
  using uint128_t = unsigned __int128;
  for (int i = 0; i < rows; ++i) {
    int64_t hashed =
        folly::hash::hash_combine(salt, folly::hash::twang_mix64(input[i]));
    if (maxValue > 1) {
      int64_t sign = hashed >> (64 - 1);
      int64_t q = sign ^
          ((static_cast<uint128_t>(multiplier) * (sign ^ hashed)) >>
           (64 + shift));
      output[i] = hashed - q * maxValue;
      VELOX_DCHECK_EQ(
          output[i],
          ((hashed % maxValue >= 0) ? hashed % maxValue
                                    : hashed % maxValue + maxValue));
    } else if (maxValue == 1) {
      output[i] = 0;
    } else {
      output[i] = hashed;
    }
  }
}

template <typename Input, typename Output>
template <typename A>
void ComputeSigridHash<Input, Output>::operator()(
    const A&,
    const Parameters& params) {
  auto [rows, input, output, salt, maxValue, multiplier, shift] = params;
  constexpr int kStep = xsimd::batch<uint64_t, A>::size;
  auto simdRows = rows / kStep * kStep;
  auto salts = xsimd::batch<uint64_t, A>::broadcast(salt);
  auto mul = xsimd::batch<uint64_t, A>::broadcast(0x9ddfea08eb382d69ULL);
  auto multipliers = xsimd::batch<int64_t, A>::broadcast(multiplier);
  for (size_t i = 0; i < simdRows; i += kStep) {
    auto key = ComputeSigridHashImpl<A>::twangMix64(
        xsimd::batch<uint64_t, A>::load_unaligned(input + i));
    auto a = (key ^ salts) * mul;
    a = a ^ (a >> 47);
    auto b = (salts ^ a) * mul;
    auto hashed =
        xsimd::bitwise_cast<xsimd::batch<int64_t, A>>((b ^ (b >> 47)) * mul);
    auto sign = hashed >> 63;
    auto q = ComputeSigridHashImpl<A>::mulhi(multipliers, sign ^ hashed);
    q = sign ^ (q >> shift);
    hashed -= q * maxValue;
    hashed.store_unaligned(output + i);
  }
  auto params2 = params;
  params2.rows -= simdRows;
  params2.input += simdRows;
  params2.output += simdRows;
  scalar(params2);
}

template <>
template <>
inline void ComputeSigridHash<int64_t, int64_t>::operator()(
    const xsimd::generic&,
    const Parameters& params) {
  scalar(params);
}

template <>
template <>
inline void ComputeSigridHash<int64_t, int32_t>::operator()(
    const xsimd::generic&,
    const Parameters& params) {
  scalar(params);
}

template <>
template <>
inline void ComputeSigridHash<int32_t, int32_t>::operator()(
    const xsimd::generic&,
    const Parameters& params) {
  scalar(params);
}

template <>
template <typename A>
void ComputeSigridHash<int32_t, int64_t>::operator()(
    const A&,
    const Parameters&) {
  VELOX_UNREACHABLE();
}

#ifdef __AVX512F__
extern template void ComputeSigridHash<int64_t, int64_t>::operator()<
    xsimd::avx512bw>(const xsimd::avx512bw&, const Parameters&);
extern template void ComputeSigridHash<int64_t, int32_t>::operator()<
    xsimd::avx512bw>(const xsimd::avx512bw&, const Parameters&);
extern template void ComputeSigridHash<int32_t, int32_t>::operator()<
    xsimd::avx512bw>(const xsimd::avx512bw&, const Parameters&);
#endif

extern template void ComputeSigridHash<int64_t, int64_t>::operator()<
    xsimd::avx2>(const xsimd::avx2&, const Parameters&);
extern template void ComputeSigridHash<int64_t, int32_t>::operator()<
    xsimd::avx2>(const xsimd::avx2&, const Parameters&);
extern template void ComputeSigridHash<int32_t, int32_t>::operator()<
    xsimd::avx2>(const xsimd::avx2&, const Parameters&);

} // namespace facebook::axiom::velox_udf
