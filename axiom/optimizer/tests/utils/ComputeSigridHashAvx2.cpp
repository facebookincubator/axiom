// Copyright 2004-present Facebook. All Rights Reserved.

#include "axiom/optimizer/tests/utils/ComputeSigridHash.h"

namespace facebook::axiom::velox_udf {

#ifdef __x86_64__

template <>
template <>
xsimd::batch<uint32_t, xsimd::avx2>
ComputeSigridHashImpl<xsimd::avx2>::shuffle<0xB1>(
    xsimd::batch<uint32_t, xsimd::avx2> x) {
  return _mm256_shuffle_epi32(x, 0xB1);
}

template <>
xsimd::batch<uint64_t, xsimd::avx2> ComputeSigridHashImpl<xsimd::avx2>::mul(
    xsimd::batch<uint32_t, xsimd::avx2> x,
    xsimd::batch<uint32_t, xsimd::avx2> y) {
  return _mm256_mul_epu32(x, y);
}

template void ComputeSigridHash<int64_t, int64_t>::operator()<xsimd::avx2>(
    const xsimd::avx2&,
    const Parameters&);
template void ComputeSigridHash<int64_t, int32_t>::operator()<xsimd::avx2>(
    const xsimd::avx2&,
    const Parameters&);
template void ComputeSigridHash<int32_t, int32_t>::operator()<xsimd::avx2>(
    const xsimd::avx2&,
    const Parameters&);

#endif

} // namespace facebook::axiom::velox_udf
