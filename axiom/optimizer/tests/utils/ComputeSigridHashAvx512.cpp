// Copyright 2004-present Facebook. All Rights Reserved.

#include "axiom/optimizer/tests/utils/ComputeSigridHash.h"

namespace facebook::axiom::velox_udf {

#ifdef __AVX512F__

template <>
template <>
xsimd::batch<uint32_t, xsimd::avx512bw>
ComputeSigridHashImpl<xsimd::avx512bw>::shuffle<0xB1>(
    xsimd::batch<uint32_t, xsimd::avx512bw> x) {
  return _mm512_shuffle_epi32(x, (_MM_PERM_ENUM)0xB1);
}

template <>
xsimd::batch<uint64_t, xsimd::avx512bw>
ComputeSigridHashImpl<xsimd::avx512bw>::mul(
    xsimd::batch<uint32_t, xsimd::avx512bw> x,
    xsimd::batch<uint32_t, xsimd::avx512bw> y) {
  return _mm512_mul_epu32(x, y);
}

template void ComputeSigridHash<int64_t, int64_t>::operator()<xsimd::avx512bw>(
    const xsimd::avx512bw&,
    const Parameters&);
template void ComputeSigridHash<int64_t, int32_t>::operator()<xsimd::avx512bw>(
    const xsimd::avx512bw&,
    const Parameters&);
template void ComputeSigridHash<int32_t, int32_t>::operator()<xsimd::avx512bw>(
    const xsimd::avx512bw&,
    const Parameters&);

#endif

} // namespace facebook::axiom::velox_udf
