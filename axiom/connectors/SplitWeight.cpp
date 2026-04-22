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

#include "axiom/connectors/SplitWeight.h"

#include <algorithm>
#include <cmath>

#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::connector {

namespace {
// Tolerance used to absorb binary floating-point representation error
// when scaling fractions to the integer weight scale. Without it,
// exact-decimal inputs like 0.07 evaluate to 7.000000000000001 and ceil
// up to 8.
constexpr double kScaleEpsilon = 1e-9;
} // namespace

int64_t SplitWeight::normalize(double fraction) {
  VELOX_USER_CHECK(
      std::isfinite(fraction) && fraction > 0.0 && fraction <= 1.0,
      "Split weight fraction must be in (0, 1]: {}",
      fraction);
  const double scaled = fraction * kStandard;
  return std::max<int64_t>(
      kMin, static_cast<int64_t>(std::ceil(scaled - kScaleEpsilon)));
}

int64_t SplitWeight::forSize(
    uint64_t splitSize,
    uint64_t standardSize,
    double minFraction) {
  VELOX_USER_CHECK_GT(standardSize, 0, "Standard split size must be positive");
  VELOX_USER_CHECK(
      std::isfinite(minFraction) && minFraction > 0.0 && minFraction <= 1.0,
      "Minimum split weight fraction must be in (0, 1]: {}",
      minFraction);
  const double raw =
      static_cast<double>(splitSize) / static_cast<double>(standardSize);
  return normalize(std::clamp(raw, minFraction, 1.0));
}

} // namespace facebook::axiom::connector
