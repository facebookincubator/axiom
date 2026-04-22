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

#include <limits>

#include <gtest/gtest.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::connector {
namespace {

TEST(SplitWeightTest, normalizeBoundaries) {
  EXPECT_EQ(SplitWeight::normalize(1.0), SplitWeight::kStandard);
  EXPECT_EQ(SplitWeight::normalize(0.5), 50);
  EXPECT_EQ(SplitWeight::normalize(0.05), 5);
}

TEST(SplitWeightTest, normalizeRoundsUpToMin) {
  // Tiny fractions ceil up to kMin so they don't collapse to 0.
  EXPECT_EQ(SplitWeight::normalize(1e-9), SplitWeight::kMin);
  EXPECT_EQ(SplitWeight::normalize(0.001), SplitWeight::kMin);
  EXPECT_EQ(SplitWeight::normalize(0.01), SplitWeight::kMin);
}

TEST(SplitWeightTest, normalizeCeilForNonExactScales) {
  // 0.011 * 100 = 1.1, ceiled to 2.
  EXPECT_EQ(SplitWeight::normalize(0.011), 2);
  // 0.555 * 100 = 55.5, ceiled to 56.
  EXPECT_EQ(SplitWeight::normalize(0.555), 56);
}

TEST(SplitWeightTest, normalizeExactDecimals) {
  // Exact-decimal inputs whose binary representation is slightly above
  // the integer must not be ceiled up. For example, 0.07 * 100 evaluates
  // to 7.000000000000001 in IEEE 754, but the canonical mapping is 7.
  EXPECT_EQ(SplitWeight::normalize(0.07), 7);
  EXPECT_EQ(SplitWeight::normalize(0.14), 14);
  EXPECT_EQ(SplitWeight::normalize(0.28), 28);
  EXPECT_EQ(SplitWeight::normalize(0.55), 55);
}

TEST(SplitWeightTest, normalizeRejectsInvalid) {
  VELOX_ASSERT_USER_THROW(
      SplitWeight::normalize(0.0), "Split weight fraction must be in (0, 1]");
  // IEEE 754 -0.0 fails the > 0.0 guard the same way 0.0 does.
  VELOX_ASSERT_USER_THROW(
      SplitWeight::normalize(-0.0), "Split weight fraction must be in (0, 1]");
  VELOX_ASSERT_USER_THROW(
      SplitWeight::normalize(-0.1), "Split weight fraction must be in (0, 1]");
  VELOX_ASSERT_USER_THROW(
      SplitWeight::normalize(1.5), "Split weight fraction must be in (0, 1]");
  VELOX_ASSERT_USER_THROW(
      SplitWeight::normalize(std::numeric_limits<double>::quiet_NaN()),
      "Split weight fraction must be in (0, 1]");
  VELOX_ASSERT_USER_THROW(
      SplitWeight::normalize(std::numeric_limits<double>::infinity()),
      "Split weight fraction must be in (0, 1]");
}

TEST(SplitWeightTest, forSizeStandardAndOver) {
  // A split exactly at standardSize gets kStandard.
  EXPECT_EQ(
      SplitWeight::forSize(256ull << 20, 256ull << 20, 0.05),
      SplitWeight::kStandard);
  // A split larger than standardSize is capped at kStandard, not over-weighted.
  EXPECT_EQ(
      SplitWeight::forSize(1ull << 30, 256ull << 20, 0.05),
      SplitWeight::kStandard);
}

TEST(SplitWeightTest, forSizeProportional) {
  // 128 MB / 256 MB = 0.5 → 50.
  EXPECT_EQ(SplitWeight::forSize(128ull << 20, 256ull << 20, 0.05), 50);
  // 64 MB / 256 MB = 0.25 → 25.
  EXPECT_EQ(SplitWeight::forSize(64ull << 20, 256ull << 20, 0.05), 25);
}

TEST(SplitWeightTest, forSizeExactDecimalRatio) {
  // 7 MB / 100 MB = 0.07, an exact decimal whose binary representation
  // is 7.000000000000001. Must produce 7, not 8.
  EXPECT_EQ(SplitWeight::forSize(7ull << 20, 100ull << 20, 0.05), 7);
}

TEST(SplitWeightTest, forSizeClampToMinFraction) {
  // 1 KB / 256 MB ~= 3.7e-6, clamped up to 0.05 → 5.
  EXPECT_EQ(SplitWeight::forSize(1ull << 10, 256ull << 20, 0.05), 5);
  // Empty split clamps to minFraction as well.
  EXPECT_EQ(SplitWeight::forSize(0, 256ull << 20, 0.05), 5);
}

TEST(SplitWeightTest, forSizeRejectsZeroStandardSize) {
  VELOX_ASSERT_USER_THROW(
      SplitWeight::forSize(1, 0, 0.05), "Standard split size must be positive");
}

TEST(SplitWeightTest, forSizeRejectsInvalidMinFraction) {
  VELOX_ASSERT_USER_THROW(
      SplitWeight::forSize(1ull << 10, 256ull << 20, 0.0),
      "Minimum split weight fraction must be in (0, 1]");
  VELOX_ASSERT_USER_THROW(
      SplitWeight::forSize(1ull << 10, 256ull << 20, -0.1),
      "Minimum split weight fraction must be in (0, 1]");
  VELOX_ASSERT_USER_THROW(
      SplitWeight::forSize(1ull << 10, 256ull << 20, 1.5),
      "Minimum split weight fraction must be in (0, 1]");
  VELOX_ASSERT_USER_THROW(
      SplitWeight::forSize(
          1ull << 10, 256ull << 20, std::numeric_limits<double>::quiet_NaN()),
      "Minimum split weight fraction must be in (0, 1]");
}

} // namespace
} // namespace facebook::axiom::connector
