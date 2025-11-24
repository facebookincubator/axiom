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

#include "axiom/optimizer/StatsUtils.h"

#include <gtest/gtest.h>
#include <cmath>

using namespace facebook::axiom::optimizer;

class StatsUtilsTest : public testing::Test {
 protected:
  // Helper to check if two values are approximately equal
  void
  expectClose(double expected, double actual, double relativeTolerance = 0.01) {
    double ratio = expected > 0 ? std::abs((actual - expected) / expected)
                                : std::abs(actual - expected);
    EXPECT_LT(ratio, relativeTolerance)
        << "Expected " << expected << " but got " << actual
        << " (difference ratio: " << ratio << ")";
  }
};

TEST_F(StatsUtilsTest, behaves_like_multiplication_when_far_from_max) {
  // When product is far from max (e.g., max/10), should behave like
  // multiplication
  double max = 100.0;
  double numbers[] = {2.0, 3.0};

  double result = saturatingProduct(max, numbers, 2);
  double expectedProduct = 6.0; // 2 * 3

  // Should be close to 6.0 when max is 100
  // Formula: 100 * 6 / (100 + 6) ≈ 5.66
  expectClose(5.66, result, 0.01);

  // Verify it's approximately the product (within 10% for this case)
  double approximationRatio = result / expectedProduct;
  EXPECT_GT(approximationRatio, 0.9);
  EXPECT_LT(approximationRatio, 1.0);
}

TEST_F(StatsUtilsTest, approaches_max_asymptotically) {
  double max = 100.0;

  // Test with increasingly large products
  double small[] = {2.0, 3.0}; // product = 6
  double medium[] = {5.0, 10.0}; // product = 50
  double large[] = {10.0, 50.0}; // product = 500
  double huge[] = {100.0, 100.0}; // product = 10000

  double resultSmall = saturatingProduct(max, small, 2);
  double resultMedium = saturatingProduct(max, medium, 2);
  double resultLarge = saturatingProduct(max, large, 2);
  double resultHuge = saturatingProduct(max, huge, 2);

  // All results should be less than max
  EXPECT_LT(resultSmall, max);
  EXPECT_LT(resultMedium, max);
  EXPECT_LT(resultLarge, max);
  EXPECT_LT(resultHuge, max);

  // Results should be monotonically increasing (larger product = closer to max)
  EXPECT_LT(resultSmall, resultMedium);
  EXPECT_LT(resultMedium, resultLarge);
  EXPECT_LT(resultLarge, resultHuge);

  // Huge product should be very close to max
  expectClose(max, resultHuge, 0.02);
}

TEST_F(StatsUtilsTest, preserves_ordering) {
  double max = 100.0;

  double numbers1[] = {2.0, 3.0}; // product = 6
  double numbers2[] = {2.0, 4.0}; // product = 8
  double numbers3[] = {3.0, 4.0}; // product = 12

  double result1 = saturatingProduct(max, numbers1, 2);
  double result2 = saturatingProduct(max, numbers2, 2);
  double result3 = saturatingProduct(max, numbers3, 2);

  // Larger products should yield values closer to (or equal to if same) max
  EXPECT_LT(result1, result2);
  EXPECT_LT(result2, result3);
}

TEST_F(StatsUtilsTest, single_number) {
  double max = 100.0;
  double number[] = {10.0};

  double result = saturatingProduct(max, number, 1);

  // Formula: max * P / (max + P) = 100 * 10 / (100 + 10) ≈ 9.09
  expectClose(9.09, result, 0.01);
}

TEST_F(StatsUtilsTest, three_numbers) {
  double max = 1000.0;
  double numbers[] = {2.0, 3.0, 5.0}; // product = 30

  double result = saturatingProduct(max, numbers, 3);

  // Formula: 1000 * 30 / (1000 + 30) ≈ 29.13
  expectClose(29.13, result, 0.01);
}

TEST_F(StatsUtilsTest, with_ones) {
  double max = 100.0;
  double numbers[] = {1.0, 1.0, 5.0}; // product = 5

  double result = saturatingProduct(max, numbers, 3);

  // Formula: 100 * 5 / (100 + 5) ≈ 4.76
  expectClose(4.76, result, 0.01);
}

TEST_F(StatsUtilsTest, with_fractional_numbers) {
  double max = 100.0;
  double numbers[] = {0.5, 2.0}; // product = 1.0

  double result = saturatingProduct(max, numbers, 2);

  // Formula: 100 * 1 / (100 + 1) ≈ 0.99
  expectClose(0.99, result, 0.01);
}

TEST_F(StatsUtilsTest, empty_array) {
  double max = 100.0;
  double* numbers = nullptr;

  double result = saturatingProduct(max, numbers, 0);

  // Product of empty set is 1.0
  // Formula: 100 * 1 / (100 + 1) ≈ 0.99
  expectClose(0.99, result, 0.01);
}
