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

#include "axiom/connectors/hive/HiveConnectorMetadata.h"

#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/type/Type.h"

namespace facebook::axiom::connector::hive {
namespace {

using namespace facebook::velox;

HivePartitionType makePartitionType(int32_t numPartitions) {
  return HivePartitionType{numPartitions, {BIGINT()}};
}

TEST(HivePartitionTypeTest, scaleDownIdentityWhenWithinLimit) {
  const auto partitionType = makePartitionType(8);

  auto scaled = partitionType.scaleDown(8);
  ASSERT_NE(scaled, nullptr);
  EXPECT_EQ(scaled->numPartitions(), 8);

  scaled = partitionType.scaleDown(16);
  ASSERT_NE(scaled, nullptr);
  EXPECT_EQ(scaled->numPartitions(), 8);
}

TEST(HivePartitionTypeTest, scaleDownPicksLargestDivisor) {
  const auto partitionType = makePartitionType(256);

  auto scaled = partitionType.scaleDown(100);
  ASSERT_NE(scaled, nullptr);
  EXPECT_EQ(scaled->numPartitions(), 64);

  scaled = partitionType.scaleDown(64);
  ASSERT_NE(scaled, nullptr);
  EXPECT_EQ(scaled->numPartitions(), 64);

  scaled = partitionType.scaleDown(63);
  ASSERT_NE(scaled, nullptr);
  EXPECT_EQ(scaled->numPartitions(), 32);
}

TEST(HivePartitionTypeTest, scaleDownToOne) {
  const auto partitionType = makePartitionType(16);

  const auto scaled = partitionType.scaleDown(1);
  ASSERT_NE(scaled, nullptr);
  EXPECT_EQ(scaled->numPartitions(), 1);
}

TEST(HivePartitionTypeTest, scaleDownPrimePartitionCount) {
  const auto partitionType = makePartitionType(7);

  auto scaled = partitionType.scaleDown(6);
  ASSERT_NE(scaled, nullptr);
  EXPECT_EQ(scaled->numPartitions(), 1);

  scaled = partitionType.scaleDown(7);
  ASSERT_NE(scaled, nullptr);
  EXPECT_EQ(scaled->numPartitions(), 7);

  scaled = partitionType.scaleDown(100);
  ASSERT_NE(scaled, nullptr);
  EXPECT_EQ(scaled->numPartitions(), 7);
}

TEST(HivePartitionTypeTest, scaleDownRejectsNonPositiveMax) {
  const auto partitionType = makePartitionType(8);

  VELOX_ASSERT_THROW(partitionType.scaleDown(0), "");
  VELOX_ASSERT_THROW(partitionType.scaleDown(-1), "");
}

TEST(HivePartitionTypeTest, ctorRejectsNonPositiveNumPartitions) {
  VELOX_ASSERT_THROW(HivePartitionType(0, std::vector<TypePtr>{BIGINT()}), "");
  VELOX_ASSERT_THROW(HivePartitionType(-1, std::vector<TypePtr>{BIGINT()}), "");
}

TEST(HivePartitionTypeTest, scaleDownPreservesKeyTypes) {
  HivePartitionType partitionType{
      12, std::vector<TypePtr>{BIGINT(), VARCHAR()}};

  const auto scaled = partitionType.scaleDown(4);
  ASSERT_NE(scaled, nullptr);
  EXPECT_EQ(scaled->numPartitions(), 4);

  EXPECT_NE(partitionType.copartition(*scaled), nullptr);
  EXPECT_NE(scaled->copartition(partitionType), nullptr);
}

} // namespace
} // namespace facebook::axiom::connector::hive
