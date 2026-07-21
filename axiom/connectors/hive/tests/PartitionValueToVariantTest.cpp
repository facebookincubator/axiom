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
#include "velox/type/Timestamp.h"
#include "velox/type/Type.h"
#include "velox/type/Variant.h"

namespace facebook::axiom::connector::hive {
namespace {

using namespace facebook::velox;

Variant toVariant(
    std::string_view value,
    const TypePtr& type,
    bool readTimestampAsLocalTime) {
  return HiveTableLayout::partitionValueToVariant(
      value, *type, readTimestampAsLocalTime);
}

TEST(PartitionValueToVariantTest, scalarTypes) {
  EXPECT_EQ(toVariant("42", BIGINT(), true), Variant(int64_t{42}));
  EXPECT_EQ(toVariant("hello", VARCHAR(), true), Variant(std::string("hello")));
  EXPECT_EQ(toVariant("true", BOOLEAN(), true), Variant(true));
}

TEST(PartitionValueToVariantTest, timestamp) {
  const auto value = toVariant("2020-01-01 12:34:56", TIMESTAMP(), true);

  // A plain TIMESTAMP partition value is stored as local time and read as GMT.
  // 2020-01-01 12:34:56 in America/Los_Angeles (PST, UTC-8) is 20:34:56 UTC =
  // 1577836800 (2020-01-01 UTC) + 74096 (20:34:56) seconds. Derived from the
  // fixed default timezone, not by re-applying the conversion under test.
  EXPECT_EQ(value.kind(), TypeKind::TIMESTAMP);
  EXPECT_EQ(value.value<TypeKind::TIMESTAMP>(), Timestamp(1'577'910'896, 0));
}

TEST(PartitionValueToVariantTest, timestampUtc) {
  // TIMESTAMP_UTC values are already UTC, so no local-time shift is applied:
  // 2020-01-01 12:34:56 stays 12:34:56 UTC = 1577836800 + 45296 seconds.
  const auto value = toVariant("2020-01-01 12:34:56", TIMESTAMP_UTC(), true);

  EXPECT_EQ(value.kind(), TypeKind::TIMESTAMP);
  EXPECT_EQ(value.value<TypeKind::TIMESTAMP>(), Timestamp(1'577'882'096, 0));
}

TEST(PartitionValueToVariantTest, timestampLocalTimeDisabled) {
  // With local-time reading disabled the value is taken as UTC, so no shift:
  // 2020-01-01 12:34:56 stays 12:34:56 UTC = 1577836800 + 45296 seconds.
  const auto value = toVariant(
      "2020-01-01 12:34:56", TIMESTAMP(), /*readTimestampAsLocalTime=*/false);

  EXPECT_EQ(value.kind(), TypeKind::TIMESTAMP);
  EXPECT_EQ(value.value<TypeKind::TIMESTAMP>(), Timestamp(1'577'882'096, 0));
}

TEST(PartitionValueToVariantTest, rejectsUnsupportedType) {
  VELOX_ASSERT_THROW(
      toVariant("1.5", DOUBLE(), true), "Unsupported partition column type");
}

} // namespace
} // namespace facebook::axiom::connector::hive
