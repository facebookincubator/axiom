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

#include "axiom/cli/QueryIdGenerator.h"
#include <gtest/gtest.h>
#include <re2/re2.h>
#include <set>

namespace axiom::cli {
namespace {

// Validates query ID format: YYYYMMdd_HHmmss_NNNNN_suffix (27 chars).
bool isValidQueryIdFormat(const std::string& queryId) {
  static const re2::RE2 kPattern(R"(\d{8}_\d{6}_\d{5}_[a-kmn-z2-9]{5})");
  return re2::RE2::FullMatch(queryId, kPattern);
}

TEST(QueryIdGeneratorTest, formatMatchesExpectedPattern) {
  QueryIdGenerator generator;
  auto queryId = generator.createNextQueryId();

  // Expected format: YYYYMMdd_HHmmss_NNNNN_suffix
  // e.g. 20260305_143022_00000_abcde
  EXPECT_TRUE(isValidQueryIdFormat(queryId))
      << "Query ID did not match expected format: " << queryId;
}

TEST(QueryIdGeneratorTest, queryIdSuffixIsFiveChars) {
  QueryIdGenerator generator;
  auto suffix = generator.suffix();
  EXPECT_EQ(suffix.size(), 5);

  // All characters should be in the base-32 alphabet.
  for (char character : suffix) {
    EXPECT_TRUE(
        (character >= 'a' && character <= 'z' && character != 'l' &&
         character != 'o') ||
        (character >= '2' && character <= '9'))
        << "Invalid base-32 character: " << character;
  }
}

TEST(QueryIdGeneratorTest, counterIncrements) {
  QueryIdGenerator generator;
  auto first = generator.createNextQueryId();
  auto second = generator.createNextQueryId();
  auto third = generator.createNextQueryId();

  // Extract counter portion (index 16..20, the 5-digit counter).
  EXPECT_EQ(first.substr(16, 5), "00000");
  EXPECT_EQ(second.substr(16, 5), "00001");
  EXPECT_EQ(third.substr(16, 5), "00002");
}

TEST(QueryIdGeneratorTest, idsAreUnique) {
  QueryIdGenerator generator;
  std::set<std::string> ids;
  constexpr int kNumIds{100};
  for (int i = 0; i < kNumIds; ++i) {
    ids.insert(generator.createNextQueryId());
  }
  EXPECT_EQ(ids.size(), kNumIds);
}

TEST(QueryIdGeneratorTest, queryIdSuffix) {
  QueryIdGenerator generator;
  auto suffix = generator.suffix();
  auto queryId = generator.createNextQueryId();

  // Query ID should end with the suffix.
  EXPECT_TRUE(queryId.ends_with(suffix))
      << "Query ID '" << queryId << "' should end with suffix '" << suffix
      << "'";
}

} // namespace
} // namespace axiom::cli
