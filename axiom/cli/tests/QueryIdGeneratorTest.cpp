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

namespace axiom::cli {
namespace {

// YYYYMMdd_HHmmss_NNNNN_suffix (27 chars).
static const re2::RE2 kQueryIdPattern(R"(\d{8}_\d{6}_\d{5}_[a-kmn-z2-9]{5})");

TEST(QueryIdGeneratorTest, format) {
  QueryIdGenerator generator;
  EXPECT_TRUE(
      re2::RE2::FullMatch(generator.createNextQueryId(), kQueryIdPattern));
}

TEST(QueryIdGeneratorTest, counterIncrements) {
  QueryIdGenerator generator;

  // Counter occupies positions 16..20 in the query ID
  // (after 8-char date + '_' + 6-char time + '_').
  auto nextId = [&]() { return generator.createNextQueryId(); };

  EXPECT_EQ(nextId().substr(16, 5), "00000");
  EXPECT_EQ(nextId().substr(16, 5), "00001");
  EXPECT_EQ(nextId().substr(16, 5), "00002");
}

} // namespace
} // namespace axiom::cli
