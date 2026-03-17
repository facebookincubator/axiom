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

#include "axiom/common/SchemaTableName.h"

#include <unordered_map>

#include <gtest/gtest.h>

namespace facebook::axiom {
namespace {

TEST(SchemaTableNameTest, equality) {
  SchemaTableName a{"schema1", "table1"};
  SchemaTableName b{"schema1", "table1"};
  SchemaTableName c{"schema2", "table1"};
  SchemaTableName d{"schema1", "table2"};

  EXPECT_EQ(a, b);
  EXPECT_NE(a, c);
  EXPECT_NE(a, d);
}

TEST(SchemaTableNameTest, hashMapKey) {
  std::unordered_map<SchemaTableName, int> map;
  map[{"schema1", "table1"}] = 1;
  map[{"schema2", "table2"}] = 2;

  EXPECT_EQ(map.size(), 2);
  EXPECT_EQ(map.at({"schema1", "table1"}), 1);
  EXPECT_EQ(map.at({"schema2", "table2"}), 2);
}

} // namespace
} // namespace facebook::axiom
