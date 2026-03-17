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

#include "axiom/common/CatalogSchemaTableName.h"

#include <unordered_map>
#include <unordered_set>

#include <gtest/gtest.h>

namespace facebook::axiom {
namespace {

TEST(CatalogSchemaTableNameTest, equality) {
  CatalogSchemaTableName a{"cat1", {"schema1", "table1"}};
  CatalogSchemaTableName b{"cat1", {"schema1", "table1"}};
  CatalogSchemaTableName c{"cat2", {"schema1", "table1"}};
  CatalogSchemaTableName d{"cat1", {"schema2", "table1"}};
  CatalogSchemaTableName e{"cat1", {"schema1", "table2"}};

  EXPECT_EQ(a, b);
  EXPECT_NE(a, c);
  EXPECT_NE(a, d);
  EXPECT_NE(a, e);
}

TEST(CatalogSchemaTableNameTest, hashMapKey) {
  std::unordered_map<CatalogSchemaTableName, int> map;
  map[{"cat1", {"s1", "t1"}}] = 1;
  map[{"cat1", {"s1", "t2"}}] = 2;
  map[{"cat2", {"s1", "t1"}}] = 3;

  EXPECT_EQ(map.size(), 3);
  EXPECT_EQ(map.at({"cat1", {"s1", "t1"}}), 1);
  EXPECT_EQ(map.at({"cat1", {"s1", "t2"}}), 2);
  EXPECT_EQ(map.at({"cat2", {"s1", "t1"}}), 3);
}

TEST(CatalogSchemaTableNameTest, hashSetKey) {
  std::unordered_set<CatalogSchemaTableName> set;
  set.insert({"cat1", {"s1", "t1"}});
  set.insert({"cat1", {"s1", "t1"}});
  set.insert({"cat2", {"s1", "t1"}});

  EXPECT_EQ(set.size(), 2);
  EXPECT_TRUE(set.contains({"cat1", {"s1", "t1"}}));
  EXPECT_TRUE(set.contains({"cat2", {"s1", "t1"}}));
}

} // namespace
} // namespace facebook::axiom
