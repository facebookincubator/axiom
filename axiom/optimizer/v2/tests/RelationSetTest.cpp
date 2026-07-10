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

#include <gtest/gtest.h>

#include "axiom/optimizer/v2/RelationSet.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::optimizer::v2::test {
namespace {

TEST(RelationSetTest, emptyAndAdd) {
  RelationSet set;
  EXPECT_TRUE(set.empty());
  EXPECT_EQ(set.size(), 0);
  EXPECT_FALSE(set.contains(0));

  set.add(0);
  set.add(63);
  EXPECT_FALSE(set.empty());
  EXPECT_EQ(set.size(), 2);
  EXPECT_TRUE(set.contains(0));
  EXPECT_TRUE(set.contains(63));
  EXPECT_FALSE(set.contains(1));
}

TEST(RelationSetTest, eraseAndClear) {
  RelationSet set;
  set.add(5);
  set.add(10);
  set.erase(5);
  EXPECT_FALSE(set.contains(5));
  EXPECT_TRUE(set.contains(10));

  set.clear();
  EXPECT_TRUE(set.empty());
}

TEST(RelationSetTest, setOperations) {
  RelationSet a;
  a.add(1);
  a.add(3);
  RelationSet b;
  b.add(3);
  b.add(5);

  EXPECT_TRUE(a.hasIntersection(b));

  RelationSet u{a};
  u.unionSet(b);
  EXPECT_EQ(u.size(), 3);
  EXPECT_TRUE(u.contains(1));
  EXPECT_TRUE(u.contains(3));
  EXPECT_TRUE(u.contains(5));

  RelationSet i{a};
  i.intersect(b);
  EXPECT_EQ(i.size(), 1);
  EXPECT_TRUE(i.contains(3));

  RelationSet d{a};
  d.except(b);
  EXPECT_EQ(d.size(), 1);
  EXPECT_TRUE(d.contains(1));
}

TEST(RelationSetTest, subset) {
  RelationSet sub;
  sub.add(2);
  RelationSet super;
  super.add(2);
  super.add(7);

  EXPECT_TRUE(sub.isSubset(super));
  EXPECT_FALSE(super.isSubset(sub));
  EXPECT_TRUE(RelationSet{}.isSubset(super));
}

TEST(RelationSetTest, forEachAscendingOrder) {
  RelationSet set;
  set.add(7);
  set.add(2);
  set.add(40);

  std::vector<int32_t> ids;
  set.forEach([&](int32_t id) { ids.push_back(id); });

  const std::vector<int32_t> expected{2, 7, 40};
  EXPECT_EQ(ids, expected);
}

TEST(RelationSetTest, anyOfShortCircuits) {
  RelationSet set;
  set.add(0);
  set.add(1);
  set.add(2);

  int visited = 0;
  const bool found = set.anyOf([&](int32_t id) {
    ++visited;
    return id == 1;
  });
  EXPECT_TRUE(found);
  EXPECT_EQ(visited, 2);
}

#ifndef NDEBUG
TEST(RelationSetTest, outOfRangeId) {
  RelationSet set;
  VELOX_ASSERT_THROW(set.add(64), "(64 vs. 64)");
  VELOX_ASSERT_THROW(set.add(-1), "(-1 vs. 0)");
}
#endif

TEST(RelationSetTest, equalityAndHash) {
  RelationSet a;
  a.add(1);
  a.add(5);
  RelationSet b;
  b.add(5);
  b.add(1);

  EXPECT_EQ(a, b);
  EXPECT_EQ(a.hash(), b.hash());
  EXPECT_EQ(std::hash<RelationSet>{}(a), a.hash());
}

} // namespace
} // namespace facebook::axiom::optimizer::v2::test
