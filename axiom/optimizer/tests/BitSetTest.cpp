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

#include "axiom/optimizer/BitSet.h"
#include <gtest/gtest.h>
#include "velox/common/memory/Memory.h"

namespace facebook::axiom::optimizer {
namespace {

class BitSetTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = velox::memory::memoryManager()->addLeafPool();
    allocator_ = std::make_unique<velox::HashStringAllocator>(pool_.get());
    ctx_ = std::make_unique<QueryGraphContext>(*allocator_);
    queryCtx() = ctx_.get();
  }

  void TearDown() override {
    queryCtx() = nullptr;
    ctx_.reset();
    allocator_.reset();
    pool_.reset();
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<velox::HashStringAllocator> allocator_;
  std::unique_ptr<QueryGraphContext> ctx_;
};

TEST_F(BitSetTest, emptySet) {
  BitSet set;
  EXPECT_TRUE(set.empty());
  EXPECT_EQ(set.size(), 0);
  EXPECT_FALSE(set.contains(0));
  EXPECT_FALSE(set.contains(100));
}

TEST_F(BitSetTest, addAndContains) {
  BitSet set;
  set.add(0);
  set.add(0);
  EXPECT_EQ(set.size(), 1);
  EXPECT_TRUE(set.contains(0));
  EXPECT_FALSE(set.contains(1));

  // Test across word boundaries.
  set.add(63);
  EXPECT_TRUE(set.contains(63));
  EXPECT_FALSE(set.contains(64));

  set.add(64);
  EXPECT_TRUE(set.contains(64));

  EXPECT_FALSE(set.empty());
  EXPECT_EQ(set.size(), 3);
}

TEST_F(BitSetTest, erase) {
  BitSet set;
  set.add(10);
  set.add(20);
  set.add(30);
  EXPECT_EQ(set.size(), 3);

  set.erase(20);
  EXPECT_FALSE(set.contains(20));
  EXPECT_TRUE(set.contains(10));
  EXPECT_TRUE(set.contains(30));
  EXPECT_EQ(set.size(), 2);

  // Erasing non-existent element should be safe.
  set.erase(100);
  EXPECT_EQ(set.size(), 2);

  // Erasing element beyond allocated size should be safe.
  set.erase(10000);
  EXPECT_EQ(set.size(), 2);
}

TEST_F(BitSetTest, clear) {
  BitSet set;
  set.add(1);
  set.add(100);
  set.add(1000);
  EXPECT_FALSE(set.empty());

  set.clear();
  EXPECT_TRUE(set.empty());
  EXPECT_EQ(set.size(), 0);
  EXPECT_FALSE(set.contains(1));
  EXPECT_FALSE(set.contains(100));
  EXPECT_FALSE(set.contains(1000));
}

TEST_F(BitSetTest, equality) {
  BitSet a;
  BitSet b;
  EXPECT_EQ(a, b);

  a.add(1);
  b.add(1);
  EXPECT_EQ(a, b);

  b.add(2);
  EXPECT_NE(a, b);

  a.add(2);
  EXPECT_EQ(a, b);
}

TEST_F(BitSetTest, hash) {
  BitSet a;
  BitSet b;
  EXPECT_EQ(a.hash(), b.hash());

  a.add(1);
  b.add(1);
  EXPECT_EQ(a.hash(), b.hash());

  b.add(3);
  EXPECT_NE(a.hash(), b.hash());
}

TEST_F(BitSetTest, isSubset) {
  BitSet a;
  BitSet b;

  // Empty set is subset of empty set.
  EXPECT_TRUE(a.isSubset(b));

  b.add(1);
  b.add(2);

  // Empty set is subset of any set.
  EXPECT_TRUE(a.isSubset(b));

  a.add(2);
  EXPECT_TRUE(a.isSubset(b));

  a.add(4);
  EXPECT_FALSE(a.isSubset(b));
}

TEST_F(BitSetTest, hasIntersection) {
  BitSet a;
  BitSet b;

  // Empty sets have no intersection.
  EXPECT_FALSE(a.hasIntersection(b));

  a.add(1);
  a.add(2);
  EXPECT_FALSE(a.hasIntersection(b));

  b.add(3);
  EXPECT_FALSE(a.hasIntersection(b));

  b.add(2);
  EXPECT_TRUE(a.hasIntersection(b));
}

TEST_F(BitSetTest, intersect) {
  BitSet a;
  a.add(1);
  a.add(2);
  a.add(3);

  BitSet b;
  b.add(2);
  b.add(3);
  b.add(4);

  a.intersect(b);
  EXPECT_FALSE(a.contains(1));
  EXPECT_TRUE(a.contains(2));
  EXPECT_TRUE(a.contains(3));
  EXPECT_FALSE(a.contains(4));
  EXPECT_EQ(a.size(), 2);
}

TEST_F(BitSetTest, unionSet) {
  BitSet a;
  a.add(1);
  a.add(2);

  BitSet b;
  b.add(3);
  b.add(4);

  a.unionSet(b);
  EXPECT_TRUE(a.contains(1));
  EXPECT_TRUE(a.contains(2));
  EXPECT_TRUE(a.contains(3));
  EXPECT_TRUE(a.contains(4));
  EXPECT_EQ(a.size(), 4);
}

TEST_F(BitSetTest, except) {
  BitSet a;
  a.add(1);
  a.add(2);
  a.add(3);
  a.add(4);

  BitSet b;
  b.add(2);
  b.add(4);

  a.except(b);
  EXPECT_TRUE(a.contains(1));
  EXPECT_FALSE(a.contains(2));
  EXPECT_TRUE(a.contains(3));
  EXPECT_FALSE(a.contains(4));
  EXPECT_EQ(a.size(), 2);
}

TEST_F(BitSetTest, forEach) {
  BitSet set;
  set.add(5);
  set.add(10);
  set.add(100);

  std::vector<int32_t> collected;
  set.forEach([&](int32_t id) { collected.push_back(id); });

  const std::vector<int32_t> expected = {5, 10, 100};
  EXPECT_EQ(collected, expected);
}

TEST_F(BitSetTest, largeIds) {
  BitSet set;

  // Test with large IDs across multiple words.
  set.add(0);
  set.add(63);
  set.add(64);
  set.add(128);
  set.add(1000);
  set.add(10000);

  EXPECT_TRUE(set.contains(0));
  EXPECT_TRUE(set.contains(63));
  EXPECT_TRUE(set.contains(64));
  EXPECT_TRUE(set.contains(128));
  EXPECT_TRUE(set.contains(1000));
  EXPECT_TRUE(set.contains(10000));
  EXPECT_EQ(set.size(), 6);

  // IDs not added should not be contained.
  EXPECT_FALSE(set.contains(1));
  EXPECT_FALSE(set.contains(62));
  EXPECT_FALSE(set.contains(65));
  EXPECT_FALSE(set.contains(999));
}

} // namespace
} // namespace facebook::axiom::optimizer
