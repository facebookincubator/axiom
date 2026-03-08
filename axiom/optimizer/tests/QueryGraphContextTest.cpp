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

#include "axiom/optimizer/QueryGraphContext.h"
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <algorithm>
#include "velox/common/memory/Memory.h"
#include "velox/type/Type.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;

class QueryGraphContextTest : public ::testing::Test {
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

TEST_F(QueryGraphContextTest, f14Map) {
  QGF14FastMap<int32_t, double> map;

  for (auto i = 0; i < 10'000; i++) {
    map.try_emplace(i, i + 0.05);
  }
  for (auto i = 0; i < 10'000; i++) {
    ASSERT_EQ(1, map.count(i));
  }

  map.clear();
  for (auto i = 0; i < 10'000; i++) {
    ASSERT_EQ(0, map.count(i));
  }

  for (auto i = 10'000; i < 20'000; i++) {
    map.try_emplace(i, i + 0.15);
  }
  for (auto i = 10'000; i < 20'000; i++) {
    ASSERT_EQ(1, map.count(i));
  }
}

TEST_F(QueryGraphContextTest, f14Set) {
  QGF14FastSet<int32_t> set;

  for (auto i = 0; i < 10'000; i++) {
    set.insert(i);
  }
  for (auto i = 0; i < 10'000; i++) {
    ASSERT_EQ(1, set.count(i));
  }

  set.clear();
  for (auto i = 0; i < 10'000; i++) {
    ASSERT_EQ(0, set.count(i));
  }

  for (auto i = 10'000; i < 20'000; i++) {
    set.insert(i);
  }
  for (auto i = 10'000; i < 20'000; i++) {
    ASSERT_EQ(1, set.count(i));
  }
}

TEST_F(QueryGraphContextTest, toType) {
  TypePtr row1 = ROW({{"c1", ROW({{"c1a", INTEGER()}})}, {"c2", DOUBLE()}});
  TypePtr row2 = row1 =
      ROW({{"c1", ROW({{"c1a", INTEGER()}})}, {"c2", DOUBLE()}});
  TypePtr largeRow = ROW(
      {{"c1", ROW({{"c1a", INTEGER()}})},
       {"c2", DOUBLE()},
       {"m1", MAP(INTEGER(), ARRAY(INTEGER()))}});
  TypePtr differentNames =
      ROW({{"different", ROW({{"c1a", INTEGER()}})}, {"c2", DOUBLE()}});

  auto* dedupRow1 = toType(row1);
  auto* dedupRow2 = toType(row2);
  auto* dedupLargeRow = toType(largeRow);
  auto* dedupDifferentNames = toType(differentNames);

  // dedupped complex types make a copy.
  EXPECT_NE(row1.get(), dedupRow1);

  // Identical types get equal pointers.
  EXPECT_EQ(dedupRow1, dedupRow2);

  // Different names differentiate types.
  EXPECT_NE(dedupDifferentNames, dedupRow1);

  // Shared complex substructure makes equal pointers.
  EXPECT_EQ(dedupRow1->childAt(0).get(), dedupLargeRow->childAt(0).get());

  // Identical child types with different names get equal pointers.
  EXPECT_EQ(dedupRow1->childAt(0).get(), dedupDifferentNames->childAt(0).get());

  auto* path = make<Path>()
                   ->subscript("field")
                   ->subscript(123)
                   ->field("f1")
                   ->cardinality();
  auto interned = queryCtx()->toPath(path);
  EXPECT_EQ(interned, path);
  auto* path2 = make<Path>()
                    ->subscript("field")
                    ->subscript(123)
                    ->field("f1")
                    ->cardinality();
  auto interned2 = queryCtx()->toPath(path2);
  EXPECT_EQ(interned2, interned);
}

TEST_F(QueryGraphContextTest, stepOrdering) {
  // Steps with different kinds.
  {
    Step a{.kind = StepKind::kField, .field = toName("a")};
    Step b{.kind = StepKind::kSubscript, .field = toName("b")};
    Step c{.kind = StepKind::kCardinality};

    // kField < kSubscript < kCardinality (enum order).
    // Verify ordering and asymmetry: if A < B then B > A.
    EXPECT_LT(a, b);
    EXPECT_GT(b, a);
    EXPECT_LT(b, c);
    EXPECT_GT(c, b);

    // Irreflexivity for <, > and reflexivity for >=.
    EXPECT_FALSE(a < a);
    EXPECT_FALSE(a > a);
    EXPECT_GE(a, a);
    EXPECT_EQ(a, a);
    EXPECT_FALSE(b < b);
    EXPECT_FALSE(b > b);
    EXPECT_GE(b, b);
    EXPECT_EQ(b, b);
  }

  // Steps with same kind but different fields.
  {
    Step a{.kind = StepKind::kField, .field = toName("a")};
    Step b{.kind = StepKind::kField, .field = toName("b")};
    // kField steps compare by id, not field pointer.
    // Both have id=0, so neither is less than the other.
    EXPECT_GE(a, b);
    EXPECT_GE(b, a);
    EXPECT_FALSE(a > b);
    EXPECT_FALSE(b > a);
  }

  // Steps with same kind but different ids.
  {
    Step a{.kind = StepKind::kSubscript, .id = 1};
    Step b{.kind = StepKind::kSubscript, .id = 2};
    EXPECT_LT(a, b);
    EXPECT_GT(b, a);
    EXPECT_GE(b, a);
    EXPECT_GE(a, a);
    EXPECT_FALSE(a > a);
  }
}

TEST_F(QueryGraphContextTest, pathOrdering) {
  // Use subscript steps with different ids so that steps have distinct values
  // under operator<.
  auto* a = make<Path>()->subscript(1)->subscript(3);
  auto* b = make<Path>()->subscript(3)->subscript(1);
  auto* c = make<Path>()->subscript(1);
  auto* d = make<Path>()->subscript(1)->subscript(3)->subscript(5);

  // Irreflexivity for < and reflexivity for >=.
  EXPECT_FALSE(*a < *a);
  EXPECT_GE(*a, *a);
  EXPECT_EQ(*a, *a);
  EXPECT_FALSE(*b < *b);
  EXPECT_GE(*b, *b);
  EXPECT_EQ(*b, *b);
  EXPECT_FALSE(*c < *c);
  EXPECT_GE(*c, *c);
  EXPECT_EQ(*c, *c);

  // Asymmetry: if A < B then B >= A.
  // c (1 step) < a (2 steps, same prefix).
  EXPECT_LT(*c, *a);
  EXPECT_GE(*a, *c);

  // a (2 steps) < d (3 steps, same prefix).
  EXPECT_LT(*a, *d);
  EXPECT_GE(*d, *a);

  // Asymmetry: a [sub(1), sub(3)] < b [sub(3), sub(1)] because
  // the first step differs (1 < 3). Verify the reverse does not hold.
  EXPECT_LT(*a, *b);
  EXPECT_GE(*b, *a);

  // Verify std::sort works properly.
  std::vector<Path> pathValues = {*a, *b, *c, *d};
  std::sort(pathValues.begin(), pathValues.end());
  for (size_t i = 1; i < pathValues.size(); ++i) {
    EXPECT_FALSE(pathValues[i] < pathValues[i - 1])
        << "Sort result not ordered at index " << i;
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
