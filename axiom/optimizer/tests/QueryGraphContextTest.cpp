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

} // namespace
} // namespace facebook::axiom::optimizer
