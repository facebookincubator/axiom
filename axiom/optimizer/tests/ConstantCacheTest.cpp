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

#include "axiom/optimizer/ConstantCache.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "axiom/optimizer/QueryGraphContext.h"
#include "velox/common/memory/Memory.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/type/Variant.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class ConstantCacheTest : public ::testing::Test {
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

// Two TIMESTAMP WITH TIME ZONE values that share an instant but carry
// different timezone IDs intern to distinct Literals: downstream
// formatting and extraction operations depend on the tz_id, so collapsing
// them would yield wrong results.
TEST_F(ConstantCacheTest, getDistinguishesTimestampWithTimeZone) {
  constexpr int64_t kMillis = 1'778'148'000'000;
  const int16_t utcId = tz::getTimeZoneID("UTC");
  const int16_t laId = tz::getTimeZoneID("America/Los_Angeles");
  ASSERT_NE(utcId, laId);

  lp::ConstantExpr utc{
      TIMESTAMP_WITH_TIME_ZONE(),
      std::make_shared<Variant>(
          Variant::typeWithCustomComparison<TypeKind::BIGINT>(
              pack(kMillis, utcId), TIMESTAMP_WITH_TIME_ZONE()))};
  lp::ConstantExpr la{
      TIMESTAMP_WITH_TIME_ZONE(),
      std::make_shared<Variant>(
          Variant::typeWithCustomComparison<TypeKind::BIGINT>(
              pack(kMillis, laId), TIMESTAMP_WITH_TIME_ZONE()))};

  ConstantCache cache;
  EXPECT_NE(cache.get(utc), cache.get(la));
}

TEST_F(ConstantCacheTest, getInternsEqualScalars) {
  lp::ConstantExpr first{BIGINT(), std::make_shared<Variant>(int64_t{42})};
  lp::ConstantExpr second{BIGINT(), std::make_shared<Variant>(int64_t{42})};

  ConstantCache cache;
  EXPECT_EQ(cache.get(first), cache.get(second));
}

TEST_F(ConstantCacheTest, getDistinguishesArraysByElement) {
  const auto type = ARRAY(BIGINT());
  lp::ConstantExpr a{
      type,
      std::make_shared<Variant>(Variant::array(
          {Variant(int64_t{1}), Variant(int64_t{2}), Variant(int64_t{3})}))};
  lp::ConstantExpr b{
      type,
      std::make_shared<Variant>(Variant::array(
          {Variant(int64_t{1}), Variant(int64_t{2}), Variant(int64_t{4})}))};

  ConstantCache cache;
  EXPECT_NE(cache.get(a), cache.get(b));
}

TEST_F(ConstantCacheTest, getDistinguishesRowsByField) {
  const auto type = ROW({"x", "y"}, {BIGINT(), VARCHAR()});
  lp::ConstantExpr a{
      type,
      std::make_shared<Variant>(
          Variant::row({Variant(int64_t{7}), Variant(std::string{"hello"})}))};
  lp::ConstantExpr b{
      type,
      std::make_shared<Variant>(
          Variant::row({Variant(int64_t{7}), Variant(std::string{"world"})}))};

  ConstantCache cache;
  EXPECT_NE(cache.get(a), cache.get(b));
}

TEST_F(ConstantCacheTest, getDistinguishesMapsByValue) {
  const auto type = MAP(BIGINT(), VARCHAR());
  std::map<Variant, Variant> entriesA{
      {Variant(int64_t{1}), Variant(std::string{"one"})},
      {Variant(int64_t{2}), Variant(std::string{"two"})}};
  std::map<Variant, Variant> entriesB{
      {Variant(int64_t{1}), Variant(std::string{"one"})},
      {Variant(int64_t{2}), Variant(std::string{"TWO"})}};

  lp::ConstantExpr a{
      type, std::make_shared<Variant>(Variant::map(std::move(entriesA)))};
  lp::ConstantExpr b{
      type, std::make_shared<Variant>(Variant::map(std::move(entriesB)))};

  ConstantCache cache;
  EXPECT_NE(cache.get(a), cache.get(b));
}

TEST_F(ConstantCacheTest, getDistinguishesNullFromNonNull) {
  lp::ConstantExpr nullExpr{
      BIGINT(), std::make_shared<Variant>(TypeKind::BIGINT)};
  lp::ConstantExpr nonNull{BIGINT(), std::make_shared<Variant>(int64_t{0})};

  ConstantCache cache;
  EXPECT_NE(cache.get(nullExpr), cache.get(nonNull));
}

} // namespace
} // namespace facebook::axiom::optimizer
