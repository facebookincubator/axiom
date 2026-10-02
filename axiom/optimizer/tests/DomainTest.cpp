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

#include "axiom/optimizer/Domain.h"

#include <gtest/gtest.h>

using namespace facebook::velox;

namespace facebook::axiom::optimizer {
namespace {
class DomainTest : public testing::Test {};

TEST_F(DomainTest, allAndNone) {
  auto all = Domain::all();
  ASSERT_TRUE(all.isAll());
  ASSERT_FALSE(all.isNone());

  auto none = Domain::none();
  ASSERT_TRUE(none.isNone());
  ASSERT_FALSE(none.isAll());
}

TEST_F(DomainTest, singleValue) {
  auto domain = Domain::singleValue(Variant("2026-03-17"));
  ASSERT_FALSE(domain.isAll());
  ASSERT_FALSE(domain.isNone());
  ASSERT_FALSE(domain.nullsAllowed());
  ASSERT_EQ(domain.ranges().size(), 1);
  ASSERT_TRUE(domain.ranges()[0].isSingleValue());
}

TEST_F(DomainTest, greaterThan) {
  auto domain = Domain::greaterThan(Variant("2026-03-17"));
  ASSERT_EQ(domain.ranges().size(), 1);
  ASSERT_FALSE(domain.ranges()[0].lowInclusive());
  ASSERT_FALSE(domain.ranges()[0].high().has_value());
}

TEST_F(DomainTest, lessThanOrEqual) {
  auto domain = Domain::lessThanOrEqual(Variant("2026-03-31"));
  ASSERT_EQ(domain.ranges().size(), 1);
  ASSERT_FALSE(domain.ranges()[0].low().has_value());
  ASSERT_TRUE(domain.ranges()[0].highInclusive());
}

TEST_F(DomainTest, intersectClosedRange) {
  auto gte = Domain::greaterThanOrEqual(Variant("2026-03-01"));
  auto lte = Domain::lessThanOrEqual(Variant("2026-03-31"));
  auto result = gte.intersect(lte);
  ASSERT_EQ(result.ranges().size(), 1);
  ASSERT_TRUE(result.ranges()[0].lowInclusive());
  ASSERT_TRUE(result.ranges()[0].highInclusive());
}

TEST_F(DomainTest, intersectContradiction) {
  auto gt = Domain::greaterThan(Variant("2026-03-31"));
  auto lt = Domain::lessThan(Variant("2026-03-01"));
  auto result = gt.intersect(lt);
  ASSERT_TRUE(result.isNone());
}

TEST_F(DomainTest, uniteDisjoint) {
  auto lhs = Domain::singleValue(Variant("2026-03-17"));
  auto rhs = Domain::singleValue(Variant("2026-03-18"));
  auto result = lhs.unite(rhs);
  ASSERT_EQ(result.ranges().size(), 2);
}

TEST_F(DomainTest, uniteOverlapping) {
  auto lhs = Domain::greaterThanOrEqual(Variant("2026-03-01"))
                 .intersect(Domain::lessThanOrEqual(Variant("2026-03-15")));
  auto rhs = Domain::greaterThanOrEqual(Variant("2026-03-10"))
                 .intersect(Domain::lessThanOrEqual(Variant("2026-03-31")));
  auto result = lhs.unite(rhs);
  ASSERT_EQ(result.ranges().size(), 1);
}

TEST_F(DomainTest, subtract) {
  auto excluded = Domain::greaterThanOrEqual(Variant(int64_t{10}))
                      .intersect(Domain::lessThanOrEqual(Variant(int64_t{20})));

  auto result = Domain::notNull().subtract(excluded);
  ASSERT_FALSE(result.nullsAllowed());
  ASSERT_EQ(result.ranges().size(), 2);
  EXPECT_EQ(result.ranges()[0].high()->value, Variant(int64_t{10}));
  EXPECT_FALSE(result.ranges()[0].highInclusive());
  EXPECT_EQ(result.ranges()[1].low()->value, Variant(int64_t{20}));
  EXPECT_FALSE(result.ranges()[1].lowInclusive());
}

TEST_F(DomainTest, nulls) {
  auto onlyNull = Domain::onlyNull();
  ASSERT_TRUE(onlyNull.nullsAllowed());
  ASSERT_TRUE(onlyNull.ranges().empty());

  auto notNull = Domain::notNull();
  ASSERT_FALSE(notNull.nullsAllowed());
  ASSERT_FALSE(notNull.isAll());
  ASSERT_FALSE(notNull.isNone());

  auto result = onlyNull.unite(Domain::singleValue(Variant("x")));
  ASSERT_TRUE(result.nullsAllowed());
  ASSERT_EQ(result.ranges().size(), 1);
}

TEST_F(DomainTest, intersectWithAll) {
  auto domain = Domain::singleValue(Variant("2026-03-17"));
  auto result = domain.intersect(Domain::all());
  ASSERT_EQ(result.ranges().size(), 1);
  ASSERT_TRUE(result.ranges()[0].isSingleValue());
}

TEST_F(DomainTest, intersectWithNone) {
  auto domain = Domain::singleValue(Variant("2026-03-17"));
  auto result = domain.intersect(Domain::none());
  ASSERT_TRUE(result.isNone());
}

TEST_F(DomainTest, bigintRanges) {
  auto gt = Domain::greaterThan(Variant(static_cast<int64_t>(100)));
  auto lte = Domain::lessThanOrEqual(Variant(static_cast<int64_t>(200)));
  auto result = gt.intersect(lte);
  ASSERT_EQ(result.ranges().size(), 1);
  ASSERT_FALSE(result.ranges()[0].lowInclusive());
  ASSERT_TRUE(result.ranges()[0].highInclusive());
}

TEST_F(DomainTest, inValues) {
  auto domain = Domain::in({
      Variant("a"),
      Variant("b"),
      Variant("c"),
  });
  ASSERT_EQ(domain.ranges().size(), 3);
  ASSERT_FALSE(domain.nullsAllowed());
  // Ranges should be sorted.
  ASSERT_TRUE(domain.ranges()[0].isSingleValue());
}

TEST_F(DomainTest, intersectNullHandling) {
  // null AND not-null = no nulls.
  auto result = Domain::onlyNull().intersect(Domain::notNull());
  ASSERT_TRUE(result.isNone());

  // all AND onlyNull = onlyNull.
  result = Domain::all().intersect(Domain::onlyNull());
  ASSERT_TRUE(result.nullsAllowed());
  ASSERT_TRUE(result.ranges().empty());
}

TEST_F(DomainTest, lessThan) {
  auto domain = Domain::lessThan(Variant("2026-03-17"));
  ASSERT_EQ(domain.ranges().size(), 1);
  ASSERT_FALSE(domain.ranges()[0].low().has_value());
  ASSERT_TRUE(domain.ranges()[0].high().has_value());
  ASSERT_FALSE(domain.ranges()[0].highInclusive());
}

TEST_F(DomainTest, greaterThanOrEqual) {
  auto domain = Domain::greaterThanOrEqual(Variant("2026-03-17"));
  ASSERT_EQ(domain.ranges().size(), 1);
  ASSERT_TRUE(domain.ranges()[0].low().has_value());
  ASSERT_TRUE(domain.ranges()[0].lowInclusive());
  ASSERT_FALSE(domain.ranges()[0].high().has_value());
}

TEST_F(DomainTest, inWithDuplicates) {
  auto domain = Domain::in({
      Variant("a"),
      Variant("b"),
      Variant("a"),
      Variant("c"),
      Variant("b"),
  });
  // Duplicates are merged by normalize. Result is sorted: a, b, c.
  ASSERT_EQ(domain.ranges().size(), 3);
  ASSERT_TRUE(domain.ranges()[0].isSingleValue());
  ASSERT_TRUE(domain.ranges()[1].isSingleValue());
  ASSERT_TRUE(domain.ranges()[2].isSingleValue());
  ASSERT_EQ(domain.ranges()[0].low()->value, Variant("a"));
  ASSERT_EQ(domain.ranges()[1].low()->value, Variant("b"));
  ASSERT_EQ(domain.ranges()[2].low()->value, Variant("c"));
}

TEST_F(DomainTest, rangeIsEmpty) {
  // A range where low > high is empty.
  Range empty(Bound{Variant("z"), true}, Bound{Variant("a"), true});
  ASSERT_TRUE(empty.isEmpty());

  // A range where low == high but one is exclusive is empty.
  Range exclusivePoint(Bound{Variant("x"), true}, Bound{Variant("x"), false});
  ASSERT_TRUE(exclusivePoint.isEmpty());

  // A valid single-value range is not empty.
  auto point = Range::singleValue(Variant("x"));
  ASSERT_FALSE(point.isEmpty());

  // An unbounded range is not empty.
  ASSERT_FALSE(Range::unbounded().isEmpty());
}

} // namespace
} // namespace facebook::axiom::optimizer
