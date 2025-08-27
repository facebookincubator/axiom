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

#include "axiom/logical_plan/ExprApi.h"
#include <gtest/gtest.h>
#include "axiom/logical_plan/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"

namespace facebook::velox::logical_plan {
namespace {

class ExprApiTest : public testing::Test {
 protected:
  void SetUp() override {
    aggregate::prestosql::registerAllAggregateFunctions();
  }

  std::string toString(const ExprApi& expr) {
    return expr.expr()->toString();
  }
};

TEST_F(ExprApiTest, arithmetic) {
  EXPECT_EQ(toString(Col("a") + 1), toString(Sql("a + 1")));
  EXPECT_EQ(toString(Col("a") - Lit(10)), toString(Sql("a - 10")));
  EXPECT_EQ(toString(Col("a") * Col("b")), toString(Sql("a * b")));
  EXPECT_EQ(toString(Col("a") / 1.2), toString(Sql("a / 1.2")));

  EXPECT_EQ(toString(Call("mod", {Col("a"), Lit(3)})), toString(Sql("a % 3")));
  EXPECT_EQ(toString(Call("plus", Col("a"), Lit(10))), toString(Sql("a + 10")));
}

TEST_F(ExprApiTest, call) {
  EXPECT_EQ(toString(Call("rand")), toString(Sql("rand()")));
  EXPECT_EQ(toString(Call("abs", Col("a"))), toString(Sql("abs(a)")));
  EXPECT_EQ(
      toString(Call("gt", Col("a"), Sql("b * c"))), toString(Sql("a > b * c")));
}

TEST_F(ExprApiTest, cast) {
  EXPECT_EQ(
      toString(Cast(BIGINT(), Col("a"))), toString(Sql("cast(a as bigint)")));
  EXPECT_EQ(toString(Cast(DOUBLE(), 1)), toString(Sql("1::double")));
}

TEST_F(ExprApiTest, lambda) {
  EXPECT_EQ(
      toString(Call("filter", Col("a"), Lambda({"x"}, Col("x") > 0))),
      toString(Sql("filter(a, x -> x > 0)")));

  EXPECT_EQ(
      toString(Call(
          "transform_values", Col("m"), Lambda({"k", "v"}, Col("k") + 10))),
      toString(Sql("transform_values(m, (k, v) -> k + 10)")));
}

TEST_F(ExprApiTest, subquery) {
  auto subquery = Subquery(PlanBuilder()
                               .values(
                                   ROW({"a"}, {BIGINT()}),
                                   std::vector<Variant>{Variant::row({123LL})})
                               .aggregate({}, {"sum(a) as x"})
                               .build());

  EXPECT_EQ(toString(Col("a") < subquery), "lt(\"a\",<subquery>)");
  EXPECT_EQ(subquery.name(), "x");
  EXPECT_EQ(subquery.as("y").name(), "y");
}

TEST_F(ExprApiTest, alias) {
  EXPECT_EQ(Col("a").name(), "a");
  EXPECT_EQ(Col("a").as("b").name(), "b");

  EXPECT_FALSE(Lit(1).name().has_value());

  EXPECT_EQ((Col("a") + 1).as("b").name(), "b");
  EXPECT_FALSE((Col("a") + 1).name().has_value());
}

TEST_F(ExprApiTest, inSubquery) {
  auto subquery = Subquery(PlanBuilder()
                               .values(
                                   ROW({"a"}, {BIGINT()}),
                                   std::vector<Variant>{Variant::row({123LL})})
                               .build());
  EXPECT_EQ(toString(In(Col("a"), subquery)), "in(\"a\",<subquery>)");
}

TEST_F(ExprApiTest, existsSubquery) {
  auto subquery = Subquery(PlanBuilder()
                               .values(
                                   ROW({"a"}, {BIGINT()}),
                                   std::vector<Variant>{Variant::row({123LL})})
                               .build());
  EXPECT_EQ(toString(Exists(subquery)), "exists(<subquery>)");
}

TEST_F(ExprApiTest, inList) {
  EXPECT_EQ(toString(In(Col("a"), Lit(1), Lit(2), Lit(3))), "in(\"a\",1,2,3)");
  EXPECT_EQ(
      toString(In(Col("a"), Lit("hello"), Lit("world"))),
      "in(\"a\",hello,world)");
  EXPECT_EQ(
      toString(In(Col("a"), Lit(1.1), Lit(2.2), Lit(3.3))),
      "in(\"a\",1.1,2.2,3.3)");
}

} // namespace
} // namespace facebook::velox::logical_plan
