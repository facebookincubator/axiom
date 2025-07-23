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

#include "axiom/logical_plan/PlanPrinter.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/connectors/tests/TestConnector.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

namespace facebook::velox::logical_plan {
namespace {

class PlanPrinterTest : public testing::Test {
 protected:
  static constexpr auto kTestConnectorId = "test";

  void SetUp() override {
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();

    auto connector =
        std::make_shared<connector::TestConnector>(kTestConnectorId);
    connector->addTable(
        "test", ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), VARCHAR()}));
    connector::registerConnector(connector);
  }

  void TearDown() override {
    connector::unregisterConnector(kTestConnectorId);
  }

  static std::vector<std::string> toLines(const LogicalPlanNodePtr& plan) {
    auto text = PlanPrinter::toText(*plan);

    LOG(INFO) << std::endl << text;

    std::vector<std::string> lines;
    folly::split("\n", text, lines);

    return lines;
  }
};

TEST_F(PlanPrinterTest, values) {
  auto plan = PlanBuilder()
                  .values(
                      ROW({"a"}, {BIGINT()}),
                      std::vector<Variant>{Variant::row({123LL})})
                  .filter("a > 10")
                  .with({"a + 2 as b"})
                  .project({"a + b as c"})
                  .sort({"c DESC"})
                  .limit(5)
                  .build();

  auto lines = toLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::StartsWith("- Limit"),
          testing::StartsWith("  - Sort"),
          testing::StartsWith("    - Project"),
          testing::StartsWith("        c := plus(a, b)"),
          testing::StartsWith("      - Project"),
          testing::StartsWith("          a := a"),
          testing::StartsWith("          b := plus(a, 2)"),
          testing::StartsWith("        - Filter: gt(a, 10)"),
          testing::StartsWith("          - Values"),
          testing::Eq("")));

  // Empty variant vector - zero rows:
  plan =
      PlanBuilder()
          .values(ROW({"a", "b"}, {BIGINT(), REAL()}), std::vector<Variant>{})
          .build();
  lines = toLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::StartsWith("- Values: 0 rows -> ROW<a:BIGINT,b:REAL>"),
          testing::Eq("")));
}

TEST_F(PlanPrinterTest, tableScan) {
  auto plan = PlanBuilder()
                  .tableScan(kTestConnectorId, "test", {"a", "b"})
                  .with({"cast(a as double) * b as c"})
                  .build();

  const auto lines = toLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::StartsWith("- Project"),
          testing::StartsWith("    a := a"),
          testing::StartsWith("    b := b"),
          testing::StartsWith("    c := multiply(CAST(a AS DOUBLE), b)"),
          testing::StartsWith("  - TableScan"),
          testing::Eq("")));
}

TEST_F(PlanPrinterTest, aggregate) {
  auto rowType = ROW({"a", "b"}, {INTEGER(), INTEGER()});
  std::vector<Variant> data{
      Variant::row({1, 10}),
      Variant::row({2, 20}),
      Variant::row({2, 21}),
      Variant::row({3, 30}),
  };

  auto plan =
      PlanBuilder()
          .values(rowType, data)
          .aggregate(
              {"a"}, {"sum(b) as total", "avg(b) as mean", "min(b + 1::int)"})
          .with({"total + 1", "mean * 0.3"})
          .build();

  const auto lines = toLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::StartsWith("- Project"),
          testing::StartsWith("    a := a"),
          testing::StartsWith("    total := total"),
          testing::StartsWith("    mean := mean"),
          testing::StartsWith("    min := min"),
          testing::StartsWith("    expr := plus(total, 1)"),
          testing::StartsWith("    expr_0 := multiply(mean, 0.3)"),
          testing::StartsWith("  - Aggregate"),
          testing::StartsWith("      total := sum(b)"),
          testing::StartsWith("      mean := avg(b)"),
          testing::StartsWith("      min := min(plus(b, "),
          testing::StartsWith("    - Values"),
          testing::Eq("")));
}

TEST_F(PlanPrinterTest, join) {
  auto leftType = ROW({"key", "v"}, {INTEGER(), INTEGER()});
  std::vector<Variant> leftData{
      Variant::row({1, 10}),
      Variant::row({2, 20}),
      Variant::row({2, 21}),
      Variant::row({3, 30}),
  };

  auto rightType = ROW({"key", "w"}, {INTEGER(), INTEGER()});
  std::vector<Variant> rightData{
      Variant::row({1, 11}),
      Variant::row({2, 22}),
  };

  auto context = PlanBuilder::Context();
  auto plan = PlanBuilder(context)
                  .values(leftType, leftData)
                  .as("l")
                  .join(
                      PlanBuilder(context).values(rightType, rightData).as("r"),
                      "l.key = r.key",
                      JoinType::kLeft)
                  .with({"l.v + r.w as z"})
                  .build();

  const auto lines = toLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::StartsWith("- Project"),
          testing::StartsWith("    key := key"),
          testing::StartsWith("    v := v"),
          testing::StartsWith("    key_0 := key_0"),
          testing::StartsWith("    w := w"),
          testing::StartsWith("    z := plus(v, w)"),
          testing::StartsWith("  - Join LEFT: eq(key, key_0)"),
          testing::StartsWith("    - Values: 4 rows"),
          testing::StartsWith("    - Values: 2 rows"),
          testing::Eq("")));
}

TEST_F(PlanPrinterTest, crossJoin) {
  auto leftType = ROW({"key", "v"}, {INTEGER(), INTEGER()});
  std::vector<Variant> leftData{
      Variant::row({1, 10}),
      Variant::row({2, 20}),
      Variant::row({3, 30}),
  };

  auto rightType = ROW({"key", "w"}, {INTEGER(), INTEGER()});
  std::vector<Variant> rightData{
      Variant::row({1, 11}),
      Variant::row({2, 22}),
  };

  PlanBuilder::Context context;
  auto plan = PlanBuilder(context)
                  .values(leftType, leftData)
                  .as("l")
                  .join(
                      PlanBuilder(context).values(rightType, rightData).as("r"),
                      "",
                      JoinType::kInner)
                  .with({"l.v + r.w as z"})
                  .build();

  const auto lines = toLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::StartsWith("- Project"),
          testing::StartsWith("    key := key"),
          testing::StartsWith("    v := v"),
          testing::StartsWith("    key_0 := key_0"),
          testing::StartsWith("    w := w"),
          testing::StartsWith("    z := plus(v, w)"),
          testing::StartsWith("  - Join INNER:"), // Condition is empty
          testing::StartsWith("    - Values: 3 rows"),
          testing::StartsWith("    - Values: 2 rows"),
          testing::Eq("")));
}

TEST_F(PlanPrinterTest, specialForms) {
  auto rowType =
      ROW({"a", "b", "c"},
          {INTEGER(), INTEGER(), ROW({"x", "y"}, {BOOLEAN(), REAL()})});
  std::vector<Variant> data{
      Variant::row({1, 10, Variant::row({true, 0.1f})}),
      Variant::row({2, 20, Variant::row({true, 0.2f})}),
      Variant::row({2, 21, Variant::row({true, 0.21f})}),
      Variant::row({3, 30, Variant::row({true, 0.3f})}),
  };

  auto plan =
      PlanBuilder()
          .values(rowType, data)
          .map({
              "a > b AND b > 10::int as a",
              "a < b OR b > 0::int as b",
              "cast(a as double) * 1.2",
              "try_cast(a / b as varchar)",
              "try(a / b)",
              "c.x",
              "coalesce(a, b, 33::int) as c",
              "if(a > b, a, b)",
              "case a when 1::int then 'a' when 2::int then 'b' else 'c' end",
              "c[2]",
          })
          .build();

  auto lines = toLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::StartsWith("- Project"),
          testing::StartsWith("    a := a_0"),
          testing::StartsWith("    b := b_1"),
          testing::StartsWith("    expr "),
          testing::StartsWith("    expr_2 "),
          testing::StartsWith("    expr_3 "),
          testing::StartsWith("    expr_4 "),
          testing::StartsWith("    c := c_5"),
          testing::StartsWith("    expr_6 "),
          testing::StartsWith("    expr_7 "),
          testing::StartsWith("    expr_8 "),
          testing::StartsWith("  - Project"),
          testing::StartsWith("      a_0 := AND"),
          testing::StartsWith("      b_1 := OR"),
          testing::StartsWith("      expr := multiply(CAST("),
          testing::StartsWith("      expr_2 := TRY_CAST"),
          testing::StartsWith("      expr_3 := TRY"),
          testing::StartsWith("      expr_4 := DEREFERENCE(c, x)"),
          testing::StartsWith("      c_5 := COALESCE"),
          testing::StartsWith("      expr_6 := IF"),
          testing::StartsWith("      expr_7 := SWITCH"),
          testing::StartsWith("      expr_8 := DEREFERENCE(c, 1)"),
          testing::StartsWith("    - Values: 4 rows"),
          testing::Eq("")));
}

TEST_F(PlanPrinterTest, lambda) {
  auto rowType = ROW({"a", "b"}, {BIGINT(), BIGINT()});
  std::vector<Variant> data{
      Variant::row({1L, 10L}),
      Variant::row({2L, 20L}),
  };

  auto plan = PlanBuilder()
                  .values(rowType, data)
                  .map({"filter(sequence(1::bigint, a), x -> (x > b))"})
                  .build();

  auto lines = toLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::StartsWith("- Project"),
          testing::StartsWith(
              "    expr := filter(sequence(CAST(1 AS BIGINT), a), x -> gt(x, b))"),
          testing::StartsWith("  - Values"),
          testing::Eq("")));
}

} // namespace
} // namespace facebook::velox::logical_plan
