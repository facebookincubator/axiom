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

  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();

    auto connector =
        std::make_shared<connector::TestConnector>(kTestConnectorId);
    connector->createTable(
        "test", ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), VARCHAR()}));
    connector::registerConnector(connector);
  }

  void TearDown() override {
    connector::unregisterConnector(kTestConnectorId);
  }

  static std::vector<std::string> toLines(const LogicalPlanNodePtr& plan) {
    auto text = PlanPrinter::toText(*plan);
    return toLines(text);
  }

  static std::vector<std::string> toSummaryLines(
      const LogicalPlanNodePtr& plan,
      const PlanSummaryOptions& options = {}) {
    auto text = PlanPrinter::summarizeToText(*plan, options);
    return toLines(text);
  }

  static std::vector<std::string> toSkeletonLines(
      const LogicalPlanNodePtr& plan) {
    auto text = PlanPrinter::toSkeletonText(*plan);
    return toLines(text);
  }

  static std::vector<std::string> toLines(const std::string& text) {
    LOG(INFO) << std::endl << text;

    std::vector<std::string> lines;
    folly::split('\n', text, lines);

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

  lines = toSummaryLines(plan);

  EXPECT_THAT(
      lines,
      // clang-format off
      testing::ElementsAre(
          testing::Eq("- LIMIT [5]: 1 fields: c BIGINT"),
          testing::Eq("      limit: 5"),
          testing::Eq("  - SORT [4]: 1 fields: c BIGINT"),
          testing::Eq("    - PROJECT [3]: 1 fields: c BIGINT"),
          testing::Eq("          expressions: call: 1, field: 2"),
          testing::Eq("          functions: plus: 1"),
          testing::Eq("          projections: 1 out of 1"),
          testing::Eq("      - PROJECT [2]: 2 fields: a BIGINT, b BIGINT"),
          testing::Eq( "            expressions: call: 1, constant: 1, field: 2"),
          testing::Eq("            functions: plus: 1"),
          testing::Eq("            constants: BIGINT: 1"),
          testing::Eq("            projections: 1 out of 2"),
          testing::Eq("        - FILTER [1]: 1 fields: a BIGINT"),
          testing::Eq("              predicate: gt(a, 10)"),
          testing::Eq("              expressions: call: 1, constant: 1, field: 1"),
          testing::Eq("              functions: gt: 1"),
          testing::Eq("              constants: BIGINT: 1"),
          testing::Eq("          - VALUES [0]: 1 fields: a BIGINT"),
          testing::Eq("                rows: 1"),
          testing::Eq(""))
      // clang-format on
  );

  lines = toSkeletonLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::Eq("- LIMIT [5]: 1 fields"),
          testing::Eq("  - SORT [4]: 1 fields"),
          testing::Eq("    - FILTER [1]: 1 fields"),
          testing::Eq("      - VALUES [0]: 1 fields"),
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

  lines = toSummaryLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::Eq("- VALUES [0]: 2 fields: a BIGINT, b REAL"),
          testing::Eq("      rows: 0"),
          testing::Eq("")));

  lines = toSkeletonLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::Eq("- VALUES [0]: 2 fields"), testing::Eq("")));
}

TEST_F(PlanPrinterTest, inList) {
  auto plan = PlanBuilder()
                  .tableScan(kTestConnectorId, "test", {"a", "b"})
                  .filter("a IN (1, 5)")
                  .build();

  auto lines = toLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::StartsWith("- Filter: IN(a, 1, 5)"),
          testing::StartsWith("  - TableScan: test.test"),
          testing::Eq("")));

  lines = toSummaryLines(plan);

  EXPECT_THAT(
      lines,
      // clang-format off
      testing::ElementsAre(
          testing::Eq("- FILTER [1]: 2 fields: a BIGINT, b DOUBLE"),
          testing::Eq("      predicate: IN(a, 1, 5)"),
          testing::Eq("      expressions: IN: 1, constant: 2, field: 1"),
          testing::Eq("      constants: BIGINT: 2"),
          testing::Eq("  - TABLE_SCAN [0]: 2 fields: a BIGINT, b DOUBLE"),
          testing::Eq("        table: test"),
          testing::Eq("        connector: test"),
          testing::Eq(""))
      // clang-format on
  );

  lines = toSkeletonLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::Eq("- FILTER [1]: 2 fields"),
          testing::Eq("  - TABLE_SCAN [0]: 2 fields"),
          testing::Eq("        table: test"),
          testing::Eq("        connector: test"),
          testing::Eq("")));
}

TEST_F(PlanPrinterTest, tableScan) {
  auto plan = PlanBuilder()
                  .tableScan(kTestConnectorId, "test", {"a", "b"})
                  .with({"cast(a as double) * b as c"})
                  .build();

  auto lines = toLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::StartsWith("- Project"),
          testing::StartsWith("    a := a"),
          testing::StartsWith("    b := b"),
          testing::StartsWith("    c := multiply(CAST(a AS DOUBLE), b)"),
          testing::StartsWith("  - TableScan"),
          testing::Eq("")));

  lines = toSummaryLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::Eq("- PROJECT [1]: 3 fields: a BIGINT, b DOUBLE, c DOUBLE"),
          testing::Eq("      expressions: CAST: 1, call: 1, field: 4"),
          testing::Eq("      functions: multiply: 1"),
          testing::Eq("      projections: 1 out of 3"),
          testing::Eq("  - TABLE_SCAN [0]: 2 fields: a BIGINT, b DOUBLE"),
          testing::Eq("        table: test"),
          testing::Eq("        connector: test"),
          testing::Eq("")));

  lines = toSkeletonLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::Eq("- TABLE_SCAN [0]: 2 fields"),
          testing::Eq("      table: test"),
          testing::Eq("      connector: test"),
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

  auto lines = toLines(plan);

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

  lines = toSummaryLines(plan);

  EXPECT_THAT(
      lines,
      // clang-format off
      testing::ElementsAre(
          testing::Eq("- PROJECT [2]: 6 fields: a INTEGER, total BIGINT, mean DOUBLE, min INTEGER, expr BIGINT, ..."),
          testing::Eq("      expressions: call: 2, constant: 2, field: 6"),
          testing::Eq("      functions: multiply: 1, plus: 1"),
          testing::Eq("      constants: BIGINT: 1, DOUBLE: 1"),
          testing::Eq("      projections: 2 out of 6"),
          testing::Eq("  - AGGREGATE [1]: 4 fields: a INTEGER, total BIGINT, mean DOUBLE, min INTEGER"),
          testing::Eq("    - VALUES [0]: 2 fields: a INTEGER, b INTEGER"),
          testing::Eq("          rows: 4"),
          testing::Eq(""))
      // clang-format on
  );

  lines = toSkeletonLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::Eq("- AGGREGATE [1]: 4 fields"),
          testing::Eq("  - VALUES [0]: 2 fields"),
          testing::Eq("")));
}

TEST_F(PlanPrinterTest, union) {
  auto type = ROW({"a", "b"}, {INTEGER(), DOUBLE()});

  std::vector<Variant> data1{
      Variant::row({1, 1.1}),
      Variant::row({2, 2.2}),
  };

  std::vector<Variant> data2{
      Variant::row({3, 3.3}),
      Variant::row({4, 4.4}),
  };

  auto context = PlanBuilder::Context();
  auto plan = PlanBuilder(context)
                  .values(type, data1)
                  .unionAll(PlanBuilder(context).values(type, data2))
                  .with({"a::double + b"})
                  .build();

  auto lines = toLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::StartsWith("- Project"),
          testing::StartsWith("    a := a"),
          testing::StartsWith("    b := b"),
          testing::StartsWith("    expr := plus(CAST(a AS DOUBLE), b)"),
          testing::StartsWith("  - UNION ALL"),
          testing::StartsWith("    - Values"),
          testing::StartsWith("    - Values"),
          testing::Eq("")));

  lines = toSummaryLines(plan);

  EXPECT_THAT(
      lines,
      // clang-format off
      testing::ElementsAre(
          testing::Eq("- PROJECT [3]: 3 fields: a INTEGER, b DOUBLE, expr DOUBLE"),
          testing::Eq("      expressions: CAST: 1, call: 1, field: 4"),
          testing::Eq("      functions: plus: 1"),
          testing::Eq("      projections: 1 out of 3"),
          testing::Eq("  - UNION ALL [2]: 2 fields: a INTEGER, b DOUBLE"),
          testing::Eq("    - VALUES [0]: 2 fields: a INTEGER, b DOUBLE"),
          testing::Eq("          rows: 2"),
          testing::Eq("    - VALUES [1]: 2 fields: a_0 INTEGER, b_1 DOUBLE"),
          testing::Eq("          rows: 2"),
          testing::Eq(""))
      // clang-format on
  );

  lines = toSkeletonLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::Eq("- UNION ALL [2]: 2 fields"),
          testing::Eq("  - VALUES [0]: 2 fields"),
          testing::Eq("  - VALUES [1]: 2 fields"),
          testing::Eq("")));
}

TEST_F(PlanPrinterTest, subquery) {
  std::vector<Variant> data{
      Variant::row({1}),
      Variant::row({2}),
      Variant::row({3}),
  };

  std::vector<Variant> lookup{
      Variant::row({1, 10}),
      Variant::row({2, 20}),
  };

  auto context = PlanBuilder::Context();

  // Subquery in a project.
  PlanBuilder::Scope scope;
  auto plan =
      PlanBuilder(context)
          .values(ROW({"a"}, {INTEGER()}), data)
          .as("l")
          .captureScope(scope)
          .with({
              Col("a") + 1,
              Subquery(
                  PlanBuilder(context, scope)
                      .values(ROW({"a", "b"}, {INTEGER(), INTEGER()}), lookup)
                      .as("r")
                      .filter("l.a = r.a")
                      .aggregate({}, {"sum(b)"})
                      .build()),
          })
          .build();

  auto lines = toLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::StartsWith("- Project:"),
          testing::StartsWith("    a := a"),
          testing::StartsWith("    expr := expr"),
          testing::StartsWith("    sum := sum_1"),
          testing::StartsWith("  - Project:"),
          testing::StartsWith("      a := a"),
          testing::StartsWith("      expr := plus(a, 1)"),
          testing::StartsWith("      sum_1 := subquery"),
          testing::StartsWith("    - Values: 3 rows"),
          testing::Eq("")));

  lines = toSummaryLines(plan);

  EXPECT_THAT(
      lines,
      // clang-format off
      testing::ElementsAre(
          testing::Eq("- PROJECT [5]: 3 fields: a INTEGER, expr INTEGER, sum BIGINT"),
          testing::Eq("      expressions: field: 3"),
          testing::Eq("  - PROJECT [4]: 3 fields: a INTEGER, expr INTEGER, sum_1 BIGINT"),
          testing::Eq("        expressions: aggregate: 1, call: 2, constant: 1, field: 5, subquery: 1"),
          testing::Eq("        functions: eq: 1, plus: 1, sum: 1"),
          testing::Eq("        constants: INTEGER: 1"),
          testing::Eq("        projections: 2 out of 3"),
          testing::Eq("    - VALUES [0]: 1 fields: a INTEGER"),
          testing::Eq("          rows: 3"),
          testing::Eq(""))
      // clang-format on
  );

  lines = toSkeletonLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::Eq("- VALUES [0]: 1 fields"), testing::Eq("")));

  // Subquery in a filter.
  context = PlanBuilder::Context();
  plan =
      PlanBuilder(context)
          .values(ROW({"a"}, {INTEGER()}), data)
          .as("l")
          .captureScope(scope)
          .filter(
              Col("a") >
              Subquery(
                  PlanBuilder(context, scope)
                      .values(ROW({"a", "b"}, {INTEGER(), INTEGER()}), lookup)
                      .as("r")
                      .filter("l.a = r.a")
                      .aggregate({}, {"max(b)"})
                      .build()))
          .build();

  lines = toLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::StartsWith("- Filter: gt(a, subquery"),
          testing::StartsWith("  - Values: 3 rows"),
          testing::Eq("")));

  lines = toSummaryLines(plan);

  EXPECT_THAT(
      lines,
      // clang-format off
      testing::ElementsAre(
          testing::Eq("- FILTER [4]: 1 fields: a INTEGER"),
          testing::Eq("      predicate: gt(a, subquery(- Aggregate() -> ROW<max:INTEGER>  ..."),
          testing::Eq("      expressions: aggregate: 1, call: 2, field: 4, subquery: 1"),
          testing::Eq("      functions: eq: 1, gt: 1, max: 1"),
          testing::Eq("  - VALUES [0]: 1 fields: a INTEGER"),
          testing::Eq("        rows: 3"),
          testing::Eq(""))
      // clang-format on
  );

  lines = toSkeletonLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::Eq("- FILTER [4]: 1 fields"),
          testing::Eq("  - VALUES [0]: 1 fields"),
          testing::Eq("")));
}

TEST_F(PlanPrinterTest, inSubquery) {
  std::vector<Variant> data{
      Variant::row({1}),
      Variant::row({2}),
      Variant::row({3}),
  };

  std::vector<Variant> lookup{
      Variant::row({1, 10}),
      Variant::row({2, 20}),
  };

  PlanBuilder::Scope scope;
  // In subquery
  auto context = PlanBuilder::Context();
  auto plan =
      PlanBuilder(context)
          .values(ROW({"a"}, {INTEGER()}), data)
          .as("l")
          .captureScope(scope)
          .filter(In(
              Col("a"),
              Subquery(
                  PlanBuilder(context, scope)
                      .values(ROW({"a", "b"}, {INTEGER(), INTEGER()}), lookup)
                      .as("r")
                      .filter("l.a = r.a")
                      .project({"b"})
                      .build())))
          .build();

  auto lines = toLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::StartsWith("- Filter: IN(a, subquery"),
          testing::StartsWith("  - Values: 3 rows"),
          testing::Eq("")));

  lines = toSummaryLines(plan);

  EXPECT_THAT(
      lines,
      // clang-format off
      testing::ElementsAre(
          testing::Eq("- FILTER [4]: 1 fields: a INTEGER"),
          testing::Eq("      predicate: IN(a, subquery(- Project: -> ROW<b:INTEGER>     b ..."),
          testing::Eq("      expressions: IN: 1, call: 1, field: 4, subquery: 1"),
          testing::Eq("      functions: eq: 1"),
          testing::Eq("  - VALUES [0]: 1 fields: a INTEGER"),
          testing::Eq("        rows: 3"),
          testing::Eq(""))
      // clang-format on
  );

  lines = toSkeletonLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::Eq("- FILTER [4]: 1 fields"),
          testing::Eq("  - VALUES [0]: 1 fields"),
          testing::Eq("")));
}

TEST_F(PlanPrinterTest, existsSubquery) {
  std::vector<Variant> data{
      Variant::row({1}),
      Variant::row({2}),
      Variant::row({3}),
  };

  std::vector<Variant> lookup{
      Variant::row({1, 10}),
      Variant::row({2, 20}),
  };

  PlanBuilder::Scope scope;
  // Exists subquery
  auto context = PlanBuilder::Context();
  auto plan =
      PlanBuilder(context)
          .values(ROW({"a"}, {INTEGER()}), data)
          .as("l")
          .captureScope(scope)
          .filter(Exists(Subquery(
              PlanBuilder(context, scope)
                  .values(ROW({"a", "b"}, {INTEGER(), INTEGER()}), lookup)
                  .as("r")
                  .filter("l.a = r.a")
                  .build())))
          .build();

  auto lines = toLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::StartsWith("- Filter: EXISTS(subquery"),
          testing::StartsWith("  - Values: 3 rows"),
          testing::Eq("")));

  lines = toSummaryLines(plan);

  EXPECT_THAT(
      lines,
      // clang-format off
      testing::ElementsAre(
          testing::Eq("- FILTER [4]: 1 fields: a INTEGER"),
          testing::Eq("      predicate: EXISTS(subquery(- Project: -> ROW<a:INTEGER,b:INTE..."),
          testing::Eq("      expressions: EXISTS: 1, call: 1, field: 4, subquery: 1"),
          testing::Eq("      functions: eq: 1"),
          testing::Eq("  - VALUES [0]: 1 fields: a INTEGER"),
          testing::Eq("        rows: 3"),
          testing::Eq(""))
      // clang-format on
  );

  lines = toSkeletonLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::Eq("- FILTER [4]: 1 fields"),
          testing::Eq("  - VALUES [0]: 1 fields"),
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

  auto lines = toLines(plan);

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

  lines = toSummaryLines(plan);

  EXPECT_THAT(
      lines,
      // clang-format off
      testing::ElementsAre(
          testing::Eq("- PROJECT [3]: 5 fields: key INTEGER, v INTEGER, key_0 INTEGER, w INTEGER, z INTEGER"),
          testing::Eq("      expressions: call: 1, field: 6"),
          testing::Eq("      functions: plus: 1"),
          testing::Eq("      projections: 1 out of 5"),
          testing::Eq("  - JOIN LEFT [2]: 4 fields: key INTEGER, v INTEGER, key_0 INTEGER, w INTEGER"),
          testing::Eq("        condition: eq(key, key_0)"),
          testing::Eq("    - VALUES [0]: 2 fields: key INTEGER, v INTEGER"),
          testing::Eq("          rows: 4"),
          testing::Eq("    - VALUES [1]: 2 fields: key_0 INTEGER, w INTEGER"),
          testing::Eq("          rows: 2"),
          testing::Eq(""))
      // clang-format on
  );

  lines = toSkeletonLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::Eq("- JOIN LEFT [2]: 4 fields"),
          testing::Eq("  - VALUES [0]: 2 fields"),
          testing::Eq("  - VALUES [1]: 2 fields"),
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

  auto lines = toLines(plan);

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

  lines = toSummaryLines(plan);

  EXPECT_THAT(
      lines,
      // clang-format off
      testing::ElementsAre(
          testing::Eq("- PROJECT [3]: 5 fields: key INTEGER, v INTEGER, key_0 INTEGER, w INTEGER, z INTEGER"),
          testing::Eq("      expressions: call: 1, field: 6"),
          testing::Eq("      functions: plus: 1"),
          testing::Eq("      projections: 1 out of 5"),
          testing::Eq("  - JOIN INNER [2]: 4 fields: key INTEGER, v INTEGER, key_0 INTEGER, w INTEGER"),
          testing::Eq("    - VALUES [0]: 2 fields: key INTEGER, v INTEGER"),
          testing::Eq("          rows: 3"),
          testing::Eq("    - VALUES [1]: 2 fields: key_0 INTEGER, w INTEGER"),
          testing::Eq("          rows: 2"),
          testing::Eq(""))
      // clang-format on
  );

  lines = toSkeletonLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::Eq("- JOIN INNER [2]: 4 fields"),
          testing::Eq("  - VALUES [0]: 2 fields"),
          testing::Eq("  - VALUES [1]: 2 fields"),
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

  lines = toSummaryLines(plan);

  EXPECT_THAT(
      lines,
      // clang-format off
      testing::ElementsAre(
          testing::Eq("- PROJECT [2]: 10 fields: a BOOLEAN, b BOOLEAN, expr DOUBLE, expr_2 VARCHAR, expr_3 INTEGER, ..."),
          testing::Eq("      expressions: field: 10"),
          testing::Eq("  - PROJECT [1]: 10 fields: a_0 BOOLEAN, b_1 BOOLEAN, expr DOUBLE, expr_2 VARCHAR, expr_3 INTEGER, ..."),
          testing::Eq("        expressions: AND: 1, CAST: 6, COALESCE: 1, DEREFERENCE: 2, IF: 1, OR: 1, SWITCH: 1, TRY: 1, TRY_CAST: 1, call: 10, constant: 11, field: 21"),
          testing::Eq("        functions: divide: 2, eq: 2, gt: 4, lt: 1, multiply: 1"),
          testing::Eq("        constants: BIGINT: 5, DOUBLE: 1, INTEGER: 1, VARCHAR: 4"),
          testing::Eq("        projections: 8 out of 10"),
          testing::Eq("        dereferences: 2 out of 10"),
          testing::Eq("    - VALUES [0]: 3 fields: a INTEGER, b INTEGER, c ROW(2)"),
          testing::Eq("          rows: 4"),
          testing::Eq(""))
      // clang-format on
  );

  lines = toSummaryLines(
      plan, {.project = {.maxProjections = 3, .maxDereferences = 1}});

  EXPECT_THAT(
      lines,
      // clang-format off
      testing::ElementsAre(
          testing::Eq("- PROJECT [2]: 10 fields: a BOOLEAN, b BOOLEAN, expr DOUBLE, expr_2 VARCHAR, expr_3 INTEGER, ..."),
          testing::Eq("      expressions: field: 10"),
          testing::Eq("  - PROJECT [1]: 10 fields: a_0 BOOLEAN, b_1 BOOLEAN, expr DOUBLE, expr_2 VARCHAR, expr_3 INTEGER, ..."),
          testing::Eq("        expressions: AND: 1, CAST: 6, COALESCE: 1, DEREFERENCE: 2, IF: 1, OR: 1, SWITCH: 1, TRY: 1, TRY_CAST: 1, call: 10, constant: 11, field: 21"),
          testing::Eq("        functions: divide: 2, eq: 2, gt: 4, lt: 1, multiply: 1"),
          testing::Eq("        constants: BIGINT: 5, DOUBLE: 1, INTEGER: 1, VARCHAR: 4"),
          testing::Eq("        projections: 8 out of 10"),
          testing::Eq( "          a_0: AND(gt(a, b), gt(b, CAST(10 AS INTEGER)))"),
          testing::Eq("          b_1: OR(lt(a, b), gt(b, CAST(0 AS INTEGER)))"),
          testing::Eq("          expr: multiply(CAST(a AS DOUBLE), 1.2)"),
          testing::Eq("          ... 5 more"),
          testing::Eq("        dereferences: 2 out of 10"),
          testing::Eq("          expr_4: DEREFERENCE(c, x)"),
          testing::Eq("          ... 1 more"),
          testing::Eq("    - VALUES [0]: 3 fields: a INTEGER, b INTEGER, c ROW(2)"),
          testing::Eq("          rows: 4"),
          testing::Eq(""))
      // clang-format on
  );

  lines = toSkeletonLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::Eq("- VALUES [0]: 3 fields"), testing::Eq("")));
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
      // clang-format off
      testing::ElementsAre(
          testing::StartsWith("- Project"),
          testing::StartsWith("    expr := filter(sequence(CAST(1 AS BIGINT), a), x -> gt(x, b))"),
          testing::StartsWith("  - Values"),
          testing::Eq(""))
      // clang-format on
  );

  lines = toSummaryLines(plan);

  EXPECT_THAT(
      lines,
      // clang-format off
      testing::ElementsAre(
          testing::Eq("- PROJECT [1]: 1 fields: expr ARRAY"),
          testing::Eq("      expressions: CAST: 1, call: 3, constant: 1, field: 3, lambda: 1"),
          testing::Eq("      functions: filter: 1, gt: 1, sequence: 1"),
          testing::Eq("      constants: BIGINT: 1"),
          testing::Eq("      projections: 1 out of 1"),
          testing::Eq("  - VALUES [0]: 2 fields: a BIGINT, b BIGINT"),
          testing::Eq("        rows: 2"),
          testing::Eq(""))
      // clang-format on
  );

  lines = toSkeletonLines(plan);

  EXPECT_THAT(
      lines,
      testing::ElementsAre(
          testing::Eq("- VALUES [0]: 2 fields"), testing::Eq("")));
}

} // namespace
} // namespace facebook::velox::logical_plan
