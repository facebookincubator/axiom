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
#include "axiom/sql/presto/tests/PrestoParserTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace axiom::sql::presto::test {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

namespace {

class SortParserTest : public PrestoParserTestBase {};

TEST_F(SortParserTest, basic) {
  {
    auto matcher = matchScan("nation")
                       .aggregate({"n_regionkey"}, {"count(1)"})
                       .sort({"count"})
                       .project({"n_regionkey"})
                       .output({"n_regionkey"});

    testSelect(
        "SELECT n_regionkey FROM nation GROUP BY 1 ORDER BY count(1)", matcher);
  }

  {
    auto matcher = matchScan("nation")
                       .aggregate({"n_regionkey"}, {"count(1)"})
                       .sort({"count"})
                       .output({"n_regionkey", "count"});

    testSelect(
        "SELECT n_regionkey, count(1) FROM nation GROUP BY 1 ORDER BY count(1)",
        matcher);

    testSelect(
        "SELECT n_regionkey, count(1) FROM nation GROUP BY 1 ORDER BY 2",
        matcher);

    testSelect(
        "SELECT n_regionkey, count(1) AS c FROM nation GROUP BY 1 ORDER BY c",
        matchScan("nation")
            .aggregate({"n_regionkey"}, {"count(1)"})
            .sort({"c"})
            .output({"n_regionkey", "c"}));
  }

  {
    auto matcher = matchScan("nation")
                       .aggregate({"n_regionkey"}, {"count(1)"})
                       .project({"n_regionkey", "count * 2::bigint as x"})
                       .sort({"x"})
                       .output({"n_regionkey", "x"});

    testSelect(
        "SELECT n_regionkey, count(1) * 2 AS x FROM nation GROUP BY 1 ORDER BY 2",
        matcher);

    testSelect(
        "SELECT n_regionkey, count(1) * 2 AS x "
        "FROM nation "
        "GROUP BY 1 "
        "ORDER BY count(1) * 2",
        matcher);
  }

  {
    auto matcher = matchScan("nation")
                       .aggregate({"n_regionkey"}, {"count(1)"})
                       .project(
                           {"n_regionkey",
                            "count * 2::bigint as doubled",
                            "count * 3::bigint as tripled"})
                       .sort({"tripled"})
                       .project({"n_regionkey", "doubled"})
                       .output({"n_regionkey", "doubled"});

    testSelect(
        "SELECT n_regionkey, count(1) * 2 AS doubled "
        "FROM nation "
        "GROUP BY 1 "
        "ORDER BY count(1) * 3",
        matcher);
  }

  // Multiple sort keys with mixed directions.
  {
    auto matcher =
        matchScan("nation")
            .sort({"n_regionkey", "n_name desc"})
            .output({"n_nationkey", "n_name", "n_regionkey", "n_comment"});

    testSelect(
        "SELECT * FROM nation ORDER BY n_regionkey, n_name DESC", matcher);
  }
}

TEST_F(SortParserTest, groupBy) {
  connector_->addTable("t", ROW({"a", "b"}, INTEGER()));

  {
    auto matcher = matchScan()
                       .aggregate({"a"}, {})
                       .project({"a as b"})
                       .sort({"b"})
                       .output({"b"});

    testSelect("SELECT a AS b FROM t GROUP BY a ORDER BY b", matcher);
  }

  {
    auto matcher = matchScan()
                       .aggregate({"a"}, {"sum(b)"})
                       .project({"a as b", "sum"})
                       .sort({"b"})
                       .output({"b", "sum"});

    testSelect("SELECT a AS b, sum(b) FROM t GROUP BY a ORDER BY b", matcher);
  }

  VELOX_ASSERT_THROW(
      parseSql("SELECT a, sum(b) FROM t GROUP BY a ORDER BY b"),
      "Cannot resolve column: b");
}

TEST_F(SortParserTest, nonSelectedColumn) {
  connector_->addTable("t", ROW({"a", "b", "c", "d", "e", "f"}, INTEGER()));

  // Base SELECT query with a mix of features:
  // - SELECT a as b: alias shadows original column name
  // - SELECT b + 1 as a: expression with alias that shadows another column
  // - SELECT c * d AS product: expression with explicit alias
  // - SELECT c + e as x: expression with new alias
  // The first PROJECT makes the SELECT list and the full schema visible so
  // ORDER BY can reference columns outside the SELECT list. The second PROJECT
  // trims back to just the SELECT list.
  const std::string baseSelect =
      "SELECT a as b, b + 1 as a, c * d AS product, c + e as x FROM t";

  auto testOrderBy = [&](const std::string& orderByClause,
                         const std::vector<std::string>& extraColumns,
                         const std::vector<std::string>& sortKeys) {
    std::vector<std::string> projections = {
        "a as b", "b + 1 as a", "c * d as product", "c + e as x"};
    projections.insert(
        projections.end(), extraColumns.begin(), extraColumns.end());

    auto builder = lp::test::LogicalPlanMatcherBuilder()
                       .tableScan()
                       .project(projections)
                       .sort(sortKeys);

    if (!extraColumns.empty()) {
      builder.project({"b", "a", "product", "x"});
    }

    auto matcher = builder.output({"b", "a", "product", "x"});
    testSelect(baseSelect + " ORDER BY " + orderByClause, matcher);
  };

  testOrderBy("e", {"e"}, {"e"});
  testOrderBy("f", {"f"}, {"f"});
  testOrderBy("b", {}, {"b"});
  testOrderBy("x", {}, {"x"});
  testOrderBy("1", {}, {"b"});
  testOrderBy("3", {}, {"product"});
  testOrderBy("e, x, 1", {"e"}, {"e", "x", "b"});

  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSql(baseSelect + " ORDER BY 5"), "is not in the select list");
}

TEST_F(SortParserTest, ambiguousAlias) {
  connector_->addTable("t", ROW({"a", "b", "c"}, INTEGER()));

  VELOX_ASSERT_THROW(
      parseSql("SELECT a as b, b FROM t ORDER BY b"), "Column is ambiguous: b");

  testSelect(
      "SELECT a as b, b FROM t ORDER BY c",
      lp::test::LogicalPlanMatcherBuilder()
          .tableScan()
          .project({"a as first_b", "b as second_b", "c"})
          .sort({"c"})
          .project({"first_b", "second_b"})
          .output({"b", "b"}));

  testSelect(
      "SELECT a as b, b FROM t ORDER BY 1",
      lp::test::LogicalPlanMatcherBuilder()
          .tableScan()
          .project({"a as first_b", "b as second_b"})
          .sort({"first_b"})
          .output({"b", "b"}));

  testSelect(
      "SELECT a as b, b FROM t ORDER BY 2",
      lp::test::LogicalPlanMatcherBuilder()
          .tableScan()
          .project({"a as first_b", "b as second_b"})
          .sort({"second_b"})
          .output({"b", "b"}));

  testSelect(
      "SELECT a as b FROM t ORDER BY b",
      lp::test::LogicalPlanMatcherBuilder()
          .tableScan()
          .project({"a as b"})
          .sort({"b"})
          .output({"b"}));
}

TEST_F(SortParserTest, star) {
  connector_->addTable("t", ROW({"a", "b", "c", "d", "e", "f"}, INTEGER()));

  testSelect(
      "SELECT * FROM t ORDER BY 1, 3, e, f, a + c",
      lp::test::LogicalPlanMatcherBuilder()
          .tableScan()
          .sort({"a", "c", "e", "f", "a + c"})
          .output({"a", "b", "c", "d", "e", "f"}));
}

TEST_F(SortParserTest, starWithHiddenColumn) {
  connector_->addTable(
      "t", ROW({"a", "b"}, INTEGER()), ROW({"$path"}, VARCHAR()));

  lp::LogicalPlanNodePtr outputNode;
  testSelect(
      "SELECT * FROM t ORDER BY \"$path\"",
      lp::test::LogicalPlanMatcherBuilder()
          .tableScan()
          .sort({"\"$path\""})
          .project([&](const auto& node) { outputNode = node; })
          .output({"a", "b"}));
  ASSERT_THAT(
      outputNode->outputType()->names(),
      ::testing::Pointwise(::testing::Eq(), {"a", "b"}));
}

TEST_F(SortParserTest, joinedTable) {
  testSelect(
      "SELECT n_name "
      "FROM nation, region "
      "WHERE n_regionkey = r_regionkey "
      "ORDER BY r_name",
      lp::test::LogicalPlanMatcherBuilder()
          .tableScan()
          .join(lp::test::LogicalPlanMatcherBuilder().tableScan().build())
          .filter()
          .project({"n_name", "r_name"})
          .sort({"r_name"})
          .project({"n_name"})
          .output({"n_name"}));
}

TEST_F(SortParserTest, distinct) {
  connector_->addTable("t", ROW({"a", "b"}, INTEGER()));
  auto matcher = lp::test::LogicalPlanMatcherBuilder()
                     .tableScan()
                     .project({"a"})
                     .aggregate({"a"}, {})
                     .sort({"a"})
                     .output({"a"});

  testSelect("SELECT DISTINCT a FROM t ORDER BY a", matcher);
  testSelect("SELECT DISTINCT a FROM t ORDER BY 1", matcher);

  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSql(
          "SELECT DISTINCT a "
          "FROM (VALUES (1, 2), (3, 4)) AS t(a, b) "
          "ORDER BY b DESC"),
      "ORDER BY expressions must be output expressions");

  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSql(
          "SELECT DISTINCT a + b "
          "FROM (VALUES (1, 2), (3, 4)) AS t(a, b) "
          "ORDER BY a DESC"),
      "ORDER BY expressions must be output expressions");

  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSql(
          "SELECT DISTINCT a "
          "FROM (VALUES (1, 2), (3, 4)) AS t(a, b) "
          "ORDER BY 2 DESC"),
      "ORDER BY position is not in the select list: 2");

  VELOX_ASSERT_THROW(
      parseSql(
          "SELECT a + b "
          "FROM (VALUES (1, 2), (3, 4)) AS t(a, b) "
          "GROUP BY 1 "
          "ORDER BY a DESC"),
      "Cannot resolve column: a not in [expr -> expr]");
}

TEST_F(SortParserTest, distinctOrderByOriginalName) {
  connector_->addTable("t", ROW({"a", "b"}, INTEGER()));

  testSelect(
      "SELECT DISTINCT a AS id FROM t ORDER BY a",
      matchScan("t")
          .project({"a"})
          .aggregate({"id"}, {})
          .sort({"id"})
          .output({"id"}));

  testSelect(
      "SELECT DISTINCT a AS x, b AS y FROM t ORDER BY b",
      matchScan("t")
          .project({"a", "b"})
          .aggregate({"x", "y"}, {})
          .sort({"y"})
          .output({"x", "y"}));
}

TEST_F(SortParserTest, complexExpressionIdentity) {
  connector_->addTable("t", ROW({"a", "b", "c"}, INTEGER()));

  testSelect(
      "SELECT CASE WHEN a > 0 THEN b ELSE c END AS result "
      "FROM t "
      "ORDER BY result",
      matchScan()
          .project(
              {lp::Call("switch", lp::Col("a") > 0, lp::Col("b"), lp::Col("c"))
                   .as("result")})
          .sort({"result"})
          .output({"result"}));

  testSelect(
      "SELECT COALESCE(a, b) AS coalesced FROM t ORDER BY coalesced",
      matchScan()
          .project({"COALESCE(a, b) AS coalesced"})
          .sort({"coalesced"})
          .output({"coalesced"}));
}

TEST_F(SortParserTest, differentExpressions) {
  connector_->addTable("t", ROW({"a", "b", "c"}, INTEGER()));

  testSelect(
      "SELECT a + b AS ab_sum FROM t ORDER BY a + c",
      lp::test::LogicalPlanMatcherBuilder()
          .tableScan()
          .project({"a + b as ab_sum", "a + c as ac_sum"})
          .sort({"ac_sum"})
          .project({"ab_sum"})
          .output({"ab_sum"}));
}

TEST_F(SortParserTest, outputAliasInExpression) {
  connector_->addTable("t", ROW({"a"}, INTEGER()));
  connector_->addTable("t2", ROW({"a", "b"}, INTEGER()));

  testSelect(
      "SELECT a * 2 AS b FROM t ORDER BY b * -1",
      lp::test::LogicalPlanMatcherBuilder()
          .tableScan()
          .project({"a * 2 as b", "a * 2 * negate(1) as sortFun"})
          .sort({"sortFun"})
          .project({"b"})
          .output({"b"}));

  testSelect(
      "SELECT a * 2 AS b FROM t ORDER BY b",
      lp::test::LogicalPlanMatcherBuilder()
          .tableScan()
          .project({"a * 2 as b"})
          .sort({"b"})
          .output({"b"}));

  testSelect(
      "SELECT a * -2 AS a FROM t ORDER BY a * -1",
      lp::test::LogicalPlanMatcherBuilder()
          .tableScan()
          .project(
              {"a * negate(2) as selectFun",
               "a * negate(2) * negate(1) as sortFun"})
          .sort({"sortFun"})
          .project({"selectFun"})
          .output({"a"}));

  testSelect(
      "SELECT a AS b, a * -2 AS c FROM t ORDER BY b + c",
      lp::test::LogicalPlanMatcherBuilder()
          .tableScan()
          .project(
              {"a as b", "a * negate(2) as c", "a + a * negate(2) as sortFun"})
          .sort({"sortFun"})
          .project({"b", "c"})
          .output({"b", "c"}));

  testSelect(
      "SELECT 1 AS x FROM t ORDER BY x + 1",
      lp::test::LogicalPlanMatcherBuilder()
          .tableScan()
          .project({"1 as x", "1 + 1 as sortFun"})
          .sort({"sortFun"})
          .project({"x"})
          .output({"x"}));

  VELOX_ASSERT_THROW(
      parseSql("SELECT 1 AS a, a FROM t2 ORDER BY a + 1"),
      "Column is ambiguous: a");

  testSelect(
      "SELECT 1 AS a, a FROM t2 ORDER BY b + 1",
      lp::test::LogicalPlanMatcherBuilder()
          .tableScan()
          .project({"1 as alias", "a as col", "b + 1 as sortFun"})
          .sort({"sortFun"})
          .project({"alias", "col"})
          .output({"a", "a"}));
}

TEST_F(SortParserTest, union) {
  {
    auto matcher = matchScan()
                       .project({"n_name"})
                       .unionAll(matchScan().project({"r_name"}).build())
                       .sort({"n_name"})
                       .output({"n_name"});

    testSelect(
        "SELECT n_name FROM nation "
        "UNION ALL SELECT r_name FROM region "
        "ORDER BY n_name",
        matcher);

    testSelect(
        "SELECT n_name FROM nation "
        "UNION ALL SELECT r_name FROM region "
        "ORDER BY 1",
        matcher);
  }

  {
    auto matcher = matchScan()
                       .project({"n_name"})
                       .unionDistinct(matchScan().project({"r_name"}).build())
                       .sort({"n_name desc"})
                       .output({"n_name"});

    testSelect(
        "SELECT n_name FROM nation "
        "UNION SELECT r_name FROM region "
        "ORDER BY n_name DESC",
        matcher);
  }

  {
    auto matcher = matchScan()
                       .project({"n_name"})
                       .unionAll(matchScan().project({"r_name"}).build())
                       .unionAll(matchScan().project().build())
                       .sort({"n_name"})
                       .output({"n_name"});

    testSelect(
        "SELECT n_name FROM nation "
        "UNION ALL SELECT r_name FROM region "
        "UNION ALL SELECT n_name FROM nation "
        "ORDER BY 1",
        matcher);
  }
}

} // namespace
} // namespace axiom::sql::presto::test
