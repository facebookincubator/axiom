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

#include "axiom/sql/presto/PrestoParseError.h"
#include "axiom/sql/presto/tests/PrestoParserTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace axiom::sql::presto::test {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

namespace {

class PrestoParserTest : public PrestoParserTestBase {};

TEST_F(PrestoParserTest, parseMultiple) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple("select 1; select 2");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleWithTrailingSemicolon) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple("select 1; select 2;");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleWithWhitespace) {
  auto parser = makeParser();

  auto statements =
      parser.parseMultiple("  select 1  ;  \n  select 2  ;  \n  select 3  ");
  ASSERT_EQ(3, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
  ASSERT_TRUE(statements[2]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleWithComments) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple(
      "-- First query\nselect 1;\n-- Second query\nselect 2");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleWithBlockComments) {
  auto parser = makeParser();

  auto statements =
      parser.parseMultiple("/* First */ select 1; /* Second */ select 2");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleWithSingleQuotes) {
  auto parser = makeParser();

  auto statements =
      parser.parseMultiple("select 'hello; world'; select 'foo''bar; baz'");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleWithDoubleQuotes) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple(
      "select 1 as \"col;name\"; select 2 as \"foo\"\"bar; baz\"");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleMixedStatements) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple(
      "select * from nation; "
      "select n_name from nation where n_nationkey = 1; "
      "select 42");
  ASSERT_EQ(3, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
  ASSERT_TRUE(statements[2]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleSingleStatement) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple("select 1");
  ASSERT_EQ(1, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleEmptyStatements) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple(";;;");
  ASSERT_EQ(0, statements.size());
}

TEST_F(PrestoParserTest, parseMultipleComplexQuery) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple(
      "select n_nationkey, n_name "
      "from nation "
      "where n_regionkey = 1 "
      "order by n_name; "
      "select count(*) from nation");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(PrestoParserTest, unnest) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().values().unnest();
    testSelect("SELECT * FROM unnest(array[1, 2, 3])", matcher);

    testSelect(
        "SELECT * FROM unnest(array[1, 2, 3], array[4, 5]) with ordinality",
        matcher);

    testSelect(
        "SELECT * FROM unnest(map(array[1, 2, 3], array[10, 20, 30]))",
        matcher);
  }

  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().values().unnest().project();
    testSelect("SELECT * FROM unnest(array[1, 2, 3]) as t(x)", matcher);

    testSelect(
        "SELECT * FROM unnest(array[1, 2, 3], array[4, 5]) with ordinality as t(x, y)",
        matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().unnest();
    testSelect(
        "SELECT * FROM nation, unnest(array[n_nationkey, n_regionkey])",
        matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().unnest();

    testSelect(
        "SELECT * FROM nation, unnest(array[n_nationkey, n_regionkey]) as t(x)",
        matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder()
                       .values()
                       .project()
                       .unnest()
                       .project();

    testSelect(
        "WITH a AS (SELECT array[1,2,3] as x) SELECT t.x + 1 FROM a, unnest(A.x) as T(X)",
        matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().unnest();

    testSelect(
        "SELECT * FROM (nation cross join unnest(array[1,2,3]) as t(x))",
        matcher);
  }
}

TEST_F(PrestoParserTest, syntaxErrors) {
  auto parser = makeParser();
  EXPECT_THAT(
      [&]() { parser.parse("SELECT * FROM"); },
      ThrowsMessage<axiom::sql::presto::PrestoParseError>(::testing::HasSubstr(
          "Syntax error at 1:13: mismatched input '<EOF>'")));

  EXPECT_THAT(
      [&]() {
        parser.parse(
            "SELECT * FROM nation\n"
            "WHERE");
      },
      ThrowsMessage<axiom::sql::presto::PrestoParseError>(::testing::HasSubstr(
          "Syntax error at 2:5: mismatched input '<EOF>'")));

  EXPECT_THAT(
      [&]() { parser.parse("SELECT * FROM (VALUES 1, 2, 3)) blah..."); },
      ThrowsMessage<axiom::sql::presto::PrestoParseError>(::testing::HasSubstr(
          "Syntax error at 1:30: mismatched input ')' expecting <EOF>")));

  EXPECT_THAT(
      [&]() { parser.parse("CREATE TABLE t (price DECIMAL('abc', 'xyz'))"); },
      ThrowsMessage<axiom::sql::presto::PrestoParseError>(::testing::HasSubstr(
          "Syntax error at 1:30: mismatched input ''abc''")));
}

TEST_F(PrestoParserTest, selectStar) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan();
    testSelect("SELECT * FROM nation", matcher);
    testSelect("(SELECT * FROM nation)", matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().project();
    testSelect("SELECT *, * FROM nation", matcher);
    testSelect("SELECT *, n_nationkey FROM nation", matcher);
    testSelect("SELECT nation.* FROM nation", matcher);
    testSelect("SELECT nation.*, n_nationkey + 1 FROM nation", matcher);
  }

  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder()
            .tableScan()
            .join(lp::test::LogicalPlanMatcherBuilder().tableScan().build())
            .filter()
            .project();
    testSelect(
        "SELECT nation.*, r_regionkey + 1 FROM nation, region WHERE n_regionkey = r_regionkey",
        matcher);
  }

  VELOX_ASSERT_THROW(parseSql("SELECT r.* FROM region"), "Alias not found: r");
}

TEST_F(PrestoParserTest, hiddenColumns) {
  connector_->addTable(
      "t", ROW({"a", "b"}, INTEGER()), ROW({"$c", "$d"}, VARCHAR()));

  auto verifyOutput = [&](const std::string& sql,
                          std::initializer_list<std::string> expectedNames) {
    lp::LogicalPlanNodePtr outputNode;
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().project(
        [&](const auto& node) { outputNode = node; });

    testSelect(sql, matcher);
    ASSERT_THAT(
        outputNode->outputType()->names(),
        ::testing::Pointwise(::testing::Eq(), expectedNames));
  };

  verifyOutput("SELECT * FROM t", {"a", "b"});
  verifyOutput("SELECT \"$c\", * FROM t", {"$c", "a", "b"});
  verifyOutput("SELECT a, \"$c\" FROM t", {"a", "$c"});

  verifyOutput("SELECT *, a FROM t", {"a", "b", "a_0"});
  verifyOutput("SELECT *, * FROM t", {"a", "b", "a_0", "b_1"});
}

TEST_F(PrestoParserTest, mixedCaseColumnNames) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().project();
  testSelect("SELECT N_NAME, n_ReGiOnKeY FROM nation", matcher);
  testSelect("SELECT nation.n_name FROM nation", matcher);
  testSelect("SELECT NATION.n_name FROM nation", matcher);
  testSelect("SELECT \"NATION\".n_name FROM nation", matcher);
}

TEST_F(PrestoParserTest, with) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().values().project();
  testSelect("WITH a as (SELECT 1 as x) SELECT * FROM a", matcher);
  testSelect("WITH a as (SELECT 1 as x) SELECT * FROM A", matcher);
  testSelect("WITH A as (SELECT 1 as x) SELECT * FROM a", matcher);

  matcher.project();
  testSelect("WITH a as (SELECT 1 as x) SELECT A.x FROM a", matcher);
}

TEST_F(PrestoParserTest, countStar) {
  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate();

    testSelect("SELECT count(*) FROM nation", matcher);
    testSelect("SELECT count(1) FROM nation", matcher);

    testSelect("SELECT count(1) \"count\" FROM nation", matcher);
    testSelect("SELECT count(1) AS \"count\" FROM nation", matcher);
  }

  {
    // Global aggregation with HAVING clause.
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate().filter();
    testSelect("SELECT count(*) FROM nation HAVING count(*) > 100", matcher);
  }
}

TEST_F(PrestoParserTest, aggregateCoercions) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate();

  testSelect("SELECT corr(n_nationkey, 1.2) FROM nation", matcher);
}

TEST_F(PrestoParserTest, simpleGroupBy) {
  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate();

    testSelect("SELECT n_name, count(1) FROM nation GROUP BY 1", matcher);
    testSelect("SELECT n_name, count(1) FROM nation GROUP BY n_name", matcher);
  }

  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate().project();
    testSelect(
        "SELECT count(1) FROM nation GROUP BY n_name, n_regionkey", matcher);
  }
}

TEST_F(PrestoParserTest, groupingSets) {
  lp::AggregateNodePtr agg;
  auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate(
      [&](const auto& node) {
        agg = std::dynamic_pointer_cast<const lp::AggregateNode>(node);
      });

  testSelect(
      "SELECT n_regionkey, count(1) FROM nation "
      "GROUP BY GROUPING SETS (n_regionkey, ())",
      matcher);
  ASSERT_TRUE(agg != nullptr);
  EXPECT_THAT(
      agg->groupingSets(),
      testing::ElementsAre(testing::ElementsAre(0), testing::IsEmpty()));

  testSelect(
      "SELECT n_regionkey, n_name, count(1) FROM nation "
      "GROUP BY GROUPING SETS ((n_regionkey, n_name), (n_regionkey), ())",
      matcher);
  ASSERT_TRUE(agg != nullptr);
  EXPECT_THAT(
      agg->groupingSets(),
      testing::ElementsAre(
          testing::ElementsAre(0, 1),
          testing::ElementsAre(0),
          testing::IsEmpty()));

  // Test ordinals in GROUPING SETS: GROUPING SETS ((1, 2), (1))
  testSelect(
      "SELECT n_regionkey, n_name, count(1) FROM nation "
      "GROUP BY GROUPING SETS ((1, 2), (1))",
      matcher);
  ASSERT_TRUE(agg != nullptr);
  EXPECT_THAT(
      agg->groupingSets(),
      testing::ElementsAre(
          testing::ElementsAre(0, 1), testing::ElementsAre(0)));
}

TEST_F(PrestoParserTest, rollup) {
  lp::AggregateNodePtr agg;
  auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate(
      [&](const auto& node) {
        agg = std::dynamic_pointer_cast<const lp::AggregateNode>(node);
      });

  testSelect(
      "SELECT n_regionkey, n_name, count(1) FROM nation "
      "GROUP BY ROLLUP(n_regionkey, n_name)",
      matcher);
  ASSERT_TRUE(agg != nullptr);
  EXPECT_THAT(
      agg->groupingSets(),
      testing::ElementsAre(
          testing::ElementsAre(0, 1),
          testing::ElementsAre(0),
          testing::IsEmpty()));

  testSelect(
      "SELECT n_regionkey, count(1) FROM nation "
      "GROUP BY ROLLUP(n_regionkey)",
      matcher);
  ASSERT_TRUE(agg != nullptr);
  EXPECT_THAT(
      agg->groupingSets(),
      testing::ElementsAre(testing::ElementsAre(0), testing::IsEmpty()));
}

TEST_F(PrestoParserTest, cube) {
  lp::AggregateNodePtr agg;
  auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate(
      [&](const auto& node) {
        agg = std::dynamic_pointer_cast<const lp::AggregateNode>(node);
      });

  testSelect(
      "SELECT n_regionkey, n_name, count(1) FROM nation "
      "GROUP BY CUBE(n_regionkey, n_name)",
      matcher);
  ASSERT_TRUE(agg != nullptr);
  EXPECT_THAT(
      agg->groupingSets(),
      testing::ElementsAre(
          testing::ElementsAre(0, 1),
          testing::ElementsAre(0),
          testing::ElementsAre(1),
          testing::IsEmpty()));

  testSelect(
      "SELECT n_regionkey, count(1) FROM nation "
      "GROUP BY CUBE(n_regionkey)",
      matcher);
  ASSERT_TRUE(agg != nullptr);
  EXPECT_THAT(
      agg->groupingSets(),
      testing::ElementsAre(testing::ElementsAre(0), testing::IsEmpty()));
}

TEST_F(PrestoParserTest, mixedGroupByWithRollup) {
  lp::AggregateNodePtr agg;
  auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate(
      [&](const auto& node) {
        agg = std::dynamic_pointer_cast<const lp::AggregateNode>(node);
      });

  testSelect(
      "SELECT n_regionkey, n_name, count(1) FROM nation "
      "GROUP BY n_regionkey, ROLLUP(n_name)",
      matcher);
  ASSERT_TRUE(agg != nullptr);
  EXPECT_THAT(
      agg->groupingSets(),
      testing::ElementsAre(
          testing::ElementsAre(0, 1), testing::ElementsAre(0)));
}

TEST_F(PrestoParserTest, groupingSetsOrdinalCaching) {
  lp::AggregateNodePtr agg;
  auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate(
      [&](const auto& node) {
        agg = std::dynamic_pointer_cast<const lp::AggregateNode>(node);
      });

  testSelect(
      "SELECT n_regionkey, n_name, count(1) FROM nation "
      "GROUP BY GROUPING SETS ((1), (1, 2), (2))",
      matcher);
  ASSERT_TRUE(agg != nullptr);
  EXPECT_THAT(
      agg->groupingSets(),
      testing::ElementsAre(
          testing::ElementsAre(0),
          testing::ElementsAre(0, 1),
          testing::ElementsAre(1)));

  testSelect(
      "SELECT n_regionkey, n_name, count(1) FROM nation "
      "GROUP BY ROLLUP(1, 2)",
      matcher);
  ASSERT_TRUE(agg != nullptr);
  EXPECT_THAT(
      agg->groupingSets(),
      testing::ElementsAre(
          testing::ElementsAre(0, 1),
          testing::ElementsAre(0),
          testing::IsEmpty()));

  testSelect(
      "SELECT n_regionkey, n_name, count(1) FROM nation "
      "GROUP BY CUBE(1, 2)",
      matcher);
  ASSERT_TRUE(agg != nullptr);
  EXPECT_THAT(
      agg->groupingSets(),
      testing::ElementsAre(
          testing::ElementsAre(0, 1),
          testing::ElementsAre(0),
          testing::ElementsAre(1),
          testing::IsEmpty()));
}

TEST_F(PrestoParserTest, groupingSetsSubqueryOrdinal) {
  lp::AggregateNodePtr agg;
  auto matcher =
      lp::test::LogicalPlanMatcherBuilder()
          .tableScan()
          .aggregate([&](const auto& node) {
            agg = std::dynamic_pointer_cast<const lp::AggregateNode>(node);
          })
          .project();

  testSelect(
      "SELECT (SELECT 1), n_name, count(1) FROM nation "
      "GROUP BY GROUPING SETS ((1), (1, 2))",
      matcher);
  ASSERT_TRUE(agg != nullptr);
  EXPECT_THAT(
      agg->groupingSets(),
      testing::ElementsAre(
          testing::ElementsAre(0), testing::ElementsAre(0, 1)));
}

TEST_F(PrestoParserTest, cubeColumnLimit) {
  // CUBE is limited to 30 columns (2^30 grouping sets).
  // Generate a query with 31 columns to verify the limit is enforced.
  std::string columns;
  for (int i = 1; i <= 31; ++i) {
    if (i > 1) {
      columns += ", ";
    }
    columns += fmt::format("c{}", i);
  }

  std::string sql = fmt::format(
      "SELECT {}, count(1) FROM (SELECT 1 as c1, 2 as c2, 3 as c3, 4 as c4, "
      "5 as c5, 6 as c6, 7 as c7, 8 as c8, 9 as c9, 10 as c10, "
      "11 as c11, 12 as c12, 13 as c13, 14 as c14, 15 as c15, 16 as c16, "
      "17 as c17, 18 as c18, 19 as c19, 20 as c20, 21 as c21, 22 as c22, "
      "23 as c23, 24 as c24, 25 as c25, 26 as c26, 27 as c27, 28 as c28, "
      "29 as c29, 30 as c30, 31 as c31) GROUP BY CUBE({})",
      columns,
      columns);

  VELOX_ASSERT_THROW(parseSql(sql), "CUBE supports at most 30 columns");
}

TEST_F(PrestoParserTest, distinct) {
  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().project().aggregate();
    testSelect("SELECT DISTINCT n_regionkey FROM nation", matcher);
    testSelect(
        "SELECT DISTINCT n_regionkey, length(n_name) FROM nation", matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder()
                       .tableScan()
                       .aggregate()
                       .project()
                       .aggregate();
    testSelect(
        "SELECT DISTINCT count(1) FROM nation GROUP BY n_regionkey", matcher);
  }

  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate();
    testSelect("SELECT DISTINCT * FROM nation", matcher);
  }
}

TEST_F(PrestoParserTest, groupingKeyExpr) {
  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate().project();

    testSelect(
        "SELECT n_name, count(1), length(n_name) FROM nation GROUP BY 1",
        matcher);
  }

  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate();
    testSelect(
        "SELECT substr(n_name, 1, 2), count(1) FROM nation GROUP BY 1",
        matcher);

    testSelect(
        "SELECT r_regionkey IN (SELECT n_regionkey FROM nation), count(1) "
        "FROM region GROUP BY 1",
        matcher);
  }

  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate().project();
    testSelect(
        "SELECT count(1) FROM nation GROUP BY substr(n_name, 1, 2)", matcher);
  }
}

TEST_F(PrestoParserTest, having) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder()
                       .tableScan()
                       .aggregate()
                       .filter()
                       .project();

    testSelect(
        "SELECT n_name FROM nation GROUP BY 1 HAVING sum(length(n_comment)) > 10",
        matcher);
  }
}

TEST_F(PrestoParserTest, scalarOverAgg) {
  auto matcher =
      lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate().project();

  testSelect(
      "SELECT sum(n_regionkey) + count(1), avg(length(n_name)) * 0.3 "
      "FROM nation",
      matcher);

  testSelect(
      "SELECT n_regionkey, sum(n_nationkey) + count(1), avg(length(n_name)) * 0.3 "
      "FROM nation "
      "GROUP BY 1",
      matcher);
}

TEST_F(PrestoParserTest, aggregateOptions) {
  lp::AggregateNodePtr agg;
  auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate(
      [&](const auto& node) {
        agg = std::dynamic_pointer_cast<const lp::AggregateNode>(node);
      });

  testSelect("SELECT array_agg(distinct n_regionkey) FROM nation", matcher);
  ASSERT_TRUE(agg != nullptr);
  ASSERT_EQ(1, agg->aggregates().size());
  ASSERT_TRUE(agg->aggregateAt(0)->isDistinct());
  ASSERT_TRUE(agg->aggregateAt(0)->filter() == nullptr);
  ASSERT_EQ(0, agg->aggregateAt(0)->ordering().size());

  testSelect(
      "SELECT array_agg(n_nationkey ORDER BY n_regionkey) FROM nation",
      matcher);
  ASSERT_TRUE(agg != nullptr);
  ASSERT_EQ(1, agg->aggregates().size());
  ASSERT_FALSE(agg->aggregateAt(0)->isDistinct());
  ASSERT_TRUE(agg->aggregateAt(0)->filter() == nullptr);
  ASSERT_EQ(1, agg->aggregateAt(0)->ordering().size());

  testSelect(
      "SELECT array_agg(n_nationkey) FILTER (WHERE n_regionkey = 1) FROM nation",
      matcher);
  ASSERT_TRUE(agg != nullptr);
  ASSERT_EQ(1, agg->aggregates().size());
  ASSERT_FALSE(agg->aggregateAt(0)->isDistinct());
  ASSERT_FALSE(agg->aggregateAt(0)->filter() == nullptr);
  ASSERT_EQ(0, agg->aggregateAt(0)->ordering().size());

  testSelect(
      "SELECT array_agg(distinct n_regionkey) FILTER (WHERE n_name like 'A%') FROM nation",
      matcher);
  ASSERT_TRUE(agg != nullptr);
  ASSERT_EQ(1, agg->aggregates().size());
  ASSERT_TRUE(agg->aggregateAt(0)->isDistinct());
  ASSERT_FALSE(agg->aggregateAt(0)->filter() == nullptr);
  ASSERT_EQ(0, agg->aggregateAt(0)->ordering().size());

  testSelect(
      "SELECT array_agg(n_regionkey ORDER BY n_name) FILTER (WHERE n_name like 'A%') FROM nation",
      matcher);
  ASSERT_TRUE(agg != nullptr);
  ASSERT_EQ(1, agg->aggregates().size());
  ASSERT_FALSE(agg->aggregateAt(0)->isDistinct());
  ASSERT_FALSE(agg->aggregateAt(0)->filter() == nullptr);
  ASSERT_EQ(1, agg->aggregateAt(0)->ordering().size());
}

TEST_F(PrestoParserTest, orderBy) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder()
                       .tableScan()
                       .aggregate()
                       .sort()
                       .project();

    testSelect(
        "select n_regionkey from nation group by 1 order by count(1)", matcher);
  }

  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate().sort();

    testSelect(
        "select n_regionkey, count(1) from nation group by 1 order by count(1)",
        matcher);

    testSelect(
        "select n_regionkey, count(1) from nation group by 1 order by 2",
        matcher);

    testSelect(
        "select n_regionkey, count(1) as c from nation group by 1 order by c",
        matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder()
                       .tableScan()
                       .aggregate()
                       .project()
                       .sort();

    testSelect(
        "select n_regionkey, count(1) * 2 from nation group by 1 order by 2",
        matcher);

    testSelect(
        "select n_regionkey, count(1) * 2 from nation group by 1 order by count(1) * 2",
        matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder()
                       .tableScan()
                       .aggregate()
                       .project()
                       .sort()
                       .project();
    testSelect(
        "select n_regionkey, count(1) * 2 from nation group by 1 order by count(1) * 3",
        matcher);
  }
}

TEST_F(PrestoParserTest, join) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().join(
        lp::test::LogicalPlanMatcherBuilder().tableScan().build());

    testSelect("SELECT * FROM nation, region", matcher);

    testSelect(
        "SELECT * FROM nation LEFT JOIN region ON n_regionkey = r_regionkey",
        matcher);

    testSelect(
        "SELECT * FROM nation RIGHT JOIN region ON nation.n_regionkey = region.r_regionkey",
        matcher);

    testSelect(
        "SELECT * FROM nation n LEFT JOIN region r ON n.n_regionkey = r.r_regionkey",
        matcher);

    testSelect(
        "SELECT * FROM nation FULL OUTER JOIN region ON n_regionkey = r_regionkey",
        matcher);
  }

  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder()
            .tableScan()
            .join(lp::test::LogicalPlanMatcherBuilder().tableScan().build())
            .filter();

    testSelect(
        "SELECT * FROM nation, region WHERE n_regionkey = r_regionkey",
        matcher);
  }

  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder()
            .tableScan()
            .join(lp::test::LogicalPlanMatcherBuilder().tableScan().build())
            .filter()
            .project();

    testSelect(
        "SELECT n_name, r_name FROM nation, region WHERE n_regionkey = r_regionkey",
        matcher);
  }
}

TEST_F(PrestoParserTest, joinOnSubquery) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().join(
      lp::test::LogicalPlanMatcherBuilder().tableScan().build());

  // Correlated subquery in JOIN ON clause referencing left-side columns works.
  testSelect(
      "SELECT * FROM nation n JOIN region r "
      "ON n.n_regionkey = r.r_regionkey "
      "AND n.n_nationkey IN (SELECT s_nationkey FROM supplier s WHERE s.s_nationkey = n.n_nationkey)",
      matcher);

  // Correlated subquery in JOIN ON clause referencing right-side columns.
  testSelect(
      "SELECT * FROM nation n JOIN region r "
      "ON n.n_regionkey = r.r_regionkey "
      "AND r.r_regionkey IN (SELECT s_nationkey FROM supplier s WHERE s.s_nationkey = r.r_regionkey)",
      matcher);

  // Correlated EXISTS referencing right-side columns.
  testSelect(
      "SELECT * FROM nation n JOIN region r "
      "ON n.n_regionkey = r.r_regionkey "
      "AND EXISTS (SELECT 1 FROM supplier s WHERE s.s_nationkey = r.r_regionkey)",
      matcher);

  // LEFT JOIN with correlated subquery referencing right-side (null-supplying)
  // columns.
  testSelect(
      "SELECT * FROM nation n LEFT JOIN region r "
      "ON n.n_regionkey = r.r_regionkey "
      "AND EXISTS (SELECT 1 FROM supplier s WHERE s.s_nationkey = r.r_regionkey)",
      matcher);
}

TEST_F(PrestoParserTest, unionAll) {
  auto matcher =
      lp::test::LogicalPlanMatcherBuilder().tableScan().project().setOperation(
          lp::SetOperation::kUnionAll,
          lp::test::LogicalPlanMatcherBuilder().tableScan().project().build());

  testSelect(
      "SELECT n_name FROM nation UNION ALL SELECT r_name FROM region", matcher);
}

TEST_F(PrestoParserTest, union) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder()
                     .tableScan()
                     .project()
                     .setOperation(
                         lp::SetOperation::kUnionAll,
                         lp::test::LogicalPlanMatcherBuilder()
                             .tableScan()
                             .project()
                             .build())
                     .aggregate();

  testSelect(
      "SELECT n_name FROM nation UNION SELECT r_name FROM region", matcher);
}

TEST_F(PrestoParserTest, except) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder()
                     .tableScan()
                     .project()
                     .setOperation(
                         lp::SetOperation::kExcept,
                         lp::test::LogicalPlanMatcherBuilder()
                             .tableScan()
                             .project()
                             .build())
                     .aggregate();

  testSelect(
      "SELECT n_name FROM nation EXCEPT SELECT r_name FROM region", matcher);
}

TEST_F(PrestoParserTest, intersect) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder()
                     .tableScan()
                     .project()
                     .setOperation(
                         lp::SetOperation::kIntersect,
                         lp::test::LogicalPlanMatcherBuilder()
                             .tableScan()
                             .project()
                             .build())
                     .aggregate();

  testSelect(
      "SELECT n_name FROM nation INTERSECT SELECT r_name FROM region", matcher);
}

TEST_F(PrestoParserTest, exists) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().filter();

    testSelect(
        "SELECT * FROM region WHERE exists (SELECT * from nation WHERE n_name like 'A%' and r_regionkey = n_regionkey)",
        matcher);

    testSelect(
        "SELECT * FROM region WHERE not exists (SELECT * from nation WHERE n_name like 'A%' and r_regionkey = n_regionkey)",
        matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().project();

    testSelect(
        "SELECT EXISTS (SELECT * from nation WHERE n_regionkey = r_regionkey) FROM region",
        matcher);
  }
}

TEST_F(PrestoParserTest, values) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().values(
        ROW({"c0", "c1", "c2"}, {INTEGER(), DOUBLE(), VARCHAR()}));

    testSelect(
        "SELECT * FROM (VALUES (1, 1.1, 'foo'), (2, null, 'bar'))", matcher);

    testSelect(
        "SELECT * FROM (VALUES (1, null, 'foo'), (2, 2.2, 'bar'))", matcher);
  }

  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().values(ROW({"c0"}, {INTEGER()}));
    testSelect("SELECT * FROM (VALUES (1), (2), (3), (4))", matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().values(
        ROW({"c0", "c1"}, {REAL(), INTEGER()}));
    testSelect("SELECT * FROM (VALUES (real '1', 1 + 2))", matcher);
  }
}

TEST_F(PrestoParserTest, tablesample) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan().sample();

    testSelect("SELECT * FROM nation TABLESAMPLE BERNOULLI (10.0)", matcher);
    testSelect("SELECT * FROM nation TABLESAMPLE SYSTEM (1.5)", matcher);

    testSelect("SELECT * FROM nation TABLESAMPLE BERNOULLI (10)", matcher);
    testSelect("SELECT * FROM nation TABLESAMPLE BERNOULLI (1 + 2)", matcher);
  }

  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().aggregate().sample();

    testSelect(
        "SELECT * FROM (SELECT l_orderkey, count(*) FROM lineitem GROUP BY 1) "
        "TABLESAMPLE BERNOULLI (1.5)",
        matcher);
  }
}

TEST_F(PrestoParserTest, everything) {
  auto matcher =
      lp::test::LogicalPlanMatcherBuilder()
          .tableScan()
          .join(lp::test::LogicalPlanMatcherBuilder().tableScan().build())
          .filter()
          .aggregate()
          .sort();

  testSelect(
      "SELECT r_name, count(*) FROM nation, region "
      "WHERE n_regionkey = r_regionkey "
      "GROUP BY 1 "
      "ORDER BY 2 DESC",
      matcher);
}

TEST_F(PrestoParserTest, explainSelect) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().tableScan();
    testExplain("EXPLAIN SELECT * FROM nation", matcher);
  }

  auto parser = makeParser();
  {
    auto statement = parser.parse("EXPLAIN ANALYZE SELECT * FROM nation");
    ASSERT_TRUE(statement->isExplain());

    auto explainStatement = statement->as<ExplainStatement>();
    ASSERT_TRUE(explainStatement->isAnalyze());
  }

  {
    auto statement =
        parser.parse("EXPLAIN (TYPE LOGICAL) SELECT * FROM nation", true);
    ASSERT_TRUE(statement->isExplain());

    auto explainStatement = statement->as<ExplainStatement>();
    ASSERT_FALSE(explainStatement->isAnalyze());
    ASSERT_TRUE(explainStatement->type() == ExplainStatement::Type::kLogical);
  }

  {
    auto statement =
        parser.parse("EXPLAIN (TYPE GRAPH) SELECT * FROM nation", true);
    ASSERT_TRUE(statement->isExplain());

    auto explainStatement = statement->as<ExplainStatement>();
    ASSERT_FALSE(explainStatement->isAnalyze());
    ASSERT_TRUE(explainStatement->type() == ExplainStatement::Type::kGraph);
  }

  {
    auto statement =
        parser.parse("EXPLAIN (TYPE OPTIMIZED) SELECT * FROM nation");
    ASSERT_TRUE(statement->isExplain());

    auto explainStatement = statement->as<ExplainStatement>();
    ASSERT_FALSE(explainStatement->isAnalyze());
    ASSERT_TRUE(explainStatement->type() == ExplainStatement::Type::kOptimized);
  }

  {
    auto statement =
        parser.parse("EXPLAIN (TYPE EXECUTABLE) SELECT * FROM nation");
    ASSERT_TRUE(statement->isExplain());

    auto explainStatement = statement->as<ExplainStatement>();
    ASSERT_FALSE(explainStatement->isAnalyze());
    ASSERT_TRUE(
        explainStatement->type() == ExplainStatement::Type::kExecutable);
  }

  {
    auto statement =
        parser.parse("EXPLAIN (TYPE DISTRIBUTED) SELECT * FROM nation");
    ASSERT_TRUE(statement->isExplain());

    auto explainStatement = statement->as<ExplainStatement>();
    ASSERT_FALSE(explainStatement->isAnalyze());
    ASSERT_TRUE(
        explainStatement->type() == ExplainStatement::Type::kExecutable);
  }
}

TEST_F(PrestoParserTest, explainShow) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().values();
  testExplain("EXPLAIN SHOW CATALOGS", matcher);

  testExplain("EXPLAIN SHOW COLUMNS FROM nation", matcher);

  testExplain("EXPLAIN SHOW FUNCTIONS", matcher);
}

TEST_F(PrestoParserTest, explainInsert) {
  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().tableScan().tableWrite();
    testExplain("EXPLAIN INSERT INTO region SELECT * FROM region", matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().values().tableWrite();
    testExplain("EXPLAIN INSERT INTO region VALUES (1, 'foo', 'bar')", matcher);
  }
}

TEST_F(PrestoParserTest, showCatalogs) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().values();
    testSelect("SHOW CATALOGS", matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().values().filter();
    testSelect("SHOW CATALOGS LIKE 'tpch'", matcher);
  }
}

TEST_F(PrestoParserTest, describe) {
  auto matcher = lp::test::LogicalPlanMatcherBuilder().values();
  testSelect("DESCRIBE nation", matcher);

  testSelect("DESC orders", matcher);

  testSelect("SHOW COLUMNS FROM lineitem", matcher);
}

TEST_F(PrestoParserTest, showFunctions) {
  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().values();
    testSelect("SHOW FUNCTIONS", matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder().values().filter();
    testSelect("SHOW FUNCTIONS LIKE 'array%'", matcher);
  }
}

TEST_F(PrestoParserTest, unqualifiedAccessAfterJoin) {
  auto sql =
      "SELECT n_name FROM (SELECT n1.n_name as n_name FROM nation n1, nation n2)";

  auto matcher =
      lp::test::LogicalPlanMatcherBuilder()
          .tableScan()
          .join(lp::test::LogicalPlanMatcherBuilder().tableScan().build())
          .project()
          .project();
  testSelect(sql, matcher);
}

TEST_F(PrestoParserTest, duplicateAliases) {
  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder().values().project().project();
    testSelect(
        "SELECT a as x, b as x FROM (VALUES (1, 2)) AS t(a, b)", matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder()
                       .values()
                       .project()
                       .unnest()
                       .project();
    testSelect(
        "SELECT a as x, u.x FROM (VALUES (1, ARRAY[10, 20])) AS t(a, b) "
        "CROSS JOIN UNNEST(b) AS u(x)",
        matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder()
                       .values()
                       .project()
                       .aggregate()
                       .project();
    testSelect(
        "SELECT sum(a) as x, sum(b) as x FROM (VALUES (1, 2)) AS t(a, b)",
        matcher);
  }

  // Referencing a duplicate column shoud fail.
  VELOX_ASSERT_THROW(
      parseSql(
          "SELECT x FROM (SELECT a as x, b as x FROM (VALUES (1, 2)) AS t(a, b))"),
      "Cannot resolve");
}

TEST_F(PrestoParserTest, qualifiedStarInUnionAfterJoin) {
  parseSql(
      "SELECT * FROM (VALUES (1)) t(id) "
      "UNION ALL "
      "SELECT a.* FROM (VALUES (1)) a(id) JOIN (VALUES (2)) b(id) ON a.id = b.id");
}

} // namespace
} // namespace axiom::sql::presto::test
