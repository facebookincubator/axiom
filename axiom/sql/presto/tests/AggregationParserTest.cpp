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

#include "axiom/sql/presto/tests/ExpectPrestoSqlError.h"
#include "axiom/sql/presto/tests/PrestoParserTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace axiom::sql::presto::test {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

namespace {

class AggregationParserTest : public PrestoParserTestBase {};

TEST_F(AggregationParserTest, countStar) {
  {
    auto matcher = matchScan().aggregate().output();

    testSelect("SELECT count(*) FROM nation", matcher);
    testSelect("SELECT count(1) FROM nation", matcher);

    testSelect("SELECT count(1) \"count\" FROM nation", matcher);
    testSelect("SELECT count(1) AS \"count\" FROM nation", matcher);
  }

  {
    // Global aggregation with HAVING clause.
    auto matcher = matchScan().aggregate().filter().output();
    testSelect("SELECT count(*) FROM nation HAVING count(*) > 100", matcher);
  }
}

TEST_F(AggregationParserTest, aggregateCoercions) {
  auto matcher = matchScan().aggregate().output();

  testSelect("SELECT corr(n_nationkey, 1.2) FROM nation", matcher);
}

TEST_F(AggregationParserTest, simpleGroupBy) {
  {
    auto matcher = matchScan().aggregate().output();

    testSelect("SELECT n_name, count(1) FROM nation GROUP BY 1", matcher);
    testSelect("SELECT n_name, count(1) FROM nation GROUP BY n_name", matcher);
  }

  {
    auto matcher = matchScan().aggregate().project().output();
    testSelect(
        "SELECT count(1) FROM nation GROUP BY n_name, n_regionkey", matcher);
  }

  // GROUP BY resolves against FROM columns, not SELECT aliases.
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSql("SELECT n_name AS x FROM nation GROUP BY x"),
      "Cannot resolve column: x");

  // GROUP BY ordinal out of range.
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSql("SELECT 1 GROUP BY 1, 2"),
      "GROUP BY position is not in select list: 2");
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSql("SELECT 1 GROUP BY 0"),
      "GROUP BY position is not in select list: 0");
}

// GROUP BY ordinals with SELECT * and other expanding items.
TEST_F(AggregationParserTest, groupByOrdinalWithSelectStar) {
  testSelect(
      "SELECT * FROM nation GROUP BY 4, 3, 2, 1",
      matchScan()
          .aggregate({"n_comment", "n_regionkey", "n_name", "n_nationkey"}, {})
          .project()
          .output());

  testSelect(
      "SELECT n.* FROM nation n GROUP BY 4, 3, 2, 1",
      matchScan()
          .aggregate({"n_comment", "n_regionkey", "n_name", "n_nationkey"}, {})
          .project()
          .output());

  // Mixed: SELECT expr, *. Ordinals 2..5 map to expanded * columns.
  // The expression at ordinal 1 is not in GROUP BY and is projected on top.
  testSelect(
      "SELECT n_name || '_suffix', * FROM nation GROUP BY 2, 3, 4, 5",
      matchScan()
          .aggregate({"n_nationkey", "n_name", "n_regionkey", "n_comment"}, {})
          .project(
              {"concat(n_name, _suffix)",
               "n_nationkey",
               "n_name",
               "n_regionkey",
               "n_comment"})
          .output());

  // SELECT COLUMNS(...) with ordinals — matches a subset of columns.
  testSelect(
      "SELECT COLUMNS('n_.*key') FROM nation GROUP BY 1, 2",
      matchScan().aggregate({"n_nationkey", "n_regionkey"}, {}).output());

  // GROUP BY ordinal out of range for expanded SELECT *.
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSql("SELECT * FROM nation GROUP BY 1, 2, 3, 4, 5"),
      "GROUP BY position is not in select list: 5");

  // GROUP BY ordinal out of range for SELECT COLUMNS(...).
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSql("SELECT COLUMNS('n_.*key') FROM nation GROUP BY 1, 2, 3"),
      "GROUP BY position is not in select list: 3");

  // SELECT * EXCLUDE with ordinals. EXCLUDE removes n_name, leaving 3 columns:
  // n_nationkey(1), n_regionkey(2), n_comment(3).
  testSelect(
      "SELECT * EXCLUDE (n_name) FROM nation GROUP BY 1, 2, 3",
      matchScan()
          .aggregate({"n_nationkey", "n_regionkey", "n_comment"}, {})
          .output());

  // SELECT * REPLACE with ordinals. REPLACE substitutes n_name with
  // concat(n_name, '_x'). Ordinal 2 maps to the replaced expression.
  testSelect(
      "SELECT * REPLACE (n_name || '_x' AS n_name) FROM nation GROUP BY 1, 2, 3, 4",
      matchScan()
          .aggregate(
              {"n_nationkey", "concat(n_name, _x)", "n_regionkey", "n_comment"},
              {})
          .output());
}

TEST_F(AggregationParserTest, groupingSets) {
  lp::AggregateNodePtr agg;
  auto matcher =
      matchScan()
          .aggregate([&](const auto& node) {
            agg = std::dynamic_pointer_cast<const lp::AggregateNode>(node);
          })
          .output();

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

  // Empty grouping set collapses to global aggregation.
  testSelect(
      "SELECT count(1) FROM nation "
      "GROUP BY GROUPING SETS (())",
      matchScan().aggregate({}, {"count(1)"}).output());

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

TEST_F(AggregationParserTest, rollup) {
  lp::AggregateNodePtr agg;
  auto matcher =
      matchScan()
          .aggregate([&](const auto& node) {
            agg = std::dynamic_pointer_cast<const lp::AggregateNode>(node);
          })
          .output();

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

TEST_F(AggregationParserTest, cube) {
  lp::AggregateNodePtr agg;
  auto matcher =
      matchScan()
          .aggregate([&](const auto& node) {
            agg = std::dynamic_pointer_cast<const lp::AggregateNode>(node);
          })
          .output();

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

TEST_F(AggregationParserTest, mixedGroupByWithRollup) {
  lp::AggregateNodePtr agg;
  auto matcher =
      matchScan()
          .aggregate([&](const auto& node) {
            agg = std::dynamic_pointer_cast<const lp::AggregateNode>(node);
          })
          .output();

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

TEST_F(AggregationParserTest, groupingSetsOrdinalCaching) {
  lp::AggregateNodePtr agg;
  auto matcher =
      matchScan()
          .aggregate([&](const auto& node) {
            agg = std::dynamic_pointer_cast<const lp::AggregateNode>(node);
          })
          .output();

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

TEST_F(AggregationParserTest, groupingSetsSubqueryOrdinal) {
  lp::AggregateNodePtr agg;
  auto matcher =
      matchScan()
          .aggregate([&](const auto& node) {
            agg = std::dynamic_pointer_cast<const lp::AggregateNode>(node);
          })
          .output();

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

TEST_F(AggregationParserTest, cubeColumnLimit) {
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

  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSql(sql), "CUBE supports at most 30 columns");
}

TEST_F(AggregationParserTest, groupByDistinct) {
  // GROUP BY DISTINCT collapses all-identical sets to regular GROUP BY.
  // (a, b), (b, a), (a, b) are identical (order-insensitive) → single set.
  {
    auto matcher = matchScan()
                       .aggregate({"n_regionkey", "n_name"}, {"count(1)"}, {})
                       .output();
    testSelect(
        "SELECT n_regionkey, n_name, count(1) FROM nation "
        "GROUP BY DISTINCT GROUPING SETS "
        "((n_regionkey, n_name), (n_name, n_regionkey), (n_regionkey, n_name))",
        matcher);
  }

  // GROUP BY DISTINCT with two genuinely different sets preserves them.
  {
    auto matcher =
        matchScan()
            .aggregate({"n_regionkey", "n_name"}, {"count(1)"}, {{0}, {1}})
            .output();
    testSelect(
        "SELECT n_regionkey, n_name, count(1) FROM nation "
        "GROUP BY DISTINCT GROUPING SETS "
        "((n_regionkey), (n_name), (n_regionkey))",
        matcher);
  }

  // Key dedup within sets + DISTINCT across sets collapse to regular GROUP BY.
  {
    auto matcher = matchScan()
                       .aggregate({"n_name", "n_regionkey"}, {"count(1)"}, {})
                       .output();
    testSelect(
        "SELECT n_name, n_regionkey, count(1) FROM nation "
        "GROUP BY DISTINCT GROUPING SETS "
        "((n_name, n_regionkey, n_name), (n_regionkey, n_name, n_regionkey))",
        matcher);
  }

  // Empty grouping set with DISTINCT collapses to global aggregation.
  testSelect(
      "SELECT count(1) FROM nation "
      "GROUP BY DISTINCT GROUPING SETS (())",
      matchScan().aggregate({}, {"count(1)"}).output());
}

TEST_F(AggregationParserTest, groupingSetsDedup) {
  // All identical grouping sets are preserved without DISTINCT.
  // The optimizer may collapse them as an optimization.
  {
    auto matcher = matchScan()
                       .aggregate(
                           {"n_regionkey", "n_name"},
                           {"count(1)"},
                           {{0, 1}, {1, 0}, {0, 1}})
                       .output();
    testSelect(
        "SELECT n_regionkey, n_name, count(1) FROM nation "
        "GROUP BY GROUPING SETS "
        "((n_regionkey, n_name), (n_name, n_regionkey), (n_regionkey, n_name))",
        matcher);
  }

  // Multiple distinct sets with duplicates — preserved per SQL standard.
  {
    auto matcher =
        matchScan()
            .aggregate({"n_regionkey", "n_name"}, {"count(1)"}, {{0}, {1}, {0}})
            .output();
    testSelect(
        "SELECT n_regionkey, n_name, count(1) FROM nation "
        "GROUP BY GROUPING SETS ((n_regionkey), (n_name), (n_regionkey))",
        matcher);
  }

  // Duplicate keys within a single grouping set are deduplicated.
  // (n_regionkey, n_regionkey) → (n_regionkey), single set → regular GROUP BY.
  {
    auto matcher =
        matchScan().aggregate({"n_regionkey"}, {"count(1)"}, {}).output();
    testSelect(
        "SELECT n_regionkey, count(1) FROM nation "
        "GROUP BY GROUPING SETS ((n_regionkey, n_regionkey))",
        matcher);
  }
}

TEST_F(AggregationParserTest, distinct) {
  {
    auto matcher = matchScan().project().distinct().output();
    testSelect("SELECT DISTINCT n_regionkey FROM nation", matcher);
    testSelect(
        "SELECT DISTINCT n_regionkey, length(n_name) FROM nation", matcher);
  }

  {
    auto matcher = matchScan().aggregate().project().distinct().output();
    testSelect(
        "SELECT DISTINCT count(1) FROM nation GROUP BY n_regionkey", matcher);
  }

  {
    auto matcher = matchScan().distinct().output();
    testSelect("SELECT DISTINCT * FROM nation", matcher);
  }
}

TEST_F(AggregationParserTest, groupingKeyExpr) {
  {
    auto matcher = matchScan().aggregate().project().output();

    testSelect(
        "SELECT n_name, count(1), length(n_name) FROM nation GROUP BY 1",
        matcher);
  }

  {
    auto matcher = matchScan().aggregate().output();
    testSelect(
        "SELECT substr(n_name, 1, 2), count(1) FROM nation GROUP BY 1",
        matcher);

    testSelect(
        "SELECT r_regionkey IN (SELECT n_regionkey FROM nation), count(1) "
        "FROM region GROUP BY 1",
        matcher);
  }

  {
    auto matcher = matchScan().aggregate().project().output();
    testSelect(
        "SELECT count(1) FROM nation GROUP BY substr(n_name, 1, 2)", matcher);
  }
}

TEST_F(AggregationParserTest, having) {
  auto matcher = matchScan().aggregate().filter().project().output();

  // HAVING with aggregate expression over a non-selected column.
  testSelect(
      "SELECT n_name FROM nation GROUP BY 1 HAVING sum(length(n_comment)) > 10",
      matcher);

  // HAVING referencing a grouping key.
  testSelect(
      "SELECT n_regionkey, count(*) FROM nation GROUP BY 1 HAVING n_regionkey > 2",
      matchScan().aggregate().filter().output());

  // HAVING referencing both a grouping key and an aggregate.
  testSelect(
      "SELECT n_regionkey, count(*) FROM nation GROUP BY 1 HAVING n_regionkey > count(*)",
      matchScan().aggregate().filter().output());

  // HAVING with count(*) not in SELECT.
  testSelect(
      "SELECT n_name FROM nation GROUP BY 1 HAVING count(*) > 5", matcher);

  // HAVING cannot reference SELECT aliases.
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSql("SELECT sum(n_regionkey) AS s FROM nation HAVING s > 10"),
      "HAVING clause cannot reference column: s");

  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSql(
          "SELECT n_regionkey AS k, count(*) FROM nation GROUP BY 1 HAVING k > 2"),
      "HAVING clause cannot reference column: k");

  // HAVING cannot reference non-grouped columns.
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSql(
          "SELECT n_regionkey FROM nation GROUP BY 1 HAVING n_comment = 'x'"),
      "HAVING clause cannot reference column: n_comment");

  // HAVING with alias-on-aggregate shadowing a FROM column must not silently
  // resolve to the aggregate. 'n_regionkey' in HAVING refers to the FROM
  // column, which is not a grouping key ('n_regionkey + 1' is).
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSql(
          "SELECT n_regionkey + 1, count(*) AS n_regionkey FROM nation "
          "GROUP BY 1 HAVING n_regionkey > 10"),
      "HAVING clause cannot reference column: n_regionkey");

  // HAVING with alias-on-grouping-key shadowing a FROM column must not
  // silently resolve to the grouping key. 'n_nationkey' in HAVING refers to
  // the FROM column, which is not a grouping key ('n_regionkey' is).
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSql(
          "SELECT n_regionkey AS n_nationkey, count(*) FROM nation "
          "GROUP BY 1 HAVING n_nationkey > 10"),
      "HAVING clause cannot reference column: n_nationkey");
}

TEST_F(AggregationParserTest, scalarOverAgg) {
  auto matcher = matchScan().aggregate().project().output();

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

TEST_F(AggregationParserTest, aggregateOptions) {
  lp::AggregateNodePtr agg;
  auto matcher =
      matchScan()
          .aggregate([&](const auto& node) {
            agg = std::dynamic_pointer_cast<const lp::AggregateNode>(node);
          })
          .output();

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

// Verifies that aggregation calls with same expression but different options
// are treated as different aggregates.
TEST_F(AggregationParserTest, aggregateDeduplication) {
  // Same expression with different DISTINCT options produces two aggregates.
  testSelect(
      "SELECT n_name, sum(n_regionkey) + 1, sum(DISTINCT n_regionkey) * 2 "
      "FROM nation GROUP BY n_name",
      matchScan()
          .aggregate(
              {"n_name"}, {"sum(n_regionkey)", "sum(DISTINCT n_regionkey)"})
          .project({
              "n_name",
              "plus(sum, CAST(1 AS BIGINT))",
              "multiply(sum_0, CAST(2 AS BIGINT))",
          })
          .output());

  // Same expression with different FILTER clauses produces two aggregates.
  testSelect(
      "SELECT n_name, "
      "sum(n_regionkey) FILTER (WHERE n_nationkey > 5) + 1, "
      "sum(n_regionkey) FILTER (WHERE n_nationkey < 10) * 2 "
      "FROM nation GROUP BY n_name",
      matchScan()
          .aggregate(
              {"n_name"},
              {"sum(n_regionkey) FILTER (WHERE gt(n_nationkey, CAST(5 AS BIGINT)))",
               "sum(n_regionkey) FILTER (WHERE lt(n_nationkey, CAST(10 AS BIGINT)))"})
          .project({
              "n_name",
              "plus(sum, CAST(1 AS BIGINT))",
              "multiply(sum_0, CAST(2 AS BIGINT))",
          })
          .output());

  // Same expression with different ORDER BY directions produces two
  // aggregates. No ProjectNode in this plan.
  testSelect(
      "SELECT n_name, "
      "array_agg(n_comment ORDER BY n_nationkey ASC) as agg1, "
      "array_agg(n_comment ORDER BY n_nationkey DESC) as agg2 "
      "FROM nation GROUP BY n_name",
      matchScan()
          .aggregate(
              {"n_name"},
              {"array_agg(n_comment ORDER BY n_nationkey ASC NULLS LAST)",
               "array_agg(n_comment ORDER BY n_nationkey DESC NULLS LAST)"})
          .output());

  // Same expression with same options should be deduplicated to one
  // aggregate. Project references the same column twice.
  testSelect(
      "SELECT n_name, "
      "sum(DISTINCT n_regionkey) FILTER (WHERE n_nationkey > 5) + 1, "
      "sum(DISTINCT n_regionkey) FILTER (WHERE n_nationkey > 5) * 2 "
      "FROM nation GROUP BY n_name",
      matchScan()
          .aggregate(
              {"n_name"},
              {"sum(DISTINCT n_regionkey) FILTER (WHERE gt(n_nationkey, CAST(5 AS BIGINT)))"})
          .project({
              "n_name",
              "plus(sum, CAST(1 AS BIGINT))",
              "multiply(sum, CAST(2 AS BIGINT))",
          })
          .output());
}

TEST_F(AggregationParserTest, groupByWithWindowFunction) {
  connector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  connector_->addTable("u", ROW({"a", "c"}, BIGINT()));
  SCOPE_EXIT {
    connector_->dropTableIfExists("t");
    connector_->dropTableIfExists("u");
  };

  // Window function in SELECT with GROUP BY.
  testSelect(
      "SELECT b, sum(a), row_number() OVER (ORDER BY b) FROM t GROUP BY b",
      matchScan("t")
          .aggregate({"b"}, {"sum(a)"})
          .project({
              "b",
              "sum",
              "row_number() OVER (ORDER BY b ASC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
          })
          .output());

  // Window function with PARTITION BY and GROUP BY.
  testSelect(
      "SELECT b, sum(a), row_number() OVER (PARTITION BY b ORDER BY b) FROM t GROUP BY b",
      matchScan("t")
          .aggregate({"b"}, {"sum(a)"})
          .project({
              "b",
              "sum",
              "row_number() OVER (PARTITION BY b ORDER BY b ASC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
          })
          .output());

  // Window function in ORDER BY with GROUP BY.
  testSelect(
      "SELECT a, sum(b) FROM t GROUP BY a ORDER BY row_number() OVER (ORDER BY a)",
      matchScan("t")
          .aggregate({"a"}, {"sum(b)"})
          .project()
          .sort()
          .project()
          .output());

  // Aggregate reference inside window ORDER BY spec.
  testSelect(
      "SELECT b, sum(a), row_number() OVER (ORDER BY sum(a)) FROM t GROUP BY b",
      matchScan("t")
          .aggregate({"b"}, {"sum(a) as total"})
          .project({"b", "total", "row_number() OVER (ORDER BY total)"})
          .output());

  // Same as above but with PARTITION BY.
  testSelect(
      "SELECT b, sum(a), row_number() OVER (PARTITION BY b ORDER BY sum(a)) FROM t GROUP BY b",
      matchScan("t")
          .aggregate({"b"}, {"sum(a) as total"})
          .project(
              {"b",
               "total",
               "row_number() OVER (PARTITION BY b ORDER BY total)"})
          .output());

  // Aggregate in window ORDER BY that is NOT in the SELECT list.
  testSelect(
      "SELECT b, row_number() OVER (ORDER BY count(*) DESC) AS rank FROM t GROUP BY b",
      matchScan("t")
          .aggregate({"b"}, {"count()"})
          .project({"b", "row_number() OVER (ORDER BY count DESC)"})
          .output({"b", "rank"}));

  // Same as above but with PARTITION BY.
  testSelect(
      "SELECT b, row_number() OVER (PARTITION BY b ORDER BY count(*) DESC) AS rank FROM t GROUP BY b",
      matchScan("t")
          .aggregate({"b"}, {"count()"})
          .project(
              {"b", "row_number() OVER (PARTITION BY b ORDER BY count DESC)"})
          .output({"b", "rank"}));

  // Aggregate inside window function argument, not in SELECT list.
  testSelect(
      "SELECT b, sum(count(a)) OVER () AS total FROM t GROUP BY b",
      matchScan("t")
          .aggregate({"b"}, {"count(a)"})
          .project({"b", "sum(count) OVER ()"})
          .output({"b", "total"}));

  // Window function nested inside arithmetic expression.
  testSelect(
      "SELECT b, sum(a) * 1.0 / sum(sum(a)) OVER () FROM t GROUP BY b",
      matchScan("t")
          .aggregate({"b"}, {"sum(a)"})
          .project({"b", "sum", "sum(sum) OVER ()"})
          .project(
              {"b", "multiply(cast(sum as double), 1) / cast(expr as double)"})
          .output());

  // Same as above but with PARTITION BY in the nested window function.
  testSelect(
      "SELECT b, sum(a) * 1.0 / sum(sum(a)) OVER (PARTITION BY b) FROM t GROUP BY b",
      matchScan("t")
          .aggregate({"b"}, {"sum(a)"})
          .project({"b", "sum", "sum(sum) OVER (PARTITION BY b)"})
          .project(
              {"b", "multiply(cast(sum as double), 1) / cast(expr as double)"})
          .output());

  // Same as above but with qualified column references.
  testSelect(
      "SELECT t.b, sum(t.a) * 1.0 / sum(sum(t.a)) OVER () FROM t GROUP BY t.b",
      matchScan("t")
          .aggregate({"b"}, {"sum(a)"})
          .project({"b", "sum", "sum(sum) OVER ()"})
          .project(
              {"b", "multiply(cast(sum as double), 1) / cast(expr as double)"})
          .output());

  // Window function call with the same signature as a plain aggregate.
  testSelect(
      "SELECT sum(a) OVER (ORDER BY b), sum(a) FROM t GROUP BY a, b",
      matchScan("t")
          .aggregate({"a", "b"}, {"sum(a)"})
          .project({
              "sum(a) OVER (ORDER BY b)",
              "sum",
          })
          .output());

  // Window function with PARTITION BY referencing a qualified group-by key
  // that is ambiguous (exists in multiple joined tables).
  testSelect(
      "SELECT t.a, u.c, rank() OVER (PARTITION BY t.a) AS rnk "
      "FROM t JOIN u ON t.a = u.a "
      "GROUP BY 1, 2",
      matchScan("t")
          .join(matchScan("u").build())
          .aggregate({"a", "c"}, {})
          .project({"a", "c", "rank() OVER (PARTITION BY a)"})
          .output({"a", "c", "rnk"}));
}

// Qualified column references (e.g. v.x) in SELECT must resolve correctly
// after GROUP BY, even when the name allocator assigns suffixed physical names
// (which happens when the same column name is used in a prior subquery).
TEST_F(AggregationParserTest, qualifiedColumnInGroupBy) {
  // Qualified ref in SELECT matches simple grouping key.
  testSelect(
      "SELECT v.x FROM (VALUES (1, 10)) v(x, y) GROUP BY x",
      matchValues().project().aggregate({"x"}, {}).output({"x"}));

  // Qualified ref as a sub-expression of a projection.
  testSelect(
      "SELECT v.x + 1 FROM (VALUES (1, 10)) v(x, y) GROUP BY x "
      "UNION ALL SELECT 1",
      matchValues()
          .project()
          .aggregate({"x"}, {})
          .project()
          .unionAll(matchValues().project().build())
          .output());

  // Qualified ref in both GROUP BY and SELECT expressions.
  testSelect(
      "SELECT v.x + 1 FROM (VALUES (1, 10)) v(x, y) GROUP BY v.x + 1 "
      "UNION ALL SELECT 1",
      matchValues()
          .project()
          .aggregate()
          .unionAll(matchValues().project().build())
          .output());

  // Cross join with prior subquery exhausting the name 'x'. The second
  // subquery gets suffixed physical names from the shared name allocator.
  // Qualified refs are normalized before aggregate rewriting.
  testSelect(
      "SELECT * FROM "
      "  (SELECT 1 as x) a, "
      "  (SELECT v.x FROM (VALUES (1, 10)) v(x, y) GROUP BY x) b",
      matchValues()
          .project()
          .join(matchValues().project().aggregate().build())
          .output({"x", "x"}));

  // Qualified ref inside expression with GROUP BY on the expression.
  testSelect(
      "SELECT * FROM "
      "  (SELECT 1 as expr) a, "
      "  (SELECT v.x + 1 FROM (VALUES (1, 10)) v(x, y) GROUP BY x + 1) b",
      matchValues()
          .project()
          .join(matchValues().project().aggregate().build())
          .output());

  // Qualified ref in GROUP BY, unqualified in SELECT. Normalization must apply
  // to grouping keys too, not just projections.
  testSelect(
      "SELECT * FROM "
      "  (SELECT 1 as x) a, "
      "  (SELECT x FROM (VALUES (1, 10)) v(x, y) GROUP BY v.x) b",
      matchValues()
          .project()
          .join(matchValues().project().aggregate().build())
          .output({"x", "x"}));

  // Qualified ref in HAVING clause.
  testSelect(
      "SELECT * FROM "
      "  (SELECT 1 as x) a, "
      "  (SELECT v.x FROM (VALUES (1, 10)) v(x, y) "
      "   GROUP BY x HAVING v.x > 0) b",
      matchValues()
          .project()
          .join(matchValues().project().aggregate().filter().build())
          .output({"x", "x"}));

  // Qualified ref in aggregate argument.
  testSelect(
      "SELECT * FROM "
      "  (SELECT 1 as x) a, "
      "  (SELECT sum(v.x) FROM (VALUES (1, 10)) v(x, y) GROUP BY y) b",
      matchValues()
          .project()
          .join(matchValues().project().aggregate().project().build())
          .output());

  // Qualified ref in ORDER BY.
  testSelect(
      "SELECT * FROM "
      "  (SELECT 1 as x) a, "
      "  (SELECT v.x FROM (VALUES (1, 10)) v(x, y) "
      "   GROUP BY x ORDER BY v.x) b",
      matchValues()
          .project()
          .join(matchValues().project().aggregate().sort().build())
          .output({"x", "x"}));

  // Ambiguous column name: both t and u have column 'n_name'. Qualified refs
  // must NOT be normalized when the unqualified name is ambiguous.
  testSelect(
      "SELECT t.n_name, sum(u.n_regionkey) FROM nation t JOIN nation u "
      "ON t.n_nationkey = u.n_nationkey GROUP BY t.n_name",
      matchScan().join(matchScan().build()).aggregate().output());

  // Qualified ref inside DISTINCT aggregate in SELECT. Normalization must
  // update aggregateOptionsMap_ so that rewritePostAggregateExprs can match
  // the aggregate expression with its DISTINCT option.
  testSelect(
      "SELECT count(DISTINCT b.s_acctbal), a.n_name "
      "FROM nation a JOIN supplier b ON a.n_nationkey = b.s_nationkey "
      "GROUP BY a.n_name",
      matchScan("nation")
          .join(matchScan("supplier").build())
          .aggregate({"n_name"}, {"count(DISTINCT s_acctbal)"})
          .project()
          .output());

  // Qualified ref inside DISTINCT aggregate in ORDER BY. The ORDER BY
  // expression comes from a separate toExpr call, producing a different
  // ExprPtr than the one in aggregates_.
  testSelect(
      "SELECT count(DISTINCT b.s_acctbal), a.n_name "
      "FROM nation a JOIN supplier b ON a.n_nationkey = b.s_nationkey "
      "GROUP BY a.n_name "
      "ORDER BY count(DISTINCT b.s_acctbal)",
      matchScan("nation")
          .join(matchScan("supplier").build())
          .aggregate({"n_name"}, {"count(DISTINCT s_acctbal) as cnt"})
          .project()
          .sort({"cnt"})
          .output());

  // Qualified ref inside DISTINCT aggregate in HAVING.
  testSelect(
      "SELECT a.n_name "
      "FROM nation a JOIN supplier b ON a.n_nationkey = b.s_nationkey "
      "GROUP BY a.n_name "
      "HAVING count(DISTINCT b.s_acctbal) > 5",
      matchScan("nation")
          .join(matchScan("supplier").build())
          .aggregate({"n_name"}, {"count(DISTINCT s_acctbal) as cnt"})
          .filter("cnt > 5::bigint")
          .project()
          .output());
}

// Tests that early canonicalization of table-qualified column references
// produces consistent expression trees across GROUP BY, SELECT, HAVING, and
// aggregate arguments, so that mixed qualified/unqualified references to the
// same column match structurally.
TEST_F(AggregationParserTest, columnCanonicalization) {
  // Qualified SELECT, unqualified HAVING.
  testSelect(
      "SELECT nation.n_regionkey, count(*) FROM nation "
      "GROUP BY 1 HAVING n_regionkey > 2",
      matchScan().aggregate({"n_regionkey"}, {"count()"}).filter().output());

  // Unqualified SELECT, qualified HAVING.
  testSelect(
      "SELECT n_regionkey, count(*) FROM nation "
      "GROUP BY 1 HAVING nation.n_regionkey > 2",
      matchScan().aggregate({"n_regionkey"}, {"count()"}).filter().output());

  // Qualified SELECT with table alias, unqualified HAVING.
  testSelect(
      "SELECT n.n_regionkey, count(*) FROM nation n "
      "GROUP BY 1 HAVING n_regionkey > 2",
      matchScan().aggregate({"n_regionkey"}, {"count()"}).filter().output());

  // Aggregate with qualified argument in SELECT, unqualified in HAVING.
  testSelect(
      "SELECT n_regionkey, sum(nation.n_nationkey) FROM nation "
      "GROUP BY 1 HAVING sum(n_nationkey) > 10",
      matchScan()
          .aggregate({"n_regionkey"}, {"sum(n_nationkey)"})
          .filter()
          .output());

  // Struct field dereference must not be confused with a table-qualified
  // column. s.x is a struct field, x is a top-level column — they differ.
  {
    connector_->addTable(
        "st",
        ROW({"x", "s"}, {INTEGER(), ROW({"x", "y"}, {VARCHAR(), DOUBLE()})}));
    AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
        parseSql(
            "SELECT s.x, count(*) FROM st "
            "GROUP BY 1 HAVING x > 0"),
        "HAVING clause cannot reference column: x");
  }

  // JOIN with GROUP BY: ambiguous columns must stay qualified.
  // t(a, b) and u(a, c) share column 'a'.
  {
    connector_->addTable("t", ROW({"a", "b"}, BIGINT()));
    connector_->addTable("u", ROW({"a", "c"}, BIGINT()));
    SCOPE_EXIT {
      connector_->dropTableIfExists("t");
      connector_->dropTableIfExists("u");
    };

    testSelect(
        "SELECT t.a, count(*) FROM t JOIN u ON t.b = u.c "
        "GROUP BY t.a HAVING t.a > 0",
        matchScan().join(matchScan().build()).aggregate().filter().output());

    AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
        parseSql(
            "SELECT t.a, count(*) FROM t JOIN u ON t.b = u.c "
            "GROUP BY t.a HAVING a > 0"),
        "HAVING clause cannot reference column: a");
  }

  // Chained LEFT JOINs with same-named columns. After two merges the
  // unqualified name 'ds' points to c's column, not a's. Canonicalization
  // must not strip the qualifier from a.ds because hasSameColumn returns
  // false (qualified a.ds != unqualified ds which is c's).
  // The grouping key must be 'ds' (a's column), not 'ds_2' (c's column).
  testSelect(
      "SELECT a.ds "
      "FROM (VALUES ('d1'), ('d2')) a(ds) "
      "LEFT JOIN (VALUES ('d3')) b(ds) ON (a.ds = b.ds) "
      "LEFT JOIN (SELECT 'x' as ds WHERE false) c ON (a.ds = c.ds) "
      "GROUP BY 1",
      matchValues()
          .project()
          .join(matchValues().project().build())
          .join(matchValues().filter().project().build())
          .aggregate({"ds"}, {})
          .output({"ds"}));
}

} // namespace
} // namespace axiom::sql::presto::test
