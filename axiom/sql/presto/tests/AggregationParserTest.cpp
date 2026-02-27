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
  VELOX_ASSERT_THROW(
      parseSql("SELECT n_name AS x FROM nation GROUP BY x"),
      "Cannot resolve column: x");
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

  VELOX_ASSERT_THROW(parseSql(sql), "CUBE supports at most 30 columns");
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
  VELOX_ASSERT_THROW(
      parseSql("SELECT sum(n_regionkey) AS s FROM nation HAVING s > 10"),
      "HAVING clause cannot reference column: s");

  VELOX_ASSERT_THROW(
      parseSql(
          "SELECT n_regionkey AS k, count(*) FROM nation GROUP BY 1 HAVING k > 2"),
      "HAVING clause cannot reference column: k");

  // HAVING cannot reference non-grouped columns.
  VELOX_ASSERT_THROW(
      parseSql(
          "SELECT n_regionkey FROM nation GROUP BY 1 HAVING n_comment = 'x'"),
      "HAVING clause cannot reference column: n_comment");

  // HAVING with alias-on-aggregate shadowing a FROM column must not silently
  // resolve to the aggregate. 'n_regionkey' in HAVING refers to the FROM
  // column, which is not a grouping key ('n_regionkey + 1' is).
  VELOX_ASSERT_THROW(
      parseSql(
          "SELECT n_regionkey + 1, count(*) AS n_regionkey FROM nation "
          "GROUP BY 1 HAVING n_regionkey > 10"),
      "HAVING clause cannot reference column: n_regionkey");

  // HAVING with alias-on-grouping-key shadowing a FROM column must not
  // silently resolve to the grouping key. 'n_nationkey' in HAVING refers to
  // the FROM column, which is not a grouping key ('n_regionkey' is).
  VELOX_ASSERT_THROW(
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
  {
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
  }

  {
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
  }

  {
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
  }

  {
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
}

} // namespace
} // namespace axiom::sql::presto::test
