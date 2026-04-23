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

#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

class SubqueryTest : public test::HiveQueriesTestBase {
 protected:
  static void SetUpTestCase() {
    test::HiveQueriesTestBase::SetUpTestCase();
    createTpchTables(
        {velox::tpch::Table::TBL_NATION,
         velox::tpch::Table::TBL_REGION,
         velox::tpch::Table::TBL_CUSTOMER,
         velox::tpch::Table::TBL_ORDERS,
         velox::tpch::Table::TBL_SUPPLIER});
  }
};

TEST_F(SubqueryTest, uncorrelatedScalar) {
  // = <subquery>
  {
    auto query =
        "select * from nation where n_regionkey "
        "= (select r_regionkey from region where r_name like 'AF%')";

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    auto matcher = matchHiveScan("nation")
                       .hashJoin(
                           core::PlanMatcherBuilder()
                               .hiveScan("region", {}, "r_name like 'AF%'")
                               .enforceSingleRow()
                               .build(),
                           velox::core::JoinType::kInner)
                       .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // IN <subquery>
  {
    auto query =
        "select * from nation where n_regionkey "
        "IN (select r_regionkey from region where r_name > 'ASIA')";

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    auto matcher = matchHiveScan("nation")
                       .hashJoin(
                           core::PlanMatcherBuilder()
                               .hiveScan("region", test::gt("r_name", "ASIA"))
                               .build(),
                           velox::core::JoinType::kLeftSemiFilter)
                       .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // IN <subquery> with coercion. The subquery returns a tinyint, which needs to
  // be coerced to bigint to match the left side of the IN predicate.
  {
    auto query =
        "select * from nation where n_regionkey "
        "IN (select cast(r_regionkey as tinyint) from region where r_name > 'ASIA')";

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    auto matcher =
        matchHiveScan("nation")
            .hashJoin(
                core::PlanMatcherBuilder()
                    .hiveScan("region", test::gt("r_name", "ASIA"))
                    .project({"cast(cast(r_regionkey as tinyint) as bigint)"})
                    .build(),
                velox::core::JoinType::kLeftSemiFilter)
            .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // NOT IN <subquery>
  {
    auto query =
        "select * from nation where n_regionkey "
        "NOT IN (select r_regionkey from region where r_name > 'ASIA')";

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    auto matcher = matchHiveScan("nation")
                       .hashJoin(
                           core::PlanMatcherBuilder()
                               .hiveScan("region", test::gt("r_name", "ASIA"))
                               .build(),
                           velox::core::JoinType::kAnti,
                           /*nullAware=*/true)
                       .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

// IN list mixing subqueries with non-subquery expressions.
TEST_F(SubqueryTest, inListWithMixedSubqueries) {
  // IN list with a scalar subquery and a literal. The scalar subquery is
  // extracted, cross-joined, and the IN becomes in(n_regionkey, max, 2).
  {
    auto query =
        "SELECT * FROM nation WHERE n_regionkey IN "
        "((SELECT max(r_regionkey) FROM region), 2)";
    SCOPED_TRACE(query);

    auto plan = toSingleNodePlan(query);
    auto matcher =
        matchHiveScan("nation")
            .nestedLoopJoin(
                matchHiveScan("region")
                    .singleAggregation({}, {"max(r_regionkey) as max_key"})
                    .build())
            .filter("\"in\"(n_regionkey, max_key, 2)")
            .project()
            .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // IN list with two scalar subqueries. Both are extracted, cross-joined, and
  // the IN becomes in(n_regionkey, max_key, min_key).
  {
    auto query =
        "SELECT * FROM nation WHERE n_regionkey IN "
        "((SELECT max(r_regionkey) FROM region), "
        "(SELECT min(r_regionkey) FROM region))";
    SCOPED_TRACE(query);

    auto plan = toSingleNodePlan(query);
    auto matcher =
        matchHiveScan("nation")
            .nestedLoopJoin(
                matchHiveScan("region")
                    .singleAggregation({}, {"max(r_regionkey) as max_key"})
                    .build())
            .nestedLoopJoin(
                matchHiveScan("region")
                    .singleAggregation({}, {"min(r_regionkey_2) as min_key"})
                    .build())
            .filter("\"in\"(n_regionkey, max_key, min_key)")
            .project()
            .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(SubqueryTest, foldable) {
  testConnector_->addTable("t", ROW({"a", "ds"}, {INTEGER(), VARCHAR()}));
  testConnector_->setDiscreteValues(
      "t",
      {"ds"},
      {
          Variant::row({"2025-10-29"}),
          Variant::row({"2025-10-30"}),
          Variant::row({"2025-10-31"}),
          Variant::row({"2025-11-01"}),
          Variant::row({"2025-11-02"}),
          Variant::row({"2025-11-03"}),
      });

  auto parseSql = [&](const std::string& sql) {
    return parseSelect(sql, kTestConnectorId);
  };

  auto makeMatcher = [&](const std::string& filter) {
    return matchScan("t").filter(filter).build();
  };

  {
    auto logicalPlan =
        parseSql("SELECT * FROM t WHERE ds = (SELECT max(ds) FROM t)");

    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = makeMatcher("ds = '2025-11-03'");

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto logicalPlan =
        parseSql("SELECT * FROM t WHERE ds = (SELECT min(ds) FROM t)");

    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = makeMatcher("ds = '2025-10-29'");

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto logicalPlan = parseSql(
        "SELECT * FROM t WHERE ds = (SELECT max(ds) FROM t WHERE ds < '2025-11-02')");

    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = makeMatcher("ds = '2025-11-01'");

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto logicalPlan = parseSql(
        "SELECT * FROM t WHERE ds = (SELECT max(ds) FROM t WHERE ds like '%-10-%')");

    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = makeMatcher("ds = '2025-10-31'");

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto logicalPlan = parseSql(
        "SELECT * FROM t WHERE ds = (SELECT max(ds) FROM t WHERE ds < '2025-01-01')");

    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = makeMatcher("null");

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // IN list with a foldable subquery and a literal. The subquery is
  // constant-folded, resulting in a regular IN with two constants.
  {
    auto logicalPlan = parseSql(
        "SELECT * FROM t WHERE ds IN ((SELECT max(ds) FROM t), '2025-10-29')");

    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = makeMatcher("ds in ('2025-11-03', '2025-10-29')");

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // IN list with two foldable subqueries (prod query pattern). Both subqueries
  // are constant-folded, resulting in a regular IN with two constants.
  {
    auto logicalPlan = parseSql(
        "SELECT * FROM t WHERE ds IN "
        "((SELECT max(ds) FROM t), (SELECT min(ds) FROM t))");

    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = makeMatcher("ds in ('2025-11-03', '2025-10-29')");

    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(SubqueryTest, correlatedExists) {
  {
    auto query =
        "SELECT * FROM nation WHERE "
        "EXISTS (SELECT * FROM region WHERE r_regionkey = n_regionkey)";

    auto matcher =
        matchHiveScan("nation")
            .hashJoin(
                core::PlanMatcherBuilder().hiveScan("region", {}).build(),
                velox::core::JoinType::kLeftSemiFilter)
            .build();

    {
      SCOPED_TRACE(query);
      auto plan = toSingleNodePlan(query);
      AXIOM_ASSERT_PLAN(plan, matcher);
    }

    query =
        "SELECT * FROM nation WHERE "
        "EXISTS (SELECT 1 FROM region WHERE r_regionkey = n_regionkey)";

    {
      SCOPED_TRACE(query);
      auto plan = toSingleNodePlan(query);
      AXIOM_ASSERT_PLAN(plan, matcher);
    }

    // EXISTS with DISTINCT. DISTINCT is semantically unnecessary for EXISTS
    // since EXISTS only checks for row existence. The optimizer should drop the
    // DISTINCT and produce the same plan as above.
    query =
        "SELECT * FROM nation WHERE "
        "EXISTS (SELECT DISTINCT r_name FROM region WHERE r_regionkey = n_regionkey)";

    {
      SCOPED_TRACE(query);
      auto plan = toSingleNodePlan(query);
      AXIOM_ASSERT_PLAN(plan, matcher);
    }
  }

  {
    auto query =
        "SELECT * FROM nation WHERE "
        "EXISTS (SELECT 1 FROM region WHERE r_regionkey > n_regionkey)";

    auto matcher = matchHiveScan("nation")
                       .nestedLoopJoin(
                           matchHiveScan("region").build(),
                           velox::core::JoinType::kLeftSemiProject)
                       .filter()
                       .project()
                       .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto query =
        "WITH a AS (SELECT * FROM nation FULL JOIN region ON n_regionkey = r_regionkey) "
        "SELECT * FROM a WHERE EXISTS(SELECT * FROM(VALUES 1, 2, 3) as t(x) WHERE n_regionkey = x) ";

    auto matcher =
        matchHiveScan("nation")
            .hashJoin(
                matchHiveScan("region").build(), velox::core::JoinType::kFull)
            .hashJoin(
                matchValues().project().build(),
                velox::core::JoinType::kLeftSemiFilter)
            .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Correlated conjuncts referencing multiple tables.
  {
    auto query =
        "WITH t as (SELECT n_nationkey AS nkey, r_regionkey AS rkey FROM nation, region WHERE n_regionkey = r_regionkey) "
        "SELECT * FROM t WHERE EXISTS (SELECT * FROM nation WHERE n_nationkey = nkey AND n_regionkey = rkey)";

    auto matcher =
        matchHiveScan("nation")
            .hashJoin(
                matchHiveScan("region").build(), velox::core::JoinType::kInner)
            .hashJoin(
                matchHiveScan("nation").build(),
                velox::core::JoinType::kLeftSemiFilter)
            .project()
            .build();

    {
      SCOPED_TRACE(query);
      auto plan = toSingleNodePlan(query);
      AXIOM_ASSERT_PLAN(plan, matcher);
    }
  }
}
TEST_F(SubqueryTest, uncorrelatedProject) {
  // Uncorrelated scalar subquery in projection.
  {
    auto query =
        "SELECT r_name, "
        "   (SELECT count(*) FROM nation) AS total_nations "
        "FROM region";

    // Uncorrelated subquery is cross-joined.
    auto matcher = matchHiveScan("region")
                       .nestedLoopJoin(
                           matchHiveScan("nation")
                               .singleAggregation({}, {"count(*)"})
                               .build(),
                           velox::core::JoinType::kInner)
                       .project()
                       .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Uncorrelated scalar subquery in projection with global aggregation in the
  // outer query.
  {
    auto query =
        "SELECT array_agg(r_name), "
        "   (SELECT count(*) FROM nation) AS total_nations "
        "FROM region";

    // The scalar subquery must be cross-joined AFTER the aggregation.
    auto matcher = matchHiveScan("region")
                       .singleAggregation({}, {"array_agg(r_name)"})
                       .nestedLoopJoin(
                           matchHiveScan("nation")
                               .singleAggregation({}, {"count(*)"})
                               .build(),
                           velox::core::JoinType::kInner)
                       .project()
                       .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Uncorrelated scalar subquery in projection with GROUP BY aggregation in the
  // outer query.
  {
    auto query =
        "SELECT n_regionkey, array_agg(n_name), "
        "   (SELECT count(*) FROM region) AS total_regions "
        "FROM nation "
        "GROUP BY n_regionkey";

    // The scalar subquery must be cross-joined AFTER the aggregation.
    auto matcher =
        matchHiveScan("nation")
            .singleAggregation({"n_regionkey"}, {"array_agg(n_name)"})
            .nestedLoopJoin(
                matchHiveScan("region")
                    .singleAggregation({}, {"count(*)"})
                    .build(),
                velox::core::JoinType::kInner)
            .project()
            .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // IN <subquery> in projection.
  {
    auto query =
        "SELECT n_name, "
        "   n_regionkey IN (SELECT r_regionkey FROM region WHERE r_name > 'ASIA') AS in_region "
        "FROM nation";

    // IN subquery in projection is transformed into a LEFT SEMI PROJECT join
    // with a mark column. nullAware is true for IN semantics.
    auto matcher = matchHiveScan("nation")
                       .hashJoin(
                           core::PlanMatcherBuilder()
                               .hiveScan("region", test::gt("r_name", "ASIA"))
                               .build(),
                           velox::core::JoinType::kLeftSemiProject,
                           true)
                       .project()
                       .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // NOT IN <subquery> in projection.
  {
    auto query =
        "SELECT n_name, "
        "   n_regionkey NOT IN (SELECT r_regionkey FROM region WHERE r_name > 'ASIA') AS not_in_region "
        "FROM nation";

    // NOT IN subquery in projection is transformed into a LEFT SEMI PROJECT
    // join with a mark column and NOT applied to the result. nullAware is true.
    auto matcher = matchHiveScan("nation")
                       .hashJoin(
                           core::PlanMatcherBuilder()
                               .hiveScan("region", test::gt("r_name", "ASIA"))
                               .build(),
                           velox::core::JoinType::kLeftSemiProject,
                           true)
                       .project()
                       .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Uncorrelated EXISTS in projection.
  {
    auto query =
        "SELECT r_name, "
        "   EXISTS (SELECT 1 FROM nation) AS has_nations "
        "FROM region";

    // Uncorrelated EXISTS in projection uses cross join with count check.
    auto matcher =
        matchHiveScan("region")
            .nestedLoopJoin(
                matchHiveScan("nation").limit().singleAggregation().build(),
                velox::core::JoinType::kInner)
            .project()
            .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Uncorrelated NOT EXISTS in projection.
  {
    auto query =
        "SELECT r_name, "
        "   NOT EXISTS (SELECT 1 FROM nation) AS no_nations "
        "FROM region";

    // Uncorrelated NOT EXISTS in projection uses cross join with count = 0
    // check.
    auto matcher =
        matchHiveScan("region")
            .nestedLoopJoin(
                matchHiveScan("nation").limit().singleAggregation().build(),
                velox::core::JoinType::kInner)
            .project()
            .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(SubqueryTest, correlatedIn) {
  // Find customers with at least one order.
  {
    auto query =
        "SELECT c.c_custkey, c.c_name FROM customer AS c "
        "WHERE c.c_custkey IN ("
        "  SELECT o.o_custkey FROM orders AS o "
        "  WHERE o.o_custkey = c.c_custkey)";

    // Correlated IN subquery creates a semi-join. The optimizer may flip
    // the join order and use RIGHT SEMI.
    auto matcher = matchHiveScan("orders")
                       .hashJoin(
                           matchHiveScan("customer").build(),
                           core::JoinType::kRightSemiFilter)
                       .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Find customers with no orders.
  {
    auto query =
        "SELECT c.c_custkey, c.c_name FROM customer AS c "
        "WHERE c.c_custkey NOT IN ("
        "  SELECT o.o_custkey FROM orders AS o "
        "  WHERE o.o_custkey = c.c_custkey)";

    // Correlated NOT IN subquery creates a RIGHT SEMI PROJECT join with
    // mark column, then filters and projects.
    auto matcher = matchHiveScan("orders")
                       .hashJoin(
                           matchHiveScan("customer").build(),
                           core::JoinType::kRightSemiProject,
                           /*nullAware=*/true)
                       .filter()
                       .project()
                       .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Correlated IN subquery with non-equality filter in the SELECT list produces
  // a null-aware semi-project join (mark column) with extra filter. The
  // optimizer must not flip this to a right semi-project join because Velox
  // does not support null-aware right semi project join with extra filter.
  {
    // Make t small and u large so the optimizer prefers the right-hash variant.
    testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))->setStats(100, {});
    testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()))
        ->setStats(10'000, {});

    auto query =
        "SELECT t.a IN ("
        "  SELECT u.x FROM u "
        "  WHERE u.y < t.b"
        ") FROM t";
    auto matcher =
        matchScan("t")
            .hashJoin(
                matchScan("u")
                    .hashJoin(
                        matchScan("t").build(), core::JoinType::kLeftSemiFilter)
                    .build(),
                core::JoinType::kLeftSemiProject,
                /*nullAware=*/true)
            .project()
            .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

// IN subquery where the left-side expression references multiple tables.
TEST_F(SubqueryTest, multiTableInSubquery) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  testConnector_->addTable("u", ROW({"c", "d"}, BIGINT()));
  testConnector_->addTable("v", ROW({"e", "f"}, BIGINT()));

  auto query =
      "SELECT * FROM t JOIN u ON t.a = u.c "
      "WHERE ROW(t.a, u.c) IN (SELECT ROW(e, f) FROM v)";

  // The ROW expression over two tables becomes the left key of a semi-join.
  // The inner join is computed first, then the semi-join filters rows.
  auto matcher =
      matchScan("t")
          .hashJoin(matchScan("u").build(), velox::core::JoinType::kInner)
          .project()
          .hashJoin(
              matchScan("v").project().build(),
              velox::core::JoinType::kLeftSemiFilter)
          .build();

  auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(SubqueryTest, correlatedScalar) {
  // Correlated scalar subquery with aggregation in filter.
  {
    auto query =
        "SELECT * FROM region "
        "WHERE r_regionkey = (SELECT min(n_nationkey) FROM nation WHERE n_regionkey = r_regionkey)";

    // The correlated scalar subquery is transformed into a LEFT JOIN with
    // aggregation grouped by the correlation key, then filtered.
    auto matcher =
        matchHiveScan("region")
            .hashJoin(
                matchHiveScan("nation")
                    .singleAggregation({"n_regionkey"}, {"min(n_nationkey)"})
                    .project()
                    .build(),
                velox::core::JoinType::kLeft)
            .filter("r_regionkey = min")
            .project()
            .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto query =
        "SELECT * FROM region "
        "WHERE r_regionkey = (SELECT count(*) FROM nation WHERE n_regionkey = r_regionkey)";

    // The correlated scalar subquery is transformed into a LEFT JOIN with
    // aggregation grouped by the correlation key. The count result is wrapped
    // with COALESCE to return 0 for unmatched rows (instead of NULL from
    // LEFT JOIN).
    auto matcher = matchHiveScan("region")
                       .hashJoin(
                           matchHiveScan("nation")
                               .singleAggregation({"n_regionkey"}, {"count(*)"})
                               .project()
                               .build(),
                           velox::core::JoinType::kLeft)
                       .filter("r_regionkey = coalesce(count, 0)")
                       .project()
                       .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto query =
        "SELECT * FROM region "
        "WHERE r_regionkey = (SELECT approx_distinct(n_name) FROM nation WHERE n_regionkey = r_regionkey)";

    auto matcher = matchHiveScan("region")
                       .hashJoin(
                           matchHiveScan("nation")
                               .singleAggregation(
                                   {"n_regionkey"}, {"approx_distinct(n_name)"})
                               .project()
                               .build(),
                           velox::core::JoinType::kLeft)
                       .filter("r_regionkey = coalesce(approx_distinct, 0)")
                       .project()
                       .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(SubqueryTest, correlatedScalarDedup) {
  // Two identical correlated scalar subqueries in the same WHERE clause.
  // Both compute avg(n_nationkey) GROUP BY n_regionkey with the same
  // correlation. After deduplication, only one LEFT JOIN should remain.
  auto query =
      "SELECT * FROM region "
      "WHERE r_regionkey > (SELECT avg(n_nationkey) FROM nation "
      "                     WHERE n_regionkey = r_regionkey) "
      "  AND r_regionkey < (SELECT avg(n_nationkey) FROM nation "
      "                     WHERE n_regionkey = r_regionkey)";

  auto matcher =
      matchHiveScan("region")
          .hashJoin(
              matchHiveScan("nation")
                  .singleAggregation({"n_regionkey"}, {"avg(n_nationkey)"})
                  .project()
                  .build(),
              velox::core::JoinType::kLeft)
          .filter()
          .project()
          .build();

  SCOPED_TRACE(query);
  auto plan = toSingleNodePlan(query);
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(SubqueryTest, uncorrelatedScalarDedup) {
  // Two identical uncorrelated scalar subqueries in the same WHERE clause.
  // Both compute avg(n_nationkey) with no correlation. After deduplication,
  // only one cross-joined single-row DT should remain.
  auto query =
      "SELECT * FROM region "
      "WHERE r_regionkey > (SELECT avg(n_nationkey) FROM nation) "
      "  AND r_regionkey < (SELECT avg(n_nationkey) FROM nation)";

  auto matcher = matchHiveScan("region")
                     .nestedLoopJoin(
                         matchHiveScan("nation")
                             .singleAggregation({}, {"avg(n_nationkey)"})
                             .build(),
                         velox::core::JoinType::kInner)
                     .filter()
                     .filter()
                     .project()
                     .build();

  SCOPED_TRACE(query);
  auto plan = toSingleNodePlan(query);
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(SubqueryTest, correlatedExistsDedup) {
  // Two identical correlated EXISTS subqueries in the same WHERE clause.
  // Both check EXISTS with the same correlation on n_regionkey = r_regionkey.
  // After deduplication, only one semi-join with a mark column should remain.
  // The filter ANDs the mark with itself (both EXISTS references resolve to
  // the same mark column).
  auto query =
      "SELECT * FROM region "
      "WHERE EXISTS (SELECT 1 FROM nation WHERE n_regionkey = r_regionkey) "
      "  AND EXISTS (SELECT 1 FROM nation WHERE n_regionkey = r_regionkey)";

  auto matcher = matchHiveScan("nation")
                     .hashJoin(
                         matchHiveScan("region").build(),
                         velox::core::JoinType::kRightSemiProject)
                     .filter()
                     .project()
                     .build();

  SCOPED_TRACE(query);
  auto plan = toSingleNodePlan(query);
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(SubqueryTest, correlatedInDedup) {
  // Two identical correlated IN subqueries in the same WHERE clause.
  // Both check n_regionkey IN (SELECT r_regionkey ...) with the same
  // correlation. After deduplication, only one semi-join with a mark column
  // should remain. The filter ANDs the mark with itself.
  auto query =
      "SELECT * FROM nation "
      "WHERE n_regionkey IN (SELECT r_regionkey FROM region "
      "                      WHERE r_regionkey = n_regionkey) "
      "  AND n_regionkey IN (SELECT r_regionkey FROM region "
      "                      WHERE r_regionkey = n_regionkey)";

  auto matcher =
      matchHiveScan("nation")
          .hashJoin(
              core::PlanMatcherBuilder().hiveScan("region", {}).build(),
              velox::core::JoinType::kLeftSemiProject,
              /*nullAware=*/true)
          .filter()
          .project()
          .build();

  SCOPED_TRACE(query);
  auto plan = toSingleNodePlan(query);
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(SubqueryTest, uncorrelatedExistsDedup) {
  // Two identical uncorrelated EXISTS subqueries in the same WHERE clause.
  // After deduplication, only one cross-joined existence check should remain.
  // The filter ANDs the NOT(count=0) check with itself.
  auto query =
      "SELECT * FROM region "
      "WHERE EXISTS (SELECT 1 FROM nation) "
      "  AND EXISTS (SELECT 1 FROM nation)";

  auto matcher = matchHiveScan("region")
                     .nestedLoopJoin(
                         matchHiveScan("nation")
                             .finalLimit(0, 1)
                             .singleAggregation({}, {"count(*) as c"})
                             .filter()
                             .build(),
                         velox::core::JoinType::kInner)
                     .build();

  SCOPED_TRACE(query);
  auto plan = toSingleNodePlan(query);
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(SubqueryTest, uncorrelatedInDedup) {
  // Two identical uncorrelated IN subqueries in the same WHERE clause.
  // After deduplication, only one semi-join should remain.
  auto query =
      "SELECT * FROM nation "
      "WHERE n_regionkey IN (SELECT r_regionkey FROM region) "
      "  AND n_regionkey IN (SELECT r_regionkey FROM region)";

  auto matcher = matchHiveScan("nation")
                     .hashJoin(
                         matchHiveScan("region").build(),
                         velox::core::JoinType::kLeftSemiProject,
                         /*nullAware=*/true)
                     .filter()
                     .project()
                     .build();

  SCOPED_TRACE(query);
  auto plan = toSingleNodePlan(query);
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(SubqueryTest, correlatedScalarDedupFilterAndProjection) {
  // Same correlated scalar subquery in both filter and projection.
  // The pre-pass dedup detects them globally and maps the duplicate to the
  // representative. Only one LEFT JOIN is produced.
  auto query =
      "SELECT "
      "   (SELECT avg(n_nationkey) FROM nation "
      "    WHERE n_regionkey = r_regionkey) AS avg_key "
      "FROM region "
      "WHERE r_regionkey > (SELECT avg(n_nationkey) FROM nation "
      "                     WHERE n_regionkey = r_regionkey)";

  // Single LEFT JOIN — the filter's subquery is processed first, and the
  // projection's duplicate reuses the result via cross-call dedup.
  auto matcher =
      matchHiveScan("nation")
          .singleAggregation()
          .project()
          .hashJoin(
              matchHiveScan("region").build(), velox::core::JoinType::kRight)
          .filter()
          .project()
          .build();

  SCOPED_TRACE(query);
  auto plan = toSingleNodePlan(query);
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(SubqueryTest, correlatedExistsDedupFilterAndProjection) {
  // Same correlated EXISTS subquery in both filter and projection.
  // The pre-pass dedup detects them globally and maps the duplicate to the
  // representative. Only one semi-join is produced.
  auto query =
      "SELECT n_name, "
      "   EXISTS (SELECT 1 FROM region WHERE r_regionkey = n_regionkey) AS has_region "
      "FROM nation "
      "WHERE EXISTS (SELECT 1 FROM region WHERE r_regionkey = n_regionkey)";

  // Single LEFT SEMI PROJECT join — the mark column serves both the filter
  // predicate and the projection output.
  auto matcher =
      matchHiveScan("nation")
          .hashJoin(
              core::PlanMatcherBuilder().hiveScan("region", {}).build(),
              velox::core::JoinType::kLeftSemiProject,
              false)
          .filter()
          .project()
          .build();

  SCOPED_TRACE(query);
  auto plan = toSingleNodePlan(query);
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(SubqueryTest, correlatedInDedupFilterAndProjection) {
  // Same correlated IN subquery in both filter and projection.
  // The pre-pass dedup detects them globally and maps the duplicate to the
  // representative. Only one semi-join is produced.
  auto query =
      "SELECT n_name, "
      "   n_regionkey IN (SELECT r_regionkey FROM region "
      "                   WHERE r_regionkey = n_regionkey) AS in_region "
      "FROM nation "
      "WHERE n_regionkey IN (SELECT r_regionkey FROM region "
      "                      WHERE r_regionkey = n_regionkey)";

  // Single LEFT SEMI PROJECT join — the mark column serves both the filter
  // predicate and the projection output.
  auto matcher = matchHiveScan("nation")
                     .hashJoin(
                         core::PlanMatcherBuilder()
                             .hiveScan("region", {})
                             .project()
                             .build(),
                         velox::core::JoinType::kLeftSemiProject,
                         true)
                     .filter()
                     .project()
                     .build();

  SCOPED_TRACE(query);
  auto plan = toSingleNodePlan(query);
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(SubqueryTest, dedupAcrossProjectionColumns) {
  // Identical scalar subqueries in separate projection columns.
  {
    auto query =
        "SELECT "
        "   (SELECT avg(n_nationkey) FROM nation "
        "    WHERE n_regionkey = r_regionkey) AS a, "
        "   (SELECT avg(n_nationkey) FROM nation "
        "    WHERE n_regionkey = r_regionkey) AS b "
        "FROM region";

    auto matcher =
        matchHiveScan("nation")
            .singleAggregation()
            .project()
            .hashJoin(
                matchHiveScan("region").build(), velox::core::JoinType::kRight)
            .project()
            .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Identical EXISTS subqueries in separate projection columns.
  {
    auto query =
        "SELECT "
        "   EXISTS (SELECT 1 FROM region WHERE r_regionkey = n_regionkey) AS a, "
        "   EXISTS (SELECT 1 FROM region WHERE r_regionkey = n_regionkey) AS b "
        "FROM nation";

    auto matcher =
        matchHiveScan("nation")
            .hashJoin(
                core::PlanMatcherBuilder().hiveScan("region", {}).build(),
                velox::core::JoinType::kLeftSemiProject,
                false)
            .project()
            .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Identical IN subqueries in separate projection columns.
  {
    auto query =
        "SELECT "
        "   n_regionkey IN (SELECT r_regionkey FROM region "
        "                   WHERE r_regionkey = n_regionkey) AS a, "
        "   n_regionkey IN (SELECT r_regionkey FROM region "
        "                   WHERE r_regionkey = n_regionkey) AS b "
        "FROM nation";

    auto matcher =
        matchHiveScan("nation")
            .hashJoin(
                core::PlanMatcherBuilder().hiveScan("region", {}).build(),
                velox::core::JoinType::kLeftSemiProject,
                true)
            .project()
            .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(SubqueryTest, uncorrelatedDedupFilterAndProjection) {
  // Identical uncorrelated scalar subquery in both filter and projection.
  {
    auto query =
        "SELECT (SELECT avg(n_nationkey) FROM nation) AS a "
        "FROM region "
        "WHERE r_regionkey > (SELECT avg(n_nationkey) FROM nation)";

    auto matcher =
        matchHiveScan("region")
            .nestedLoopJoin(matchHiveScan("nation").singleAggregation().build())
            .filter()
            .project()
            .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Identical uncorrelated EXISTS subquery in both filter and projection.
  {
    auto query =
        "SELECT EXISTS (SELECT 1 FROM region) AS a "
        "FROM nation "
        "WHERE EXISTS (SELECT 1 FROM region)";

    // Single cross-joined existence check. The NOT(count=0) filter is on the
    // EXISTS subquery side; the result is cross-joined with the outer table.
    auto matcher = matchHiveScan("region")
                       .finalLimit(0, 1)
                       .singleAggregation()
                       .filter()
                       .nestedLoopJoin(matchHiveScan("nation").build())
                       .project()
                       .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Identical uncorrelated IN subquery in both filter and projection.
  {
    auto query =
        "SELECT n_regionkey IN (SELECT r_regionkey FROM region) AS a "
        "FROM nation "
        "WHERE n_regionkey IN (SELECT r_regionkey FROM region)";

    auto matcher = matchHiveScan("nation")
                       .hashJoin(
                           core::PlanMatcherBuilder()
                               .hiveScan("region", {})
                               .project()
                               .build(),
                           velox::core::JoinType::kLeftSemiProject,
                           true)
                       .filter()
                       .project()
                       .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(SubqueryTest, uncorrelatedMergeAcrossColumns) {
  // Uncorrelated scalar merge across separate projection columns.
  {
    auto query =
        "SELECT "
        "   (SELECT avg(n_nationkey) FROM nation WHERE n_nationkey > 0) AS a, "
        "   (SELECT max(n_nationkey) FROM nation WHERE n_nationkey > 0) AS b "
        "FROM region";

    auto matcher =
        matchHiveScan("region")
            .nestedLoopJoin(matchHiveScan("nation").singleAggregation().build())
            .project()
            .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Uncorrelated scalar merge across filter and projection.
  {
    auto query =
        "SELECT "
        "   (SELECT avg(n_nationkey) FROM nation WHERE n_nationkey > 0) AS a "
        "FROM region "
        "WHERE r_regionkey < "
        "   (SELECT max(n_nationkey) FROM nation WHERE n_nationkey > 0)";

    auto matcher =
        matchHiveScan("region")
            .nestedLoopJoin(matchHiveScan("nation").singleAggregation().build())
            .filter()
            .project()
            .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

// TODO: Support dedup across union branches. Currently only support
// deduplication within the same union branch.
TEST_F(SubqueryTest, subqueryDedupInUnionBranches) {
  // Correlated scalar subquery.
  {
    auto query =
        "SELECT * FROM ("
        "  SELECT r_name FROM region "
        "  WHERE r_regionkey > (SELECT avg(n_nationkey) FROM nation "
        "                       WHERE n_regionkey = r_regionkey) "
        "    AND r_regionkey < (SELECT avg(n_nationkey) FROM nation "
        "                       WHERE n_regionkey = r_regionkey) "
        "  UNION ALL "
        "  SELECT r_comment FROM region "
        "  WHERE r_regionkey > (SELECT avg(n_nationkey) FROM nation "
        "                       WHERE n_regionkey = r_regionkey) "
        "    AND r_regionkey < (SELECT avg(n_nationkey) FROM nation "
        "                       WHERE n_regionkey = r_regionkey) "
        ") sub";

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);

    auto makeBranch = [&]() {
      return matchHiveScan("region")
          .hashJoin(
              matchHiveScan("nation").singleAggregation().project().build(),
              velox::core::JoinType::kLeft)
          .filter()
          .project();
    };

    auto matcher = makeBranch().localPartition(makeBranch().build()).build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Uncorrelated scalar subquery.
  {
    auto query =
        "SELECT * FROM ("
        "  SELECT n_name FROM nation "
        "  WHERE n_nationkey > (SELECT avg(n_nationkey) FROM nation) "
        "    AND n_regionkey < (SELECT avg(n_nationkey) FROM nation) "
        "  UNION ALL "
        "  SELECT n_comment FROM nation "
        "  WHERE n_nationkey > (SELECT avg(n_nationkey) FROM nation) "
        "    AND n_regionkey < (SELECT avg(n_nationkey) FROM nation) "
        ") sub";

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);

    auto makeBranch = [&]() {
      return matchHiveScan("nation")
          .nestedLoopJoin(
              matchHiveScan("nation").singleAggregation().build(),
              velox::core::JoinType::kInner)
          .filter()
          .filter()
          .project();
    };

    auto matcher = makeBranch().localPartition(makeBranch().build()).build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Uncorrelated IN subquery.
  {
    auto query =
        "SELECT * FROM ("
        "  SELECT n_name FROM nation "
        "  WHERE n_regionkey IN (SELECT r_regionkey FROM region) "
        "    AND n_regionkey IN (SELECT r_regionkey FROM region) "
        "  UNION ALL "
        "  SELECT n_comment FROM nation "
        "  WHERE n_regionkey IN (SELECT r_regionkey FROM region) "
        "    AND n_regionkey IN (SELECT r_regionkey FROM region) "
        ") sub";

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);

    // The second UNION branch has disambiguated column names, requiring a
    // Project on the build side to remap them.
    auto matcher =
        matchHiveScan("nation")
            .hashJoin(
                matchHiveScan("region").build(),
                velox::core::JoinType::kLeftSemiProject,
                /*nullAware=*/true)
            .filter()
            .project()
            .localPartition(matchHiveScan("nation")
                                .hashJoin(
                                    matchHiveScan("region").project().build(),
                                    velox::core::JoinType::kLeftSemiProject,
                                    /*nullAware=*/true)
                                .filter()
                                .project()
                                .build())
            .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Correlated IN subquery.
  {
    auto query =
        "SELECT * FROM ("
        "  SELECT n_name FROM nation "
        "  WHERE n_regionkey IN (SELECT r_regionkey FROM region "
        "                        WHERE r_regionkey = n_regionkey) "
        "    AND n_regionkey IN (SELECT r_regionkey FROM region "
        "                        WHERE r_regionkey = n_regionkey) "
        "  UNION ALL "
        "  SELECT n_comment FROM nation "
        "  WHERE n_regionkey IN (SELECT r_regionkey FROM region "
        "                        WHERE r_regionkey = n_regionkey) "
        "    AND n_regionkey IN (SELECT r_regionkey FROM region "
        "                        WHERE r_regionkey = n_regionkey) "
        ") sub";

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);

    auto matcher =
        matchHiveScan("nation")
            .hashJoin(
                core::PlanMatcherBuilder().hiveScan("region", {}).build(),
                velox::core::JoinType::kLeftSemiProject,
                /*nullAware=*/true)
            .filter()
            .project()
            .localPartition(matchHiveScan("nation")
                                .hashJoin(
                                    core::PlanMatcherBuilder()
                                        .hiveScan("region", {})
                                        .project()
                                        .build(),
                                    velox::core::JoinType::kLeftSemiProject,
                                    /*nullAware=*/true)
                                .filter()
                                .project()
                                .build())
            .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Uncorrelated EXISTS subquery.
  {
    auto query =
        "SELECT * FROM ("
        "  SELECT n_name FROM nation "
        "  WHERE EXISTS (SELECT 1 FROM region) "
        "    AND EXISTS (SELECT 1 FROM region) "
        "  UNION ALL "
        "  SELECT n_comment FROM nation "
        "  WHERE EXISTS (SELECT 1 FROM region) "
        "    AND EXISTS (SELECT 1 FROM region) "
        ") sub";

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);

    // The existence check is entirely in the build side (filter on count != 0).
    // The first branch uses original column names (no trailing project),
    // while the second branch needs a project for column renaming.
    auto buildSide = [&]() {
      return matchHiveScan("region")
          .finalLimit(0, 1)
          .singleAggregation({}, {"count(*) as c"})
          .filter()
          .build();
    };

    auto matcher =
        matchHiveScan("nation")
            .nestedLoopJoin(buildSide(), velox::core::JoinType::kInner)
            .localPartition(
                matchHiveScan("nation")
                    .nestedLoopJoin(buildSide(), velox::core::JoinType::kInner)
                    .project()
                    .build())
            .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Correlated EXISTS subquery.
  {
    auto query =
        "SELECT * FROM ("
        "  SELECT n_name FROM nation "
        "  WHERE EXISTS (SELECT 1 FROM region "
        "                WHERE r_regionkey = n_regionkey) "
        "    AND EXISTS (SELECT 1 FROM region "
        "                WHERE r_regionkey = n_regionkey) "
        "  UNION ALL "
        "  SELECT n_comment FROM nation "
        "  WHERE EXISTS (SELECT 1 FROM region "
        "                WHERE r_regionkey = n_regionkey) "
        "    AND EXISTS (SELECT 1 FROM region "
        "                WHERE r_regionkey = n_regionkey) "
        ") sub";

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);

    // Both branches have the same structure — the correlated semi-join uses
    // the region column directly, so no renaming project is needed.
    auto makeBranch = [&]() {
      return matchHiveScan("nation")
          .hashJoin(
              core::PlanMatcherBuilder().hiveScan("region", {}).build(),
              velox::core::JoinType::kLeftSemiProject,
              false)
          .filter()
          .project();
    };

    auto matcher = makeBranch().localPartition(makeBranch().build()).build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(SubqueryTest, correlatedScalarMerge) {
  // Two correlated scalar subqueries with the same FROM table and correlation
  // but different aggregates (avg vs max). After merging, a single LEFT JOIN
  // with both aggregates should be produced instead of two separate joins.
  {
    auto query =
        "SELECT * FROM region "
        "WHERE r_regionkey > (SELECT avg(n_nationkey) FROM nation "
        "                     WHERE n_regionkey = r_regionkey) "
        "  AND r_regionkey < (SELECT max(n_nationkey) FROM nation "
        "                     WHERE n_regionkey = r_regionkey)";

    auto matcher = matchHiveScan("region")
                       .hashJoin(
                           matchHiveScan("nation")
                               .singleAggregation(
                                   {"n_regionkey"},
                                   {"avg(n_nationkey)", "max(n_nationkey)"})
                               .project()
                               .build(),
                           velox::core::JoinType::kLeft)
                       .filter()
                       .project()
                       .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Two mergeable correlated scalar subqueries in the same projection
  // expression. Both are extracted in the same processSubqueries call, so
  // within-batch merge applies.
  {
    auto query =
        "SELECT r_name, "
        "   (SELECT avg(n_nationkey) FROM nation "
        "    WHERE n_regionkey = r_regionkey) "
        " + (SELECT max(n_nationkey) FROM nation "
        "    WHERE n_regionkey = r_regionkey) AS combined "
        "FROM region";

    // Merged into a single LEFT JOIN with both avg and max aggregates.
    // The optimizer may swap join sides (RIGHT instead of LEFT).
    auto matcher =
        matchHiveScan("nation")
            .singleAggregation()
            .project()
            .hashJoin(
                matchHiveScan("region").build(), velox::core::JoinType::kRight)
            .project()
            .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Two mergeable correlated scalar subqueries in separate projection
  // expressions. Both are extracted in the same batched processSubqueries call,
  // so within-batch merge applies.
  {
    auto query =
        "SELECT r_name, "
        "   (SELECT avg(n_nationkey) FROM nation "
        "    WHERE n_regionkey = r_regionkey) AS avg_key, "
        "   (SELECT max(n_nationkey) FROM nation "
        "    WHERE n_regionkey = r_regionkey) AS max_key "
        "FROM region";

    // Merged into a single LEFT JOIN with both avg and max aggregates.
    // The optimizer may swap join sides (RIGHT instead of LEFT).
    auto matcher =
        matchHiveScan("nation")
            .singleAggregation()
            .project()
            .hashJoin(
                matchHiveScan("region").build(), velox::core::JoinType::kRight)
            .project()
            .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Mergeable subqueries split across filter and projection. The pre-pass
  // merge map detects them globally, so they are merged into a single LEFT JOIN
  // with both aggregates despite being in different processSubqueries calls.
  {
    auto query =
        "SELECT "
        "   (SELECT avg(n_nationkey) FROM nation "
        "    WHERE n_regionkey = r_regionkey) AS avg_key "
        "FROM region "
        "WHERE r_regionkey < (SELECT max(n_nationkey) FROM nation "
        "                     WHERE n_regionkey = r_regionkey)";

    // Single LEFT JOIN with both avg and max aggregates. The filter subquery
    // is processed first; the projection subquery finds it already processed
    // via cross-call dedup and reuses the merged result.
    auto matcher =
        matchHiveScan("nation")
            .singleAggregation()
            .project()
            .hashJoin(
                matchHiveScan("region").build(), velox::core::JoinType::kRight)
            .filter()
            .project()
            .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Two uncorrelated scalar subqueries with same FROM and filter but different
  // aggregates in WHERE. Merged into a single cross-joined aggregation.
  {
    auto query =
        "SELECT * FROM region "
        "WHERE r_regionkey > (SELECT avg(n_nationkey) FROM nation "
        "                     WHERE n_nationkey > 0) "
        "  AND r_regionkey < (SELECT max(n_nationkey) FROM nation "
        "                     WHERE n_nationkey > 0)";

    // The uncorrelated merged subquery produces a single Aggregation with both
    // avg and max, cross-joined with the outer table.
    auto matcher =
        matchHiveScan("region")
            .nestedLoopJoin(matchHiveScan("nation").singleAggregation().build())
            .filter()
            .filter()
            .project()
            .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Two uncorrelated scalar subqueries with same FROM and filter but different
  // aggregates in the same projection expression. Merged into a single
  // cross-joined aggregation.
  {
    auto query =
        "SELECT r_name, "
        "   (SELECT avg(n_nationkey) FROM nation WHERE n_nationkey > 0) "
        " + (SELECT max(n_nationkey) FROM nation WHERE n_nationkey > 0) "
        "   AS combined "
        "FROM region";

    auto matcher =
        matchHiveScan("region")
            .nestedLoopJoin(matchHiveScan("nation").singleAggregation().build())
            .project()
            .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Merge with non-trivial aggregate inputs: arithmetic expression in
  // argument, ORDER BY key, and FILTER clause. Verifies that nested
  // InputReferenceExpr names are remapped recursively, not just at the leaf
  // level.
  {
    auto query =
        "SELECT * FROM region "
        "WHERE r_regionkey > "
        "   (SELECT sum(n_nationkey + 1 ORDER BY n_nationkey + 1) "
        "    FILTER (WHERE n_nationkey > 0) "
        "    FROM nation WHERE n_regionkey = r_regionkey) "
        "  AND r_regionkey < "
        "   (SELECT array_agg(n_nationkey + 1 ORDER BY n_nationkey + 1) "
        "    FILTER (WHERE n_nationkey > 0) "
        "    FROM nation WHERE n_regionkey = r_regionkey)[1]";

    // Single LEFT JOIN with both sum and array_agg merged into one
    // aggregation. The arithmetic (n_nationkey + 1), filter (n_nationkey > 0),
    // and order-by key are all remapped correctly across subquery plans.
    // The Project below the aggregation computes the arithmetic and filter
    // expressions.
    auto matcher = matchHiveScan("region")
                       .hashJoin(
                           matchHiveScan("nation")
                               .project()
                               .singleAggregation()
                               .project()
                               .build(),
                           velox::core::JoinType::kLeft)
                       .filter()
                       .project()
                       .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(SubqueryTest, correlatedProject) {
  auto matchAggNation = [&]() {
    return matchHiveScan("nation").singleAggregation().project().build();
  };

  // Correlated scalar subquery in projection with COUNT aggregation.
  {
    auto query =
        "SELECT r_name, "
        "   (SELECT count(*) FROM nation WHERE n_regionkey = r_regionkey) AS cnt "
        "FROM region";

    // The correlated scalar subquery is transformed into a LEFT JOIN with
    // aggregation grouped by the correlation key.
    auto matcher = matchHiveScan("region")
                       .hashJoin(matchAggNation(), velox::core::JoinType::kLeft)
                       .project()
                       .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Correlated scalar subquery in projection with SUM aggregation.
  {
    auto query =
        "SELECT r_name, "
        "(SELECT sum(n_nationkey) FROM nation WHERE n_regionkey = r_regionkey) AS total "
        "FROM region";

    auto matcher = matchHiveScan("region")
                       .hashJoin(matchAggNation(), velox::core::JoinType::kLeft)
                       .project()
                       .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Multiple scalar subqueries in projection. With batched subquery
  // processing, count(*) and max(n_nationkey) are merged into a single
  // LEFT JOIN with both aggregates.
  {
    auto query =
        "SELECT r_name, "
        "   (SELECT count(*) FROM nation WHERE n_regionkey = r_regionkey) AS cnt, "
        "   (SELECT max(n_nationkey) FROM nation WHERE n_regionkey = r_regionkey) AS max_key "
        "FROM region";

    // The optimizer may swap join sides (RIGHT instead of LEFT).
    auto matcher =
        matchHiveScan("nation")
            .singleAggregation()
            .project()
            .hashJoin(
                matchHiveScan("region").build(), velox::core::JoinType::kRight)
            .project()
            .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // EXISTS <subquery> in projection.
  {
    auto query =
        "SELECT n_name, "
        "   EXISTS (SELECT 1 FROM region WHERE r_regionkey = n_regionkey) AS has_region "
        "FROM nation";

    // EXISTS subquery in projection is transformed into a LEFT SEMI PROJECT
    // join with a mark column. nullAware is false for EXISTS semantics.
    auto matcher =
        matchHiveScan("nation")
            .hashJoin(
                core::PlanMatcherBuilder().hiveScan("region", {}).build(),
                velox::core::JoinType::kLeftSemiProject,
                false)
            .project()
            .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Correlated IN <subquery> in projection.
  {
    auto query =
        "SELECT n_name, "
        "   n_regionkey IN (SELECT r_regionkey FROM region WHERE r_regionkey = n_regionkey) AS in_region "
        "FROM nation";

    // Correlated IN subquery in projection is transformed into a LEFT SEMI
    // PROJECT join with a mark column.
    auto matcher =
        matchHiveScan("nation")
            .hashJoin(
                core::PlanMatcherBuilder().hiveScan("region", {}).build(),
                velox::core::JoinType::kLeftSemiProject,
                true)
            .project()
            .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Correlated NOT IN <subquery> in projection.
  {
    auto query =
        "SELECT n_name, "
        "   n_regionkey NOT IN (SELECT r_regionkey FROM region WHERE r_regionkey = n_regionkey) AS not_in_region "
        "FROM nation";

    // Correlated NOT IN subquery in projection is transformed into a LEFT SEMI
    // PROJECT join with a mark column and NOT applied.
    auto matcher =
        matchHiveScan("nation")
            .hashJoin(
                core::PlanMatcherBuilder().hiveScan("region", {}).build(),
                velox::core::JoinType::kLeftSemiProject,
                true)
            .project()
            .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(SubqueryTest, uncorrelatedExists) {
  // Uncorrelated EXISTS: returns all rows if subquery has rows.
  {
    auto query = "SELECT * FROM region WHERE EXISTS (SELECT 1 FROM nation)";

    // EXISTS uses NOT(count = 0) to check if any rows exist.
    auto matcher = matchHiveScan("region")
                       .nestedLoopJoin(
                           matchHiveScan("nation")
                               .finalLimit(0, 1)
                               .singleAggregation({}, {"count(*) as c"})
                               .filter("not(eq(c, 0))")
                               .build(),
                           velox::core::JoinType::kInner)
                       .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Uncorrelated NOT EXISTS: returns all rows if subquery has no rows.
  {
    auto query = "SELECT * FROM region WHERE NOT EXISTS (SELECT 1 FROM nation)";

    // NOT EXISTS uses NOT(NOT(count = 0)) to check if no rows exist.
    auto matcher = matchHiveScan("region")
                       .nestedLoopJoin(
                           matchHiveScan("nation")
                               .finalLimit(0, 1)
                               .singleAggregation({}, {"count(*) as c"})
                               .filter("not(not(eq(c, 0)))")
                               .build(),
                           velox::core::JoinType::kInner)
                       .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(SubqueryTest, correlatedNotExists) {
  // NOT EXISTS with equality correlation in filter.
  {
    auto query =
        "SELECT * FROM nation WHERE "
        "NOT EXISTS (SELECT * FROM region WHERE r_regionkey = n_regionkey)";

    auto matcher =
        matchHiveScan("nation")
            .hashJoin(
                core::PlanMatcherBuilder().hiveScan("region", {}).build(),
                velox::core::JoinType::kAnti)
            .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // NOT EXISTS with non-equality correlation in filter.
  {
    auto query =
        "SELECT * FROM nation WHERE "
        "NOT EXISTS (SELECT 1 FROM region WHERE r_regionkey > n_regionkey)";

    // NOT EXISTS with non-equality correlation uses nested loop join with
    // LEFT SEMI PROJECT, then filters and negates the result.
    auto matcher = matchHiveScan("nation")
                       .nestedLoopJoin(
                           matchHiveScan("region").build(),
                           velox::core::JoinType::kLeftSemiProject)
                       .filter()
                       .project()
                       .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // NOT EXISTS in projection.
  {
    auto query =
        "SELECT n_name, "
        "   NOT EXISTS (SELECT 1 FROM region WHERE r_regionkey = n_regionkey) AS no_region "
        "FROM nation";

    // NOT EXISTS subquery in projection is transformed into a LEFT SEMI PROJECT
    // join with a mark column and NOT applied.
    auto matcher =
        matchHiveScan("nation")
            .hashJoin(
                core::PlanMatcherBuilder().hiveScan("region", {}).build(),
                velox::core::JoinType::kLeftSemiProject,
                false)
            .project()
            .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(SubqueryTest, unnest) {
  testConnector_->addTable("t", ROW({"a"}, ARRAY(BIGINT())));
  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()));
  testConnector_->addTable("v", ROW({"n", "m"}, BIGINT()));

  {
    auto query =
        "select * from t, unnest(a) as v(n) where n in (SELECT x FROM u) ";

    auto logicalPlan = parseSelect(query, kTestConnectorId);
    auto matcher =
        matchScan("t").unnest().hashJoin(matchScan("u").build()).build();

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto query =
        "select * from t, unnest(a) as v(n) where EXISTS (SELECT * FROM u WHERE x = n) ";

    auto logicalPlan = parseSelect(query, kTestConnectorId);
    auto matcher =
        matchScan("t").unnest().hashJoin(matchScan("u").build()).build();

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto query =
        "select (SELECT sum(y) FROM u WHERE x = n) from t, unnest(a) as v(n)";

    auto logicalPlan = parseSelect(query, kTestConnectorId);
    auto matcher = matchScan("t")
                       .unnest()
                       .hashJoin(
                           matchScan("u").aggregation().project().build(),
                           core::JoinType::kLeft)
                       .build();

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(SubqueryTest, enforceSingleRow) {
  auto query =
      "SELECT * FROM region "
      "WHERE r_regionkey > (SELECT n_regionkey FROM nation)";
  auto logicalPlan = parseSelect(query);

  {
    auto matcher =
        matchHiveScan("region")
            .nestedLoopJoin(matchHiveScan("nation").enforceSingleRow().build())
            .filter()
            .project()
            .build();

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);

    VELOX_ASSERT_THROW(runVelox(plan), "Expected single row of input.");
  }

  {
    auto matcher =
        matchHiveScan("region")
            .nestedLoopJoin(
                matchHiveScan("nation").enforceSingleRow().broadcast().build())
            .filter()
            .project()
            .gather()
            .build();

    auto distributedPlan = planVelox(logicalPlan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, matcher);
  }
}

TEST_F(SubqueryTest, uncorrelatedGroupingKey) {
  auto query =
      "SELECT r_name, (SELECT count(*) FROM nation) FROM region GROUP BY 1, 2";
  SCOPED_TRACE(query);

  auto matcher =
      matchHiveScan("region")
          .nestedLoopJoin(matchHiveScan("nation").aggregation().build())
          .aggregation()
          .build();

  auto plan = toSingleNodePlan(query);
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(SubqueryTest, correlatedGroupingKey) {
  auto query =
      "SELECT r_name, (SELECT count(*) FROM nation WHERE n_regionkey = r_regionkey) + 1 "
      "FROM region GROUP BY 1, 2";
  SCOPED_TRACE(query);

  auto matcher =
      matchHiveScan("region")
          .hashJoin(
              matchHiveScan("nation").aggregation().project().build(),
              core::JoinType::kLeft)
          .project()
          .aggregation()
          .build();

  auto plan = toSingleNodePlan(query);
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(SubqueryTest, nonEquiCorrelatedScalar) {
  // Correlated scalar subquery with non-equi correlation condition.
  {
    auto query =
        "SELECT * FROM region "
        "WHERE (SELECT count(*) FROM nation WHERE n_regionkey < r_regionkey) > 3";
    SCOPED_TRACE(query);

    auto logicalPlan = parseSelect(query);

    {
      auto matcher = matchHiveScan("region")
                         .assignUniqueId("unique_id")
                         .nestedLoopJoin(
                             matchHiveScan("nation")
                                 .project({"true as marker", "n_regionkey"})
                                 .build(),
                             velox::core::JoinType::kLeft)
                         .streamingAggregation(
                             {"unique_id"},
                             {
                                 "count(*) filter (where marker) as cnt",
                                 "arbitrary(r_regionkey)",
                                 "arbitrary(r_name)",
                                 "arbitrary(r_comment)",
                             })
                         .filter("cnt > 3")
                         .project()
                         .build();

      auto plan = toSingleNodePlan(logicalPlan);
      AXIOM_ASSERT_PLAN(plan, matcher);
    }

    {
      auto matcher = matchHiveScan("region")
                         .assignUniqueId("unique_id")
                         .nestedLoopJoin(
                             matchHiveScan("nation")
                                 .project({"true as marker", "n_regionkey"})
                                 .broadcast()
                                 .build(),
                             velox::core::JoinType::kLeft)
                         .streamingAggregation(
                             {"unique_id"},
                             {
                                 "count(*) filter (where marker) as cnt",
                                 "arbitrary(r_regionkey)",
                                 "arbitrary(r_name)",
                                 "arbitrary(r_comment)",
                             })
                         .filter("cnt > 3")
                         .project()
                         .gather()
                         .build();

      auto distributedPlan = planVelox(logicalPlan);
      AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, matcher);
    }
  }

  // Equi AND non-equi correlation clauses.
  {
    auto query =
        "SELECT r_name FROM region "
        "WHERE (SELECT count(*) FROM nation "
        "         WHERE n_regionkey = r_regionkey "
        "               AND length(n_name) < length(r_name)) > 3";
    SCOPED_TRACE(query);

    auto logicalPlan = parseSelect(query);

    {
      auto matcher =
          matchHiveScan("region")
              .assignUniqueId("unique_id")
              .hashJoin(
                  matchHiveScan("nation")
                      .project({"true as marker", "n_name", "n_regionkey"})
                      .build(),
                  velox::core::JoinType::kLeft)
              .streamingAggregation(
                  {"unique_id"},
                  {
                      "count(*) filter (where marker) as cnt",
                      "arbitrary(r_regionkey)",
                      "arbitrary(r_name)",
                  })
              .filter("cnt > 3")
              .project()
              .build();

      auto plan = toSingleNodePlan(logicalPlan);
      AXIOM_ASSERT_PLAN(plan, matcher);
    }
  }
}

// Correlated scalar subquery over a source that contains aggregation (e.g., a
// CTE with GROUP BY). The subquery's own aggregation (COUNT(*)) is stacked on
// top of the inner aggregation, producing two AggregateNode levels. The
// correlation predicates include a non-equi condition (BETWEEN).
TEST_F(SubqueryTest, nonEquiCorrelatedScalarWithNestedAggregation) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  testConnector_->addTable("u", ROW({"c", "d"}, BIGINT()));

  // The inner derived table (SELECT c, COUNT(*) ...) produces an aggregation.
  // The outer COUNT(*) adds a second aggregation level. The correlation
  // references (t.a = x.c AND x.cnt BETWEEN 1 AND t.a) include both equi and
  // non-equi conditions.
  auto query =
      "SELECT * FROM t "
      "WHERE (SELECT COUNT(*) "
      "       FROM (SELECT c, COUNT(*) AS cnt FROM u GROUP BY c) x "
      "       WHERE t.a = x.c AND x.cnt BETWEEN 1 AND t.a) > 0";

  auto matcher =
      matchScan("t")
          .assignUniqueId("unique_id")
          .hashJoin(
              matchScan("u")
                  .singleAggregation({"c"}, {"count(*) as inner_cnt"})
                  .project({"true as marker", "inner_cnt", "c"})
                  .build(),
              velox::core::JoinType::kLeft)
          .streamingAggregation(
              {"unique_id"},
              {
                  "count(*) filter (where marker) as cnt",
                  "arbitrary(a)",
                  "arbitrary(b)",
              })
          .filter("cnt > 0")
          .project()
          .build();

  auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(SubqueryTest, nonEquiCorrelatedProject) {
  // Correlated scalar subquery with non-equi correlation condition.
  {
    auto query =
        "SELECT length(r_name), (SELECT count(*) FROM nation WHERE n_regionkey < r_regionkey) FROM region";
    SCOPED_TRACE(query);

    auto logicalPlan = parseSelect(query);

    {
      auto matcher = matchHiveScan("region")
                         .assignUniqueId("unique_id")
                         .nestedLoopJoin(
                             matchHiveScan("nation")
                                 .project({"true as marker", "n_regionkey"})
                                 .build(),
                             velox::core::JoinType::kLeft)
                         .streamingAggregation(
                             {"unique_id"},
                             {
                                 "count(*) filter (where marker) as cnt",
                                 "arbitrary(r_regionkey)",
                                 "arbitrary(r_name) as r_name",
                             })
                         .project({"length(r_name)", "cnt"})
                         .project()
                         .build();

      auto plan = toSingleNodePlan(logicalPlan);
      AXIOM_ASSERT_PLAN(plan, matcher);
    }

    {
      auto matcher = matchHiveScan("region")
                         .assignUniqueId("unique_id")
                         .nestedLoopJoin(
                             matchHiveScan("nation")
                                 .project({"true as marker", "n_regionkey"})
                                 .broadcast()
                                 .build(),
                             velox::core::JoinType::kLeft)
                         .streamingAggregation(
                             {"unique_id"},
                             {
                                 "count(*) filter (where marker) as cnt",
                                 "arbitrary(r_regionkey)",
                                 "arbitrary(r_name) as r_name",
                             })
                         .project({"length(r_name)", "cnt"})
                         .gather()
                         .project()
                         .build();

      auto distributedPlan = planVelox(logicalPlan);
      AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, matcher);
    }
  }
}

// Correlated scalar subqueries without aggregation.
// These require EnforceDistinct to validate single-row semantics.
TEST_F(SubqueryTest, correlatedScalarWithoutAggregation) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  testConnector_->addTable("u", ROW({"c", "d"}, BIGINT()));

  // Equi-correlation: d = b.
  {
    auto query = "SELECT * FROM t WHERE a > (SELECT c FROM u WHERE d = b)";
    SCOPED_TRACE(query);

    auto matcher =
        matchScan("t")
            .assignUniqueId("unique_id")
            .hashJoin(matchScan("u").build(), velox::core::JoinType::kLeft)
            .enforceDistinct({"unique_id"})
            .filter("a > c")
            .project()
            .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto query = "SELECT a + (SELECT c FROM u WHERE d = b) FROM t";
    SCOPED_TRACE(query);

    auto matcher =
        matchScan("t")
            .assignUniqueId("unique_id")
            .hashJoin(matchScan("u").build(), velox::core::JoinType::kLeft)
            .enforceDistinct({"unique_id"})
            .project({"a + c"})
            .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Non-equi correlation: d < b.
  {
    auto query = "SELECT * FROM t WHERE a > (SELECT c + d FROM u WHERE d < b)";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .assignUniqueId("unique_id")
                       .nestedLoopJoin(
                           matchScan("u").project({"c + d as cd", "d"}).build(),
                           velox::core::JoinType::kLeft)
                       .enforceDistinct({"unique_id"})
                       .filter("a > cd")
                       .project()
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto query = "SELECT a + (SELECT c + d FROM u WHERE d < b) FROM t";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .assignUniqueId("unique_id")
                       .nestedLoopJoin(
                           matchScan("u").project({"c + d as cd", "d"}).build(),
                           velox::core::JoinType::kLeft)
                       .enforceDistinct({"unique_id"})
                       .project({"a + cd"})
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(SubqueryTest, innerJoinOnSubquery) {
  // Subqueries in inner join ON clauses are processed as cross join + filter,
  // reusing the WHERE clause subquery infrastructure.
  const std::string baseJoin =
      "SELECT * FROM nation n JOIN region r ON r.r_regionkey = n.n_regionkey";

  // Uncorrelated scalar subquery in ON clause.
  {
    auto query = baseJoin +
        " AND n.n_nationkey = (SELECT min(s_nationkey) FROM supplier)";
    SCOPED_TRACE(query);

    auto matcher =
        matchHiveScan("nation")
            .hashJoin(
                matchHiveScan("supplier")
                    .singleAggregation({}, {"min(s_nationkey)"})
                    .build(),
                velox::core::JoinType::kInner)
            .hashJoin(
                matchHiveScan("region").build(), velox::core::JoinType::kInner)
            .build();

    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // IN subquery in ON clause.
  {
    auto query =
        baseJoin + " AND n.n_nationkey IN (SELECT s_nationkey FROM supplier)";
    SCOPED_TRACE(query);

    auto matcher =
        matchHiveScan("supplier")
            .hashJoin(
                matchHiveScan("nation").build(),
                velox::core::JoinType::kRightSemiFilter)
            .hashJoin(
                matchHiveScan("region").build(), velox::core::JoinType::kInner)
            .build();

    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // NOT IN subquery in ON clause.
  {
    auto query = baseJoin +
        " AND n.n_nationkey NOT IN (SELECT s_nationkey FROM supplier)";
    SCOPED_TRACE(query);

    auto matcher =
        matchHiveScan("supplier")
            .hashJoin(
                matchHiveScan("nation").build(),
                velox::core::JoinType::kRightSemiProject,
                /*nullAware=*/true)
            .filter()
            .hashJoin(
                matchHiveScan("region").build(), velox::core::JoinType::kInner)
            .build();

    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // EXISTS subquery in ON clause.
  {
    auto query = baseJoin +
        " AND EXISTS (SELECT 1 FROM supplier s"
        " WHERE s.s_nationkey = n.n_nationkey)";
    SCOPED_TRACE(query);

    auto matcher =
        matchHiveScan("supplier")
            .hashJoin(
                matchHiveScan("nation").build(),
                velox::core::JoinType::kRightSemiFilter)
            .hashJoin(
                matchHiveScan("region").build(), velox::core::JoinType::kInner)
            .build();

    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // NOT EXISTS subquery in ON clause.
  {
    auto query = baseJoin +
        " AND NOT EXISTS (SELECT 1 FROM supplier s"
        " WHERE s.s_nationkey = n.n_nationkey)";
    SCOPED_TRACE(query);

    auto matcher =
        matchHiveScan("supplier")
            .hashJoin(
                matchHiveScan("nation").build(),
                velox::core::JoinType::kRightSemiProject)
            .filter()
            .hashJoin(
                matchHiveScan("region").build(), velox::core::JoinType::kInner)
            .build();

    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Correlated scalar
  {
    auto query = baseJoin +
        " AND n.n_nationkey > "
        "(SELECT count(*) FROM supplier s WHERE s.s_nationkey = n.n_nationkey)";
    SCOPED_TRACE(query);

    auto matcher =
        matchHiveScan("nation")
            .hashJoin(
                matchHiveScan("supplier")
                    .singleAggregation({"s_nationkey"}, {"count(*) as cnt"})
                    .project()
                    .build(),
                velox::core::JoinType::kLeft)
            .filter("n_nationkey > coalesce(cnt, 0)")
            .hashJoin(
                matchHiveScan("region").build(), velox::core::JoinType::kInner)
            .build();

    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Uncorrelated scalar subquery referencing both sides of the join.
  {
    auto query = baseJoin +
        " AND n.n_nationkey + r.r_regionkey > "
        "(SELECT min(s_nationkey) FROM supplier)";
    SCOPED_TRACE(query);

    auto matcher =
        matchHiveScan("nation")
            .hashJoin(
                matchHiveScan("region").build(), velox::core::JoinType::kInner)
            .nestedLoopJoin(
                matchHiveScan("supplier")
                    .singleAggregation({}, {"min(s_nationkey)"})
                    .build(),
                velox::core::JoinType::kInner)
            .filter()
            .project()
            .build();

    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(SubqueryTest, leftJoinOnSubquery) {
  // Subqueries in LEFT JOIN ON clauses are supported when they reference only
  // the right (null-supplying) side. See Subqueries.md.
  const std::string baseJoin =
      "SELECT * FROM nation n LEFT JOIN region r ON r.r_regionkey = n.n_regionkey";

  // Uncorrelated IN subquery in LEFT JOIN ON clause.
  {
    auto query = baseJoin + " AND r.r_name IN (SELECT s_name FROM supplier)";
    SCOPED_TRACE(query);

    auto matcher = matchHiveScan("nation")
                       .hashJoin(
                           matchHiveScan("supplier")
                               .hashJoin(
                                   matchHiveScan("region").build(),
                                   core::JoinType::kRightSemiFilter)
                               .build(),
                           core::JoinType::kLeft)
                       .build();

    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Uncorrelated scalar subquery in LEFT JOIN ON clause.
  {
    auto query = baseJoin +
        " AND r.r_regionkey > (SELECT min(s_nationkey) FROM supplier)";
    SCOPED_TRACE(query);

    auto matcher =
        matchHiveScan("nation")
            .hashJoin(
                matchHiveScan("region")
                    .nestedLoopJoin(
                        matchHiveScan("supplier")
                            .singleAggregation({}, {"min(s_nationkey) as m"})
                            .build(),
                        core::JoinType::kInner)
                    .filter("r_regionkey > m")
                    .project()
                    .build(),
                core::JoinType::kLeft)
            .build();

    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // NOT IN subquery in LEFT JOIN ON clause.
  {
    auto query =
        baseJoin + " AND r.r_name NOT IN (SELECT s_name FROM supplier)";
    SCOPED_TRACE(query);

    // The optimizer uses RIGHT SEMI PROJECT with null-aware mark column,
    // then filters NOT(mark) to keep non-matching rows.
    auto matcher = matchHiveScan("nation")
                       .hashJoin(
                           matchHiveScan("supplier")
                               .hashJoin(
                                   matchHiveScan("region").build(),
                                   core::JoinType::kRightSemiProject,
                                   /*nullAware=*/true)
                               .filter()
                               .project()
                               .build(),
                           core::JoinType::kLeft)
                       .build();

    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // All conjuncts contain subqueries (no non-subquery condition remains).
  {
    auto query =
        "SELECT * FROM nation n LEFT JOIN region r "
        "ON r.r_name IN (SELECT s_name FROM supplier)";
    SCOPED_TRACE(query);

    auto matcher = matchHiveScan("nation")
                       .nestedLoopJoin(
                           matchHiveScan("supplier")
                               .hashJoin(
                                   matchHiveScan("region").build(),
                                   core::JoinType::kRightSemiFilter)
                               .build(),
                           core::JoinType::kLeft)
                       .build();

    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Correlated EXISTS referencing the right (null-supplying) side.
  {
    auto query = baseJoin +
        " AND EXISTS (SELECT 1 FROM supplier s"
        " WHERE s.s_nationkey = r.r_regionkey)";
    SCOPED_TRACE(query);

    auto matcher = matchHiveScan("nation")
                       .hashJoin(
                           matchHiveScan("supplier")
                               .hashJoin(
                                   matchHiveScan("region").build(),
                                   core::JoinType::kRightSemiFilter)
                               .build(),
                           core::JoinType::kLeft)
                       .build();

    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Correlated NOT EXISTS referencing the right (null-supplying) side.
  {
    auto query = baseJoin +
        " AND NOT EXISTS (SELECT 1 FROM supplier s"
        " WHERE s.s_nationkey = r.r_regionkey)";
    SCOPED_TRACE(query);

    auto matcher = matchHiveScan("nation")
                       .hashJoin(
                           matchHiveScan("supplier")
                               .hashJoin(
                                   matchHiveScan("region").build(),
                                   core::JoinType::kRightSemiProject)
                               .filter()
                               .project()
                               .build(),
                           core::JoinType::kLeft)
                       .build();

    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Correlated scalar subquery referencing the right (null-supplying) side.
  {
    auto query = baseJoin +
        " AND r.r_regionkey > "
        "(SELECT count(*) FROM supplier s WHERE s.s_nationkey = r.r_regionkey)";
    SCOPED_TRACE(query);

    // The correlated subquery is decorrelated: supplier is semi-joined with
    // a fresh region scan, then aggregated and LEFT JOINed with the outer
    // region scan.
    auto matcher =
        matchHiveScan("nation")
            .hashJoin(
                matchHiveScan("region")
                    .hashJoin(
                        matchHiveScan("supplier")
                            .hashJoin(
                                matchHiveScan("region").build(),
                                core::JoinType::kLeftSemiFilter)
                            .singleAggregation({"s_nationkey"}, {"count(*)"})
                            .project()
                            .build(),
                        core::JoinType::kLeft)
                    .filter()
                    .project()
                    .build(),
                core::JoinType::kLeft)
            .build();

    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

// Non-equi LEFT JOIN where the left side contains a scalar subquery. The
// scalar subquery cross-join adds an extra table to the left side's DT.
TEST_F(SubqueryTest, nonEquiLeftJoinWithScalarSubquery) {
  testConnector_->addTable("t", ROW({"a", "b"}, {BIGINT(), BIGINT()}));
  testConnector_->addTable("u", ROW({"c", "d"}, {BIGINT(), BIGINT()}));
  testConnector_->addTable("v", ROW({"e"}, {BIGINT()}));

  auto query =
      "WITH base AS ("
      "  SELECT a, b, (SELECT e FROM v) x FROM t"
      ") "
      "SELECT base.*, u.d "
      "FROM base LEFT JOIN u ON u.c > base.b";
  SCOPED_TRACE(query);

  auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));

  // The scalar subquery is cross-joined, and the non-equi LEFT JOIN uses a
  // nested-loop join.
  auto matcher =
      matchScan("t")
          .nestedLoopJoin(matchScan("u").build(), velox::core::JoinType::kLeft)
          .nestedLoopJoin(matchScan("v").enforceSingleRow().build())
          .project()
          .build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(SubqueryTest, rightJoinOnSubquery) {
  // RIGHT JOIN is normalized to LEFT JOIN. Subqueries referencing the
  // null-supplying side (left in SQL, right after normalization) are supported.
  // A final Project reorders columns to match the original SQL column order.
  const std::string baseJoin =
      "SELECT * FROM region r RIGHT JOIN nation n ON r.r_regionkey = n.n_regionkey";

  // IN subquery on the null-supplying side.
  {
    auto query = baseJoin + " AND r.r_name IN (SELECT s_name FROM supplier)";
    SCOPED_TRACE(query);

    auto matcher = matchHiveScan("nation")
                       .hashJoin(
                           matchHiveScan("supplier")
                               .hashJoin(
                                   matchHiveScan("region").build(),
                                   core::JoinType::kRightSemiFilter)
                               .build(),
                           core::JoinType::kLeft)
                       .project()
                       .build();

    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Correlated EXISTS referencing the null-supplying side.
  {
    auto query = baseJoin +
        " AND EXISTS (SELECT 1 FROM supplier s"
        " WHERE s.s_nationkey = r.r_regionkey)";
    SCOPED_TRACE(query);

    auto matcher = matchHiveScan("nation")
                       .hashJoin(
                           matchHiveScan("supplier")
                               .hashJoin(
                                   matchHiveScan("region").build(),
                                   core::JoinType::kRightSemiFilter)
                               .build(),
                           core::JoinType::kLeft)
                       .project()
                       .build();

    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(SubqueryTest, unsupportedSubqueryInJoin) {
  // Left-side subquery in LEFT JOIN ON clause.
  VELOX_ASSERT_THROW(
      toSingleNodePlan(
          "SELECT * FROM nation n LEFT JOIN region r "
          "ON r.r_regionkey = n.n_regionkey "
          "AND n.n_regionkey > (SELECT min(s_nationkey) FROM supplier)"),
      "Unsupported subqueries in the ON clause of a LEFT or RIGHT join");

  // Right-side (preserved) subquery in RIGHT JOIN ON clause.
  VELOX_ASSERT_THROW(
      toSingleNodePlan(
          "SELECT * FROM region r RIGHT JOIN nation n "
          "ON r.r_regionkey = n.n_regionkey "
          "AND n.n_regionkey > (SELECT min(s_nationkey) FROM supplier)"),
      "Unsupported subqueries in the ON clause of a LEFT or RIGHT join");

  // Subquery in FULL JOIN ON clause.
  VELOX_ASSERT_THROW(
      toSingleNodePlan(
          "SELECT * FROM nation n FULL JOIN region r "
          "ON r.r_regionkey = n.n_regionkey "
          "AND r.r_name IN (SELECT s_name FROM supplier)"),
      "Unexpected expression: Subquery");
}

TEST_F(SubqueryTest, inSubqueryInsideAggregate) {
  auto matchJoin = [&]() {
    return matchHiveScan("nation").hashJoin(
        matchHiveScan("region").build(),
        core::JoinType::kLeftSemiProject,
        /*nullAware=*/true);
  };

  // IN <subquery> inside an aggregate expression.
  {
    auto query =
        "SELECT SUM(CASE WHEN n_regionkey IN "
        "(SELECT r_regionkey FROM region) THEN 1 ELSE 0 END) FROM nation";

    auto matcher = matchJoin().project().singleAggregation().build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // IN <subquery> inside an aggregate FILTER clause.
  {
    auto query =
        "SELECT COUNT(*) FILTER (WHERE n_regionkey IN "
        "(SELECT r_regionkey FROM region)) FROM nation";

    auto matcher = matchJoin().singleAggregation().build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(SubqueryTest, nestedInSubqueries) {
  // IN subquery on a column derived from another IN subquery. The inner IN
  // produces a mark column used to compute 'flag'. The outer IN uses 'flag'
  // as its left key. The optimizer wraps the inner semi-join in a child DT
  // so the outer semi-join references the child DT, not the current DT.
  auto query =
      "SELECT IF(flag IN (SELECT 1), 'y', 'n') "
      "FROM ("
      " SELECT IF(n_regionkey IN (SELECT r_regionkey FROM region), 1, 0) AS flag "
      " FROM nation"
      ") t";

  auto matcher = matchHiveScan("nation")
                     .hashJoin(
                         matchHiveScan("region").build(),
                         velox::core::JoinType::kLeftSemiProject,
                         /*nullAware=*/true)
                     .project()
                     .hashJoin(
                         matchValues().project().build(),
                         velox::core::JoinType::kLeftSemiProject,
                         /*nullAware=*/true)
                     .project()
                     .build();

  SCOPED_TRACE(query);
  auto plan = toSingleNodePlan(query);
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// EXISTS (SELECT 1 WHERE <condition>) with no FROM clause is equivalent to
// just <condition>. The optimizer should simplify this and not attempt
// subquery decorrelation.
TEST_F(SubqueryTest, existsWithNoFromClause) {
  testConnector_->addTable("t", ROW("a", BIGINT()));
  testConnector_->addTable("u", ROW("x", BIGINT()));

  auto query =
      "WITH matched AS ("
      "    SELECT t.a FROM t, u"
      "    WHERE EXISTS (SELECT 1 WHERE t.a = u.x)"
      ") "
      "SELECT (SELECT count(*) FROM matched) FROM t";

  auto matcher =
      matchScan("t")
          .nestedLoopJoin(
              matchScan("t")
                  .hashJoin(matchScan("u").build(), core::JoinType::kInner)
                  .aggregation()
                  .build())
          .build();

  auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// EXISTS (SELECT ... LIMIT 0) should fold to false because the subquery
// produces zero rows.
TEST_F(SubqueryTest, existsWithLimitZero) {
  auto plan = toSingleNodePlan("SELECT EXISTS (SELECT 1 LIMIT 0)");

  auto matcher = matchValues(ROW({})).project({"false"}).build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// IN subquery in a projection combined with a correlated NOT EXISTS in the
// WHERE clause. The IN subquery creates a semi-join inside the join input,
// which triggers DT wrapping (excludeOuterJoins). The NOT EXISTS subquery
// must still be processed correctly.
TEST_F(SubqueryTest, inSubqueryWithCorrelatedNotExists) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  testConnector_->addTable("u", ROW("x", BIGINT()));
  testConnector_->addTable("v", ROW("y", BIGINT()));

  auto query =
      "SELECT t.a, sub.flag "
      "FROM t "
      "JOIN ("
      "    SELECT u.x, u.x IN (SELECT y FROM v) AS flag FROM u"
      ") sub ON t.a = sub.x "
      "WHERE NOT EXISTS (SELECT 1 FROM v WHERE v.y = t.a)";

  // The IN subquery becomes a LEFT SEMI PROJECT (mark) join with v, wrapped
  // in its own DT. The inner join combines u's projection with t. The NOT
  // EXISTS becomes an anti-join with v.
  auto matcher = matchScan("u")
                     .hashJoin(
                         matchScan("v").build(),
                         core::JoinType::kLeftSemiProject,
                         /*nullAware=*/true)
                     .project()
                     .hashJoin(matchScan("t").build(), core::JoinType::kInner)
                     .hashJoin(matchScan("v").build(), core::JoinType::kAnti)
                     .build();

  auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// Verifies that the distributed plan for IN / NOT IN subqueries sets
// replicateNullsAndAny on the PartitionedOutput feeding the null-aware join.
// Without this flag, NULL rows on the right side are hash-partitioned to only
// one worker, producing wrong results:
//   NOT IN: workers missing the NULL incorrectly return rows.
//   IN: workers missing the NULL produce false instead of NULL for the mark.
TEST_F(SubqueryTest, inReplicateNullsAndAny) {
  // Make both tables large enough to trigger hash partitioning instead of
  // broadcast.
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(10'000'000, {});
  testConnector_->addTable("u", ROW({"c", "d"}, BIGINT()))
      ->setStats(1'000'000, {});

  // NOT IN: the build side (u) must use replicateNullsAndAny=true.
  {
    auto query = "SELECT * FROM t WHERE a NOT IN (SELECT c FROM u)";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .shuffle({"a"})
                       .hashJoin(
                           matchScan("u")
                               .shuffle({"c"}, /*replicateNullsAndAny=*/true)
                               .build(),
                           velox::core::JoinType::kAnti,
                           /*nullAware=*/true)
                       .gather()
                       .build();

    auto distributedPlan = planVelox(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, matcher);
  }

  // IN in projection: stays as kLeftSemiProject with null-aware. The build
  // side needs replicateNullsAndAny so all workers can produce NULL (not
  // false) when the build side contains a NULL.
  {
    auto query = "SELECT a, a IN (SELECT c FROM u) AS flag FROM t";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .shuffle({"a"})
                       .hashJoin(
                           matchScan("u")
                               .shuffle({"c"}, /*replicateNullsAndAny=*/true)
                               .build(),
                           velox::core::JoinType::kLeftSemiProject,
                           /*nullAware=*/true)
                       .project()
                       .gather()
                       .build();

    auto distributedPlan = planVelox(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, matcher);
  }

  // IN in projection with reversed table sizes — exercises joinByHashRight.
  {
    auto query = "SELECT c, c IN (SELECT a FROM t) AS flag FROM u";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .shuffle({"a"}, /*replicateNullsAndAny=*/true)
                       .hashJoin(
                           matchScan("u").shuffle({"c"}).build(),
                           velox::core::JoinType::kRightSemiProject,
                           /*nullAware=*/true)
                       .project()
                       .gather()
                       .build();

    auto distributedPlan = planVelox(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, matcher);
  }
}

TEST_F(SubqueryTest, constantFoldingWithoutExecutor) {
  auto& ctx = getQueryCtx();
  ctx = velox::core::QueryCtx::create(nullptr, velox::core::QueryConfig{{}});

  auto logicalPlan =
      parseSelect("SELECT * FROM nation WHERE n_regionkey > (SELECT 1)");

  auto plan = planVelox(logicalPlan, {.numWorkers = 4, .numDrivers = 4});
  EXPECT_EQ(3, plan.plan->fragments().size());
}

} // namespace
} // namespace facebook::axiom::optimizer
