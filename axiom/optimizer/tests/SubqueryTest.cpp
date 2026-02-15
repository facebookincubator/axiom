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

#include "axiom/connectors/tests/TestConnector.h"
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
  static constexpr auto kTestConnectorId = "test";

  void SetUp() override {
    test::HiveQueriesTestBase::SetUp();

    testConnector_ =
        std::make_shared<connector::TestConnector>(kTestConnectorId);
    velox::connector::registerConnector(testConnector_);
  }

  void TearDown() override {
    velox::connector::unregisterConnector(kTestConnectorId);

    HiveQueriesTestBase::TearDown();
  }

  std::shared_ptr<connector::TestConnector> testConnector_;
};

TEST_F(SubqueryTest, uncorrelatedScalar) {
  // = <subquery>
  {
    auto query =
        "select * from nation where n_regionkey "
        "= (select r_regionkey from region where r_name like 'AF%')";

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
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
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
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
        core::PlanMatcherBuilder()
            .tableScan("nation")
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
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
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
    return core::PlanMatcherBuilder().tableScan("t").filter(filter).build();
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
    auto matcher = makeMatcher("ds = null");

    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(SubqueryTest, correlatedExists) {
  {
    auto query =
        "SELECT * FROM nation WHERE "
        "EXISTS (SELECT * FROM region WHERE r_regionkey = n_regionkey)";

    auto matcher =
        core::PlanMatcherBuilder()
            .tableScan("nation")
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

    auto matcher =
        core::PlanMatcherBuilder()
            .tableScan("nation")
            .nestedLoopJoin(
                core::PlanMatcherBuilder().tableScan("region").build(),
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
        core::PlanMatcherBuilder()
            .tableScan("nation")
            .hashJoin(
                core::PlanMatcherBuilder().tableScan("region").build(),
                velox::core::JoinType::kFull)
            .hashJoin(
                core::PlanMatcherBuilder().values().project().build(),
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
        core::PlanMatcherBuilder()
            .tableScan("nation")
            .hashJoin(
                core::PlanMatcherBuilder().tableScan("region").build(),
                velox::core::JoinType::kInner)
            .hashJoin(
                core::PlanMatcherBuilder().tableScan("nation").build(),
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
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("region")
                       .nestedLoopJoin(
                           core::PlanMatcherBuilder()
                               .tableScan("nation")
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
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("region")
                       .singleAggregation({}, {"array_agg(r_name)"})
                       .nestedLoopJoin(
                           core::PlanMatcherBuilder()
                               .tableScan("nation")
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
        core::PlanMatcherBuilder()
            .tableScan("nation")
            .singleAggregation({"n_regionkey"}, {"array_agg(n_name)"})
            .nestedLoopJoin(
                core::PlanMatcherBuilder()
                    .tableScan("region")
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
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
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
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
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
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("region")
                       .nestedLoopJoin(
                           core::PlanMatcherBuilder()
                               .tableScan("nation")
                               .limit()
                               .singleAggregation()
                               .build(),
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
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("region")
                       .nestedLoopJoin(
                           core::PlanMatcherBuilder()
                               .tableScan("nation")
                               .limit()
                               .singleAggregation()
                               .build(),
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
    auto matcher =
        core::PlanMatcherBuilder()
            .tableScan("orders")
            .hashJoin(
                core::PlanMatcherBuilder().tableScan("customer").build(),
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
    auto matcher =
        core::PlanMatcherBuilder()
            .tableScan("orders")
            .hashJoin(
                core::PlanMatcherBuilder().tableScan("customer").build(),
                core::JoinType::kRightSemiProject,
                /*nullAware=*/true)
            .filter()
            .project()
            .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
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
        core::PlanMatcherBuilder()
            .tableScan("region")
            .hashJoin(
                core::PlanMatcherBuilder()
                    .tableScan("nation")
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
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("region")
                       .hashJoin(
                           core::PlanMatcherBuilder()
                               .tableScan("nation")
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

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("region")
                       .hashJoin(
                           core::PlanMatcherBuilder()
                               .tableScan("nation")
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

TEST_F(SubqueryTest, correlatedProject) {
  auto matchAggNation = [&]() {
    return core::PlanMatcherBuilder()
        .tableScan("nation")
        .singleAggregation()
        .project()
        .build();
  };

  // Correlated scalar subquery in projection with COUNT aggregation.
  {
    auto query =
        "SELECT r_name, "
        "   (SELECT count(*) FROM nation WHERE n_regionkey = r_regionkey) AS cnt "
        "FROM region";

    // The correlated scalar subquery is transformed into a LEFT JOIN with
    // aggregation grouped by the correlation key.
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("region")
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

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("region")
                       .hashJoin(matchAggNation(), velox::core::JoinType::kLeft)
                       .project()
                       .build();

    SCOPED_TRACE(query);
    auto plan = toSingleNodePlan(query);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Multiple scalar subqueries in projection.
  {
    auto query =
        "SELECT r_name, "
        "   (SELECT count(*) FROM nation WHERE n_regionkey = r_regionkey) AS cnt, "
        "   (SELECT max(n_nationkey) FROM nation WHERE n_regionkey = r_regionkey) AS max_key "
        "FROM region";

    // Each subquery produces a separate LEFT JOIN.
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("region")
                       // TODO Optimize to combine the two LEFT JOINs into one.
                       .hashJoin(matchAggNation(), velox::core::JoinType::kLeft)
                       .hashJoin(matchAggNation(), velox::core::JoinType::kLeft)
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
        core::PlanMatcherBuilder()
            .tableScan("nation")
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
        core::PlanMatcherBuilder()
            .tableScan("nation")
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
        core::PlanMatcherBuilder()
            .tableScan("nation")
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
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("region")
                       .nestedLoopJoin(
                           core::PlanMatcherBuilder()
                               .tableScan("nation")
                               .finalLimit(0, 1)
                               .singleAggregation({}, {"count(*) as c"})
                               .filter("not(eq(c, 0))")
                               .project()
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
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("region")
                       .nestedLoopJoin(
                           core::PlanMatcherBuilder()
                               .tableScan("nation")
                               .finalLimit(0, 1)
                               .singleAggregation({}, {"count(*) as c"})
                               .filter("not(not(eq(c, 0)))")
                               .project()
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
        core::PlanMatcherBuilder()
            .tableScan("nation")
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
    auto matcher =
        core::PlanMatcherBuilder()
            .tableScan("nation")
            .nestedLoopJoin(
                core::PlanMatcherBuilder().tableScan("region").build(),
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
        core::PlanMatcherBuilder()
            .tableScan("nation")
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
        core::PlanMatcherBuilder()
            .tableScan("t")
            .unnest()
            .hashJoin(core::PlanMatcherBuilder().tableScan("u").build())
            .build();

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto query =
        "select * from t, unnest(a) as v(n) where EXISTS (SELECT * FROM u WHERE x = n) ";

    auto logicalPlan = parseSelect(query, kTestConnectorId);
    auto matcher =
        core::PlanMatcherBuilder()
            .tableScan("t")
            .unnest()
            .hashJoin(core::PlanMatcherBuilder().tableScan("u").build())
            .build();

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto query =
        "select (SELECT sum(y) FROM u WHERE x = n) from t, unnest(a) as v(n)";

    auto logicalPlan = parseSelect(query, kTestConnectorId);
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("t")
                       .unnest()
                       .hashJoin(
                           core::PlanMatcherBuilder()
                               .tableScan("u")
                               .aggregation()
                               .project()
                               .build(),
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

  auto matchScan = [&](const auto& tableName) {
    return core::PlanMatcherBuilder().tableScan(tableName);
  };

  {
    auto matcher =
        matchScan("region")
            .nestedLoopJoin(matchScan("nation").enforceSingleRow().build())
            .filter()
            .project()
            .build();

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);

    VELOX_ASSERT_THROW(runVelox(plan), "Expected single row of input.");
  }

  {
    auto matcher =
        matchScan("region")
            .nestedLoopJoin(
                matchScan("nation").enforceSingleRow().broadcast().build())
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

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan("region")
                     .nestedLoopJoin(
                         core::PlanMatcherBuilder()
                             .tableScan("nation")
                             .aggregation()
                             .build())
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

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan("region")
                     .hashJoin(
                         core::PlanMatcherBuilder()
                             .tableScan("nation")
                             .aggregation()
                             .project()
                             .build(),
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
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("region")
                         .assignUniqueId("unique_id")
                         .nestedLoopJoin(
                             core::PlanMatcherBuilder()
                                 .tableScan("nation")
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
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("region")
                         .assignUniqueId("unique_id")
                         .nestedLoopJoin(
                             core::PlanMatcherBuilder()
                                 .tableScan("nation")
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
          core::PlanMatcherBuilder()
              .tableScan("region")
              .assignUniqueId("unique_id")
              .hashJoin(
                  core::PlanMatcherBuilder()
                      .tableScan("nation")
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

TEST_F(SubqueryTest, nonEquiCorrelatedProject) {
  // Correlated scalar subquery with non-equi correlation condition.
  {
    auto query =
        "SELECT length(r_name), (SELECT count(*) FROM nation WHERE n_regionkey < r_regionkey) FROM region";
    SCOPED_TRACE(query);

    auto logicalPlan = parseSelect(query);

    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("region")
                         .assignUniqueId("unique_id")
                         .nestedLoopJoin(
                             core::PlanMatcherBuilder()
                                 .tableScan("nation")
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
                         .build();

      auto plan = toSingleNodePlan(logicalPlan);
      AXIOM_ASSERT_PLAN(plan, matcher);
    }

    {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan("region")
                         .assignUniqueId("unique_id")
                         .nestedLoopJoin(
                             core::PlanMatcherBuilder()
                                 .tableScan("nation")
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

  auto matchScan = [&](const auto& tableName) {
    return core::PlanMatcherBuilder().tableScan(tableName);
  };

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

} // namespace
} // namespace facebook::axiom::optimizer
