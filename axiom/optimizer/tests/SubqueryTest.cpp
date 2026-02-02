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

TEST_F(SubqueryTest, scalar) {
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
TEST_F(SubqueryTest, project) {
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
    auto matcher =
        core::PlanMatcherBuilder()
            .tableScan("u")
            .aggregation()
            .project()
            .hashJoin(
                core::PlanMatcherBuilder().tableScan("t").unnest().build(),
                core::JoinType::kRight)
            .build();

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
