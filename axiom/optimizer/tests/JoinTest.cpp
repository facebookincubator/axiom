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
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

class JoinTest : public test::QueryTestBase {
 protected:
  static constexpr auto kTestConnectorId = "test";

  void SetUp() override {
    test::QueryTestBase::SetUp();

    testConnector_ =
        std::make_shared<connector::TestConnector>(kTestConnectorId);
    velox::connector::registerConnector(testConnector_);

    testConnector_->addTable(
        "region",
        ROW({"r_regionkey", "r_name", "r_comment"},
            {BIGINT(), VARCHAR(), VARCHAR()}));
    testConnector_->addTable(
        "nation",
        ROW({"n_nationkey", "n_name", "n_regionkey", "n_comment"},
            {BIGINT(), VARCHAR(), BIGINT(), VARCHAR()}));
    testConnector_->addTable(
        "customer",
        ROW({"c_custkey",
             "c_name",
             "c_address",
             "c_nationkey",
             "c_phone",
             "c_acctbal",
             "c_mktsegment",
             "c_comment"},
            {BIGINT(),
             VARCHAR(),
             VARCHAR(),
             BIGINT(),
             VARCHAR(),
             DOUBLE(),
             VARCHAR(),
             VARCHAR()}));
    testConnector_->addTable(
        "orders",
        ROW({"o_orderkey",
             "o_custkey",
             "o_orderstatus",
             "o_totalprice",
             "o_orderdate",
             "o_orderpriority",
             "o_clerk",
             "o_shippriority",
             "o_comment"},
            {BIGINT(),
             BIGINT(),
             VARCHAR(),
             DOUBLE(),
             DATE(),
             VARCHAR(),
             VARCHAR(),
             INTEGER(),
             VARCHAR()}));
  }

  void TearDown() override {
    velox::connector::unregisterConnector(kTestConnectorId);

    test::QueryTestBase::TearDown();
  }

  std::shared_ptr<connector::TestConnector> testConnector_;
};

TEST_F(JoinTest, pushdownFilterThroughJoin) {
  testConnector_->addTable("t", ROW({"t_id", "t_data"}, BIGINT()));
  testConnector_->addTable("u", ROW({"u_id", "u_data"}, BIGINT()));

  auto makePlan = [&](lp::JoinType joinType) {
    lp::PlanBuilder::Context ctx{kTestConnectorId};
    return lp::PlanBuilder{ctx}
        .tableScan("t")
        .join(lp::PlanBuilder{ctx}.tableScan("u"), "t_id = u_id", joinType)
        .filter("t_data IS NULL")
        .filter("u_data IS NULL")
        .build();
  };

  {
    SCOPED_TRACE("Inner Join");
    auto logicalPlan = makePlan(lp::JoinType::kInner);
    auto matcher = core::PlanMatcherBuilder{}
                       .tableScan("t")
                       .filter("t_data IS NULL")
                       .hashJoin(
                           core::PlanMatcherBuilder{}
                               .tableScan("u")
                               .filter("u_data IS NULL")
                               .build(),
                           core::JoinType::kInner)
                       .build();
    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    SCOPED_TRACE("Left Join");
    auto logicalPlan = makePlan(lp::JoinType::kLeft);
    auto matcher = core::PlanMatcherBuilder{}
                       .tableScan("t")
                       .filter("t_data IS NULL")
                       .hashJoin(
                           core::PlanMatcherBuilder{}.tableScan("u").build(),
                           core::JoinType::kLeft)
                       .filter("u_data IS NULL")
                       .build();
    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    SCOPED_TRACE("Right Join");
    auto logicalPlan = makePlan(lp::JoinType::kRight);
    auto matcher = core::PlanMatcherBuilder{}
                       .tableScan("u")
                       .filter("u_data IS NULL")
                       .hashJoin(
                           core::PlanMatcherBuilder{}.tableScan("t").build(),
                           core::JoinType::kLeft)
                       .filter("t_data IS NULL")
                       .project()
                       .build();
    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    SCOPED_TRACE("Full Join");
    auto logicalPlan = makePlan(lp::JoinType::kFull);
    auto matcher = core::PlanMatcherBuilder{}
                       .tableScan("t")
                       .hashJoin(
                           core::PlanMatcherBuilder{}.tableScan("u").build(),
                           core::JoinType::kFull)
                       .filter("t_data IS NULL AND u_data IS NULL")
                       .build();
    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(JoinTest, hyperEdge) {
  testConnector_->addTable("t", ROW({"t_id", "t_key", "t_data"}, BIGINT()));
  testConnector_->addTable("u", ROW({"u_id", "u_key", "u_data"}, BIGINT()));
  testConnector_->addTable("v", ROW({"v_key", "v_data"}, BIGINT()));

  lp::PlanBuilder::Context ctx{kTestConnectorId};
  auto logicalPlan = lp::PlanBuilder{ctx}
                         .from({"t", "u"})
                         .filter("t_id = u_id")
                         .join(
                             lp::PlanBuilder{ctx}.tableScan("v"),
                             "t_key = v_key AND u_key = v_key",
                             lp::JoinType::kLeft)
                         .build();

  auto matcher = core::PlanMatcherBuilder{}
                     .tableScan("t")
                     .hashJoin(
                         core::PlanMatcherBuilder{}.tableScan("u").build(),
                         core::JoinType::kInner)
                     .hashJoin(
                         core::PlanMatcherBuilder{}.tableScan("v").build(),
                         core::JoinType::kLeft)
                     .build();
  auto plan = toSingleNodePlan(logicalPlan);
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(JoinTest, joinWithFilterOverLimit) {
  testConnector_->addTable("t", ROW({"a", "b", "c"}, BIGINT()));
  testConnector_->addTable("u", ROW({"x", "y", "z"}, BIGINT()));

  lp::PlanBuilder::Context ctx(kTestConnectorId);
  auto logicalPlan =
      lp::PlanBuilder(ctx)
          .tableScan("t")
          .limit(100)
          .filter("b > 50")
          .join(
              lp::PlanBuilder(ctx).tableScan("u").limit(50).filter("y < 100"),
              "a = x",
              lp::JoinType::kInner)
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("t")
                       .limit()
                       .filter("b > 50")
                       .hashJoin(
                           core::PlanMatcherBuilder()
                               .tableScan("u")
                               .limit()
                               .filter("y < 100")
                               .build())
                       .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(JoinTest, outerJoinWithInnerJoin) {
  testConnector_->addTable("t", ROW({"a", "b", "c"}, BIGINT()));
  testConnector_->addTable("v", ROW({"vx", "vy", "vz"}, BIGINT()));
  testConnector_->addTable("u", ROW({"x", "y", "z"}, BIGINT()));

  auto startMatcher = [&](const auto& tableName) {
    return core::PlanMatcherBuilder().tableScan(tableName);
  };

  {
    SCOPED_TRACE("left join with inner join on right");

    lp::PlanBuilder::Context ctx(kTestConnectorId);
    auto logicalPlan = lp::PlanBuilder(ctx)
                           .tableScan("t")
                           .filter("b > 50")
                           .join(
                               lp::PlanBuilder(ctx).tableScan("u").join(
                                   lp::PlanBuilder(ctx).tableScan("v"),
                                   "x = vx",
                                   lp::JoinType::kInner),
                               "a = x",
                               lp::JoinType::kLeft)
                           .build();

    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher =
        startMatcher("u")
            .hashJoin(startMatcher("v").build(), core::JoinType::kInner)
            .hashJoin(
                startMatcher("t").filter("b > 50").build(),
                core::JoinType::kRight)
            .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    SCOPED_TRACE("aggregation left join filter over inner join");

    lp::PlanBuilder::Context ctx(kTestConnectorId);
    auto logicalPlan = lp::PlanBuilder(ctx)
                           .tableScan("t")
                           .filter("b > 50")
                           .aggregate({"a", "b"}, {"sum(c)"})
                           .join(
                               lp::PlanBuilder(ctx)
                                   .tableScan("u")
                                   .join(
                                       lp::PlanBuilder(ctx).tableScan("v"),
                                       "x = vx",
                                       lp::JoinType::kInner)
                                   .filter("not(x = vy)"),
                               "a = x",
                               lp::JoinType::kLeft)
                           .build();

    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher =
        startMatcher("u")
            .hashJoin(startMatcher("v").build())
            .filter()
            .hashJoin(startMatcher("t").filter().aggregation().build())
            .project()
            .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(JoinTest, nestedOuterJoins) {
  auto sql =
      "SELECT r2.r_name "
      "FROM nation n "
      "   FULL OUTER JOIN region r1 ON n.n_regionkey = r1.r_regionkey "
      "   RIGHT OUTER JOIN region r2 ON n.n_regionkey = r2.r_regionkey "
      "GROUP BY 1";

  auto logicalPlan = parseSelect(sql, kTestConnectorId);
  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher =
      core::PlanMatcherBuilder()
          .tableScan("region")
          .hashJoin(
              core::PlanMatcherBuilder()
                  .tableScan("nation")
                  .hashJoin(
                      core::PlanMatcherBuilder().tableScan("region").build(),
                      core::JoinType::kFull)
                  .build(),
              core::JoinType::kLeft)
          .aggregation()
          .project()
          .build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(JoinTest, joinWithComputedKeys) {
  auto sql =
      "SELECT count(1) FROM nation n RIGHT JOIN region ON coalesce(n_regionkey, 1) = r_regionkey";

  auto logicalPlan = parseSelect(sql, kTestConnectorId);
  {
    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher =
        core::PlanMatcherBuilder()
            .tableScan("nation")
            // TODO Remove redundant projection of 'n_regionkey'.
            .project({"n_regionkey", "coalesce(n_regionkey, 1)"})
            .hashJoin(
                core::PlanMatcherBuilder().tableScan("region").build(),
                core::JoinType::kRight)
            .aggregation()
            .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto distributedPlan = planVelox(logicalPlan);

    auto rightSideMatcher =
        core::PlanMatcherBuilder().tableScan("region").shuffle().build();

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("nation")
                       // TODO Remove redundant projection of 'n_regionkey'.
                       .project({"n_regionkey", "coalesce(n_regionkey, 1)"})
                       .shuffle()
                       .hashJoin(rightSideMatcher, core::JoinType::kRight)
                       .partialAggregation()
                       .shuffle()
                       .localPartition()
                       .finalAggregation()
                       .build();

    AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, matcher);
  }
}

TEST_F(JoinTest, crossJoin) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()));
  testConnector_->addTable("v", ROW({"n", "m"}, BIGINT()));

  auto matchScan = [&](const auto& tableName) {
    return core::PlanMatcherBuilder().tableScan(tableName);
  };

  {
    lp::PlanBuilder::Context ctx{kTestConnectorId};
    auto logicalPlan =
        lp::PlanBuilder{ctx}.from({"t", "u"}).project({"a + x"}).build();

    auto matcher = matchScan("t")
                       .nestedLoopJoin(matchScan("u").build())
                       .project({"a + x"})
                       .build();

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);

    ASSERT_NO_THROW(planVelox(logicalPlan));
  }

  {
    lp::PlanBuilder::Context ctx{kTestConnectorId};
    auto logicalPlan =
        lp::PlanBuilder{ctx}.from({"t", "u"}).filter("a > x").build();

    auto matcher = matchScan("t")
                       .nestedLoopJoin(matchScan("u").build())
                       .filter("a > x")
                       .build();

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);

    ASSERT_NO_THROW(planVelox(logicalPlan));
  }

  {
    lp::PlanBuilder::Context ctx{kTestConnectorId};
    auto logicalPlan = lp::PlanBuilder{ctx}
                           .from({"t", "u"})
                           .aggregate({}, {"count(1)"})
                           .build();

    auto matcher = matchScan("t")
                       .nestedLoopJoin(matchScan("u").build())
                       .aggregation()
                       .build();

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);

    ASSERT_NO_THROW(planVelox(logicalPlan));
  }

  {
    auto logicalPlan =
        parseSelect("SELECT * FROM t, u, v WHERE a = x", kTestConnectorId);

    auto matcher = matchScan("t")
                       .hashJoin(matchScan("u").build())
                       .nestedLoopJoin(matchScan("v").build())
                       .build();

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);

    ASSERT_NO_THROW(planVelox(logicalPlan));
  }

  // Cross join with a single-row subquery whose output is not used. The
  // subquery is ignored.
  {
    lp::PlanBuilder::Context ctx{kTestConnectorId};
    auto logicalPlan = parseSelect(
        "SELECT a FROM t, (SELECT count(*) FROM u)", kTestConnectorId);

    auto matcher = matchScan("t").build();

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);

    ASSERT_NO_THROW(planVelox(logicalPlan));
  }

  {
    lp::PlanBuilder::Context ctx{kTestConnectorId};
    auto logicalPlan = parseSelect(
        "SELECT * FROM t, (SELECT count(*) FROM u)", kTestConnectorId);

    auto matcher = matchScan("t")
                       .nestedLoopJoin(matchScan("u").aggregation().build())
                       .build();

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);

    ASSERT_NO_THROW(planVelox(logicalPlan));
  }

  // Cross join with a subquery that looks like single-row, but may not be. The
  // subquery is not ignored.
  {
    lp::PlanBuilder::Context ctx{kTestConnectorId};
    auto logicalPlan = parseSelect(
        "SELECT a FROM t, (SELECT * FROM u LIMIT 1)", kTestConnectorId);

    auto matcher =
        matchScan("t").nestedLoopJoin(matchScan("u").limit().build()).build();

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);

    ASSERT_NO_THROW(planVelox(logicalPlan));
  }
}

TEST_F(JoinTest, leftCrossJoin) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()));

  {
    auto logicalPlan = parseSelect(
        "SELECT * FROM t LEFT JOIN (SELECT count(*) FROM u) ON 1 = 1",
        kTestConnectorId);

    auto matcher =
        core::PlanMatcherBuilder()
            .tableScan("t")
            .nestedLoopJoin(
                core::PlanMatcherBuilder().tableScan("u").aggregation().build(),
                core::JoinType::kLeft)
            .build();

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);

    ASSERT_NO_THROW(planVelox(logicalPlan));
  }

  {
    auto logicalPlan = parseSelect(
        "SELECT * FROM (SELECT count(*) FROM t) LEFT JOIN (SELECT count(*) FROM u) ON 1 = 1",
        kTestConnectorId);

    auto matcher =
        core::PlanMatcherBuilder()
            .tableScan("t")
            .aggregation()
            .nestedLoopJoin(
                core::PlanMatcherBuilder().tableScan("u").aggregation().build(),
                core::JoinType::kLeft)
            .build();

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);

    ASSERT_NO_THROW(planVelox(logicalPlan));
  }

  {
    auto logicalPlan = parseSelect(
        "SELECT a FROM t LEFT JOIN u ON 1 = 1 WHERE coalesce(x, 1) > 0",
        kTestConnectorId);

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("t")
                       .nestedLoopJoin(
                           core::PlanMatcherBuilder().tableScan("u").build(),
                           core::JoinType::kLeft)
                       .filter()
                       .project()
                       .build();

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);

    ASSERT_NO_THROW(planVelox(logicalPlan));
  }
}

TEST_F(JoinTest, crossThenLeft) {
  testConnector_->addTable("t", ROW({"t0", "t1"}, INTEGER()));
  testConnector_->addTable("u", ROW({"u0", "u1"}, BIGINT()));

  // Cross join t with u, then left join with an aggregation over values.
  auto query =
      "WITH v AS (SELECT v0, count(1) as v1 FROM (VALUES 1, 2, 3) as v(v0) GROUP BY 1) "
      "SELECT count(1) FROM (SELECT * FROM t, u) LEFT JOIN v ON t0 = v0 AND u0 = v1";
  SCOPED_TRACE(query);

  auto matcher =
      core::PlanMatcherBuilder()
          .values()
          .aggregation()
          // TODO Remove redundant projection.
          .project()
          .hashJoin(
              core::PlanMatcherBuilder()
                  .tableScan("u")
                  .nestedLoopJoin(
                      core::PlanMatcherBuilder().tableScan("t").build())
                  .build(),
              velox::core::JoinType::kRight)
          .aggregation()
          .build();

  auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(JoinTest, joinWithComputedAndProjectedKeys) {
  testConnector_->addTable("t", ROW({"t0", "t1"}, BIGINT()));
  testConnector_->addTable("u", ROW({"u0", "u1"}, BIGINT()));

  auto query =
      "WITH v AS (SELECT coalesce(t0, 0) as v0 FROM t) "
      "SELECT * FROM u LEFT JOIN v ON u0 = v0";
  SCOPED_TRACE(query);

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan("u")
                     .hashJoin(
                         core::PlanMatcherBuilder()
                             .tableScan("t")
                             // TODO Remove redundant projection of 't0'.
                             .project({"t0", "coalesce(t0, 0)"})
                             .build())
                     .project()
                     .build();

  auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(JoinTest, crossThanOrderBy) {
  auto query = "SELECT length(n_name) FROM nation, region ORDER BY 1";
  SCOPED_TRACE(query);

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan("nation")
                     .nestedLoopJoin(
                         core::PlanMatcherBuilder().tableScan("region").build())
                     .project({"length(n_name) as l"})
                     .orderBy({"l"})
                     .project()
                     .build();

  auto logicalPlan = parseSelect(query, kTestConnectorId);
  auto plan = toSingleNodePlan(logicalPlan);
  AXIOM_ASSERT_PLAN(plan, matcher);

  ASSERT_NO_THROW(planVelox(logicalPlan));
}

TEST_F(JoinTest, filterPushdownThroughCrossJoinUnnest) {
  {
    testConnector_->addTable(
        "t", ROW({"t0", "t1"}, {ROW({"a", "b"}, BIGINT()), ARRAY(BIGINT())}));

    auto query = "SELECT * FROM t, UNNEST(t1) WHERE t0.a > 0";
    SCOPED_TRACE(query);

    auto matcher =
        core::PlanMatcherBuilder().tableScan("t").filter().unnest().build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto query =
        "SELECT * FROM (VALUES row(row(1, 2))) as t(x), UNNEST(array[1,2,3]) WHERE x.field0 > 0";
    SCOPED_TRACE(query);

    auto matcher = core::PlanMatcherBuilder()
                       .values()
                       .filter()
                       // TODO Combine 2 projects into one.
                       .project()
                       .project()
                       .unnest()
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(JoinTest, joinOnClause) {
  testConnector_->addTable("t", ROW({"t0"}, ROW({"a", "b"}, BIGINT())));
  testConnector_->addTable("u", ROW({"u0"}, ROW({"a", "b"}, BIGINT())));

  {
    auto query = "SELECT * FROM t JOIN u ON t0.a = u0.a";
    SCOPED_TRACE(query);

    auto matcher =
        core::PlanMatcherBuilder()
            .tableScan("t")
            .project()
            .hashJoin(
                core::PlanMatcherBuilder().tableScan("u").project().build())
            .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto query = "SELECT * FROM (SELECT t0, 1 FROM t) JOIN u ON t0.a = u0.a";
    SCOPED_TRACE(query);

    auto matcher =
        core::PlanMatcherBuilder()
            .tableScan("t")
            .project()
            .hashJoin(
                core::PlanMatcherBuilder().tableScan("u").project().build())
            .project()
            .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(JoinTest, leftJoinOverValues) {
  auto query =
      "SELECT * FROM (VALUES 1, 2, 3) as t(x) LEFT JOIN (VALUES 1, 2, 3) as u(y) ON x = y";
  SCOPED_TRACE(query);

  auto matcher = core::PlanMatcherBuilder()
                     .values()
                     .hashJoin(
                         core::PlanMatcherBuilder().values().build(),
                         core::JoinType::kLeft)
                     .project()
                     .build();

  auto logicalPlan = parseSelect(query, kTestConnectorId);

  {
    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto distributedPlan = planVelox(logicalPlan).plan;
    EXPECT_EQ(1, distributedPlan->fragments().size());
    auto plan = distributedPlan->fragments().at(0).fragment.planNode;
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(JoinTest, leftThenFilter) {
  testConnector_->addTable("t", ROW({"a", "b", "c"}, BIGINT()));
  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()));

  auto matchScan = [&](const auto& tableName) {
    return core::PlanMatcherBuilder().tableScan(tableName);
  };

  // Post-LEFT JOIN filter that references only columns from the right-hand
  // (optional) table and doesn't eliminate NULLs.
  {
    auto query =
        "SELECT * FROM t LEFT JOIN (SELECT x, y + 1 as z FROM u) ON a = x "
        "WHERE coalesce(z, 1) > 0";
    SCOPED_TRACE(query);

    auto matcher =
        matchScan("t")
            .hashJoin(matchScan("u").project().build(), core::JoinType::kLeft)
            .filter()
            .project()
            .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // A filter that does eliminate NULLs turns the LEFT JOIN into an INNER JOIN.
  {
    auto query =
        "SELECT * FROM t LEFT JOIN (SELECT x, y + 1 as z FROM u) ON a = x "
        "WHERE z > 0";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .hashJoin(
                           matchScan("u").filter("y + 1 > 0").build(),
                           core::JoinType::kInner)
                       .project()
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // A filter with multiple conjuncts, some of which eliminate NULLs.
  {
    auto query =
        "SELECT * FROM t LEFT JOIN (SELECT x, y + 1 as z FROM u) ON a = x "
        "WHERE coalesce(z, 1) > 0 AND z > 0";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .hashJoin(
                           matchScan("u")
                               .filter("coalesce(y + 1, 1) > 0 AND y + 1 > 0")
                               .build(),
                           core::JoinType::kInner)
                       .project()
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // The filter 'z > 0' references the optional side of the LEFT join. This
  // filter eliminates NULLs on the right side, so the LEFT join can be
  // converted to an INNER join. The filter is pushed down below the join.
  // Column 'z' is produced by the join and becomes unavailable after the join
  // is converted to INNER. Hence, references to 'z' in projections,
  // aggregations and order-by clauses must be replaced with the underlying
  // expression 'y + 1'.

  // Projection that references optional side column.
  {
    auto query =
        "SELECT z * 2 FROM t LEFT JOIN (SELECT x, y + 1 as z FROM u) ON a = x "
        "WHERE z > 0";
    SCOPED_TRACE(query);

    auto matcher = matchScan("u")
                       .filter("y + 1 > 0")
                       .hashJoin(matchScan("t").build(), core::JoinType::kInner)
                       .project()
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Aggregation that references optional side column.
  {
    auto query =
        "SELECT sum(z) FROM t LEFT JOIN (SELECT x, y + 1 as z FROM u) ON a = x "
        "WHERE z > 0";
    SCOPED_TRACE(query);

    auto matcher = matchScan("u")
                       .filter("y + 1 > 0")
                       .hashJoin(matchScan("t").build(), core::JoinType::kInner)
                       .project()
                       .aggregation()
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Order-by that references optional side column.
  {
    auto query =
        "SELECT * FROM t LEFT JOIN (SELECT x, y + 1 as z FROM u) ON a = x "
        "WHERE z > 0 "
        "ORDER BY z DESC";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .hashJoin(
                           matchScan("u").filter("y + 1 > 0").build(),
                           core::JoinType::kInner)
                       .project()
                       .orderBy()
                       .project()
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(JoinTest, fullThenFilter) {
  testConnector_->addTable("t", ROW({"a", "b", "c"}, BIGINT()));
  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()));

  auto matchScan = [&](const auto& tableName) {
    return core::PlanMatcherBuilder().tableScan(tableName);
  };

  // Post-FULL JOIN filter made of conjuncts that each reference only one side
  // of the join and do not eliminate NULLs.
  {
    auto query =
        "SELECT * FROM t FULL JOIN (SELECT x, y + 1 as z FROM u) ON a = x "
        "WHERE coalesce(z, 1) > 0 AND is_null(a)";
    SCOPED_TRACE(query);

    auto matcher =
        matchScan("t")
            .hashJoin(
                matchScan("u").project({"x", "y", "y + 1 as z"}).build(),
                core::JoinType::kFull)
            .filter("coalesce(z, 1) > 0 AND is_null(a)")
            .project()
            .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Null-eliminating conjunct that references only right side turns FULL join
  // into LEFT or RIGHT join and gets pushed down below the join.
  {
    auto query =
        "SELECT * FROM t FULL JOIN (SELECT x, y + 1 as z FROM u) ON a = x "
        "WHERE z > 0 AND is_null(a)";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .hashJoin(
                           matchScan("u").filter("y + 1 > 0").build(),
                           core::JoinType::kRight)
                       .filter("is_null(a)")
                       .project()
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto query =
        "SELECT * FROM t FULL JOIN (SELECT x, y + 1 as z FROM u) ON a = x "
        "WHERE coalesce(z, 1) > 0 AND a > 0";
    SCOPED_TRACE(query);

    auto matcher =
        matchScan("t")
            .filter("a > 0")
            .hashJoin(
                matchScan("u").project({"x", "y", "y + 1 as z"}).build(),
                core::JoinType::kLeft)
            .filter("coalesce(z, 1) > 0")
            .project()
            .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Two null-eliminating conjuncts, one on each side of the join turn the join
  // into an INNER join.
  {
    auto query =
        "SELECT * FROM t FULL JOIN (SELECT x, y + 1 as z FROM u) ON a = x "
        "WHERE z > 0 AND a > 0";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .filter("a > 0")
                       .hashJoin(
                           matchScan("u").filter("y + 1 > 0").build(),
                           core::JoinType::kInner)
                       .project()
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
