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

#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

class JoinTest : public test::QueryTestBase {
 protected:
  lp::PlanBuilder::Context makeContext() const {
    return lp::PlanBuilder::Context{kTestConnectorId, kDefaultSchema};
  }
};

TEST_F(JoinTest, pushdownFilterThroughJoin) {
  testConnector_->addTable("t", ROW({"t_id", "t_data"}, BIGINT()));
  testConnector_->addTable("u", ROW({"u_id", "u_data"}, BIGINT()));

  auto makePlan = [&](lp::JoinType joinType) {
    auto ctx = makeContext();
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
    auto matcher = matchScan("t")
                       .filter("t_data IS NULL")
                       .hashJoin(
                           matchScan("u").filter("u_data IS NULL").build(),
                           core::JoinType::kInner)
                       .build();
    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    SCOPED_TRACE("Left Join");
    auto logicalPlan = makePlan(lp::JoinType::kLeft);
    auto matcher = matchScan("t")
                       .filter("t_data IS NULL")
                       .hashJoin(matchScan("u").build(), core::JoinType::kLeft)
                       .filter("u_data IS NULL")
                       .build();
    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    SCOPED_TRACE("Right Join");
    auto logicalPlan = makePlan(lp::JoinType::kRight);
    auto matcher = matchScan("t")
                       .hashJoin(
                           matchScan("u").filter("u_data IS NULL").build(),
                           core::JoinType::kRight)
                       .filter("t_data IS NULL")
                       .project()
                       .build();
    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    SCOPED_TRACE("Full Join");
    auto logicalPlan = makePlan(lp::JoinType::kFull);
    auto matcher = matchScan("t")
                       .hashJoin(matchScan("u").build(), core::JoinType::kFull)
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

  auto ctx = makeContext();
  auto logicalPlan = lp::PlanBuilder{ctx}
                         .from({"t", "u"})
                         .filter("t_id = u_id")
                         .join(
                             lp::PlanBuilder{ctx}.tableScan("v"),
                             "t_key = v_key AND u_key = v_key",
                             lp::JoinType::kLeft)
                         .build();

  auto matcher = matchScan("t")
                     .hashJoin(matchScan("u").build(), core::JoinType::kInner)
                     .hashJoin(matchScan("v").build(), core::JoinType::kLeft)
                     .build();
  auto plan = toSingleNodePlan(logicalPlan);
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(JoinTest, joinWithFilterOverLimit) {
  testConnector_->addTable("t", ROW({"a", "b", "c"}, BIGINT()));
  testConnector_->addTable("u", ROW({"x", "y", "z"}, BIGINT()));

  auto ctx = makeContext();
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
    auto matcher =
        matchScan("t")
            .limit()
            .filter("b > 50")
            .hashJoin(matchScan("u").limit().filter("y < 100").build())
            .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(JoinTest, outerJoinWithInnerJoin) {
  testConnector_->addTable("t", ROW({"a", "b", "c"}, BIGINT()));
  testConnector_->addTable("v", ROW({"vx", "vy", "vz"}, BIGINT()));
  testConnector_->addTable("u", ROW({"x", "y", "z"}, BIGINT()));

  auto startMatcher = [&](const auto& tableName) {
    return matchScan(tableName);
  };

  {
    SCOPED_TRACE("left join with inner join on right");

    auto ctx = makeContext();
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
        startMatcher("t")
            .filter("b > 50")
            .hashJoin(
                startMatcher("u")
                    .hashJoin(startMatcher("v").build(), core::JoinType::kInner)
                    .build(),
                core::JoinType::kLeft)
            .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    SCOPED_TRACE("aggregation left join filter over inner join");

    auto ctx = makeContext();
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
    auto matcher = startMatcher("t")
                       .filter()
                       .aggregation()
                       .hashJoin(
                           startMatcher("u")
                               .hashJoin(startMatcher("v").build())
                               .filter()
                               .build(),
                           core::JoinType::kLeft)
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
      matchScan("nation")
          .hashJoin(matchScan("region").build(), core::JoinType::kFull)
          .hashJoin(matchScan("region").build(), core::JoinType::kRight)
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
        matchScan("nation")
            .project({"coalesce(n_regionkey, 1)"})
            .hashJoin(matchScan("region").build(), core::JoinType::kRight)
            .aggregation()
            .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto distributedPlan = planVelox(logicalPlan);

    auto rightSideMatcher = matchScan("region").shuffle().build();

    auto matcher = matchScan("nation")
                       .project({"coalesce(n_regionkey, 1)"})
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

TEST_F(JoinTest, broadcastSizeLimitGatesBroadcast) {
  testConnector_->addTable("probe", ROW({"p_key", "p_data"}, BIGINT()))
      ->setStats(100'000, {{"p_key", {.numDistinct = 100'000}}});
  testConnector_->addTable("build", ROW({"b_key", "b_data"}, BIGINT()))
      ->setStats(1'000, {{"b_key", {.numDistinct = 1'000}}});

  OptimizerOptions options;
  options.syntacticJoinOrder = true;

  auto ctx = makeContext();
  auto logicalPlan = lp::PlanBuilder{ctx}
                         .tableScan("probe")
                         .join(
                             lp::PlanBuilder{ctx}.tableScan("build"),
                             "p_key = b_key",
                             lp::JoinType::kInner)
                         .build();

  // The 1000-row build (~16KB) is well under the default 100MB limit, so it is
  // broadcast rather than hash-partitioned.
  {
    auto distributedPlan =
        planVelox(logicalPlan, {.numWorkers = 4, .numDrivers = 1}, options);
    auto matcher =
        matchScan("probe")
            .hashJoin(
                matchScan("build").broadcast().build(), core::JoinType::kInner)
            .gather()
            .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, matcher);
  }

  // Lowering the limit below the build size makes it ineligible for broadcast,
  // so both sides are hash-partitioned on the join key.
  {
    options.broadcastSizeLimit = 1024;
    auto distributedPlan =
        planVelox(logicalPlan, {.numWorkers = 4, .numDrivers = 1}, options);
    auto matcher = matchScan("probe")
                       .shuffle({"p_key"})
                       .hashJoin(
                           matchScan("build").shuffle({"b_key"}).build(),
                           core::JoinType::kInner)
                       .gather()
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, matcher);
  }
}

TEST_F(JoinTest, crossJoin) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()));
  testConnector_->addTable("v", ROW({"n", "m"}, BIGINT()));

  {
    auto ctx = makeContext();
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
    auto ctx = makeContext();
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
    auto ctx = makeContext();
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
    auto ctx = makeContext();
    auto logicalPlan = parseSelect(
        "SELECT a FROM t, (SELECT count(*) FROM u)", kTestConnectorId);

    auto matcher = matchScan("t").build();

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);

    ASSERT_NO_THROW(planVelox(logicalPlan));
  }

  {
    auto ctx = makeContext();
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
    auto ctx = makeContext();
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
        matchScan("t")
            .nestedLoopJoin(
                matchScan("u").aggregation().build(), core::JoinType::kLeft)
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
        matchScan("t")
            .aggregation()
            .nestedLoopJoin(
                matchScan("u").aggregation().build(), core::JoinType::kLeft)
            .build();

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);

    ASSERT_NO_THROW(planVelox(logicalPlan));
  }

  {
    auto logicalPlan = parseSelect(
        "SELECT a FROM t LEFT JOIN u ON 1 = 1 WHERE coalesce(x, 1) > 0",
        kTestConnectorId);

    auto matcher =
        matchScan("t")
            .nestedLoopJoin(matchScan("u").build(), core::JoinType::kLeft)
            .filter()
            .project()
            .build();

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);

    ASSERT_NO_THROW(planVelox(logicalPlan));
  }

  // Verify projection optimization on build side of cross join.
  // Only 'x' and 'y + 1 as z' are needed from build (not 'y').
  {
    auto query =
        "SELECT * FROM t LEFT JOIN (SELECT x, y + 1 as z FROM u) ON coalesce(a, x) > 0";
    SCOPED_TRACE(query);

    auto logicalPlan = parseSelect(query, kTestConnectorId);

    auto matcher = matchScan("t")
                       .nestedLoopJoin(
                           matchScan("u").project({"x", "y + 1"}).build(),
                           core::JoinType::kLeft)
                       .project()
                       .build();

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(JoinTest, rightJoin) {
  // Make t small and u large so optimizer swaps sides and uses right hash join.
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(
          100, {{"a", {.numDistinct = 100}}, {"b", {.numDistinct = 50}}});

  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()))
      ->setStats(1'000, {{"x", {.numDistinct = 100}}});

  // Only 'x' and 'y + 1 as z' should be projected from the subquery (not 'y').
  {
    auto query =
        "SELECT * FROM t LEFT JOIN (SELECT x, y + 1 as z FROM u) ON a = x";
    SCOPED_TRACE(query);

    auto logicalPlan = parseSelect(query, kTestConnectorId);

    auto matcher = matchScan("u")
                       .project({"x", "y + 1"})
                       .hashJoin(matchScan("t").build(), core::JoinType::kRight)
                       .project()
                       .build();

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matcher);
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
      matchScan("u")
          .nestedLoopJoin(matchScan("t").build())
          .hashJoin(
              matchValues().aggregation().build(), velox::core::JoinType::kLeft)
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

  auto matcher =
      matchScan("u")
          .hashJoin(matchScan("t").project({"coalesce(t0, 0)"}).build())
          .project()
          .build();

  auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(JoinTest, crossThanOrderBy) {
  auto query = "SELECT length(n_name) FROM nation, region ORDER BY 1";
  SCOPED_TRACE(query);

  auto matcher = matchScan("nation")
                     .nestedLoopJoin(matchScan("region").build())
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

    auto matcher = matchScan("t").filter().unnest().build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto query =
        "SELECT * FROM (VALUES row(row(1, 2))) as t(x), UNNEST(array[1,2,3]) WHERE x.field0 > 0";
    SCOPED_TRACE(query);

    auto matcher = matchValues().filter().project().unnest().project().build();

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

    auto matcher = matchScan("t")
                       .project()
                       .hashJoin(matchScan("u").project().build())
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto query = "SELECT * FROM (SELECT t0, 1 FROM t) JOIN u ON t0.a = u0.a";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .project()
                       .hashJoin(matchScan("u").project().build())
                       .project()
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(JoinTest, leftJoinOverValues) {
  auto query =
      "SELECT * FROM (VALUES 1, 2, 3, 4) as t(x) LEFT JOIN (VALUES 1, 2) as u(y) ON x = y";
  SCOPED_TRACE(query);

  auto matcher = matchValues()
                     .hashJoin(matchValues().build(), core::JoinType::kLeft)
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
  testConnector_->addTable("v", ROW({"p", "q"}, BIGINT()));

  // Post-LEFT JOIN filter that references only columns from the right-hand
  // (optional) table and doesn't eliminate NULLs.
  {
    auto query =
        "SELECT * FROM t LEFT JOIN (SELECT x, y + 1 as z FROM u) ON a = x "
        "WHERE coalesce(z, 1) > 0";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .hashJoin(
                           matchScan("u").project({"x", "y + 1"}).build(),
                           core::JoinType::kLeft)
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

    auto matcher = matchScan("t")
                       .hashJoin(
                           matchScan("u").filter("y + 1 > 0").build(),
                           core::JoinType::kInner)
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

    auto matcher = matchScan("t")
                       .hashJoin(
                           matchScan("u").filter("y + 1 > 0").build(),
                           core::JoinType::kInner)
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

  // Multi-table filter that references both sides of the join. Since the filter
  // references the right side with default null behavior, it eliminates NULLs
  // on the right side, converting the LEFT join to an INNER join. The filter
  // cannot be pushed down below the join since it references both sides.
  {
    auto query =
        "SELECT * FROM t LEFT JOIN (SELECT * FROM u) ON a = x "
        "WHERE a > y";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .hashJoin(matchScan("u").build(), core::JoinType::kInner)
                       .filter("a > y")
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // cardinality(coalesce(...)) wrapping a column from the optional side.
  // cardinality is translated as a subfield access. The subfield getter must
  // propagate non-default null behavior from coalesce so that the LEFT join
  // is not incorrectly converted to INNER.
  {
    testConnector_->addTable(
        "card_t", ROW({"a", "b"}, {BIGINT(), ARRAY(BIGINT())}));
    testConnector_->addTable(
        "card_u", ROW({"x", "y"}, {BIGINT(), ARRAY(BIGINT())}));

    auto query =
        "SELECT * FROM card_t LEFT JOIN card_u ON a = x "
        "WHERE cardinality(coalesce(y, b)) > 0";
    SCOPED_TRACE(query);

    auto matcher =
        matchScan("card_t")
            .hashJoin(matchScan("card_u").build(), core::JoinType::kLeft)
            .filter("cardinality(coalesce(y, b)) > 0")
            .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Two LEFT JOINs where coalesce(left_col, outer_col) appears in both the
  // second JOIN condition and the WHERE clause. When the conjunct is pushed
  // between DerivedTables, replaceInputs must preserve non-default null
  // behavior flags.
  {
    auto query =
        "SELECT t.a, coalesce(t.b, u.y) "
        "FROM t "
        "LEFT JOIN u ON t.a = u.x AND t.b IS NULL "
        "LEFT JOIN v ON coalesce(t.b, u.y) = v.p "
        "WHERE coalesce(t.b, u.y) IS NOT NULL";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .hashJoin(matchScan("u").build(), core::JoinType::kLeft)
                       .filter("not(is_null(coalesce(b, y)))")
                       .project()
                       .hashJoin(matchScan("v").build(), core::JoinType::kLeft)
                       .project()
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(JoinTest, fullThenFilter) {
  testConnector_->addTable("t", ROW({"a", "b", "c"}, BIGINT()));
  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()));

  // Post-FULL JOIN filter made of conjuncts that each reference only one side
  // of the join and do not eliminate NULLs.
  {
    auto query =
        "SELECT * FROM t FULL JOIN (SELECT x, y + 1 as z FROM u) ON a = x "
        "WHERE coalesce(z, 1) > 0 AND is_null(a)";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .hashJoin(
                           matchScan("u").project({"x", "y + 1 as z"}).build(),
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

    auto matcher = matchScan("t")
                       .filter("a > 0")
                       .hashJoin(
                           matchScan("u").project({"x", "y + 1 as z"}).build(),
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

  // The filter references both left and right sides with default null behavior.
  // It eliminates both left-only rows (NULLs on right) and right-only rows
  // (NULLs on left), converting FULL join directly to INNER join.
  {
    auto query =
        "SELECT * FROM t FULL JOIN (SELECT * FROM u) ON a = x "
        "WHERE a > y";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .hashJoin(matchScan("u").build(), core::JoinType::kInner)
                       .filter("a > y")
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

// Verifies that ON clause conjuncts referencing only the right (null-supplying)
// side of a LEFT JOIN are pushed down as filters on the right input.
TEST_F(JoinTest, leftJoinOnClausePushdown) {
  testConnector_->addTable("t", ROW({"a", "b", "c"}, BIGINT()));
  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()));

  // Right-side-only ON conjunct is pushed down as a filter on the right input.
  {
    auto query = "SELECT * FROM t LEFT JOIN u ON a = x AND y > 0";
    SCOPED_TRACE(query);

    auto matcher =
        matchScan("t")
            .hashJoin(
                matchScan("u").filter("y > 0").build(), core::JoinType::kLeft)
            .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Left-side-only ON conjunct is NOT pushed down. It stays in the join
  // condition.
  {
    auto query = "SELECT * FROM t LEFT JOIN u ON a = x AND b > 0";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .hashJoin(matchScan("u").build(), core::JoinType::kLeft)
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Both-sides ON conjunct is NOT pushed down.
  {
    auto query = "SELECT * FROM t LEFT JOIN u ON a = x AND b > y";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .hashJoin(matchScan("u").build(), core::JoinType::kLeft)
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Right-side-only ON conjunct pushed through a subquery DT.
  {
    auto query =
        "SELECT * FROM t LEFT JOIN (SELECT x, y + 1 as z FROM u) "
        "ON a = x AND z > 0";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .hashJoin(
                           matchScan("u")
                               .filter("y + 1 > 0")
                               .project({"x", "y + 1"})
                               .build(),
                           core::JoinType::kLeft)
                       .project()
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Right-side-only ON conjunct pushed into HAVING of a DT with aggregation.
  {
    auto query =
        "SELECT * FROM t LEFT JOIN (SELECT x, sum(y) as s FROM u GROUP BY x) "
        " ON a = x AND s > 10";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .hashJoin(
                           matchScan("u")
                               .singleAggregation({"x"}, {"sum(y) as s"})
                               .filter("s > 10")
                               .build(),
                           core::JoinType::kLeft)
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Right-side-only ON conjunct on a grouping key pushed below aggregation.
  {
    auto query =
        "SELECT * FROM t LEFT JOIN (SELECT x, sum(y) as s FROM u GROUP BY x) "
        "ON a = x AND x > 0";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .hashJoin(
                           matchScan("u")
                               .filter("x > 0")
                               .singleAggregation({"x"}, {"sum(y)"})
                               .build(),
                           core::JoinType::kLeft)
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Right-side-only ON conjunct NOT pushed below LIMIT. It stays in the join
  // condition.
  // TODO: Support pushing conjuncts below LIMIT by wrapping in a DT.
  {
    auto query =
        "SELECT * FROM t LEFT JOIN (SELECT x, y FROM u LIMIT 10) "
        " ON a = x AND y > 0";
    SCOPED_TRACE(query);

    auto matcher =
        matchScan("t")
            .hashJoin(matchScan("u").limit().build(), core::JoinType::kLeft)
            .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Constant false ON conjunct (1 > 2) means no right rows can ever match.
  // The join is eliminated entirely; left rows survive with NULL right columns.
  {
    auto query = "SELECT * FROM t LEFT JOIN u ON a = x AND 1 > 2";
    SCOPED_TRACE(query);

    // The optimal plan scans only 't' and projects NULLs for 'u' columns.
    auto matcher =
        matchScan("t").project({"a", "b", "c", "null", "null"}).build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Constant false ON conjunct (1 > 2) means no left rows can ever match.
  // The join is eliminated entirely; right rows survive with NULL left columns.
  {
    auto query = "SELECT * FROM t RIGHT JOIN u ON a = x AND 1 > 2";
    SCOPED_TRACE(query);

    // The optimal plan scans only 'u' and projects NULLs for 't' columns.
    auto matcher =
        matchScan("u").project({"null", "null", "null", "x", "y"}).build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(JoinTest, impliedJoins) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(
          10'000,
          {{"a", {.numDistinct = 10'000}}, {"b", {.numDistinct = 10'000}}});
  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()))
      ->setStats(
          1'000, {{"x", {.numDistinct = 10}}, {"y", {.numDistinct = 1'000}}});
  testConnector_->addTable("v", ROW({"n", "m"}, BIGINT()))
      ->setStats(100, {{"n", {.numDistinct = 100}}});

  // Transitive:

  // Transitive: t.a = u.x AND u.x = v.n implies t.a = v.n. With v being the
  // smallest table, the optimizer should join t with v first using the
  // implied join key.
  {
    auto query = "SELECT count(*) FROM t, u, v WHERE t.a = u.x AND u.x = v.n";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .hashJoin(matchScan("v").build(), core::JoinType::kInner)
                       .hashJoin(matchScan("u").build(), core::JoinType::kInner)
                       .aggregation()
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Same-table columns in one equivalence class: t.a = u.x AND t.a = t.b puts
  // t.a and t.b (both from t) in the same class. addImpliedJoins must skip
  // same-table pairs. t.a = t.b becomes a filter on t, and the redundant
  // implied key (t.b, u.x) is removed.
  {
    auto query = "SELECT count(*) FROM t JOIN u ON t.a = u.x AND t.a = t.b";
    SCOPED_TRACE(query);

    auto matcher =
        matchScan("u")
            .hashJoin(
                matchScan("t").filter("a = b").build(), core::JoinType::kInner)
            .aggregation()
            .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Same-table equality dedup: ON t.a = u.x AND t.b = u.x merges {t.a, t.b,
  // u.x}, so an explicit WHERE t.a = t.b becomes the same interned Call as
  // the inferred one and is added only once.
  {
    auto query =
        "SELECT count(*) FROM t JOIN u ON t.a = u.x AND t.b = u.x WHERE t.a = t.b";
    SCOPED_TRACE(query);

    auto matcher =
        matchScan("u")
            .hashJoin(
                matchScan("t").filter("a = b").build(), core::JoinType::kInner)
            .aggregation()
            .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto query = "SELECT count(*) FROM t JOIN u ON t.a = u.x AND t.b = u.x";
    SCOPED_TRACE(query);

    auto matcher =
        matchScan("u")
            .hashJoin(
                matchScan("t").filter("a = b").build(), core::JoinType::kInner)
            .aggregation()
            .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Separate equivalence classes: no same-table filter.
  {
    auto query = "SELECT count(*) FROM t JOIN u ON t.a = u.x AND t.b = u.y";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .hashJoin(matchScan("u").build(), core::JoinType::kInner)
                       .aggregation()
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // LEFT JOIN: no same-table filter on preserved side; both keys preserved.
  {
    auto query =
        "SELECT count(*) FROM t LEFT JOIN u ON t.a = u.x AND t.b = u.x";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .hashJoin(matchScan("u").build(), core::JoinType::kLeft)
                       .aggregation()
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Three tables with same-table filter on t.
  {
    auto query =
        "SELECT count(*) FROM t "
        "JOIN u ON t.a = u.x AND t.b = u.x "
        "JOIN v ON u.x = v.n";
    SCOPED_TRACE(query);

    auto matcher = matchScan("u")
                       .hashJoin(
                           matchScan("v")
                               .hashJoin(
                                   matchScan("t").filter("a = b").build(),
                                   core::JoinType::kInner)
                               .build(),
                           core::JoinType::kInner)
                       .aggregation()
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // t.a = u.x creates equivalence class {t.a, u.x}. The IN subquery
  // produces a semi-join on u.x → v.n. The implied edge t.a → v.n has
  // fanout 100/10000 = 0.01, so the semi-join is placed before the inner
  // join with u.
  {
    auto query =
        "SELECT * FROM t, u "
        "WHERE t.a = u.x AND u.x IN (SELECT n FROM v)";
    SCOPED_TRACE(query);

    auto matcher =
        matchScan("t")
            .hashJoin(matchScan("v").build(), core::JoinType::kLeftSemiFilter)
            .hashJoin(matchScan("u").build(), core::JoinType::kInner)
            .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Inner join is on t.b = u.y (not equivalent to u.x). No implied edge,
  // so the semi-join stays on u and is placed after the inner join.
  {
    auto query =
        "SELECT * FROM t, u "
        "WHERE t.b = u.y AND u.x IN (SELECT n FROM v)";
    SCOPED_TRACE(query);

    auto matcher =
        matchScan("t")
            .hashJoin(matchScan("u").build(), core::JoinType::kInner)
            .hashJoin(matchScan("v").build(), core::JoinType::kLeftSemiFilter)
            .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

// Implied join edges from an equivalence class must not reference tables
// outside a subset DT's tableSet.
TEST_F(JoinTest, impliedJoinChain) {
  testConnector_->addTable("t1", ROW({"id1", "data1"}, BIGINT()))
      ->setStats(1'000'000, {{"id1", {.numDistinct = 1'000'000}}});
  testConnector_->addTable("t2", ROW({"id2", "data2"}, BIGINT()))
      ->setStats(100, {{"id2", {.numDistinct = 100}}});
  testConnector_->addTable("t3", ROW({"id3", "data3"}, BIGINT()))
      ->setStats(100, {{"id3", {.numDistinct = 100}}});
  testConnector_->addTable("t4", ROW({"id4", "data4"}, BIGINT()))
      ->setStats(100, {{"id4", {.numDistinct = 100}}});

  auto ctx = makeContext();
  auto logicalPlan = lp::PlanBuilder{ctx}
                         .tableScan("t1")
                         .join(
                             lp::PlanBuilder{ctx}.tableScan("t2"),
                             "id1 = id2",
                             lp::JoinType::kInner)
                         .join(
                             lp::PlanBuilder{ctx}.tableScan("t3"),
                             "id2 = id3",
                             lp::JoinType::kInner)
                         .join(
                             lp::PlanBuilder{ctx}.tableScan("t4"),
                             "id3 = id4",
                             lp::JoinType::kInner)
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchScan("t3")
                     .hashJoin(matchScan("t1")
                                   .hashJoin(matchScan("t2").build())
                                   .hashJoin(matchScan("t4").build())
                                   .build())
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(JoinTest, impliedFilters) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(10'000, {{"a", {.numDistinct = 10'000}}});
  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()))
      ->setStats(1'000, {{"x", {.numDistinct = 100}}});
  testConnector_->addTable("v", ROW({"k", "m"}, BIGINT()))
      ->setStats(100, {{"k", {.numDistinct = 100}}});

  // Equality propagates from t.a to u.x via the join equivalence.
  {
    auto query = "SELECT * FROM t, u WHERE t.a = u.x AND t.a = 5";
    SCOPED_TRACE(query);

    auto matcher =
        matchScan("u")
            .filter("x = 5")
            .hashJoin(
                matchScan("t").filter("a = 5").build(), core::JoinType::kInner)
            .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Range propagates similarly.
  {
    auto query = "SELECT * FROM t, u WHERE t.a = u.x AND t.a > 100";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .filter("a > 100")
                       .hashJoin(
                           matchScan("u").filter("x > 100").build(),
                           core::JoinType::kInner)
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // IN propagates across the equivalence.
  {
    auto query = "SELECT * FROM t, u WHERE t.a = u.x AND t.a IN (1, 2, 3)";
    SCOPED_TRACE(query);

    auto matcher = matchScan("u")
                       .filter("x IN (1, 2, 3)")
                       .hashJoin(
                           matchScan("t").filter("a IN (1, 2, 3)").build(),
                           core::JoinType::kInner)
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // IS NULL propagates across the equivalence. Sound under inner-join
  // semantics: t.a = u.x already rejects NULL on both sides, so adding
  // u.x IS NULL on u (and the original t.a IS NULL on t) does not change the
  // result, but lets the storage layer prune NULL rows before the join.
  {
    auto query = "SELECT * FROM t, u WHERE t.a = u.x AND t.a IS NULL";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .filter("a IS NULL")
                       .hashJoin(
                           matchScan("u").filter("x IS NULL").build(),
                           core::JoinType::kInner)
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // IS NOT NULL likewise propagates.
  {
    auto query = "SELECT * FROM t, u WHERE t.a = u.x AND t.a IS NOT NULL";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .filter("a IS NOT NULL")
                       .hashJoin(
                           matchScan("u").filter("x IS NOT NULL").build(),
                           core::JoinType::kInner)
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Non-deterministic predicates do not propagate.
  {
    auto query =
        "SELECT * FROM t, u "
        "WHERE t.a = u.x AND t.a = cast(random() * 100 as bigint)";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .hashJoin(matchScan("u").build(), core::JoinType::kInner)
                       .filter("a = cast(random() * 100.0 as bigint)")
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Predicates already present on a sibling column should not be re-pushed.
  // Both t.a = 5 and u.x = 5 exist before inference; inference sees that
  // u.x = 5 is already on u (sameOrEqual finds it) and skips the redundant
  // push, and likewise for the reverse direction.
  {
    auto query = "SELECT * FROM t, u WHERE t.a = u.x AND t.a = 5 AND u.x = 5";
    SCOPED_TRACE(query);

    auto matcher =
        matchScan("u")
            .filter("x = 5")
            .hashJoin(
                matchScan("t").filter("a = 5").build(), core::JoinType::kInner)
            .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Three-table chain: t.a = u.x AND u.x = v.k puts t.a, u.x, v.k in one
  // equivalence class. A filter on t.a propagates to both u.x and v.k.
  {
    auto query =
        "SELECT * FROM t, u, v "
        "WHERE t.a = u.x AND u.x = v.k AND t.a = 5";
    SCOPED_TRACE(query);

    auto matcher =
        matchScan("u")
            .filter("x = 5")
            .hashJoin(
                matchScan("t").filter("a = 5").build(), core::JoinType::kInner)
            .hashJoin(
                matchScan("v").filter("k = 5").build(), core::JoinType::kInner)
            .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

// Three or more columns from the same table in one equivalence class.
TEST_F(JoinTest, impliedSameTableEquality) {
  testConnector_->addTable("t", ROW({"a", "b", "c"}, BIGINT()));
  testConnector_->addTable("u", ROW({"x"}, BIGINT()));

  // Three same-table columns produce a = b and a = c filters.
  {
    auto query =
        "SELECT count(*) FROM t "
        "JOIN u ON t.a = u.x AND t.b = u.x AND t.c = u.x";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .filter("a = b AND a = c")
                       .hashJoin(matchScan("u").build(), core::JoinType::kInner)
                       .aggregation()
                       .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

// Equivalence class with same-table columns on both sides of the join.
TEST_F(JoinTest, impliedSameTableEqualityBothSides) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(10'000, {{"a", {.numDistinct = 10'000}}});
  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()))
      ->setStats(1'000, {{"x", {.numDistinct = 10}}});

  // t.a = u.x AND t.b = u.x AND t.a = u.y creates equivalence class
  // {t.a, t.b, u.x, u.y}. Pushes a = b on t and x = y on u.
  {
    auto query =
        "SELECT count(*) FROM t "
        "JOIN u ON t.a = u.x AND t.b = u.x AND t.a = u.y";
    SCOPED_TRACE(query);

    auto matcher =
        matchScan("t")
            .filter("a = b")
            .hashJoin(
                matchScan("u").filter("x = y").build(), core::JoinType::kInner)
            .aggregation()
            .build();

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

// With GROUP BY a, b, the synthesized a = b references only grouping keys
// and is pushed below the aggregation onto t's scan.
TEST_F(JoinTest, impliedSameTableEqualityBelowAggregation) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  testConnector_->addTable("u", ROW({"x"}, BIGINT()));

  auto query =
      "SELECT count(*) FROM (SELECT a, b FROM t GROUP BY a, b) AS gb "
      "JOIN u ON gb.a = u.x AND gb.b = u.x";
  SCOPED_TRACE(query);

  auto matcher = matchScan("t")
                     .filter("a = b")
                     .singleAggregation({"a", "b"}, {})
                     .project()
                     .hashJoin(matchScan("u").build(), core::JoinType::kInner)
                     .aggregation()
                     .build();

  auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// Synthesis into HAVING: when an equivalence class member is an aggregate
// output, the implied equality can't push below the aggregation and stays
// as a post-aggregation Filter (HAVING).
TEST_F(JoinTest, impliedSameTableEqualityInHaving) {
  testConnector_->addTable("t", ROW({"k"}, BIGINT()))
      ->setStats(10'000, {{"k", {.numDistinct = 10'000}}});
  testConnector_->addTable("u", ROW({"x"}, BIGINT()))
      ->setStats(1'000, {{"x", {.numDistinct = 10}}});

  auto query =
      "SELECT count(*) FROM (SELECT k, COUNT(*) AS c FROM t GROUP BY k) AS agg "
      "JOIN u ON agg.k = u.x AND agg.c = u.x";
  SCOPED_TRACE(query);

  auto matcher = matchScan("u")
                     .hashJoin(
                         matchScan("t")
                             .singleAggregation({"k"}, {"count(*) as c"})
                             .filter("k = c")
                             .project()
                             .build(),
                         core::JoinType::kInner)
                     .aggregation()
                     .build();

  auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// With a LIMIT in the way, the synthesized equality lands as a Filter
// above the Limit. The redundant join key is still dropped.
TEST_F(JoinTest, impliedSameTableEqualityBlockedByLimit) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  testConnector_->addTable("u", ROW({"x"}, BIGINT()));

  auto query =
      "SELECT count(*) FROM (SELECT a, b FROM t LIMIT 100) AS lim "
      "JOIN u ON lim.a = u.x AND lim.b = u.x";
  SCOPED_TRACE(query);

  auto matcher = matchScan("t")
                     .limit()
                     .filter("a = b")
                     .hashJoin(matchScan("u").build(), core::JoinType::kInner)
                     .aggregation()
                     .build();

  auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// Explicit WHERE and synthesized equality converge on the same LIMIT DT
// target. The filter appears exactly once above the Limit.
TEST_F(JoinTest, impliedSameTableEqualityBlockedByLimitDedup) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  testConnector_->addTable("u", ROW({"x"}, BIGINT()));

  auto query =
      "SELECT count(*) FROM (SELECT a, b FROM t LIMIT 100) AS lim "
      "JOIN u ON lim.a = u.x AND lim.b = u.x "
      "WHERE lim.a = lim.b";
  SCOPED_TRACE(query);

  auto matcher = matchScan("t")
                     .limit()
                     .filter("a = b")
                     .hashJoin(matchScan("u").build(), core::JoinType::kInner)
                     .aggregation()
                     .build();

  auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// The null-supplying (right) side of a LEFT join gets same-table equality
// filters when multiple ON-clause conditions pair different right-side
// columns with the same left-side column. For t LEFT JOIN u ON u.x = t.a
// AND u.y = t.a, any u row in the output must satisfy both conditions, so
// u.x = u.y holds.
TEST_F(JoinTest, impliedSameTableEqualityOuterJoin) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(10'000, {{"a", {.numDistinct = 10'000}}});
  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()))
      ->setStats(1'000, {{"x", {.numDistinct = 10}}});

  // u.x = t.a AND u.y = t.a implies u.x = u.y, which can be pushed
  // to u's scan since unmatched u rows never appear in the result.
  auto query = "SELECT count(*) FROM t LEFT JOIN u ON u.x = t.a AND u.y = t.a";
  SCOPED_TRACE(query);

  auto matcher =
      matchScan("t")
          .hashJoin(
              matchScan("u").filter("x = y").build(), core::JoinType::kLeft)
          .aggregation()
          .build();

  auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// SEMI join (EXISTS) gets slot-pattern same-table eq synthesis on its
// subquery side: surviving left rows require a matching right row, so the
// same-table eq on the right side is sound.
TEST_F(JoinTest, impliedSameTableEqualitySemiJoin) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(10'000, {{"a", {.numDistinct = 10'000}}});
  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()))
      ->setStats(1'000, {{"x", {.numDistinct = 10}}});

  auto query =
      "SELECT count(*) FROM t WHERE EXISTS ("
      "  SELECT 1 FROM u WHERE u.x = t.a AND u.y = t.a)";
  SCOPED_TRACE(query);

  auto matcher = matchScan("t")
                     .hashJoin(
                         matchScan("u").filter("x = y").build(),
                         core::JoinType::kLeftSemiFilter)
                     .aggregation()
                     .build();

  auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// RIGHT JOIN is normalized to LEFT JOIN; same-table synthesis fires on
// the (now-right) u side, producing u.x = u.y.
TEST_F(JoinTest, impliedSameTableEqualityRightJoinNormalized) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(10'000, {{"a", {.numDistinct = 10'000}}});
  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()))
      ->setStats(
          1'000, {{"x", {.numDistinct = 10}}, {"y", {.numDistinct = 1'000}}});

  auto query = "SELECT count(*) FROM u RIGHT JOIN t ON u.x = t.a AND u.y = t.a";
  SCOPED_TRACE(query);

  auto matcher =
      matchScan("t")
          .hashJoin(
              matchScan("u").filter("x = y").build(), core::JoinType::kLeft)
          .aggregation()
          .build();

  auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// FULL OUTER slot synthesis is unsound on either side (unmatched rows on
// both sides survive with NULLs; a filter would drop them). u must NOT
// get u.x = u.y.
TEST_F(JoinTest, impliedSameTableEqualityFullOuterJoinSkipped) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(10'000, {{"a", {.numDistinct = 10'000}}});
  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()))
      ->setStats(1'000, {{"x", {.numDistinct = 10}}});

  auto query =
      "SELECT count(*) FROM t FULL OUTER JOIN u ON u.x = t.a AND u.y = t.a";
  SCOPED_TRACE(query);

  // u must have no filter — slot synthesis is unsound for FULL OUTER.
  auto matcher = matchScan("t")
                     .hashJoin(matchScan("u").build(), core::JoinType::kFull)
                     .aggregation()
                     .build();

  auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// Same-table equalities must NOT be inferred for the null-supplying side
// when the right-side keys are paired with *different* left-side keys. For
// t LEFT JOIN u ON t.a = u.x AND t.b = u.y, u.x = u.y does not follow
// because t.a and t.b can differ.
TEST_F(JoinTest, impliedSameTableEqualityMismatchedLeftKeys) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(10'000, {{"a", {.numDistinct = 10'000}}});
  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()))
      ->setStats(1'000, {{"x", {.numDistinct = 10}}});

  auto query = "SELECT count(*) FROM t LEFT JOIN u ON t.a = u.x AND t.b = u.y";
  SCOPED_TRACE(query);

  // u must have no filter — u.x = u.y must not be inferred.
  auto matcher = matchScan("t")
                     .hashJoin(matchScan("u").build(), core::JoinType::kLeft)
                     .aggregation()
                     .build();

  auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// Same-table equalities must NOT be pushed to the preserved (left) side of a
// left join. t rows can appear in the output without matching any u row, so
// t.a = t.b cannot be inferred from ON t.a = u.x AND t.b = u.x.
TEST_F(JoinTest, impliedSameTableEqualityPreservedSide) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(10'000, {{"a", {.numDistinct = 10'000}}});
  testConnector_->addTable("u", ROW({"x"}, BIGINT()))
      ->setStats(1'000, {{"x", {.numDistinct = 10}}});

  auto query = "SELECT count(*) FROM t LEFT JOIN u ON t.a = u.x AND t.b = u.x";
  SCOPED_TRACE(query);

  // t must have no filter — t.a = t.b must not be inferred.
  auto matcher = matchScan("t")
                     .hashJoin(matchScan("u").build(), core::JoinType::kLeft)
                     .aggregation()
                     .build();

  auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// LEFT JOIN with no equalities when the DT has 3+ tables. The comma join
// between nation and region is inlined into the same DT. The EXISTS semi-join
// adds a 3rd table. The subsequent LEFT JOIN ON clause uses contains() which
// has no equalities, so leftTables must be inferred from the DT.
TEST_F(JoinTest, leftJoinNoEqualitiesMultipleTables) {
  auto query =
      "WITH base AS ("
      "   SELECT n_nationkey, n_name "
      "   FROM nation, region "
      "   WHERE n_regionkey = r_regionkey"
      "), "
      "with_exists AS ("
      "   SELECT *, "
      "       EXISTS ("
      "           SELECT 1 FROM customer WHERE c_nationkey = n_nationkey"
      "       ) AS has_customer "
      "   FROM base"
      ") "
      "SELECT * FROM with_exists LEFT JOIN supplier ON s_nationkey > n_nationkey";
  SCOPED_TRACE(query);

  auto matcher =
      matchScan("nation")
          .hashJoin(matchScan("region").build(), core::JoinType::kInner)
          .hashJoin(
              matchScan("customer").build(),
              core::JoinType::kLeftSemiProject,
              {.nullAware = false})
          .project()
          .nestedLoopJoin(matchScan("supplier").build(), core::JoinType::kLeft)
          .build();

  auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// LEFT-to-INNER JOIN conversion with aggregation. replaceJoinOutputs must not
// replace post-aggregation references (exprs) with pre-aggregation expressions.
TEST_F(JoinTest, leftToInnerWithAggregation) {
  testConnector_->addTable("t", ROW("a", INTEGER()));
  testConnector_->addTable("u", ROW({"x", "y"}, {INTEGER(), DOUBLE()}));

  // The WHERE filter on b.x triggers LEFT-to-INNER conversion. The DISTINCT
  // creates an aggregation whose grouping key is the join output column for
  // CAST(y AS REAL). replaceJoinOutputs must not replace the post-aggregation
  // reference in exprs with the raw pre-aggregation expression.
  auto query =
      "SELECT DISTINCT b.c "
      "FROM t "
      "LEFT JOIN ("
      "    SELECT x, CAST(y AS REAL) AS c FROM u"
      "    CROSS JOIN UNNEST(ARRAY[1]) AS v(n)"
      ") AS b ON t.a = b.x "
      "WHERE b.x > 0";
  SCOPED_TRACE(query);

  auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));

  auto matcher = matchScan("t")
                     .hashJoin(
                         matchScan("u")
                             .filter("x > 0")
                             .project()
                             .unnest()
                             .project()
                             .build(),
                         core::JoinType::kInner)
                     .project({"cast(y as REAL) as c"})
                     .distinct()
                     .project()
                     .build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

// Two aliases of the same source column (b AS x, b AS y) from a LEFT JOIN.
// addJoinColumns must deduplicate and produce only one join output column.
TEST_F(JoinTest, duplicateJoinOutputColumns) {
  testConnector_->addTable("t", ROW("k", INTEGER()));
  testConnector_->addTable("u", ROW({"k", "a", "b"}, INTEGER()));

  // Bare SELECT: the join outputs a single column for b, a Project above
  // produces both x and y from it.
  {
    auto query =
        "SELECT x, y "
        "FROM t "
        "LEFT JOIN ("
        "    SELECT k, b AS x, b AS y FROM u"
        ") AS s ON t.k = s.k";
    SCOPED_TRACE(query);

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));

    auto matcher = matchScan("t")
                       .hashJoin(matchScan("u").build(), core::JoinType::kLeft)
                       .project({"b as x", "b as y"})
                       .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // DISTINCT: the aggregation must not have duplicate grouping keys.
  {
    auto query =
        "SELECT DISTINCT s.x, s.y "
        "FROM t "
        "LEFT JOIN ("
        "    SELECT k, b AS x, b AS y FROM u"
        ") AS s ON t.k = s.k";
    SCOPED_TRACE(query);

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));

    auto matcher = matchScan("t")
                       .hashJoin(matchScan("u").build(), core::JoinType::kLeft)
                       .distinct()
                       .project({"b as x", "b as y"})
                       .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // DISTINCT + WHERE that converts LEFT to INNER.
  {
    auto query =
        "SELECT DISTINCT s.x, s.y "
        "FROM t "
        "LEFT JOIN ("
        "    SELECT k, a, b AS x, b AS y FROM u"
        ") AS s ON t.k = s.k "
        "WHERE s.a = 1";
    SCOPED_TRACE(query);

    auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));

    auto matcher =
        matchScan("t")
            .hashJoin(
                matchScan("u").filter("a = 1").build(), core::JoinType::kInner)
            .distinct()
            .project({"b as x", "b as y"})
            .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(JoinTest, greedyJoinOrder) {
  testConnector_->addTable("t1", ROW({"id1", "data1"}, BIGINT()))
      ->setStats(100, {{"id1", {.numDistinct = 100}}});
  testConnector_->addTable("t2", ROW({"id2", "data2"}, BIGINT()))
      ->setStats(200, {{"id2", {.numDistinct = 200}}});
  testConnector_->addTable("t3", ROW({"id3", "data3"}, BIGINT()))
      ->setStats(400, {{"id3", {.numDistinct = 400}}});
  testConnector_->addTable("t4", ROW({"id4", "data4"}, BIGINT()))
      ->setStats(800, {{"id4", {.numDistinct = 800}}});

  auto ctx = makeContext();
  auto logicalPlan = lp::PlanBuilder{ctx}
                         .tableScan("t1")
                         .join(
                             lp::PlanBuilder{ctx}.tableScan("t2"),
                             "id1 = id2",
                             lp::JoinType::kInner)
                         .join(
                             lp::PlanBuilder{ctx}.tableScan("t3"),
                             "id2 = id3",
                             lp::JoinType::kInner)
                         .join(
                             lp::PlanBuilder{ctx}.tableScan("t4"),
                             "id3 = id4",
                             lp::JoinType::kInner)
                         .build();

  {
    SCOPED_TRACE("greedy fires (threshold=4)");
    optimizerOptions_.greedyJoinThreshold = 4;
    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher = matchScan("t4")
                       .hashJoin(matchScan("t1").build())
                       .hashJoin(matchScan("t2").build())
                       .hashJoin(matchScan("t3").build())
                       .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    SCOPED_TRACE("branch-and-bound (threshold=100)");
    optimizerOptions_.greedyJoinThreshold = 100;
    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher =
        matchScan("t4")
            .hashJoin(matchScan("t3")
                          .hashJoin(matchScan("t2")
                                        .hashJoin(matchScan("t1").build())
                                        .build())
                          .build())
            .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(JoinTest, greedyDtStart) {
  testConnector_->addTable("t", ROW({"a", "b"}, {BIGINT(), BIGINT()}))
      ->setStats(1000, {});
  testConnector_->addTable("base1", ROW({"id1", "x1"}, {BIGINT(), BIGINT()}))
      ->setStats(500, {});

  auto ctx = makeContext();
  auto logicalPlan = lp::PlanBuilder{ctx}
                         .tableScan("t")
                         .aggregate({"a"}, {"sum(b) as s"})
                         .join(
                             lp::PlanBuilder{ctx}.tableScan("base1"),
                             "a = id1",
                             lp::JoinType::kLeft)
                         .build();

  optimizerOptions_.greedyJoinThreshold = 2;
  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher =
      matchScan("t")
          .singleAggregation({"a"}, {"sum(b) as s"})
          .hashJoin(matchScan("base1").build(), core::JoinType::kLeft)
          .project()
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(JoinTest, greedySnowflakeLeftDeep) {
  testConnector_->addTable("fact", ROW({"fact_a", "fact_b"}, BIGINT()))
      ->setStats(
          10'000'000,
          {{"fact_a", {.numDistinct = 100}}, {"fact_b", {.numDistinct = 100}}});
  testConnector_->addTable("dim_a", ROW({"da_key", "da_sub"}, BIGINT()))
      ->setStats(
          100,
          {{"da_key", {.numDistinct = 100}}, {"da_sub", {.numDistinct = 100}}});
  testConnector_->addTable("sub_a", ROW({"sa_key"}, BIGINT()))
      ->setStats(10, {{"sa_key", {.numDistinct = 10}}});
  testConnector_->addTable("dim_b", ROW({"db_key", "db_sub"}, BIGINT()))
      ->setStats(
          100,
          {{"db_key", {.numDistinct = 100}}, {"db_sub", {.numDistinct = 100}}});
  testConnector_->addTable("sub_b", ROW({"sb_key"}, BIGINT()))
      ->setStats(10, {{"sb_key", {.numDistinct = 10}}});

  auto ctx = makeContext();
  auto logicalPlan = lp::PlanBuilder{ctx}
                         .tableScan("fact")
                         .join(
                             lp::PlanBuilder{ctx}.tableScan("dim_a"),
                             "fact_a = da_key",
                             lp::JoinType::kInner)
                         .join(
                             lp::PlanBuilder{ctx}.tableScan("sub_a"),
                             "da_sub = sa_key",
                             lp::JoinType::kInner)
                         .join(
                             lp::PlanBuilder{ctx}.tableScan("dim_b"),
                             "fact_b = db_key",
                             lp::JoinType::kInner)
                         .join(
                             lp::PlanBuilder{ctx}.tableScan("sub_b"),
                             "db_sub = sb_key",
                             lp::JoinType::kInner)
                         .build();

  optimizerOptions_.greedyJoinThreshold = 5;
  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchScan("fact")
                     .hashJoin(matchScan("dim_a").build())
                     .hashJoin(matchScan("sub_a").build())
                     .hashJoin(matchScan("dim_b").build())
                     .hashJoin(matchScan("sub_b").build())
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(JoinTest, greedyValuesStart) {
  testConnector_->addTable("v_t1", ROW({"a", "b"}, BIGINT()))
      ->setStats(1000, {{"a", {.numDistinct = 1000}}});
  testConnector_->addTable("v_t2", ROW({"c", "d"}, BIGINT()))
      ->setStats(2000, {{"c", {.numDistinct = 2000}}});
  testConnector_->addTable("v_t3", ROW({"e", "f"}, BIGINT()))
      ->setStats(4000, {{"e", {.numDistinct = 4000}}});

  auto rowVector = makeRowVector({"v"}, {makeFlatVector<int64_t>({1, 2, 3})});

  auto ctx = makeContext();
  auto logicalPlan = lp::PlanBuilder{ctx}
                         .values({rowVector})
                         .join(
                             lp::PlanBuilder{ctx}.tableScan("v_t1"),
                             "v = a",
                             lp::JoinType::kInner)
                         .join(
                             lp::PlanBuilder{ctx}.tableScan("v_t2"),
                             "a = c",
                             lp::JoinType::kInner)
                         .join(
                             lp::PlanBuilder{ctx}.tableScan("v_t3"),
                             "c = e",
                             lp::JoinType::kInner)
                         .build();

  optimizerOptions_.greedyJoinThreshold = 2;
  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchScan("v_t3")
                     .hashJoin(core::PlanMatcherBuilder{}.values().build())
                     .hashJoin(matchScan("v_t1").build())
                     .hashJoin(matchScan("v_t2").build())
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

} // namespace
} // namespace facebook::axiom::optimizer
