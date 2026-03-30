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
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

class SetTest : public test::HiveQueriesTestBase {
 protected:
  static void SetUpTestCase() {
    test::HiveQueriesTestBase::SetUpTestCase();
    createTpchTables(
        {velox::tpch::Table::TBL_NATION,
         velox::tpch::Table::TBL_PART,
         velox::tpch::Table::TBL_PARTSUPP});
  }
};

TEST_F(SetTest, unionAll) {
  auto nationType = getSchema("nation");

  const std::vector<std::string>& names = nationType->names();

  lp::PlanBuilder::Context ctx{exec::test::kHiveConnectorId, kDefaultSchema};
  auto t1 = lp::PlanBuilder(ctx)
                .tableScan("nation", names)
                .filter("n_nationkey < 11");
  auto t2 = lp::PlanBuilder(ctx)
                .tableScan("nation", names)
                .filter("n_nationkey > 13");

  auto logicalPlan = t1.unionAll(t2)
                         .project({"n_regionkey + 1 as rk"})
                         .filter("rk % 3 = 1")
                         .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder()
                       .hiveScan(
                           "nation",
                           test::lte("n_nationkey", 10),
                           "(n_regionkey + 1) % 3 = 1")
                       .localPartition(
                           core::PlanMatcherBuilder()
                               .hiveScan(
                                   "nation",
                                   test::gte("n_nationkey", 14),
                                   "(n_regionkey + 1) % 3 = 1")
                               .project()
                               .build())
                       .project()
                       .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  auto referencePlan = exec::test::PlanBuilder(pool_.get())
                           .tableScan("nation", nationType)
                           .filter("n_nationkey < 11 or n_nationkey > 13")
                           .project({"n_regionkey + 1 as rk"})
                           .filter("rk % 3 = 1")
                           .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(SetTest, lambdaFilterPushdownThroughUnionAll) {
  testConnector_->addTable("t", ROW({"a", "b"}, ARRAY(BIGINT())));
  testConnector_->addTable("u", ROW({"a", "b"}, ARRAY(BIGINT())));

  lp::PlanBuilder::Context ctx(kTestConnectorId, kDefaultSchema);
  auto logicalPlan = lp::PlanBuilder(ctx)
                         .tableScan("t")
                         .unionAll(lp::PlanBuilder(ctx).tableScan("u"))
                         .filter("all_match(a, x -> x > 1)")
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchScan("t")
                     .filter()
                     .localPartition(matchScan("u").filter().project().build())
                     .build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(SetTest, unionJoin) {
  auto partType = ROW({"p_partkey", "p_retailprice"}, {BIGINT(), DOUBLE()});
  auto partSuppType = ROW({"ps_partkey", "ps_availqty"}, {BIGINT(), INTEGER()});

  lp::PlanBuilder::Context ctx(exec::test::kHiveConnectorId, kDefaultSchema);
  auto ps1 = lp::PlanBuilder(ctx)
                 .tableScan("partsupp", {"ps_partkey", "ps_availqty"})
                 .filter("ps_availqty < 1000::int")
                 .project({"ps_partkey"});

  auto ps2 = lp::PlanBuilder(ctx)
                 .tableScan("partsupp", {"ps_partkey", "ps_availqty"})
                 .filter("ps_availqty > 2000::int")
                 .project({"ps_partkey"});

  auto ps3 = lp::PlanBuilder(ctx)
                 .tableScan("partsupp", {"ps_partkey", "ps_availqty"})
                 .filter("ps_availqty between 1200::int and 1400::int")
                 .project({"ps_partkey"});

  // The shape of the partsupp union is ps1 union all (ps2 union all ps3). We
  // verify that a stack of multiple set ops works.
  auto psu2 = ps2.unionAll(ps3);

  auto p1 = lp::PlanBuilder(ctx)
                .tableScan("part", {"p_partkey", "p_retailprice"})
                .filter("p_retailprice < 1100.0");

  auto p2 = lp::PlanBuilder(ctx)
                .tableScan("part", {"p_partkey", "p_retailprice"})
                .filter("p_retailprice > 1200.0");

  auto logicalPlan =
      ps1.unionAll(psu2)
          .join(p1.unionAll(p2), "ps_partkey = p_partkey", lp::JoinType::kInner)
          .aggregate({}, {"sum(1)"})
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher =
        core::PlanMatcherBuilder()
            .hiveScan("partsupp", test::lte("ps_availqty", 999))
            .localPartition({
                core::PlanMatcherBuilder()
                    .hiveScan("partsupp", test::gte("ps_availqty", 2001))
                    .project()
                    .build(),
                core::PlanMatcherBuilder()
                    .hiveScan(
                        "partsupp", test::between("ps_availqty", 1200, 1400))
                    .project()
                    .build(),
            })
            .hashJoin(
                core::PlanMatcherBuilder()
                    .hiveScan("part", test::lt("p_retailprice", 1100.0))
                    .localPartition(
                        core::PlanMatcherBuilder()
                            .hiveScan("part", test::gt("p_retailprice", 1200.0))
                            .project()
                            .build())
                    .build(),
                core::JoinType::kInner)
            .singleAggregation({}, {"sum(1)"})
            .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  auto idGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto referencePlan =
      exec::test::PlanBuilder(idGenerator)
          .tableScan("partsupp", partSuppType)
          .filter(
              "ps_availqty < 1000::int or ps_availqty > 2000::int "
              "or ps_availqty between 1200::int and 1400::int")
          .hashJoin(
              {"ps_partkey"},
              {"p_partkey"},
              exec::test::PlanBuilder(idGenerator)
                  .tableScan("part", partType)
                  .filter("p_retailprice < 1100.0 or p_retailprice > 1200.0")
                  .planNode(),
              "",
              {"p_partkey"})
          .project({"p_partkey"})
          .localPartition({})
          .singleAggregation({}, {"sum(1)"})
          .planNode();

  checkSame(logicalPlan, referencePlan);
}

// Checks
// - UNION ALL of two UNION ALL (should be flatten)
// - UNION ALL of two UNION (shouldn't be flatten)
// - UNION of two UNION ALL (should be flatten)
// - UNION of two UNION (should be flatten)
TEST_F(SetTest, unionFlatten) {
  auto nationType = getSchema("nation");

  const std::vector<std::string>& names = nationType->names();

  for (auto [rootType, leftType, rightType] : {
           std::tuple{
               lp::SetOperation::kUnion,
               lp::SetOperation::kUnion,
               lp::SetOperation::kUnion,
           },
           {
               lp::SetOperation::kUnion,
               lp::SetOperation::kUnionAll,
               lp::SetOperation::kUnionAll,
           },
           {
               lp::SetOperation::kUnionAll,
               lp::SetOperation::kUnion,
               lp::SetOperation::kUnion,
           },
           {
               lp::SetOperation::kUnionAll,
               lp::SetOperation::kUnionAll,
               lp::SetOperation::kUnionAll,
           },

       }) {
    lp::PlanBuilder::Context ctx{exec::test::kHiveConnectorId, kDefaultSchema};
    auto makeT1 = [&] {
      return lp::PlanBuilder(ctx)
          .tableScan("nation", names)
          .filter("n_nationkey < 11");
    };
    auto makeT2 = [&] {
      return lp::PlanBuilder(ctx)
          .tableScan("nation", names)
          .filter("n_nationkey > 13");
    };

    SCOPED_TRACE(
        fmt::format(
            "rootType={}, leftType={}, rightType={}",
            rootType,
            leftType,
            rightType));

    auto logicalPlan =
        makeT1()
            .setOperation(leftType, makeT1())
            .setOperation(rootType, makeT2().setOperation(rightType, makeT2()))
            .build();

    auto plan = toSingleNodePlan(logicalPlan);

    auto nonFirstChildMatcher =
        core::PlanMatcherBuilder().tableScan().project().build();
    if (rootType == lp::SetOperation::kUnion) {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan()
                         .localPartition({
                             nonFirstChildMatcher,
                             nonFirstChildMatcher,
                             nonFirstChildMatcher,
                         })
                         .aggregation()
                         .build();

      AXIOM_ASSERT_PLAN(plan, matcher);
    } else if (
        leftType == lp::SetOperation::kUnionAll &&
        rightType == lp::SetOperation::kUnionAll) {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan()
                         .localPartition({
                             nonFirstChildMatcher,
                             nonFirstChildMatcher,
                             nonFirstChildMatcher,
                         })
                         .build();

      AXIOM_ASSERT_PLAN(plan, matcher);
      continue;
    } else {
      // We cannot flatten UNION inside UNION ALL.
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan()
                         .localPartition(nonFirstChildMatcher)
                         .aggregation()
                         .localPartition(
                             core::PlanMatcherBuilder()
                                 .tableScan()
                                 .localPartition(nonFirstChildMatcher)
                                 .aggregation()
                                 .project()
                                 .build())
                         .build();

      AXIOM_ASSERT_PLAN(plan, matcher);
    }

    auto referencePlan = exec::test::PlanBuilder(pool_.get())
                             .tableScan("nation", nationType)
                             .filter("n_nationkey < 11 or n_nationkey > 13")
                             .singleAggregation(names, {})
                             .planNode();

    // Skip distributed run. Problem with local exchange source with
    // multiple inputs.
    checkSame(logicalPlan, referencePlan, {.numWorkers = 1, .numDrivers = 4});
  }
}

TEST_F(SetTest, intersect) {
  auto nationType = getSchema("nation");

  const std::vector<std::string>& names = nationType->names();

  lp::PlanBuilder::Context ctx{exec::test::kHiveConnectorId, kDefaultSchema};
  auto t1 = lp::PlanBuilder(ctx)
                .tableScan("nation", names)
                .filter("n_nationkey < 21")
                .project({"n_nationkey", "n_regionkey"});
  auto t2 = lp::PlanBuilder(ctx)
                .tableScan("nation", names)
                .filter("n_nationkey > 11")
                .project({"n_nationkey", "n_regionkey"});
  auto t3 = lp::PlanBuilder(ctx)
                .tableScan("nation", names)
                .filter("n_nationkey > 12")
                .project({"n_nationkey", "n_regionkey"});

  auto logicalPlan =
      lp::PlanBuilder(ctx)
          .setOperation(lp::SetOperation::kIntersect, {t1, t2, t3})
          .project({"n_regionkey + 1 as rk"})
          .filter("rk % 3 = 1")
          .build();

  {
    auto startMatcher = [&](auto&& filters,
                            const std::string& remainingFilter = "") {
      return core::PlanMatcherBuilder().hiveScan(
          "nation", std::move(filters), remainingFilter);
    };

    // TODO Fix this plan to push down (n_regionkey + 1) % 3
    // = 1 to all branches of 'intersect'.

    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = startMatcher(test::gte("n_nationkey", 13))
                       .hashJoin(
                           startMatcher(test::gte("n_nationkey", 12))
                               .hashJoin(
                                   startMatcher(
                                       test::lte("n_nationkey", 20),
                                       "(n_regionkey + 1) % 3 = 1")
                                       .build(),
                                   core::JoinType::kRightSemiFilter)
                               .build(),
                           core::JoinType::kRightSemiFilter)
                       .singleAggregation()
                       .project()
                       .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  auto referencePlan = exec::test::PlanBuilder(pool_.get())
                           .tableScan("nation", nationType)
                           .filter("n_nationkey > 12 and n_nationkey < 21")
                           .project({"n_regionkey + 1 as rk"})
                           .filter("rk % 3 = 1")
                           .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(SetTest, except) {
  auto nationType = getSchema("nation");

  const std::vector<std::string>& names = nationType->names();

  lp::PlanBuilder::Context ctx{exec::test::kHiveConnectorId, kDefaultSchema};
  auto t1 = lp::PlanBuilder(ctx)
                .tableScan("nation", names)
                .filter("n_nationkey < 21")
                .project({"n_nationkey", "n_regionkey"});
  auto t2 = lp::PlanBuilder(ctx)
                .tableScan("nation", names)
                .filter("n_nationkey > 16")
                .project({"n_nationkey", "n_regionkey"});
  auto t3 = lp::PlanBuilder(ctx)
                .tableScan("nation", names)
                .filter("n_nationkey <= 5")
                .project({"n_nationkey", "n_regionkey"});

  auto logicalPlan = lp::PlanBuilder(ctx)
                         .setOperation(lp::SetOperation::kExcept, {t1, t2, t3})
                         .project({"n_nationkey", "n_regionkey + 1 as rk"})
                         .filter("rk % 3 = 1")
                         .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder()
                       .hiveScan(
                           "nation",
                           test::lte("n_nationkey", 20),
                           "(n_regionkey + 1) % 3 = 1")
                       .hashJoin(
                           core::PlanMatcherBuilder()
                               // TODO Fix this plan to push down (n_regionkey +
                               // 1) % 3 = 1 to all branches of 'except'.
                               .hiveScan("nation", test::gte("n_nationkey", 17))
                               .build(),
                           core::JoinType::kAnti)
                       .hashJoin(
                           core::PlanMatcherBuilder()
                               .hiveScan("nation", test::lte("n_nationkey", 5))
                               .build(),
                           core::JoinType::kAnti)
                       .singleAggregation()
                       .project()
                       .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  auto referencePlan = exec::test::PlanBuilder(pool_.get())
                           .tableScan("nation", nationType)
                           .filter("n_nationkey > 5 and n_nationkey <= 16")
                           .project({"n_nationkey", "n_regionkey + 1 as rk"})
                           .filter("rk % 3 = 1")
                           .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(SetTest, exceptAll) {
  auto logicalPlan = parseSelect(
      "SELECT n_nationkey, n_regionkey FROM nation WHERE n_nationkey < 21 "
      "EXCEPT ALL "
      "SELECT n_nationkey, n_regionkey FROM nation WHERE n_nationkey > 16 "
      "EXCEPT ALL "
      "SELECT n_nationkey, n_regionkey FROM nation WHERE n_nationkey <= 5");

  auto matchBuild = [](auto filters) {
    return core::PlanMatcherBuilder()
        .hiveScan("nation", std::move(filters))
        .project()
        .build();
  };

  {
    // Single-node, single-driver: counting anti joins, no aggregation.
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher = core::PlanMatcherBuilder()
                       .hiveScan("nation", test::lte("n_nationkey", 20))
                       .hashJoin(
                           matchBuild(test::gte("n_nationkey", 17)),
                           core::JoinType::kCountingAnti)
                       .hashJoin(
                           matchBuild(test::lte("n_nationkey", 5)),
                           core::JoinType::kCountingAnti)
                       .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    // Multi-driver: local hash repartition on probe side. The second
    // counting join reuses the partition from the first (no extra repartition).
    auto plan = toSingleNodePlan(logicalPlan, /*numDrivers=*/4);
    auto matcher = core::PlanMatcherBuilder()
                       .hiveScan("nation", test::lte("n_nationkey", 20))
                       .localPartition({"n_nationkey", "n_regionkey"})
                       .hashJoin(
                           matchBuild(test::gte("n_nationkey", 17)),
                           core::JoinType::kCountingAnti)
                       .hashJoin(
                           matchBuild(test::lte("n_nationkey", 5)),
                           core::JoinType::kCountingAnti)
                       .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  auto matchPartitionedBuild = [](auto filters) {
    return core::PlanMatcherBuilder()
        .hiveScan("nation", std::move(filters))
        .project()
        .shuffle()
        .build();
  };

  {
    // Distributed: all sides co-partitioned by join keys (no broadcast).
    auto distributedPlan = planVelox(logicalPlan);
    auto matcher = core::PlanMatcherBuilder()
                       .hiveScan("nation", test::lte("n_nationkey", 20))
                       .shuffle({"n_nationkey", "n_regionkey"})
                       .localPartition({"n_nationkey", "n_regionkey"})
                       .hashJoin(
                           matchPartitionedBuild(test::gte("n_nationkey", 17)),
                           core::JoinType::kCountingAnti)
                       .hashJoin(
                           matchPartitionedBuild(test::lte("n_nationkey", 5)),
                           core::JoinType::kCountingAnti)
                       .gather()
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, matcher);
  }
}

TEST_F(SetTest, intersectAll) {
  // t1 is much larger than t2 and t3, so the optimizer keeps t1 on probe.
  testConnector_->addTable("t1", ROW({"a"}, BIGINT()))
      ->setStats(10'000, {{"a", {.numDistinct = 10'000}}});
  testConnector_->addTable("t2", ROW({"a"}, BIGINT()))
      ->setStats(500, {{"a", {.numDistinct = 500}}});
  testConnector_->addTable("t3", ROW({"a"}, BIGINT()))
      ->setStats(100, {{"a", {.numDistinct = 100}}});

  auto logicalPlan = parseSelect(
      "SELECT a FROM t1 "
      "INTERSECT ALL SELECT a FROM t2 "
      "INTERSECT ALL SELECT a FROM t3",
      kTestConnectorId);

  auto matchBuild = [](const std::string& table) {
    return matchScan(table).project().build();
  };
  {
    // Single-driver: counting left semi filter joins, no aggregation.
    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher =
        matchScan("t1")
            .hashJoin(matchBuild("t2"), core::JoinType::kCountingLeftSemiFilter)
            .hashJoin(matchBuild("t3"), core::JoinType::kCountingLeftSemiFilter)
            .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    // Multi-driver: local hash repartition on probe side. The second
    // counting join reuses the partition from the first.
    auto plan = toSingleNodePlan(logicalPlan, /*numDrivers=*/4);
    auto matcher =
        matchScan("t1")
            .localPartition({"a"})
            .hashJoin(matchBuild("t2"), core::JoinType::kCountingLeftSemiFilter)
            .hashJoin(matchBuild("t3"), core::JoinType::kCountingLeftSemiFilter)
            .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  auto matchPartitionedBuild = [](const std::string& table) {
    return matchScan(table).project().shuffle().build();
  };

  {
    // Distributed: all sides co-partitioned by join keys (no broadcast).
    auto distributedPlan = planVelox(logicalPlan);
    auto matcher = matchScan("t1")
                       .shuffle({"a"})
                       .localPartition({"a"})
                       .hashJoin(
                           matchPartitionedBuild("t2"),
                           core::JoinType::kCountingLeftSemiFilter)
                       .hashJoin(
                           matchPartitionedBuild("t3"),
                           core::JoinType::kCountingLeftSemiFilter)
                       .gather()
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, matcher);
  }
}

// Verifies the optimizer places the smaller table on build for INTERSECT ALL
// regardless of input order (INTERSECT ALL is symmetric).
TEST_F(SetTest, intersectAllSideSwap) {
  testConnector_->addTable("big", ROW({"a"}, BIGINT()))
      ->setStats(10'000, {{"a", {.numDistinct = 10'000}}});
  testConnector_->addTable("small", ROW({"x"}, BIGINT()))
      ->setStats(100, {{"x", {.numDistinct = 100}}});

  auto startMatcher = [&]() {
    return matchScan("big").hashJoin(
        matchScan("small").build(), core::JoinType::kCountingLeftSemiFilter);
  };

  auto startDistributedMatcher = [&]() {
    return matchScan("big").shuffle({"a"}).localPartition({"a"}).hashJoin(
        matchScan("small").shuffle().build(),
        core::JoinType::kCountingLeftSemiFilter);
  };

  // big INTERSECT ALL small: 'big' is probe, 'small' is build.
  {
    auto logicalPlan = parseSelect(
        "SELECT a FROM big INTERSECT ALL SELECT x FROM small",
        kTestConnectorId);
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), startMatcher().build());

    auto distributedPlan = planVelox(logicalPlan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        distributedPlan.plan, startDistributedMatcher().gather().build());
  }

  // Reversed input: optimizer swaps sides — 'big' is still probe, 'small'
  // is still build. A rename project maps probe columns to output schema.
  {
    auto logicalPlan = parseSelect(
        "SELECT x FROM small INTERSECT ALL SELECT a FROM big",
        kTestConnectorId);
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan), startMatcher().project().build());

    // Output rename project: join outputs 'a' (probe) but query expects
    // 'x' (the original left side column name).
    auto distributedPlan = planVelox(logicalPlan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        distributedPlan.plan,
        startDistributedMatcher().project().gather().build());
  }
}

// Verifies that joining with a UNION ALL subquery does not crash with an
// assertion failure in importJoinsIntoFirstDt.
TEST_F(SetTest, joinWithUnionAll) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()));
  testConnector_->addTable("v", ROW({"x", "y"}, BIGINT()));

  auto sql =
      "SELECT a FROM t "
      "JOIN (SELECT x FROM u UNION ALL SELECT x FROM v) w "
      "ON a = w.x";

  auto logicalPlan = parseSelect(sql, kTestConnectorId);
  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchScan("u")
                     .localPartition(matchScan("v").project().build())
                     .hashJoin(matchScan("t").build(), core::JoinType::kInner)
                     .build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

// Verifies that filtering a UNION ALL works when two columns in a child branch
// map to the same expression object (e.g., SELECT 'x' as a, 'x' as b).
TEST_F(SetTest, filterOnDuplicateConstantInUnionAll) {
  auto sql =
      "SELECT b FROM ("
      "  SELECT 'x' as a, 'x' as b"
      "  UNION ALL"
      "  SELECT 'y', 'z'"
      ") WHERE a <> ''";

  auto logicalPlan = parseSelect(sql);

  // Constant filters ('x' <> '' and 'y' <> '') are folded and eliminated.
  auto buildMatcher = [&] {
    return core::PlanMatcherBuilder().values().project().localPartition(
        core::PlanMatcherBuilder().values().project().build());
  };

  auto plan = toSingleNodePlan(logicalPlan);
  AXIOM_ASSERT_PLAN(plan, buildMatcher().build());

  // TODO: The distributed plan has a redundant gather since there are no
  // table scans. Check isSingleThreadedPipeline in ToVelox.
  auto distributedPlan = planVelox(logicalPlan);
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      distributedPlan.plan, buildMatcher().gather().build());
}

// Same as above but with a table column referenced twice (SELECT x as a, x as
// b) instead of constant expressions.
TEST_F(SetTest, filterOnDuplicateColumnInUnionAll) {
  createEmptyTable("t", ROW("x", BIGINT()));

  auto sql =
      "SELECT b FROM ("
      "  SELECT x as a, x as b FROM t"
      "  UNION ALL"
      "  SELECT 2, 3"
      ") WHERE a > 0";

  auto logicalPlan = parseSelect(sql);

  // Filter is pushed into the HiveScan as a subfield filter on x.
  // The Values child's constant filter (2 > 0) is folded and eliminated.
  auto buildMatcher = [&] {
    return core::PlanMatcherBuilder()
        .hiveScan("t", test::gt("x", int64_t{0}))
        .project()
        .localPartition(core::PlanMatcherBuilder().values().project().build());
  };

  auto plan = toSingleNodePlan(logicalPlan);
  AXIOM_ASSERT_PLAN(plan, buildMatcher().build());

  auto distributedPlan = planVelox(logicalPlan);
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      distributedPlan.plan, buildMatcher().gather().build());
}

// Same as above but with a non-trivial expression referenced twice
// (SELECT x + 1 as a, x + 1 as b). The expression deduplication produces a
// single expression object shared by both columns.
TEST_F(SetTest, filterOnDuplicateExpressionInUnionAll) {
  createEmptyTable("t", ROW("x", BIGINT()));

  auto sql =
      "SELECT b FROM ("
      "  SELECT x + 1 as a, x + 1 as b FROM t"
      "  UNION ALL"
      "  SELECT x + 2, x + 3 FROM t"
      ") WHERE a > 0";

  auto logicalPlan = parseSelect(sql);

  // Filter 'a > 0' becomes remaining filters 'x + 1 > 0' and 'x + 2 > 0'
  // on the respective scan branches.
  auto buildMatcher = [&] {
    return core::PlanMatcherBuilder()
        .hiveScan("t", {}, "x + 1 > 0")
        .project()
        .localPartition(
            core::PlanMatcherBuilder()
                .hiveScan("t", {}, "x + 2 > 0")
                .project()
                .build());
  };

  auto plan = toSingleNodePlan(logicalPlan);
  AXIOM_ASSERT_PLAN(plan, buildMatcher().build());

  auto distributedPlan = planVelox(logicalPlan);
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      distributedPlan.plan, buildMatcher().gather().build());
}

// Verifies that filtering a UNION ALL on a column not included in the outer
// SELECT works correctly when the filter is pushed into the scan.
TEST_F(SetTest, filterColumnPruningInUnionAll) {
  createEmptyTable("t", ROW({"x", "y"}, BIGINT()));

  auto sql =
      "SELECT y FROM ("
      "  SELECT x, y FROM t"
      "  UNION ALL"
      "  SELECT x, y FROM t"
      ") WHERE x > 0";

  auto logicalPlan = parseSelect(sql);

  auto buildMatcher = [&] {
    return core::PlanMatcherBuilder()
        .hiveScan("t", test::gt("x", int64_t{0}))
        .localPartition(
            core::PlanMatcherBuilder()
                .hiveScan("t", test::gt("x", int64_t{0}))
                .project()
                .build());
  };

  auto plan = toSingleNodePlan(logicalPlan);
  AXIOM_ASSERT_PLAN(plan, buildMatcher().build());

  auto distributedPlan = planVelox(logicalPlan);
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      distributedPlan.plan, buildMatcher().gather().build());
}

// Constant-false filters on UNION ALL branches cause them to be replaced with
// empty ValuesTable (zero rows) and pruned. Tests three cases:
// - One branch false, one survives: UNION ALL dissolved.
// - All branches false: entire UNION ALL becomes empty ValuesTable.
// - Mix of true/false: false branch pruned, true branches survive without
//   filters.
TEST_F(SetTest, constantFalseFilterInUnionAll) {
  createEmptyTable("t", ROW({"x", "y"}, BIGINT()));

  // One constant-false branch (0 > 0), one table scan branch survives.
  {
    auto logicalPlan = parseSelect(
        "SELECT y FROM ("
        "  SELECT x, y FROM t"
        "  UNION ALL"
        "  SELECT 0, 3"
        ") WHERE x > 0");

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(
        plan,
        core::PlanMatcherBuilder()
            .hiveScan("t", test::gt("x", int64_t{0}))
            .build());
  }

  // All branches constant-false (0 > 0 and -1 > 0).
  auto matchValues = [&]() { return core::PlanMatcherBuilder().values(); };
  {
    auto logicalPlan = parseSelect(
        "SELECT b FROM ("
        "  SELECT 0 as a, 1 as b"
        "  UNION ALL"
        "  SELECT -1, 2"
        ") WHERE a > 0");

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(plan, matchValues().project().build());
  }

  // Mix: branch 1 (5 > 0) true, branch 2 (0 > 0) false, branch 3 (3 > 0)
  // true. False branch pruned, two surviving branches have no filters.
  {
    auto logicalPlan = parseSelect(
        "SELECT b FROM ("
        "  SELECT 5 as a, 1 as b"
        "  UNION ALL"
        "  SELECT 0, 2"
        "  UNION ALL"
        "  SELECT 3, 3"
        ") WHERE a > 0");

    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(
        plan,
        matchValues()
            .project()
            .localPartition(matchValues().project().build())
            .build());
  }
}

// Verifies that a filter is pushed down below UNION / UNION ALL when one branch
// has a window function.
TEST_F(SetTest, filterOnUnionWithWindow) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()));

  // UNION ALL.
  {
    auto logicalPlan = parseSelect(
        "SELECT * FROM ("
        "  (SELECT a, row_number() OVER (ORDER BY a) AS r FROM t)"
        "  UNION ALL"
        "  (SELECT x, y FROM u)"
        ") WHERE r > 1",
        kTestConnectorId);

    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher =
        matchScan("t")
            .window({"row_number() OVER (ORDER BY a) AS r"})
            .filter("r > 1")
            .localPartition(matchScan("u").filter("y > 1").project().build())
            .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // UNION.
  {
    auto logicalPlan = parseSelect(
        "SELECT * FROM ("
        "  (SELECT a, row_number() OVER (ORDER BY a) AS r FROM t)"
        "  UNION"
        "  (SELECT x, y FROM u)"
        ") WHERE r > 1",
        kTestConnectorId);

    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher =
        matchScan("t")
            .window({"row_number() OVER (ORDER BY a) AS r"})
            .filter("r > 1")
            .localPartition(matchScan("u").filter("y > 1").project().build())
            .distinct()
            .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

// UNION ALL of two EXCEPT branches produces mismatched column names when
// the same table appears in multiple EXCEPT operands.
TEST_F(SetTest, exceptUnionAll) {
  testConnector_->addTable("t", ROW("a", BIGINT()));
  testConnector_->addTable("u", ROW("x", BIGINT()));

  auto query =
      "SELECT count(*) FROM ("
      "    (SELECT * FROM t EXCEPT SELECT * FROM u)"
      "    UNION ALL"
      "    (SELECT * FROM u EXCEPT SELECT * FROM t)"
      ")";

  // Each EXCEPT becomes anti-join + aggregation (distinct). The UNION ALL
  // combines the two branches via LocalPartition. The second branch gets a
  // rename project to align column names with the first branch.
  auto matchExcept = [](const std::string& left, const std::string& right) {
    return matchScan(left)
        .hashJoin(matchScan(right).build(), core::JoinType::kAnti)
        .singleAggregation();
  };

  auto matcher = matchExcept("t", "u")
                     .localPartition(matchExcept("u", "t").project().build())
                     .singleAggregation()
                     .build();

  auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(SetTest, unionDistinctWithUnnestMultipleReferences) {
  testConnector_->addTable("t", ROW({"a", "b"}, {BIGINT(), ARRAY(BIGINT())}));

  auto query =
      "WITH u AS ("
      "  SELECT CAST(r AS BIGINT) AS id"
      "  FROM t CROSS JOIN UNNEST(b) AS u(r)"
      "  UNION"
      "  SELECT a FROM t"
      ")"
      "SELECT (SELECT COUNT(*) FROM u), (SELECT COUNT(*) FROM u)";

  auto plan = toSingleNodePlan(parseSelect(query, kTestConnectorId));

  auto matchUnion = [&] {
    return matchScan("t")
        .unnest()
        .project()
        .localPartition(matchScan("t").project().build())
        .distinct()
        .singleAggregation({}, {"count(*)"});
  };

  // TODO: Deduplicate identical scalar subqueries.
  auto matcher = core::PlanMatcherBuilder()
                     .values()
                     .nestedLoopJoin(matchUnion().build())
                     .nestedLoopJoin(matchUnion().build())
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(SetTest, unionAllColumnCountMismatchCrash) {
  createEmptyTable("t", ROW("a", BIGINT()));

  auto sql =
      "SELECT COUNT(*) = 0 as is_empty FROM ("
      "  SELECT DISTINCT a FROM t WHERE a > 0"
      "  UNION ALL"
      "  SELECT 1 AS a"
      ")";

  auto logicalPlan = parseSelect(sql);
  VELOX_ASSERT_THROW(toSingleNodePlan(logicalPlan), "");
}

} // namespace
} // namespace facebook::axiom::optimizer
