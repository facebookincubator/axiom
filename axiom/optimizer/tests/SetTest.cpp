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

#include <fmt/format.h>

#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

// Verifies set-operation planning (UNION / INTERSECT / EXCEPT flattening,
// semi/anti-join lowering, dedup aggregation, local partitioning). Tables are
// registered in TestConnector with TPC-H statistics, so the optimizer plans
// instantly with no data generation. The test connector does not push filters
// into the scan, so filters appear as separate Filter nodes. Result correctness
// is verified separately in tests/sql/set.sql against DuckDB.
class SetTest : public test::QueryTestBase {
 protected:
  static constexpr double kScaleFactor = 1.0;

  void configureTestConnector() override {
    testConnector_->addTpchTables(kScaleFactor);
  }

  lp::LogicalPlanNodePtr parseSelect(std::string_view sql) {
    return test::QueryTestBase::parseSelect(sql, kTestConnectorId);
  }
};

TEST_F(SetTest, unionAll) {
  lp::PlanBuilder::Context ctx{kTestConnectorId, kDefaultSchema};
  auto t1 = lp::PlanBuilder(ctx).tableScan("nation").filter("n_nationkey < 11");
  auto t2 = lp::PlanBuilder(ctx).tableScan("nation").filter("n_nationkey > 13");

  auto logicalPlan = t1.unionAll(t2)
                         .project({"n_regionkey + 1 as rk"})
                         .filter("rk % 3 = 1")
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);
  // The outer 'rk % 3 = 1' filter is pushed into both legs and combined with
  // the leg's own predicate. Aliases bind stable names to each leg's scan
  // output (the second leg's columns are disambiguated).
  auto matchLeg = [](const std::string& filter) {
    return matchScan("nation")
        .aliases({"regionkey", "nationkey"})
        .filter(filter)
        .project();
  };
  auto matcher =
      matchLeg("nationkey < 11 and (regionkey + 1) % 3 = 1")
          .localPartition(
              matchLeg("nationkey > 13 and (regionkey + 1) % 3 = 1").build())
          .project()
          .build();

  AXIOM_ASSERT_PLAN(plan, matcher);
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
  lp::PlanBuilder::Context ctx(kTestConnectorId, kDefaultSchema);
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

  auto plan = toSingleNodePlan(logicalPlan);
  // Aliases bind a stable name to each leg's filter column (the non-first scans
  // of a table are disambiguated, e.g. ps_availqty_1).
  auto matchLeg = [](const std::string& table,
                     const std::string& alias,
                     const std::string& filter) {
    return matchScan(table)
        .aliases({std::nullopt, alias})
        .filter(filter)
        .project();
  };
  auto matcher =
      matchLeg("partsupp", "availqty", "availqty < 1000")
          .localPartition({
              matchLeg("partsupp", "availqty", "availqty > 2000").build(),
              matchLeg("partsupp", "availqty", "availqty between 1200 and 1400")
                  .build(),
          })
          .hashJoin(
              matchLeg("part", "retailprice", "retailprice < 1100.0")
                  .localPartition(
                      matchLeg("part", "retailprice", "retailprice > 1200.0")
                          .build())
                  .build(),
              core::JoinType::kInner)
          .singleAggregation({}, {"sum(1)"})
          .build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

// Checks
// - UNION ALL of two UNION ALL (should be flatten)
// - UNION ALL of two UNION (shouldn't be flatten)
// - UNION of two UNION ALL (should be flatten)
// - UNION of two UNION (should be flatten)
TEST_F(SetTest, unionFlatten) {
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
    lp::PlanBuilder::Context ctx{kTestConnectorId, kDefaultSchema};
    auto makeT1 = [&] {
      return lp::PlanBuilder(ctx).tableScan("nation").filter(
          "n_nationkey < 11");
    };
    auto makeT2 = [&] {
      return lp::PlanBuilder(ctx).tableScan("nation").filter(
          "n_nationkey > 13");
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

    // Each leg now carries a separate Filter node above its TableScan. The
    // first child of a LocalPartition has no rename Project; non-first children
    // do.
    auto nonFirstChildMatcher =
        core::PlanMatcherBuilder().tableScan().filter().project().build();
    if (rootType == lp::SetOperation::kUnion) {
      auto matcher = core::PlanMatcherBuilder()
                         .tableScan()
                         .filter()
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
                         .filter()
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
                         .filter()
                         .localPartition(nonFirstChildMatcher)
                         .aggregation()
                         .localPartition(
                             core::PlanMatcherBuilder()
                                 .tableScan()
                                 .filter()
                                 .localPartition(nonFirstChildMatcher)
                                 .aggregation()
                                 .project()
                                 .build())
                         .build();

      AXIOM_ASSERT_PLAN(plan, matcher);
    }
  }
}

TEST_F(SetTest, intersect) {
  lp::PlanBuilder::Context ctx{kTestConnectorId, kDefaultSchema};
  auto t1 = lp::PlanBuilder(ctx)
                .tableScan("nation")
                .filter("n_nationkey < 21")
                .project({"n_nationkey", "n_regionkey"});
  auto t2 = lp::PlanBuilder(ctx)
                .tableScan("nation")
                .filter("n_nationkey > 11")
                .project({"n_nationkey", "n_regionkey"});
  auto t3 = lp::PlanBuilder(ctx)
                .tableScan("nation")
                .filter("n_nationkey > 12")
                .project({"n_nationkey", "n_regionkey"});

  auto logicalPlan =
      lp::PlanBuilder(ctx)
          .setOperation(lp::SetOperation::kIntersect, {t1, t2, t3})
          .project({"n_regionkey + 1 as rk"})
          .filter("rk % 3 = 1")
          .build();

  {
    // TODO Fix this plan to push down (n_regionkey + 1) % 3
    // = 1 to all branches of 'intersect'.

    auto plan = toSingleNodePlan(logicalPlan);
    // Aliases bind stable names to each leg's scan output (the non-first scans
    // of nation are disambiguated). The t1 leg also carries the combined
    // (regionkey + 1) % 3 = 1 predicate pushed down from above the intersect.
    auto matchLeg = [](const std::string& filter) {
      return matchScan("nation")
          .aliases({"nationkey", "regionkey"})
          .filter(filter);
    };
    auto matcher =
        matchLeg("nationkey > 12")
            .hashJoin(
                matchLeg("nationkey > 11")
                    .hashJoin(
                        matchLeg("nationkey < 21 and (regionkey + 1) % 3 = 1")
                            .build(),
                        core::JoinType::kRightSemiFilter)
                    .build(),
                core::JoinType::kRightSemiFilter)
            .singleAggregation()
            .project()
            .project()
            .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_F(SetTest, except) {
  lp::PlanBuilder::Context ctx{kTestConnectorId, kDefaultSchema};
  auto t1 = lp::PlanBuilder(ctx)
                .tableScan("nation")
                .filter("n_nationkey < 21")
                .project({"n_nationkey", "n_regionkey"});
  auto t2 = lp::PlanBuilder(ctx)
                .tableScan("nation")
                .filter("n_nationkey > 16")
                .project({"n_nationkey", "n_regionkey"});
  auto t3 = lp::PlanBuilder(ctx)
                .tableScan("nation")
                .filter("n_nationkey <= 5")
                .project({"n_nationkey", "n_regionkey"});

  auto logicalPlan = lp::PlanBuilder(ctx)
                         .setOperation(lp::SetOperation::kExcept, {t1, t2, t3})
                         .project({"n_nationkey", "n_regionkey + 1 as rk"})
                         .filter("rk % 3 = 1")
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);
  // Aliases bind stable names to each leg's scan output (the non-first scans of
  // nation are disambiguated). The t1 (probe) leg also carries the combined
  // (regionkey + 1) % 3 = 1 predicate pushed down from above the except.
  // TODO Fix this plan to push down (n_regionkey + 1) % 3 = 1 to all branches
  // of 'except'.
  auto matchLeg = [](const std::string& filter) {
    return matchScan("nation")
        .aliases({"nationkey", "regionkey"})
        .filter(filter);
  };
  auto matcher = matchLeg("nationkey < 21 and (regionkey + 1) % 3 = 1")
                     .hashJoin(
                         matchLeg("nationkey > 16").build(),
                         core::JoinType::kAnti,
                         {.nullAware = false})
                     .hashJoin(
                         matchLeg("nationkey <= 5").build(),
                         core::JoinType::kAnti,
                         {.nullAware = false})
                     .singleAggregation()
                     .project()
                     .build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(SetTest, exceptAll) {
  auto logicalPlan = parseSelect(
      "SELECT n_nationkey, n_regionkey FROM nation WHERE n_nationkey < 21 "
      "EXCEPT ALL "
      "SELECT n_nationkey, n_regionkey FROM nation WHERE n_nationkey > 16 "
      "EXCEPT ALL "
      "SELECT n_nationkey, n_regionkey FROM nation WHERE n_nationkey <= 5");

  // The build legs (non-first scans of nation) read disambiguated columns, so
  // aliases bind stable names for the filter predicates.
  auto matchBuild = [](const std::string& filter) {
    return matchScan("nation")
        .aliases({"nationkey", "regionkey"})
        .filter(filter)
        .build();
  };

  {
    // Single-node, single-driver: counting anti joins, no aggregation.
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher =
        matchScan("nation")
            .filter("n_nationkey < 21")
            .hashJoin(
                matchBuild("nationkey > 16"), core::JoinType::kCountingAnti)
            .hashJoin(
                matchBuild("nationkey <= 5"), core::JoinType::kCountingAnti)
            .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    // Multi-driver: local hash repartition on probe side. The second
    // counting join reuses the partition from the first (no extra repartition).
    auto plan = toSingleNodePlan(logicalPlan, /*numDrivers=*/4);
    auto matcher =
        matchScan("nation")
            .filter("n_nationkey < 21")
            .localPartition({"n_nationkey", "n_regionkey"})
            .hashJoin(
                matchBuild("nationkey > 16"), core::JoinType::kCountingAnti)
            .hashJoin(
                matchBuild("nationkey <= 5"), core::JoinType::kCountingAnti)
            .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  auto matchPartitionedBuild = [](const std::string& filter) {
    return matchScan("nation")
        .aliases({"nationkey", "regionkey"})
        .filter(filter)
        .shuffle()
        .build();
  };

  {
    // Distributed: all sides co-partitioned by join keys (no broadcast).
    auto distributedPlan = planVelox(logicalPlan);
    auto matcher = matchScan("nation")
                       .filter("n_nationkey < 21")
                       .shuffle({"n_nationkey", "n_regionkey"})
                       .localPartition({"n_nationkey", "n_regionkey"})
                       .hashJoin(
                           matchPartitionedBuild("nationkey > 16"),
                           core::JoinType::kCountingAnti)
                       .hashJoin(
                           matchPartitionedBuild("nationkey <= 5"),
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
      "INTERSECT ALL SELECT a FROM t3");

  auto matchBuild = [](const std::string& table) {
    return matchScan(table).build();
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
    return matchScan(table).shuffle().build();
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
    auto logicalPlan =
        parseSelect("SELECT a FROM big INTERSECT ALL SELECT x FROM small");
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), startMatcher().build());

    auto distributedPlan = planVelox(logicalPlan);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        distributedPlan.plan, startDistributedMatcher().gather().build());
  }

  // Reversed input: optimizer swaps sides — 'big' is still probe, 'small'
  // is still build. A rename project maps probe columns to output schema.
  {
    auto logicalPlan =
        parseSelect("SELECT x FROM small INTERSECT ALL SELECT a FROM big");
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

  auto logicalPlan = parseSelect(sql);
  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchScan("t")
                     .hashJoin(
                         matchScan("u")
                             .localPartition(matchScan("v").project().build())
                             .build(),
                         core::JoinType::kInner)
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
    return matchValues().project().localPartition(
        matchValues().project().build());
  };

  auto plan = toSingleNodePlan(logicalPlan);
  AXIOM_ASSERT_PLAN(plan, buildMatcher().build());

  // All-gather UnionAll output stays gather; no extra gather Repartition
  // needed.
  auto distributedPlan = planVelox(logicalPlan);
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, buildMatcher().build());
}

// UNION DISTINCT over all-gather inputs (e.g., constant Values) plans as a
// single fragment with no remote shuffles. The dedup runs locally because
// all data is already on a single task.
TEST_F(SetTest, unionDistinctOverGatherInputs) {
  auto logicalPlan = parseSelect("SELECT 1 AS k UNION SELECT 2");

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(logicalPlan),
      matchValues()
          .project()
          .localPartition(matchValues().project().build())
          .singleAggregation({"k"}, {})
          .build());

  // TODO: The inner localPartition({"k"}) is a redundant hash repartition
  // immediately after the round-robin local merge from UnionAll. ToVelox's
  // makeAggregation adds it for multi-driver hash co-location even though
  // the round-robin just spread the data. Folding the two would let
  // makeAggregation skip this step.
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planVelox(logicalPlan).plan,
      matchValues()
          .project()
          .localPartition(matchValues().project().build())
          .localPartition({"k"})
          .singleAggregation({"k"}, {})
          .build());
}

// A nondeterministic filter pushes below UNION ALL (no dedup → safe) but
// must stay above UNION DISTINCT's dedup — pushing it into the legs would
// let each duplicate row evaluate the predicate independently before dedup,
// changing query semantics.
//
// The filter references a column (`k > rand()` rather than
// `rand() < 0.5`) so that distributeConjuncts considers it for pushdown
// (the pushdown path requires the conjunct to reference a single table).
TEST_F(SetTest, nondeterministicFilterAboveUnion) {
  // UNION ALL: filter pushes into both scan legs (no dedup → safe). The
  // pushed predicate becomes a Filter node above each scan leg.
  {
    auto logicalPlan = parseSelect(
        "SELECT k FROM (SELECT n_nationkey AS k FROM nation "
        "UNION ALL SELECT r_regionkey AS k FROM region) WHERE k > rand()");

    auto buildMatcher = [&] {
      return matchScan("nation")
          .filter("rand() < cast(n_nationkey as double)")
          .project()
          .localPartition(matchScan("region")
                              .filter("rand() < cast(r_regionkey as double)")
                              .project()
                              .build());
    };

    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), buildMatcher().build());
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        planVelox(logicalPlan).plan, buildMatcher().gather().build());
  }

  // UNION DISTINCT: filter stays above the aggregation (dedup), not pushed
  // into the scan legs.
  {
    auto logicalPlan = parseSelect(
        "SELECT k FROM (SELECT n_nationkey AS k FROM nation "
        "UNION SELECT r_regionkey AS k FROM region) WHERE k > rand()");

    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("nation")
            .project()
            .localPartition(matchScan("region").project().build())
            .singleAggregation({"k"}, {})
            .filter("cast(k as double) > rand()")
            .build());

    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        planVelox(logicalPlan).plan,
        matchScan("nation")
            .project()
            .localPartition(matchScan("region").project().build())
            .distributedSingleAggregation({"k"}, {})
            .filter("cast(k as double) > rand()")
            .gather()
            .build());
  }
}

// Same as above but with a table column referenced twice (SELECT x as a, x as
// b) instead of constant expressions.
TEST_F(SetTest, filterOnDuplicateColumnInUnionAll) {
  testConnector_->addTable("t", ROW("x", BIGINT()));

  auto sql =
      "SELECT b FROM ("
      "  SELECT x as a, x as b FROM t"
      "  UNION ALL"
      "  SELECT 2, 3"
      ") WHERE a > 0";

  auto logicalPlan = parseSelect(sql);

  // Filter is pushed into the leg below the union as a Filter on x.
  // The Values child's constant filter (2 > 0) is folded and eliminated.
  {
    auto matcher = matchScan("t")
                       .filter("x > 0")
                       .project()
                       .localPartition(matchValues().project().build())
                       .build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    // Values is isolated behind an arbitrary exchange.
    auto matcher =
        matchScan("t")
            .filter("x > 0")
            .project()
            .localPartition(matchValues().project().arbitrary().build())
            .gather()
            .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// Same as above but with a non-trivial expression referenced twice
// (SELECT x + 1 as a, x + 1 as b). The expression deduplication produces a
// single expression object shared by both columns.
TEST_F(SetTest, filterOnDuplicateExpressionInUnionAll) {
  testConnector_->addTable("t", ROW("x", BIGINT()));

  auto sql =
      "SELECT b FROM ("
      "  SELECT x + 1 as a, x + 1 as b FROM t"
      "  UNION ALL"
      "  SELECT x + 2, x + 3 FROM t"
      ") WHERE a > 0";

  auto logicalPlan = parseSelect(sql);

  // Filter 'a > 0' becomes remaining filters 'x + 1 > 0' and 'x + 2 > 0' on the
  // respective scan branches. The alias binds the (disambiguated on the second
  // branch) scan column.
  auto matchLeg = [](const std::string& filter) {
    return matchScan("t").aliases({"x"}).filter(filter);
  };

  {
    auto matcher = matchLeg("x + 1 > 0")
                       .project()
                       .localPartition(matchLeg("x + 2 > 0").project().build())
                       .build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    auto matcher = matchLeg("x + 1 > 0")
                       .project()
                       .localPartition(matchLeg("x + 2 > 0").project().build())
                       .gather()
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// Verifies that filtering a UNION ALL on a column not included in the outer
// SELECT works correctly when the filter is pushed into the scan.
TEST_F(SetTest, filterColumnPruningInUnionAll) {
  testConnector_->addTable("t", ROW({"x", "y"}, BIGINT()));

  auto sql =
      "SELECT y FROM ("
      "  SELECT x, y FROM t"
      "  UNION ALL"
      "  SELECT x, y FROM t"
      ") WHERE x > 0";

  auto logicalPlan = parseSelect(sql);

  // 'x' is filtered but not in the output, so it is pruned from each leg. The
  // alias binds the (disambiguated on the second leg) filter column.
  auto matchLeg = [](const std::string& filter) {
    return matchScan("t").aliases({std::nullopt, "x"}).filter(filter).project();
  };

  {
    auto matcher =
        matchLeg("x > 0").localPartition(matchLeg("x > 0").build()).build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    auto matcher = matchLeg("x > 0")
                       .localPartition(matchLeg("x > 0").build())
                       .gather()
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// Constant-false filters on UNION ALL branches cause them to be replaced with
// empty ValuesTable (zero rows) and pruned. Tests three cases:
// - One branch false, one survives: UNION ALL dissolved.
// - All branches false: entire UNION ALL becomes empty ValuesTable.
// - Mix of true/false: false branch pruned, true branches survive without
//   filters.
TEST_F(SetTest, constantFalseFilterInUnionAll) {
  testConnector_->addTable("t", ROW({"x", "y"}, BIGINT()));

  // One constant-false branch (0 > 0), one table scan branch survives.
  {
    auto logicalPlan = parseSelect(
        "SELECT y FROM ("
        "  SELECT x, y FROM t"
        "  UNION ALL"
        "  SELECT 0, 3"
        ") WHERE x > 0");

    auto plan = toSingleNodePlan(logicalPlan);
    // The surviving branch ends with a rename Project that prunes x and keeps
    // y.
    AXIOM_ASSERT_PLAN(plan, matchScan("t").filter("x > 0").project().build());
  }

  // All branches constant-false (0 > 0 and -1 > 0).
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
        ") WHERE r > 1");

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
        ") WHERE r > 1");

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
  // combines the two branches via LocalPartition. Both branches get a
  // rename project to align column names through LocalPartition.
  auto matchExcept = [](const std::string& left, const std::string& right) {
    return matchScan(left)
        .hashJoin(
            matchScan(right).build(),
            core::JoinType::kAnti,
            {.nullAware = false})
        .singleAggregation()
        .project();
  };

  auto matcher = matchExcept("t", "u")
                     .localPartition(matchExcept("u", "t").build())
                     .singleAggregation()
                     .build();

  auto plan = toSingleNodePlan(parseSelect(query));
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

  auto plan = toSingleNodePlan(parseSelect(query));

  // The two SELECT items reference the same scalar subquery, which is
  // planned once and joined once; the outer Project emits the join's
  // single output column twice (one per SELECT item).
  auto matcher =
      matchValues()
          .nestedLoopJoin(matchScan("t")
                              .unnest()
                              .project()
                              .localPartition(matchScan("t").project().build())
                              .distinct()
                              .singleAggregation({}, {"count(*) as cnt"})
                              .build())
          .project({"cnt", "cnt"})
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_F(SetTest, unionAllWithDistinctAndCountStar) {
  testConnector_->addTable("t", ROW("a", BIGINT()));

  {
    auto logicalPlan = parseSelect(
        "SELECT COUNT(*) FROM ("
        "  SELECT DISTINCT a FROM t"
        "  UNION ALL"
        "  SELECT 1"
        ")");
    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(
        plan,
        matchScan("t")
            .singleAggregation({"a"}, {})
            .project({})
            .localPartition(core::PlanMatcherBuilder().values(ROW({})).build())
            .singleAggregation({}, {"count(*) as c"})
            .build());
  }

  {
    auto logicalPlan = parseSelect(
        "SELECT COUNT(*) FROM ("
        "  SELECT DISTINCT a FROM t"
        "  UNION ALL"
        "  SELECT 1"
        "  UNION ALL"
        "  SELECT 2"
        ")");
    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(
        plan,
        matchScan("t")
            .singleAggregation({"a"}, {})
            .project({})
            .localPartition(
                {core::PlanMatcherBuilder().values(ROW({})).build(),
                 core::PlanMatcherBuilder().values(ROW({})).build()})
            .singleAggregation({}, {"count(*) as c"})
            .build());
  }

  {
    auto logicalPlan = parseSelect(
        "SELECT * FROM ("
        "  SELECT COUNT(*) as cnt FROM ("
        "    SELECT DISTINCT a FROM t"
        "    UNION ALL"
        "    SELECT 1"
        "  )"
        ") WHERE cnt > 0");
    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(
        plan,
        matchScan("t")
            .singleAggregation({"a"}, {})
            .project({})
            .localPartition(core::PlanMatcherBuilder().values(ROW({})).build())
            .singleAggregation({}, {"count(*) as cnt"})
            .filter("cnt > 0")
            .build());
  }
}

TEST_F(SetTest, rowSubfieldAccessInUnionAll) {
  {
    auto logicalPlan = parseSelect(
        "SELECT x.a FROM ("
        "  SELECT ROW(1 AS a, 2 AS b) AS x"
        "  UNION ALL"
        "  SELECT ROW(3 AS a, 4 AS b)"
        ")");
    auto plan = toSingleNodePlan(logicalPlan);

    AXIOM_ASSERT_PLAN(
        plan,
        matchValues(ROW({}))
            .project({"row_constructor(1, null)"})
            .localPartition(matchValues(ROW({}))
                                .project({"row_constructor(3, null)"})
                                .build())
            .project({"x.a"})
            .build());
  }

  {
    auto logicalPlan = parseSelect(
        "SELECT x[1] FROM ("
        "  SELECT ROW(1, 2) AS x"
        "  UNION ALL"
        "  SELECT ROW(3, 4)"
        ") WHERE x[1] > 0");
    auto plan = toSingleNodePlan(logicalPlan);

    AXIOM_ASSERT_PLAN(
        plan,
        matchValues(ROW({}))
            .filter()
            .project({"row_constructor(1, null)"})
            .localPartition(matchValues(ROW({}))
                                .filter()
                                .project({"row_constructor(3, null)"})
                                .build())
            .project({"x[1]"})
            .build());
  }

  {
    auto logicalPlan = parseSelect(
        "SELECT y FROM ("
        "  SELECT x[1] AS y FROM ("
        "    SELECT ROW(1, 2) AS x"
        "    UNION ALL"
        "    SELECT ROW(3, 4)"
        "  )"
        "  UNION ALL"
        "  SELECT 5"
        ")");
    auto plan = toSingleNodePlan(logicalPlan);

    AXIOM_ASSERT_PLAN(
        plan,
        matchValues(ROW({}))
            .project({"row_constructor(1, null)"})
            .localPartition(matchValues(ROW({}))
                                .project({"row_constructor(3, null)"})
                                .build())
            .project({"x[1]"})
            .localPartition(matchValues(ROW({})).project({"5 as y"}).build())
            .build());
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
