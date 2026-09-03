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

#include <fmt/core.h>
#include <gtest/gtest.h>
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class AggregationTest : public test::QueryTestBase,
                        public ::testing::WithParamInterface<bool> {
 protected:
  void SetUp() override {
    useV2_ = GetParam();
    test::QueryTestBase::SetUp();
  }

  lp::PlanBuilder::Context makeContext() const {
    return lp::PlanBuilder::Context{kTestConnectorId, kDefaultSchema};
  }
};

TEST_P(AggregationTest, dedupGroupingKeysAndAggregates) {
  testConnector_->addTable(
      "numbers", ROW({"a", "b", "c"}, {BIGINT(), BIGINT(), DOUBLE()}));

  {
    auto logicalPlan = lp::PlanBuilder(makeContext())
                           .tableScan("numbers")
                           .project({"a + b as x", "a + b as y", "c"})
                           .aggregate({"x", "y"}, {"count(1)", "count(1)"})
                           .build();

    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher = matchScan("numbers")
                       .project({"a + b"})
                       .singleAggregation({"x"}, {"count(1)"})
                       .project({"x", "x", "count", "count"})
                       .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

TEST_P(AggregationTest, duplicatesBetweenGroupAndAggregate) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));

  auto logicalPlan = lp::PlanBuilder(makeContext())
                         .tableScan("t")
                         .project({"a + b AS ab1", "a + b AS ab2"})
                         .aggregate({"ab1", "ab2"}, {"count(ab2) AS c1"})
                         .project({"ab1 AS x", "ab2 AS y", "c1 AS z"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchScan("t")
                     .project({"plus(a, b)"})
                     .singleAggregation({"ab1"}, {"count(ab1)"})
                     .project({"ab1", "ab1", "c1"})
                     .build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_P(AggregationTest, dedupMask) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));

  auto logicalPlan = lp::PlanBuilder(makeContext(), /*enableCoercions=*/true)
                         .tableScan("t")
                         .aggregate(
                             {},
                             {"sum(a) FILTER (WHERE b > 0)",
                              "sum(a) FILTER (WHERE b < 0)",
                              "sum(a) FILTER (WHERE b > 0)"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchScan("t")
                     .project({"a", "b > 0 as m1", "b < 0 as m2"})
                     .singleAggregation(
                         {},
                         {
                             "sum(a) FILTER (WHERE m1) as s1",
                             "sum(a) FILTER (WHERE m2) as s2",
                         })
                     .project({"s1", "s2", "s1"})
                     .build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_P(AggregationTest, dedupOrderBy) {
  testConnector_->addTable("t", ROW({"a", "b", "c"}, BIGINT()));

  auto logicalPlan = lp::PlanBuilder(makeContext(), /*enableCoercions=*/true)
                         .tableScan("t")
                         .aggregate(
                             {},
                             {"array_agg(a ORDER BY a, a)",
                              "array_agg(b ORDER BY b, a, b, a)",
                              "array_agg(a ORDER BY a + b, a + b DESC, c)",
                              "array_agg(c ORDER BY b * 2, b * 2)"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchScan("t")
                     .project({"a", "b", "a + b as p0", "c", "b * 2 as p1"})
                     .singleAggregation(
                         {},
                         {"array_agg(a ORDER BY a)",
                          "array_agg(b ORDER BY b, a)",
                          "array_agg(a ORDER BY p0, c)",
                          "array_agg(c ORDER BY p1)"})
                     .build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

TEST_P(AggregationTest, dedupSameOptions) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));

  auto logicalPlan =
      lp::PlanBuilder(makeContext(), /*enableCoercions=*/true)
          .tableScan("t")
          .aggregate(
              {},
              {"array_agg(a ORDER BY a, a, a)",
               "array_agg(a ORDER BY a DESC)",
               "array_agg(a ORDER BY a, a)",
               "array_agg(a ORDER BY a)",
               "sum(a) FILTER (WHERE b > 0)",
               "sum(a) FILTER (WHERE b < 0)",
               "sum(a) FILTER (WHERE b > 0)",
               "array_agg(a ORDER BY a) FILTER (WHERE b > 0)",
               "array_agg(a ORDER BY a DESC) FILTER (WHERE b > 0)",
               "array_agg(a ORDER BY a) FILTER (WHERE b > 0)"})
          .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher =
      matchScan("t")
          .project({"a", "b > 0 as m1", "b < 0 as m2"})
          .singleAggregation(
              {},
              {"array_agg(a ORDER BY a) as agg1",
               "array_agg(a ORDER BY a DESC) as agg2",
               "sum(a) FILTER (WHERE m1) as sum1",
               "sum(a) FILTER (WHERE m2) as sum2",
               "array_agg(a ORDER BY a) FILTER (WHERE m1) as combo1",
               "array_agg(a ORDER BY a DESC) FILTER (WHERE m1) as combo2"})
          .project(
              {"agg1",
               "agg2",
               "agg1",
               "agg1",
               "sum1",
               "sum2",
               "sum1",
               "combo1",
               "combo2",
               "combo1"})
          .build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

// Verifies that aggregation with ORDER BY keys always uses single-step
// aggregation, even in distributed mode where partial+final would normally
// be used. This is required because partial aggregation cannot preserve
// global ordering across workers.
TEST_P(AggregationTest, orderBy) {
  auto schema = ROW({"k", "v1", "v2"}, {BIGINT(), BIGINT(), DOUBLE()});
  testConnector_->addTable("t", schema);
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  // 10 rows with only 2 distinct group_key values (0 and 1). Adding data to the
  // test table is necessary to trigger split aggregation steps by default.
  constexpr int kNumRows = 10;
  auto rowVector = makeRowVector({
      makeFlatVector<int64_t>(kNumRows, [](auto row) { return row % 2; }),
      makeFlatVector<int64_t>(kNumRows, [](auto row) { return row; }),
      makeFlatVector<double>(kNumRows, [](auto row) { return row * 1.5; }),
  });
  testConnector_->appendData("t", rowVector);

  // Query with ORDER BY in aggregate should use single aggregation step, even
  // if the optimizer option requires always planning partial aggregation.
  auto logicalPlan =
      lp::PlanBuilder(makeContext())
          .tableScan("t")
          .aggregate({"k"}, {"array_agg(v1 ORDER BY v2)", "sum(v1)"})
          .build();
  auto matcher = matchScan("t")
                     .distributedSingleAggregation(
                         {"k"}, {"array_agg(v1 ORDER BY v2)", "sum(v1)"})
                     .shuffle()
                     .build();

  for (auto i = 0; i < 2; ++i) {
    OptimizerOptions option;
    option.alwaysPlanPartialAggregation = (i == 0);
    auto plan = planVelox(
        logicalPlan,
        MultiFragmentPlan::Options{.numWorkers = 4, .numDrivers = 4},
        option);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
  }

  // Query without ORDER BY - should use partial + final aggregation.
  logicalPlan = lp::PlanBuilder(makeContext())
                    .tableScan("t")
                    .aggregate({"k"}, {"sum(v1)"})
                    .build();
  auto plan = planVelox(logicalPlan);

  matcher = matchScan("t")
                .partialAggregation({"k"}, {"sum(v1)"})
                .shuffle()
                .localPartition()
                .finalAggregation()
                .shuffle()
                .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
}

// Verifies plan construction succeeds when partial-aggregation
// `maxCardinality` greatly exceeds `inputBeforePartial`. Guards the
// `expectedNumDistincts` invariant `result <= min(numRows, numDistinct)`.
TEST_P(AggregationTest, fanoutPrecisionRegression) {
  // Stats sized to push `maxCardinality` far above `inputBeforePartial`:
  // the selective filter narrows `inputBeforePartial` to ~10 and scales each
  // post-filter per-key NDV to ~10, so with 11 keys the saturating-product
  // `maxGroups` reaches ~5e10.
  constexpr int64_t kHugeStat = 100'000'000'000LL;
  constexpr int kNumKeys = 11;

  std::vector<std::string> keys;
  keys.reserve(kNumKeys);
  for (int i = 0; i < kNumKeys; ++i) {
    keys.push_back(fmt::format("k{}", i));
  }

  std::unordered_map<std::string, connector::ColumnStatistics> colStats;
  colStats[keys[0]] = {
      .min = velox::Variant{int64_t{1}},
      .max = velox::Variant{kHugeStat},
      .numDistinct = kHugeStat};
  for (int i = 1; i < kNumKeys; ++i) {
    colStats[keys[i]] = {.numDistinct = kHugeStat};
  }

  testConnector_->addTable("t", ROW(folly::copy(keys), BIGINT()))
      ->setStats(kHugeStat, colStats);

  auto logicalPlan = lp::PlanBuilder(makeContext())
                         .tableScan("t")
                         .filter(fmt::format("{} > 99999999990", keys[0]))
                         .aggregate(keys, {"count(1)"})
                         .build();

  OptimizerOptions options;
  options.alwaysPlanPartialAggregation = true;
  auto plan = planVelox(
                  logicalPlan,
                  MultiFragmentPlan::Options{.numWorkers = 4, .numDrivers = 4},
                  options)
                  .plan;

  auto matcher = matchScan("t")
                     .filter(fmt::format("{} > 99999999990", keys[0]))
                     .partialAggregation(keys, {"count(1)"})
                     .shuffle()
                     .localPartition()
                     .finalAggregation()
                     .shuffle()
                     .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(plan, matcher);
}

// Verifies that repartitionForAgg correctly determines when shuffle is needed
// based on the relationship between the current partition keys and the required
// grouping keys.
// - When partitionKeys ⊆ groupingKeys: no shuffle needed
// - When partitionKeys ⊄ groupingKeys: shuffle is needed
//
// Uses two nested aggregations to test this: the first aggregation creates a
// distribution partitioned by its grouping keys, and the second aggregation
// tests whether a shuffle is added based on the relationship between current
// partition keys and the required grouping keys.
TEST_P(AggregationTest, repartitionForAggPartitionSubset) {
  auto schema = ROW({"a", "b", "c", "v"}, BIGINT());
  testConnector_->addTable("t", schema);
  // Unique grouping keys (NDV == row count): grouping does not reduce rows, so
  // partial pre-aggregation has no benefit and single-stage aggregation wins.
  testConnector_->setStats(
      "t",
      1'000,
      {{"a", {.numDistinct = 1'000}},
       {"b", {.numDistinct = 1'000}},
       {"c", {.numDistinct = 1'000}},
       {"v", {.numDistinct = 1'000}}});
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  {
    SCOPED_TRACE(
        "partitionKeys is a subset of groupingKeys, no shuffle needed");
    auto logicalPlan = lp::PlanBuilder(makeContext())
                           .tableScan("t")
                           .aggregate({"a", "b"}, {})
                           .with({"a + b as d"})
                           .aggregate({"a", "b", "d"}, {})
                           .build();
    auto plan = planVelox(logicalPlan);

    // There should be only ONE shuffle (for the first
    // aggregation). The second aggregation should NOT require a shuffle
    // because partitionKeys [a, b] ⊆ groupingKeys [a, b, d].
    auto matcher = matchScan("t")
                       .distributedSingleAggregation({"a", "b"}, {})
                       .project()
                       // No shuffle here - partitionKeys ⊆ groupingKeys
                       .localPartition()
                       .singleAggregation({"a", "b", "d"}, {})
                       .shuffle()
                       .build();
    // v2 declares driver-scope partitioning in PhysicalProperties but never
    // derives it, so EmitPass cannot see that the input already co-locates
    // [a, b, d] and adds a local repartition anyway.
    AXIOM_ASSERT_DISTRIBUTED_PLAN_V1(plan.plan, matcher);
  }

  {
    SCOPED_TRACE(
        "partitionKeys is not a subset of groupingKeys, shuffle needed");
    auto logicalPlan = lp::PlanBuilder(makeContext())
                           .tableScan("t")
                           .aggregate({"a", "b", "c"}, {})
                           .aggregate({"a", "b"}, {})
                           .build();
    auto plan = planVelox(logicalPlan);

    // There should be TWO shuffles. The second aggregation
    // MUST be after a shuffle because partitionKeys [a, b, c] ⊄ groupingKeys
    // [a, b].
    auto matcher = matchScan("t")
                       .distributedSingleAggregation({"a", "b", "c"}, {})
                       .project()
                       .distributedSingleAggregation({"a", "b"}, {})
                       .shuffle()
                       .build();
    // v2 two-stages every eligible aggregate, so it plans partial + final
    // where the unique grouping keys above make single-stage the better plan.
    AXIOM_ASSERT_DISTRIBUTED_PLAN_V1(plan.plan, matcher);
  }
}

TEST_P(AggregationTest, bucketedAggregation) {
  // Table 't' bucketed on 'k' with ~100 rows per (k, g) group, so grouping by
  // [k, g] reduces cardinality ~100x.
  testConnector_->addTable(
      "t",
      ROW({"k", "g", "v"}, {BIGINT(), BIGINT(), DOUBLE()}),
      velox::ROW({}),
      connector::TestBucketSpec{{"k"}, 128});
  testConnector_->setStats(
      "t",
      1'000'000,
      {{"k", {.numDistinct = 1'000}},
       {"g", {.numDistinct = 10}},
       {"v", {.numDistinct = 500'000}}});
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  auto logicalPlan = lp::PlanBuilder(makeContext())
                         .tableScan("t")
                         .aggregate({"k", "g"}, {"sum(v)"})
                         .build();

  {
    SCOPED_TRACE("multiple drivers: partial reduces before the local exchange");
    auto plan = planVelox(logicalPlan, {.numWorkers = 4, .numDrivers = 4});
    auto matcher = matchScan("t")
                       .partialAggregation({"k", "g"}, {"sum(v) as s"})
                       .localPartition({"k", "g"})
                       .finalAggregation({"k", "g"}, {"sum(s) as s"})
                       .bucketed()
                       .fragmentWidth(4)
                       .gather()
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
  }

  {
    SCOPED_TRACE("single driver: no local exchange, single-step aggregation");
    auto plan = planVelox(logicalPlan, {.numWorkers = 4, .numDrivers = 1});
    auto matcher = matchScan("t")
                       .multiThreaded(false)
                       .singleAggregation({"k", "g"}, {"sum(v)"})
                       .bucketed()
                       .fragmentWidth(4)
                       .gather()
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
  }
}

// TODO: Add tests for maybeProject() cost tracking once Project::unitCost is
// implemented (currently 0, see the TODO in Project::Project). The
// optimizationCost() helper is available for verifying cost differences between
// plans with and without projections.

TEST_P(AggregationTest, groupingSets) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c"}, {BIGINT(), BIGINT(), DOUBLE()}));

  auto logicalPlan = lp::PlanBuilder(makeContext())
                         .tableScan("t")
                         .rollup({"a", "b"}, {"sum(c) as total"}, "gid")
                         .build();

  // Single-node plan shape.
  {
    auto plan = toSingleNodePlan(logicalPlan);

    // ROLLUP(a, b) expands to grouping sets: {a, b}, {a}, {}.
    // Grouping keys get auto-generated output names in GroupId.
    auto matcher =
        matchScan("t")
            .groupId({{"a", "b"}, {"a"}, {}}, {"c"}, "gid")
            .singleAggregation({"a", "b", "gid"}, {"sum(c) as total"})
            .project({"a", "b", "total", "gid"})
            .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Distributed plan shape.
  {
    auto plan = planVelox(logicalPlan);

    auto matcher =
        matchScan("t")
            .groupId({{"a", "b"}, {"a"}, {}}, {"c"}, "gid")
            .distributedAggregation({"a", "b", "gid"}, {"sum(c) as total"})
            .project({"a", "b", "total", "gid"})
            .gather()
            .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
  }
}

// Verifies that a grouping key can also be an aggregation input.
// SELECT a, SUM(a) FROM t GROUP BY ROLLUP(a) requires 'a' to appear both as
// a grouping key (subject to NULL-ing) and as an aggregation input (preserved).
TEST_P(AggregationTest, groupingSetsKeyIsAggInput) {
  testConnector_->addTable("t", ROW({"a", "b"}, {BIGINT(), DOUBLE()}));

  auto logicalPlan = lp::PlanBuilder(makeContext())
                         .tableScan("t")
                         .rollup({"a"}, {"sum(a) as total"}, "gid")
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchScan("t")
                     .groupId({{"a"}, {}}, {"a"}, "gid", {{"a", "key_a"}})
                     .singleAggregation({"key_a", "gid"}, {"sum(a) as total"})
                     .project({"key_a as a", "total", "gid"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// TODO: Identical grouping sets compute separately today. Follow-up
// optimization: detect identical sets, compute once, and replicate rows.
TEST_P(AggregationTest, groupingSetsCrossSetOptimization) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c"}, {BIGINT(), BIGINT(), DOUBLE()}));

  // GROUP BY GROUPING SETS ((a, b), (b, a), (a, b)) — all three sets are
  // order-insensitively identical but treated as separate sets today.
  auto logicalPlan =
      lp::PlanBuilder(makeContext())
          .tableScan("t")
          .aggregate(
              {{"a", "b"}, {"b", "a"}, {"a", "b"}}, {"count(1) as c"}, "gid")
          .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchScan("t")
                     .groupId({{"a", "b"}, {"b", "a"}, {"a", "b"}}, {}, "gid")
                     .singleAggregation({"a", "b", "gid"}, {"count(1) as c"})
                     .project({"a", "b", "c", "gid"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// Verifies aggregation with GROUPING SETS that contain no empty set —
// exercises both single-node and distributed plan shapes.
TEST_P(AggregationTest, groupingSetsNoGlobalSet) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c"}, {BIGINT(), BIGINT(), DOUBLE()}));

  // GROUPING SETS ((a), (b)) — no empty set.
  auto logicalPlan = lp::PlanBuilder(makeContext())
                         .tableScan("t")
                         .aggregate({{"a"}, {"b"}}, {"count(1) as c"}, "gid")
                         .build();

  // Single-node plan.
  {
    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher = matchScan("t")
                       .groupId({{"a"}, {"b"}}, {}, "gid")
                       .singleAggregation({"a", "b", "gid"}, {"count(1) as c"})
                       .project({"a", "b", "c", "gid"})
                       .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Distributed plan — without global sets, the optimizer chooses
  // split (partial + final) aggregation based on cost.
  {
    auto plan = planVelox(logicalPlan);

    auto matcher = matchScan("t")
                       .groupId({{"a"}, {"b"}}, {}, "gid")
                       .partialAggregation({"a", "b", "gid"}, {"count(1) as c"})
                       .shuffle()
                       .localPartition()
                       .finalAggregation({"a", "b", "gid"}, {"count(c) as c"})
                       .project({"a", "b", "c", "gid"})
                       .gather()
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
  }
}

// Verifies a per-aggregate ORDER BY with a global grouping set plans
// single-step.
TEST_P(AggregationTest, groupingSetsOrderByWithGlobalSet) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  auto logicalPlan =
      lp::PlanBuilder(makeContext())
          .tableScan("t")
          .rollup({"a"}, {"array_agg(b ORDER BY b) as arr"}, "gid")
          .build();

  // Single-node plan shape.
  {
    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher =
        matchScan("t")
            .groupId({{"a"}, {}}, {"b"}, "gid")
            .singleAggregation({"a", "gid"}, {"array_agg(b ORDER BY b) as arr"})
            .project({"a", "arr", "gid"})
            .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Distributed plan shape.
  {
    auto plan = planVelox(
        logicalPlan,
        MultiFragmentPlan::Options{.numWorkers = 4, .numDrivers = 4});

    auto matcher =
        matchScan("t")
            .groupId({{"a"}, {}}, {"b"}, "gid")
            .gather()
            .localPartition({"a", "gid"})
            .singleAggregation({"a", "gid"}, {"array_agg(b ORDER BY b) as arr"})
            .project({"a", "arr", "gid"})
            .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
  }
}

// Literal-only aggregate args (count(1)) — aggregation inputs list passed to
// GroupId should be empty since literals are not column references.
TEST_P(AggregationTest, groupingSetsLiteralArgs) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));

  auto logicalPlan = lp::PlanBuilder(makeContext())
                         .tableScan("t")
                         .rollup({"a"}, {"count(1) as c"}, "gid")
                         .build();

  // Single-node plan — GroupId has empty aggregation inputs.
  {
    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher = matchScan("t")
                       .groupId({{"a"}, {}}, {}, "gid")
                       .singleAggregation({"a", "gid"}, {"count(1) as c"})
                       .project({"a", "c", "gid"})
                       .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Distributed plan.
  {
    auto plan = planVelox(logicalPlan);

    auto matcher = matchScan("t")
                       .groupId({{"a"}, {}}, {}, "gid")
                       .partialAggregation({"a", "gid"}, {"count(1) as c"})
                       .shuffle()
                       .localPartition()
                       .finalAggregation({"a", "gid"}, {"count(c) as c"})
                       .project({"a", "c", "gid"})
                       .gather()
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
  }
}

TEST_P(AggregationTest, mask) {
  auto logicalPlan =
      lp::PlanBuilder(makeContext())
          .tableScan("nation")
          .aggregate(
              {},
              {"sum(n_nationkey) FILTER (WHERE n_nationkey > 10)",
               "avg(n_regionkey)"})
          .build();

  const std::vector<std::string> projection = {
      "n_nationkey", "n_nationkey > 10 as mask", "n_regionkey"};

  auto matcher =
      matchScan("nation")
          .project(projection)
          .singleAggregation(
              {}, {"sum(n_nationkey) FILTER (mask)", "avg(n_regionkey)"})
          .build();
  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);

  // The mask applies to the partial aggregation only; the final aggregation
  // combines already-masked values.
  auto distributedMatcher =
      matchScan("nation")
          .project(projection)
          .partialAggregation(
              {},
              {"sum(n_nationkey) FILTER (mask) as s", "avg(n_regionkey) as a"})
          .shuffle()
          .localPartition()
          .finalAggregation({}, {"sum(s)", "avg(a)"})
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planVelox(logicalPlan, {.numWorkers = 4, .numDrivers = 4}).plan,
      distributedMatcher);
}

TEST_P(AggregationTest, distinctAggregate) {
  auto logicalPlan = lp::PlanBuilder(makeContext())
                         .tableScan("nation")
                         .aggregate({}, {"count(distinct n_regionkey)"})
                         .build();

  auto matcher = matchScan("nation")
                     .singleAggregation({}, {"count(distinct n_regionkey)"})
                     .build();
  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);

  // Distributed, the distinct values are deduplicated by an aggregation over
  // the distinct key before being counted.
  auto distributedMatcher = matchScan("nation")
                                .partialAggregation({"n_regionkey"}, {})
                                .shuffle()
                                .localPartition()
                                .finalAggregation({"n_regionkey"}, {})
                                .partialAggregation({}, {"count(n_regionkey)"})
                                .shuffle()
                                .localPartition()
                                .finalAggregation({}, {"count(count)"})
                                .build();
  // isSplittableAggregate() excludes DISTINCT, so v2 gathers to one task and
  // counts there rather than deduplicating per partition first.
  AXIOM_ASSERT_DISTRIBUTED_PLAN_V1(
      planVelox(logicalPlan).plan, distributedMatcher);
}

TEST_P(AggregationTest, orderedAggregate) {
  auto logicalPlan =
      lp::PlanBuilder(makeContext())
          .tableScan("nation")
          .aggregate(
              {"n_regionkey"},
              {"array_agg(n_nationkey ORDER BY n_nationkey DESC)",
               "array_agg(n_name ORDER BY n_nationkey)"})
          .build();

  auto matcher = matchScan("nation")
                     .singleAggregation(
                         {"n_regionkey"},
                         {"array_agg(n_nationkey ORDER BY n_nationkey DESC)",
                          "array_agg(n_name ORDER BY n_nationkey)"})
                     .build();
  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);

  // An ordered aggregate cannot be split into partial and final steps.
  auto distributedMatcher =
      matchScan("nation")
          .distributedSingleAggregation(
              {"n_regionkey"},
              {"array_agg(n_nationkey ORDER BY n_nationkey DESC)",
               "array_agg(n_name ORDER BY n_nationkey)"})
          .shuffle()
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planVelox(logicalPlan).plan, distributedMatcher);
}

TEST_P(AggregationTest, maskWithOrderedAggregate) {
  auto logicalPlan =
      lp::PlanBuilder(makeContext())
          .tableScan("nation")
          .aggregate(
              {"n_regionkey"},
              {"array_agg(n_name ORDER BY n_nationkey) FILTER (WHERE n_nationkey < 20)"})
          .build();

  const std::vector<std::string> projection = {
      "n_regionkey", "n_name", "n_nationkey < 20 as mask", "n_nationkey"};

  auto matcher =
      matchScan("nation")
          .project(projection)
          .singleAggregation(
              {"n_regionkey"},
              {"array_agg(n_name ORDER BY n_nationkey) FILTER (mask)"})
          .build();
  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);

  // v1 computes the mask below the shuffle, v2 above it; v2 therefore ships
  // one fewer column.
  auto distributedMatcher =
      matchScan("nation")
          .projectIf(!useV2_, projection)
          .shuffle()
          .projectIf(useV2_, projection)
          .localPartition()
          .singleAggregation(
              {"n_regionkey"},
              {"array_agg(n_name ORDER BY n_nationkey) FILTER (mask)"})
          .shuffle()
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planVelox(logicalPlan).plan, distributedMatcher);
}

TEST_P(AggregationTest, dedupDistinctAggregates) {
  // DISTINCT is dropped from aggregates that ignore duplicates, which makes
  // several of these the same aggregate; the trailing project restores the
  // requested output order.
  auto logicalPlan =
      lp::PlanBuilder(makeContext())
          .tableScan("nation")
          .aggregate(
              {},
              {"bool_and(DISTINCT n_nationkey % 2 = 0)",
               "bool_or(DISTINCT n_regionkey % 2 = 0)",
               "bool_and(n_nationkey % 2 = 0)",
               "bool_or(DISTINCT n_nationkey % 2 = 0)",
               "bool_and(DISTINCT n_nationkey % 2 = 0) FILTER (WHERE n_nationkey > 10)",
               "bool_or(DISTINCT n_nationkey % 2 = 0) FILTER (WHERE n_nationkey < 20)"})
          .build();

  auto matcher = matchScan("nation")
                     .project(
                         {"n_nationkey % 2 = 0 as m1",
                          "n_regionkey % 2 = 0 as m2",
                          "n_nationkey > 10 as m3",
                          "n_nationkey < 20 as m4"})
                     .singleAggregation(
                         {},
                         {"bool_and(m1) as agg1",
                          "bool_or(m2) as agg2",
                          "bool_or(m1) as agg3",
                          "bool_and(m1) FILTER (WHERE m3) as agg4",
                          "bool_or(m1) FILTER (WHERE m4) as agg5"})
                     .project({"agg1", "agg2", "agg1", "agg3", "agg4", "agg5"})
                     .build();
  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);

  auto distributedMatcher = matchScan("nation")
                                .project(
                                    {"n_nationkey % 2 = 0 as m1",
                                     "n_regionkey % 2 = 0 as m2",
                                     "n_nationkey > 10 as m3",
                                     "n_nationkey < 20 as m4"})
                                .partialAggregation(
                                    {},
                                    {"bool_and(m1)",
                                     "bool_or(m2)",
                                     "bool_or(m1)",
                                     "bool_and(m1) FILTER (WHERE m3)",
                                     "bool_or(m1) FILTER (WHERE m4)"})
                                .shuffle()
                                .localPartition()
                                .finalAggregation()
                                .project()
                                .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planVelox(logicalPlan).plan, distributedMatcher);
}

TEST_P(AggregationTest, dropOrderByFromOrderInsensitiveAggregates) {
  // sum and count do not depend on input order, so their ORDER BY is dropped,
  // which makes the first two the same aggregate.
  auto logicalPlan =
      lp::PlanBuilder(makeContext())
          .tableScan("nation")
          .aggregate(
              {},
              {"sum(n_nationkey ORDER BY n_regionkey)",
               "sum(n_nationkey ORDER BY n_nationkey DESC, n_regionkey)",
               "count(n_regionkey ORDER BY n_nationkey)",
               "sum(n_nationkey ORDER BY n_regionkey) FILTER (WHERE n_nationkey > 10)",
               "count(n_regionkey ORDER BY n_nationkey) FILTER (WHERE n_nationkey < 20)"})
          .build();

  auto matcher = matchScan("nation")
                     .project(
                         {"n_nationkey",
                          "n_regionkey",
                          "n_nationkey > 10 as m1",
                          "n_nationkey < 20 as m2"})
                     .singleAggregation(
                         {},
                         {"sum(n_nationkey) as agg1",
                          "count(n_regionkey) as agg2",
                          "sum(n_nationkey) FILTER (WHERE m1) as agg3",
                          "count(n_regionkey) FILTER (WHERE m2) as agg4"})
                     .project({"agg1", "agg1", "agg2", "agg3", "agg4"})
                     .build();
  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);

  auto distributedMatcher = matchScan("nation")
                                .project(
                                    {"n_nationkey",
                                     "n_regionkey",
                                     "n_nationkey > 10 as m1",
                                     "n_nationkey < 20 as m2"})
                                .partialAggregation(
                                    {},
                                    {"sum(n_nationkey)",
                                     "count(n_regionkey)",
                                     "sum(n_nationkey) FILTER (WHERE m1)",
                                     "count(n_regionkey) FILTER (WHERE m2)"})
                                .shuffle()
                                .localPartition()
                                .finalAggregation()
                                .project()
                                .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planVelox(logicalPlan, {.numWorkers = 4, .numDrivers = 4}).plan,
      distributedMatcher);
}

TEST_P(AggregationTest, orderInsensitiveOrderBySubqueryNotJoined) {
  // The ORDER BY of an order-insensitive aggregate is not translated at all, so
  // a scalar subquery there never joins onto the aggregation's input.
  testConnector_->addTable("t", ROW({"a"}, BIGINT()));
  testConnector_->addTable("u", ROW({"x"}, BIGINT()));

  auto logicalPlan = parseSelect(
      "SELECT sum(a ORDER BY (SELECT max(x) FROM u)) FROM t", kTestConnectorId);

  auto matcher = matchScan("t").singleAggregation({}, {"sum(a)"}).build();
  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
}

TEST_P(AggregationTest, orderInsensitiveOrderByColumnNotRead) {
  // A column read only by the ORDER BY of an order-insensitive aggregate is not
  // read from the table.
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));

  auto logicalPlan = lp::PlanBuilder(makeContext())
                         .tableScan("t")
                         .aggregate({}, {"sum(a ORDER BY b)"})
                         .build();

  auto matcher = matchScan("t", ROW("a", BIGINT()))
                     .singleAggregation({}, {"sum(a)"})
                     .build();
  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
}

TEST_P(AggregationTest, dedupDistinctAndOrderedAggregates) {
  // Both DISTINCT and ORDER BY are dropped from aggregates that ignore them,
  // leaving three distinct aggregates for four requested ones.
  auto logicalPlan =
      lp::PlanBuilder(makeContext())
          .tableScan("nation")
          .aggregate(
              {},
              {
                  "bool_and(DISTINCT n_nationkey % 2 = 0 ORDER BY n_nationkey % 2 = 0)",
                  "bool_or(DISTINCT n_nationkey % 2 = 0 ORDER BY n_nationkey % 2 = 0 DESC)",
                  "bool_and(n_nationkey % 2 = 0 ORDER BY n_nationkey % 2 = 0)",
                  "bool_and(DISTINCT n_nationkey % 2 = 0 ORDER BY n_nationkey % 2 = 0) FILTER (WHERE n_nationkey > 10)",
              })
          .build();

  auto matcher =
      matchScan("nation")
          .project({"n_nationkey % 2 = 0 as m1", "n_nationkey > 10 as m2"})
          .singleAggregation(
              {},
              {"bool_and(m1) as agg1",
               "bool_or(m1) as agg2",
               "bool_and(m1) FILTER (WHERE m2) as agg3"})
          .project({"agg1", "agg2", "agg1", "agg3"})
          .build();
  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);

  auto distributedMatcher =
      matchScan("nation")
          .project({"n_nationkey % 2 = 0 as m1", "n_nationkey > 10 as m2"})
          .partialAggregation(
              {},
              {
                  "bool_and(m1)",
                  "bool_or(m1)",
                  "bool_and(m1) FILTER (WHERE m2)",
              })
          .shuffle()
          .localPartition()
          .finalAggregation()
          .project()
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planVelox(logicalPlan).plan, distributedMatcher);
}

TEST_P(AggregationTest, alwaysPlanPartialAggregation) {
  testConnector_->addTable(
      "numbers", ROW({"a", "b", "c"}, {DOUBLE(), DOUBLE(), VARCHAR()}));

  optimizerOptions_.alwaysPlanPartialAggregation = true;

  auto logicalPlan = lp::PlanBuilder(makeContext())
                         .tableScan("numbers", {"a", "b"})
                         .aggregate({"a"}, {"sum(a + b)"})
                         .build();

  // A single driver has nothing to combine, so the aggregation stays whole.
  auto matcher = matchScan("numbers")
                     .project({"a", "a + b as ab"})
                     .singleAggregation({"a"}, {"sum(ab)"})
                     .build();
  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);

  auto multiDriverMatcher = matchScan("numbers")
                                .project({"a", "a + b as ab"})
                                .partialAggregation({"a"}, {"sum(ab)"})
                                .localPartition()
                                .finalAggregation()
                                .build();
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(logicalPlan, /*numDrivers=*/2), multiDriverMatcher);
}

AXIOM_INSTANTIATE_V1_V2(AggregationTest);

} // namespace
} // namespace facebook::axiom::optimizer
