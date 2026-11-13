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
#include <gmock/gmock.h>
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

TEST_P(AggregationTest, constantGroupingKeys) {
  testConnector_->addTable("cg_t", ROW({"a", "b"}, BIGINT()));

  {
    SCOPED_TRACE("Variable and constant grouping keys");
    const auto logicalPlan = parseSelect(
        "SELECT a, b, label, count(*) AS c "
        "FROM (SELECT a, b, 'foo' AS label FROM cg_t) "
        "GROUP BY 1, 2, 3");

    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan),
        matchScan("cg_t")
            .singleAggregation({"a", "b"}, {"count(*) as c"})
            .project({"a", "b", R"('foo' as label)", "c"})
            .build());
  }

  {
    SCOPED_TRACE("Only constant grouping keys");
    const auto logicalPlan = parseSelect(
        "SELECT 'foo' AS x, 'bar' AS y, count(*) AS c FROM cg_t GROUP BY 1, 2");

    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan),
        matchScan("cg_t")
            .project({R"('foo' as x)"})
            .singleAggregation({"x"}, {"count(*) as c"})
            .project({R"('foo' as x)", R"('bar' as y)", "c"})
            .build());
  }

  {
    SCOPED_TRACE("Filtered grouping key referenced above aggregation");
    const auto logicalPlan = parseSelect(
        "SELECT CASE WHEN a = 1 THEN 10 ELSE 20 END AS x "
        "FROM cg_t WHERE a = 1 GROUP BY a");

    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan),
        matchScan("cg_t", ROW("a", BIGINT()))
            .filter("a = 1")
            .project({"1 as a"})
            .singleAggregation({"a"}, {})
            .project({"10"})
            .build());
  }

  {
    SCOPED_TRACE("Every grouping set contains filtered key");
    const auto logicalPlan = parseSelect(
        "SELECT CASE WHEN a = 1 THEN 10 ELSE 20 END AS x "
        "FROM cg_t WHERE a = 1 "
        "GROUP BY GROUPING SETS ((a, b), (a))");

    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan),
        matchScan("cg_t")
            .filter("a = 1")
            .project({"b", "1 as fixed_a"})
            .groupId({{"fixed_a", "b"}, {"fixed_a"}}, {}, "group_id")
            .singleAggregation({"b", "group_id"}, {})
            .project({"10"})
            .build());
  }

  {
    SCOPED_TRACE("Grouping set omits filtered key");
    const auto logicalPlan = parseSelect(
        "SELECT CASE WHEN a = 1 THEN 10 ELSE 20 END AS x "
        "FROM cg_t WHERE a = 1 GROUP BY GROUPING SETS ((a), ())");

    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan),
        matchScan("cg_t", ROW("a", BIGINT()))
            .filter("a = 1")
            .project({"1 as fixed_a"})
            .groupId({{"fixed_a"}, {}}, {}, "group_id")
            .singleAggregation({"fixed_a", "group_id"}, {})
            .project({"if(fixed_a = 1, 10, 20)"})
            .build());
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

TEST_P(AggregationTest, constantFilter) {
  testConnector_->addTable("t", ROW({"a", "b", "c"}, BIGINT()));
  testConnector_->addTable("u", ROW({{"a", BIGINT()}, {"b", BOOLEAN()}}));

  const auto logicalPlan = parseSelect(
      "SELECT t.b, count(v.a) FILTER (WHERE v.b), sum(t.c) FILTER (WHERE v.b IS NULL) "
      "FROM t "
      "LEFT JOIN (SELECT * FROM u WHERE a = 2) v ON t.a = v.a "
      "WHERE t.a = 1 "
      "GROUP BY 1");

  AXIOM_ASSERT_PLAN_V2(
      toSingleNodePlan(logicalPlan),
      matchScan("t")
          .filter("a = 1")
          .singleAggregation({"b"}, {"sum(c) as s"})
          .project({"b", "0", "s"})
          .build());
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
        MultiFragmentPlan::Options{
            .maxRemotePartitions = 4, .maxLocalPartitions = 4},
        option);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
  }

  // Query without ORDER BY - should use partial + final aggregation.
  logicalPlan = lp::PlanBuilder(makeContext())
                    .tableScan("t")
                    .aggregate({"k"}, {"sum(v1)"})
                    .build();
  matcher = matchScan("t")
                .partialAggregation({"k"}, {"sum(v1)"})
                .shuffle()
                .localPartition()
                .finalAggregation()
                .shuffle()
                .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
}

// array_agg keeps every input row, so on its own it runs in a single step.
// Next to sum, it splits into partial and final steps.
TEST_P(AggregationTest, nonReducingAggregates) {
  testConnector_->addTable("t", ROW({"k", "v"}, BIGINT()));

  auto logicalPlan = parseSelect("SELECT k, array_agg(v) FROM t GROUP BY k");
  AXIOM_ASSERT_PLAN_V2(
      toSingleNodePlan(logicalPlan, /*numDrivers=*/4),
      matchScan("t")
          .localPartition({"k"})
          .singleAggregation({"k"}, {"array_agg(v)"})
          .build());
  AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
      planVelox(logicalPlan).plan,
      matchScan("t")
          .distributedSingleAggregation({"k"}, {"array_agg(v)"})
          .shuffle()
          .build());

  logicalPlan = parseSelect("SELECT k, array_agg(v), sum(v) FROM t GROUP BY k");
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(logicalPlan, /*numDrivers=*/4),
      matchScan("t")
          .localAggregation({"k"}, {"array_agg(v)", "sum(v)"})
          .build());
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planVelox(logicalPlan).plan,
      matchScan("t")
          .distributedAggregation({"k"}, {"array_agg(v)", "sum(v)"})
          .shuffle()
          .build());
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
                  MultiFragmentPlan::Options{
                      .maxRemotePartitions = 4, .maxLocalPartitions = 4},
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
    // The projection preserves partitioning on [a, b], which also co-locates
    // the second aggregation's groups on [a, b, d].
    auto matcher = matchScan("t")
                       .partialAggregation({"a", "b"}, {})
                       .shuffle({"a", "b"})
                       .localPartition({"a", "b"})
                       .finalAggregation({"a", "b"}, {})
                       .project({"a", "b", "a + b as d"})
                       .streamingAggregation({"a", "b", "d"}, {})
                       .gather()
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(planVelox(logicalPlan).plan, matcher);
  }

  {
    SCOPED_TRACE(
        "partitionKeys is not a subset of groupingKeys, shuffle needed");
    auto logicalPlan = lp::PlanBuilder(makeContext())
                           .tableScan("t")
                           .aggregate({"a", "b", "c"}, {})
                           .aggregate({"a", "b"}, {})
                           .build();
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
    AXIOM_ASSERT_DISTRIBUTED_PLAN_V1(planVelox(logicalPlan).plan, matcher);
  }
}

TEST_P(AggregationTest, rightSemiJoinPartitioning) {
  testConnector_->addTable("t", ROW("a", BIGINT()))
      ->setStats(100, {{"a", {.numDistinct = 100}}});
  testConnector_->addTable("u", ROW("b", BIGINT()))
      ->setStats(10'000, {{"b", {.numDistinct = 10'000}}});

  const auto logicalPlan = parseSelect(
      "SELECT a, count(*) "
      "FROM t "
      "WHERE EXISTS (SELECT 1 FROM u WHERE b = a) "
      "GROUP BY a");

  // EXISTS lowers to a right semi filter that preserves its right input's
  // partitioning, so the aggregation requires no remote shuffle.
  AXIOM_ASSERT_PLAN_V2(
      toSingleNodePlan(logicalPlan, /*numDrivers=*/4),
      matchScan("u")
          .hashJoinRightSemiFilter(
              matchScan("t"),
              {.keys = {{"b = a"}}, .outputColumnNames = {{"a"}}})
          .localAggregation({"a"}, {"count(*) as count"})
          .build());

  AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
      planVelox(logicalPlan).plan,
      matchScan("u")
          .shuffle({"b"})
          .hashJoinRightSemiFilter(
              matchScan("t").shuffle({"a"}),
              {.keys = {{"b = a"}}, .outputColumnNames = {{"a"}}})
          .partialAggregation({"a"}, {"count(*) as count"})
          .localPartition({"a"})
          .finalAggregation({"a"}, {"count(count) as count"})
          .gather()
          .build());
}

TEST_P(AggregationTest, reversedAntiJoinPartitioning) {
  testConnector_->addTable("t", ROW("a", BIGINT()))
      ->setStats(100, {{"a", {.numDistinct = 100}}});
  testConnector_->addTable("u", ROW("b", BIGINT()))
      ->setStats(10'000, {{"b", {.numDistinct = 10'000}}});

  {
    SCOPED_TRACE("NOT EXISTS");
    const auto logicalPlan = parseSelect(
        "SELECT a, count(*) "
        "FROM t "
        "WHERE NOT EXISTS (SELECT 1 FROM u WHERE b = a) "
        "GROUP BY a");

    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan, /*numDrivers=*/4),
        matchScan("u")
            .hashJoinRightSemiProject(
                matchScan("t"), {.nullAware = false, .keys = {{"b = a"}}})
            .aliases({"a", "matched"})
            .filter("not(matched)")
            .project({"a"})
            .localAggregation({"a"}, {"count(*) as count"})
            .build());

    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(logicalPlan).plan,
        matchScan("u")
            .shuffle({"b"})
            .hashJoinRightSemiProject(
                matchScan("t").shuffle({"a"}),
                {.nullAware = false, .keys = {{"b = a"}}})
            .aliases({"a", "matched"})
            .filter("not(matched)")
            .project({"a"})
            .partialAggregation({"a"}, {"count(*) as count"})
            .localPartition({"a"})
            .finalAggregation({"a"}, {"count(count) as count"})
            .gather()
            .build());
  }

  {
    SCOPED_TRACE("NOT IN");
    const auto logicalPlan = parseSelect(
        "SELECT a, count(*) "
        "FROM t "
        "WHERE a NOT IN (SELECT b FROM u) "
        "GROUP BY a");

    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan, /*numDrivers=*/4),
        matchScan("u")
            .hashJoinRightSemiProject(
                matchScan("t"), {.nullAware = true, .keys = {{"b = a"}}})
            .aliases({"a", "matched"})
            .filter("not(matched)")
            .project({"a"})
            .localAggregation({"a"}, {"count(*) as count"})
            .build());

    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(logicalPlan).plan,
        matchScan("u")
            .shuffle({"b"}, /*replicateNullsAndAny=*/true)
            .hashJoinRightSemiProject(
                matchScan("t").shuffle({"a"}),
                {.nullAware = true, .keys = {{"b = a"}}})
            .aliases({"a", "matched"})
            .filter("not(matched)")
            .project({"a"})
            .partialAggregation({"a"}, {"count(*) as count"})
            .localPartition({"a"})
            .finalAggregation({"a"}, {"count(count) as count"})
            .gather()
            .build());
  }
}

TEST_P(AggregationTest, rightJoinPartitioning) {
  testConnector_->addTable("t", ROW("a", BIGINT()))
      ->setStats(10'000, {{"a", {.numDistinct = 10'000}}});
  testConnector_->addTable("u", ROW({"b", "b2"}, BIGINT()))
      ->setStats(
          100, {{"b", {.numDistinct = 100}}, {"b2", {.numDistinct = 100}}});

  {
    SCOPED_TRACE("grouping on the preserved join key");
    const auto logicalPlan = parseSelect(
        "SELECT b, count(*) "
        "FROM t RIGHT JOIN u ON a = b "
        "GROUP BY b");

    // The aggregation reuses the right join's partitioning on its preserved
    // right key.
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan, /*numDrivers=*/4),
        matchScan("t")
            .hashJoinRight(
                matchScan("u"),
                {.keys = {{"a = b"}}, .outputColumnNames = {{"b"}}})
            .localAggregation({"b"}, {"count(*) as count"})
            .build());

    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(logicalPlan).plan,
        matchScan("t")
            .shuffle({"a"})
            .hashJoinRight(
                matchScan("u").shuffle({"b"}),
                {.keys = {{"a = b"}}, .outputColumnNames = {{"b"}}})
            .partialAggregation({"b"}, {"count(*) as count"})
            .localPartition({"b"})
            .finalAggregation({"b"}, {"count(count) as count"})
            .gather()
            .build());
  }

  {
    SCOPED_TRACE("join key dropped from the output");
    const auto logicalPlan = parseSelect(
        "SELECT b2, count(*) "
        "FROM t RIGHT JOIN u ON a = b "
        "GROUP BY b2");

    // The join key does not reach the output, so the join reports no key
    // partitioning and the aggregation shuffles on the grouping key.
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan, /*numDrivers=*/4),
        matchScan("t")
            .hashJoinRight(
                matchScan("u"),
                {.keys = {{"a = b"}}, .outputColumnNames = {{"b2"}}})
            .localAggregation({"b2"}, {"count(*) as count"})
            .build());

    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(logicalPlan).plan,
        matchScan("t")
            .shuffle({"a"})
            .hashJoinRight(
                matchScan("u").shuffle({"b"}),
                {.keys = {{"a = b"}}, .outputColumnNames = {{"b2"}}})
            .partialAggregation({"b2"}, {"count(*) as count"})
            .shuffle({"b2"})
            .localPartition({"b2"})
            .finalAggregation({"b2"}, {"count(count) as count"})
            .gather()
            .build());
  }
}

// An inner join makes its keys equal, so grouping on either key reuses the
// join's partitioning.
TEST_P(AggregationTest, innerJoinPartitioning) {
  testConnector_->addTable("t", ROW("a", BIGINT()));
  testConnector_->addTable("u", ROW("b", BIGINT()));
  optimizerOptions_.broadcastSizeLimit = 0;

  // Grouping on either key while counting the left key: both read the left
  // key, which is all the join outputs.
  for (const std::string key : {"a", "b"}) {
    const auto sql = fmt::format(
        "SELECT {}, count(a) FROM t JOIN u ON a = b GROUP BY 1", key);
    SCOPED_TRACE(sql);
    const auto logicalPlan = parseSelect(sql);

    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan, /*numDrivers=*/4),
        matchScan("t")
            .hashJoinInner(
                matchScan("u"),
                {.keys = {{"a = b"}}, .outputColumnNames = {{"a"}}})
            .localAggregation({"a"}, {"count(a) as count"})
            .projectIf(key == "b", {"a as b", "count"})
            .build());

    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(logicalPlan).plan,
        matchScan("t")
            .shuffle({"a"})
            .hashJoinInner(
                matchScan("u").shuffle({"b"}),
                {.keys = {{"a = b"}}, .outputColumnNames = {{"a"}}})
            .partialAggregation({"a"}, {"count(a) as count"})
            .localPartition({"a"})
            .finalAggregation({"a"}, {"count(count) as count"})
            .projectIf(key == "b", {"a as b", "count"})
            .gather()
            .build());
  }

  {
    // Equal grouping keys are grouped once.
    const auto logicalPlan = parseSelect(
        "SELECT a, b, count(*) FROM t JOIN u ON a = b GROUP BY 1, 2");

    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan, /*numDrivers=*/4),
        matchScan("t")
            .hashJoinInner(
                matchScan("u"),
                {.keys = {{"a = b"}}, .outputColumnNames = {{"a"}}})
            .localAggregation({"a"}, {"count(*) as count"})
            .project({"a", "a as b", "count"})
            .build());

    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(logicalPlan).plan,
        matchScan("t")
            .shuffle({"a"})
            .hashJoinInner(
                matchScan("u").shuffle({"b"}),
                {.keys = {{"a = b"}}, .outputColumnNames = {{"a"}}})
            .partialAggregation({"a"}, {"count(*) as count"})
            .localPartition({"a"})
            .finalAggregation({"a"}, {"count(count) as count"})
            .project({"a", "a as b", "count"})
            .gather()
            .build());
  }
}

// A join keeps each row of a side it preserves and does not replicate on that
// row's task, so a key unique on that side keeps its groups on one task even
// where the join repeats its rows, and grouping on it needs no shuffle. A
// padded side's key does not, and neither does a key after a later shuffle.
TEST_P(AggregationTest, joinColocation) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  testConnector_->addTable("u", ROW({"c", "d"}, BIGINT()));
  testConnector_->addTable("w", ROW({"e", "f"}, BIGINT()));
  testConnector_->addTable("small", ROW({"a", "b"}, BIGINT()))
      ->setStats(10, {{"a", {.numDistinct = 10}}, {"b", {.numDistinct = 10}}});
  testConnector_->addTable("large", ROW({"c", "d"}, BIGINT()))
      ->setStats(
          1'000'000,
          {{"c", {.numDistinct = 1'000'000}}, {"d", {.numDistinct = 10}}});
  optimizerOptions_.broadcastSizeLimit = 0;

  // The key 'a' of the aggregated side, before the join.
  const auto matchKeyed = [&]() {
    return matchScan("t")
        .partialAggregation({"a"}, {"max(b) as v"})
        .shuffle({"a"})
        .localPartition({"a"})
        .finalAggregation({"a"}, {"max(v) as v"})
        .shuffle({"v"});
  };

  // Grouping on a key whose rows sit on several tasks shuffles.
  const auto matchShuffledGroups = [&](auto joined, std::string_view key) {
    return joined.partialAggregation({std::string(key)}, {"count(*) as count"})
        .shuffle({std::string(key)})
        .localPartition({std::string(key)})
        .finalAggregation({std::string(key)}, {"count(count) as count"});
  };

  {
    SCOPED_TRACE("INNER");
    auto logicalPlan = parseSelect(
        "SELECT x.k, count(*) "
        "FROM (SELECT a AS k, max(b) AS v FROM t GROUP BY a) x "
        "JOIN u ON x.v = u.d "
        "GROUP BY x.k");
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation({"a"}, {"max(b) as v"})
            .hashJoinInner(matchScan("u"), {.keys = {{"v = d"}}})
            .streamingAggregation({"a"}, {"count(*) as count"})
            .project({"a as k", "count"})
            .build());
    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(logicalPlan).plan,
        matchKeyed()
            .hashJoinInner(matchScan("u").shuffle({"d"}), {.keys = {{"v = d"}}})
            .streamingAggregation({"a"}, {"count(*) as count"})
            .project({"a as k", "count"})
            .gather()
            .build());
  }

  {
    SCOPED_TRACE("INNER, both sides keyed");
    auto logicalPlan = parseSelect(
        "SELECT y.m, count(*) "
        "FROM (SELECT a AS k, max(b) AS v FROM t GROUP BY a) x "
        "JOIN (SELECT c AS m, max(d) AS w FROM u GROUP BY c) y "
        "ON x.v = y.w "
        "GROUP BY y.m");
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation({"a"}, {"max(b) as v"})
            .hashJoinInner(
                matchScan("u").singleAggregation({"c"}, {"max(d) as w"}),
                {.keys = {{"v = w"}}})
            .singleAggregation({"c"}, {"count(*) as count"})
            .project({"c as m", "count"})
            .build());
    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(logicalPlan).plan,
        matchKeyed()
            .hashJoinInner(
                matchScan("u")
                    .partialAggregation({"c"}, {"max(d) as w"})
                    .shuffle({"c"})
                    .localPartition({"c"})
                    .finalAggregation({"c"}, {"max(w) as w"})
                    .shuffle({"w"}),
                {.keys = {{"v = w"}}})
            .partialAggregation({"c"}, {"count(*) as count"})
            .localPartition({"c"})
            .finalAggregation({"c"}, {"count(count) as count"})
            .project({"c as m", "count"})
            .gather()
            .build());
  }

  {
    SCOPED_TRACE("RIGHT");
    auto logicalPlan = parseSelect(
        "SELECT x.k, count(*) "
        "FROM (SELECT a AS k, max(b) AS v FROM t GROUP BY a) x "
        "RIGHT JOIN u ON x.v = u.d "
        "GROUP BY x.k");
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation({"a"}, {"max(b) as v"})
            .hashJoin(
                matchScan("u"), core::JoinType::kRight, {.keys = {{"v = d"}}})
            .singleAggregation({"a"}, {"count(*) as count"})
            .project({"a as k", "count"})
            .build());
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        planVelox(logicalPlan).plan,
        matchShuffledGroups(
            matchKeyed().hashJoin(
                matchScan("u").shuffle({"d"}),
                core::JoinType::kRight,
                {.keys = {{"v = d"}}}),
            "a")
            .project({"a as k", "count"})
            .gather()
            .build());
  }

  {
    SCOPED_TRACE("LEFT, keyed build side");
    auto logicalPlan = parseSelect(
        "SELECT y.m, count(*) "
        "FROM u "
        "LEFT JOIN (SELECT a AS m, max(b) AS w FROM t GROUP BY a) y "
        "ON u.d = y.w "
        "GROUP BY y.m");
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("u")
            .hashJoin(
                matchScan("t").singleAggregation({"a"}, {"max(b) as w"}),
                core::JoinType::kLeft,
                {.keys = {{"d = w"}}})
            .singleAggregation({"a"}, {"count(*) as count"})
            .project({"a as m", "count"})
            .build());
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        planVelox(logicalPlan).plan,
        matchShuffledGroups(
            matchScan("u").shuffle({"d"}).hashJoin(
                matchScan("t")
                    .partialAggregation({"a"}, {"max(b) as w"})
                    .shuffle({"a"})
                    .localPartition({"a"})
                    .finalAggregation({"a"}, {"max(w) as w"})
                    .shuffle({"w"}),
                core::JoinType::kLeft,
                {.keys = {{"d = w"}}}),
            "a")
            .project({"a as m", "count"})
            .gather()
            .build());
  }

  {
    SCOPED_TRACE("a shuffle in between");
    auto logicalPlan = parseSelect(
        "SELECT x.k, count(*) "
        "FROM (SELECT a AS k, max(b) AS v FROM t GROUP BY a) x "
        "JOIN u ON x.v = u.d "
        "JOIN w ON u.c = w.e "
        "GROUP BY x.k");
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation({"a"}, {"max(b) as v"})
            .hashJoinInner(matchScan("u"), {.keys = {{"v = d"}}})
            .hashJoinInner(matchScan("w"), {.keys = {{"c = e"}}})
            .streamingAggregation({"a"}, {"count(*) as count"})
            .project({"a as k", "count"})
            .build());
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        planVelox(logicalPlan).plan,
        matchShuffledGroups(
            matchKeyed()
                .hashJoinInner(
                    matchScan("u").shuffle({"d"}), {.keys = {{"v = d"}}})
                .shuffle({"c"})
                .hashJoinInner(
                    matchScan("w").shuffle({"e"}), {.keys = {{"c = e"}}}),
            "a")
            .project({"a as k", "count"})
            .gather()
            .build());
  }

  {
    SCOPED_TRACE("replicated side");
    auto logicalPlan = parseSelect(
        "SELECT x.k, count(*) "
        "FROM (SELECT a AS k, max(b) AS v FROM small GROUP BY a) x "
        "JOIN large ON x.v = large.d "
        "GROUP BY x.k");
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("large")
            .hashJoinInner(
                matchScan("small").singleAggregation({"a"}, {"max(b) as v"}),
                {.keys = {{"d = v"}}})
            .singleAggregation({"a"}, {"count(*) as count"})
            .project({"a as k", "count"})
            .build());
    // Every task holds a copy of the aggregated side, so its key repeats
    // across tasks.
    optimizerOptions_.broadcastSizeLimit =
        OptimizerOptions{}.broadcastSizeLimit;
    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(logicalPlan).plan,
        matchShuffledGroups(
            matchScan("large").hashJoinInner(
                matchScan("small")
                    .partialAggregation({"a"}, {"max(b) as v"})
                    .shuffle({"a"})
                    .localPartition({"a"})
                    .finalAggregation({"a"}, {"max(v) as v"})
                    .broadcast(),
                {.keys = {{"d = v"}}}),
            "a")
            .project({"a as k", "count"})
            .gather()
            .build());
  }
}

// Grouping on the element an Unnest produces shuffles. The Unnest keeps no
// colocated set here: it replicates no column, and its input's set is over the
// array it unnests.
TEST_P(AggregationTest, unnestColocation) {
  testConnector_->addTable("v", ROW("a", ARRAY(BIGINT())));
  optimizerOptions_.broadcastSizeLimit = 0;
  auto logicalPlan = parseSelect(
      "SELECT e, count(*) "
      "FROM (SELECT DISTINCT a FROM v) x "
      "CROSS JOIN UNNEST(x.a) AS w(e) "
      "GROUP BY e");
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(logicalPlan),
      matchScan("v")
          .singleAggregation({"a"}, {})
          .unnest({}, {"a"})
          .aliases({"e"})
          .singleAggregation({"e"}, {"count(*) as count"})
          .build());
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planVelox(logicalPlan).plan,
      matchScan("v")
          .partialAggregation({"a"}, {})
          .shuffle({"a"})
          .localPartition({"a"})
          .finalAggregation({"a"}, {})
          .unnest({}, {"a"})
          .aliases({"e"})
          .partialAggregation({"e"}, {"count(*) as count"})
          .shuffle({"e"})
          .localPartition({"e"})
          .finalAggregation({"e"}, {"count(count) as count"})
          .gather()
          .build());
}

// A consumer that needs each key's rows on one task runs without a shuffle over
// a join that keeps the key's rows on one task: the single-row check of a
// scalar subquery on each outer row's ID, also behind a row numbering and
// behind an aggregation whose keys contain the ID, and a window, a per-key
// top-n and a DISTINCT with a LIMIT on a key unique on one side of the join.
TEST_P(AggregationTest, colocatedConsumers) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  testConnector_->addTable("u", ROW({"c", "d"}, BIGINT()));
  optimizerOptions_.broadcastSizeLimit = 0;

  // The key 'a' of the aggregated side, before the join.
  const auto matchKeyed = [&]() {
    return matchScan("t")
        .partialAggregation({"a"}, {"max(b) as v"})
        .shuffle({"a"})
        .localPartition({"a"})
        .finalAggregation({"a"}, {"max(v) as v"});
  };

  // Each row of the aggregated side gets an ID, and a LEFT join keeps the rows
  // of an ID on its task.
  const auto matchLeftJoined = [&]() {
    return matchKeyed().assignUniqueId("row_id").shuffle({"v"}).hashJoin(
        matchScan("u").shuffle({"d"}),
        core::JoinType::kLeft,
        {.keys = {{"v = d"}}});
  };

  // An inner join keeps the rows of each key of the aggregated side on its
  // task.
  const auto matchJoined = [&]() {
    return matchKeyed().shuffle({"v"}).hashJoinInner(
        matchScan("u").shuffle({"d"}), {.keys = {{"v = d"}}});
  };

  {
    SCOPED_TRACE("scalar subquery");
    auto logicalPlan = parseSelect(
        "SELECT x.k, (SELECT u.c FROM u WHERE u.d = x.v) "
        "FROM (SELECT a AS k, max(b) AS v FROM t GROUP BY a) x");
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation({"a"}, {"max(b) as v"})
            .assignUniqueId("row_id")
            .hashJoin(
                matchScan("u"), core::JoinType::kLeft, {.keys = {{"v = d"}}})
            .enforceDistinct({"row_id"})
            .project({"a as k", "c"})
            .build());
    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(logicalPlan).plan,
        matchLeftJoined()
            .localPartition({"row_id"})
            .enforceDistinct({"row_id"})
            .project({"a as k", "c"})
            .gather()
            .build());
  }

  {
    SCOPED_TRACE("window");
    auto logicalPlan = parseSelect(
        "SELECT x.k, u.c, sum(u.c) OVER (PARTITION BY x.k) AS s "
        "FROM (SELECT a AS k, max(b) AS v FROM t GROUP BY a) x "
        "JOIN u ON x.v = u.d");
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation({"a"}, {"max(b) as v"})
            .hashJoinInner(matchScan("u"), {.keys = {{"v = d"}}})
            .window({"sum(c) OVER (PARTITION BY a) as s"})
            .project({"a as k", "c", "s"})
            .build());
    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(logicalPlan).plan,
        matchJoined()
            .window({"sum(c) OVER (PARTITION BY a) as s"})
            .project({"a as k", "c", "s"})
            .gather()
            .build());
  }

  {
    SCOPED_TRACE("per-key top-n");
    auto logicalPlan = parseSelect(
        "SELECT k, c "
        "FROM ("
        "  SELECT x.k, u.c, "
        "    row_number() OVER (PARTITION BY x.k ORDER BY u.c) AS rn "
        "  FROM (SELECT a AS k, max(b) AS v FROM t GROUP BY a) x "
        "  JOIN u ON x.v = u.d"
        ") "
        "WHERE rn <= 2");
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation({"a"}, {"max(b) as v"})
            .hashJoinInner(matchScan("u"), {.keys = {{"v = d"}}})
            .topNRowNumber({"a"}, {"c"}, 2, false)
            .project({"a as k", "c"})
            .build());
    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(logicalPlan).plan,
        matchJoined()
            .topNRowNumber({"a"}, {"c"}, 2, false)
            .project({"a as k", "c"})
            .gather()
            .build());
  }

  {
    SCOPED_TRACE("DISTINCT with a LIMIT");
    auto logicalPlan = parseSelect(
        "SELECT DISTINCT x.k, u.c "
        "FROM (SELECT a AS k, max(b) AS v FROM t GROUP BY a) x "
        "JOIN u ON x.v = u.d "
        "LIMIT 5");
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation({"a"}, {"max(b) as v"})
            .hashJoinInner(matchScan("u"), {.keys = {{"v = d"}}})
            .singleAggregation({"a", "c"}, {})
            .finalLimit(0, 5)
            .project({"a as k", "c"})
            .build());
    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(logicalPlan).plan,
        matchJoined()
            .singleAggregation({"a", "c"}, {})
            .partialLimit(0, 5)
            .localGather()
            .finalLimit(0, 5)
            .gather()
            .finalLimit(0, 5)
            .project({"a as k", "c"})
            .build());
  }

  // v1 fails to plan the shapes below.
  if (!useV2_) {
    return;
  }

  {
    SCOPED_TRACE("row numbering, then the single-row check");
    auto logicalPlan = parseSelect(
        "SELECT x.k, (SELECT u.c FROM u WHERE u.d = x.v LIMIT 2) "
        "FROM (SELECT a AS k, max(b) AS v FROM t GROUP BY a) x");
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation({"a"}, {"max(b) as v"})
            .assignUniqueId("row_id")
            .hashJoin(
                matchScan("u"), core::JoinType::kLeft, {.keys = {{"v = d"}}})
            .rowNumber({"row_id"}, 2)
            .enforceDistinct({"row_id"})
            .project({"a as k", "c"})
            .build());
    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(logicalPlan).plan,
        matchLeftJoined()
            .localPartition({"row_id"})
            .rowNumber({"row_id"}, 2)
            .enforceDistinct({"row_id"})
            .project({"a as k", "c"})
            .gather()
            .build());
  }

  {
    SCOPED_TRACE("grouped body, then the single-row check");
    auto logicalPlan = parseSelect(
        "SELECT x.k, (SELECT count(*) FROM u WHERE u.d < x.v GROUP BY u.c) "
        "FROM (SELECT a AS k, max(b) AS v FROM t GROUP BY a) x");
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation({"a"}, {"max(b) as v"})
            .assignUniqueId("row_id")
            .nestedLoopJoin(
                matchScan("u").project({"c", "d", "true as marker"}),
                core::JoinType::kLeft,
                "v > d")
            .singleAggregation(
                {"row_id", "c"},
                {"count() filter (where marker) as n",
                 "arbitrary(a) as a",
                 "arbitrary(marker) as real_group"})
            .enforceDistinct({"row_id"})
            .project({"a as k", "if(real_group, n, null)"})
            .build());
    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(logicalPlan).plan,
        matchKeyed()
            .assignUniqueId("row_id")
            .nestedLoopJoin(
                matchScan("u")
                    .project({"c", "d", "true as marker"})
                    .broadcast(),
                core::JoinType::kLeft,
                "v > d")
            .partialAggregation(
                {"row_id", "c"},
                {"count() filter (where marker) as n",
                 "arbitrary(a) as a",
                 "arbitrary(marker) as real_group"})
            .localPartition({"row_id", "c"})
            .finalAggregation(
                {"row_id", "c"},
                {"count(n) as n",
                 "arbitrary(a) as a",
                 "arbitrary(real_group) as real_group"})
            .localPartition({"row_id"})
            .enforceDistinct({"row_id"})
            .project({"a as k", "if(real_group, n, null)"})
            .gather()
            .build());
  }
}

// A colocated set that another set implies is not kept: one that contains a
// non-empty unique key or another colocated set. Of equal sets one stays, and
// unique keys are kept. A unique empty key drops no set.
TEST_P(AggregationTest, colocatedSetPruning) {
  if (!useV2_) {
    return;
  }
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  verifyOptimization(
      *parseSelect("SELECT a, b FROM t"),
      Optimizer::Pass::kPlanPhysical,
      [&](NodeCP root) {
        const auto a = PlanObjectSet::single(root->outputColumns()[0]);
        const auto b = PlanObjectSet::single(root->outputColumns()[1]);
        auto both = a;
        both.unionSet(b);
        const auto pruned = [](PhysicalProperties properties) {
          properties.pruneColocated();
          return properties;
        };
        {
          SCOPED_TRACE("contains a unique key");
          EXPECT_THAT(
              pruned({.globalUnique = {a}, .globalColocated = {both}})
                  .globalColocated,
              testing::IsEmpty());
        }
        {
          SCOPED_TRACE("contains another colocated set");
          EXPECT_THAT(
              pruned({.globalColocated = {both, b, b}}).globalColocated,
              testing::ElementsAre(b));
        }
        {
          SCOPED_TRACE("inside a unique key");
          const auto properties =
              pruned({.globalUnique = {both}, .globalColocated = {b}});
          EXPECT_THAT(properties.globalColocated, testing::ElementsAre(b));
          EXPECT_THAT(properties.globalUnique, testing::ElementsAre(both));
        }
        {
          SCOPED_TRACE("unique empty key");
          EXPECT_THAT(
              pruned(
                  {.globalUnique = {PlanObjectSet{}}, .globalColocated = {b}})
                  .globalColocated,
              testing::ElementsAre(b));
        }
      });
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
    auto plan = planVelox(
        logicalPlan, {.maxRemotePartitions = 4, .maxLocalPartitions = 4});
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
    auto plan = planVelox(
        logicalPlan, {.maxRemotePartitions = 4, .maxLocalPartitions = 1});
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

TEST_P(AggregationTest, uniqueInput) {
  if (!useV2_) {
    return;
  }

  // Grouping by a superset of a unique key puts each group in one row, so the
  // aggregation runs where the rows are read, without an exchange. A filter
  // that makes a key column constant leaves the rest of the key unique.
  auto definition =
      connector::TableDefinition::create(ROW({"k1", "k2", "g"}, BIGINT()), {});
  definition.primaryKey = {"k1", "k2"};
  testConnector_->addTable("v", std::move(definition));

  {
    SCOPED_TRACE("primary key");
    const auto logicalPlan = parseSelect(
        "SELECT k1, k2, g, count(*) AS n "
        "FROM v "
        "GROUP BY k1, k2, g");

    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan, /*numDrivers=*/4),
        matchScan("v")
            .streamingAggregation({"k1", "k2", "g"}, {"count(*)"})
            .build());

    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(logicalPlan).plan,
        matchScan("v")
            .streamingAggregation({"k1", "k2", "g"}, {"count(*)"})
            .gather()
            .build());
  }

  {
    SCOPED_TRACE("primary key with a constant column");
    const auto logicalPlan = parseSelect(
        "SELECT k1, g, count(*) AS n "
        "FROM v "
        "WHERE k2 = 1 "
        "GROUP BY k1, g");

    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan, /*numDrivers=*/4),
        matchScan("v")
            .filter("k2 = 1")
            .project({"k1", "g"})
            .streamingAggregation({"k1", "g"}, {"count(*)"})
            .build());

    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(logicalPlan).plan,
        matchScan("v")
            .filter("k2 = 1")
            .project({"k1", "g"})
            .streamingAggregation({"k1", "g"}, {"count(*)"})
            .gather()
            .build());
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
    auto matcher =
        matchScan("t")
            .groupId({{"a", "b"}, {"a"}, {}}, {"c"}, "gid")
            .distributedAggregation({"a", "b", "gid"}, {"sum(c) as total"})
            .project({"a", "b", "total", "gid"})
            .gather()
            .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
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
    auto matcher = matchScan("t")
                       .groupId({{"a"}, {"b"}}, {}, "gid")
                       .partialAggregation({"a", "b", "gid"}, {"count(1) as c"})
                       .shuffle()
                       .localPartition()
                       .finalAggregation({"a", "b", "gid"}, {"count(c) as c"})
                       .project({"a", "b", "c", "gid"})
                       .gather()
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
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
    auto matcher =
        matchScan("t")
            .groupId({{"a"}, {}}, {"b"}, "gid")
            .gather()
            .localPartition({"a", "gid"})
            .singleAggregation({"a", "gid"}, {"array_agg(b ORDER BY b) as arr"})
            .project({"a", "arr", "gid"})
            .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
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
    auto matcher = matchScan("t")
                       .groupId({{"a"}, {}}, {}, "gid")
                       .partialAggregation({"a", "gid"}, {"count(1) as c"})
                       .shuffle()
                       .localPartition()
                       .finalAggregation({"a", "gid"}, {"count(c) as c"})
                       .project({"a", "c", "gid"})
                       .gather()
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
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
      planVelox(logicalPlan).plan, distributedMatcher);
}

TEST_P(AggregationTest, ifToMask) {
  testConnector_->addTable("t", ROW({"a", "b", "c"}, BIGINT()));

  auto logicalPlan = lp::PlanBuilder(makeContext())
                         .tableScan("t")
                         .project(
                             {"if(b > 0, a, null) as x",
                              "if(c > 0, a, null) as y",
                              "if(b > 0, c, null) as w",
                              "b",
                              "if(b > 0, a, null) as z"})
                         .aggregate(
                             {},
                             {"sum(x)",
                              "sum(y) FILTER (WHERE b < 100)",
                              "sum(w)",
                              "array_agg(z)"})
                         .build();

  AXIOM_ASSERT_PLAN_V2(
      toSingleNodePlan(logicalPlan),
      matchScan("t")
          .project(
              {"a",
               "b > 0 as m1",
               "b < 100 AND c > 0 as m2",
               "c",
               "if(b > 0, a, null) as p"})
          .singleAggregation(
              {},
              {"sum(a) FILTER (WHERE m1)",
               "sum(a) FILTER (WHERE m2)",
               "sum(c) FILTER (WHERE m1)",
               "array_agg(p)"})
          .build());
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
  // v2 does not distribute DISTINCT aggregates: it gathers to one task and
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
      planVelox(logicalPlan).plan, distributedMatcher);
}

// The ORDER BY of an order-insensitive aggregate is never translated, so
// nothing it reads reaches the plan.
TEST_P(AggregationTest, orderInsensitiveOrderByNotTranslated) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  testConnector_->addTable("u", ROW({"x"}, BIGINT()));

  auto matcher = matchScan("t", ROW("a", BIGINT()))
                     .singleAggregation({}, {"sum(a)"})
                     .build();

  {
    auto logicalPlan = parseSelect("SELECT sum(a ORDER BY b) FROM t");
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    auto logicalPlan =
        parseSelect("SELECT sum(a ORDER BY (SELECT max(x) FROM u)) FROM t");
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }
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
