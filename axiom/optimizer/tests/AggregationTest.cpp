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

#include <gtest/gtest.h>
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class AggregationTest : public test::QueryTestBase {
 protected:
  lp::PlanBuilder::Context makeContext() const {
    return lp::PlanBuilder::Context{kTestConnectorId, kDefaultSchema};
  }
};

TEST_F(AggregationTest, dedupGroupingKeysAndAggregates) {
  testConnector_->addTable(
      "numbers", ROW({"a", "b", "c"}, {BIGINT(), BIGINT(), DOUBLE()}));

  {
    auto logicalPlan = lp::PlanBuilder(makeContext())
                           .tableScan("numbers")
                           .project({"a + b as x", "a + b as y", "c"})
                           .aggregate({"x", "y"}, {"count(1)", "count(1)"})
                           .build();

    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan()
                       .project({"a + b"})
                       .singleAggregation({"x"}, {"count(1)"})
                       .project({"x", "x", "count", "count"})
                       .build();

    ASSERT_TRUE(matcher->match(plan));
  }
}

TEST_F(AggregationTest, duplicatesBetweenGroupAndAggregate) {
  testConnector_->addTable("t", ROW({"a", "b"}, {BIGINT(), BIGINT()}));

  auto logicalPlan = lp::PlanBuilder(makeContext())
                         .tableScan("t")
                         .project({"a + b AS ab1", "a + b AS ab2"})
                         .aggregate({"ab1", "ab2"}, {"count(ab2) AS c1"})
                         .project({"ab1 AS x", "ab2 AS y", "c1 AS z"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan()
                     .project({"plus(a, b)"})
                     .singleAggregation({"ab1"}, {"count(ab1)"})
                     .project({"ab1", "ab1", "c1"})
                     .build();

  ASSERT_TRUE(matcher->match(plan));
}

TEST_F(AggregationTest, dedupMask) {
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

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan()
                     .project({"b > 0 as m1", "a", "b < 0 as m2"})
                     .singleAggregation(
                         {},
                         {
                             "sum(a) FILTER (WHERE m1) as s1",
                             "sum(a) FILTER (WHERE m2) as s2",
                         })
                     .project({"s1", "s2", "s1"})
                     .build();

  ASSERT_TRUE(matcher->match(plan));
}

TEST_F(AggregationTest, dedupOrderBy) {
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

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan()
                     .project({"a", "b", "a + b as p0", "c", "b * 2 as p1"})
                     .singleAggregation(
                         {},
                         {"array_agg(a ORDER BY a)",
                          "array_agg(b ORDER BY b, a)",
                          "array_agg(a ORDER BY p0, c)",
                          "array_agg(c ORDER BY p1)"})
                     .build();

  ASSERT_TRUE(matcher->match(plan));
}

TEST_F(AggregationTest, dedupSameOptions) {
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
      core::PlanMatcherBuilder()
          .tableScan()
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

  ASSERT_TRUE(matcher->match(plan));
}

// Verifies that aggregation with ORDER BY keys always uses single-step
// aggregation, even in distributed mode where partial+final would normally
// be used. This is required because partial aggregation cannot preserve
// global ordering across workers.
TEST_F(AggregationTest, orderBy) {
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
  auto matcher = core::PlanMatcherBuilder()
                     .tableScan()
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

  matcher = core::PlanMatcherBuilder()
                .tableScan()
                .partialAggregation({"k"}, {"sum(v1)"})
                .shuffle()
                .localPartition()
                .finalAggregation()
                .shuffle()
                .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
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
TEST_F(AggregationTest, repartitionForAggPartitionSubset) {
  auto schema = ROW({"a", "b", "c", "v"}, BIGINT());
  testConnector_->addTable("t", schema);
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
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan()
                       .distributedSingleAggregation({"a", "b"}, {})
                       .project()
                       // No shuffle here - partitionKeys ⊆ groupingKeys
                       .localPartition()
                       .singleAggregation({"a", "b", "d"}, {})
                       .shuffle()
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
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
    auto matcher = core::PlanMatcherBuilder()
                       .tableScan()
                       .distributedSingleAggregation({"a", "b", "c"}, {})
                       .project()
                       .distributedSingleAggregation({"a", "b"}, {})
                       .shuffle()
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
  }
}

// TODO: Add tests for maybeProject() cost tracking once Project::unitCost is
// implemented (currently 0, see the TODO in Project::Project). The
// optimizationCost() helper is available for verifying cost differences between
// plans with and without projections.

TEST_F(AggregationTest, groupingSets) {
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
        core::PlanMatcherBuilder()
            .tableScan()
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
        core::PlanMatcherBuilder()
            .tableScan()
            .groupId({{"a", "b"}, {"a"}, {}}, {"c"}, "gid")
            .partialAggregation({"a", "b", "gid"}, {"sum(c) as total"})
            .shuffle()
            .localPartition()
            .finalAggregation({"a", "b", "gid"}, {"sum(total) as total"})
            .project({"a", "b", "total", "gid"})
            .gather()
            .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
  }
}

// Verifies that a grouping key can also be an aggregation input.
// SELECT a, SUM(a) FROM t GROUP BY ROLLUP(a) requires 'a' to appear both as
// a grouping key (subject to NULL-ing) and as an aggregation input (preserved).
TEST_F(AggregationTest, groupingSetsKeyIsAggInput) {
  testConnector_->addTable("t", ROW({"a", "b"}, {BIGINT(), DOUBLE()}));

  auto logicalPlan = lp::PlanBuilder(makeContext())
                         .tableScan("t")
                         .rollup({"a"}, {"sum(a) as total"}, "gid")
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  // 'a' is both a grouping key and an aggregate input. Capture the
  // auto-generated key name via keyAliases to avoid hardcoding it.
  auto matcher = core::PlanMatcherBuilder()
                     .tableScan()
                     .groupId({{"a"}, {}}, {"a"}, "gid", {{"a", "key_a"}})
                     .singleAggregation({"key_a", "gid"}, {"sum(a) as total"})
                     .project({"key_a as a", "total", "gid"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// TODO: Identical grouping sets compute separately today. Follow-up
// optimization: detect identical sets, compute once, and replicate rows.
TEST_F(AggregationTest, groupingSetsCrossSetOptimization) {
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

  // Three separate grouping sets in GroupId. Follow-up: collapse to one set
  // with row replication.
  auto matcher = core::PlanMatcherBuilder()
                     .tableScan()
                     .groupId({{"a", "b"}, {"b", "a"}, {"a", "b"}}, {}, "gid")
                     .singleAggregation({"a", "b", "gid"}, {"count(1) as c"})
                     .project({"a", "b", "c", "gid"})
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// No global (empty) grouping set — all sets have at least one key.
// This exercises the globalGroupingSets.empty() branch where aggGroupIdColumn
// is nullptr and forcePartial is not triggered by global sets.
TEST_F(AggregationTest, groupingSetsNoGlobalSet) {
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

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan()
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

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan()
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

// ORDER BY in aggregates requires single-step aggregation (partial aggregation
// cannot preserve ORDER BY semantics), but global grouping sets require
// partial+final. With kSingle, empty driver partitions emit spurious default
// rows for the global set.
TEST_F(AggregationTest, groupingSetsOrderByWithGlobalSet) {
  testConnector_->addTable("t", ROW({"a", "b"}, {BIGINT(), BIGINT()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  auto logicalPlan = lp::PlanBuilder(makeContext())
                         .tableScan("t")
                         .rollup({"a"}, {"array_agg(b ORDER BY b)"}, "gid")
                         .build();

  VELOX_ASSERT_THROW(
      planVelox(
          logicalPlan,
          MultiFragmentPlan::Options{.numWorkers = 4, .numDrivers = 4}),
      "ORDER BY in aggregate functions is not supported with global grouping sets");
}

TEST_F(AggregationTest, groupingSetsDistinctAggregation) {
  testConnector_->addTable("t", ROW({"a", "b"}, {BIGINT(), BIGINT()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  auto logicalPlan = lp::PlanBuilder(makeContext())
                         .tableScan("t")
                         .rollup({"a"}, {"count(DISTINCT b)"}, "gid")
                         .build();

  VELOX_ASSERT_THROW(
      planVelox(
          logicalPlan,
          MultiFragmentPlan::Options{.numWorkers = 4, .numDrivers = 4}),
      "DISTINCT aggregation with grouping sets is not supported yet");
}

// Literal-only aggregate args (count(1)) — aggregation inputs list passed to
// GroupId should be empty since literals are not column references.
TEST_F(AggregationTest, groupingSetsLiteralArgs) {
  testConnector_->addTable("t", ROW({"a", "b"}, {BIGINT(), BIGINT()}));

  auto logicalPlan = lp::PlanBuilder(makeContext())
                         .tableScan("t")
                         .rollup({"a"}, {"count(1) as c"}, "gid")
                         .build();

  // Single-node plan — GroupId has empty aggregation inputs.
  {
    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan()
                       .groupId({{"a"}, {}}, {}, "gid")
                       .singleAggregation({"a", "gid"}, {"count(1) as c"})
                       .project({"a", "c", "gid"})
                       .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Distributed plan.
  {
    auto plan = planVelox(logicalPlan);

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan()
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

} // namespace
} // namespace facebook::axiom::optimizer
