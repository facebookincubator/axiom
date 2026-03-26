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

class AggregationPlanTest : public test::QueryTestBase {
 protected:
  lp::PlanBuilder::Context makeContext() const {
    return lp::PlanBuilder::Context{kTestConnectorId, kDefaultSchema};
  }
};

TEST_F(AggregationPlanTest, dedupGroupingKeysAndAggregates) {
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

TEST_F(AggregationPlanTest, duplicatesBetweenGroupAndAggregate) {
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

TEST_F(AggregationPlanTest, dedupMask) {
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

TEST_F(AggregationPlanTest, dedupOrderBy) {
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

TEST_F(AggregationPlanTest, dedupSameOptions) {
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
TEST_F(AggregationPlanTest, orderBy) {
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
  auto matcher =
      core::PlanMatcherBuilder()
          .tableScan()
          .shuffle()
          .localPartition()
          .singleAggregation({"k"}, {"array_agg(v1 ORDER BY v2)", "sum(v1)"})
          .shuffle()
          .build();

  for (auto i = 0; i < 2; ++i) {
    OptimizerOptions option{.alwaysPlanPartialAggregation = (i == 0)};
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
TEST_F(AggregationPlanTest, repartitionForAggPartitionSubset) {
  auto schema = ROW({"a", "b", "c", "v"}, BIGINT());
  testConnector_->addTable("t", schema);
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  // Test current partitionKeys ⊆ required groupingKeys --> no shuffle needed.
  {
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
                       .shuffle()
                       .localPartition()
                       .singleAggregation({"a", "b"}, {})
                       .project()
                       // No shuffle here - partitionKeys ⊆ groupingKeys
                       .localPartition()
                       .singleAggregation({"a", "b", "d"}, {})
                       .shuffle()
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
  }

  // Test current partitionKeys ⊄ required groupingKeys --> shuffle is needed.
  {
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
                       .shuffle()
                       .localPartition()
                       .singleAggregation({"a", "b", "c"}, {})
                       .project()
                       .shuffle()
                       .localPartition()
                       .singleAggregation({"a", "b"}, {})
                       .shuffle()
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
  }
}

// Verifies that when all aggregates are DISTINCT with the same input columns
// and no filters, the optimizer transforms them into a two-level aggregation:
// 1. Inner: GROUP BY (original_keys + distinct_args) - for deduplication
// 2. Outer: Regular aggregation without DISTINCT flag
// This avoids the overhead of tracking distinct values in each aggregate.
TEST_F(AggregationPlanTest, singleDistinctToGroupBy) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), DOUBLE()}));

  auto buildMatcher = [](const std::vector<std::string>& projections,
                         const std::vector<std::string>& innerGroupingKeys,
                         const std::vector<std::string>& outerGroupingKeys,
                         const std::vector<std::string>& aggregates,
                         bool useSingleStepOuterAgg = false) {
    auto builder = core::PlanMatcherBuilder().tableScan();
    if (!projections.empty()) {
      builder.project(projections);
    }
    builder.partialAggregation(innerGroupingKeys, {})
        .shuffle()
        .localPartition(innerGroupingKeys)
        .finalAggregation(innerGroupingKeys, {});
    // When inner and outer keys are the same, the optimizer merges the
    // inner and outer shuffles, so we skip the shuffle between inner final
    // and outer aggregation.
    bool sameKeys = innerGroupingKeys == outerGroupingKeys;
    if (useSingleStepOuterAgg) {
      if (!sameKeys) {
        builder.shuffle();
      }
      if (outerGroupingKeys.empty()) {
        builder.localGather();
      } else {
        builder.localPartition(outerGroupingKeys);
      }
      builder.singleAggregation(outerGroupingKeys, aggregates);
    } else {
      builder.partialAggregation(outerGroupingKeys, aggregates);
      if (!sameKeys) {
        builder.shuffle();
      }
      if (outerGroupingKeys.empty()) {
        builder.localGather();
      } else {
        builder.localPartition(outerGroupingKeys);
      }
      builder.finalAggregation();
    }
    if (!outerGroupingKeys.empty()) {
      builder.shuffle();
    }
    return builder.build();
  };

  // Builds a logical plan, optimizes it, and asserts it matches the expected
  // distributed plan.
  OptimizerOptions options{.alwaysPlanPartialAggregation = true};
  auto assertPlan =
      [&](const std::vector<std::string>& groupingKeys,
          const std::vector<std::string>& aggregates,
          const std::shared_ptr<core::PlanMatcher>& expectedMatcher) {
        auto logicalPlan = lp::PlanBuilder(makeContext())
                               .tableScan("t")
                               .aggregate(groupingKeys, aggregates)
                               .build();
        auto plan = planVelox(
            logicalPlan,
            MultiFragmentPlan::Options{.numWorkers = 4, .numDrivers = 4},
            options);
        AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, expectedMatcher);
      };

  {
    // Test global aggregation with multiple DISTINCT aggregates on the same set
    // of columns.
    assertPlan(
        {},
        {"count(DISTINCT b)", "covar_pop(DISTINCT b, b)"},
        buildMatcher(
            /*projections=*/{},
            /*innerGroupingKeys=*/{"b"},
            /*outerGroupingKeys=*/{},
            /*aggregates=*/{"count(b)", "covar_pop(b, b)"}));
  }

  {
    // Test single DISTINCT aggregate with grouping keys.
    assertPlan(
        {"a"},
        {"count(DISTINCT b)"},
        buildMatcher(
            /*projections=*/{},
            /*innerGroupingKeys=*/{"a", "b"},
            /*outerGroupingKeys=*/{"a"},
            /*aggregates=*/{"count(b)"}));
  }

  {
    // Test multiple DISTINCT aggregates on the same set of columns.
    assertPlan(
        {"a"},
        {"count(DISTINCT b)", "covar_pop(DISTINCT b, b)"},
        buildMatcher(
            /*projections=*/{},
            /*innerGroupingKeys=*/{"a", "b"},
            /*outerGroupingKeys=*/{"a"},
            /*aggregates=*/{"count(b)", "covar_pop(b, b)"}));
  }

  {
    // Test expression-based grouping keys and distinct args.
    assertPlan(
        {"a + 1"},
        {"count(DISTINCT b + c)", "sum(DISTINCT b + c)"},
        buildMatcher(
            /*projections=*/{"a + 1 as p0", "b + c as p1"},
            /*innerGroupingKeys=*/{"p0", "p1"},
            /*outerGroupingKeys=*/{"p0"},
            /*aggregates=*/{"count(p1)", "sum(p1)"}));
  }

  {
    // Test same set of distinct args with different order and duplicates: (b,
    // c) and (c, b) have the same set {b, c}.
    assertPlan(
        {"a"},
        {"covar_pop(DISTINCT b, c)", "covar_samp(DISTINCT c, b)"},
        buildMatcher(
            /*projections=*/{},
            /*innerGroupingKeys=*/{"a", "b", "c"},
            /*outerGroupingKeys=*/{"a"},
            /*aggregates=*/{"covar_pop(b, c)", "covar_samp(c, b)"}));
  }

  {
    // Test DISTINCT argument overlap with grouping keys.
    assertPlan(
        {"b"},
        {"covar_pop(DISTINCT b, c)"},
        buildMatcher(
            /*projections=*/{},
            /*innerGroupingKeys=*/{"b", "c"},
            /*outerGroupingKeys=*/{"b"},
            /*aggregates=*/{"covar_pop(b, c)"}));
  }

  {
    // Test DISTINCT with ORDER BY where ORDER BY keys are a subset of distinct
    // args.
    assertPlan(
        {"c"},
        {"max_by(DISTINCT a, b ORDER BY a)",
         "min_by(DISTINCT a, b ORDER BY b)"},
        buildMatcher(
            /*projections=*/{},
            /*innerGroupingKeys=*/{"c", "a", "b"},
            /*outerGroupingKeys=*/{"c"},
            /*aggregates=*/
            {"max_by(a, b ORDER BY a)", "min_by(a, b ORDER BY b)"},
            /*useSingleStepOuterAgg=*/true));
  }

  {
    // Test DISTINCT with ORDER BY and literal args. The literal should be
    // skipped while the column is kept in inner GROUP BY keys.
    assertPlan(
        {"a"},
        {"max_by(DISTINCT b, 1 ORDER BY b)",
         "min_by(DISTINCT b, 2 ORDER BY b)"},
        buildMatcher(
            /*projections=*/{},
            /*innerGroupingKeys=*/{"a", "b"},
            /*outerGroupingKeys=*/{"a"},
            /*aggregates=*/
            {"max_by(b, 1 ORDER BY b)", "min_by(b, 2 ORDER BY b)"},
            /*useSingleStepOuterAgg=*/true));
  }

  {
    // Test DISTINCT aggregate with mixed column and literal args. The literal
    // should be skipped while the column is kept in inner GROUP BY keys.
    assertPlan(
        {"a"},
        {"max_by(DISTINCT b, 1)", "min_by(DISTINCT b, 2)"},
        buildMatcher(
            /*projections=*/{},
            /*innerGroupingKeys=*/{"a", "b"},
            /*outerGroupingKeys=*/{"a"},
            /*aggregates=*/{"max_by(b, 1)", "min_by(b, 2)"}));
  }

  {
    // Test DISTINCT aggregate where all arguments are literals. The inner
    // GROUP BY keys should be just the grouping keys with no additions.
    assertPlan(
        {"a"},
        {"count(DISTINCT 1)", "count(DISTINCT 2)"},
        buildMatcher(
            /*projections=*/{},
            /*innerGroupingKeys=*/{"a"},
            /*outerGroupingKeys=*/{"a"},
            /*aggregates=*/{"count(1)", "count(2)"}));
  }
}

TEST_F(AggregationPlanTest, unsupportedAggregationOverDistinct) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), DOUBLE()}));

  {
    // Different DISTINCT arguments across aggregates is not supported yet.
    auto logicalPlan =
        lp::PlanBuilder(makeContext())
            .tableScan("t")
            .aggregate({"a"}, {"count(DISTINCT b)", "sum(DISTINCT c)"})
            .build();

    VELOX_ASSERT_THROW(
        test::QueryTestBase::planVelox(logicalPlan),
        "DISTINCT aggregates have multiple sets of arguments");
  }

  {
    // Different DISTINCT argument sets: {b, c} vs {b}.
    auto logicalPlan =
        lp::PlanBuilder(makeContext())
            .tableScan("t")
            .aggregate(
                {"a"},
                {"covar_pop(DISTINCT b, c)", "covar_samp(DISTINCT b, b)"})
            .build();

    VELOX_ASSERT_THROW(
        test::QueryTestBase::planVelox(logicalPlan),
        "DISTINCT aggregates have multiple sets of arguments");
  }

  {
    // Mix of DISTINCT and non-DISTINCT aggregates is not supported yet.
    auto logicalPlan = lp::PlanBuilder(makeContext())
                           .tableScan("t")
                           .aggregate({"a"}, {"count(DISTINCT b)", "sum(c)"})
                           .build();

    VELOX_ASSERT_THROW(
        test::QueryTestBase::planVelox(logicalPlan),
        "Mix of DISTINCT and non-DISTINCT aggregates");
  }

  {
    // First aggregate has all-literal args, second has column args. The column
    // arg sets differ (empty vs {b}), so this is unsupported.
    auto logicalPlan =
        lp::PlanBuilder(makeContext())
            .tableScan("t")
            .aggregate({"a"}, {"count(DISTINCT 1)", "count(DISTINCT b)"})
            .build();

    VELOX_ASSERT_THROW(
        test::QueryTestBase::planVelox(logicalPlan),
        "DISTINCT aggregates have multiple sets of arguments");
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
