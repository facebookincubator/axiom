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
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class AggregationPlanTest : public test::QueryTestBase {
 protected:
  static constexpr auto kTestConnectorId = "test";

  void SetUp() override {
    test::QueryTestBase::SetUp();

    testConnector_ =
        std::make_shared<connector::TestConnector>(kTestConnectorId);
    velox::connector::registerConnector(testConnector_);

    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
  }

  void TearDown() override {
    velox::connector::unregisterConnector(kTestConnectorId);

    test::QueryTestBase::TearDown();
  }

  std::shared_ptr<connector::TestConnector> testConnector_;
};

TEST_F(AggregationPlanTest, dedupGroupingKeysAndAggregates) {
  testConnector_->addTable(
      "numbers", ROW({"a", "b", "c"}, {BIGINT(), BIGINT(), DOUBLE()}));

  {
    auto logicalPlan = lp::PlanBuilder{}
                           .tableScan(kTestConnectorId, "numbers")
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

  auto logicalPlan = lp::PlanBuilder{}
                         .tableScan(kTestConnectorId, "t")
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

  auto logicalPlan = lp::PlanBuilder(/*enableCoersions=*/true)
                         .tableScan(kTestConnectorId, "t")
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

  auto logicalPlan = lp::PlanBuilder(/*enableCoersions=*/true)
                         .tableScan(kTestConnectorId, "t")
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
      lp::PlanBuilder(/*enableCoersions=*/true)
          .tableScan(kTestConnectorId, "t")
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
      lp::PlanBuilder()
          .tableScan(kTestConnectorId, "t")
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
        runner::MultiFragmentPlan::Options{.numWorkers = 4, .numDrivers = 4},
        option);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher);
  }

  // Query without ORDER BY - should use partial + final aggregation.
  logicalPlan = lp::PlanBuilder()
                    .tableScan(kTestConnectorId, "t")
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
    auto logicalPlan = lp::PlanBuilder()
                           .tableScan(kTestConnectorId, "t")
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
    auto logicalPlan = lp::PlanBuilder()
                           .tableScan(kTestConnectorId, "t")
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
  auto table = testConnector_->addTable(
      "t", ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), DOUBLE()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  // Set table statistics with high duplicate ratio for grouping key + distinct
  // args. The inner GROUP BY should deduplicate aggressively, making the
  // GroupBy plan cheaper than the MarkDistinct plan.
  table->setStats(
      10'000,
      {{"a", {.numDistinct = 10}},
       {"b", {.numDistinct = 5}},
       {"c", {.numDistinct = 5}}});

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
        .localPartition()
        .finalAggregation(innerGroupingKeys, {});
    if (useSingleStepOuterAgg) {
      builder.shuffle().localPartition().singleAggregation(
          outerGroupingKeys, aggregates);
    } else {
      builder.partialAggregation(outerGroupingKeys, aggregates)
          .shuffle()
          .localPartition()
          .finalAggregation();
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
        auto logicalPlan = lp::PlanBuilder()
                               .tableScan(kTestConnectorId, "t")
                               .aggregate(groupingKeys, aggregates)
                               .build();
        auto plan = planVelox(
            logicalPlan,
            runner::MultiFragmentPlan::Options{
                .numWorkers = 4, .numDrivers = 4},
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
    // DISTINCT with ORDER BY where ORDER BY keys are a subset of distinct args.
    // Both aggregates have distinct args {a, b}.
    // With alwaysPlanPartialAggregation=true:
    // - Inner aggregation is split (partial + final) for deduplication.
    // - Outer aggregation is single even if alwaysPlanPartialAggregation is
    //   true, because ORDER BY requires single aggregation step.
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
}

// Verifies that when there are multiple DISTINCT aggregates with different
// sets of arguments, the optimizer uses the MarkDistinct transformation:
TEST_F(AggregationPlanTest, multipleDistinctToMarkDistinct) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c", "d"}, {BIGINT(), DOUBLE(), DOUBLE(), BIGINT()}));

  // Builds an expected plan matcher for the MarkDistinct transformation.
  // When 'singleStep' is true, expects a single aggregation step. Otherwise,
  // uses partial + final aggregation steps.
  auto buildMatcher =
      [](const std::vector<std::string>& projections,
         const std::vector<std::vector<std::string>>& markDistinctKeys,
         const std::vector<std::string>& groupingKeys,
         bool singleStep,
         const std::vector<std::string>& aggregates) {
        auto builder = core::PlanMatcherBuilder().tableScan();
        if (!projections.empty()) {
          builder.project(projections);
        }

        for (const auto& keys : markDistinctKeys) {
          builder.shuffle().markDistinct(keys);
        }
        if (singleStep) {
          builder.shuffle().localPartition().singleAggregation(
              groupingKeys, aggregates);
        } else {
          builder.partialAggregation(groupingKeys, aggregates)
              .shuffle()
              .localPartition()
              .finalAggregation();
        }
        if (!groupingKeys.empty()) {
          builder.shuffle();
        }
        return builder.build();
      };

  // Builds a logical plan, optimizes it, and asserts it matches the expected
  // distributed plan.
  auto assertPlan =
      [&](const std::vector<std::string>& groupingKeys,
          const std::vector<std::string>& aggregates,
          const std::shared_ptr<core::PlanMatcher>& expectedMatcher) {
        auto logicalPlan = lp::PlanBuilder()
                               .tableScan(kTestConnectorId, "t")
                               .aggregate(groupingKeys, aggregates)
                               .build();

        OptimizerOptions options{.alwaysPlanPartialAggregation = true};
        auto plan = planVelox(
            logicalPlan,
            runner::MultiFragmentPlan::Options{
                .numWorkers = 4, .numDrivers = 4},
            options);
        AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, expectedMatcher);
      };

  {
    // Test multiple DISTINCT aggregates with different argument sets.
    assertPlan(
        {"a"},
        {"count(DISTINCT b)", "sum(DISTINCT d % 5)"},
        buildMatcher(
            {"a as g", "b as p0", "d % 5 as p1"},
            {{"g", "p0"}, {"g", "p1"}},
            {"g"},
            /*singleStep=*/false,
            {"count(p0) filter (where m0)", "sum(p1) filter (where m1)"}));
  }

  {
    // Test global aggregation with multiple DISTINCT sets.
    assertPlan(
        {},
        {"count(DISTINCT b)", "sum(DISTINCT d % 5)"},
        buildMatcher(
            {"b as p0", "d % 5 as p1"},
            {{"p0"}, {"p1"}},
            {},
            /*singleStep=*/false,
            {"count(p0) filter (where m0)", "sum(p1) filter (where m1)"}));
  }

  {
    // Test mix of DISTINCT with different args and non-DISTINCT.
    assertPlan(
        {"a"},
        {"count(DISTINCT b)", "sum(DISTINCT d % 5)", "avg(b)"},
        buildMatcher(
            {"a as g", "b as p0", "d % 5 as p1"},
            {{"g", "p0"}, {"g", "p1"}},
            {"g"},
            /*singleStep=*/false,
            {"count(p0) filter (where m0)",
             "sum(p1) filter (where m1)",
             "avg(p0)"}));
  }

  {
    // Test DISTINCT with ORDER BY uses single aggregation step.
    assertPlan(
        {"a"},
        {"array_agg(DISTINCT b ORDER BY b)",
         "array_agg(DISTINCT d % 5 ORDER BY d % 5)",
         "array_agg(b ORDER BY b)"},
        buildMatcher(
            {"a as g", "b as p0", "d % 5 as p1"},
            {{"g", "p0"}, {"g", "p1"}},
            {"g"},
            /*singleStep=*/true,
            {"array_agg(p0 ORDER BY p0 ASC NULLS LAST) filter (where m0)",
             "array_agg(p1 ORDER BY p1 ASC NULLS LAST) filter (where m1)",
             "array_agg(p0 ORDER BY p0 ASC NULLS LAST)"}));
  }

  {
    // Test DISTINCT aggregates with the same set of non-grouping-key arguments
    // share a single marker column. Grouping keys in distinct arguments are
    // ignored since they always have unique values during aggregation.
    assertPlan(
        {"b"},
        {"count(DISTINCT c)", "covar_pop(DISTINCT b, c)"},
        buildMatcher(
            {},
            {{"b", "c"}},
            {"b"},
            /*singleStep=*/false,
            {"count(c) filter (where m0)",
             "covar_pop(b, c) filter (where m0)"}));
  }

  {
    // Test DISTINCT args overlapping with grouping keys are deduplicated in
    // MarkDistinct keys.
    assertPlan(
        {"a"},
        {"count(DISTINCT a)", "sum(DISTINCT b)"},
        buildMatcher(
            {},
            {{"a", "b"}},
            {"a"},
            /*singleStep=*/false,
            {"count(a)", "sum(b) filter (where m0)"}));

    assertPlan(
        {"a"},
        {"count(DISTINCT a)", "sum(DISTINCT a)"},
        buildMatcher(
            {},
            {},
            {"a"},
            /*singleStep=*/false,
            {"count(a)", "sum(a)"}));
  }

  {
    // Test multi-argument DISTINCT aggregates with different arg sets.
    assertPlan(
        {"a"},
        {"covar_pop(DISTINCT b, c)", "count(DISTINCT d)"},
        buildMatcher(
            {},
            {{"a", "b", "c"}, {"a", "d"}},
            {"a"},
            /*singleStep=*/false,
            {"covar_pop(b, c) filter (where m0)",
             "count(d) filter (where m1)"}));
  }
}

TEST_F(AggregationPlanTest, unsupportedAggregationOverDistinct) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), DOUBLE()}));

  {
    // DISTINCT with ORDER BY keys that are not a subset of distinct arguments
    // is not supported yet.
    auto logicalPlan =
        lp::PlanBuilder()
            .tableScan(kTestConnectorId, "t")
            .aggregate({"a"}, {"array_agg(DISTINCT b ORDER BY c)"})
            .build();

    VELOX_ASSERT_THROW(
        test::QueryTestBase::planVelox(logicalPlan),
        "For DISTINCT aggregations with parallel execution, ORDER BY keys must appear in aggregation arguments.");
  }

  {
    // DISTINCT aggregate with a filter condition is not supported yet.
    auto logicalPlan =
        lp::PlanBuilder(/*enableCoercions=*/true)
            .tableScan(kTestConnectorId, "t")
            .aggregate({"a"}, {"count(DISTINCT b) FILTER (WHERE c > 0)"})
            .build();

    VELOX_ASSERT_THROW(
        test::QueryTestBase::planVelox(logicalPlan),
        "Distinct aggregation plan not eligible for transformation to GroupBy or MarkDistinct.");
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
