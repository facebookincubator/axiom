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

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class DistinctAggregationTest : public test::QueryTestBase {
 public:
  DistinctAggregationTest() {
    runnerOptions_.numWorkers = 4;
    runnerOptions_.numDrivers = 4;
    optimizerOptions_.alwaysPlanPartialAggregation = true;
  }

 protected:
  lp::PlanBuilder::Context makeContext() const {
    return lp::PlanBuilder::Context{kTestConnectorId, kDefaultSchema};
  }

  MultiFragmentPlan::Options runnerOptions_;
  OptimizerOptions optimizerOptions_;
};

// Verifies that when all aggregates are DISTINCT with the same input columns
// and no filters, the optimizer transforms them into a two-level aggregation:
// 1. Inner: GROUP BY (original_keys + distinct_args) - for deduplication
// 2. Outer: Regular aggregation without DISTINCT flag
// This avoids the overhead of tracking distinct values in each aggregate.
TEST_F(DistinctAggregationTest, singleDistinctToGroupBy) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), DOUBLE()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  {
    SCOPED_TRACE(
        "Global aggregation with multiple DISTINCT aggregates on the same set of columns.");
    auto logicalPlan =
        lp::PlanBuilder(makeContext())
            .tableScan("t")
            .aggregate({}, {"count(DISTINCT b)", "covar_pop(DISTINCT b, b)"})
            .build();
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .distributedAggregation({"b"}, {})
            .distributedAggregation({}, {"count(b)", "covar_pop(b, b)"})
            .build());
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation(
                {}, {"count(DISTINCT b)", "covar_pop(DISTINCT b, b)"})
            .build());
  }

  {
    SCOPED_TRACE("Single DISTINCT aggregate with grouping keys.");
    auto logicalPlan = lp::PlanBuilder(makeContext())
                           .tableScan("t")
                           .aggregate({"a"}, {"count(DISTINCT b)"})
                           .build();
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .distributedAggregation({"a", "b"}, {})
            .distributedAggregation({"a"}, {"count(b)"})
            .shuffle()
            .build());
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t").singleAggregation({"a"}, {"count(DISTINCT b)"}).build());
  }

  {
    SCOPED_TRACE("Multiple DISTINCT aggregates on the same set of columns.");
    auto logicalPlan =
        lp::PlanBuilder(makeContext())
            .tableScan("t")
            .aggregate({"a"}, {"count(DISTINCT b)", "covar_pop(DISTINCT b, b)"})
            .build();
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .distributedAggregation({"a", "b"}, {})
            .distributedAggregation({"a"}, {"count(b)", "covar_pop(b, b)"})
            .shuffle()
            .build());
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation(
                {"a"}, {"count(DISTINCT b)", "covar_pop(DISTINCT b, b)"})
            .build());
  }
}

TEST_F(DistinctAggregationTest, singleDistinctToGroupByWithExpressionInputs) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), DOUBLE()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  {
    SCOPED_TRACE("Expression-based grouping keys and distinct args.");
    auto logicalPlan =
        lp::PlanBuilder(makeContext())
            .tableScan("t")
            .aggregate(
                {"a + 1"}, {"count(DISTINCT b + c)", "sum(DISTINCT b + c)"})
            .build();
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .project({"a + 1 as p0", "b + c as p1"})
            .distributedAggregation({"p0", "p1"}, {})
            .distributedAggregation({"p0"}, {"count(p1)", "sum(p1)"})
            .shuffle()
            .build());
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .project({"a + 1 as p0", "b + c as p1"})
            .singleAggregation(
                {"p0"}, {"count(DISTINCT p1)", "sum(DISTINCT p1)"})
            .build());
  }

  {
    SCOPED_TRACE(
        "Same set of distinct args with different order and duplicates: (b, c) and (c, b) have the same set {b, c}.");
    auto logicalPlan =
        lp::PlanBuilder(makeContext())
            .tableScan("t")
            .aggregate(
                {"a"},
                {"covar_pop(DISTINCT b, c)", "covar_samp(DISTINCT c, b)"})
            .build();
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .distributedAggregation({"a", "b", "c"}, {})
            .distributedAggregation(
                {"a"}, {"covar_pop(b, c)", "covar_samp(c, b)"})
            .shuffle()
            .build());
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation(
                {"a"},
                {"covar_pop(DISTINCT b, c)", "covar_samp(DISTINCT c, b)"})
            .build());
  }

  {
    SCOPED_TRACE("DISTINCT argument overlap with grouping keys.");
    auto logicalPlan = lp::PlanBuilder(makeContext())
                           .tableScan("t")
                           .aggregate({"b"}, {"covar_pop(DISTINCT b, c)"})
                           .build();
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .distributedAggregation({"b", "c"}, {})
            .distributedAggregation({"b"}, {"covar_pop(b, c)"})
            .shuffle()
            .build());
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation({"b"}, {"covar_pop(DISTINCT b, c)"})
            .build());
  }
}

TEST_F(DistinctAggregationTest, singleDistinctToGroupByWithOrderBy) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), DOUBLE()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  {
    SCOPED_TRACE(
        "DISTINCT with ORDER BY where ORDER BY keys are a subset of distinct args.");
    auto logicalPlan = lp::PlanBuilder(makeContext())
                           .tableScan("t")
                           .aggregate(
                               {"c"},
                               {"max_by(DISTINCT a, b ORDER BY a)",
                                "min_by(DISTINCT a, b ORDER BY b)"})
                           .build();
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .distributedAggregation({"c", "a", "b"}, {})
            .distributedSingleAggregation(
                {"c"}, {"max_by(a, b ORDER BY a)", "min_by(a, b ORDER BY b)"})
            .shuffle()
            .build());
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation(
                {"c"},
                {"max_by(DISTINCT a, b ORDER BY a)",
                 "min_by(DISTINCT a, b ORDER BY b)"})
            .build());
  }

  {
    SCOPED_TRACE("DISTINCT with ORDER BY and literal args.");
    auto logicalPlan = lp::PlanBuilder(makeContext())
                           .tableScan("t")
                           .aggregate(
                               {"a"},
                               {"max_by(DISTINCT b, 1 ORDER BY b)",
                                "min_by(DISTINCT b, 2 ORDER BY b)"})
                           .build();
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .distributedAggregation({"a", "b"}, {})
            .distributedSingleAggregation(
                {"a"}, {"max_by(b, 1 ORDER BY b)", "min_by(b, 2 ORDER BY b)"})
            .shuffle()
            .build());
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation(
                {"a"},
                {"max_by(DISTINCT b, 1 ORDER BY b)",
                 "min_by(DISTINCT b, 2 ORDER BY b)"})
            .build());
  }
}

TEST_F(DistinctAggregationTest, singleDistinctToGroupByWithLiterals) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), DOUBLE()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  {
    SCOPED_TRACE("DISTINCT aggregate with mixed column and literal args.");
    auto logicalPlan =
        lp::PlanBuilder(makeContext())
            .tableScan("t")
            .aggregate(
                {"a"}, {"max_by(DISTINCT b, 1)", "min_by(DISTINCT b, 2)"})
            .build();
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .distributedAggregation({"a", "b"}, {})
            .distributedAggregation({"a"}, {"max_by(b, 1)", "min_by(b, 2)"})
            .shuffle()
            .build());
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation(
                {"a"}, {"max_by(DISTINCT b, 1)", "min_by(DISTINCT b, 2)"})
            .build());
  }

  {
    SCOPED_TRACE("DISTINCT aggregate where all arguments are literals.");
    // The inner GROUP BY keys are just the grouping keys. There is no shuffle
    // between inner final and outer partial aggregation since inner and outer
    // keys are the same.
    auto logicalPlan =
        lp::PlanBuilder(makeContext())
            .tableScan("t")
            .aggregate({"a"}, {"count(DISTINCT 1)", "count(DISTINCT 2)"})
            .build();
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .distributedAggregation({"a"}, {})
            .partialAggregation({"a"}, {"count(1)", "count(2)"})
            .localPartition({"a"})
            .finalAggregation()
            .shuffle()
            .build());
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation(
                {"a"}, {"count(DISTINCT 1)", "count(DISTINCT 2)"})
            .build());
  }
}

TEST_F(DistinctAggregationTest, markDistinctDifferentArgSets) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c", "d"}, {BIGINT(), DOUBLE(), DOUBLE(), BIGINT()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  auto logicalPlan =
      lp::PlanBuilder(makeContext())
          .tableScan("t")
          .aggregate({"a"}, {"count(DISTINCT b)", "sum(DISTINCT d % 5)"})
          .build();
  auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchScan("t")
          .project({"a", "b as p0", "d % 5 as p1"})
          .distributedMarkDistinct({"a", "p0"}, {"m0"})
          .distributedMarkDistinct({"a", "p1"}, {"m1"})
          .distributedAggregation(
              {"a"},
              {"count(p0) filter (where m0)", "sum(p1) filter (where m1)"})
          .shuffle()
          .build());
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(logicalPlan),
      matchScan("t")
          .project({"a", "b", "d % 5 as p0"})
          .singleAggregation({"a"}, {"count(DISTINCT b)", "sum(DISTINCT p0)"})
          .build());
}

TEST_F(DistinctAggregationTest, markDistinctGlobalWithMultipleSets) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c", "d"}, {BIGINT(), DOUBLE(), DOUBLE(), BIGINT()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  auto logicalPlan =
      lp::PlanBuilder(makeContext())
          .tableScan("t")
          .aggregate({}, {"count(DISTINCT b)", "sum(DISTINCT d % 5)"})
          .build();
  auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchScan("t")
          .project({"b as p0", "d % 5 as p1"})
          .distributedMarkDistinct({"p0"}, {"m0"})
          .distributedMarkDistinct({"p1"}, {"m1"})
          .partialAggregation(
              {}, {"count(p0) filter (where m0)", "sum(p1) filter (where m1)"})
          .shuffle()
          .localGather()
          .finalAggregation()
          .build());
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(logicalPlan),
      matchScan("t")
          .project({"b", "d % 5 as p0"})
          .singleAggregation({}, {"count(DISTINCT b)", "sum(DISTINCT p0)"})
          .build());
}

TEST_F(DistinctAggregationTest, markDistinctMixedDistinctAndNonDistinct) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c", "d"}, {BIGINT(), DOUBLE(), DOUBLE(), BIGINT()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  auto logicalPlan =
      lp::PlanBuilder(makeContext())
          .tableScan("t")
          .aggregate(
              {"a"}, {"count(DISTINCT b)", "sum(DISTINCT d % 5)", "avg(b)"})
          .build();
  auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchScan("t")
          .project({"a", "b as p0", "d % 5 as p1"})
          .distributedMarkDistinct({"a", "p0"}, {"m0"})
          .distributedMarkDistinct({"a", "p1"}, {"m1"})
          .distributedAggregation(
              {"a"},
              {"count(p0) filter (where m0)",
               "sum(p1) filter (where m1)",
               "avg(p0)"})
          .shuffle()
          .build());
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(logicalPlan),
      matchScan("t")
          .project({"a", "b", "d % 5 as p0"})
          .singleAggregation(
              {"a"}, {"count(DISTINCT b)", "sum(DISTINCT p0)", "avg(b)"})
          .build());
}

TEST_F(DistinctAggregationTest, markDistinctMultiArgAggregates) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c", "d"}, {BIGINT(), DOUBLE(), DOUBLE(), BIGINT()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  auto logicalPlan =
      lp::PlanBuilder(makeContext())
          .tableScan("t")
          .aggregate({"a"}, {"covar_pop(DISTINCT b, c)", "count(DISTINCT d)"})
          .build();
  auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchScan("t")
          .distributedMarkDistinct({"a", "b", "c"}, {"m0"})
          .distributedMarkDistinct({"a", "d"}, {"m1"})
          .distributedAggregation(
              {"a"},
              {"covar_pop(b, c) filter (where m0)",
               "count(d) filter (where m1)"})
          .shuffle()
          .build());
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(logicalPlan),
      matchScan("t")
          .singleAggregation(
              {"a"}, {"covar_pop(DISTINCT b, c)", "count(DISTINCT d)"})
          .build());
}

TEST_F(DistinctAggregationTest, markDistinctSharedMarkers) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c", "d"}, {BIGINT(), DOUBLE(), DOUBLE(), BIGINT()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  {
    SCOPED_TRACE(
        "DISTINCT aggregates with the same set of non-grouping-key arguments share a single marker column.");
    auto logicalPlan =
        lp::PlanBuilder(makeContext())
            .tableScan("t")
            .aggregate(
                {"b"},
                {"count(DISTINCT c)", "covar_pop(DISTINCT b, c)", "sum(c)"})
            .build();
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .distributedMarkDistinct({"b", "c"}, {"m0"})
            .distributedAggregation(
                {"b"},
                {"count(c) filter (where m0)",
                 "covar_pop(b, c) filter (where m0)",
                 "sum(c)"})
            .shuffle()
            .build());
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation(
                {"b"},
                {"count(DISTINCT c)", "covar_pop(DISTINCT b, c)", "sum(c)"})
            .build());
  }

  {
    SCOPED_TRACE(
        "DISTINCT args overlapping with grouping keys are deduplicated in MarkDistinct keys.");
    auto logicalPlan =
        lp::PlanBuilder(makeContext())
            .tableScan("t")
            .aggregate({"b"}, {"covar_pop(DISTINCT b, c)", "count(DISTINCT b)"})
            .build();
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .distributedMarkDistinct({"b", "c"}, {"m0"})
            .distributedSingleAggregation(
                {"b"},
                {"covar_pop(b, c) filter (where m0)", "count(DISTINCT b)"})
            .shuffle()
            .build());
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation(
                {"b"}, {"covar_pop(DISTINCT b, c)", "count(DISTINCT b)"})
            .build());
  }
}

TEST_F(DistinctAggregationTest, markDistinctOrderBy) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c", "d"}, {BIGINT(), DOUBLE(), DOUBLE(), BIGINT()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  {
    SCOPED_TRACE("GroupBy with DISTINCT + ORDER BY via MarkDistinct.");
    auto logicalPlan = lp::PlanBuilder(makeContext())
                           .tableScan("t")
                           .aggregate(
                               {"a"},
                               {"array_agg(DISTINCT b ORDER BY b)",
                                "array_agg(DISTINCT d % 5 ORDER BY d % 5)",
                                "array_agg(b ORDER BY b)"})
                           .build();
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .project({"a", "b as p0", "d % 5 as p1"})
            .distributedMarkDistinct({"a", "p0"}, {"m0"})
            .distributedMarkDistinct({"a", "p1"}, {"m1"})
            .distributedSingleAggregation(
                {"a"},
                {"array_agg(p0 ORDER BY p0 ASC NULLS LAST) filter (where m0)",
                 "array_agg(p1 ORDER BY p1 ASC NULLS LAST) filter (where m1)",
                 "array_agg(p0 ORDER BY p0 ASC NULLS LAST)"})
            .shuffle()
            .build());
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .project({"a", "b as p0", "d % 5 as p1"})
            .singleAggregation(
                {"a"},
                {"array_agg(DISTINCT p0 ORDER BY p0)",
                 "array_agg(DISTINCT p1 ORDER BY p1)",
                 "array_agg(p0 ORDER BY p0)"})
            .build());
  }

  {
    SCOPED_TRACE(
        "Global aggregation with DISTINCT + ORDER BY via MarkDistinct.");
    auto logicalPlan = lp::PlanBuilder(makeContext())
                           .tableScan("t")
                           .aggregate(
                               {},
                               {"array_agg(DISTINCT b ORDER BY b)",
                                "array_agg(DISTINCT d % 5 ORDER BY d % 5)"})
                           .build();
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .project({"b as p0", "d % 5 as p1"})
            .distributedMarkDistinct({"p0"}, {"m0"})
            .distributedMarkDistinct({"p1"}, {"m1"})
            .distributedSingleAggregation(
                {},
                {"array_agg(p0 ORDER BY p0 ASC NULLS LAST) filter (where m0)",
                 "array_agg(p1 ORDER BY p1 ASC NULLS LAST) filter (where m1)"})
            .build());
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .project({"b as p0", "d % 5 as p1"})
            .singleAggregation(
                {},
                {"array_agg(DISTINCT p0 ORDER BY p0)",
                 "array_agg(DISTINCT p1 ORDER BY p1)"})
            .build());
  }
}

TEST_F(DistinctAggregationTest, markDistinctLiterals) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c", "d"}, {BIGINT(), DOUBLE(), DOUBLE(), BIGINT()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  {
    SCOPED_TRACE("Literal in args alongside different DISTINCT column sets.");
    // The literal should not be included as MarkDistinct keys.
    auto logicalPlan =
        lp::PlanBuilder(makeContext())
            .tableScan("t")
            .aggregate({"a"}, {"count(DISTINCT b)", "max_by(DISTINCT d, 1)"})
            .build();
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .distributedMarkDistinct({"a", "b"}, {"m0"})
            .distributedMarkDistinct({"a", "d"}, {"m1"})
            .distributedAggregation(
                {"a"},
                {"count(b) filter (where m0)",
                 "max_by(d, 1) filter (where m1)"})
            .shuffle()
            .build());
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation(
                {"a"}, {"count(DISTINCT b)", "max_by(DISTINCT d, 1)"})
            .build());
  }

  {
    SCOPED_TRACE(
        "DISTINCT aggregate whose column args are all grouping keys plus a literal.");
    auto logicalPlan =
        lp::PlanBuilder(makeContext())
            .tableScan("t")
            .aggregate({"a"}, {"count(DISTINCT b)", "max_by(DISTINCT a, 1)"})
            .build();
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .distributedMarkDistinct({"a", "b"}, {"m0"})
            .distributedSingleAggregation(
                {"a"}, {"count(b) filter (where m0)", "max_by(DISTINCT a, 1)"})
            .shuffle()
            .build());
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation(
                {"a"}, {"count(DISTINCT b)", "max_by(DISTINCT a, 1)"})
            .build());
  }

  {
    SCOPED_TRACE(
        "DISTINCT aggregate with all-literal args alongside DISTINCT aggregate with column args.");
    auto logicalPlan =
        lp::PlanBuilder(makeContext())
            .tableScan("t")
            .aggregate({"a"}, {"count(DISTINCT b)", "count(DISTINCT 1)"})
            .build();
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .distributedMarkDistinct({"a", "b"}, {"m0"})
            .distributedSingleAggregation(
                {"a"}, {"count(b) filter (where m0)", "count(DISTINCT 1)"})
            .shuffle()
            .build());
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation(
                {"a"}, {"count(DISTINCT b)", "count(DISTINCT 1)"})
            .build());
  }

  {
    SCOPED_TRACE(
        "all-literal DISTINCT with FILTER + column DISTINCT → one MarkDistinct only for column DISTINCT");
    auto logicalPlan =
        lp::PlanBuilder(makeContext())
            .tableScan("t")
            .aggregate(
                {"a"},
                {"count(DISTINCT b)", "count(DISTINCT 1) FILTER (WHERE d > 0)"})
            .build();
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .project({"a", "b", "d > 0 as p0"})
            .distributedMarkDistinct({"a", "b"}, {"m0"})
            .distributedSingleAggregation(
                {"a"},
                {"count(b) filter (where m0)",
                 "count(DISTINCT 1) filter (where p0)"})
            .shuffle()
            .build());
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .project({"a", "b", "d > 0 as p0"})
            .singleAggregation(
                {"a"},
                {"count(DISTINCT b)", "count(DISTINCT 1) filter (where p0)"})
            .build());
  }
}

TEST_F(DistinctAggregationTest, multipleMarkDistinctWithNoShuffleInBetween) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c", "d"}, {BIGINT(), DOUBLE(), DOUBLE(), BIGINT()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  auto logicalPlan =
      lp::PlanBuilder(makeContext())
          .tableScan("t")
          .aggregate({"a"}, {"count(DISTINCT b)", "covar_pop(DISTINCT b, c)"})
          .build();
  auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchScan("t")
          .shuffle()
          .localPartition({"a", "b"})
          .markDistinct({"a", "b"}, {"m0"})
          .markDistinct({"a", "b", "c"}, {"m1"})
          .distributedAggregation(
              {"a"},
              {"count(b) filter (where m0)",
               "covar_pop(b, c) filter (where m1)"})
          .shuffle()
          .build());
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(logicalPlan),
      matchScan("t")
          .singleAggregation(
              {"a"}, {"count(DISTINCT b)", "covar_pop(DISTINCT b, c)"})
          .build());
}

TEST_F(DistinctAggregationTest, markDistinctFilterDifferentArgSets) {
  testConnector_->addTable(
      "t",
      ROW({"a", "b", "c", "d", "e"},
          {BIGINT(), DOUBLE(), BIGINT(), BOOLEAN(), BOOLEAN()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  {
    SCOPED_TRACE("same key set, different filters → one MarkDistinct");
    auto logicalPlan = lp::PlanBuilder(makeContext())
                           .tableScan("t")
                           .aggregate(
                               {"a"},
                               {"count(DISTINCT b) FILTER (WHERE d)",
                                "count(DISTINCT b) FILTER (WHERE e)"})
                           .build();
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .distributedMarkDistinct({"a", "b"}, {"m0", "m1", "m2"})
            .distributedAggregation(
                {"a"},
                {"count(b) filter (where m1)", "count(b) filter (where m2)"})
            .shuffle()
            .build());
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation(
                {"a"},
                {"count(DISTINCT b) filter (where d)",
                 "count(DISTINCT b) filter (where e)"})
            .build());
  }

  {
    SCOPED_TRACE("different key sets → separate MarkDistincts");
    auto logicalPlan = lp::PlanBuilder(makeContext())
                           .tableScan("t")
                           .aggregate(
                               {"a"},
                               {"count(DISTINCT b) FILTER (WHERE d)",
                                "count(DISTINCT c) FILTER (WHERE e)"})
                           .build();
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .distributedMarkDistinct({"a", "b"}, {"m0", "m1"})
            .distributedMarkDistinct({"a", "c"}, {"m2", "m3"})
            .distributedAggregation(
                {"a"},
                {"count(b) filter (where m1)", "count(c) filter (where m3)"})
            .shuffle()
            .build());
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation(
                {"a"},
                {"count(DISTINCT b) filter (where d)",
                 "count(DISTINCT c) filter (where e)"})
            .build());
  }
}

TEST_F(DistinctAggregationTest, markDistinctFilterGlobalAggregation) {
  testConnector_->addTable("t", ROW({"a", "b"}, {BIGINT(), BOOLEAN()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  auto logicalPlan = lp::PlanBuilder(makeContext())
                         .tableScan("t")
                         .aggregate({}, {"count(DISTINCT a) FILTER (WHERE b)"})
                         .build();
  auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchScan("t")
          .distributedMarkDistinct({"a"}, {"m0", "m1"})
          .partialAggregation({}, {"count(a) filter (where m1)"})
          .shuffle()
          .localGather()
          .finalAggregation()
          .build());
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(logicalPlan),
      matchScan("t")
          .singleAggregation({}, {"count(DISTINCT a) filter (where b)"})
          .build());
}

TEST_F(DistinctAggregationTest, markDistinctFilterSharedMarkers) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), BOOLEAN()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  {
    SCOPED_TRACE("same args + same filter → shared marker");
    auto logicalPlan = lp::PlanBuilder(makeContext())
                           .tableScan("t")
                           .aggregate(
                               {"a"},
                               {"count(DISTINCT b) FILTER (WHERE c)",
                                "sum(DISTINCT b) FILTER (WHERE c)"})
                           .build();
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .distributedMarkDistinct({"a", "b"}, {"m0", "m1"})
            .distributedAggregation(
                {"a"},
                {"count(b) filter (where m1)", "sum(b) filter (where m1)"})
            .shuffle()
            .build());
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation(
                {"a"},
                {"count(DISTINCT b) filter (where c)",
                 "sum(DISTINCT b) filter (where c)"})
            .build());
  }

  {
    SCOPED_TRACE("filtered + unfiltered same args → use no-mask marker");
    auto logicalPlan =
        lp::PlanBuilder(makeContext())
            .tableScan("t")
            .aggregate(
                {"a"},
                {"count(DISTINCT b)", "count(DISTINCT b) FILTER (WHERE c)"})
            .build();
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .distributedMarkDistinct({"a", "b"}, {"m0", "m1"})
            .distributedAggregation(
                {"a"},
                {"count(b) filter (where m0)", "count(b) filter (where m1)"})
            .shuffle()
            .build());
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation(
                {"a"},
                {"count(DISTINCT b)", "count(DISTINCT b) filter (where c)"})
            .build());
  }
}

TEST_F(DistinctAggregationTest, markDistinctFilterOrderBy) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), BOOLEAN()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  auto logicalPlan =
      lp::PlanBuilder(makeContext())
          .tableScan("t")
          .aggregate(
              {"a"}, {"array_agg(DISTINCT b ORDER BY b) FILTER (WHERE c)"})
          .build();
  auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchScan("t")
          .distributedMarkDistinct({"a", "b"}, {"m0", "m1"})
          .distributedSingleAggregation(
              {"a"},
              {"array_agg(b ORDER BY b ASC NULLS LAST) filter (where m1)"})
          .shuffle()
          .build());
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(logicalPlan),
      matchScan("t")
          .singleAggregation(
              {"a"}, {"array_agg(DISTINCT b ORDER BY b) filter (where c)"})
          .build());
}

TEST_F(DistinctAggregationTest, markDistinctFilterMixedDistinctAndNonDistinct) {
  testConnector_->addTable(
      "t",
      ROW({"a", "b", "c", "d", "e"},
          {BIGINT(), DOUBLE(), BIGINT(), BOOLEAN(), BOOLEAN()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  auto logicalPlan =
      lp::PlanBuilder(makeContext())
          .tableScan("t")
          .aggregate(
              {"a"},
              {"sum(b) FILTER (WHERE e)", "count(DISTINCT c) FILTER (WHERE d)"})
          .build();
  auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchScan("t")
          .distributedMarkDistinct({"a", "c"}, {"m0", "m1"})
          .distributedAggregation(
              {"a"}, {"sum(b) filter (where e)", "count(c) filter (where m1)"})
          .shuffle()
          .build());
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(logicalPlan),
      matchScan("t")
          .singleAggregation(
              {"a"},
              {"sum(b) filter (where e)", "count(DISTINCT c) filter (where d)"})
          .build());
}

TEST_F(DistinctAggregationTest, markDistinctFilterRedundantKeys) {
  testConnector_->addTable("t", ROW({"a", "b"}, {BIGINT(), BOOLEAN()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  // Distinct args equal grouping keys: aggregation's GROUP BY already
  // partitions on those columns, so DISTINCT is kept on the aggregate and no
  // MarkDistinct is added.
  auto logicalPlan =
      lp::PlanBuilder(makeContext())
          .tableScan("t")
          .aggregate({"a"}, {"count(DISTINCT a) FILTER (WHERE b)"})
          .build();
  auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchScan("t")
          .distributedSingleAggregation(
              {"a"}, {"count(DISTINCT a) filter (where b)"})
          .shuffle()
          .build());
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(logicalPlan),
      matchScan("t")
          .singleAggregation({"a"}, {"count(DISTINCT a) filter (where b)"})
          .build());
}

TEST_F(DistinctAggregationTest, markDistinctFilterExpressionCondition) {
  testConnector_->addTable(
      "t",
      ROW({"a", "b", "c", "d"}, {BIGINT(), DOUBLE(), DOUBLE(), BOOLEAN()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  // Two filters on the same distinct key set: one boolean column, one
  // expression. Verifies expression filters are materialized into a Project
  // before MarkDistinct and that both kinds fold into one MarkDistinct.
  auto logicalPlan = lp::PlanBuilder(makeContext())
                         .tableScan("t")
                         .aggregate(
                             {"a"},
                             {"count(DISTINCT b) FILTER (WHERE d)",
                              "count(DISTINCT b) FILTER (WHERE c > 0.0)"})
                         .build();
  auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      plan.plan,
      matchScan("t")
          .project({"a", "d", "b", "c > 0.0"})
          .distributedMarkDistinct({"a", "b"}, {"m0", "m1", "m2"})
          .distributedAggregation(
              {"a"},
              {"count(b) filter (where m1)", "count(b) filter (where m2)"})
          .shuffle()
          .build());
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(logicalPlan),
      matchScan("t")
          .project({"a", "d", "b", "c > 0.0 as p0"})
          .singleAggregation(
              {"a"},
              {"count(DISTINCT b) filter (where d)",
               "count(DISTINCT b) filter (where p0)"})
          .build());
}

TEST_F(
    DistinctAggregationTest,
    markDistinctAllLiteralDistinctMixColumnDistinct) {
  testConnector_->addTable(
      "t", ROW({"a", "b", "c", "d"}, {BIGINT(), DOUBLE(), DOUBLE(), BIGINT()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  {
    SCOPED_TRACE(
        "Global aggregation mixing column DISTINCT and all-literal DISTINCT.");
    auto logicalPlan =
        lp::PlanBuilder(makeContext())
            .tableScan("t")
            .aggregate({}, {"count(DISTINCT b)", "count(DISTINCT 1)"})
            .build();
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .distributedMarkDistinct({"b"}, {"m0"})
            .distributedSingleAggregation(
                {}, {"count(b) filter (where m0)", "count(DISTINCT 1)"})
            .build());
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation({}, {"count(DISTINCT b)", "count(DISTINCT 1)"})
            .build());
  }

  {
    SCOPED_TRACE(
        "Grouped aggregation mixing column DISTINCT and all-literal DISTINCT.");
    auto logicalPlan =
        lp::PlanBuilder(makeContext())
            .tableScan("t")
            .aggregate({"a"}, {"count(DISTINCT b)", "count(DISTINCT 1)"})
            .build();
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .distributedMarkDistinct({"a", "b"}, {"m0"})
            .distributedSingleAggregation(
                {"a"}, {"count(b) filter (where m0)", "count(DISTINCT 1)"})
            .shuffle()
            .build());
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .singleAggregation(
                {"a"}, {"count(DISTINCT b)", "count(DISTINCT 1)"})
            .build());
  }
}

TEST_F(DistinctAggregationTest, groupingSetsDistinctToGroupBy) {
  testConnector_->addTable("t", ROW({"a", "b"}, {BIGINT(), BIGINT()}));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  auto logicalPlan = lp::PlanBuilder(makeContext())
                         .tableScan("t")
                         .rollup({"a"}, {"count(DISTINCT b) as cnt"}, "gid")
                         .build();

  // Distributed plan. ROLLUP(a) contains the global set (), so the outer
  // aggregation is forced split (partial + final).
  {
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .groupId({{"a"}, {}}, {"b"}, "gid")
            .distributedAggregation({"a", "gid", "b"}, {})
            .distributedAggregation({"a", "gid"}, {"count(b) as cnt"})
            .project({"a", "cnt", "gid"})
            .gather()
            .build());
  }

  // Single-node plan: a single aggregation computes DISTINCT natively.
  {
    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(
        plan,
        matchScan("t")
            .groupId({{"a"}, {}}, {"b"}, "gid")
            .singleAggregation({"a", "gid"}, {"count(DISTINCT b) as cnt"})
            .project({"a", "cnt", "gid"})
            .build());
  }
}

TEST_F(DistinctAggregationTest, groupingSetsDistinctToMarkDistinct) {
  testConnector_->addTable("t", ROW({"a", "b", "c"}, BIGINT()));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  auto logicalPlan =
      lp::PlanBuilder(makeContext())
          .tableScan("t")
          .rollup(
              {"a"},
              {"count(DISTINCT b) as a0", "sum(DISTINCT c) as a1"},
              "gid")
          .build();

  // Distributed plan.
  {
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .groupId({{"a"}, {}}, {"b", "c"}, "gid")
            .distributedMarkDistinct({"a", "gid", "b"}, {"m0"})
            .distributedMarkDistinct({"a", "gid", "c"}, {"m1"})
            .distributedAggregation(
                {"a", "gid"},
                {"count(b) filter (where m0) as a0",
                 "sum(c) filter (where m1) as a1"})
            .project({"a", "a0", "a1", "gid"})
            .gather()
            .build());
  }

  // Single-node plan: one kSingle Aggregation computes the DISTINCT aggregates
  // natively.
  {
    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(
        plan,
        matchScan("t")
            .groupId({{"a"}, {}}, {"b", "c"}, "gid")
            .singleAggregation(
                {"a", "gid"},
                {"count(DISTINCT b) as a0", "sum(DISTINCT c) as a1"})
            .project({"a", "a0", "a1", "gid"})
            .build());
  }
}

// DISTINCT aggregate combined with ORDER BY and grouping sets without a global
// set. ORDER BY forces the result-producing aggregation to single-step.
TEST_F(DistinctAggregationTest, groupingSetsDistinctWithOrderBy) {
  testConnector_->addTable("t", ROW({"a", "b", "c"}, BIGINT()));
  SCOPE_EXIT {
    testConnector_->dropTableIfExists("t");
  };

  auto logicalPlan =
      lp::PlanBuilder(makeContext())
          .tableScan("t")
          .aggregate(
              {{"a"}, {"b"}}, {"array_agg(DISTINCT c ORDER BY c) as a0"}, "gid")
          .build();

  // Distributed plan.
  {
    auto plan = planVelox(logicalPlan, runnerOptions_, optimizerOptions_);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        plan.plan,
        matchScan("t")
            .groupId({{"a"}, {"b"}}, {"c"}, "gid")
            .distributedAggregation({"a", "b", "gid", "c"}, {})
            .shuffle()
            .localPartition()
            .singleAggregation(
                {"a", "b", "gid"}, {"array_agg(c ORDER BY c) as a0"})
            .project({"a", "b", "a0", "gid"})
            .gather()
            .build());
  }

  // Single-node plan: one kSingle Aggregation computes DISTINCT + ORDER BY
  // natively.
  {
    auto plan = toSingleNodePlan(logicalPlan);
    AXIOM_ASSERT_PLAN(
        plan,
        matchScan("t")
            .groupId({{"a"}, {"b"}}, {"c"}, "gid")
            .singleAggregation(
                {"a", "b", "gid"}, {"array_agg(DISTINCT c ORDER BY c) as a0"})
            .project({"a", "b", "a0", "gid"})
            .build());
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
