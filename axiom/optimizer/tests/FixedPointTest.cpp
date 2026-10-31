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
#include <gtest/gtest.h>
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class FixedPointTest : public test::QueryTestBase {
 protected:
  lp::PlanBuilder singleRow(const std::string& name, int64_t value) {
    return lp::PlanBuilder(context_).values(
        ROW(name, BIGINT()), std::vector<Variant>{Variant::row({value})});
  }

  lp::PlanBuilder::Context context_;
};

TEST_F(FixedPointTest, recursiveCte) {
  static constexpr int32_t kRecursionLimit = 37;
  optimizerOptions_.recursionLimit = kRecursionLimit;

  auto logicalPlan = parseSelect(
      "WITH RECURSIVE counter(n) AS ("
      "VALUES 1 UNION ALL SELECT n + 1 FROM counter WHERE n < 10) "
      "SELECT * FROM counter");
  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher =
      matchFixedPoint(startFixedPoint("counter")
                          .outputState(/*append=*/true, matchValues())
                          .plan(matchDelta("counter", {"n"})
                                    .filter("n < 10")
                                    .project({"n + 1"}))
                          .convergeOnEmpty({.maxIterations = kRecursionLimit}))
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// An anchor preserves separate recursive-state columns for equal join keys.
TEST_F(FixedPointTest, joinKeyState) {
  testConnector_->addTable("t", ROW("t_k", BIGINT()));
  testConnector_->addTable("u", ROW("u_k", BIGINT()));

  const auto query =
      "WITH RECURSIVE r(t_k, u_k) AS ("
      "  SELECT t_k, u_k FROM t JOIN u ON t_k = u_k "
      "  UNION ALL "
      "  SELECT t_k + 1, u_k + 1 FROM r WHERE t_k < 10) "
      "SELECT t_k, u_k FROM r";
  SCOPED_TRACE(query);

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(parseSelect(query)),
      matchFixedPoint(
          startFixedPoint("r")
              .outputState(
                  /*append=*/true,
                  matchScan("t")
                      .hashJoinInner(matchScan("u"), {.keys = {{"t_k = u_k"}}})
                      .project({"t_k", "t_k as u_k"}))
              .plan(matchDelta("r", {"t_k", "u_k"})
                        .filter("t_k < 10")
                        .project({"t_k + 1", "u_k + 1"})))
          .build());
}

// A step's join reads the table through a hash table built once for the loop,
// whichever side of the join the query writes the table on, with or without
// statistics for the table: 'e' has none, 'f' has them.
TEST_F(FixedPointTest, stepJoin) {
  testConnector_->addTable("e", ROW({"src", "dst"}, BIGINT()));
  testConnector_->addTable("f", ROW({"src", "dst"}, BIGINT()))
      ->setStats(
          1'000,
          {
              {"src", {.numDistinct = 1'000}},
              {"dst", {.numDistinct = 1'000}},
          });

  for (const std::string table : {"e", "f"}) {
    auto matcher =
        matchFixedPoint(startFixedPoint("r")
                            .outputState(/*append=*/true, matchValues())
                            .hashTableState(matchScan(table))
                            .plan(matchDelta("r", {"n"})
                                      .stateHashJoin({"n"})
                                      .project({"dst"})))
            .build();
    for (const auto& join :
         {fmt::format("r JOIN {}", table), fmt::format("{} JOIN r", table)}) {
      SCOPED_TRACE(join);
      auto logicalPlan = parseSelect(
          fmt::format(
              "WITH RECURSIVE r(n) AS ("
              "     SELECT CAST(1 AS BIGINT) "
              "     UNION ALL "
              "     SELECT dst FROM {} ON src = n"
              ") "
              "SELECT n FROM r",
              join));
      AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
    }
  }
}

// A step that joins two reads of the state sees each read's columns
// separately.
TEST_F(FixedPointTest, selfJoinInStep) {
  testConnector_->addTable("t", ROW({"k", "v"}, BIGINT()))
      ->addData(makeRowVector({
          makeFlatVector<int64_t>({1, 2}),
          makeFlatVector<int64_t>({10, 20}),
      }));

  auto context = makeContext();
  auto anchor = lp::PlanBuilder(context).tableScan("t");
  auto state = [&](std::string_view alias) {
    return lp::PlanBuilder(context)
        .fixedPointState("r", anchor)
        .as(std::string(alias));
  };

  auto logicalPlan =
      anchor
          .iterate(
              "r",
              state("x")
                  .join(state("y"), "x.k = y.k", lp::JoinType::kInner)
                  .project({"x.k AS k", "x.v + y.v AS v"}),
              /*numIterations=*/2,
              /*keyColumns=*/{"k"})
          .build();

  // The loop plans on one worker and on several.
  ASSERT_NO_THROW(toSingleNodePlan(logicalPlan));
  ASSERT_NO_THROW(planVelox(logicalPlan));

  checkSame(
      logicalPlan,
      {makeRowVector({
          makeFlatVector<int64_t>({1, 2}),
          makeFlatVector<int64_t>({40, 80}),
      })});
}

// A loop's step runs with the task's drivers, as any other subtree does: its
// aggregation splits into a partial and a final step around a local gather.
TEST_F(FixedPointTest, stepDrivers) {
  auto counter = singleRow("n", 1);
  auto recursiveStep = lp::PlanBuilder(context_)
                           .fixedPointState("counter", counter)
                           .filter("n < 10")
                           .aggregate({}, {"count(*) as n"});
  auto logicalPlan = counter.recursiveCte("counter", recursiveStep)
                         .aggregate({}, {"count(*)"})
                         .build();

  auto matcher =
      matchFixedPoint(startFixedPoint("counter")
                          .outputState(/*append=*/true, matchValues())
                          .plan(matchDelta("counter", {"n"})
                                    .filter("n < 10")
                                    .localAggregation({}, {"count(*)"}))
                          .convergeOnEmpty())
          .localAggregation({}, {"count(*)"})
          .build();
  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan, /*numDrivers=*/4), matcher);
}

TEST_F(FixedPointTest, outerLimit) {
  auto logicalPlan = parseSelect(
      "WITH RECURSIVE counter(n) AS ("
      "VALUES 1 UNION ALL SELECT n + 1 FROM counter WHERE n < 20) "
      "SELECT n FROM counter LIMIT 5");

  auto matcher = matchFixedPoint().finalLimit(0, 5).build();
  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
}

// With several workers, each task that reads the anchor runs its own loop, and
// the step reads every other input in full. A step whose rows each derive from
// one row of the loop state allows that. Otherwise the loop runs on one task.
TEST_F(FixedPointTest, distributed) {
  testConnector_->addTable("t", ROW("a", BIGINT()));
  testConnector_->addTable("e", ROW({"src", "dst"}, BIGINT()));
  {
    // The anchor is one row, so the loop runs on one task, and the table the
    // step joins is gathered to it.
    auto logicalPlan = parseSelect(
        "WITH RECURSIVE r(n) AS ("
        "  SELECT CAST(1 AS BIGINT) "
        "  UNION ALL "
        "  SELECT dst FROM r JOIN e ON src = n) "
        "SELECT n FROM r");
    auto makeMatcher = [](bool distributed) {
      return matchFixedPoint(
                 startFixedPoint("r")
                     .outputState(/*append=*/true, matchValues())
                     .hashTableState(matchScan("e").gatherIf(distributed))
                     .plan(matchDelta("r", {"n"})
                               .stateHashJoin({"n"})
                               .project({"dst"})))
          .build();
    };
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), makeMatcher(false));
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        planVelox(logicalPlan).plan, makeMatcher(true));
  }
  {
    auto logicalPlan = parseSelect(
        "WITH RECURSIVE r(n) AS ("
        "  SELECT a FROM t "
        "  UNION ALL "
        "  SELECT dst FROM r JOIN e ON src = n) "
        "SELECT n FROM r");
    auto makeMatcher = [](bool distributed) {
      return matchFixedPoint(
                 startFixedPoint("r")
                     .outputState(/*append=*/true, matchScan("t"))
                     .hashTableState(matchScan("e").broadcastIf(distributed))
                     .plan(matchDelta("r", {"n"})
                               .stateHashJoin({"n"})
                               .project({"dst"})))
          .project({"a AS n"})
          .gatherIf(distributed)
          .build();
    };
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), makeMatcher(false));
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        planVelox(logicalPlan).plan, makeMatcher(true));
  }
  {
    // SQL rejects an aggregate in the recursive term; a logical plan can
    // have one.
    auto context = makeContext();
    auto anchor = lp::PlanBuilder(context).tableScan("t").project({"a AS n"});
    auto step = lp::PlanBuilder(context)
                    .fixedPointState("r", anchor)
                    .filter("n < 10")
                    .aggregate({}, {"max(n) AS m"})
                    .project({"m + 1 AS n"});
    auto logicalPlan = anchor.recursiveCte("r", step).build();
    auto makeMatcher = [](bool distributed) {
      return matchFixedPoint(
                 startFixedPoint("r")
                     .outputState(
                         /*append=*/true, matchScan("t").gatherIf(distributed))
                     .plan(matchDelta("r", {"n"})
                               .filter("n < 10")
                               .singleAggregationIf(
                                   !distributed, {}, {"max(n) AS m"})
                               .localAggregationIf(distributed, {}, {"max(n)"})
                               .project({"m + 1"})))
          .project({"a AS n"})
          .build();
    };
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), makeMatcher(false));
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        planVelox(logicalPlan).plan, makeMatcher(true));
  }
}

// With several workers, a step that needs every row of a key on one task, or
// that joins a table too large to broadcast, runs as one loop whose state is
// spread over the workers by a key. Each iteration repartitions what the step
// produces, and the workers stop together once no worker has new rows.
TEST_F(FixedPointTest, partitionedLoop) {
  testConnector_->addTable("t", ROW("a", BIGINT()));
  testConnector_->addTable("e", ROW({"src", "dst"}, BIGINT()))
      ->setStats(100'000'000, {});
  {
    auto context = makeContext();
    auto anchor = lp::PlanBuilder(context).tableScan("t").project({"a AS n"});
    auto logicalPlan = anchor
                           .recursiveCte(
                               "r",
                               lp::PlanBuilder(context)
                                   .fixedPointState("r", anchor)
                                   .filter("n < 10")
                                   .aggregate({"n"}, {})
                                   .project({"n + 1 AS n"}))
                           .build();
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchFixedPoint(startFixedPoint("r")
                            .outputState(/*append=*/true, matchScan("t"))
                            .plan(matchDelta("r", {"n"})
                                      .filter("n < 10")
                                      .singleAggregation({"n"}, {})
                                      .project({"n + 1"}))
                            .convergeOnEmpty())
            .project({"a AS n"})
            .build());

    auto matcher =
        matchFixedPoint(startFixedPoint("r")
                            .outputState(
                                /*append=*/true, matchScan("t").shuffle({"a"}))
                            .plan(matchDelta("r", {"n"})
                                      .filter("n < 10")
                                      .localAggregation({"n"}, {})
                                      .project({"n + 1 AS m"})
                                      .shuffle({"m"}))
                            .convergeOnEmptyEverywhere())
            .project({"a AS n"})
            .gather()
            .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
  {
    auto logicalPlan = parseSelect(
        "WITH RECURSIVE r(n) AS ("
        "  SELECT a FROM t "
        "  UNION ALL "
        "  SELECT dst FROM r JOIN e ON src = n) "
        "SELECT n FROM r");
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchFixedPoint(startFixedPoint("r")
                            .outputState(/*append=*/true, matchScan("t"))
                            .hashTableState(matchScan("e"))
                            .plan(matchDelta("r", {"n"})
                                      .stateHashJoin({"n"})
                                      .project({"dst"}))
                            .convergeOnEmpty())
            .project({"a AS n"})
            .build());

    auto matcher =
        matchFixedPoint(
            startFixedPoint("r")
                .outputState(/*append=*/true, matchScan("t").shuffle({"a"}))
                .hashTableState(matchScan("e").shuffle({"src"}))
                .plan(matchDelta("r", {"n"})
                          .stateHashJoin({"n"})
                          .project({"dst"})
                          .shuffle({"dst"}))
                .convergeOnEmptyEverywhere())
            .project({"a AS n"})
            .gather()
            .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
  {
    // A join key computed from the state leaves the state unpartitioned: each
    // step repartitions the delta on the key it computes.
    auto logicalPlan = parseSelect(
        "WITH RECURSIVE r(n) AS ("
        "  SELECT a FROM t "
        "  UNION ALL "
        "  SELECT dst FROM r JOIN e ON src = n + 1) "
        "SELECT n FROM r");
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchFixedPoint(startFixedPoint("r")
                            .outputState(/*append=*/true, matchScan("t"))
                            .hashTableState(matchScan("e"))
                            .plan(matchDelta("r", {"n"})
                                      .project({"n + 1 AS k"})
                                      .stateHashJoin({"k"})
                                      .project({"dst"}))
                            .convergeOnEmpty())
            .project({"a AS n"})
            .build());

    auto matcher =
        matchFixedPoint(startFixedPoint("r")
                            .outputState(
                                /*append=*/true,
                                matchScan("t")
                                    .project({"a", "a + 1 AS k"})
                                    .shuffle({"k"})
                                    .project({"a"}))
                            .hashTableState(matchScan("e").shuffle({"src"}))
                            .plan(matchDelta("r", {"n"})
                                      .project({"n", "n + 1 AS k"})
                                      .shuffle({"k"})
                                      .stateHashJoin({"k"})
                                      .project({"dst"}))
                            .convergeOnEmptyEverywhere())
            .project({"a AS n"})
            .gather()
            .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
  {
    // The join key reads a column the step computes, so the anchor cannot be
    // placed by it: each task loops over its own anchor rows instead.
    auto logicalPlan = parseSelect(
        "WITH RECURSIVE r(n) AS ("
        "  SELECT a FROM t "
        "  UNION ALL "
        "  SELECT dst FROM r CROSS JOIN UNNEST(ARRAY[n, n + 1]) AS u(k) "
        "  JOIN e ON src = k) "
        "SELECT n FROM r");
    auto makeMatcher = [](bool distributed) {
      return matchFixedPoint(
                 startFixedPoint("r")
                     .outputState(/*append=*/true, matchScan("t"))
                     .hashTableState(matchScan("e").broadcastIf(distributed))
                     .plan(matchDelta("r", {"n"})
                               .project()
                               .unnest()
                               .stateHashJoin({"k"})
                               .project({"dst"}))
                     .convergeOnEmpty())
          .project({"a AS n"})
          .gatherIf(distributed)
          .build();
    };
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), makeMatcher(false));
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        planVelox(logicalPlan).plan, makeMatcher(true));
  }
  {
    // Grouping on n places the state by n, but the global aggregate then
    // gathers it, so the loop runs on one task.
    auto context = makeContext();
    auto anchor = lp::PlanBuilder(context).tableScan("t").project({"a AS n"});
    auto logicalPlan = anchor
                           .recursiveCte(
                               "r",
                               lp::PlanBuilder(context)
                                   .fixedPointState("r", anchor)
                                   .aggregate({"n"}, {})
                                   .aggregate({}, {"max(n) AS m"})
                                   .project({"m + 1 AS n"}))
                           .build();
    auto makeMatcher = [](bool distributed) {
      return matchFixedPoint(
                 startFixedPoint("r")
                     .outputState(
                         /*append=*/true, matchScan("t").gatherIf(distributed))
                     .plan(matchDelta("r", {"n"})
                               .singleAggregationIf(!distributed, {"n"}, {})
                               .localAggregationIf(distributed, {"n"}, {})
                               .singleAggregationIf(
                                   !distributed, {}, {"max(n) AS m"})
                               .localAggregationIf(distributed, {}, {"max(n)"})
                               .project({"m + 1"}))
                     .convergeOnEmpty())
          .project({"a AS n"})
          .build();
    };
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), makeMatcher(false));
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        planVelox(logicalPlan).plan, makeMatcher(true));
  }
}

// A loop that appends its rows stops on a convergence over the latest delta,
// or at the bound, where it fails or returns its rows.
TEST_F(FixedPointTest, appendConvergenceAndBound) {
  auto anchor = singleRow("n", 1);
  auto step = lp::PlanBuilder(context_)
                  .fixedPointState("counter", anchor)
                  .project({"n + 1 as n"});
  auto makeLogicalPlan = [&](const lp::LogicalPlanNodePtr& convergence,
                             bool failAtBound) {
    return std::make_shared<lp::FixedPointNode>(
        "fixed_point",
        "counter",
        anchor.planNode(),
        step.planNode(),
        lp::FixedPointNode::OutputMode::kAppend,
        convergence,
        /*maxIterations=*/10,
        failAtBound,
        /*keyColumns=*/std::vector<std::string>{});
  };

  auto numbers = [&](int64_t last) {
    return makeRowVector(
        {makeFlatVector<int64_t>(last, [](auto row) { return row + 1; })});
  };

  {
    // Stops once the latest delta reaches 10.
    auto logicalPlan = makeLogicalPlan(
        lp::PlanBuilder(context_)
            .fixedPointState("counter", anchor)
            .aggregate({}, {"max(n) AS m"})
            .project({"m >= 10"})
            .planNode(),
        /*failAtBound=*/true);
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchFixedPoint(
            startFixedPoint("counter")
                .outputState(/*append=*/true, matchValues())
                .plan(matchDelta("counter", {"n"}).project({"n + 1"}))
                .converge(
                    matchDelta("counter", {"n"})
                        .singleAggregation({}, {"max(n)"})
                        .project(),
                    {.maxIterations = 10}))
            .build());
    checkSame(logicalPlan, {numbers(10)});
  }
  {
    // Returns its rows at the bound.
    auto logicalPlan = makeLogicalPlan(nullptr, /*failAtBound=*/false);
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchFixedPoint(startFixedPoint("counter").convergeOnEmpty(
                            {.maxIterations = 10, .failAtBound = false}))
            .build());
    checkSame(logicalPlan, {numbers(11)});
  }
  {
    // Fails at the bound.
    VELOX_ASSERT_THROW(
        runVelox(makeLogicalPlan(nullptr, /*failAtBound=*/true)),
        "Fixed point did not converge within 10 iterations");
  }
}

// A loop that replaces its state a fixed number of times returns its state at
// the bound.
TEST_F(FixedPointTest, fixedIterationCount) {
  auto anchor = singleRow("n", 1);
  auto logicalPlan = lp::PlanBuilder(anchor)
                         .iterate(
                             "counter",
                             lp::PlanBuilder(context_)
                                 .fixedPointState("counter", anchor)
                                 .project({"n + 1 as n"}),
                             /*numIterations=*/5,
                             /*keyColumns=*/{})
                         .build();

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(logicalPlan),
      matchFixedPoint(
          startFixedPoint("counter")
              .outputState(/*append=*/false, matchValues())
              .plan(matchDelta("counter", {"n"}).project({"n + 1"}))
              .convergeOnEmpty({.maxIterations = 5, .failAtBound = false}))
          .build());

  checkSame(logicalPlan, {makeRowVector({makeFlatVector<int64_t>({6})})});
}

// With several workers, a loop with a convergence and a row-local step runs on
// one task.
TEST_F(FixedPointTest, convergenceOnOneTask) {
  testConnector_->addTable("t", ROW("n", BIGINT()))
      ->addData(makeRowVector({makeFlatVector<int64_t>({1, 5})}));

  auto context = makeContext();
  auto anchor = lp::PlanBuilder(context).tableScan("t");
  auto logicalPlan = lp::PlanBuilder(anchor)
                         .iterateUntil(
                             "r",
                             lp::PlanBuilder(context)
                                 .fixedPointState("r", anchor)
                                 .project({"n + 1 AS n"}),
                             lp::PlanBuilder(context)
                                 .fixedPointState("r", anchor)
                                 .aggregate({}, {"max(n) AS m"})
                                 .project({"m >= 10"}),
                             /*maxIterations=*/100,
                             /*failAtBound=*/true,
                             /*keyColumns=*/{})
                         .build();

  auto makeMatcher = [](bool distributed) {
    return matchFixedPoint(
               startFixedPoint("r")
                   .outputState(
                       /*append=*/false, matchScan("t").gatherIf(distributed))
                   .plan(matchDelta("r", {"n"}).project({"n + 1"}))
                   .converge(matchDelta("r", {"n"})
                                 .singleAggregation({}, {"max(n)"})
                                 .project()))
        .build();
  };
  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), makeMatcher(false));
  AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, makeMatcher(true));

  // The loop stops once 5 reaches 10, after five iterations, and returns its
  // last state.
  checkSame(logicalPlan, {makeRowVector({makeFlatVector<int64_t>({6, 10})})});
}

// A partitioned loop gathers its convergence, so every worker stops on one
// answer, even when the convergence reads one worker's rows.
TEST_F(FixedPointTest, partitionedConvergence) {
  testConnector_->addTable("t", ROW("k", BIGINT()))
      ->addData(makeRowVector({makeFlatVector<int64_t>({1, 2, 3, 4})}));

  auto context = makeContext();
  auto anchor =
      lp::PlanBuilder(context).tableScan("t").project({"k", "false AS done"});
  auto state = [&] {
    return lp::PlanBuilder(context).fixedPointState("r", anchor);
  };
  auto logicalPlan = lp::PlanBuilder(anchor)
                         .iterateUntil(
                             "r",
                             state()
                                 .aggregate({"k", "done"}, {})
                                 .project({"k", "true AS done"}),
                             state().filter("k = 1").project({"done"}),
                             /*maxIterations=*/10,
                             /*failAtBound=*/true,
                             /*keyColumns=*/{})
                         .build();

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(logicalPlan),
      matchFixedPoint(
          startFixedPoint("r")
              .outputState(/*append=*/false, matchScan("t").project())
              .plan(matchDelta("r", {"k", "done"})
                        .singleAggregation({"k", "done"}, {})
                        .project())
              .converge(
                  matchDelta("r", {"k", "done"}).filter("k = 1").project(),
                  {.maxIterations = 10}))
          .build());

  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planVelox(logicalPlan).plan,
      matchFixedPoint(
          startFixedPoint("r")
              .outputState(
                  /*append=*/false, matchScan("t").project().shuffle())
              .converge(
                  matchDelta("r", {"k", "done"})
                      .filter("k = 1")
                      .project()
                      .gather(),
                  {.maxIterations = 10}))
          .gather()
          .build());

  checkSame(
      logicalPlan,
      {makeRowVector({
          makeFlatVector<int64_t>({1, 2, 3, 4}),
          makeConstant(true, 4),
      })});
}

// A step or a convergence that the optimizer proves does not depend on the
// loop's state fails the query.
TEST_F(FixedPointTest, stateIndependentBranchRejected) {
  auto anchor = singleRow("n", 1);
  auto state = [&] {
    return lp::PlanBuilder(context_).fixedPointState("counter", anchor);
  };
  auto stop = [&] {
    return state().aggregate({}, {"count(*) AS c"}).project({"c = 0"});
  };
  {
    auto logicalPlan =
        lp::PlanBuilder(anchor)
            .iterateUntil(
                "counter",
                state().filter("false").unionAll(singleRow("n", 2)),
                stop(),
                /*maxIterations=*/10,
                /*failAtBound=*/false,
                /*keyColumns=*/{})
            .build();
    VELOX_ASSERT_THROW(
        toSingleNodePlan(logicalPlan),
        "FixedPoint step does not depend on the loop's state");
  }
  {
    auto logicalPlan = lp::PlanBuilder(anchor)
                           .iterateUntil(
                               "counter",
                               state().project({"n + 1 AS n"}),
                               state()
                                   .filter("false")
                                   .aggregate({}, {"count(*) AS c"})
                                   .project({"c = 0"}),
                               /*maxIterations=*/10,
                               /*failAtBound=*/false,
                               /*keyColumns=*/{})
                           .build();
    VELOX_ASSERT_THROW(
        toSingleNodePlan(logicalPlan),
        "FixedPoint convergence does not depend on the loop's state");
  }
}

// With several workers, a loop with a convergence runs as one partitioned loop
// when its step has keys to partition on, even when the step is row-local.
TEST_F(FixedPointTest, rowLocalConvergencePartitioned) {
  testConnector_->addTable("t", ROW("a", BIGINT()))
      ->addData(makeRowVector({makeFlatVector<int64_t>({1, 2})}));
  testConnector_->addTable("e", ROW({"src", "dst"}, BIGINT()))
      ->addData(makeRowVector({
          makeFlatVector<int64_t>({1, 2}),
          makeFlatVector<int64_t>({2, 3}),
      }));

  auto context = makeContext();
  auto anchor = lp::PlanBuilder(context).tableScan("t").project({"a AS n"});
  auto state = [&] {
    return lp::PlanBuilder(context).fixedPointState("r", anchor);
  };
  auto logicalPlan =
      lp::PlanBuilder(anchor)
          .iterateUntil(
              "r",
              state()
                  .join(
                      lp::PlanBuilder(context).tableScan("e"),
                      "n = src",
                      lp::JoinType::kInner)
                  .project({"dst AS n"}),
              state().aggregate({}, {"max(n) AS m"}).project({"m >= 3"}),
              /*maxIterations=*/10,
              /*failAtBound=*/true,
              /*keyColumns=*/{})
          .build();

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(logicalPlan),
      matchFixedPoint(
          startFixedPoint("r")
              .outputState(/*append=*/false, matchScan("t"))
              .hashTableState(matchScan("e"))
              .plan(
                  matchDelta("r", {"n"}).stateHashJoin({"n"}).project({"dst"}))
              .converge(matchDelta("r", {"n"})
                            .singleAggregation({}, {"max(n)"})
                            .project()))
          .project({"a AS n"})
          .build());
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planVelox(logicalPlan).plan,
      matchFixedPoint(
          startFixedPoint("r")
              .outputState(/*append=*/false, matchScan("t").shuffle({"a"}))
              .hashTableState(matchScan("e").shuffle({"src"}))
              .plan(matchDelta("r", {"n"})
                        .stateHashJoin({"n"})
                        .project({"dst"})
                        .shuffle({"dst"}))
              .converge(matchDelta("r", {"n"})
                            .partialAggregation({}, {"max(n)"})
                            .gather()
                            .finalAggregation()
                            .project()))
          .project({"a AS n"})
          .gather()
          .build());

  checkSame(logicalPlan, {makeRowVector({makeFlatVector<int64_t>({2, 3})})});
}

// With several workers, a loop whose step joins on a shuffled input that cannot
// be the join's probe side runs on one task.
TEST_F(FixedPointTest, unswappableJoinOnOneTask) {
  testConnector_->addTable("t", ROW("a", BIGINT()))
      ->addData(makeRowVector({makeFlatVector<int64_t>({1, 2})}));
  testConnector_->addTable("e", ROW({"src", "dst"}, BIGINT()))
      ->addData(makeRowVector({
          makeFlatVector<int64_t>({1, 2}),
          makeFlatVector<int64_t>({2, 3}),
      }));

  auto context = makeContext();
  auto anchor = lp::PlanBuilder(context).tableScan("t").project({"a AS n"});
  auto state = [&] {
    return lp::PlanBuilder(context).fixedPointState("r", anchor);
  };
  // Keeps the vertices that some vertex of the state links to.
  auto logicalPlan =
      lp::PlanBuilder(anchor)
          .iterateUntil(
              "r",
              state()
                  .filter(
                      lp::In(
                          lp::Col("n"),
                          lp::Subquery(
                              state()
                                  .join(
                                      lp::PlanBuilder(context).tableScan("e"),
                                      "n = src",
                                      lp::JoinType::kInner)
                                  .aggregate({"dst"}, {})
                                  .planNode())))
                  .project({"n + 1 AS n"}),
              state().aggregate({}, {"max(n) AS m"}).project({"m >= 3"}),
              /*maxIterations=*/10,
              /*failAtBound=*/true,
              /*keyColumns=*/{})
          .build();

  auto makeMatcher = [](bool distributed) {
    return matchFixedPoint(
               startFixedPoint("r")
                   .outputState(
                       /*append=*/false, matchScan("t").gatherIf(distributed))
                   .hashTableState(matchScan("e").gatherIf(distributed)))
        .project({"a AS n"})
        .build();
  };
  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), makeMatcher(false));
  AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, makeMatcher(true));
  checkSame(logicalPlan, {makeRowVector({makeFlatVector<int64_t>({3})})});
}

// With an empty anchor, the loop still runs its step, which can produce rows.
TEST_F(FixedPointTest, emptyAnchor) {
  auto anchor = lp::PlanBuilder(context_).values(
      ROW("n", BIGINT()), std::vector<Variant>{});
  auto state = [&] {
    return lp::PlanBuilder(context_).fixedPointState("counter", anchor);
  };
  auto logicalPlan =
      lp::PlanBuilder(anchor)
          .iterateUntil(
              "counter",
              state().project({"n + 1 AS n"}).unionAll(singleRow("n", 1)),
              state().aggregate({}, {"count(*) AS c"}).project({"c > 0"}),
              /*maxIterations=*/10,
              /*failAtBound=*/true,
              /*keyColumns=*/{})
          .build();

  // The first iteration adds 1 to no rows and unions in 1.
  checkSame(logicalPlan, {makeRowVector({makeFlatVector<int64_t>({1})})});
}

// A loop that replaces its state with a step that produces no rows returns no
// rows.
TEST_F(FixedPointTest, replaceWithEmptyStep) {
  auto anchor = singleRow("n", 1);
  auto state = [&] {
    return lp::PlanBuilder(context_).fixedPointState("counter", anchor);
  };
  auto logicalPlan =
      lp::PlanBuilder(anchor)
          .iterateUntil(
              "counter",
              state().filter("n > 100"),
              state().aggregate({}, {"count(*) AS c"}).project({"c = 0"}),
              /*maxIterations=*/10,
              /*failAtBound=*/true,
              /*keyColumns=*/{})
          .build();

  checkSame(logicalPlan, {makeRowVector({makeFlatVector<int64_t>({})})});
}

TEST_F(FixedPointTest, outerJoinInAnchor) {
  // The anchor outer join remains outer when the step maps NULL to non-NULL.
  testConnector_->addTable("u", ROW("n", INTEGER()));

  auto logicalPlan = parseSelect(
      "WITH RECURSIVE t(n) AS ("
      "  SELECT r.n FROM (VALUES 1) l(k) "
      "  LEFT JOIN (VALUES (2, 2)) r(k, n) ON l.k = r.k "
      "  UNION ALL "
      "  SELECT coalesce(n, 1) FROM t WHERE n IS NULL) "
      "SELECT t.n FROM t JOIN u ON t.n = u.n");

  auto matcher = matchFixedPoint(startFixedPoint("t")
                                     .outputState(
                                         /*append=*/true, matchValues())
                                     .plan(matchDelta("t", {"n"})
                                               .filter("n IS NULL")
                                               .project({"coalesce(n, 1)"})))
                     .aliases({"n"})
                     .hashJoinInner(matchScan("u"))
                     .project({"n"})
                     .build();
  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
}

TEST_F(FixedPointTest, outerFilter) {
  auto logicalPlan = parseSelect(
      "WITH RECURSIVE counter(n) AS ("
      "     VALUES 1 "
      "     UNION ALL "
      "     SELECT n + 1 FROM counter WHERE n < 20"
      ") "
      "SELECT n FROM counter WHERE n > 15");

  auto matcher =
      matchFixedPoint(startFixedPoint("counter")
                          .outputState(/*append=*/true, matchValues())
                          .plan(matchDelta("counter", {"n"})
                                    .filter("n < 20")
                                    .project({"n + 1"})))
          .aliases({"n"})
          .filter("n > 15")
          .build();
  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
}

TEST_F(FixedPointTest, outerSelectsOneColumn) {
  // The recursive state keeps every column the step writes, even when the
  // outer query reads only one of them.
  auto logicalPlan = parseSelect(
      "WITH RECURSIVE counter(n, carried) AS ("
      "     VALUES (1, 7) "
      "     UNION ALL "
      "     SELECT n + 1, carried FROM counter WHERE n < 10"
      ") "
      "SELECT n FROM counter");

  auto matcher =
      matchFixedPoint(startFixedPoint("counter")
                          .outputState(/*append=*/true, matchValues())
                          .plan(matchDelta("counter", {"n", "carried"})
                                    .filter("n < 10")
                                    .project({"n + 1", "carried"}))
                          .convergeOnEmpty())
          .project({"n"})
          .build();
  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
}

TEST_F(FixedPointTest, orphanRecursiveReference) {
  auto counter = singleRow("n", 1);
  auto orphan =
      lp::PlanBuilder(context_).fixedPointState("counter", counter).build();

  VELOX_ASSERT_THROW(
      toSingleNodePlan(orphan),
      "RecursiveReferenceNode outside any enclosing FixedPoint");
}

TEST_F(FixedPointTest, siblingRecursions) {
  auto logicalPlan = parseSelect(
      "WITH RECURSIVE "
      "     r1(a) AS (VALUES 0 UNION ALL SELECT a + 1 FROM r1 WHERE a < 3), "
      "     r2(b) AS (SELECT a FROM r1 UNION ALL SELECT b + 10 FROM r2 WHERE b < 30) "
      "SELECT b FROM r2");

  auto matcher =
      matchFixedPoint(
          startFixedPoint("r2")
              .outputState(
                  /*append=*/true,
                  matchFixedPoint(startFixedPoint("r1")
                                      .outputState(
                                          /*append=*/true, matchValues())
                                      .plan(matchDelta("r1", {"a"})
                                                .filter("a < 3")
                                                .project({"a + 1"}))))
              .plan(
                  matchDelta("r2", {"b"}).filter("b < 30").project({"b + 10"})))
          .project({"a as b"})
          .build();

  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
}

TEST_F(FixedPointTest, twoRecursiveReferencesRejected) {
  auto logicalPlan = parseSelect(
      "WITH RECURSIVE reach(src, dst) AS ("
      "     VALUES (1, 2) "
      "     UNION ALL "
      "     SELECT a.src, b.dst FROM reach a JOIN reach b ON a.dst = b.src"
      ") "
      "SELECT src, dst FROM reach");

  VELOX_ASSERT_THROW(
      toSingleNodePlan(logicalPlan),
      "Optimizer v2 supports exactly one RecursiveReferenceNode per FixedPoint step");
}

TEST_F(FixedPointTest, twoRecursionsInUnionRejected) {
  auto logicalPlan = parseSelect(
      "WITH RECURSIVE r(n) AS (VALUES 1 UNION ALL SELECT n + 1 FROM r WHERE n < 10) "
      "SELECT n FROM r UNION ALL SELECT n FROM r");

  VELOX_ASSERT_THROW(
      toSingleNodePlan(logicalPlan),
      "Multiple FixedPoint nodes in one execution fragment are not yet supported");
}

TEST_F(FixedPointTest, twoRecursionsInJoinRejected) {
  auto logicalPlan = parseSelect(
      "WITH RECURSIVE r(n) AS ("
      "     VALUES 1 "
      "     UNION ALL "
      "     SELECT n + 1 FROM r WHERE n < 10"
      ") "
      "SELECT l.n FROM r l JOIN r r2 ON l.n = r2.n");

  VELOX_ASSERT_THROW(
      toSingleNodePlan(logicalPlan),
      "Multiple FixedPoint nodes in one execution fragment are not yet supported");
}

TEST_F(FixedPointTest, outerJoinAndFilter) {
  // A predicate above the fixed point stays there, and still reaches the other
  // side of the join through the equality.
  testConnector_->addTable("u", ROW("n", INTEGER()));

  auto logicalPlan = parseSelect(
      "WITH RECURSIVE r(n) AS ("
      "     VALUES 1 "
      "     UNION ALL "
      "     SELECT n + 1 FROM r WHERE n < 10"
      ") "
      " SELECT r.n + 1 FROM r JOIN u ON r.n = u.n "
      "WHERE r.n > 0");

  auto matcher =
      matchFixedPoint(
          startFixedPoint("r")
              .outputState(/*append=*/true, matchValues())
              .plan(matchDelta("r", {"n"}).filter("n < 10").project({"n + 1"})))
          .aliases({"n"})
          .filter("n > 0")
          .hashJoinInner(matchScan("u").aliases({"n"}).filter("n > 0"))
          .project({"n + 1"})
          .build();

  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
}

TEST_F(FixedPointTest, nestedRecursionRejected) {
  auto inner = singleRow("n", 1);
  auto innerBody = lp::PlanBuilder(context_)
                       .fixedPointState("inner", inner)
                       .project({"n + 1 as n"});
  inner.recursiveCte("inner", innerBody);

  auto outer = singleRow("n", 1);
  auto outerBody =
      lp::PlanBuilder(context_).fixedPointState("outer", outer).unionAll(inner);
  auto plan = outer.recursiveCte("outer", outerBody).build();

  VELOX_ASSERT_THROW(
      toSingleNodePlan(plan),
      "Nested FixedPoint translation is not yet implemented");
}

// PageRank as PALM computes it (palm/programs/PageRank.cpp). Each iteration
// gives every vertex (1 - d) / N plus d times the weighted rank of its in-edges
// and an even share of the rank held by vertices with no out-edges, and stops
// once the total squared change is below N * tolerance^2.
TEST_F(FixedPointTest, pageRank) {
  // 1 -> 2, 1 -> 3, 2 -> 3, 3 -> 1; 4 has no out-edges.
  testConnector_
      ->addTable("vertices", ROW({"vertex_id", "out_degree"}, BIGINT()))
      ->addData(makeRowVector({
          makeFlatVector<int64_t>({1, 2, 3, 4}),
          makeFlatVector<int64_t>({2, 1, 1, 0}),
      }));
  testConnector_
      ->addTable(
          "edges",
          ROW({"src", "dst", "weight"}, {BIGINT(), BIGINT(), DOUBLE()}))
      ->addData(makeRowVector({
          makeFlatVector<int64_t>({1, 1, 2, 3}),
          makeFlatVector<int64_t>({2, 3, 3, 1}),
          makeFlatVector<double>({0.5, 0.5, 1.0, 1.0}),
      }));

  static constexpr auto kRanks = "ranks";

  // The PageRank loop over the edges in 'edgesTable'.
  auto pageRank = [&](std::string_view edgesTable) {
    auto context = makeContext();

    // N = 4 vertices, damping factor d = 0.85, tolerance 1e-4.
    auto anchor = lp::PlanBuilder(context)
                      .tableScan("vertices")
                      .project(
                          {"vertex_id",
                           "0.25 AS rank",
                           "out_degree",
                           /*placeholder*/ "0.0 AS squared_error"});

    auto state = [&](std::optional<std::string> alias = std::nullopt) {
      auto builder = lp::PlanBuilder(context).fixedPointState(kRanks, anchor);
      if (alias.has_value()) {
        builder.as(*alias);
      }
      return builder;
    };

    auto step =
        state("c")
            // Weighted rank each vertex receives along its in-edges.
            .join(
                state("s")
                    .filter("s.out_degree > 0")
                    .join(
                        lp::PlanBuilder(context)
                            .tableScan(std::string(edgesTable))
                            .as("e"),
                        "s.vertex_id = e.src",
                        lp::JoinType::kInner)
                    .aggregate(
                        {"e.dst"}, {"sum(s.rank * e.weight) AS inbound_rank"})
                    .as("i"),
                "c.vertex_id = i.dst",
                lp::JoinType::kLeft)
            // Rank held by vertices with no out-edges, shared evenly by all
            // vertices.
            .crossJoin(
                state()
                    .aggregate(
                        {}, {"sum(if(out_degree = 0, rank, 0.0)) AS dangling"})
                    .as("g"))
            .project({
                "c.vertex_id AS vertex_id",
                "0.0375 + 0.85 * (coalesce(i.inbound_rank, 0.0) + g.dangling / 4.0) AS rank",
                "c.out_degree AS out_degree",
                "c.rank AS previous",
            })
            .project({
                "vertex_id",
                "rank",
                "out_degree",
                "(rank - previous) * (rank - previous) AS squared_error",
            });
    return anchor
        .iterateUntil(
            kRanks,
            step,
            state()
                .aggregate({}, {"sum(squared_error) AS total"})
                .project({"coalesce(total, 0.0) < 4e-8"}),
            /*maxIterations=*/100,
            /*failAtBound=*/false,
            /*keyColumns=*/{"vertex_id"})
        .project({"vertex_id", "rank"})
        .build();
  };
  auto logicalPlan = pageRank("edges");

  auto ranks = [&] {
    return matchDelta(
        kRanks, {"vertex_id", "rank", "out_degree", "squared_error"});
  };
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(logicalPlan),
      matchFixedPoint(
          startFixedPoint(kRanks)
              .outputState(/*append=*/false, matchScan("vertices").project())
              .hashTableState(matchScan("edges"))
              .plan(
                  ranks()
                      .hashJoinLeft(
                          ranks()
                              .filter("out_degree > 0")
                              .stateHashJoin({"vertex_id"})
                              .project()
                              .project()
                              .singleAggregation())
                      .nestedLoopJoin(ranks().project().singleAggregation())
                      .project())
              .converge(
                  ranks()
                      .singleAggregation({}, {"sum(squared_error)"})
                      .project(),
                  {.maxIterations = 100, .failAtBound = false}))
          .project()
          .build());

  testConnector_
      ->addTable(
          "large_edges",
          ROW({"src", "dst", "weight"}, {BIGINT(), BIGINT(), DOUBLE()}))
      ->setStats(
          100'000'000,
          {{"src", {.numDistinct = 4}}, {"dst", {.numDistinct = 4}}});
  // With edges too large to broadcast, every worker holds all the ranks and
  // joins them with its own share of the edges. The inbound sums are shuffled
  // on the destination, and their totals are broadcast to every worker. Worker
  // 0 decides when to stop and emits the result. Each step runs with all of a
  // worker's drivers, so its aggregations split around local exchanges.
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planVelox(pageRank("large_edges")).plan,
      matchFixedPoint(
          startFixedPoint(kRanks)
              .outputState(
                  /*append=*/false, matchScan("vertices").project().broadcast())
              .hashTableState(matchScan("large_edges"))
              .plan(
                  ranks()
                      .filter("out_degree > 0")
                      .stateHashJoin({"vertex_id"})
                      .project()
                      .project()
                      .partialAggregation()
                      .shuffle()
                      .localPartition()
                      .finalAggregation()
                      .broadcast()
                      .hashJoinRight(ranks())
                      .nestedLoopJoin(
                          ranks()
                              .project()
                              .partialAggregation()
                              .localGather()
                              .finalAggregation())
                      .project())
              .convergeOnWorkerZero(
                  ranks()
                      .singleAggregation({}, {"sum(squared_error)"})
                      .project(),
                  {.maxIterations = 100, .failAtBound = false})
              .outputOnWorkerZero())
          .project()
          .gather()
          .build());

  // Converges after 15 iterations.
  checkSame(
      logicalPlan,
      {makeRowVector({
          makeFlatVector<int64_t>({1, 2, 3, 4}),
          makeFlatVector<double>({
              0.3693651552827742,
              0.20454346105795695,
              0.37847233602375613,
              0.04761904763551231,
          }),
      })});
}

// Connected components: each iteration lowers every vertex's label to the
// smallest label among its in-neighbors and stops once no label changes. Only
// vertices whose label changed in the last iteration send it.
TEST_F(FixedPointTest, connectedComponents) {
  // Components {1, 2, 3}, {4, 5} and {6}, each edge in both directions.
  testConnector_->addTable("vertices", ROW("vertex", BIGINT()))
      ->addData(makeRowVector({makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6})}));
  testConnector_->addTable("edges", ROW({"src", "dst"}, BIGINT()))
      ->addData(makeRowVector({
          makeFlatVector<int64_t>({1, 2, 2, 3, 4, 5}),
          makeFlatVector<int64_t>({2, 1, 3, 2, 5, 4}),
      }));

  static constexpr auto kLabels = "labels";

  // The connected-components loop over the edges in 'edgesTable'.
  auto connectedComponents = [&](std::string_view edgesTable) {
    auto context = makeContext();

    auto anchor = lp::PlanBuilder(context)
                      .tableScan("vertices")
                      .project(
                          {"vertex",
                           "vertex AS label",
                           /*placeholder*/ "true AS changed"});

    auto state = [&](std::optional<std::string> alias = std::nullopt) {
      auto builder = lp::PlanBuilder(context).fixedPointState(kLabels, anchor);
      if (alias.has_value()) {
        builder.as(*alias);
      }
      return builder;
    };

    auto step = state("v")
                    .join(
                        state("n")
                            .filter("n.changed")
                            .join(
                                lp::PlanBuilder(context)
                                    .tableScan(std::string(edgesTable))
                                    .as("e"),
                                "n.vertex = e.src",
                                lp::JoinType::kInner)
                            .aggregate({"e.dst"}, {"min(n.label) AS label"})
                            .as("m"),
                        "v.vertex = m.dst",
                        lp::JoinType::kLeft)
                    .project({
                        "v.vertex AS vertex",
                        "least(v.label, coalesce(m.label, v.label)) AS label",
                        "coalesce(m.label < v.label, false) AS changed",
                    });
    return anchor
        .iterateUntil(
            kLabels,
            step,
            state()
                .aggregate({}, {"count_if(changed) AS num_changed"})
                .project({"num_changed = 0"}),
            /*maxIterations=*/100,
            /*failAtBound=*/true,
            /*keyColumns=*/{"vertex"})
        .build();
  };
  auto logicalPlan = connectedComponents("edges");

  auto labels = [&] {
    return matchDelta(kLabels, {"vertex", "label", "changed"});
  };
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(logicalPlan),
      matchFixedPoint(
          startFixedPoint(kLabels)
              .outputState(/*append=*/false, matchScan("vertices").project())
              .hashTableState(matchScan("edges"))
              .plan(
                  labels()
                      .hashJoinLeft(
                          labels()
                              .filter("changed")
                              .stateHashJoin({"vertex"})
                              .project()
                              .singleAggregation({"dst"}, {"min(label)"}))
                      .project())
              .converge(
                  labels()
                      .singleAggregation({}, {"count_if(changed)"})
                      .project(),
                  {.maxIterations = 100}))
          .build());

  // The state stays partitioned on vertex: each worker probes its vertices'
  // out-edges and shuffles the smallest label it sends to each destination.
  // Replicating the state moves the same shuffle and the new labels to every
  // worker as well, so the partitioned loop wins also when the edges are too
  // large to broadcast.
  testConnector_->addTable("large_edges", ROW({"src", "dst"}, BIGINT()))
      ->setStats(
          100'000'000,
          {{"src", {.numDistinct = 6}}, {"dst", {.numDistinct = 6}}});
  for (std::string_view edgesTable : {"edges", "large_edges"}) {
    SCOPED_TRACE(edgesTable);
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        planVelox(connectedComponents(edgesTable)).plan,
        matchFixedPoint(
            startFixedPoint(kLabels)
                .outputState(
                    /*append=*/false,
                    matchScan("vertices").project().shuffle({"vertex"}))
                .hashTableState(
                    matchScan(std::string(edgesTable)).shuffle({"src"}))
                .plan(
                    labels()
                        .filter("changed")
                        .stateHashJoin({"vertex"})
                        .project()
                        .partialAggregation({"dst"}, {"min(label)"})
                        .shuffle({"dst"})
                        .localPartition()
                        .finalAggregation()
                        .hashJoinRight(labels())
                        .project())
                .converge(
                    labels()
                        .partialAggregation({}, {"count_if(changed)"})
                        .gather()
                        .finalAggregation()
                        .project(),
                    {.maxIterations = 100}))
            .gather()
            .build());
  }

  checkSame(
      logicalPlan,
      {makeRowVector({
          makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6}),
          makeFlatVector<int64_t>({1, 1, 1, 4, 4, 6}),
          makeConstant<bool>(false, 6),
      })});
}

// KMeans: each iteration assigns every point to its nearest centroid, moves
// each centroid to the mean of its points, keeps the centroid of an empty
// cluster, and stops once no centroid moves more than a tolerance. Each point
// is one row that computes its distances to all centroids.
TEST_F(FixedPointTest, kMeans) {
  // Two groups of three points, near (0, 0) and near (10, 10).
  testConnector_
      ->addTable("points", ROW({"id", "coords"}, {BIGINT(), ARRAY(DOUBLE())}))
      ->addData(makeRowVector({
          makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6}),
          makeArrayVector<double>(
              {{0, 0}, {0, 1}, {1, 0}, {10, 10}, {10, 11}, {11, 10}}),
      }));
  testConnector_
      ->addTable(
          "seeds", ROW({"cluster", "coords"}, {BIGINT(), ARRAY(DOUBLE())}))
      ->addData(makeRowVector({
          makeFlatVector<int64_t>({1, 2}),
          makeArrayVector<double>({{0, 0}, {5, 5}}),
      }));

  auto context = makeContext();
  static constexpr auto kCentroids = "centroids";

  auto distance = [](std::string_view left, std::string_view right) {
    return fmt::format(
        "array_sum(zip_with({}, {}, (a, b) -> (a - b) * (a - b)))",
        left,
        right);
  };

  auto anchor = lp::PlanBuilder(context).tableScan("seeds").project(
      {"cluster", "coords", /*placeholder*/ "false AS converged"});

  auto state = [&](std::optional<std::string> alias = std::nullopt) {
    auto builder = lp::PlanBuilder(context).fixedPointState(kCentroids, anchor);
    if (alias.has_value()) {
      builder.as(*alias);
    }
    return builder;
  };

  auto step =
      state("old")
          .join(
              lp::PlanBuilder(context)
                  .tableScan("points")
                  .as("p")
                  .crossJoin(
                      state()
                          .aggregate(
                              {},
                              {"array_agg(row_constructor(cluster, coords)) AS centers"})
                          .as("c"))
                  // The nearest center, the lowest cluster on a tie.
                  .project(
                      {fmt::format(
                           "array_min_by(c.centers, center -> row_constructor({}, center[1]))[1] AS cluster",
                           distance("p.coords", "center[2]")),
                       "p.coords AS coords"})
                  .aggregate(
                      {"cluster"},
                      {"vector_sum(coords) AS total", "count(*) AS num_points"})
                  .project({
                      "cluster",
                      "transform(total, v -> v / cast(num_points AS DOUBLE)) AS coords",
                  })
                  .as("new"),
              "old.cluster = new.cluster",
              lp::JoinType::kLeft)
          .project({
              "old.cluster AS cluster",
              "coalesce(new.coords, old.coords) AS coords",
              "old.coords AS previous",
          })
          .project(
              {"cluster",
               "coords",
               fmt::format(
                   "{} < 0.0001 AS converged",
                   distance("coords", "previous"))});
  auto logicalPlan =
      anchor
          .iterateUntil(
              kCentroids,
              step,
              // Stops at the first centroid that still moves.
              lp::PlanBuilder(context)
                  .values(ROW({}), std::vector<Variant>{Variant::row({})})
                  .project({lp::Call(
                      "is_null",
                      lp::Subquery(
                          state()
                              .filter("NOT converged")
                              .limit(1)
                              .project({"converged"})
                              .planNode()))}),
              /*maxIterations=*/20,
              /*failAtBound=*/false,
              /*keyColumns=*/{"cluster"})
          .build();

  auto centroids = [&] {
    return matchDelta(kCentroids, {"cluster", "coords", "converged"});
  };
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(logicalPlan),
      matchFixedPoint(
          startFixedPoint(kCentroids)
              .outputState(/*append=*/false, matchScan("seeds").project())
              .vectorState("points", matchScan("points"))
              .plan(
                  centroids()
                      .hashJoinLeft(
                          core::PlanMatcherBuilder()
                              .stateSource("points", /*delta=*/false)
                              .nestedLoopJoin(
                                  centroids().project().singleAggregation())
                              .project()
                              .singleAggregation()
                              .project())
                      .project())
              .converge(
                  centroids()
                      .filter("NOT converged")
                      .finalLimit(0, 1)
                      .project()
                      .enforceSingleRow()
                      .project(),
                  {.maxIterations = 20, .failAtBound = false}))
          .build());

  // With several workers, every worker holds all the centroids. Each assigns
  // the points it scanned and sums them per cluster; the sums are shuffled on
  // the cluster, and the new means are broadcast to every worker. Worker 0
  // decides when to stop and emits the result. Each step runs with all of a
  // worker's drivers, so its aggregations split around local exchanges.
  AXIOM_ASSERT_DISTRIBUTED_PLAN(
      planVelox(logicalPlan).plan,
      matchFixedPoint(
          startFixedPoint(kCentroids)
              .outputState(
                  /*append=*/false, matchScan("seeds").project().broadcast())
              .vectorState("points", matchScan("points"))
              .plan(
                  core::PlanMatcherBuilder()
                      .stateSource("points", /*delta=*/false)
                      .nestedLoopJoin(
                          centroids()
                              .project()
                              .partialAggregation()
                              .localGather()
                              .finalAggregation())
                      .project()
                      .partialAggregation()
                      .shuffle()
                      .localPartition()
                      .finalAggregation()
                      .project()
                      .broadcast()
                      .hashJoinRight(centroids())
                      .project())
              .convergeOnWorkerZero(
                  centroids()
                      .filter("NOT converged")
                      .finalLimit(0, 1)
                      .project()
                      .enforceSingleRow()
                      .project(),
                  {.maxIterations = 20, .failAtBound = false})
              .outputOnWorkerZero())
          .gather()
          .build());

  // The first iteration moves both centroids to their clusters' means; the
  // second moves neither.
  checkSame(
      logicalPlan,
      {makeRowVector({
          makeFlatVector<int64_t>({1, 2}),
          makeArrayVector<double>({{1.0 / 3, 1.0 / 3}, {31.0 / 3, 31.0 / 3}}),
          makeConstant(true, 2),
      })});
}

// A union inside a loop's step whose state every worker holds: a union of
// state reads is on every worker too, and a union of the state with rows that
// are not has those rows broadcast. Each point counts once per row of the
// union that matches its cluster.
TEST_F(FixedPointTest, unionInReplicatedStep) {
  testConnector_->addTable("points", ROW("cluster", BIGINT()))
      ->addData(makeRowVector({makeFlatVector<int64_t>({1, 1, 1, 2, 2, 2})}));
  testConnector_->addTable("seeds", ROW({"cluster", "n"}, BIGINT()))
      ->addData(makeRowVector({
          makeFlatVector<int64_t>({1, 2}),
          makeFlatVector<int64_t>({0, 0}),
      }));
  testConnector_->addTable("extra", ROW("cluster", BIGINT()))
      ->addData(makeRowVector({makeFlatVector<int64_t>({1})}));

  static constexpr auto kCounts = "counts";
  // Counts the points of each cluster against the state's clusters united
  // with the state's clusters again, or with "extra".
  auto countMatches = [&](bool unionWithState) {
    auto context = makeContext();
    auto anchor = lp::PlanBuilder(context).tableScan("seeds");
    auto state = [&](std::optional<std::string> alias = std::nullopt) {
      auto builder = lp::PlanBuilder(context).fixedPointState(kCounts, anchor);
      if (alias.has_value()) {
        builder.as(*alias);
      }
      return builder;
    };
    auto other = unionWithState ? state().project({"cluster"})
                                : lp::PlanBuilder(context).tableScan("extra");
    auto step =
        state("old")
            .join(
                lp::PlanBuilder(context)
                    .tableScan("points")
                    .as("p")
                    .join(
                        state().project({"cluster"}).unionAll(other).as("u"),
                        "p.cluster = u.cluster",
                        lp::JoinType::kInner)
                    .aggregate({"p.cluster"}, {"count(*) AS n"})
                    .as("new"),
                "old.cluster = new.cluster",
                lp::JoinType::kLeft)
            .project({"old.cluster AS cluster", "coalesce(new.n, 0) AS n"});
    return anchor
        .iterate(kCounts, step, /*numIterations=*/1, /*keyColumns=*/{"cluster"})
        .build();
  };

  // Each worker builds the points it scanned into a hash table once. The
  // state and "extra" are broadcast to every worker.
  for (const bool unionWithState : {true, false}) {
    auto loop =
        startFixedPoint(kCounts)
            .outputState(/*append=*/false, matchScan("seeds").broadcast())
            .hashTableState(matchScan("points"))
            .outputOnWorkerZero();
    if (!unionWithState) {
      loop.vectorState("extra", matchScan("extra").broadcast());
    }
    AXIOM_ASSERT_DISTRIBUTED_PLAN(
        planVelox(countMatches(unionWithState)).plan,
        matchFixedPoint(loop).gather().build());
  }

  checkSame(
      countMatches(true),
      {makeRowVector({
          makeFlatVector<int64_t>({1, 2}),
          makeFlatVector<int64_t>({6, 6}),
      })});
  checkSame(
      countMatches(false),
      {makeRowVector({
          makeFlatVector<int64_t>({1, 2}),
          makeFlatVector<int64_t>({6, 3}),
      })});
}

} // namespace
} // namespace facebook::axiom::optimizer
