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

core::FixedPointMatch matchFixedPoint(const std::string& name) {
  return core::FixedPointMatch(name);
}

core::PlanMatcherBuilder matchDelta(
    const std::string& name,
    const std::vector<std::optional<std::string>>& columns) {
  return core::PlanMatcherBuilder()
      .stateSource(name, /*delta=*/true)
      .aliases(columns);
}

class FixedPointTest : public test::QueryTestBase {
 protected:
  lp::PlanBuilder singleRow(const std::string& name, int64_t value) {
    return lp::PlanBuilder(context_).values(
        ROW(name, BIGINT()), std::vector<Variant>{Variant::row({value})});
  }

  lp::PlanBuilder::Context context_{
      lp::PlanBuilder::Context::forConnectorlessPlan()};
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
      core::PlanMatcherBuilder()
          .fixedPoint(matchFixedPoint("counter")
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
      core::PlanMatcherBuilder()
          .fixedPoint(matchFixedPoint("r")
                          .outputState(
                              /*append=*/true,
                              matchScan("t")
                                  .hashJoinInner(
                                      matchScan("u"), {.keys = {{"t_k = u_k"}}})
                                  .project({"t_k", "t_k as u_k"}))
                          .plan(matchDelta("r", {"t_k", "u_k"})
                                    .filter("t_k < 10")
                                    .project({"t_k + 1", "u_k + 1"})))
          .build());
}

// A step's join reads the table through a hash table built once for the loop.
TEST_F(FixedPointTest, stepJoin) {
  testConnector_->addTable("e", ROW({"src", "dst"}, BIGINT()));

  auto logicalPlan = parseSelect(
      "WITH RECURSIVE r(n) AS ("
      "     SELECT CAST(1 AS BIGINT) "
      "     UNION ALL "
      "     SELECT dst FROM r JOIN e ON src = n"
      ") "
      "SELECT n FROM r");

  auto matcher =
      core::PlanMatcherBuilder()
          .fixedPoint(matchFixedPoint("r")
                          .outputState(/*append=*/true, matchValues())
                          .hashTableState(matchScan("e"))
                          .plan(matchDelta("r", {"n"})
                                    .stateHashJoin({"n"})
                                    .project({"dst"})))
          .build();
  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
}

TEST_F(FixedPointTest, singleDriverStep) {
  auto counter = singleRow("n", 1);
  auto recursiveStep = lp::PlanBuilder(context_)
                           .recursiveRef("counter", counter)
                           .filter("n < 10")
                           .aggregate({}, {"count(*) as n"})
                           .planNode();
  auto logicalPlan = counter.fixedPoint("counter", recursiveStep)
                         .aggregate({}, {"count(*)"})
                         .build();

  auto matcher =
      core::PlanMatcherBuilder()
          .fixedPoint(matchFixedPoint("counter")
                          .outputState(/*append=*/true, matchValues())
                          .plan(matchDelta("counter", {"n"})
                                    .filter("n < 10")
                                    .singleAggregation({}, {"count(*)"}))
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

  auto matcher =
      core::PlanMatcherBuilder().fixedPoint().finalLimit(0, 5).build();
  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
}

TEST_F(FixedPointTest, multipleWorkersRejected) {
  auto logicalPlan = parseSelect(
      "WITH RECURSIVE counter(n) AS ("
      "VALUES 1 UNION ALL SELECT n + 1 FROM counter WHERE n < 10) "
      "SELECT * FROM counter");

  VELOX_ASSERT_THROW(
      planVelox(
          logicalPlan, {.maxRemotePartitions = 2, .maxLocalPartitions = 1}),
      "Distributed FixedPoint execution is not yet implemented");
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

  auto matcher = core::PlanMatcherBuilder()
                     .fixedPoint(matchFixedPoint("t")
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
      "VALUES 1 UNION ALL SELECT n + 1 FROM counter WHERE n < 20) "
      "SELECT n FROM counter WHERE n > 15");

  auto matcher =
      core::PlanMatcherBuilder()
          .fixedPoint(matchFixedPoint("counter")
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
      "VALUES (1, 7) UNION ALL "
      "SELECT n + 1, carried FROM counter WHERE n < 10) "
      "SELECT n FROM counter");

  auto matcher =
      core::PlanMatcherBuilder()
          .fixedPoint(
              matchFixedPoint("counter")
                  .outputState(/*append=*/true, matchValues())
                  .plan(matchDelta("counter", {"n", "carried"})
                            .filter("n < 10")
                            .project({"n + 1", "carried"}))
                  .convergeOnEmpty({.stateColumns = {{"n", "carried"}}}))
          .project({"n"})
          .build();
  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
}

TEST_F(FixedPointTest, orphanRecursiveReference) {
  auto counter = singleRow("n", 1);
  auto orphan =
      lp::PlanBuilder(context_).recursiveRef("counter", counter).build();

  VELOX_ASSERT_THROW(
      toSingleNodePlan(orphan),
      "RecursiveReferenceNode outside any enclosing FixedPoint");
}

TEST_F(FixedPointTest, siblingRecursions) {
  auto logicalPlan = parseSelect(
      "WITH RECURSIVE "
      "r1(a) AS (VALUES 0 UNION ALL SELECT a + 1 FROM r1 WHERE a < 3), "
      "r2(b) AS (SELECT a FROM r1 UNION ALL "
      "SELECT b + 10 FROM r2 WHERE b < 30) "
      "SELECT b FROM r2");

  auto matcher =
      core::PlanMatcherBuilder()
          .fixedPoint(matchFixedPoint("r2")
                          .outputState(
                              /*append=*/true,
                              core::PlanMatcherBuilder().fixedPoint(
                                  matchFixedPoint("r1")
                                      .outputState(
                                          /*append=*/true, matchValues())
                                      .plan(matchDelta("r1", {"a"})
                                                .filter("a < 3")
                                                .project({"a + 1"}))))
                          .plan(matchDelta("r2", {"b"})
                                    .filter("b < 30")
                                    .project({"b + 10"})))
          .project({"a as b"})
          .build();

  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
}

TEST_F(FixedPointTest, twoRecursiveReferencesRejected) {
  auto logicalPlan = parseSelect(
      "WITH RECURSIVE reach(src, dst) AS ("
      "VALUES (1, 2) UNION ALL "
      "SELECT a.src, b.dst FROM reach a JOIN reach b ON a.dst = b.src) "
      "SELECT src, dst FROM reach");

  VELOX_ASSERT_THROW(
      toSingleNodePlan(logicalPlan),
      "Optimizer v2 supports exactly one RecursiveReferenceNode per FixedPoint step");
}

TEST_F(FixedPointTest, twoRecursionsInUnionRejected) {
  auto logicalPlan = parseSelect(
      "WITH RECURSIVE r(n) AS ("
      "VALUES 1 UNION ALL SELECT n + 1 FROM r WHERE n < 10) "
      "SELECT n FROM r UNION ALL SELECT n FROM r");

  VELOX_ASSERT_THROW(
      toSingleNodePlan(logicalPlan),
      "Multiple FixedPoint nodes in one execution fragment are not yet supported");
}

TEST_F(FixedPointTest, twoRecursionsInJoinRejected) {
  auto logicalPlan = parseSelect(
      "WITH RECURSIVE r(n) AS ("
      "VALUES 1 UNION ALL SELECT n + 1 FROM r WHERE n < 10) "
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
      "VALUES 1 UNION ALL SELECT n + 1 FROM r WHERE n < 10) "
      "SELECT r.n + 1 FROM r JOIN u ON r.n = u.n "
      "WHERE r.n > 0");

  auto matcher =
      core::PlanMatcherBuilder()
          .fixedPoint(matchFixedPoint("r")
                          .outputState(/*append=*/true, matchValues())
                          .plan(matchDelta("r", {"n"})
                                    .filter("n < 10")
                                    .project({"n + 1"})))
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
                       .recursiveRef("inner", inner)
                       .project({"n + 1 as n"})
                       .planNode();
  inner.fixedPoint("inner", innerBody);

  auto outer = singleRow("n", 1);
  auto outerBody = lp::PlanBuilder(context_)
                       .recursiveRef("outer", outer)
                       .unionAll(inner)
                       .planNode();
  auto plan = outer.fixedPoint("outer", outerBody).build();

  VELOX_ASSERT_THROW(
      toSingleNodePlan(plan),
      "Nested FixedPoint translation is not yet implemented");
}

} // namespace
} // namespace facebook::axiom::optimizer
