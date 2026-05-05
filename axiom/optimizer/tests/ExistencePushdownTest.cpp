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

#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;

// Tests for the existence pushdown optimization described in
// docs/ExistencePushdown.md.
class ExistencePushdownTest : public test::QueryTestBase {
 protected:
  void SetUp() override {
    test::QueryTestBase::SetUp();

    // Small table — the pushed table.
    testConnector_->addTable("t", ROW({"a", "b", "c"}, BIGINT()))
        ->setStats(
            100,
            {{"a", {.min = 1LL, .max = 100LL, .numDistinct = 100}},
             {"b", {.min = 1LL, .max = 100LL, .numDistinct = 100}},
             {"c", {.min = 1LL, .max = 100LL, .numDistinct = 100}}});
    // Large table — inside the GROUP BY subquery.
    testConnector_->addTable("u", ROW({"x", "y", "z"}, BIGINT()))
        ->setStats(
            10'000,
            {{"x", {.min = 1LL, .max = 10'000LL, .numDistinct = 10'000}},
             {"y", {.min = 1LL, .max = 10'000LL, .numDistinct = 10'000}},
             {"z", {.min = 1LL, .max = 10'000LL, .numDistinct = 10'000}}});
    // Another small table.
    testConnector_->addTable("r", ROW({"a", "b"}, BIGINT()))
        ->setStats(
            100,
            {{"a", {.min = 1LL, .max = 100LL, .numDistinct = 100}},
             {"b", {.min = 1LL, .max = 100LL, .numDistinct = 100}}});
    // Another small table for chain joins.
    testConnector_->addTable("s", ROW({"a", "b"}, BIGINT()))
        ->setStats(
            100,
            {{"a", {.min = 1LL, .max = 100LL, .numDistinct = 100}},
             {"b", {.min = 1LL, .max = 100LL, .numDistinct = 100}}});
    // Table for DerivedTable-as-other.
    testConnector_->addTable("v", ROW({"a", "b"}, BIGINT()))
        ->setStats(
            100,
            {{"a", {.min = 1LL, .max = 100LL, .numDistinct = 100}},
             {"b", {.min = 1LL, .max = 100LL, .numDistinct = 100}}});
  }

  using QueryTestBase::toSingleNodePlan;

  velox::core::PlanNodePtr toSingleNodePlan(const std::string& sql) {
    return QueryTestBase::toSingleNodePlan(parseSelect(sql, kTestConnectorId));
  }
};

// --- Yes rows: pushdown should fire ---

// TODO: For each test that verifies pushdown, add a variant that does NOT
// trigger pushdown — e.g., different stats (pushed table is larger than the
// grouped table), or join on aggregate key instead of grouping key. This
// ensures the optimizer only pushes when it's beneficial.

// Row 1, 4, 8, 13.
TEST_F(ExistencePushdownTest, innerJoinGroupBy) {
  auto logicalPlan = parseSelect(
      "SELECT t.a, dt.x, dt.cnt "
      "FROM t "
      "JOIN (SELECT x, COUNT(*) AS cnt FROM u GROUP BY x) dt ON t.a = dt.x "
      "WHERE t.b < 100",
      kTestConnectorId);

  // Single-node plan.
  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher =
      matchScan("u")
          .hashJoin(
              matchScan("t").filter("b < 100").build(),
              core::JoinType::kLeftSemiFilter)
          .singleAggregation({"x"}, {"count(*) as cnt"})
          .hashJoin(
              matchScan("t").filter("b < 100").build(), core::JoinType::kInner)
          .project()
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // Distributed plan.
  //
  // TODO: The shuffle before aggregation is suboptimal. See the TODO in the
  // semiJoin test.
  auto distributedPlan = planVelox(logicalPlan);

  auto distributedMatcher =
      matchScan("u")
          .hashJoin(
              matchScan("t").filter("b < 100").broadcast().build(),
              core::JoinType::kLeftSemiFilter)
          .shuffle({"x"})
          .localPartition({"x"})
          .singleAggregation({"x"}, {"count(*) as cnt"})
          .hashJoin(
              matchScan("t").filter("b < 100").shuffle({"a"}).build(),
              core::JoinType::kInner)
          .project()
          .gather()
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, distributedMatcher);
}

// Row 5.
TEST_F(ExistencePushdownTest, semiJoin) {
  auto logicalPlan = parseSelect(
      "SELECT * FROM t "
      "WHERE t.b < 100 "
      "  AND a IN (SELECT x FROM u GROUP BY x HAVING COUNT(*) > 1)",
      kTestConnectorId);

  // Single-node plan.
  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchScan("t")
                     .filter("b < 100")
                     .hashJoin(
                         matchScan("u")
                             .hashJoin(
                                 matchScan("t").filter("b < 100").build(),
                                 core::JoinType::kLeftSemiFilter)
                             .singleAggregation({"x"}, {"count(*) as cnt"})
                             .filter("cnt > 1")
                             .project()
                             .build(),
                         core::JoinType::kLeftSemiFilter)
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // Distributed plan.
  //
  // TODO: The shuffle before aggregation is suboptimal given the existence
  // pushdown reduces u from ~10K to ~100 rows. Gather + single aggregation
  // would be cheaper. The aggregation planner always repartitions by grouping
  // keys — it does not consider gather as an alternative when the input
  // cardinality is small.
  auto distributedPlan = planVelox(logicalPlan);

  auto distributedMatcher =
      matchScan("t")
          .filter("b < 100")
          .hashJoin(
              matchScan("u")
                  .hashJoin(
                      matchScan("t").filter("b < 100").broadcast().build(),
                      core::JoinType::kLeftSemiFilter)
                  .shuffle({"x"})
                  .localPartition({"x"})
                  .singleAggregation({"x"}, {"count(*) as cnt"})
                  .filter("cnt > 1")
                  .project()
                  .broadcast()
                  .build(),
              core::JoinType::kLeftSemiFilter)
          .gather()
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, distributedMatcher);
}

// Row 6.
TEST_F(ExistencePushdownTest, leftJoinDtIsOptional) {
  auto logicalPlan = parseSelect(
      "SELECT t.a, dt.x, dt.cnt "
      "FROM t "
      "LEFT JOIN (SELECT x, COUNT(*) AS cnt FROM u GROUP BY x) dt "
      "  ON t.a = dt.x "
      "WHERE t.b < 100",
      kTestConnectorId);

  // Single-node plan.
  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchScan("t")
                     .filter("b < 100")
                     .hashJoin(
                         matchScan("u")
                             .hashJoin(
                                 matchScan("t").filter("b < 100").build(),
                                 core::JoinType::kLeftSemiFilter)
                             .singleAggregation({"x"}, {"count(*) as cnt"})
                             .build(),
                         core::JoinType::kLeft)
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // Distributed plan.
  auto distributedPlan = planVelox(logicalPlan);

  auto distributedMatcher =
      matchScan("u")
          .hashJoin(
              matchScan("t").filter("b < 100").broadcast().build(),
              core::JoinType::kLeftSemiFilter)
          .shuffle({"x"})
          .localPartition({"x"})
          .singleAggregation({"x"}, {"count(*) as cnt"})
          .hashJoin(
              matchScan("t").filter("b < 100").shuffle({"a"}).build(),
              core::JoinType::kRight)
          .gather()
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, distributedMatcher);
}

// Row 9: pushed table is a DerivedTable (subquery).
// TODO: A more optimal plan would push dt2 as existence inside dt1's
// aggregation, filtering u before GROUP BY.
TEST_F(ExistencePushdownTest, otherIsDerivedTable) {
  auto logicalPlan = parseSelect(
      "SELECT dt1.x, dt1.cnt, dt2.a "
      "FROM (SELECT x, COUNT(*) AS cnt FROM u GROUP BY x) dt1 "
      "JOIN (SELECT DISTINCT a FROM v WHERE a < 10) dt2 ON dt1.x = dt2.a",
      kTestConnectorId);

  // Single-node plan.
  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchScan("u")
                     .singleAggregation({"x"}, {"count(*) as cnt"})
                     .hashJoin(
                         matchScan("v")
                             .filter("a < 10")
                             .singleAggregation({"a"}, {})
                             .build(),
                         core::JoinType::kInner)
                     .project()
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // Distributed plan.
  auto distributedPlan = planVelox(logicalPlan);

  auto distributedMatcher = matchScan("u")
                                .shuffle({"x"})
                                .localPartition({"x"})
                                .singleAggregation({"x"}, {"count(*) as cnt"})
                                .hashJoin(
                                    matchScan("v")
                                        .filter("a < 10")
                                        .partialAggregation({"a"}, {})
                                        .shuffle({"a"})
                                        .localPartition({"a"})
                                        .finalAggregation({"a"}, {})
                                        .build(),
                                    core::JoinType::kInner)
                                .project()
                                .gather()
                                .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, distributedMatcher);
}

// Row 10.
TEST_F(ExistencePushdownTest, chainJoin) {
  auto logicalPlan = parseSelect(
      "SELECT dt.cnt "
      "FROM (SELECT x, COUNT(*) AS cnt FROM u GROUP BY x) dt "
      "JOIN r ON dt.x = r.a "
      "JOIN s ON r.b = s.a "
      "WHERE r.b < 100",
      kTestConnectorId);

  // Single-node plan.
  auto plan = toSingleNodePlan(logicalPlan);

  // r + s wrapped into a chainDt, pushed as existence inside the aggregation.
  // Transitive predicate inference also pushes 'a_0 < 100' on s.
  auto matcher =
      matchScan("u")
          .hashJoin(
              matchScan("r")
                  .filter("b < 100")
                  .hashJoin(
                      matchScan("s").filter("a_0 < 100").build(),
                      core::JoinType::kInner)
                  .project()
                  .build(),
              core::JoinType::kLeftSemiFilter)
          .singleAggregation({"x"}, {"count(*) as cnt"})
          .hashJoin(
              matchScan("r").filter("b < 100").build(), core::JoinType::kInner)
          .hashJoin(
              matchScan("s").filter("a_0 < 100").build(),
              core::JoinType::kInner)
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // Distributed plan.
  auto distributedPlan = planVelox(logicalPlan);

  auto distributedMatcher =
      matchScan("r")
          .filter("b < 100")
          .hashJoin(
              matchScan("s").filter("a_0 < 100").broadcast().build(),
              core::JoinType::kInner)
          .hashJoin(
              matchScan("u")
                  .hashJoin(
                      matchScan("r")
                          .filter("b < 100")
                          .hashJoin(
                              matchScan("s")
                                  .filter("a_0 < 100")
                                  .broadcast()
                                  .build(),
                              core::JoinType::kInner)
                          .project()
                          .broadcast()
                          .build(),
                      core::JoinType::kLeftSemiFilter)
                  .shuffle({"x"})
                  .localPartition({"x"})
                  .singleAggregation({"x"}, {"count(*) as cnt"})
                  .broadcast()
                  .build(),
              core::JoinType::kInner)
          .gather()
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, distributedMatcher);
}

// Row 11: r and s pushed independently on different grouping keys.
TEST_F(ExistencePushdownTest, multipleTables) {
  auto logicalPlan = parseSelect(
      "SELECT r.a, s.a, dt.x, dt.y "
      "FROM r "
      "JOIN (SELECT x, y, COUNT(*) AS cnt FROM u GROUP BY x, y) dt "
      "  ON r.a = dt.x "
      "JOIN s ON s.a = dt.y "
      "WHERE r.b < 100",
      kTestConnectorId);

  // Single-node plan.
  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher =
      matchScan("s")
          .hashJoin(
              matchScan("u")
                  .hashJoin(
                      matchScan("r").filter("b < 100").build(),
                      core::JoinType::kLeftSemiFilter)
                  .hashJoin(
                      matchScan("s").build(), core::JoinType::kLeftSemiFilter)
                  .singleAggregation({"x", "y"}, {})
                  .build(),
              core::JoinType::kInner)
          .hashJoin(
              matchScan("r").filter("b < 100").build(), core::JoinType::kInner)
          .project()
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // Distributed plan.
  auto distributedPlan = planVelox(logicalPlan);

  auto distributedMatcher =
      matchScan("s")
          .hashJoin(
              matchScan("u")
                  .hashJoin(
                      matchScan("r").filter("b < 100").broadcast().build(),
                      core::JoinType::kLeftSemiFilter)
                  .hashJoin(
                      matchScan("s").broadcast().build(),
                      core::JoinType::kLeftSemiFilter)
                  .shuffle({"x", "y"})
                  .localPartition({"x", "y"})
                  .singleAggregation({"x", "y"}, {})
                  .broadcast()
                  .build(),
              core::JoinType::kInner)
          .hashJoin(
              matchScan("r").filter("b < 100").broadcast().build(),
              core::JoinType::kInner)
          .project()
          .gather()
          .project()
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, distributedMatcher);
}

// Row 12.
TEST_F(ExistencePushdownTest, partialPush) {
  auto logicalPlan = parseSelect(
      "SELECT t.a, r.a, dt.x, dt.cnt "
      "FROM t "
      "JOIN (SELECT x, COUNT(*) AS cnt FROM u GROUP BY x) dt ON t.a = dt.x "
      "JOIN r ON dt.cnt = r.a "
      "WHERE t.b < 100",
      kTestConnectorId);

  // Single-node plan.
  auto plan = toSingleNodePlan(logicalPlan);

  // No existence pushdown: dt.cnt (an aggregate result, not a grouping key)
  // is used in the join with r, so the aggregation cannot be deferred.
  auto matcher =
      matchScan("u")
          .singleAggregation({"x"}, {"count(*) as cnt"})
          .hashJoin(
              matchScan("t").filter("b < 100").build(), core::JoinType::kInner)
          .hashJoin(matchScan("r").build(), core::JoinType::kInner)
          .project()
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // Distributed plan.
  auto distributedPlan = planVelox(logicalPlan);

  auto distributedMatcher =
      matchScan("u")
          .shuffle({"x"})
          .localPartition({"x"})
          .singleAggregation({"x"}, {"count(*) as cnt"})
          .hashJoin(
              matchScan("t").filter("b < 100").shuffle({"a"}).build(),
              core::JoinType::kInner)
          .hashJoin(matchScan("r").broadcast().build(), core::JoinType::kInner)
          .project()
          .gather()
          .project()
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, distributedMatcher);
}

// Row 14.
TEST_F(ExistencePushdownTest, distinctSubquery) {
  auto logicalPlan = parseSelect(
      "SELECT t.a, dt.x "
      "FROM t "
      "JOIN (SELECT DISTINCT x FROM u) dt ON t.a = dt.x "
      "WHERE t.b < 100",
      kTestConnectorId);

  // Single-node plan.
  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchScan("t")
                     .filter("b < 100")
                     .hashJoin(
                         matchScan("u")
                             .hashJoin(
                                 matchScan("t").filter("b < 100").build(),
                                 core::JoinType::kLeftSemiFilter)
                             .singleAggregation({"x"}, {})
                             .build(),
                         core::JoinType::kInner)
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // Distributed plan.
  //
  // TODO: The shuffle before aggregation is suboptimal. See the TODO in the
  // semiJoin test.
  auto distributedPlan = planVelox(logicalPlan);

  auto distributedMatcher =
      matchScan("t")
          .filter("b < 100")
          .hashJoin(
              matchScan("u")
                  .hashJoin(
                      matchScan("t").filter("b < 100").broadcast().build(),
                      core::JoinType::kLeftSemiFilter)
                  .partialAggregation({"x"}, {})
                  .shuffle({"x"})
                  .localPartition({"x"})
                  .finalAggregation({"x"}, {})
                  .broadcast()
                  .build(),
              core::JoinType::kInner)
          .gather()
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, distributedMatcher);
}

// Row 2.
TEST_F(ExistencePushdownTest, multiKeyJoin) {
  auto logicalPlan = parseSelect(
      "SELECT t.a, t.b, dt.x, dt.y, dt.cnt "
      "FROM t "
      "JOIN (SELECT x, y, COUNT(*) AS cnt FROM u GROUP BY x, y) dt "
      "  ON t.a = dt.x AND t.b = dt.y "
      "WHERE t.c < 100",
      kTestConnectorId);

  auto plan = toSingleNodePlan(logicalPlan);

  // Multi-key join existence is pushed down into plan.
  auto matcher =
      matchScan("u")
          .hashJoin(
              matchScan("t").filter("c < 100").build(),
              core::JoinType::kLeftSemiFilter)
          .singleAggregation({"x", "y"}, {"count(*) as cnt"})
          .hashJoin(
              matchScan("t").filter("c < 100").build(), core::JoinType::kInner)
          .project()
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // Distributed plan.
  auto distributedPlan = planVelox(logicalPlan);

  auto distributedMatcher =
      matchScan("u")
          .hashJoin(
              matchScan("t").filter("c < 100").broadcast().build(),
              core::JoinType::kLeftSemiFilter)
          .shuffle({"x", "y"})
          .localPartition({"x", "y"})
          .singleAggregation({"x", "y"}, {"count(*) as cnt"})
          .hashJoin(
              matchScan("t").filter("c < 100").shuffle({"a", "b"}).build(),
              core::JoinType::kInner)
          .project()
          .gather()
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, distributedMatcher);
}

// Row 3: pushdown fires even with non-equality filter — equality part is
// pushed, filter stays outside.
TEST_F(ExistencePushdownTest, joinWithFilter) {
  auto logicalPlan = parseSelect(
      "SELECT t.a, dt.x, dt.cnt "
      "FROM t "
      "JOIN (SELECT x, COUNT(*) AS cnt FROM u GROUP BY x) dt "
      "  ON t.a = dt.x AND dt.x > t.b "
      "WHERE t.c < 100",
      kTestConnectorId);

  // Single-node plan.
  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher =
      matchScan("u")
          .hashJoin(
              matchScan("t").filter("c < 100").build(),
              core::JoinType::kLeftSemiFilter)
          .singleAggregation({"x"}, {"count(*) as cnt"})
          .hashJoin(
              matchScan("t").filter("c < 100").build(), core::JoinType::kInner)
          .filter("b < x")
          .project()
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // Distributed plan.
  auto distributedPlan = planVelox(logicalPlan);

  auto distributedMatcher =
      matchScan("t")
          .filter("c < 100")
          .hashJoin(
              matchScan("u")
                  .hashJoin(
                      matchScan("t").filter("c < 100").broadcast().build(),
                      core::JoinType::kLeftSemiFilter)
                  .shuffle({"x"})
                  .localPartition({"x"})
                  .singleAggregation({"x"}, {"count(*) as cnt"})
                  .broadcast()
                  .build(),
              core::JoinType::kInner)
          .filter("b < x")
          .project()
          .gather()
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, distributedMatcher);
}

// Row 7: no pushdown — DT is the preserved side of LEFT JOIN.
TEST_F(ExistencePushdownTest, leftJoinDtIsPreserved) {
  auto logicalPlan = parseSelect(
      "SELECT dt.x, dt.cnt, t.a "
      "FROM (SELECT x, COUNT(*) AS cnt FROM u GROUP BY x) dt "
      "LEFT JOIN t ON dt.x = t.a",
      kTestConnectorId);

  // Single-node plan.
  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchScan("u")
                     .singleAggregation({"x"}, {"count(*) as cnt"})
                     .hashJoin(matchScan("t").build(), core::JoinType::kLeft)
                     .project()
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // Distributed plan.
  auto distributedPlan = planVelox(logicalPlan);

  auto distributedMatcher =
      matchScan("u")
          .shuffle({"x"})
          .localPartition({"x"})
          .singleAggregation({"x"}, {"count(*) as cnt"})
          .hashJoin(
              matchScan("t").shuffle({"a"}).build(), core::JoinType::kLeft)
          .project()
          .gather()
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, distributedMatcher);
}

// Row 16: join key matches window PARTITION BY — pushdown is valid.
// Correct plan has semijoin below the window, original join above.
TEST_F(ExistencePushdownTest, windowSubquery) {
  auto logicalPlan = parseSelect(
      "SELECT t.a, dt.x, dt.rn "
      "FROM t "
      "JOIN ("
      "  SELECT x, ROW_NUMBER() OVER (PARTITION BY x ORDER BY y) AS rn FROM u"
      ") dt ON t.a = dt.x "
      "WHERE t.b < 100",
      kTestConnectorId);

  // Single-node plan.
  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher =
      matchScan("u")
          .hashJoin(
              matchScan("t").filter("b < 100").build(),
              core::JoinType::kLeftSemiFilter)
          .window({"row_number() OVER (PARTITION BY x ORDER BY y)"})
          .project()
          .hashJoin(
              matchScan("t").filter("b < 100").build(), core::JoinType::kInner)
          .project()
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // Distributed plan.
  auto distributedPlan = planVelox(logicalPlan);

  auto distributedMatcher =
      matchScan("u")
          .hashJoin(
              matchScan("t").filter("b < 100").broadcast().build(),
              core::JoinType::kLeftSemiFilter)
          .shuffle({"x"})
          .localPartition({"x"})
          .window({"row_number() OVER (PARTITION BY x ORDER BY y)"})
          .project()
          .hashJoin(
              matchScan("t").filter("b < 100").shuffle({"a"}).build(),
              core::JoinType::kInner)
          .project()
          .gather()
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, distributedMatcher);
}

// No pushdown: LIMIT blocks it.
TEST_F(ExistencePushdownTest, limitOnFirstDt) {
  auto logicalPlan = parseSelect(
      "SELECT t.a, dt.x "
      "FROM t "
      "JOIN (SELECT x, COUNT(*) AS cnt FROM u GROUP BY x LIMIT 10) dt "
      "  ON t.a = dt.x "
      "WHERE t.b < 100",
      kTestConnectorId);

  // Single-node plan.
  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchScan("t")
                     .filter("b < 100")
                     .hashJoin(
                         matchScan("u")
                             .singleAggregation({"x"}, {})
                             .finalLimit(0, 10)
                             .build(),
                         core::JoinType::kInner)
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // Distributed plan.
  auto distributedPlan = planVelox(logicalPlan);

  auto distributedMatcher = matchScan("t")
                                .filter("b < 100")
                                .hashJoin(
                                    matchScan("u")
                                        .partialAggregation({"x"}, {})
                                        .shuffle({"x"})
                                        .localPartition({"x"})
                                        .finalAggregation({"x"}, {})
                                        .distributedLimit(0, 10)
                                        .broadcast()
                                        .build(),
                                    core::JoinType::kInner)
                                .gather()
                                .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, distributedMatcher);
}

TEST_F(ExistencePushdownTest, orderByOnFirstDt) {
  auto logicalPlan = parseSelect(
      "SELECT t.a, dt.x "
      "FROM t "
      "JOIN (SELECT x, COUNT(*) AS cnt FROM u GROUP BY x ORDER BY cnt) dt "
      "  ON t.a = dt.x "
      "WHERE t.b < 100",
      kTestConnectorId);

  // Single-node plan.
  auto plan = toSingleNodePlan(logicalPlan);

  // No pushdown: ORDER BY blocks it.
  //
  // TODO: ORDER BY inside a FROM-clause subquery without LIMIT is semantically
  // meaningless — the parent join discards the order. Eliminating it early
  // (e.g., via dropOrderBy() in ToGraph) would unblock existence pushdown here
  // and simplify the distributed plan (removing shuffleMerge). The check is in
  // DerivedTable::validatePushdown (DerivedTable.cpp), which rejects subqueries
  // with hasOrderBy().
  auto matcher =
      matchScan("u")
          .singleAggregation({"x"}, {"count(*) as cnt"})
          .orderBy({"cnt"})
          .project()
          .hashJoin(
              matchScan("t").filter("b < 100").build(), core::JoinType::kInner)
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // Distributed plan.
  auto distributedPlan = planVelox(logicalPlan);

  auto distributedMatcher =
      matchScan("u")
          .shuffle({"x"})
          .localPartition({"x"})
          .singleAggregation({"x"}, {"count(*) as cnt"})
          .orderBy({"cnt"})
          .localMerge()
          .shuffleMerge()
          .project()
          .hashJoin(
              matchScan("t").filter("b < 100").broadcast().build(),
              core::JoinType::kInner)
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, distributedMatcher);
}

TEST_F(ExistencePushdownTest, aggregateKey) {
  auto logicalPlan = parseSelect(
      "SELECT t.a, dt.cnt "
      "FROM t "
      "JOIN (SELECT x, COUNT(*) AS cnt FROM u GROUP BY x) dt ON t.a = dt.cnt "
      "WHERE t.b < 100",
      kTestConnectorId);

  // Single-node plan.
  auto plan = toSingleNodePlan(logicalPlan);

  // No pushdown: join is on aggregate result (cnt), not grouping key.
  auto matcher =
      matchScan("u")
          .singleAggregation({"x"}, {"count(*) as cnt"})
          .project()
          .hashJoin(
              matchScan("t").filter("b < 100").build(), core::JoinType::kInner)
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // Distributed plan.
  auto distributedPlan = planVelox(logicalPlan);

  auto distributedMatcher =
      matchScan("u")
          .shuffle({"x"})
          .localPartition({"x"})
          .singleAggregation({"x"}, {"count(*) as cnt"})
          .project()
          .hashJoin(
              matchScan("t").filter("b < 100").broadcast().build(),
              core::JoinType::kInner)
          .gather()
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, distributedMatcher);
}

// Pushed table is a DT with unnest. The join key on firstDt's side is a
// grouping key (valid). The unnest is inside the pushed table, not firstDt.
// TODO: A more optimal plan would push w as existence inside dt's
// aggregation, filtering u before GROUP BY.
TEST_F(ExistencePushdownTest, otherIsUnnestDerivedTable) {
  auto logicalPlan = parseSelect(
      "SELECT dt.x, dt.cnt, w.n "
      "FROM (SELECT x, COUNT(*) AS cnt FROM u GROUP BY x) dt "
      "JOIN ("
      "  SELECT DISTINCT n FROM t CROSS JOIN UNNEST(ARRAY[a, b]) AS v(n)"
      ") w ON dt.x = w.n",
      kTestConnectorId);

  // Single-node plan.
  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchScan("u")
                     .singleAggregation({"x"}, {"count(*) as cnt"})
                     .hashJoin(
                         matchScan("t")
                             .project({"array[a, b] as a"})
                             .unnest({}, {"a"})
                             .singleAggregation({"n"}, {})
                             .build(),
                         core::JoinType::kInner)
                     .project()
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // Distributed plan.
  auto distributedPlan = planVelox(logicalPlan);

  auto distributedMatcher = matchScan("u")
                                .shuffle({"x"})
                                .localPartition({"x"})
                                .singleAggregation({"x"}, {"count(*) as cnt"})
                                .hashJoin(
                                    matchScan("t")
                                        .project({"array[a, b] as a"})
                                        .unnest({}, {"a"})
                                        .partialAggregation({"n"}, {})
                                        .shuffle({"n"})
                                        .localPartition({"n"})
                                        .finalAggregation({"n"}, {})
                                        .build(),
                                    core::JoinType::kInner)
                                .project()
                                .gather()
                                .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, distributedMatcher);
}

// Pushdown into a DT where the grouping key comes from unnest. The semijoin
// should filter the unnest output, not the unnest table itself.
// TODO: A more optimal plan would push r as existence below the aggregation,
// filtering unnested rows before GROUP BY.
TEST_F(ExistencePushdownTest, unnestGroupBy) {
  auto logicalPlan = parseSelect(
      "SELECT r.a, dt.n, dt.cnt "
      "FROM r "
      "JOIN ("
      "  SELECT n, COUNT(*) AS cnt "
      "  FROM t CROSS JOIN UNNEST(ARRAY[a, b, c]) AS v(n) "
      "  GROUP BY n"
      ") dt ON r.a = dt.n "
      "WHERE r.b < 100",
      kTestConnectorId);

  // Single-node plan.
  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = matchScan("r")
                     .filter("b < 100")
                     .hashJoin(
                         matchScan("t")
                             .project()
                             .unnest()
                             .singleAggregation({"n"}, {"count(*) as cnt"})
                             .build(),
                         core::JoinType::kInner)
                     .project()
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // Distributed plan.
  auto distributedPlan = planVelox(logicalPlan);

  auto distributedMatcher =
      matchScan("r")
          .filter("b < 100")
          .hashJoin(
              matchScan("t")
                  .project()
                  .unnest()
                  .partialAggregation({"n"}, {"count(*) as cnt"})
                  .shuffle({"n"})
                  .localPartition({"n"})
                  .finalAggregation({"n"}, {"count(cnt) as cnt"})
                  .broadcast()
                  .build(),
              core::JoinType::kInner)
          .project()
          .gather()
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, distributedMatcher);
}

TEST_F(ExistencePushdownTest, unnestKey) {
  // Use a table with no stats to trigger the code path where
  // findReducingBushyJoins pushes the base table into the subquery DT.
  testConnector_->addTable("t_no_stats", ROW("a", INTEGER()));

  auto plan = toSingleNodePlan(
      "SELECT 1 FROM t_no_stats "
      "WHERE a IN ("
      "  SELECT n "
      "  FROM (VALUES (ARRAY[1, 2]), (ARRAY[3])) AS w(numbers) "
      "  CROSS JOIN UNNEST(numbers) AS v(n)"
      ")");

  // The join key 'n' resolves to an unnest table column. Pushdown is skipped.
  // The plan has a semi-join between the unnest output and t_no_stats.
  auto matcher =
      core::PlanMatcherBuilder{}
          .values()
          .unnest()
          .hashJoin(
              matchScan("t_no_stats").build(), core::JoinType::kRightSemiFilter)
          .project()
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// No PARTITION BY — pushdown below window is invalid. Join must stay above.
TEST_F(ExistencePushdownTest, windowNonPartitionKey) {
  auto logicalPlan = parseSelect(
      "SELECT t.a, dt.x, dt.rn "
      "FROM t "
      "JOIN ("
      "  SELECT x, ROW_NUMBER() OVER (ORDER BY y) AS rn FROM u"
      ") dt ON t.a = dt.x "
      "WHERE t.b < 100",
      kTestConnectorId);

  // Single-node plan.
  auto plan = toSingleNodePlan(logicalPlan);

  // No pushdown: window has no PARTITION BY. Join stays above window.
  auto matcher =
      matchScan("u")
          .window({"row_number() OVER (ORDER BY y)"})
          .project()
          .hashJoin(
              matchScan("t").filter("b < 100").build(), core::JoinType::kInner)
          .project()
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  // Distributed plan.
  //
  // Window has no PARTITION BY, so all data must be gathered to a single
  // worker before the window function can execute.
  auto distributedPlan = planVelox(logicalPlan);

  auto distributedMatcher =
      matchScan("u")
          .gather()
          .localGather()
          .window({"row_number() OVER (ORDER BY y)"})
          .project()
          .hashJoin(
              matchScan("t").filter("b < 100").broadcast().build(),
              core::JoinType::kInner)
          .project()
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, distributedMatcher);
}

TEST_F(ExistencePushdownTest, functionOfGroupingKey) {
  // Column grouping key (GROUP BY x): pushdown fires.
  {
    auto plan = toSingleNodePlan(
        "SELECT 1 "
        "FROM t "
        "LEFT JOIN ("
        "  SELECT DISTINCT x, y FROM u"
        ") dt ON abs(dt.x) = abs(t.a)");

    // Semi-join pushdown fires — abs(dt.x) resolves to abs(u.x) which
    // references a valid pushdown column (grouping key x).
    auto matcher = matchScan("t")
                       .project({"abs(a)"})
                       .hashJoin(
                           matchScan("u")
                               .project()
                               .hashJoin(
                                   matchScan("t").project().build(),
                                   core::JoinType::kLeftSemiFilter)
                               .singleAggregation({"x", "y"}, {})
                               .project()
                               .project()
                               .build(),
                           core::JoinType::kLeft)
                       .project()
                       .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  // Expression grouping key (GROUP BY abs(x)): pushdown skipped.
  {
    auto plan = toSingleNodePlan(
        "SELECT 1 "
        "FROM t "
        "LEFT JOIN ("
        "  SELECT DISTINCT x + 2 AS x, y FROM u"
        ") dt ON abs(dt.x) = abs(t.a)");

    // No semi-join — reversed LEFT JOIN (RIGHT JOIN with u as probe).
    auto matcher = matchScan("u")
                       .project({"x + 2 as p", "y"})
                       .singleAggregation({"p", "y"}, {})
                       .project()
                       .project()
                       .hashJoin(
                           matchScan("t").project({"abs(a)"}).build(),
                           core::JoinType::kRight)
                       .project()
                       .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }
}

// Join key is a function of a non-grouping key (aggregate output). The
// existence pushdown must not push a semi-join below the aggregation on
// a non-grouping column — the aggregation changes the row set, so filtering
// by an aggregate output before aggregation is invalid.
TEST_F(ExistencePushdownTest, functionOfAggregate) {
  auto plan = toSingleNodePlan(
      "SELECT 1 "
      "FROM t "
      "LEFT JOIN ("
      "  SELECT x, COUNT(*) AS cnt FROM u GROUP BY x"
      ") dt ON abs(dt.cnt) = abs(t.a)");

  // No semi-join pushdown — reversed LEFT JOIN (RIGHT JOIN with u as probe).
  auto matcher =
      matchScan("u")
          .singleAggregation({"x"}, {"count(*) as cnt"})
          .project()
          .project()
          .hashJoin(matchScan("t").project().build(), core::JoinType::kRight)
          .project()
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

} // namespace
} // namespace facebook::axiom::optimizer
