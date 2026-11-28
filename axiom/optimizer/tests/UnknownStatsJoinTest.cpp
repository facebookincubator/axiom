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

// Exercises join planning when table cardinality or join-key NDV is missing.
class UnknownStatsJoinTest : public test::QueryTestBase,
                             public ::testing::WithParamInterface<bool> {
 protected:
  void SetUp() override {
    useV2_ = GetParam();
    test::QueryTestBase::SetUp();
  }

  velox::core::PlanNodePtr plan(const std::string& sql) {
    return toSingleNodePlan(parseSelect(sql));
  }
};

TEST_P(UnknownStatsJoinTest, automaticFallback) {
  testConnector_->addTable("t", ROW("t_k", BIGINT()))
      ->setStats(1'000'000, {{"t_k", {.numDistinct = 1'000'000}}});
  testConnector_->addTable("u", ROW("u_k", BIGINT()))->setStats(1'000, {});
  testConnector_->addTable("v", ROW("k", BIGINT()))
      ->setStats(100'000, {{"k", {.numDistinct = 100'000}}});

  for (const auto& [from, innerBuild, outerBuild] : {
           std::tuple{"t JOIN u ON t_k = u_k JOIN v ON u_k = k", "u", "v"},
           std::tuple{"t JOIN v ON t_k = k JOIN u ON u_k = k", "v", "u"},
       }) {
    const auto query = fmt::format("SELECT count(*) FROM {}", from);
    SCOPED_TRACE(query);
    AXIOM_ASSERT_PLAN_V2(
        plan(query),
        matchScan("t")
            .hashJoinInner(matchScan(innerBuild))
            .hashJoinInner(matchScan(outerBuild))
            .singleAggregation({}, {"count(*)"})
            .build());
  }
}

// A join with unknown key NDV uses input sizes to choose its build orientation.
// A non-preserved build is broadcast when it fits the limit.
TEST_P(UnknownStatsJoinTest, broadcastEquiJoin) {
  testConnector_->addTable("t", ROW("t_k", BIGINT()))
      ->setStats(1'000'000, {{"t_k", {.numDistinct = 1'000'000}}});
  testConnector_->addTable("u", ROW("u_k", BIGINT()))->setStats(1'000, {});

  for (const auto& [from, joinType] : {
           std::pair{"t JOIN u ON t_k = u_k", core::JoinType::kInner},
           std::pair{"u JOIN t ON t_k = u_k", core::JoinType::kInner},
           std::pair{"t LEFT JOIN u ON t_k = u_k", core::JoinType::kLeft},
           std::pair{"u RIGHT JOIN t ON t_k = u_k", core::JoinType::kLeft},
       }) {
    const auto query = fmt::format("SELECT count(*) FROM {}", from);
    SCOPED_TRACE(query);

    const auto logicalPlan = parseSelect(query);
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .hashJoin(matchScan("u"), joinType)
            .singleAggregation({}, {"count(*)"})
            .build());

    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(logicalPlan).plan,
        matchScan("t")
            .hashJoin(matchScan("u").broadcast(), joinType)
            .distributedAggregation({}, {"count(*)"})
            .build());
  }

  for (const auto from : {
           "u LEFT JOIN t ON t_k = u_k",
           "t RIGHT JOIN u ON t_k = u_k",
       }) {
    const auto query = fmt::format("SELECT count(*) FROM {}", from);
    SCOPED_TRACE(query);

    const auto logicalPlan = parseSelect(query);
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .hashJoinRight(matchScan("u"))
            .singleAggregation({}, {"count(*)"})
            .build());
    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(logicalPlan).plan,
        matchScan("t")
            .shuffle({"t_k"})
            .hashJoinRight(matchScan("u").shuffle({"u_k"}))
            .distributedAggregation({}, {"count(*)"})
            .build());
  }

  for (const auto& [predicate, joinType] : {
           std::pair{
               "EXISTS (SELECT 1 FROM u WHERE u_k = t_k)",
               core::JoinType::kLeftSemiFilter},
           std::pair{
               "NOT EXISTS (SELECT 1 FROM u WHERE u_k = t_k)",
               core::JoinType::kAnti},
       }) {
    const auto query =
        fmt::format("SELECT count(*) FROM t WHERE {}", predicate);
    SCOPED_TRACE(query);

    const auto logicalPlan = parseSelect(query);
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .hashJoin(matchScan("u"), joinType, {.nullAware = false})
            .singleAggregation({}, {"count(*)"})
            .build());

    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(logicalPlan).plan,
        matchScan("t")
            .hashJoin(
                matchScan("u").broadcast(), joinType, {.nullAware = false})
            .distributedAggregation({}, {"count(*)"})
            .build());
  }

  {
    const auto plan =
        parseSelect("SELECT t_k FROM t WHERE t_k NOT IN (SELECT u_k FROM u)");
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(plan),
        matchScan("t")
            .hashJoin(
                matchScan("u"),
                core::JoinType::kAnti,
                {.nullAware = true, .keys = {{"t_k = u_k"}}})
            .build());
    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(plan).plan,
        matchScan("t")
            .hashJoin(
                matchScan("u").broadcast(),
                core::JoinType::kAnti,
                {.nullAware = true, .keys = {{"t_k = u_k"}}})
            .gather()
            .build());
  }

  // The broadcast limit changes distribution without changing build
  // orientation.
  for (const auto broadcastSizeLimit : {0, 1}) {
    SCOPED_TRACE(broadcastSizeLimit);
    optimizerOptions_.broadcastSizeLimit = broadcastSizeLimit;
    const auto logicalPlan =
        parseSelect("SELECT count(*) FROM u JOIN t ON t_k = u_k");
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .hashJoinInner(matchScan("u"))
            .singleAggregation({}, {"count(*)"})
            .build());
    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(logicalPlan).plan,
        matchScan("t")
            .shuffle({"t_k"})
            .hashJoinInner(matchScan("u").shuffle({"u_k"}))
            .distributedAggregation({}, {"count(*)"})
            .build());
  }
}

// A build already partitioned on the key is broadcast when copying it costs
// less than repartitioning the probe.
TEST_P(UnknownStatsJoinTest, partitionedBuildDistribution) {
  testConnector_->addTable("t", ROW("t_k", BIGINT()))->setStats(1'000'000, {});
  testConnector_->addTable("u", ROW("u_k", BIGINT()))
      ->setStats(1'000, {{"u_k", {.numDistinct = 1'000}}});
  testConnector_->addTable("v", ROW("v_k", BIGINT()))->setStats(2'000, {});

  {
    const auto query =
        "SELECT count(*) FROM t "
        "JOIN (SELECT u_k FROM u GROUP BY u_k) ON t_k = u_k";
    SCOPED_TRACE(query);
    const auto logicalPlan = parseSelect(query);
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .hashJoinInner(matchScan("u").singleAggregation({"u_k"}, {}))
            .singleAggregation({}, {"count(*)"})
            .build());

    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(logicalPlan).plan,
        matchScan("t")
            .hashJoinInner(matchScan("u")
                               .partialAggregation({"u_k"}, {})
                               .shuffle({"u_k"})
                               .localPartition()
                               .finalAggregation({"u_k"}, {})
                               .broadcast())
            .distributedAggregation({}, {"count(*)"})
            .build());
  }

  {
    const auto query =
        "SELECT count(*) FROM v "
        "JOIN (SELECT u_k FROM u GROUP BY u_k) ON v_k = u_k";
    SCOPED_TRACE(query);
    const auto logicalPlan = parseSelect(query);
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan),
        matchScan("v")
            .hashJoinInner(matchScan("u").singleAggregation({"u_k"}, {}))
            .singleAggregation({}, {"count(*)"})
            .build());

    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(logicalPlan).plan,
        matchScan("v")
            .shuffle({"v_k"})
            .hashJoinInner(matchScan("u")
                               .partialAggregation({"u_k"}, {})
                               .shuffle({"u_k"})
                               .localPartition()
                               .finalAggregation({"u_k"}, {}))
            .distributedAggregation({}, {"count(*)"})
            .build());
  }
}

// A keyless join broadcasts one side whenever its semantics allow it. Size
// estimates choose the build orientation, but do not determine whether to
// broadcast because there is no partitioned alternative.
TEST_P(UnknownStatsJoinTest, broadcastThetaJoin) {
  testConnector_->addTable("t", ROW("t_k", BIGINT()))
      ->setStats(1'000'000, {{"t_k", {.numDistinct = 1'000'000}}});
  testConnector_->addTable("u", ROW("u_k", BIGINT()))->setStats(1'000, {});

  const auto writtenRightJoinPlan =
      parseSelect("SELECT count(t_k + u_k) FROM t RIGHT JOIN u ON t_k < u_k");
  AXIOM_ASSERT_PLAN_V2(
      toSingleNodePlan(writtenRightJoinPlan),
      matchScan("t")
          .nestedLoopJoinRight(matchScan("u"), "t_k < u_k")
          .project({"u_k + t_k as sum"})
          .singleAggregation({}, {"count(sum)"})
          .build());

  for (const auto broadcastSizeLimit : {0, 1}) {
    SCOPED_TRACE(broadcastSizeLimit);
    optimizerOptions_.broadcastSizeLimit = broadcastSizeLimit;
    const auto logicalPlan =
        parseSelect("SELECT count(t_k + u_k) FROM u CROSS JOIN t");
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(logicalPlan),
        matchScan("t")
            .nestedLoopJoin(matchScan("u"))
            .project({"u_k + t_k as sum"})
            .singleAggregation({}, {"count(sum)"})
            .build());
    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(logicalPlan).plan,
        matchScan("t")
            .nestedLoopJoin(matchScan("u").broadcast())
            .project({"u_k + t_k as sum"})
            .distributedAggregation({}, {"count(sum)"})
            .build());

    const auto swappedRightJoinPlan =
        parseSelect("SELECT count(t_k + u_k) FROM u RIGHT JOIN t ON u_k < t_k");
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(swappedRightJoinPlan),
        matchScan("t")
            .nestedLoopJoinLeft(matchScan("u"), "u_k < t_k")
            .project({"t_k + u_k as sum"})
            .singleAggregation({}, {"count(sum)"})
            .build());
    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(swappedRightJoinPlan).plan,
        matchScan("t")
            .nestedLoopJoinLeft(matchScan("u").broadcast(), "u_k < t_k")
            .project({"t_k + u_k as sum"})
            .distributedAggregation({}, {"count(sum)"})
            .build());
  }
}

// The fallback is per derived table: an unknown-cost join does not disable
// cost-based ordering of an independent join elsewhere in the query. A
// non-deterministic filter between (u JOIN t) and the join with 'v' keeps the
// two joins in separate derived tables.
TEST_P(UnknownStatsJoinTest, twoJoins) {
  const auto query =
      "SELECT count(*) "
      "FROM (SELECT u.k AS k FROM u JOIN t ON u.k = t.k WHERE rand() < 0.1) AS s "
      "   JOIN v ON s.k = v.k";

  const auto altQuery =
      "SELECT count(*) "
      "FROM (SELECT u.k AS k FROM t JOIN u ON u.k = t.k WHERE rand() < 0.1) AS s "
      "   JOIN v ON s.k = v.k";

  auto matchPlan = []() {
    return matchScan("v")
        .hashJoinInner(matchScan("t").hashJoinInner(matchScan("u")))
        .aggregation()
        .build();
  };

  testConnector_->addTable("t", ROW({"a", "k"}, BIGINT()))
      ->setStats(1'000'000, {{"k", {.numDistinct = 1'000'000}}});
  testConnector_->addTable("u", ROW({"b", "k"}, BIGINT()))
      ->setStats(1'000, {{"k", {.numDistinct = 1'000}}});
  testConnector_->addTable("v", ROW({"c", "k"}, BIGINT()))
      ->setStats(100'000, {});

  // 'v' has no key NDV. The inner join is still cost-ordered and builds the
  // smaller 'u'.
  AXIOM_ASSERT_PLAN_V2(plan(query), matchPlan());
  AXIOM_ASSERT_PLAN_V2(plan(altQuery), matchPlan());
}

// When one input has no point estimate, the join keeps its written orientation:
// the right input builds.
TEST_P(UnknownStatsJoinTest, unknownCardinality) {
  testConnector_->addTable("t", ROW({"a", "k"}, BIGINT()))
      ->setStats(1'000'000, {{"k", {.numDistinct = 1'000'000}}});
  testConnector_->addTable(
      "u",
      ROW({"b", "k"}, BIGINT()),
      {
          {std::string{connector::TestConnectorMetadata::kCollectStatistics},
           Variant(false)},
      });

  auto matchJoin = [](const std::string& probe, const std::string& build) {
    return matchScan(probe)
        .hashJoinInner(matchScan(build))
        .aggregation()
        .build();
  };

  for (const auto& [sql, probe, build] : {
           std::tuple{"SELECT count(*) FROM u JOIN t ON t.k = u.k", "u", "t"},
           std::tuple{"SELECT count(*) FROM t JOIN u ON t.k = u.k", "t", "u"},
       }) {
    SCOPED_TRACE(sql);
    AXIOM_ASSERT_PLAN_V2(plan(sql), matchJoin(probe, build));
  }

  // Limit gives the unknown input an upper bound but no point estimate.
  const auto query =
      "SELECT count(*) FROM t "
      "JOIN (SELECT * FROM u LIMIT 10000000) s ON t.k = s.k";
  SCOPED_TRACE(query);
  AXIOM_ASSERT_PLAN_V2(
      plan(query),
      matchScan("t")
          .hashJoinInner(matchScan("u").finalLimit(0, 10'000'000))
          .aggregation()
          .build());
}

// Without estimates, the single-row input of an inner join without keys still
// goes on the build side.
TEST_P(UnknownStatsJoinTest, singleRowBuild) {
  using core::JoinType;
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
      ->setStats(1'000, {{"a", {.numDistinct = 1'000}}});
  testConnector_->addTable("u", ROW("a", BIGINT()));
  testConnector_->addTable("w", ROW("a", BIGINT()));
  testConnector_->addTable("v", ROW({"a", "b"}, BIGINT()));
  auto count = [] {
    return matchScan("t").singleAggregation({}, {"count(*) as c"});
  };

  {
    SCOPED_TRACE("missing estimate");
    AXIOM_ASSERT_PLAN_V2(
        plan(
            "SELECT s.c, u.a FROM (SELECT count(*) AS c FROM t) s CROSS JOIN u"),
        matchScan("u")
            .aliases({"ua"})
            .nestedLoopJoin(count(), JoinType::kInner, "")
            .project({"c", "ua"})
            .build());
  }

  // Without statistics, the keyed tables join as v, u, w, and the count is the
  // build of the cross product above them.
  {
    SCOPED_TRACE("fallback planner");
    AXIOM_ASSERT_PLAN_V2(
        plan(
            "SELECT s.c, l.a, d.a "
            "FROM (SELECT count(*) AS c FROM t) s "
            "CROSS JOIN u l "
            "CROSS JOIN w d "
            "JOIN v r ON l.a = r.a AND d.a = r.b"),
        matchScan("v")
            .aliases({"va", "vb"})
            .hashJoinInner(
                matchScan("u").aliases({"ua"}), {.keys = {{"va = ua"}}})
            .hashJoinInner(
                matchScan("w").aliases({"wa"}), {.keys = {{"vb = wa"}}})
            .nestedLoopJoin(count(), JoinType::kInner, "")
            .project({"c", "ua", "wa"})
            .build());
  }
}

TEST_P(UnknownStatsJoinTest, crossJoinFallback) {
  // 't' and 'u' join only through 'v', whose join-key NDV is absent, so the
  // join cost is unknown.
  testConnector_->addTable("t", ROW({"a", "k", "t_c"}, BIGINT()))
      ->setStats(1'000'000, {{"k", {.numDistinct = 1'000'000}}});
  testConnector_->addTable("u", ROW({"b", "k", "u_c"}, BIGINT()))
      ->setStats(1'000, {{"k", {.numDistinct = 1'000}}});
  testConnector_->addTable("v", ROW({"x", "y", "c"}, BIGINT()))
      ->setStats(100'000, {});

  // Reorders a plain three-way join to avoid the written cross join.
  {
    const auto query =
        "SELECT count(*) FROM t, u, v WHERE t.k = v.x AND u.k = v.y";
    SCOPED_TRACE(query);

    optimizerOptions_.syntacticJoinOrder = false;
    AXIOM_ASSERT_PLAN(
        plan(query),
        matchScan("t")
            .hashJoinInner(matchScan("v"))
            .hashJoinInner(matchScan("u"))
            .aggregation()
            .build());

    optimizerOptions_.syntacticJoinOrder = true;
    AXIOM_ASSERT_PLAN(
        plan(query),
        matchScan("t")
            .nestedLoopJoin(matchScan("u"))
            .hashJoinInner(matchScan("v"))
            .aggregation()
            .build());
  }

  // Materializes an expression key at the point required by each join order.
  {
    const auto query =
        "SELECT count(*) FROM t, u, v WHERE t.k + 0 = v.x AND u.k = v.y";
    SCOPED_TRACE(query);

    optimizerOptions_.syntacticJoinOrder = false;
    AXIOM_ASSERT_PLAN(
        plan(query),
        matchScan("t")
            .project()
            .hashJoinInner(matchScan("v"))
            .hashJoinInner(matchScan("u"))
            .aggregation()
            .build());

    optimizerOptions_.syntacticJoinOrder = true;
    AXIOM_ASSERT_PLAN(
        plan(query),
        matchScan("t")
            .nestedLoopJoin(matchScan("u"))
            .project()
            .hashJoinInner(matchScan("v"))
            .aggregation()
            .build());
  }

  // Preserves a straddling equality until all referenced inputs are available.
  {
    const auto query =
        "SELECT count(*) FROM t, u, v "
        "WHERE t.k = v.x AND u.k = v.y AND t.t_c + u.u_c = v.c";
    SCOPED_TRACE(query);

    optimizerOptions_.syntacticJoinOrder = false;
    AXIOM_ASSERT_PLAN_V2(
        plan(query),
        matchScan("t")
            .hashJoinInner(matchScan("v"))
            .hashJoinInner(matchScan("u"))
            .filter("c = t_c + u_c")
            .project()
            .aggregation()
            .build());

    optimizerOptions_.syntacticJoinOrder = true;
    AXIOM_ASSERT_PLAN_V2(
        plan(query),
        matchScan("t")
            .nestedLoopJoin(matchScan("u"))
            .project()
            .hashJoinInner(matchScan("v"))
            .aggregation()
            .build());
  }

  // Does not physically rewrite an aggregate input a second time.
  {
    const auto query =
        "SELECT sum(n) "
        "FROM (SELECT k, count(*) AS n FROM t GROUP BY k) t, u, v "
        "WHERE t.k = v.x AND u.k = v.y";
    SCOPED_TRACE(query);

    optimizerOptions_.syntacticJoinOrder = false;
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(parseSelect(query), /*numDrivers=*/4),
        matchScan("t")
            .partialAggregation({"k"}, {"count(*) as n"})
            .localPartition({"k"})
            .finalAggregation()
            .hashJoinInner(matchScan("v"))
            .hashJoinInner(matchScan("u"))
            .partialAggregation({}, {"sum(n)"})
            .localPartition()
            .finalAggregation()
            .build());

    optimizerOptions_.syntacticJoinOrder = true;
    AXIOM_ASSERT_PLAN_V2(
        toSingleNodePlan(parseSelect(query), /*numDrivers=*/4),
        matchScan("t")
            .partialAggregation({"k"}, {"count(*) as n"})
            .localPartition({"k"})
            .finalAggregation()
            .nestedLoopJoin(matchScan("u"))
            .hashJoinInner(matchScan("v"))
            .partialAggregation({}, {"sum(n)"})
            .localPartition()
            .finalAggregation()
            .build());
  }

  // Retains an unavoidable cross join to a disconnected input.
  {
    testConnector_->addTable("w", ROW({"c", "k"}, BIGINT()));
    const auto query =
        "SELECT count(*) FROM t, u, v, w WHERE t.k = v.x AND u.k = v.y";
    SCOPED_TRACE(query);

    optimizerOptions_.syntacticJoinOrder = false;
    AXIOM_ASSERT_PLAN(
        plan(query),
        matchScan("t")
            .hashJoinInner(matchScan("v"))
            .hashJoinInner(matchScan("u"))
            .nestedLoopJoin(matchScan("w"))
            .aggregation()
            .build());

    optimizerOptions_.syntacticJoinOrder = true;
    AXIOM_ASSERT_PLAN(
        plan(query),
        matchScan("t")
            .nestedLoopJoin(matchScan("u"))
            .hashJoinInner(matchScan("v"))
            .nestedLoopJoin(matchScan("w"))
            .aggregation()
            .build());
  }
}

AXIOM_INSTANTIATE_V1_V2(UnknownStatsJoinTest);

} // namespace
} // namespace facebook::axiom::optimizer
