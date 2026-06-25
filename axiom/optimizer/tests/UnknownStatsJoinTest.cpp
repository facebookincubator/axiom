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

// When a join-key NDV is missing the join cost is unknown, so the optimizer
// falls back to the query's syntactic join order instead of a cost-based one.
// 't' is large and 'u' is small; 'k' is the join key.
class UnknownStatsJoinTest : public test::QueryTestBase {
 protected:
  velox::core::PlanNodePtr plan(const std::string& sql) {
    return toSingleNodePlan(parseSelect(sql, kTestConnectorId));
  }
};

TEST_F(UnknownStatsJoinTest, singleJoin) {
  testConnector_->addTable("t", ROW({"a", "k"}, BIGINT()))
      ->setStats(1'000'000, {{"k", {.numDistinct = 1'000'000}}});
  testConnector_->addTable("u", ROW({"b", "k"}, BIGINT()))
      ->setStats(1'000, {{"k", {.numDistinct = 1'000}}});

  auto matchJoin = [&](const std::string& probe, const std::string& build) {
    return matchScan(probe)
        .hashJoinInner(matchScan(build).build())
        .aggregation()
        .build();
  };

  const auto query = "SELECT count(*) FROM u JOIN t ON t.k = u.k";
  const auto altQuery = "SELECT count(*) FROM t JOIN u ON t.k = u.k";

  // With statistics the build side is chosen by size, so both join orders build
  // the smaller 'u'.
  AXIOM_ASSERT_PLAN(plan(query), matchJoin("t", "u"));
  AXIOM_ASSERT_PLAN(plan(altQuery), matchJoin("t", "u"));

  // Drop 'u's column NDV, leaving only its row count. The join cost is now
  // unknown, so each query falls back to its syntactic join order.
  testConnector_->setStats("u", 1'000, {});

  AXIOM_ASSERT_PLAN(plan(query), matchJoin("u", "t"));
  AXIOM_ASSERT_PLAN(plan(altQuery), matchJoin("t", "u"));
}

// The fallback is per derived table: an unknown-cost join does not disable
// cost-based ordering of an independent join elsewhere in the query. A
// non-deterministic filter between (u JOIN t) and the join with 'v' keeps the
// two joins in separate derived tables.
TEST_F(UnknownStatsJoinTest, twoJoins) {
  const auto query =
      "SELECT count(*) "
      "FROM (SELECT u.k AS k FROM u JOIN t ON u.k = t.k WHERE rand() < 0.1) AS s "
      "   JOIN v ON s.k = v.k";

  const auto altQuery =
      "SELECT count(*) "
      "FROM (SELECT u.k AS k FROM t JOIN u ON u.k = t.k WHERE rand() < 0.1) AS s "
      "   JOIN v ON s.k = v.k";

  // 'innerProbe'/'innerBuild' are the probe and build of the inner (u JOIN t)
  // join; the outer join always builds 'v'.
  auto matchPlan = [&](const std::string& innerProbe,
                       const std::string& innerBuild) {
    return matchScan(innerProbe)
        .hashJoinInner(matchScan(innerBuild).build())
        .filter()
        .hashJoinInner(matchScan("v").build())
        .aggregation()
        .build();
  };

  testConnector_->addTable("t", ROW({"a", "k"}, BIGINT()))
      ->setStats(1'000'000, {{"k", {.numDistinct = 1'000'000}}});
  testConnector_->addTable("u", ROW({"b", "k"}, BIGINT()))
      ->setStats(1'000, {{"k", {.numDistinct = 1'000}}});
  testConnector_->addTable("v", ROW({"c", "k"}, BIGINT()));

  // 't' and 'u' are statted; 'v' has no statistics. The inner join is still
  // cost-ordered and builds the smaller 'u'.
  AXIOM_ASSERT_PLAN(plan(query), matchPlan("t", "u"));
  AXIOM_ASSERT_PLAN(plan(altQuery), matchPlan("t", "u"));

  // Drop 't's column NDV. The inner join cost is now unknown, so it falls back
  // to its syntactic order.
  testConnector_->setStats("t", 1'000'000, {});

  AXIOM_ASSERT_PLAN(plan(query), matchPlan("u", "t"));
  AXIOM_ASSERT_PLAN(plan(altQuery), matchPlan("t", "u"));
}
// A base table with no statistics at all must fall back to syntactic join
// order, not crash on the unknown cardinality.
TEST_F(UnknownStatsJoinTest, joinWithUnknownTableCardinality) {
  testConnector_->addTable("t", ROW({"a", "k"}, BIGINT()))
      ->setStats(1'000'000, {{"k", {.numDistinct = 1'000'000}}});
  testConnector_->addTable("u", ROW({"b", "k"}, BIGINT()));

  auto matchJoin = [&](const std::string& probe, const std::string& build) {
    return matchScan(probe)
        .hashJoinInner(matchScan(build).build())
        .aggregation()
        .build();
  };

  const auto query = "SELECT count(*) FROM u JOIN t ON t.k = u.k";
  const auto altQuery = "SELECT count(*) FROM t JOIN u ON t.k = u.k";

  AXIOM_ASSERT_PLAN(plan(query), matchJoin("u", "t"));
  AXIOM_ASSERT_PLAN(plan(altQuery), matchJoin("t", "u"));
}

// The join sampler must tolerate an unknown build-side cardinality.
TEST_F(UnknownStatsJoinTest, sampledJoinWithUnknownCardinality) {
  optimizerOptions_.sampleJoins = true;

  testConnector_->addTable("t", ROW({"a", "k"}, BIGINT()))
      ->setStats(1'000'000, {{"k", {.numDistinct = 1'000'000}}});
  testConnector_->addTable("u", ROW({"b", "k"}, BIGINT()));

  auto matchJoin = [&](const std::string& probe, const std::string& build) {
    return matchScan(probe)
        .hashJoinInner(matchScan(build).build())
        .aggregation()
        .build();
  };

  AXIOM_ASSERT_PLAN(
      plan("SELECT count(*) FROM u JOIN t ON t.k = u.k"), matchJoin("u", "t"));
}

// Enabling sampleJoins must not change the chosen plan when a side has
// unknown cardinality.
TEST_F(UnknownStatsJoinTest, sampledJoinMatchesUnsampledOnUnknownCardinality) {
  testConnector_->addTable("t", ROW({"a", "k"}, BIGINT()))
      ->setStats(1'000'000, {{"k", {.numDistinct = 1'000'000}}});
  testConnector_->addTable("u", ROW({"b", "k"}, BIGINT()));

  auto matchJoin = [&](const std::string& probe, const std::string& build) {
    return matchScan(probe)
        .hashJoinInner(matchScan(build).build())
        .aggregation()
        .build();
  };

  const auto query = "SELECT count(*) FROM u JOIN t ON t.k = u.k";
  const auto altQuery = "SELECT count(*) FROM t JOIN u ON t.k = u.k";

  AXIOM_ASSERT_PLAN(plan(query), matchJoin("u", "t"));
  AXIOM_ASSERT_PLAN(plan(altQuery), matchJoin("t", "u"));

  optimizerOptions_.sampleJoins = true;

  AXIOM_ASSERT_PLAN(plan(query), matchJoin("u", "t"));
  AXIOM_ASSERT_PLAN(plan(altQuery), matchJoin("t", "u"));
}

} // namespace
} // namespace facebook::axiom::optimizer
