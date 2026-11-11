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

// A join evaluates its filter once per candidate pair, so v2 moves the parts
// that read a single side into that input, as far below the join as their
// dependencies and semantics allow.
class JoinFilterProjectionTest : public test::QueryTestBase {
 protected:
  void SetUp() override {
    test::QueryTestBase::SetUp();
    testConnector_->addTable("t", ROW({{"a", BIGINT()}, {"b", VARCHAR()}}));
    testConnector_->addTable("u", ROW({{"x", BIGINT()}, {"y", VARCHAR()}}));
  }

  core::PlanNodePtr plan(const std::string& sql) {
    return toSingleNodePlan(parseSelect(sql));
  }
};

// An operand reading one side moves into that side's input and the join
// compares columns. An operand reading both sides stays, with its single-side
// arguments moved. The inputs project only what the join needs, dropping the
// columns that fed the moved expressions.
TEST_F(JoinFilterProjectionTest, basic) {
  auto matcher = matchScan("t")
                     .project({"a", "length(b) as pt"})
                     .nestedLoopJoin(
                         matchScan("u").project({"x", "length(y) as pu"}),
                         core::JoinType::kInner,
                         "\"and\"(pt < pu, pt + pu > 10, a < x)")
                     .build();

  AXIOM_ASSERT_PLAN(
      plan(
          "SELECT a, x "
          "FROM t, u "
          "WHERE length(b) < length(y) AND length(b) + length(y) > 10 AND a < x"),
      matcher);
}

// A lambda whose body reads the other side keeps the call in the filter. Only
// the call's other arguments move, and the outer columns the body reads have to
// reach the join -- here 'x', which the join does not output.
TEST_F(JoinFilterProjectionTest, lambdaReadingBothSides) {
  auto matcher = matchScan("t")
                     .project({"a", "sequence(1, a + 10) as arr"})
                     .nestedLoopJoin(
                         matchScan("u"),
                         core::JoinType::kInner,
                         "cardinality(filter(arr, z -> x < z)) > 0")
                     .build();

  AXIOM_ASSERT_PLAN(
      plan(
          "SELECT a "
          "FROM t, u "
          "WHERE cardinality(filter(sequence(1, a + 10), z -> z > x)) > 0"),
      matcher);
}

// A call whose lambda reads only one side moves as a whole, leaving the join
// comparing columns.
TEST_F(JoinFilterProjectionTest, lambdaReadingOneSide) {
  auto matcher =
      matchScan("t")
          .project(
              {"a",
               "cardinality(filter(sequence(1, a + 10), z -> a < z)) as c"})
          .nestedLoopJoin(matchScan("u"), core::JoinType::kInner, "x < c")
          .build();

  AXIOM_ASSERT_PLAN(
      plan(
          "SELECT a "
          "FROM t, u "
          "WHERE cardinality(filter(sequence(1, a + 10), z -> z > a)) > x"),
      matcher);
}

// A non-deterministic call produces a new value per evaluation, so moving it
// out of the filter would change results. It stays, while the deterministic
// operands around it move.

TEST_F(JoinFilterProjectionTest, nonDeterministic) {
  auto matcher =
      matchScan("t")
          .project({"a", "cast(length(b) as DOUBLE) as pt"})
          .nestedLoopJoin(
              matchScan("u").project({"x", "cast(length(y) as DOUBLE) as pu"}),
              core::JoinType::kInner,
              "pt + random() < pu")
          .build();

  AXIOM_ASSERT_PLAN(
      plan(
          "SELECT a, x "
          "FROM t, u "
          "WHERE length(b) + random() < length(y)"),
      matcher);
}

// The move happens before distribution is chosen, so the expression is computed
// on the side that supplies it and only its value crosses the exchange. Here
// the build is broadcast to every worker, so what crosses is a bigint rather
// than the string it was computed from.
TEST_F(JoinFilterProjectionTest, oneSidedExpressionComputedBeforeShuffle) {
  testConnector_->addTable("wide", ROW("name", VARCHAR()))
      ->setStats(3, {{"name", {.numDistinct = 3}}});
  testConnector_->addTable("big", ROW("threshold", BIGINT()))
      ->setStats(10'000, {{"threshold", {.numDistinct = 10'000}}});

  auto logicalPlan = parseSelect(
      "SELECT count(*) "
      "FROM big, wide "
      "WHERE length(wide.name) > big.threshold");

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(logicalPlan),
      matchScan("big")
          .nestedLoopJoin(
              matchScan("wide").project({"length(name) as p"}),
              core::JoinType::kInner,
              "threshold < p")
          .singleAggregation({}, {"count(*)"})
          .build());

  AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
      planVelox(logicalPlan).plan,
      matchScan("big")
          .nestedLoopJoin(
              matchScan("wide").project({"length(name) as p"}).shuffle(),
              core::JoinType::kInner,
              "threshold < p")
          .distributedAggregation({}, {"count(*)"})
          .build());
}

// An input with fewer rows than the pairs a join matches on its keys computes
// the filter parts that read it: each of the 100 rows of 'l' and of 'r'
// matches 10 rows of the other, so the join evaluates its filter on 1'000
// pairs. This holds for every join type with a filter.
TEST_F(JoinFilterProjectionTest, equiJoinFanout) {
  testConnector_->addTable("l", ROW({{"lk", BIGINT()}, {"b", VARCHAR()}}))
      ->setStats(100, {{"lk", {.numDistinct = 10}}});
  testConnector_->addTable("r", ROW({{"rk", BIGINT()}, {"v", VARCHAR()}}))
      ->setStats(100, {{"rk", {.numDistinct = 10}}});

  const std::vector<std::pair<std::string, core::JoinType>> cases = {
      {"SELECT count(*) "
       "FROM l JOIN r ON l.lk = r.rk AND upper(l.b) >= upper(r.v)",
       core::JoinType::kInner},
      {"SELECT count(*) "
       "FROM l LEFT JOIN r ON l.lk = r.rk AND upper(l.b) >= upper(r.v)",
       core::JoinType::kLeft},
      {"SELECT count(*) "
       "FROM l "
       "WHERE EXISTS ("
       "  SELECT 1 "
       "  FROM r "
       "  WHERE r.rk = l.lk AND upper(l.b) >= upper(r.v))",
       core::JoinType::kLeftSemiFilter},
      {"SELECT count(*) "
       "FROM l "
       "WHERE NOT EXISTS ("
       "  SELECT 1 "
       "  FROM r "
       "  WHERE r.rk = l.lk AND upper(l.b) >= upper(r.v))",
       core::JoinType::kAnti},
  };
  for (const auto& [sql, joinType] : cases) {
    SCOPED_TRACE(sql);
    AXIOM_ASSERT_PLAN(
        plan(sql),
        matchScan("l")
            .project({"lk", "upper(b) as pl"})
            .hashJoin(
                matchScan("r").project({"rk", "upper(v) as pr"}),
                joinType,
                {.keys = {{"lk = rk"}}, .filter = "pl >= pr"})
            .singleAggregation({}, {"count(*)"})
            .build());
  }
}

// An input with at least as many rows as the pairs leaves its parts in the
// filter: each of the 50 rows of 'f', the build input, matches one of the 100
// rows of 'd', so the join evaluates its filter on 50 pairs.
TEST_F(JoinFilterProjectionTest, equiJoinNoFanout) {
  testConnector_->addTable("f", ROW({{"fk", BIGINT()}, {"b", VARCHAR()}}))
      ->setStats(50, {{"fk", {.numDistinct = 50}}});
  testConnector_->addTable("d", ROW({{"dk", BIGINT()}, {"v", VARCHAR()}}))
      ->setStats(100, {{"dk", {.numDistinct = 100}}});

  AXIOM_ASSERT_PLAN(
      plan(
          "SELECT count(*) "
          "FROM f "
          "JOIN d ON f.fk = d.dk AND f.b >= upper(d.v)"),
      matchScan("d")
          .hashJoinInner(
              matchScan("f"),
              {.keys = {{"dk = fk"}}, .filter = "b >= upper(v)"})
          .singleAggregation({}, {"count(*)"})
          .build());
}

// Without key NDVs the number of pairs is unknown. The build input, with fewer
// rows than the probe input, computes the filter parts that read it, before
// the build crosses its exchange, which carries the part's value rather than
// the column it reads.
TEST_F(JoinFilterProjectionTest, equiJoinUnknownPairs) {
  auto probe =
      testConnector_->addTable("f", ROW({{"fk", BIGINT()}, {"b", VARCHAR()}}));
  testConnector_->addTable("d", ROW({{"dk", BIGINT()}, {"v", VARCHAR()}}))
      ->setStats(100, {});
  for (const auto [numProbeRows, broadcast] :
       {std::pair{1'000, false}, std::pair{1'000'000, true}}) {
    SCOPED_TRACE(fmt::format("numProbeRows: {}", numProbeRows));
    probe->setStats(numProbeRows, {});

    const auto logicalPlan = parseSelect(
        "SELECT count(*) "
        "FROM f "
        "JOIN d ON f.fk = d.dk AND f.b >= upper(d.v)");
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchScan("f")
            .hashJoinInner(
                matchScan("d").project({"dk", "upper(v) as p"}),
                {.keys = {{"fk = dk"}}, .filter = "b >= p"})
            .singleAggregation({}, {"count(*)"})
            .build());

    AXIOM_ASSERT_DISTRIBUTED_PLAN_V2(
        planVelox(logicalPlan).plan,
        matchScan("f")
            .hashJoinInner(
                matchScan("d")
                    .project({"dk", "upper(v) as p"})
                    .shuffleIf(!broadcast)
                    .broadcastIf(broadcast),
                {.keys = {{"fk = dk"}}, .filter = "b >= p"})
            .distributedAggregation({}, {"count(*)"})
            .build());
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
