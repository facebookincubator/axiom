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
// of the filter that read a single side into that side's input, where they are
// evaluated once per row. v1 leaves the filter intact, so these plans are
// v2-only.
class JoinFilterProjectionTest : public test::QueryTestBase {
 protected:
  JoinFilterProjectionTest() {
    useV2_ = true;
  }

  void SetUp() override {
    test::QueryTestBase::SetUp();
    testConnector_->addTable("t", ROW({"a", "b"}, {BIGINT(), VARCHAR()}));
    testConnector_->addTable("u", ROW({"x", "y"}, {BIGINT(), VARCHAR()}));
  }

  core::PlanNodePtr plan(const std::string& sql) {
    return toSingleNodePlan(parseSelect(sql, kTestConnectorId));
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
          "SELECT a, x FROM t, u "
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
          "SELECT a FROM t, u "
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
          "SELECT a FROM t, u "
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
      plan("SELECT a, x FROM t, u WHERE length(b) + random() < length(y)"),
      matcher);
}

} // namespace
} // namespace facebook::axiom::optimizer
