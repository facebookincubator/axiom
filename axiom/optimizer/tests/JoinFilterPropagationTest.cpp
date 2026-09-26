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

class JoinFilterPropagationTest : public test::QueryTestBase {
 protected:
  void SetUp() override {
    test::QueryTestBase::SetUp();
    testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()))
        ->setStats(10'000, {{"a", {.numDistinct = 1'000}}});
    testConnector_->addTable("u", ROW({"x", "y"}, BIGINT()))
        ->setStats(1'000, {{"x", {.numDistinct = 100}}});
  }

  core::PlanNodePtr plan(const std::string& sql) {
    return toSingleNodePlan(parseSelect(sql, kTestConnectorId));
  }
};

// A filter guaranteed by the preserved input also restricts matching rows
// from the null-supplying input.
TEST_F(JoinFilterPropagationTest, leftOuter) {
  const auto query =
      "SELECT d.a, count(u.y) "
      "FROM (SELECT DISTINCT a FROM t WHERE a > 100) d "
      "LEFT JOIN u ON d.a = u.x "
      "GROUP BY d.a";
  SCOPED_TRACE(query);

  auto matcher =
      matchScan("u")
          .filter("x > 100")
          .hashJoinRight(
              matchScan("t").filter("a > 100").singleAggregation({"a"}, {}),
              {.keys = {{"x = a"}}})
          .singleAggregation({"a"}, {"count(y)"})
          .build();

  AXIOM_ASSERT_PLAN(plan(query), matcher);
}

TEST_F(JoinFilterPropagationTest, rightOuter) {
  const auto query =
      "SELECT d.x, count(t.b) "
      "FROM t "
      "RIGHT JOIN (SELECT DISTINCT x FROM u WHERE x > 100) d ON t.a = d.x "
      "GROUP BY d.x";
  SCOPED_TRACE(query);

  auto matcher =
      matchScan("t")
          .filter("a > 100")
          .hashJoinRight(
              matchScan("u").filter("x > 100").singleAggregation({"x"}, {}),
              {.keys = {{"a = x"}}})
          .singleAggregation({"x"}, {"count(b)"})
          .build();

  AXIOM_ASSERT_PLAN(plan(query), matcher);
}

// A FULL JOIN cannot use a predicate from either input to restrict the other.
TEST_F(JoinFilterPropagationTest, fullOuter) {
  const auto query =
      "SELECT * "
      "FROM (SELECT * FROM t WHERE a > 100) t "
      "FULL JOIN u ON t.a = u.x";
  SCOPED_TRACE(query);

  auto matcher = matchScan("t")
                     .filter("a > 100")
                     .hashJoin(matchScan("u"), core::JoinType::kFull)
                     .build();

  AXIOM_ASSERT_PLAN(plan(query), matcher);
}

// A filter guaranteed by an inner join also restricts matching rows from the
// nullable input of a parent LEFT JOIN.
TEST_F(JoinFilterPropagationTest, nestedJoins) {
  testConnector_->addTable("v", ROW({"n", "m"}, BIGINT()))
      ->setStats(100, {{"n", {.numDistinct = 10}}});

  const auto query =
      "SELECT q.a, count(v.m) "
      "FROM ("
      "  SELECT d.a, u.x "
      "  FROM (SELECT DISTINCT a FROM t WHERE a > 100) d "
      "  JOIN u ON d.a = u.x"
      ") q "
      "LEFT JOIN v ON q.x = v.n "
      "GROUP BY q.a";
  SCOPED_TRACE(query);

  auto matcher =
      matchScan("t")
          .filter("a > 100")
          .singleAggregation({"a"}, {})
          .hashJoinInner(
              matchScan("u").filter("x > 100"), {.keys = {{"a = x"}}})
          .hashJoinLeft(matchScan("v").filter("n > 100"), {.keys = {{"a = n"}}})
          .streamingAggregation({"a"}, {"count(m)"})
          .build();

  AXIOM_ASSERT_PLAN(plan(query), matcher);
}

// An equality pushed into a nested cross join can propagate a filter collected
// below either input.
TEST_F(JoinFilterPropagationTest, pendingEqualityInNestedJoin) {
  testConnector_->addTable("v", ROW({"n", "m"}, BIGINT()));

  const auto query =
      "SELECT q.a, v.m "
      "FROM ("
      "  SELECT t.a, t.b, u.x "
      "  FROM (SELECT * FROM t WHERE b > 100) t CROSS JOIN u"
      ") q "
      "LEFT JOIN v ON q.a = v.n "
      "WHERE q.b = q.x";
  SCOPED_TRACE(query);

  AXIOM_ASSERT_PLAN(
      plan(query),
      matchScan("t")
          .filter("b > 100")
          .hashJoinInner(
              matchScan("u").filter("x > 100"), {.keys = {{"b = x"}}})
          .hashJoinLeft(matchScan("v"), {.keys = {{"a = n"}}})
          .build());
}

// A predicate remains available through every output alias of a union leg.
TEST_F(JoinFilterPropagationTest, repeatedUnionColumn) {
  testConnector_->addTable("v", ROW({"n", "m"}, BIGINT()));

  const auto query =
      "SELECT q.value, v.m "
      "FROM ("
      "  SELECT a AS value, a AS key FROM t WHERE a > 100 "
      "  UNION ALL "
      "  SELECT x AS value, x AS key FROM u WHERE x > 100"
      ") q "
      "LEFT JOIN v ON q.value = v.n AND q.key = v.m";
  SCOPED_TRACE(query);

  AXIOM_ASSERT_PLAN(
      plan(query),
      matchScan("t")
          .filter("a > 100")
          .project({"a as value", "a as key"})
          .localPartition(matchScan("u").filter("x > 100").project(
              {"x as value", "x as key"}))
          .hashJoinLeft(
              matchScan("v").filter("\"and\"(n > 100, m > 100)"),
              {.keys = std::vector<std::string>{"value = n", "key = m"}})
          .build());
}

// Filters guaranteed by either input of a filtering semi join constrain the
// other input.
TEST_F(JoinFilterPropagationTest, semiFilter) {
  const std::vector<std::string> queries{
      "SELECT t.a "
      "FROM (SELECT * FROM t WHERE a > 100) t "
      "WHERE EXISTS (SELECT 1 FROM u WHERE u.x = t.a)",
      "SELECT t.a "
      "FROM t "
      "WHERE EXISTS ("
      "  SELECT 1 FROM (SELECT * FROM u WHERE x > 100) u "
      "  WHERE u.x = t.a)",
      "SELECT t.a "
      "FROM (SELECT * FROM t WHERE a > 100) t "
      "WHERE t.a IN (SELECT x FROM u)",
  };

  for (const auto& query : queries) {
    SCOPED_TRACE(query);
    AXIOM_ASSERT_PLAN(
        plan(query),
        matchScan("t")
            .filter("a > 100")
            .hashJoin(
                matchScan("u").filter("x > 100"),
                core::JoinType::kLeftSemiFilter)
            .build());
  }
}

// A non-null-aware semi-project join may restrict its matching input.
TEST_F(JoinFilterPropagationTest, semiProject) {
  const auto query =
      "SELECT t.a, EXISTS (SELECT 1 FROM u WHERE u.x = t.a) AS matched "
      "FROM (SELECT * FROM t WHERE a > 100) t";
  SCOPED_TRACE(query);

  AXIOM_ASSERT_PLAN(
      plan(query),
      matchScan("t")
          .filter("a > 100")
          .hashJoin(
              matchScan("u").filter("x > 100"),
              core::JoinType::kLeftSemiProject,
              {.nullAware = false})
          .project()
          .build());
}

// A non-null-aware anti join may restrict its matching input.
TEST_F(JoinFilterPropagationTest, anti) {
  const auto query =
      "SELECT t.a "
      "FROM (SELECT * FROM t WHERE a > 100) t "
      "WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.x = t.a)";
  SCOPED_TRACE(query);

  AXIOM_ASSERT_PLAN(
      plan(query),
      matchScan("t")
          .filter("a > 100")
          .hashJoin(
              matchScan("u").filter("x > 100"),
              core::JoinType::kAnti,
              {.nullAware = false})
          .build());
}

// A NULL outside the translated range still changes an IN or NOT IN result,
// so null-aware project and anti joins retain it.
TEST_F(JoinFilterPropagationTest, nullAware) {
  {
    const auto query =
        "SELECT t.a, t.a IN (SELECT x FROM u) AS matched "
        "FROM (SELECT * FROM t WHERE a > 100) t";
    SCOPED_TRACE(query);

    AXIOM_ASSERT_PLAN(
        plan(query),
        matchScan("t")
            .filter("a > 100")
            .hashJoin(
                matchScan("u"),
                core::JoinType::kLeftSemiProject,
                {.nullAware = true})
            .project()
            .build());
  }

  {
    const auto query =
        "SELECT t.a "
        "FROM (SELECT * FROM t WHERE a > 100) t "
        "WHERE t.a NOT IN (SELECT x FROM u)";
    SCOPED_TRACE(query);

    AXIOM_ASSERT_PLAN(
        plan(query),
        matchScan("t")
            .filter("a > 100")
            .hashJoin(
                matchScan("u"), core::JoinType::kAnti, {.nullAware = true})
            .build());
  }
}

// Counting semi and anti joins use the same equality-based restriction while
// preserving multiset cardinalities.
TEST_F(JoinFilterPropagationTest, countingJoins) {
  {
    const auto query =
        "SELECT a FROM (SELECT a FROM t WHERE a > 100) "
        "INTERSECT ALL SELECT x FROM u";
    SCOPED_TRACE(query);

    auto matcher = matchScan("t")
                       .filter("a > 100")
                       .hashJoin(
                           matchScan("u").filter("x > 100"),
                           core::JoinType::kCountingLeftSemiFilter)
                       .build();

    AXIOM_ASSERT_PLAN(plan(query), matcher);
  }

  {
    const auto query =
        "SELECT a FROM (SELECT a FROM t WHERE a > 100) "
        "EXCEPT ALL SELECT x FROM u";
    SCOPED_TRACE(query);

    auto matcher =
        matchScan("t")
            .filter("a > 100")
            .hashJoin(
                matchScan("u").filter("x > 100"), core::JoinType::kCountingAnti)
            .build();

    AXIOM_ASSERT_PLAN(plan(query), matcher);
  }
}

// The same propagation applies when a semi join preserves its right input.
TEST_F(JoinFilterPropagationTest, rightSemi) {
  testConnector_->addTable("small", ROW({"a", "b"}, BIGINT()))
      ->setStats(100, {{"a", {.numDistinct = 100}}});
  testConnector_->addTable("large", ROW({"x", "y"}, BIGINT()))
      ->setStats(10'000, {{"x", {.numDistinct = 1'000}}});

  {
    const auto query =
        "SELECT t.a "
        "FROM (SELECT * FROM small WHERE a > 10) t "
        "WHERE t.a IN (SELECT u.x FROM large u WHERE u.x = t.a)";
    SCOPED_TRACE(query);

    auto matcher = matchScan("large")
                       .filter("x > 10")
                       .hashJoin(
                           matchScan("small").filter("a > 10"),
                           core::JoinType::kRightSemiFilter)
                       .build();

    AXIOM_ASSERT_PLAN(plan(query), matcher);
  }

  {
    const auto query =
        "SELECT t.a, "
        "  t.a IN (SELECT u.x FROM large u WHERE u.x = t.a) AS matched "
        "FROM (SELECT * FROM small WHERE a > 10) t";
    SCOPED_TRACE(query);

    auto matcher = matchScan("large")
                       .filter("x > 10")
                       .hashJoin(
                           matchScan("small").filter("a > 10"),
                           core::JoinType::kRightSemiProject,
                           {.nullAware = false})
                       .project()
                       .build();

    AXIOM_ASSERT_PLAN(plan(query), matcher);
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
