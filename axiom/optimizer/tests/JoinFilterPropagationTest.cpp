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
    return toSingleNodePlan(parseSelect(sql));
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

  {
    SCOPED_TRACE("Equality guaranteed by the preserved input");
    AXIOM_ASSERT_PLAN(
        plan(
            "SELECT * "
            "FROM (SELECT * FROM t WHERE a = b AND a > 100) t "
            "LEFT JOIN u ON t.b = u.x"),
        matchScan("t")
            .filter("a = b AND a > 100")
            .hashJoinLeft(
                matchScan("u").filter("x > 100"), {.keys = {{"b = x"}}})
            .build());
  }

  {
    SCOPED_TRACE("ON and WHERE conditions on the preserved input");
    AXIOM_ASSERT_PLAN(
        plan(
            "SELECT * "
            "FROM (SELECT * FROM t WHERE a = b) t "
            "LEFT JOIN u ON t.b = u.x AND t.a > 100 "
            "WHERE t.a < 200"),
        matchScan("t")
            .filter("a = b AND a < 200")
            .hashJoinLeft(
                matchScan("u").filter("x > 100 AND x < 200"),
                {.keys = {{"b = x"}}, .filter = "a > 100"})
            .build());
  }

  // Guard: every row of the preserved input is kept.
  {
    SCOPED_TRACE("Equality guaranteed by the null-supplying input");
    AXIOM_ASSERT_PLAN(
        plan(
            "SELECT * "
            "FROM t "
            "LEFT JOIN (SELECT * FROM u WHERE x = y AND y > 100) q "
            "ON t.a = q.x"),
        matchScan("t")
            .hashJoinLeft(
                matchScan("u").filter("x = y AND y > 100"),
                {.keys = {{"a = x"}}})
            .build());
  }
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

// Filters and equalities an input guarantees join the keys and the pending
// equalities of the join above it, so a filter reaches every input of a chain.
TEST_F(JoinFilterPropagationTest, inputFacts) {
  testConnector_->addTable("v", ROW({"n", "m"}, BIGINT()))
      ->setStats(100, {{"n", {.numDistinct = 10}}});

  const auto matchAllScansFiltered = [&]() {
    return matchScan("t")
        .filter("a > 100")
        .hashJoinInner(matchScan("v").filter("n > 100"), {.keys = {{"a = n"}}})
        .hashJoinInner(matchScan("u").filter("x > 100"), {.keys = {{"a = x"}}})
        .project({"a", "b", "a", "y", "a", "m"})
        .build();
  };

  {
    SCOPED_TRACE("Filter guaranteed by an input, equalities pending");
    AXIOM_ASSERT_PLAN(
        plan(
            "SELECT * "
            "FROM (SELECT * FROM t WHERE a > 100) t, u, v "
            "WHERE t.a = u.x AND u.x = v.n"),
        matchAllScansFiltered());
  }

  {
    SCOPED_TRACE("Equality guaranteed by an input, filter pending");
    AXIOM_ASSERT_PLAN(
        plan(
            "SELECT * "
            "FROM (SELECT * FROM t, u WHERE t.a = u.x) q, v "
            "WHERE q.x = v.n AND q.a > 100"),
        matchAllScansFiltered());
  }
}

// A filter on a column an input does not return reaches the columns it returns
// through the input's equalities.
TEST_F(JoinFilterPropagationTest, hiddenColumns) {
  testConnector_->addTable("v", ROW({"n", "m"}, BIGINT()))
      ->setStats(100, {{"n", {.numDistinct = 10}}});

  {
    SCOPED_TRACE("IN subquery that selects only its key");
    AXIOM_ASSERT_PLAN(
        plan(
            "SELECT t.a "
            "FROM t "
            "WHERE t.a IN (SELECT x FROM u WHERE x = y AND y > 100)"),
        matchScan("t")
            .filter("a > 100")
            .hashJoin(
                matchScan("u").filter("x = y AND y > 100").project({"x"}),
                core::JoinType::kLeftSemiFilter)
            .build());
  }

  {
    SCOPED_TRACE("Join that outputs one of its keys");
    AXIOM_ASSERT_PLAN(
        plan(
            "SELECT * "
            "FROM ("
            "  SELECT u.x "
            "  FROM t JOIN u ON t.a = u.x AND t.a = t.b AND t.b > 100"
            ") q "
            "JOIN v ON q.x = v.n"),
        matchScan("t")
            .filter("a = b AND b > 100")
            .project({"a"})
            .hashJoinInner(
                matchScan("u").filter("x > 100"), {.keys = {{"a = x"}}})
            .hashJoinInner(
                matchScan("v").filter("n > 100"), {.keys = {{"a = n"}}})
            .project({"a", "a", "m"})
            .build());
  }
}

// A join keeps only its matched rows under the true mark of a semi join or a
// condition that rejects the NULLs an outer join pads with, so the filters of
// those rows reach the inputs of the join above.
TEST_F(JoinFilterPropagationTest, matchedRows) {
  testConnector_->addTable("v", ROW({"n", "m"}, BIGINT()))
      ->setStats(100, {{"n", {.numDistinct = 10}}});
  testConnector_->addTable("w", ROW({"p", "q"}, BIGINT()))
      ->setStats(100, {{"p", {.numDistinct = 10}}});

  {
    SCOPED_TRACE("IN subquery below a join");
    AXIOM_ASSERT_PLAN(
        plan(
            "SELECT * "
            "FROM (SELECT * FROM t WHERE t.a IN (SELECT x FROM u WHERE x > 100)) q "
            "JOIN v ON q.a = v.n"),
        matchScan("t")
            .filter("a > 100")
            .hashJoinInner(
                matchScan("v").filter("n > 100"), {.keys = {{"a = n"}}})
            .hashJoin(
                matchScan("u").filter("x > 100"),
                core::JoinType::kLeftSemiFilter)
            .project({"a", "b", "a", "m"})
            .build());
  }

  const auto matchReducedToInner = [&]() {
    return matchScan("t")
        .filter("a > 100")
        .hashJoinInner(
            matchScan("u").filter("y > 0 AND x > 100"), {.keys = {{"a = x"}}})
        .hashJoinInner(matchScan("v").filter("n > 100"), {.keys = {{"a = n"}}})
        .project({"a", "y", "a", "m"})
        .build();
  };

  {
    SCOPED_TRACE("Outer join made inner by its subquery's WHERE");
    AXIOM_ASSERT_PLAN(
        plan(
            "SELECT * "
            "FROM ("
            "  SELECT t.a, u.y "
            "  FROM t LEFT JOIN (SELECT * FROM u WHERE x > 100) u ON t.a = u.x "
            "  WHERE u.y > 0"
            ") q "
            "JOIN v ON q.a = v.n"),
        matchReducedToInner());
  }

  {
    SCOPED_TRACE("Outer join made inner by the WHERE of the join above");
    AXIOM_ASSERT_PLAN(
        plan(
            "SELECT * "
            "FROM ("
            "  SELECT t.a, u.y "
            "  FROM t LEFT JOIN (SELECT * FROM u WHERE x > 100) u ON t.a = u.x"
            ") q "
            "JOIN v ON q.a = v.n "
            "WHERE q.y > 0"),
        matchReducedToInner());
  }

  {
    SCOPED_TRACE("Outer join made inner by the key of the join above");
    AXIOM_ASSERT_PLAN(
        plan(
            "SELECT * "
            "FROM ("
            "  SELECT t.a, u.x "
            "  FROM (SELECT * FROM t WHERE a > 100) t LEFT JOIN u ON t.a = u.x"
            ") q "
            "JOIN v ON q.x = v.n"),
        matchScan("t")
            .filter("a > 100")
            .hashJoinInner(
                matchScan("v").filter("n > 100"), {.keys = {{"a = n"}}})
            .hashJoinInner(
                matchScan("u").filter("x > 100"), {.keys = {{"a = x"}}})
            .project({"a", "a", "a", "m"})
            .build());
  }

  {
    SCOPED_TRACE("Key of a join that reports its filters to the join above");
    AXIOM_ASSERT_PLAN(
        plan(
            "SELECT * "
            "FROM ("
            "  SELECT t.a, u.x "
            "  FROM (SELECT * FROM t WHERE a > 100) t LEFT JOIN u ON t.a = u.x"
            ") q "
            "JOIN v ON q.x = v.n "
            "JOIN w ON v.n = w.p"),
        matchScan("t")
            .filter("a > 100")
            .hashJoinInner(
                matchScan("v").filter("n > 100"), {.keys = {{"a = n"}}})
            .hashJoinInner(
                matchScan("u").filter("x > 100").hashJoinInner(
                    matchScan("w").filter("p > 100"), {.keys = {{"x = p"}}}),
                {.keys = {{"a = p"}}})
            .project({"a", "a", "a", "m", "a", "q"})
            .build());
  }

  // A filter copied onto the key a LEFT join pads with rejects its padded rows,
  // so the LEFT join becomes inner.
  {
    SCOPED_TRACE("Copy onto the key a LEFT join pads with");
    AXIOM_ASSERT_PLAN(
        plan(
            "SELECT * "
            "FROM t "
            "WHERE a > 100 AND EXISTS ("
            "  SELECT 1 "
            "  FROM u LEFT JOIN (SELECT * FROM v WHERE n > 100) v ON u.x = v.n "
            "  WHERE v.n = t.a"
            ")"),
        matchScan("t")
            .filter("a > 100")
            .hashJoin(
                matchScan("u").filter("x > 100").hashJoinInner(
                    matchScan("v").filter("n > 100"), {.keys = {{"x = n"}}}),
                core::JoinType::kLeftSemiFilter)
            .build());
  }
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

// A filter that every leg of a union guarantees on some column of a class the
// leg makes equal holds on the union's output.
TEST_F(JoinFilterPropagationTest, unionClasses) {
  testConnector_->addTable("v", ROW({"n", "m"}, BIGINT()))
      ->setStats(100, {{"n", {.numDistinct = 10}}});

  AXIOM_ASSERT_PLAN(
      plan(
          "SELECT * "
          "FROM ("
          "  SELECT t.a AS c, l.x AS d "
          "  FROM (SELECT * FROM t WHERE a > 5) t, "
          "    LATERAL (SELECT * FROM u WHERE u.x = t.a) l "
          "  UNION ALL "
          "  SELECT t.a, l.x "
          "  FROM t, "
          "    LATERAL (SELECT * FROM (SELECT * FROM u WHERE x > 5) u "
          "      WHERE u.x = t.a) l"
          ") w "
          "JOIN v ON w.c = v.n"),
      matchScan("t")
          .filter("a > 5")
          .hashJoinInner(matchScan("u").filter("x > 5"), {.keys = {{"a = x"}}})
          .project({"a as c", "a as d"})
          .localPartition(
              matchScan("t")
                  .aliases({"a2"})
                  .filter("a2 > 5")
                  .hashJoinInner(
                      matchScan("u").aliases({"x2"}).filter("x2 > 5"),
                      {.keys = {{"a2 = x2"}}})
                  .project({"a2 as c", "a2 as d"}))
          .hashJoinInner(matchScan("v").filter("n > 5"), {.keys = {{"c = n"}}})
          .project({"c", "d", "c", "m"})
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

// A copy that the target input only implies through its equalities is still
// added: it gives the scan a filter on another column.
TEST_F(JoinFilterPropagationTest, impliedCopies) {
  AXIOM_ASSERT_PLAN(
      plan(
          "SELECT count(*) "
          "FROM (SELECT * FROM t WHERE a > 5) t "
          "JOIN (SELECT DISTINCT x FROM u WHERE x = y AND y > 5) u "
          "ON t.a = u.x"),
      matchScan("t")
          .filter("a > 5")
          .hashJoinInner(
              matchScan("u")
                  .filter("\"and\"(x > 5, x = y, y > 5)")
                  .project({"x"})
                  .singleAggregation({"x"}, {}),
              {.keys = {{"a = x"}}})
          .singleAggregation({}, {"count(*)"})
          .build());
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

  testConnector_->addTable("v", ROW({"n", "m"}, BIGINT()))
      ->setStats(100, {{"n", {.numDistinct = 10}}});

  // Keys that match NULL to NULL give their columns one value, so a filter on
  // one reaches the input joined on another.
  {
    SCOPED_TRACE("INTERSECT ALL keyed twice on one column, below a join");
    AXIOM_ASSERT_PLAN(
        plan(
            "SELECT * "
            "FROM ("
            "  SELECT a, b FROM t "
            "  INTERSECT ALL "
            "  SELECT x, x FROM (SELECT * FROM u WHERE x IS NULL OR x > 5) u"
            ") q "
            "JOIN v ON q.b = v.n"),
        matchScan("t")
            .filter("(a IS NULL OR a > 5) AND (b IS NULL OR b > 5)")
            .hashJoin(
                matchScan("u").filter("x IS NULL OR x > 5"),
                core::JoinType::kCountingLeftSemiFilter)
            .hashJoinInner(
                matchScan("v").filter("n > 5"), {.keys = {{"b = n"}}})
            .project({"a", "b", "b", "m"})
            .build());
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
