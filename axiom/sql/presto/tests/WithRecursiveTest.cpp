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

#include "axiom/sql/presto/tests/ExpectPrestoSqlError.h"
#include "axiom/sql/presto/tests/LogicalPlanMatcher.h"
#include "axiom/sql/presto/tests/PrestoParserTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace axiom::sql::presto::test {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

namespace {

class WithRecursiveTest : public PrestoParserTestBase {};

TEST_F(WithRecursiveTest, simpleCounter) {
  // Self-contained recursion — no base table needed.
  auto step = lp::test::LogicalPlanMatcherBuilder()
                  .recursiveRef("counter")
                  .filter()
                  .project()
                  .build();
  testSelect(
      R"(
        WITH RECURSIVE counter(n) AS (
          SELECT CAST(1 AS BIGINT)
          UNION ALL
          SELECT n + 1 FROM counter WHERE n < 10
        )
        SELECT * FROM counter
      )",
      matchValues().project().project().fixedPoint(step, {"n"}).output());
}

TEST_F(WithRecursiveTest, joinInStep) {
  // Step body joins the recursive reference with a base table.
  auto step = lp::test::LogicalPlanMatcherBuilder()
                  .recursiveRef("anc")
                  .join(matchScan("nation").build())
                  .project()
                  .build();
  testSelect(
      R"(
        WITH RECURSIVE anc(nation_key, region_key) AS (
          SELECT n_nationkey, n_regionkey
          FROM nation
          WHERE n_regionkey = 0
          UNION ALL
          SELECT n.n_nationkey, n.n_regionkey
          FROM anc a
          JOIN nation n ON a.nation_key = n.n_nationkey
        )
        SELECT nation_key, region_key FROM anc
      )",
      matchScan("nation")
          .filter()
          .project()
          .project()
          .fixedPoint(step, {"nation_key", "region_key"})
          .project()
          .output());
}

TEST_F(WithRecursiveTest, qualifiedReferenceToCte) {
  // Qualified reference (`SELECT r.x FROM r`) to a recursive CTE resolves.
  const auto sql = R"(
        WITH RECURSIVE r(x, y) AS (
          SELECT n_nationkey, n_regionkey FROM nation WHERE n_nationkey = 0
          UNION ALL
          SELECT n.n_nationkey, n.n_regionkey FROM r JOIN nation n ON r.x = n.n_nationkey
        )
        SELECT r.x, r.y FROM r
      )";

  auto step = lp::test::LogicalPlanMatcherBuilder()
                  .recursiveRef("r")
                  .join(matchScan("nation").build())
                  .project()
                  .build();
  testSelect(
      sql,
      matchScan("nation")
          .filter()
          .project()
          .project()
          .fixedPoint(step, {"x", "y"})
          .project()
          .output({"x", "y"}));
}

TEST_F(WithRecursiveTest, columnAliasesVisibleInOuterQuery) {
  // Column aliases applied to the anchor must be visible as the
  // FixedPointNode's output column names and the outer SELECT's output.
  const auto sql = R"(
        WITH RECURSIVE r(x, y) AS (
          SELECT n_nationkey, n_regionkey FROM nation WHERE n_nationkey = 0
          UNION ALL
          SELECT n.n_nationkey, n.n_regionkey FROM r JOIN nation n ON r.x = n.n_nationkey
        )
        SELECT x, y FROM r
      )";

  auto step = lp::test::LogicalPlanMatcherBuilder()
                  .recursiveRef("r")
                  .join(matchScan("nation").build())
                  .project()
                  .build();
  testSelect(
      sql,
      matchScan("nation")
          .filter()
          .project()
          .project()
          .fixedPoint(step, {"x", "y"})
          .project()
          .output({"x", "y"}));
}

TEST_F(WithRecursiveTest, cteWithOuterFilter) {
  // WHERE in the outer query must not be pushed into the FixedPointNode.
  // The anchor matcher chain (matchValues().project().project()) pins the
  // anchor's exact shape -- a Filter anywhere inside the anchor would fail
  // the match.
  auto step = lp::test::LogicalPlanMatcherBuilder()
                  .recursiveRef("r")
                  .filter()
                  .project()
                  .build();
  testSelect(
      R"(
    WITH RECURSIVE r(a) AS (
      SELECT CAST(0 AS BIGINT)
      UNION ALL
      SELECT a + 1 FROM r WHERE a < 100
    )
    SELECT a FROM r WHERE a > 50
  )",
      matchValues()
          .project()
          .project()
          .fixedPoint(step, {"a"})
          .filter()
          .project()
          .output());
}

TEST_F(WithRecursiveTest, rejectsUnionDistinct) {
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parseSelect(R"(
        WITH RECURSIVE r(a) AS (
          SELECT CAST(1 AS BIGINT)
          UNION
          SELECT a + 1 FROM r
        )
        SELECT * FROM r
      )"),
      "body must be UNION ALL");
}

TEST_F(WithRecursiveTest, rejectsIntersect) {
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parseSelect(R"(
        WITH RECURSIVE r(a) AS (
          SELECT CAST(1 AS BIGINT)
          INTERSECT
          SELECT a + 1 FROM r
        )
        SELECT * FROM r
      )"),
      "body must be UNION ALL");
}

TEST_F(WithRecursiveTest, rejectsExcept) {
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parseSelect(R"(
        WITH RECURSIVE r(a) AS (
          SELECT CAST(1 AS BIGINT)
          EXCEPT
          SELECT a + 1 FROM r
        )
        SELECT * FROM r
      )"),
      "body must be UNION ALL");
}

TEST_F(WithRecursiveTest, rejectsNesting) {
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parseSelect(R"(
        WITH RECURSIVE outer_r(a) AS (
          SELECT CAST(1 AS BIGINT)
          UNION ALL
          SELECT a + 1 FROM outer_r WHERE a IN (
            WITH RECURSIVE inner_r(b) AS (
              SELECT CAST(1 AS BIGINT)
              UNION ALL
              SELECT b + 1 FROM inner_r
            )
            SELECT b FROM inner_r
          )
        )
        SELECT * FROM outer_r
      )"),
      "WITH RECURSIVE does not support nested recursive CTEs");
}

// Rejection: SELECT DISTINCT at the top of the recursive term changes the
// per-iteration semantics (only first-iteration's distinct rows survive),
// so ANSI forbids it.
TEST_F(WithRecursiveTest, rejectsDistinctInRecursiveTerm) {
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parseSelect(R"(
        WITH RECURSIVE r(a) AS (
          SELECT CAST(0 AS BIGINT)
          UNION ALL
          SELECT DISTINCT a + 1 FROM r WHERE a < 5
        )
        SELECT * FROM r
      )"),
      "WITH RECURSIVE recursive term does not support DISTINCT");
}

// Rejection: GROUP BY at the top of the recursive term implies aggregation
// across the working table, which ANSI forbids.
TEST_F(WithRecursiveTest, rejectsGroupByInRecursiveTerm) {
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parseSelect(R"(
        WITH RECURSIVE r(a) AS (
          SELECT CAST(0 AS BIGINT)
          UNION ALL
          SELECT a + 1 FROM r WHERE a < 5 GROUP BY a
        )
        SELECT * FROM r
      )"),
      "WITH RECURSIVE recursive term does not support GROUP BY");
}

// Rejection: ORDER BY inside the recursive term is meaningless (order
// across iterations is the executor's concern) and ANSI forbids it.
TEST_F(WithRecursiveTest, rejectsOrderByInRecursiveTerm) {
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parseSelect(R"(
        WITH RECURSIVE r(a) AS (
          SELECT CAST(0 AS BIGINT)
          UNION ALL
          SELECT a + 1 FROM r WHERE a < 5 ORDER BY a
        )
        SELECT * FROM r
      )"),
      "WITH RECURSIVE does not support ORDER BY in the recursive CTE");
}

// Rejection: LIMIT inside the recursive term short-circuits iteration in a
// way ANSI does not specify; reject so users use max_recursion_depth or an
// outer LIMIT instead.
TEST_F(WithRecursiveTest, rejectsLimitInRecursiveTerm) {
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parseSelect(R"(
        WITH RECURSIVE r(a) AS (
          SELECT CAST(0 AS BIGINT)
          UNION ALL
          SELECT a + 1 FROM r WHERE a < 5 LIMIT 10
        )
        SELECT * FROM r
      )"),
      "WITH RECURSIVE does not support LIMIT in the recursive CTE");
}

// Rejection: OFFSET inside the recursive term has the same problems as
// LIMIT (truncates per-iteration output unpredictably). ANSI forbids it.
TEST_F(WithRecursiveTest, rejectsOffsetInRecursiveTerm) {
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parseSelect(R"(
        WITH RECURSIVE r(a) AS (
          SELECT CAST(0 AS BIGINT)
          UNION ALL
          SELECT a + 1 FROM r WHERE a < 5 OFFSET 1
        )
        SELECT * FROM r
      )"),
      "WITH RECURSIVE does not support OFFSET in the recursive CTE");
}

// Rejection: HAVING in the recursive term implies aggregation over the
// working table (the single implicit group), which ANSI forbids alongside
// GROUP BY.
TEST_F(WithRecursiveTest, rejectsHavingInRecursiveTerm) {
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parseSelect(R"(
        WITH RECURSIVE r(a) AS (
          SELECT CAST(0 AS BIGINT)
          UNION ALL
          SELECT a + 1 FROM r WHERE a < 5 HAVING COUNT(*) > 0
        )
        SELECT * FROM r
      )"),
      "WITH RECURSIVE recursive term does not support HAVING");
}

// Rejection: window functions in the recursive term have ill-defined
// semantics -- the PARTITION/ORDER spans only the per-iteration working
// table, not the accumulated set. ANSI forbids them.
TEST_F(WithRecursiveTest, rejectsWindowFunctionInRecursiveTerm) {
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parseSelect(R"(
        WITH RECURSIVE r(a, rn) AS (
          SELECT CAST(0 AS BIGINT), CAST(0 AS BIGINT)
          UNION ALL
          SELECT a + 1, ROW_NUMBER() OVER () FROM r WHERE a < 5
        )
        SELECT * FROM r
      )"),
      "WITH RECURSIVE recursive term does not support window functions");
}

// Rejection: aggregate over the recursive reference (e.g. COUNT/SUM/MAX
// of a column from r). Postgres and DuckDB reject this; non-monotonic over
// the per-iteration working table so the fixed point is not well-defined.
TEST_F(WithRecursiveTest, rejectsAggregateOverRecursiveRef) {
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parseSelect(R"(
        WITH RECURSIVE r(a) AS (
          SELECT CAST(0 AS BIGINT)
          UNION ALL
          SELECT MAX(a) + 1 FROM r WHERE a < 5
        )
        SELECT * FROM r
      )"),
      "WITH RECURSIVE recursive term does not support aggregate functions");
}

// Rejection: recursive reference inside an EXISTS subquery. The subquery
// re-reads the working table per outer-row, which is not what users intend.
// Postgres and DuckDB reject; we follow.
TEST_F(WithRecursiveTest, rejectsRefInExistsSubquery) {
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parseSelect(R"(
        WITH RECURSIVE r(a) AS (
          SELECT CAST(0 AS BIGINT)
          UNION ALL
          SELECT a + 1 FROM r
          WHERE a < 5 AND EXISTS (SELECT 1 FROM r WHERE a > 0)
        )
        SELECT * FROM r
      )"),
      "scalar/IN/EXISTS subquery");
}

TEST_F(WithRecursiveTest, rejectsRefInScalarSubquery) {
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parseSelect(R"(
        WITH RECURSIVE r(a) AS (
          SELECT CAST(0 AS BIGINT)
          UNION ALL
          SELECT a + (SELECT a FROM r WHERE a < 1 LIMIT 1)
          FROM r WHERE a < 5
        )
        SELECT * FROM r
      )"),
      "scalar/IN/EXISTS subquery");
}

// Rejection: recursive reference inside an IN subquery.
TEST_F(WithRecursiveTest, rejectsRefInInSubquery) {
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parseSelect(R"(
        WITH RECURSIVE r(a) AS (
          SELECT CAST(0 AS BIGINT)
          UNION ALL
          SELECT a + 1 FROM r WHERE a IN (SELECT a FROM r)
        )
        SELECT * FROM r
      )"),
      "scalar/IN/EXISTS subquery");
}

// Rejection: recursive reference on the nullable side of a LEFT JOIN.
// A NULL-padded row produced from the right side can re-join with itself in
// the next iteration, breaking fixed-point monotonicity.
TEST_F(WithRecursiveTest, rejectsRefOnNullableSideOfLeftJoin) {
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parseSelect(R"(
        WITH RECURSIVE r(a) AS (
          SELECT CAST(0 AS BIGINT)
          UNION ALL
          SELECT t.x FROM (SELECT 1 AS x) t LEFT JOIN r ON t.x = r.a
        )
        SELECT * FROM r
      )"),
      "WITH RECURSIVE recursive reference may not appear on the nullable right side of an outer join");
}

TEST_F(WithRecursiveTest, rejectsRefOnNullableSideOfRightJoin) {
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parseSelect(R"(
        WITH RECURSIVE r(a) AS (
          SELECT CAST(0 AS BIGINT)
          UNION ALL
          SELECT t.x FROM r RIGHT JOIN (SELECT 1 AS x) t ON t.x = r.a
        )
        SELECT * FROM r
      )"),
      "WITH RECURSIVE recursive reference may not appear on the nullable left side of an outer join");
}

TEST_F(WithRecursiveTest, rejectsRefOnNullableSideOfFullJoin) {
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parseSelect(R"(
        WITH RECURSIVE r(a) AS (
          SELECT CAST(0 AS BIGINT)
          UNION ALL
          SELECT t.x FROM r FULL JOIN (SELECT 1 AS x) t ON t.x = r.a
        )
        SELECT * FROM r
      )"),
      "WITH RECURSIVE recursive reference may not appear on the nullable left side of an outer join");
}

// Allowed: recursive reference inside an INNER JOIN (neither side is
// nullable). Pins the kInner case in visitJoin.
TEST_F(WithRecursiveTest, allowsRefInInnerJoin) {
  auto step = lp::test::LogicalPlanMatcherBuilder()
                  .recursiveRef("r")
                  .join(matchValues().project().build())
                  .filter()
                  .project()
                  .build();
  testSelect(
      R"(
    WITH RECURSIVE r(a) AS (
      SELECT CAST(0 AS BIGINT)
      UNION ALL
      SELECT t.x + r.a FROM r INNER JOIN (SELECT 1 AS x) t ON t.x = r.a
      WHERE r.a < 3
    )
    SELECT * FROM r
  )",
      matchValues().project().project().fixedPoint(step).output());
}

// The step branch itself uses a UNION ALL inside a subquery, combining two
// separate recursive branches before returning them to the accumulator.
TEST_F(WithRecursiveTest, unionInStepBranch) {
  auto stepRight = lp::test::LogicalPlanMatcherBuilder()
                       .recursiveRef("r")
                       .filter()
                       .project()
                       .build();
  auto step = lp::test::LogicalPlanMatcherBuilder()
                  .recursiveRef("r")
                  .project()
                  .unionAll(stepRight)
                  .project()
                  .build();
  testSelect(
      R"(
    WITH RECURSIVE r(a) AS (
      SELECT CAST(0 AS BIGINT)
      UNION ALL
      SELECT new_a
      FROM (
        SELECT a + 1 AS new_a FROM r
        UNION ALL
        SELECT a + 10 AS new_a FROM r WHERE a < 5
      ) t
    )
    SELECT * FROM r WHERE a < 20
  )",
      matchValues().project().project().fixedPoint(step).filter().output());
}

// A WITH RECURSIVE block that mixes a plain (non-recursive) CTE with a
// recursive CTE. The recursive CTE's anchor is seeded from the plain CTE.
TEST_F(WithRecursiveTest, nonRecursiveCteAsAnchorSeed) {
  auto step = lp::test::LogicalPlanMatcherBuilder()
                  .recursiveRef("r")
                  .filter()
                  .project()
                  .build();
  testSelect(
      R"(
        WITH RECURSIVE
          base(nation_key) AS (
            SELECT n_nationkey FROM nation WHERE n_regionkey = 0
          ),
          r(nation_key) AS (
            SELECT nation_key FROM base
            UNION ALL
            SELECT r2.nation_key + 1 AS nation_key
            FROM r r2
            WHERE r2.nation_key < 10
          )
        SELECT nation_key FROM r
      )",
      matchScan("nation")
          .filter()
          .project()
          .project()
          .project()
          .project()
          .fixedPoint(step)
          .project()
          .output());
}

// The RecursiveReferenceNode in the step carries the same column types as
// the anchor and the same name as the enclosing FixedPointNode.
TEST_F(WithRecursiveTest, refTypesMatchAnchor) {
  auto step = lp::test::LogicalPlanMatcherBuilder()
                  .recursiveRef("r", {"nation_key", "region_key"})
                  .join(matchScan("nation").build())
                  .project()
                  .build();
  testSelect(
      R"(
    WITH RECURSIVE r(nation_key, region_key) AS (
      SELECT n_nationkey, n_regionkey FROM nation WHERE n_nationkey = 0
      UNION ALL
      SELECT n.n_nationkey, n.n_regionkey
      FROM r
      JOIN nation n ON r.nation_key = n.n_nationkey
    )
    SELECT nation_key, region_key FROM r
  )",
      matchScan("nation")
          .filter()
          .project()
          .project()
          .fixedPoint(step, {"nation_key", "region_key"})
          .project()
          .output({"nation_key", "region_key"}));
}

// When CTE column aliases are applied, the FixedPointNode's output type carries
// the aliased names, and the RecursiveReferenceNode carries the matching types
// so step-body column resolution (e.g. `r.x`) succeeds.
TEST_F(WithRecursiveTest, aliasedSchemaConsistency) {
  auto step = lp::test::LogicalPlanMatcherBuilder()
                  .recursiveRef("r", {"x", "y"})
                  .join(matchScan("nation").build())
                  .project()
                  .build();
  testSelect(
      R"(
    WITH RECURSIVE r(x, y) AS (
      SELECT n_nationkey, n_regionkey FROM nation WHERE n_nationkey = 0
      UNION ALL
      SELECT n.n_nationkey, n.n_regionkey
      FROM r
      JOIN nation n ON r.x = n.n_nationkey
    )
    SELECT x, y FROM r
  )",
      matchScan("nation")
          .filter()
          .project()
          .project()
          .fixedPoint(step, {"x", "y"})
          .project()
          .output({"x", "y"}));
}

// Anchor and step producing incompatible types must be caught at FixedPointNode
// construction, not silently accepted.
TEST_F(WithRecursiveTest, typeMismatchRejected) {
  // The anchor column is BIGINT; the recursive term re-emits it as VARCHAR, so
  // the FixedPointNode's anchor/step schemas are incompatible.
  VELOX_ASSERT_THROW(
      parseSelect(R"(
        WITH RECURSIVE r(a) AS (
          SELECT n_nationkey FROM nation WHERE n_nationkey = 0
          UNION ALL
          SELECT CAST(a AS VARCHAR) FROM r
        )
        SELECT a FROM r
      )"),
      "equivalent output schemas");
}

TEST_F(WithRecursiveTest, columnCountMismatchRejected) {
  // The anchor produces two columns; the recursive term produces one. The
  // FixedPointNode constructor must reject the mismatched output arity.
  VELOX_ASSERT_THROW(
      parseSelect(R"(
        WITH RECURSIVE r(a, b) AS (
          SELECT n_nationkey, n_name FROM nation WHERE n_nationkey = 0
          UNION ALL
          SELECT a FROM r
        )
        SELECT a FROM r
      )"),
      "equivalent output schemas");
}

TEST_F(WithRecursiveTest, nonSelfReferentialUnionAllIsPlainCte) {
  // The CTE never references itself, so WITH RECURSIVE here is just a plain
  // UNION ALL CTE -- the positive matcher pins the plain shape (no
  // FixedPointNode).
  testSelect(
      R"(
        WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT 2)
        SELECT * FROM r
      )",
      matchValues()
          .project()
          .unionAll(matchValues().project().build())
          .project()
          .output());
}

TEST_F(WithRecursiveTest, nonRecursiveIntersectIsPlainCte) {
  // A non-self-referential INTERSECT in a WITH RECURSIVE block is a valid
  // ordinary CTE; it must lower as a plain Intersect, not be rejected as
  // "requires UNION ALL".
  testSelect(
      R"(
        WITH RECURSIVE r(n) AS (SELECT 1 INTERSECT SELECT 1)
        SELECT * FROM r
      )",
      matchValues()
          .project()
          .intersect(matchValues().project().build())
          .project()
          .output());
}

TEST_F(WithRecursiveTest, selfReferenceInAnchorRejected) {
  // The self-reference is in the anchor (left) term, not the recursive term.
  // This must be rejected with a clear error rather than "Table not found".
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parseSelect(R"(
        WITH RECURSIVE r(n) AS (SELECT n FROM r UNION ALL SELECT 1)
        SELECT * FROM r
      )"),
      "self-reference must appear in the recursive term");
}

TEST_F(WithRecursiveTest, emptyAnchor) {
  // Empty anchor (anchor produces zero rows): the FixedPoint should still
  // parse and lower; the executor decides convergence on the first iteration.
  auto step = lp::test::LogicalPlanMatcherBuilder()
                  .recursiveRef("r")
                  .filter()
                  .project()
                  .build();
  testSelect(
      R"(
    WITH RECURSIVE r(n) AS (
      SELECT CAST(1 AS BIGINT) WHERE 1 = 0
      UNION ALL
      SELECT n + 1 FROM r WHERE n < 5
    )
    SELECT * FROM r
  )",
      matchValues().filter().project().project().fixedPoint(step).output());
}

TEST_F(WithRecursiveTest, multipleSiblings) {
  // Two recursive CTEs in the same WITH RECURSIVE block; r2's anchor reads
  // r1's accumulated output. Exercises sibling recursive CTEs being
  // translated independently without leaking state across them.
  auto stepR1 = lp::test::LogicalPlanMatcherBuilder()
                    .recursiveRef("r1")
                    .filter()
                    .project()
                    .build();
  auto stepR2 = lp::test::LogicalPlanMatcherBuilder()
                    .recursiveRef("r2")
                    .filter()
                    .project()
                    .build();
  testSelect(
      R"(
    WITH RECURSIVE
      r1(a) AS (
        SELECT CAST(0 AS BIGINT)
        UNION ALL
        SELECT a + 1 FROM r1 WHERE a < 3
      ),
      r2(b) AS (
        SELECT a FROM r1
        UNION ALL
        SELECT b + 10 FROM r2 WHERE b < 30
      )
    SELECT b FROM r2
  )",
      matchValues()
          .project()
          .project()
          .fixedPoint(stepR1)
          .project()
          .project()
          .fixedPoint(stepR2)
          .project()
          .output());
}

// Presto identifiers are case-insensitive regardless of quoting. The
// recursive CTE name canonicalizes to lowercase and resolves the same
// whether the declaration or the references use mixed case or quoting.
TEST_F(WithRecursiveTest, caseInsensitiveCteName) {
  auto step = lp::test::LogicalPlanMatcherBuilder()
                  .recursiveRef("myr")
                  .filter()
                  .project()
                  .build();
  testSelect(
      R"(
    WITH RECURSIVE "MyR"(a) AS (
      SELECT CAST(0 AS BIGINT)
      UNION ALL
      SELECT a + 1 FROM MYR WHERE a < 3
    )
    SELECT * FROM myR
  )",
      matchValues().project().project().fixedPoint(step).output());
}

// Inner WITH (non-recursive) reusing the outer recursive CTE name shadows
// the outer name: inside the inner SELECT, `r` resolves to the inner CTE,
// not to the outer recursive CTE. The outer recursive `r` is therefore
// never referenced and the plan contains no FixedPointNode — the matcher
// pins the inner CTE's plain values/project shape end-to-end.
TEST_F(WithRecursiveTest, shadowedByInnerWith) {
  testSelect(
      R"(
    WITH RECURSIVE r(a) AS (
      SELECT CAST(0 AS BIGINT)
      UNION ALL
      SELECT a + 1 FROM r WHERE a < 3
    )
    SELECT * FROM (
      WITH r AS (SELECT CAST(99 AS BIGINT) AS x)
      SELECT x FROM r
    ) t
  )",
      matchValues().project({"99::bigint"}).project().output({"x"}));
}

// An inner WITH binding inside the recursive step body shadows the outer
// recursive CTE name -- the step body's `FROM r` inside the subquery
// resolves to the inner CTE's column `b`, not the outer's column `a`.
// Without an inner CTE-guard the inner `r` would route through the outer's
// RecursiveReferenceNode path and surface column `a` instead of the inner CTE's
// values subtree.
TEST_F(WithRecursiveTest, innerWithInsideStepShadows) {
  auto innerCte = matchValues().project().project().build();
  auto step = lp::test::LogicalPlanMatcherBuilder()
                  .recursiveRef("r")
                  .join(innerCte)
                  .filter()
                  .project()
                  .build();
  testSelect(
      R"(
    WITH RECURSIVE r(a) AS (
      SELECT CAST(0 AS BIGINT)
      UNION ALL
      SELECT r.a + 1 FROM r CROSS JOIN (
        WITH r AS (SELECT 99 AS b) SELECT b FROM r
      ) inner_r
      WHERE r.a < 3
    )
    SELECT * FROM r
  )",
      matchValues().project().project().fixedPoint(step).output());
}

// A nested non-recursive CTE shadows an outer recursive CTE and references that
// name in its body. The reference resolves to the outer recursive CTE and
// expands into a FixedPointNode. It reads column n, which only the outer CTE
// provides, so the binding is observable.
TEST_F(WithRecursiveTest, nestedShadowsRecursiveCte) {
  auto step = lp::test::LogicalPlanMatcherBuilder()
                  .recursiveRef("r")
                  .filter()
                  .project()
                  .build();
  testSelect(
      R"(
        WITH RECURSIVE r(n) AS (
          SELECT CAST(1 AS BIGINT)
          UNION ALL
          SELECT n + 1 FROM r WHERE n < 10
        )
        SELECT * FROM (WITH r AS (SELECT n AS m FROM r) SELECT m FROM r) sub
      )",
      matchValues()
          .project()
          .project()
          .fixedPoint(step, {"n"})
          .project({"n"})
          .project()
          .output({"m"}));
}

// Constructs that ANSI forbids in the recursive term (DISTINCT, ORDER BY,
// LIMIT) are allowed inside a FROM-clause subquery within the step, because
// a subquery is a sealed scope.
TEST_F(WithRecursiveTest, allowsRestrictedConstructsInsideStepSubquery) {
  auto step =
      matchValues()
          .project()
          .project()
          .distinct()
          .sort()
          .limit(0, 1)
          .join(lp::test::LogicalPlanMatcherBuilder().recursiveRef("r").build())
          .filter()
          .project()
          .build();
  testSelect(
      R"(
    WITH RECURSIVE r(a) AS (
      SELECT CAST(0 AS BIGINT)
      UNION ALL
      SELECT a + 1 FROM (
        SELECT DISTINCT n
        FROM (SELECT 1 AS n) t
        ORDER BY n
        LIMIT 1
      ) sub
      CROSS JOIN r
      WHERE a < 3
    )
    SELECT * FROM r
  )",
      matchValues().project().project().fixedPoint(step).output());
}

TEST_F(WithRecursiveTest, nonRecursiveCte) {
  // A plain-SELECT CTE in a WITH RECURSIVE block lowers to the same shape as
  // an ordinary non-recursive CTE — no FixedPointNode is introduced.
  testSelect(
      "WITH RECURSIVE t AS (SELECT 1 AS x) SELECT * FROM t",
      matchValues().project().output());
}

// Outer query that references the recursive relation twice via UNION ALL.
// Each reference must produce its own FixedPointNode subtree -- a shared
// subtree would silently produce a DAG plan that the execution layer cannot
// run.
TEST_F(WithRecursiveTest, doubleReferenceToRecursiveCte) {
  auto step = lp::test::LogicalPlanMatcherBuilder()
                  .recursiveRef("r")
                  .filter()
                  .project()
                  .build();
  auto rightBranch =
      matchValues().project().project().fixedPoint(step).project().build();
  testSelect(
      R"(
    WITH RECURSIVE r(a) AS (
      SELECT CAST(0 AS BIGINT)
      UNION ALL
      SELECT a + 1 FROM r WHERE a < 3
    )
    SELECT a FROM r
    UNION ALL
    SELECT a FROM r
  )",
      matchValues()
          .project()
          .project()
          .fixedPoint(step)
          .project()
          .unionAll(rightBranch)
          .output());
}

// Self-join over a recursive CTE: each reference site is independently
// translated, so the two references have disjoint column ids. A shared
// FixedPointNode subtree would surface as a JoinNode duplicate-name error.
TEST_F(WithRecursiveTest, selfJoinOnRecursiveCte) {
  auto step = lp::test::LogicalPlanMatcherBuilder()
                  .recursiveRef("r")
                  .filter()
                  .project()
                  .build();
  auto rightFixedPoint =
      matchValues().project().project().fixedPoint(step).build();
  testSelect(
      R"(
    WITH RECURSIVE r(a) AS (
      SELECT CAST(0 AS BIGINT)
      UNION ALL
      SELECT a + 1 FROM r WHERE a < 3
    )
    SELECT * FROM r r1 JOIN r r2 ON r1.a = r2.a
  )",
      matchValues()
          .project()
          .project()
          .fixedPoint(step)
          .join(rightFixedPoint)
          .output());
}

} // namespace
} // namespace axiom::sql::presto::test
