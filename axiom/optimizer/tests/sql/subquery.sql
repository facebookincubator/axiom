-- setup_file: common_setup.sql
-- setup
CREATE TABLE u AS FROM (VALUES (1), (2), (3), (4), (5)) AS _(a)
----
CREATE TABLE v AS FROM (VALUES (2), (4), (6), (8), (10)) AS _(a)
-- end_setup

-- Subquery tests.

SELECT EXISTS(SELECT 1), EXISTS(SELECT 1), EXISTS(SELECT 3), NOT EXISTS(SELECT 1), NOT EXISTS(SELECT 1 WHERE false)
----
SELECT (EXISTS(SELECT 1)) = (EXISTS(SELECT 3)) WHERE NOT EXISTS(SELECT 1 WHERE false)
----
-- EXISTS with LIMIT 0 should return false.
SELECT EXISTS(SELECT 1 LIMIT 0), NOT EXISTS(SELECT 1 LIMIT 0)
----
-- IN list with a scalar subquery and a literal.
SELECT a, b FROM t WHERE a IN ((SELECT max(a) FROM t), 1)
----
-- IN list with two scalar subqueries.
SELECT a, b FROM t WHERE a IN ((SELECT max(a) FROM t), (SELECT min(a) FROM t))
----
-- Same scalar subquery in both SELECT and GROUP BY must resolve as a single
-- grouping key.
SELECT COALESCE(t.a, (SELECT max(a) FROM u))
FROM t
GROUP BY COALESCE(t.a, (SELECT max(a) FROM u))
----
-- a <> 0 is evaluable before the single-row subquery cross join, so it must
-- install below it; otherwise the grouping key divides by the a = 0 row.
-- count 1
WITH
t AS (SELECT * FROM (VALUES 0, 1) AS _(a)),
u AS (SELECT * FROM (VALUES 10) AS _(b))
SELECT (SELECT max(b) FROM u) / a AS pt
FROM t WHERE a <> 0 GROUP BY 1
HAVING (SELECT max(b) FROM u) / a > 1
----
-- Scalar subquery and EXISTS over the same inner subquery must produce
-- distinct columns (a scalar value vs a boolean).
SELECT (SELECT max(a) FROM u), EXISTS (SELECT max(a) FROM u) FROM t
----
-- Case-insensitive CTE alias resolution.
WITH a AS (SELECT * FROM (VALUES (1)) t(a)) SELECT A.a FROM A
----
-- Case-insensitive CTE alias with wildcard expansion.
WITH a AS (SELECT * FROM (VALUES (1)) t(a)) SELECT A.* FROM A
----
-- Quoted CTE alias with wildcard expansion (Presto ignores quotes for case).
-- duckdb: WITH "UpperCase" AS (SELECT * FROM (VALUES (1, 2)) t(x, y)) SELECT "UpperCase".* FROM "UpperCase"
WITH "UpperCase" AS (SELECT * FROM (VALUES (1, 2)) t(x, y)) SELECT "uPPERcASE".* FROM "uppercase"
----
-- Case-insensitive alias with wildcard in JOIN (via processAliasedRelation).
SELECT T.* FROM (VALUES (1)) t(a) JOIN (VALUES (2)) u(b) ON true
----
-- Correlated IN subquery in SELECT with non-equality filter. Produces a
-- null-aware semi-project join with extra filter; the optimizer must not flip
-- this to a right semi-project join that is unsupported in Velox.
SELECT CASE WHEN a.x IN (SELECT t.a FROM t WHERE t.b < a.y) THEN 'p' ELSE 'f' END FROM ( VALUES ( 1, 100 ) ) a ( x, y )
----
-- Correlated scalar subquery referencing a CTE that contains a NOT IN
-- subquery.
WITH u AS (
  SELECT a FROM t WHERE a NOT IN (SELECT 5)
)
SELECT (SELECT count(*) FROM u WHERE a > v.a) FROM (SELECT 1 AS a) v
----
-- A CTE that contains a correlated scalar subquery, referenced multiple
-- times from outer scalar subqueries. Each outer reference reparses the
-- CTE body with freshly uniquified column names; the inner correlated
-- reference must resolve to each expansion's own outer column, not stay
-- bound to the first expansion's name.
WITH u AS (
  SELECT (SELECT count(*) FROM (VALUES (1)) t(a) WHERE a > u.k) AS c
  FROM (VALUES (1)) u(k)
)
SELECT
  (SELECT count(*) FROM u WHERE c > 0),
  (SELECT count(*) FROM u WHERE c = 0)
----
-- 3 levels with cross-level references and name shadowing.
-- Level 0 (v): x=20, y=30. Level 1 (u): x=10 (shadows v.x), a=5.
-- Level 2 references u.a (level 1), v.y (level 0), u.x (level 1 shadow).
SELECT
  (SELECT
    (SELECT count(*)
     FROM (VALUES (5), (15), (25)) t(b)
     WHERE b > u.a AND b > u.x AND b < v.y)
   FROM (SELECT 5 AS a, 10 AS x) u)
FROM (SELECT 20 AS x, 30 AS y) v
----
-- Multiple correlated scalar count(*) subqueries with non-equi predicates
-- in the same SELECT list, each correlating on a different outer column.
SELECT
    (SELECT count(*) FROM u WHERE u.a > t.a) AS x,
    (SELECT count(*) FROM v WHERE v.a > t.b) AS y
FROM t
----
SELECT
    (SELECT count(*) FROM u WHERE u.a > t.a) AS x,
    (SELECT count(*) FROM v WHERE v.a > t.b) AS y,
    (SELECT count(*) FROM u WHERE u.a < t.c) AS z
FROM t
----
-- Multiple subqueries in a SELECT list — pairwise shape coverage. Shapes:
-- U   = uncorrelated scalar
-- CE  = correlated equality, returns a value (no aggregation)
-- CEA = correlated equality, with aggregation
-- CN  = correlated non-equality, returns a value (no aggregation)
-- CNA = correlated non-equality, with aggregation
--
-- Blocks below cover combinations of these shapes:
--   Block A: pairs of scalar subqueries (and one triple).
--   Block B: scalar + boolean predicate (EXISTS / IN).
--   Block C: pairs of boolean predicates.
--   Block D: structural / layout variants for the heavy CNA case.
--
-- Block A.1: U + U
SELECT
    (SELECT max(u.a) FROM u) AS x,
    (SELECT min(v.a) FROM v) AS y
FROM t
----
-- Block A.2: U + CEA
SELECT
    (SELECT max(u.a) FROM u) AS x,
    (SELECT count(*) FROM u WHERE u.a = t.a) AS y
FROM t
----
-- Block A.3: U + CNA
SELECT
    (SELECT max(u.a) FROM u) AS x,
    (SELECT count(*) FROM v WHERE v.a > t.a) AS y
FROM t
----
-- Block A.4: CE + CE
SELECT
    (SELECT u.a FROM u WHERE u.a = t.a) AS x,
    (SELECT v.a FROM v WHERE v.a = t.b) AS y
FROM t
----
-- Block A.5: CE + CEA
SELECT
    (SELECT u.a FROM u WHERE u.a = t.a) AS x,
    (SELECT count(*) FROM v WHERE v.a = t.b) AS y
FROM t
----
-- Block A.6: CEA + CNA
SELECT
    (SELECT count(*) FROM u WHERE u.a = t.a) AS x,
    (SELECT count(*) FROM v WHERE v.a > t.b) AS y
FROM t
----
-- Block A.7: CN + CN
SELECT
    (SELECT u.a FROM u WHERE u.a > t.a AND u.a <= t.a + 1) AS x,
    (SELECT v.a FROM v WHERE v.a > t.b AND v.a <= t.b + 2) AS y
FROM t
----
-- Block A.8: CN + CNA
SELECT
    (SELECT u.a FROM u WHERE u.a > t.a AND u.a <= t.a + 1) AS x,
    (SELECT count(*) FROM v WHERE v.a > t.b) AS y
FROM t
----
-- Block A.9: CNA + CNA on the same outer column
SELECT
    (SELECT count(*) FROM u WHERE u.a > t.a) AS x,
    (SELECT count(*) FROM v WHERE v.a > t.a) AS y
FROM t
----
-- Block A.10: CNA + CNA on a compound outer reference
SELECT
    (SELECT count(*) FROM u WHERE u.a > t.a + t.b) AS x,
    (SELECT count(*) FROM v WHERE v.a > t.b - t.a) AS y
FROM t
----
-- Block B: scalar + boolean predicate (EXISTS / IN) in the same SELECT list.
--
-- Block B.1: CNA scalar + correlated EXISTS
SELECT
    (SELECT count(*) FROM u WHERE u.a > t.a) AS x,
    EXISTS (SELECT 1 FROM v WHERE v.a > t.b) AS y
FROM t
----
-- Block B.2: CNA scalar + correlated NOT EXISTS
SELECT
    (SELECT count(*) FROM u WHERE u.a > t.a) AS x,
    NOT EXISTS (SELECT 1 FROM v WHERE v.a > t.b) AS y
FROM t
----
-- Block B.3: CNA scalar + correlated IN
SELECT
    (SELECT count(*) FROM u WHERE u.a > t.a) AS x,
    t.a IN (SELECT v.a FROM v WHERE v.a > t.b) AS y
FROM t
----
-- Block B.4: CNA scalar + correlated NOT IN
SELECT
    (SELECT count(*) FROM u WHERE u.a > t.a) AS x,
    t.a NOT IN (SELECT v.a FROM v WHERE v.a > t.b) AS y
FROM t
----
-- Block B.5: CEA scalar + correlated EXISTS
SELECT
    (SELECT count(*) FROM u WHERE u.a = t.a) AS x,
    EXISTS (SELECT 1 FROM v WHERE v.a = t.b) AS y
FROM t
----
-- Block B.6: CN scalar + correlated EXISTS
SELECT
    (SELECT u.a FROM u WHERE u.a > t.a AND u.a <= t.a + 1) AS x,
    EXISTS (SELECT 1 FROM v WHERE v.a > t.b) AS y
FROM t
----
-- Block C: multiple boolean predicates in the SELECT list.
--
-- Block C.1: two correlated EXISTS
SELECT
    EXISTS (SELECT 1 FROM u WHERE u.a > t.a) AS x,
    EXISTS (SELECT 1 FROM v WHERE v.a > t.b) AS y
FROM t
----
-- Block C.2: two correlated IN
SELECT
    t.a IN (SELECT u.a FROM u WHERE u.a > 0) AS x,
    t.b IN (SELECT v.a FROM v WHERE v.a > 0) AS y
FROM t
----
-- Block C.3: EXISTS + IN mixed
SELECT
    EXISTS (SELECT 1 FROM u WHERE u.a > t.a) AS x,
    t.b IN (SELECT v.a FROM v WHERE v.a > 0) AS y
FROM t
----
-- Block C.4: two NOT EXISTS
SELECT
    NOT EXISTS (SELECT 1 FROM u WHERE u.a > t.a) AS x,
    NOT EXISTS (SELECT 1 FROM v WHERE v.a > t.b) AS y
FROM t
----
-- Block D: layout / structural variants for the heavy CNA case.
--
-- Block D.1: two CNA inside a single CASE expression
SELECT
    CASE
        WHEN (SELECT count(*) FROM u WHERE u.a > t.a) <= 5
         AND (SELECT count(*) FROM v WHERE v.a > t.b) <= 5
        THEN 'pass' ELSE 'fail'
    END AS r
FROM t
----
-- Block D.2: three CNA inside a single CASE expression
SELECT
    CASE
        WHEN (SELECT count(*) FROM u WHERE u.a > t.a) <= 5
         AND (SELECT count(*) FROM v WHERE v.a > t.b) <= 5
         AND (SELECT count(*) FROM u WHERE u.a > t.b) <= 5
        THEN 'pass' ELSE 'fail'
    END AS r
FROM t
----
-- Block D.3: two CNA on a compound outer reference
SELECT
    (SELECT count(*) FROM u WHERE u.a > (t.a + t.b)) AS x,
    (SELECT count(*) FROM v WHERE v.a > (t.a + t.b)) AS y
FROM t
----
-- Block D.4: mixed layout — two CNA inside a CASE plus a CNA in a separate column
SELECT
    CASE
        WHEN (SELECT count(*) FROM u WHERE u.a > t.a) <= 5
         AND (SELECT count(*) FROM v WHERE v.a > t.b) <= 5
        THEN 'pass' ELSE 'fail'
    END AS r,
    (SELECT count(*) FROM u WHERE u.a < t.c) AS z
FROM t
----
-- Correlated IN subquery with single correlation equality.
SELECT t.a IN (SELECT t2.a FROM t t2 WHERE t2.b = t.b) FROM t
----
-- Correlated NOT IN subquery with single correlation equality.
SELECT t.a NOT IN (SELECT t2.a FROM t t2 WHERE t2.b = t.b) FROM t
----
-- Correlated IN subquery with multiple correlation equalities.
SELECT t.a IN (SELECT t2.a FROM t t2 WHERE t2.b = t.b AND t2.c = t.c) FROM t
----
-- Correlated NOT IN subquery with multiple correlation equalities.
SELECT t.a NOT IN (SELECT t2.a FROM t t2 WHERE t2.b = t.b AND t2.c = t.c) FROM t
----
-- Correlated IN subquery with mixed equality and non-equality correlation.
SELECT t.a IN (SELECT t2.a FROM t t2 WHERE t2.b = t.b AND t2.c < t.c) FROM t
----
-- Correlated NOT IN subquery with mixed equality and non-equality correlation.
SELECT t.a NOT IN (SELECT t2.a FROM t t2 WHERE t2.b = t.b AND t2.c < t.c) FROM t
----
-- Correlated IN subquery where correlation equality is redundant with IN key.
SELECT t.a IN (SELECT t2.a FROM t t2 WHERE t2.a = t.a) FROM t
----
-- Correlated NOT IN subquery where correlation equality is redundant with IN key.
SELECT t.a NOT IN (SELECT t2.a FROM t t2 WHERE t2.a = t.a) FROM t
----
-- Correlated IN subquery with reversed operand order in correlation.
SELECT t.a IN (SELECT t2.a FROM t t2 WHERE t.b = t2.b) FROM t
----
-- Correlated NOT IN subquery with reversed operand order in correlation.
SELECT t.a NOT IN (SELECT t2.a FROM t t2 WHERE t.b = t2.b) FROM t
----
-- NOT IN excludes rows where the left key is NULL.
SELECT a FROM (VALUES (1), (CAST(NULL AS INTEGER)), (5)) AS l(a)
WHERE a NOT IN (SELECT b FROM (VALUES (1), (3)) AS r(b))
----
-- Uncorrelated `WHERE NOT EXISTS` over an empty subquery returns every
-- outer row.
SELECT a FROM t WHERE NOT EXISTS (SELECT 1 FROM v WHERE false)
----
-- Uncorrelated `WHERE EXISTS` over a non-empty subquery returns every
-- outer row.
SELECT a FROM t WHERE EXISTS (SELECT 1 FROM v)
----
-- Uncorrelated scalar subquery whose source needs runtime single-row
-- enforcement. Returns one row per outer row.
SELECT (SELECT u.a FROM u WHERE u.a = 1) FROM t
----
-- Scalar subquery whose SELECT references an outer column. The subquery
-- has no FROM clause: result is just the outer column.
SELECT (SELECT a) FROM t
----
-- As above, but the inner SELECT is an expression over the outer column.
SELECT (SELECT a + 1) FROM t
----
-- Scalar subquery with FROM, no correlated WHERE, projection mixes outer
-- and inner columns at top level.
SELECT (SELECT t.a + u.a FROM u WHERE u.a = 1) FROM t
----
-- Correlated WHERE plus correlated projection: outer column added to an
-- inner aggregate result.
SELECT (SELECT max(u.a) + t.a FROM u WHERE u.a = t.a) FROM t
----
-- Outer column inside an aggregate body.
SELECT (SELECT max(u.a + t.a) FROM u WHERE u.a = t.a) FROM t
----
-- Outer column inside an aggregate body AND wrapping the aggregate result.
SELECT (SELECT max(u.a + t.a) + t.a FROM u WHERE u.a = t.a) FROM t
----
-- WHERE, aggregate body, and post-aggregate residual each reference a
-- different outer column.
SELECT (SELECT max(u.a + t.b) + t.c FROM u WHERE u.a = t.a) FROM t
----
-- Aggregate body and post-aggregate residual reference different outer
-- columns; no correlated WHERE.
SELECT (SELECT max(u.a + t.b) + t.c FROM u) FROM t
----
-- Correlated projection but no correlated WHERE: outer column added to
-- an inner global aggregate.
SELECT (SELECT t.a + max(u.a) FROM u) FROM t
----
-- Correlated WHERE where the inner side of the equality is a constant
-- projection. Constant folding collapses one side, leaving a pure-outer
-- gating condition: per outer row, the scalar returns the aggregate
-- when the condition holds, else NULL.
SELECT (SELECT max(o.b) FROM (SELECT 1 AS a, 42 AS b) o WHERE o.a = t.a) FROM t
----
-- Same shape inside an IN subquery: gate fails ⇒ no inner row matches
-- ⇒ IN returns false.
SELECT t.a IN (SELECT o.a FROM (SELECT 1 AS a) o WHERE o.a = t.a) FROM t
----
-- Same shape inside an EXISTS subquery: gate fails ⇒ no inner row ⇒
-- EXISTS returns false.
SELECT EXISTS (SELECT 1 FROM (SELECT 1 AS a) o WHERE o.a = t.a) FROM t
----
-- Two-level nested correlated scalar subqueries: the innermost body
-- correlates on the middle scope's u, and the middle body correlates on
-- the top scope's t.
SELECT (SELECT (SELECT max(v.a) FROM v WHERE v.a > u.a) FROM u WHERE u.a = t.a) FROM t
----
-- No-FROM subquery body with a correlated WHERE. Per outer row the
-- WHERE filters whether the single empty-tuple row passes; the scalar
-- subquery returns the SELECT expression or NULL.
SELECT (SELECT t.a WHERE t.a = 1) FROM t
----
SELECT (SELECT t.b + 100 WHERE t.a > 1) FROM t
----
-- Pure-outer aggregate: max(t.a) binds to the outer scope. Returns
-- one row with max(t.a) over all t.
SELECT (SELECT max(t.a)) FROM t
----
-- No-FROM subquery body with a cardinality-neutral aggregate. count(*)
-- over the single empty-tuple row produces 1; per outer row the result
-- is t.a + 1.
SELECT (SELECT count(*) + t.a) FROM t
----
-- No-FROM subquery body with LIMIT 0 — the single row is cut to zero,
-- so the scalar subquery returns NULL per outer row.
SELECT (SELECT t.a LIMIT 0) FROM t
----
-- Correlated WHERE plus correlated projection over a count-style
-- aggregate. count(*) over empty input is 0 (not NULL), so per-outer-row
-- result is t.a when no matching u row exists.
SELECT (SELECT count(*) + t.a FROM u WHERE u.a = t.a) FROM t
----
-- Same shape with a correlation that no outer row matches (t.a values
-- are 1..3, u.a values are 1..5, t.a + 100 is never in u).
SELECT (SELECT count(*) + t.a FROM u WHERE u.a = t.a + 100) FROM t
----
-- Outer-column reference in a non-INNER join's ON condition inside a
-- correlated subquery.
-- error: Cannot resolve column name: a
SELECT (SELECT max(u.a) FROM u LEFT JOIN v ON v.a = t.a) FROM t
----
-- Correlated subquery whose body is a UNION ALL of two branches that each
-- reference an outer column.
-- error: Correlated reference inside a UNION ALL branch is not supported yet
SELECT (SELECT max(a) FROM (SELECT u.a FROM u WHERE u.a = t.a UNION ALL SELECT v.a FROM v WHERE v.a = t.a)) FROM t
----
-- Outer-column reference in the SELECT of an IN subquery: the
-- comparison value combines an inner column with an outer column.
SELECT t.a IN (SELECT u.a + t.b FROM u WHERE u.a > 0) FROM t
----
-- Outer-column reference inside an aggregate body of an IN subquery
-- (HAVING max(u.a + t.b) > 0).
-- error: Outer-column reference in the aggregate body of an IN subquery is not supported yet
SELECT t.a IN (SELECT u.a FROM u GROUP BY u.a HAVING max(u.a + t.b) > 0) FROM t
----
-- EXISTS ignores the subquery's SELECT projection, so an outer-column
-- reference there is harmless: row existence is decided by the
-- correlated WHERE alone.
SELECT EXISTS (SELECT u.a + t.b FROM u WHERE u.a = t.a) FROM t
----
-- A global aggregate over the (possibly empty) inner relation always
-- produces exactly one row, so EXISTS over an aggregating body is true
-- for every outer row regardless of correlation.
SELECT EXISTS (SELECT max(u.a + t.b) FROM u WHERE u.a = t.a) FROM t
----
-- Multi-arg aggregate with one arg referencing inner and another
-- referencing outer.
SELECT (SELECT min_by(u.a, t.b) FROM u WHERE u.a > 0) FROM t
----
-- The following pure-outer-aggregate queries use `-- duckdb:`
-- overrides because DuckDB's subquery form does not implement the
-- outer-scope lift consistently with its own explicit-aggregation
-- form. See https://github.com/duckdb/duckdb/issues/23063.
--
-- Pure-outer aggregate with an empty body. count over zero rows = 0.
-- duckdb: SELECT count(t.a) FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.a > 999)
SELECT (SELECT count(t.a) FROM u WHERE u.a > 999) FROM t
----
-- Pure-outer aggregate with a correlated body filter. Every outer
-- row qualifies, so max returns 3.
-- duckdb: SELECT max(t.a) FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.a = t.a)
SELECT (SELECT max(t.a) FROM u WHERE u.a = t.a) FROM t
----
-- Single-row outer: per-row evaluation gives the same answer.
SELECT (SELECT count(t.a) FROM u WHERE u.a > 0) FROM (VALUES (1)) AS t(a)
----
-- Pure-outer aggregate inside a HAVING predicate.
-- duckdb: SELECT count(*) FROM t HAVING EXISTS(SELECT 1) AND (SELECT max(a) FROM t) > 0
SELECT count(*) FROM t HAVING (SELECT max(t.a)) > 0
----
-- Pure-outer aggregate wrapped in arithmetic.
SELECT (SELECT max(t.a) + 1 FROM u WHERE u.a > 0) FROM t
----
-- Multiple pure-outer aggregates in one subquery expression, sharing
-- the body's FROM/WHERE as the EXISTS gate.
SELECT (SELECT max(t.a) - min(t.a) FROM u WHERE u.a > 0) FROM t
----
-- Pure-outer aggregate inside an ORDER BY key is not yet supported.
-- error: Cannot replace inputs on AggregateCallExpr with filter
SELECT t.a FROM t ORDER BY (SELECT max(t.a))
----
-- Multiple aggregates, each referencing an outer column.
SELECT (SELECT max(u.a + t.b) + min(u.a + t.c) FROM u WHERE u.a > 0) FROM t
----
-- Multiple aggregates where one references outer and another does not
-- (constant arg, no args, inner-only arg).
SELECT (SELECT max(u.a + t.b) + count(1) FROM u WHERE u.a > 0) FROM t
----
SELECT (SELECT max(u.a + t.b) + count(*) FROM u WHERE u.a > 0) FROM t
----
SELECT (SELECT max(u.a + t.b) + sum(u.a) FROM u WHERE u.a > 0) FROM t
----
-- Scalar subquery whose SELECT references the same inner column more
-- than once alongside an outer column.
SELECT (SELECT u.a + t.a + u.a + 1 FROM u WHERE u.a = t.a) FROM t
----
-- IN subquery in a JOIN ON clause whose correlation references a sibling
-- of the IN's outer table.
SELECT *
FROM (VALUES ('a')) AS t(a)
INNER JOIN (VALUES ('a')) AS u(k)
  ON t.a IN (
      SELECT v.b
      FROM (VALUES ('a', 'a')) AS v(k, b)
      WHERE v.k = u.k
  )
----
-- IN subquery whose SELECT references a sibling outer table ('u.b') not
-- touched by the IN's left key ('t.a') or the correlation conjunct
-- ('v.k = t.a'). For (t.a=1, u.b=1) the inner row passes 'v.k = 1' and
-- yields 1, so 't.a = 1' matches.
SELECT *
FROM (VALUES (1)) AS t(a), (VALUES (1)) AS u(b)
WHERE t.a IN (
    SELECT u.b
    FROM (VALUES (1)) AS v(k)
    WHERE v.k = t.a
)
----
-- EXISTS subquery in a JOIN ON clause whose non-equi correlation
-- references a sibling of the EXISTS's outer table.
SELECT *
FROM (VALUES (1)) AS t(a)
INNER JOIN (VALUES (1)) AS u(k)
  ON EXISTS (
      SELECT 1
      FROM (VALUES (1, 1)) AS v(b, k)
      WHERE v.k = t.a AND v.b >= u.k
  )
----
-- Scalar subquery in a JOIN ON clause whose non-equi correlation
-- references a sibling of the subquery's outer table. For (t.a=1, u.k=1)
-- the inner aggregate over 'v.k=1 AND v.b>1' is empty, so 'max(v.b)' is
-- NULL and 't.a = NULL' is unknown — no rows match.
-- count 0
SELECT *
FROM (VALUES (1)) AS t(a)
INNER JOIN (VALUES (1)) AS u(k)
  ON t.a = (
      SELECT max(v.b)
      FROM (VALUES (1, 1)) AS v(b, k)
      WHERE v.k = t.a AND v.b > u.k
  )
----
-- Same shape as above, but the inner aggregate matches: for (t.a=2, u.k=1)
-- 'v.b > 1 AND v.k = 2' selects (2, 2), so 'max(v.b)' is 2 and 't.a = 2'
-- holds. Sibling outer column 'u.k' must appear in the output row.
SELECT *
FROM (VALUES (2)) AS t(a)
INNER JOIN (VALUES (1)) AS u(k)
  ON t.a = (
      SELECT max(v.b)
      FROM (VALUES (1, 2), (2, 2)) AS v(b, k)
      WHERE v.k = t.a AND v.b > u.k
  )
----
-- Two sibling cross-joined subqueries, each containing the same scalar
-- uncorrelated subquery in WHERE.
SELECT *
FROM (SELECT a FROM t WHERE a = (SELECT max(a) FROM t)) AS u,
     (SELECT a FROM t WHERE a = (SELECT max(a) FROM t)) AS v
----
-- Correlated IN-subquery in a JOIN's ON clause whose left key is a
-- payload column from a prior LEFT JOIN's right side.
SELECT u.b
FROM (VALUES (1)) AS t(a)
LEFT JOIN (VALUES (1, 'x')) AS u(k, b) ON t.a = u.k
INNER JOIN (VALUES (1)) AS v(c)
  ON u.b IN (SELECT 'x' FROM (VALUES (1)) AS w(d) WHERE d = v.c)
----
-- Shared CTE with a nested-IN filter, referenced from both UNION legs,
-- second leg wrapping it in GROUP BY.
WITH s AS (
    SELECT x FROM (VALUES (1)) t(x) WHERE x IN (SELECT 1 WHERE 1 IN (SELECT 1))
)
SELECT x FROM s
UNION ALL
SELECT x FROM (SELECT x, sum(x) AS sx FROM s GROUP BY x) WHERE sx > 0
----
-- Same shape with a single reference inside a GROUP BY.
WITH s AS (
    SELECT x FROM (VALUES (1)) t(x) WHERE x IN (SELECT 1 WHERE 1 IN (SELECT 1))
)
SELECT x FROM (SELECT x, sum(x) AS sx FROM s GROUP BY x) WHERE sx > 0
----
-- IN with a constant left-hand side over a no-FROM subquery.
SELECT 1 WHERE 1 IN (SELECT 1)
----
-- Scalar subquery in aggregate ORDER BY expression.
SELECT array_agg(a ORDER BY a + (SELECT 1)) AS vals FROM t
----
-- Scalar subquery in aggregate ORDER BY with GROUP BY.
SELECT a, array_agg(b ORDER BY b + (SELECT 0)) AS vals FROM t GROUP BY a
----
-- Non-order-sensitive aggregate with ORDER BY containing a subquery.
SELECT sum(a ORDER BY a + (SELECT 1)) AS total FROM t
----
-- Correlated scalar subquery whose body returns more than one row per
-- outer row (multiple t.b for each t.a) must fail at runtime.
-- error: Scalar sub-query has returned multiple rows
SELECT (SELECT t2.b FROM t t2 WHERE t2.a = t.a) FROM t
----
-- Uncorrelated scalar subquery whose body returns more than one row
-- must fail at runtime.
-- error: Expected single row of input. Received 5 rows.
SELECT (SELECT a FROM u) FROM t
----
-- Correlated EXISTS over a scalar-aggregate body. EXISTS is true iff
-- the per-outer aggregate produces a row — for count(*) without
-- HAVING, that's iff u has any matching row.
SELECT t.a FROM t WHERE EXISTS (
  SELECT count(*) FROM u WHERE u.a = t.a
)
----
-- Correlated EXISTS over an aggregate body with HAVING. No `u.a` value
-- has more than one row, so HAVING is never satisfied and the result
-- is empty.
-- count 0
-- error: EXISTS over a global aggregate with HAVING or OFFSET is not supported yet
SELECT t.a FROM t WHERE EXISTS (
  SELECT count(*) FROM u WHERE u.a = t.a HAVING count(*) > 1
)
----
-- Correlated EXISTS over an aggregate body with HAVING that is
-- satisfied for outers whose key appears in u.
-- error: EXISTS over a global aggregate with HAVING or OFFSET is not supported yet
SELECT t.a FROM t WHERE EXISTS (
  SELECT count(*) FROM u WHERE u.a = t.a HAVING count(*) >= 1
)
----
-- Correlated IN over a grouping aggregate body where the IN right-
-- hand side is one of the grouping keys.
SELECT t.a FROM t WHERE t.a IN (
  SELECT u.a FROM u WHERE u.a >= t.a GROUP BY u.a
)
----
-- Correlated NOT EXISTS over an aggregate body with HAVING. Outers
-- with no surviving group are kept.
-- error: EXISTS over a global aggregate with HAVING or OFFSET is not supported yet
SELECT t.a FROM t WHERE NOT EXISTS (
  SELECT count(*) FROM u WHERE u.a = t.a HAVING count(*) > 5
)
----
-- IN whose subquery body is a scalar aggregate, so the IN right-hand
-- side column is the aggregate result itself (not a grouping key).
-- error: IN over a correlated global aggregate is not supported yet
SELECT t.a FROM t WHERE t.a IN (
  SELECT max(u.a) FROM u WHERE u.a <= t.a
)
----
-- Correlated scalar subquery whose body is a UNION ALL of two
-- branches, each filtering by an outer column.
-- error: Correlated reference inside a UNION ALL branch is not supported yet
SELECT t.a, (
  SELECT count(*) FROM (
    SELECT u.a FROM u WHERE u.a > t.a
    UNION ALL
    SELECT v.a FROM v WHERE v.a > t.a
  )
) FROM t
----
-- Correlated scalar subquery whose body has a Limit between the
-- Aggregate and the correlated Filter.
SELECT t.a, (
  SELECT max(x) FROM (
    SELECT u.a AS x FROM u WHERE u.a > t.a LIMIT 10
  )
) FROM t
----
-- Uncorrelated scalar subquery inside the UNNEST array constructor.
-- error: Unexpected expression: Subquery
SELECT v.x
FROM (VALUES (ARRAY[10, 20, 30])) s(arr)
CROSS JOIN UNNEST(ARRAY[(SELECT max(a) FROM u), arr[1]]) AS v(x)
----
-- Correlated scalar subquery inside the UNNEST array constructor;
-- correlates to the input row above the UNNEST. DuckDB rejects nested
-- lateral joins, so the expected result is hardcoded.
-- duckdb: VALUES (1, 1), (2, 2)
-- error: Unexpected expression: Subquery
SELECT s.k, v.x
FROM (VALUES (1), (2)) s(k)
CROSS JOIN UNNEST(ARRAY[(SELECT max(a) FROM u WHERE u.a = s.k)]) AS v(x)
----
-- Correlated scalar subquery with LIMIT 1 body. The no-match outer
-- must survive with NULL.
-- error: LIMIT in a correlated scalar subquery is not supported yet
SELECT u.a, (SELECT t.b FROM t WHERE t.a = u.a + 1 LIMIT 1) FROM u
----
-- Correlated EXISTS with LIMIT 1 body.
SELECT u.a FROM u WHERE EXISTS (SELECT 1 FROM t WHERE t.a > u.a LIMIT 1)
----
-- Correlated NOT EXISTS with LIMIT 1 body.
SELECT u.a FROM u WHERE NOT EXISTS (SELECT 1 FROM t WHERE t.a > u.a LIMIT 1)
----
-- A correlated scalar whose body is a Join of two correlated
-- single-row subqueries returns one row per outer.
-- error: Nested correlation across subquery boundaries is not supported yet
SELECT u.a,
       (SELECT l.b + r.b
        FROM (SELECT max(b) AS b FROM t WHERE t.a = u.a) l,
             (SELECT max(b) AS b FROM t WHERE t.a = u.a + 1) r)
FROM u
----
-- Correlated scalar whose body joins a correlated single-row subquery
-- with an uncorrelated single-row subquery. Outers with no matching
-- left row must surface a single NULL row, not |right| NULL-extended
-- rows.
-- error: Nested correlation across subquery boundaries is not supported yet
SELECT u.a,
       (SELECT l.b + r.b
        FROM (SELECT max(b) AS b FROM t WHERE t.a = u.a + 10) l,
             (SELECT max(b) AS b FROM t) r)
FROM u
----
-- Correlated EXISTS over a Join with single-side correlation.
-- error: Nested correlation across subquery boundaries is not supported yet
SELECT u.a
FROM u
WHERE EXISTS (
  SELECT 1
  FROM (SELECT t.a FROM t WHERE t.a = u.a + 1) l,
       (SELECT t.a FROM t) r)
----
-- Correlated IN whose body has a multi-conjunct Filter mixing a
-- correlation predicate with an uncorrelated predicate. Both
-- conjuncts must survive pull-through into the semi-join condition.
SELECT u.a IN (SELECT t.a FROM t WHERE t.a = u.a AND t.b > 0) FROM u
----
-- Correlated EXISTS whose body has a multi-conjunct Filter mixing a
-- correlation predicate with an uncorrelated predicate. Both
-- conjuncts must survive pull-through into the semi-join condition.
SELECT u.a FROM u
WHERE EXISTS (SELECT 1 FROM t WHERE t.a = u.a AND t.b > 0)
----
-- Correlated scalar `count(*)` body with a HAVING predicate over the
-- aggregate result.
SELECT (SELECT count(*) FROM u WHERE u.a = t.a HAVING count(*) > 0) FROM t
----
-- Correlated IN whose right-hand side is the result of a scalar
-- `count(*)` over a correlated body. For outers with no matching u
-- rows, `count(*)` returns 0 — so `t.a IN (...)` matches when t.a = 0.
-- error: IN over a correlated aggregation is not supported yet
SELECT t.a IN (SELECT count(*) FROM u WHERE u.a = t.a) FROM t
----
-- Correlated IN whose right-hand side is the aggregate result of a
-- grouping aggregate (not a grouping key).
SELECT t.a IN (
  SELECT count(*) FROM u WHERE u.a = t.a GROUP BY u.a
) FROM t
----
-- Correlated grouped aggregate whose correlation predicate references
-- an inner column outside the GROUP BY clause: t.b is consumed by the
-- aggregate (grouped by t.a only) and cannot become a join key at the
-- outer level.
-- error: Correlation predicate references a column not in GROUP BY is not supported yet
SELECT u.a IN (
  SELECT count(*) FROM t WHERE t.b = u.a GROUP BY t.a
) FROM u
----
-- Correlated IN whose body is a scalar aggregate with HAVING.
-- error: IN over a correlated aggregation is not supported yet
SELECT t.a IN (
  SELECT count(*) FROM u WHERE u.a = t.a HAVING count(*) > 0
) FROM t
----
-- Correlated NOT IN whose body is a scalar aggregate that can return
-- NULL (max over empty body). NULL on the right-hand side propagates
-- NULL through NOT IN per SQL three-valued logic.
-- error: IN over a correlated global aggregate is not supported yet
SELECT t.a NOT IN (SELECT max(u.a) FROM u WHERE u.a > t.a) FROM t
----
-- EXISTS over a correlated `count(*)` body where the WHERE
-- eliminates every body row. Scalar aggregates always emit one
-- row, so EXISTS sees it → TRUE for every outer.
SELECT EXISTS (SELECT count(*) FROM u WHERE u.a = t.a + 100) FROM t
----
-- Uncorrelated IN with a constant (table-less) left side over a real source.
-- The optimizer wraps the constant in a one-row probe to anchor the IN
-- semi-join.
SELECT 1 IN (SELECT a FROM u)
----
-- A non-constant, table-less IN left side (random()) has no plan-time value to
-- embed as a one-row probe, so planning fails with a clear error.
-- error: Non-constant table-less left side of IN <subquery> is not supported yet
SELECT random() IN (SELECT a FROM u)
