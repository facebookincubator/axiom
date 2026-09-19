-- setup_file: common_setup.sql
-- setup
CREATE TABLE u AS FROM (VALUES (1), (2), (3), (4), (5)) AS _(a)
----
CREATE TABLE v AS FROM (VALUES (2), (4), (6), (8), (10)) AS _(a)
-- end_setup

-- Outer-column references: correlated projections, no-FROM bodies, and
-- aggregates reading outer columns.

-- Uncorrelated scalar subquery whose source needs runtime single-row
-- enforcement. Returns one row per outer row.
SELECT (SELECT u.a FROM u WHERE u.a = 1) FROM t
----
-- The same uncorrelated scalar subquery referenced twice in one expression.
SELECT (SELECT max(u.a) FROM u) + (SELECT max(u.a) FROM u) AS s FROM t
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
-- Correlated count whose equi correlation key is computed on the subquery
-- side (u.a + 1 = t.a).
SELECT (SELECT count(*) FROM u WHERE u.a + 1 = t.a) FROM t
----
-- Correlated count with a two-column equi correlation key (self-correlated t).
SELECT (SELECT count(*) FROM t t2 WHERE t2.a = t.a AND t2.b = t.b) FROM t
----
-- Outer-column reference in a non-INNER join's ON condition inside a
-- correlated subquery.
-- error_v1: Cannot resolve column name: a
-- duckdb: SELECT 5::bigint FROM t
SELECT (SELECT max(u.a) FROM u LEFT JOIN v ON v.a = t.a) FROM t
----
-- Correlated subquery whose body is a UNION ALL of two branches that each
-- reference an outer column.
-- error_v1: Correlated reference inside a UNION ALL branch is not supported yet
-- error_v2: Correlated reference inside a UnionAll branch is not supported yet
SELECT (SELECT max(a) FROM (SELECT u.a FROM u WHERE u.a = t.a UNION ALL SELECT v.a FROM v WHERE v.a = t.a)) FROM t
----
-- Outer-column reference in the SELECT of an IN subquery: the
-- comparison value combines an inner column with an outer column.
SELECT t.a IN (SELECT u.a + t.b FROM u WHERE u.a > 0) FROM t
----
-- Outer-column reference inside an aggregate body of an IN subquery
-- (HAVING max(u.a + t.b) > 0).
-- error_v1: Outer-column reference in the aggregate body of an IN subquery is not supported yet
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
-- Pure-outer aggregate inside an ORDER BY key is not yet supported. The
-- aggregate makes the block a global aggregation, leaving 't.a' non-grouped.
-- error: Cannot resolve column: a
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
-- The same read as a value rather than a filter, over a matching list, a
-- non-matching one, and one holding only NULL.
SELECT 1 IN (SELECT 1) AS a, 1 IN (SELECT 2) AS b,
       1 IN (SELECT CAST(NULL AS INTEGER)) AS c
----
-- A constant left-hand side alongside a relation the subquery does not name.
SELECT x FROM UNNEST(ARRAY[1, 2]) AS t(x)
WHERE 1 IN (SELECT c FROM (VALUES (1), (2)) AS s(c))
----
-- The negated form keeps no row.
-- count 0
SELECT x FROM UNNEST(ARRAY[1, 2]) AS t(x)
WHERE 1 NOT IN (SELECT c FROM (VALUES (1), (2)) AS s(c))
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
