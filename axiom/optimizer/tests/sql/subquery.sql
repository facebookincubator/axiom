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
-- NYI: no-FROM subquery body with an aggregate whose body references an
-- outer column.
-- error: Correlated aggregate in a no-FROM subquery body is not supported yet
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
-- NYI: outer-column reference in a non-inner join's ON condition inside
-- a correlated subquery.
-- error: Cannot resolve column in join input
SELECT (SELECT max(u.a) FROM u LEFT JOIN v ON v.a = t.a) FROM t
----
-- NYI: correlated subquery whose body is a UNION ALL of two branches
-- that each reference an outer column.
-- error: references table not in tableSet
SELECT (SELECT max(a) FROM (SELECT u.a FROM u WHERE u.a = t.a UNION ALL SELECT v.a FROM v WHERE v.a = t.a)) FROM t
----
-- NYI: outer-column reference in the SELECT of an IN subquery.
-- error: Outer-column reference in the SELECT of an IN subquery is not supported yet
SELECT t.a IN (SELECT u.a + t.b FROM u WHERE u.a > 0) FROM t
----
-- NYI: outer-column reference inside an aggregate body of an IN
-- subquery. The aggregate is in HAVING (not SELECT), so the SELECT
-- projection stays purely local and only the aggregate-body lift fires.
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
-- NYI: aggregate whose args reference only outer columns.
-- error: aggregate whose arguments reference only outer columns is not supported yet
SELECT (SELECT count(t.a) FROM u WHERE u.a > 999) FROM t
----
-- Multiple aggregates, each referencing an outer column.
SELECT (SELECT max(u.a + t.b) + min(u.a + t.c) FROM u WHERE u.a > 0) FROM t
----
-- NYI: multiple aggregates where one references outer and another
-- does not.
-- error: Aggregate column does not reference DT
SELECT (SELECT max(u.a + t.b) + count(1) FROM u WHERE u.a > 0) FROM t
----
-- error: Aggregate column does not reference DT
SELECT (SELECT max(u.a + t.b) + count(*) FROM u WHERE u.a > 0) FROM t
----
-- error: Aggregate column does not reference DT
SELECT (SELECT max(u.a + t.b) + sum(u.a) FROM u WHERE u.a > 0) FROM t
