-- Subquery tests.

SELECT EXISTS(SELECT 1), EXISTS(SELECT 1), EXISTS(SELECT 3), NOT EXISTS(SELECT 1), NOT EXISTS(SELECT 1 WHERE false)
----
SELECT (EXISTS(SELECT 1)) = (EXISTS(SELECT 3)) WHERE NOT EXISTS(SELECT 1 WHERE false)
----
-- EXISTS with LIMIT 0 should return false.
SELECT EXISTS(SELECT 1 LIMIT 0), NOT EXISTS(SELECT 1 LIMIT 0)
----
-- IN list with a scalar subquery and a literal.
-- duckdb: SELECT a, b FROM t WHERE a IN ((SELECT max(a) FROM t), 1)
SELECT a, b FROM t WHERE a IN ((SELECT max(a) FROM t), 1)
----
-- IN list with two scalar subqueries.
-- duckdb: SELECT a, b FROM t WHERE a IN ((SELECT max(a) FROM t), (SELECT min(a) FROM t))
SELECT a, b FROM t WHERE a IN ((SELECT max(a) FROM t), (SELECT min(a) FROM t))
----
-- Case-insensitive CTE alias resolution.
-- duckdb: WITH a AS (SELECT * FROM (VALUES (1)) t(a)) SELECT A.a FROM A
WITH a AS (SELECT * FROM (VALUES (1)) t(a)) SELECT A.a FROM A
----
-- Case-insensitive CTE alias with wildcard expansion.
-- duckdb: WITH a AS (SELECT * FROM (VALUES (1)) t(a)) SELECT A.* FROM A
WITH a AS (SELECT * FROM (VALUES (1)) t(a)) SELECT A.* FROM A
----
-- Quoted CTE alias with wildcard expansion (Presto ignores quotes for case).
-- duckdb: WITH "UpperCase" AS (SELECT * FROM (VALUES (1, 2)) t(x, y)) SELECT "UpperCase".* FROM "UpperCase"
WITH "UpperCase" AS (SELECT * FROM (VALUES (1, 2)) t(x, y)) SELECT "uPPERcASE".* FROM "uppercase"
----
-- Case-insensitive alias with wildcard in JOIN (via processAliasedRelation).
-- duckdb: SELECT T.* FROM (VALUES (1)) t(a) JOIN (VALUES (2)) u(b) ON true
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
-- Correlated IN subquery with correlation filter.
SELECT t.a IN (SELECT t2.a FROM t t2 WHERE t2.b = t.b) FROM t
