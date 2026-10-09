-- setup_file: common_setup.sql
-- setup
CREATE TABLE u AS FROM (VALUES (1), (2), (3), (4), (5)) AS _(a)
----
CREATE TABLE v AS FROM (VALUES (2), (4), (6), (8), (10)) AS _(a)
-- end_setup

SELECT EXISTS(SELECT 1), EXISTS(SELECT 1), EXISTS(SELECT 3), NOT EXISTS(SELECT 1), NOT EXISTS(SELECT 1 WHERE false)
----
-- EXISTS and IN shapes, CTE alias resolution, and several subqueries in
-- one SELECT list.

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
-- Equal constant semi-join keys retain the probe rows when the build has rows.
SELECT b
FROM (VALUES (1, 10), (1, 20)) AS probe(a, b)
WHERE a IN (SELECT x FROM (VALUES (1), (1)) AS build(x))
----
-- A constant build key filters the probe key before the existence test.
SELECT b
FROM (VALUES (1, 10), (2, 20)) AS probe(a, b)
WHERE a IN (SELECT x FROM (VALUES (1), (1)) AS build(x))
----
-- Distinct constant semi-join keys cannot match.
-- count 0
SELECT b
FROM (VALUES (1, 10), (1, 20)) AS probe(a, b)
WHERE a IN (SELECT x FROM (VALUES (2), (2)) AS build(x))
----
-- A NULL constant semi-join key cannot produce a true IN result.
-- count 0
SELECT b
FROM (VALUES (1, 10), (1, 20)) AS probe(a, b)
WHERE a IN (
  SELECT x FROM (VALUES (CAST(NULL AS INTEGER))) AS build(x)
)
----
-- A null-aware marker is false when its build is empty.
SELECT a, a IN (
  SELECT x FROM (VALUES (1)) AS build(x) WHERE false
)
FROM (VALUES (1), (CAST(NULL AS INTEGER))) AS probe(a)
----
-- A NULL build key makes a nonmatching marker NULL when the build has rows.
SELECT a, a IN (
  SELECT x FROM (VALUES (CAST(NULL AS INTEGER))) AS build(x)
)
FROM (VALUES (1), (CAST(NULL AS INTEGER))) AS probe(a)
----
-- A marker over a one-row build, output above a LIMIT.
SELECT m, b FROM (SELECT b, a IN (SELECT 2) AS m FROM t) ORDER BY b LIMIT 3
----
-- The same, read by an aggregate.
SELECT count(*) FILTER (WHERE NOT m) AS c, max(b) AS mb
FROM (SELECT m, b FROM (SELECT b, a IN (SELECT 2) AS m FROM t) ORDER BY b LIMIT 3)
----
-- The same, output above a window.
SELECT m, rn
FROM (SELECT m, row_number() OVER (ORDER BY b) AS rn FROM (SELECT b, a IN (SELECT 2) AS m FROM t))
----
-- A NULL probe key is NULL when the build has rows.
SELECT CAST(NULL AS INTEGER) IN (
  SELECT x FROM (VALUES (1), (2)) AS build(x)
)
----
-- A constant non-NULL probe keeps build NULLs, which make a miss NULL.
SELECT a IN (
  SELECT a FROM u WHERE a = 2
  UNION ALL
  SELECT CAST(NULL AS BIGINT)
)
FROM t
WHERE a = 1
----
-- NOT IN keeps every probe row when its build is empty, including NULL.
SELECT a, a NOT IN (
  SELECT x FROM (VALUES (1)) AS build(x) WHERE false
)
FROM (VALUES (1), (CAST(NULL AS INTEGER))) AS probe(a)
----
-- A constant non-NULL build key rejects an equal probe, keeps a distinct
-- probe, and leaves a NULL probe unknown.
SELECT a, a NOT IN (
  SELECT x FROM (VALUES (1)) AS build(x)
)
FROM (VALUES (1), (2), (CAST(NULL AS INTEGER))) AS probe(a)
----
-- A NULL build key makes every NOT IN result unknown when the build has rows.
SELECT a, a NOT IN (
  SELECT x FROM (VALUES (CAST(NULL AS INTEGER))) AS build(x)
)
FROM (VALUES (1), (CAST(NULL AS INTEGER))) AS probe(a)
----
-- A constant non-NULL probe keeps build NULLs, which make a miss unknown.
SELECT 2 NOT IN (
  SELECT x FROM (VALUES (1), (CAST(NULL AS INTEGER))) AS build(x)
)
----
-- Same scalar subquery in both SELECT and GROUP BY must resolve as a single
-- grouping key.
SELECT COALESCE(t.a, (SELECT max(a) FROM u))
FROM t
GROUP BY COALESCE(t.a, (SELECT max(a) FROM u))
----
-- A constant computed beside an empty scalar subquery keeps its value when
-- the subquery computes the same constant before producing its null row.
SELECT (SELECT 0 FROM v WHERE a > 100) AS scalar_value, 0 AS outer_value
----
-- A scalar correlated only to a pending scalar lift pads its own value with
-- NULL without changing the equal constant computed beside it.
SELECT
  (SELECT 100 FROM u WHERE u.a = (SELECT 200 FROM v LIMIT 1)) AS scalar_value,
  100 AS outer_value
----
-- A scalar used by another scalar keeps its value through the empty-result
-- padding boundary.
SELECT (
  SELECT (SELECT min(v.a) FROM v)
  FROM u
  WHERE u.a = (SELECT min(v.a) FROM v)
)
----
-- A correlated EXISTS whose body is itself an existence test keeps the outers
-- that have a matching row.
SELECT u.a FROM u
WHERE EXISTS (SELECT 1 FROM v v2
              WHERE v2.a = u.a * 2 AND v2.a IN (SELECT v3.a FROM v v3 WHERE v3.a < 8))
----
-- The same shape written with a nested EXISTS instead of IN.
SELECT u.a FROM u
WHERE EXISTS (SELECT 1 FROM v v2
              WHERE v2.a = u.a * 2
                AND EXISTS (SELECT 1 FROM v v3 WHERE v3.a = v2.a AND v3.a < 8))
----
-- A nested EXISTS covers both an empty correlated left side and a left row
-- whose nested test finds no match.
-- error_v1: Failed to place a table
SELECT outer_u.a FROM (VALUES (0), (1), (2)) outer_u(a)
WHERE EXISTS (SELECT 1 FROM u left_u
              WHERE left_u.a = outer_u.a
                AND NOT EXISTS (SELECT 1 FROM v inner_v WHERE inner_v.a = outer_u.a))
----
-- EXISTS over a LEFT JOIN preserves rows whose right side is absent.
-- duckdb: VALUES (1), (3), (5)
SELECT outer_u.a FROM u outer_u
WHERE EXISTS (
  SELECT 1
  FROM u left_u LEFT JOIN v right_v ON right_v.a = left_u.a
  WHERE left_u.a = outer_u.a AND right_v.a IS NULL
)
----
-- EXISTS over a LEFT JOIN depends only on matching the left side.
SELECT outer_u.a FROM (VALUES (0), (1), (2)) outer_u(a)
WHERE EXISTS (
  SELECT 1
  FROM u left_u LEFT JOIN v right_v ON right_v.a = left_u.a
  WHERE left_u.a = outer_u.a
)
----
-- IN over a correlated inner join returns false for an empty left input, NULL
-- for only an unknown comparison, and true for a match.
-- error_v1: Nested correlation across subquery boundaries is not supported yet
SELECT outer_u.a, 2 * outer_u.a IN (
  SELECT right_u.a
  FROM (
    SELECT a + outer_u.a AS a
    FROM u
    WHERE a = outer_u.a
  ) left_u
  JOIN (VALUES (NULL), (0), (4)) right_u(a)
    ON right_u.a IS NULL OR left_u.a = right_u.a
)
FROM (VALUES (0), (1), (2)) outer_u(a)
----
-- A NULL on the left of IN reads NULL when the body has a row, and false when
-- the body is empty.
SELECT outer_u.a, outer_u.a IN (
  SELECT right_u.a
  FROM (SELECT t.a + outer_u.b AS a FROM (VALUES (1), (2)) t(a)) left_u
  JOIN (VALUES (1), (3)) right_u(a) ON left_u.a = right_u.a
)
FROM (
  VALUES
    (CAST(NULL AS INTEGER), 0),
    (CAST(NULL AS INTEGER), 100),
    (1, 0)
) outer_u(a, b)
----
-- A scalar subquery over more than one row fails, whether or not a projection
-- sits over the rows.
-- error_v1: Expected single row of input. Received 2 rows.
-- error_v2: Scalar subquery produced more than one row
SELECT (SELECT x + 1 FROM (VALUES (1), (2)) t(x))
----
-- Two scopes read one uncorrelated scalar subquery and are then joined. Each
-- scope evaluates it, and the columns the join sees are named apart.
-- error_v1: Duplicate column name found on join's left and right sides
WITH a AS (SELECT (SELECT sum(x) FROM (VALUES (1), (2)) s(x)) AS n),
     b AS (SELECT (SELECT sum(x) FROM (VALUES (1), (2)) s(x)) AS m)
SELECT n, m FROM a, b
----
-- A scalar subquery can appear both directly and inside another scalar
-- subquery in the same SELECT list.
SELECT
  (SELECT a FROM u ORDER BY a LIMIT 1),
  (SELECT (SELECT a FROM u ORDER BY a LIMIT 1))
----
-- A scalar subquery body and the query around it read one uncorrelated
-- scalar. The body reads its own copy, so the FULL JOIN it sits under stays
-- uncorrelated.
-- disabled_v1: crashes in join enumeration
-- duckdb: VALUES (1, 3), (2, 3)
WITH m AS (SELECT max(x) AS v FROM (VALUES (1), (2), (3)) q(x))
SELECT
  t.a,
  (SELECT count(*)
   FROM (VALUES (1), (2)) l(x)
   FULL JOIN (VALUES (1), (3)) r(y) ON l.x = r.y AND r.y < (SELECT v FROM m))
FROM (VALUES (1), (2)) t(a)
WHERE t.a < (SELECT v FROM m)
----
-- Scalar subquery and EXISTS over the same inner subquery must produce
-- distinct columns (a scalar value vs a boolean).
SELECT (SELECT max(a) FROM u), EXISTS (SELECT max(a) FROM u) FROM t
----
-- `k` inside the subquery binds to the subquery's own column, not to the
-- same-named alias in the outer SELECT.
SELECT a AS k, (SELECT max(k) FROM (VALUES (10), (20)) AS _(k))
FROM (VALUES (1), (2)) AS _(a)
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
-- A correlated scalar subquery projecting a constant reads NULL, not the
-- constant, for an outer row the subquery has no row for.
SELECT a, (SELECT 1 FROM v WHERE v.a = t.a) AS one FROM t
----
-- A correlated count(*) reads 0, not NULL, for an outer row the subquery
-- has no row for, so a HAVING on that count still sees 0.
-- error_v1: (0 vs. 1)
SELECT a, (SELECT count(*) FROM u WHERE u.a > t.a HAVING count(*) = 0) AS c FROM t
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
-- NOT EXISTS over a subquery that runs on one task.
-- count 0
SELECT 1 WHERE NOT EXISTS (SELECT a FROM t GROUP BY a LIMIT 1)
----
-- The same, over a subquery with no rows.
SELECT 1 WHERE NOT EXISTS (SELECT a FROM t WHERE a > 100 GROUP BY a LIMIT 1)
