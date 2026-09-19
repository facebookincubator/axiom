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
-- Same scalar subquery in both SELECT and GROUP BY must resolve as a single
-- grouping key.
SELECT COALESCE(t.a, (SELECT max(a) FROM u))
FROM t
GROUP BY COALESCE(t.a, (SELECT max(a) FROM u))
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
