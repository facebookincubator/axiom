-- setup_file: common_setup.sql
-- setup
CREATE TABLE u AS FROM (VALUES (1), (2), (3), (4), (5)) AS _(a)
----
CREATE TABLE v AS FROM (VALUES (2), (4), (6), (8), (10)) AS _(a)
-- end_setup

-- LATERAL joins, subqueries in ON conditions, one uncorrelated subquery read
-- from several scopes, and correlation binding above an aggregation.

-- LATERAL join tests.
--
-- CROSS JOIN LATERAL whose no-FROM body projects an outer column.
-- error_v1: Unsupported PlanNode LATERAL_JOIN
WITH t(x) AS (VALUES (1), (2), (3))
SELECT t.x, g.y
FROM t
CROSS JOIN LATERAL (SELECT t.x + 1 AS y) g
----
-- CROSS JOIN LATERAL with a correlated WHERE; INNER drops outer rows whose
-- body is empty.
-- error_v1: Unsupported PlanNode LATERAL_JOIN
WITH t(x) AS (VALUES (1), (2), (9)),
     u(a) AS (VALUES (1), (2))
SELECT t.x, g.m
FROM t
CROSS JOIN LATERAL (SELECT u.a AS m FROM u WHERE u.a = t.x) g
----
-- CROSS JOIN LATERAL whose body returns several rows per outer row.
-- error_v1: Unsupported PlanNode LATERAL_JOIN
WITH t(x) AS (VALUES (2), (4)),
     u(a) AS (VALUES (1), (2), (3))
SELECT t.x, g.m
FROM t
CROSS JOIN LATERAL (SELECT u.a AS m FROM u WHERE u.a < t.x) g
----
-- CROSS JOIN LATERAL whose body produces multiple columns: a body column and
-- an expression combining the body with an outer column.
-- error_v1: Unsupported PlanNode LATERAL_JOIN
WITH t(x) AS (VALUES (2), (4)),
     u(a) AS (VALUES (1), (2), (3))
SELECT t.x, g.a, g.b
FROM t
CROSS JOIN LATERAL (SELECT u.a AS a, u.a + t.x AS b FROM u WHERE u.a < t.x) g
----
-- error_v1: Unsupported PlanNode LATERAL_JOIN
-- error_v2: INNER LATERAL over an Aggregate body is not yet supported
WITH t(x) AS (VALUES (1), (2)),
     u(a) AS (VALUES (1), (2), (3))
SELECT t.x, g.c
FROM t
CROSS JOIN LATERAL (SELECT count(*) AS c FROM u WHERE u.a = t.x) g
----
-- LEFT JOIN LATERAL: a matched outer fans out to several rows; an outer whose
-- body rows are all rejected by the ON survives NULL-padded.
-- error_v1: Unsupported PlanNode LATERAL_JOIN
WITH t(x) AS (VALUES (1), (4)),
     u(a) AS (VALUES (1), (2), (3), (4))
SELECT t.x, g.m
FROM t
LEFT JOIN LATERAL (SELECT u.a AS m FROM u WHERE u.a <= t.x) g ON g.m < t.x
----
-- INNER JOIN LATERAL with an ON condition combining outer and lateral
-- columns.
-- error_v1: Unsupported PlanNode LATERAL_JOIN
WITH t(x) AS (VALUES (1), (2), (3)),
     u(a) AS (VALUES (1), (2), (3), (4))
SELECT t.x, g.m
FROM t
INNER JOIN LATERAL (SELECT u.a AS m FROM u) g ON g.m = t.x + 1
----
-- A subquery inside a LATERAL ON condition is not supported.
-- error_v1: Unsupported PlanNode LATERAL_JOIN
-- error_v2: Subquery in a LATERAL join ON condition is not supported
WITH t(x) AS (VALUES (1), (2)),
     u(a) AS (VALUES (1), (2), (3))
SELECT t.x, g.m
FROM t
INNER JOIN LATERAL (SELECT u.a AS m FROM u) g ON g.m IN (SELECT t.x)
----
-- Uncorrelated scalar subquery in an outer join's ON condition.
-- error_v1: Unsupported subqueries in the ON clause of a LEFT or RIGHT join
-- duckdb: VALUES (1, 10), (2, null)
SELECT l.a, r.a FROM (VALUES 1, 2) l(a)
LEFT JOIN (VALUES 10, 20) r(a)
  ON r.a = l.a + (SELECT max(x) FROM (VALUES 8, 9) s(x))
----
-- Scalar subquery in an outer join's ON condition correlated to the left input.
-- error_v1: Failed to place a table
-- duckdb: VALUES (1, 15), (2, 15)
SELECT l.a, r.a FROM (VALUES 1, 2) l(a)
LEFT JOIN (VALUES 10, 15, 20) r(a)
  ON r.a = (SELECT max(x) FROM (VALUES 10, 15) s(x) WHERE x > l.a)
----
-- Scalar subquery in an outer join's ON condition reading both inputs is
-- unsupported: it cannot be lifted onto either one.
-- error_v1: Unsupported subqueries in the ON clause of a LEFT or RIGHT join
-- error_v2: Subquery in an outer join's ON clause referencing both inputs
SELECT l.a, r.a FROM (VALUES 1, 2) l(a)
LEFT JOIN (VALUES 10, 15, 20) r(a)
  ON l.a = (SELECT max(x) FROM (VALUES 10, 15) s(x) WHERE x > r.a)
----
-- Two references to one uncorrelated aggregate subquery in the same scope
-- read the same value.
SELECT
  (SELECT max(x) FROM (VALUES (1), (7)) s(x)) AS a,
  (SELECT max(x) FROM (VALUES (1), (7)) s(x)) + 1 AS b
FROM (VALUES (10)) r(p)
----
-- Three references to one uncorrelated aggregate subquery -- in a conditional,
-- inside another subquery's body, and in a top-level projection -- all read
-- the same value.
SELECT
  IF(
    p > (SELECT max(x) FROM (VALUES (1), (7)) s(x)),
    (
      SELECT x FROM (VALUES (1), (7)) s(x)
      WHERE x = (SELECT max(x) FROM (VALUES (1), (7)) s(x))
    ),
    2
  ) AS a,
  (SELECT max(x) FROM (VALUES (1), (7)) s(x)) AS b
FROM (VALUES (10)) r(p)
----
-- One branch of a UNION reads an uncorrelated aggregate subquery that the
-- other branch also reads from inside a subquery body, which derives a
-- different value from it.
SELECT (SELECT max(x) FROM (VALUES (1), (7)) s(x)) AS a
FROM (VALUES (1)) t1(p)
UNION ALL
SELECT (
  SELECT y - 6 FROM (VALUES (1), (7)) w(y)
  WHERE y = (SELECT max(x) FROM (VALUES (1), (7)) s(x))
) AS a
FROM (VALUES (2)) t2(q)
----
-- An uncorrelated aggregate subquery read both inside a LATERAL body and
-- outside it yields the same value in both places.
-- error_v1: Unsupported PlanNode LATERAL_JOIN
SELECT a, m, (SELECT max(x) FROM (VALUES (2), (5)) s(x)) AS o
FROM (VALUES (1)) t(a),
     LATERAL (SELECT (SELECT max(x) FROM (VALUES (2), (5)) s(x)) - 3 AS m) g
----
-- An uncorrelated aggregate subquery and an uncorrelated IN subquery in the
-- same scope. The IN reads nothing from the aggregate, so it filters the
-- outer rows rather than the aggregate's single row.
SELECT a
FROM (VALUES (1), (2), (3)) r(a)
WHERE a > (SELECT max(x) FROM (VALUES (0), (1)) s(x))
  AND a IN (SELECT y FROM (VALUES (2), (3)) w(y))
----
-- An IN subquery whose body reads the same uncorrelated aggregate as the
-- enclosing filter. The IN still tests the outer rows.
SELECT a
FROM (VALUES (1), (2), (3)) r(a)
WHERE a > (SELECT max(x) FROM (VALUES (0), (1)) s(x))
  AND a IN (
    SELECT y FROM (VALUES (2), (3)) w(y)
    WHERE y > (SELECT max(x) FROM (VALUES (0), (1)) s(x))
  )
----
-- A scalar repeated in a nested UNION leg is evaluated separately from the
-- same scalar in its enclosing UNION leg.
SELECT c_name AS name FROM (VALUES ('c')) customer(c_name)
UNION ALL
SELECT s_name AS name FROM (VALUES ('b')) supplier(s_name)
WHERE s_name = (
    SELECT max(r_name) FROM (VALUES ('a'), ('b')) region(r_name))
  AND s_name IN (
    SELECT n_name FROM (VALUES ('a'), ('b')) nation(n_name)
    WHERE n_name = (
        SELECT max(r_name) FROM (VALUES ('a'), ('b')) region(r_name))
      AND n_name <= (
        SELECT max(r_name) FROM (VALUES ('a'), ('b')) region(r_name))
    UNION ALL
    SELECT r_name FROM (VALUES ('z')) other_region(r_name)
  )
----
-- An EXISTS subquery whose body reads the same uncorrelated aggregate as the
-- enclosing filter. Its result is the same for every outer row.
SELECT a
FROM (VALUES (1), (2), (3)) r(a)
WHERE a > (SELECT max(x) FROM (VALUES (0), (1)) s(x))
  AND EXISTS (
    SELECT 1 FROM (VALUES (2), (3)) w(y)
    WHERE y > (SELECT max(x) FROM (VALUES (0), (1)) s(x))
  )
----
-- A correlated subquery in a clause evaluated above an aggregation binds to
-- the grouping key's output column. Both tables name the column 'a', so the
-- key's name is disambiguated across the aggregation.
SELECT (SELECT 1 WHERE v.a = 2)
FROM u, v
GROUP BY v.a
----
-- The same correlation in HAVING.
SELECT v.a
FROM u, v
GROUP BY v.a
HAVING (SELECT v.a) > 4
----
-- And in ORDER BY.
-- ordered
SELECT v.a
FROM u, v
GROUP BY v.a
ORDER BY (SELECT v.a) DESC
----
-- An aggregate's argument is evaluated by the aggregation, so a correlation
-- there reads the aggregation's input rather than its output.
SELECT sum((SELECT v.a))
FROM u, v
GROUP BY v.a
----
-- The same holds for an aggregate carrying a FILTER, whose predicate the
-- aggregation also evaluates.
SELECT sum((SELECT v.a)) FILTER (WHERE (SELECT v.a) > 4)
FROM u, v
GROUP BY v.a
----
-- A correlated subquery that is both a DISTINCT output and the ORDER BY key
-- is one expression, so the sort key pairs with the output it sorts.
-- ordered
SELECT DISTINCT (SELECT v.a)
FROM u, v
GROUP BY v.a
ORDER BY (SELECT v.a)
----
-- A correlation written with the table qualifier resolves against a grouping
-- key written without one. Single-table, so the key keeps its column name and
-- the aggregation has to publish the qualified name alongside it.
SELECT (SELECT 1 WHERE v.a = 2)
FROM v
GROUP BY a
----
-- A subquery in a lambda body reads a value the row already has: it is
-- evaluated once for the row, and every element of the array sees it. The
-- smallest 'v.a' is 2, so one element of each array survives. DuckDB rejects
-- a subquery in a lambda, so the expected rows are stated here.
-- duckdb: VALUES (1, 1), (2, 1), (3, 1), (4, 1), (5, 1)
-- error_v1: Unexpected expression: Subquery
SELECT u.a, cardinality(filter(ARRAY[1, 2, 3], x -> x > (SELECT min(a) FROM v)))
FROM u
----
-- The same where the subquery is correlated to the row: the array survives
-- whole for the rows whose value 'v' holds, and empties for the rest.
-- duckdb: VALUES (1, 0), (2, 3), (3, 0), (4, 3), (5, 0)
-- error_v1: Unexpected expression: Subquery
SELECT
  u.a,
  cardinality(filter(ARRAY[1, 2, 3], x -> EXISTS (SELECT 1 FROM v WHERE v.a = u.a)))
FROM u
