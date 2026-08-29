-- setup
CREATE TABLE arrays AS
SELECT * FROM (VALUES
  (7, ARRAY[10, 20, 30], ARRAY[1, 2], ARRAY[ARRAY[1, 2], ARRAY[3]]),
  (8, ARRAY[30, 10], ARRAY[5], ARRAY[ARRAY[4]]),
  (9, ARRAY[40], ARRAY[6, 7, 8], ARRAY[ARRAY[5, 6]])
) AS _(x, ys, zs, nested)
-- end_setup

-- One row per element of the unnested array; 'x' is replicated.
SELECT x, y FROM arrays CROSS JOIN UNNEST(ys) AS _(y)
----
-- The unnested expression is computed before unnesting.
SELECT x, y FROM arrays CROSS JOIN UNNEST(array_distinct(ys)) AS _(y)
----
-- Unnest of the result of an unnest.
SELECT x, v FROM arrays
CROSS JOIN UNNEST(nested) AS _(y)
CROSS JOIN UNNEST(y) AS __(v)
----
-- Chained Unnests on an optional side preserve unmatched outer rows.
SELECT l.a, y
FROM (VALUES (1), (2)) AS l(a)
LEFT JOIN (
  (VALUES (1, ARRAY[ARRAY[10]])) AS r(b, xs)
  CROSS JOIN UNNEST(xs) AS u(ys)
  CROSS JOIN UNNEST(ys) AS v(y)
) ON l.a = r.b
----
-- Two arrays unnested together are zipped, and the shorter one is padded with
-- nulls. DuckDB unnests each list independently, so state the rows directly.
-- duckdb: VALUES (7, 10, 1), (7, 20, 2), (7, 30, NULL), (8, 30, 5), (8, 10, NULL), (9, 40, 6), (9, NULL, 7), (9, NULL, 8)
SELECT x, y, z FROM arrays CROSS JOIN UNNEST(ys, zs) AS _(y, z)
----
-- WITH ORDINALITY numbers the elements starting at 1. DuckDB has no such
-- clause.
-- duckdb: VALUES (7, 10, 1), (7, 20, 2), (7, 30, 3), (8, 30, 1), (8, 10, 2), (9, 40, 1)
SELECT x, y, o FROM arrays CROSS JOIN UNNEST(ys) WITH ORDINALITY AS _(y, o)
----
-- Filter on a replicated column does not depend on the unnested one.
SELECT x, y FROM arrays CROSS JOIN UNNEST(ys) AS _(y) WHERE x > 7
----
-- Filter on an unnested column can only be evaluated after unnesting.
SELECT x, y FROM arrays CROSS JOIN UNNEST(ys) AS _(y) WHERE y > 15
----
-- GROUP BY a replicated column counts the elements of each array.
SELECT x, count(*) FROM arrays CROSS JOIN UNNEST(ys) AS _(y) GROUP BY 1
----
-- GROUP BY an unnested column groups across input rows.
SELECT y, count(*) FROM arrays CROSS JOIN UNNEST(ys) AS _(y) GROUP BY 1
----
-- ORDER BY a replicated column and an unnested one.
-- ordered
SELECT x, y FROM arrays CROSS JOIN UNNEST(ys) AS _(y) ORDER BY x, y
----
-- LIMIT applies to the unnest output, not to the input rows.
-- ordered
SELECT x, y FROM arrays CROSS JOIN UNNEST(ys) AS _(y) ORDER BY x, y LIMIT 3
----
-- Unnest of a subquery that produces a single input row.
SELECT x, y FROM (SELECT * FROM arrays WHERE x = 7) CROSS JOIN UNNEST(ys) AS _(y)
----
-- Join on a replicated column, with the unnest above the join.
SELECT a.x, u.k, y
FROM arrays a
JOIN (VALUES (7), (8)) AS u(k) ON u.k = a.x
CROSS JOIN UNNEST(a.ys) AS _(y)
----
-- The same rows with the unnest written below the join, where the join reads
-- no unnested column.
SELECT a.x, u.k, y
FROM arrays a
CROSS JOIN UNNEST(a.ys) AS _(y)
JOIN (VALUES (7), (8)) AS u(k) ON u.k = a.x
----
-- Join on an unnested column.
SELECT a.x, y
FROM arrays a
CROSS JOIN UNNEST(a.ys) AS _(y)
JOIN (VALUES (10), (40)) AS u(k) ON u.k = y
----
-- `alias.*` on an UNNEST relation whose alias carries no column list.
SELECT u.* FROM (VALUES (1, ARRAY[10, 20])) AS s(a, b) CROSS JOIN UNNEST(b) AS u
----
-- Join whose other side unnests a constant array.
SELECT a.x, s
FROM arrays a
JOIN (SELECT s FROM UNNEST(ARRAY[7, 8]) AS u(s)) ON a.x = s
----
-- The same, with the unnest on the null-padded side of an outer join.
SELECT a.x, s
FROM arrays a
LEFT JOIN (SELECT s FROM UNNEST(ARRAY[7, 8]) AS u(s)) ON a.x = s
----
-- A join on both a replicated column and an unnested one, projecting a column
-- from each side.
SELECT a.x, y, u.k
FROM arrays a
CROSS JOIN UNNEST(a.ys) AS _(y)
JOIN (VALUES (7, 10), (8, 30)) AS u(k, m) ON u.k = a.x AND u.m = y
----
-- An IN subquery whose source unnests a column of another relation.
SELECT a
FROM (VALUES ('x')) AS s(a)
WHERE a IN (
  SELECT e FROM (VALUES (ARRAY['x'])) AS m(data) CROSS JOIN UNNEST(data) AS t(e))
----
-- An EXISTS subquery whose source unnests a column, correlated only by a
-- comparison on the unnested column.
SELECT x
FROM arrays a
WHERE EXISTS (
  SELECT 1
  FROM (VALUES (ARRAY[8, 5])) AS m(data) CROSS JOIN UNNEST(data) AS t(e)
  WHERE e > a.x)
----
-- Returns the innermost element that matches the joined value 1.
SELECT c
FROM arrays AS t
CROSS JOIN UNNEST(t.nested) AS n(b)
CROSS JOIN UNNEST(n.b) AS m(c)
JOIN (VALUES (1)) AS u(x) ON c = u.x
----
-- EXISTS over an UNNEST of the outer row's own array asks whether any element
-- matches. An outer row whose array has no matching element answers false.
-- error_v1: Cannot resolve column name: ys
SELECT x, EXISTS (SELECT 1 FROM UNNEST(ys) AS _(y) WHERE y > 25) FROM arrays
----
-- The same over an array that is empty or NULL for some rows: those answer
-- false rather than dropping out. DuckDB spells the empty array literal
-- differently, so state the rows directly.
-- error_v1: Cannot resolve column name: a
-- duckdb: VALUES (1, true), (2, false), (3, false), (4, false)
SELECT k, EXISTS (SELECT 1 FROM UNNEST(a) AS _(e) WHERE e > 15)
FROM (VALUES
  (1, ARRAY[10, 20]),
  (2, ARRAY[10]),
  (3, CAST(ARRAY[] AS ARRAY(INTEGER))),
  (4, CAST(NULL AS ARRAY(INTEGER)))
) AS s(k, a)
----
-- NOT EXISTS over the outer row's array. Only the row whose array holds a
-- larger element is excluded.
-- error_v1: Cannot resolve column name: ys
SELECT x FROM arrays WHERE NOT EXISTS (SELECT 1 FROM UNNEST(ys) AS _(y) WHERE y > 35)
----
-- A LATERAL subquery unnesting the outer row's array, filtered on the
-- unnested column.
-- error_v1: Unsupported PlanNode LATERAL_JOIN
SELECT x, y FROM arrays, LATERAL (SELECT e AS y FROM UNNEST(ys) AS _(e) WHERE e > 25) AS l
----
-- IN over the outer row's array is null-aware: true when an element equals
-- the left side, unknown when a NULL element or a NULL left side could be
-- hiding a match, and false when the array holds nothing at all.
-- error_v1: Cannot resolve column name: a
-- duckdb: VALUES (1, false), (2, true), (3, false), (4, NULL), (5, true), (6, NULL), (7, false)
SELECT k, v IN (SELECT e FROM UNNEST(a) AS _(e))
FROM (VALUES
  (1, 99, ARRAY[10, 20]),
  (2, 20, ARRAY[10, 20]),
  (3, 5, CAST(ARRAY[] AS ARRAY(INTEGER))),
  (4, 99, ARRAY[10, NULL]),
  (5, 20, ARRAY[NULL, 20]),
  (6, CAST(NULL AS INTEGER), ARRAY[10]),
  (7, CAST(NULL AS INTEGER), CAST(ARRAY[] AS ARRAY(INTEGER)))
) AS s(k, v, a)
----
-- IN over the outer row's array whose subquery predicate is unknown for some
-- elements. An element the predicate neither accepts nor rejects is not in the
-- subquery's result, so it cannot hide a match: the answer is unknown only when
-- an element the predicate accepts is NULL, or the left side is NULL and the
-- result holds anything at all. DuckDB unnests a single list only, so state the
-- rows directly.
-- error_v1: Cannot resolve column name: a
-- duckdb: VALUES (1, true), (2, false), (3, NULL), (4, NULL), (5, false)
SELECT k, v IN (SELECT e FROM UNNEST(a, b) AS _(e, f) WHERE f > 0)
FROM (VALUES
  (1, 20, ARRAY[20], ARRAY[1]),
  (2, 20, ARRAY[20], CAST(ARRAY[NULL] AS ARRAY(INTEGER))),
  (3, 99, CAST(ARRAY[NULL] AS ARRAY(INTEGER)), ARRAY[1]),
  (4, CAST(NULL AS INTEGER), ARRAY[10], ARRAY[1]),
  (5, CAST(NULL AS INTEGER), ARRAY[10], CAST(ARRAY[NULL] AS ARRAY(INTEGER)))
) AS s(k, v, a, b)
----
-- The subquery's predicate reads the ordinality of the unnested element.
-- DuckDB does not implement WITH ORDINALITY, so state the rows directly.
-- error_v1: Cannot resolve column name: a
-- duckdb: VALUES (1, true), (2, false)
SELECT k, EXISTS (SELECT 1 FROM UNNEST(a) WITH ORDINALITY AS _(e, o) WHERE o = 2)
FROM (VALUES (1, ARRAY[10, 20]), (2, ARRAY[30])) AS s(k, a)
----
-- Two arrays unnested together inside the subquery are zipped. DuckDB unnests
-- a single list only, so state the rows directly.
-- error_v1: Cannot resolve column name: a
-- duckdb: VALUES (1, true), (2, false)
SELECT k, EXISTS (SELECT 1 FROM UNNEST(a, b) AS _(e, f) WHERE e > f)
FROM (VALUES (1, ARRAY[10, 20], ARRAY[5, 50]), (2, ARRAY[1], ARRAY[9])) AS s(k, a, b)
