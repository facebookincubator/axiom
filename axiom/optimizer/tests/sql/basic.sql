-- Table t(a BIGINT, b BIGINT, c DOUBLE) with 15 rows across 3 splits:
--   a |   b |    c
--  ---+-----+------
--   1 |  10 |  1.5
--   2 |  20 |  2.5
--   3 |  30 |  3.5
--   1 |  40 |  4.5
--   2 |  50 |  5.5
--   3 |  60 |  6.5
--   1 |  70 |  7.5
--   2 |  80 |  8.5
--   3 |  90 |  9.5
--   1 | 100 | 10.5
--   2 | 110 | 11.5
--   3 | 120 | 12.5
--   1 | 130 | 13.5
--   2 | 140 | 14.5
--   3 | 150 | 15.5
--
-- Basic queries.
-- count 15
SELECT * FROM t
----
-- count 0
SELECT * FROM t LIMIT 0
----
-- Filter.
SELECT a, b FROM t WHERE a > 1
----
-- IN list with non-literal elements.
SELECT a, b FROM t WHERE 1 IN (a, a + 1)
----
-- IN list with mix of literal and non-literal elements.
SELECT a, b FROM t WHERE a IN (b, 1)
----
-- Aggregation.
SELECT a, sum(b) FROM t GROUP BY 1
----
-- Ordered results.
-- ordered
SELECT a, b FROM t ORDER BY b DESC
----
-- Window function.
SELECT a, b, sum(b) OVER (PARTITION BY a) AS total_b FROM t
----
-- Runtime error.
-- error: division by zero
SELECT a / 0 FROM t
----
-- UNION ALL.
SELECT a FROM t UNION ALL SELECT b FROM t
----
-- Named ROW constructor.
-- duckdb: SELECT {'x': 1, 'y': 2}
SELECT ROW(1 AS x, 2 AS y)
----
-- Named ROW constructor with field access.
-- duckdb: SELECT ({'x': 1, 'y': 2}).x
SELECT a.x FROM (SELECT ROW(1 AS x, 2 AS y) AS a)
----
-- CAST to different types on the same column must not be deduplicated.
SELECT ROW(CAST(a AS varchar), CAST(a AS double)) FROM t
----
-- duckdb: SELECT NULL
SELECT element_at(filter(ARRAY[1, 2, 3], x -> x > 5), 1)
----
-- error: Array subscript index out of bounds
SELECT filter(ARRAY[1, 2, 3], x -> x > 5)[1]
----
-- Subscript on MAP with DOUBLE key.
-- duckdb: SELECT 'hello'
SELECT MAP(ARRAY[CAST(1.0 AS DOUBLE)], ARRAY['hello'])[1.0]
----
-- ROW constructor with NULL fields is not NULL.
-- duckdb: SELECT true FROM t
SELECT ROW(a, CAST(null AS BIGINT)) IS NOT NULL FROM t
----
-- VALUES with compatible types coerced to common supertype.
-- duckdb: VALUES (1::bigint), (2::bigint)
SELECT * FROM (VALUES ROW(1), ROW(CAST(2 AS bigint))) AS t(x)
----
-- VALUES with complex types requiring mixed-direction coercion per child.
-- duckdb: VALUES (MAP {1::bigint: 1.0}), (MAP {2::bigint: 2.0})
SELECT * FROM (VALUES ROW(MAP(ARRAY[1], ARRAY[1.0])), ROW(MAP(ARRAY[CAST(2 AS bigint)], ARRAY[CAST(2.0 AS real)]))) AS t(x)
----
-- error: Grouping sets are not supported yet
SELECT count(*) FROM t GROUP BY GROUPING SETS ((a), ())
