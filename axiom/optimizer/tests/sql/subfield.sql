-- setup_file: common_setup.sql

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
-- Subfield resolution tests.

-- Exploded function (row_constructor) with subfield access.
-- duckdb: SELECT 1
SELECT a.x FROM (SELECT ROW(1 AS x, 2 AS y) AS a)
----
-- A lambda argument shadows a relation alias of the same name, so 't.f' is a
-- field of the argument.
-- duckdb: SELECT [5]
SELECT transform(ARRAY[CAST(ROW(5) AS ROW(f INTEGER))], t -> t.f)
FROM (VALUES (1)) t(f)
----
-- The same when the relation's 'v.f' names two columns.
-- duckdb: SELECT [5]
SELECT transform(ARRAY[CAST(ROW(5) AS ROW(f INTEGER))], v -> v.f)
FROM ((VALUES (1)) t(f) CROSS JOIN (VALUES (2)) u(f)) v
----
-- Nested aggregation across DT boundary: transform wrapping array_agg in
-- a CTE, with the result used inside an aggregate in the outer query.
-- duckdb: SELECT a, 5 AS b FROM (VALUES (1), (2), (3)) AS data(a)
WITH s AS (
    SELECT a, transform(array_agg(b), x -> x + 1) AS b
    FROM t
    GROUP BY a
)
SELECT a, sum(cardinality(b)) AS b FROM s GROUP BY a
