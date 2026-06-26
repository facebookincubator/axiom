-- setup_file: common_setup.sql
-- Table t(a BIGINT, b BIGINT, c DOUBLE) with 15 rows across 3 splits:
--   a |   b |    c
--  ---+-----+------
--   1 |  10
--   2 |  20
--   3 |  30
--   1 |  40
--   2 |  50
--   3 |  60
--   1 |  70
--   2 |  80
--   3 |  90
--   1 | 100
--   2 | 110
--   3 | 120
--   1 | 130
--   2 | 140
--   3 | 150
--
-- GROUPING SETS / ROLLUP / CUBE queries.

-- Basic ROLLUP with SUM.
SELECT a, sum(b) AS s FROM t GROUP BY ROLLUP(a)
----
-- CUBE with SUM (same as ROLLUP for single key).
SELECT a, sum(b) AS s FROM t GROUP BY CUBE(a)
----
-- Explicit GROUPING SETS.
SELECT a, sum(b) AS s FROM t GROUP BY GROUPING SETS ((a), ())
----
-- ROLLUP with multiple keys.
SELECT a, b, count(*) AS s FROM t GROUP BY ROLLUP(a, b)
----
-- Multiple aggregates with ROLLUP.
SELECT a, sum(b) AS s, count(b) AS c, min(b) AS mn, max(b) AS mx FROM t GROUP BY ROLLUP(a)
----
-- Explicit GROUPING SETS with no global (empty) set.
SELECT a, b, sum(b) AS s FROM t GROUP BY GROUPING SETS ((a), (b))
----
-- Grouping key is also an aggregation input.
SELECT a, sum(a) AS s FROM t GROUP BY ROLLUP(a)
----
-- Literal aggregate argument.
SELECT a, count(1) AS c FROM t GROUP BY ROLLUP(a)
----
-- Composite aggregate argument with grouping key overlap.
SELECT a, sum(a + b) AS s FROM t GROUP BY ROLLUP(a)
----
-- All identical grouping sets — each set computes separately per SQL standard.
SELECT a, b, count(*) AS c FROM t GROUP BY GROUPING SETS ((a, b), (b, a), (a, b))
----
-- Multiple distinct sets with duplicates — preserved per SQL standard.
SELECT a, b, count(*) AS c FROM t GROUP BY GROUPING SETS ((a), (b), (a))
----
-- Duplicate keys within a single grouping set are deduplicated.
SELECT a, b, count(*) AS c FROM t GROUP BY GROUPING SETS ((a, a), (b, b), (a, b))
----
-- GROUP BY DISTINCT removes duplicate grouping sets after expansion.
-- duckdb: SELECT a, b, count(*) AS c FROM t GROUP BY a, b
SELECT a, b, count(*) AS c FROM t GROUP BY DISTINCT GROUPING SETS ((a, b), (b, a), (a, b))
----
-- GROUP BY DISTINCT with ROLLUP deduplicates expanded sets.
-- duckdb: SELECT a, sum(b) AS s FROM t GROUP BY ROLLUP(a)
SELECT a, sum(b) AS s FROM t GROUP BY DISTINCT ROLLUP(a), ROLLUP(a)
----
-- Multi-key CUBE expands to all subsets: {a,b}, {a}, {b}, {}.
SELECT a, b, sum(b) AS s FROM t GROUP BY CUBE(a, b)
----
-- WHERE filter with grouping sets.
SELECT a, sum(b) AS s FROM t WHERE a > 1 GROUP BY ROLLUP(a)
----
-- HAVING filter with grouping sets (with aggregate).
SELECT a, sum(b) AS s FROM t GROUP BY GROUPING SETS ((a), (a, b)) HAVING b IS NOT NULL
----
-- HAVING filter with grouping sets (no aggregate, keys only).
SELECT a FROM t GROUP BY GROUPING SETS ((a), (a, b)) HAVING b IS NOT NULL
----
-- HAVING + ORDER BY with grouping sets.
SELECT a AS foo FROM t GROUP BY GROUPING SETS ((a), (a, b)) HAVING b IS NOT NULL ORDER BY a DESC
----
-- GROUPING() returns bitmask: 0 if column present, 1 if aggregated.
SELECT a, GROUPING(a), sum(b) AS s FROM t GROUP BY ROLLUP(a)
----
-- GROUPING() with multiple columns.
SELECT a, b, GROUPING(a, b), sum(b) AS s FROM t GROUP BY CUBE(a, b)
----
-- GROUPING() with single grouping set.
SELECT a, GROUPING(a), count(*) AS c FROM t GROUP BY GROUPING SETS ((a))
----
-- GROUPING() with plain GROUP BY returns 0.
SELECT a, GROUPING(a), count(*) AS c FROM t GROUP BY a
----
-- GROUPING() in HAVING.
SELECT a, sum(b) AS s FROM t GROUP BY ROLLUP(a) HAVING GROUPING(a) = 0
----
-- GROUPING() in ORDER BY.
-- ordered
SELECT a, GROUPING(a) AS grp, sum(b) AS s FROM t GROUP BY ROLLUP(a) ORDER BY GROUPING(a), a
----
-- GROUPING() with reversed arg order.
SELECT a, b, GROUPING(b, a), sum(b) AS s FROM t GROUP BY CUBE(a, b)
----
-- GROUPING() with empty grouping set.
SELECT a, GROUPING(a), sum(b) AS s FROM t GROUP BY GROUPING SETS ((a), ())
----
-- GROUPING() with duplicate grouping sets.
SELECT a, GROUPING(a), count(*) AS c FROM t GROUP BY GROUPING SETS ((a), (a), (a, b))
----
-- GROUPING() with a column duplicated across a self-join.
SELECT GROUPING(t1.a, t2.a), count(*) AS c FROM t t1, t t2 WHERE t1.a = t2.a GROUP BY GROUPING SETS ((t1.a), (t2.a))
----
-- SELECT aliases are not visible in HAVING (standard SQL — HAVING runs before SELECT).
-- error: HAVING clause cannot reference column
SELECT a, GROUPING(a) AS grp, sum(b) AS s FROM t GROUP BY ROLLUP(a) HAVING grp = 0
----
-- GROUPING() with non-grouping column is rejected.
-- error: Not a grouping column
SELECT a, GROUPING(b), count(*) AS c FROM t GROUP BY ROLLUP(a)
----
-- ORDER BY in an aggregate with a global grouping set.
SELECT a, array_agg(b ORDER BY b) AS arr FROM t GROUP BY ROLLUP(a)
----
-- DISTINCT aggregate with a global grouping set.
SELECT a, count(DISTINCT b) AS c FROM t GROUP BY CUBE(a)
----
-- DISTINCT and ORDER BY aggregate with a global grouping set.
SELECT a, array_agg(DISTINCT b ORDER BY b) AS arr FROM t GROUP BY ROLLUP(a)
----
