-- Table t(a BIGINT, b BIGINT) with 15 rows across 3 splits:
--   a |   b
--  ---+-----
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
-- HAVING filter with grouping sets (with aggregate).
SELECT a, sum(b) AS s FROM t GROUP BY GROUPING SETS ((a), (a, b)) HAVING b IS NOT NULL
----
-- HAVING filter with grouping sets (no aggregate, keys only).
SELECT a FROM t GROUP BY GROUPING SETS ((a), (a, b)) HAVING b IS NOT NULL
----
-- HAVING + ORDER BY with grouping sets.
SELECT a AS foo FROM t GROUP BY GROUPING SETS ((a), (a, b)) HAVING b IS NOT NULL ORDER BY a DESC
