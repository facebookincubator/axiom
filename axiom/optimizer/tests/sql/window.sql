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
-- Window functions.

-- row_number with partition and order.
SELECT a, b, row_number() OVER (PARTITION BY a ORDER BY b) AS rn FROM t
----
-- rank with partition and order.
SELECT a, b, rank() OVER (PARTITION BY a ORDER BY b) AS r FROM t
----
-- dense_rank with partition and order.
SELECT a, b, dense_rank() OVER (PARTITION BY a ORDER BY b) AS dr FROM t
----
-- sum over partition.
SELECT a, b, sum(b) OVER (PARTITION BY a) AS total_b FROM t
----
-- Count over entire table.
SELECT a, b, count(*) OVER () AS cnt FROM t
----
-- min and max over partition.
SELECT a, b, min(b) OVER (PARTITION BY a) AS min_b, max(b) OVER (PARTITION BY a) AS max_b FROM t
----
-- avg over partition.
SELECT a, b, avg(b) OVER (PARTITION BY a) AS avg_b FROM t
----
-- Running sum (ROWS UNBOUNDED PRECEDING to CURRENT ROW).
SELECT a, b, sum(b) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum FROM t
----
-- Running count.
SELECT a, b, count(*) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count FROM t
----
-- Full-partition frame with ORDER BY (ORDER BY is redundant).
SELECT a, b, sum(b) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_b FROM t
----
-- Multiple window functions with same specification.
SELECT a, b, sum(b) OVER (PARTITION BY a ORDER BY b) AS s, count(*) OVER (PARTITION BY a ORDER BY b) AS c FROM t
----
-- Multiple window functions with different specifications.
SELECT a, b, sum(b) OVER (PARTITION BY a) AS s, count(*) OVER () AS c FROM t
----
-- Window function with GROUP BY (via subquery).
SELECT a, sum_b, count(*) OVER () AS cnt FROM (SELECT a, sum(b) AS sum_b FROM t GROUP BY 1)
----
-- Window function with GROUP BY in same SELECT.
SELECT a, sum(b), row_number() OVER (ORDER BY a) FROM t GROUP BY a
----
-- Window function with PARTITION BY and GROUP BY.
SELECT b, sum(a), row_number() OVER (PARTITION BY b ORDER BY a) FROM t GROUP BY b, a
----
-- Window function in ORDER BY with GROUP BY.
-- ordered
SELECT a, sum(b) FROM t GROUP BY a ORDER BY row_number() OVER (ORDER BY a)
----
-- Subquery with window function and outer filter.
SELECT * FROM (SELECT a, b, row_number() OVER (PARTITION BY a ORDER BY b) AS rn FROM t) WHERE rn = 1
----
-- Filter on ranking function output (rn <= N).
SELECT * FROM (SELECT a, b, row_number() OVER (PARTITION BY a ORDER BY b) AS rn FROM t) WHERE rn <= 1
----
-- Filter on ranking function output (rn > N).
SELECT a, b FROM (SELECT a, b, row_number() OVER (PARTITION BY a ORDER BY b) AS rn FROM t) WHERE rn > 1
----
-- Window function combined with ORDER BY and LIMIT.
-- ordered
SELECT a, b, row_number() OVER (PARTITION BY a ORDER BY b) AS rn FROM t ORDER BY a, b LIMIT 3
----
-- ROWS N PRECEDING to CURRENT ROW.
SELECT a, b, sum(b) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM t
----
-- ROWS CURRENT ROW to N FOLLOWING.
SELECT a, b, sum(b) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS s FROM t
----
-- RANGE frame.
SELECT a, b, sum(b) OVER (PARTITION BY a ORDER BY b RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s FROM t
----
-- Partition-key filter pushed below window.
SELECT * FROM (SELECT a, b, sum(b) OVER (PARTITION BY a) AS s FROM t) WHERE a = 1
----
-- Non-partition filter stays above window.
SELECT * FROM (SELECT a, b, sum(b) OVER (PARTITION BY a) AS s FROM t) WHERE s > 40
----
-- Window function in expression.
-- columns
SELECT a, b, sum(b) OVER (PARTITION BY a) * 2 AS doubled FROM t
----
-- Window function mixed with *.
-- columns
SELECT *, sum(b) OVER (PARTITION BY a) AS total_b FROM t
----
-- Window function with same signature as plain aggregate in GROUP BY.
SELECT a, sum(a), sum(a) OVER (ORDER BY a) FROM t GROUP BY a
----
-- first_value with constant argument.
SELECT a, b, first_value(42) OVER (PARTITION BY a ORDER BY b) AS fv FROM t
----
-- last_value with constant argument.
SELECT a, b, last_value(42) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv FROM t
----
-- nth_value with constant argument.
SELECT a, b, nth_value(42, 2) OVER (PARTITION BY a ORDER BY b) AS nv FROM t
----
-- nth_value with constant argument and out-of-range offset.
SELECT a, b, nth_value(42, 100) OVER (PARTITION BY a ORDER BY b) AS nv FROM t
----
-- lag with constant argument.
SELECT a, b, lag(42, 1) OVER (PARTITION BY a ORDER BY b) AS lg FROM t
----
-- lead with constant argument.
SELECT a, b, lead(42, 1) OVER (PARTITION BY a ORDER BY b) AS ld FROM t
----
-- first_value with constant argument and empty frames.
SELECT a, b, first_value(42) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS fv FROM t
----
-- lead with constant null argument.
-- duckdb: SELECT a, b, lead(CAST(NULL AS BIGINT), 1) OVER (PARTITION BY a ORDER BY b) AS ld FROM t
SELECT a, b, lead(null, 1) OVER (PARTITION BY a ORDER BY b) AS ld FROM t
----
-- Window function output used as GROUP BY key in outer query.
SELECT a, max_a, sum(b) FROM (SELECT a, b, max(a) OVER (ORDER BY b) AS max_a FROM t) GROUP BY 1, 2
