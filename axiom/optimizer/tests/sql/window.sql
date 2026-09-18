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
-- Multiple window functions reuse a named specification.
SELECT a, b, sum(b) OVER w AS s, count(*) OVER w AS c FROM t WINDOW w AS (PARTITION BY a ORDER BY b)
----
-- A named specification orders grouped rows by an aggregate.
SELECT a, sum(b) AS total, row_number() OVER w AS rn
FROM t
GROUP BY a
WINDOW w AS (ORDER BY sum(b))
----
-- An aggregate in a named specification makes the query an aggregation.
SELECT row_number() OVER w FROM t WINDOW w AS (ORDER BY sum(b))
----
-- An aggregate in an unused named specification makes the query an aggregation.
-- count 1
SELECT 1 FROM t WINDOW unused AS (ORDER BY sum(b))
----
-- An aggregate in an unused named specification makes wildcard columns
-- subject to grouping validation.
-- error: Cannot resolve column: a
SELECT * FROM t WINDOW unused AS (ORDER BY sum(b))
----
-- An unused named specification obeys grouping rules.
-- error: WINDOW clause cannot reference column: b
SELECT max(a) FROM t GROUP BY a WINDOW unused AS (PARTITION BY b)
----
-- GROUPING() is valid in a named specification of a grouped query.
-- ordered
SELECT a, row_number() OVER w AS rn
FROM t
GROUP BY GROUPING SETS ((a))
WINDOW w AS (PARTITION BY grouping(a) ORDER BY a)
ORDER BY a
----
-- A named window is refined with an ordering and frame.
-- Velox's DuckDB does not preserve inherited parts through this chain.
-- duckdb: SELECT a, b, sum(b) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM t
SELECT a, b, sum(b) OVER (ordered ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s
FROM t
WINDOW partitioned AS (PARTITION BY a), ordered AS (partitioned ORDER BY b)
----
-- A direct reference retains the named window's frame.
SELECT a, b, sum(b) OVER framed AS s
FROM t
WINDOW framed AS (PARTITION BY a ORDER BY b ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
----
-- A query ORDER BY can reference a named window from its query specification.
-- ordered
SELECT a, b FROM t WINDOW w AS (ORDER BY b) ORDER BY row_number() OVER w LIMIT 3
----
-- A named specification is evaluated after a WHERE subquery.
SELECT a, b, row_number() OVER w AS rn
FROM t AS outer_t
WHERE EXISTS (
  SELECT 1 FROM t AS inner_t
  WHERE inner_t.a = outer_t.a AND inner_t.b > outer_t.b)
WINDOW w AS (PARTITION BY a ORDER BY b)
----
-- A named specification resolves input columns before SELECT aliases.
-- ordered
SELECT a AS b
FROM t
WINDOW w AS (ORDER BY b)
ORDER BY row_number() OVER w
LIMIT 3
----
-- An inline specification in query ORDER BY resolves SELECT aliases first.
-- ordered
-- Velox's DuckDB resolves b to the input column instead of the SELECT alias.
-- duckdb: SELECT a AS b FROM t ORDER BY row_number() OVER (ORDER BY a)
SELECT a AS b FROM t ORDER BY row_number() OVER (ORDER BY b)
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
-- Outer SELECT projects only the ranking column.
SELECT rn FROM (SELECT a, b, row_number() OVER (PARTITION BY a ORDER BY b) AS rn FROM t) WHERE rn = 1
----
-- Filter on ranking function output (rn <= N).
SELECT * FROM (SELECT a, b, row_number() OVER (PARTITION BY a ORDER BY b) AS rn FROM t) WHERE rn <= 1
----
-- Filter on ranking function output (rn > N).
SELECT a, b FROM (SELECT a, b, row_number() OVER (PARTITION BY a ORDER BY b) AS rn FROM t) WHERE rn > 1
----
-- dense_rank with ORDER BY column also in PARTITION BY + ranking filter. The
-- ORDER BY is redundant (constant within partition), so dense_rank is always
-- 1 and 'dr = 1' matches every row.
SELECT a, b FROM (SELECT a, b, dense_rank() OVER (PARTITION BY a, b ORDER BY a) AS dr FROM t) WHERE dr = 1
----
-- Same as above but with rank().
SELECT a, b FROM (SELECT a, b, rank() OVER (PARTITION BY a, b ORDER BY a) AS r FROM t) WHERE r = 1
----
-- row_number without ORDER BY: which rows get which numbers is arbitrary, so
-- the query checks how many rows each partition keeps, not their numbers.
SELECT a, count(*) AS kept FROM (SELECT a, row_number() OVER (PARTITION BY a) AS rn FROM t) WHERE rn <= 2 GROUP BY a
----
-- row_number without PARTITION BY or ORDER BY, bounded by a filter.
-- count 4
SELECT rn FROM (SELECT row_number() OVER () AS rn FROM t) WHERE rn <= 4
----
-- Computed PARTITION BY: rows are numbered within the value of the expression.
SELECT a, b FROM (SELECT a, b, row_number() OVER (PARTITION BY a % 2 ORDER BY b) AS rn FROM t) WHERE rn = 1
----
-- RANGE frame with a CURRENT ROW bound and no ORDER BY.
SELECT a, b, sum(b) OVER (PARTITION BY a RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS s FROM t
----
SELECT a, b, sum(b) OVER (PARTITION BY a RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s FROM t
----
-- Window function with OFFSET and no LIMIT: nothing bounds the partitions, so
-- every row is numbered and the offset drops an arbitrary 5 of them.
-- count 10
SELECT a, b, rn FROM (SELECT a, b, row_number() OVER (PARTITION BY a ORDER BY b) AS rn FROM t) OFFSET 5
----
-- The window and the query both order by an expression: the rows come back in
-- that order.
-- ordered
SELECT a, b, row_number() OVER (ORDER BY a + b) AS rn FROM t ORDER BY a + b LIMIT 3
----
-- rank() with a query ORDER BY matching the window's: ties may exceed the
-- limit, so the rows kept must still be the first ones in that order.
-- ordered
SELECT b, rank() OVER (ORDER BY b) AS r FROM t ORDER BY b LIMIT 3
----
-- OFFSET with LIMIT over a ranking: the ranking must keep enough rows per
-- partition for the offset to skip.
-- ordered
SELECT b, row_number() OVER (ORDER BY b) AS rn FROM t ORDER BY b OFFSET 5 LIMIT 4
----
-- Query ORDER BY matches the window's ORDER BY over a single partition: the
-- rows must still come back in that order.
-- ordered
SELECT b, row_number() OVER (ORDER BY b) AS rn FROM t ORDER BY b LIMIT 3
----
-- rank() with no ORDER BY: every row of a partition ties at rank 1, so the
-- filter keeps every row.
SELECT a, b FROM (SELECT a, b, rank() OVER (PARTITION BY a) AS rk FROM t) WHERE rk = 1
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
-- RANGE frame: DATE ORDER BY with INTERVAL DAY frame bounds.
SELECT d, sum(n) OVER (ORDER BY d RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW) AS s
FROM (VALUES (DATE '2025-04-01', 1), (DATE '2025-04-02', 2), (DATE '2025-04-05', 3)) AS t(d, n)
----
-- RANGE frame: TIMESTAMP ORDER BY with INTERVAL HOUR frame bounds.
SELECT ts, sum(n) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW) AS s
FROM (VALUES (TIMESTAMP '2025-04-01 00:00:00', 1), (TIMESTAMP '2025-04-01 01:00:00', 2), (TIMESTAMP '2025-04-01 03:00:00', 3)) AS t(ts, n)
----
-- Numeric RANGE with same-type BIGINT bounds.
SELECT a, b, sum(b) OVER (PARTITION BY a ORDER BY b RANGE BETWEEN BIGINT '30' PRECEDING AND BIGINT '30' FOLLOWING) AS s FROM t
----
-- Numeric RANGE: INTEGER bound against BIGINT ORDER BY.
SELECT a, b, sum(b) OVER (PARTITION BY a ORDER BY b RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING) AS s FROM t
----
-- Numeric RANGE with DESC ordering.
SELECT a, b, sum(b) OVER (PARTITION BY a ORDER BY b DESC RANGE BETWEEN BIGINT '30' PRECEDING AND BIGINT '30' FOLLOWING) AS s FROM t
----
-- Numeric RANGE with FOLLOWING-only bounds (no PRECEDING).
SELECT a, b, sum(b) OVER (PARTITION BY a ORDER BY b RANGE BETWEEN CURRENT ROW AND BIGINT '30' FOLLOWING) AS s FROM t
----
-- Numeric RANGE over a DOUBLE ORDER BY column.
SELECT a, c, sum(b) OVER (PARTITION BY a ORDER BY c RANGE BETWEEN 3e0 PRECEDING AND 3e0 FOLLOWING) AS s FROM t
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
----
-- Nested window functions: inner window result mixed with a regular column in
-- an expression, used as input to an outer window function.
SELECT sum(n) OVER (ORDER BY a, b)
FROM (
    SELECT a, b, a + lag(b) OVER (ORDER BY a, b) AS n
    FROM t
)
----
-- Dependent window via scalar expression over multiple window outputs.
SELECT lag(pct) OVER (PARTITION BY a ORDER BY b)
FROM (
    SELECT a, b,
        floor(sum(b) OVER (PARTITION BY a ORDER BY b) * 100.0
            / sum(b) OVER (PARTITION BY a)) AS pct
    FROM t
)
----
-- ORDER BY a window nested in an expression that also appears in the SELECT
-- list: the sort key matches the SELECT item and reuses its window.
-- ordered
SELECT a, c, c / sum(c) OVER () AS share FROM t ORDER BY c / sum(c) OVER () DESC
----
-- ORDER BY a window nested in an expression not present in the SELECT list.
-- ordered
SELECT a, c FROM t ORDER BY c / sum(c) OVER () DESC
----
-- SELECT DISTINCT with a nested window in ORDER BY that repeats a SELECT item.
-- ordered
SELECT DISTINCT c / sum(c) OVER () AS s FROM t ORDER BY c / sum(c) OVER ()
----
-- ORDER BY a window nested in an expression over a GROUP BY, where the
-- expression is not in the SELECT list.
-- ordered
SELECT a, sum(b) FROM t GROUP BY a ORDER BY sum(b) * 1.0 / sum(sum(b)) OVER () DESC
----
-- Same nested window over a GROUP BY, but the ORDER BY expression also appears
-- in the SELECT list.
-- ordered
SELECT a, sum(b) * 1.0 / sum(sum(b)) OVER () AS share
FROM t
GROUP BY a
ORDER BY sum(b) * 1.0 / sum(sum(b)) OVER () DESC
----
-- A window whose ORDER BY key is an aggregate and whose frame is a RANGE
-- offset, over a GROUP BY.
-- ordered
SELECT count(*) OVER (ORDER BY max(d) RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW) AS c
FROM (VALUES (DATE '2025-01-01', 1), (DATE '2025-01-02', 2), (DATE '2025-01-03', 3)) AS t(d, n)
GROUP BY d
----
-- A RANGE offset frame whose ORDER BY key is a constant. Every row is a peer,
-- so the frame covers the whole partition.
SELECT array_agg(a) OVER (ORDER BY a RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)
FROM (SELECT 1 AS a FROM (VALUES (1), (2), (3)) AS t(x)) AS u(a)
----
-- The same, with the constant written in the ORDER BY itself.
SELECT array_agg(x) OVER (ORDER BY (1 + 1) RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)
FROM (VALUES (1), (2)) AS t(x)
----
-- Repeated partition keys: a literally duplicated column and two distinct
-- columns holding the same constant.
SELECT sum(x) OVER (PARTITION BY x, x, a, b ORDER BY x)
FROM (SELECT y AS x, 'All' AS a, 'All' AS b FROM (VALUES (1), (2), (2)) AS t(y)) AS u
----
-- A window aggregate whose argument is computed, over UNNEST of an aggregated
-- array, with two columns holding the same constant.
SELECT a, b, sum(v + 1) OVER (PARTITION BY d) AS r
FROM (
    SELECT 'All' AS a, 'All' AS b, d, array_agg(i) AS vals
    FROM (VALUES ('x', 1)) AS s(d, i)
    GROUP BY d
) AS t
CROSS JOIN UNNEST(vals) AS n(v)
----
-- HAVING drops a group before the window functions run, so a window nested in
-- an expression sums only the groups that survive it.
SELECT a, count(*) AS c,
    1.0 * count(*) / sum(count(*)) OVER (PARTITION BY a) AS frac
FROM (VALUES (1, 1), (1, 1), (1, 2),
             (2, 3), (2, 3), (2, 4)) AS t(a, b)
GROUP BY a, b
HAVING count(*) > 1
