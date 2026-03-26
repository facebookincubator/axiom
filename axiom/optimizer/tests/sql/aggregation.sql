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
-- Aggregation queries.

-- Dedup: duplicate grouping key expressions and aggregate calls.
SELECT a + b AS x, a + b AS y, count(1) AS c1, count(1) AS c2 FROM t GROUP BY 1, 2
----
-- Dedup: column used in both GROUP BY and aggregate.
SELECT a + b AS x, a + b AS y, count(a + b) AS z FROM t GROUP BY 1, 2
----
-- Dedup: identical FILTER masks.
SELECT sum(a) FILTER (WHERE b > 0), sum(a) FILTER (WHERE b < 0), sum(a) FILTER (WHERE b > 0) FROM t
----
-- Dedup: column ORDER BY keys within aggregates.
SELECT array_agg(a ORDER BY a, a), array_agg(b ORDER BY b, a, b, a) FROM t
----
-- Dedup: expression ORDER BY keys within aggregates.
SELECT array_agg(a ORDER BY a + b, a + b DESC, c), array_agg(c ORDER BY b * 2, b * 2) FROM t
----
-- Dedup: identical aggregates with same ORDER BY and FILTER combinations.
SELECT array_agg(a ORDER BY a, a, a), array_agg(a ORDER BY a DESC), array_agg(a ORDER BY a, a), array_agg(a ORDER BY a), sum(a) FILTER (WHERE b > 0), sum(a) FILTER (WHERE b < 0), sum(a) FILTER (WHERE b > 0), array_agg(a ORDER BY a) FILTER (WHERE b > 0), array_agg(a ORDER BY a DESC) FILTER (WHERE b > 0), array_agg(a ORDER BY a) FILTER (WHERE b > 0) FROM t
----
-- ORDER BY forces single-step aggregation even in distributed mode.
SELECT a, array_agg(b ORDER BY c), sum(b) FROM t GROUP BY a
----
-- Aggregation without ORDER BY uses partial + final.
SELECT a, sum(b) FROM t GROUP BY a
----
-- Nested aggregation: partition keys subset of grouping keys (no extra shuffle).
SELECT a, b, a + b AS d FROM (SELECT a, b FROM t GROUP BY a, b) GROUP BY 1, 2, 3
----
-- Nested aggregation: partition keys not subset (extra shuffle needed).
SELECT a, b FROM (SELECT a, b, c FROM t GROUP BY a, b, c) GROUP BY a, b
----
-- DISTINCT: global aggregation with multiple aggregates on same column set.
SELECT count(DISTINCT c), covar_pop(DISTINCT c, c) FROM t
----
-- DISTINCT: with grouping key.
SELECT a, count(DISTINCT c) FROM t GROUP BY a
----
-- DISTINCT: multiple aggregates on same column set with grouping key.
SELECT a, count(DISTINCT c), covar_pop(DISTINCT c, c) FROM t GROUP BY a
----
-- DISTINCT: expression-based grouping keys and distinct args.
SELECT a + 1, count(DISTINCT CAST(b AS DOUBLE) + c), sum(DISTINCT CAST(b AS DOUBLE) + c) FROM t GROUP BY 1
----
-- DISTINCT: same args in different order across aggregates.
SELECT a, covar_pop(DISTINCT CAST(b AS DOUBLE), c), covar_samp(DISTINCT c, CAST(b AS DOUBLE)) FROM t GROUP BY a
----
-- DISTINCT: argument overlap with grouping keys (c is both grouping key and DISTINCT arg).
SELECT c, covar_pop(DISTINCT c, CAST(b AS DOUBLE)) FROM t GROUP BY c
----
-- DISTINCT with ORDER BY where ORDER BY keys are subset of distinct args.
SELECT c, max_by(DISTINCT a, CAST(b AS DOUBLE) ORDER BY a), min_by(DISTINCT a, CAST(b AS DOUBLE) ORDER BY CAST(b AS DOUBLE)) FROM t GROUP BY c
----
-- error: DISTINCT aggregates have multiple sets of arguments
SELECT a, count(DISTINCT c), sum(DISTINCT CAST(b AS DOUBLE)) FROM t GROUP BY a
----
-- error: DISTINCT aggregates have multiple sets of arguments
SELECT a, covar_pop(DISTINCT c, CAST(b AS DOUBLE)), covar_samp(DISTINCT CAST(b AS DOUBLE), CAST(b AS DOUBLE)) FROM t GROUP BY a
----
-- error: Mix of DISTINCT and non-DISTINCT aggregates
SELECT a, count(DISTINCT c), sum(CAST(b AS DOUBLE)) FROM t GROUP BY a
