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
-- Distinct aggregation queries.

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
-- DISTINCT: mixed column and literal args (literals differ across aggregates).
SELECT a, covar_pop(DISTINCT c, 1.0), covar_samp(DISTINCT c, 2.0) FROM t GROUP BY a
----
-- DISTINCT: all arguments are literals.
SELECT a, count(DISTINCT 1), count(DISTINCT 2) FROM t GROUP BY a
----
-- error: DISTINCT aggregates have multiple sets of arguments
SELECT a, count(DISTINCT 1), count(DISTINCT b) FROM t GROUP BY a
----
-- DISTINCT with ORDER BY and literal args.
SELECT a, max_by(DISTINCT b, 1 ORDER BY b), min_by(DISTINCT b, 2 ORDER BY b) FROM t GROUP BY a
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
