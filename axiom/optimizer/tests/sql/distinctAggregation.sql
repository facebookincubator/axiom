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
-- DISTINCT with ORDER BY and literal args.
SELECT a, max_by(DISTINCT b, 1 ORDER BY b), min_by(DISTINCT b, 2 ORDER BY b) FROM t GROUP BY a
----
-- DISTINCT with ORDER BY where ORDER BY keys are subset of distinct args.
SELECT c, max_by(DISTINCT a, CAST(b AS DOUBLE) ORDER BY a), min_by(DISTINCT a, CAST(b AS DOUBLE) ORDER BY CAST(b AS DOUBLE)) FROM t GROUP BY c
----
-- DISTINCT: multiple aggregates with different argument sets.
SELECT a, count(DISTINCT c), sum(DISTINCT CAST(b AS DOUBLE)) FROM t GROUP BY a
----
-- DISTINCT: multi-argument aggregates with different argument sets.
SELECT a, covar_pop(DISTINCT c, CAST(b AS DOUBLE)), covar_samp(DISTINCT CAST(b AS DOUBLE), CAST(b AS DOUBLE)) FROM t GROUP BY a
----
-- DISTINCT: mix of DISTINCT and non-DISTINCT aggregates.
SELECT a, count(DISTINCT c), sum(CAST(b AS DOUBLE)) FROM t GROUP BY a
----
-- DISTINCT: global aggregation with multiple DISTINCT sets.
SELECT count(DISTINCT c), sum(DISTINCT CAST(b AS DOUBLE)) FROM t
----
-- DISTINCT: ORDER BY with multiple DISTINCT sets forces single-step aggregation.
SELECT a, array_agg(DISTINCT c ORDER BY c), array_agg(DISTINCT CAST(b AS DOUBLE) ORDER BY CAST(b AS DOUBLE)), array_agg(c ORDER BY c) FROM t GROUP BY a
----
-- DISTINCT: aggregates sharing a marker (same non-grouping-key args after dedup).
SELECT c, count(DISTINCT CAST(b AS DOUBLE)), covar_pop(DISTINCT c, CAST(b AS DOUBLE)) FROM t GROUP BY c
----
-- DISTINCT: args overlapping with grouping keys.
SELECT a, count(DISTINCT a), sum(DISTINCT c) FROM t GROUP BY a
----
-- DISTINCT: all args are grouping keys, no markers needed.
SELECT a, count(DISTINCT a), sum(DISTINCT a) FROM t GROUP BY a
----
-- DISTINCT: mixed column and literal args (literals differ across aggregates).
SELECT a, covar_pop(DISTINCT c, 1.0), covar_samp(DISTINCT c, 2.0) FROM t GROUP BY a
----
-- DISTINCT: literal in args alongside different DISTINCT column sets.
SELECT a, count(DISTINCT c), covar_pop(DISTINCT CAST(b AS DOUBLE), 1.0) FROM t GROUP BY a
----
-- error: Distinct aggregation with FILTER is not supported
SELECT a, count(DISTINCT c) FILTER (WHERE c > 0) FROM t GROUP BY a
----
-- DISTINCT: combination of distinct on literal args and on grouping keys.
SELECT a, count(DISTINCT 1), count(DISTINCT a), count(DISTINCT b) FROM t GROUP BY a
----
-- DISTINCT: global aggregation mixing column DISTINCT and all-literal DISTINCT.
SELECT count(DISTINCT c), count(DISTINCT 1) FROM t
----
-- DISTINCT: the first MarkDistinct key is a subset of the second, hence no shuffle between multiple MarkDistincts.
SELECT a, count(DISTINCT b), covar_pop(DISTINCT b, c) FROM t GROUP BY a
