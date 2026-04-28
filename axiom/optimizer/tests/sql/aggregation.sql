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
-- Lambda aggregate function.
-- duckdb: SELECT sum(a) FROM t
SELECT reduce_agg(a, 0, (s, x) -> s + x, (s1, s2) -> s1 + s2) FROM t
----
-- duckdb: SELECT b, sum(a) FROM t GROUP BY 1
SELECT b, reduce_agg(a, 0, (s, x) -> s + x, (s1, s2) -> s1 + s2) FROM t GROUP BY 1
----
-- Lambda aggregate with FILTER.
-- duckdb: SELECT sum(a) FROM t WHERE b > 50
SELECT reduce_agg(a, 0, (s, x) -> s + x, (s1, s2) -> s1 + s2) FILTER (WHERE b > 50) FROM t
----
-- Lambda aggregate with GROUP BY on VALUES (single step).
-- duckdb: VALUES (3), (7)
SELECT reduce_agg(a, 0, (s, x) -> s + x, (s1, s2) -> s1 + s2) FROM (VALUES (1, 'x'), (2, 'x'), (3, 'y'), (4, 'y')) AS t(a, k) GROUP BY k
----
-- Multiple lambda aggregates in same query.
-- duckdb: VALUES (10, 24)
SELECT reduce_agg(a, 0, (s, x) -> s + x, (s1, s2) -> s1 + s2), reduce_agg(a, 1, (s, x) -> s * x, (s1, s2) -> s1 * s2) FROM (VALUES (1), (2), (3), (4)) AS t(a)
----
-- Lambda aggregate functions with lambda captures are not supported.
-- error: Lambda captures are not supported in aggregate functions
SELECT reduce_agg(a, 0, (s, x) -> s + x, (s1, s2) -> s1 + s2 + b) FROM t GROUP BY b
----
-- DISTINCT applied on top of a GROUP BY, ordered by the grouping key.
-- ordered
SELECT DISTINCT a, COUNT(*) AS cnt FROM t GROUP BY a ORDER BY a
