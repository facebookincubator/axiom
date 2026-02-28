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
