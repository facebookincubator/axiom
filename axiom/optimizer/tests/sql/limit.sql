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
-- ordered
SELECT a, b FROM t ORDER BY b LIMIT 3
----
-- count 0
SELECT a, b FROM t LIMIT 0
----
-- LIMIT ALL means "no limit" — returns all rows.
SELECT * FROM t LIMIT ALL
----
-- LIMIT 0 in CTE with self-join.
-- count 0
WITH final AS (SELECT a, SUM(b) AS total FROM t GROUP BY 1 LIMIT 0)
SELECT x.total FROM final x, final y WHERE x.a = y.a AND x.a = 1
----
SELECT a, b FROM t OFFSET 0
----
-- ordered
SELECT a, b FROM t ORDER BY b OFFSET 0
----
-- ordered
SELECT a, b FROM t ORDER BY b OFFSET 0 LIMIT 3
----
-- count 0
SELECT a, b FROM t OFFSET 0 LIMIT 0
