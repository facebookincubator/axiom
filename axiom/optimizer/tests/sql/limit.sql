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
-- ordered
SELECT a, b FROM t ORDER BY b LIMIT 3
----
-- ORDER BY an expression that is not in the select list, with LIMIT.
-- ordered
SELECT * FROM t ORDER BY b + c DESC LIMIT 3
----
-- Same, over an input that is already on one task.
-- ordered
SELECT * FROM (VALUES (1, 2), (3, 1), (2, 5)) t(a, b) ORDER BY a + b DESC LIMIT 2
----
-- A WHERE above ORDER BY with LIMIT filters the rows the limit keeps, the three
-- largest b, even when nothing above the WHERE reads a column of them. Two of
-- the three pass. Smaller b pass the WHERE too, and the limit drops them.
SELECT count(*) FROM (SELECT b FROM t ORDER BY b DESC LIMIT 3) WHERE b < 145
----
-- Same, returning a constant for each row that passes.
SELECT 1 FROM (SELECT b FROM t ORDER BY b DESC LIMIT 3) WHERE b < 145
----
-- Same, with an OFFSET that skips the largest b.
SELECT count(*) FROM (SELECT b FROM t ORDER BY b DESC OFFSET 1 LIMIT 2)
WHERE b < 135
----
-- Same, filtering on a column that is not the ORDER BY key.
SELECT count(*) FROM (SELECT b, c FROM t ORDER BY b DESC LIMIT 3) WHERE c < 15
----
-- Same, filtering on an expression computed above the limit.
SELECT count(*)
FROM (SELECT b + 1 AS w FROM (SELECT b FROM t ORDER BY b DESC LIMIT 3))
WHERE w < 146
----
-- Same, filtering one side of a cross join.
SELECT count(*)
FROM (SELECT b FROM t ORDER BY b DESC LIMIT 3) x
CROSS JOIN (VALUES (1), (2)) u(z)
WHERE x.b < 145
----
-- Same, with the filter in the ON clause of a join.
SELECT count(*)
FROM (SELECT b FROM t ORDER BY b DESC LIMIT 3) x
JOIN (VALUES (1), (2)) u(z) ON x.b < 145
----
-- Same, inside an EXISTS. None of the three largest b passes.
SELECT count(*) FROM (VALUES (1), (2)) u(z)
WHERE EXISTS (
  SELECT 1 FROM (SELECT b FROM t ORDER BY b DESC LIMIT 3) WHERE b < 125)
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
----
-- count 13
SELECT a, b FROM t OFFSET 2
----
-- ordered
SELECT a, b FROM t ORDER BY b DESC OFFSET 5 LIMIT 3
----
-- ordered
SELECT a, b FROM t ORDER BY b DESC OFFSET 12
----
-- An offset whose sum with the limit exceeds the maximum int64.
-- count 0
SELECT a, b FROM t OFFSET 9223372036854775802 LIMIT 100
