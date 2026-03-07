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
-- Column name checking (-- columns) verifies that the grouping set ID column
-- does not leak into the output.

-- columns
-- Basic ROLLUP with SUM.
SELECT a, sum(b) AS s FROM t GROUP BY ROLLUP(a)
----
-- columns
-- CUBE with SUM (same as ROLLUP for single key).
SELECT a, sum(b) AS s FROM t GROUP BY CUBE(a)
----
-- columns
-- Explicit GROUPING SETS.
SELECT a, sum(b) AS s FROM t GROUP BY GROUPING SETS ((a), ())
----
-- columns
-- ROLLUP with multiple keys.
SELECT a, b, count(*) AS s FROM t GROUP BY ROLLUP(a, b)
----
-- columns
-- Multiple aggregates with ROLLUP.
SELECT a, sum(b) AS s, count(b) AS c, min(b) AS mn, max(b) AS mx FROM t GROUP BY ROLLUP(a)
----
-- columns
-- Explicit GROUPING SETS with no global (empty) set.
SELECT a, b, sum(b) AS s FROM t GROUP BY GROUPING SETS ((a), (b))
----
-- columns
-- Grouping key is also an aggregation input.
SELECT a, sum(a) AS s FROM t GROUP BY ROLLUP(a)
----
-- columns
-- Literal aggregate argument (not a column reference, should not flow through
-- GroupId as an aggregation input).
SELECT a, count(1) AS c FROM t GROUP BY ROLLUP(a)
