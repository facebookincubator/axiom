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
-- DuckDB 0.8.1 silently ignores the ALL keyword in EXCEPT ALL / INTERSECT ALL
-- and returns DISTINCT results. Use '-- duckdb: VALUES(...)' to hardcode
-- expected results until DuckDB is upgraded.
--
-- Partial duplicate removal: the right side removes some but not all copies.
-- Left (a < 3): {1x5, 2x5}, Right (a <= 1 AND b <= 40): {1x2}
-- EXCEPT ALL: 5-2=3 ones, 5-0=5 twos = {1x3, 2x5} = 8 rows.
-- EXCEPT would give {2} = 1 row (removes all ones).
-- duckdb: VALUES (1), (1), (1), (2), (2), (2), (2), (2)
SELECT a FROM t WHERE a < 3
EXCEPT ALL
SELECT a FROM t WHERE a <= 1 AND b <= 40
----
-- Same query with EXCEPT (DISTINCT).
-- duckdb: VALUES (2)
SELECT a FROM t WHERE a < 3
EXCEPT
SELECT a FROM t WHERE a <= 1 AND b <= 40
----
-- Partial duplicate retention.
-- Left (a < 3): {1x5, 2x5}, Right (a <= 1 AND b <= 40): {1x2}
-- INTERSECT ALL: min(5,2)=2 ones, min(5,0)=0 twos = {1x2} = 2 rows.
-- INTERSECT would give {1} = 1 row.
-- duckdb: VALUES (1), (1)
SELECT a FROM t WHERE a < 3
INTERSECT ALL
SELECT a FROM t WHERE a <= 1 AND b <= 40
----
-- Same query with INTERSECT (DISTINCT).
-- duckdb: VALUES (1)
SELECT a FROM t WHERE a < 3
INTERSECT
SELECT a FROM t WHERE a <= 1 AND b <= 40
----
-- Right side has more copies than left: EXCEPT ALL returns empty.
-- Left (a = 1 AND b <= 70): {1x3}, Right (a = 1): {1x5}
-- count 0
SELECT a FROM t WHERE a = 1 AND b <= 70
EXCEPT ALL
SELECT a FROM t WHERE a = 1
----
-- Right side has more copies than left: INTERSECT ALL keeps left count.
-- Left (a = 1 AND b <= 70): {1x3}, Right (a = 1): {1x5}
-- INTERSECT ALL: min(3,5)=3 ones.
-- duckdb: VALUES (1), (1), (1)
SELECT a FROM t WHERE a = 1 AND b <= 70
INTERSECT ALL
SELECT a FROM t WHERE a = 1
----
-- No matching keys.
-- EXCEPT ALL: all left rows pass through.
-- duckdb: VALUES (1), (1), (1), (1), (1)
SELECT a FROM t WHERE a = 1
EXCEPT ALL
SELECT a FROM t WHERE a = 2
----
-- No matching keys: INTERSECT ALL returns empty.
-- count 0
SELECT a FROM t WHERE a = 1
INTERSECT ALL
SELECT a FROM t WHERE a = 2
----
-- Empty right side: EXCEPT ALL returns all left rows.
-- duckdb: SELECT a, b FROM t
SELECT a, b FROM t
EXCEPT ALL
SELECT a, b FROM t WHERE false
----
-- Empty right side: INTERSECT ALL returns empty.
-- count 0
SELECT a, b FROM t
INTERSECT ALL
SELECT a, b FROM t WHERE false
----
-- Multiple columns.
-- Each (a, b) pair is unique, so ALL and DISTINCT behave the same.
-- duckdb: SELECT a, b FROM t WHERE a = 2
SELECT a, b FROM t WHERE a <= 2
EXCEPT ALL
SELECT a, b FROM t WHERE a = 1
----
-- duckdb: SELECT a, b FROM t WHERE a = 1
SELECT a, b FROM t WHERE a <= 2
INTERSECT ALL
SELECT a, b FROM t WHERE a = 1
----
-- Chained EXCEPT ALL with 3 inputs.
-- {1x5, 2x5, 3x5} minus {1x5} minus {2x5} = {3x5}.
-- duckdb: VALUES (3), (3), (3), (3), (3)
SELECT a FROM t
EXCEPT ALL
SELECT a FROM t WHERE a = 1
EXCEPT ALL
SELECT a FROM t WHERE a = 2
----
-- Chained INTERSECT ALL with 3 inputs.
-- {1x5, 2x5, 3x5} intersect {1x5, 2x5} intersect {1x5} = {1x5}.
-- duckdb: VALUES (1), (1), (1), (1), (1)
SELECT a FROM t
INTERSECT ALL
SELECT a FROM t WHERE a <= 2
INTERSECT ALL
SELECT a FROM t WHERE a = 1
----
-- Expression in SELECT: a + 1 is projected before the join.
-- Left: a + 1 for all rows = {2x5, 3x5, 4x5}.
-- Right: a for a = 2 = {2x5}.
-- EXCEPT ALL: {2x5} - {2x5} = {}, keep {3x5, 4x5} = 10 rows.
-- duckdb: VALUES (3), (3), (3), (3), (3), (4), (4), (4), (4), (4)
SELECT a + 1 as x FROM t
EXCEPT ALL
SELECT a as x FROM t WHERE a = 2
----
-- Expression in SELECT: INTERSECT ALL.
-- Left: a + 1 = {2x5, 3x5, 4x5}, Right: a = {2x5}.
-- INTERSECT ALL: min(5, 5) = 5 twos.
-- duckdb: VALUES (2), (2), (2), (2), (2)
SELECT a + 1 as x FROM t
INTERSECT ALL
SELECT a as x FROM t WHERE a = 2
----
-- NULL handling: set operations use IS NOT DISTINCT FROM semantics where
-- NULL = NULL.
--
-- EXCEPT with mix of null and non-null rows.
-- Left: (1, null), (2, 3), (null, null). Right: (1, null), (null, null).
-- (1, null) and (null, null) cancel out; (2, 3) has no match.
-- duckdb: VALUES (2, 3)
SELECT * FROM (VALUES (1, null), (2, 3), (null, null)) t(x, y)
EXCEPT
SELECT * FROM (VALUES (1, null), (null, null)) t(x, y)
----
-- INTERSECT with mix of null and non-null rows.
-- Left: (1, null), (2, 3), (null, null). Right: (1, null), (null, null), (4, 5).
-- (1, null) and (null, null) match; (2, 3) and (4, 5) don't.
-- duckdb: VALUES (1, null::int), (null::int, null::int)
SELECT * FROM (VALUES (1, null), (2, 3), (null, null)) t(x, y)
INTERSECT
SELECT * FROM (VALUES (1, null), (null, null), (4, 5)) t(x, y)
----
-- EXCEPT ALL with nulls: count-based subtraction.
-- Left: (1, null) x2, (2, 3). Right: (1, null) x1.
-- (1, null): 2-1=1 remains. (2, 3): no match.
-- duckdb: VALUES (1, null::int), (2, 3)
SELECT * FROM (VALUES (1, null), (1, null), (2, 3)) t(x, y)
EXCEPT ALL
SELECT * FROM (VALUES (1, null)) t(x, y)
----
-- INTERSECT ALL with nulls: count-based intersection.
-- Left: (1, null) x2, (2, 3). Right: (1, null) x1, (2, 3) x1.
-- (1, null): min(2,1)=1. (2, 3): min(1,1)=1.
-- duckdb: VALUES (1, null::int), (2, 3)
SELECT * FROM (VALUES (1, null), (1, null), (2, 3)) t(x, y)
INTERSECT ALL
SELECT * FROM (VALUES (1, null), (2, 3)) t(x, y)
----
-- Multi-column: nulls match but non-null column differs — should NOT cancel.
-- duckdb: VALUES (1, null::int, 'a')
SELECT * FROM (VALUES (1, null, 'a')) t(x, y, z)
EXCEPT
SELECT * FROM (VALUES (1, null, 'b')) t(x, y, z)
----
-- Multi-column INTERSECT: nulls match but non-null column differs — no match.
-- count 0
SELECT * FROM (VALUES (1, null, 'a')) t(x, y, z)
INTERSECT
SELECT * FROM (VALUES (1, null, 'b')) t(x, y, z)
----
-- ROW subfield access in UNION ALL with positional subscript.
-- duckdb: VALUES (1), (3)
SELECT x[1] FROM (SELECT ROW(1, 2) AS x UNION ALL SELECT ROW(3, 4))
----
-- ROW subfield access in UNION ALL with named field.
-- duckdb: VALUES (1), (3)
SELECT x.a FROM (SELECT ROW(1 AS a, 2 AS b) AS x UNION ALL SELECT ROW(3 AS a, 4 AS b))
----
-- EXCEPT nested inside UNION ALL.
SELECT * FROM (
  SELECT a FROM t WHERE a <= 2
  EXCEPT
  SELECT a FROM t WHERE a = 1
) UNION ALL SELECT 42
----
-- INTERSECT nested inside UNION ALL.
SELECT * FROM (
  SELECT a FROM t WHERE a <= 2
  INTERSECT
  SELECT a FROM t WHERE a = 1
) UNION ALL SELECT 42
----
-- EXCEPT inside UNION ALL with multiple literal inputs.
SELECT * FROM (
  SELECT a FROM t WHERE a <= 2
  EXCEPT
  SELECT a FROM t WHERE a = 1
) UNION ALL SELECT 42 UNION ALL SELECT 99
----
-- Table scan + literal in UNION ALL.
SELECT * FROM (
  SELECT a FROM t
  UNION ALL SELECT 42
) ORDER BY 1
