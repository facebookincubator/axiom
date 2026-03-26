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
