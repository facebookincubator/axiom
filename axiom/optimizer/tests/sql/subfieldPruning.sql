-- connector: hive
-- setup
CREATE TABLE t AS
SELECT * FROM (VALUES
  (0, MAP(ARRAY[0, 1], ARRAY[ARRAY[10, 20], ARRAY[11, 21]]), ARRAY[1, 2, 3]),
  (1, MAP(ARRAY[0, 1], ARRAY[ARRAY[12, 22], ARRAY[13, 23]]), ARRAY[4, 5, 6])
) AS _(k, m, a)
-- end_setup

-- Table t(k INTEGER, m MAP(INTEGER, ARRAY(INTEGER)), a ARRAY(INTEGER)):
--   k | m                              | a
--  ---+--------------------------------+-----------
--   0 | {0: [10, 20], 1: [11, 21]}     | [1, 2, 3]
--   1 | {0: [12, 22], 1: [13, 23]}     | [4, 5, 6]
--
-- The scan reads only the parts of a column the query uses. DuckDB's map
-- subscript returns a list of the matching values, so map queries compare
-- against an extra [1].

-- An element of the value at a constant map key.
-- duckdb: SELECT k, m[1][1][2] FROM t
SELECT k, m[1][2] FROM t
----
-- An element of the value at a key computed per row.
-- duckdb: SELECT k, m[k][1][1] FROM t
SELECT k, m[k][1] FROM t
----
-- One map read by a constant key and by a computed key.
-- duckdb: SELECT k, m[0][1][1], m[k][1][1] FROM t
SELECT k, m[0][1], m[k][1] FROM t
----
-- One map read by a constant key and by a computed key, at different elements.
-- duckdb: SELECT k, m[0][1][2], m[k][1][1] FROM t
SELECT k, m[0][2], m[k][1] FROM t
----
-- An array element by index.
SELECT k, a[2] FROM t
----
-- An array element counted from the end.
-- duckdb: SELECT k, a[-1] FROM t
SELECT k, element_at(a, -1) FROM t
