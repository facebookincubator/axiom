-- setup_file: common_setup.sql

-- A non-recursive CTE is not in scope within its own body, so a nested
-- same-name WITH resolves to the enclosing binding. These queries prove the
-- correctly-bound CTEs return the right values, not just that they parse.

-- Single-level shadowing. Inner body binds to outer t(a), main query to inner
-- t(b, c): a=1 -> b=1, c=2 -> r=3.
WITH t(a) AS (SELECT 1)
SELECT * FROM (
  WITH t(b, c) AS (SELECT a, a + 1 FROM t)
  SELECT b + c AS r FROM t) sub
----
-- Multi-level shadowing: inner reads mid's b, mid reads outer's a.
-- DuckDB rejects nested same-name CTEs as a circular reference, so the expected
-- result is supplied directly.
-- duckdb: SELECT 1 AS r
WITH t(a) AS (SELECT 1)
SELECT * FROM (
  WITH t(b) AS (SELECT a FROM t)
  SELECT * FROM (WITH t(c) AS (SELECT b FROM t) SELECT c AS r FROM t) s2) s1
