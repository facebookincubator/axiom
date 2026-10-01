-- setup_file: common_setup.sql

SELECT CAST(c AS DECIMAL(4,1)) * CAST(b AS DOUBLE) FROM t
----
SELECT CAST(c AS DECIMAL(4,1)) + CAST(b AS DOUBLE) FROM t
----
SELECT CAST(c AS DECIMAL(4,1)) - CAST(b AS DOUBLE) FROM t
----
SELECT CAST(c AS DECIMAL(10,2)) * c FROM t
----
SELECT CAST(b AS DECIMAL(10,0)) * c FROM t
----
SELECT a, SUM(CAST(c AS DECIMAL(4,1)) * CAST(b AS DOUBLE)) FROM t GROUP BY a
----
SELECT CAST(c AS DECIMAL(4,1)) = CAST(b AS DOUBLE) FROM t
----
SELECT CAST(c AS DECIMAL(4,1)) / CAST(b AS DOUBLE) FROM t
----
SELECT SIN(CAST(c AS DECIMAL(4,1))) FROM t
----
-- duckdb: SELECT 2
SELECT cardinality(
    CAST(ARRAY[CAST('2803:6082:f836::/48' AS ipprefix), CAST('2803:6082:f837::/48' AS ipprefix)]
            AS array(varchar)))
----
-- Expressions in a VALUES list are evaluated, and a NULL takes its type from
-- the other rows.
SELECT * FROM (VALUES (1 + 2, CAST(0.1 AS REAL), 'foo'), (10 + 20, CAST(0.2 AS REAL), NULL)) AS t(a, b, c)
----
-- A NULL initial state of reduce takes the type the input lambda returns.
-- duckdb: SELECT 1
SELECT reduce(ARRAY[CAST(ROW(1) AS ROW(a INTEGER))], NULL, (s, x) -> COALESCE(s, x), s -> s.a)
----
-- duckdb: SELECT 'p/a'
SELECT reduce(ARRAY['a'], NULL, (s, x) -> x, s -> CASE WHEN s IS NULL THEN '' ELSE 'p/' || s END)
