-- Join tests: various join types, self-joins, cross joins, and edge cases
-- around column naming and subquery handling.

-- JOIN with UNION ALL subquery.
SELECT t1.a, t1.b
FROM t t1 JOIN (SELECT a FROM t WHERE a = 1 UNION ALL SELECT a FROM t WHERE a = 2) t2 ON t1.a = t2.a
----
-- LEFT JOIN with cardinality(coalesce(...)) in WHERE clause.
-- duckdb: SELECT a, 2, NULL FROM t
WITH s AS (SELECT a, ARRAY[a, b] AS numbers FROM t),
r AS (SELECT a, ARRAY[a, b] AS numbers FROM t WHERE false)
SELECT s.a, cardinality(coalesce(r.numbers, s.numbers)), cardinality(r.numbers)
FROM s LEFT JOIN r ON s.a = r.a WHERE cardinality(coalesce(r.numbers, s.numbers)) > 0
----
-- LEFT JOIN with element_at(coalesce(...)) in WHERE clause.
-- duckdb: SELECT a, b FROM t
WITH s AS (SELECT a, ARRAY[a, b] AS numbers FROM t),
r AS (SELECT a, ARRAY[a, b] AS numbers FROM t WHERE false)
SELECT s.a, element_at(coalesce(r.numbers, s.numbers), 2)
FROM s LEFT JOIN r ON s.a = r.a WHERE element_at(coalesce(r.numbers, s.numbers), 1) > 0
----
-- Self-join with SELECT * produces duplicate column names on both sides.
SELECT * FROM t t1 JOIN t t2 ON t1.a = t2.a
----
-- Self-join with explicit column references from both sides.
SELECT t1.a, t2.a FROM t t1 JOIN t t2 ON t1.a = t2.a
----
-- Three-way self-join with SELECT *.
SELECT * FROM t t1 JOIN t t2 ON t1.a = t2.a JOIN t t3 ON t1.a = t3.a
----
-- WHERE filter after join with duplicate column names.
SELECT * FROM t t1 JOIN t t2 ON t1.a = t2.a WHERE t1.b > 0
----
-- CROSS JOIN with duplicate column names (production bug pattern).
SELECT * FROM (SELECT 2 AS entries) t1 CROSS JOIN (SELECT 4 AS entries) t2
----
-- LEFT JOIN with duplicate column names.
SELECT * FROM t t1 LEFT JOIN t t2 ON t1.a = t2.a
----
-- Aggregation after join with duplicate column names.
SELECT count(*) FROM t t1 JOIN t t2 ON t1.a = t2.a
