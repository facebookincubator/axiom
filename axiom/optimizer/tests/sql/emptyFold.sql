-- setup_file: common_setup.sql

-- Queries over empty inputs.

-- Empty scan returns no rows.
-- count 0
SELECT a FROM (SELECT * FROM t LIMIT 0) s
----
-- LIMIT 0 returns no rows.
-- count 0
SELECT * FROM t LIMIT 0
----
-- ORDER BY + LIMIT 0 returns no rows.
-- count 0
SELECT * FROM t ORDER BY a LIMIT 0
----
-- Inner (equi) join with an empty side returns no rows.
-- count 0
SELECT t.a AS ta, s.a AS sa FROM t JOIN (SELECT * FROM t LIMIT 0) s ON t.a = s.a
----
-- Cross join with an empty side returns no rows.
-- count 0
SELECT t.a AS ta, s.a AS sa FROM t CROSS JOIN (SELECT * FROM t LIMIT 0) s
----
-- Chained inner joins return no rows when any input is empty.
-- count 0
SELECT t1.a FROM t t1 JOIN t t2 ON t1.a = t2.a JOIN (SELECT a FROM t LIMIT 0) e ON t2.a = e.a
----
-- Projection, filter, order by, and limit over empty input return no rows.
-- count 0
SELECT a + 1 AS x FROM (SELECT * FROM t LIMIT 0) s WHERE a > 1 ORDER BY 1 LIMIT 5
----
-- Window over empty input returns no rows.
-- count 0
SELECT row_number() OVER (ORDER BY a) AS rn FROM (SELECT * FROM t LIMIT 0) s
----
-- GROUP BY over empty input returns no rows.
-- count 0
SELECT a, count(*) AS n FROM (SELECT * FROM t LIMIT 0) s GROUP BY a
----
-- DISTINCT over empty input returns no rows.
-- count 0
SELECT DISTINCT a FROM (SELECT * FROM t LIMIT 0) s
----
-- Global aggregation over empty input returns one row.
SELECT count(*) AS n FROM (SELECT * FROM t LIMIT 0) s
----
-- Global aggregation with a column argument over empty input (one row, NULL).
SELECT sum(a) AS s FROM (SELECT * FROM t LIMIT 0) x
----
-- Global aggregation over an expression on empty input (one row, NULL).
SELECT sum(a + 1) AS s FROM (SELECT * FROM t LIMIT 0) x
----
-- Left join with an empty right side preserves the left rows, null-filling right.
SELECT t.a AS ta, s.a AS sa FROM t LEFT JOIN (SELECT * FROM t LIMIT 0) s ON t.a = s.a
----
-- Right join with an empty left side preserves the right rows, null-filling left.
SELECT e.a AS ea, t.a AS ta FROM (SELECT * FROM t LIMIT 0) e RIGHT JOIN t ON e.a = t.a
----
-- Full join with one empty side preserves the non-empty side, null-filling the other.
SELECT t.a AS ta, s.a AS sa FROM t FULL JOIN (SELECT * FROM t LIMIT 0) s ON t.a = s.a
----
-- Left join with an empty preserved side returns no rows.
-- count 0
SELECT e.a AS ea, t.a AS ta FROM (SELECT * FROM t LIMIT 0) e LEFT JOIN t ON e.a = t.a
----
-- Right join with an empty preserved side returns no rows.
-- count 0
SELECT t.a AS ta, e.a AS ea FROM t RIGHT JOIN (SELECT * FROM t LIMIT 0) e ON t.a = e.a
----
-- Full join with both sides empty returns no rows.
-- count 0
SELECT e1.a AS a1, e2.a AS a2 FROM (SELECT * FROM t LIMIT 0) e1 FULL JOIN (SELECT * FROM t LIMIT 0) e2 ON e1.a = e2.a
----
-- Semi join (EXISTS) with an empty filtering side yields no rows.
-- count 0
SELECT a FROM t WHERE EXISTS (SELECT 1 FROM (SELECT * FROM t LIMIT 0) s WHERE s.a = t.a)
----
-- Anti join (NOT EXISTS) with an empty filtering side keeps all rows.
SELECT a FROM t WHERE NOT EXISTS (SELECT 1 FROM (SELECT * FROM t LIMIT 0) s WHERE s.a = t.a)
----
-- INTERSECT ALL with an empty side returns no rows.
-- count 0
SELECT a FROM t INTERSECT ALL SELECT a FROM (SELECT * FROM t LIMIT 0) s
----
-- IN over an empty subquery keeps input rows and marks the predicate false.
SELECT a IN (SELECT a FROM (SELECT * FROM t LIMIT 0) s) AS in_empty FROM t
----
-- EXCEPT ALL with an empty right side keeps the left rows.
-- duckdb: SELECT a FROM t
SELECT a FROM t EXCEPT ALL SELECT a FROM (SELECT * FROM t LIMIT 0) s
----
-- GROUPING SETS without a global grouping set over empty input returns no rows.
-- count 0
SELECT a, b, count(*) AS n FROM (SELECT * FROM t LIMIT 0) s GROUP BY GROUPING SETS ((a), (b))
----
-- GROUPING SETS with a global grouping set over empty input keeps the grand-total row.
SELECT a, count(*) AS n FROM (SELECT * FROM t LIMIT 0) s GROUP BY GROUPING SETS ((a), ())
----
-- UNNEST over empty input returns no rows.
-- count 0
SELECT item FROM (SELECT * FROM t LIMIT 0) s CROSS JOIN UNNEST(ARRAY[a]) AS u(item)
----
-- UNION ALL with an empty second input keeps the first.
SELECT a FROM t UNION ALL SELECT a FROM (SELECT * FROM t LIMIT 0) s
----
-- UNION ALL with an empty first input keeps the second.
SELECT a FROM (SELECT * FROM t LIMIT 0) s UNION ALL SELECT a FROM t
----
-- UNION ALL with all inputs empty returns no rows.
-- count 0
SELECT a FROM (SELECT * FROM t LIMIT 0) s UNION ALL SELECT a FROM (SELECT * FROM t LIMIT 0) r
----
-- UNION ALL with one empty input and two surviving inputs keeps the survivors.
SELECT a FROM t UNION ALL SELECT a FROM (SELECT * FROM t LIMIT 0) s UNION ALL SELECT a FROM t
----
-- UNION ALL whose single surviving leg is a TopN preserves ORDER BY + LIMIT.
(SELECT a FROM t ORDER BY a LIMIT 3) UNION ALL SELECT a FROM (SELECT * FROM t LIMIT 0) s
----
-- UNION ALL with reordered and duplicated columns returns the surviving rows.
SELECT b, a, b FROM t UNION ALL SELECT a, b, a FROM (SELECT * FROM t LIMIT 0) s
----
-- UNION DISTINCT with one empty input keeps the dedup over the non-empty input.
SELECT a FROM (SELECT * FROM t LIMIT 0) s UNION SELECT a FROM t
