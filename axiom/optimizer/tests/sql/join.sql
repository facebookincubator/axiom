-- setup_file: common_setup.sql

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
-- LEFT-to-INNER JOIN conversion with aggregation. replaceJoinOutputs must not
-- replace post-aggregation references (exprs) with pre-aggregation expressions.
SELECT DISTINCT b.c
FROM t AS a
LEFT JOIN (
    SELECT a, CAST(c AS REAL) AS c FROM t
    CROSS JOIN UNNEST(ARRAY[1]) AS v(x)
) AS b ON a.a = b.a
WHERE b.a > 0
----
-- Two aliases of the same source column (v AS x, v AS y) from a LEFT JOIN.
-- Join output columns must not produce duplicates.
SELECT x, y
FROM (SELECT 1 AS k) AS a
LEFT JOIN (
    SELECT k, m, v AS x, v AS y
    FROM (SELECT 1 AS k, 1 AS m, 1 AS v)
) AS b ON a.k = b.k
----
-- Same join, with DISTINCT. The duplicate aliases must not produce duplicate
-- grouping keys in the aggregation.
SELECT DISTINCT b.x, b.y
FROM (SELECT 1 AS k) AS a
LEFT JOIN (
    SELECT k, m, v AS x, v AS y
    FROM (SELECT 1 AS k, 1 AS m, 1 AS v)
) AS b ON a.k = b.k
----
-- Same join, with DISTINCT and WHERE that converts LEFT to INNER. The
-- aggregation must not have duplicate grouping keys after join replacement.
SELECT DISTINCT b.x, b.y
FROM (SELECT 1 AS k) AS a
LEFT JOIN (
    SELECT k, m, v AS x, v AS y
    FROM (SELECT 1 AS k, 1 AS m, 1 AS v)
) AS b ON a.k = b.k
WHERE b.m = 1
----
-- CROSS JOIN UNNEST with two JOINs that have constant equality filters.
-- The reducing-join optimization must not prune columns needed by the output.
-- duckdb: VALUES (1, 2)
SELECT t.a, t.b
FROM (
    SELECT a, b, items
    FROM (VALUES
        (1, 2, ARRAY[ROW(10 AS k)]),
        (3, 4, ARRAY[ROW(20 AS k)])
    ) _(a, b, items)
) t
CROSS JOIN UNNEST(t.items) _(r)
JOIN (VALUES (1, 10), (1, 20)) u(c, k) ON u.k = r.k AND u.c = 1
JOIN (VALUES (1, 10)) v(c, k) ON v.k = u.k AND v.c = 1
----
-- Chained LEFT JOINs with same-named columns and GROUP BY.
-- a.ds must group by a's column, not c's.
SELECT a.ds
FROM (VALUES ('d1'), ('d2')) a(ds)
LEFT JOIN (VALUES ('d3')) b(ds) ON (a.ds = b.ds)
LEFT JOIN (SELECT 'x' as ds WHERE false) c ON (a.ds = c.ds)
GROUP BY 1
----
-- Same-table equality from equivalence class: a = b is inferred and pushed
-- as a filter on the left side. Only rows where a = b survive, projecting
-- (a, b): (1, 1), (3, 3), (5, 5).
SELECT t.a, t.b
FROM (VALUES (1, 1), (2, 20), (3, 3), (4, 40), (5, 5)) AS t(a, b)
JOIN (SELECT DISTINCT a FROM (VALUES (1), (2), (3), (4), (5)) AS v(a)) AS u(a)
  ON t.a = u.a AND t.b = u.a
----
-- LEFT JOIN slot synthesis: u.x = u.y inferred from u.x = t.a AND u.y = t.a.
SELECT t.a, u.x
FROM (VALUES (1), (2), (3)) AS t(a)
LEFT JOIN (VALUES (1, 1), (3, 3), (4, 5)) AS u(x, y)
  ON u.x = t.a AND u.y = t.a
----
-- SEMI join slot synthesis: u.x = u.y inferred, so only rows where x = y
-- survive the semi-join filter. t.a = 2 has no matching u row (2, 3 fails).
SELECT t.a
FROM (VALUES (1), (2), (3)) AS t(a)
WHERE EXISTS (
  SELECT 1 FROM (VALUES (1, 1), (2, 3), (3, 3)) AS u(x, y)
  WHERE u.x = t.a AND u.y = t.a
)
----
-- 8-way self-join hitting the greedy join-enumeration cutoff.
SELECT count(*)
FROM t t1
JOIN t t2 ON t1.b = t2.b
JOIN t t3 ON t2.b = t3.b
JOIN t t4 ON t3.b = t4.b
JOIN t t5 ON t4.b = t5.b
JOIN t t6 ON t5.b = t6.b
JOIN t t7 ON t6.b = t7.b
JOIN t t8 ON t7.b = t8.b
----
-- 20-way self-join. Stress-tests greedy at multiples of the default cutoff;
-- result count is validated end-to-end against DuckDB.
SELECT count(*)
FROM t t1
JOIN t t2 ON t1.b = t2.b
JOIN t t3 ON t2.b = t3.b
JOIN t t4 ON t3.b = t4.b
JOIN t t5 ON t4.b = t5.b
JOIN t t6 ON t5.b = t6.b
JOIN t t7 ON t6.b = t7.b
JOIN t t8 ON t7.b = t8.b
JOIN t t9 ON t8.b = t9.b
JOIN t t10 ON t9.b = t10.b
JOIN t t11 ON t10.b = t11.b
JOIN t t12 ON t11.b = t12.b
JOIN t t13 ON t12.b = t13.b
JOIN t t14 ON t13.b = t14.b
JOIN t t15 ON t14.b = t15.b
JOIN t t16 ON t15.b = t16.b
JOIN t t17 ON t16.b = t17.b
JOIN t t18 ON t17.b = t18.b
JOIN t t19 ON t18.b = t19.b
JOIN t t20 ON t19.b = t20.b
----
-- RIGHT JOIN whose inferred left keys span multiple tables (hyper-edge).
-- error: Failed to place a table
SELECT 1
FROM t AS ref_4
RIGHT JOIN (SELECT (SELECT c FROM t LIMIT 1) AS c0) AS subq_0
  ON CAST(null AS double) > subq_0.c0
WHERE ref_4.b = ref_4.b
