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
-- A repeated equi-condition joins on that pair once.
SELECT * FROM (VALUES (1)) t(a) JOIN (VALUES (1)) u(b) ON t.a = u.b AND t.a = u.b
----
-- Same, with the repeat written in the opposite orientation.
SELECT * FROM (VALUES (1)) t(a) JOIN (VALUES (1)) u(b) ON t.a = u.b AND u.b = t.a
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
-- RIGHT JOIN with a non-equi ON against a FROM-less scalar subquery and a
-- null-rejecting WHERE on the optional side. Returns the optional-side rows
-- that pass the predicate.
SELECT a.*
FROM t AS a
RIGHT JOIN (SELECT (SELECT c FROM t LIMIT 1) AS c0) AS u ON a.a < u.c0
WHERE a.b = 10
----
-- RIGHT JOIN with equi-key ON conditions against two FROM-less scalar
-- subqueries and a null-rejecting WHERE on the optional side. Returns the
-- single optional-side row whose keys match.
SELECT a.*
FROM t AS a
RIGHT JOIN (
  SELECT (SELECT a FROM t ORDER BY a LIMIT 1) AS a0,
         (SELECT b FROM t ORDER BY b LIMIT 1) AS b0
) AS u ON a.a = u.a0 AND a.b = u.b0
WHERE a.b = 10
----
-- FULL JOIN with a null-rejecting conjunct on the right input demotes to RIGHT,
-- so the right input keeps its unmatched rows.
SELECT a, b, x, y
FROM (VALUES (1, 10), (2, 20)) t(a, b)
FULL JOIN (VALUES (1, 1), (3, 3)) u(x, y) ON a = x
WHERE y > 0
----
-- FULL JOIN with a null-rejecting conjunct on the left input demotes to LEFT,
-- so the left input keeps its unmatched rows.
SELECT a, b, x, y
FROM (VALUES (1, 10), (2, 20)) t(a, b)
FULL JOIN (VALUES (1, 1), (3, 3)) u(x, y) ON a = x
WHERE a > 0
----
-- FULL JOIN with null-rejecting conjuncts on both inputs demotes to INNER.
SELECT a, b, x, y
FROM (VALUES (1, 10), (2, 20)) t(a, b)
FULL JOIN (VALUES (1, 1), (3, 3)) u(x, y) ON a = x
WHERE a > 0 AND y > 0
----
-- FULL JOIN whose only conjunct does not reject nulls stays FULL.
SELECT a, b, x, y
FROM (VALUES (1, 10), (2, 20)) t(a, b)
FULL JOIN (VALUES (1, 1), (3, 3)) u(x, y) ON a = x
WHERE coalesce(y, 1) > 0
----
-- The projected b is the left side's, not the right side's filtered b.
SELECT t_left.b FROM t t_left JOIN t t_right ON t_left.a = t_right.a WHERE t_right.b = 10
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
-- 20-way FULL JOIN of an aggregate over a table with no statistics, on a key
-- USING coalesces. Verifies the chain plans and returns the matched rows.
WITH base AS (
  SELECT a, b, count(*) AS n
  FROM t_no_stats
  GROUP BY 1, 2
)
SELECT b, t1.n AS n1, t2.n AS n2, t3.n AS n3, t4.n AS n4, t5.n AS n5, t6.n AS n6, t7.n AS n7, t8.n AS n8, t9.n AS n9, t10.n AS n10, t11.n AS n11, t12.n AS n12, t13.n AS n13, t14.n AS n14, t15.n AS n15, t16.n AS n16, t17.n AS n17, t18.n AS n18, t19.n AS n19, t20.n AS n20
FROM base t1
FULL JOIN base t2 USING (b)
FULL JOIN base t3 USING (b)
FULL JOIN base t4 USING (b)
FULL JOIN base t5 USING (b)
FULL JOIN base t6 USING (b)
FULL JOIN base t7 USING (b)
FULL JOIN base t8 USING (b)
FULL JOIN base t9 USING (b)
FULL JOIN base t10 USING (b)
FULL JOIN base t11 USING (b)
FULL JOIN base t12 USING (b)
FULL JOIN base t13 USING (b)
FULL JOIN base t14 USING (b)
FULL JOIN base t15 USING (b)
FULL JOIN base t16 USING (b)
FULL JOIN base t17 USING (b)
FULL JOIN base t18 USING (b)
FULL JOIN base t19 USING (b)
FULL JOIN base t20 USING (b)
----
-- Greedy join enumeration (>= 5 tables) driven by a UNION ALL subquery.
SELECT b.v, s.k
FROM (SELECT k FROM (VALUES (1)) AS t (k) UNION ALL SELECT k FROM (VALUES (1)) AS t (k)) AS s
JOIN (VALUES (1)) AS a (k) ON s.k = a.k
JOIN (VALUES (1)) AS c (k) ON s.k = c.k
JOIN (VALUES (1)) AS d (k) ON s.k = d.k
JOIN (VALUES (1, 2)) AS b (k, v) ON s.k = b.k
----
-- Cross join where one relation has no equi-predicate and is connected
-- only by an inequality: the theta predicate must still be applied.
SELECT t1.a, t2.a FROM t t1, t t2, t t3 WHERE t1.a = t3.a AND t1.b < t2.b
----
-- A join filter discards an error raised by one conjunct for a row that
-- another conjunct evaluates to false. v2 computes 1000 / (150 - t1.b) in the
-- input instead, where nothing masks it, so the row with t1.b = 150 fails --
-- even though no pair containing it satisfies t1.b < t2.b.
-- error_v2: division by zero
SELECT t1.a, t2.a FROM t t1, t t2 WHERE t1.b < t2.b AND 1000 / (150 - t1.b) > t2.a
----
-- Two parallel equality chains connect t through u to v.
SELECT v.m
FROM (
  VALUES (1, 10)
) AS t(a, b)
JOIN (
  VALUES (1, 10), (1, 11)
) AS u(x, y) ON t.a = u.x AND t.b = u.y
JOIN (
  VALUES (1, 10, 1000), (1, 11, 1001)
) AS v(k, l, m) ON u.x = v.k AND u.y = v.l
----
-- An equality chain connects t through u to v alongside a direct t-v
-- equality.
SELECT v.m
FROM (
  VALUES (1, 10)
) AS t(a, b)
JOIN (
  VALUES (1), (9)
) AS u(x) ON t.a = u.x
JOIN (
  VALUES (1, 10, 1000), (9, 10, 1001)
) AS v(k, l, m) ON u.x = v.k AND t.b = v.l
----
-- JOIN of two ORDER BY ... LIMIT subqueries.
SELECT l.a, l.b, r.b
FROM (SELECT * FROM t ORDER BY b LIMIT 5) l JOIN (SELECT * FROM t ORDER BY b DESC LIMIT 5) r ON l.a = r.a
----
-- An inner join above a left join, on a column of the null-padded side wrapped
-- in COALESCE. The COALESCE does not reject nulls, so a row with no match on
-- the left join can still satisfy the inner join and reach the output with
-- NULLs for that side.
SELECT l.a, c.m, e.d
FROM (VALUES (1, 'x'), (2, 'y')) AS l(k, a)
LEFT JOIN (VALUES (1, 10)) AS c(k, m) ON c.k = l.k
JOIN (VALUES (1, 10, 'p'), (2, 0, 'q')) AS e(k, m, d)
  ON e.k = l.k AND e.m = coalesce(c.m, 0)
----
-- The same, with a WHERE predicate that reads both sides of the left join and
-- so can only be evaluated once both are joined.
SELECT l.a, c.m, e.d
FROM (VALUES (1, 'x'), (2, 'y')) AS l(k, a)
LEFT JOIN (VALUES (1, 10)) AS c(k, m) ON c.k = l.k
JOIN (VALUES (1, 10, 5), (2, 0, 7)) AS e(k, m, d)
  ON e.k = l.k AND e.m = coalesce(c.m, 0)
WHERE e.d < coalesce(c.m, 99)
----
-- One column equated to a column of each of two other tables. A filter on the
-- shared column must reach every leg and select the same rows either way.
SELECT t.a, t.b
FROM t
JOIN t u ON u.a = t.a
JOIN t v ON v.b = u.b AND v.b = t.b
WHERE t.b > 100
