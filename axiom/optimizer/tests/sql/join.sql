-- setup_file: common_setup.sql
-- setup
-- A larger relation with a strict key subset gives joins a stable physical
-- orientation while retaining both matching and non-matching keys.
CREATE TABLE t_large AS
SELECT source.a AS k, source.b AS v
FROM t source CROSS JOIN t multiplier
WHERE source.a < 3
-- end_setup

-- A join condition no row satisfies keeps every left row and reads NULL for
-- the right side.
SELECT t1.a, t2.b FROM t t1 LEFT JOIN t t2 ON t1.a = t2.a AND 1 = 2
----
-- The same condition on an inner join yields nothing.
-- count 0
SELECT t1.a FROM t t1 JOIN t t2 ON t1.a = t2.a AND 1 = 2
----
-- A WHERE reading the side that never matches sees the NULLs the outer join
-- pads with: an IS NULL keeps every row.
SELECT t1.a FROM t t1 LEFT JOIN t t2 ON t1.a = t2.a AND 1 = 2 WHERE t2.b IS NULL
----
-- A comparison against those NULLs keeps none.
-- count 0
SELECT t1.a FROM t t1 LEFT JOIN t t2 ON t1.a = t2.a AND 1 = 2 WHERE t2.b > 5
----
-- A filter between two joins, kept out of both inputs by the outer join's
-- null padding, still lets the joins be reordered. Only a = 3 finds a match
-- above 140, so the surviving rows are the null-padded ones: pushing the
-- predicate into 'r' would keep a = 3 as well.
-- ordered
SELECT o.a, o.b
FROM t o
LEFT JOIN t r ON o.a = r.a AND r.b > 140
JOIN t s ON o.b = s.b
WHERE r.b IS NULL OR r.b > 200
ORDER BY o.a, o.b

----
-- The same shape over a full outer join. The predicate admits nulls on the
-- right side, so the rows the join pads there are kept.
SELECT count(*), count(o.a), count(r.b)
FROM t o
FULL OUTER JOIN t r ON o.a = r.a AND r.b > 140
LEFT JOIN t s ON o.b = s.b
WHERE r.b IS NULL OR r.b > 200
----
-- The same predicate on the left side, which a full outer join pads as well.
SELECT count(*), count(o.a), count(r.b)
FROM t o
FULL OUTER JOIN t r ON o.a = r.a AND r.b > 140
LEFT JOIN t s ON o.b = s.b
WHERE o.b IS NULL OR o.b > 200
----
-- An IS NULL filter on a column two nested left joins pad keeps only the row
-- the outer join pads.
SELECT t.k
FROM (VALUES (1), (2)) AS t(k)
LEFT JOIN (
  (VALUES (1)) AS u(k) LEFT JOIN (VALUES (1)) AS v(k) ON u.k = v.k
) ON t.k = u.k
JOIN (VALUES (1), (2)) AS w(k) ON w.k = t.k
WHERE v.k IS NULL
----
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
-- Both operand orders return the left key for matched and unmatched left rows.
SELECT coalesce(t_k, u_k), coalesce(u_k, t_k)
FROM (VALUES (1), (2), (NULL)) AS t(t_k)
LEFT JOIN (VALUES (1), (3)) AS u(u_k) ON t_k = u_k

----
-- Both operand orders return the right key for matched and unmatched right rows.
SELECT coalesce(t_k, u_k), coalesce(u_k, t_k)
FROM (VALUES (1), (2)) AS t(t_k)
RIGHT JOIN (VALUES (1), (3), (NULL)) AS u(u_k) ON t_k = u_k

----
-- An inner join with the smaller input written first returns all matching
-- rows and does not match NULL keys.
-- ordered
SELECT t_k, u_k
FROM (VALUES (1), (3), (NULL)) AS u(u_k)
JOIN (VALUES (1), (2), (3), (4), (NULL)) AS t(t_k) ON u_k = t_k
ORDER BY t_k, u_k

----
-- An inner join key is non-null in rows consumed by a subsequent outer join.
SELECT t.a, v.k
FROM (VALUES (NULL), (1)) AS t(a)
JOIN (VALUES (0), (1)) AS u(x) ON coalesce(t.a, 0) = u.x
LEFT JOIN (VALUES (0), (1), (2)) AS v(k) ON coalesce(u.x, 0) = v.k

----
-- NULL keys matched by INTERSECT remain nullable in a subsequent join.
SELECT i.x, v.k
FROM (
  SELECT x FROM (VALUES (NULL), (1)) AS a(x)
  INTERSECT
  SELECT x FROM (VALUES (NULL), (1)) AS b(x)
) AS i
LEFT JOIN (VALUES (0), (1)) AS v(k) ON coalesce(i.x, 0) = v.k

----
-- An empty scalar subquery supplies NULL to a subsequent join key.
SELECT s.x, v.k
FROM (SELECT (SELECT 1 FROM t WHERE a = 999) AS x) AS s
LEFT JOIN (VALUES (0), (1)) AS v(k) ON coalesce(s.x, 0) = v.k

----
-- An outer join makes a non-null input column nullable in subsequent joins.
SELECT t.a, u.x, v.k
FROM (VALUES (1)) AS t(a)
LEFT JOIN (VALUES (2)) AS u(x) ON t.a = u.x
LEFT JOIN (VALUES (0)) AS v(k) ON coalesce(u.x, 0) = v.k

----
-- A null-extended output remains available when an inner join below it
-- equates its source to a different column.
WITH
  t(k, a) AS (VALUES (0, 'x'), (1, 'y'), (2, 'z')),
  u(k, z) AS (VALUES (0, 'x'), (1, 'y')),
  s AS (
    SELECT t.k, u.z
    FROM u
    JOIN t ON u.z = t.a
  )
SELECT t.k, count(s.z)
FROM t
LEFT JOIN s ON t.k = s.k
GROUP BY t.k

----
-- Two inputs connected only through a third can join through that input.
SELECT t_k, u_k, k
FROM (VALUES (1), (2)) AS t(t_k)
CROSS JOIN (VALUES (1), (3)) AS u(u_k)
JOIN (VALUES (1), (2), (3)) AS v(k) ON t_k = k AND u_k = k

----
-- An equality that needs columns from both earlier inputs is still applied.
SELECT t_k, u_k, k
FROM (VALUES (1, 10), (2, 20)) AS t(t_k, t_c)
CROSS JOIN (VALUES (1, 1), (2, 2)) AS u(u_k, u_c)
JOIN (VALUES (1, 11), (2, 99)) AS v(k, c)
  ON t_k = k AND u_k = k AND t_c + u_c = c

----
-- A RIGHT theta join preserves unmatched right rows, including a NULL key.
-- ordered
SELECT t_k, u_k
FROM (VALUES (NULL), (2)) AS u(u_k)
RIGHT JOIN (VALUES (1), (3), (NULL)) AS t(t_k) ON u_k < t_k
ORDER BY t_k NULLS LAST, u_k NULLS LAST

----
-- Both operand orders return the available key for every FULL join row.
SELECT coalesce(t_k, u_k), coalesce(u_k, t_k)
FROM (VALUES (1), (2), (NULL)) AS t(t_k)
FULL JOIN (VALUES (1), (3), (NULL)) AS u(u_k) ON t_k = u_k

----
-- A null-padded key with non-default null behavior cannot be discarded.
SELECT coalesce(t_k, coalesce(u_k, 0))
FROM (VALUES (NULL), (1), (2)) AS t(t_k)
LEFT JOIN (VALUES (NULL), (1), (3)) AS u(u_k)
  ON t_k = coalesce(u_k, 0)

----
-- COALESCE of equal join keys can be rewritten after grouping by both keys.
SELECT coalesce(t_k, u_k), count(*)
FROM (VALUES (NULL), (1), (2)) AS t(t_k)
LEFT JOIN (VALUES (NULL), (1), (3)) AS u(u_k) ON t_k = u_k
GROUP BY t_k, u_k

----
-- Groups matched and unmatched LEFT JOIN rows by their available key.
SELECT coalesce(t_k, u_k) AS user_rid, count(*)
FROM (VALUES (NULL), (1), (2)) AS t(t_k)
LEFT JOIN (VALUES (NULL), (1), (3)) AS u(u_k) ON t_k = u_k
GROUP BY 1

----
-- Groups by the COALESCE of the join keys while counting the left key.
SELECT coalesce(t_k, u_k) AS user_rid, count(t_k)
FROM (VALUES (NULL), (1), (2)) AS t(t_k)
LEFT JOIN (VALUES (NULL), (1), (3)) AS u(u_k) ON t_k = u_k
GROUP BY 1

----
-- Groups by both the COALESCE of the join keys and the left key.
SELECT coalesce(t_k, u_k) AS user_rid, t_k, count(*)
FROM (VALUES (NULL), (1), (2)) AS t(t_k)
LEFT JOIN (VALUES (NULL), (1), (3)) AS u(u_k) ON t_k = u_k
GROUP BY 1, 2
----
-- A filter on an outer join's null-producing side does not hold for padded
-- rows above the join.
SELECT CASE WHEN matching.b = 10 THEN 1 ELSE preserved.b END
FROM t preserved
LEFT JOIN (SELECT a, b FROM t WHERE b = 10) matching
  ON preserved.a = matching.a
----
-- A constant in an outer join condition does not hold for padded rows above
-- the join.
SELECT CASE WHEN matching.b = 10 THEN 1 ELSE preserved.b END
FROM t preserved
LEFT JOIN t matching
  ON preserved.a = matching.a AND matching.b = 10
----
-- A constant from an inner join condition holds above the join.
SELECT CASE WHEN matching.b = 10 THEN 1 ELSE preserved.b END
FROM t preserved
JOIN t matching ON preserved.a = matching.a AND matching.b = 10
----
-- A constant computed above a full join keeps its value on rows where the
-- join null-extends an equal constant computed by one input.
SELECT padded.v, preserved.k, 0 AS above
FROM (SELECT k, 0 AS v FROM (VALUES (1)) AS input(k)) AS padded
FULL JOIN (VALUES (2)) AS preserved(k) ON padded.k = preserved.k
----
-- A left join's padded key passes through a full join above it when an inner
-- join on the padded side equates the key with another column.
SELECT t.k, u.z
FROM (VALUES (1), (2)) AS t(k)
LEFT JOIN (
  (SELECT 1 AS z UNION ALL SELECT 3) AS u
  JOIN (VALUES (1), (3)) AS v(k) ON u.z = v.k
) ON t.k = u.z
FULL JOIN (VALUES (1), (2)) AS w(k) ON t.k = w.k

----
-- Groups by the right key of an inner join while counting the left key.
SELECT u_k, count(t_k)
FROM (VALUES (NULL), (1), (2), (2)) AS t(t_k)
JOIN (VALUES (NULL), (1), (2), (3)) AS u(u_k) ON t_k = u_k
GROUP BY u_k

----
-- Returns both keys of an inner join.
SELECT t_k, u_k
FROM (VALUES (NULL), (1), (2), (2)) AS t(t_k)
JOIN (VALUES (NULL), (1), (2), (3)) AS u(u_k) ON t_k = u_k

----
-- Returns every key from a chain of inner joins.
SELECT t_k, u_k, k
FROM (VALUES (1), (2)) AS v(k)
JOIN (
  (VALUES (1), (2)) AS t(t_k)
  JOIN (VALUES (1), (2)) AS u(u_k) ON t_k = u_k
) ON k = u_k

----
-- Groups by both keys of an inner join.
SELECT u_k, t_k, count(*)
FROM (VALUES (NULL), (1), (2), (2)) AS t(t_k)
JOIN (VALUES (NULL), (1), (2), (3)) AS u(u_k) ON t_k = u_k
GROUP BY u_k, t_k

----
-- ROLLUP keeps equal inner-join keys as separate grouping dimensions.
SELECT u_k, t_k, grouping(u_k), grouping(t_k), count(*)
FROM (VALUES (NULL), (1), (2), (2)) AS t(t_k)
JOIN (VALUES (NULL), (1), (2), (3)) AS u(u_k) ON t_k = u_k
GROUP BY ROLLUP(u_k, t_k)

----
-- ROLLUP can null the two join keys independently.
SELECT coalesce(t_k, u_k), grouping(u_k), grouping(t_k), count(*)
FROM (VALUES (NULL), (1), (2)) AS t(t_k)
LEFT JOIN (VALUES (NULL), (1), (3)) AS u(u_k) ON t_k = u_k
GROUP BY ROLLUP(u_k, t_k)

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
-- A RIGHT JOIN preserves equal columns from its NULL-padded input.
-- duckdb: SELECT NULL::BIGINT, NULL::BIGINT, a FROM t CROSS JOIN UNNEST(ARRAY[1, 2, 3])
SELECT t.b, u.b, v.a
FROM t JOIN t AS u ON t.b = u.b
RIGHT JOIN (
    SELECT a, b
    FROM t CROSS JOIN UNNEST(ARRAY[1, 2, 3]) AS _(x)
) v ON t.a = v.b
----
-- Chained LEFT JOINs with same-named columns and GROUP BY.
-- a.ds must group by a's column, not c's.
SELECT a.ds
FROM (VALUES ('d1'), ('d2')) a(ds)
LEFT JOIN (VALUES ('d3')) b(ds) ON (a.ds = b.ds)
LEFT JOIN (SELECT 'x' as ds WHERE false) c ON (a.ds = c.ds)
GROUP BY 1
----
-- The right side of a join is itself a join, and all three relations have a
-- column 'x'. Each qualified name reads its own relation's column.
SELECT t.x, u.x, v.x
FROM (VALUES (1)) t(x)
JOIN ((VALUES (2)) u(x) CROSS JOIN (VALUES (3)) v(x)) ON true
----
-- The same when the left side has no alias.
SELECT u.x, v.x
FROM (SELECT 1 AS x)
JOIN ((VALUES (2)) u(x) CROSS JOIN (VALUES (3)) v(x)) ON true
----
-- The same when the inner join names its key with USING.
SELECT t.x, u.x, v.x
FROM (VALUES (1, 0)) t(x, k)
JOIN ((VALUES (2, 0)) u(x, k) JOIN (VALUES (3, 0)) v(x, k) USING (k)) ON true
----
-- A comma binds looser than JOIN, so u JOIN v is the right side. The filter
-- on u.ds reads u's column.
SELECT t.id, u.id, u.ds, v.ds
FROM (VALUES (1, 'd1'), (2, 'd2')) t(id, ds),
  (VALUES (1, 'd1'), (2, 'd2')) u(id, ds)
  JOIN (VALUES (1, 'd1'), (2, 'd2')) v(id, ds) ON u.id = v.id
WHERE t.ds = 'd1' AND u.ds = 'd2'
----
-- The same for a join condition in WHERE: t.k = u.k compares t with u.
SELECT count(*)
FROM (VALUES (1), (2), (3)) t(k),
  (VALUES (1), (2), (3)) u(k)
  JOIN (VALUES (1), (2), (3)) v(k) ON u.k = v.k
WHERE t.k = u.k
----
-- The same under a LEFT JOIN with a parenthesized right side.
SELECT t.ds, u.ds, v.ds
FROM (VALUES (1, 'd1'), (1, 'd2')) t(id, ds)
LEFT JOIN (
  (VALUES (1, 'd1'), (1, 'd2')) u(id, ds)
  JOIN (VALUES (1, 'd1'), (1, 'd2')) v(id, ds)
    ON u.id = v.id AND u.ds = v.ds)
  ON t.id = u.id
WHERE u.ds = 'd1'
----
-- GROUP BY and ORDER BY a qualified column of the right side.
-- ordered
SELECT u.b, count(*)
FROM t
JOIN (t u JOIN t v ON u.b = v.b) ON t.b + 10 = u.b
GROUP BY u.b
ORDER BY u.b
----
-- Without parentheses, 'JOIN u JOIN v ON .. ON ..' nests u JOIN v the same
-- way.
SELECT t.b, u.b, v.b
FROM t
JOIN t u JOIN t v ON u.b = v.b ON t.b + 10 = u.b
----
-- An alias over a join hides the aliases inside it, so 't.x' names the
-- relation outside it. DuckDB rejects a reused alias.
-- duckdb: VALUES (3)
SELECT t.x
FROM ((VALUES (1)) t(x) CROSS JOIN (VALUES (2)) t(x)) u
CROSS JOIN (VALUES (3)) t(x)
----
-- The same for a derived table.
-- duckdb: VALUES (3)
SELECT t.x
FROM (SELECT * FROM (VALUES (1)) t(x) CROSS JOIN (VALUES (2)) t(x))
CROSS JOIN (VALUES (3)) t(x)
----
-- 'w.*' expands to every column of the aliased join, including columns whose
-- names are ambiguous.
-- duckdb: VALUES (1, 2)
SELECT w.* FROM ((VALUES (1)) t(x) CROSS JOIN (VALUES (2)) u(x)) w
----
-- duckdb: VALUES (1, 2)
SELECT w.* FROM ((VALUES (1)) t(x) CROSS JOIN (VALUES (2)) t(x)) w
----
-- duckdb: VALUES (1, 2, 3)
SELECT w.*
FROM ((VALUES (1)) t(x) JOIN ((VALUES (2)) u(x) CROSS JOIN (VALUES (3)) v(x)) ON true) w
----
-- duckdb: VALUES (1, 2, 3)
SELECT w.*
FROM ((VALUES (1)) t(x) JOIN ((VALUES (2)) u(x) CROSS JOIN (VALUES (3)) u(x)) ON true) w
----
-- duckdb: VALUES (1, 5, 2, 3)
SELECT w.*
FROM ((VALUES (1, 5)) t(x, y) JOIN ((VALUES (2)) u(x) CROSS JOIN (VALUES (3)) v(x)) ON true) w
----
-- The same after a USING join with another relation, which keeps w's columns
-- except the USING column.
-- duckdb: VALUES (1, 2)
SELECT w.*
FROM ((VALUES (1, 0)) t(x, k) CROSS JOIN (VALUES (2)) u(x)) w
JOIN (VALUES (0)) v(k) USING (k)
----
-- 't.*' expands to the columns of both relations named 't'.
-- duckdb: VALUES (1, 2)
SELECT t.* FROM (VALUES (1)) t(x) CROSS JOIN (VALUES (2)) t(x)
----
-- A repeated equi-condition joins on that pair once.
SELECT * FROM (VALUES (1)) t(a) JOIN (VALUES (1)) u(b) ON t.a = u.b AND t.a = u.b
----
-- Same, with the repeat written in the opposite orientation.
SELECT * FROM (VALUES (1)) t(a) JOIN (VALUES (1)) u(b) ON t.a = u.b AND u.b = t.a
----
-- A filter below DISTINCT on the preserved side of a LEFT JOIN also restricts
-- matching rows from the nullable side.
SELECT d.a, count(u.b)
FROM (SELECT DISTINCT a FROM t WHERE a >= 2) d
LEFT JOIN t u ON d.a = u.a
GROUP BY d.a
----
-- The same propagation from the preserved input of a RIGHT JOIN.
SELECT d.a, count(t.b)
FROM t
RIGHT JOIN (SELECT DISTINCT a FROM t WHERE a >= 2) d ON t.a = d.a
GROUP BY d.a
----
-- A FULL JOIN cannot use a predicate from one input to restrict the other.
SELECT *
FROM (SELECT * FROM t WHERE a >= 2) t
FULL JOIN t u ON t.a = u.a
----
-- A filter can cross two joins when the first join preserves it on its output.
SELECT q.a, count(v.b)
FROM (
  SELECT d.a, u.a AS x
  FROM (SELECT DISTINCT a FROM t WHERE a >= 2) d
  JOIN t u ON d.a = u.a
) q
LEFT JOIN t v ON q.x = v.a
GROUP BY q.a
----
-- A filter on the preserved input of a semi join also restricts its matching
-- input.
SELECT t.a
FROM (SELECT * FROM t WHERE a >= 2) t
WHERE EXISTS (SELECT 1 FROM t u WHERE u.a = t.a)
----
-- A filter on the preserved input remains valid when EXISTS produces a mark.
SELECT t.a, EXISTS (SELECT 1 FROM t u WHERE u.a = t.a) AS matched
FROM (SELECT * FROM t WHERE a >= 2) t
----
-- NOT EXISTS preserves the filter while selecting non-matching rows.
-- count 0
SELECT t.a
FROM (SELECT * FROM t WHERE a >= 2) t
WHERE NOT EXISTS (SELECT 1 FROM t u WHERE u.a = t.a)
----
-- Null-aware marks and anti joins must retain NULLs on the matching input.
SELECT t.a, t.a IN (SELECT x FROM (VALUES (2), (null)) u(x)) AS matched
FROM (SELECT * FROM t WHERE a >= 2) t
----
-- count 0
SELECT t.a
FROM (SELECT * FROM t WHERE a >= 2) t
WHERE t.a NOT IN (SELECT x FROM (VALUES (2), (null)) u(x))
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
-- FULL JOIN with a null-rejecting conjunct on the right input reduces to RIGHT,
-- so the right input keeps its unmatched rows.
SELECT a, b, x, y
FROM (VALUES (1, 10), (2, 20)) t(a, b)
FULL JOIN (VALUES (1, 1), (3, 3)) u(x, y) ON a = x
WHERE y > 0
----
-- FULL JOIN with a null-rejecting conjunct on the left input reduces to LEFT,
-- so the left input keeps its unmatched rows.
SELECT a, b, x, y
FROM (VALUES (1, 10), (2, 20)) t(a, b)
FULL JOIN (VALUES (1, 1), (3, 3)) u(x, y) ON a = x
WHERE a > 0
----
-- FULL JOIN with null-rejecting conjuncts on both inputs reduces to INNER.
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
-- An inner join on a column of a LEFT JOIN's null-padded side drops the padded
-- rows. A window between the two joins still sees them: count(*) counts all 15
-- rows, and each partition of l.a holds 5.
SELECT s.b, s.rb, s.cnt, s.cnt_a
FROM (
  SELECT l.b, r.b AS rb,
         count(*) OVER () AS cnt,
         count(*) OVER (PARTITION BY l.a) AS cnt_a
  FROM t l LEFT JOIN t r ON l.b = r.b AND r.b IN (100, 140, 150)
) s
JOIN t u ON s.rb = u.b
----
-- The same for a rank: the padded rows rank ahead of b = 100.
SELECT s.b, s.rb, s.rnk
FROM (
  SELECT l.b, r.b AS rb, rank() OVER (ORDER BY l.b DESC) AS rnk
  FROM t l LEFT JOIN t r ON l.b = r.b AND r.b IN (100, 140, 150)
) s
JOIN t u ON s.rb = u.b
----
-- A window ordered by the inner join's column still reads the padded rows:
-- they sort first, so b = 100 is row 13.
SELECT s.b, s.rb, s.rn
FROM (
  SELECT l.b, r.b AS rb,
         row_number() OVER (ORDER BY r.b NULLS FIRST, l.b) AS rn
  FROM t l LEFT JOIN t r ON l.b = r.b AND r.b IN (100, 140, 150)
) s
JOIN t u ON s.rb = u.b
----
-- The top row of each l.a partition. It is padded for a = 1, so the inner join
-- drops that partition.
SELECT s.a, s.rb
FROM (
  SELECT l.a, r.b AS rb,
         row_number() OVER (PARTITION BY l.a ORDER BY l.b DESC) AS rn
  FROM t l LEFT JOIN t r ON l.b = r.b AND r.b IN (100, 140, 150)
) s
JOIN t u ON s.rb = u.b
WHERE s.rn = 1
----
-- A window partitioned by the inner join's column: the padded rows form the
-- partition the inner join drops.
SELECT s.b, s.rb, s.cnt
FROM (
  SELECT l.b, r.b AS rb, count(*) OVER (PARTITION BY r.b) AS cnt
  FROM t l LEFT JOIN t r ON l.b = r.b AND r.b IN (100, 140, 150)
) s
JOIN t u ON s.rb = u.b
----
-- A window partitioned by an expression that is also NULL for b = 100: that
-- row shares the partition of the 12 padded rows, so its count is 13.
SELECT s.b, s.rb, s.cnt
FROM (
  SELECT l.b, r.b AS rb, count(*) OVER (PARTITION BY nullif(r.b, 100)) AS cnt
  FROM t l LEFT JOIN t r ON l.b = r.b AND r.b IN (100, 140, 150)
) s
JOIN t u ON s.rb = u.b
----
-- A FULL JOIN keeps its padded rows under a window as well: count(*) counts
-- all 15 rows.
SELECT s.lb, s.rb, s.cnt
FROM (
  SELECT l.b AS lb, r.b AS rb, count(*) OVER () AS cnt
  FROM (SELECT b FROM t WHERE b <= 100) l
  FULL JOIN (SELECT b FROM t WHERE b >= 100) r ON l.b = r.b
) s
JOIN t u ON s.rb = u.b
----
-- ORDER BY with LIMIT keeps the three largest b, and one of them is padded.
SELECT s.b, s.rb
FROM (
  SELECT l.b, r.b AS rb
  FROM t l LEFT JOIN t r ON l.b = r.b AND r.b IN (100, 140, 150)
  ORDER BY l.b DESC
  LIMIT 3
) s
JOIN t u ON s.rb = u.b
----
-- ORDER BY with OFFSET skips the twelve smallest b, and 100 is among them.
SELECT s.b, s.rb
FROM (
  SELECT l.b, r.b AS rb
  FROM t l LEFT JOIN t r ON l.b = r.b AND r.b IN (100, 140, 150)
  ORDER BY l.b
  OFFSET 12
) s
JOIN t u ON s.rb = u.b
----
-- ORDER BY with LIMIT over a RIGHT JOIN, whose null-padded side is on the left.
SELECT s.b, s.rb
FROM (
  SELECT l.b, r.b AS rb
  FROM t r RIGHT JOIN t l ON l.b = r.b AND r.b IN (100, 140, 150)
  ORDER BY l.b DESC
  LIMIT 3
) s
JOIN t u ON s.rb = u.b
----
-- A scalar subquery over the LEFT JOIN returns its 15 rows, padded ones
-- included, and fails.
-- error: Expected single row of input.
SELECT u.b
FROM t u
JOIN (
  SELECT (SELECT r.b FROM t l LEFT JOIN t r ON l.b = r.b AND r.b = 100) AS rb
) s ON u.b = s.rb
----
-- The same for a correlated scalar subquery, which returns 5 rows for each o.a.
-- error: Scalar sub-query has returned multiple rows
SELECT s.a, s.rb
FROM (
  SELECT o.a,
         (SELECT r.b FROM t l LEFT JOIN t r ON l.b = r.b AND r.b = 100 WHERE l.a = o.a) AS rb
  FROM t o
) s
JOIN t u ON s.rb = u.b
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
-- An untaken IF branch does not raise its error, including when the condition
-- and that branch read different tables.
SELECT t1.a, t2.a FROM t t1, t t2 WHERE if(t1.b > 0, true, 1000 / (150 - t2.b) > 0)
----
-- TRY catches an error raised by the expression it guards, including when that
-- expression reads more than one table.
-- duckdb: VALUES (100, 1), (100, 2)
SELECT t1.b, t2.a FROM (VALUES (150), (100)) AS t1(b), (VALUES (1), (2)) AS t2(a)
WHERE try(1000 / (150 - t1.b) > t2.a)
----
-- A self-join must enforce every equality after reordering.
WITH ids (id) AS (VALUES (1)),
     labels (id, label) AS (VALUES (1, 'a'), (1, 'b')),
     extra (id) AS (VALUES (1)),
     labeled AS (
       SELECT ids.id, labels.label
       FROM ids
       JOIN labels ON labels.id = ids.id
       LEFT JOIN extra ON extra.id = ids.id)
SELECT count(*)
FROM labeled AS x
JOIN labeled AS y ON x.id = y.id AND x.label = y.label
----
-- A join-key operand that references two relations must remain applicable
-- when the third relation joins.
SELECT m
FROM (VALUES (1, 10)) AS t(a, b)
JOIN (VALUES (1, 1, 100)) AS u(x, y, z) ON t.a = u.x
JOIN (VALUES (100, 11, 1001), (100, 99, 1002)) AS v(k, l, m)
  ON u.z = v.k AND t.b + u.y = v.l
----
-- Reordering a semi-join nested between inner joins must preserve every
-- equality and return the matching row.
-- duckdb: SELECT 1 AS s
WITH n AS (
       SELECT i AS k
       FROM UNNEST(sequence(1, 1)) AS t(i)),
     b(k) AS (VALUES (1)),
     d(k) AS (VALUES (1))
SELECT ax.k
FROM (
  SELECT a.k
  FROM n AS a
  JOIN n AS x ON a.k = x.k
  WHERE EXISTS (
    SELECT 1
    FROM b LEFT JOIN d ON b.k = d.k
    WHERE b.k = a.k)
) AS ax
JOIN n AS c ON ax.k = c.k
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
----
-- Two columns of one relation equated to columns of two different relations.
-- Neither equality implies the other, so both must be applied.
SELECT count(*)
FROM (VALUES (1), (2)) AS t(x)
JOIN (VALUES (1), (1), (2)) AS u(z) ON t.x = u.z
JOIN (VALUES (1, 1), (1, 2), (2, 2)) AS v(p, q) ON v.p = t.x AND v.q = u.z
----
-- The same, returning the equated columns rather than a count.
SELECT t.x, u.z, v.p, v.q
FROM (VALUES (1), (2)) AS t(x)
JOIN (VALUES (1), (1), (2)) AS u(z) ON t.x = u.z
JOIN (VALUES (1, 1), (1, 2), (2, 2)) AS v(p, q) ON v.p = t.x AND v.q = u.z
----
-- Two columns of one relation equated to the same column of another.
SELECT count(*) FROM t JOIN t u ON t.a = u.a AND t.b = u.a
----
-- Set operations match NULL to NULL, so a leg holding NULLs in both of the
-- columns another leg equates must still be matched.
SELECT x, x FROM (VALUES (null), (1)) AS t(x)
INTERSECT
SELECT p, q FROM (VALUES (null, null), (1, 1), (2, 3)) AS u(p, q)
----
-- The same for EXCEPT.
SELECT x, x FROM (VALUES (null), (1), (2)) AS t(x)
EXCEPT
SELECT p, q FROM (VALUES (null, null), (1, 1)) AS u(p, q)
----
-- Both of u's columns are equated to the same column of t, so a u row only
-- joins when they agree. Rows of t that join nothing are still returned.
SELECT t.a, u.x
FROM (VALUES (1), (2), (3)) AS t(a)
LEFT JOIN (VALUES (1, 1), (null, 1), (1, null), (2, 2)) AS u(x, y)
  ON u.x = t.a AND u.y = t.a
----
-- The same condition under EXISTS.
SELECT a FROM (VALUES (1), (2), (3)) AS t(a)
WHERE EXISTS (
  SELECT 1 FROM (VALUES (1, 1), (null, 1), (1, null), (2, 2)) AS u(x, y)
  WHERE u.x = t.a AND u.y = t.a)
----
-- The same condition under NOT EXISTS, where a row of t is returned only
-- when no row of u joins it.
SELECT a FROM (VALUES (1), (2), (3)) AS t(a)
WHERE NOT EXISTS (
  SELECT 1 FROM (VALUES (1, 1), (null, 1), (1, null), (2, 2)) AS u(x, y)
  WHERE u.x = t.a AND u.y = t.a)
----
-- A one-row map used as a broadcast column by multiple lookups.
-- duckdb: SELECT a, 10000 + a, 10001 + a FROM t
WITH lookup(keys, map_values) AS (
  VALUES (
    sequence(1, 10000),
    sequence(10001, 20000)
  )
)
SELECT a, map(keys, map_values)[a], map(keys, map_values)[a + 1]
FROM t CROSS JOIN lookup
----
-- Grouping a full outer join by the coalesce of its key pair.
SELECT coalesce(o.a, r.a) AS k, count(*)
FROM t o
FULL OUTER JOIN t r ON o.a = r.a AND r.b > 140
GROUP BY 1
----
-- Join the coalesced key from a full join that has an additional key.
SELECT coalesce(t1.a, t2.a) AS k
FROM t t1
FULL OUTER JOIN t t2 ON t1.a = t2.a AND t1.b = t2.b
JOIN t t3 ON coalesce(t1.a, t2.a) = t3.a
ORDER BY 1
----
-- A right join grouped by its preserved right key.
SELECT preserved.a, count(*)
FROM t_large matching
RIGHT JOIN t preserved ON matching.k = preserved.a
GROUP BY preserved.a
----
-- A right join feeding a left join on its preserved right key. The second
-- join condition also reads the first join's null-supplying side.
SELECT preserved.a, count(parent.a)
FROM t_large matching
RIGHT JOIN t preserved ON matching.k = preserved.a
LEFT JOIN t parent
  ON preserved.a = parent.a
  AND (matching.k IS NULL OR matching.v < parent.b)
GROUP BY preserved.a
----
-- EXISTS grouped by its correlated outer key.
SELECT outer_table.a, count(*)
FROM t outer_table
WHERE EXISTS (
  SELECT 1 FROM t_large inner_table
  WHERE inner_table.k = outer_table.a)
GROUP BY outer_table.a
----
-- NOT EXISTS grouped by its correlated outer key.
SELECT outer_table.a, count(*)
FROM t outer_table
WHERE NOT EXISTS (
  SELECT 1 FROM t_large inner_table
  WHERE inner_table.k = outer_table.a)
GROUP BY outer_table.a
----
-- NOT IN grouped by its left-hand key.
SELECT outer_table.a, count(*)
FROM t outer_table
WHERE outer_table.a NOT IN (SELECT k FROM t_large)
GROUP BY outer_table.a
----
-- A right join with an empty null-supplying side emits every preserved row,
-- padded with nulls, before the aggregation.
SELECT preserved.a, missing.y, count(*)
FROM (
  SELECT *
  FROM (VALUES (1, 10)) AS empty_side(x, y)
  WHERE false
) missing
RIGHT JOIN (VALUES (1), (2)) AS preserved(a)
  ON missing.x = preserved.a
GROUP BY 1, 2
