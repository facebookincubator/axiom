-- setup_file: common_setup.sql
-- setup
CREATE TABLE u AS FROM (VALUES (1), (2), (3), (4), (5)) AS _(a)
----
CREATE TABLE v AS FROM (VALUES (2), (4), (6), (8), (10)) AS _(a)
-- end_setup

-- Subqueries inside a subquery body, and correlated IN / NOT IN.


-- A correlated subquery whose body filters on an IN subquery.
SELECT t.a, (SELECT count(*) FROM u WHERE u.a = t.a AND u.a IN (SELECT v.a FROM v)) AS n
FROM t
----
-- An outer whose body rows are all rejected by the IN still produces a row.
SELECT t.a, (SELECT count(*) FROM u WHERE u.a = t.a AND u.a IN (SELECT v.a FROM v WHERE v.a > 100)) AS n
FROM t
----
-- An IN predicate in the subquery's SELECT list rather than its WHERE.
SELECT t.a, (SELECT max(CAST(u.a IN (SELECT v.a FROM v) AS INTEGER)) FROM u WHERE u.a = t.a) AS m
FROM t
----
-- The same for EXISTS, where no body row belongs to the outer.
SELECT t.a, (SELECT max(CAST(EXISTS (SELECT 1 FROM v WHERE v.a = u.a) AS INTEGER)) FROM u WHERE u.a = t.a + 1000) AS m
FROM t
----
-- NOT IN over a list holding NULL is never true, so no body row survives.
SELECT t.a, (SELECT count(*) FROM u WHERE u.a = t.a AND u.a NOT IN (SELECT w.a FROM (VALUES (1), (CAST(NULL AS BIGINT))) AS w(a))) AS n
FROM t
----
-- EXISTS in the body of a correlated subquery.
SELECT t.a, (SELECT count(*) FROM u WHERE u.a = t.a AND EXISTS (SELECT 1 FROM v WHERE v.a = u.a)) AS n
FROM t
----
-- A body predicate beyond the correlation equality and the IN.
SELECT t.a, (SELECT count(*) FROM u WHERE u.a = t.a AND u.a > 1 AND u.a IN (SELECT v.a FROM v)) AS n
FROM t
----
-- A subquery returning a value rather than an aggregate: an outer whose body
-- rows are all rejected by the IN reads NULL.
SELECT t.a, (SELECT u.a FROM u WHERE u.a = t.a AND u.a IN (SELECT v.a FROM v WHERE v.a > 100)) AS b
FROM t
----
-- More than one body row survives the IN, which a single-value subquery
-- rejects at runtime.
-- error: Scalar sub-query has returned multiple rows
SELECT t.a, (SELECT u.a FROM u WHERE u.a > t.a AND u.a IN (SELECT v.a FROM v)) AS b
FROM t
----
-- EXISTS whose body joins a correlated relation to another under an ON
-- predicate: existence is a property of the pair, not of either side.
-- error_v1: Nested correlation across subquery boundaries is not supported yet
SELECT t.a, EXISTS (SELECT 1 FROM (SELECT u.a FROM u WHERE u.a = t.a) q JOIN v ON q.a = v.a) AS e
FROM t
----
-- The same where several pairs match.
-- error_v1: Nested correlation across subquery boundaries is not supported yet
SELECT t.a, EXISTS (SELECT 1 FROM (SELECT u.a FROM u WHERE u.a <= t.a) q JOIN v ON q.a <= v.a) AS e
FROM t
----
-- A predicate above the join inside the subquery narrows which pairs count.
-- error_v1: Nested correlation across subquery boundaries is not supported yet
SELECT t.a, EXISTS (SELECT 1 FROM (SELECT u.a FROM u WHERE u.a <= t.a) q JOIN v ON q.a <= v.a WHERE v.a > 4) AS e
FROM t
----
-- A NULL join key never matches, so the outer holding it reads false.
-- error_v1: Nested correlation across subquery boundaries is not supported yet
SELECT t.a,
  EXISTS (SELECT 1 FROM (SELECT u.a FROM u WHERE u.a = t.a) q
          JOIN (VALUES (2), (CAST(NULL AS BIGINT))) AS n(a) ON q.a = n.a) AS e
FROM t
----
-- No row of the join's correlated side belongs to the outer at all.
-- error_v1: Nested correlation across subquery boundaries is not supported yet
SELECT t.a, EXISTS (SELECT 1 FROM (SELECT u.a FROM u WHERE u.a = t.a + 100) q JOIN v ON q.a = v.a) AS e
FROM t
----
-- NOT EXISTS over the same body.
-- error_v1: Nested correlation across subquery boundaries is not supported yet
SELECT t.a, NOT EXISTS (SELECT 1 FROM (SELECT u.a FROM u WHERE u.a = t.a) q JOIN v ON q.a = v.a) AS e
FROM t
----
-- A correlated EXISTS whose body also reads a scalar subquery: true exactly
-- for the outer rows whose `a` equals `min(v.a)`.
SELECT t.a, EXISTS (SELECT 1 FROM u WHERE u.a = t.a AND u.a = (SELECT min(a) FROM v)) AS e
FROM t
----
-- The same with a comparison instead: true exactly for the outer rows whose
-- `a` exceeds `min(v.a)`, and no `u` row qualifies for the rest.
SELECT t.a, EXISTS (SELECT 1 FROM u WHERE u.a = t.a AND u.a > (SELECT min(a) FROM v)) AS e
FROM t
----
-- The negation: true exactly for the outer rows whose `a` differs from
-- `min(v.a)`, and never NULL.
SELECT t.a, NOT EXISTS (SELECT 1 FROM u WHERE u.a = t.a AND u.a = (SELECT min(a) FROM v)) AS e
FROM t
----
-- An IN whose subquery reads an outer column.
-- error_v1: Join filter references column from unplaced non-single-row table
SELECT t.a, (SELECT count(*) FROM u WHERE u.a = t.a AND u.a IN (SELECT v.a + t.a FROM v)) AS n
FROM t
----
-- An IN subquery in a LEFT JOIN's ON clause reading the null-supplying side:
-- a row whose only match the IN rejects is kept, NULL-padded.
WITH n(a) AS (VALUES (1), (2), (3)),
     m(a) AS (VALUES (1), (2), (3)),
     w(a) AS (VALUES (2), (4))
SELECT n.a, m.a FROM n LEFT JOIN m ON m.a = n.a AND m.a IN (SELECT a FROM w)
----
-- The same for a correlated EXISTS.
WITH n(a) AS (VALUES (1), (2), (3)),
     m(a) AS (VALUES (1), (2), (3)),
     w(a) AS (VALUES (2), (4))
SELECT n.a, m.a
FROM n LEFT JOIN m ON m.a = n.a AND EXISTS (SELECT 1 FROM w WHERE w.a = m.a)
----
-- A RIGHT JOIN whose ON clause reads its preserved side: a preserved row the
-- IN rejects matches nothing. DuckDB rejects a subquery in a non-inner
-- join's ON clause, so the result is spelled out.
-- error_v1: Failed to place a table
-- duckdb: VALUES (1, NULL), (2, 2), (3, NULL)
WITH n(a) AS (VALUES (1), (2), (3)),
     m(a) AS (VALUES (1), (2), (3)),
     w(a) AS (VALUES (2), (4))
SELECT n.a, m.a FROM m RIGHT JOIN n ON m.a = n.a AND n.a IN (SELECT a FROM w)
----
-- A FULL JOIN keeps the unmatched rows of both sides.
-- error_v1: Unexpected expression: Subquery
-- duckdb: VALUES (1, NULL), (2, 2), (3, NULL), (NULL, 1), (NULL, 3)
WITH n(a) AS (VALUES (1), (2), (3)),
     m(a) AS (VALUES (1), (2), (3)),
     w(a) AS (VALUES (2), (4))
SELECT n.a, m.a FROM n FULL JOIN m ON m.a = n.a AND m.a IN (SELECT a FROM w)
----
-- A NULL in the IN list makes the mark NULL, which is not a match, so only
-- the value the list names can match.
-- error_v1: Unexpected expression: Subquery
-- duckdb: VALUES (1, NULL), (2, 2), (3, NULL), (NULL, 1), (NULL, 3)
WITH n(a) AS (VALUES (1), (2), (3)), m(a) AS (VALUES (1), (2), (3))
SELECT n.a, m.a
FROM n FULL JOIN m
  ON m.a = n.a AND m.a IN (SELECT w.a FROM (VALUES (2), (CAST(NULL AS INTEGER))) AS w(a))
----
-- Correlated IN subquery with single correlation equality.
SELECT t.a IN (SELECT t2.a FROM t t2 WHERE t2.b = t.b) FROM t
----
-- Correlated NOT IN subquery with single correlation equality.
SELECT t.a NOT IN (SELECT t2.a FROM t t2 WHERE t2.b = t.b) FROM t
----
-- Correlated IN subquery with multiple correlation equalities.
SELECT t.a IN (SELECT t2.a FROM t t2 WHERE t2.b = t.b AND t2.c = t.c) FROM t
----
-- Correlated NOT IN subquery with multiple correlation equalities.
SELECT t.a NOT IN (SELECT t2.a FROM t t2 WHERE t2.b = t.b AND t2.c = t.c) FROM t
----
-- Correlated IN subquery with mixed equality and non-equality correlation.
SELECT t.a IN (SELECT t2.a FROM t t2 WHERE t2.b = t.b AND t2.c < t.c) FROM t
----
-- '= ANY' means IN.
SELECT t.a = ANY (SELECT t2.a FROM t t2 WHERE t2.b = t.b) FROM t
----
-- '= SOME' means IN.
SELECT t.a = SOME (SELECT t2.a FROM t t2 WHERE t2.b = t.b) FROM t
----
-- '<> ALL' means NOT IN.
SELECT t.a <> ALL (SELECT t2.a FROM t t2 WHERE t2.b = t.b) FROM t
----
-- Correlated NOT IN subquery with mixed equality and non-equality correlation.
SELECT t.a NOT IN (SELECT t2.a FROM t t2 WHERE t2.b = t.b AND t2.c < t.c) FROM t
----
-- Correlated IN subquery whose correlation repeats the IN equality.
SELECT t.a IN (SELECT t2.a FROM t t2 WHERE t2.a = t.a) FROM t
----
-- Correlated NOT IN subquery whose correlation repeats the IN equality.
SELECT t.a NOT IN (SELECT t2.a FROM t t2 WHERE t2.a = t.a) FROM t
----
-- The same over a source holding NULLs, which is what tells the answer apart
-- from an unknown one: a row whose subquery is empty is not in it.
WITH n(a) AS (VALUES (1), (2), (CAST(NULL AS INTEGER))),
     m(x) AS (VALUES (2), (CAST(NULL AS INTEGER)))
SELECT n.a,
       n.a IN (SELECT m.x FROM m WHERE m.x = n.a) AS present,
       n.a NOT IN (SELECT m.x FROM m WHERE m.x = n.a) AS absent
FROM n
----
-- Correlated IN subquery with reversed operand order in correlation.
SELECT t.a IN (SELECT t2.a FROM t t2 WHERE t.b = t2.b) FROM t
----
-- Correlated NOT IN subquery with reversed operand order in correlation.
SELECT t.a NOT IN (SELECT t2.a FROM t t2 WHERE t.b = t2.b) FROM t
----
-- NOT IN excludes rows where the left key is NULL.
SELECT a FROM (VALUES (1), (CAST(NULL AS INTEGER)), (5)) AS l(a)
WHERE a NOT IN (SELECT b FROM (VALUES (1), (3)) AS r(b))
----
-- NOT IN where the subquery contains a NULL: every comparison is unknown, so
-- no outer row qualifies (empty result). Exercises null-aware anti with a NULL
-- build key.
-- count 0
SELECT a FROM (VALUES (1), (2), (5)) AS l(a)
WHERE a NOT IN (SELECT b FROM (VALUES (2), (CAST(NULL AS INTEGER))) AS r(b))
----
-- Projected IN returns a three-valued flag: true on a hit, false on a clean
-- miss, and NULL when the probe is NULL or the subquery holds a NULL and there
-- is no hit.
SELECT a, a IN (SELECT b FROM (VALUES (2), (CAST(NULL AS INTEGER))) AS r(b)) AS flag
FROM (VALUES (1), (2), (CAST(NULL AS INTEGER))) AS l(a)
----
-- Correlated NOT EXISTS keeps every outer row with no matching subquery row,
-- including the NULL outer key (whose correlation never matches).
SELECT a FROM (VALUES (1), (3), (CAST(NULL AS INTEGER))) AS l(a)
WHERE NOT EXISTS (SELECT 1 FROM v WHERE v.a = l.a)
----
-- NOT IN with a small outer and larger subquery (no NULL in the subquery):
-- a non-matching outer is kept, a matching one excluded, and the NULL outer
-- key yields NULL and is excluded. Outer smaller than the subquery so the
-- antijoin may build on the outer side.
SELECT a FROM (VALUES (7), (3), (CAST(NULL AS INTEGER))) AS l(a)
WHERE a NOT IN (SELECT b FROM (VALUES (1), (2), (3), (4), (5)) AS r(b))
----
-- Uncorrelated `WHERE NOT EXISTS` over an empty subquery returns every
-- outer row.
SELECT a FROM t WHERE NOT EXISTS (SELECT 1 FROM v WHERE false)
----
-- Uncorrelated `WHERE EXISTS` over a non-empty subquery returns every
-- outer row.
SELECT a FROM t WHERE EXISTS (SELECT 1 FROM v)
