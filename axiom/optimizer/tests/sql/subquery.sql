-- setup_file: common_setup.sql
-- setup
CREATE TABLE u AS FROM (VALUES (1), (2), (3), (4), (5)) AS _(a)
----
CREATE TABLE v AS FROM (VALUES (2), (4), (6), (8), (10)) AS _(a)
-- end_setup

-- Subquery tests.

SELECT EXISTS(SELECT 1), EXISTS(SELECT 1), EXISTS(SELECT 3), NOT EXISTS(SELECT 1), NOT EXISTS(SELECT 1 WHERE false)
----
SELECT (EXISTS(SELECT 1)) = (EXISTS(SELECT 3)) WHERE NOT EXISTS(SELECT 1 WHERE false)
----
-- EXISTS with LIMIT 0 should return false.
SELECT EXISTS(SELECT 1 LIMIT 0), NOT EXISTS(SELECT 1 LIMIT 0)
----
-- IN list with a scalar subquery and a literal.
SELECT a, b FROM t WHERE a IN ((SELECT max(a) FROM t), 1)
----
-- IN list with two scalar subqueries.
SELECT a, b FROM t WHERE a IN ((SELECT max(a) FROM t), (SELECT min(a) FROM t))
----
-- Same scalar subquery in both SELECT and GROUP BY must resolve as a single
-- grouping key.
SELECT COALESCE(t.a, (SELECT max(a) FROM u))
FROM t
GROUP BY COALESCE(t.a, (SELECT max(a) FROM u))
----
-- A correlated EXISTS whose body is itself an existence test keeps the outers
-- that have a matching row.
SELECT u.a FROM u
WHERE EXISTS (SELECT 1 FROM v v2
              WHERE v2.a = u.a * 2 AND v2.a IN (SELECT v3.a FROM v v3 WHERE v3.a < 8))
----
-- The same shape written with a nested EXISTS instead of IN.
SELECT u.a FROM u
WHERE EXISTS (SELECT 1 FROM v v2
              WHERE v2.a = u.a * 2
                AND EXISTS (SELECT 1 FROM v v3 WHERE v3.a = v2.a AND v3.a < 8))
----
-- A nested EXISTS covers both an empty correlated left side and a left row
-- whose nested test finds no match.
-- error_v1: Failed to place a table
SELECT outer_u.a FROM (VALUES (0), (1), (2)) outer_u(a)
WHERE EXISTS (SELECT 1 FROM u left_u
              WHERE left_u.a = outer_u.a
                AND NOT EXISTS (SELECT 1 FROM v inner_v WHERE inner_v.a = outer_u.a))
----
-- EXISTS over a LEFT JOIN preserves rows whose right side is absent.
-- duckdb: VALUES (1), (3), (5)
SELECT outer_u.a FROM u outer_u
WHERE EXISTS (
  SELECT 1
  FROM u left_u LEFT JOIN v right_v ON right_v.a = left_u.a
  WHERE left_u.a = outer_u.a AND right_v.a IS NULL
)
----
-- EXISTS over a LEFT JOIN depends only on matching the left side.
SELECT outer_u.a FROM (VALUES (0), (1), (2)) outer_u(a)
WHERE EXISTS (
  SELECT 1
  FROM u left_u LEFT JOIN v right_v ON right_v.a = left_u.a
  WHERE left_u.a = outer_u.a
)
----
-- IN over a correlated inner join returns false for an empty left input, NULL
-- for only an unknown comparison, and true for a match.
-- error_v1: Nested correlation across subquery boundaries is not supported yet
SELECT outer_u.a, 2 * outer_u.a IN (
  SELECT right_u.a
  FROM (
    SELECT a + outer_u.a AS a
    FROM u
    WHERE a = outer_u.a
  ) left_u
  JOIN (VALUES (NULL), (0), (4)) right_u(a)
    ON right_u.a IS NULL OR left_u.a = right_u.a
)
FROM (VALUES (0), (1), (2)) outer_u(a)
----
-- A NULL on the left of IN reads NULL when the body has a row, and false when
-- the body is empty.
SELECT outer_u.a, outer_u.a IN (
  SELECT right_u.a
  FROM (SELECT t.a + outer_u.b AS a FROM (VALUES (1), (2)) t(a)) left_u
  JOIN (VALUES (1), (3)) right_u(a) ON left_u.a = right_u.a
)
FROM (
  VALUES
    (CAST(NULL AS INTEGER), 0),
    (CAST(NULL AS INTEGER), 100),
    (1, 0)
) outer_u(a, b)
----
-- A scalar subquery over more than one row fails, whether or not a projection
-- sits over the rows.
-- error_v1: Expected single row of input. Received 2 rows.
-- error_v2: Scalar subquery produced more than one row
SELECT (SELECT x + 1 FROM (VALUES (1), (2)) t(x))
----
-- Two scopes read one uncorrelated scalar subquery and are then joined. Each
-- scope evaluates it, and the columns the join sees are named apart.
-- error_v1: Duplicate column name found on join's left and right sides
WITH a AS (SELECT (SELECT sum(x) FROM (VALUES (1), (2)) s(x)) AS n),
     b AS (SELECT (SELECT sum(x) FROM (VALUES (1), (2)) s(x)) AS m)
SELECT n, m FROM a, b
----
-- A scalar subquery body and the query around it read one uncorrelated
-- scalar. The body reads its own copy, so the FULL JOIN it sits under stays
-- uncorrelated.
-- disabled_v1: crashes in join enumeration
-- duckdb: VALUES (1, 3), (2, 3)
WITH m AS (SELECT max(x) AS v FROM (VALUES (1), (2), (3)) q(x))
SELECT
  t.a,
  (SELECT count(*)
   FROM (VALUES (1), (2)) l(x)
   FULL JOIN (VALUES (1), (3)) r(y) ON l.x = r.y AND r.y < (SELECT v FROM m))
FROM (VALUES (1), (2)) t(a)
WHERE t.a < (SELECT v FROM m)
----
-- Scalar subquery and EXISTS over the same inner subquery must produce
-- distinct columns (a scalar value vs a boolean).
SELECT (SELECT max(a) FROM u), EXISTS (SELECT max(a) FROM u) FROM t
----
-- `k` inside the subquery binds to the subquery's own column, not to the
-- same-named alias in the outer SELECT.
SELECT a AS k, (SELECT max(k) FROM (VALUES (10), (20)) AS _(k))
FROM (VALUES (1), (2)) AS _(a)
----
-- Case-insensitive CTE alias resolution.
WITH a AS (SELECT * FROM (VALUES (1)) t(a)) SELECT A.a FROM A
----
-- Case-insensitive CTE alias with wildcard expansion.
WITH a AS (SELECT * FROM (VALUES (1)) t(a)) SELECT A.* FROM A
----
-- Quoted CTE alias with wildcard expansion (Presto ignores quotes for case).
-- duckdb: WITH "UpperCase" AS (SELECT * FROM (VALUES (1, 2)) t(x, y)) SELECT "UpperCase".* FROM "UpperCase"
WITH "UpperCase" AS (SELECT * FROM (VALUES (1, 2)) t(x, y)) SELECT "uPPERcASE".* FROM "uppercase"
----
-- Case-insensitive alias with wildcard in JOIN (via processAliasedRelation).
SELECT T.* FROM (VALUES (1)) t(a) JOIN (VALUES (2)) u(b) ON true
----
-- Correlated IN subquery in SELECT with non-equality filter. Produces a
-- null-aware semi-project join with extra filter; the optimizer must not flip
-- this to a right semi-project join that is unsupported in Velox.
SELECT CASE WHEN a.x IN (SELECT t.a FROM t WHERE t.b < a.y) THEN 'p' ELSE 'f' END FROM ( VALUES ( 1, 100 ) ) a ( x, y )
----
-- Correlated scalar subquery referencing a CTE that contains a NOT IN
-- subquery.
WITH u AS (
  SELECT a FROM t WHERE a NOT IN (SELECT 5)
)
SELECT (SELECT count(*) FROM u WHERE a > v.a) FROM (SELECT 1 AS a) v
----
-- A CTE that contains a correlated scalar subquery, referenced multiple
-- times from outer scalar subqueries. Each outer reference reparses the
-- CTE body with freshly uniquified column names; the inner correlated
-- reference must resolve to each expansion's own outer column, not stay
-- bound to the first expansion's name.
WITH u AS (
  SELECT (SELECT count(*) FROM (VALUES (1)) t(a) WHERE a > u.k) AS c
  FROM (VALUES (1)) u(k)
)
SELECT
  (SELECT count(*) FROM u WHERE c > 0),
  (SELECT count(*) FROM u WHERE c = 0)
----
-- 3 levels with cross-level references and name shadowing.
-- Level 0 (v): x=20, y=30. Level 1 (u): x=10 (shadows v.x), a=5.
-- Level 2 references u.a (level 1), v.y (level 0), u.x (level 1 shadow).
SELECT
  (SELECT
    (SELECT count(*)
     FROM (VALUES (5), (15), (25)) t(b)
     WHERE b > u.a AND b > u.x AND b < v.y)
   FROM (SELECT 5 AS a, 10 AS x) u)
FROM (SELECT 20 AS x, 30 AS y) v
----
-- A correlated scalar subquery projecting a constant reads NULL, not the
-- constant, for an outer row the subquery has no row for.
SELECT a, (SELECT 1 FROM v WHERE v.a = t.a) AS one FROM t
----
-- A correlated count(*) reads 0, not NULL, for an outer row the subquery
-- has no row for, so a HAVING on that count still sees 0.
-- error_v1: (0 vs. 1)
SELECT a, (SELECT count(*) FROM u WHERE u.a > t.a HAVING count(*) = 0) AS c FROM t
----
-- Multiple correlated scalar count(*) subqueries with non-equi predicates
-- in the same SELECT list, each correlating on a different outer column.
SELECT
    (SELECT count(*) FROM u WHERE u.a > t.a) AS x,
    (SELECT count(*) FROM v WHERE v.a > t.b) AS y
FROM t
----
SELECT
    (SELECT count(*) FROM u WHERE u.a > t.a) AS x,
    (SELECT count(*) FROM v WHERE v.a > t.b) AS y,
    (SELECT count(*) FROM u WHERE u.a < t.c) AS z
FROM t
----
-- Multiple subqueries in a SELECT list — pairwise shape coverage. Shapes:
-- U   = uncorrelated scalar
-- CE  = correlated equality, returns a value (no aggregation)
-- CEA = correlated equality, with aggregation
-- CN  = correlated non-equality, returns a value (no aggregation)
-- CNA = correlated non-equality, with aggregation
--
-- Blocks below cover combinations of these shapes:
--   Block A: pairs of scalar subqueries (and one triple).
--   Block B: scalar + boolean predicate (EXISTS / IN).
--   Block C: pairs of boolean predicates.
--   Block D: structural / layout variants for the heavy CNA case.
--
-- Block A.1: U + U
SELECT
    (SELECT max(u.a) FROM u) AS x,
    (SELECT min(v.a) FROM v) AS y
FROM t
----
-- Block A.2: U + CEA
SELECT
    (SELECT max(u.a) FROM u) AS x,
    (SELECT count(*) FROM u WHERE u.a = t.a) AS y
FROM t
----
-- Block A.3: U + CNA
SELECT
    (SELECT max(u.a) FROM u) AS x,
    (SELECT count(*) FROM v WHERE v.a > t.a) AS y
FROM t
----
-- Block A.4: CE + CE
SELECT
    (SELECT u.a FROM u WHERE u.a = t.a) AS x,
    (SELECT v.a FROM v WHERE v.a = t.b) AS y
FROM t
----
-- Block A.5: CE + CEA
SELECT
    (SELECT u.a FROM u WHERE u.a = t.a) AS x,
    (SELECT count(*) FROM v WHERE v.a = t.b) AS y
FROM t
----
-- Block A.6: CEA + CNA
SELECT
    (SELECT count(*) FROM u WHERE u.a = t.a) AS x,
    (SELECT count(*) FROM v WHERE v.a > t.b) AS y
FROM t
----
-- Block A.7: CN + CN
SELECT
    (SELECT u.a FROM u WHERE u.a > t.a AND u.a <= t.a + 1) AS x,
    (SELECT v.a FROM v WHERE v.a > t.b AND v.a <= t.b + 2) AS y
FROM t
----
-- Block A.8: CN + CNA
SELECT
    (SELECT u.a FROM u WHERE u.a > t.a AND u.a <= t.a + 1) AS x,
    (SELECT count(*) FROM v WHERE v.a > t.b) AS y
FROM t
----
-- Block A.9: CNA + CNA on the same outer column
SELECT
    (SELECT count(*) FROM u WHERE u.a > t.a) AS x,
    (SELECT count(*) FROM v WHERE v.a > t.a) AS y
FROM t
----
-- Block A.10: CNA + CNA on a compound outer reference
SELECT
    (SELECT count(*) FROM u WHERE u.a > t.a + t.b) AS x,
    (SELECT count(*) FROM v WHERE v.a > t.b - t.a) AS y
FROM t
----
-- Block B: scalar + boolean predicate (EXISTS / IN) in the same SELECT list.
--
-- Block B.1: CNA scalar + correlated EXISTS
SELECT
    (SELECT count(*) FROM u WHERE u.a > t.a) AS x,
    EXISTS (SELECT 1 FROM v WHERE v.a > t.b) AS y
FROM t
----
-- Block B.2: CNA scalar + correlated NOT EXISTS
SELECT
    (SELECT count(*) FROM u WHERE u.a > t.a) AS x,
    NOT EXISTS (SELECT 1 FROM v WHERE v.a > t.b) AS y
FROM t
----
-- Block B.3: CNA scalar + correlated IN
SELECT
    (SELECT count(*) FROM u WHERE u.a > t.a) AS x,
    t.a IN (SELECT v.a FROM v WHERE v.a > t.b) AS y
FROM t
----
-- Block B.4: CNA scalar + correlated NOT IN
SELECT
    (SELECT count(*) FROM u WHERE u.a > t.a) AS x,
    t.a NOT IN (SELECT v.a FROM v WHERE v.a > t.b) AS y
FROM t
----
-- Block B.5: CEA scalar + correlated EXISTS
SELECT
    (SELECT count(*) FROM u WHERE u.a = t.a) AS x,
    EXISTS (SELECT 1 FROM v WHERE v.a = t.b) AS y
FROM t
----
-- Block B.6: CN scalar + correlated EXISTS
SELECT
    (SELECT u.a FROM u WHERE u.a > t.a AND u.a <= t.a + 1) AS x,
    EXISTS (SELECT 1 FROM v WHERE v.a > t.b) AS y
FROM t
----
-- Block C: multiple boolean predicates in the SELECT list.
--
-- Block C.1: two correlated EXISTS
SELECT
    EXISTS (SELECT 1 FROM u WHERE u.a > t.a) AS x,
    EXISTS (SELECT 1 FROM v WHERE v.a > t.b) AS y
FROM t
----
-- Block C.2: two correlated IN
SELECT
    t.a IN (SELECT u.a FROM u WHERE u.a > 0) AS x,
    t.b IN (SELECT v.a FROM v WHERE v.a > 0) AS y
FROM t
----
-- Block C.3: EXISTS + IN mixed
SELECT
    EXISTS (SELECT 1 FROM u WHERE u.a > t.a) AS x,
    t.b IN (SELECT v.a FROM v WHERE v.a > 0) AS y
FROM t
----
-- Block C.4: two NOT EXISTS
SELECT
    NOT EXISTS (SELECT 1 FROM u WHERE u.a > t.a) AS x,
    NOT EXISTS (SELECT 1 FROM v WHERE v.a > t.b) AS y
FROM t
----
-- Block D: layout / structural variants for the heavy CNA case.
--
-- Block D.1: two CNA inside a single CASE expression
SELECT
    CASE
        WHEN (SELECT count(*) FROM u WHERE u.a > t.a) <= 5
         AND (SELECT count(*) FROM v WHERE v.a > t.b) <= 5
        THEN 'pass' ELSE 'fail'
    END AS r
FROM t
----
-- Block D.2: three CNA inside a single CASE expression
SELECT
    CASE
        WHEN (SELECT count(*) FROM u WHERE u.a > t.a) <= 5
         AND (SELECT count(*) FROM v WHERE v.a > t.b) <= 5
         AND (SELECT count(*) FROM u WHERE u.a > t.b) <= 5
        THEN 'pass' ELSE 'fail'
    END AS r
FROM t
----
-- Block D.3: two CNA on a compound outer reference
SELECT
    (SELECT count(*) FROM u WHERE u.a > (t.a + t.b)) AS x,
    (SELECT count(*) FROM v WHERE v.a > (t.a + t.b)) AS y
FROM t
----
-- Block D.4: mixed layout — two CNA inside a CASE plus a CNA in a separate column
SELECT
    CASE
        WHEN (SELECT count(*) FROM u WHERE u.a > t.a) <= 5
         AND (SELECT count(*) FROM v WHERE v.a > t.b) <= 5
        THEN 'pass' ELSE 'fail'
    END AS r,
    (SELECT count(*) FROM u WHERE u.a < t.c) AS z
FROM t
----

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
----
-- Uncorrelated scalar subquery whose source needs runtime single-row
-- enforcement. Returns one row per outer row.
SELECT (SELECT u.a FROM u WHERE u.a = 1) FROM t
----
-- The same uncorrelated scalar subquery referenced twice in one expression.
SELECT (SELECT max(u.a) FROM u) + (SELECT max(u.a) FROM u) AS s FROM t
----
-- Scalar subquery whose SELECT references an outer column. The subquery
-- has no FROM clause: result is just the outer column.
SELECT (SELECT a) FROM t
----
-- As above, but the inner SELECT is an expression over the outer column.
SELECT (SELECT a + 1) FROM t
----
-- Scalar subquery with FROM, no correlated WHERE, projection mixes outer
-- and inner columns at top level.
SELECT (SELECT t.a + u.a FROM u WHERE u.a = 1) FROM t
----
-- Correlated WHERE plus correlated projection: outer column added to an
-- inner aggregate result.
SELECT (SELECT max(u.a) + t.a FROM u WHERE u.a = t.a) FROM t
----
-- Outer column inside an aggregate body.
SELECT (SELECT max(u.a + t.a) FROM u WHERE u.a = t.a) FROM t
----
-- Outer column inside an aggregate body AND wrapping the aggregate result.
SELECT (SELECT max(u.a + t.a) + t.a FROM u WHERE u.a = t.a) FROM t
----
-- WHERE, aggregate body, and post-aggregate residual each reference a
-- different outer column.
SELECT (SELECT max(u.a + t.b) + t.c FROM u WHERE u.a = t.a) FROM t
----
-- Aggregate body and post-aggregate residual reference different outer
-- columns; no correlated WHERE.
SELECT (SELECT max(u.a + t.b) + t.c FROM u) FROM t
----
-- Correlated projection but no correlated WHERE: outer column added to
-- an inner global aggregate.
SELECT (SELECT t.a + max(u.a) FROM u) FROM t
----
-- Correlated WHERE where the inner side of the equality is a constant
-- projection. Constant folding collapses one side, leaving a pure-outer
-- gating condition: per outer row, the scalar returns the aggregate
-- when the condition holds, else NULL.
SELECT (SELECT max(o.b) FROM (SELECT 1 AS a, 42 AS b) o WHERE o.a = t.a) FROM t
----
-- Same shape inside an IN subquery: gate fails ⇒ no inner row matches
-- ⇒ IN returns false.
SELECT t.a IN (SELECT o.a FROM (SELECT 1 AS a) o WHERE o.a = t.a) FROM t
----
-- Same shape inside an EXISTS subquery: gate fails ⇒ no inner row ⇒
-- EXISTS returns false.
SELECT EXISTS (SELECT 1 FROM (SELECT 1 AS a) o WHERE o.a = t.a) FROM t
----
-- Two-level nested correlated scalar subqueries: the innermost body
-- correlates on the middle scope's u, and the middle body correlates on
-- the top scope's t.
SELECT (SELECT (SELECT max(v.a) FROM v WHERE v.a > u.a) FROM u WHERE u.a = t.a) FROM t
----
-- No-FROM subquery body with a correlated WHERE. Per outer row the
-- WHERE filters whether the single empty-tuple row passes; the scalar
-- subquery returns the SELECT expression or NULL.
SELECT (SELECT t.a WHERE t.a = 1) FROM t
----
SELECT (SELECT t.b + 100 WHERE t.a > 1) FROM t
----
-- Pure-outer aggregate: max(t.a) binds to the outer scope. Returns
-- one row with max(t.a) over all t.
SELECT (SELECT max(t.a)) FROM t
----
-- No-FROM subquery body with a cardinality-neutral aggregate. count(*)
-- over the single empty-tuple row produces 1; per outer row the result
-- is t.a + 1.
SELECT (SELECT count(*) + t.a) FROM t
----
-- No-FROM subquery body with LIMIT 0 — the single row is cut to zero,
-- so the scalar subquery returns NULL per outer row.
SELECT (SELECT t.a LIMIT 0) FROM t
----
-- Correlated WHERE plus correlated projection over a count-style
-- aggregate. count(*) over empty input is 0 (not NULL), so per-outer-row
-- result is t.a when no matching u row exists.
SELECT (SELECT count(*) + t.a FROM u WHERE u.a = t.a) FROM t
----
-- Same shape with a correlation that no outer row matches (t.a values
-- are 1..3, u.a values are 1..5, t.a + 100 is never in u).
SELECT (SELECT count(*) + t.a FROM u WHERE u.a = t.a + 100) FROM t
----
-- Correlated count whose equi correlation key is computed on the subquery
-- side (u.a + 1 = t.a).
SELECT (SELECT count(*) FROM u WHERE u.a + 1 = t.a) FROM t
----
-- Correlated count with a two-column equi correlation key (self-correlated t).
SELECT (SELECT count(*) FROM t t2 WHERE t2.a = t.a AND t2.b = t.b) FROM t
----
-- Outer-column reference in a non-INNER join's ON condition inside a
-- correlated subquery.
-- error_v1: Cannot resolve column name: a
-- duckdb: SELECT 5::bigint FROM t
SELECT (SELECT max(u.a) FROM u LEFT JOIN v ON v.a = t.a) FROM t
----
-- Correlated subquery whose body is a UNION ALL of two branches that each
-- reference an outer column.
-- error_v1: Correlated reference inside a UNION ALL branch is not supported yet
-- error_v2: Correlated reference inside a UnionAll branch is not supported yet
SELECT (SELECT max(a) FROM (SELECT u.a FROM u WHERE u.a = t.a UNION ALL SELECT v.a FROM v WHERE v.a = t.a)) FROM t
----
-- Outer-column reference in the SELECT of an IN subquery: the
-- comparison value combines an inner column with an outer column.
SELECT t.a IN (SELECT u.a + t.b FROM u WHERE u.a > 0) FROM t
----
-- Outer-column reference inside an aggregate body of an IN subquery
-- (HAVING max(u.a + t.b) > 0).
-- error_v1: Outer-column reference in the aggregate body of an IN subquery is not supported yet
SELECT t.a IN (SELECT u.a FROM u GROUP BY u.a HAVING max(u.a + t.b) > 0) FROM t
----
-- EXISTS ignores the subquery's SELECT projection, so an outer-column
-- reference there is harmless: row existence is decided by the
-- correlated WHERE alone.
SELECT EXISTS (SELECT u.a + t.b FROM u WHERE u.a = t.a) FROM t
----
-- A global aggregate over the (possibly empty) inner relation always
-- produces exactly one row, so EXISTS over an aggregating body is true
-- for every outer row regardless of correlation.
SELECT EXISTS (SELECT max(u.a + t.b) FROM u WHERE u.a = t.a) FROM t
----
-- Multi-arg aggregate with one arg referencing inner and another
-- referencing outer.
SELECT (SELECT min_by(u.a, t.b) FROM u WHERE u.a > 0) FROM t
----
-- The following pure-outer-aggregate queries use `-- duckdb:`
-- overrides because DuckDB's subquery form does not implement the
-- outer-scope lift consistently with its own explicit-aggregation
-- form. See https://github.com/duckdb/duckdb/issues/23063.
--
-- Pure-outer aggregate with an empty body. count over zero rows = 0.
-- duckdb: SELECT count(t.a) FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.a > 999)
SELECT (SELECT count(t.a) FROM u WHERE u.a > 999) FROM t
----
-- Pure-outer aggregate with a correlated body filter. Every outer
-- row qualifies, so max returns 3.
-- duckdb: SELECT max(t.a) FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.a = t.a)
SELECT (SELECT max(t.a) FROM u WHERE u.a = t.a) FROM t
----
-- Single-row outer: per-row evaluation gives the same answer.
SELECT (SELECT count(t.a) FROM u WHERE u.a > 0) FROM (VALUES (1)) AS t(a)
----
-- Pure-outer aggregate inside a HAVING predicate.
-- duckdb: SELECT count(*) FROM t HAVING EXISTS(SELECT 1) AND (SELECT max(a) FROM t) > 0
SELECT count(*) FROM t HAVING (SELECT max(t.a)) > 0
----
-- Pure-outer aggregate wrapped in arithmetic.
SELECT (SELECT max(t.a) + 1 FROM u WHERE u.a > 0) FROM t
----
-- Multiple pure-outer aggregates in one subquery expression, sharing
-- the body's FROM/WHERE as the EXISTS gate.
SELECT (SELECT max(t.a) - min(t.a) FROM u WHERE u.a > 0) FROM t
----
-- Pure-outer aggregate inside an ORDER BY key is not yet supported. The
-- aggregate makes the block a global aggregation, leaving 't.a' non-grouped.
-- error: Cannot resolve column: a
SELECT t.a FROM t ORDER BY (SELECT max(t.a))
----
-- Multiple aggregates, each referencing an outer column.
SELECT (SELECT max(u.a + t.b) + min(u.a + t.c) FROM u WHERE u.a > 0) FROM t
----
-- Multiple aggregates where one references outer and another does not
-- (constant arg, no args, inner-only arg).
SELECT (SELECT max(u.a + t.b) + count(1) FROM u WHERE u.a > 0) FROM t
----
SELECT (SELECT max(u.a + t.b) + count(*) FROM u WHERE u.a > 0) FROM t
----
SELECT (SELECT max(u.a + t.b) + sum(u.a) FROM u WHERE u.a > 0) FROM t
----
-- Scalar subquery whose SELECT references the same inner column more
-- than once alongside an outer column.
SELECT (SELECT u.a + t.a + u.a + 1 FROM u WHERE u.a = t.a) FROM t
----
-- IN subquery in a JOIN ON clause whose correlation references a sibling
-- of the IN's outer table.
SELECT *
FROM (VALUES ('a')) AS t(a)
INNER JOIN (VALUES ('a')) AS u(k)
  ON t.a IN (
      SELECT v.b
      FROM (VALUES ('a', 'a')) AS v(k, b)
      WHERE v.k = u.k
  )
----
-- IN subquery whose SELECT references a sibling outer table ('u.b') not
-- touched by the IN's left key ('t.a') or the correlation conjunct
-- ('v.k = t.a'). For (t.a=1, u.b=1) the inner row passes 'v.k = 1' and
-- yields 1, so 't.a = 1' matches.
SELECT *
FROM (VALUES (1)) AS t(a), (VALUES (1)) AS u(b)
WHERE t.a IN (
    SELECT u.b
    FROM (VALUES (1)) AS v(k)
    WHERE v.k = t.a
)
----
-- EXISTS subquery in a JOIN ON clause whose non-equi correlation
-- references a sibling of the EXISTS's outer table.
SELECT *
FROM (VALUES (1)) AS t(a)
INNER JOIN (VALUES (1)) AS u(k)
  ON EXISTS (
      SELECT 1
      FROM (VALUES (1, 1)) AS v(b, k)
      WHERE v.k = t.a AND v.b >= u.k
  )
----
-- Scalar subquery in a JOIN ON clause whose non-equi correlation
-- references a sibling of the subquery's outer table. For (t.a=1, u.k=1)
-- the inner aggregate over 'v.k=1 AND v.b>1' is empty, so 'max(v.b)' is
-- NULL and 't.a = NULL' is unknown — no rows match.
-- count 0
SELECT *
FROM (VALUES (1)) AS t(a)
INNER JOIN (VALUES (1)) AS u(k)
  ON t.a = (
      SELECT max(v.b)
      FROM (VALUES (1, 1)) AS v(b, k)
      WHERE v.k = t.a AND v.b > u.k
  )
----
-- Same shape as above, but the inner aggregate matches: for (t.a=2, u.k=1)
-- 'v.b > 1 AND v.k = 2' selects (2, 2), so 'max(v.b)' is 2 and 't.a = 2'
-- holds. Sibling outer column 'u.k' must appear in the output row.
SELECT *
FROM (VALUES (2)) AS t(a)
INNER JOIN (VALUES (1)) AS u(k)
  ON t.a = (
      SELECT max(v.b)
      FROM (VALUES (1, 2), (2, 2)) AS v(b, k)
      WHERE v.k = t.a AND v.b > u.k
  )
----
-- Two sibling cross-joined subqueries, each containing the same scalar
-- uncorrelated subquery in WHERE.
SELECT *
FROM (SELECT a FROM t WHERE a = (SELECT max(a) FROM t)) AS u,
     (SELECT a FROM t WHERE a = (SELECT max(a) FROM t)) AS v
----
-- Correlated IN-subquery in a JOIN's ON clause whose left key is a
-- payload column from a prior LEFT JOIN's right side.
SELECT u.b
FROM (VALUES (1)) AS t(a)
LEFT JOIN (VALUES (1, 'x')) AS u(k, b) ON t.a = u.k
INNER JOIN (VALUES (1)) AS v(c)
  ON u.b IN (SELECT 'x' FROM (VALUES (1)) AS w(d) WHERE d = v.c)
----
-- Shared CTE with a nested-IN filter, referenced from both UNION legs,
-- second leg wrapping it in GROUP BY.
WITH s AS (
    SELECT x FROM (VALUES (1)) t(x) WHERE x IN (SELECT 1 WHERE 1 IN (SELECT 1))
)
SELECT x FROM s
UNION ALL
SELECT x FROM (SELECT x, sum(x) AS sx FROM s GROUP BY x) WHERE sx > 0
----
-- Same shape with a single reference inside a GROUP BY.
WITH s AS (
    SELECT x FROM (VALUES (1)) t(x) WHERE x IN (SELECT 1 WHERE 1 IN (SELECT 1))
)
SELECT x FROM (SELECT x, sum(x) AS sx FROM s GROUP BY x) WHERE sx > 0
----
-- IN with a constant left-hand side over a no-FROM subquery.
SELECT 1 WHERE 1 IN (SELECT 1)
----
-- The same read as a value rather than a filter, over a matching list, a
-- non-matching one, and one holding only NULL.
SELECT 1 IN (SELECT 1) AS a, 1 IN (SELECT 2) AS b,
       1 IN (SELECT CAST(NULL AS INTEGER)) AS c
----
-- A constant left-hand side alongside a relation the subquery does not name.
SELECT x FROM UNNEST(ARRAY[1, 2]) AS t(x)
WHERE 1 IN (SELECT c FROM (VALUES (1), (2)) AS s(c))
----
-- The negated form keeps no row.
-- count 0
SELECT x FROM UNNEST(ARRAY[1, 2]) AS t(x)
WHERE 1 NOT IN (SELECT c FROM (VALUES (1), (2)) AS s(c))
----
-- Scalar subquery in aggregate ORDER BY expression.
SELECT array_agg(a ORDER BY a + (SELECT 1)) AS vals FROM t
----
-- Scalar subquery in aggregate ORDER BY with GROUP BY.
SELECT a, array_agg(b ORDER BY b + (SELECT 0)) AS vals FROM t GROUP BY a
----
-- Non-order-sensitive aggregate with ORDER BY containing a subquery.
SELECT sum(a ORDER BY a + (SELECT 1)) AS total FROM t
----
-- Correlated scalar subquery whose body returns more than one row per
-- outer row (multiple t.b for each t.a) must fail at runtime.
-- error: Scalar sub-query has returned multiple rows
SELECT (SELECT t2.b FROM t t2 WHERE t2.a = t.a) FROM t
----
-- Uncorrelated scalar subquery whose body returns more than one row
-- must fail at runtime.
-- error: Expected single row of input. Received 5 rows.
SELECT (SELECT a FROM u) FROM t
----
-- A disjunct that is always true decides the WHERE on its own, so the EXISTS
-- beside it settles nothing and every row is counted.
WITH ids AS (SELECT * FROM (VALUES (1), (2)) AS _(id))
SELECT count(*) FROM ids
WHERE 1 = 1 OR EXISTS (SELECT 1 FROM u WHERE u.a = ids.id)
----
-- The same, counting distinct values instead of rows.
WITH ids AS (SELECT * FROM (VALUES (1), (2)) AS _(id))
SELECT count(DISTINCT ids.id) FROM ids
WHERE 1 = 1 OR EXISTS (SELECT 1 FROM u WHERE u.a = ids.id)
----
-- Correlated EXISTS over a scalar-aggregate body. EXISTS is true iff
-- the per-outer aggregate produces a row — for count(*) without
-- HAVING, that's iff u has any matching row.
SELECT t.a FROM t WHERE EXISTS (
  SELECT count(*) FROM u WHERE u.a = t.a
)
----
-- Correlated EXISTS over an aggregate body with HAVING. No `u.a` value
-- has more than one row, so HAVING is never satisfied and the result
-- is empty.
-- count 0
-- error_v1: EXISTS over a global aggregate with HAVING or OFFSET is not supported yet
SELECT t.a FROM t WHERE EXISTS (
  SELECT count(*) FROM u WHERE u.a = t.a HAVING count(*) > 1
)
----
-- Correlated EXISTS over an aggregate body with HAVING that is
-- satisfied for outers whose key appears in u.
-- error_v1: EXISTS over a global aggregate with HAVING or OFFSET is not supported yet
SELECT t.a FROM t WHERE EXISTS (
  SELECT count(*) FROM u WHERE u.a = t.a HAVING count(*) >= 1
)
----
-- Correlated IN over a grouping aggregate body where the IN right-
-- hand side is one of the grouping keys.
SELECT t.a FROM t WHERE t.a IN (
  SELECT u.a FROM u WHERE u.a >= t.a GROUP BY u.a
)
----
-- Correlated NOT EXISTS over an aggregate body with HAVING. Outers
-- with no surviving group are kept.
-- error_v1: EXISTS over a global aggregate with HAVING or OFFSET is not supported yet
SELECT t.a FROM t WHERE NOT EXISTS (
  SELECT count(*) FROM u WHERE u.a = t.a HAVING count(*) > 5
)
----
-- IN whose subquery body is a scalar aggregate, so the IN right-hand
-- side column is the aggregate result itself (not a grouping key).
-- error_v1: IN over a correlated global aggregate is not supported yet
SELECT t.a FROM t WHERE t.a IN (
  SELECT max(u.a) FROM u WHERE u.a <= t.a
)
----
-- Correlated scalar subquery whose body is a UNION ALL of two
-- branches, each filtering by an outer column.
-- error_v1: Correlated reference inside a UNION ALL branch is not supported yet
-- error_v2: Correlated reference inside a UnionAll branch is not supported yet
SELECT t.a, (
  SELECT count(*) FROM (
    SELECT u.a FROM u WHERE u.a > t.a
    UNION ALL
    SELECT v.a FROM v WHERE v.a > t.a
  )
) FROM t
----
-- Correlated scalar subquery whose body has a Limit between the
-- Aggregate and the correlated Filter.
SELECT t.a, (
  SELECT max(x) FROM (
    SELECT u.a AS x FROM u WHERE u.a > t.a LIMIT 10
  )
) FROM t
----
-- Uncorrelated scalar subquery inside the UNNEST array constructor.
-- error_v1: Unexpected expression: Subquery
SELECT v.x
FROM (VALUES (ARRAY[10, 20, 30])) s(arr)
CROSS JOIN UNNEST(ARRAY[(SELECT max(a) FROM u), arr[1]]) AS v(x)
----
-- Correlated scalar subquery inside the UNNEST array constructor;
-- correlates to the input row above the UNNEST. DuckDB rejects nested
-- lateral joins, so the expected result is hardcoded.
-- duckdb: VALUES (1, 1), (2, 2)
-- error_v1: Unexpected expression: Subquery
SELECT s.k, v.x
FROM (VALUES (1), (2)) s(k)
CROSS JOIN UNNEST(ARRAY[(SELECT max(a) FROM u WHERE u.a = s.k)]) AS v(x)
----
-- Correlated scalar subquery with LIMIT 1 body: the no-match outer row
-- survives with NULL.
-- error_v1: LIMIT in a correlated scalar subquery is not supported yet
SELECT u.a, (SELECT t.b FROM t WHERE t.a = u.a + 1 ORDER BY t.b LIMIT 1) FROM u
----
-- A correlated subquery ordering by a column it does not select reads the
-- first row of that order. Two outers share a key and read the same row, and
-- an outer matching nothing reads NULL.
-- error_v1: LIMIT in a correlated scalar subquery is not supported yet
WITH b(k, x, o) AS (VALUES (1, 'p', 2), (1, 'q', 1), (2, 'r', 5))
SELECT t.k, (SELECT b.x FROM b WHERE b.k = t.k ORDER BY b.o LIMIT 1)
FROM (VALUES (1), (2), (3), (1)) AS t(k)
----
-- Reversing the order selects the other row.
-- error_v1: LIMIT in a correlated scalar subquery is not supported yet
WITH b(k, x, o) AS (VALUES (1, 'p', 2), (1, 'q', 1), (2, 'r', 5))
SELECT t.k, (SELECT b.x FROM b WHERE b.k = t.k ORDER BY b.o DESC LIMIT 1)
FROM (VALUES (1), (2), (3)) AS t(k)
----
-- An outer the subquery has no row for reads NULL, even where the expression
-- above it would turn a row into a value.
-- error_v1: LIMIT in a correlated scalar subquery is not supported yet
WITH b(k, x) AS (VALUES (1, 'p'), (2, 'q'))
SELECT t.k,
       (SELECT s.x IS NULL
        FROM (SELECT b.x FROM b WHERE b.k = t.k ORDER BY b.x LIMIT 1) s)
FROM (VALUES (1), (3)) AS t(k)
----
-- A NULL correlation key matches no row, so the subquery reads NULL.
-- error_v1: LIMIT in a correlated scalar subquery is not supported yet
WITH b(k, x, o) AS (VALUES (1, 'p', 2))
SELECT t.k, (SELECT b.x FROM b WHERE b.k = t.k ORDER BY b.o LIMIT 1)
FROM (VALUES (1), (CAST(NULL AS INTEGER))) AS t(k)
----
-- A LIMIT above one row is not a scalar bound, so the body ranks to that
-- many rows per key and the aggregate above reads them all.
WITH b(k, x, o) AS (VALUES (1, 10, 3), (1, 20, 1), (1, 40, 2))
SELECT t.k,
       (SELECT sum(s.x)
        FROM (SELECT b.x FROM b WHERE b.k = t.k ORDER BY b.o LIMIT 2) s)
FROM (VALUES (1), (3)) AS t(k)
----
-- The same LIMIT under a scalar bound takes the per-outer form instead. Each
-- key has one row, so the bound holds.
-- error_v1: LIMIT in a correlated scalar subquery is not supported yet
WITH b(k, x) AS (VALUES (1, 'p'), (2, 'q'))
SELECT t.k, (SELECT b.x FROM b WHERE b.k = t.k LIMIT 2)
FROM (VALUES (1), (2), (3)) AS t(k)
----
-- Correlated EXISTS with LIMIT 1 body.
SELECT u.a FROM u WHERE EXISTS (SELECT 1 FROM t WHERE t.a > u.a LIMIT 1)
----
-- Correlated NOT EXISTS with LIMIT 1 body.
SELECT u.a FROM u WHERE NOT EXISTS (SELECT 1 FROM t WHERE t.a > u.a LIMIT 1)
----
-- A correlated scalar whose body is a Join of two correlated
-- single-row subqueries returns one row per outer.
-- error_v1: Nested correlation across subquery boundaries is not supported yet
SELECT u.a,
       (SELECT l.b + r.b
        FROM (SELECT max(b) AS b FROM t WHERE t.a = u.a) l,
             (SELECT max(b) AS b FROM t WHERE t.a = u.a + 1) r)
FROM u
----
-- Correlated scalar whose body joins a correlated single-row subquery
-- with an uncorrelated single-row subquery. Outers with no matching
-- left row must surface a single NULL row, not |right| NULL-extended
-- rows.
-- error_v1: Nested correlation across subquery boundaries is not supported yet
SELECT u.a,
       (SELECT l.b + r.b
        FROM (SELECT max(b) AS b FROM t WHERE t.a = u.a + 10) l,
             (SELECT max(b) AS b FROM t) r)
FROM u
----
-- A correlated scalar whose body cross-joins two correlated derived
-- tables. For k=1 the left side has two rows and the right side none,
-- so the cross join is empty and the scalar is NULL; k=2 has one row on
-- each side and returns their sum.
-- error_v1: Nested correlation across subquery boundaries is not supported yet
WITH u(k, x) AS (VALUES (1, 10), (1, 20), (2, 30)),
     v(k, x) AS (VALUES (2, 99))
SELECT t.k,
       (SELECT l.x + r.x
        FROM (SELECT x FROM u WHERE u.k = t.k) l,
             (SELECT x FROM v WHERE v.k = t.k) r)
FROM (VALUES (1), (2)) AS t(k)
----
-- When the left side is empty and the right side has multiple rows,
-- the cross join is empty, so the scalar is NULL for that outer.
-- error_v1: Nested correlation across subquery boundaries is not supported yet
WITH u(k, x) AS (VALUES (2, 99)),
     v(k, x) AS (VALUES (1, 10), (1, 20), (2, 30))
SELECT t.k,
       (SELECT l.x + r.x
        FROM (SELECT x FROM u WHERE u.k = t.k) l,
             (SELECT x FROM v WHERE v.k = t.k) r)
FROM (VALUES (1), (2)) AS t(k)
----
-- Correlated scalar over a cross join with a predicate spanning both
-- sides: only matching pairs contribute, and an outer with no matching
-- pair yields NULL.
-- error_v1: Nested correlation across subquery boundaries is not supported yet
WITH u(k, x) AS (VALUES (1, 10), (1, 20), (1, 30)),
     v(k, x) AS (VALUES (1, 20))
SELECT t.k,
       (SELECT l.x + r.x
        FROM (SELECT x FROM u WHERE u.k = t.k) l,
             (SELECT x FROM v WHERE v.k = t.k) r
        WHERE l.x = r.x)
FROM (VALUES (1), (2)) AS t(k)
----
-- A cross join that yields more than one row for an outer is a
-- scalar-subquery cardinality violation.
-- error_v1: Nested correlation across subquery boundaries is not supported yet
-- error_v2: Scalar sub-query has returned multiple rows
WITH u(k, x) AS (VALUES (1, 10), (1, 20)),
     v(k, x) AS (VALUES (1, 100), (1, 200))
SELECT t.k,
       (SELECT l.x + r.x
        FROM (SELECT x FROM u WHERE u.k = t.k) l,
             (SELECT x FROM v WHERE v.k = t.k) r)
FROM (VALUES (1)) AS t(k)
----
-- A scalar that selects only the left side: an empty cross join must
-- still yield NULL, not the left value carried on the empty-side row.
-- error_v1: Nested correlation across subquery boundaries is not supported yet
WITH u(k, x) AS (VALUES (1, 10), (1, 20), (2, 30)),
     v(k, x) AS (VALUES (2, 99))
SELECT t.k,
       (SELECT l.x
        FROM (SELECT x FROM u WHERE u.k = t.k) l,
             (SELECT x FROM v WHERE v.k = t.k) r)
FROM (VALUES (1), (2)) AS t(k)
----
-- A correlated scalar subquery over a join that carries the correlation in
-- its ON clause. Every outer has a matching pair.
SELECT u.a, (SELECT v2.a FROM v v2 JOIN v v3 ON v3.a = v2.a AND v2.a = u.a * 2)
FROM u
----
-- The same shape where some outers have no matching pair, which reads NULL.
SELECT u.a, (SELECT v2.a FROM v v2 JOIN v v3 ON v3.a = v2.a AND v2.a = u.a * 4)
FROM u
----
-- Both the ON predicate and a WHERE above the join decide which pairs
-- contribute.
SELECT u.a, (SELECT v2.a FROM v v2 JOIN v v3 ON v3.a = v2.a AND v2.a = u.a * 2 WHERE v3.a < 8)
FROM u
----
-- An ON predicate reading only one side still selects the pairs.
SELECT u.a, (SELECT v2.a FROM v v2 JOIN v v3 ON v3.a = v2.a AND v3.a < 8 AND v2.a = u.a * 2)
FROM u
----
-- A WHERE above a LEFT JOIN removes rows the join padded, so an outer whose
-- rows it all rejects reads NULL rather than the padded left value.
-- error_v1: Nested correlation across subquery boundaries is not supported yet
SELECT u.a, (SELECT x.a FROM (SELECT a FROM u u2 WHERE u2.a = u.a) x LEFT JOIN v v3 ON v3.a = x.a WHERE v3.a < 0)
FROM u
----
-- A correlated scalar subquery grouped inside, where the HAVING selects one
-- group for one outer and rejects every group for the other.
-- error_v1: Correlation predicate references a column not in GROUP BY is not supported yet
WITH b(k, g, x) AS (VALUES (1, 'a', 10), (1, 'a', 20), (1, 'b', 30), (2, 'c', 40))
SELECT t.k, (SELECT max(b.x) FROM b WHERE b.k = t.k GROUP BY b.g HAVING count(*) > 1)
FROM (VALUES (1), (2)) AS t(k)
----
-- The HAVING rejects every group of every outer, so each reads NULL.
-- error_v1: Correlation predicate references a column not in GROUP BY is not supported yet
WITH b(k, g, x) AS (VALUES (1, 'a', 10), (2, 'b', 20))
SELECT t.k, (SELECT max(b.x) FROM b WHERE b.k = t.k GROUP BY b.g HAVING count(*) > 1)
FROM (VALUES (1), (2)) AS t(k)
----
-- Grouping by the correlation column itself: one key, not two.
-- error_v1: Decorrelated aggregated subquery must expose one result column plus one per lifted correlation key
WITH b(k, x) AS (VALUES (1, 10), (1, 20), (2, 30))
SELECT t.k, (SELECT max(b.x) FROM b WHERE b.k = t.k GROUP BY b.k HAVING count(*) > 1)
FROM (VALUES (1), (2)) AS t(k)
----
-- An outer with no body rows has no groups, so a HAVING that would accept an
-- empty group's count must not manufacture one.
-- error_v1: Correlation predicate references a column not in GROUP BY is not supported yet
WITH b(k, g) AS (VALUES (5, 'a'))
SELECT t.k, (SELECT count(*) FROM b WHERE b.k < t.k GROUP BY b.g HAVING count(*) < 5)
FROM (VALUES (1), (9)) AS t(k)
----
-- A subquery that reads the key it correlates and groups on: the key reaches
-- the outer as the subquery's own value.
-- error_v1: Decorrelated aggregated subquery must expose one result column plus one per lifted correlation key
WITH b(k, x) AS (VALUES (1, 10), (1, 20), (2, 30))
SELECT t.k, (SELECT b.k FROM b WHERE b.k = t.k GROUP BY b.k HAVING count(*) > 1)
FROM (VALUES (1), (2)) AS t(k)
----
-- A computed key that is both the correlation and the grouping key is one
-- key, published once.
-- error_v1: Decorrelated aggregated subquery must expose one result column plus one per lifted correlation key
WITH b(k, x) AS (VALUES (1, 10), (1, 20), (2, 30))
SELECT t.k, (SELECT max(b.x) FROM b WHERE b.k + 1 = t.k GROUP BY b.k + 1 HAVING count(*) > 1)
FROM (VALUES (2), (3)) AS t(k)
----
-- A grouped subquery reads NULL for an outer it matched no rows of, whether
-- or not the correlation is an equality: no rows means no groups, not a group
-- counting zero.
-- error_v1: Correlation predicate references a column not in GROUP BY is not supported yet
WITH b(k, g) AS (VALUES (5, 'a'))
SELECT t.k, (SELECT count(*) FROM b WHERE b.k = t.k GROUP BY b.g)
FROM (VALUES (1), (5)) AS t(k)
----
-- error_v1: Correlation predicate references a column not in GROUP BY is not supported yet
WITH b(k, g) AS (VALUES (5, 'a'))
SELECT t.k, (SELECT count(*) FROM b WHERE b.k < t.k GROUP BY b.g)
FROM (VALUES (1), (9)) AS t(k)
----
-- A correlation that is not an equality: one outer has a surviving group, one
-- has none, and one matches no body rows at all.
-- error_v1: Correlation predicate references a column not in GROUP BY is not supported yet
WITH b(k, g, x) AS (VALUES (1, 'a', 10), (1, 'a', 20), (3, 'b', 30))
SELECT t.k, (SELECT max(b.x) FROM b WHERE b.k < t.k GROUP BY b.g HAVING count(*) > 1)
FROM (VALUES (1), (2), (4)) AS t(k)
----
-- An outer with no matching row reads NULL, and so does an expression over
-- it that would turn a row into a value.
-- error_v1: Correlation predicate references a column not in GROUP BY is not supported yet
WITH b(k, g, x) AS (VALUES (1, 'a', 10), (2, 'b', 20))
SELECT t.k,
       (SELECT s.m IS NULL
        FROM (SELECT max(b.x) AS m FROM b WHERE b.k = t.k GROUP BY b.g) s)
FROM (VALUES (1), (3)) AS t(k)
----
-- A filter above the aggregate compares each group against the outer row it
-- belongs to. An outer it rejects every group of reads NULL, as does one
-- with no group.
-- error_v1: Correlation predicate references a column not in GROUP BY is not supported yet
WITH b(k, g, x) AS (VALUES (1, 'a', 10), (1, 'a', 20), (2, 'b', 30))
SELECT t.k,
       (SELECT s.m
        FROM (SELECT max(b.x) AS m, count(*) AS c
              FROM b WHERE b.k = t.k GROUP BY b.g) s
        WHERE s.c > t.k)
FROM (VALUES (1), (2), (3)) AS t(k)
----
-- A grouping key reading an outer column groups the rows of each outer
-- separately, yielding one value per outer and NULL where nothing matches.
-- error_v1: Cannot resolve column name: k
WITH b(k, g, x) AS (VALUES (1, 10, 100), (1, 10, 200), (2, 30, 300))
SELECT t.k, (SELECT max(b.x) FROM b WHERE b.k = t.k GROUP BY b.g + t.k)
FROM (VALUES (1), (3)) AS t(k)
----
-- A FILTER mask reading an outer column selects rows against the outer row
-- they belong to, so the same group reads a different value per outer. v1
-- fails an internal size check on this shape, which the message below is.
-- error_v1: (3 vs. 4)
WITH b(k, g, x) AS (VALUES (1, 10, 100), (1, 10, 200), (2, 30, 300))
SELECT t.k,
       (SELECT max(b.x) FILTER (WHERE b.x > t.k) FROM b WHERE b.k = t.k
        GROUP BY b.g)
FROM (VALUES (1), (3)) AS t(k)
----
-- Several groups for one outer break the scalar contract with no filter to
-- select among them either.
-- error_v1: Correlation predicate references a column not in GROUP BY is not supported yet
-- error_v2: Scalar sub-query has returned multiple rows
WITH b(k, g, x) AS (VALUES (1, 'a', 10), (1, 'b', 20))
SELECT t.k, (SELECT max(b.x) FROM b WHERE b.k < t.k GROUP BY b.g)
FROM (VALUES (2)) AS t(k)
----
-- More than one group surviving the HAVING breaks the scalar contract.
-- error_v1: Correlation predicate references a column not in GROUP BY is not supported yet
-- error_v2: Scalar sub-query has returned multiple rows
WITH b(k, g, x) AS (VALUES (1, 'a', 10), (1, 'a', 20), (1, 'b', 30), (1, 'b', 40))
SELECT t.k, (SELECT max(b.x) FROM b WHERE b.k = t.k GROUP BY b.g HAVING count(*) > 1)
FROM (VALUES (1)) AS t(k)
----
-- Duplicate outer rows with the same correlation value stay
-- independent: each produces its own result row.
-- error_v1: Nested correlation across subquery boundaries is not supported yet
WITH u(k, x) AS (VALUES (1, 10), (1, 20), (2, 30)),
     v(k, x) AS (VALUES (2, 99))
SELECT t.k,
       (SELECT l.x + r.x
        FROM (SELECT x FROM u WHERE u.k = t.k) l,
             (SELECT x FROM v WHERE v.k = t.k) r)
FROM (VALUES (2), (2), (1)) AS t(k)
----
-- A scalar over a three-way correlated cross join. An empty side
-- yields NULL; when every side has one row the scalar is their sum.
-- error_v1: Nested correlation across subquery boundaries is not supported yet
WITH u(k, x) AS (VALUES (1, 10), (1, 20), (2, 30)),
     v(k, x) AS (VALUES (2, 99)),
     w(k, x) AS (VALUES (1, 7), (2, 8))
SELECT t.k,
       (SELECT a.x + b.x + c.x
        FROM (SELECT x FROM u WHERE u.k = t.k) a,
             (SELECT x FROM v WHERE v.k = t.k) b,
             (SELECT x FROM w WHERE w.k = t.k) c)
FROM (VALUES (1), (2)) AS t(k)
----
-- Correlated EXISTS over a Join with single-side correlation.
-- error_v1: Nested correlation across subquery boundaries is not supported yet
SELECT u.a
FROM u
WHERE EXISTS (
  SELECT 1
  FROM (SELECT t.a FROM t WHERE t.a = u.a + 1) l,
       (SELECT t.a FROM t) r)
----
-- Correlated IN whose body has a multi-conjunct Filter mixing a
-- correlation predicate with an uncorrelated predicate. Both
-- conjuncts must survive pull-through into the semi-join condition.
SELECT u.a IN (SELECT t.a FROM t WHERE t.a = u.a AND t.b > 0) FROM u
----
-- Correlated EXISTS whose body has a multi-conjunct Filter mixing a
-- correlation predicate with an uncorrelated predicate. Both
-- conjuncts must survive pull-through into the semi-join condition.
SELECT u.a FROM u
WHERE EXISTS (SELECT 1 FROM t WHERE t.a = u.a AND t.b > 0)
----
-- Correlated scalar `count(*)` body with a HAVING predicate over the
-- aggregate result.
SELECT (SELECT count(*) FROM u WHERE u.a = t.a HAVING count(*) > 0) FROM t
----
-- Correlated IN whose right-hand side is the result of a scalar
-- `count(*)` over a correlated body. For outers with no matching u
-- rows, `count(*)` returns 0 — so `t.a IN (...)` matches when t.a = 0.
-- error_v1: IN over a correlated aggregation is not supported yet
SELECT t.a IN (SELECT count(*) FROM u WHERE u.a = t.a) FROM t
----
-- Correlated IN whose right-hand side is the aggregate result of a
-- grouping aggregate (not a grouping key).
SELECT t.a IN (
  SELECT count(*) FROM u WHERE u.a = t.a GROUP BY u.a
) FROM t
----
-- Correlated grouped aggregate whose correlation predicate references
-- an inner column outside the GROUP BY clause: t.b is consumed by the
-- aggregate (grouped by t.a only) and cannot become a join key at the
-- outer level.
-- error_v1: Correlation predicate references a column not in GROUP BY is not supported yet
SELECT u.a IN (
  SELECT count(*) FROM t WHERE t.b = u.a GROUP BY t.a
) FROM u
----
-- Correlated IN whose body is a scalar aggregate with HAVING.
-- error_v1: IN over a correlated aggregation is not supported yet
SELECT t.a IN (
  SELECT count(*) FROM u WHERE u.a = t.a HAVING count(*) > 0
) FROM t
----
-- Correlated NOT IN whose body is a scalar aggregate that can return
-- NULL (max over empty body). NULL on the right-hand side propagates
-- NULL through NOT IN per SQL three-valued logic.
-- error_v1: IN over a correlated global aggregate is not supported yet
SELECT t.a NOT IN (SELECT max(u.a) FROM u WHERE u.a > t.a) FROM t
----
-- EXISTS over a correlated `count(*)` body where the WHERE
-- eliminates every body row. Scalar aggregates always emit one
-- row, so EXISTS sees it → TRUE for every outer.
SELECT EXISTS (SELECT count(*) FROM u WHERE u.a = t.a + 100) FROM t
----
-- Uncorrelated IN with a constant (table-less) left side over a real source.
-- The optimizer wraps the constant in a one-row probe to anchor the IN
-- semi-join.
SELECT 1 IN (SELECT a FROM u)
----
-- A non-constant, table-less IN left side (random()) has no plan-time value to
-- embed as a one-row probe, so planning fails with a clear error.
-- error_v1: Non-constant table-less left side of IN <subquery> is not supported yet
SELECT random() IN (SELECT a FROM u)
----
-- Correlated EXISTS over a GROUP BY body with no HAVING.
SELECT EXISTS (SELECT 1 FROM u WHERE u.a > t.b GROUP BY u.a) FROM t
----
-- Correlated EXISTS where HAVING references only grouping keys.
SELECT EXISTS (
  SELECT 1 FROM u WHERE u.a > t.b GROUP BY u.a HAVING u.a > 2
) FROM t
----
-- Correlated IN whose inner key is a grouping-key expression.
SELECT t.a IN (SELECT u.a FROM u WHERE u.a > t.b GROUP BY u.a) FROM t
----
-- Correlated IN whose inner key is an aggregate result.
SELECT t.a IN (
  SELECT max(u.a) FROM u WHERE u.a > t.b GROUP BY u.a
) FROM t
----
-- Correlated scalar subquery whose inner GROUP BY can return several rows per
-- outer row; the scalar must error on the first outer row with multiple groups.
-- error_v1: 0 vs. 1
-- error_v2: Scalar sub-query has returned multiple rows
SELECT (SELECT max(u.a) FROM u WHERE u.a > t.a GROUP BY u.a % 2) FROM t
----
-- LATERAL join tests.
--
-- CROSS JOIN LATERAL whose no-FROM body projects an outer column.
-- error_v1: Unsupported PlanNode LATERAL_JOIN
WITH t(x) AS (VALUES (1), (2), (3))
SELECT t.x, g.y
FROM t
CROSS JOIN LATERAL (SELECT t.x + 1 AS y) g
----
-- CROSS JOIN LATERAL with a correlated WHERE; INNER drops outer rows whose
-- body is empty.
-- error_v1: Unsupported PlanNode LATERAL_JOIN
WITH t(x) AS (VALUES (1), (2), (9)),
     u(a) AS (VALUES (1), (2))
SELECT t.x, g.m
FROM t
CROSS JOIN LATERAL (SELECT u.a AS m FROM u WHERE u.a = t.x) g
----
-- CROSS JOIN LATERAL whose body returns several rows per outer row.
-- error_v1: Unsupported PlanNode LATERAL_JOIN
WITH t(x) AS (VALUES (2), (4)),
     u(a) AS (VALUES (1), (2), (3))
SELECT t.x, g.m
FROM t
CROSS JOIN LATERAL (SELECT u.a AS m FROM u WHERE u.a < t.x) g
----
-- CROSS JOIN LATERAL whose body produces multiple columns: a body column and
-- an expression combining the body with an outer column.
-- error_v1: Unsupported PlanNode LATERAL_JOIN
WITH t(x) AS (VALUES (2), (4)),
     u(a) AS (VALUES (1), (2), (3))
SELECT t.x, g.a, g.b
FROM t
CROSS JOIN LATERAL (SELECT u.a AS a, u.a + t.x AS b FROM u WHERE u.a < t.x) g
----
-- error_v1: Unsupported PlanNode LATERAL_JOIN
-- error_v2: INNER LATERAL over an Aggregate body is not yet supported
WITH t(x) AS (VALUES (1), (2)),
     u(a) AS (VALUES (1), (2), (3))
SELECT t.x, g.c
FROM t
CROSS JOIN LATERAL (SELECT count(*) AS c FROM u WHERE u.a = t.x) g
----
-- LEFT JOIN LATERAL: a matched outer fans out to several rows; an outer whose
-- body rows are all rejected by the ON survives NULL-padded.
-- error_v1: Unsupported PlanNode LATERAL_JOIN
WITH t(x) AS (VALUES (1), (4)),
     u(a) AS (VALUES (1), (2), (3), (4))
SELECT t.x, g.m
FROM t
LEFT JOIN LATERAL (SELECT u.a AS m FROM u WHERE u.a <= t.x) g ON g.m < t.x
----
-- INNER JOIN LATERAL with an ON condition combining outer and lateral
-- columns.
-- error_v1: Unsupported PlanNode LATERAL_JOIN
WITH t(x) AS (VALUES (1), (2), (3)),
     u(a) AS (VALUES (1), (2), (3), (4))
SELECT t.x, g.m
FROM t
INNER JOIN LATERAL (SELECT u.a AS m FROM u) g ON g.m = t.x + 1
----
-- A subquery inside a LATERAL ON condition is not supported.
-- error_v1: Unsupported PlanNode LATERAL_JOIN
-- error_v2: Subquery in a LATERAL join ON condition is not supported
WITH t(x) AS (VALUES (1), (2)),
     u(a) AS (VALUES (1), (2), (3))
SELECT t.x, g.m
FROM t
INNER JOIN LATERAL (SELECT u.a AS m FROM u) g ON g.m IN (SELECT t.x)
----
-- Uncorrelated scalar subquery in an outer join's ON condition.
-- error_v1: Unsupported subqueries in the ON clause of a LEFT or RIGHT join
-- duckdb: VALUES (1, 10), (2, null)
SELECT l.a, r.a FROM (VALUES 1, 2) l(a)
LEFT JOIN (VALUES 10, 20) r(a)
  ON r.a = l.a + (SELECT max(x) FROM (VALUES 8, 9) s(x))
----
-- Scalar subquery in an outer join's ON condition correlated to the left input.
-- error_v1: Failed to place a table
-- duckdb: VALUES (1, 15), (2, 15)
SELECT l.a, r.a FROM (VALUES 1, 2) l(a)
LEFT JOIN (VALUES 10, 15, 20) r(a)
  ON r.a = (SELECT max(x) FROM (VALUES 10, 15) s(x) WHERE x > l.a)
----
-- Scalar subquery in an outer join's ON condition reading both inputs is
-- unsupported: it cannot be lifted onto either one.
-- error_v1: Unsupported subqueries in the ON clause of a LEFT or RIGHT join
-- error_v2: Subquery in an outer join's ON clause referencing both inputs
SELECT l.a, r.a FROM (VALUES 1, 2) l(a)
LEFT JOIN (VALUES 10, 15, 20) r(a)
  ON l.a = (SELECT max(x) FROM (VALUES 10, 15) s(x) WHERE x > r.a)
----
-- Two references to one uncorrelated aggregate subquery in the same scope
-- read the same value.
SELECT
  (SELECT max(x) FROM (VALUES (1), (7)) s(x)) AS a,
  (SELECT max(x) FROM (VALUES (1), (7)) s(x)) + 1 AS b
FROM (VALUES (10)) r(p)
----
-- Three references to one uncorrelated aggregate subquery -- in a conditional,
-- inside another subquery's body, and in a top-level projection -- all read
-- the same value.
SELECT
  IF(
    p > (SELECT max(x) FROM (VALUES (1), (7)) s(x)),
    (
      SELECT x FROM (VALUES (1), (7)) s(x)
      WHERE x = (SELECT max(x) FROM (VALUES (1), (7)) s(x))
    ),
    2
  ) AS a,
  (SELECT max(x) FROM (VALUES (1), (7)) s(x)) AS b
FROM (VALUES (10)) r(p)
----
-- One branch of a UNION reads an uncorrelated aggregate subquery that the
-- other branch also reads from inside a subquery body, which derives a
-- different value from it.
SELECT (SELECT max(x) FROM (VALUES (1), (7)) s(x)) AS a
FROM (VALUES (1)) t1(p)
UNION ALL
SELECT (
  SELECT y - 6 FROM (VALUES (1), (7)) w(y)
  WHERE y = (SELECT max(x) FROM (VALUES (1), (7)) s(x))
) AS a
FROM (VALUES (2)) t2(q)
----
-- An uncorrelated aggregate subquery read both inside a LATERAL body and
-- outside it yields the same value in both places.
-- error_v1: Unsupported PlanNode LATERAL_JOIN
SELECT a, m, (SELECT max(x) FROM (VALUES (2), (5)) s(x)) AS o
FROM (VALUES (1)) t(a),
     LATERAL (SELECT (SELECT max(x) FROM (VALUES (2), (5)) s(x)) - 3 AS m) g
----
-- An uncorrelated aggregate subquery and an uncorrelated IN subquery in the
-- same scope. The IN reads nothing from the aggregate, so it filters the
-- outer rows rather than the aggregate's single row.
SELECT a
FROM (VALUES (1), (2), (3)) r(a)
WHERE a > (SELECT max(x) FROM (VALUES (0), (1)) s(x))
  AND a IN (SELECT y FROM (VALUES (2), (3)) w(y))
----
-- An IN subquery whose body reads the same uncorrelated aggregate as the
-- enclosing filter. The IN still tests the outer rows.
SELECT a
FROM (VALUES (1), (2), (3)) r(a)
WHERE a > (SELECT max(x) FROM (VALUES (0), (1)) s(x))
  AND a IN (
    SELECT y FROM (VALUES (2), (3)) w(y)
    WHERE y > (SELECT max(x) FROM (VALUES (0), (1)) s(x))
  )
----
-- A scalar repeated in a nested UNION leg is evaluated separately from the
-- same scalar in its enclosing UNION leg.
SELECT c_name AS name FROM (VALUES ('c')) customer(c_name)
UNION ALL
SELECT s_name AS name FROM (VALUES ('b')) supplier(s_name)
WHERE s_name = (
    SELECT max(r_name) FROM (VALUES ('a'), ('b')) region(r_name))
  AND s_name IN (
    SELECT n_name FROM (VALUES ('a'), ('b')) nation(n_name)
    WHERE n_name = (
        SELECT max(r_name) FROM (VALUES ('a'), ('b')) region(r_name))
      AND n_name <= (
        SELECT max(r_name) FROM (VALUES ('a'), ('b')) region(r_name))
    UNION ALL
    SELECT r_name FROM (VALUES ('z')) other_region(r_name)
  )
----
-- An EXISTS subquery whose body reads the same uncorrelated aggregate as the
-- enclosing filter. Its result is the same for every outer row.
SELECT a
FROM (VALUES (1), (2), (3)) r(a)
WHERE a > (SELECT max(x) FROM (VALUES (0), (1)) s(x))
  AND EXISTS (
    SELECT 1 FROM (VALUES (2), (3)) w(y)
    WHERE y > (SELECT max(x) FROM (VALUES (0), (1)) s(x))
  )
----
-- A correlated subquery in a clause evaluated above an aggregation binds to
-- the grouping key's output column. Both tables name the column 'a', so the
-- key's name is disambiguated across the aggregation.
SELECT (SELECT 1 WHERE v.a = 2)
FROM u, v
GROUP BY v.a
----
-- The same correlation in HAVING.
SELECT v.a
FROM u, v
GROUP BY v.a
HAVING (SELECT v.a) > 4
----
-- And in ORDER BY.
-- ordered
SELECT v.a
FROM u, v
GROUP BY v.a
ORDER BY (SELECT v.a) DESC
----
-- An aggregate's argument is evaluated by the aggregation, so a correlation
-- there reads the aggregation's input rather than its output.
SELECT sum((SELECT v.a))
FROM u, v
GROUP BY v.a
----
-- The same holds for an aggregate carrying a FILTER, whose predicate the
-- aggregation also evaluates.
SELECT sum((SELECT v.a)) FILTER (WHERE (SELECT v.a) > 4)
FROM u, v
GROUP BY v.a
----
-- A correlated subquery that is both a DISTINCT output and the ORDER BY key
-- is one expression, so the sort key pairs with the output it sorts.
-- ordered
SELECT DISTINCT (SELECT v.a)
FROM u, v
GROUP BY v.a
ORDER BY (SELECT v.a)
----
-- A correlation written with the table qualifier resolves against a grouping
-- key written without one. Single-table, so the key keeps its column name and
-- the aggregation has to publish the qualified name alongside it.
SELECT (SELECT 1 WHERE v.a = 2)
FROM v
GROUP BY a
----
-- A subquery in a lambda body reads a value the row already has: it is
-- evaluated once for the row, and every element of the array sees it. The
-- smallest 'v.a' is 2, so one element of each array survives. DuckDB rejects
-- a subquery in a lambda, so the expected rows are stated here.
-- duckdb: VALUES (1, 1), (2, 1), (3, 1), (4, 1), (5, 1)
-- error_v1: Unexpected expression: Subquery
SELECT u.a, cardinality(filter(ARRAY[1, 2, 3], x -> x > (SELECT min(a) FROM v)))
FROM u
----
-- The same where the subquery is correlated to the row: the array survives
-- whole for the rows whose value 'v' holds, and empties for the rest.
-- duckdb: VALUES (1, 0), (2, 3), (3, 0), (4, 3), (5, 0)
-- error_v1: Unexpected expression: Subquery
SELECT
  u.a,
  cardinality(filter(ARRAY[1, 2, 3], x -> EXISTS (SELECT 1 FROM v WHERE v.a = u.a)))
FROM u
