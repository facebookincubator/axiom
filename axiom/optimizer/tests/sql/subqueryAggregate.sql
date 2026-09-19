-- setup_file: common_setup.sql
-- setup
CREATE TABLE u AS FROM (VALUES (1), (2), (3), (4), (5)) AS _(a)
----
CREATE TABLE v AS FROM (VALUES (2), (4), (6), (8), (10)) AS _(a)
-- end_setup

-- Subquery bodies that aggregate: grouping, HAVING, and scalar aggregates.

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
