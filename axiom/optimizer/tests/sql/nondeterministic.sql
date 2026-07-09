-- setup_file: common_setup.sql

-- A non-deterministic projection referenced more than once is computed once.
-- count 15
SELECT a FROM (SELECT y AS p, y AS q, a FROM (SELECT concat(cast(uuid() AS varchar), '-x') AS y, a FROM t)) WHERE p = q
----
-- A non-deterministic value compared against itself is one draw, always equal.
-- count 15
SELECT eq FROM (SELECT (r = r) AS eq FROM (SELECT rand() AS r FROM t)) WHERE eq
----
-- r <> r on one draw is never true, so no rows pass.
-- count 0
SELECT a FROM (SELECT rand() AS r, a FROM t) WHERE r <> r
----
-- A subfield compared against itself reads one materialized element.
-- count 15
SELECT eq FROM (SELECT (s[1] = s[1]) AS eq FROM (SELECT shuffle(sequence(1, a + 5)) AS s FROM t)) WHERE eq
----
-- Two independent non-deterministic draws.
-- count 15
SELECT uuid() AS x, uuid() AS y FROM t
----
-- A reused deterministic projection is inlined.
SELECT a + 1 AS p, (a + 1) * 2 AS q FROM t
----
-- A non-deterministic source read via subfield more than once is computed once.
-- count 15
SELECT a FROM (SELECT s[1] AS x, s[1] AS y, a FROM (SELECT shuffle(sequence(1, 100)) AS s, a FROM t)) WHERE x = y
----
-- A subfield of a subfield over a draw is shared.
-- count 15
SELECT a FROM (SELECT f[1] AS x, f[1] AS y, a FROM (SELECT shuffle(array[array[1, 2, 3], array[4, 5, 6]])[1] AS f, a FROM t)) WHERE x = y
----
-- A draw reused within a UNION ALL leg is one draw per leg.
-- count 30
SELECT a FROM (SELECT u AS x, u AS y, a FROM (SELECT cast(uuid() AS varchar) AS u, a FROM t) UNION ALL SELECT u AS x, u AS y, a FROM (SELECT cast(uuid() AS varchar) AS u, a FROM t)) WHERE x = y
----
-- A draw referenced more than once in one filter is one draw.
-- count 15
SELECT a FROM (SELECT cast(uuid() AS varchar) AS u, a FROM t) WHERE u >= '0' AND u <= 'zzzzz'
----
-- A draw shared by a filter and a projection is one draw.
-- count 15
SELECT x FROM (SELECT u AS x, u AS y FROM (SELECT cast(uuid() AS varchar) AS u FROM t) WHERE u <= 'zzzzz') WHERE x = y
----
-- A base-column filter pushes below the draw; the reused subfield stays one
-- draw.
-- count 15
SELECT k FROM (SELECT s[1] AS x, s[1] AS y, k FROM (SELECT shuffle(sequence(1, k)) AS s, k FROM (SELECT a + 5 AS k FROM t)) WHERE k > 0) WHERE x = y
----
-- A subfield of a draw reused within a UNION ALL leg is one draw per leg.
-- count 30
SELECT a FROM (SELECT s[1] AS x, s[1] AS y, a FROM (SELECT shuffle(sequence(1, a + 10)) AS s, a FROM t) UNION ALL SELECT s[1] AS x, s[1] AS y, a FROM (SELECT shuffle(sequence(1, a + 10)) AS s, a FROM t)) WHERE x = y
----
-- A draw computed above an aggregate and reused is one draw per group.
-- count 3
SELECT mx FROM (SELECT u AS x, u AS y, mx FROM (SELECT cast(uuid() AS varchar) AS u, mx FROM (SELECT max(b) AS mx FROM t GROUP BY a % 3))) WHERE x = y
----
-- A pre-aggregation draw reused across several aggregate arguments is computed
-- once.
-- count 1
SELECT sum(r) AS s, count(r) AS cnt, max(r) AS mx FROM (SELECT rand() AS r FROM t)
----
-- A pre-window draw reused across two window functions is computed once.
-- count 15
SELECT max(r) OVER (PARTITION BY a) AS mx, min(r) OVER (PARTITION BY a) AS mn FROM (SELECT rand() AS r, a FROM t)
----
-- A draw used as a grouping key and reused post-aggregation is one draw.
-- count 15
SELECT cnt FROM (SELECT u AS x, u AS y, cnt FROM (SELECT u, count(1) AS cnt FROM (SELECT cast(uuid() AS varchar) AS u FROM t) GROUP BY u)) WHERE x = y
----
-- A draw used as an equi-join key is materialized once per side.
-- count 0
SELECT l.u AS x, l.u AS y FROM (SELECT cast(uuid() AS varchar) AS u FROM t) l JOIN (SELECT cast(uuid() AS varchar) AS s FROM t) r ON l.u = r.s
----
-- A draw read by an equi-join key and a post-join filter stays materialized.
-- count 0
SELECT l.u AS x FROM (SELECT cast(uuid() AS varchar) AS u FROM t) l JOIN (SELECT cast(uuid() AS varchar) AS s FROM t) r ON l.u = r.s WHERE l.u > '0'
----
-- A draw on the null-supplying side of a LEFT JOIN stays materialized.
-- count 15
SELECT l.a AS lk, r.u AS uu FROM t l LEFT JOIN (SELECT cast(uuid() AS varchar) AS u, b AS v FROM t) r ON l.a = r.v AND r.u > '0'
----
-- A reused draw in a filter alongside a window on a base column is one draw.
-- count 15
SELECT row_number() OVER (PARTITION BY a) AS rn, u AS uu FROM (SELECT cast(uuid() AS varchar) AS u, a FROM t) WHERE u >= '0' AND u <= 'zzzzz'
----
-- A draw computed alongside a window output and reused is one draw.
-- count 15
SELECT m FROM (SELECT u AS x, u AS y, m FROM (SELECT row_number() OVER (PARTITION BY a ORDER BY a) AS m, cast(uuid() AS varchar) AS u FROM t)) WHERE x = y
----
-- A draw read by a window argument and a filter stays materialized.
-- count 15
SELECT max(u) OVER (PARTITION BY a ORDER BY b) AS m FROM (SELECT cast(uuid() AS varchar) AS u, a, b FROM t) WHERE u >= '0' AND u <= 'zzzzz'
----
-- A filter on a reused draw over a scan is not pushed down.
-- count 15
SELECT x FROM (SELECT s[1] AS x, s[1] AS y FROM (SELECT shuffle(sequence(1, a + 10)) AS s FROM t)) WHERE x = y AND x > -1
----
-- A draw used only in a filter is pushed to the scan.
-- count 15
SELECT a FROM (SELECT rand() AS r, a FROM t) WHERE r > -1.0
----
-- A draw read through a single subfield is pushed to the scan.
-- count 15
SELECT a FROM (SELECT shuffle(sequence(1, a + 10)) AS s, a FROM t) WHERE s[1] > -1
----
-- A per-row draw captured in a lambda body is one draw.
-- count 15
SELECT a FROM (SELECT rand() AS r, a FROM t) WHERE cardinality(array_distinct(transform(sequence(1, a + 2), x -> r))) = 1
----
-- A draw materialized once above a UNION ALL, reused by a filter and two
-- outputs.
-- count 30
SELECT p FROM (SELECT arr[1] AS p, arr[1] AS q FROM (SELECT shuffle(sequence(1, a + 10)) AS arr FROM (SELECT a FROM t UNION ALL SELECT a FROM t))) WHERE p = q AND p > -1
----
-- A field of a field over a draw, reused, is one draw.
-- count 15
SELECT a FROM (SELECT field2 AS x, field2 AS y, a FROM (SELECT field1[2] AS field2, a FROM (SELECT f0[1] AS field1, a FROM (SELECT shuffle(array[array[1, 2, 3], array[4, 5, 6]]) AS f0, a FROM t)))) WHERE x = y
----
-- A multi-step subscript chain over a draw, reused, is one draw.
-- count 15
SELECT a FROM (SELECT field1[1] AS x, field1[1] AS y, a FROM (SELECT f0[1][1] AS field1, a FROM (SELECT shuffle(array[array[array[1, 2], array[3, 4]], array[array[5, 6], array[7, 8]]]) AS f0, a FROM t))) WHERE x = y
----
-- A draw referenced by ORDER BY and the projection is one draw.
-- count 15
SELECT x FROM (SELECT u AS x, u AS y FROM (SELECT shuffle(sequence(1, a + 10))[1] AS u FROM t)) WHERE x = y ORDER BY x
----
-- A reused draw with ORDER BY on a base column, no limit.
-- count 15
SELECT x FROM (SELECT u AS x, u AS y, a FROM (SELECT cast(uuid() AS varchar) AS u, a FROM t)) WHERE x = y ORDER BY a
----
-- A draw over a subquery LIMIT; the LIMIT is a cardinality barrier below the
-- draw.
-- count 3
SELECT cast(uuid() AS varchar) AS u FROM (SELECT a FROM t LIMIT 3)
----
-- A draw over ORDER BY ... LIMIT; the draw sits above the top-N.
-- count 3
SELECT cast(uuid() AS varchar) AS u, a FROM (SELECT a FROM t ORDER BY a LIMIT 3)
----
-- A draw reused standalone and nested inside another reused expression.
-- count 15
SELECT uu FROM (SELECT u AS uu, concat(u, 'x') AS v1, concat(u, 'x') AS v2 FROM (SELECT cast(uuid() AS varchar) AS u FROM t)) WHERE v1 = v2
----
-- A draw read via subfield feeds the argument of a second draw; both reused.
-- count 15
SELECT mm FROM (SELECT m AS mm, arr[1] AS b1, arr[1] AS b2 FROM (SELECT m, shuffle(sequence(1, m + 1)) AS arr FROM (SELECT shuffle(sequence(1, 10))[1] AS m FROM t))) WHERE b1 = b2
----
-- A non-deterministic lambda result reused is materialized once.
-- count 15
SELECT a FROM (SELECT arr AS p, arr AS q, a FROM (SELECT transform(sequence(1, 3), x -> rand()) AS arr, a FROM t)) WHERE p = q
----
-- A single-use non-deterministic lambda needs no shared materialization.
-- count 15
SELECT transform(sequence(1, 3), x -> rand()) AS a FROM t
----
-- Two parallel draws (a uuid and a lambda array), each materialized once.
-- count 15
SELECT a1 FROM (SELECT u AS u1, u AS u2, arr AS a1 FROM (SELECT cast(uuid() AS varchar) AS u, transform(sequence(1, 3), x -> rand()) AS arr FROM t)) WHERE u1 = u2
----
-- A filter on a constant tag drops one UNION ALL leg; the survivor's reused
-- draw is one draw.
-- count 15
SELECT x FROM (SELECT u AS x, u AS y, 1 AS tag FROM (SELECT cast(uuid() AS varchar) AS u FROM t) UNION ALL SELECT 'z' AS x, 'z' AS y, 2 AS tag FROM t) WHERE tag = 1 AND x = y
----
-- A reused draw in a subquery joined with another table is materialized below
-- the join.
-- count 45
SELECT x FROM (SELECT r AS x, r AS y FROM (SELECT cast(uuid() AS varchar) AS r FROM t)) CROSS JOIN (SELECT b FROM t WHERE b <= 30) WHERE x = y
----
-- Reused draws from both sides of a join, each materialized once below the join.
-- count 45
SELECT l.lr AS a, l.lr AS b, r.rr AS c, r.rr AS d FROM (SELECT cast(uuid() AS varchar) AS lr FROM t) l CROSS JOIN (SELECT cast(uuid() AS varchar) AS rr FROM t WHERE b <= 30) r
----
-- An inline draw as a window argument over a LIMIT; the LIMIT stays below the
-- window.
-- count 3
SELECT count(rand()) OVER (PARTITION BY a) AS c FROM (SELECT a FROM t LIMIT 3)
----
-- A window whose argument references another window's output, in a
-- non-deterministic projection.
-- count 15
SELECT sum(w) OVER (PARTITION BY a) AS s, rand() AS r FROM (SELECT row_number() OVER (PARTITION BY a ORDER BY b) AS w, a FROM t)
----
-- A draw read both as a whole value and via a subfield is one draw; the
-- subfield of the whole matches the separate subfield read.
-- count 15
SELECT a FROM (SELECT s AS x, s[1] AS y, a FROM (SELECT shuffle(sequence(1, 5)) AS s, a FROM t)) WHERE x[1] = y
----
-- A struct field read out of a non-deterministic source, reused, is one draw.
-- count 15
SELECT a FROM (SELECT f AS x, f AS y, a FROM (SELECT shuffle(array[cast(row(a, a) AS row(f1 bigint, f2 bigint))])[1].f1 AS f, a FROM t)) WHERE x = y
----
-- Reading two subfields of a struct that carries a non-deterministic field; the
-- deterministic field still equals its base column.
-- count 15
SELECT q FROM (SELECT n.x AS p, n.y AS q, a FROM (SELECT cast(row(a, rand()) AS row(x bigint, y double)) AS n, a FROM t)) WHERE p = a
----
-- A non-deterministic value used as an array subscript index (single use).
-- count 15
SELECT s[i] AS u FROM (SELECT array[10, 20, 30, 40, 50] AS s, cast(rand() * 3 AS integer) + 1 AS i FROM t)
