-- setup_file: common_setup.sql

-- Reject: both references resolve to one shared inline expression that would be
-- re-drawn.
-- error: Non-deterministic expression reused
SELECT u AS x, u AS y
FROM (SELECT cast(uuid() AS varchar) AS u FROM t)
----
-- Reject: a reused complex-typed non-deterministic source read through
-- subfields.
-- error: Non-deterministic expression reused
SELECT s[1] AS x, s[1] AS y
FROM (SELECT shuffle(sequence(1, 5)) AS s FROM t)
----
-- Reject: a non-deterministic source read by both a filter and a projection is
-- reused.
-- error: Non-deterministic expression reused
SELECT r FROM (SELECT rand() AS r FROM t) WHERE r > 0.5
----
-- Reject: a reused complex source read once as a whole value and once via a
-- subfield.
-- error: Non-deterministic expression reused
SELECT s AS x, s[1] AS y
FROM (SELECT shuffle(sequence(1, 5)) AS s FROM t)
----
-- Reject: a reused lambda result.
-- error: Non-deterministic expression reused
SELECT v AS x, v AS y
FROM (SELECT transform(sequence(1, 3), e -> e + cast(rand() * 1000000 AS bigint)) AS v FROM t)
----
-- Reject: reuse of a name bound to a struct field read out of a
-- non-deterministic source.
-- error: Non-deterministic expression reused
SELECT s AS x, s AS y
FROM (SELECT shuffle(array[cast(row(a, a) AS row(f1 bigint, f2 bigint))])[1].f1 AS s FROM t)
----
-- Reject: nested non-deterministic subscript over a non-deterministic call.
-- error: Non-deterministic expression reused
SELECT s[1] AS x, s[1] AS y
FROM (SELECT shuffle(array[sequence(1, 5)])[1] AS s FROM t)
----
-- Known conservative over-rejection: reading more than one subfield of a struct
-- that carries a non-deterministic field.
-- error: Non-deterministic expression reused
SELECT n.x AS p, n.y AS q
FROM (SELECT cast(row(a, rand()) AS row(x bigint, y double)) AS n FROM t)
----
-- Known conservative over-rejection: a single use of an aliased
-- non-deterministic subscript index.
-- error: Non-deterministic expression reused
SELECT s[i] AS u
FROM (SELECT array[10, 20, 30, 40, 50] AS s, cast(rand() * 3 AS integer) + 1 AS i FROM t)
----
-- Accept: two independent non-deterministic draws.
-- count 15
SELECT uuid() AS x, uuid() AS y FROM t
----
-- Accept: a non-deterministic source used only once in a filter.
-- count 15
SELECT a FROM t WHERE rand() < 2.0
----
-- Accept: a non-deterministic source referenced once without rename.
-- count 15
SELECT r FROM (SELECT rand() AS r FROM t)
----
-- Accept: a non-deterministic source referenced once, through a single rename.
-- count 15
SELECT r AS x FROM (SELECT rand() AS r FROM t)
----
-- Accept: a non-deterministic source renamed across nested projections but used
-- once.
-- count 15
SELECT y AS z FROM (SELECT x AS y FROM (SELECT rand() AS x FROM t))
----
-- Accept: a complex source read by a single subfield.
-- count 15
SELECT s[1] AS x FROM (SELECT shuffle(sequence(1, 5)) AS s FROM t)
----
-- Accept: a deterministic complex value reused via subfield.
-- count 15
SELECT s[1] AS x, s[1] AS y FROM (SELECT sequence(1, 5) AS s FROM t)
