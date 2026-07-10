-- setup_file: common_setup.sql

-- A reused non-deterministic binding yields equal values at both references.
-- error_v1: Non-deterministic expression reused
SELECT x = y
FROM (SELECT u AS x, u AS y FROM (SELECT cast(uuid() AS varchar) AS u FROM t))
----
-- A reused complex-typed non-deterministic source read through subfields; the
-- two references never diverge.
-- error_v1: Non-deterministic expression reused
-- count 0
SELECT x
FROM (SELECT s[1] AS x, s[1] AS y FROM (SELECT shuffle(sequence(1, 5)) AS s FROM t))
WHERE x <> y
----
-- A non-deterministic source reused across a filter and a projection observes
-- the same single draw: a value that passed the r > 0.5 filter is never <= 0.5
-- in the projection.
-- error_v1: Non-deterministic expression reused
-- count 0
SELECT o
FROM (SELECT r AS o FROM (SELECT random() AS r FROM t) WHERE r > 0.5)
WHERE o <= 0.5
----
-- A reused complex source read once as a whole value and once via a subfield.
-- error_v1: Non-deterministic expression reused
-- count 0
SELECT x
FROM (SELECT s AS x, s[1] AS y FROM (SELECT shuffle(sequence(1, 5)) AS s FROM t))
WHERE x[1] <> y
----
-- A reused lambda result.
-- error_v1: Non-deterministic expression reused
-- count 0
SELECT x
FROM (
  SELECT v AS x, v AS y
  FROM (SELECT transform(sequence(1, 3), e -> e + cast(random() * 1000000 AS bigint)) AS v FROM t))
WHERE x <> y
----
-- Reuse of a name bound to a struct field read out of a non-deterministic
-- source.
-- error_v1: Non-deterministic expression reused
-- count 0
SELECT x
FROM (
  SELECT s AS x, s AS y
  FROM (SELECT shuffle(array[cast(row(a, a) AS row(f1 bigint, f2 bigint))])[1].f1 AS s FROM t))
WHERE x <> y
----
-- Nested non-deterministic subscript over a non-deterministic call.
-- error_v1: Non-deterministic expression reused
-- count 0
SELECT x
FROM (SELECT s[1] AS x, s[1] AS y FROM (SELECT shuffle(array[sequence(1, 5)])[1] AS s FROM t))
WHERE x <> y
----
-- Reading two subfields of a struct with a non-deterministic field returns the
-- deterministic subfield and an in-range draw.
-- error_v1: Non-deterministic expression reused
-- count 0
SELECT p
FROM (
  SELECT n.x AS p, n.y AS q, a
  FROM (SELECT cast(row(a, random()) AS row(x bigint, y double)) AS n, a FROM t))
WHERE p <> a OR q < 0 OR q >= 1
----
-- An aliased non-deterministic subscript index reads a valid array element.
-- error_v1: Non-deterministic expression reused
-- count 0
SELECT u
FROM (
  SELECT s[i] AS u
  FROM (SELECT array[10, 20, 30, 40, 50] AS s, cast(random() * 3 AS integer) + 1 AS i FROM t))
WHERE u NOT IN (10, 20, 30, 40, 50)
----
-- A non-deterministic call is evaluated per row, not folded to a constant: the
-- 15 rows produce 15 distinct draws.
SELECT count(DISTINCT cast(uuid() AS varchar)) FROM t
----
-- Two independent draws differ.
SELECT uuid() = uuid() FROM t
----
-- A non-deterministic source used once in a filter that never passes.
-- count 0
SELECT a FROM t WHERE random() >= 2.0
----
-- A non-deterministic source referenced once is evaluated per row: distinct
-- draws in every row.
SELECT count(DISTINCT r) FROM (SELECT random() AS r FROM t)
----
-- Per-row evaluation survives a single rename.
SELECT count(DISTINCT x) FROM (SELECT r AS x FROM (SELECT random() AS r FROM t))
----
-- Per-row evaluation survives a rename chain across nested projections.
SELECT count(DISTINCT z) FROM (SELECT y AS z FROM (SELECT x AS y FROM (SELECT random() AS x FROM t)))
----
-- A complex source read by a single subfield yields an element of the source.
-- duckdb: SELECT true FROM t
SELECT x BETWEEN 1 AND 5
FROM (SELECT s[1] AS x FROM (SELECT shuffle(sequence(1, 5)) AS s FROM t))
----
-- A deterministic complex value reused via subfield.
-- duckdb: SELECT 1, 1 FROM t
SELECT s[1] AS x, s[1] AS y FROM (SELECT sequence(1, 5) AS s FROM t)
