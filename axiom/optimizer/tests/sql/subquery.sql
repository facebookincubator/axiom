-- Subquery tests.

SELECT EXISTS(SELECT 1), EXISTS(SELECT 1), EXISTS(SELECT 3), NOT EXISTS(SELECT 1), NOT EXISTS(SELECT 1 WHERE false)
----
SELECT (EXISTS(SELECT 1)) = (EXISTS(SELECT 3)) WHERE NOT EXISTS(SELECT 1 WHERE false)
----
-- IN list with a scalar subquery and a literal.
-- duckdb: SELECT a, b FROM t WHERE a IN ((SELECT max(a) FROM t), 1)
SELECT a, b FROM t WHERE a IN ((SELECT max(a) FROM t), 1)
----
-- IN list with two scalar subqueries.
-- duckdb: SELECT a, b FROM t WHERE a IN ((SELECT max(a) FROM t), (SELECT min(a) FROM t))
SELECT a, b FROM t WHERE a IN ((SELECT max(a) FROM t), (SELECT min(a) FROM t))
----
-- Case-insensitive CTE alias resolution.
-- duckdb: WITH a AS (SELECT * FROM (VALUES (1)) t(a)) SELECT A.a FROM A
WITH a AS (SELECT * FROM (VALUES (1)) t(a)) SELECT A.a FROM A
----
-- Case-insensitive CTE alias with wildcard expansion.
-- duckdb: WITH a AS (SELECT * FROM (VALUES (1)) t(a)) SELECT A.* FROM A
WITH a AS (SELECT * FROM (VALUES (1)) t(a)) SELECT A.* FROM A
----
-- Quoted CTE alias with wildcard expansion (Presto ignores quotes for case).
-- duckdb: WITH "UpperCase" AS (SELECT * FROM (VALUES (1, 2)) t(x, y)) SELECT "UpperCase".* FROM "UpperCase"
WITH "UpperCase" AS (SELECT * FROM (VALUES (1, 2)) t(x, y)) SELECT "uPPERcASE".* FROM "uppercase"
----
-- Case-insensitive alias with wildcard in JOIN (via processAliasedRelation).
-- duckdb: SELECT T.* FROM (VALUES (1)) t(a) JOIN (VALUES (2)) u(b) ON true
SELECT T.* FROM (VALUES (1)) t(a) JOIN (VALUES (2)) u(b) ON true
