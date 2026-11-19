-- connector: hive
-- setup
-- The Hive connector turns a `rand() < p` filter into the scan's sample rate.
-- These queries check filters whose threshold is at or outside [0, 1).
CREATE TABLE t AS SELECT * FROM (VALUES (1), (2), (3)) AS v (a)
-- end_setup

-- A comparison of rand() with a literal that every value in [0, 1) satisfies, or
-- none does, keeps every row or none.
-- duckdb: SELECT 0::BIGINT
SELECT count(*) FROM t WHERE rand() < 0
----
-- random() is an alias for rand().
-- duckdb: SELECT 0::BIGINT
SELECT count(*) FROM t WHERE random() < 0
----
-- duckdb: SELECT count(*) FROM t
SELECT count(*) FROM t WHERE rand() < 1
----
-- duckdb: SELECT count(*) FROM t
SELECT count(*) FROM t WHERE rand() >= 0
