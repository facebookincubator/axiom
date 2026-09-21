-- pushdown_table: pushdown_result
-- setup
CREATE TABLE source(a BIGINT, b BIGINT)
----
INSERT INTO source VALUES (1, 10), (1, 20), (2, 30)
----
CREATE TABLE pushdown_result(a BIGINT, s BIGINT)
----
INSERT INTO pushdown_result VALUES (7, 100)
-- end_setup

-- Distinct source and replacement results prove that the accepted subtree was replaced.
-- duckdb: SELECT a, s FROM pushdown_result
SELECT a, sum(b) AS s FROM source GROUP BY a
