-- connector: hive
-- setup
CREATE TABLE m (v BIGINT, k BIGINT) WITH (partitioned_by = ARRAY['k'])
----
INSERT INTO m VALUES
  (CAST(NULL AS BIGINT), 0),
  (CAST(NULL AS BIGINT), 0),
  (5, 1),
  (6, 1),
  (7, 2)
-- end_setup

-- DuckDB has no approx_count_star; compare against count(*).
-- duckdb: SELECT count(*) FROM m
SELECT approx_count_star() FROM m
----
-- duckdb: SELECT k, count(*) FROM m GROUP BY 1
SELECT k, approx_count_star() FROM m GROUP BY 1
----
-- duckdb: SELECT count(*) - count(v) FROM m
SELECT approx_null_count(v) FROM m
----
-- duckdb: SELECT k, count(*) - count(v) FROM m GROUP BY 1
SELECT k, approx_null_count(v) FROM m GROUP BY 1
----
-- duckdb: SELECT count(v) FROM m
SELECT approx_non_null_count(v) FROM m
----
-- duckdb: SELECT k, count(v) FROM m GROUP BY 1
SELECT k, approx_non_null_count(v) FROM m GROUP BY 1
----
-- A regular aggregate alongside a metadata aggregate.
-- duckdb: SELECT count(*), sum(v) FROM m
SELECT approx_count_star(), sum(v) FROM m
----
-- One leg is a metadata aggregate, the other a regular aggregate.
-- duckdb: SELECT count(*) FROM m UNION ALL SELECT sum(v) FROM m
SELECT approx_count_star() FROM m UNION ALL SELECT sum(v) FROM m
