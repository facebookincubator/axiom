-- Tests for nested aggregations across DT boundaries
-- These verify that inner aliased aggregate functions are properly resolved
-- in outer queries without attempting to re-expand them after the aggregate
-- no longer exists.

-- Basic nested aggregation with reverse(array_agg())
-- duckdb: WITH grouped AS (SELECT id, array_agg(val) AS vals FROM (VALUES (1, 5), (1, 3), (2, 7)) t(id, val) GROUP BY id) SELECT id, sum(array_length(vals)) AS result FROM grouped GROUP BY id
WITH grouped AS (
  SELECT id, reverse(array_agg(val)) AS vals
  FROM (VALUES (1, 5), (1, 3), (2, 7)) t(id, val)
  GROUP BY id
)
SELECT id, sum(cardinality(vals)) AS result
FROM grouped
GROUP BY id
----
-- Nested aggregation with CASE expression containing array_agg()
-- duckdb: WITH grouped AS (SELECT id, CASE WHEN id = 1 THEN array_agg(val) ELSE ARRAY[0] END AS vals FROM (VALUES (1, 5), (1, 3), (2, 7)) t(id, val) GROUP BY id) SELECT sum(array_length(vals)) AS result FROM grouped
WITH grouped AS (
  SELECT id, CASE WHEN id = 1 THEN array_agg(val) ELSE ARRAY[0] END AS vals
  FROM (VALUES (1, 5), (1, 3), (2, 7)) t(id, val)
  GROUP BY id
)
SELECT sum(cardinality(vals)) AS result
FROM grouped
