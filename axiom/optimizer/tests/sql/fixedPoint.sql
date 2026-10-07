-- A recursive CTE produces one row per iteration until its step returns none.
WITH RECURSIVE r(n) AS (
  SELECT 1
  UNION ALL
  SELECT n + 1 FROM r WHERE n < 5
)
SELECT n FROM r
----
-- A constant above a recursive CTE is computed for every iteration, rather
-- than reading the equal constant that produced the anchor row.
WITH RECURSIVE r(n) AS (
  SELECT 1
  UNION ALL
  SELECT n + 1 FROM r WHERE n < 3
)
SELECT n, 1 AS one FROM r
----
-- 'r.*' in the step expands to every column of the recursive CTE, including
-- the two columns named 'x'.
-- duckdb: VALUES (1, 7, 2)
WITH RECURSIVE r AS (
  SELECT * FROM ((VALUES (1, 7)) t(x, y) CROSS JOIN (VALUES (2)) u(x)) v
  UNION ALL
  SELECT r.* FROM r WHERE r.y < 7
)
SELECT * FROM r
