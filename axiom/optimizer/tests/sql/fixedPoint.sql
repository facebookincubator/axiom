-- setup_file: common_setup.sql
-- A recursive CTE produces one row per iteration until its step returns none.
WITH RECURSIVE r(n) AS (
  SELECT 1
  UNION ALL
  SELECT n + 1 FROM r WHERE n < 5
)
SELECT n FROM r
----
-- A step that returns no rows leaves only the anchor's rows.
WITH RECURSIVE r(n) AS (
  SELECT 1
  UNION ALL
  SELECT n + 1 FROM r WHERE false
)
SELECT n FROM r
----
-- An empty anchor still runs the step once, which can return rows from an
-- empty iteration.
WITH RECURSIVE r(n) AS (
  SELECT CAST(1 AS BIGINT) WHERE false
  UNION ALL
  SELECT c + 1 FROM (SELECT count(*) AS c FROM r) WHERE c = 0
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
-- A step that joins a table follows its rows from each iteration's values.
WITH RECURSIVE r(n, d) AS (
  SELECT CAST(1 AS BIGINT), 0
  UNION ALL
  SELECT t.a % 3 + 1, d + 1 FROM r JOIN t ON t.a = r.n WHERE d < 2
)
SELECT n, d, count(*) FROM r GROUP BY n, d
----
-- A step that joins a table the optimizer cannot size runs as one loop spread
-- over the workers by the iteration's values.
WITH RECURSIVE r(n, d) AS (
  SELECT CAST(1 AS BIGINT), 0
  UNION ALL
  SELECT t_no_stats.a % 3 + 1, d + 1
  FROM r JOIN t_no_stats ON t_no_stats.a = r.n
  WHERE d < 3
)
SELECT n, d, count(*) FROM r GROUP BY n, d
----
-- A step whose join key is computed from the iteration's values runs as one
-- loop that repartitions each iteration's rows on that key.
WITH RECURSIVE r(n, d) AS (
  SELECT CAST(0 AS BIGINT), 0
  UNION ALL
  SELECT t_no_stats.a % 3, d + 1
  FROM r JOIN t_no_stats ON t_no_stats.a = r.n + 1
  WHERE d < 3
)
SELECT n, d, count(*) FROM r GROUP BY n, d
----
-- A join condition beyond the keys reads the joined table's key and other
-- columns.
WITH RECURSIVE r(n, d) AS (
  SELECT CAST(1 AS BIGINT), 0
  UNION ALL
  SELECT t.a % 3 + 1, d + 1 FROM r JOIN t ON t.a = r.n AND t.b > t.a * 20 + d
  WHERE d < 3
)
SELECT n, d, count(*) FROM r GROUP BY n, d
----
-- A step returns the joined table's key.
WITH RECURSIVE r(n, k, d) AS (
  SELECT CAST(1 AS BIGINT), CAST(0 AS BIGINT), 0
  UNION ALL
  SELECT t.a % 3 + 1, t.a, d + 1 FROM t JOIN r ON t.a = r.n WHERE d < 2
)
SELECT n, k, d, count(*) FROM r GROUP BY n, k, d
----
-- A step that outer joins a table keeps the iteration's rows with no match.
WITH RECURSIVE r(n) AS (
  SELECT CAST(1 AS BIGINT)
  UNION ALL
  SELECT coalesce(t.a, 0) FROM r LEFT JOIN t ON t.a = r.n + 1 WHERE r.n < 3
)
SELECT n, count(*) FROM r GROUP BY n
----
-- A step that joins a table on an inequality.
WITH RECURSIVE r(n) AS (
  SELECT CAST(1 AS BIGINT)
  UNION ALL
  SELECT t.a FROM r JOIN t ON t.a > r.n WHERE r.n < 3
)
SELECT n, count(*) FROM r GROUP BY n
----
-- A step that crosses a relation of one row, computed from the state, with a
-- table.
WITH RECURSIVE r(n) AS (
  SELECT CAST(1 AS BIGINT)
  UNION ALL
  SELECT u.a
  FROM (SELECT sum(n) AS m FROM r) s
  CROSS JOIN t_no_stats u
  WHERE s.m < 2
)
SELECT n, count(*) FROM r GROUP BY n
