-- The WHERE predicate removes the a = 0 row before the scalar-subquery
-- expression divides by a. The remaining a = 1 row satisfies HAVING.
WITH
t AS (SELECT * FROM (VALUES (0), (1)) AS _(a)),
u AS (SELECT * FROM (VALUES (10)) AS _(b))
SELECT (SELECT max(b) FROM u) / a AS pt
FROM t WHERE a <> 0 GROUP BY 1
HAVING (SELECT max(b) FROM u) / a > 1
----
-- The base-column predicate 1/a > 7 is pushed onto t, so it evaluates on the
-- a = 0 row before the join predicate can remove that row.
-- error: division by zero
WITH
t AS (SELECT * FROM (VALUES 0, 1) AS _(a)),
u AS (SELECT * FROM (VALUES 1, 2) AS _(b))
SELECT * FROM t, u WHERE a >= b AND 1/a > 7
----
-- Nested Boolean filters spanning both join inputs preserve branch
-- correlations.
WITH
t(k, a) AS (VALUES (1, 1), (2, 1), (3, 2), (4, 2), (5, 2), (6, 3)),
u(k, x, y) AS (
  VALUES
    (1, 10, 100),
    (2, 20, 200),
    (3, 30, 300),
    (4, 40, 400),
    (5, 30, 400),
    (6, 10, 100)
)
SELECT t.k
FROM t JOIN u ON t.k = u.k
WHERE
  (t.a = 1 AND ((u.x = 10 AND u.y = 100) OR (u.x = 20 AND u.y = 200)))
  OR
  (t.a = 2 AND ((u.x = 30 AND u.y = 300) OR (u.x = 40 AND u.y = 400)))
----
-- duckdb: VALUES (1, true, NULL, true, true, true), (2, false, NULL, NULL, false, true), (3, NULL, NULL, NULL, NULL, true), (4, false, false, NULL, NULL, NULL), (5, NULL, NULL, NULL, NULL, NULL), (6, false, false, NULL, false, NULL)
-- Row-valued IN preserves three-valued semantics when a row field or list
-- element is NULL.
WITH t(id, a, b) AS (
  VALUES
    (1, 1, 2),
    (2, 1, 3),
    (3, 1, NULL),
    (4, 2, NULL),
    (5, NULL, 2),
    (6, 2, 2)
)
SELECT
  id,
  (a, b) IN ((1, 2)),
  (a, b) IN ((1, NULL)),
  (a, b) IN ((1, 2), NULL),
  (a, b) IN ((1, 2), (2, 3)),
  ROW(a) IN (ROW(1), NULL)
FROM t
----
-- IS DISTINCT FROM NULL keeps every non-NULL value.
WITH t(a) AS (
  VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(NULL AS BIGINT))
)
SELECT a
FROM t
WHERE a IS DISTINCT FROM NULL
----
-- IS NOT DISTINCT FROM NULL keeps only NULL.
WITH t(a) AS (
  VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(NULL AS BIGINT))
)
SELECT a
FROM t
WHERE a IS NOT DISTINCT FROM NULL
----
-- IS DISTINCT FROM a non-NULL value keeps NULL and unequal values.
WITH t(a) AS (
  VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(NULL AS BIGINT))
)
SELECT a
FROM t
WHERE a IS DISTINCT FROM 1
----
-- IS NOT DISTINCT FROM a non-NULL value keeps only equal values.
WITH t(a) AS (
  VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(NULL AS BIGINT))
)
SELECT a
FROM t
WHERE a IS NOT DISTINCT FROM 1
----
-- count 0
WITH t(a) AS (
  VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(NULL AS BIGINT))
)
SELECT a
FROM t
WHERE a IS NOT DISTINCT FROM NULL AND a > 1
