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
