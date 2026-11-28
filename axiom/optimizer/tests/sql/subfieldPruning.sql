-- connector: hive
-- setup
CREATE TABLE t AS
SELECT * FROM (VALUES
  (0, MAP(ARRAY[0, 1], ARRAY[ARRAY[10, 20], ARRAY[11, 21]]), ARRAY[1, 2, 3]),
  (1, MAP(ARRAY[0, 1], ARRAY[ARRAY[12, 22], ARRAY[13, 23]]), ARRAY[4, 5, 6])
) AS _(k, m, a)
----
CREATE TABLE u AS
SELECT CAST(ROW(f, g) AS ROW(f BIGINT, g BIGINT)) AS s
FROM (VALUES (1, 10), (2, 20)) AS _(f, g)
----
CREATE TABLE v AS
SELECT CAST(ROW(f, g) AS ROW(f BIGINT, g BIGINT)) AS s, CAST(k AS BIGINT) AS k
FROM (VALUES (3, 30, 0), (4, 40, 1)) AS _(f, g, k)
----
CREATE TABLE w AS
SELECT CAST(ROW(5, 50) AS ROW(f BIGINT, g BIGINT)) AS s, CAST(5 AS BIGINT) AS k
----
CREATE TABLE pair AS
SELECT
  CAST(ROW(1, 10) AS ROW(f BIGINT, g BIGINT)) AS a,
  CAST(ROW(2, 20) AS ROW(f BIGINT, g BIGINT)) AS b
----
CREATE TABLE gate AS SELECT CAST(1 AS BIGINT) AS k
----
CREATE TABLE person AS
SELECT CAST(id AS BIGINT) AS id, name
FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS _(id, name)
----
CREATE TABLE knows AS
SELECT
  CAST(src AS BIGINT) AS src,
  CAST(dst AS BIGINT) AS dst,
  CAST(ROW(f, g) AS ROW(f BIGINT, g BIGINT)) AS p,
  ARRAY[CAST(f AS BIGINT), CAST(g AS BIGINT)] AS tags
FROM (VALUES (1, 2, 12, 120), (2, 3, 23, 230)) AS _(src, dst, f, g)
-- end_setup

-- define graph
GRAPH g AS (
  VERTEX TABLES (person KEY (id) LABEL Person PROPERTIES (id, name))
  EDGE TABLES (
    knows KEY (src, dst)
      SOURCE KEY (src) REFERENCES person (id)
      DESTINATION KEY (dst) REFERENCES person (id)
      LABEL Knows))
-- end_define

-- Table t(k INTEGER, m MAP(INTEGER, ARRAY(INTEGER)), a ARRAY(INTEGER)):
--   k | m                              | a
--  ---+--------------------------------+-----------
--   0 | {0: [10, 20], 1: [11, 21]}     | [1, 2, 3]
--   1 | {0: [12, 22], 1: [13, 23]}     | [4, 5, 6]
--
-- Tables u, v and w have a struct column s ROW(f BIGINT, g BIGINT):
--   u: s = (1, 10), (2, 20)
--   v: s = (3, 30) with k = 0, (4, 40) with k = 1
--   w: s = (5, 50) with k = 5
-- Table pair has struct columns a = (1, 10) and b = (2, 20) of the same type,
-- and table gate has one row with k = 1.
--
-- Graph g has persons 1, 2 and 3, and edges 1 -> 2 and 2 -> 3 of table knows
-- with a struct property p ROW(f BIGINT, g BIGINT) and an array property tags:
--   1 -> 2: p = (12, 120), tags = [12, 120]
--   2 -> 3: p = (23, 230), tags = [23, 230]
--
-- The scan reads only the parts of a column the query uses. DuckDB's map
-- subscript returns a list of the matching values, so map queries compare
-- against an extra [1].

-- An element of the value at a constant map key.
-- duckdb: SELECT k, m[1][1][2] FROM t
SELECT k, m[1][2] FROM t
----
-- An element of the value at a key computed per row.
-- duckdb: SELECT k, m[k][1][1] FROM t
SELECT k, m[k][1] FROM t
----
-- One map read by a constant key and by a computed key.
-- duckdb: SELECT k, m[0][1][1], m[k][1][1] FROM t
SELECT k, m[0][1], m[k][1] FROM t
----
-- One map read by a constant key and by a computed key, at different elements.
-- duckdb: SELECT k, m[0][1][2], m[k][1][1] FROM t
SELECT k, m[0][2], m[k][1] FROM t
----
-- An array element by index.
SELECT k, a[2] FROM t
----
-- An array element counted from the end.
-- duckdb: SELECT k, a[-1] FROM t
SELECT k, element_at(a, -1) FROM t
----
-- A scalar subquery returns its row whole, though its filter reads one field.
SELECT (SELECT s FROM u WHERE s.f = 1)
----
-- Later iterations of a recursive CTE return a field the step does not read
-- when the step builds its rows in a union.
WITH RECURSIVE r(x, d) AS (
  SELECT s, CAST(0 AS BIGINT) FROM u
  UNION ALL
  SELECT y, e
  FROM (
    SELECT v.s AS y, r.d + 1 AS e
    FROM r
    JOIN v ON r.d = v.k
    WHERE r.d < 2
    UNION ALL
    SELECT s, k FROM w WHERE k < 0
  )
  WHERE y.f > 0
)
SELECT x.g, d FROM r
----
-- Later iterations of a recursive CTE return the fields the query reads when
-- the step swaps two state columns behind a LEFT join.
WITH RECURSIVE r(x, y, d) AS (
  SELECT a, b, CAST(0 AS BIGINT) FROM pair
  UNION ALL
  SELECT step.y, step.x, step.d + 1
  FROM (
    SELECT q.x, q.y, q.d
    FROM gate
    LEFT JOIN (
      SELECT x, y, d
      FROM r
      WHERE d < 1 AND y.g > 0
    ) q ON gate.k = q.d + 1
    WHERE coalesce(q.d, -1) >= 0
  ) step
)
SELECT x.f, y.g, d FROM r
----
-- Later iterations of a recursive CTE return the field the query reads when a
-- join in the step moves a state column to another position.
WITH RECURSIVE r(x, y) AS (
  SELECT u.s AS x, w.s AS y FROM u, w
  UNION ALL
  SELECT v.s, r.x
  FROM v
  JOIN r ON v.k = r.x.f
)
SELECT y.g FROM r
----
-- A walk collects a struct edge property of each hop into an array.
-- duckdb: VALUES (12), (23), (12)
WITH ${graph}
SELECT ps[1].f
FROM GRAPH_TABLE (g
  MATCH (a IS Person) -[k IS Knows]->{1,2} (b IS Person)
  COLUMNS (k.p AS ps))
----
-- A walk collects an array edge property of each hop into an array, and each
-- element holds the hop's whole array.
-- duckdb: VALUES (ARRAY[12, 120]), (ARRAY[23, 230]), (ARRAY[12, 120])
WITH ${graph}
SELECT ts[1]
FROM GRAPH_TABLE (g
  MATCH (a IS Person) -[k IS Knows]->{1,2} (b IS Person)
  COLUMNS (k.tags AS ts))
----
-- An optional subpath returns an element of an array edge property, and NULL
-- for each path that skips the subpath.
-- duckdb: VALUES (NULL), (12), (NULL), (23), (NULL)
WITH ${graph}
SELECT tag
FROM GRAPH_TABLE (g
  MATCH (a IS Person)
    ((x IS Person) -[e IS Knows]-> (m IS Person))? (b IS Person)
  COLUMNS (e.tags[1] AS tag))
