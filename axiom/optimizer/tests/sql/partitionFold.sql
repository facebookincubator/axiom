-- connector: hive
-- setup
CREATE TABLE t (
  a BIGINT,
  b BIGINT,
  c BIGINT,
  ds VARCHAR,
  k BIGINT
) WITH (
  partitioned_by = ARRAY['ds', 'k']
)
----
INSERT INTO t VALUES
  (1, 10, 100, '2025-01-01', 0),
  (2, 20, 200, '2025-01-02', 1),
  (3, 30, 300, '2025-01-03', 0),
  (4, 40, 400, '2025-01-03', 0)
----
CREATE TABLE u (d VARCHAR)
----
INSERT INTO u VALUES ('2025-01-01'), ('2025-01-02'), ('2025-01-03')
-- end_setup

-- An aggregate reading only a partition column is answered from the listing.
SELECT max(ds) FROM t
----
SELECT max(ds), min(ds) FROM t
----
-- DISTINCT reads each partition value once, regardless of its row count.
-- ordered
SELECT DISTINCT ds FROM t ORDER BY ds
----
-- DISTINCT returns no rows when no partition value matches the filter.
-- count 0
SELECT DISTINCT ds FROM t WHERE ds > '2026'
----
-- Grouping and a duplicate-insensitive aggregate are answered together from
-- the partition listing.
-- ordered
SELECT k, max(ds) FROM t GROUP BY k ORDER BY k
----
-- Each UNION ALL branch is answered on its own.
SELECT max(ds) AS m FROM t UNION ALL SELECT min(ds) AS m FROM t
----
-- count() counts rows, not partitions.
SELECT count(*) FROM t
----
-- count 0
SELECT u.d
FROM u
JOIN t ON u.d = t.ds
WHERE t.ds = '1900-01-01'
----
-- A LEFT JOIN preserves its non-empty side when the other side has no
-- matching partitions.
SELECT u.d, t.a
FROM u
LEFT JOIN (
  SELECT a, ds
  FROM t
  WHERE ds = '1900-01-01'
) t ON u.d = t.ds
----
SELECT count(*) FROM t WHERE ds = '1900-01-01'
----
-- An empty UNION ALL branch contributes no rows.
SELECT a FROM t WHERE ds = '1900-01-01'
UNION ALL
SELECT 1 FROM u
----
-- Ranking remains valid when an empty UNION ALL branch is removed.
WITH ranked AS (
  SELECT
    a,
    row_number() OVER (PARTITION BY b) AS rn
  FROM (
    SELECT a, b, c
    FROM t
    WHERE ds = '1900-01-01'
    UNION ALL
    SELECT *
    FROM (VALUES (2, 20, 200), (3, 30, 300)) AS x(a, b, c)
  )
  WHERE c = 200
)
SELECT a FROM ranked WHERE rn <= 2
----
-- Ranking stays correct when removing a branch that matches no partition makes
-- the PARTITION BY and ORDER BY keys one column.
SELECT a, count(*), max(rn)
FROM (
  SELECT a, row_number() OVER (PARTITION BY a ORDER BY b) AS rn
  FROM (
    SELECT k, k FROM t
    UNION ALL
    SELECT k, a FROM t WHERE ds = '1900-01-01'
  ) AS v(a, b)
)
GROUP BY a
----
-- A scalar subquery over no matching partitions produces NULL.
SELECT (SELECT a FROM t WHERE ds = '1900-01-01')
----
-- An empty recursive step leaves the anchor row.
-- error_v1: Fixed-point (recursive) plan execution is not yet implemented
WITH RECURSIVE r(n) AS (
  VALUES (1)
  UNION ALL
  SELECT n + 1
  FROM r
  JOIN t ON t.a = r.n
  WHERE t.ds = '1900-01-01'
)
SELECT n FROM r
----
-- An aggregation whose value is never read still produces its single row.
SELECT count(*) FROM (SELECT max(ds) FROM t)
----
-- The folded value restricts the outer scan.
SELECT a FROM t WHERE ds = (SELECT max(ds) FROM t)
----
-- A filter on a non-partition column is not answerable from the listing.
SELECT a FROM t WHERE ds = (SELECT max(ds) FROM t WHERE a > 1)
----
-- HAVING can reject the aggregate's row, leaving the subquery null, so
-- nothing matches.
-- count 0
SELECT a FROM t WHERE ds = (SELECT max(ds) FROM t HAVING max(ds) > '2030-01-01')
----
-- A correlated predicate reads a column the listing does not have.
SELECT d, (SELECT max(ds) FROM t WHERE t.ds > u.d) AS m FROM u
