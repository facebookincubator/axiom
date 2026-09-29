-- connector: hive
-- setup
CREATE TABLE t WITH (bucket_count = 8, bucketed_by = ARRAY['k']) AS
  SELECT * FROM (VALUES
    (1, 100, 10),
    (2, 200, 20),
    (3, 300, 30),
    (1, 100, 40),
    (2, 200, 50),
    (3, 300, 60),
    (4, 400, 70),
    (5, 500, 80),
    (1, 100, 90))
  AS _(k, j, v)
----
CREATE TABLE u WITH (bucket_count = 16, bucketed_by = ARRAY['k']) AS
  SELECT * FROM (VALUES
    (1, 100, 10),
    (2, 200, 20),
    (3, 300, 30),
    (4, 400, 70),
    (5, 500, 80))
  AS _(k, j, v)
----
CREATE TABLE v WITH (bucket_count = 8, bucketed_by = ARRAY['j']) AS
  SELECT * FROM (VALUES
    (1, 100, 11),
    (2, 200, 22),
    (3, 300, 33),
    (4, 400, 44))
  AS _(k, j, v)
----
CREATE TABLE b16 WITH (bucket_count = 16, bucketed_by = ARRAY['k']) AS
  SELECT * FROM (VALUES
    (1, 1),
    (2, 2),
    (3, 3),
    (4, 4),
    (5, 5),
    (6, 6),
    (7, 7),
    (8, 8),
    (9, 9),
    (10, 10),
    (11, 11),
    (12, 12),
    (13, 13),
    (14, 14),
    (15, 15),
    (16, 16),
    (17, 17),
    (18, 18),
    (19, 19),
    (20, 20))
  AS _(k, v)
----
CREATE TABLE b2 WITH (bucket_count = 2, bucketed_by = ARRAY['k']) AS
  SELECT k, v * 100 AS m FROM b16
----
CREATE TABLE plain AS
  SELECT k, v AS p FROM b16
-- end_setup

-- Grouping on a non-bucket key: the bucketing cannot help.
SELECT j, count(*), sum(v) FROM t GROUP BY j
----
-- Two tables bucketed the same way on the join key, counts 8 and 16 (one
-- divides the other), so the join can pair them without a shuffle.
SELECT a.k, a.v, b.v FROM t a JOIN u b ON a.k = b.k
----
-- Only one side is bucketed on the join key: the other's bucketing is on a
-- different column and cannot be used to pair them.
SELECT a.k, a.v, c.v FROM t a JOIN v c ON a.k = c.k
----
-- Two join keys where each side is bucketed on only one of them.
SELECT a.v, c.v FROM t a JOIN v c ON a.k = c.k AND a.j = c.j
----
-- Aggregation above a join, grouping on the probe's bucket key.
SELECT a.k, count(*) FROM t a JOIN u b ON a.k = b.k GROUP BY a.k
----
-- A right join over co-bucketed inputs feeds a left join on its preserved
-- right key, followed by grouping on that key. The second join condition also
-- reads the first join's null-supplying side.
SELECT preserved.k, count(parent.k)
FROM u matching
RIGHT JOIN u preserved ON matching.k = preserved.k
LEFT JOIN b16 parent
  ON preserved.k = parent.k
  AND (matching.k IS NULL OR matching.v > parent.v)
GROUP BY preserved.k
----
-- Every leg of a union bucketed the same way.
SELECT k, count(*) FROM (SELECT k FROM t UNION ALL SELECT k FROM u) GROUP BY k
----
-- One leg bucketed on a different column: the union cannot be read grouped.
SELECT k, count(*) FROM (SELECT k FROM t UNION ALL SELECT j AS k FROM v) GROUP BY k

----
-- Bucket counts that coarsen to different widths, joined with an unbucketed
-- table on the same key.
SELECT b16.k, b2.m, count(*) FROM b16, b2, plain
  WHERE b16.k = b2.k AND b16.k = plain.k
  GROUP BY b16.k, b2.m

----
-- Existence join on the bucket key.
SELECT k, v FROM t WHERE k IN (SELECT k FROM u)
----
-- Anti join on the bucket key.
SELECT k, v FROM t WHERE k NOT IN (SELECT k FROM u WHERE k < 3)
----
-- count(DISTINCT) grouped by the bucket key.
SELECT k, count(DISTINCT v) FROM t GROUP BY k
----
-- HAVING over an aggregation on the bucket key.
SELECT k, count(*) FROM t GROUP BY k HAVING count(*) > 1
----
-- Window partitioned by the bucket key.
SELECT k, v, row_number() OVER (PARTITION BY k ORDER BY v) AS rn FROM t

----
-- Aggregation over the wider table, joined with a table bucketed on the same
-- key but into fewer buckets.
SELECT x.k, x.cnt, b2.m FROM
  (SELECT k, count(*) AS cnt FROM b16 GROUP BY k) x, b2
  WHERE x.k = b2.k
