-- connector: hive
-- setup
-- Vectors at the given angles, in degrees: two groups of three in each
-- partition. A vector's length is its id + 1, so the search finds the
-- nearest directions only if it ignores length.
CREATE TABLE points WITH (partitioned_by = ARRAY['ds']) AS
  SELECT CAST(id AS BIGINT) AS id,
      ARRAY[CAST((id + 1) * cos(radians(angle)) AS REAL),
          CAST((id + 1) * sin(radians(angle)) AS REAL)] AS embedding,
      ds
  FROM (VALUES
    -- 2026-01-01
    (0,  0,   '2026-01-01'),
    (1,  10,  '2026-01-01'),
    (2,  20,  '2026-01-01'),
    (3,  180, '2026-01-01'),
    (4,  190, '2026-01-01'),
    (5,  200, '2026-01-01'),
    -- 2026-01-02
    (10, 90,  '2026-01-02'),
    (11, 100, '2026-01-02'),
    (12, 110, '2026-01-02'),
    (13, 270, '2026-01-02'),
    (14, 280, '2026-01-02'),
    (15, 290, '2026-01-02'))
  AS _(id, angle, ds)
----
ALTER TABLE points ADD VECTOR INDEX v (embedding) KEY (id)
  WITH (distance_metric = 'cosine', index_type = 'flat', num_clusters = 2,
        dimension = 2)
----
REFRESH VECTOR INDEX v ON points WHERE ds IN ('2026-01-01', '2026-01-02')
----
-- Embeddings to search for, named by their direction: 0 and 90 degrees.
CREATE TABLE queries AS
  SELECT * FROM (VALUES
    ('east', ARRAY[REAL '1', REAL '0']),
    ('north', ARRAY[REAL '0', REAL '1']))
  AS _(name, embedding)
----
-- 2,000 unit vectors in one partition, at 0.1 degree steps in two groups:
-- even ids from 0 degrees up, odd ids from 180 degrees up. Enough rows that
-- codec training uses a sample.
CREATE TABLE many_points WITH (partitioned_by = ARRAY['ds']) AS
  SELECT id,
      ARRAY[CAST(cos(radians(angle)) AS REAL),
          CAST(sin(radians(angle)) AS REAL)] AS embedding,
      '2026-01-03' AS ds
  FROM (
    WITH digits AS (
      SELECT * FROM (VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9))
      AS _(d))
    SELECT CAST(2 * (t.d + 10 * u.d + 100 * v.d) + g AS BIGINT) AS id,
        g * 180e0 + (t.d + 10 * u.d + 100 * v.d) * 1e-1 AS angle
    FROM digits AS t, digits AS u, digits AS v, (VALUES (0), (1)) AS w(g))
----
ALTER TABLE many_points ADD VECTOR INDEX w (embedding) KEY (id)
  WITH (distance_metric = 'cosine', index_type = 'ivf_flat', num_clusters = 2,
        dimension = 2, index_options = MAP(ARRAY['ivf.nlist'], ARRAY['4']))
----
REFRESH VECTOR INDEX w ON many_points WHERE ds = '2026-01-03'
-- end_setup

-- The two points of partition '2026-01-01' nearest to 0 degrees.
-- duckdb: VALUES (0), (1)
SELECT id
FROM TABLE(vector_search(
  query => ARRAY[REAL '1', REAL '0'],
  corpus => TABLE(SELECT * FROM points WHERE ds = '2026-01-01'),
  embeddings => DESCRIPTOR(embedding),
  metric => 'cosine',
  k => 2,
  hints => MAP(ARRAY['num_probes'], ARRAY['2'])))
----
-- Each query's nearest point across both partitions.
-- duckdb: VALUES ('east', 0, '2026-01-01'), ('north', 10, '2026-01-02')
SELECT name, id, ds
FROM TABLE(vector_search(
  queries => TABLE(queries),
  corpus => TABLE(points),
  embeddings => DESCRIPTOR(embedding),
  metric => 'cosine',
  k => 1,
  hints => MAP(ARRAY['num_probes'], ARRAY['2'])))
----
-- The four points nearest to 45 degrees come from both partitions: 20, 10
-- and 0 degrees in '2026-01-01', and 90 degrees in '2026-01-02'.
-- duckdb:
--   VALUES
--     (2, '2026-01-01'),
--     (1, '2026-01-01'),
--     (0, '2026-01-01'),
--     (10, '2026-01-02')
SELECT id, ds
FROM TABLE(vector_search(
  query => ARRAY[REAL '1', REAL '1'],
  corpus => TABLE(points),
  embeddings => DESCRIPTOR(embedding),
  metric => 'cosine',
  k => 4,
  hints => MAP(ARRAY['num_probes'], ARRAY['2'])))
----
-- The four points of partition '2026-01-01' nearest to 135 degrees come from
-- both of its groups: 180, 190 and 200 degrees, then 20 degrees.
-- duckdb: VALUES (3), (4), (5), (2)
SELECT id
FROM TABLE(vector_search(
  query => ARRAY[REAL '-1', REAL '1'],
  corpus => TABLE(SELECT * FROM points WHERE ds = '2026-01-01'),
  embeddings => DESCRIPTOR(embedding),
  metric => 'cosine',
  k => 4,
  hints => MAP(ARRAY['num_probes'], ARRAY['2'])))
----
-- With one probe, each search reads only the cluster nearest to its query,
-- which holds the query's three nearest points.
-- duckdb:
--   VALUES
--     ('east', 0),
--     ('east', 1),
--     ('east', 2),
--     ('west', 3),
--     ('west', 4),
--     ('west', 5)
SELECT name, id
FROM TABLE(vector_search(
  queries => TABLE(
    SELECT * FROM (VALUES
      ('east', ARRAY[REAL '1', REAL '0']),
      ('west', ARRAY[REAL '-1', REAL '0']))
    AS _(name, embedding)),
  corpus => TABLE(SELECT * FROM points WHERE ds = '2026-01-01'),
  embeddings => DESCRIPTOR(embedding),
  metric => 'cosine',
  k => 3,
  hints => MAP(ARRAY['num_probes'], ARRAY['1'])))
----
-- Equal queries each get their own k matches. A query with a NULL embedding
-- matches nothing.
-- duckdb: VALUES ('east', 0), ('east', 0)
SELECT name, id
FROM TABLE(vector_search(
  queries => TABLE(
    SELECT * FROM (VALUES
      ('east', ARRAY[REAL '1', REAL '0']),
      ('east', ARRAY[REAL '1', REAL '0']),
      ('none', CAST(NULL AS ARRAY(REAL))))
    AS _(name, embedding)),
  corpus => TABLE(SELECT * FROM points WHERE ds = '2026-01-01'),
  embeddings => DESCRIPTOR(embedding),
  metric => 'cosine',
  k => 1,
  hints => MAP(ARRAY['num_probes'], ARRAY['2'])))
----
-- Each query's nearest point in partition '2026-01-02'.
-- duckdb: VALUES ('east', 15), ('north', 10)
SELECT name, id
FROM TABLE(vector_search(
  queries => TABLE(queries),
  corpus => TABLE(SELECT * FROM points WHERE ds = '2026-01-02'),
  embeddings => DESCRIPTOR(embedding),
  metric => 'cosine',
  k => 1,
  hints => MAP(ARRAY['num_probes'], ARRAY['2'])))
----
-- Filters on a query column and on a result column: of the three points
-- nearest to 'north', 10, 11 and 12, the ones after 10.
-- duckdb: VALUES ('north', 11), ('north', 12)
SELECT name, id
FROM TABLE(vector_search(
  queries => TABLE(queries),
  corpus => TABLE(points),
  embeddings => DESCRIPTOR(embedding),
  metric => 'cosine',
  k => 3,
  hints => MAP(ARRAY['num_probes'], ARRAY['2'])))
WHERE name = 'north' AND id > 10
----
-- Each query's nearest point has its direction, with a score of 1. Without
-- a num_probes hint, the search reads the default number of clusters.
-- duckdb: VALUES ('east', 0, 1.0::DOUBLE), ('north', 10, 1.0::DOUBLE)
SELECT name, id, CAST(round(score, 3) AS DOUBLE)
FROM TABLE(vector_search(
  queries => TABLE(queries),
  corpus => TABLE(points),
  embeddings => DESCRIPTOR(embedding),
  metric => 'cosine',
  k => 1,
  score => DESCRIPTOR(score)))
----
-- A search of the clusters of both partitions: each query's nearest cluster. A
-- local index setting does not apply to the clusters.
-- duckdb: VALUES ('east', '2026-01-01'), ('north', '2026-01-02')
SELECT name, ds FROM TABLE(vector_search(
  queries => TABLE(queries),
  corpus => TABLE("points$v$centroids"),
  embeddings => DESCRIPTOR(embedding, centroid),
  metric => 'cosine',
  k => 1,
  hints => MAP(ARRAY['ivf.nprobe'], ARRAY['1'])))
----
-- A search of an IVF index whose codec was trained on a sample: the three
-- points nearest to 0 degrees. Probing all of a local index's lists makes the
-- search exact within the cluster.
-- duckdb: VALUES (0), (2), (4)
SELECT id
FROM TABLE(vector_search(
  query => ARRAY[REAL '1', REAL '0'],
  corpus => TABLE(many_points),
  embeddings => DESCRIPTOR(embedding),
  metric => 'cosine',
  k => 3,
  hints => MAP(ARRAY['num_probes', 'ivf.nprobe'], ARRAY['1', '4'])))
