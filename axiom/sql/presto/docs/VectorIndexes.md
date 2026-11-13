# Vector Indexes

A vector index finds the rows of a table whose embeddings are nearest to a
query embedding. It belongs to a table and indexes one of its `ARRAY(REAL)`
columns. Axiom adds statements to add, build and drop indexes, and the
`vector_search` table function to search through them. The connector decides
how an index is stored; see
[connectors/hive/README.md](../../../connectors/hive/README.md#vector-indexes)
for the local Hive connector. The design is in
[VectorSearch.md](../../../docs/VectorSearch.md).

## Adding and dropping indexes

`CREATE TABLE` declares indexes as elements, and `ALTER TABLE` adds and drops
them:

```sql
CREATE TABLE products (
    id BIGINT,
    embedding ARRAY(REAL),
    ds VARCHAR,
    PRIMARY KEY (id),
    VECTOR INDEX product_index (embedding) WITH (
        distance_metric = 'cosine',
        index_type = 'flat',
        num_clusters = 1000,
        dimension = 768))
WITH (partitioned_by = ARRAY['ds'])

ALTER TABLE products ADD VECTOR INDEX product_ivf (embedding) KEY (id)
WITH (distance_metric = 'cosine', index_type = 'ivf_flat',
      num_clusters = 1000, dimension = 768)

ALTER TABLE products DROP VECTOR INDEX product_ivf
```

The syntax:

```
VECTOR INDEX name (column) [KEY (column, ...)] [WITH (property = value, ...)]

ALTER TABLE [IF EXISTS] table ADD VECTOR INDEX [IF NOT EXISTS]
    name (column) [KEY (column, ...)] [WITH (property = value, ...)]

ALTER TABLE [IF EXISTS] table DROP VECTOR INDEX [IF EXISTS] name
```

The first is an element of `CREATE TABLE`, alongside the columns.

- `KEY` names the columns a search returns for each match, which find the row
  it came from. It defaults to the table's primary key, must name the primary
  key when the table has one, and is required when the table has none. For
  now, the key must be one `BIGINT` column.
- A table may have several indexes, on the same column too. Index names are
  unique within the table.
- The index's partitions are the table's. A partition column may not be named
  `codec`, `codec_id`, `cluster_id`, `group_id`, `local_index` or `centroid`:
  the index's storage uses these names.

The index's properties:

- `distance_metric`, required: `cosine`, the only metric supported so far.
  The metric decides which embeddings are nearest, so an index answers
  searches under the metric it was built with.
- `index_type`, required: `flat`, `ivf_flat`, `ivf_sq` or `hnsw_sq`.
- `num_clusters`, required: the number of clusters each partition's
  embeddings are grouped into, a positive integer.
- `dimension`, required: the length of every embedding, a positive integer.
- `codec_policy`, optional: `per_partition` (the default), where each refresh
  trains its own clusters, or `shared`, where later refreshes reuse the
  clusters of the latest partition.
- `index_options`, optional: a `MAP(VARCHAR, VARCHAR)` of local index
  settings, listed below.

Any other property applies to the tables that store the index, as the
connector defines. For example, Hive's `bucket_count` sets the number of buckets
of the table of local indexes.

The local index settings take positive integer values. Each applies only to
the index types listed. An unset setting takes a default that depends on the
number of embeddings in the local index. A search setting set on the index is
the default for searches, which a `vector_search` hint overrides.

| Setting | Index types | Used by | Meaning |
|---|---|---|---|
| `ivf.nlist` | `ivf_flat`, `ivf_sq` | build | The number of lists of the IVF index. |
| `ivf.nprobe` | `ivf_flat`, `ivf_sq` | search | The number of lists a search scans. Defaults to all of them. |
| `hnsw.M` | `hnsw_sq` | build | The number of neighbors of each embedding in the graph, from 2 to 256. |
| `hnsw.efConstruction` | `hnsw_sq` | build | The number of candidates a build explores. |
| `hnsw.efSearch` | `hnsw_sq` | search | The number of candidates a search explores. |

`SHOW CREATE TABLE` lists the table's indexes with their key and the index's
own properties. It shows `codec_policy` even when the index takes the default,
and `index_options` only with the settings the index sets. For the `products`
table above, its output has this element:

```
   VECTOR INDEX product_index (embedding) KEY (id) WITH (
      codec_policy = 'per_partition',
      dimension = 768,
      distance_metric = 'cosine',
      index_type = 'flat',
      num_clusters = 1000
   )
```

`DESCRIBE` names the indexes of each column in its `extra` field:

```
 column    | type        | extra                      | comment
-----------+-------------+----------------------------+---------
 id        | BIGINT      |                            |
 embedding | ARRAY<REAL> | vector index product_index |
 ds        | VARCHAR     | partition key              |
```

## REFRESH VECTOR INDEX

```sql
REFRESH VECTOR INDEX product_index ON products WHERE ds = '2026-10-31'
```

Builds the index for the partitions of the table the condition selects. The
refresh is one index write, which runs two plans in order:

1. For each partition, trains a codec: k-means over a sample of the
   partition's rows, which finds the clusters.
2. Assigns each row to the nearest cluster of its partition's codec, and
   builds a local index for each cluster.

The result is the number of embeddings written, summed over the local indexes.
For a partition of 30 valid rows and 3 clusters, the refresh writes 3 local
indexes holding 30 embeddings:

```
 rows
------
   30
```

`EXPLAIN` shows both plans; `EXPLAIN ANALYZE` runs the refresh and shows both
plans with their runtime stats:

```sql
EXPLAIN ANALYZE REFRESH VECTOR INDEX product_index ON products WHERE ds = '2026-10-31'
```

<details>
<summary>Output (abridged)</summary>

```
Train plan:
Fragment 1: SINGLE:
-- TableWrite[11][hive, codec, ds, codec_id, ...] -> rows:BIGINT, ...
   Output: 3 rows (155.10KB, 2 batches), Cpu time: 5.94ms, Wall time: 6.03ms, Peak memory: 1.00MB, ...
  ...
        -- Aggregation[7][SINGLE [p0] codec := vector_index_train("embedding","id",3,cosine)] -> p0:VARCHAR, codec:VECTOR_INDEX
           Output: 1 rows (431.72KB, 1 batches), Cpu time: 11.98ms, Wall time: 15.46ms, Peak memory: 757.38KB, ...
          ...
            -- TopNRowNumber[5][row_number order by (__p5 ASC NULLS LAST) limit 768] -> id:BIGINT, embedding:ARRAY<REAL>, __p5:VARBINARY
               Output: 30 rows (383.91KB, 1 batches), Cpu time: 1.87ms, Wall time: 1.89ms, Peak memory: 576.62KB, ...
              ...

Build plan:
Fragment 1: SINGLE:
-- TableWrite[14][hive, codec_id, cluster_id, group_id, local_index, ds, ...] -> rows:BIGINT, ...
   Output: 3 rows (196.50KB, 2 batches), Cpu time: 35.95ms, Wall time: 40.35ms, Peak memory: 1.00MB, ...
  ...
      -- Aggregation[11][SINGLE [cluster_id_6, group_id] local_index := vector_index_build("embedding","id",flat,cosine,<empty>)] -> cluster_id_6:BIGINT, group_id:BIGINT, local_index:VECTOR_INDEX
         Output: 3 rows (431.62KB, 1 batches), Cpu time: 1.91ms, Wall time: 1.94ms, Peak memory: 950.12KB, ...
        ...
                -- VectorSearchJoin[6][LEFT embedding nearest in codec19 (faiss, cosine), k: 1] -> id:BIGINT, embedding:ARRAY<REAL>, __query_id22:BIGINT, __cluster_id23:BIGINT, __score24:REAL
                   Output: 30 rows (1.62KB, 1 batches), Cpu time: 1.25ms, Wall time: 1.27ms, Peak memory: 3.62KB, ...
                  ...
```

</details>

The refresh skips a row whose key is `NULL`, or whose embedding is `NULL`, has
a `NULL`, `NaN` or infinite element, is all zeros, or has a length other than
`dimension`. A valid row with a `NULL` partition value fails the refresh.

Not supported yet:

- Refreshing a partition the index already covers.
- An index with `codec_policy = 'shared'`.
- An index of a table with no partition columns.

## vector_search

The `vector_search` table function finds the rows of a table nearest to each
query embedding, through one of its vector indexes.

```sql
SELECT q.name, p.id, similarity
FROM TABLE(vector_search(
    queries => TABLE(SELECT name, embedding FROM new_products) AS q,
    corpus => TABLE(SELECT * FROM products WHERE ds = '2026-09-30') AS p,
    embeddings => DESCRIPTOR(embedding),
    metric => 'cosine',
    k => 10,
    score => DESCRIPTOR(similarity),
    hints => MAP(ARRAY['num_probes'], ARRAY['50'])))
```

Its arguments are named:

- `queries`: a table of the embeddings to search for. Or `query`: one
  `ARRAY(REAL)` expression.
- `corpus`: the table to search, `TABLE(name)`, or
  `TABLE(SELECT * FROM name WHERE <predicate>)` to search only the partitions
  the predicate selects. The predicate may use only partition columns.
- `embeddings`: the columns to compare, `DESCRIPTOR(query column, corpus
  column)`. `DESCRIPTOR(column)` names a column both sides have, or only the
  corpus's column with `query`.
- `metric`: how to compare embeddings, such as `'cosine'`. The search reads an
  index on the corpus's column with this metric.
- `k`: a positive integer literal, the number of matches per query.
- `score`, optional: `DESCRIPTOR(column)` adds a `REAL` column with each match's
  score under that name. A larger score is nearer.
- `hints`, optional: how the search runs, a `MAP(ARRAY[...], ARRAY[...])` of
  `VARCHAR` literals:
  - `index`: the index to search. Without it, the first by name of the corpus's
    indexes on the column with the metric.
  - `num_probes`: the number of clusters nearest to each query to search in
    each partition. Defaults to a tenth of the clusters.
  - the search settings of the index's local indexes, `ivf.nprobe` and
    `hnsw.efSearch` (see the settings above).

A table argument may have an alias, as in `TABLE(new_products) AS q`, and
column names, which rename its columns by position and must name all of them:
`TABLE(SELECT name, embedding FROM new_products) AS q(product, embedding)`.

The result has the score, if requested, then the queries' columns and the
corpus's columns: up to `k` rows per query. Each column of a table argument
keeps its alias as its qualifier, or the table's name for `TABLE(name)`. A name
both sides have resolves qualified, as in a join: `q.ds` and `p.ds`. A query
may read only the corpus's key and partition columns, which the index returns;
reading other columns is not supported yet.

The search is approximate: larger hint values read more of the index and return
results closer to the exact nearest rows. A query with a `NULL` embedding
matches nothing. A search fails if it reads a partition the index does not
cover yet.

The corpus may also be an index's clusters, its centroids table, named after
the table and the index: `TABLE("products$product_index$centroids")`. It has a
row per cluster, with the partition columns, `codec_id`, `cluster_id` and
`centroid`. A search compares embeddings with `centroid` by the index's metric
and returns the other columns.
