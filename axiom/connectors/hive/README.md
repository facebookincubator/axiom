# Hive Connector

## Overview

The Hive connector provides access to data stored in Hive-style directory
layouts with Parquet, DWRF, or TEXT (including CSV) files.

The current implementation (`LocalHiveConnectorMetadata`) reads and writes
files on the local filesystem. It is designed for testing and development.
Production implementations are expected to connect to a metadata server.

## Capabilities

The local Hive connector supports reading and writing tables stored as files in
a local directory.

**Reads:**
- Scan Parquet, DWRF, or TEXT (including CSV) files.
- Hive-style partitioning: partition column values are encoded in directory
  names (e.g. `ds=2024-01-01/`).
- Hash bucketing and sort order within buckets.
- Hidden columns available for explicit queries (not included in `SELECT *`):
  - `$path` — file path of the data file.
  - `$bucket` — bucket number for bucketed tables.
  - `$file_size` — size of the data file in bytes.
  - `$row_number` — zero-based position of the row within its file.
  - `$row_group_id` — name of the file the row was read from.
  - `$row_id` — the two above plus the identity of the table and partition
    version they are relative to, as one struct. Used for row-level deletes.
    This connector has no versioning or partition ids, so it fills those fields
    with zeroes and the table name.
- Filter pushdown: the optimizer pushes filters down to the connector, which
  evaluates them during scan. This includes partition pruning (skipping
  entire files based on partition key values) and within-file filtering.
- Table and column statistics for the optimizer (see [Statistics](#statistics)).
- Discrete predicate (partition value) listing: the optimizer lists partition
  key values to constant-fold aggregations over partition columns (e.g.
  `SELECT max(ds) FROM t`) without scanning data. Filters on partition columns
  narrow the listing. The `hive_max_partitions_per_discrete_predicates` session
  property caps how many partitions a single listing may return.

**Writes:**
- `CREATE TABLE` with partitioning, bucketing, sort order, file format, and
  compression options specified via a WITH clause.
- `CREATE TABLE AS SELECT`.
- `INSERT INTO`.
- `DROP TABLE`.
- Vector indexes: `VECTOR INDEX` elements of `CREATE TABLE`, and
  `ALTER TABLE ADD/DROP VECTOR INDEX`.
- Per-column statistics (`.stats` files) are persisted on write for use by
  the optimizer.

**Not supported:**
- Transactions. Writes go directly into the table directory.
- `ALTER TABLE`, `RENAME TABLE`.

## Usage

Create a directory for storing tables and launch the CLI with `--data_path`
pointing to it. The directory can be empty; tables will be created in it via
SQL statements. See the [CLI README](../../cli/README.md) for how to launch
`axiom_sql`.

```bash
mkdir -p /tmp/my_data
axiom_sql --data_path /tmp/my_data
```

The Hive connector becomes the default catalog. Use `--data_format` to specify
the file format (`parquet`, `dwrf`, or `text`; defaults to `parquet`).

```sql
-- Create a new table from TPC-H data.
SQL> CREATE TABLE t AS SELECT * FROM tpch.tiny.lineitem;

-- Query the table.
SQL> SELECT count(*) FROM t;

-- Append more data.
SQL> INSERT INTO t SELECT * FROM tpch.sf1.lineitem;

-- Drop a table.
SQL> DROP TABLE t;
```

### Generating TPC-H Data

Use the `tpchgen` tool to generate all 8 TPC-H tables as Parquet or DWRF files.

| Flag | Default | Description |
|------|---------|-------------|
| `--data_path` | | Output directory (required). Created if it doesn't exist. |
| `--sf` | `0.1` | TPC-H scale factor (e.g. `0.1`, `1`, `10`). |
| `--data_format` | `parquet` | File format: `parquet` or `dwrf`. |
| `--compression` | `none` | Compression: `none`, `snappy`, `zlib`, `zstd`, `lz4`, `gzip`. |

The example below generates data at scale factor 0.1 (~87K total rows) in
Parquet format with Snappy compression:

```bash
axiom_tpchgen \
    --data_path /tmp/tpch/sf0.1 --sf 0.1 \
    --data_format parquet --compression snappy
```

Then point the CLI at the generated directory:

```bash
axiom_sql --data_path /tmp/tpch/sf0.1
```

## Directory Structure

Each subdirectory under the data path is interpreted as a table. For
unpartitioned tables, data files are placed directly in the table directory.
For partitioned tables, data files are organized into subdirectories named
`<partition_column>=<value>` (e.g. `ds=2024-01-01/`), nesting one directory
level per partition column in the order declared in `.schema` (e.g.
`ds=2024-01-01/hour=00/` for a table partitioned by `(ds, hour)`):

```
/data/
├── orders/
│   ├── .schema              # schema and table properties
│   ├── .stats               # persisted write-time statistics
│   ├── file1.parquet
│   └── file2.parquet
├── lineitem/
│   ├── .schema
│   ├── ds=2024-01-01/       # Hive partition directories
│   │   ├── .stats           # per-partition statistics
│   │   └── data.parquet
│   └── ds=2024-01-02/
│       ├── .stats
│       └── data.parquet
└── users/
    ├── .schema               # required: schema and table properties
    ├── .stats                # required: persisted statistics
    └── data.parquet
```

Every table directory must contain a `.schema` file with column definitions,
partition and bucketing information, and file format. A `.stats` file with
row counts and per-column statistics is also required. Both files are produced
automatically by the write pipeline (`CREATE TABLE`, `INSERT INTO`) or by
`LocalTableBuilder::build()` for externally created data.

The `.schema` is the source of truth for partitioning. At load time the
connector descends exactly one directory level per declared partition column,
in declared order, and reads the leaf `.stats` file for each partition. A
directory whose name does not match the expected partition column, or a leaf
partition missing its `.stats` file, fails the load.

## .schema File Format

The `.schema` file is JSON with the following structure:

```json
{
  "dataColumns": [
    {"name": "orderkey", "type": "bigint"},
    {"name": "custkey", "type": "bigint"},
    {"name": "totalprice", "type": "double"},
    {"name": "comment", "type": "string"}
  ],
  "partitionColumns": [
    {"name": "ds", "type": "string"}
  ],
  "fileFormat": "PARQUET",
  "compressionKind": "ZSTD",
  "bucketProperty": {
    "bucketCount": "16",
    "bucketedBy": ["orderkey"],
    "sortedBy": ["orderkey"]
  },
  "serdeOptions": {
    "fieldDelim": ",",
    "nullString": "\\N"
  }
}
```

All fields except `dataColumns` are optional:

**`dataColumns`** — Array of `{name, type}` objects. Types use Velox's JSON
serialization format, e.g. `{"name": "Type", "type": "BIGINT"}` for scalars,
or nested objects with `"children"` for complex types like arrays and maps.

**`partitionColumns`** — Array of `{name, type}` objects for Hive partition
columns. Must correspond to the directory structure.

**`fileFormat`** — `PARQUET`, `DWRF`, or `TEXT`. Defaults to the `--data_format`
CLI flag.

**`compressionKind`** — `NONE`, `ZSTD` (default), `SNAPPY`, `GZIP`, `LZ4`,
`ZLIB`, `LZO`.

**`bucketProperty`** — Bucketing configuration:

- `bucketCount` (required) — number of buckets, must be a power of 2.
- `bucketedBy` (required) — array of bucketing column names.
- `sortedBy` (optional) — array of sort column names within each bucket.

**`serdeOptions`** — Serialization options for TEXT format:

- `fieldDelim` — field delimiter, a single character.
- `nullString` — string representation of null values.

## .stats File Format

The `.stats` file is JSON with per-table or per-partition statistics. It is
written by `INSERT INTO` and `CREATE TABLE AS SELECT` and read at load time.

For unpartitioned tables, a single `.stats` file is placed in the table
directory. For partitioned tables, each partition directory has its own
`.stats` file.

```json
{
  "numRows": 60175,
  "columns": [
    {
      "name": "orderkey",
      "numValues": 60175,
      "nullPct": 0,
      "min": {"type": "BIGINT", "value": 1},
      "max": {"type": "BIGINT", "value": 600000},
      "numDistinct": 60175
    },
    {
      "name": "comment",
      "numValues": 60175,
      "nullPct": 0,
      "maxLength": 79,
      "avgLength": 32
    }
  ]
}
```

**`numRows`** — Total number of rows.

**`columns`** — Array of per-column statistics:

- `name` (required) — column name.
- `numValues` (required) — count of non-null values.
- `nullPct` (required) — percentage of null values.
- `min`, `max` (optional) — minimum and maximum values, serialized as Velox
  Variants.
- `numDistinct` (optional) — estimated number of distinct values.
- `maxLength`, `avgLength` (optional) — for variable-length types.

When multiple writes append to the same table or partition, stats are merged:
row counts and value counts are summed, min/max are updated, and NDV is
approximated as the max across writes.

## CREATE TABLE Options

Table properties are specified via a WITH clause. The following options are
supported:

| Option | Description |
|--------|-------------|
| `partitioned_by` | Array of Hive partition column names, e.g. `ARRAY['ds']`. Partition columns must be listed last in the column definition. |
| `bucketed_by` | Array of bucketing column names, e.g. `ARRAY['key']`. Requires `bucket_count`. |
| `bucket_count` | Number of buckets (must be a power of 2). Requires `bucketed_by`. |
| `sorted_by` | Array of sort column names (within each bucket), e.g. `ARRAY['key']`. Only supported for bucketed tables. |
| `format` | Storage format: `PARQUET`, `DWRF`, `TEXT`. Defaults to `--data_format`. |
| `compression_kind` | Compression: `NONE`, `ZSTD` (default), `SNAPPY`, `GZIP`, `LZ4`, `ZLIB`, `LZO`. |

### CREATE TABLE

```sql
-- Simple table.
CREATE TABLE t (a BIGINT, b VARCHAR);

-- Partitioned table. Partition columns must be last.
CREATE TABLE events (
    event_id BIGINT,
    payload VARCHAR,
    ds VARCHAR
)
WITH (partitioned_by = ARRAY['ds']);

-- Bucketed and sorted table.
CREATE TABLE bucketed_orders (
    orderkey BIGINT,
    custkey BIGINT,
    totalprice DOUBLE
)
WITH (
    bucketed_by = ARRAY['orderkey'],
    bucket_count = 16,
    sorted_by = ARRAY['orderkey']
);

-- Partitioned, bucketed table with non-default format and compression.
CREATE TABLE detailed_orders (
    orderkey BIGINT,
    custkey BIGINT,
    totalprice DOUBLE,
    ds VARCHAR
)
WITH (
    partitioned_by = ARRAY['ds'],
    bucketed_by = ARRAY['orderkey'],
    bucket_count = 16,
    sorted_by = ARRAY['orderkey'],
    format = 'DWRF',
    compression_kind = 'SNAPPY'
);
```

### CREATE TABLE AS SELECT

```sql
-- Simple.
CREATE TABLE t AS SELECT * FROM tpch.tiny.lineitem;

-- With table properties.
CREATE TABLE t
WITH (partitioned_by = ARRAY['ds'], format = 'DWRF')
AS SELECT orderkey, totalprice, ds FROM orders;
```

### INSERT INTO

```sql
INSERT INTO t SELECT * FROM tpch.sf1.lineitem;
```

### DROP TABLE

```sql
DROP TABLE t;
```

### Vector indexes

```sql
CREATE TABLE products (
    id BIGINT,
    embedding ARRAY(REAL),
    ds VARCHAR,
    VECTOR INDEX product_index (embedding) KEY (id) WITH (
        distance_metric = 'cosine',
        index_type = 'flat',
        num_clusters = 1000))
WITH (partitioned_by = ARRAY['ds']);

ALTER TABLE products ADD VECTOR INDEX product_ivf (embedding) KEY (id)
WITH (distance_metric = 'cosine', index_type = 'ivf_flat', num_clusters = 1000);

ALTER TABLE products DROP VECTOR INDEX product_ivf;
```

An index's properties:

- `distance_metric`, required: `cosine`.
- `index_type`, required: `flat`, `ivf_flat`, `ivf_sq` or `hnsw_sq`.
- `num_clusters`, required: a positive integer.
- `codec_policy`, optional: `per_partition` (the default) or `shared`.
- `index_options`, optional: a `MAP(VARCHAR, VARCHAR)` of local index settings,
  such as `ivf.nlist`.

Any other property is a table property of both of its storage tables. The
index's partitions are its table's, so `partitioned_by`, `bucketed_by`,
`bucket_count` and `sorted_by` are not supported.

The table's `.schema` records each index's settings. The index is stored as two
tables named after the table and the index, `products__product_index__codecs`
and `products__product_index__index`, partitioned as the table is. Dropping the
index removes its record, then both tables; dropping the table drops its
indexes' tables too. A DELETE of partitions of the table removes the same
partitions from both tables of each of its indexes.

## Statistics

Tables require `.schema` and `.stats` metadata files. These are produced in
two ways:

### Write pipeline (CREATE TABLE, INSERT INTO)

Tables created through the write pipeline (`CREATE TABLE`, `INSERT INTO`)
produce `.schema` and `.stats` files automatically. The `.stats` files store
per-partition row counts and per-column statistics (min, max, count, NDV).
At load time, statistics are read directly from these files without opening
data files.

- **Cost estimation**: The optimizer applies partition key filters to the
  persisted partition stats, merges column stats across matching partitions,
  and computes null percentages. NDV is merged using max across partitions.
  TODO: Replace NDV max with HLL sketch merging for accurate union.

### LocalTableBuilder (external data)

For tables created outside the write pipeline (e.g., data files dropped into
a directory), use `LocalTableBuilder` to generate `.schema` and `.stats`
files. The builder reads file headers to infer schema and collect per-column
statistics, and optionally samples data to estimate NDV.

```cpp
LocalTableBuilder builder(pool, fileFormat, connector);
builder.build("/path/to/table_dir");
```
