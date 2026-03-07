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
- Filter pushdown: the optimizer pushes filters down to the connector, which
  evaluates them during scan. This includes partition pruning (skipping
  entire files based on partition key values) and within-file filtering.
- Automatic statistics collection from file headers (row counts, and for DWRF
  also null counts and min/max values).
- Sampling: a percentage of rows is read on startup to estimate per-column
  statistics such as number of distinct values (see
  [Statistics and Sampling](#statistics-and-sampling)).

**Writes:**
- `CREATE TABLE` with partitioning, bucketing, sort order, file format, and
  compression options specified via a WITH clause.
- `CREATE TABLE AS SELECT`.
- `INSERT INTO`.
- `DROP TABLE`.

**Not supported:**
- Transactions. Writes go directly into the table directory.
- `ALTER TABLE`, `RENAME TABLE`.
- Discrete predicate values. The optimizer uses these to constant-fold
  aggregations over partition key columns (e.g. `SELECT max(ds) FROM t`
  without scanning data). TODO: Implement `discretePredicateColumns` and
  `discretePredicates` APIs to report partition key values.

## Usage

Create a directory for storing tables and launch the CLI with `--data_path`
pointing to it. The directory can be empty; tables will be created in it via
SQL statements.

```bash
mkdir -p /tmp/my_data
buck run @mode/opt axiom/cli:cli -- --data_path /tmp/my_data
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
buck run @mode/opt axiom/cli:tpchgen -- \
    --data_path /tmp/tpch/sf0.1 --sf 0.1 \
    --data_format parquet --compression snappy
```

Then point the CLI at the generated directory:

```bash
buck run @mode/opt axiom/cli:cli -- --data_path /tmp/tpch/sf0.1
```

## Directory Structure

Each subdirectory under the data path is interpreted as a table. For
unpartitioned tables, data files are placed directly in the table directory.
For partitioned tables, data files are organized into subdirectories named
`<partition_column>=<value>` (e.g. `ds=2024-01-01/`):

```
/data/
├── orders/
│   ├── .schema              # optional: schema and table properties
│   ├── file1.parquet
│   └── file2.parquet
├── lineitem/
│   ├── .schema
│   ├── ds=2024-01-01/       # Hive partition directories
│   │   └── data.parquet
│   └── ds=2024-01-02/
│       └── data.parquet
└── users/
    └── data.parquet          # no .schema: schema inferred from files
```

If a table directory contains a `.schema` file, column definitions, partition
and bucketing information, and file format are read from it. Otherwise, the
schema is inferred from data files, file format defaults to `--data_format`,
and no bucketing or Hive partitioning is assumed.

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

**`dataColumns`** — Array of `{name, type}` objects. Types use Hive syntax
(e.g. `bigint`, `array<string>`, `struct<a:int,b:string>`).

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

## CREATE TABLE Options

Table properties are specified via a WITH clause. The following options are
supported:

| Option | Description |
|--------|-------------|
| `partitioned_by` | Array of Hive partition column names, e.g. `ARRAY['ds']`. Partition columns must be listed last in the column definition. |
| `bucketed_by` | Array of bucketing column names, e.g. `ARRAY['key']`. Requires `bucket_count`. |
| `bucket_count` | Number of buckets (must be a power of 2). Requires `bucketed_by`. |
| `sorted_by` | Array of sort column names (within each bucket), e.g. `ARRAY['key']`. Only supported for bucketed tables. |
| `file_format` | Storage format: `PARQUET`, `DWRF`, `TEXT`. Defaults to `--data_format`. |
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
    file_format = 'DWRF',
    compression_kind = 'SNAPPY'
);
```

### CREATE TABLE AS SELECT

```sql
-- Simple.
CREATE TABLE t AS SELECT * FROM tpch.tiny.lineitem;

-- With table properties.
CREATE TABLE t
WITH (partitioned_by = ARRAY['ds'], file_format = 'DWRF')
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

## Statistics and Sampling

The local implementation collects statistics at startup and after writes:

- **File-header stats**: Row counts are read from file metadata without
  reading the data itself. DWRF files also provide per-column stats: null
  counts and min/max values. Parquet files currently provide only row counts
  at the file level. TODO: implement file-level `columnStatistics()` in the
  Parquet reader.
- **Sampling**: On startup, a percentage of rows is read from each table to
  estimate NDV and other column-level statistics. For tables with more than
  1M rows, approximately 100K rows are sampled; for smaller tables, 10% of
  rows are sampled. This can slow down startup significantly for large
  datasets. TODO: defer sampling until the optimizer needs it.
- **Cost estimation**: The optimizer uses partition key filters from query
  predicates to skip irrelevant files, then aggregates per-file stats to
  produce cost estimates without reading data.

The optimizer caches sampling results and does not re-run the same sample.
