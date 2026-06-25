# File Connector

## Overview

The file connector provides read-only access to individual files on disk. Point
a query at a file path and no catalog, no metastore, no table registration is needed. It is a multi-utility feature for doing deterministic data analytics on top of the raw file directly.
SQL is only one front-end; the connector is queryable through any Axiom
front-end (e.g. the dataframe API).

The connector is **file-type/format-agnostic by design**. The core layer handles
table-name parsing, split management, and column handles without any knowledge
of how bytes on disk are structured. All format-specific logic is isolated
behind the **`FileHandler`** interface: each handler teaches the connector how
to read one file type — how to extract the schema, how to stream row data, and
what synthetic metadata tables to expose. See the `FileHandler` interface in
`FileHandler.h` for how to add support for a new file format.

## Capabilities

**Reads:**
- Scan row data from a supported file via its absolute file path.
- Auto-detect schema from the file header.
- Column projection — only the requested columns are decoded.
- Streaming reads — row data is returned one batch at a time, not buffered in
  memory.
- Synthetic metadata tables via `$`-suffix naming (see
  [Appendix: Handler Reference](#appendix-handler-reference)).

## Usage

The file connector is registered under catalog `file`. The schema name selects
the file-format handler (e.g. `parquet`). Table names are absolute file paths,
quoted in SQL. See the [CLI README](../../cli/README.md) for how to launch
`axiom_sql`.

### Reading row data

```sql
-- Read all rows from a Parquet file.
SELECT * FROM file."parquet"."/data/file.parquet" LIMIT 10;

-- Project specific columns.
SELECT id, name
FROM file."parquet"."/data/file.parquet"
WHERE name = 'alpha';
```

### Inspecting file metadata

Append a `$`-suffix to the file path to query a synthetic metadata table. No
data rows are read — the connector extracts metadata from the file header and
internal structure.

```sql
-- Inspect per-column-chunk encodings, compression, and statistics.
SELECT name, encodings, compression, min, max, null_count
FROM file."parquet"."/data/file.parquet$column_chunks";
```

Which metadata tables are available depends on the file handler. See
[Appendix: Handler Reference](#appendix-handler-reference) for the tables each
handler exposes.

## Table Name Format

A table name is a file path with an optional metadata-table suffix:

```
<file-path>[$<suffix>]
```

- A plain path (no `$` suffix) maps to the **data table** — row data from the
  file.
- A path ending with a recognized `$`-suffix maps to the corresponding
  **metadata table** (e.g. `$row_groups`, `$column_chunks`).
- An unrecognized `$`-suffix produces an error from the handler.

A `$` inside a directory name (e.g. `/data/$data2/file.example`) or a file name
(e.g. `/tmp/foo$bar.file`) is part of the path, not a suffix, and resolves to
the data table.

Known limitation: a `$` in an extensionless file name (e.g. `/data/foo$bar`) is read as a suffix separator, so the name splits into path `/data/foo` and suffix `bar` instead of resolving to the data table.

---

## Appendix: Handler Reference

### Parquet Handler

`parquet/ParquetFileHandler.h` provides read support for Parquet files and
exposes two synthetic metadata tables: `$row_groups` and `$column_chunks`.

#### Parquet $row_groups

Lists one row per row group with size and row-count statistics.

| Column | Type | Description |
|--------|------|-------------|
| `row_group_id` | BIGINT | Zero-based row group index within the file. |
| `num_rows` | BIGINT | Number of rows in the row group. |
| `total_byte_size` | BIGINT | Total uncompressed byte size of all column data in the row group. |
| `total_compressed_size` | BIGINT | Total compressed byte size of all column data in the row group. |

```sql
SELECT row_group_id, num_rows, total_byte_size, total_compressed_size
FROM file."parquet"."/data/file.parquet$row_groups";
```

Example output:

```
 row_group_id | num_rows | total_byte_size | total_compressed_size
--------------+----------+-----------------+-----------------------
            0 |       25 |            2987 |                  1627
```

#### Parquet $column_chunks

Lists one row per (row group, column chunk) with its page encodings,
compression codec, byte sizes, value count, and column-chunk statistics.

| Column | Type | Description |
|--------|------|-------------|
| `row_group_id` | BIGINT | Zero-based row group index. |
| `column_id` | BIGINT | Zero-based column index within the row group. |
| `name` | VARCHAR | Column name from the file schema. |
| `compression` | VARCHAR | Compression codec (e.g. `none`, `zstd`, `snappy`, `gzip`). |
| `encodings` | VARCHAR | Comma-separated Parquet page encodings (e.g. `RLE,PLAIN`, `RLE_DICTIONARY`). |
| `compressed_size` | BIGINT | Total compressed byte size of the column chunk. |
| `uncompressed_size` | BIGINT | Total uncompressed byte size of the column chunk. |
| `num_values` | BIGINT | Number of values (including nulls) in the column chunk. |
| `min` | VARCHAR | Minimum value from the column-chunk statistics, formatted as text. NULL when the file carries no statistics for the column. |
| `max` | VARCHAR | Maximum value from the column-chunk statistics, formatted as text. NULL when the file carries no statistics for the column. |
| `null_count` | BIGINT | Number of null values in the column chunk. NULL when the file carries no statistics. |

Parquet column-chunk statistics can also carry a distinct-value count, but it is
not exposed today: the Velox Parquet reader does not surface it and most writers
leave it unset.

```sql
-- Inspect encodings and compression for every column chunk.
SELECT row_group_id, name, encodings, compression
FROM file."parquet"."/data/file.parquet$column_chunks";

-- Find columns with high compression ratios.
SELECT name, compression,
       uncompressed_size / NULLIF(compressed_size, 0) AS ratio
FROM file."parquet"."/data/file.parquet$column_chunks"
WHERE compressed_size > 0
ORDER BY ratio DESC;

-- Inspect per-column value ranges.
SELECT name, min, max, null_count
FROM file."parquet"."/data/file.parquet$column_chunks";
```

Example output:

```
 row_group_id | column_id | name | compression | encodings          | compressed_size | uncompressed_size | num_values | min   | max  | null_count
--------------+-----------+------+-------------+--------------------+-----------------+-------------------+------------+-------+------+-----------
            0 |         0 | id   | zstd        | RLE,PLAIN          |             187 |               305 |         25 | 1     | 25   |          0
            0 |         1 | name | zstd        | RLE,RLE_DICTIONARY |             279 |               360 |         25 | alpha | zeta |          0
```
