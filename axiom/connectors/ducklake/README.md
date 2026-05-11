# DuckLake Connector

The DuckLake connector lets Axiom query DuckLake tables by reading DuckLake
metadata from the catalog database and executing the live Parquet files through
Velox's Iceberg reader path.

## Usage

Build Axiom:

```bash
make debug
```

Run `axiom_sql` with a DuckLake catalog URL:

```bash
_build/debug/axiom/cli/axiom_sql \
  --ducklake_catalog ducklake:/path/to/metadata.ducklake \
  --query "SELECT * FROM my_table LIMIT 10"
```

The default DuckLake catalog name is `ducklake` and the default schema is
`main`, so these are equivalent:

```sql
SELECT * FROM my_table;
SELECT * FROM ducklake.main.my_table;
```

You can also spell the DuckDB catalog backend explicitly:

```bash
_build/debug/axiom/cli/axiom_sql \
  --ducklake_catalog ducklake:duckdb:/path/to/metadata.ducklake \
  --query "SELECT count(*) FROM my_table"
```

The connector does not need a DuckLake data path flag. DuckLake stores the data
path in catalog metadata, and Axiom resolves table and file paths from that
metadata.

DuckLake enables data inlining by default for small writes. To create test data
for the current Axiom connector, either disable inlining on attach or flush
inlined data to Parquet before querying through Axiom:

```sql
ATTACH 'ducklake:metadata.ducklake' AS lake
  (DATA_PATH 'data', DATA_INLINING_ROW_LIMIT 0);
```

## How It Works

The CLI registers the DuckLake connector with a `ducklake_catalog` URL. Axiom
passes that URL into the connector config and registers a Velox Iceberg
connector under the `ducklake` catalog name.

At planning time, `DuckLakeCatalogClient` opens the DuckLake catalog database
read-only and reads the latest snapshot metadata. It resolves the catalog,
schema, table, and file paths from DuckLake metadata tables, converts top-level
DuckLake column types to Velox types, rejects unsupported DuckLake features, and
returns the live Parquet files for the table.

`DuckLakeConnectorMetadata` turns that metadata into Axiom table and layout
objects. The layout creates Iceberg column handles using DuckLake column ids as
Parquet field ids, so Velox can bind file columns by stable ids. The split
manager exposes one logical partition and expands the live DuckLake files into
Velox Iceberg/Hive splits. The actual Parquet reads are then executed by the
existing Velox Iceberg reader path.

## Current Scope

The first implementation supports read-only scans with:

- DuckDB-backed DuckLake catalog metadata.
- Parquet data files.
- Top-level primitive columns.
- Hidden Hive-style columns such as `$path` and `$file_size`.

The connector rejects unsupported features with explicit errors, including
SQLite/PostgreSQL catalog backends, encrypted tables or files, delete files,
inlined data tables, partial data files, and unsupported column types.
