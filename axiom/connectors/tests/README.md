# Test Connector

## Overview

The Test connector is a fully in-memory, read-write connector. Tables are
created and populated at runtime via SQL statements or C++ APIs. Data does not
persist across process restarts.

The Test connector is designed for three use cases:

1. **Interactive SQL exploration** — Use the CLI to experiment with SQL syntax,
   explore query plans, and test optimizer behavior without setting up data
   files on disk.

2. **End-to-end query tests** — Create tables, insert data via SQL, run queries,
   and verify results, all in memory with no filesystem dependencies.

3. **Optimizer unit tests** — Create tables with controlled statistics (row
   counts, NDV, min/max, null percentages) without loading real data. This
   allows testing optimizer cost estimation and plan selection in isolation.

## Capabilities

**Reads:**
- In-memory table scans. Data is organized into splits, where each split
  corresponds to one batch of inserted rows. Splits are the unit of parallel
  execution — multiple drivers can scan different splits concurrently.
- User-defined hidden columns (specified at table creation time, not included
  in `SELECT *`, but available for explicit queries).
- A hidden BIGINT `$row_id` column whose values are stable and unique for the
  lifetime of a table.

**Writes:**
- `CREATE TABLE`.
- `CREATE TABLE AS SELECT`.
- `INSERT INTO`.
- Row-level `DELETE`.
- `DROP TABLE`.
- Vector indexes: `VECTOR INDEX` elements of `CREATE TABLE`, and
  `ALTER TABLE ADD/DROP VECTOR INDEX` (see
  [Partitioned Tables and Vector Indexes](#partitioned-tables-and-vector-indexes)).
- Automatic statistics collection: per-column NDV, min/max, null percentage,
  max and average length, and average size in bytes of variable-width values
  are computed incrementally as data is added.
- User-specified statistics: row counts and per-column stats can be set
  explicitly without adding actual data, enabling optimizer testing with
  controlled cost estimates (see [Setting Statistics Without Data](#setting-statistics-without-data)).
- Suppressed statistics: a table can be created to report none at all, so the
  optimizer sees a table it cannot cost (see
  [Tables Without Statistics](#tables-without-statistics)).

**Supported:**
- Hash bucketing. Tables can be created bucketed via the C++ API
  (`addTable(..., TestBucketSpec{...})`). Rows are routed to buckets via a
  generic hash partition function on append, and splits are emitted per
  bucket. When the split manager is asked for splits with a non-null
  `PartitionType`, each emitted Split is tagged with a `remotePartition`
  derived from its bucket, enabling end-to-end exercise of the
  bucketed-execution scheduling contract.

**Not supported:**
- Filter pushdown.
- Partitions of data. A table may declare partition columns, but they are
  metadata only (see
  [Partitioned Tables and Vector Indexes](#partitioned-tables-and-vector-indexes)).
- Sort-within-bucket.
- Persistence. All data is lost when the process exits.

## Usage

The Test connector is always registered under catalog `test` with a single
schema called `default`. All tables are created in `test.default`.

See the [CLI README](../../cli/README.md) for how to launch `axiom_sql`.

```bash
axiom_sql
```

```sql
SQL> USE test.default;
SQL> CREATE TABLE t (a INTEGER, b VARCHAR);
SQL> INSERT INTO t VALUES (1, 'hello'), (2, 'world');
SQL> SELECT * FROM t;
SQL> DROP TABLE t;
```

Create tables from TPC-H data:

```sql
SQL> CREATE TABLE t AS SELECT * FROM tpch.tiny.lineitem;
```

Use `--init` to pre-populate tables on startup. Create a SQL file with
semicolon-separated statements, then pass it via the `--init` flag. The
statements run before entering interactive mode or executing `--query`. The
file must start with `USE test.default` because the default catalog is `tpch`,
not `test`:

```sql
-- init.sql
USE test.default;
CREATE TABLE t AS SELECT * FROM unnest(ARRAY[1, 2, 3], ARRAY[10, 20, 30]) AS t(a, b);
```

```bash
axiom_sql --init init.sql --query "SELECT sum(b) FROM t"
```

## C++ API

### Creating Tables and Adding Data

```cpp
auto connector = std::make_shared<TestConnector>("test");
velox::connector::registerConnector(connector);
ConnectorMetadataRegistry::global().insert(
    connector->connectorId(), connector->metadata());

// Create a table with schema.
auto table = connector->addTable(
    "users",
    ROW({"id", "name", "age"}, {BIGINT(), VARCHAR(), INTEGER()}));

// Add data as RowVectors.
table->addData(makeRowVector({"id", "name", "age"}, {
    makeFlatVector<int64_t>({1, 2, 3}),
    makeFlatVector<std::string>({"Alice", "Bob", "Carol"}),
    makeFlatVector<int32_t>({30, 25, 35}),
}));
```

### Setting Statistics Without Data

For optimizer testing, you can set row count and per-column statistics (number
of distinct values, min/max range) without providing actual data. The optimizer
uses these statistics for cost estimation and plan selection.

The example below creates a table with 1.5M rows where `orderkey` has 1.5M
distinct values (i.e. is unique) in the range [1, 6M] and `custkey` has 100K
distinct values in the range [1, 150K]:

```cpp
connector->addTable("orders", ROW({"orderkey", "custkey"}, BIGINT()))
    ->setStats(1'500'000, {
        {"orderkey", ColumnStatistics{.numDistinct = 1'500'000, .min = 1, .max = 6'000'000}},
        {"custkey", ColumnStatistics{.numDistinct = 100'000, .min = 1, .max = 150'000}},
    });
```

When all columns share the same type, use `ROW(names, type)` instead of
`ROW(names, {type, type, ...})`.

`setStats()` and `addData()` cannot be combined on the same table.

### Tables Without Statistics

A table created with `collect_statistics = false` reports no statistics at all:
no row count and no per-column statistics, however much data it holds. Use it
to test what the optimizer does with a table the metastore knows nothing
about, such as falling back to the written join order because no join over it
can be costed.

```sql
CREATE TABLE events (a BIGINT, b BIGINT) WITH (collect_statistics = false);
```

The data is still there and still queried; only the statistics are withheld.

### Bucketed Tables

Pass a `TestBucketSpec` to `addTable` to mark a table as hash-bucketed. Rows
appended via `addData` are routed to buckets via a generic hash partition
function (the test connector defines its own `TestPartitionType` and does
not depend on the Hive connector). The split manager emits one split per
non-empty bucket. The optimizer treats the table as bucket-partitioned for
planning purposes.

```cpp
auto table = connector->addTable(
    "orders",
    ROW({"customer_id", "amount"}, {BIGINT(), DOUBLE()}),
    velox::ROW({}),                          // no hidden columns
    TestBucketSpec{{"customer_id"}, 16});    // 16 buckets on customer_id
```

`TestBucketSpec::bucketColumns` lists the stored column names that compose the
bucket key (visible columns or the generated `$row_id`); `numBuckets` is the
fixed bucket count.

### Hidden Columns

Tables can have hidden columns that are not included in `SELECT *`, `DESC`,
or `SHOW CREATE TABLE` output but can be queried explicitly by name.

Every table has a hidden BIGINT `$row_id` by default. The connector assigns
monotonically increasing values as rows are appended and never reuses an ID,
including after a delete.

Hidden columns can be defined via SQL using the `hidden` property:

```sql
CREATE TABLE events (event_id BIGINT, payload VARCHAR)
  WITH (hidden = ARRAY['$timestamp', '$source']);
```

Hidden column names must not conflict with schema column names. All hidden
columns created through the `hidden` property are VARCHAR. `$row_id` is
reserved and cannot be declared through that property or in the visible schema.

Hidden columns can also be defined via the C++ API with explicit types:

```cpp
auto table = connector->addTable(
    "events",
    ROW({"event_id", "payload"}, {BIGINT(), VARCHAR()}),          // visible
    ROW({"$timestamp", "$source"}, {TIMESTAMP(), VARCHAR()}));    // hidden
```

```sql
-- Returns only event_id and payload.
SELECT * FROM events;

-- Hidden columns must be referenced explicitly.
SELECT event_id, "$timestamp" FROM events;
```

### Row-Level DELETE

`DELETE` reads the `$row_id` of each matching row and hands the IDs to a
writer, which stages them; the rows are removed when the DELETE commits. By
default a table's DELETE layout asks for no shuffle and no sort, so each writer
receives whatever rows reach it. To make the optimizer distribute or order the
rows, set a row-level layout on the table. This one spreads the rows over 4
writers by `$row_id` and sorts each writer's rows by it:

```cpp
table->setRowLevelDeleteLayout(DeleteLayout::rowLevel(
    /*computedColumns=*/{},
    /*shuffleKeys=*/{"$row_id"},
    std::make_shared<TestPartitionType>(
        4, std::vector<TypePtr>{BIGINT()}, ROW("$row_id", BIGINT())),
    /*sortKeys=*/{"$row_id"},
    /*sortOrders=*/{{/*isAscending=*/true, /*isNullsFirst=*/false}}));
```

The layout must have a writer: TestConnector deletes rows only through one, so
`DeleteLayout::metadata()` is rejected.

Tests observe a DELETE through callbacks on the connector:
`setOnFinishDelete` receives the row IDs each writer staged, once per DELETE
that runs its writers, and `setOnAbortDelete` runs once per aborted DELETE.

### Discrete Predicate Values

Discrete predicate values specify the complete set of values a column can take.
The optimizer uses these to enumerate possible values for planning, similar to
how the Hive connector reports partition key values. The Test connector accepts
any values without verifying that they match the actual data.

The example below specifies that the `status` column of the `orders`
table has exactly three possible values: `"O"`, `"F"`, and `"P"`:

```cpp
connector->addTable("orders", ...)->setDiscreteValues({"status"}, {
    Variant::row({Variant("O")}),
    Variant::row({Variant("F")}),
    Variant::row({Variant("P")}),
});
```

A table already added takes `connector->setDiscreteValues("orders", ...)`.

### Partitioned Tables and Vector Indexes

A table may declare partition columns with the `partitioned_by` property, or
`addPartitionedTable` in C++. They
are metadata: the table holds no partitions of data, and `setDiscreteValues`
records the partitions it has. A vector index of the table is partitioned by
the same columns. Adding an index creates its codec table and index table,
named after the table and the index, e.g. `products__i__codecs` and
`products__i__index`. Dropping the table or the index drops them; dropping a
storage table directly fails.

The index covers a partition of the table once the partitions recorded for its
index table include it. A table without partition columns is covered once its
index table has rows. A search fails when the index does not cover every
partition of the table; coverage does not prune by the search's filters.

The example below adds a partitioned `products` table with an index that
covers both of its partitions, and gives the index table a row count:

```cpp
const std::vector<Variant> partitions{
    Variant::row({Variant("2026-01-01")}),
    Variant::row({Variant("2026-01-02")}),
};
connector
    ->addPartitionedTable(
        "products",
        ROW({"id", "embedding", "ds"}, {BIGINT(), ARRAY(REAL()), VARCHAR()}),
        {"ds"})
    ->setDiscreteValues({"ds"}, partitions);

connector->addVectorIndex(
    "products",
    {.name = "i",
     .column = "embedding",
     .key = {"id"},
     .properties = {
         {"distance_metric", Variant("cosine")},
         {"index_type", Variant("flat")},
         {"num_clusters", Variant(int64_t{4})},
     }});
connector->setDiscreteValues("products__i__index", {"ds"}, partitions);
connector->setStats("products__i__index", 1'000'000, {});
```

### TPC-H Tables

Register all 8 TPC-H tables with their canonical schemas (no data, just schema):

```cpp
connector->addTpchTables();
```

### Views

Views are named queries with a fixed schema. The connector stores the view
name, schema, and SQL text without verifying that the SQL is valid or that it
matches the declared schema:

```cpp
connector->createView("active_users", ROW({"id", "name"}, {BIGINT(), VARCHAR()}),
    "SELECT id, name FROM users WHERE age > 18");
connector->dropView("active_users");
```
