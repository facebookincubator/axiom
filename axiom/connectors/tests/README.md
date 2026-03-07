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

**Writes:**
- `CREATE TABLE`.
- `CREATE TABLE AS SELECT`.
- `INSERT INTO`.
- `DROP TABLE`.
- Automatic statistics collection: per-column NDV, min/max, null percentage,
  and max length are computed incrementally as data is added.
- User-specified statistics: row counts and per-column stats can be set
  explicitly without adding actual data, enabling optimizer testing with
  controlled cost estimates (see [Setting Statistics Without Data](#setting-statistics-without-data)).

**Not supported:**
- Filter pushdown.
- Partitioning and bucketing.
- Persistence. All data is lost when the process exits.

## Usage

The Test connector is always registered under catalog `test` with a single
schema called `default`. All tables are created in `test.default`.

```bash
buck run @mode/opt axiom/cli:cli
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
buck run @mode/opt axiom/cli:cli -- --init init.sql --query "SELECT sum(b) FROM t"
```

## C++ API

### Creating Tables and Adding Data

```cpp
auto connector = std::make_shared<TestConnector>("test");
velox::connector::registerConnector(connector);

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

### Hidden Columns

Tables can have hidden columns that are not included in `SELECT *` or `DESC`
output but can be queried explicitly. Hidden columns can only be defined via
the C++ API; there is no SQL syntax to specify them. TODO: Add SQL syntax for
defining hidden columns.

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

### Discrete Predicate Values

Discrete predicate values specify the complete set of values a column can take.
The optimizer uses these to enumerate possible values for planning, similar to
how the Hive connector reports partition key values. The Test connector accepts
any values without verifying that they match the actual data.

The example below specifies that the `status` column of the `orders`
table has exactly three possible values: `"O"`, `"F"`, and `"P"`:

```cpp
connector->setDiscreteValues("orders", {"status"}, {
    Variant("O"), Variant("F"), Variant("P"),
});
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
