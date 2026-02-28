# Axiom CLI

Interactive SQL command line for executing queries against in-memory TPC-H
dataset, local Hive data, or Test Connector tables.

## Launch

From a CMake build:

```bash
cd _build/release
./axiom/cli/axiom_sql
```

Using Buck:

```bash
buck run @mode/opt axiom/cli:cli
```

## Command-Line Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--query` | | SQL text to execute. Supports multiple semicolon-separated statements. If empty, enters interactive mode. |
| `--init` | | Path to a SQL file with semicolon-separated statements to execute on startup before entering interactive mode or running `--query`. |
| `--catalog` | | Default catalog (connector). If not specified, defaults to `hive` when `--data_path` is set, `tpch` otherwise. |
| `--schema` | | Default schema. If not specified, defaults to `tiny` for TPC-H and `default` for Hive and Test connectors. |
| `--data_path` | | Hive specific: root path for Hive-style partitioned data. Registers local Hive connector. |
| `--data_format` | `parquet` | Hive specific: data format, `parquet` or `dwrf`. |
| `--split_target_bytes` | `16MB` | Hive specific: approximate bytes per split. |
| `--num_workers` | `4` | Number of in-process workers. |
| `--num_drivers` | `4` | Number of drivers per worker (parallelism). |
| `--max_rows` | `100` | Maximum number of printed result rows. |
| `--optimizer_trace` | `0` | Optimizer trace level. |
| `--debug` | `false` | Enable debug mode (logging to stderr). |

## Connectors

Three connectors are available out of the box.

**TPC-H** (`tpch.tiny`) — Always registered. Read-only. Provides standard TPC-H tables
(`nation`, `region`, `part`, `supplier`, `partsupp`, `customer`, `orders`,
`lineitem`) generated in memory. Use `--schema` to select the scale factor:
`tiny` (default), `sf1`, `sf10`, etc. Default catalog when `--data_path` is not set.
The scale factor can also be specified as a schema qualifier in the query.

```
$ ./axiom_sql --query "select count(*) from orders"
15000

$ ./axiom_sql --query "select count(*) from orders" --schema sf1
1500000

$ ./axiom_sql --query "select count(*) from sf1.orders"
1500000
```

**Hive** (`hive.default`) — Registered when `--data_path` is set. Reads and writes
Parquet or DWRF files in a local directory with Hive-style partitioning. Supports
`CREATE TABLE`, `CREATE TABLE AS SELECT`, `INSERT`, and `DROP TABLE`. Becomes the
default catalog when registered.

```
$ ./axiom_sql --data_path /path/to/data --data_format parquet
```

**Test** (`test.default`) — Always registered. An in-memory connector that supports
`CREATE TABLE`, `CREATE TABLE AS SELECT`, `INSERT`, and `DROP TABLE`. Tables do
not persist across CLI launches. Use with `--init` to pre-populate tables on
startup:

```sql
-- start.sql
use test.default;
create table t as select * from unnest(array[1,2,3], array[10,20,30]) as t(a, b);
```

```
$ ./axiom_sql --init start.sql
SQL> select * from t;
```

## Examples

### Interactive Mode

```
$ ./axiom_sql

SQL> select count(*) from nation;
ROW<count:BIGINT>
-----
count
-----
   25
(1 rows in 1 batches)
```

### Single Query

```
$ ./axiom_sql --query "select count(*) from nation"
```

### EXPLAIN

```
SQL> explain select count(*) from nation;
```

Use `explain analyze` to execute the query and print the plan annotated with
runtime statistics.

## Interactive Commands

| Command | Description |
|---------|-------------|
| `help;` | Print help text. |
| `flag <name> = <value>;` | Set a gflag at runtime (e.g., `num_workers`, `num_drivers`, `max_rows`, `optimizer_trace`). |
| `clear <name>;` | Reset a flag to its default value. |
| `flags;` | List modified flags. |
| `session <name> = <value>;` | Set a session config property. |
| `exit;` or `quit;` | Exit the CLI. |
