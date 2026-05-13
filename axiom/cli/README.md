# Axiom CLI

Interactive SQL command line for executing queries against in-memory TPC-H
dataset, local Hive data, or Test Connector tables.

## Launch

Using Buck:

```bash
buck run @mode/opt axiom/cli:cli
```

From a CMake build:

```bash
_build/release/axiom/cli/axiom_sql
```

The examples below use `axiom_sql` for brevity. With Buck, replace
`axiom_sql <args>` with `buck run @mode/opt axiom/cli:cli -- <args>`.

## Command-Line Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--query` | | SQL text to execute. Supports multiple semicolon-separated statements. If not specified, enters interactive mode. If set to an empty string (`--query ""`), reads semicolon-separated SQL statements from stdin. |
| `--init` | | Path to a SQL file with semicolon-separated statements to execute on startup before entering interactive mode or running `--query`. |
| `--catalog` | | Default catalog (connector). If not specified, defaults to `hive` when `--data_path` is set, `tpch` otherwise. |
| `--schema` | | Default schema. If not specified, defaults to `tiny` for TPC-H and `default` for Hive and Test connectors. |
| `--etc_dir` | | Path to a directory of catalog `.properties` files. Mutually exclusive with `--data_path`. External catalogs are not selected automatically; use `--catalog` or fully-qualified names in SQL. |
| `--data_path` | | Hive specific: root path for Hive-style partitioned data. Registers local Hive connector. Mutually exclusive with `--etc_dir`. |
| `--data_format` | `parquet` | Hive specific: data format, `parquet`, `dwrf`, or `text`. |
| `--split_target_bytes` | `16MB` | Hive specific: approximate bytes per split. |
| `--num_workers` | `4` | Number of in-process workers. |
| `--num_drivers` | `4` | Number of drivers per worker (parallelism). |
| `--max_rows` | `100` | Maximum number of printed result rows. |
| `--debug` | `false` | Enable debug mode (logging to stderr). |

## Connectors

Four connectors are available out of the box. See individual connector READMEs
for detailed documentation.

**[TPC-H](../connectors/tpch/README.md)** (`tpch.tiny`) — Always registered. Read-only. Provides standard TPC-H tables
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

**[Hive](../connectors/hive/README.md)** (`hive.default`) — Registered via `--data_path` or configured through `.properties` files.
Reads and writes Parquet, DWRF, or TEXT (including CSV) files in a local directory with Hive-style partitioning. Supports
`CREATE TABLE`, `CREATE TABLE AS SELECT`, `INSERT`, and `DROP TABLE`.

Using `--data_path`:

```
$ ./axiom_sql --data_path /path/to/data --data_format parquet
```

Configuring through `--etc_dir`:

```properties
# etc/hive.properties
connector.name=hive
hive_local_data_path=/path/to/data
hive_local_file_format=parquet
```

```bash
$ ./axiom_sql --etc_dir etc/
```

When `--etc_dir` is used, catalogs are available by name, but the
CLI does not auto-select one. Use `--catalog <name>` or a fully qualified
catalog.schema name in SQL, for example `USE hive2.default;`.

**[System](../connectors/system/README.md)** (`system`) — Always registered. Read-only. Provides metadata tables
such as session properties (`system.metadata.session_properties`).

**[Test](../connectors/tests/README.md)** (`test.default`) — Always registered. An in-memory connector that supports
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

## Catalog Configuration Files

Configuration files (`.properties` files) define catalogs for use with `--etc_dir`.
Each file name (without `.properties` extension) becomes the catalog name.

**Format:**
- One property per line in `key=value` format
- Lines starting with `#` are comments and are ignored
- Whitespace before and after keys and values is trimmed
- All whitespace is preserved within keys and values

**Required properties:**
- `connector.name` — The connector type: `hive`, `tpch`, or `test`

**Example:**

```properties
# Hive connector for local data
connector.name=hive
hive_local_data_path=/path/to/data
hive_local_file_format=parquet
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

The `--query` flag accepts multiple semicolon-separated statements:

```
$ ./axiom_sql --query "select count(*) from nation; select count(*) from region"
```

### Query from File

Pipe a SQL file into the CLI using `--query ""`. The file can contain multiple
semicolon-separated statements:

```
$ cat query.sql | ./axiom_sql --query ""
```

### EXPLAIN

```
SQL> explain select count(*) from nation;
```

Use `explain analyze` to execute the query and print the plan annotated with
runtime statistics.

## Timing Output

In interactive mode, each statement prints timing after execution:

```
Parsing: 10.58ms / 8.76ms user / 1.83ms system (100%)
Optimizing: 3.27ms | Executing: 5.27ms | Total: 19.26ms / 16.36ms user / 4.65ms system (109%)
```

| Field | Description |
|-------|-------------|
| Parsing | Time to parse the SQL text into an AST. |
| Optimizing | Time in the query optimizer. |
| Executing | Time running the Velox execution plan. |
| Total | Wall-clock time for the entire query lifecycle. |

Each field shows wall time, user CPU, system CPU, and CPU utilization %.

## Query History

The CLI persists query history across sessions. Previous commands are available
via the up/down arrow keys even after restarting the CLI.

- History is stored in `~/.axiom_cli.history`.
- History is saved after each command (not just on exit) to survive crashes
  during long-running queries.
- Up to 1024 entries are retained.
- If `HOME` is unset, history silently degrades to in-memory only (no
  persistence).

## Interactive Commands

Dot-commands do not require a semicolon terminator.

| Command | Description |
|---------|-------------|
| `.help` | Print help text. |
| `.run <file>` | Execute SQL statements from a file. |
| `.set <name> <value>` | Set a gflag at runtime (e.g., `num_workers`, `num_drivers`, `max_rows`). |
| `.clear <name>` | Reset a flag to its default value. |
| `.flags` | List modified flags. |
| `.exit` or `.quit` | Exit the CLI. |

Session config properties can be set using the `SET SESSION` SQL statement.
