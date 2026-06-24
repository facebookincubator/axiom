# Table-Valued Functions (TVFs) Design

## Motivation

Axiom supports querying Parquet, DWRF, and CSV files via the Hive connector,
but the workflow requires multiple steps: organizing files into directories,
running `axiom_hive_import` to generate schema and stats metadata, and
specifying `--data_path` at startup.

We want to support a simpler syntax:

```sql
SELECT * FROM read_parquet('/tmp/trips.parquet')
SELECT * FROM read_parquet('/tmp/jan.parquet', '/tmp/feb.parquet')
SELECT * FROM read_csv('/tmp/data.csv')
SELECT * FROM hive.default.read_parquet('/tmp/trips.parquet')
```

This requires adding support for Table-Valued Functions (TVFs) — functions that
appear in FROM clauses and produce a relation (a set of rows with a schema).

Beyond file reading, a generic TVF framework enables connectors to expose
their native capabilities as SQL-callable functions. For example:

```sql
-- Push MySQL-specific full-text search to the source database.
SELECT * FROM mysql.default.query('
    SELECT user_id, MATCH(bio) AGAINST("engineer" IN BOOLEAN MODE) AS relevance
    FROM users WHERE relevance > 0
')

-- Run a native Elasticsearch query.
SELECT * FROM elasticsearch.default.search('{
    "query": {"match": {"title": "table valued functions"}},
    "size": 10
}')

-- Combine MySQL full-text search with local Parquet data.
-- MySQL's MATCH...AGAINST syntax is not valid Presto SQL, but query() passes
-- it through to MySQL directly.
SELECT u.user_id, u.relevance, t.total
FROM mysql.default.query('
    SELECT user_id, MATCH(bio) AGAINST("+engineer +distributed" IN BOOLEAN MODE) AS relevance
    FROM users HAVING relevance > 0 ORDER BY relevance DESC LIMIT 100
') AS u
JOIN read_parquet('/tmp/transactions.parquet') AS t ON u.user_id = t.user_id
```

Each connector can add its own TVFs without Axiom needing to understand the
underlying query language. The framework we build for file-reading TVFs
supports all of these use cases.

## Prior Art

### TVF Categories Across Databases

TVFs fall into two broad categories:

**Computational TVFs** generate rows from scalar arguments without any I/O.
These are widely supported across all major databases:

| Function | PostgreSQL | SQL Server | DuckDB | Spark SQL | Snowflake | BigQuery | Trino | Presto/Velox |
|----------|-----------|------------|--------|-----------|-----------|----------|-------|-------------|
| `generate_series(1, 100)` | Yes | No | Yes | No | `GENERATOR` | `GENERATE_ARRAY` | `sequence()` | `UNNEST(sequence(...))` |
| `unnest(array)` | Yes | No | Yes | `explode` | `FLATTEN` | Yes | Yes | Yes |
| `json_each(json)` | Yes | `OPENJSON` | Yes | `explode` | `FLATTEN` | No | No | `UNNEST(cast(...))` |
| `regexp_matches(text, pat)` | Yes | No | Yes | No | No | No | No | `UNNEST(regexp_extract_all(...))` |
| `string_split(str, delim)` | `regexp_split_to_table` | `STRING_SPLIT` | `str_split` | `split` | `SPLIT_TO_TABLE` | No | No | `UNNEST(split(...))` |

In Presto/Velox, computational TVFs are handled by combining scalar functions
that return arrays or maps with `UNNEST`. This provides equivalent
functionality without dedicated TVF infrastructure. For example:

```sql
-- generate_series(1, 100)
SELECT * FROM UNNEST(sequence(1, 100)) AS t(x)

-- json_each('[{"key": "a", "value": 1}, {"key": "b", "value": 2}]')
-- UNNEST with flattenArrayOfRows expands each ROW into top-level columns.
SELECT * FROM UNNEST(
    CAST(json_parse('[{"key": "a", "value": 1}, {"key": "b", "value": 2}]')
         AS ARRAY(ROW(key VARCHAR, value INTEGER)))
) AS t(key, value)
```

**File-reading TVFs** produce a table scan from data files. The output schema
depends on the file contents, not just argument types. Unlike computational
TVFs, these are not widely available as inline SQL functions:

| Database | Inline file reading in SQL | Syntax |
|----------|--------------------------|--------|
| DuckDB | Yes — full TVF support | `SELECT * FROM read_parquet('/path')` |
| SQL Server | Partial — `OPENROWSET` | `SELECT * FROM OPENROWSET(BULK '/path', ...) AS t` (requires explicit schema) |
| Snowflake | Partial — staged files | `SELECT $1, $2 FROM @stage/file.csv` (positional columns, no schema inference) |
| Spark SQL | No — DataFrame API or DDL only | `spark.read.parquet("/path")` or `CREATE TABLE ... USING PARQUET LOCATION '/path'` |
| PostgreSQL | No — foreign data wrappers | `CREATE FOREIGN TABLE ... OPTIONS (filename '/path')` then `SELECT * FROM table` |
| BigQuery | No — external tables | `CREATE EXTERNAL TABLE ... OPTIONS (uris=['/path'])` then `SELECT * FROM table` |
| Presto | No | TVF framework exists in the SPI, but no production TVFs have been implemented by any connector |
| Trino | No | Built-in `sequence()` and `exclude_columns()` TVFs, but no file-reading TVFs |

DuckDB is the only database that supports the clean `read_parquet('/path')`
syntax with automatic schema inference. SQL Server's `OPENROWSET` and
Snowflake's staged file access come closest, but both require explicit
schema specification or produce untyped positional columns. All other
databases require a separate DDL step to define a table object before
querying files.

This gap motivates our design — bringing DuckDB-style file-reading TVFs to
Axiom while leveraging the existing Velox connector infrastructure.

### DuckDB

- The parser treats any `function(args)` in a FROM clause as a generic
  `TableFunctionRef` AST node. No special grammar per function.
- The binder resolves the function name via catalog lookup, calls the
  function's `bind` callback to infer the output schema, and produces a
  `LogicalGet` node (the same operator used for regular table scans).
- Each `TableFunction` provides callbacks: `bind`, `function` (scan),
  `init_global`, `init_local`, `statistics`, `cardinality`.
- TVFs are global catalog entries, not connector-scoped.
- File-reading TVFs (`read_parquet`, `read_csv`, `read_json`) support
  multiple files, glob patterns, named parameters (`sep=','`, `header=true`),
  auto-detection of delimiters and types, and schema merging across files
  via `union_by_name=true`.

### Presto

- TVFs are connector-scoped: each connector declares its TVFs via
  `Connector.getTableFunctions()`.
- The parser produces a `TableFunctionInvocation` AST node (extends `Relation`).
- Planning happens in two steps:
  1. **Analysis**: `ConnectorTableFunction.analyze(args)` validates arguments,
     infers the output schema, and returns a `ConnectorTableFunctionHandle`
     (opaque handle carrying connector-specific state).
  2. **Rewrite**: An optimizer rule (`RewriteTableFunctionToTableScan`) calls
     `ConnectorMetadata.applyTableFunction(handle)`, which returns a
     `ConnectorTableHandle` + column handles. The rule replaces
     `TableFunctionNode` with a standard `TableScanNode`.
- After rewrite, the engine treats the scan like any other table scan — splits,
  filter pushdown, and execution all work unchanged.
- [RFC-0020](https://github.com/prestodb/rfcs/blob/main/RFC-0020-tvf.md)
  describes the TVF framework design. It proposes three built-in TVFs:
  `sequence()`, `exclude_columns()`, and `query()`. No file-reading TVFs are
  mentioned. The design deliberately follows Trino's approach for ecosystem
  compatibility.
- No production connector has implemented any TVFs. All
  `ConnectorTableFunction` implementations exist only in test code.

### Trino

- TVF framework similar to Presto (shared heritage), with the `TABLE(...)`
  keyword wrapper in SQL syntax:
  `SELECT * FROM TABLE(sequence(start => 0, stop => 100))`.
- Two built-in TVFs shipped in production:
  - `sequence(start, stop, step)` — generates a single-column table of bigint
    values.
  - `exclude_columns(input, columns)` — takes a table and returns it minus
    specified columns (a table-in-table-out function). DuckDB handles this
    more naturally with `SELECT * EXCLUDE (col1, col2) FROM t` syntax
    directly in the SELECT clause, which is cleaner than wrapping in a TVF.
    Axiom will extend PrestoSQL to support `EXCLUDE` / `REPLACE` syntax
    rather than implementing `exclude_columns` as a TVF.
- Three execution paths for connectors:
  - **Pushdown** via `applyTableFunction()` — rewrites to table scan.
  - **`TableFunctionSplitProcessor`** — for source TVFs (leaf operators).
  - **`TableFunctionDataProcessor`** — for table-in-table-out TVFs.
- No connector has implemented file-reading TVFs (`read_parquet`, etc.).

### Lateral Joins with TVFs

All major databases support lateral joins (or equivalent) with computational
TVFs that reference outer columns:

| Database | Syntax | Example |
|----------|--------|---------|
| PostgreSQL | `LATERAL` (implicit for SRFs) | `FROM t, generate_series(1, t.n)` |
| SQL Server | `CROSS APPLY` / `OUTER APPLY` | `FROM t CROSS APPLY dbo.Func(t.col)` |
| Snowflake | `LATERAL FLATTEN` / `TABLE()` | `FROM t, LATERAL FLATTEN(t.data)` |
| BigQuery | `UNNEST` (implicit lateral) | `FROM t, UNNEST(t.array_col)` |
| Spark SQL | `LATERAL VIEW` | `FROM t LATERAL VIEW explode(t.arr)` |
| DuckDB | `LATERAL` | `FROM t, LATERAL generate_series(1, t.n)` |

DuckDB is the only database with file-reading TVFs, and even DuckDB does not
support lateral joins with them (e.g., `FROM t, read_parquet(t.path)`). The
file path is a runtime value, so the schema cannot be inferred at plan time.
Typed column aliases could address this by letting the user declare the
schema explicitly:

```sql
SELECT t.label, r.*
FROM file_index AS t, read_parquet(t.path) AS r(id INTEGER, name VARCHAR, amount DOUBLE)
```

The output schema is known at plan time (from the alias), but the file path
varies per row. This cannot be lowered to a static `TableScanNode` — it
requires a lateral-style execution operator that opens files dynamically
during execution.

### Design Comparison

| Aspect | DuckDB | Presto | Trino |
|--------|--------|--------|-------|
| TVF scope | Global catalog | Connector-scoped | Connector-scoped |
| Schema inference | `bind` callback | `analyze` method | `analyze` method |
| Plan representation | `LogicalGet` (same as table scan) | `TableFunctionNode` → rewritten to `TableScanNode` | `TableFunctionNode` → rewritten to `TableScanNode` |
| Execution | Dedicated scan callbacks | Reuses table scan infrastructure | Reuses table scan infrastructure + dedicated processors |
| Name resolution | Global by name | `catalog.schema.function` | `catalog.schema.function` |
| SQL syntax | `FROM read_parquet(...)` | `FROM TABLE(catalog.schema.func(...))` | `FROM TABLE(catalog.schema.func(...))` |
| Production file-reading TVFs | Yes | No | No |

## Scope

As discussed in Prior Art, computational TVFs (e.g., `generate_series`,
`json_each`) are already covered by `UNNEST(scalar_function(...))` in
Axiom/Velox and do not require new infrastructure. File-reading TVFs, on the
other hand, genuinely cannot be expressed any other way — they are the primary
motivation for adding TVF support. The framework we build for file-reading
TVFs is generic and supports any connector-provided TVF (e.g., `query()` for
database passthrough, `search()` for Elasticsearch).

This design covers **file-reading TVFs** with constant file paths (known at
plan time). Lateral joins where the file path comes from a column value
(e.g., `FROM t, read_parquet(t.path) AS r(...)`) require a different
execution model and are discussed in Future Extensions.

Examples:

```sql
-- Single file.
SELECT * FROM read_parquet('/tmp/trips.parquet')
SELECT * FROM read_dwrf('/tmp/trips.dwrf')

-- Multiple files (same schema, unioned).
SELECT * FROM read_parquet('/tmp/jan.parquet', '/tmp/feb.parquet')

-- Delimiter-specific convenience functions.
SELECT * FROM read_csv('/tmp/data.csv')
SELECT * FROM read_tsv('/tmp/data.tsv')

-- General-purpose text reader with options map.
SELECT * FROM read_text(
    MAP(ARRAY['delimiter', 'header'], ARRAY['|', 'true']),
    '/tmp/a.csv', '/tmp/b.csv')
```

### Engine (Axiom)

The engine provides the TVF framework: SQL parser support, name resolution,
`TableFunctionNode` in the logical plan, and query graph construction that
converts TVFs to `BaseTable`. The engine also provides **typed column aliases** —
an extension to the `columnAliases` grammar that allows overriding column
names and types on any TVF result:

```sql
SELECT * FROM read_csv('/tmp/data.csv') AS t(id INTEGER, name VARCHAR, amount DOUBLE)
```

The column definition list must have exactly as many entries as the TVF
returns. Two modes are supported — rename only or rename + cast. Mixing
(some entries with types, some without) is not allowed.

```sql
-- Rename only. All types preserved from the source.
SELECT * FROM read_csv('/tmp/data.csv') AS t(id, name, amount)

-- Rename and cast. All entries must specify a type.
SELECT * FROM read_csv('/tmp/data.csv') AS t(id INTEGER, name VARCHAR, amount DOUBLE)
```

### Filter and Cast Pushdown

Since TVFs become regular `BaseTable` nodes in the query graph, existing
filter pushdown and column pruning logic applies automatically. For Parquet
and DWRF, this enables row group/stripe skipping based on column statistics.

For text formats (CSV, TSV), typed column aliases introduce type overrides.
The desired output types are passed to `findTableFunction` as a hint. The
connector performs **partial cast pushdown** — it converts the types it can
and returns a `Table` with its actual output types. `ToGraph` compares the
`Table`'s actual types with the desired `outputType` from
`TableFunctionNode`. For mismatched columns, it adds cast expressions in
the `DerivedTable` output:

```
-- read_csv(...) AS t(id INTEGER, name VARCHAR, ts TIMESTAMP)
-- Connector handles INTEGER but not TIMESTAMP.

BaseTable t1: ROW(id INTEGER, name VARCHAR, ts VARCHAR)
  ↓
DerivedTable dt1:
  id := t1.id                      (types match — no cast)
  name := t1.name                  (types match — no cast)
  ts := CAST(t1.ts AS TIMESTAMP)   (type mismatch — cast inserted by ToGraph)
```

Filters on columns where the connector produced the desired type (e.g.,
`id`) can be pushed into the scan. Filters on columns with engine-level
casts (e.g., `ts`) depend on whether the optimizer can push through the
cast expression.

Connectors can improve over time — supporting more direct type conversions
reduces the need for engine-level casts and enables more filter pushdown,
without any engine changes.

### Hive Connector

The Hive connector will provide the following TVFs:

| Function | Format | Schema Source | Arguments |
|----------|--------|-------------|-----------|
| `read_parquet(paths...)` | Parquet | File header | One or more file paths |
| `read_dwrf(paths...)` | DWRF/ORC | File header | One or more file paths |
| `read_csv(paths...)` | CSV (comma-delimited) | Header row | One or more file paths |
| `read_tsv(paths...)` | TSV (tab-delimited) | Header row | One or more file paths |
| `read_text(options, paths...)` | Any delimited | Options | Options map + one or more file paths |

`read_csv` and `read_tsv` are convenience wrappers around `read_text` with
pre-set delimiters and `header=true`. `read_text` accepts a leading `MAP(VARCHAR, VARCHAR)`
argument with format-specific options:

| Option | Description | Default |
|--------|-------------|---------|
| `delimiter` | Field delimiter character | Required |
| `header` | `'true'` if the first row contains column names | `'false'` |

For `read_parquet` and `read_dwrf`, column names and types are inferred from
file metadata (headers/footers). For text formats, column names are inferred
from the header row (if `header` is `'true'`); all column types are VARCHAR.
Without a header row, the number of columns is inferred from the first data
row and columns are named `_c0`, `_c1`, etc. An empty file is an error.

All file-reading TVFs expose a hidden `$path` virtual column containing the
source file path. This is useful when reading multiple files:

```sql
SELECT "$path", count(*) FROM read_parquet('/tmp/jan.parquet', '/tmp/feb.parquet') GROUP BY 1
```

## Design

Similar to Presto, TVFs are connector-scoped, analyzed during planning, and
rewritten to table scans during optimization. Key differences from Presto
include: no opaque handle between analysis and lowering (the logical plan
stays connector-agnostic), typed column aliases for schema override, and
cast pushdown into the connector to enable filter pushdown and column
pruning.

### Name Resolution

TVFs follow the same 3-part naming convention as tables for consistency:

| Parts | Tables | TVFs |
|-------|--------|------|
| 1 | `defaultCatalog.defaultSchema.table` | `defaultConnector.defaultSchema.function` |
| 2 | `defaultCatalog.schema.table` | `defaultConnector.schema.function` |
| 3 | `catalog.schema.table` | `connector.schema.function` |

```sql
-- Unqualified: uses the default connector and default schema.
SELECT * FROM read_parquet('/tmp/trips.parquet')

-- S3 paths work with the Hive connector (s3:// is a filesystem, not a connector).
SELECT * FROM read_parquet('s3://bucket/data.parquet')

-- Fully qualified: explicit connector and schema.
SELECT * FROM mysql.default.query('SELECT ...')

-- Federated query: join MySQL full-text search results with local Parquet data.
SELECT c.name, c.relevance, o.total
FROM mysql.default.query('
    SELECT id, name, MATCH(bio) AGAINST("+engineer +backend" IN BOOLEAN MODE) AS relevance
    FROM customers HAVING relevance > 0 ORDER BY relevance DESC LIMIT 100
') AS c
JOIN read_parquet('/data/orders.parquet') AS o ON c.id = o.customer_id
WHERE o.total > 1000
```

The common case is simple — unqualified `read_parquet(...)` resolves against
the default connector. Multi-connector queries use 3-part names. This reuses
the existing name resolution logic (`toConnectorTable` pattern) and avoids
introducing TVF-specific resolution rules.

### Connector Interface

Two new methods on `ConnectorMetadata`:

#### 1. `resolveTableFunction` (called during planning)

Validates arguments, infers the output schema, and returns it. The planner
evaluates all argument expressions to constant values before calling the
connector.

TVF arguments must be constant expressions — literals, constant-folded
expressions, or expressions that can be fully evaluated at plan time. Column
references and other runtime values are not supported (see Future Extensions
for lateral joins where this restriction is lifted). For functions that
accept options, the options are passed as a `MAP(VARCHAR, VARCHAR)` argument
(e.g., `read_text(MAP(...), '/tmp/data.csv')`).

Unlike scalar functions, TVFs do not support static type resolution. The
output schema depends on the **values** of constant arguments — the file
path determines which Parquet file to read, which determines the column
names and types. This is why `resolveTableFunction` receives evaluated
argument values, not just argument types.

```cpp
/// Analyzes a table function invocation. Returns the inferred output
/// schema, or nullopt if the connector does not recognize the function
/// name. Throws a user error if the function is recognized but the
/// arguments are invalid (wrong number, wrong types, file not found, etc.).
virtual std::optional<RowTypePtr> resolveTableFunction(
    const std::string& functionName,
    const std::vector<velox::Variant>& arguments) = 0;
```

The planner interprets `nullopt` as "function not found" and a thrown
exception as "function exists but arguments are invalid" (e.g.,
"read_parquet requires at least one file path argument",
"File not found: /tmp/missing.parquet").

#### 2. `findTableFunction` (called during query graph construction)

Creates a `Table` object for the TVF invocation. The optimizer treats this
identically to a table returned by `findTable` — same code path for query
graph construction (`BaseTable`), plan enumeration (`TableScan` RelationOp),
handle creation, and split generation.

The `outputType` parameter is the desired output schema, which may include
type overrides from typed column aliases. The connector may use it as a
hint to produce the requested types directly (cast pushdown). The
connector is not required to honor it — the engine checks the `Table`'s
actual types against `outputType` and inserts casts if they differ.

```cpp
/// Creates a Table for the given table function invocation. 'outputType'
/// is the desired output schema (may include type overrides from typed
/// column aliases). The connector should produce matching types when
/// possible (cast pushdown), but may return its natural types — the
/// engine inserts casts for any remaining type mismatches.
///
/// @return nullptr if the function doesn't exist. Throws a user error
/// if the function is recognized but the arguments are invalid.
virtual TablePtr findTableFunction(
    const std::string& functionName,
    const std::vector<velox::Variant>& arguments,
    const RowTypePtr& outputType) = 0;
```

The returned `Table` is scoped to the query — it is held by the query
compilation pipeline and released after compilation completes. Unlike
regular tables which live for the lifetime of the connector, TVF tables are
ephemeral and are not cached in `Schema`.

Split generation does not depend on the `Table` object. The runner calls
`ConnectorSplitManager::getSplitSource(tableHandle, ...)` at execution
time. The connector uses the `ConnectorTableHandle` (created by `ToVelox`
via `layout->createTableHandle()`) to identify the TVF invocation and
produce splits from its own internal state. The `Table` and `TableLayout`
are only needed during compilation.

### Hive Connector Implementation

The Hive connector implements both methods for `read_parquet`, `read_dwrf`,
`read_csv`, `read_tsv`, and `read_text`:

**`resolveTableFunction`**:
1. Validates the function name. Determines the file format:
   - `read_parquet` → Parquet, `read_dwrf` → DWRF, `read_csv` → CSV,
     `read_tsv` → TSV, `read_text` → format from `delimiter` option.
2. Validates arguments. For `read_text`, the first argument must be a
   `MAP(VARCHAR, VARCHAR)` with options; remaining arguments are file paths.
   For all other functions, all arguments are file paths (VARCHAR strings).
3. For binary formats (Parquet, DWRF): opens the first file and reads the
   header/footer using Velox's `dwio::common` readers. Extracts column
   names and types → `outputType`.
4. For text formats (CSV, TSV): infers schema from the header row if
   `header` option is `'true'`. Without a header row, all columns are
   returned as VARCHAR with generated names (`_c0`, `_c1`, ...). Users
   can override names and types via typed column aliases:
   `read_csv(...) AS t(id INTEGER, name VARCHAR)`.
**`findTableFunction`**:
1. Creates a `Table` with columns at the types the connector can produce.
   For binary formats, these match the file types. For text formats, the
   connector may honor some type overrides from `outputType` (e.g.,
   parsing integers directly) while leaving others as VARCHAR.
2. Creates a `TableLayout` with the file path(s), format, and options.
3. Returns the `Table`. The connector retains internal state needed for
   split generation at execution time.

### Plan Representation

#### SQL Parser (AST)

A new grammar rule in `PrestoSql.g4`:

```antlr
relationPrimary
    : qualifiedName '(' (expression (',' expression)*)? ')'  #tableFunctionInvocation
    | qualifiedName tableVersionExpression?                    #tableName
    | ...
    ;
```

A new AST node `TableFunctionInvocation` holding the function name
(`QualifiedName`) and argument expressions. The parser does not need to know
about specific TVF names — it just recognizes the syntactic pattern of a
function call in FROM position.

The `columnAliases` rule is extended to support optional types for typed
column aliases:

```antlr
columnAliases: '(' columnDefinition (',' columnDefinition)* ')'
columnDefinition: identifier type?
```

Without types, it is a rename (backward compatible with existing alias
syntax). With types, all entries must include a type; the planner inserts
implicit casts from the inferred type to the specified type.

#### Logical Plan

A new `TableFunctionNode`:

```cpp
enum class NodeKind { ..., kTableFunction };

class TableFunctionNode : public LogicalPlanNode {
  /// Connector that owns this function.
  std::string connectorId_;

  /// Function name (e.g., "read_parquet").
  std::string functionName_;

  /// Evaluated arguments (e.g., ["/tmp/trips.parquet"]).
  std::vector<velox::Variant> arguments_;

  // outputType_ is inherited from LogicalPlanNode.
};
```

The logical plan stores only connector-agnostic data, consistent with
`TableScanNode` which stores `connectorId` + `SchemaTableName` without
any connector-specific handles.

#### Query Graph Construction (ToGraph)

`ToGraph` handles `TableFunctionNode` by calling
`ConnectorMetadata::findTableFunction` to get a `Table`, then creates a
`BaseTable` in the query graph — the same path used for regular
`TableScanNode`. From this point on, the optimizer sees a `BaseTable` and
all existing optimization logic applies unchanged: predicate pushdown,
column pruning, join reordering, and cost estimation.

### Table Display Name

The `Table` base class gets a virtual `displayName()` method:

```cpp
class Table {
    // Existing...
    virtual std::string displayName() const { return name().toString(); }
};
```

For regular tables, this returns `"schema"."table"`. For TVF-produced tables,
the connector overrides it to return the function call, e.g.,
`read_parquet('/tmp/trips.parquet')`. The query graph printer and EXPLAIN
output use `displayName()` for human-readable display.

### EXPLAIN Output

The `TableFunctionNode` appears in the logical plan. After query graph
construction, it becomes a `BaseTable` and from there a regular `TableScan`.

```
-- LOGICAL: Shows the TableFunctionNode.
SQL> EXPLAIN (TYPE LOGICAL) SELECT count(*) FROM read_parquet('/tmp/trips.parquet');

- Aggregate() -> ROW<count:BIGINT>
    count := count()
  - TableFunction: read_parquet('/tmp/trips.parquet') -> ROW<col1:BIGINT, col2:VARCHAR>

-- GRAPH: Shows the BaseTable (TVF has been resolved to a table).
SQL> EXPLAIN (TYPE GRAPH) SELECT count(*) FROM read_parquet('/tmp/trips.parquet');

t1: 2964624.00 rows, <no output columns>
  table: read_parquet('/tmp/trips.parquet')

-- OPTIMIZED: Shows the physical plan with TableScan.
SQL> EXPLAIN (TYPE OPTIMIZED) SELECT count(*) FROM read_parquet('/tmp/trips.parquet');

  Aggregation [1.00 rows] -> dt1.count
      dt1.count := count()
    TableScan [2964624.00 rows] ->
      table: read_parquet('/tmp/trips.parquet')

-- EXECUTABLE (default): Shows the Velox multi-fragment plan.
SQL> EXPLAIN SELECT count(*) FROM read_parquet('/tmp/trips.parquet');

Fragment 0: numWorkers=4:
-- PartitionedOutput[SINGLE Presto] -> count:BIGINT
  -- Aggregation[PARTIAL count := count()] -> count:BIGINT
    -- TableScan[table: read_parquet('/tmp/trips.parquet')] ->
       Estimate: 2964624 rows

Fragment 1: numWorkers=1:
-- Aggregation[FINAL count := count("count")] -> count:BIGINT
  -- Exchange[Presto] -> count:BIGINT
```

### Query Processing Flow

```
SQL:  SELECT * FROM read_parquet('/tmp/jan.parquet', '/tmp/feb.parquet')

1. SQL Parser + RelationPlanner (PrestoParser.cpp):
   Parses SQL into AST, then converts to logical plan.
   Resolves connector via 3-part name (or default).
   Evaluates argument expressions to constant values.
   Calls connector.resolveTableFunction("read_parquet", args)
     → connector reads Parquet headers, validates schemas match
     → returns outputType (e.g., ROW(col1 BIGINT, col2 VARCHAR))
   Applies typed column aliases if present.
   Emits TableFunctionNode in the logical plan.

2. Optimizer — ToGraph (ToGraph.cpp):
   Calls connector.findTableFunction("read_parquet", args, outputType)
     → connector returns a Table with columns, layouts, stats
   Creates BaseTable in the query graph — same path as regular tables.
   Table is scoped to the query, not cached.

3. Optimizer — Plan Enumeration:
   Join ordering, filter pushdown, column pruning — all unchanged.
   Produces TableScan RelationOp referencing the BaseTable.

4. Optimizer — ToVelox (ToVelox.cpp):
   Creates Velox TableScanNode via layout->createColumnHandle() and
   layout->createTableHandle() — same as regular tables.

5. Runner (LocalRunner.cpp):
   Creates splits via ConnectorSplitManager::getSplitSource(tableHandle).
   Standard Velox TableScan execution — no changes needed.
```

### Velox Changes

**None required.** File-reading TVFs are lowered to `TableScanNode` before
reaching Velox. The Velox execution engine, `TableScan` operator, and
`HiveConnector` all work unchanged.

The only Velox-adjacent change is in the connector metadata interface
(`resolveTableFunction` and `findTableFunction`), which lives in the
connector layer shared between Axiom and Velox.

## Implementation Plan

### Connector Interface

1. Add `resolveTableFunction` and `findTableFunction` to
   `ConnectorMetadata`.
2. Add `displayName()` virtual method to `Table`.

### SQL Parser

3. Add `tableFunctionInvocation` grammar rule and AST node in
   `PrestoSql.g4` and `AstBuilder` visitor.
4. Add typed `columnAliases` extension to the grammar.
5. Handle `TableFunctionInvocation` AST node in `RelationPlanner` —
   resolve connector via 3-part name, call `resolveTableFunction`, apply
   typed column aliases, emit `TableFunctionNode`.

### Logical Plan

6. Add `TableFunctionNode`.

### Optimizer

7. Handle `TableFunctionNode` in `ToGraph` — call `findTableFunction`,
   create `BaseTable` from the returned `Table`. Insert cast expressions
   in `DerivedTable` for any type mismatches.
8. Update EXPLAIN printers to use `Table::displayName()` for
   GRAPH, OPTIMIZED, and EXECUTABLE output.

### Hive Connector

9. Implement `resolveTableFunction` and `findTableFunction` for
   `read_parquet`, `read_dwrf`, `read_csv`, `read_tsv`, and `read_text`.
   Schema inference via Velox's `dwio::common` readers for binary formats;
   header-row inference for text formats. Support multiple file paths and
   options map.


## Testing

The TestConnector is extended with `resolveTableFunction` and
`findTableFunction` to enable unit testing of the TVF framework without
depending on the Hive connector or real files. The TestConnector provides
a `table(name)` TVF that returns an in-memory table previously registered
via `CREATE TABLE`:

```sql
use test.default;
CREATE TABLE t AS SELECT * FROM UNNEST(sequence(1, 100)) AS t(a);

-- These are equivalent:
SELECT * FROM t;
SELECT * FROM table('t');
```

This enables testing:
- Parser: `tableFunctionInvocation` grammar rule, typed column aliases.
- RelationPlanner: connector resolution, `resolveTableFunction` call,
  typed column alias application.
- ToGraph: `findTableFunction` call, `BaseTable` creation.
- Optimizer: filter pushdown, column pruning, join ordering — all through
  TVF-produced tables.
- EXPLAIN: `displayName()` output at all levels.
- Typed column aliases: rename and cast modes.

```sql
-- Test typed column aliases with cast.
SELECT * FROM table('t') AS t(id BIGINT)

-- Test TVF in joins.
SELECT * FROM table('t1') AS a JOIN table('t2') AS b ON a.id = b.id

-- Test filter on TVF result.
SELECT * FROM table('t') AS t(id INTEGER) WHERE id > 50
```

## Design Decisions

### Computational TVFs are out of scope

Computational TVFs like `generate_series(1, 100)` or `json_each(json)` are
already expressible as `UNNEST(scalar_function(...))` in Presto/Velox. Adding
dedicated TVF syntax for these provides no new functionality — only syntactic
sugar. File-reading TVFs, on the other hand, genuinely cannot be expressed any
other way.

### TVF return types depend on argument values, not just types

Unlike scalar and aggregate functions, where the return type is determined
by argument **types** alone (e.g., `abs(BIGINT) → BIGINT`), TVF return types
depend on argument **values**. `read_parquet('/tmp/a.parquet')` and
`read_parquet('/tmp/b.parquet')` have the same argument type (VARCHAR) but
can return completely different schemas. The connector must inspect the actual
file to determine the output columns and types. This is why
`resolveTableFunction` receives evaluated constant values, not just type
signatures, and why TVF arguments must be constant expressions.

### TVFs are connector-scoped with 3-part naming

TVFs are resolved through the connector that owns them, using the same
3-part `catalog.schema.function` naming convention as tables. This avoids
ambiguity when multiple connectors provide functions with the same name
(e.g., a local Hive connector and an S3 connector both providing
`read_parquet`) and reuses the existing name resolution infrastructure.

DuckDB uses global catalog entries for TVFs (no qualification). Presto and
Trino use 3-part names consistent with tables. We follow Presto's approach
for consistency — the common case (`read_parquet(...)`) is simple, and
multi-connector queries use fully qualified names
(`mysql.default.query(...)`).

For the `query()` TVF, 3-part naming is especially useful: the catalog
identifies the connector, and the schema provides the default database
context for the SQL string:

```sql
-- 'mysql' routes to the MySQL connector; 'mydb' sets the default database.
SELECT * FROM mysql.mydb.query('SELECT * FROM users')
-- MySQL executes: SELECT * FROM mydb.users
```

### TVFs rewrite to TableScanNode, not a new operator

File-reading TVFs are rewritten to standard `TableScanNode` during
optimization, rather than introducing a new plan node type for execution.
This means the optimizer, runner, and Velox execution engine require no
changes — predicate pushdown, column pruning, split generation, and parallel
execution all work unchanged.

This follows Presto's `RewriteTableFunctionToTableScan` pattern. DuckDB
also reuses its `LogicalGet` operator for both table scans and TVFs.

### No Velox execution changes needed

Because file-reading TVFs lower to `TableScanNode` before reaching Velox,
the Velox execution engine is untouched. The only Velox-adjacent changes are
in the connector metadata interface (`resolveTableFunction` and
`findTableFunction`), which are new methods on `ConnectorMetadata`. If
computational TVFs are added in the future, Velox would need a new plan
node and operator.

### Logical plan stores no connector-specific data

The `TableFunctionNode` stores only the connector ID, function name,
evaluated arguments, and output type — all connector-agnostic, consistent
with how `TableScanNode` stores just `connectorId` + `SchemaTableName`.
Connector-specific artifacts (`ConnectorTableHandle`, column handles) are
produced during plan lowering when the optimizer calls
`findTableFunction`. The connector may re-read file headers at this point;
it can optionally cache metadata from `resolveTableFunction` to avoid
redundant I/O.

### Schema override uses typed column aliases, not CAST

We considered `CAST(read_csv(...) AS ROW(id INTEGER, ...))` for overriding
column types, but `CAST ... AS ROW(...)` produces a single struct-typed
column, not top-level columns. `SELECT id FROM CAST(read_csv(...) AS ROW(...))`
would not work — the struct would need to be dereferenced. Instead, we use
typed column definition lists in the alias:
`read_csv(...) AS t(id INTEGER, name VARCHAR)`, which matches the existing
`UNNEST` syntax and produces proper top-level columns.

### Typed column aliases are all-or-nothing

The column definition list in the alias supports two modes: rename only
(`AS t(id, name, amount)`) or rename + cast (`AS t(id INTEGER, name VARCHAR,
amount DOUBLE)`). Mixing entries with and without types is not allowed. This
avoids ambiguity: for Parquet files where columns already have non-VARCHAR
types, omitting a type could mean "keep the source type" or "keep VARCHAR",
which are different things. All-or-nothing keeps the semantics clear.

### Options use MAP, not named parameters

DuckDB uses `key=value` syntax for TVF options (e.g.,
`read_csv('file.csv', sep='|', header=true)`). This is not valid Presto SQL.
Rather than extending the parser with a new named-parameter syntax, we use a
standard `MAP(VARCHAR, VARCHAR)` argument for the `read_text` function. The
convenience functions (`read_csv`, `read_tsv`) have sensible defaults and
rarely need explicit options.

### No TRY(read_...) support

`TRY` in Presto/Velox wraps scalar expressions and returns NULL on error.
A TVF returns a relation, not a scalar — `TRY(read_csv(...))` would need to
produce an empty relation on error, which is a different semantic. Error
handling for missing or empty files should be done outside the query or via
future options (e.g., `ignore_missing => 'true'` in the options map).

### Empty files are an error

For text formats without a header row, the number of columns is inferred from
the first data row. An empty file has no rows, so the schema cannot be
inferred. Rather than silently returning an empty result with zero columns,
we report an error. Users can provide an explicit schema via typed column
aliases to handle files that may be empty:
`read_csv('/tmp/data.csv') AS t(id INTEGER, name VARCHAR)`.

## Future Extensions

### Engine (Axiom)

- **Lateral joins with file-reading TVFs**: Support column references as
  TVF arguments with user-declared output schema. Requires a lateral-style
  execution operator in Velox.

  ```sql
  -- Read a different Parquet file for each row in the file_index table.
  SELECT t.label, r.*
  FROM file_index AS t, read_parquet(t.path) AS r(id INTEGER, name VARCHAR, amount DOUBLE)

  -- Join each customer's data file with the customer record.
  SELECT c.name, d.*
  FROM customers AS c, read_csv(c.data_path) AS d(order_id INTEGER, total DOUBLE)
  ```

- **Subquery arguments**: A simpler form of the above — a scalar subquery
  produces a constant value at plan time, avoiding the need for a
  lateral-style execution operator:

  ```sql
  SELECT * FROM read_parquet((SELECT array_agg(path) FROM file_index))
  ```

  The subquery is evaluated during planning and the result is passed to
  `resolveTableFunction` as a constant argument.

### Hive Connector

- **Glob patterns**: `read_parquet('/tmp/data/*.parquet')` — the connector
  expands the glob and returns multiple file paths.
- **Type inference for text formats**: Automatically infer numeric and date
  types from sampled text column values instead of defaulting to VARCHAR.
