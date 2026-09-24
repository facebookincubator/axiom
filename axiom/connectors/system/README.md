# System Connector

## Overview

The system connector provides read-only access to runtime metadata —
active queries, session properties, and other operational state. The
application registers it under a catalog name of its choice (the CLI
uses `system`).

It also serves the `information_schema` relations of every other
catalog, since those describe schemas, tables and columns rather than
belonging to any one connector.

## Schemas and Tables

### system.metadata

Configuration metadata, including session-scoped properties and function
signatures.

| Table | Description |
|-------|-------------|
| `session_properties` | All registered session properties with current values, defaults, and descriptions. |
| `functions` | All registered function signatures with types, arguments, and metadata. |

### <catalog>.information_schema

What a catalog contains: its schemas, tables, views, and columns. A
query names these per catalog, as `hive.information_schema.columns`.
The parser resolves that to this connector, carrying the catalog in the
schema: `system."$info_schema@hive".columns`. Nobody types the resolved
name; `$info_schema@` is reserved so a catalog name cannot collide with
`runtime` or `metadata`.

| Table | Description |
|-------|-------------|
| `schemata` | One row per schema. |
| `tables` | One row per table or view, with its type. |
| `views` | One row per view, with the text it was defined with. |
| `columns` | One row per column of a table or view. |

#### Schema enumeration

`schemata` lists the schemas the catalog advertises, plus `information_schema`.
It accepts any filter, or none.

A catalog may accept schemas it does not advertise. TPC-H lists a fixed set of
scale factors, so `sf42` does not appear in `schemata` even though
`tpch.sf42.nation` can be queried.

#### Table metadata

The `tables` and `columns` relations describe the relations in
`information_schema` when a query names that schema and relation.

**A query must name the tables to describe.** Its filters have to pin
`table_schema` and `table_name`, and only these forms name them:

| Form | Example |
|------|---------|
| Equality | `table_name = 'orders'` |
| `IN` list | `table_name IN ('orders', 'lineitem')` |

Anything else leaves the query unbounded and fails at planning, with a
message naming the filter to add:

```
Querying information_schema.columns requires a filter naming table_name,
e.g. table_name = 't'
```

Unbounded forms include a pattern (`table_name LIKE 'o%'`), a range
(`table_name > 'o'`), a name that reaches the scan through an `OR` or a
subquery, and naming only one of the two columns. Naming neither fails
on `table_schema` first.

Filters on any other column — `data_type`, `table_catalog`, `is_nullable`
— narrow the result but name nothing, so they are applied above the scan
and never make a query servable on their own.

A named table the catalog does not have, or a named schema it does not
have, contributes no rows rather than failing. So does a filter no value
passes, such as `table_name IN ()`.

This is a deliberate divergence from Presto, which answers an unfiltered
query by listing every table in a schema and reading each one's
metadata. That does not hold up on a large catalog, so Axiom asks the
catalog only about names a query gives it.

#### Other restrictions

- Served for the v2 optimizer. The v1 optimizer offers a connector one
  filter at a time, so a query never names both the schema and the
  table, and it fails as unbounded.
- `DESCRIBE`, `SHOW COLUMNS` and `SHOW TABLES` resolve these relations
  like any other table, so `SHOW TABLES FROM hive.information_schema`
  lists them.
- `data_type` is spelled the way the type system spells it, lower-cased.
  A decimal reads as `decimal(10,2)`; complex types do not match
  Presto's spelling.
- `view_owner`, `column_default`, `comment` and `length` are always
  null: ownership, defaults, comments and declared widths are not
  tracked.
- A query reports the relation it scanned as a table of this connector,
  e.g. `system."$info_schema@hive".columns`, not as a table of the
  catalog it describes. Anything reading a query's referenced tables,
  such as an access check, sees that name.
- The rows are read through the catalog's globally registered metadata,
  with a session of this connector rather than of the catalog being
  described.

The rows come from catalog metadata, which only the coordinator can
read, so these scans are placed there.

### system.runtime

Runtime state that changes as the server operates.

| Table | Description |
|-------|-------------|
| `queries` | One row per active or recent query with state, timing, resource usage, and split progress. |

## Usage

```sql
-- Show all session properties for a component.
SELECT name, current_value, default_value
FROM system.metadata.session_properties
WHERE component = 'optimizer';

-- Find overridden session properties.
SELECT component, name, current_value, default_value
FROM system.metadata.session_properties
WHERE current_value <> default_value;

-- Distinct function names by kind.
SELECT DISTINCT name FROM system.metadata.functions
WHERE kind = 'aggregate' ORDER BY 1;

-- Find functions that accept a variable number of arguments.
SELECT DISTINCT name FROM system.metadata.functions
WHERE is_variadic ORDER BY 1;

-- List all active queries.
SELECT query_id, state, query, elapsed_time_ms
FROM system.runtime.queries;

-- Find a schema by name.
SELECT catalog_name, schema_name
FROM information_schema.schemata
WHERE schema_name = 'sales';

-- Columns of one table, as a client introspecting a schema asks for them.
SELECT column_name, data_type, ordinal_position
FROM information_schema.columns
WHERE table_schema = 'sales' AND table_name = 'orders'
ORDER BY ordinal_position;

-- Whether a name is a table or a view.
SELECT table_name, table_type
FROM hive.information_schema.tables
WHERE table_schema = 'sales' AND table_name IN ('orders', 'daily_orders');

-- The text a view was defined with.
SELECT view_definition
FROM information_schema.views
WHERE table_schema = 'sales' AND table_name = 'daily_orders';
```

## Table Schemas

### <catalog>.information_schema.schemata

| Column | Type | Description |
|--------|------|-------------|
| `catalog_name` | VARCHAR | Catalog the schema belongs to. |
| `schema_name` | VARCHAR | Schema name. |

### system.metadata.session_properties

| Column | Type | Description |
|--------|------|-------------|
| `component` | VARCHAR | Namespace prefix (e.g. `optimizer`, `execution`, connector ID). |
| `name` | VARCHAR | Property name. |
| `type` | VARCHAR | Property type (BOOLEAN, INTEGER, DOUBLE, STRING). |
| `default_value` | VARCHAR | Default value (empty string if none). |
| `current_value` | VARCHAR | Current session value (reflects SET SESSION overrides). |
| `description` | VARCHAR | Human-readable description. |

### system.metadata.functions

| Column | Type | Description |
|--------|------|-------------|
| `name` | VARCHAR | Function name. |
| `kind` | VARCHAR | `scalar`, `aggregate`, or `window`. |
| `return_type` | VARCHAR | Return type signature (e.g. `bigint`, `array(T)`). |
| `argument_types` | ARRAY(VARCHAR) | Argument type signatures. |
| `is_variadic` | BOOLEAN | Whether the function accepts a variable number of arguments. |
| `owner` | VARCHAR | Responsible team (empty if not set). |
| `properties` | VARCHAR | Type-specific metadata as JSON (e.g. `{"deterministic": true}`). |

One row per function signature (overload).

<details>
<summary>system.runtime.queries (30 columns)</summary>

| Column | Type | Description |
|--------|------|-------------|
| `query_id` | VARCHAR | Unique query identifier. |
| `state` | VARCHAR | Current state (QUEUED, RUNNING, FINISHED, FAILED). |
| `query` | VARCHAR | SQL text. |
| `catalog` | VARCHAR | Default catalog. |
| `schema` | VARCHAR | Default schema. |
| `user` | VARCHAR | User who submitted the query. |
| `source` | VARCHAR | Client source identifier (nullable). |
| `query_type` | VARCHAR | Statement type (SELECT, INSERT, etc.). |
| `planning_time_ms` | BIGINT | Time spent in planning (ms). |
| `optimization_time_ms` | BIGINT | Time spent in optimization (ms). |
| `queue_time_ms` | BIGINT | Time spent in queue (ms). |
| `execution_time_ms` | BIGINT | Time spent in execution (ms). |
| `elapsed_time_ms` | BIGINT | Total wall-clock time (ms). |
| `cpu_time_ms` | BIGINT | Total CPU time (ms). |
| `wall_time_ms` | BIGINT | Total wall time (ms). |
| `total_splits` | BIGINT | Total number of splits. |
| `queued_splits` | BIGINT | Splits waiting to run. |
| `running_splits` | BIGINT | Splits currently executing. |
| `finished_splits` | BIGINT | Splits completed. |
| `output_rows` | BIGINT | Rows returned to client. |
| `output_bytes` | BIGINT | Bytes returned to client. |
| `processed_rows` | BIGINT | Rows read from storage. |
| `processed_bytes` | BIGINT | Bytes read from storage. |
| `written_rows` | BIGINT | Rows written (INSERT queries). |
| `written_bytes` | BIGINT | Bytes written (INSERT queries). |
| `peak_memory_bytes` | BIGINT | Peak memory usage. |
| `spilled_bytes` | BIGINT | Bytes spilled to disk. |
| `create_time` | TIMESTAMP | When the query was created. |
| `start_time` | TIMESTAMP | When execution started (nullable). |
| `end_time` | TIMESTAMP | When execution finished (nullable). |

</details>

### <catalog>.information_schema.tables

| Column | Type | Description |
|--------|------|-------------|
| `table_catalog` | VARCHAR | Catalog the table belongs to. |
| `table_schema` | VARCHAR | Schema the table belongs to. |
| `table_name` | VARCHAR | Name of the table. |
| `table_type` | VARCHAR | `BASE TABLE` or `VIEW`. |

### <catalog>.information_schema.views

| Column | Type | Description |
|--------|------|-------------|
| `table_catalog` | VARCHAR | Catalog the view belongs to. |
| `table_schema` | VARCHAR | Schema the view belongs to. |
| `table_name` | VARCHAR | Name of the view. |
| `view_owner` | VARCHAR | Always null; ownership is not tracked. |
| `view_definition` | VARCHAR | The text the view was defined with. |

### <catalog>.information_schema.columns

| Column | Type | Description |
|--------|------|-------------|
| `table_catalog` | VARCHAR | Catalog the column's table belongs to. |
| `table_schema` | VARCHAR | Schema the column's table belongs to. |
| `table_name` | VARCHAR | Table the column belongs to. |
| `column_name` | VARCHAR | Name of the column. |
| `ordinal_position` | BIGINT | Position of the column, counting from one. |
| `column_default` | VARCHAR | Always null; defaults apply to writes. |
| `is_nullable` | VARCHAR | Always `YES`; nullability is not tracked. |
| `data_type` | VARCHAR | Type of the column, in lower case. |
| `comment` | VARCHAR | Always null; comments are not tracked. |
| `extra_info` | VARCHAR | What the connector says about the column's role, e.g. `partition key`. |
| `precision` | BIGINT | Digits an integer or decimal type holds, mantissa bits for a floating-point one; null for other types. |
| `scale` | BIGINT | Scale of a decimal; null for other types. |
| `length` | BIGINT | Always null; types carry no declared width. |

## Architecture

The system connector has two layers:

- **Velox layer** (`SystemConnector`): implements the Velox `Connector`
  interface. Dispatches `createDataSource()` to the appropriate data
  source based on the table being scanned.

- **Axiom metadata layer** (`SystemConnectorMetadata`): implements the
  Axiom `ConnectorMetadata` interface. Provides table discovery, column
  handles, table handles, and split management.

Each system table has a corresponding data source class that knows how
to populate its columns. The data source reads from a **provider
interface** — `QueryInfoProvider` for the queries table,
`SessionPropertiesProvider` for session properties, and
`FunctionsProvider` for function metadata. These interfaces
decouple the connector from the rest of the system: the connector
defines what data it needs, and the application supplies it.

At registration time, the application creates provider implementations
and passes them to `SystemConnector`. For example, the CLI creates an
adapter that reads from `SessionConfig` and passes it as the
`SessionPropertiesProvider`:

```
Registration (startup):
  Application creates provider adapters
    → passes them to SystemConnector
    → creates SystemConnectorMetadata(connector)
    → registers both globally

Query time:
  SQL "SELECT ... FROM system.metadata.session_properties"
    → optimizer resolves table via SystemConnectorMetadata::findTable()
    → creates SystemTableHandle + SystemColumnHandles
    → runner calls SystemConnector::createDataSource()
    → connector dispatches to SessionPropertiesDataSource
    → data source calls SessionPropertiesProvider::getSessionProperties()
    → returns rows
```

The information_schema relations take the same path but read another
catalog rather than a provider:

```
Query time:
  SQL "SELECT ... FROM hive.information_schema.columns WHERE ..."
    → parser resolves it to system."$info_schema@hive".columns
    → optimizer offers the filters to createTableHandle(), which takes the
      ones naming schemas and tables and rejects the rest, failing the
      query if what it took does not name the tables to describe
    → runner calls SystemConnector::createDataSource()
    → data source looks up each named table in the hive catalog's ConnectorMetadata
      and describes it, a batch of rows at a time
```
