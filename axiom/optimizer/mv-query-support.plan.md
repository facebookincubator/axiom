# Design: Materialized View Query Support in Axiom

## Scope

Axiom supports **querying** materialized views. Creating MVs (`CREATE MATERIALIZED VIEW`)
and refreshing MVs (`REFRESH MATERIALIZED VIEW`) are currently performed by the Presto
coordinator. This design covers only the query path — MV creation and refresh support
in Axiom are separate future work items.

Axiom reads the MV definition and partition data that Presto has already written to the
Hive metastore.

## Overview

Axiom supports querying materialized views (MVs) with automatic **partition stitching**:
when an MV is partially refreshed (some partitions are missing or stale), Axiom
transparently reads fresh data from the MV and fills in the gaps from the base table.

**Important:** The stitching happens in the **optimizer**, not the parser. The parser
treats an MV as a plain table scan — it does not rewrite the SQL or expand the MV's
definition. The optimizer's `MaterializedViewRewrite` pass inspects the table's metadata,
queries the connector for partition freshness, and transforms the **query graph**
(DerivedTable tree) by replacing the MV BaseTable node with a UNION ALL of MV + base
table scans when needed.

### Example

An MV `mv_sales` is defined as `SELECT region, ds, SUM(revenue) AS total FROM sales
GROUP BY region, ds`, partitioned by `ds`. The base table `sales` has data for
`ds='2026-01-21'` through `ds='2026-01-31'`.

```sql
SELECT * FROM mv_sales WHERE ds > '2026-01-20'
```

The parser produces a simple plan:

```
TableScan(mv_sales)
  filter: ds > '2026-01-20'
```

The `MaterializedViewRewrite` pass then transforms the query graph depending on the
MV's freshness status:

#### Case 1: MV not refreshed (empty)

The MV has no data at all. The rewrite **replaces** the MV scan with a base table
scan. Column names are mapped via the MV definition (e.g., `total` -> `revenue`).

```
TableScan(sales)                          -- reads from base table instead
  filter: ds > '2026-01-20'              -- user filter preserved
  project: region, ds, revenue AS total  -- column mapping applied
```

#### Case 2: Partially materialized

The MV has been refreshed for `ds='2026-01-21'` through `ds='2026-01-28'`, but
`ds='2026-01-29'` through `ds='2026-01-31'` are missing.

The `MaterializedViewRewrite` pass builds a **UNION ALL** query graph:

```
UNION ALL
+-- TableScan(mv_sales)                        -- fresh MV partitions
|     filter: ds > '2026-01-20'                -- user filter (pushed down)
|             AND ds >= '2026-01-21'           -- range: MV has data here
|             AND ds <= '2026-01-28'
+-- TableScan(sales)                           -- missing partitions from base
      filter: ds > '2026-01-20'                -- user filter (pushed down)
              AND (ds < '2026-01-21'           -- complement: outside MV range
                   OR ds > '2026-01-28')
      project: region, ds, revenue AS total    -- column mapping applied
```

The user's `WHERE ds > '2026-01-20'` filter is pushed into both sides automatically
by `distributeConjuncts()`, which runs after the MV rewrite.

#### Case 3: Fully materialized

The MV covers all partitions in the base table. The rewrite leaves the query graph
unchanged:

```
TableScan(mv_sales)
  filter: ds > '2026-01-20'
```

No rewrite is needed — the MV is scanned as-is.

---

## Preconditions for Correct Partition Stitching

Partition stitching produces correct results only when the following conditions hold.
These are checked or assumed by the `MaterializedViewRewrite` pass.

1. **Single base table.** The MV must depend on exactly one base table. MVs defined
   as joins across multiple tables are not yet supported — the data structures store
   multiple `baseTables`, but the rewrite and connector only process the first one.

2. **Refresh columns defined.** The MV definition must include `refreshColumns` with
   at least one partition column (e.g., `ds`). Without this, the rewrite cannot
   determine which partitions are fresh.

3. **Complete column mapping.** Every MV output column must map to exactly one base
   table column via `columnMappings`. This ensures that the base table side can
   produce rows with the same output schema as the MV side.

4. **Partition column exists in both tables.** The refresh column must exist in both
   the MV and the base table (possibly under different names, resolved via column
   mappings) with compatible types.

5. **MV partition values are a subset of base partition values.** An MV partition value
   that does not exist in the base table is ignored during boundary computation. Stale
   partition detection (MV has data but it is outdated) is deferred to Phase 2.

**Correctness argument.** When these preconditions hold, the UNION ALL of
`MV(fresh partitions) UNION ALL Base(complement partitions)` is equivalent to querying
the base table directly:
- The MV range filter (`refreshCol >= lo AND refreshCol <= hi`) selects exactly the
  fresh MV partitions.
- The base complement filter (`refreshCol < lo OR refreshCol > hi`) selects exactly the
  remaining partitions.
- Together, the two filters are **complementary and exhaustive** over the partition
  domain: every partition value in the base table is covered by exactly one side.
- For non-contiguous ranges with multiple `[lo, hi]` pairs: the MV filter uses OR'd
  ranges and the base filter uses AND'd complements (De Morgan), maintaining the same
  exhaustive coverage.

---

## End-to-End Flow

```
User query: SELECT * FROM mv_sales WHERE ds > '2026-01-20'
    |
    v
1. TABLE RESOLUTION (PrismConnectorMetadata::findTable)
    - Resolves "mv_sales" from metastore
    - Detects it is an MV (not a logical view)
    - Parses MaterializedViewDefinition from catalog JSON
    - Attaches definition to the Table object
    |
    v
2. SQL PARSING (PrestoParser)
    - Sees "mv_sales" via findTable() -> gets a Table (not expanded)
    - Logical views would be expanded; MVs are NOT expanded
    - Produces: LogicalPlan with TableScanNode("mv_sales")
    |
    v
3. QUERY GRAPH CONSTRUCTION (ToGraph::makeQueryGraph)
    - Converts LogicalPlan -> QueryGraph (DerivedTable tree)
    - mv_sales becomes a BaseTable node with its schema columns
    |
    v
4. MV REWRITE (MaterializedViewRewrite, runs before initializePlans)
    - Detects BaseTable carries a MaterializedViewDefinition
    - Calls ConnectorMetadata::getMaterializedViewStatus(mvTable)
      -> Connector fetches MV + base partition lists, computes boundaries
      -> Returns: { kPartiallyMaterialized, ["2026-01-21", "2026-01-28"] }
    - Constructs UNION ALL DerivedTable:
      - MV child:   range filter ds >= '2026-01-21' AND ds <= '2026-01-28'
      - Base child:  complement  ds < '2026-01-21' OR ds > '2026-01-28'
      - Column name mapping applied to base table side
    - Replaces original BaseTable with UNION ALL DerivedTable
    |
    v
5. PLAN INITIALIZATION (initializePlans)
    - distributeConjuncts() pushes user filters (ds > '2026-01-20')
      into both UNION ALL children automatically
    |
    v
6. OPTIMIZATION + EXECUTION
    - Join planning, predicate pushdown, split generation proceed normally
    - Both sides produce columns with the same output names
```

---

## Data Structures

### MaterializedViewDefinition

Stored on the `Table` object. Contains structured metadata parsed from the Hive metastore
catalog (not from SQL parsing -- see "Metadata Source" below).

```
axiom/connectors/MaterializedViewDefinition.h
namespace facebook::axiom::connector

class MaterializedViewDefinition:
  originalSql_        : string        // Original CREATE MV SQL (for diagnostics only; never parsed)
  schema_             : string        // MV's schema name (from metastore JSON; may duplicate Table::name_)
  table_              : string        // MV's table name (from metastore JSON; may duplicate Table::name_)
  baseTables_         : vector<SchemaTableName>  // Base tables the MV depends on
  columnMappings_     : vector<ColumnMapping>    // MV column -> base column mappings
  baseTablesOnOuterJoinSide_ : vector<SchemaTableName>  // See note below
  refreshColumns_     : optional<vector<string>> // Partition columns for refresh (e.g., "ds")
  owner_              : optional<string>
  securityMode_       : optional<ViewSecurity>   // INVOKER or DEFINER
```

**Notes:**
- `schema_` and `table_` are sourced from the metastore JSON blob and may duplicate the
  containing `Table` object's name. They are kept because `MaterializedViewDefinition` is
  parsed independently before the `Table` is fully constructed.
- `baseTablesOnOuterJoinSide_`: Tables on the outer side of an outer join in the MV
  definition. Populated from the metastore JSON when the MV involves outer joins.
  Currently unused — needed when multi-base-table (join) MVs are supported, to determine
  which base tables have nullable columns due to outer join semantics, affecting refresh
  correctness and column mapping.
- `originalSql_`: Stored for debugging/diagnostics via `toString()`. Not parsed or used
  in query processing.

**Supporting types:**

```
struct SchemaTableName { string schema; string table; }
    // Uses axiom::SchemaTableName from common/SchemaTableName.h

struct TableColumn { SchemaTableName tableName; string columnName; optional<bool> isDirectMapped; }
    // Identifies a column by table name + column name (catalog-level, not runtime).
    // The tableName is repeated per column to match the Presto metastore JSON format 1:1,
    // which simplifies parsing and debugging.

struct ColumnMapping { TableColumn viewColumn; vector<TableColumn> baseTableColumns; }
    // Maps one MV column to its source column(s) in the base table
```

### MaterializedViewStatus

Returned by `ConnectorMetadata::getMaterializedViewStatus()`. Tells the rewrite pass
what action to take without exposing partition details.

```
axiom/connectors/ConnectorMetadata.h
namespace facebook::axiom::connector

enum class MaterializedViewState:
  kNotMaterialized          // MV has no data -> read base table entirely
  kTooManyPartitionsMissing // Too many gaps -> read base table entirely
  kPartiallyMaterialized    // Some data fresh -> stitch MV + base via UNION ALL
  kFullyMaterialized        // MV covers everything -> read MV only

struct MaterializedViewStatus:
  state           : MaterializedViewState
  boundaryValues  : vector<string>
    // Pairs of [lo, hi] defining inclusive ranges where MV has fresh data.
    // E.g., {"2026-01-21", "2026-01-28"} means MV has ds in [21, 28].
    // Multiple pairs for non-contiguous ranges:
    //   {"2026-01-21", "2026-01-24", "2026-01-26", "2026-01-28"}
    //   means MV has [21,24] and [26,28], but not 25.
    //
    // Values are compared lexicographically as strings. This is correct for
    // date-formatted partition keys (e.g., ds='2026-01-28'). Numeric partition
    // keys would require type-aware comparison (future work).
```

### ViewType

Distinguishes logical views (expanded at parse time) from materialized views (treated
as table scans).

```
enum class ViewType { kLogicalView, kMaterializedView }
```

The `View` class carries a `viewType()` field. The parser checks this: logical views
are expanded into their SQL definition; materialized views are left as table scans.

### Table extensions

The base `Table` class (in `ConnectorMetadata.h`) carries an optional MV definition:

```cpp
class Table {
  optional<MaterializedViewDefinition> materializedViewDefinition_;
public:
  const optional<MaterializedViewDefinition>& materializedViewDefinition() const;
  void setMaterializedViewDefinition(MaterializedViewDefinition def);
};
```

This is set during `findTable()` by the connector (e.g., `PrismConnectorMetadata`)
when it detects that a table is an MV. The optimizer accesses it generically via
`baseTable->schemaTable->connectorTable->materializedViewDefinition()` without
needing connector-specific downcasts.

**TODO:** Consider removing the setter and setting the MV definition during `Table`
construction or via a factory method in `findTable()`, to make `materializedViewDefinition_`
immutable like the other `Table` members.

---

## Metadata Source

The `MaterializedViewDefinition` metadata is **not produced by parsing the MV's SQL**
in Axiom. It comes from the **Hive metastore catalog** as pre-computed structured data.

### How it gets there

1. **MV creation (Presto coordinator):** `CREATE MATERIALIZED VIEW mv AS SELECT ...`
   causes Presto to analyze the SQL, compute column mappings, identify base tables,
   and write everything as a **base64-encoded JSON blob** into the metastore's
   `viewOriginalText` field.

2. **MV resolution (Axiom -- `PrismConnectorMetadata`):** When `findTable()` resolves
   an MV, `parseMaterializedViewDefinition()` **decodes the JSON** from the metastore.
   The JSON already contains structured fields:

   ```json
   {
     "originalSql": "SELECT ds, SUM(revenue) AS total FROM sales GROUP BY ds",
     "schema": "prod", "table": "mv_sales",
     "baseTables": [{"schema": "prod", "table": "sales"}],
     "columnMappings": [
       {"viewColumn": {"tableName": {"schema":"prod","table":"mv_sales"}, "columnName": "ds"},
        "baseTableColumns": [{"tableName": {"schema":"prod","table":"sales"}, "columnName": "ds"}]},
       {"viewColumn": {"tableName": {"schema":"prod","table":"mv_sales"}, "columnName": "total"},
        "baseTableColumns": [{"tableName": {"schema":"prod","table":"sales"}, "columnName": "revenue"}]}
     ],
     "validRefreshColumns": ["ds"]
   }
   ```

3. **Optimizer consumption:** The `MaterializedViewRewrite` pass reads only the
   structured fields. The `originalSql` field is stored for diagnostics but
   **never parsed** by any query processing code path.

---

## Component Details

### 1. Table Resolution

**File:** `PrismConnectorMetadata.cpp`

`findTable()` resolves MVs the same way as regular tables. It only rejects **logical
views** (which go through `findView()` instead). For MVs:

```cpp
TablePtr PrismConnectorMetadata::findTable(const SchemaTableName& tableName) {
  auto result = client_->co_resolveTable(ns, table);
  if (result.value()->isLogicalView()) {
    return nullptr;  // Logical views are NOT returned as tables
  }
  auto table = updateTableCache(tableName, result.value());
  if (result.value()->isMaterializedView()) {
    table->setMaterializedViewDefinition(
        *parseMaterializedViewDefinition(result.value()->viewOriginalText()));
  }
  return table;
}
```

### 2. Parser Handling

**File:** `PrestoParser` (SQL parser)

When resolving a table reference like `FROM mv_sales`:

1. Call `findTable("mv_sales")` -- succeeds for MVs, returns a Table with MV definition
2. If `findTable` returns null, call `findView("mv_sales")`
3. If the view is a **logical view** (`viewType() == kLogicalView`), expand it inline
4. If the view is a **materialized view**, do NOT expand -- treat as a table scan

This means MVs appear in the logical plan as `TableScanNode`, not as expanded subqueries.

### 3. Freshness Status Computation

**File:** `PrismConnectorMetadata.cpp`

`getMaterializedViewStatus()` computes the freshness boundaries:

```cpp
optional<MaterializedViewStatus>
PrismConnectorMetadata::getMaterializedViewStatus(const Table& mvTable) {
  // 1. Get refresh column from MV definition
  const auto& refreshColumn = mvDef->refreshColumns()->front();

  // 2. Fetch MV + base table partition lists in parallel
  auto [mvPartitions, basePartitions] = blockingWait(collectAll(
      client_->co_resolvePartitionNamesByFilter(mvNs, mvName, ""),
      client_->co_resolvePartitionNamesByFilter(baseSchema, baseTable, "")));

  // 3. Extract refresh column values from partition names
  set<string> mvValues, baseValues;
  // ... parse partition names like "ds=2026-01-21/country=US" ...

  // 4. Compute status via computeMaterializedViewStatus()
  return computeMaterializedViewStatus(mvValues, baseValues);
}
```

#### Algorithm: `computeMaterializedViewStatus()`

A pure function that compares two partition value sets and produces boundary ranges.

```
Input:  mvValues  : set<string>   -- partition values present in the MV
        baseValues: set<string>   -- partition values present in the base table

Algorithm:
  1. If mvValues is empty -> return kNotMaterialized.
  2. Compute freshValues = mvValues intersection baseValues.
  3. If freshValues == baseValues -> return kFullyMaterialized.
  4. Otherwise -> kPartiallyMaterialized. Compute boundary ranges:
     a. Sort freshValues lexicographically.
     b. Walk sorted values, grouping into contiguous ranges.
        Two values are "contiguous" if they are adjacent in the sorted
        baseValues set (no base value exists between them that is missing
        from the MV).
     c. Output: pairs [lo1, hi1, lo2, hi2, ...] for each contiguous range.

Correctness:
  - The MV range filter (refreshCol >= lo AND refreshCol <= hi) selects
    exactly the fresh partitions within each contiguous range.
  - The base complement filter (refreshCol < lo OR refreshCol > hi) selects
    exactly the remaining partitions.
  - For non-contiguous ranges: MV filter uses OR'd ranges, base filter uses
    AND'd complements (De Morgan duals), maintaining exhaustive coverage.
  - Together, every partition value in baseValues is covered by exactly one
    side — no gaps, no overlaps.
```

**Example:** MV has `{21, 22, 24, 25}`, base has `{21, 22, 23, 24, 25}`.
Result: `kPartiallyMaterialized`, boundaries = `["21", "22", "24", "25"]` (two ranges,
gap at 23).

**Note:** `getMaterializedViewStatus()` currently uses `blockingWait` which blocks the
calling thread. For queries referencing multiple MVs, statuses are fetched sequentially.
**TODO:** Add an async `co_getMaterializedViewStatus()` coroutine interface so the rewrite
pass can fetch all MV statuses concurrently via `collectAll`.

### 4. Optimizer Rewrite

**File:** `axiom/optimizer/MaterializedViewRewrite.h/.cpp`

The `MaterializedViewRewrite` pass runs **before** `initializePlans()` in the
`Optimization` constructor. This timing is critical: it runs after `ToGraph` builds
the query graph but before `distributeConjuncts()`, so user WHERE filters are naturally
pushed into both UNION ALL children.

```cpp
// In Optimization constructor:
root_ = toGraph_.makeQueryGraph(*planRoot);
MaterializedViewRewrite mvRewrite(toGraph_);
mvRewrite.rewrite(*root_);      // <-- MV rewrite here
root_->initializePlans();        // distributeConjuncts() pushes filters down
```

**TODO:** Change to a static function call: `MaterializedViewRewrite::rewrite(toGraph_, *root_);`
since the class holds no mutable state between calls.

The rewrite constructs DerivedTable/BaseTable nodes directly, using `ToGraph` only for
correlation name generation (`newCName()`) and schema lookups (`schema().findTable()`).
It does not call `ToGraph::makeQueryGraph()` because the rewrite operates on an
already-built query graph. The nodes are wired to match ToGraph's invariants (column
ownership, expr mapping, table registration) manually.

#### Rewrite algorithm

For each BaseTable with a `MaterializedViewDefinition`:

1. **Query status:** Call `getMaterializedViewStatus(mvTable)` on the connector.

2. **Dispatch on state:**
   - `kFullyMaterialized` -> no-op (scan MV as-is)
   - `kNotMaterialized` / `kTooManyPartitionsMissing` -> replace MV scan with full
     base table scan via `replaceWithBaseTable()`
   - `kPartiallyMaterialized` -> construct UNION ALL (step 3)

3. **Build UNION ALL** (for `kPartiallyMaterialized`):

   ```
   UNION ALL (DerivedTable, setOp = kUnionAll)
   +-- MV child (DerivedTable)
   |   +-- BaseTable(mv_sales)
   |       filter: ds >= '2026-01-21' AND ds <= '2026-01-28'
   +-- Base child (DerivedTable)
       +-- BaseTable(sales)
           filter: ds < '2026-01-21' OR ds > '2026-01-28'
           columns mapped: total -> revenue, ds -> ds
   ```

   - **MV child:** Wraps the original BaseTable, adds range filter from boundaries.
   - **Base child:** New BaseTable for the base table. Column names mapped via
     `columnMappings()` (e.g., MV's `total` maps to base's `revenue`). Complement
     filter added.
   - **Shared columns:** Both children share the same output Column objects owned
     by the UNION ALL DerivedTable.

4. **Multiple boundary ranges** (non-contiguous fresh data):
   - MV filter: OR'd ranges `(ds >= lo1 AND ds <= hi1) OR (ds >= lo2 AND ds <= hi2)`
   - Base filter: AND'd complements `(ds < lo1 OR ds > hi1) AND (ds < lo2 OR ds > hi2)`

5. **Replace in parent:** Swap original BaseTable for the UNION ALL DerivedTable.
   Remap all parent expression references (exprs, conjuncts, having, orderKeys).

---

## Supported View Shapes

The partition stitching is **shape-agnostic** -- it operates on partition boundaries
and column name mappings, not on the view's SQL structure.

| View Shape | Example MV Definition | How Stitching Works |
|---|---|---|
| Simple projection | `SELECT * FROM t` | 1:1 column mapping |
| Column subset | `SELECT a, ds FROM t` | MV has fewer columns; base reads only mapped columns |
| Column renaming | `SELECT a AS x FROM t` | `columnMappings` maps `x` -> `a` |
| Expressions | `SELECT a + b AS c FROM t` | MV stores computed `c`; base reads mapped source column |
| Filters | `SELECT * FROM t WHERE active` | MV data has filter applied; base reads full partitions |
| Aggregations | `SELECT ds, SUM(x) AS s FROM t GROUP BY ds` | MV has pre-aggregated rows; base returns raw rows |
| Joins | `SELECT ... FROM t1 JOIN t2` | **Not yet supported.** See Preconditions (#1) and Limitations. |

**Key design point:** The base table side returns **raw rows**, not re-computed MV
expressions. For aggregation MVs, the MV side returns pre-aggregated results
(e.g., `ds='01-21', total=100`) while the base side returns raw rows
(e.g., `ds='01-29', revenue=5`). If the user's query includes aggregation, the
query-level aggregation produces the correct final result. This matches Presto's
MV stitching behavior.

---

## Test Plan

### Unit tests for `computeMaterializedViewStatus()`

**File:** `fb_axiom/connectors/prism/tests/PrismConnectorMetadataTest.cpp`

Covers all `MaterializedViewState` values and boundary edge cases:
- Empty MV -> `kNotMaterialized`
- MV covers all base partitions -> `kFullyMaterialized`
- Missing partitions at start, end, and middle -> `kPartiallyMaterialized` with correct boundaries
- Multiple disjoint fresh ranges -> multiple boundary pairs
- Single-partition MV
- MV with values not present in base table (ignored in boundary computation)

### Unit tests for `MaterializedViewRewrite`

**File:** `axiom/optimizer/tests/MaterializedViewRewriteTest.cpp`

Verifies the query graph structure after rewrite for each `MaterializedViewState`:
- UNION ALL structure (correct children, set operation type)
- Filter expressions on MV and base sides
- Column mapping from MV names to base table names
- Parent reference remapping (exprs, conjuncts, orderKeys)

### End-to-end tests

Verify that stitched results match full base table scans:
- MV not refreshed (empty) -> results equal base table query
- MV partially refreshed -> results equal base table query
- MV fully refreshed -> results equal MV-only query

### Edge case tests

- MV with no refresh columns -> rewrite is skipped
- Mismatched schemas
- Empty base table
- Single-partition MV
- Non-contiguous fresh ranges with multiple gaps

---

## Key Files

| File | Role |
|---|---|
| `axiom/connectors/MaterializedViewDefinition.h` | `MaterializedViewDefinition`, `ColumnMapping`, `TableColumn` data structures |
| `axiom/connectors/ConnectorMetadata.h` | `MaterializedViewStatus`, `MaterializedViewState`, `ViewType` enums; `Table::materializedViewDefinition()` accessor; `getMaterializedViewStatus()` virtual method |
| `fb_axiom/connectors/prism/PrismConnectorMetadata.cpp` | `parseMaterializedViewDefinition()` (JSON decode); `getMaterializedViewStatus()` (partition comparison); `computeMaterializedViewStatus()` (boundary computation) |
| `fb_axiom/connectors/prism/PrismView.h` | `PrismView` -- carries `ViewType` for logical vs materialized |
| `axiom/optimizer/MaterializedViewRewrite.h/.cpp` | Rewrite pass: detects MV tables, builds UNION ALL, replaces in query graph |
| `axiom/optimizer/Optimization.cpp` | Integration point: calls `MaterializedViewRewrite` before `initializePlans()` |

---

## Limitations and Future Work

| Limitation | Description | Status |
|---|---|---|
| Multi-base-table MVs | MVs defined as joins across 2+ tables: `MaterializedViewDefinition` already stores multiple `baseTables`, but the rewrite (`rewriteBaseTable`, `replaceWithBaseTable`) and connector (`getMaterializedViewStatus`) currently only process the first base table. Extending requires per-base-table boundary computation and multi-way column mapping. | Future |
| No expression re-evaluation | Base table side returns raw rows, not re-computed MV expressions | By design (matches Presto) |
| No query-domain scoping | `getMaterializedViewStatus()` fetches all partitions, not scoped to query's WHERE clause | Future optimization |
| String-based boundary values | Boundary values are compared lexicographically. Correct for date-formatted partition keys (`ds='2026-01-28'`). Numeric partition keys would require type-aware comparison via Variant or a typed wrapper. | Future generalization |
| No MV auto-selection | User must reference the MV explicitly; optimizer does not automatically choose the best MV for a query | Future feature |
| No stale partition detection | Phase 1 only detects missing partitions. Phase 2 will use `partitionFreshness` from metastore to also exclude stale partitions from fresh ranges | Planned |
| Synchronous status fetching | `getMaterializedViewStatus()` uses `blockingWait`; queries with multiple MVs fetch statuses sequentially. Need async `co_getMaterializedViewStatus()` with batched `collectAll` in the rewrite pass. | TODO |
| Mutable Table setter | `setMaterializedViewDefinition()` mutates the `Table` after construction. Should set during construction to match the immutability of other `Table` members. | TODO |
| Static rewrite interface | `MaterializedViewRewrite` holds no mutable state; should use a static `rewrite(toGraph, root)` instead of object instantiation. | TODO |
