# Plan: Materialized View Query Support with Partition Stitching in Axiom

## Problem Statement

Currently, Axiom treats a materialized view (MV) the same as a regular table — it reads all partitions directly from the MV's data table. When querying an MV that is **partially refreshed** (has missing or stale partitions), Axiom should transparently stitch data from the MV's base table for those partitions. The result should be a **UNION ALL** of:
- Fresh partitions read from the MV
- Missing/stale partitions read from the base table

**Example:** For `SELECT a, b FROM mv WHERE ds > '2026-01-20'`, if the MV only has fresh partitions up to `ds='2026-01-28'` (base table has data beyond that), the optimizer should rewrite this to:
```sql
SELECT a, b FROM mv WHERE ds > '2026-01-20' AND ds <= '2026-01-28'   -- fresh partitions from MV
UNION ALL
SELECT a_base, b_base FROM base_table WHERE ds > '2026-01-28'        -- missing partitions from base
```

---

## Existing Foundation (Already Committed)

The following are already in the current stack and can be used directly:

### D92019614 — `MaterializedViewDefinition` class
**File:** `axiom/connectors/MaterializedViewDefinition.h`

Full C++ definition with:
- `originalSql()` — the original SQL of the MV definition
- `schema()` / `table()` — the MV's data table (where MV data is stored)
- `baseTables()` — `vector<SchemaTableName>` of base tables the MV depends on
- `columnMappings()` — `vector<ColumnMapping>` mapping MV columns to base table columns
- `baseTablesOnOuterJoinSide()` — tables on outer side of outer joins
- `validRefreshColumns()` — columns used for incremental refresh (e.g., `ds`)

Supporting structs: `SchemaTableName`, `TableColumn`, `ColumnMapping`, `ViewSecurity`.

### D92085304 — MV info in ConnectorMetadata
**File:** `axiom/connectors/ConnectorMetadata.h`

- `ViewType` enum: `kLogicalView`, `kMaterializedView`
- `View` class now carries `viewType()` field
- `MaterializedViewDefinitionPtr` typedef (`shared_ptr<const MaterializedViewDefinition>`)
- `ConnectorMetadata::getMaterializedViewDefinition(name)` — virtual method returning `MaterializedViewDefinitionPtr`

**File:** `axiom/facebook/prism/PrismConnectorMetadata.h/.cpp`

- `PrismConnectorMetadata::getMaterializedViewDefinition()` — parses the JSON-encoded MV definition from metastore's `viewOriginalText` field. Already extracts `baseTables`, `columnMappings`, `owner`, `securityMode`, `baseTablesOnOuterJoinSide`, `validRefreshColumns`.

### D92541143 — Parser flow for MV
- `PrismConnectorMetadata::findTable()` now accepts MVs (only rejects logical views)
- `PrismView` carries `ViewType` for distinguishing logical vs materialized views
- PrestoParser updated to only expand logical views, not MVs
- MV is treated as a table scan, but no partition stitching yet

### Design Note: `MaterializedViewDefinition` stored on base `Table`, not `PrismTable`

The MV definition should be stored on the base `connector::Table` class in `ConnectorMetadata.h`,
**not** on `PrismTable`. The reason is dependency layering:

```
axiom/optimizer/  →  depends on  →  axiom/connectors/      (generic: Table, ConnectorMetadata)
                     does NOT depend on  →  axiom/facebook/prism/  (specific: PrismTable)
```

The optimizer's `MaterializedViewRewrite` pass is the primary consumer of MV metadata.
If the field were on `PrismTable`, the optimizer would need to `#include "axiom/facebook/prism/PrismTable.h"`
and downcast via `connectorTable->as<PrismTable>()`, breaking the connector-agnostic abstraction.

`PrismTable` already has Prism-specific fields (`parameters_`, `tableType_`, `partitionedByKeys_`),
but those are only accessed by Prism-layer code — not by the optimizer.

By placing it on `Table`, the optimizer accesses it generically:
```cpp
if (const auto& mvDef = baseTable->schemaTable->connectorTable->materializedViewDefinition()) {
  // This is an MV — do partition stitching.
}
```

And `PrismConnectorMetadata::findTable()` populates it during table resolution:
```cpp
auto table = updateTableCache(name, metadata);
if (metadata->isMaterializedView()) {
  auto mvDef = parseMaterializedViewDefinition(metadata->viewOriginalText());
  table->setMaterializedViewDefinition(std::move(*mvDef));
}
return table;
```

**Note:** `getMaterializedViewDefinition(name)` on `ConnectorMetadata` is still useful for callers
that start from a name (e.g., DDL operations like `REFRESH MATERIALIZED VIEW`), but the optimizer
should use the `Table`-based accessor to avoid extra metastore lookups.

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                    QUERY: SELECT a, b FROM mv WHERE ds > X          │
└──────────────────┬───────────────────────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────────────────────┐
│  1. Table Resolution (already done in D92541143 + new changes)      │
│     - MV resolved as a table via findTable()                        │
│     - MaterializedViewDefinition populated on Table during findTable│
└──────────────────┬───────────────────────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────────────────────┐
│  2. Optimizer Rewrite (NEW — MaterializedViewRewrite pass)          │
│     a. Detect MV BaseTable via Table::materializedViewDefinition()  │
│     b. Call ConnectorMetadata::getMaterializedViewStatus()           │
│        → Connector computes boundaries internally (partition-agnostic│
│        → Returns: refreshColumn + boundaryValues vector             │
│     c. Construct UNION ALL:                                         │
│        - Left child:  MV scan + filter (refreshCol <= boundary)     │
│        - Right child: base table scan + filter (refreshCol > bound) │
│        - Column mapping applied to base table scan                  │
│     d. Replace original BaseTable with UNION ALL DerivedTable       │
└──────────────────────────────────────────────────────────────────────┘
```

### Key Design: Partition-Agnostic `getMaterializedViewStatus()`

The optimizer does NOT access `SplitManager` or partition lists directly. Instead,
the connector encapsulates all partition/freshness logic behind a single call:

```cpp
// In axiom/connectors/ConnectorMetadata.h

enum class MaterializedViewState {
  kNotMaterialized,
  kTooManyPartitionsMissing,
  kPartiallyMaterialized,
  kFullyMaterialized,
};

struct MaterializedViewStatus {
  MaterializedViewState state;

  /// Boundary values defining fresh ranges in the MV. Each consecutive pair
  /// [boundaryValues[2i], boundaryValues[2i+1]] defines an inclusive range
  /// of the refresh column where the MV has data. The optimizer reads
  /// everything outside these ranges from the base table.
  /// The refresh column name is not included here — the optimizer gets it
  /// from MaterializedViewDefinition::validRefreshColumns().
  /// Only meaningful when state is kPartiallyMaterialized.
  ///
  /// Phase 1 (missing only): Single pair, e.g., {"2026-01-21", "2026-01-28"}
  ///   → MV: ds >= '2026-01-21' AND ds <= '2026-01-28'
  ///   → Base: ds < '2026-01-21' OR ds > '2026-01-28'
  ///
  /// Phase 2 (missing + stale): Multiple pairs for non-contiguous fresh ranges,
  ///   e.g., {"2026-01-21", "2026-01-24", "2026-01-26", "2026-01-28"}
  ///   → MV: (ds >= '..21' AND ds <= '..24') OR (ds >= '..26' AND ds <= '..28')
  ///   → Base: everything else (includes stale ds='2026-01-25')
  std::vector<std::string> boundaryValues;
};

// On ConnectorMetadata:
virtual std::optional<MaterializedViewStatus> getMaterializedViewStatus(
    const Table& mvTable) = 0;
```

**Why this design:**
- **Partition-agnostic**: The optimizer never sees partition names, handles, or
  freshness values. It only gets a column + boundaries, and constructs predicates.
- **Connector encapsulation**: The connector knows how to compute the boundaries:
  - **Hive/Prism**: Compares partition sets, finds fresh MV ranges.
  - **Iceberg (future)**: Uses snapshot metadata to determine refresh boundaries.
  - **Any future format**: Just produces a column + boundary values.
- **Same interface for both phases**: Phase 1 returns a single range (all existing
  MV partitions). Phase 2 returns potentially multiple ranges (only fresh partitions).
  The optimizer code handles both cases with the same range-building logic.
- **Stale partitions handled transparently**: In Phase 2, the connector excludes
  stale partitions from the boundary ranges. The optimizer treats them as missing
  — the base table scan covers everything outside the fresh ranges.

---

## Phase 1: Missing Partition Detection and Stitching

Phase 1 handles the case where the MV simply **doesn't have** certain partitions
that exist in the base table. This requires no changes to the partition freshness
pipeline — it only compares partition sets.

### 1.1 Add `MaterializedViewDefinition` to `Table`

**File:** `axiom/connectors/ConnectorMetadata.h/.cpp`

Add an optional `MaterializedViewDefinition` field to the base `Table` class:

```cpp
class Table : public std::enable_shared_from_this<Table> {
  // ... existing fields ...
  std::optional<MaterializedViewDefinition> materializedViewDefinition_;
public:
  const std::optional<MaterializedViewDefinition>& materializedViewDefinition() const;
  void setMaterializedViewDefinition(MaterializedViewDefinition definition);
};
```

### 1.2 Populate `MaterializedViewDefinition` in `findTable()`

**File:** `axiom/facebook/prism/PrismConnectorMetadata.cpp`

In `findTable()`, after `updateTableCache()` creates the table, check if the table
is a materialized view and populate the definition. The parsing logic already exists
in `parseMaterializedViewDefinition()` (used by `getMaterializedViewDefinition()`).

### 1.3 New Optimizer Pass: `MaterializedViewRewrite`

**File:** `axiom/optimizer/MaterializedViewRewrite.h` (NEW)
**File:** `axiom/optimizer/MaterializedViewRewrite.cpp` (NEW)

```cpp
class MaterializedViewRewrite {
public:
  MaterializedViewRewrite(
      QueryGraphContext& context,
      const connector::SchemaResolver& schemaResolver);

  /// Traverses the DerivedTable tree and rewrites any MV BaseTable
  /// into a UNION ALL of MV partitions + missing base table partitions.
  /// Returns true if any rewrite was performed.
  bool rewrite(DerivedTable& root);

private:
  bool rewriteBaseTable(DerivedTable& parent, BaseTable* mvTable);

  /// Resolves the base table's SchemaTable.
  const SchemaTable* resolveBaseTable(
      const connector::SchemaTableName& baseTableName);

  /// Builds column mapping: for each MV column, find the corresponding
  /// base table column using MaterializedViewDefinition::columnMappings().
  folly::F14FastMap<std::string, std::string> buildColumnNameMapping(
      const connector::MaterializedViewDefinition& mvDef,
      const connector::SchemaTableName& baseTableName);

  /// Creates an IN-list filter for a set of partition values.
  ExprCP makePartitionFilter(
      const std::vector<std::string>& partitionValues,
      const Column* partitionColumn);

  QueryGraphContext& context_;
  const connector::SchemaResolver& schemaResolver_;
};
```

#### Algorithm for Phase 1 — `rewriteBaseTable()` (with `getMaterializedViewStatus` approach):

```
1. Get MV definition from Table object:
   mvDef = baseTable->schemaTable->connectorTable->materializedViewDefinition()
2. If mvDef has no value → not an MV, skip.

3. Call ConnectorMetadata::getMaterializedViewStatus(mvTable, queryFilter):
   → Connector internally compares MV partitions vs base table partitions.
   → Returns: MaterializedViewStatus { refreshColumn="ds",
       boundaryValues={"2026-01-21", "2026-01-28"} }
   → Returns std::nullopt if MV covers everything (no stitching needed).

4. If result is nullopt → MV is complete for this query, skip.

5. Build column name mapping from mvDef.columnMappings().

6. Construct range predicates from boundaryValues:
   - For each pair [boundaryValues[2i], boundaryValues[2i+1]]:
     mvRanges += (refreshCol >= val[2i] AND refreshCol <= val[2i+1])
   - basePredicate = NOT (mvRanges)  // everything outside the fresh ranges

   Phase 1 example (single pair {"2026-01-21", "2026-01-28"}):
     MV filter:   ds >= '2026-01-21' AND ds <= '2026-01-28'
     Base filter:  ds < '2026-01-21' OR ds > '2026-01-28'

   Phase 2 example (two pairs {"2026-01-21", "2026-01-24", "2026-01-26", "2026-01-28"}):
     MV filter:   (ds >= '..21' AND ds <= '..24') OR (ds >= '..26' AND ds <= '..28')
     Base filter:  everything else (includes stale ds='2026-01-25')

7. Construct UNION ALL DerivedTable:
   a. mvDt (left child): DerivedTable containing original BaseTable
      - Add mvRanges filter + existing user filters

   b. baseDt (right child): DerivedTable containing new BaseTable for base table
      - Resolve base table via SchemaResolver
      - Create BaseTable with base table's columns
      - Add basePredicate filter + existing user filters (translated via column mapping)
      - Apply column mapping (base col names → MV col names) via DerivedTable.exprs

   c. parentDt: DerivedTable with setOp = kUnionAll, children = [mvDt, baseDt]
      - columns match original MV query output

8. Replace original BaseTable in parent DerivedTable with parentDt.
```

**Key insight:** The optimizer never deals with partition lists. It receives
a column + boundary vector from the connector and constructs range predicates.
The same logic handles both Phase 1 (single range) and Phase 2 (multiple ranges
with gaps for stale partitions).

#### Alternative Algorithm — Direct `SplitManager` Approach (for reference):

The alternative approach has the optimizer access partitions directly instead of
going through `getMaterializedViewStatus()`. This is kept here for comparison.

```
1. Get MV definition from Table object (same as above).
2. Get the MV's partition column from mvDef.validRefreshColumns() (e.g., "ds").
3. Fetch MV partition list via SplitManager (using query's WHERE predicate).
   → Returns: {"ds=2026-01-21", "ds=2026-01-22", ..., "ds=2026-01-28"}
4. Fetch base table partition list via SplitManager (using same WHERE predicate).
   → Returns: {"ds=2026-01-21", "ds=2026-01-22", ..., "ds=2026-01-30"}
5. Compute missing partitions = base table partitions − MV partitions.
   → Missing: {"ds=2026-01-29", "ds=2026-01-30"}
6. If no missing partitions → skip (MV is complete for this query).
7. Build column name mapping from mvDef.columnMappings().
8. Construct UNION ALL DerivedTable:
   a. mvDt: keep existing filters (missing partitions don't exist in MV)
   b. baseDt: add partitionCol IN (missing partition values)
   c. parentDt: setOp = kUnionAll, children = [mvDt, baseDt]
9. Replace original BaseTable with parentDt.
```

**Tradeoffs vs `getMaterializedViewStatus()`:**
- **Pro:** More precise — uses exact IN-list filters instead of range predicates,
  potentially fewer partitions read from base table.
- **Con:** Optimizer depends on SplitManager/partition concepts, not partition-agnostic.
- **Con:** Doesn't support Iceberg or other formats without named partitions.
- **Con:** The optimizer currently doesn't hold a `tableHandle` needed for the
  standard `SplitManager::listPartitions()` call. Would need to use the
  Prism-specific overload `listPartitions(layout, subfieldFilters)` which
  breaks connector-agnostic abstraction.

### 1.4 Integration Point

**File:** `axiom/optimizer/Optimization.cpp`

The optimization pipeline starts in the `Optimization` constructor, which builds the
query graph and initializes plans:

```cpp
// Optimization constructor (simplified):
root_ = toGraph_.makeQueryGraph(*logicalPlan_);
root_->initializePlans();
```

Then `Optimization::bestPlan()` performs join planning and returns the best plan:

```cpp
PlanP Optimization::bestPlan() {
  PlanObjectSet targetColumns;
  targetColumns.unionObjects(root_->columns);
  topState_.dt = root_;
  topState_.setTargetExprsForDt(targetColumns);
  makeJoins(topState_);
  return topState_.plans.best();
}
```

The MV rewrite should be inserted **after** `initializePlans()` in the constructor
and **before** `bestPlan()` begins join planning. This ensures the UNION ALL
DerivedTable is in place before the optimizer plans joins and selects access paths.

```cpp
// In Optimization constructor, after initializePlans():
root_ = toGraph_.makeQueryGraph(*logicalPlan_);
root_->initializePlans();

// MV partition stitching rewrite.
MaterializedViewRewrite mvRewrite(context_, resolver_);
mvRewrite.rewrite(*root_);
```

### 1.5 Column Mapping Details

Using the existing `MaterializedViewDefinition::columnMappings()`:
```cpp
struct ColumnMapping {
  TableColumn viewColumn;                  // MV column
  std::vector<TableColumn> baseTableColumns; // base table source columns
};

struct TableColumn {
  SchemaTableName tableName;
  std::string columnName;
  std::optional<bool> isDirectMapped;
};
```

For each column in the MV query, find the matching `ColumnMapping` where
`viewColumn.columnName` matches. Then use `baseTableColumns[i].columnName`
(for the matching base table) as the source column in the base table scan.

When constructing `baseDt`:
- `baseDt->exprs` = base table column references (using base column names)
- `baseDt->columns` = shared with parent DerivedTable (using MV column names)

This makes the UNION ALL seamless — both children produce columns with the same
output names.

### 1.6 Build System

**File:** `axiom/optimizer/BUCK`

Add `MaterializedViewRewrite.cpp` to the optimizer library target.

### Phase 1 Files Changed

| File | Change Type | Description |
|------|-------------|-------------|
| `axiom/connectors/ConnectorMetadata.h/.cpp` | Modify | Add `materializedViewDefinition` field and accessor to base `Table` class |
| `axiom/facebook/prism/PrismConnectorMetadata.cpp` | Modify | Populate `MaterializedViewDefinition` on `Table` in `findTable()` |
| `axiom/optimizer/MaterializedViewRewrite.h` | **NEW** | MV rewrite pass header |
| `axiom/optimizer/MaterializedViewRewrite.cpp` | **NEW** | MV rewrite pass — missing partition detection and UNION ALL construction |
| `axiom/optimizer/Optimization.cpp` | Modify | Call `MaterializedViewRewrite` in optimization pipeline |
| `axiom/optimizer/BUCK` | Modify | Add new files to build |

---

## Phase 2: Stale Partition Detection via `partitionFreshness`

Phase 2 extends Phase 1 to also detect **stale** partitions — partitions that exist
in the MV but contain outdated data (base table was updated after the MV was last
refreshed). A partition is stale when `partitionFreshness == 2` (STALE) in the
metastore response.

### Key Design: No Optimizer Changes for Phase 2

With the `getMaterializedViewStatus()` approach, **no optimizer code changes are
needed** for Phase 2. The difference is entirely inside the connector:

- **Phase 1:** `getMaterializedViewStatus()` computes boundary ranges from the set of
  existing MV partitions.
- **Phase 2:** `getMaterializedViewStatus()` computes boundary ranges from the set of
  **existing AND fresh** MV partitions. Stale partitions are excluded from the fresh
  ranges, so the optimizer naturally reads them from the base table.

The optimizer receives the same `MaterializedViewStatus` struct (refreshColumn +
boundaryValues vector) and constructs the same UNION ALL — it doesn't know or care
whether the boundaries were computed from missing partitions, stale partitions, or
both.

### 2.1 Thread `partitionFreshness` through Prism partition pipeline

This is the connector-side work needed to make `getMaterializedViewStatus()` aware
of freshness.

#### 2.1.1 Extend `PartitionNameWithVersion` to include freshness

**File:** `axiom/facebook/prism/PrismCatalogClient.h`

```cpp
struct PartitionNameWithVersion {
  std::string partitionName;
  int64_t version;
  std::optional<int16_t> partitionFreshness; // 1=FRESH, 2=STALE
};
```

#### 2.1.2 Extract `partitionFreshness` from thrift response

**File:** `axiom/facebook/prism/metastore/client/PrismMetastoreClient.cpp`

In `co_getPartitionNamesWithVersionByFilter()`, the thrift
`PartitionNameWithVersion` response has field 6: `partitionFreshness`.
Currently only `partitionName` and `metadataVersion` are extracted.
Change the return type from `vector<pair<string, int64_t>>` to include
freshness.

#### 2.1.3 Use freshness in `getMaterializedViewStatus()` implementation

**File:** `axiom/facebook/prism/PrismConnectorMetadata.cpp`

When computing boundary ranges:
- Phase 1: All MV partitions are considered fresh (freshness not checked).
- Phase 2: Filter out stale partitions (freshness == 2) before computing ranges.
  The remaining fresh partitions form the boundary ranges returned to the optimizer.

### Alternative: Direct SplitManager Approach for Phase 2 (for reference)

If using the direct SplitManager approach (see Phase 1 alternative algorithm), Phase 2
would additionally require:

#### Propagate freshness to `PrismPartitionHandle`

**File:** `axiom/facebook/prism/PrismPartitionHandle.h/.cpp`

Add `partitionFreshness` field:
```cpp
class PrismPartitionHandle : public connector::PartitionHandle {
  // ... existing fields ...
  std::optional<int16_t> partitionFreshness_;
public:
  std::optional<int16_t> partitionFreshness() const;
};
```

**File:** `axiom/facebook/prism/PrismSplitManager.cpp`

In `listPartitions()`, propagate `partitionFreshness` from
`PartitionNameWithVersion` to `PrismPartitionHandle`.

#### Update `MaterializedViewRewrite` to use freshness

**File:** `axiom/optimizer/MaterializedViewRewrite.cpp`

Extend the rewrite algorithm to also check freshness:

```
Phase 1 (existing): missing = basePartitions − mvPartitions
Phase 2 (new):      stale   = {p ∈ mvPartitions | p.freshness == STALE}

Partitions to read from base table = missing ∪ stale
Partitions to read from MV         = mvPartitions − stale
```

When stale partitions exist, the MV scan now needs an **additional filter** to
exclude them:
- `partitionCol IN (fresh MV partition values)` — only read fresh partitions from MV
- `partitionCol IN (stale + missing partition values)` — read the rest from base table

**Tradeoff:** This approach requires optimizer changes for Phase 2 (filtering stale
partitions), whereas the `getMaterializedViewStatus()` approach does not.

### Phase 2 Files Changed

With `getMaterializedViewStatus()` approach:

| File | Change Type | Description |
|------|-------------|-------------|
| `axiom/facebook/prism/PrismCatalogClient.h` | Modify | Add `partitionFreshness` to `PartitionNameWithVersion` |
| `axiom/facebook/prism/metastore/client/PrismMetastoreClient.cpp` | Modify | Extract `partitionFreshness` from thrift response |
| `axiom/facebook/prism/PrismConnectorMetadata.cpp` | Modify | Use freshness when computing boundary ranges in `getMaterializedViewStatus()` |

Note: **No optimizer files changed** — the optimizer code from Phase 1 handles
Phase 2 transparently via the boundary vector.

---

## Testing Strategy

### Phase 1 Tests
- MV with all partitions present → no rewrite
- MV with some missing partitions → UNION ALL with base table for missing
- MV with no partitions at all → full base table scan (or fall back to base)
- Column mapping applied correctly (different column names in MV vs base)
- Non-partition filters preserved on both sides of UNION ALL

### Phase 2 Tests
- MV with all fresh partitions → no rewrite
- MV with some stale partitions → UNION ALL, MV side filtered to fresh only
- MV with mix of stale + missing → UNION ALL handles both correctly
- MV with all stale partitions → full base table scan

### Integration & Regression
- End-to-end test with mock metastore returning mixed partition states
- Run existing optimizer tests to verify no regressions

---

## Open Questions / Risks

1. **Multi-base-table MVs**: Initial implementation assumes a single base table. MVs defined as joins across multiple base tables would need `getMaterializedViewStatus()` to return per-base-table boundaries and the optimizer to construct a more complex rewrite. Suggest deferring to a future phase.
2. **Non-contiguous fresh ranges**: With the boundary vector approach, scattered stale partitions produce multiple fresh ranges (e.g., ds=21–24 fresh, ds=25 stale, ds=26–28 fresh). The optimizer constructs OR'd range predicates, which may be less efficient than precise IN-list filters. Acceptable for correctness; can be optimized later.
3. **`getMaterializedViewStatus()` latency**: The connector must fetch MV and base table partition lists to compute boundaries. This adds metastore RPCs during optimization. Consider caching partition metadata or making the call async. Also, if the MV is complete (no stitching needed), the call should be cheap — ideally a fast-path check before full partition comparison.
4. **Boundary value type**: The current design uses `std::string` for boundary values. This works for date-string partitions (`ds='2026-01-28'`) but may need generalization for numeric or timestamp partition columns. The optimizer needs to know the column type to construct correct comparison expressions.
5. **`queryFilter` parameter in `getMaterializedViewStatus()`**: The connector needs the query's WHERE clause to scope the partition comparison (e.g., only compare partitions where `ds > '2026-01-20'`). The expression type for this parameter needs to be defined — the optimizer works with `ExprCP`, but the connector layer doesn't understand optimizer expressions. May need a lightweight filter representation or the connector fetches all partitions and lets the optimizer intersect with the query filter.
6. **Column mapping for partition/refresh columns**: If the MV's refresh column has a different name than the base table's partition column, the column mapping in `MaterializedViewDefinition` must handle this. The boundary's `refreshColumn` refers to the MV column name; the base table scan needs to translate it via the mapping.
7. **Empty or fully-stale MV**: If `boundaryValues` is empty (no fresh data at all), the optimizer should eliminate the MV side entirely and read everything from the base table. Need to handle this edge case cleanly.
8. **UNION ALL overhead**: The UNION ALL rewrite adds a set operation to every MV query, even when only a small number of partitions are missing. For queries where the MV covers 99% of partitions, this is acceptable. But if the optimizer can detect that the base table side would be empty (all partitions within the query's range are in the MV), it should skip the rewrite entirely — this is handled by `getMaterializedViewStatus()` returning `std::nullopt`.
9. **Interaction with existing optimizer passes**: The MV rewrite inserts a UNION ALL DerivedTable into the query graph. Need to verify this doesn't interfere with subsequent optimization passes (join planning, predicate pushdown, constant folding). The rewrite runs after `initializePlans()` but before `bestPlan()`'s `makeJoins()` — verify this ordering is safe.
