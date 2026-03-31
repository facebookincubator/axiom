# Existence Pushdown into Derived Tables

This document describes the optimization that pushes existence semijoins inside
derived tables (subqueries) to reduce cardinality before aggregation.

## Motivation

Consider a query that joins a base table with a grouped subquery:

```sql
SELECT t.a, dt.x, dt.cnt
FROM t
JOIN (SELECT x, COUNT(*) AS cnt FROM u GROUP BY x) dt ON t.a = dt.x
```

Without optimization, the executor must:
1. Scan all rows of `u` and compute `GROUP BY x` over the full table.
2. Build a hash table on the grouped result.
3. Probe with `t` to find matching rows.

If `t` has a selective filter (e.g., `WHERE t.a < 100`), the full aggregation
of `u` is wasteful — most grouped rows will be discarded by the join. The
optimizer can do better by pushing an existence semijoin on `t.a` *inside* the
subquery, below the aggregation:

```
HashJoin (t.a = dt.x)
├── TableScan t
└── DerivedTable dt
    ├── Aggregation (GROUP BY x) → COUNT(*)
    │   └── HashJoin LEFT SEMI FILTER (u.x = t.a)  ← pushed inside
    │       ├── TableScan u
    │       └── TableScan t  ← existence copy
    └── ...
```

Now `u` is filtered by a semijoin against `t` before the GROUP BY runs,
reducing the number of groups and the amount of data aggregated.

This is safe because:
- The semijoin is on the grouping key (`x`), so it only removes entire groups.
- Using a semijoin (existence check) rather than a regular join preserves
  cardinality — no duplicate rows are introduced.
- The original join remains in place above the aggregation.

## The Optimization in Theory

The optimization applies whenever a derived table is joined to other tables
and the join key maps to an expression inside the derived table that can
serve as a filter below some operation boundary (e.g., aggregation).

The following subsections are organized to match the "Current Implementation
Scope" table below — each category here has a corresponding set of rows in
the table showing what is and isn't implemented.

### Join key constraints

The join key on the DT side determines whether pushdown is valid:

- **Grouping key**: safe — pushdown removes entire groups.
- **Aggregate expression**: invalid — can't push below the aggregation
  boundary on an aggregate result.
- **Window partition key**: safe — pushdown removes entire partitions, window
  computation within surviving partitions is unchanged.
- **Window non-partition key or no PARTITION BY**: invalid — changes the
  window computation (different row counts, different rankings).
- **Unnest table column**: invalid — unnest tables have special cross-join
  semantics that cannot accept arbitrary existence semijoins.
- **Multiple equality keys**: each conjunct on a grouping key could be pushed
  independently, or a composite existence could filter on all keys.
- **Non-equality filter**: a join with `ON dt.x = t.a AND dt.x > t.b` could
  push the equality part as an existence even if the non-equality filter
  (which references both sides) stays on the join.

### Join type constraints

- **Inner join, semi-join**: always OK.
- **Left/right join**: OK only if the DT is the optional side. If the DT is
  on the preserved side, pushing an existence from the optional table would
  incorrectly remove DT rows that should appear with NULLs.
- **Full outer join**: never OK — both sides must preserve unmatched rows.

### What can be pushed (`other`)

Any join partner of the DT can potentially be pushed:

- **Single base table**: added directly inside the DT.
- **DerivedTable (subquery)**: pushed as-is after re-planning.
- **Chain of tables**: if the join partner has its own join partners (e.g.,
  `t JOIN r ON t.fk = r.id`), the entire chain can be wrapped in a sub-DT
  and pushed as a single existence.
- **Multiple partners**: if the DT joins multiple tables on grouping keys,
  each can be pushed independently.

### What is the DT (subquery)

The derived table can be:
- A subquery with **GROUP BY** — the primary case.
- A **DISTINCT** subquery (GROUP BY all columns internally).
- A subquery with **window functions** (pushdown valid on partition keys).
- A **UNION ALL** subquery.
- A correlated subquery after decorrelation.

Pushdown is invalid when the subquery has **LIMIT** or **ORDER BY** — these
change which rows survive, so filtering below them changes the result set.

When multiple tables are joined to the DT, some may be pushable and others
not (e.g., one join key is a grouping key, another is an aggregate). The
current implementation uses all-or-nothing semantics: if any table cannot be
pushed, the entire pushdown is skipped. A future enhancement could push
what it can and leave the rest outside.

### Query patterns that benefit

In all examples below, the pushed table has a selective filter so the
existence semijoin meaningfully reduces cardinality inside the DT.

#### Inner join on grouping key

```sql
SELECT t.a, dt.x, dt.cnt
FROM t
JOIN (SELECT x, COUNT(*) AS cnt FROM u GROUP BY x) dt ON t.a = dt.x
WHERE t.b < 100
```

#### Correlated scalar subquery (after decorrelation)

```sql
SELECT *
FROM lineitem l, part p
WHERE l.l_partkey = p.p_partkey
  AND p.p_brand = 'Brand#23'
  AND l.l_quantity < (
    SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p.p_partkey
  )
```

After decorrelation, the correlated subquery becomes a DerivedTable with
`GROUP BY l_partkey`. The outer table `part` (filtered by brand) is pushed as
an existence semijoin inside the aggregation on `l_partkey`. This is TPC-H
Q17.

#### Multi-key join

```sql
SELECT t.a, t.b, dt.cnt
FROM t
JOIN (SELECT x, y, COUNT(*) AS cnt FROM u GROUP BY x, y) dt
  ON t.a = dt.x AND t.b = dt.y
```

Each equality conjunct on a grouping key could be pushed independently, or a
composite existence could filter on both keys simultaneously.

### TPC-H queries

#### Q2 — Minimum Cost Supplier

The correlated scalar subquery `ps.ps_supplycost = (SELECT MIN(...) FROM
partsupp ... WHERE p.p_partkey = ps.ps_partkey)` is decorrelated into a
DerivedTable with `GROUP BY ps_partkey`. The `part` table is pushed as a
semijoin inside the aggregation on `ps_partkey`, filtering `partsupp` rows
before the MIN aggregation.

#### Q17 — Small-Quantity-Order Revenue

The correlated subquery `l.l_quantity < (SELECT 0.2 * AVG(l_quantity) FROM
lineitem WHERE l_partkey = p.p_partkey)` becomes a DerivedTable with
`GROUP BY l_partkey`. The `part` table (filtered by brand and container) is
pushed as a semijoin inside the aggregation. Since only ~0.2% of parts match
the filter, this dramatically reduces the lineitem rows aggregated.

`TpchPlanTest::q17` verifies plan shape including `kLeftSemiFilter` inside the
lineitem aggregation. Other TPC-H tests verify query results but not plan
shape.

#### Q15, Q20 — Potential opportunities

Q15 (`supplier` joined to `revenue` grouped by `l_suppkey`) has the pattern
but `supplier` is unfiltered, so the cardinality benefit is minimal.

Q20 (`partsupp` joined to lineitem aggregation grouped by `(l_partkey,
l_suppkey)`) has a multi-key grouping. The `part` table (filtered by
`p_name LIKE 'forest%'`) could reduce lineitem rows before grouping if
pushed on `l_partkey`.

## Current Implementation Scope

The current implementation (`DerivedTable::import` and
`DerivedTable::pushExistencesIntoSubquery`) handles a practical subset of the
full optimization.

The table below lists optimization opportunities and correctness constraints.
"Yes" means the current implementation handles it; "No" means it is a valid
optimization but not yet implemented; "N/A" means pushdown is always invalid
(correctness constraint).

| # | Aspect | Supported | Test | Notes |
|---|--------|-----------|------|-------|
| | **Join key** | | | |
| 1 | Single equality key on grouping key | Yes | innerJoinGroupBy | Core case |
| 2 | Multi-key join (each key is a grouping key) | Yes | multiKeyJoin | Each key validated; all must resolve to same inner table |
| 3 | Equality key + non-equality filter | Yes | joinWithFilter | Equality pushed, filter stays outside |
| 4 | Join key maps to aggregate expression | N/A | aggregateKey | Can't push below aggregation boundary |
| 5 | Join key maps to unnest table column | N/A | unnestKey | Unnest has special cross-join semantics |
| | **Join type** | | | |
| 6 | Inner join | Yes | innerJoinGroupBy | |
| 7 | Semi-join (IN) | Yes | semiJoin | |
| 8 | Left/right join (DT is optional side) | Yes | leftJoinDtIsOptional | |
| 9 | Left/right join (DT is preserved side) | N/A | leftJoinDtIsPreserved | Would incorrectly remove rows that should appear with NULLs |
| | **What is pushed (`other`)** | | | |
| 10 | Base table | Yes | innerJoinGroupBy | Added directly to newFirst |
| 11 | DerivedTable (subquery) | No | otherIsDerivedTable | Optimizer doesn't choose pushdown |
| 12 | DerivedTable with unnest | No | otherIsUnnestDerivedTable | Optimizer doesn't choose pushdown |
| 13 | Chain of tables | Yes | chainJoin | Wrapped in `chainDt` |
| 14 | Multiple tables in one pass | Yes | multipleTables | Multiple loop iterations |
| 15 | Partial push (some pushable, some not) | No | partialPush | All-or-nothing: skips entire pushdown |
| | **What is the DT (subquery)** | | | |
| 16 | GROUP BY subquery | Yes | innerJoinGroupBy | Core case |
| 17 | DISTINCT subquery | Yes | distinctSubquery | GROUP BY variant |
| 18 | UNION ALL subquery | No | | Join keys reference union DT's columns, not children's |
| 19 | Window function (key is partition key) | Yes | windowSubquery | |
| 20 | Window function (key is not partition key) | N/A | windowNonPartitionKey | Changes window computation |
| 21 | Unnest GROUP BY on unnest output | No | unnestGroupBy | Optimizer doesn't choose pushdown |
| 22 | Subquery with LIMIT | N/A | limitOnFirstDt | Filtering changes which rows survive |
| 23 | Subquery with ORDER BY | N/A | orderByOnFirstDt | Filtering changes which rows survive |
| 24 | Recursive pushdown into nested subqueries | No | | Existence stops at the first aggregation level |

## Implementation Details

The optimization is split across two layers: a **discovery** layer that
identifies reducing existence opportunities and a **construction** layer that
builds the modified DerivedTable with existence semijoins pushed inside.

### Discovery: findReducingJoins

The optimizer discovers reducing joins using `findReducingJoins` in
`Optimization.cpp`, which runs two passes:

1. **Bushy reducing inner joins** (`findReducingBushyJoins`) — DFS from a
   start table over unplaced neighbors. If the cumulative fanout is below
   `kReducingPathThreshold`, the discovered tables are bundled into a bushy
   build side (`ReducingJoinsResult::bushyTables`).

2. **Reducing existences** (`findReducingExistences`) — a second DFS that
   also traverses already-placed tables and tables found in pass 1. For
   every reducing leaf whose cumulative fanout is below
   `kExistenceReductionThreshold`, a semi-join (existence) is recorded
   (`ReducingJoinsResult::existences`).

Both `reducingJoins` (called from `nextJoins` during join enumeration) and
`placeDerivedTable` (called when a DT is the start table) use
`findReducingJoins`. This ensures bushy tables go into `MemoKey.tables` and
existences go into `MemoKey.existences` — preventing infinite recursion that
occurred when existences were placed into `MemoKey.tables`.

The `importedExistences` set on each DerivedTable tracks tables that have
already been imported as existences, preventing re-discovery in subsequent
planning rounds. The `noImportOfExists` flag prevents importing existences
into a DT that was itself created as an existence container.

### Construction: DerivedTable::import

Called from `Optimization::makeDtPlan` when building a plan for a MemoKey.
Receives `firstTable` (the main table), `superTables` (all tables for this
plan), `existences` (reducing semijoin groups from discovery), and
`existsFanout` (cumulative fanout estimate).

Steps:
1. Copy tables, join order, and joins from the parent DT, filtered to the
   subset in `superTables` (via `copySubset`).
2. If `existences` is non-empty, add existence semijoins that filter
   `firstTable` at the current DT level. Single-table existences are added
   directly; multi-table existences are wrapped in their own DerivedTable
   via `makeExistsDtAndJoin`.
3. Set `noImportOfExists` to prevent recursive re-import.
4. If `firstTable` is a subquery and `this` is a passthrough DT
   (`isWrapOnly()`), flatten and return. Otherwise, call
   `pushExistencesIntoSubquery` to push the existence tables inside the
   subquery below its aggregation boundary.

### Construction: DerivedTable::pushExistencesIntoSubquery

Takes the other tables in `this` DT (alongside the subquery) and pushes them
inside the subquery as existence semijoins, below the aggregation boundary.

Uses all-or-nothing semantics: `validatePushdown` checks all tables first,
and if any table cannot be pushed, the entire pushdown is skipped. The
existences arrive as a single package with a single fanout estimate, so
partial pushdown may not achieve the expected cardinality reduction.

**`validatePushdown` checks:**

*DT-level guards* — no tables can be pushed when:
- The subquery has LIMIT or ORDER BY (these change which rows survive, so
  filtering below them changes the result set).

*Valid pushdown columns* — computed from the subquery's grouping keys and
window partition keys. When both aggregation and window are present, the
valid set is the intersection (the key must be safe for both operations).
When a window function has no PARTITION BY, it contributes no valid columns.

*Per-table checks* — each table must satisfy:
- Each equality key in the join must translate to a valid pushdown column. The
  key is translated through two levels of `replaceInputs`: first through the
  outer DT's projection (`this->columns/exprs`), then through the subquery's
  projection (`subquery.columns/exprs`). Multi-key joins are supported when
  all keys map to grouping keys (or window partition keys).
- All translated join keys must resolve to the same inner table.
- No translated join key resolves to an unnest table column (unnest tables
  have special cross-join semantics).

Note: non-equality filters do not block pushdown. The optimizer separates
equality and non-equality conditions before this code runs, so `join->filter()`
is empty. The equality gets pushed as the existence key; the non-equality
filter stays on the outer join.

**Steps:**
1. Call `validatePushdown`. If it returns false, skip pushdown entirely.
2. Create a mutable copy of the subquery.
3. For each validated join:
   a. Find transitive join partners of `other` via `joinChain`.
   b. **Simple case** (no chain): add `other` directly to the copy with an
      existence semijoin. If `other` is a DerivedTable, erase its memo entry
      and re-plan it first.
   c. **Chain case**: wrap `other` + its chain partners into a new `chainDt`,
      call `chainDt->import(...)` recursively, add an existence semijoin
      to `chainDt`.
   d. Remove imported tables from `this`.
4. Mark all initial tables in `importedExistences` to prevent re-discovery.
5. Replace `tables[0]` with the modified copy and flatten.

### Key helpers

- `makeExists` — creates an existence semijoin edge between two tables.
- `joinChain` — walks the join graph from `other` to find transitive join
  partners that must be moved together.
- `replaceInputs` — translates an expression from outer column references to
  inner expressions, crossing the DT projection boundary.

### joinChain explained

When pushing `other` inside the subquery, `other` might have its own join
partners within `this` DT. For example:

```sql
SELECT ...
FROM (SELECT x, COUNT(*) FROM big GROUP BY x) dt
JOIN small_a ON dt.x = small_a.a
JOIN small_b ON small_a.b = small_b.b
```

Here `small_a` joins the subquery directly, but `small_b` joins `small_a`
(not the subquery). `joinChain(small_a, ...)` discovers `small_b`. Both are
wrapped into a `chainDt` and pushed together as a single existence semijoin.

## Testing

Tests live in `ExistencePushdownTest.cpp` and use SQL queries via
`parseSelect` + `toSingleNodePlan`, verifying plan structure with
`AXIOM_ASSERT_PLAN` matchers. For "Yes" rows in the scope table, the test
asserts that `kLeftSemiFilter` appears inside the subquery's aggregation. For
"No" rows, the test asserts the query plans without crashing and no
`kLeftSemiFilter` appears inside the aggregation.

The pushed table should have a selective filter so the optimizer chooses to
fire the optimization.

TODO: Add e2e tests via `SqlTest.cpp` that run queries with DuckDB comparison
to verify correctness of results (not just plan structure).

## Limitations

### No partial pushdown

When multiple tables join the subquery and some are pushable but others are not,
the entire pushdown is skipped. The existences arrive as a single package with
a single fanout estimate, so partial pushdown may not achieve the expected
cardinality reduction. A future enhancement could push what it can and leave
the rest outside.

### No pushdown when other is a DerivedTable

When the table being pushed is itself a DerivedTable (subquery) or contains an
unnest, the optimizer does not choose pushdown. The existence mechanism supports
it structurally, but `findReducingJoins` does not select these cases.

### No recursive pushdown into nested subqueries

Existence pushdown stops at the first aggregation level. If a subquery contains
a nested subquery with its own aggregation, the existence is not pushed through
the inner boundary. Each level would need its own discovery and validation pass.

### No existence pushdown into UNION ALL children

When a UNION ALL subquery is joined with a selective table, it would be
beneficial to push an existence semijoin into each union child independently:

```sql
SELECT a FROM t
JOIN (SELECT x FROM u UNION ALL SELECT x FROM v) w ON a = w.x
WHERE t.a < 100
```

Ideally, the optimizer would produce:

```
HashJoin (t.a = w.x)
├── TableScan t
└── UnionAll
    ├── HashJoin LEFT SEMI (u.x = t.a)  ← pushed into child
    │   ├── TableScan u
    │   └── TableScan t
    └── HashJoin LEFT SEMI (v.x = t.a)  ← pushed into child
        ├── TableScan v
        └── TableScan t
```

This is not implemented. The join edge `t ↔ w` references `w`'s columns, not
the children's columns. Pushing into children would require translating join
keys from `w`'s column space to each child's column space. Additionally,
`addExistences` runs before `flattenDt` in `import`, so the child's internal
columns are not yet visible when existences are processed.

Currently, `makeUnionPlan` creates child MemoKeys without existences, and the
join with `t` happens above the union.
