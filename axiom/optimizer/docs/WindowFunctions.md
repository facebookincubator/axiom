# Window Function Design Proposal

*February 25, 2026*

## Overview

Window functions compute values across a set of rows related to the current
row, without reducing the number of rows. They appear in SELECT expressions
with OVER clauses:

```sql
SELECT
  row_number() OVER (PARTITION BY a ORDER BY b),
  sum(x) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
  avg(y) OVER (PARTITION BY a ORDER BY c)
FROM t
```

## Current State

Parsing and logical plan representation are complete. Window functions are
represented as `WindowExpr` nodes inside `ProjectNode` expressions. The
optimizer does not handle `WindowExpr` — `SubfieldTracker` hits
`VELOX_UNREACHABLE` in `markSubfields` and `ConstantExprEvaluator` hits
`VELOX_NYI`. The `ConstantExprEvaluator` fix is trivial: window functions are
never constant, so return the expression unchanged.

## Window Specification

A window specification consists of:
- **Partition keys**: PARTITION BY columns. Data must be co-located by these
  keys.
- **Order keys**: ORDER BY columns with sort directions. Data must be sorted
  within each partition.
- **Frame**: ROWS/RANGE/GROUPS with start and end bounds. This is per-function,
  not per-specification.

Two window functions share the same specification when they have identical
partition keys and identical order keys (same columns, same order, same sort
directions).

## Planning Algorithm

Planning happens in two phases:

### Phase 1: DT Construction (ToGraph)

During `makeQueryGraph`, multiple ProjectNodes and FilterNodes are folded into
the same DerivedTable via the `renames_` map. Window expressions from all
folded ProjectNodes accumulate in `renames_` as `WindowExpr` nodes embedded in
expression trees. The DT boundary rules (see below) determine when a new DT
must be started — e.g., when a filter references a window output, or when a
Project with window expressions is added to a DT that already has a limit.

When translating a `WindowExpr`, normalize its partition keys and order keys:
- Deduplicate, keeping the first occurrence of each column. A duplicate column
  is always redundant regardless of sort direction — it only breaks ties on
  itself, which is a no-op.
- Remove order keys that appear in partition keys — within a partition all rows
  have the same value for that key, so sorting by it is a no-op.

For example:
- `PARTITION BY x, x` → `PARTITION BY x`
- `ORDER BY x, x` → `ORDER BY x`
- `ORDER BY x ASC, x DESC` → `ORDER BY x ASC`
- `PARTITION BY x ORDER BY x, y` → `PARTITION BY x ORDER BY y`

After folding, a single DT may contain window expressions originating from
multiple ProjectNodes. These are all planned together in Phase 2.

### Phase 2: Window Planning (addPostprocess)

Once a DT is finalized with its accumulated expressions, window functions are
planned during `addPostprocess`, after aggregation and HAVING filters but
before the final projection and ORDER BY.

#### Step 1: Collect

Extract all `WindowExpr` nodes from the DT's expressions.

#### Step 2: Group by Specification

Group window functions by their window specification (partition keys + order
keys). Functions with identical specifications share one Window operator.

The frame is per-function, not per-specification. Functions with different
frames but the same partition + order keys are grouped together.

Additionally, when all window functions in the shorter-ORDER-BY group use ROWS
frames (not RANGE or GROUPS), and another group shares the same partition keys
with a longer ORDER BY that extends the shorter group's ORDER BY as a prefix,
the two groups can be merged into one Window operator using the longer ORDER BY.
This is safe because ROWS frames depend on row position, not on the values of
ORDER BY columns — adding sort keys only refines the ordering among ties, which
is non-deterministic anyway. The longer-ORDER-BY group's frame type does not
matter — its ORDER BY stays the same. This optimization does not apply when the
shorter-ORDER-BY group has RANGE or GROUPS frames: RANGE frame boundaries
depend on ORDER BY values, and GROUPS frame boundaries depend on peer groups
(rows with equal ORDER BY values). In both cases, extending the ORDER BY
changes which rows are peers, altering the frame boundaries and changing
results.

#### Step 3: Order Groups for Sort Reuse

Order the groups so that groups sharing the same partition keys are adjacent.
When partition keys match, process groups with longer ORDER BY first. If a
subsequent group's ORDER BY is a prefix of the previous group's ORDER BY (same
columns in the same positions with the same sort directions and nulls ordering),
the data is already sorted and the subsequent Window operator can set
`inputsSorted=true`, skipping the sort entirely.

For example, given groups with specifications:
- `(PARTITION BY a ORDER BY b, c)`
- `(PARTITION BY a ORDER BY b)`
- `(PARTITION BY d ORDER BY e)`

Order them as:
1. `(PARTITION BY a ORDER BY b, c)` — longer ORDER BY first
2. `(PARTITION BY a ORDER BY b)` — prefix of previous, `inputsSorted=true`
3. `(PARTITION BY d ORDER BY e)` — different partition keys

Group 2 reuses both the repartitioning and the sort from group 1.

Groups with empty partition keys (requiring gather to a single node) are
processed last. This avoids funneling all data through a single node before
redistributing for groups that need hash repartitioning.

When two groups have equal ordering priority (e.g., different partition keys
with no prefix relationship), use a deterministic tiebreaker such as
lexicographic comparison of column names. This ensures stable, reproducible
plan generation.

#### Step 4: Create Physical Plan

For each group, add:
1. **Repartition** (if needed): hash repartition by partition keys. Skip if all
   rows sharing the same window partition key values are already co-located.
   This holds when the input is partitioned by any subset of the window's
   partition keys: partitioning by fewer keys produces coarser (larger)
   partitions, so rows matching on more keys are necessarily in the same
   partition. For example, data partitioned by `(a)` is already valid for a
   window with `PARTITION BY a, b` — but data partitioned by `(a, b)` is not
   valid for `PARTITION BY a` because rows with the same `a` may be split across
   partitions with different `b` values. Also skip if running on a single
   worker. If partition keys are empty, gather to a single node.
   This logic already exists for aggregation in `repartitionForAgg`
   (`Optimization.cpp` ~line 584) — reuse the same subset check for window
   partition keys.
2. **Window operator**: computes all window functions in the group. Takes
   partition keys, order keys, and a list of (function, frame) pairs.

When consecutive groups share partition keys, the repartition from the first
group is reused — only re-sorting (if ORDER BY differs) is needed.

#### Cost Model

All decisions are heuristic:
- Repartition if not already partitioned by the right keys.
- Sort within partitions by order keys.
- No cost-based alternatives (unlike aggregation's partial/final split).

Window functions preserve all input rows, so there is no reduction to exploit
via partial computation.

#### Future: Data-Expanding Functions

Some aggregate functions used as window functions produce large results per row
(e.g., `array_agg`), significantly increasing data size. Running these early
means subsequent window operators process larger rows — more data to
repartition, sort, and shuffle. However, deferring them may require additional
repartitioning or sorting that would not be needed if they were grouped with
compatible functions earlier. This is a cost-based decision: estimate output
size per window function and weigh the data expansion cost against the
repartition/sort savings.

### Ranking Function Optimizations

Ranking functions (`row_number`, `rank`, `dense_rank`) enable optimizations
that are not possible for general window functions. Velox provides specialized
operators — `RowNumberNode` and `TopNRowNumberNode` — that Axiom emits when it
detects the patterns below.

#### Pattern 1: `row_number()` without ORDER BY

```sql
SELECT *, row_number() OVER (PARTITION BY a) as rn FROM t
```

Emit `RowNumberNode` instead of `WindowNode`. Simpler and more efficient: no
sorting keys or frame, just partition keys. The per-partition limit is not used
here (see Pattern 3).

#### Pattern 2: `row_number()` without ORDER BY + LIMIT

```sql
SELECT *, row_number() OVER (PARTITION BY x) FROM t LIMIT 10

-- Optimized: limit first, then assign row numbers
SELECT *, row_number() OVER (PARTITION BY x) FROM (SELECT * FROM t LIMIT 10)
```

Push `LIMIT` below the window function entirely. The row numbering is
non-deterministic (no ORDER BY), so computing row numbers on a smaller set
produces a valid result. This avoids both computing row numbers and
repartitioning rows that will be discarded. Works with or without partition
keys.

Does not apply to other window functions — they need full partitions to compute
correctly. Does not apply to `row_number()` with ORDER BY — the ordering
determines which rows get which numbers; limiting first changes the result.

#### Pattern 3: Ranking function + filter on output

```sql
-- Top 3 orders per customer by date
SELECT * FROM (
  SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY order_date DESC) as rn
  FROM orders
) WHERE rn <= 3
```

Emit `TopNRowNumberNode(limit=3)` (with ORDER BY) or `RowNumberNode(limit=3)`
(without ORDER BY) instead of `WindowNode` + filter. The node processes all
input but only keeps the top N rows per partition. Works with or without
partition keys.

`TopNRowNumberNode` supports `row_number()`, `rank()`, and `dense_rank()` via
a `RankFunction` enum (`kRowNumber`, `kRank`, `kDenseRank`).

Supported predicate forms:
- `rn <= N` → limit = N
- `rn < N` → limit = N - 1
- `rn = 1` → limit = 1 (common deduplication pattern)
- `rn BETWEEN K AND N` → limit = N, with residual filter `rn >= K` if K > 1 [^1]

Not supported: `rn >= N`, `rn > N` (no upper bound), `rn = N` for N > 1
(uncommon, and would require computing top N then filtering to keep only the
Nth).

[^1]: Prerequisite: decompose `BETWEEN` into `>= AND <=` during query graph
construction. This decomposition does not exist yet — BETWEEN is currently kept
as a single `between()` function call from parser through optimizer. A previous
attempt ([PR #875](https://github.com/facebookincubator/axiom/pull/875)) was
not merged due to concerns: (1) correctness for non-deterministic arguments
(e.g., `between(rand(), 0.5, 0.9)` evaluates `rand()` once, but `rand() >=
0.5 AND rand() <= 0.9` evaluates it twice), (2) `between()` is expected to
perform better in execution than two separate comparisons. A possible approach:
decompose for optimizer analysis, then recombine back to `between()` in
ToVelox. This is a separate work item not specific to window functions —
decomposition also benefits filter selectivity estimation and enables filter
pushdown for patterns like `(a BETWEEN 1 AND 10 AND b = 3) OR (a BETWEEN 1
AND 5 AND b = 7)` → `a >= 1 AND ((a <= 10 AND b = 3) OR (a <= 5 AND b = 7))`
(e.g., TPC-H Q19, see `axiom/optimizer/tests/tpch/queries/q19.sql`). Without it, the
`rn BETWEEN K AND N` pattern will not be detected; the other predicate forms
(`rn <= N`, `rn < N`, `rn = 1`) work independently.

The filter is removed if it contains only the ranking predicate. When
additional predicates exist (including a lower bound like `rn >= K`), the
ranking predicate is absorbed into the limit and remaining predicates are
preserved as a filter on top.

**Detection**: since a filter on a window function output triggers a DT
boundary, the window is in the inner DT and the filter is in the outer DT. The
ranking predicate starts in the outer DT's conjuncts. During filter pushdown
(`tryPushdownConjunct` in `DerivedTable.cpp`), it gets pushed down into the
inner DT as a regular conjunct (it references a column that the inner DT will
produce). In the inner DT's `addPostprocess`,
`placeConjuncts` skips it because the window output column doesn't exist yet
(the window hasn't been planned). After `addWindow` creates the Window or
RowNumber RelationOp and the output column becomes available, `addPostprocess`
scans remaining unplaced conjuncts, recognizes the ranking predicate, and
extracts its upper bound as a limit for the specialized node —
`TopNRowNumber(limit=N)` or `RowNumber(limit=N)`. The assertion that checks
all conjuncts are placed needs to be relaxed for conjuncts referencing window
outputs until after `addWindow` runs.

#### Pattern 4: Ranking function with ORDER BY + LIMIT

```sql
SELECT *, rank() OVER (ORDER BY b) as rn FROM t LIMIT 10
```

Absorb `LIMIT` into `TopNRowNumberNode(limit=10)` and remove the `LIMIT` node.
When there are no partition keys, per-partition limit equals global limit. The
node still processes all input but only keeps the top N internally.

With partition keys, the `LIMIT` node stays on top but `TopNRowNumberNode` is
still beneficial — it processes all input rows but only keeps the top N per
partition (no need to sort entire partitions), significantly cutting the data
volume before the global `LIMIT` selects the final N rows.

```sql
-- TopNRowNumberNode(limit=10) + LIMIT 10 on top
SELECT *, rank() OVER (PARTITION BY a ORDER BY b) as rn FROM t LIMIT 10
```

#### Detection of Patterns 2 and 4

Both patterns involve the interaction of LIMIT with window functions within the
same DT. In `ToGraph`, `addLimit` sets `currentDt_->limit` on the current DT
without creating a new DT. When the logical plan is `Limit → Project(window) →
Scan`, ToGraph processes bottom-up: Scan → Project (adds window to DT) → Limit
(sets limit on same DT). So both window functions and the limit are in the same
DT.

Detection happens in `addWindow` (called from `addPostprocess`). After
collecting and grouping window functions, `addWindow` checks `dt->hasLimit()`:

- **Pattern 2** (`row_number()` without ORDER BY + LIMIT): the only window
  function is `row_number()` with no ORDER BY. Create a `Limit` RelationOp
  before the `RowNumber` RelationOp (limit below window). The DT's limit is
  consumed and `addPostprocess` does not create a second Limit.

- **Pattern 4** (ranking function with ORDER BY + LIMIT, no partition keys):
  the only window function is a ranking function with ORDER BY. Create
  `TopNRowNumber(limit=dt->limit)`. For `row_number()`, the limit is fully
  absorbed — `row_number()` never produces ties, so the TopNRowNumber output
  has exactly `limit` rows. For `rank()`/`dense_rank()`, a Limit is added
  after TopNRowNumber because ties may produce more rows than the limit. In
  both cases, the DT's limit is consumed. With partition keys, per-partition
  limit differs from global limit, so the DT's limit is not consumed and
  `addPostprocess` creates the Limit normally.

These checks only apply when the DT has exactly one window function. Pattern 2
requires `row_number()` specifically; Pattern 4 applies to any ranking function.
When multiple window functions exist, or when the window function is not a
ranking function, the limit is left for `addPostprocess` to handle normally.

#### Distributed Limit for Gathered Input

When a Limit's input is already gathered (e.g., Limit after TopNRowNumber with
no partition keys where data is gathered first), ToVelox skips the distributed
limit pattern (partialLimit → exchange → finalLimit) and creates a simple
single-node limit instead.

## Comparison with Presto

Presto creates one WindowNode per function initially, then uses iterative
optimizer rules to merge and reorder nodes.

### Presto's Reordering Heuristics

Presto uses two rules applied iteratively (via `GatherAndMergeWindows`):

1. **MergeAdjacentWindowsOverProjects**: merges two adjacent WindowNodes
   (possibly separated by up to 4 identity ProjectNodes) if their
   specifications are exactly equal and neither depends on the other's output.
   Combines the function maps into a single node.

2. **SwapAdjacentWindowsBySpecifications**: reorders adjacent WindowNodes to
   bring matching specifications together. Swaps parent and child if the parent
   has a "smaller" specification by lexicographic comparison — first compare
   partition keys, then order keys. Falls back to node ID for determinism.

This is essentially a bubble sort applied through iterative rule matching.
After sorting, nodes with the same specification are adjacent and can be merged
by rule 1.

### Comparison

| Aspect | Presto | Axiom |
|--------|--------|-------|
| Initial plan | One node per function | Group by specification upfront |
| Merging | Iterative rule-based merge | No merge needed — grouped from the start |
| Reordering | Bubble sort by (partition, order) | Direct sort during planning |
| Sort reuse | `preSortedOrderPrefix` per node | `inputsSorted` + ordering by prefix |
| Frame grouping | Per-function within node | Same |
| ORDER BY union | Not done | Not done (except ROWS-only merge) |
| Empty partition keys | Processed first (fewer keys sort first) | Processed last |
| Ranking optimizations | `WindowFilterPushDown` pass | Filter pushdown pass |
| `row_number()` w/o ORDER BY | `RowNumberNode` | Same |
| LIMIT below `row_number()` | Not done | Push LIMIT below window |

Axiom achieves better grouping in a single pass during planning, without
the overhead of iterative rule application, and with improved ordering of
groups (see below). Presto's approach is more general (rules can fire
independently as the plan evolves), but for window function grouping the
single-pass approach is sufficient.

### Empty Partition Key Ordering

Axiom intentionally diverges from Presto on the ordering of groups with empty
partition keys. Presto's lexicographic comparison treats fewer partition keys as
"smaller," placing empty-partition-key windows first (computed earliest). This
means all data is gathered to a single node first, the unpartitioned window
runs, then data is redistributed for partitioned windows — creating a
bottleneck.

Axiom processes empty-partition-key groups last. Partitioned windows run distributed
first, then the gather collects results for unpartitioned windows. The gather
is unavoidable either way, but Axiom's ordering avoids redistributing data after a
single-node bottleneck.

### ORDER BY Prefix Ordering

Axiom also diverges from Presto on how groups with the same partition keys but
different ORDER BY keys are ordered. Presto processes shorter ORDER BY first
(fewer keys are "smaller" in the lexicographic comparison), then uses
`preSortedOrderPrefix` for the longer ORDER BY to avoid re-sorting the common
prefix — but still requires an incremental sort to add the remaining keys.
Two sort passes total.

Axiom processes longer ORDER BY first. The longer sort subsumes the shorter one, so
the subsequent group sets `inputsSorted=true` and skips sorting entirely. One
sort pass total.

### Ranking Function Optimizations

Presto's `WindowFilterPushDown` is a separate optimizer pass that walks the
plan tree looking for `Filter → Window` and `Limit → Window` patterns. It
converts `WindowNode` to `RowNumberNode` (for `row_number()` without ORDER BY)
and to `TopNRowNumberNode` (for ranking functions with a filter or limit). The
`rank()`/`dense_rank()` optimization is gated behind a session property and
only enabled for native execution.

Axiom performs the same optimizations during the filter pushdown pass
(Patterns 3 and 4) and during Phase 2 window planning (Pattern 1). Axiom also
pushes `LIMIT` below `row_number()` without ORDER BY (Pattern 2), which Presto
does not do — Presto always computes row numbers for all rows before applying
the limit.

## DerivedTable Boundary Rules

### Background

In the optimizer, multiple ProjectNodes and FilterNodes are folded into the
same DerivedTable via the `renames_` map. Each ProjectNode updates `renames_`
with translated expressions; each FilterNode appends conjuncts resolved through
`renames_`. The DerivedTable's final columns and expressions are set only at
the end via `setDtUsedOutput`.

Window functions in the logical plan are `WindowExpr` nodes embedded in
ProjectNode expressions. Through `renames_`, these expressions propagate into
the DerivedTable's expression trees. When a subsequent node references a
window function's output, the `WindowExpr` is substituted in via `renames_`.

### Rules for DerivedTable Boundaries

Window functions execute after WHERE, GROUP BY, and HAVING, but before ORDER BY
and LIMIT. A DT boundary is needed when other nodes reference window function
outputs, or when adding window expressions to a DT that already has a limit.

#### Adding a Project that contains a WindowExpr

| Scenario | Rule |
|----------|------|
| **DT has `limit`** | Finalize the DT before adding the project. The inner DT applies the limit; the outer DT computes the window functions on the limited dataset. |
| **DT has `orderKeys` but no `limit`** | Clear the `orderKeys` if the window has partition keys or order keys — repartitioning or re-sorting invalidates the existing order. Keep the `orderKeys` if the window has neither (e.g., `row_number() OVER ()`) — the window just appends a column without changing row order. See Clearing Existing Sort below. |
| **WindowExpr references another WindowExpr output** | If the new WindowExpr's inputs (arguments, partition keys, or order keys) reference an existing WindowExpr via `renames_`, finalize the current DT. The inner DT computes the first window; the outer DT computes the second. This arises from subqueries like `SELECT sum(rn) OVER (...) FROM (SELECT row_number() OVER (...) as rn ...)`. [^2] |

[^2]: When consecutive window DTs share compatible partition keys, the outer
DT's window does not need repartitioning — the data is already partitioned
correctly from the inner DT. Similarly, if the outer window's ORDER BY is a
prefix of the inner's, the sort can be skipped. This is not window-specific
logic — it is handled by the generic repartition/sort skip in `Optimization`
that recognizes when the current dataset is already partitioned or sorted
as needed.

#### Adding a node to a DT that already has window functions

These rules apply when `renames_` contains a `WindowExpr` (i.e., a pending
window computation exists in the DT) and a new node references or interacts
with it.

| Node Kind | Rule |
|-----------|------|
| **Project without WindowExpr** | If a non-window expression references a `WindowExpr` output (via `renames_`), it can stay in the same DT — the window is computed before the final projection. |
| **Filter** | If the predicate references a `WindowExpr` (directly or transitively via `renames_`), finalize the current DT and place the filter in a new outer DT. The inner DT computes the window functions; the outer DT applies the filter. |
| **Aggregate** | If grouping keys or aggregate arguments reference a `WindowExpr`, finalize the current DT. The inner DT computes windows; the outer DT aggregates. |
| **Sort** | No changes. Sort folds into the DT's `orderKeys` as usual. If a subsequent Project with WindowExpr is added, the "DT has `orderKeys`" rule above handles it. |
| **Limit** | No changes. Limit folds into `dt->limit` as usual. If a subsequent Project with WindowExpr is added, the "DT has `limit`" rule above handles it. |
| **Join** | If join condition references a `WindowExpr`, finalize the DT containing the window. (Unusual in practice.) |

### Clearing Existing Sort

When adding window functions to a DT that already has `orderKeys` (from a child
Sort node that was folded in), clear the `orderKeys` if the window will
invalidate the existing sort order. This is analogous to how aggregation clears
`orderKeys` in `ToGraph.cpp`.

Cases where the existing sort is invalidated:
- Non-empty partition keys that require repartition: hash repartition destroys
  order.
- Window has ORDER BY keys: window's sort within partitions overwrites previous
  ordering.

Cases where the existing sort is preserved:
- Empty partition keys on a single worker or already-gathered data, with no
  ORDER BY keys in the window (e.g., `row_number() OVER ()`): the window just
  appends a column without changing row order.

### Example: Filter on Window Output

```sql
SELECT * FROM (
  SELECT *, sum(x) OVER (PARTITION BY a) as s FROM t
) WHERE s > 10
```

Logical plan:
```text
Filter [s > 10]
  Project [sum(x) OVER (PARTITION BY a) as s, ...]
    Scan
```

Without window awareness, all three nodes fold into one DT with the filter as a
conjunct. But the filter references `s`, which resolves to a `WindowExpr`. The
window must be computed before the filter can evaluate.

With window awareness: when `addFilter` detects the predicate references a
`WindowExpr`, it finalizes the current DT (which includes the window
expression) and creates a new outer DT for the filter.

```text
Outer DT: conjuncts=[s > 10]
  Inner DT: window=[sum(x) OVER (PARTITION BY a) as s]
    Scan
```

## QueryGraph Representation

### Expression Level: WindowFunction

`WindowFunction` extends `Call` (`PlanType::kWindowExpr`). It is the query graph
representation of a window function call, translated from the logical plan's
`WindowExpr` (see Appendix) during Phase 1. It stores all per-function
properties:

```cpp
class WindowFunction : public Call {
 public:
  WindowFunction(
      Name name,
      const Value& value,
      ExprVector args,
      FunctionSet functions,
      ExprVector partitionKeys,
      ExprVector orderKeys,
      OrderTypeVector orderTypes,
      Frame frame,
      bool ignoreNulls);

 private:
  ExprVector partitionKeys_;
  ExprVector orderKeys_;
  OrderTypeVector orderTypes_;
  Frame frame_;           // {type, startType, startValue, endType, endValue}
  bool ignoreNulls_;
};
```

Partition keys and order keys are stored per-function because they are needed
during Phase 2 to group by specification. The `Frame` struct mirrors Velox's
`WindowNode::Frame`.

### DerivedTable

No new fields. Window functions are stored in the existing `columns` and `exprs`
fields. For each window function translated in Phase 1, an entry is added to
`columns` (the output Column) and `exprs` (the WindowFunction expression).
To check whether a DT has window functions, scan `exprs` for
`PlanType::kWindowExpr`.

During Phase 2 (`addPostprocess`), the DT's `exprs` are scanned for
`WindowFunction` entries. These are extracted, grouped by specification,
and converted to physical `Window` operators. The corresponding `columns`
entries become the output columns of the `Window` operators.

#### Conjuncts Referencing Window Outputs

When a filter on a window function output triggers a DT boundary (Pattern 3),
the ranking predicate starts in the outer DT's conjuncts.
During filter pushdown (`tryPushdownConjunct` in `DerivedTable.cpp`), it gets
pushed down into the inner DT as a regular conjunct.

In the inner DT's `addPostprocess`, `placeConjuncts` skips it because
the window output column doesn't exist yet. After `addWindow` creates the
Window or RowNumber RelationOp and the output column becomes available,
`addPostprocess` scans remaining unplaced conjuncts: ranking predicates are
extracted as limits for specialized nodes (`TopNRowNumber(limit=N)` or
`RowNumber(limit=N)`); other window-output predicates are placed as regular
Filter RelationOps. The assertion that checks all conjuncts are placed needs
to be relaxed until after `addWindow` runs.

### Physical Level: Window RelationOp

`Window` extends `RelationOp` (`RelType::kWindow`):

```cpp
struct Window : public RelationOp {
  Window(
      RelationOpPtr input,
      ExprVector partitionKeys,
      ExprVector orderKeys,
      OrderTypeVector orderTypes,
      QGVector<WindowFunctionCP> windowFunctions,
      bool inputsSorted,
      ColumnVector columns);

  const ExprVector partitionKeys;
  const ExprVector orderKeys;
  const OrderTypeVector orderTypes;
  const QGVector<WindowFunctionCP> windowFunctions;
  const bool inputsSorted;  // Input is already partitioned by partitionKeys
                            // and sorted by orderKeys within each partition.
                            // When true, the Window operator skips both
                            // repartitioning and sorting.
};
```

Output columns: all input columns (passthrough) plus one new column per window
function. Multiple `Window` operators may be stacked (one per specification
group).

### Physical Level: RowNumber RelationOp

For Pattern 1 (`row_number()` without ORDER BY):

```cpp
struct RowNumber : public RelationOp {
  RowNumber(
      RelationOpPtr input,
      ExprVector partitionKeys,
      std::optional<int32_t> limit,
      ColumnCP outputColumn);

  const ExprVector partitionKeys;
  const std::optional<int32_t> limit;
  const ColumnCP outputColumn;
};
```

Maps directly to Velox's `RowNumberNode`. The optional `limit` is set when
Pattern 3 (filter pushdown) detects a ranking predicate on a `row_number()`
without ORDER BY.

### Physical Level: TopNRowNumber RelationOp

For Patterns 3 and 4 (ranking function with ORDER BY + limit):

```cpp
enum class RankFunction { kRowNumber, kRank, kDenseRank };

struct TopNRowNumber : public RelationOp {
  TopNRowNumber(
      RelationOpPtr input,
      ExprVector partitionKeys,
      ExprVector orderKeys,
      OrderTypeVector orderTypes,
      RankFunction rankFunction,
      int32_t limit,
      ColumnCP outputColumn);

  const ExprVector partitionKeys;
  const ExprVector orderKeys;
  const OrderTypeVector orderTypes;
  const RankFunction rankFunction;
  const int32_t limit;
  const ColumnCP outputColumn;
};
```

Maps directly to Velox's `TopNRowNumberNode`. The `limit` comes from filter
pushdown (Pattern 3) or LIMIT absorption (Pattern 4).

Pattern 2 (`row_number()` without ORDER BY + LIMIT) does not need a new
RelationOp — it reorders the existing Limit before the RowNumber operator
during `addPostprocess`.

### addPostprocess Pipeline

Window planning slots into `addPostprocess` after aggregation and HAVING
filters, but before ORDER BY and projection:

```text
1. WRITE (early return)
2. GROUP BY (addAggregation)
3. HAVING (filter)
4. WINDOW (addWindow)          <-- new
5. ORDER BY / small LIMIT
6. SELECT (projection)
7. Large LIMIT
8. EnforceSingleRow
```

`addWindow` extracts `WindowFunction` entries from the DT's `exprs`, groups
them by specification (Steps 1-3 of Phase 2), and creates physical operators:
`Window`, `RowNumber`, or `TopNRowNumber` RelationOps depending on the detected
pattern (see Ranking Function Optimizations). When a ranking optimization
consumes the DT's LIMIT (Patterns 2 or 4), `addPostprocess` skips creating a
separate Limit.

### ToVelox Translation

The new RelationOps need corresponding `make` functions in `ToVelox.cpp` to
translate to Velox plan nodes:

- `Window` → `velox::core::WindowNode`: map partition keys, order keys,
  window functions (with frames), and `inputsSorted` flag.
- `RowNumber` → `velox::core::RowNumberNode`: map partition keys, optional
  limit, and output column.
- `TopNRowNumber` → `velox::core::TopNRowNumberNode`: map partition keys,
  order keys, ranking function enum, limit, and output column.

Follow the existing `makeAggregation` pattern in `ToVelox.cpp`. The mapping is
straightforward — the Axiom RelationOps are designed to mirror the Velox node
interfaces.

Note: Velox's `WindowNode::WindowType` only supports `kRows` and `kRange` —
GROUPS frames are not yet implemented. Axiom's `WindowExpr` supports GROUPS at
the logical plan level (parsing, serde, planning all work), but translation to
Velox will need to either raise an unsupported error or wait for Velox to add
GROUPS support.

## SubfieldTracker

SubfieldTracker traces which columns and subfields are accessed by downstream
consumers. It walks the logical plan tree, distinguishing control subfields
(used for partitioning, sorting, filtering — need all values preserved exactly)
from payload subfields (data flowing through computations — may allow pruning
nested fields).

`WindowExpr` currently hits `VELOX_UNREACHABLE` in `markSubfields`. The fix
adds a `WindowExpr` case following the `AggregateNode` pattern:

- **Partition keys → control**: these determine data distribution (hash
  repartition). All values must be preserved exactly for correct partitioning.
- **Order keys → control**: these determine sort order within partitions. All
  values must be preserved exactly for correct ordering.
- **Function arguments → payload**: these are the data being aggregated (e.g.,
  the `x` in `sum(x) OVER (...)`). Inherit `isControl` from the caller — if
  the window function output is used as a control input downstream, its
  arguments are also control.
- **Frame bound expressions → payload**: `startValue` and `endValue` if
  non-null (e.g., `ROWS BETWEEN expr PRECEDING AND expr FOLLOWING`). These are
  computed values, not keys.

Subfield paths from the window function output do not propagate to its inputs.
SubfieldTracker uses a `steps` vector to track subfield access paths (e.g.,
`row.address.city`). Window functions compute new values, so a downstream
access like `window_output.foo` is meaningless for the window's inputs.
Starting with a fresh empty `steps` vector means inputs are marked as fully
accessed (no subfield pruning). Aggregation uses the same conservative
approach. Note: Axiom's `FunctionRegistry` already has a `FunctionMetadata`
mechanism (`subfieldArg`, `fieldIndexForArg`, `valuePathToArgPath`) for
propagating subfield paths through scalar and lambda functions. Extending this
to aggregate and window functions could enable pruning for cases like
`map_agg(x,y)[k]` or `array_agg(x)[i].field`.

Unlike `AggregateNode` which has a separate `markFieldAccessed` overload,
`WindowExpr` appears inline in project expressions, so the handling goes
directly in `markSubfields`.

## Example

```sql
SELECT
  row_number() OVER (PARTITION BY a ORDER BY b),
  sum(x) OVER (PARTITION BY a ORDER BY b ROWS UNBOUNDED PRECEDING),
  avg(y) OVER (PARTITION BY a ORDER BY b, c),
  max(z) OVER (PARTITION BY d ORDER BY e)
FROM t
```

Groups by specification:
1. `(a, ORDER BY b)`: `row_number()`, `sum(x)` — different frames, same spec.
   Note: `row_number()` has ORDER BY here, so Pattern 1 does not apply.
2. `(a, ORDER BY b, c)`: `avg(y)`
3. `(d, ORDER BY e)`: `max(z)`

Groups 1 and 2 share partition key `a` and group 1's ORDER BY is a prefix of
group 2's. Process the longer ORDER BY first:

Physical plan:
```text
Project [final column selection]
  Window [partition=d, order=e] -> max(z)
    Repartition [by d]
      Window [partition=a, order=b, inputsSorted=true] -> row_number(), sum(x)
        Window [partition=a, order=b, c] -> avg(y)
          Repartition [by a]
            Scan
```

Groups 1 and 2 share one repartition. Group 2 (longer ORDER BY) runs first.
Group 1 sets `inputsSorted=true` because ORDER BY `b` is a prefix of `b, c`.

## Appendix: WindowExpr Logical Plan Node

<details>
<summary>WindowExpr definition (axiom/logical_plan/Expr.h)</summary>

```cpp
class WindowExpr : public Expr {
 public:
  enum class WindowType { kRange, kRows, kGroups };

  enum class BoundType {
    kUnboundedPreceding, kPreceding, kCurrentRow,
    kFollowing, kUnboundedFollowing,
  };

  struct Frame {
    WindowType type;
    BoundType startType;
    ExprPtr startValue;     // null for UNBOUNDED/CURRENT ROW
    BoundType endType;
    ExprPtr endValue;       // null for UNBOUNDED/CURRENT ROW
  };

  WindowExpr(
      velox::TypePtr type,
      std::string name,                     // function name
      std::vector<ExprPtr> inputs,          // function arguments
      std::vector<ExprPtr> partitionKeys,
      std::vector<SortingField> ordering,   // {expression, sortOrder}
      Frame frame,
      bool ignoreNulls);

 private:
  const std::string name_;
  const std::vector<ExprPtr> partitionKeys_;
  const std::vector<SortingField> ordering_;
  const Frame frame_;
  const bool ignoreNulls_;
};
```

This is the input that `ToGraph` translates into `WindowFunction` (QueryGraph
expression) during Phase 1.

</details>

## Future Optimizations

### Window Run Conditions (Early Partition Termination)

PostgreSQL 15 introduced window run conditions — early termination of
partition processing for monotonic functions. For example, with
`row_number() OVER (ORDER BY x) <= 5`, once 5 rows have been output from a
partition, the remaining rows are guaranteed to have `row_number() > 5` and
processing can stop. This generalizes beyond TopNRowNumber: it applies to any
monotonic window function with a filter, such as `sum(positive_col) OVER
(ORDER BY x) > threshold` for early start, or `ntile(10) OVER (ORDER BY x)
<= 3` for early stop.

This would require Velox-level support — the Window operator would need a "run
condition" predicate evaluated per-row to decide when to stop (or start)
processing a partition. The optimizer would detect filter patterns on monotonic
window functions and push the predicate into the Window operator as a run
condition.

### Partition-Wide Frame Merging

Window functions with `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED
FOLLOWING` compute over the entire partition — the result is the same for every
row regardless of ORDER BY. Two such functions with the same partition keys but
different ORDER BY could be merged into one Window operator since ORDER BY
is irrelevant when the frame covers all rows. In practice this is uncommon:
users who omit ORDER BY get this frame as the default, and those functions
already share the same spec (no order keys) and are grouped together.

## Testing

### Optimizer Plan Tests

Add `WindowTest.cpp` to `axiom/optimizer/tests/` (extends
`HiveQueriesTestBase`). Parse SQL via `parseSelect()`, verify single-node plans
with `toSingleNodePlan()` + `PlanMatcherBuilder`, and distributed plans with
`planVelox()` + `AXIOM_ASSERT_DISTRIBUTED_PLAN`.

`PlanMatcherBuilder` needs new matchers: `window()`, `rowNumber()`,
`topNRowNumber()` in `PlanMatcher.h/cpp`. Follow the `singleAggregation()`
pattern.

Test cases:

**Grouping and sort reuse:**
- Single window function → one Window operator.
- Multiple functions with same specification → grouped into one operator.
- ROWS-only merge: shorter ORDER BY merged into longer ORDER BY group.
- Sort reuse: `inputsSorted=true` when ORDER BY is a prefix.
- Empty partition keys processed last.

**Repartitioning:**
- Distributed plan: repartition + window.
- Repartition skip after aggregation: `GROUP BY a` followed by window with
  `PARTITION BY a, b` — aggregation already partitions by subset of window's
  partition keys, so all rows with same `(a, b)` are already co-located.
- Consecutive windows with compatible partitioning: `SELECT sum(rn) OVER
  (PARTITION BY a) FROM (SELECT *, row_number() OVER (PARTITION BY a ORDER BY
  b) as rn FROM t)` → second window reuses first's repartitioning by `a`.
- Consecutive windows with different partitioning: same as above but outer
  window uses `PARTITION BY c` → requires its own repartitioning.

**Interaction with other operators:**
- Window after LIMIT: `SELECT *, row_number() OVER (PARTITION BY a ORDER BY b)
  FROM (SELECT * FROM t LIMIT 10)` → inner DT applies limit, outer computes
  window.
- Window clears orderKeys: `SELECT *, sum(x) OVER (PARTITION BY a ORDER BY b)
  FROM (SELECT * FROM t ORDER BY c)` → ORDER BY cleared because window
  repartitions and re-sorts.
- Window preserves orderKeys: `SELECT *, row_number() OVER () FROM (SELECT *
  FROM t ORDER BY c)` → ORDER BY preserved because window has no partition keys
  or order keys.
- Non-window projection references window output: `SELECT s + 1 FROM (SELECT
  sum(x) OVER (PARTITION BY a) as s FROM t)` → single DT.
- Filter on window output: `SELECT * FROM (SELECT *, sum(x) OVER (PARTITION BY
  a) as s FROM t) WHERE s > 10` → inner DT computes window, outer filters.
- Aggregate on window output: `SELECT max(rn) FROM (SELECT row_number() OVER
  (PARTITION BY a ORDER BY b) as rn FROM t)` → inner DT computes window, outer
  aggregates.
- Filter on partition key pushes below window: `SELECT * FROM (SELECT *,
  sum(x) OVER (PARTITION BY a) as s FROM t) WHERE a = 5 AND s > 10` → `a = 5`
  pushes below window (doesn't reference window output), `s > 10` stays above.
  Verify the scan sees `a = 5` as a pushed-down predicate.

Target: `buck test fbcode//axiom/optimizer/tests:window` (new target)

### Ranking Function Optimization Tests

Add `RankingTest.cpp` to `axiom/optimizer/tests/` (extends
`HiveQueriesTestBase`). Parse SQL via `parseSelect()`, verify single-node plans
with `toSingleNodePlan()` + `PlanMatcherBuilder`, and distributed plans with
`planVelox()` + `AXIOM_ASSERT_DISTRIBUTED_PLAN`.

Test cases:

**RowNumber optimization (no ORDER BY → RowNumberNode):**
- `row_number()` without partition or ORDER BY → RowNumber.
- `row_number()` with PARTITION BY → RowNumber with partition keys.
- `row_number()` + LIMIT → Limit pushed below RowNumber.
- `row_number()` with PARTITION BY + LIMIT → same with partition keys.

**TopNRowNumber optimization (ORDER BY + LIMIT → TopNRowNumberNode):**
- `row_number()` with ORDER BY + LIMIT → limit absorbed (no ties).
- `rank()` with ORDER BY + LIMIT → limit preserved on top (ties).
- `dense_rank()` with ORDER BY + LIMIT → same as rank().
- `row_number()` with PARTITION BY + ORDER BY + LIMIT → with partition keys.
- `row_number()` with ORDER BY + LIMIT + matching query ORDER BY → redundant
  ORDER BY absorbed.

**No optimization (→ WindowNode):**
- `row_number()` / `rank()` with ORDER BY, no LIMIT → Window (no TopN without
  LIMIT).
- `rank()` / `dense_rank()` without ORDER BY → Window (only `row_number()` gets
  RowNumber optimization).
- `rank()` with LIMIT, no ORDER BY → Window + Limit (Pattern 2 does not apply
  to rank).
- Multiple window functions with LIMIT → Window (ranking optimization requires
  single function).

Target: `buck test fbcode//axiom/optimizer/tests:ranking` (new target)

### Subfield Tests

Add window function tests to `axiom/optimizer/tests/SubfieldTest.cpp` (extends
`QueryTestBase`, parameterized). Parse SQL, optimize via `planVelox()`, and
verify required subfields on scan nodes using `verifyRequiredSubfields()`.

For example, given a table with struct column `s` with fields `{x, y, z, w}`:

```sql
SELECT sum(s.x) OVER (PARTITION BY s.y ORDER BY s.z) FROM t
```

The scan should request only subfields `{.x, .y, .z}` from column `s`. The
partition key `s.y` and order key `s.z` are control, the function argument
`s.x` is payload. Field `s.w` is pruned.

Another case: `row_number() OVER (PARTITION BY s.y)` → scan requests only
`{.y}`.

Target: `buck test fbcode//axiom/optimizer/tests:subfields`

### End-to-End Tests

Add `WindowE2ETest.cpp` to `axiom/optimizer/tests/` (extends
`HiveQueriesTestBase`). Generate data using `makeRowVector` /
`makeFlatVector`, load it into both HiveConnector (as a table) and DuckDB (via
`createDuckDbTable`). Parse SQL via `parseSelect()`, optimize and translate to
a Velox plan via `planVelox()`, then execute using Velox's
`AssertQueryBuilder(plan, duckDbQueryRunner_).assertResults(duckDbSql)` to
compare results against DuckDB.

This reuses Velox's existing `DuckDbQueryRunner` and `AssertQueryBuilder`
infrastructure (from `velox/exec/tests/utils/`). Axiom does not currently use
DuckDB-based result comparison — this would be the first test to do so.

Test cases:
- Basic window functions: `sum`, `avg`, `count`, `min`, `max` with PARTITION
  BY and ORDER BY.
- All frame types: ROWS, RANGE with various bounds (UNBOUNDED PRECEDING,
  N PRECEDING, CURRENT ROW, N FOLLOWING, UNBOUNDED FOLLOWING).
- Ranking functions: `row_number()`, `rank()`, `dense_rank()`.
- Multiple window functions with same and different specifications.
- Window functions combined with GROUP BY, HAVING, ORDER BY, LIMIT.
- Subquery with window function and outer filter.
- Ranking function optimizations (Patterns 1–4) — verify correct results
  despite plan transformations.

Target: `buck test fbcode//axiom/optimizer/tests:window_e2e` (new target)

### Verification Commands

```bash
buck test fbcode//axiom/optimizer/tests:window
buck test fbcode//axiom/optimizer/tests:ranking
buck test fbcode//axiom/optimizer/tests:window_e2e
buck test fbcode//axiom/optimizer/tests:subfields
buck build fbcode//axiom/...
```
