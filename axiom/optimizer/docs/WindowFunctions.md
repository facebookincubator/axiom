# Window functions

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

#### Filter Pushdown Through Windows

Window functions compute over the full input set of a partition. Pushing a
filter below a window function changes which rows the window sees, altering the
results. For example:

```sql
SELECT * FROM (
  SELECT x, count(*) OVER () as cnt FROM t
) WHERE x <= 2
```

`count(*) OVER ()` should count all rows in `t`, not just those where `x <= 2`.
If the filter pushes below the window, the count reflects only the filtered
rows — a wrong result.

**General rule:** filters cannot be pushed below window functions.

There are two exceptions where pushdown is safe:

1. **Partition-key-aligned filters.** If the predicate depends only on columns
   that are partition keys of every window function in the DT, pushing it below
   is semantically equivalent. For `PARTITION BY a`, filtering on `a = 5` before
   the window gives the same result because rows with `a = 5` already form their
   own partition — the window never mixes them with other rows. This is analogous
   to pushing filters below aggregation when the filter is on grouping keys.

2. **Ranking upper-bound predicates** (see Pattern 5 below). A predicate like
   `rn <= 5` on a ranking function output can be absorbed as a limit in
   `TopNRowNumber` or `RowNumber`. This is only valid when the DT has exactly
   one window function — the ranking function itself. With multiple window
   functions, the predicate cannot be absorbed and must stay as a filter above
   the window operators.

These rules apply both during DT construction (when a FilterNode is folded into
a DT that has windows — see DT Boundary Rules below) and during filter pushdown
(`distributeConjuncts`), when conjuncts from an outer DT are pushed into an
inner DT that has window functions.

#### Expression Folding

During `makeQueryGraph`, multiple ProjectNodes and FilterNodes are folded into
the same DerivedTable via the `renames_` map. Window expressions from all
folded ProjectNodes accumulate in `renames_` as `WindowExpr` nodes embedded in
expression trees. The DT boundary rules (see below) determine when a new DT
must be started — e.g., when a FilterNode follows a Project with window
expressions, or when a Project with window expressions is added to a DT that
already has a limit.

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

Extract all window functions from `windowPlan->functions`.

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

See "Full-partition frame merge" in Future Optimizations below.

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

See "Data-expanding functions" in Future Optimizations below.

### Ranking Function Optimizations

Ranking functions (`row_number`, `rank`, `dense_rank`) and filter pushdown
through windows enable several optimizations. Velox provides specialized
operators — `RowNumberNode` and `TopNRowNumberNode` — that Axiom emits when it
detects the patterns below.

| Pattern | Description | Operator | Restriction |
|---------|-------------|----------|-------------|
| 1 | `row_number()` without ORDER BY | `RowNumber` | Single window function |
| 2 | `row_number()` without ORDER BY + LIMIT | `Limit` below `RowNumber` | Single window function |
| 3 | Ranking function with ORDER BY + LIMIT | `TopNRowNumber` | Single window function |
| 4 | Filter on partition keys | Push filter below window | Filter columns must be partition keys of every window function |
| 5 | Ranking function + filter on output | `TopNRowNumber` or `RowNumber` with limit | Single window function |

#### Pattern 1: `row_number()` without ORDER BY

```sql
SELECT *, row_number() OVER (PARTITION BY a) as rn FROM t
```

Emit `RowNumberNode` instead of `WindowNode`. Simpler and more efficient: no
sorting keys or frame, just partition keys. The per-partition limit is not used
here (see Pattern 5).

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

#### Pattern 3: Ranking function with ORDER BY + LIMIT

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

#### Detection of Patterns 2 and 3

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

- **Pattern 3** (ranking function with ORDER BY + LIMIT, no partition keys):
  the only window function is a ranking function with ORDER BY. Create
  `TopNRowNumber(limit=dt->limit)`. For `row_number()`, the limit is fully
  absorbed — `row_number()` never produces ties, so the TopNRowNumber output
  has exactly `limit` rows. For `rank()`/`dense_rank()`, a Limit is added
  after TopNRowNumber because ties may produce more rows than the limit. In
  both cases, the DT's limit is consumed. With partition keys, per-partition
  limit differs from global limit, so the DT's limit is not consumed and
  `addPostprocess` creates the Limit normally.

These checks only apply when the DT has exactly one window function. Pattern 2
requires `row_number()` specifically; Pattern 3 applies to any ranking function.
When multiple window functions exist, or when the window function is not a
ranking function, the limit is left for `addPostprocess` to handle normally.

#### Distributed Limit for Gathered Input

When a Limit's input is already gathered (e.g., Limit after TopNRowNumber with
no partition keys where data is gathered first), ToVelox skips the distributed
limit pattern (partialLimit → exchange → finalLimit) and creates a simple
single-node limit instead.

#### Pattern 4: Filter on partition keys

```sql
SELECT * FROM (
  SELECT *, sum(x) OVER (PARTITION BY a) as s FROM t
) WHERE a = 5
```

If a filter predicate depends only on columns that are partition keys of every
window function in the DT, pushing it below the window is semantically
equivalent. Rows with `a = 5` already form their own partition — the window
never mixes them with other rows. Pushing the filter below reduces the input to
the window, avoiding unnecessary computation. This is analogous to pushing
filters below aggregation when the filter is on grouping keys.

This optimization is applied in `addFilter`: a predicate pushed into a DT with
window functions is accepted if it depends only on columns present in the
partition keys of every window function in that DT. The check uses
`isPartitionKeyFilter`, which builds a `PlanObjectSet` of each window
function's partition key columns and verifies that the predicate's columns are a
subset.

See "Per-operator filter placement" in Future Optimizations below.

#### Pattern 5: Ranking function + filter on output

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
(`distributeConjuncts`), the predicate is pushed into the inner DT only when
the inner DT has exactly one window function — the ranking function itself.
With multiple window functions, the predicate cannot be absorbed as a
TopNRowNumber limit and stays in the outer DT as a regular filter. When pushed,
the predicate is validated using `isRankingUpperBoundPredicate` on the imported
form (where the column reference is replaced with the underlying WindowFunction
expression). The limit is extracted and stored on the `WindowPlan` via
`withRankingLimit` (see Ranking Limit in the DerivedTable section). Non-ranking
window predicates (e.g., `sum() > 10`) are rejected and stay in the outer DT
as a regular Filter.

In `addWindow`, the ranking limit is read from `WindowPlan::rankingLimit()`
and passed to `makeWindowOp`, which creates `TopNRowNumber(limit=N)` or
`RowNumber(limit=N)`.

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
(Patterns 3 and 5) and during Phase 2 window planning (Pattern 1). Axiom also
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
and LIMIT. A DT boundary is needed when a filter follows window expressions
(to prevent pushdown below windows — see Filter Pushdown Through Windows
above), when other nodes reference window function outputs, or when adding
window expressions to a DT that already has a limit.

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
| **Filter** | Finalize the current DT and place the filter in a new outer DT. The inner DT computes the window functions; the outer DT applies the filter. This prevents filters from being placed below window operators, which would change the window semantics (see Filter Pushdown Through Windows above). |
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
conjunct. But the DT has window functions, and placing a filter in the same DT
would put the filter below the window — changing which rows the window sees.

With window awareness: the FilterNode triggers a DT boundary because the
current DT has window functions. The filter is placed in a new outer DT.

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

Window functions are represented explicitly in the DerivedTable, similar to
aggregation. A new `WindowPlanCP windowPlan` field stores the window functions
and their metadata:

```cpp
class WindowPlan : public PlanObject {
 public:
  /// Window function expressions, 1:1 with output columns.
  const QGVector<WindowFunctionCP>& functions() const;

  /// Output columns, 1:1 with functions.
  const ColumnVector& columns() const;

  /// Ranking limit absorbed from a filter predicate (e.g., row_number() <= 5).
  std::optional<int32_t> rankingLimit() const;

  /// Returns a new WindowPlan with the given ranking limit.
  const WindowPlan* withRankingLimit(int32_t limit) const;

  /// Returns a new WindowPlan with additional window functions and columns
  /// appended. Used when merging windows from stacked project nodes.
  const WindowPlan* withFunctions(
      QGVector<WindowFunctionCP> functions,
      ColumnVector columns) const;
};
```

During Phase 1, `ToGraph` populates `windowPlan` when translating `WindowExpr`
nodes. Each window function adds an entry to `windowPlan->functions` (the
`WindowFunction` expression) and `windowPlan->columns` (the output column).
The DT's `exprs` reference the output columns, not the `WindowFunction`
expressions themselves — the same pattern used for aggregation.

To check whether a DT has window functions: `windowPlan != nullptr`. To count
them: `windowPlan->functions.size()`.

#### Filter Pushdown into Window DTs

When a filter triggers a DT boundary (because the current DT has window
functions), the filter becomes a conjunct in the outer DT. During filter
pushdown (`distributeConjuncts`), the inner DT's `addFilter` decides whether to
accept or reject each conjunct:

1. **Non-window predicates on partition keys.** If the predicate depends only on
   columns that are partition keys of every window function in the DT, the
   predicate is accepted and added to `conjuncts` (or `having` if the DT has
   aggregation). It will be placed below the
   window operators during `addPostprocess`, reducing the input to the window.
   This is Pattern 4.

2. **Ranking upper-bound predicates.** If the predicate references a window
   function output and passes `isRankingUpperBoundPredicate`, and the DT has
   exactly one window function, the limit is extracted and stored on the
   `WindowFunction` (see Ranking Limit below). This is Pattern 5.

3. **All other predicates.** Rejected. They stay in the outer DT as regular
   filters — either because they reference non-partition columns (pushing below
   would change window semantics) or because they reference window outputs that
   are not ranking upper bounds.

#### Ranking Limit

When a ranking upper-bound predicate (e.g., `rn <= 5`) is pushed into a
single-window DT during filter pushdown (`distributeConjuncts`), the limit is
stored on the `WindowPlan` via `withRankingLimit`. The `WindowPlan` enforces the
invariant that `rankingLimit` is only valid when there is exactly one window
function.

In `addFilter`, when a ranking predicate passes validation
(`isRankingUpperBoundPredicate`), the limit is extracted and a new `WindowPlan`
is created with the limit set. In `addWindow`, the limit is read from
`WindowPlan::rankingLimit()` and passed to `makeWindowOp`.

#### Phase 2 Processing

During Phase 2 (`addPostprocess`), `addWindow` reads `windowPlan->functions`,
groups them by specification, and creates physical operators. The corresponding
`windowPlan->columns` become the output columns of the `Window` operators.

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
Pattern 5 (filter pushdown) detects a ranking predicate on a `row_number()`
without ORDER BY.

### Physical Level: TopNRowNumber RelationOp

For Patterns 3 and 5 (ranking function with ORDER BY + limit):

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
pushdown (Pattern 5) or LIMIT absorption (Pattern 3).

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

`addWindow` reads `windowPlan->functions`, groups
them by specification (Steps 1-3 of Phase 2), and creates physical operators:
`Window`, `RowNumber`, or `TopNRowNumber` RelationOps depending on the detected
pattern (see Ranking Function Optimizations). When a ranking optimization
consumes the DT's LIMIT (Patterns 2 or 3), `addPostprocess` skips creating a
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

### Full-Partition Frame Merge

The ROWS-only merge rule (Step 2) is overly conservative for full-partition
frames (`UNBOUNDED PRECEDING` to `UNBOUNDED FOLLOWING`). A full-partition frame
processes every row in the partition regardless of ordering, so the frame type
(`ROWS`, `RANGE`, `GROUPS`) is irrelevant — the result is the same under any
sort order. This matters because the SQL standard defaults to `RANGE` when no
explicit frame is specified, and aggregate window functions without ORDER BY
(e.g., `count(*) OVER (PARTITION BY a)`) get the full-partition RANGE frame.
These could safely be merged into any group with the same partition keys, but
`allRowsFrames` currently rejects them. Extending the merge to accept
full-partition frames of any type would allow combining e.g. `row_number() OVER
(PARTITION BY a ORDER BY b)` with `count(*) OVER (PARTITION BY a)` into a
single Window operator.

### Drop ORDER BY for Full-Partition Frames

When a window function uses a full-partition frame (`UNBOUNDED PRECEDING` to
`UNBOUNDED FOLLOWING`) with an ORDER BY, the ORDER BY can be dropped — the
frame already includes every row in the partition regardless of order. For
example:

```sql
-- Original (ORDER BY is redundant — frame covers the entire partition)
SELECT sum(x) OVER (PARTITION BY a ORDER BY b
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t

-- Simplified (same result, no sort needed)
SELECT sum(x) OVER (PARTITION BY a) FROM t
```

The ORDER BY only affects which rows fall within the frame. When the frame is
the entire partition, ordering is irrelevant for any aggregate. The benefit is
eliminating a sort within each partition. This also enables the full-partition
aggregation rewrite (below), which converts the window to a GROUP BY + join.

### Full-Partition Aggregation Rewrite

When a window function uses a full-partition frame (no ORDER BY, or explicit
`UNBOUNDED PRECEDING` to `UNBOUNDED FOLLOWING`), the window can be rewritten as
a regular aggregation joined back to the original table:

```sql
-- Original
SELECT *, agg(x) OVER (PARTITION BY y) FROM t

-- Rewritten
SELECT t.*, g.agg_x
FROM t JOIN (SELECT y, agg(x) AS agg_x FROM t GROUP BY y) g ON t.y = g.y
```

This enables partial/final aggregation, which reduces data volume before the
shuffle — the window path must shuffle all rows. For `OVER ()` (empty partition
keys), the aggregation produces a single row that can be cross-joined.

Caveats:
- Only valid for full-partition frames. Running or sliding frames produce
  per-row results that cannot be expressed as GROUP BY.
- NULL partition keys require `IS NOT DISTINCT FROM` semantics in the join,
  since window partitioning treats NULLs as equal but standard equi-joins
  do not.
- Reads the input twice (once for the base rows, once for the aggregation)
  unless scan sharing is available.
- Only applies to standard aggregates (`sum`, `count`, `avg`, `min`, `max`,
  etc.). Functions like `row_number`, `rank`, `lag`, `lead` have no GROUP BY
  equivalent.

Velox's Window operator partially mitigates the per-row cost: the incremental
aggregation path in `AggregateWindow.cpp` detects that all rows share the same
frame bounds and copies the result instead of recomputing. But the per-row
iteration and copy overhead remains, and the distributed shuffle cost is
unaffected.

### Per-Operator Filter Placement

The current filter pushdown approach (Pattern 4) is all-or-nothing: a filter is
either pushed below all window operators or stays above all of them. A more
granular approach would push the filter between window operators. Consider:

```sql
SELECT * FROM (
  SELECT *, count(*) OVER () as cnt,
    row_number() OVER (PARTITION BY a ORDER BY b) as rn
  FROM t
) WHERE a = 5
```

`count(*) OVER ()` needs all rows, so the filter cannot go below it. But
`row_number() OVER (PARTITION BY a ORDER BY b)` partitions by `a`, so filtering
on `a = 5` before it is safe. The optimal plan is: `count(*) OVER ()` → filter
→ `row_number() OVER (PARTITION BY a ORDER BY b)`. This requires reordering
window operators so that those incompatible with the filter run first, then the
filter, then the remaining windows.

### Data-Expanding Functions

Some aggregate functions used as window functions produce large results per row
(e.g., `array_agg`), significantly increasing data size. Running these early
means subsequent window operators process larger rows — more data to
repartition, sort, and shuffle. However, deferring them may require additional
repartitioning or sorting that would not be needed if they were grouped with
compatible functions earlier. This is a cost-based decision: estimate output
size per window function and weigh the data expansion cost against the
repartition/sort savings.

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

## Testing

### Optimizer Plan Tests

`WindowTest.cpp` in `axiom/optimizer/tests/` (extends `HiveQueriesTestBase`).
Parses SQL via `parseSelect()`, verifies single-node plans with
`toSingleNodePlan()` + `PlanMatcherBuilder`, and distributed plans with
`planVelox()` + `AXIOM_ASSERT_DISTRIBUTED_PLAN`.

`PlanMatcherBuilder` matchers: `window()`, `rowNumber()`, `topNRowNumber()` in
`PlanMatcher.h/cpp`. Follow the `singleAggregation()` pattern.

Test cases:

**Grouping and sort reuse:**
- Single window function → one Window operator.
- Multiple functions with same specification → grouped into one operator.
- ROWS-only merge: shorter ORDER BY merged into longer ORDER BY group.
- Sort reuse: `inputsSorted=true` when ORDER BY is a prefix.
- Empty partition keys processed last.

**Stacked windows:**
- Window functions from stacked projects with different specs → separate Window
  operators.
- Window functions from stacked projects with the same spec → combined into a
  single Window operator.

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
- Partition-key filter pushdown: `SELECT * FROM (SELECT *,
  sum(x) OVER (PARTITION BY a) as s FROM t) WHERE a = 5 AND s > 10` → `s > 10`
  stays above the window. `a = 5` is pushed below the window because `a` is a
  partition key of every window function (Pattern 4).

Target: `buck test fbcode//axiom/optimizer/tests:window`

### Ranking Function Optimization Tests

`RankingTest.cpp` in `axiom/optimizer/tests/` (extends
`HiveQueriesTestBase`). Parses SQL via `parseSelect()`, verifies single-node
plans with `toSingleNodePlan()` + `PlanMatcherBuilder`, and distributed plans
with `planVelox()` + `AXIOM_ASSERT_DISTRIBUTED_PLAN`.

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

**Filter on ranking function output (Pattern 5):**
- `rn <= 5` → TopNRowNumber(limit=5). Both single-node and distributed.
- `rn <= 5` with PARTITION BY → TopNRowNumber with partition keys. Distributed:
  shuffle + TopNRowNumber.
- `rn <= 5` without ORDER BY → RowNumber(limit=5).
- `rn < 5` → TopNRowNumber(limit=4).
- `rn = 1` → TopNRowNumber(limit=1).
- `rn <= 5` with additional non-window predicates → TopNRowNumber + filter.
- `rn >= 3 AND rn <= 10` → TopNRowNumber(limit=10) + filter on `rn >= 3`.
- `rn <= 5` with multiple window functions → Window + filter (no optimization).
- Non-window filter with window function → Window + filter (no optimization).
- `rn <= 5` with LIMIT → TopNRowNumber + Limit.

**Partition-key filter pushdown (Pattern 4):**
- Filter on partition key → pushed below window.
- Filter on non-partition column → stays above window.
- Filter on partition key shared by all window functions → pushed below.
- Filter on partition key of one but not all window functions → stays above.

Target: `buck test fbcode//axiom/optimizer/tests:ranking`

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

Add window function test queries to `axiom/optimizer/tests/sql/window.sql`. The
`SqlTest` harness (`SqlTest.cpp` /
`SqlTestBase.h`) parses SQL queries from `.sql` files, runs each through the
full Axiom pipeline (parse → optimize → execute via `LocalRunner`), and
compares results against DuckDB.

Each query in the `.sql` file becomes a separate gtest case. Annotations
control behavior:
- Default: unordered result comparison against DuckDB.
- `-- ordered`: ordered result comparison.
- `-- duckdb: <sql>`: use alternate SQL for the DuckDB reference (useful when
  Axiom and DuckDB syntax differs).
- `-- error: <message>`: expect a failure containing the given message.

Test cases:
- Basic window functions: `sum`, `avg`, `count`, `min`, `max` with PARTITION
  BY and ORDER BY.
- All frame types: ROWS, RANGE with various bounds (UNBOUNDED PRECEDING,
  N PRECEDING, CURRENT ROW, N FOLLOWING, UNBOUNDED FOLLOWING).
- Ranking functions: `row_number()`, `rank()`, `dense_rank()`.
- Multiple window functions with same and different specifications.
- Window functions combined with GROUP BY, HAVING, ORDER BY, LIMIT.
- Subquery with window function and outer filter.
- Ranking function optimizations (Patterns 1–5) — verify correct results
  despite plan transformations.

Target: `buck test fbcode//axiom/optimizer/tests:sql`

### Verification Commands

```bash
buck test fbcode//axiom/optimizer/tests:window
buck test fbcode//axiom/optimizer/tests:ranking
buck test fbcode//axiom/optimizer/tests:sql
buck test fbcode//axiom/optimizer/tests:subfields
buck build fbcode//axiom/...
```
