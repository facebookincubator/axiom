# Optimizer v2 Pass Cheat Sheet

This page maps an optimization to the pass that owns it. The source of truth
for pass order is `Optimizer::planTo` in `Optimize.cpp`.

## Pipeline

```text
logical plan
  -> Translate
Node IR
  -> Decorrelate
  -> LimitAndOrder
  -> PushdownAndPrune
  -> FoldMetadataAggregate
  -> ConnectorPushdown       (when a connector supports subtree pushdown)
  -> EstimateLeafStats       (when useFilteredTableStats is enabled)
  -> LowerRepeat             (when the plan has a Repeat)
  -> PlanPhysical
  -> Emit
MultiFragmentPlan of Velox plan nodes
```

`Translate` produces the Node IR. Every stage through `PlanPhysical` consumes
and returns that IR. `EstimateLeafStats` also annotates shared table and column
objects in place. `Emit` lowers the finished IR and is therefore not an
`Optimizer::Pass` boundary.

## Quick reference

| Stage | Traversal | Main work | Typical result |
|---|---|---|---|
| `Translate` | Logical plan -> Node IR | Builds Node IR, resolves expressions, performs early constant folding and normalization, and lifts subqueries | Constant-false filter -> empty `Values`; subquery -> `Apply` |
| `Decorrelate` | Bottom-up, then iterative per `Apply` | Eliminates correlated `Apply` nodes while preserving scalar, `IN`, and `EXISTS` semantics | Correlated subquery -> join plus any required grouping, ranking, or cardinality checks |
| `LimitAndOrder` | Top-down | Combines row bounds and moves them through safe operators; removes unobserved ordering | `Sort` + bounded `Limit` -> `TopN`; nested limits -> one bound |
| `PushdownAndPrune` | Top-down decisions, bottom-up rebuild | Pushes predicates, prunes columns and computations, simplifies joins and ranking, and rewrites expressions using facts learned below | Filter reaches scan; outer join -> inner join; unused output disappears |
| `FoldMetadataAggregate` | Bottom-up | Resolves aggregates that a connector can answer from metadata | Metadata count over `Scan` -> `Values`, or an equivalent aggregate that scans rows |
| `ConnectorPushdown` | Bottom-up discovery, then substitution | Offers maximal connector-local subtrees and replaces accepted roots | Relational subtree -> scan of a connector-provided virtual table |
| `EstimateLeafStats` | Scan collection, then bottom-up rewrite | Fetches post-filter connector statistics and removes subtrees proven empty | Base-table estimates become available; an empty inner-join input removes the join |
| `LowerRepeat` | Bottom-up | Replaces each `Repeat` with a `FixedPoint` that walks from the side with fewer estimated rows | Repeated graph pattern -> anchor, step over the hop, and a join with the other side |
| `PlanPhysical` | Bottom-up with join enumeration | Chooses join order and data distribution and inserts exchanges | Logical join cluster -> costed join tree; global operators -> distributed stages |
| `Emit` | Node IR -> `MultiFragmentPlan` | Lowers IR nodes and exchanges into executable Velox plan nodes and fragments | User-visible output layout and executable fragments |

## Pass details

### 1. Translate

`TranslatePass` converts `logical_plan` nodes and expressions into the v2 Node
IR, recording the user-visible output columns and names and whether any
referenced connector supports subtree pushdown.

While building expressions and nodes, it performs normalization and early
simplification:

- Folds calls whose arguments are literals and simplifies filter conjuncts.
- Propagates literals fixed by filter equalities through projections, grouping
  keys, and set operations when their semantics preserve the equality.
- Replaces a filter that is always false or NULL with empty `Values`.
- Normalizes `IN` lists, including duplicate constants and single-value lists.
- Removes duplicate ordering keys that cannot refine an order.
- Lifts subqueries into `Apply` nodes for decorrelation.
- Folds supported uncorrelated scalar subqueries whose result can be computed
  from constants or enumerated discrete-predicate values.

This pass establishes IR identity and expression sharing. It does not choose
join order or distribution.

### 2. Decorrelate

`DecorrelatePass` removes every `Apply` before passes that only understand
ordinary relational nodes. It first decorrelates nested `Apply` nodes, then
repeatedly peels the outermost operator from each correlated body until the
body no longer references the input.

Peel rules cover filters, projects, aggregates, limits, joins,
`AssignUniqueId`, and `Unnest`. The terminal rewrite produces a join and adds
the operators needed to preserve the original subquery semantics, including
single-row checks and per-input-row grouping or ranking. Unsupported correlated
shapes fail with a targeted `VELOX_NYI`.

See [Decorrelate overview](Decorrelate.md) for the rule-specific documents.

### 3. LimitAndOrder

`LimitAndOrderPass` carries a pending row bound down the tree and tracks whether
the query still observes an input order.

- Collapses nested `Limit` and `TopN` bounds, including offsets.
- Fuses a bounded `Limit` over `Sort` into `TopN`.
- Pushes limits through one-to-one operators such as `Project`,
  `AssignUniqueId`, and `MarkDistinct`.
- Gives each `UnionAll` leg a local `offset + count` bound while retaining the
  global bound above the union.
- Limits the build of an unconditional existence join to one row.
- Drops a `Sort` when no consumer observes its order.
- Pushes a bound through an unordered single `row_number`; other windows are
  barriers because their values depend on the full partition.

A zero-row bound becomes empty `Values`. Operators that can change the meaning
of the bound keep it immediately above them.

### 4. PushdownAndPrune

`PushdownAndPrunePass` combines predicate placement and required-column
tracking in one traversal. On descent it carries pending conjuncts and columns
required by consumers. On ascent it rebuilds expressions and nodes using facts
established by their inputs.

Predicate work:

- Splits filters into conjuncts and pushes each as far as its referenced
  columns and evaluation semantics allow.
- Substitutes deterministic project expressions and grouping keys into
  predicates.
- Routes join predicates to an input, the join filter, or a filter above the
  join.
- Converts outer joins to less preserving join kinds when a predicate or a
  proven non-NULL column rejects a padded side.
- Converts a consumed semi-project mark into a semi or anti filter join.
- Offers predicates that reach a scan to the connector and keeps rejected
  predicates in one filter above the scan.
- Derives additional scan filters implied by equalities.

Pruning and simplification work:

- Simplifies node expressions with constants fixed by predicates pushed below
  the node before determining which input columns they require.
- Removes unused project expressions, aggregate calls, window functions,
  unique IDs, and unused outputs from other nodes.
- Replaces constant-false filters and joins with the appropriate empty or
  preserved-side plan.
- Specializes eligible ranking windows to `RowNumber` or `TopNRowNumber` and
  consumes compatible rank bounds.
- Replaces an `Unnest` with a non-empty-input test when duplicate rows and
  unnested outputs are irrelevant to the consumer.
- Rewrites expressions using equi-join semantics. An inner join replaces its
  right key column with the equal left key and removes the right key from its
  output. Inner and one-sided outer joins replace
  `coalesce(left_key, right_key)` with one key; full joins canonicalize operand
  order. Null-padded key expressions must return NULL on NULL input. `GroupId`
  stops these identities because grouping sets can null the keys independently.

These identities are discovered while the tree is rebuilt upward. Each parent
rewrites its expressions and column lists before propagating the identities.
The plan root, `TableWrite` input, and each `FixedPoint` branch restore their
original column layout with a `Project`. Predicate routing and column pruning
for that traversal have already happened, so a newly simplified expression may
leave an extra column or miss another pushdown opportunity. A second run can
recover those opportunities if needed.

### 5. FoldMetadataAggregate

SQL expression planning creates metadata aggregates. In the Presto dialect,
these are `approx_count_star`, `approx_null_count`, and
`approx_non_null_count`. Each carries a `SpecialAggregateKind` and, when one
exists, an equivalent row-scanning aggregate.

When an aggregation contains only metadata aggregates directly over a `Scan`,
the pass asks the connector for grouped row and null counts. A successful
answer becomes a `Values` node. Otherwise, `FoldMetadataAggregatePass` uses the
row-scanning aggregate: `count(*)`, `count_if(is_null(column))`, or
`count(column)`, respectively. A metadata aggregate that has neither a metadata
answer nor a row-scanning equivalent is rejected here, before emit.

This pass runs after scan-filter pushdown so the connector answers for the same
filtered input the query will read.

### 6. ConnectorPushdown

`ConnectorPushdownPass` finds maximal subtrees that belong to one connector
and do not require optimizer state the connector cannot reproduce. It offers
those subtrees to connector metadata implementations concurrently.

A connector may accept the offered root or disjoint roots within it. Each
accepted root becomes a scan of a connector-provided virtual table, with a
projection restoring the column identities expected above the replaced
subtree. The pass is skipped when translation found no connector that
advertises this capability.

This is whole-subtree pushdown. Predicate pushdown into an existing scan belongs
to `PushdownAndPrunePass`.

### 7. EstimateLeafStats

`EstimateLeafStatsPass` visits the reachable `Scan` nodes and asks each
connector for statistics using the scan handle produced after filter and
subtree pushdown. The requests run concurrently. The pass annotates:

- `BaseTable::filteredCardinality` with the post-filter row count.
- Each column value with min, max, and NDV statistics.

When a connector proves that accepted filters match no rows, the pass also
propagates emptiness upward. Operators that cannot produce rows from empty
input disappear. An `EnforceSingleRow` becomes one `Values` row of NULLs. An
operator that must still execute, such as a global aggregation, remains with an
empty `Values` input.

`EstimateProvider` uses these annotations during physical planning and falls
back to constraint-based estimates when connector statistics are unavailable.
It is `EstimateProvider`, not this pass, that derives estimates for intermediate
nodes. The entire pass is gated by `useFilteredTableStats`.

See [Cardinality estimation](CardinalityEstimation.md).

### 8. LowerRepeat

`LowerRepeatPass` replaces each `Repeat`, which a repeated graph pattern
produces, with a `FixedPoint`. The walk starts from the side with fewer
estimated rows: the start, or the end when it is present and smaller. The
state carries the columns of that side the result reads, the walk's current
vertex, and its hop count.

- The anchor is that side, with zero hops.
- The step joins the latest walks that have fewer than the maximum hops with
  the hop relation, on the current vertex and the hop's source, and moves the
  current vertex to the hop's destination.
- The result keeps the walks with at least the minimum hops and joins the
  other side on the current vertex. A `Repeat` whose walk ends at a row of the
  start keeps the walks whose current vertex is that row.

Walks of at most one hop need no loop: the anchor for zero hops, the side
joined with the hop for one, or the `UNION ALL` of the two.

The pass runs after `EstimateLeafStats` because it chooses the side by
estimate, and before `PlanPhysical`, which plans the joins it produces. It is
skipped when translation produced no `Repeat`.

See [Graph queries — repeated edges](GraphTable-repetition.md).

### 9. PlanPhysical

`PlanPhysicalPass` turns the logical Node IR into a distributed physical tree.
The optimizer chooses the worker count immediately before this pass, and the
pass uses that width when costing joins and shaping exchanges.

Join planning:

- Finds maximal clusters of reorderable equi-joins.
- Uses DPhyp and the cost model to choose join order, build side, and
  distribution.
- Falls back to greedy enumeration when the DPhyp budget is exhausted.
- Keeps the written join tree when no valid costed plan is available or a join
  is not reorderable. These automatic fallbacks still choose the build side
  and assign valid distributed inputs.
- Uses broadcast, hash partitioning, gather, or grouped reads from bucketed
  tables according to join semantics and cost.

Other physical planning:

- Splits eligible aggregations into partial and final stages and inserts a
  remote exchange when groups must move between tasks. Emit supplies any local
  exchange needed between drivers.
- Co-locates window partitions, row numbering, and distinct checks on their
  keys; keyless global operations gather to one task.
- Distributes global sort as local sorts followed by a merge gather.
- Distributes `Limit` and `TopN` with partial per-task bounds before the final
  gather.
- Preserves compatible bucketing through scans and unions where it avoids a
  shuffle.
- Repartitions table writes on the target layout when required. For a
  row-level DELETE, asks the connector for its layout, adds the columns it
  computes from row IDs, and sorts each writer's rows when the layout requires
  an order.

See [Distributed execution](DistributedExecution.md) and
[Join enumeration scaling](JoinEnumerationScaling.md).

### 10. Emit

`EmitPass` lowers the physical Node IR to Velox plan nodes. Each IR `Exchange`
becomes a fragment boundary with a producer and consumer. Emit also restores
the user-visible output layout, attaches estimates to emitted node IDs, records
table-write completion information, and validates every fragment.

Emit implements representation-specific lowering. Logical and physical choices
belong to earlier passes.

## Inspecting pass boundaries

Use `last_pass` with v2 `EXPLAIN (TYPE OPTIMIZED)` to stop after a named IR
pass:

```sql
EXPLAIN (
  TYPE OPTIMIZED WITH (last_pass = 'pushdown_and_prune')
)
SELECT ...;
```

Pass names are case-insensitive:

```text
TRANSLATE
DECORRELATE
LIMIT_AND_ORDER
PUSHDOWN_AND_PRUNE
FOLD_METADATA_AGGREGATE
CONNECTOR_PUSHDOWN
ESTIMATE_LEAF_STATS
LOWER_REPEAT
PLAN_PHYSICAL
```

Compare adjacent boundaries to identify the pass that introduced a node or
rewrite. Estimates appear starting at `ESTIMATE_LEAF_STATS`. Use
`EXPLAIN (TYPE EXECUTABLE)` to inspect the Velox plan after `Emit`.

## Key ordering dependencies

- `Decorrelate` removes every `Apply` before pushdown and physical planning.
- `PushdownAndPrune` finalizes scan predicates before metadata aggregation and
  filtered-table statistics consult connectors.
- `ConnectorPushdown` runs before leaf statistics so virtual-table scans can be
  estimated like other scans.
- `EstimateLeafStats` supplies the base statistics used by `LowerRepeat` and
  `PlanPhysical`.
- `LowerRepeat` replaces every `Repeat` before physical planning.
- `PlanPhysical` introduces the exchanges that `Emit` turns into fragment
  boundaries.

For related debugging and test workflows, see
[Debugging tips](DebuggingTips.md) and
[Optimizer testing](Testing.md).
