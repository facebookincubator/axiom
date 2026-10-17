# Fixed Point

*How a recursive query becomes a Velox plan: the `FixedPoint` / `WorkingTable`
IR nodes, the invariants each pass must preserve, and what is deliberately
rejected.*

**Status:** recursive queries plan and run on one or more workers — see
[Distributed execution](#distributed-execution). Nested recursion, multiple
recursive references per step, and multiple fixed points per execution
fragment are rejected — see [Boundaries](#boundaries).

## Introduction

Most SQL queries have linear logical chains: data flows from input sources to
output results. Iterative and recursive logic is historically uncommon, but
matters for newer workloads like graph and vector search.

A **fixed point** operator is a customizable for-loop:

```
delta = anchor();
result = delta;
while (!convergence(delta)) {
  delta = step(delta);
  result += delta;
}
```

Reaching `maxIterations` without `convergence` holding is an error, not a
partial answer — see invariant 6.

Three components describe it:

- **`anchor()`** — initializes the set.
- **`step(delta)`** — the loop body.
- **`convergence(delta)`** — decides when to terminate.

In practice a **recursion limit** bounds the loop, and further simplifying
assumptions apply — notably **append-only results**. Several are expandable;
see [Invariants](#invariants).

The problems this shape solves:

| Surface syntax | Underlying computation |
| --- | --- |
| SQL `WITH RECURSIVE` | anchor + step, converge on empty delta |
| Cypher `-[:R*1..n]->` / SQL/PGQ `MATCH` | fixed point over the edge relation, bounded iterations |
| Reachability / transitive closure | fixed point, UNION ALL step |
| Shortest path, connected components, PageRank | *general* fixed point (aggregation-in-step, custom convergence) |

Only the first row is supported today. Recursive CTEs are the narrow SQL entry
point; `LogicalPlan` is the dialect-agnostic input, so a fixed point is also
constructible directly through `PlanBuilder::fixedPoint`. Cypher is the intended
next surface: variable-length patterns are currently planned by inline-expanding
UNIONs for a fixed hop count (see
[UnionAllPlanning.md](UnionAllPlanning.md)), and a fixed point
replaces that unrolling.

## End-to-end mapping

A counter query:

```sql
WITH RECURSIVE counter(n) AS (
  SELECT 1                                -- anchor
  UNION ALL
  SELECT n + 1 FROM counter WHERE n < 10  -- step
)
SELECT * FROM counter;
```

It maps directly onto the loop above: the anchor is `SELECT 1`, the step is
`SELECT n + 1 WHERE n < 10`, the loop stops once an iteration adds no rows, and
the result is `{1, 2, … 10}`. Every fixed point the optimizer plans stops this
way; a convergence test of another shape is a Velox capability the optimizer
does not use.

### 1. Logical plan

The self-reference is a `RecursiveReferenceNode`. There is no working table
yet — Translate introduces it.

```
- Project                                  (SELECT *)   -> n:INTEGER
  - FixedPoint[name=counter]                             -> n:INTEGER
    anchor:
    - Values (1)                                         -> n:INTEGER
    step:
    - Project n + 1
      - Filter n < 10
        - RecursiveReference[counter]                    -> n:INTEGER
```

### 2. Optimizer v2 IR

The IR tracks internal details that change over time.

```
- FixedPoint[name=counter, maxIterations=1000, recursiveNumDrivers=unplanned] -> c01:INTEGER
  anchor:
  - Values -> c01:INTEGER
  step:
  - Project -> expr2:INTEGER
    expr2 := plus(c01, 1)
    - Filter -> c01:INTEGER
      predicate: lt(c01, 10)
      - WorkingTable[name=counter, readMode=latestDelta] -> c01:INTEGER
```

### 3. Velox plan

The delta / working-table representation becomes a state that outlives
iterations and is seeded from the anchor.

```
- ProjectNode                                        (SELECT *)
  - FixedPointNode[state=counter, maxIterations=1000]
      stateDeclarations:
        VectorStateDeclaration[counter, append=true]:
          - ValuesNode                               (VALUES 1)   <- seeds state
      plans (step):
        - ProjectNode  n + 1
          - FilterNode  n < 10
            - StateSourceNode[counter, delta=true]                <- latest delta
      convergence: stop when an iteration writes no rows
```

### Cross-layer correspondence

| Logical plan | Optimizer v2 IR | Velox plan |
| --- | --- | --- |
| `RecursiveReferenceNode[counter]` | `WorkingTable[counter, latestDelta]` | `StateSourceNode(counter, delta=true)` |
| — | inner equi-join in the step, build side without a `WorkingTable` | `HashTableStateDeclaration` + `StateHashJoinNode` |
| — | other subtree of the step without a `WorkingTable` | `VectorStateDeclaration` + `StateSourceNode(delta=false)` |
| anchor branch | anchor branch | `VectorStateDeclaration(counter, append=true)` |
| — | — | `ConvergenceConfig::whenDeltaEmpty(maxIterations)` |

`WorkingTableReadMode` also has an `kAccumulated` mode, which emits
`delta=false` — a read of the whole state rather than the last iteration's
output. Translate never produces it today; it exists for step shapes that need
the full result set.

## Invariants

Each item below is one `VELOX_CHECK` in the `FixedPoint` or `WorkingTable`
constructor. Stated up front so they can be checked against rather than
inferred. Policies that other passes impose, rather than the constructor, are
listed separately after the list.

1. **Named state.** A `FixedPoint`'s name is non-null, and a `WorkingTable`
   references its fixed point by that name: the constructor rejects a branch that
   reads any state other than the one this fixed point binds. An orphan recursive
   reference with no enclosing fixed point is rejected in Translate.

2. **Each branch reads the state it should, with the state's own columns.** The
   anchor must read no recursive state at all; the step must read exactly this
   fixed point's state, with columns pointer-identical to the fixed-point
   output. `Node::requiredStates()` carries, per node, the recursive
   states that subtree needs an enclosing `FixedPoint` to provide, each mapped to
   the columns its reads present: the union of its inputs' entries, where a
   `WorkingTable` requires the state it reads and a `FixedPoint` satisfies the
   one it binds. The map is empty for a self-contained subtree, and the
   constructor checks presence *and* column identity with one lookup per branch
   instead of searching the subtrees. Two reads of one state that disagree on
   columns are rejected where they meet. `Builder::make` primes the map once per
   node, so a shared subplan is walked once.

3. **Schema equivalence, not name equality.** Anchor and step output types must
   be equivalent in types and positions, not identical column ids. The step's
   final projection may allocate fresh canonical ids from the shared
   `NameAllocator`, so an identity check would spuriously fail.

4. **Recursive-state identity is explicit.** The fixed-point output and working
   table share `outputColumns`; the anchor supplies the initial values through
   the positionally aligned `sourceColumns`. A pass may rewrite columns within
   a branch as long as it restores both boundary schemas before rebuilding the
   `FixedPoint`.

5. **Bounded.** `maxIterations >= 1`. A SQL recursive CTE carries no depth hint of
   its own, so it inherits the session `recursion_limit` (default 1000) as a
   safety cap; a shape with its own bound — a `1..5` hop pattern — sets
   `maxIterations` per node instead. Emit builds the `ConvergenceConfig` with
   `ConvergenceConfig::whenDeltaEmpty`, which sets
   `errorWhenMaxIterationReached`, so a query that has not converged within the
   bound fails rather than returning a partial answer.

### Policies the passes impose

These hold for every plan the optimizer produces today, but no constructor check
enforces them — a hand-built node may violate them.

- **Append-only state.** The loop only adds rows; Emit declares the state
  `append`.

## Per-pass changes

- **Translate** — lowers a recursive CTE into `FixedPoint(anchor, step)`.
  `translateRecursiveRef` binds each `RecursiveReferenceNode` to the anchor's
  `Column*`s and emits a latest-delta `WorkingTable`.

- **PrecomputeProjections** — `rewriteFixedPoint` restores each branch's schema
  after projection precompute, so the anchor and step keep presenting equivalent
  output (invariant 3).

- **PushdownAndPrune** — an outer filter is not pushed into the anchor, and the
  anchor and step keep `nonNullColumns` empty; otherwise `OuterJoinReduction`
  could drop null-padded seed rows whose step descendants become non-null via
  `coalesce`. The pass may replace a right inner-join key within a branch; it
  restores every branch's original output columns before rebuilding the
  `FixedPoint` (invariant 4).

- **LimitAndOrder** — `FixedPoint` and `WorkingTable` are limit barriers. An
  outer `LIMIT` or ordering cannot be pushed across the loop boundary.

- **EstimateLeafStats** — `FixedPoint` and `WorkingTable` report unknown
  cardinality. Convergence depth and per-iteration frontier size are
  workload-dependent, and a single-pass sum would understate them and skew join
  placement, so a dependent join cluster is left uncostable and falls back to its
  written join shape at that boundary.

- **PlanPhysical** — plans the anchor with the query's workers, and the step
  with a nested rewriter pinned to one worker and one driver, and records the
  result as `recursiveNumDrivers`. With several workers, each subtree of the
  step that reads no recursive state is planned with the query's workers and
  placed on every task that runs the loop — see
  [Distributed execution](#distributed-execution).

- **Emit** — emits `ConvergenceConfig::whenDeltaEmpty(maxIterations)`: Velox
  stops on the row count of the delta it already holds. The step is emitted
  single-driver by swapping `numDrivers` in the emitter's options copy for the
  duration of the recursive subtree, so the surrounding query keeps its own
  parallelism. Restoring parallelism *inside* the
  loop is intended but not shipped.

  A fixed point's fragment takes its type from its inputs: a source fragment
  when its anchor scans a table, a single-task one when the anchor is gathered.

  Velox delivers splits to a fixed point's initial plans and to the nodes above
  the loop, so every part of the step that reads no recursive state becomes an
  initial plan, run once before the first iteration. An inner equi-join's build
  side becomes a `HashTableStateDeclaration`, probed by a `StateHashJoinNode`.
  Any other such subtree becomes a `VectorStateDeclaration`, read by a
  `StateSourceNode`.

## Distributed execution

With several workers, the fixed point runs on the tasks that run its anchor,
each as an independent loop over its own anchor rows. Every subtree of the step
that reads no recursive state is placed whole on each of those tasks: broadcast
when the anchor is spread over several tasks, gathered when it is on one. Each
task's loop stops when its own delta is empty.

The loops together compute the fixed point when each row of the step derives
from one row of the recursive state: the state flows only through filters,
projections, unnests, inner joins, and outer, semi and anti joins on the side
whose rows they keep. Otherwise PlanPhysical gathers the anchor, and the loop runs on one task. The
fixed point's output has no partitioning, or is gathered when its anchor is.

A step that needs every row of a key on one task, such as a visited-set
anti-join, needs partitioned loops: the delta repartitioned on a key each
iteration, and one stop decision over every worker's delta. Velox supports
both: a body that repartitions, and a verdict plan that worker 0 runs for every
worker (`ConvergenceConfig::globalVerdict`). The optimizer does not plan them
yet.

## Boundaries

Each of these is rejected with a specific error rather than mis-planned.

- **Multiple recursive references per step (non-linear recursion).** A step with
  two references would compute `delta ⋈ delta`. The SQL standard and PostgreSQL
  reject a recursive reference appearing more than once; DuckDB evaluates
  `(delta, delta)`. Semi-naive evaluation assumes a single linear reference, so
  both the semantics and the delta-iteration strategy are undecided. Because v2
  consumes a logical plan directly rather than only SQL, a two-reference step is
  constructible via `PlanBuilder`, so the rejection lives in the translator, not
  only in the SQL parser.

- **Nested recursion and multiple fixed points per fragment.** Two enforcement
  points, not a missing capability: Translate rejects a nested fixed point
  because `activeFixedPoint_` tracks a single enclosing loop, and Emit's
  `validateFixedPointLeaves` raises `VELOX_NYI` when more than one
  `FixedPointNode` appears in *leaf position* within one fragment. Per-loop
  identity is not the blocker — the interned state `Name` already distinguishes
  loops end to end, through `WorkingTable`, `FixedPoint`, `RequiredStates`, and
  the emitted `StateSourceNode` and `VectorStateDeclaration`. Sibling fixed
  points that are not both fragment leaves already plan today, as
  `FixedPointTest.siblingRecursions` shows.

## Tests

- `optimizer/tests/FixedPointTest.cpp` — SQL-level and `PlanBuilder`-level plan
  assertions via `PlanMatcher`, including every rejection above.
  `FixedPointMatch` describes the parts of a `FixedPointNode` to check: the
  output state and its initial plan, each per-iteration plan, and that the loop
  stops on an empty delta within its iteration bound.
- `optimizer/v2/tests/NodePrinterTest.cpp` — the IR printer's rendering of a
  fixed point, built directly through `Builder`.

## References

- D111499361 — [Axiom] feat(optimizer/v2): Plan and lower WITH RECURSIVE queries
- Workplace post: [*Velox and Axiom for graph queries: a primer*](https://fb.workplace.com/groups/1588734491493342/permalink/2823843241315788/) — motivates the
  CQL roadmap and introduces recursive CTEs, variable-length patterns, and
  fixpoint operators.
