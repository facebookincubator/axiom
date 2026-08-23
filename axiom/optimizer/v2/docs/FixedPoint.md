# Fixed Point

*How a recursive query becomes a Velox plan: the `FixedPoint` / `WorkingTable`
IR nodes, the invariants each pass must preserve, and what is deliberately
rejected.*

**Status: planning and lowering only.** This machinery produces a Velox plan for
a recursive query; it does not run one yet. Distributed recursion, nested
recursion, multiple recursive references per step, and multiple fixed points per
execution fragment are rejected — see [Boundaries](#boundaries).

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
[UnionAllPlanning.md](../../docs/UnionAllPlanning.md)), and a fixed point
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
`SELECT n + 1 WHERE n < 10`, convergence is `count(*) == 0` over the latest
delta, and the result is `{1, 2, … 10}`.

### 1. Logical plan

The self-reference is a `RecursiveReferenceNode`. There is no convergence branch
and no working table yet — Translate introduces both.

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

The IR tracks internal details that change over time. The default empty-delta
convergence is expanded into a real subtree so it is optimized like any other
plan.

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
  convergence:
  - Project -> converged4:BOOLEAN
    converged4 := eq(__converged_count3, 0)
    - Aggregate -> __converged_count3:BIGINT
      aggregates: count()
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
      convergence (ConvergenceConfig):
        - ProjectNode  eq(count, 0) -> converged
          - AggregationNode  count()
            - StateSourceNode[counter, delta=true]                <- latest delta
```

### Cross-layer correspondence

| Logical plan | Optimizer v2 IR | Velox plan |
| --- | --- | --- |
| `RecursiveReferenceNode[counter]` | `WorkingTable[counter, latestDelta]` | `StateSourceNode(counter, delta=true)` |
| anchor branch | anchor branch | `VectorStateDeclaration(counter, append=true)` |
| — (synthesized in Translate) | `convergence:` `Aggregate count()` -> `Project eq(count, 0)` | `ConvergenceConfig.plan`: `AggregationNode(count)` -> `ProjectNode(count == 0)` |

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
   anchor must read no recursive state at all; the step and convergence must each
   read exactly this fixed point's state, with columns pointer-identical to the
   fixed-point output. `Node::requiredStates()` carries, per node, the recursive
   states that subtree needs an enclosing `FixedPoint` to provide, each mapped to
   the columns its reads present: the union of its inputs' entries, where a
   `WorkingTable` requires the state it reads and a `FixedPoint` satisfies the
   one it binds. The map is empty for a self-contained subtree, and the
   constructor checks presence *and* column identity with one lookup per branch
   instead of searching three subtrees. Two reads of one state that disagree on
   columns are rejected where they meet. `Builder::make` primes the map once per
   node, so a shared subplan is walked once.

3. **Schema equivalence, not name equality.** Anchor and step output types must
   be equivalent in types and positions, not identical column ids. The step's
   final projection may allocate fresh canonical ids from the shared
   `NameAllocator`, so an identity check would spuriously fail.

4. **Column-pointer identity is shared.** The anchor, the working table, and the
   fixed-point output refer to the same `Column*` objects for the recursive
   state, which invariant 2 enforces. It is also why output pruning is deferred:
   dropping or renaming a fixed-point output column would break the identity the
   step relies on. Pruning the state is possible, but only as one coordinated
   rewrite of the output, all three branches, and every `WorkingTable` — see the
   TODO in `TranslatePass::translateFixedPoint`.

5. **One BOOLEAN convergence column.** The convergence plan must produce exactly
   one BOOLEAN column. Any single-column BOOLEAN subtree that reads the state is
   accepted; the constructor does not check how that boolean is computed.

6. **Bounded.** `maxIterations >= 1`. A SQL recursive CTE carries no depth hint of
   its own, so it inherits the session `recursion_limit` (default 1000) as a
   safety cap; a shape with its own bound — a `1..5` hop pattern — sets
   `maxIterations` per node instead. Emit builds the `ConvergenceConfig` with
   `ConvergenceConfig::converging`, which sets
   `errorWhenMaxIterationReached`, so a query that has not converged within the
   bound fails rather than returning a partial answer.

### Policies the passes impose

These hold for every plan the optimizer produces today, but no constructor check
enforces them — a hand-built node may violate them.

- **Append-only state.** The loop only adds rows; Emit declares the state
  `append`.
- **Convergence is an empty delta.** Translate synthesizes `count(*) == 0` over
  the latest delta as the convergence plan. Invariant 5 admits any one-column
  BOOLEAN shape, so a future step could supply its own predicate.

## Per-pass changes

- **Translate** — lowers a recursive CTE into `FixedPoint(anchor, step,
  convergence)` and synthesizes the convergence plan.
  `translateRecursiveRef` binds each `RecursiveReferenceNode` to the anchor's
  `Column*`s and emits a latest-delta `WorkingTable`.

- **PrecomputeProjections** — `rewriteFixedPoint` restores each branch's schema
  after projection precompute, so the anchor and step keep presenting equivalent
  output (invariant 3).

- **PushdownAndPrune** — an outer filter is not pushed into the anchor, and the
  anchor and step keep `nonNullColumns` empty; otherwise `demoteOuterToInner`
  could drop null-padded seed rows whose step descendants become non-null via
  `coalesce`. Output pruning is deferred to preserve column identity
  (invariant 4).

- **LimitAndOrder** — `FixedPoint` and `WorkingTable` are limit barriers. An
  outer `LIMIT` or ordering cannot be pushed across the loop boundary.

- **EstimateLeafStats** — `FixedPoint` and `WorkingTable` report unknown
  cardinality. Convergence depth and per-iteration frontier size are
  workload-dependent, and a single-pass sum would understate them and skew join
  placement, so a dependent join cluster is left uncostable and falls back to its
  written join shape at that boundary.

- **PlanPhysical** — plans the step and convergence with a nested rewriter
  pinned to one worker and one driver, and records the result as
  `recursiveNumDrivers`.

- **Emit** — transcribes the convergence branch already present in the IR into a
  Velox `ConvergenceConfig`; nothing about *when* to stop is decided at emit. The
  step and convergence are emitted single-driver by swapping `numDrivers` in the
  emitter's options copy for the duration of the recursive subtree, so the
  surrounding query keeps its own parallelism. Restoring parallelism *inside* the
  loop is intended but not shipped.

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

- **Distributed recursion (`numWorkers > 1`).** The working-table state is shared
  and mutated across iterations, and the step and convergence run single-driver.
  Distributing needs coordinated cross-worker state and a distributed
  convergence test. Emit raises `VELOX_NYI` for any query containing a fixed
  point at `numWorkers > 1`.

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
  output state and its initial plan, each per-iteration plan, and the
  convergence shape and iteration bound.
- `optimizer/v2/tests/NodePrinterTest.cpp` — the IR printer's rendering of a
  fixed point, built directly through `Builder`.

## References

- D111499361 — [Axiom] feat(optimizer/v2): Plan and lower WITH RECURSIVE queries
- Workplace post: [*Velox and Axiom for graph queries: a primer*](https://fb.workplace.com/groups/1588734491493342/permalink/2823843241315788/) — motivates the
  CQL roadmap and introduces recursive CTEs, variable-length patterns, and
  fixpoint operators.
