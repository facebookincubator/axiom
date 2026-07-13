# Distributed (Cross-Fragment) Dynamic Filters

*Design for optimizer-assisted dynamic filters that cross exchange boundaries.
Companion to [DistributedPlanning.md](DistributedPlanning.md) and the runtime
contract in [../../docs/DistributedExecution.md](../../docs/DistributedExecution.md).
The plan of record forward-referenced by
[TpchV1V2PlanComparison.md](TpchV1V2PlanComparison.md)'s "Dynamic-filter
dependence" section.*

Provenance tags per [HowWeWork.md](HowWeWork.md): **[verified: file:line]**,
**[inferred]**, **[unverified]**. Anything unmarked is verified-grade.

## 1. Motivation and scope

v2 emits multi-fragment plans, and several v2 plans match v1's reductions by
relying on Velox's runtime dynamic filter — a hash-join build pushes a filter
into the probe-side `TableScan`. That filter does **not** cross an exchange, so
once the probe is partitioned (shuffle join) the reduction is lost. Measured at
4 workers **[verified: TpchV1V2PlanComparison.md §Multi-node]**:

- **q17** — subquery `lineitem` scan runs the full 60M rows vs. the single-node
  60M→61k reduction (sf10 **+286%**, sf1 +31%).
- **q9_alt** — sf10 **+22%**.

These are the acceptance targets: distributed DF must restore those reductions.

**Scope — cross-fragment only.** The distinguishing factor is whether the target
scan is reachable from the join **without crossing an exchange** — *not* whether
the join is broadcast or partitioned. When the scan is in the same fragment as
the join (all single-node plans, and broadcast/co-located joins with no upstream
shuffle on the probe side), Velox's adaptive runtime pushdown already handles it,
unchanged; optimizer assistance is heavy and not justified there. This design
covers **only** the case Velox's runtime walk cannot reach: the target scan is
separated from the join by ≥1 exchange. That arises in two ways:
- **Partitioned join** — the probe is shuffled on the key, so the pre-shuffle
  probe scan sits in an upstream fragment.
- **Broadcast join with an upstream shuffle** — e.g. `scan → agg on k → join on
  k` where the aggregation forces a shuffle on `k` but the join then broadcasts
  its build. The join is co-located with the agg's output, yet the build's filter
  must still travel back across the *agg's* shuffle to reach the scan.

So the trigger is the exchange between join and scan, wherever it comes from; the
join's own distribution is irrelevant to *whether* a DF is cross-fragment (it
only affects producer completeness — §5.2).

This design assumes a **streaming-shuffle** runtime (e.g. Prestissimo): the producer
reads the filter from a *live* hash-join build. Durable, Spark-like shuffle needs a
producer-side extension — consumer seam unchanged — sketched in §11.

## 2. The topology (why this needs the Runner)

Take a hash join on key `k`, build side `B`, probe side `P`, with an exchange
somewhere between the join and the scan of `P`:

- **PROBE-SRC fragment** — the fragment containing `TableScan(P)`, upstream of
  the exchange. The scan reads rows *before* they are partitioned by `k`, so it
  sees all key values and needs the **global** build filter (the union over all
  build keys). This scan is the DF **consumer**.
- **JOIN fragment** — `HashProbe ⟵ [probe input]`, build side materialized. The
  build hash table completes here, and the local filter is available from it
  (`VectorHasher::getFilter` / `getBloomFilter`) **[verified: HashProbe.cpp
  383-432; VectorHasher.cpp 689-728]**. The DF **producer** is the join operator
  across all JOIN tasks. How much of the global filter each JOIN task holds
  depends on the build's distribution:
  - **partitioned build** — each task holds one hash partition of the build keys;
    the global filter is the **union across all JOIN tasks**.
  - **broadcast build** — each task holds the *full* build, so each task's filter
    is already the global filter (all identical); the union is idempotent.

The filter must travel **JOIN tasks → PROBE-SRC tasks**: *backwards* against
dataflow (PROBE-SRC feeds JOIN, not the reverse) and *fan-in/fan-out* (union
across all JOIN tasks, then broadcast to all PROBE-SRC tasks). No exchange
provides that path — exchanges are forward, point-to-point data edges. The path
exists only at the component that sees all tasks at once: the **Runner**
**[verified: LocalRunner::makeStages LocalRunner.cpp:641-778]**. This is the
core reason cross-fragment DF is a Runner concern and intra-fragment DF is not.

**The DF is not free.** The two pipelines feeding the join — the probe scan and
the build — normally run concurrently. A cross-fragment DF makes the probe scan
depend on the build finishing (that is when the filter exists), so making the
scan *wait* for it removes the probe/build overlap: the scan does no work until
the build completes. This is the key difference from the local case — there the
probe join itself blocks on the build, so the wait is free; here the consumer is
the *upstream scan*, which otherwise overlaps the build. In wall-clock terms the DF
pays off only when the reduction in probe scan + shuffle exceeds this lost overlap;
a weakly-selective filter that we blocked on is a pure latency loss. But the
marking gate (§4.1) cannot price the overlap — it works in `Cost.cost`, additive
total work with no latency dimension, and the acceptance metric (total CPU) doesn't
see blocking either. So the gate is a **selectivity check** (§4.1), and the overlap
stays a latency *caveat*: it is small when the build is the selective side (the
usual case for a reducing DF) and large exactly when the filter is least likely to
help — a correlation the gate relies on implicitly rather than a term it weighs.

## 3. Architecture

Two responsibilities, cleanly split — mirroring how Axiom already divides the
optimizer (decides placement; emit then mechanically lowers it) from the runtime
(orchestrates distributed execution — creates tasks, distributes splits, wires
exchanges, drives the fragment DAG):

1. **Optimizer statically places the DF.** At plan time it decides, per
   candidate DF: the producer node (the join), the consumer node (a specific
   `TableScan`), the key-column mapping, and the bloom sizing. It resolves the
   consumer by following **filter-pushdown legality** from the probe side of the
   join down through the plan — through projections/renames, through aggregations
   on grouping keys, and across the probe-side exchange into the PROBE-SRC
   fragment — stopping where the key column no longer survives (normally the
   scan). The runtime does **not** rediscover this by walking
   `identityProjections()` at execution time; that walk (`Driver::pushdownFilters`
   **[verified: Driver.cpp:1171-1238]**) remains the intra-fragment mechanism
   only.

2. **Velox exposes three per-task DF calls; the Runner mediates.** No task talks
   to another. A *producer* (Join) task publishes its own filter; the Runner awaits
   it; the Runner delivers the aggregate into a *consumer* (Scan) task, which was
   blocked waiting. The three calls — and their analogs to existing `Task` APIs —
   are defined normatively in §8.2.

3. **The Runner aggregates.** `LocalRunner` (separate from Velox) launched the
   tasks and holds the plan, so it knows every producer and consumer task for a
   `dfId`. It calls `dynamicFilterFuture` on all producer tasks and combines with
   `collectAll` (partitioned → OR-union) or `collectAny` (broadcast → first),
   disabling on any opt-out; then it calls `addDynamicFilter` on every consumer
   scan task. A future distributed `Runner` (Axel) makes the two calls async RPCs;
   `common::Filter` is `ISerializable` **[verified: Filter.h:68]**, so only the
   transport differs.

The DF is control, not data — it never touches output buffers or the data path; it
flows **Task → Runner → Task**.

### 3.1 End-to-end

One cross-fragment DF, plan to execution (partitioned build; broadcast differs only
at step 5):

**Plan (`PlanPhysical`)**
1. At a hash join, gate on estimated selectivity (§4.1) — a non-selective join gets
   no DF. Otherwise mark a candidate DF per join key (no composite-tuple DF, §7).
2. Walk from the join's probe side to the target scan(s) (§4.2); emit only if the
   path crosses ≥1 exchange (else Velox's adaptive path handles it). Fix the shared
   filter kind/size from the total build estimate (§6).

**Emit**
3. Record the DF on the `HashJoinNode` (produced: dfId, build-key channel,
   kind/size), the `TableScanNode` (consumed: dfId, output channel), and the
   `MultiFragmentPlan` (producer/consumer fragments, distribution) — §8.1, §5.1.

**Run (`LocalRunner`)**
4. Each JOIN task, on build completion, publishes its own filter (or opt-out) via
   `publishDynamicFilter` (§5.3).
5. The Runner awaits producers via `dynamicFilterFuture` and aggregates —
   `collectAll` → union (partitioned) / `collectAny` → first (broadcast); any
   opt-out → disable (§5.2).
6. The Runner delivers the one aggregate to **every** consumer scan task via
   `addDynamicFilter`.
7. Each probe scan, parked on `kWaitForDynamicFilter`, applies it via the
   connector's `DataSource::addDynamicFilter` and scans reduced — or proceeds
   unfiltered on disable (§5.4).

The probe scan does no work until the DF arrives (build complete + aggregate); that
wait's cost is bounded only by the step-1 gate (§2).

## 4. Optimizer

### 4.1 Candidate marking

Candidate marking is a **cost decision in the cost model's own currency**
(`Cost.cost`, additive total work), not "the build is small." The *benefit* is the
probe-side reduction — `(1 − selectivity) × probe(scan + shuffle) cost`, where
`selectivity` is the join's match fraction, the cost model's `innerEdgeSelectivity
= 1/max(jointNdv)` **[verified: CostModel.cpp:80,108]**. The *cost* is the bloom
build + transfer. Mark a DF when the benefit exceeds the cost — which, because a
selective join makes `(1 − selectivity)·probe` dwarf the bloom cost, is
**effectively a selectivity check**: a non-selective join (most probe keys present
in the build → `selectivity ≈ 1`) gets no benefit and must not be marked,
regardless of how small the build is. Build size feeds bloom sizing and the cost
side, not the benefit.

**The lost probe/build overlap (§2) is not a term in this gate.** It is wall-clock
latency — a blocked scan does the same or less *total* work — and `Cost` has no
parallelism/latency dimension to represent it; the acceptance metric (total CPU)
doesn't see it either. It stays a latency caveat on blocking (§2, §5.4), not part
of the marking decision.

At any hash join whose placement walk (§4.2) crosses an exchange — broadcast or
partitioned — and that clears the cost gate above, record a candidate DF **per join
key** from the physical plan (`Join` carries `leftKeys`/`rightKeys` **[verified:
RelationOp.h:536-609]**); v1 builds no composite-tuple DF (§7):
- per key `i`: the build key (`rightKeys[i]`), the probe key (`leftKeys[i]`), a DF id;
- the **build distribution** (broadcast vs partitioned), which sets the
  aggregator's completeness condition (§5.2);
- **bloom size derived from the estimated total build cardinality** so every
  JOIN task's bloom has identical `m`/`k` and the OR-union is exact (§6). Sizing
  per-partition would make the OR'd filter over-full and blow up its FPP.

Marking is a *candidate*: at runtime each build partition publishes its filter (or
∅ when empty), and opts out — disabling the DF — only when it is unanalyzable
(§5.2–5.3), protecting against bad stats turning the DF into a pessimization.

### 4.2 Static placement (the pushdown-legality walk)

From the probe input of the marked join, walk **down** the plan toward the leaves
following the same rules any filter on the key columns would follow:
- **Project / rename** — remap the key columns through the projection; stop the
  branch if a key column is not produced.
- **Aggregation** — pass through when every key column is a grouping key
  (identity-projected); stop otherwise. This is the existing "DF crosses an
  aggregation on its grouping key" behavior, now decided at plan time rather than
  emergent **[verified: TpchV1V2PlanComparison.md note; HashAggregation.cpp
  grouping-key identity projections]**.
- **Exchange** — any exchange on the path (the join's own probe shuffle, or an
  upstream one such as an aggregation's, per §1) — cross into the upstream
  fragment. Crossing ≥1 exchange is what makes the DF cross-fragment.
- **TableScan** — terminus. Record consumer node id + the output-channel mapping
  for the key columns.

The decision lives in **`PlanPhysical`** (`PhysicalPlanRewriter`), which has all
three inputs it needs: join selectivity and build-size estimates (the memo / cost
model) and the placed `ir.Exchange` nodes (so the walk can tell whether the path
from join to scan crosses one) **[verified: DistributedPlanning.md §10–11 — exchange
placement and rewrites are in `PlanPhysical.cpp`'s `PhysicalPlanRewriter`]**. It
decides at IR-node granularity: producer node (the join), consumer node (the scan),
key-column mapping, build distribution, selectivity gate. Emit is then mechanical —
it translates those chosen IR nodes to their emitted Velox `PlanNodeId`s + fragment
`taskPrefix` and records the descriptors on `MultiFragmentPlan`, making no
decisions.

### 4.3 Cross-fragment gate

Emit DF metadata **only when the walk crosses an exchange boundary** (producer
and consumer fragments differ). If the target scan lands in the same fragment as
the join, emit nothing — Velox's adaptive pushdown already handles it. This is
the scope boundary of §1 enforced structurally.

## 5. Runtime (Runner)

### 5.1 Plan metadata

A new list of cross-fragment DF descriptors on `MultiFragmentPlan` (analogous to
`InputStage`, but a control edge rather than a data edge) **[inferred; new
field]**:
- DF id;
- producer node id + producer fragment `taskPrefix`;
- consumer node id + consumer fragment `taskPrefix`;
- build-key → probe-scan output-channel mapping;
- build distribution (broadcast vs partitioned), for the completeness condition;
- filter kind + bloom sizing.

### 5.2 Runner aggregation

The Runner knows the producer and consumer task sets for each `dfId` (it launched
them, from the plan) and the build distribution. Velox does no aggregation — it
only exposes the per-task calls (§8).

- **Per-task publish** — each JOIN task reports one of three things for its
  partition (§5.3): its **filter**; an **empty filter (∅, always-false)** when the
  partition holds no build keys — the OR-identity, so it drops out of the union; or
  an **opt-out (`nullptr`)** only when the partition is unanalyzable
  (`hashMode == kHash`). The deviation from local behavior is sizing: the filter
  *kind* and bloom size (`m`,`k`) are fixed up front from the total estimate (§4.1,
  §6) so all partitions are OR-compatible — Velox's local `getBloomFilter` sizes
  per-partition, which is not.
- **Await + aggregate** — the Runner calls `dynamicFilterFuture(dfId)` on every
  producer task and combines by build distribution:
  - **Partitioned build** — `collectAll` the producer futures, then OR-union via
    `Filter::merge` **[verified: Filter.cpp merge impls]** (bitwise OR for blooms;
    min-of-mins/max-of-maxes for ranges; set-union to a cap). Empty (∅) partitions
    are the OR-identity and vanish; the non-empty ones form the global filter. A
    partial union would drop matching probe rows, so all producers are required.
  - **Broadcast build** — every task holds the full build, so its filter is already
    global: `collectAny` — act on the first, don't wait for the rest.
  - **Opt-out → disable** — any producer opt-out (`nullptr`, i.e. an unanalyzable
    `hashMode == kHash` partition) disables the DF (scans proceed unfiltered).
    Build-task *failure* is separate — a query failure (§7), not an opt-out.
- **Deliver** — call `addDynamicFilter(dfId, filter | disable)` on each consumer
  scan task, completing the wait its `TableScan` is parked on.

The transport is the three `Task` calls of §8.2. `LocalRunner` drives them from the
tasks it holds **[verified: LocalRunner.cpp:641-778]**.

### 5.3 Apply-or-drop

Two-valued signal: **apply** the global filter, or **disable** (all scans proceed
unfiltered), decided **at the build** — not by measuring selectivity at the
aggregator (which has no probe-side count to measure against). Each partition maps
to one of three per-task reports (§5.2):

- **analyzable, non-empty** → its filter, built from the keys via
  `getFilter`/`getBloomFilter` at the shared kind/size.
- **analyzable, empty** → the **empty filter (∅, always-false)**, the OR-identity
  — it vanishes from the union. Reuse the local criterion's *analyzability*, but
  **not** its `numDistinct() != 0` guard: locally an empty build means "don't
  bother", but in the union a single empty partition of a non-empty build must
  still contribute ∅, or it would over-disable a good DF. (An empty build's
  `getFilter` already yields the empty value-set = ∅; the non-empty guard is what's
  in the way.) Whole-build-empty is separate — the inner join short-circuits on its
  own **[verified: HashProbe.cpp:471-483]**.
- **unanalyzable** (`hashMode == kHash`, no filter constructible **[verified:
  HashProbe.cpp:484-504; VectorHasher.cpp:689-728]**) → **opt-out (`nullptr`)**,
  which **disables** the whole DF: a union missing a partition's keys would wrongly
  drop matching probe rows, so a partition that genuinely can't describe its keys
  must fail the whole DF, not silently narrow it.

There is no new drop threshold to tune; the only genuinely new choice is the shared
filter kind/size, which the optimizer fixes from the total estimate so the OR is
well-defined (§5.2, §6).

### 5.4 Consumer (probe scan)

The PROBE-SRC `TableScan` **blocks** for the DF via a new `BlockingReason`
`kWaitForDynamicFilter` + a future — the exact `Task::getSplitOrFuture`
park-the-driver pattern **[verified: Task.h:482; TableScan.cpp:318-330;
BlockingReason.h]**. The Runner completes it by delivering the aggregate into the
task with `Task::addDynamicFilter(dfId, filter)` (§8), the `addSplit` analog. On
resolution: filter → apply via the connector's `DataSource::addDynamicFilter`
**[verified: TableScan.cpp:36-67, 522-531; Connector.h:284-287]**; disable → scan
unfiltered.

Blocking is the **default**, not a tunable mode, because non-blocking late-apply —
start scanning immediately and filter only the not-yet-opened splits when the DF
arrives — was tried for the intra-fragment DF and **defeated the purpose**: the
scan outruns the DF's arrival, so too much of the table is already read and the
captured reduction is small **[per user; intra-DF experience]**. The cross-fragment
DF is no earlier (it too is unavailable until the build completes), so late-apply
is no better here.

The lost probe/build overlap (§2) is a latency caveat, not a priced term: the
marking gate (§4.1) is a total-work selectivity check and can't represent
wall-clock overlap, and total CPU doesn't see blocking. A DF is placed only when it
is *estimated* selective (§4.1); at runtime it is dropped only by the build-side
opt-out (§5.3) — a build that can't produce a usable filter — never by a measured
reduction check (the aggregator has no probe-side count to measure against). There
is **no timeout**: the only way the DF never arrives is that the
build never completed, which hangs the query regardless of the DF, so a timeout
would rescue nothing (§7).

## 6. Aggregating filters

Bloom merge is bitwise OR of the bit arrays; the result is the bloom of the union
with the normal FPP for the combined element count — exact, no extra error —
**provided every input has identical size (`m`) and hash functions (`k`, seeds)**,
including block count/seed for `SplitBlockBloomFilter`
**[verified: Filter.h:1293-1364; SplitBlockBloomFilter.h]**. This is why §4.1
sizes for the total union cardinality up front. Ranges union by min/max; discrete
value sets union up to a cap, degrading to range/bloom past it.

## 7. What we are NOT building (and why)

- **No timeout on the consumer wait.** The only way the DF never arrives is that
  the build never completed — which hangs the query regardless of the DF (the join
  can't produce output either). There is no independent DF-delivery failure mode a
  timeout could rescue, so the blocking wait has none (§5.4).
- **No "build failed → proceed unfiltered" branch.** A build-task failure is a
  query failure (no partial-fragment recovery), so the aggregator never emits a
  partial DF. The signal is two-valued: apply or drop.
- **No composite (key-tuple) DF — per-key only, for all workloads.** A multi-key
  join emits one per-column DF per key, applied conjunctively at the scan; v1 builds
  no single filter over the tuple `(k1, k2, …)`. This mirrors Velox's intra-fragment
  DF (per-column filters, no tuple filter) and reuses the existing
  `DataSource::addDynamicFilter` per-column path — no new filter type, no new
  connector contract. The tuple filter is strictly more precise — per-key
  conjunction admits false pairs (`k1 ∈ build-k1` and `k2 ∈ build-k2` with
  `(k1,k2) ∉ build` passes) — so it is a deferred *precision* improvement, not a
  correctness gap. Multi-key joins are fully supported in v1 via the per-key form.
- **No Axel / distributed transport now.** LocalRunner-first. The seam stays
  serialization-ready (`Filter` is `ISerializable`) so Axel can implement it
  later without redesign.
- **No intra-fragment change.** Velox's adaptive pushdown is untouched.

## 8. Velox changes: plan nodes + three Task calls

Two Velox additions: DF ids on the plan nodes so each operator knows its role, and
three per-task `Task` methods for the Task↔Runner hops — **no aggregation in Velox**
(that is the Runner, §5.2), and no task-to-task communication.

### 8.1 Plan-node DF descriptors

Emit records the placement (§4.2) **on the Velox plan nodes**, so each operator
learns its `dfId` and role from its own node — no separate lookup:
- **`HashJoinNode`** (producer) gains a **list** of the DFs it produces — **one DF
  per join key** (each with `dfId`, its build key channel, and the shared filter
  kind/size, §5.2), so an N-key join yields N per-key DFs applied conjunctively at
  the scan. v1 does **not** build a single *composite* filter over the key tuple
  (deferred — §7).
- **`TableScanNode`** (consumer) gains a **list** of the DF(s) it consumes, each
  with `dfId` and the output channel it applies to. The list is genuinely
  multi-valued — **one scan can receive filters from several joins**, on the same
  column or different ones. The scan adds each to its `DataSource` as it arrives;
  applying them correctly (conjunctively, including several on one channel) is the
  connector's contract, not this design's concern.

These are new fields on the Velox nodes, set by Axiom's emit. (The
`MultiFragmentPlan` DF descriptor of §5.1 carries the cross-task wiring —
producer/consumer fragment `taskPrefix`, build distribution — that the *Runner*
needs; the node fields carry what the *operator* needs. The Runner learns the
producer/consumer task sets by launching them, so nothing DF-related is registered
on the task at init.)

### 8.2 Three Task calls

The Task↔Runner hops are three new `Task` methods, each modeled on an existing
`Task` API. Velox never aggregates and no task calls another — every DF goes
Task→Runner→Task (§3). **This section is the normative definition of the three
calls; §3 and §5.2 refer here.**

Two writers (one per role) and one reader. `dynamicFilterFuture` is the **read**
side on *any* task; the write differs by whether the task produces or consumes the
`dfId`:

```cpp
// Write, producer task: the join publishes its own per-task filter (nullptr =
// opt-out, §5.2) when its build completes.
void Task::publishDynamicFilter(const std::string& dfId, common::FilterPtr filter);

// Write, consumer task: the Runner delivers the aggregated result (the addSplit
// analog **[verified: Task.h:262]**). filter == nullptr = disabled.
void Task::addDynamicFilter(const std::string& dfId, common::FilterPtr filter);

// Read (both roles): a value-carrying future for 'dfId' on this task, modeled on
// taskCompletionFuture()/stateChangeFuture(). Immediately ready if already set;
// no timeout (if it never fires the build never finished, which hangs the query
// regardless, §7).
folly::SemiFuture<common::FilterPtr> Task::dynamicFilterFuture(const std::string& dfId);
```

- **Producer task:** the join calls `publishDynamicFilter`; the **Runner** calls
  `dynamicFilterFuture` to collect that task's filter.
- **Consumer task:** the **Runner** calls `addDynamicFilter` with the aggregate; the
  **Scan** calls `dynamicFilterFuture` — that is the future it parks on with
  `kWaitForDynamicFilter` (the `getSplitOrFuture` pattern **[verified: Task.h:482]**).

A task is producer *or* consumer for a given `dfId`, never both, so per `(task,
dfId)` it is always one writer plus `dynamicFilterFuture` as the reader. The promise
is **created on demand** (whichever call comes first), and operators learn their
`dfId`s from the plan nodes (§8.1) — so no task-level pre-registration is needed.

### 8.3 Operator responsibilities

- **Producer (`HashProbe`)** — build complete and its `HashJoinNode` carries a
  producer DF descriptor: build the filter (`getFilter`/`getBloomFilter`) at the
  shared kind/size (∅ if the partition is empty), or opt out only if unanalyzable
  (§5.3), then `task->publishDynamicFilter(dfId, f)`. One call per JOIN task per
  `dfId`.
- **Consumer (`TableScan`)** — its `TableScanNode` carries a list of consumer DF
  descriptors (§8.1). It blocks (`kWaitForDynamicFilter`) until the Runner has
  delivered each `dfId` via `addDynamicFilter`, then scans by the conjunction
  (§5.4). Each delivered filter → `dataSource_->addDynamicFilter(channel, filter)`
  (existing replay path) **[verified: TableScan.cpp:36-67, 522-531]**; disabled →
  that DF drops out.

### 8.4 Distributed (Axel) and testing

**Axel** keeps the same operator-facing API and makes `dynamicFilterFuture` /
`addDynamicFilter` async RPCs between task and coordinator: producers publish
locally, the coordinator awaits (long-poll) and delivers — `common::Filter` is
`ISerializable` **[verified: Filter.h:68]** (Axel internals **[unverified]**). No
new filter types. The Velox half (the three `Task` calls + the scan wait) is
unit-testable with a stub Runner ahead of the Axiom wiring.

## 9. Build order

1. **Velox**: plan-node DF fields (§8.1); the three `Task` calls —
   `publishDynamicFilter`, `dynamicFilterFuture`, `addDynamicFilter` — + `TableScan`
   `kWaitForDynamicFilter` wait (apply via replay path). Unit-tested with a stub
   Runner.
2. **Optimizer**: candidate marking at cross-fragment joins (partitioned, or
   broadcast with an upstream shuffle) + static placement walk + cross-fragment
   gate + emit DF descriptors on `MultiFragmentPlan` and the plan nodes.
3. **LocalRunner**: `dynamicFilterFuture` on producer tasks, `collectAll`/`collectAny`
   → aggregate (union / first), opt-out → disable, `addDynamicFilter` on consumer
   scans (§5.2).
4. **Validation**: q17 and q9_alt at 4 workers, sf1/sf10 — confirm the reduction
   returns (q17 `lineitem` 60M→~61k), results unchanged, total-CPU regression
   closed.

## 10. Test plan

Four layers: the Velox seam + Runner aggregation in isolation (unit), structural
shape coverage (plan *and* results), end-to-end correctness, and perf — the last
three through `LocalRunner`.

### 10.1 Velox seam + Runner aggregation (unit)

Two standalone contracts (the [HowWeWork.md](HowWeWork.md) exception for standalone
algorithms).

**Velox `Task` calls** — with a stub Runner and a small `HashProbe`→`TableScan`
pipeline:
- **publish → future** — `publishDynamicFilter` fulfills the `dynamicFilterFuture`,
  in both orders (read-before-publish and publish-before-read, exercising the
  on-demand promise).
- **deliver → apply** — `Task::addDynamicFilter` completes the scan's
  `kWaitForDynamicFilter` wait; the scan applies via `DataSource::addDynamicFilter`
  (filter) or proceeds (disable / `nullptr`).
- **Multiple DFs per scan** — several DFs on the same column (intersection) and on
  different columns applied conjunctively; one disabled DF drops out while others
  still apply.

**Runner aggregation** — with stub producer tasks:
- **OR-union (partitioned)** — `collectAll` + `Filter::merge`; assert the OR of
  per-partition blooms equals the bloom of the union, and exact value-set/range
  unions.
- **First (broadcast)** — `collectAny` resolves on the first producer without
  waiting for the rest.
- **Empty partition → ∅ drops out** — a partition publishing the empty
  (always-false) filter vanishes from the union; the DF still applies from the
  non-empty partitions (must **not** disable).
- **Opt-out → disable** — a producer future carrying `nullptr` (an unanalyzable
  partition) disables the DF; consumers get the disable signal and proceed
  unfiltered.
- **Blocking/wakeup + teardown** — the scan's `isBlocked` future is completed on
  delivery; `removeTask` of the last participant drops the DF state (no leak).

### 10.2 Structural shape coverage

TPC-H is a **validation** set, not the coverage driver — its 22 queries exercise
only some shapes incidentally. Cover the structural space with **purpose-built**
queries/plans (small SQL, injected stats to force broadcast vs partitioned), each
checked at **both** the plan level (`PlanMatcher` — is the DF placed as expected)
and the result level (DF on == DF off == single-node). The axes, varied
independently:

- **Build distribution** — **partitioned** build (Runner `collectAll` → union) and
  **broadcast** build (Runner `collectAny` → first). Both must place a DF and both
  must be correct; this is the only axis that exercises the two aggregation paths.
- **Probe under the join** — join directly over a shuffled **scan** (DF target =
  scan, one exchange) vs join over an **aggregation** (DF pushes through the
  grouping key to the scan beneath, §4.2).
- **Distance scan→join** — **one** exchange vs **multiple** exchanges (e.g.
  `scan → agg(shuffle) → … → (shuffle) → join`), exercising the placement walk
  across several hops and Runner routing across them.
- **Keys** — single-key (one DF) and **multi-key** (one DF per key, applied
  conjunctively at the scan; no composite-tuple filter, §7/§8.1).
- **Multiplicity** — one scan consuming DFs from **multiple joins**, same column
  (intersect) and different columns.
- **Negative (no DF)** — non-selective join (gate, §4.1); co-located/broadcast join
  with no upstream shuffle (intra-fragment, untouched, §4.3).

`TpchPlanTest` then locks the realistic cases on top (q17/q9_alt: DF placed
producer-join ↔ consumer-scan across the exchange, q17 through the subquery
aggregation's grouping key).

### 10.3 End-to-end correctness (`LocalRunner`)

The §10.2 shapes are run for results (DF on == off == single-node), plus
`tpch_result` at `numWorkers > 1` **and** `numDrivers > 1` as the realistic set —
including a DF disabled by a producer opt-out (a build that can't produce a usable
filter, e.g. too many distinct keys). This is the guard that a wrong/partial union
never drops matching rows.

### 10.4 Perf (acceptance / regression gate)

The §9 step-4 validation measurement, by the
[TpchV1V2PlanComparison.md](TpchV1V2PlanComparison.md) method (`measure_baseline.py`,
median total CPU, noise band), at **4 workers, sf1 + sf10**:
- **Reductions restored** — q17 subquery `lineitem` returns to ~61k (from the full
  60M) and its +286% sf10 gap closes; q9_alt's +22% closes.
- **No new regression** — queries with no cross-fragment DF are unchanged; no query
  that gains a DF regresses (the selectivity gate + opt-out must hold in practice,
  not just in theory). Re-baseline `baseline_4w.md` for the queries that move.

## 11. Durable (Spark-like) shuffle — limitation and extension

Everything above assumes a **streaming-shuffle** runtime (e.g. Prestissimo): the join stage
runs concurrently with the fragments feeding it, so the build hash table is live
while the query runs and `publishDynamicFilter` (§8.2) reads the filter from it
mid-flight. This section records where that assumption breaks under **durable,
Spark-like shuffle** and sketches the extension — a direction, not a full design.

### 11.1 The gap

Only the *producer* is streaming-specific. Durable shuffle stages are **barriers**:
each stage's output is materialized to durable storage, and a consuming stage starts
only after its inputs are complete. So the build and probe sides both materialize
*before* the join stage runs — and when the probe-side scan runs, there is **no live
join** to collect a filter from. The consumer seam (§5.4, §8) does not assume
concurrency; join-side collection does.

### 11.2 Extension: emit the filter from the build shuffle-write

The consumer is unchanged; only the producer moves. Instead of collecting from a
live `HashBuild`, the **build-side shuffle-write fragment emits the filter as
side-output metadata alongside the durable shuffle data** — the way `TableWrite`
emits per-column stats alongside the rows it writes. Precedent: a `TableWriteNode`
carries an optional `columnStatsSpec`, and when present `TableWriter` builds a
`ColumnStatsCollector` that runs partial/single aggregation over the written rows
and surfaces the result as extra output **[verified: TableWriter.cpp:52-65;
`TableWriteNode::columnStatsSpec`, PlanNode.h]**.

By analogy: the shuffle-write carries an optional **DF spec** (`dfId`, key channels,
shared bloom size) and runs a bloom aggregation over the keys as it writes, emitting
the filter as metadata — one operator's responsibility, reusing aggregation
machinery internally, just as `TableWrite` owns its stats. The Runtime collects it
at build-stage completion (as it already collects `TableWrite` stats), OR-aggregates
across write tasks (§5.2), and delivers it to the probe-scan stage. The consumer
seam (§5.4, §8) is unchanged and is agnostic to how the runtime drives tasks: the
scan blocks on `dynamicFilterFuture(dfId)` and applies the filter when it resolves.
Under streaming (async `Task::start`) the probe stage starts concurrently with the
build — doing split prefetch and other prep — and parks on `kWaitForDynamicFilter`
until the Runner delivers. Under durable, synchronous execution (`Task::next`, as
Gluten runs today) the Runner delivers the filter before running the probe-scan
stage, so the future is already resolved and the scan never parks. The design
requires neither — the same seam serves both. The §4 cost gate still applies:
ordering the build stage before the probe-scan stage removes their overlap, the same
latency cost as the streaming wait, now paid as stage ordering.

So the seam extends along the `TableWrite`-stats precedent: **no new consumer path,
no new aggregation mechanism, no new filter type** — only the producer relocates
from the live join to the shuffle-write.

Note this is also the **efficient** path for Spark/Gluten: fuse the filter into the
build shuffle-write in one pass, rather than running a separate `bloom_filter_agg`
query over the build ([current Spark/Gluten behavior](https://github.com/facebookincubator/velox/pull/18064#issuecomment-4936000896)).
