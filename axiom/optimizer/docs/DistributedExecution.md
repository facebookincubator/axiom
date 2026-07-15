# Distributed Execution Planning in Axiom's Optimizer

## Overview

This document describes how Axiom's optimizer plans distributed execution: how plans are split into fragments, how fragments are scheduled and wired at runtime, and how grouped (bucketed) execution is modeled.

The optimizer's role is to decide where exchanges go, what partitioning each exchange uses, and how many tasks each fragment should run with. The runtime takes the resulting `MultiFragmentPlan` and: creates tasks, enumerates and distributes splits to them, and wires producer/consumer exchanges between them.

**Implementation status.** Sections 1 and 2 describe the fragment and exchange model that is in place today, including `kCoordinator` fragments and SystemConnector isolation. Grouped execution is the one exception:

- **Grouped execution** (Section 3) — including the `PartitionType` API, `groupedNodes` / `groupedExecution` fields on `ExecutableFragment`, `GroupedSplitSource`, and split-level `groupId` tagging — is a design proposal, not yet implemented.

### 1. Fragment Structure and Scheduling Constraints

A **fragment** is a portion of a plan delimited by exchange boundaries. One
or more instances of a fragment execute in parallel as **tasks** — each
task runs the same operations on a different portion of the input data.

Fragments differ by where their input data comes from and how their output
data is partitioned. The source of input constrains how many tasks can run
and where. The output partitioning constrains how many downstream tasks are
needed to consume the output.

#### Why exchanges are needed

In single-node execution, the whole plan runs in one fragment. In
distributed execution, the plan must be split into fragments that can run
independently on different nodes. Exchanges are the boundaries where
fragments connect — they define how data moves between fragments.

> **Note.** Single-node multi-threaded execution faces the same concerns at a different scale: data still needs to be repartitioned, gathered, or broadcast — just between threads within one task instead of between tasks on different nodes. Velox handles this through `LocalPartitionNode`. Conceptually, local and remote exchanges are the same primitive at different scopes, but this document focuses on the cross-fragment case. A unified optimizer model that treats both uniformly is future work — it is not a straightforward reuse: local and remote exchanges have very different cost profiles (local is in-process memory copy; remote adds serialization, network transfer, and buffering), and mixed plans (parallelism within a fragment plus parallelism across fragments) require reasoning about both at once.

Exchanges are needed for:

1. **Data repartitioning** — operators like joins, aggregations, and window
   functions need data organized by specific keys. A hash join needs all
   rows with the same join key on the same task. A grouped aggregation
   needs all rows with the same grouping key. A window function needs all
   rows with the same PARTITION BY keys. When the data isn't already
   partitioned that way, an exchange repartitions it.

2. **Gathering** — some operators need all data on a single task: ORDER BY
   to produce a globally sorted result, LIMIT/OFFSET to select the right
   rows, global aggregation (no grouping keys) to produce a single result.
   The final query output also needs to be collected on a single node to
   return to the client.

3. **Broadcasting** — when one side of a join is small enough to fit in
   memory on every task, it's more efficient to broadcast (replicate) it
   to all tasks rather than repartitioning both sides. The probe side
   stays wherever it is (no shuffle), and each task gets a full copy of
   the build side.

   > **Note.** The current optimizer chooses broadcast *structurally*, not by
   > cost: a build is broadcast whenever it is eligible — its estimated size
   > (rows × row width) is at most `broadcast_size_limit` (default 100MB) — and
   > hash-partitioned otherwise. The broadcast cost (replication × number of
   > workers) therefore feeds join-order ranking, not the broadcast-vs-partition
   > choice itself. Making that choice cost-based is future work.

4. **Combining independently computed inputs (UNION ALL)** — each UNION
   ALL input may involve its own joins, aggregations, or other operations
   that produce separate fragments. Exchanges connect these fragments to
   the consumer that concatenates them. The exchange type depends on what
   the consumer needs — hash-partitioned if a downstream operator requires
   it (e.g., GROUP BY), arbitrary otherwise. When some inputs require a
   single task (Values, global aggregation), they are placed in a separate
   fragment so they don't constrain the parallelism of other inputs.

5. **Distributing writes** — parallel writing distributes rows across
   writer tasks. Arbitrary exchange spreads the work without requiring
   any particular partitioning. Bucketed writes use hash-partitioned
   exchange to ensure rows land in the correct bucket.

#### Data sources

A fragment's input data comes from one or more of these sources:

1. **Connector (table scan)** — data read from a data source via connector splits.
2. **Exchange** — data received from an upstream fragment via remote splits (each remote split identifies a producer task and an output buffer to read from).
3. **Values** — constants embedded in the plan.

Each data source, considered independently, imposes constraints on how many
tasks can run the fragment and where.

**Connector (scan):** Parallelism is data-driven. The runtime enumerates
splits via connector APIs, creates tasks, and assigns splits to tasks. A
single task can process multiple splits, potentially on multiple threads
(drivers). The task count can grow as more splits are discovered — the
runtime doesn't need to wait for all splits before starting execution.
The optimizer doesn't know the final task count at plan time, but may suggest a starting count based on cardinality estimates.

**SystemConnector:** Parallelism is 1, must run on the coordinator. This
connector provides access to in-process runtime state (active queries,
session properties, function metadata) via provider interfaces registered
at startup. Unlike other connectors, it doesn't follow the split-based
scheduling model. See [axiom/connectors/system/README.md](../../connectors/system/README.md).

**Exchange (hash-partitioned):** The producer partitions rows into exactly
N buckets by key. The consumer typically has N tasks (one per bucket), though the runtime may downscale and assign multiple buckets to the same task (e.g., N/2 tasks each handling 2 buckets).

**Exchange (arbitrary):** The producer places rows into a shared pool.
Consumer tasks pull data on demand. Any number of consumer tasks works.

**Exchange (gather):** The producer sends all data to a single partition.
Exactly 1 consumer task receives data.

**Exchange (broadcast):** The producer sends all data to every consumer
task. Any consumer task count works.

**Values:** Typically runs as a single task. Each task independently
produces the full constant set, so running N tasks duplicates the data.
The exception is broadcast joins — when Values is the build side, running
it on every task is more efficient than producing on one task and
broadcasting (not implemented today — Values is currently always isolated into its own kSingle fragment).

#### Output partitioning

A fragment makes its results available to exactly one downstream fragment. A `MultiFragmentPlan` is therefore a tree of fragments — no diamonds, no cycles. A fragment whose output is consumed in two places is duplicated into two fragments, one per consumer.

The partitioning scheme determines how many downstream tasks are needed:

1. **Hash-partitioned** — rows are partitioned by key into exactly N
   buckets. The downstream fragment must have exactly N tasks — a
   mismatch is a hard failure. The optimizer should only produce
   hash-partitioned output when the downstream operator needs data
   organized by key (e.g., hash join, grouped aggregation).
2. **Arbitrary** — rows go into a shared pool. Downstream tasks pull data
   on demand — whichever task is ready gets the next chunk. Supports any
   number of downstream tasks.
3. **Broadcast** — all rows are sent to every downstream task. Supports any
   number of downstream tasks.
4. **Gather** — all rows flow to a single partition. When the downstream
   fragment has multiple tasks (e.g., UNION ALL with other inputs), only
   one task reads from this exchange; the others do not connect to it.

#### Mixing data sources in a fragment

Only multi-input plan nodes can bring different data sources into the same
fragment: **joins** (hash, merge, nested loop, spatial) and **UNION ALL**. Index lookup joins are different — the lookup side
is accessed via the connector's index lookup API, not through splits or
exchanges.

**Joins** have three valid input combinations:

1. Both inputs come from hash-partitioned exchanges that partition into the
   same N partitions on the join keys. The fragment runs exactly N tasks.
   No scans in the fragment.
2. The data in the connector is already partitioned on the join keys (e.g.,
   bucketed Hive tables). If both sides are bucketed compatibly, no
   exchanges are needed — each task reads a matching pair of partitions.
   The number of tasks is at most the number of buckets; when there are
   more buckets than available workers, a task may process multiple
   buckets — the runtime can assign buckets dynamically as earlier ones
   complete, similarly to how it assigns scan splits. If only one side is
   bucketed, the non-bucketed side goes through a hash-partitioned
   exchange. The optimizer uses `scaleDown` to determine the exchange
   width and partition function — see Section 3 for details. (Not implemented yet — see the Section 3 status callout.)
3. One input comes from a broadcast exchange, the other can be anything
   (scan, exchange, Values). The non-broadcast side determines the task
   count.

**UNION ALL** takes N inputs and concatenates them. Unlike joins, there is
no co-partitioning requirement — each input is independent.

Some UNION ALL inputs require exactly 1 task:

- **Values** — to avoid duplicating the constant data.
- **SystemConnector** — must run on the coordinator. Should also not
  share a fragment with data-intensive operations to avoid overloading
  the coordinator, which has other responsibilities (query coordination,
  metadata management).
- **Single-task operators** — global aggregation (no grouping keys),
  ORDER BY, LIMIT, TopN, and others that produce output requiring a
  single task.

When all UNION ALL inputs are single-task, no isolation is needed — the union fragment simply runs as 1 task.

When a UNION ALL mixes single-task inputs with parallel inputs (scans, exchanges), the single-task inputs must be moved into a separate upstream fragment behind an exchange. Otherwise the single-task constraint would pin the entire union fragment to 1 task, defeating the parallelism of the other inputs. Multiple single-task inputs can share that upstream fragment since they are mutually compatible. The exchange type matches what the union's consumer needs — hash-partitioned if a downstream operator requires it (e.g., GROUP BY on the union output), arbitrary otherwise.

For the full UNION ALL fragment-placement rules — co-location decisions, per-leg `Repartition` choices, and how the parent's desired distribution is reconciled with leg distributions — see [UnionAllPlanning.md](UnionAllPlanning.md).

### 2. Runtime Contract: Task Creation and Data Wiring

The runtime receives a `MultiFragmentPlan` — a list of
`ExecutableFragment`s linked by `inputStages`. For each fragment, the
runtime must decide how many tasks to create, how to wire data between
producer and consumer tasks, and how to distribute splits to leaf
fragments.

```cpp
// Links an ExchangeNode in the consumer to its producer fragment.
struct InputStage {
    /// ExchangeNode in the consumer fragment.
    velox::core::PlanNodeId consumerNodeId;
    /// Identifies the producer fragment.
    std::string producerTaskPrefix;
};

/// Determines how the runtime decides the task count for a fragment.
enum class FragmentType {
    kSource,       /// Task count driven by splits at runtime.
    kFixed,        /// Exactly 'width' tasks.
    kSingle,       /// Exactly 1 task, any worker.
    kCoordinator,  /// Exactly 1 task, on coordinator.
};

struct ExecutableFragment {
    std::string taskPrefix;
    FragmentType type;
    /// Required for kFixed, optional hint for kSource, nullopt otherwise.
    std::optional<int32_t> width;
    velox::core::PlanFragment fragment;
    std::vector<InputStage> inputStages;
    /// See Section 3 for grouped execution fields.
};
```

#### Task count

The runtime determines task count based on `FragmentType`:

- **kFixed** — create exactly `width` tasks. The optimizer guarantees that
  all hash-partitioned exchange inputs into this fragment use the same
  `numPartitions` equal to `width`.
- **kSingle** — create exactly 1 task.
- **kCoordinator** — create exactly 1 task, on the coordinator node.
- **kSource** — runtime decides. The optimizer may set `width` as a
  recommended task count. The runtime may use this as a starting point
  but is not bound by it — it may adjust based on actual split count
  and available workers.

> **Future work: cardinality-based width selection.** Today the
> optimizer typically sets `width = numWorkers` for kFixed fragments
> and leaves kSource width unset. This wastes resources on small
> queries — a global aggregation over a few thousand rows should not
> fan out to 100 workers. The optimizer should use cardinality
> estimates to pick a smaller `width` when the data is small (kFixed:
> set `width` and the matching upstream `numPartitions`; kSource: set
> `width` as a hint to the runtime). Cardinality estimates at fragment
> boundaries are already available — the optimizer just needs to
> consult them when choosing `width`.

#### Data wiring

Each `InputStage` in a consumer fragment identifies an `ExchangeNode` (by
plan node ID) and a producer fragment (by task prefix). The runtime must
connect producer tasks to consumer tasks so that data flows correctly.

The producer fragment's root node is always a `PartitionedOutputNode`.
Its `Kind` determines how data is distributed to consumers. The runtime
wires them by adding remote splits to consumer tasks. Each remote split
identifies a producer task and an output buffer to read from.

**kPartitioned (hash):** The producer partitions rows into N buckets using
a hash function on partition keys. The runtime adds a remote split for
each producer task to each consumer task. Each split specifies which
output buffer to read from — consumer task `i` reads bucket `i` from
all producer tasks.

**kBroadcast:** The producer makes all data available to every consumer
task. The runtime adds a remote split for each producer task to each
consumer task, and tells each producer how many consumers there are (via
`updateOutputBuffers`).

**kArbitrary:** The producer places data into a shared pool. The runtime
adds a remote split for each producer task to each consumer task, and
tells each producer how many consumers there are (via
`updateOutputBuffers`). Consumer tasks pull data on demand — the
producer-side output buffer distributes data to whichever consumer is
ready rather than replicating.

**Single (gather):** The producer writes all data to a single output
buffer. The runtime adds a remote split for the producer to exactly one
consumer task. Other consumer tasks do not connect to this exchange.

#### Split generation and distribution

For fragments with table scans (kSource), the runtime enumerates splits
via connector APIs and distributes them to tasks. Each split is assigned
to exactly one task. A task may receive multiple splits and process them
sequentially (or in parallel using multiple drivers).

For fragments without scans (kSingle, kCoordinator), there are no
connector splits. Data arrives through exchanges or is embedded in the
plan (e.g., Values). kFixed fragments typically have no scans, but may have them in grouped execution (see Section 3) or when a scan leg is co-located in a kFixed UNION ALL fragment (see [UnionAllPlanning.md](UnionAllPlanning.md)).

In single-node mode (`numWorkers == 1`), the entire plan runs in a single
kSingle fragment with no exchanges.

#### Examples

**UNION ALL with Values:**

`SELECT a FROM t UNION ALL SELECT 42`. The optimizer produces:

- Fragment 0 (kSingle): Values(42), output via arbitrary.
- Fragment 1 (kSource): UNION ALL combining inline TableScan(t)
  with exchange from fragment 0. Output via gather (final result).

The runtime wires:

- Fragment 0: creates 1 task (kSingle), no splits needed.
- Fragment 1: creates tasks based on available splits for TableScan(t).
  Adds remote splits from fragment 0 (arbitrary) to all tasks; tasks pull data on demand.

**UNION ALL with GROUP BY:**

`SELECT * FROM t UNION ALL (SELECT k, COUNT(*) FROM u GROUP BY k)`. The optimizer produces:

- Fragment 0 (kSource): TableScan(u), output via hash-partitioned on `k`.
- Fragment 1 (kFixed N): UNION ALL combining
  - inline TableScan(t) (kSource scan, adapts to N tasks), and
  - aggregation on `k` reading from the hash exchange from fragment 0.

  Output via gather (final result).

The runtime wires:

- Fragment 0: creates tasks based on available splits for TableScan(u). Output is hash-partitioned into N buckets.
- Fragment 1: creates N tasks (kFixed). Reads scan splits for TableScan(t) (each task gets a share). Adds remote splits from fragment 0 (hash-partitioned) — task `i` reads bucket `i`.

The FINAL aggregation lives in the same fragment as the UNION ALL because A6 (kSource + kFixed N) co-locates the kFixed leg with the union — see [UnionAllPlanning.md](UnionAllPlanning.md).

### 3. Grouped Execution (Bucketed Tables)

> **Status: design proposal — not yet implemented.** This section
> describes the intended model. The `PartitionType` API,
> `groupedNodes` / `groupedExecution` fields on `ExecutableFragment`,
> `GroupedSplitSource`, and split-level `groupId` tagging do not
> exist in the codebase today (2026-05-08).

Bucketed tables (e.g., Hive tables with `bucketed_by`) store data
pre-partitioned by key into a fixed number of **buckets**. The
optimizer and runtime can exploit this pre-partitioning for two
benefits:

1. **Avoid shuffles.** When data is already partitioned on the right
   keys, there's no need to repartition through an exchange. This
   eliminates network transfer and serialization overhead.

2. **Conserve memory.** Per-group processing lets the runtime process
   one group at a time, clearing hash tables between groups. Memory
   usage becomes proportional to per-group data — typically 1/g of
   the total. For a table with 256 buckets, this is a 256x reduction
   in peak memory. Without this, a join or aggregation builds hash
   tables over the entire dataset, which may not fit in memory.

Both benefits are valuable. Skipping the shuffle without per-group
processing still builds full-sized hash tables. Velox already
supports grouped execution with group-aware split assignment. The design here is about the optimizer
producing the right plan structure and the coordinator distributing
splits correctly so that Velox's existing grouped execution machinery
can be used.

Three cases arise:

**Aggregations and window functions.** When a single scan reads a
bucketed table and the grouping/partition keys match the bucket keys,
the runtime processes one bucket at a time, clearing hash tables
between buckets. No shuffle is needed — the data is already
partitioned correctly.

**Joins — all sides bucketed (co-bucketed).** When all join inputs
scan tables bucketed on the join keys with compatible bucketing, no
shuffle is needed. The runtime reads matching buckets from all scans
together and processes them as a group, clearing state between groups.
When tables have different bucket counts (e.g., 128 vs 256), a
**group** combines matching buckets across tables — the connector's
partition type determines the mapping (e.g., for Hive, bucket 0 from
a 128-bucket table matches buckets 0 and 128 from a 256-bucket table
because the hash function is the same and the bucket counts are
integer multiples).

**Joins — mixed inputs.** When some join inputs are bucketed scans
and others are non-bucketed (intermediate results or non-bucketed
tables), the non-bucketed side goes through a hash-partitioned
exchange compatible with the bucketing. The fragment processes
exchange partitions and scan splits together in groups.

#### ExecutableFragment extensions

Grouped execution adds two fields to `ExecutableFragment`:

```cpp
struct ExecutableFragment {
    // ... base fields from Section 2 ...

    /// Per-node PartitionType for grouped leaf nodes. Scan nodes
    /// carry the PartitionType passed to getGroupedSplitSource /
    /// getSplitSource — the connector uses it to compute group IDs.
    /// Exchange nodes have nullptr (their partitions are already
    /// aligned with groups by construction). Empty when not grouped.
    std::unordered_map<
        velox::core::PlanNodeId,
        std::shared_ptr<PartitionType>> groupedNodes;

    /// When true, the runtime processes one group at a time and clears
    /// hash tables between groups. When false, all groups are processed
    /// at once (shuffle avoidance only, no memory reduction).
    /// Only meaningful when groupedNodes is non-empty.
    bool groupedExecution{false};
};
```

The group count is derived from the `PartitionType` via
`numPartitions()`. All `PartitionType`s in a fragment's `groupedNodes` are aligned —
same group count and compatible grouping, so that group `i` from
every node contains rows with matching key values. This is
guaranteed by the optimizer's `copartition()` logic. The runtime can read the
group count from any entry.

A fragment has at most one grouped set. When `groupedNodes` is
non-empty, the runtime routes splits to tasks by group ID, ensuring
co-bucketed splits land on the same task. When `groupedExecution`
is also true, the runtime processes one group at a time and clears
hash tables between groups. Not all leaf nodes in a fragment
necessarily participate in grouping.

For example, in a fragment that co-bucket-joins t and u and then broadcast-joins the result with a small dimension d, `groupedNodes` includes the scans for t and u but not the broadcast exchange for d — see examples 9 and 10 below.

**The model is connector-agnostic.** It applies to any connector whose physical layout induces a partitioning of rows, not just Hive bucketing. The `PartitionType` is a connector-defined projection from the file's partition tuple to a group ID in `[0, numPartitions())`, so the runtime can route splits and consume hash exchanges uniformly without knowing how each connector computes group IDs:

- **Hive** uses `bucket % numGroups`. When the table is also partitioned by `ds` and the query joins on both `ds` and the bucket key, `(partition, bucket)` pairs are the groups.
- **Iceberg** can use any partition transform — `bucket(N, k)`, value-based transforms like `day(event_time)`, or a compound projection across multiple transforms. The group count may come from the data (e.g., the number of distinct days in the manifest).

Hive's `CLUSTERED BY (a, b)` and Iceberg's `bucket(16, a), bucket(32, b)` are fundamentally different despite looking similar. Hive hashes both columns together — `hash(a, b) % N`. Iceberg hashes each column independently — `hash(a) % 16` and `hash(b) % 32`. A join on `a` alone can leverage Iceberg's bucketing (rows with the same `a` are in the same `a`-bucket regardless of `b`) but cannot leverage Hive's (rows with the same `a` may be in different buckets due to different `b` values). A typed `PartitionType` lets the connector encode these rules; a scalar group count cannot.

See "Generalized partitioning" below for the Iceberg-specific scenarios this enables.

#### Optimizer: plan construction

The optimizer detects bucketed tables, computes group counts, and
sets up fragments for grouped execution.

**PartitionType API.** `PartitionType` is an abstract interface. Each connector provides its own subclass (e.g., `HivePartitionType`, `IcebergPartitionType`) that implements all methods — including `copartition()` and `scaleDown()` — using its own bucketing rules.

```cpp
class PartitionType {
 public:
  /// Returns a PartitionType compatible with both 'this' and 'other',
  /// or nullptr if incompatible. The returned type's numPartitions()
  /// gives the common group count.
  virtual std::shared_ptr<PartitionType> copartition(
      const PartitionType& other) const = 0;

  /// Returns a PartitionType with at most 'maxPartitions' partitions,
  /// compatible with this partitioning. Used to find exchange width
  /// when mixing bucketed scans with hash exchanges.
  virtual std::shared_ptr<PartitionType> scaleDown(
      int32_t maxPartitions) const = 0;

  /// Returns the number of partitions (group count).
  virtual int32_t numPartitions() const = 0;

  /// Returns a factory that makes partition functions.
  virtual velox::core::PartitionFunctionSpecPtr makeSpec(
      const std::vector<velox::column_index_t>& channels,
      const std::vector<velox::VectorPtr>& constants,
      bool isLocal) const = 0;
};
```

**Copartition.** When multiple bucketed tables are joined, `copartition()` finds a common partitioning scheme. It returns an owned `PartitionType` compatible with both inputs (when the two have different bucket counts, the result is a new object that neither input holds), or nullptr if they are incompatible. The returned type's `numPartitions()` gives the group count `g`, and its `makeSpec` produces a partition function that maps rows to `0..g-1`.

For Hive, `copartition` returns a `HivePartitionType` with `min(bucketsA, bucketsB)` groups when one divides the other.

`copartition` and `makeSpec` are conceptually the same thing — both align two data sources so matching keys land on the same task. `copartition` does it across two scans (each side is already partitioned by its own rules; the result is a partitioning compatible with both). `makeSpec` does it across a scan and an exchange (the scan is fixed; the exchange is repartitioned to match). Both produce per-row group assignments in `[0, numPartitions())` using the connector-defined hash function.

**Mixed inputs.** When joining a bucketed table with a non-bucketed table or intermediate query result, the optimizer hash-partitions the non-bucketed side using the same partition function as the bucketed side.

The optimizer computes group count `g` from the bucketed side(s) via `copartition()`, then finds the exchange width `N` using `scaleDown(numWorkers)`. `N` is the largest value <= `numWorkers` that the connector considers valid given `g` (for Hive, the largest divisor of `g` that is <= `numWorkers`). The resulting `PartitionType` encapsulates both `g` and `N`, so its `makeSpec` produces the optimal partition function (e.g., Hive emits `bucket % N` directly rather than `bucket % g % N`).

A hash-partitioned exchange with N partitions is conceptually similar to a table bucketed into N buckets — partition `p` contains all rows whose key hashes to `p`. This is why `groupedNodes` can include both scan nodes and exchange nodes.

`scaleDown` exists because of streaming shuffle's constraint: the consumer fragment must run exactly N tasks concurrently, so N can be at most `numWorkers`. With durable shuffle, the consumer can process groups dynamically and `scaleDown` is unnecessary — the producer can use the bucketed side's native group count `g` directly. See "Mixed inputs (scans + exchanges)" under Runtime contract below for how the streaming vs. durable choice changes fragment type.

The consumer fragment is `kFixed` with `width = N`. `groupedNodes` includes both the scan node(s) and the exchange node(s).

For Hive, `scaleDown(maxPartitions)` returns a `HivePartitionType` with the largest divisor of `numBuckets` that is <= `maxPartitions`; its `makeSpec` produces `nativeBucketFunction % N`.

**Selecting partition types: TableLayout API.** The optimizer needs to know *which* `PartitionType`s are applicable to a given join, and how to pair them across the two sides. Two methods on `TableLayout` handle this:

> Not required for v1 of bucketed execution. Basic Hive bucketing — one `CLUSTERED BY` column set per table, single applicable `PartitionType` per side — can use `PartitionType::copartition()` directly without going through this API. The `TableLayout` API is needed to support Hive `(partition, bucket)` grouping, Iceberg's multiple/value-based transforms, and any case where a side has more than one applicable `PartitionType`. It will most likely be an extension on top of the initial version.

```cpp
/// Returns all partition types that could be used for grouped
/// execution on a join with the given keys. The connector returns
/// all applicable schemes — individual transforms, compound
/// transforms, or the full partitioning scheme.
virtual std::vector<std::shared_ptr<PartitionType>> getPartitionTypes(
    const std::vector<const Column*>& columns) const {
  return {};
}

/// Pairs of columns from the left and right sides of a join that are
/// equated by join predicates (e.g., left.a = right.c).
using JoinEqualities =
    std::vector<std::pair<const Column*, const Column*>>;

struct CopartitionResult {
    std::shared_ptr<PartitionType> left;
    std::shared_ptr<PartitionType> right;
};

/// Given join equalities and the other side's partition types,
/// returns the best compatible pair of PartitionTypes, or nullopt
/// if no compatible partitioning exists.
virtual std::optional<CopartitionResult> copartition(
    const JoinEqualities& equalities,
    const std::vector<std::shared_ptr<PartitionType>>& otherSide) const {
  return std::nullopt;
}
```

The optimizer flow: call `getPartitionTypes(joinKeys)` on one side, then call `copartition(equalities, otherPartitionTypes)` on the other side. The connector returns the best compatible pair, having checked column alignment, hash function compatibility, and group count compatibility — all connector-specific. The optimizer may still reject the result if grouped execution is not beneficial (e.g., per-group data is too large).

> **`makeSpec` extension for exchange use.** When the bucketed side's `PartitionType` is used to configure an exchange on the non-bucketed side, `makeSpec` must be told how to map the bucketed side's partition columns to the non-bucketed side's join columns. The current `channels` parameter assumes positional alignment, which is fine when the optimizer can supply column indices in the right order. With multi-column or multi-transform partition schemes (e.g., Iceberg `bucket(16, a), bucket(32, b)`), the optimizer needs to thread the join equalities into `makeSpec` — analogous to how it threads them into `copartition` above — so the exchange-side partition function applies the same per-column transforms to the matching join keys. This is an extension on top of v1.

For Hive with `CLUSTERED BY (a, b)`: `getPartitionTypes` returns `[hivePartitionType({a, b})]` if both `a` and `b` are in the join keys, empty otherwise. For Iceberg with `bucket(16, a), bucket(32, b)` and join keys `{a, b}`: returns `[icebergPT(16, {a}), icebergPT(32, {b}), icebergPT(512, {a, b})]` — individual transforms plus a compound option.

#### Runtime contract

The following subsections parallel Section 2 — task count, data
wiring, and split generation — describing how each changes for
grouped execution.

**Task count.**

*Aggregations, window functions, and co-bucketed joins (all scans).*
The fragment is kSource. The runtime may use fewer workers than
groups, assigning multiple groups to each worker sequentially.
Workers that finish early can pick up additional unstarted groups.

*Mixed inputs (scans + exchanges).* The key difference from scans
is how exchange partitions can be consumed. With **streaming
shuffle**, every exchange output buffer must have an active
consumer — if a buffer has no reader, it fills up and blocks the
producer. The fragment is kFixed with `width = N`, and each task
processes exactly one group. With **durable shuffle**, exchange
partitions are written to storage and can be read independently.
The fragment can be kSource with dynamic group assignment — workers
pick up groups on demand, reading the matching exchange partition
and scan splits for each group.

**Data wiring.**

For exchange nodes in `groupedNodes`, the runtime wires exchange
partition `i` to the task processing group `i` using the standard
remote split mechanism. No new wiring logic is needed — the
exchange's hash partitioning guarantees alignment.

**Split generation.**

Both single-scan and multi-scan grouped execution use
`GroupedSplitSource`, which produces complete groups of splits.
A bucket may span multiple files (e.g., different partitions
stored in different formats), so a group may contain multiple
splits. The runtime processes all splits for a group before
clearing state and moving to the next group.

The connector uses the `PartitionType` to determine group
membership (e.g., for Hive, `bucket % partitionType.numPartitions()`).
In the mixed case (scans + exchanges), the `PartitionType` comes
from `scaleDown`. Group `i` contains scan splits for all native
buckets that map to group `i` under the connector's remapping.

The `co_getGroups` method is a pagination API — the caller requests
up to `maxNumGroups` at a time, and the connector returns as many
as are ready.

```cpp
struct SplitGroup {
    /// Value in 0..partitionType.numPartitions()-1, matches exchange
    /// partition numbers.
    int32_t groupId;
    std::vector<std::shared_ptr<ConnectorSplit>> splits;
};

struct SplitGroupBatch {
    std::vector<SplitGroup> groups;
    bool noMoreGroups{false};
};

class GroupedSplitSource {
    /// Returns up to 'maxNumGroups' complete groups in group ID order.
    /// May return fewer if not all splits for the next groups are
    /// ready yet. Must report all groups in
    /// 0..partitionType.numPartitions()-1, including empty ones
    /// (groups with no splits).
    virtual folly::coro::Task<SplitGroupBatch> co_getGroups(
        uint32_t maxNumGroups) = 0;
};

/// Creates a GroupedSplitSource for one table scan. The
/// PartitionType tells the connector how to map native partitions
/// to groups (e.g., for Hive, bucket % partitionType.numPartitions();
/// for Iceberg, using the transform encoded in the PartitionType).
virtual std::shared_ptr<GroupedSplitSource> getGroupedSplitSource(
    const ConnectorSessionPtr& session,
    const ConnectorTableHandlePtr& tableHandle,
    const std::vector<PartitionHandlePtr>& partitions,
    const std::shared_ptr<PartitionType>& partitionType) = 0;
```

The runtime requests groups from each scan's `GroupedSplitSource`,
consuming all scans for a group before moving to the next. A worker
processing multiple groups clears hash tables between groups,
keeping memory proportional to per-group data.

Exchange nodes in `groupedNodes` do not use `GroupedSplitSource`
— their partitions are already correctly aligned with groups by
construction (partition `i` = group `i`).

#### Shuffle avoidance without per-group processing

Some connectors discover splits incrementally in arbitrary order —
Hive Metastore returns partitions unpredictably, file listings on some remote storage systems are paginated without ordering guarantees, and bucket numbers may only
be known after parsing file names. `GroupedSplitSource` requires complete groups in order,
which would force the connector to wait for all discovery to
complete before emitting any groups, hurting interactive query
latency.

When throughput matters more than latency, grouped execution is still the better choice — both benefits outweigh the wait. But for low-latency queries where split-enumeration latency dominates, the wait is unacceptable: overlapping split enumeration with execution is key to keeping latency low. For connectors in this regime, a fallback mode provides shuffle avoidance without per-group hash table clearing.

**Optimizer setup.** The optimizer uses the same `copartition()` /
`scaleDown()` flow as grouped execution to determine a shared
`PartitionType`. For co-bucketed joins, `copartition()` finds the
common partitioning across all bucketed inputs. Then
`scaleDown(numWorkers)` reduces the group count to at most
`numWorkers`, producing a `PartitionType` with
`numPartitions() = N` where N <= numWorkers. The fragment is
kFixed with `width = N` — the optimizer sets the task count so
the runtime can co-schedule splits by `groupId` deterministically.
(kSource is only possible with `GroupedSplitSource`, which delivers
complete groups in order — connectors like Hive that discover
splits incrementally cannot support it, hence this fallback.)

The optimizer populates `groupedNodes` with the scaled
`PartitionType` for each scan node (exchange nodes get nullptr)
and sets `groupedExecution = false`.

**Split delivery.** Since `ConnectorSplit` is a Velox type and
cannot carry `groupId` directly, Axiom defines its own
`axiom::connector::Split` that wraps a `ConnectorSplit` with an
optional `groupId`. The runtime uses `groupId` to co-schedule
splits from the same group on the same task, then passes the inner
`ConnectorSplit` to Velox for execution. Splits arrive
incrementally as they are discovered — no ordering or completeness
requirement. Tasks start processing immediately; hash tables are
not cleared between groups, so memory is proportional to total
data / numTasks rather than per-group data.

```cpp
namespace axiom::connector {

/// Wraps a Velox ConnectorSplit with an optional group ID for
/// bucketed execution routing.
struct Split {
    std::shared_ptr<velox::connector::ConnectorSplit> connectorSplit;
    std::optional<int32_t> groupId;
};

struct SplitBatch {
    std::vector<Split> splits;
    bool noMoreSplits{false};
};

/// Produces splits incrementally. When configured with a
/// PartitionType, each Split carries a groupId computed by
/// the connector.
class SplitSource {
 public:
    /// Returns the next batch of splits. Returns an empty vector
    /// with noMoreSplits=true when exhausted.
    virtual folly::coro::Task<SplitBatch> co_getSplits(
        uint32_t maxSplits) = 0;
};

} // namespace axiom::connector
```

The existing `getSplitSource` on `ConnectorSplitManager` gains
an optional `PartitionType` parameter:

```cpp
virtual std::shared_ptr<SplitSource> getSplitSource(
    const ConnectorSessionPtr& session,
    const ConnectorTableHandlePtr& tableHandle,
    const std::vector<PartitionHandlePtr>& partitions,
    const std::shared_ptr<PartitionType>& partitionType = nullptr) = 0;
```

When `partitionType` is set, the connector sets `groupId` on each
`Split` to a value in `0..partitionType->numPartitions()-1`,
computed using the mapping encoded in the `PartitionType` (e.g.,
for Hive, `bucketId % partitionType->numPartitions()`).

| | Grouped execution | Shuffle avoidance only |
|---|---|---|
| Fragment type | kSource | kFixed |
| Split delivery | `GroupedSplitSource` (complete groups) | `SplitSource` (incremental, with `groupId`) |
| Per-group clearing | Yes | No |
| Latency | May wait for group completion | Incremental, tasks start immediately |
| Memory per task | Per-group data | Total data / numTasks |

Shuffle avoidance uses the regular `SplitSource` with `groupId`
on each split — no per-group clearing. A future grouped-execution
path may introduce a `GroupedSplitSource` and a corresponding
capability bit on `ConnectorSplitManager`; the API surface is not
defined here.

#### Examples

These examples explain the design and validate it against diverse scenarios. All examples assume `numWorkers = 100`, Hive bucketing, and
`remoteOutput = true` — results remain distributed across workers.
When `remoteOutput = false`, a kSingle output fragment is added to
gather results to a single node. The output fragment is omitted
from the examples below.

| # | Case | All leaves grouped? | Grouped nodes | Grouped scans / exchanges |
|---|------|:-------------------:|:-------------:|:-------------------------:|
| 1 | Bucketed agg | yes | scans | 1 / 0 |
| 2 | Bucketed window | yes | scans | 1 / 0 |
| 3 | Co-bucketed join | yes | scans | 2 / 0 |
| 4 | Mixed join | yes | mix | 1 / 1 |
| 5 | 3-way co-bucketed | yes | scans | 3 / 0 |
| 6 | 2 bucketed + 1 not | yes | mix | 2 / 1 |
| 7 | 1 bucketed + 2 not | yes | mix | 1 / 2 |
| 8 | Agg → co-bucketed join | yes | scans | 2 / 0 |
| 9 | Agg → broadcast join | no | scans | 1 / 0 |
| 10 | Broadcast → agg | no | scans | 1 / 0 |

**1. Aggregation on a bucketed table.**

`SELECT k, COUNT(*) FROM t GROUP BY k` where t has 256 buckets on k.

- Fragment 0 (kSource, groupedNodes={scan_t: hive256}):
  TableScan(t) → Aggregation.

The runtime uses `GroupedSplitSource` (configured with t's
`PartitionType`) to get complete groups of splits, processes one
group at a time, and clears the aggregation hash table between
groups.

**2. Window function on a bucketed table.**

`SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) FROM t`
where t has 256 buckets on k.

- Fragment 0 (kSource, groupedNodes={scan_t: hive256}):
  TableScan(t) → Window.

Same as aggregation — per-group processing via `GroupedSplitSource`
(configured with t's `PartitionType`), clearing window state
between groups.

**3. Join — both sides bucketed.**

`SELECT * FROM t JOIN u ON t.k = u.k` where t has 256 buckets and
u has 128 buckets, both on k.

`copartition(t, u)` returns a `PartitionType` with `g = 128`
(min of 256 and 128, since 128 divides 256).

- Fragment 0 (kSource, groupedNodes={scan_t: copart128,
  scan_u: copart128}):
  TableScan(t) → HashJoin ← TableScan(u).

The `PartitionType` (`copart128`) has `numPartitions() = 128`. Each
scan's `GroupedSplitSource` receives this `PartitionType` and uses
it to compute group membership. Group `i` contains: from t, splits
for buckets {i, i+128}; from u, the split for bucket i. Workers
pick up groups dynamically — no `scaleDown` is needed because there
are no exchanges.

**4. Join — one side bucketed.**

`SELECT * FROM t JOIN w ON t.k = w.k` where t has 256 buckets on k,
w is not bucketed.

`scaleDown(100)` on t's partition type returns N = 64 (largest
divisor of 256 that is <= 100).

- Fragment 0 (kSource): TableScan(w), output via hash-partitioned
  on k into 64 partitions using `scaleDown`'s `makeSpec`.
- Fragment 1 (kFixed, width=64,
  groupedNodes={scan_t: scaled64, exchange_w: nullptr}):
  TableScan(t) → HashJoin ← Exchange(w).

Task `i` gets exchange partition `i` and scan splits for buckets
{i, i+64, i+128, i+192} from `GroupedSplitSource` (configured
with the scaled `PartitionType`).

**5. Two joins — all three inputs bucketed.**

`SELECT * FROM t, u, v WHERE t.k = u.k AND t.k = v.k` where t has
256 buckets, u has 128 buckets, v has 64 buckets, all on k.

`copartition(t, u)` → g = 128. `copartition(result, v)` → g = 64.

- Fragment 0 (kSource, groupedNodes={scan_t: copart64,
  scan_u: copart64, scan_v: copart64}):
  TableScan(t) → HashJoin ← TableScan(u) → HashJoin ← TableScan(v).

Group `i` contains: from t, splits for buckets {i, i+64, i+128,
i+192}; from u, splits for buckets {i, i+64}; from v, the split
for bucket i.

**6. Two joins — two inputs bucketed, one not.**

`SELECT * FROM t, u, w WHERE t.k = u.k AND t.k = w.k` where t has
256 buckets, u has 128 buckets (both on k), w is not bucketed.

`copartition(t, u)` → g = 128. `scaleDown(100)` → N = 64 (largest
divisor of 128 that is <= 100).

- Fragment 0 (kSource): TableScan(w), output via hash-partitioned
  on k into 64 partitions.
- Fragment 1 (kFixed, width=64,
  groupedNodes={scan_t: scaled64, scan_u: scaled64,
  exchange_w: nullptr}):
  TableScan(t) → HashJoin ← TableScan(u) → HashJoin ← Exchange(w).

Group `i` contains: from t, splits for buckets {i, i+64, i+128,
i+192}; from u, splits for buckets {i, i+64}; from exchange,
partition i.

**7. Two joins — one input bucketed, two not.**

`SELECT * FROM t, w1, w2 WHERE t.k = w1.k AND t.k = w2.k` where
t has 256 buckets on k, w1 and w2 are not bucketed.

`scaleDown(100)` on t's partition type → N = 64.

- Fragment 0 (kSource): TableScan(w1), output via hash-partitioned
  on k into 64 partitions.
- Fragment 1 (kSource): TableScan(w2), output via hash-partitioned
  on k into 64 partitions.
- Fragment 2 (kFixed, width=64,
  groupedNodes={scan_t: scaled64, exchange_w1: nullptr,
  exchange_w2: nullptr}):
  TableScan(t) → HashJoin ← Exchange(w1) → HashJoin ← Exchange(w2).

Two exchange nodes in `groupedNodes`. Task `i` gets exchange
partitions `i` from both w1 and w2, plus scan splits for buckets
{i, i+64, i+128, i+192} from t.

**8. Bucketed aggregation followed by bucketed join.**

`SELECT * FROM (SELECT k, COUNT(*) AS c FROM t GROUP BY k) x
 JOIN u ON x.k = u.k` where t has 256 buckets and u has 128 buckets,
both on k.

`copartition(t, u)` → g = 128. No `scaleDown` needed — the fragment
is kSource with no exchanges, so the runtime assigns groups to
workers dynamically.

- Fragment 0 (kSource, groupedNodes={scan_t: copart128,
  scan_u: copart128}):
  TableScan(t) → Aggregation(k) → HashJoin ← TableScan(u).

The aggregation and join are in the same fragment. Both benefit from
per-group processing — the aggregation hash table and the join hash
table are cleared between groups.

**9. Bucketed aggregation followed by broadcast join.**

`SELECT * FROM (SELECT k, COUNT(*) AS c FROM t GROUP BY k) x
 JOIN d ON x.v = d.v` where t has 256 buckets on k, d is a small
dimension table (broadcast).

- Fragment 0 (kSource): TableScan(d), output via broadcast.
- Fragment 1 (kSource, groupedNodes={scan_t: hive256}):
  TableScan(t) → Aggregation(k) → HashJoin ← Exchange(d).

`groupedNodes` includes only scan_t. The broadcast exchange is
not grouped — every task gets the full copy of d. The aggregation
hash table is cleared between groups; the broadcast join's hash
table (built from d) is the same for all groups and is not cleared.

**10. Broadcast join followed by bucketed aggregation.**

`SELECT k, SUM(t.v * d.w) FROM t JOIN d ON t.x = d.x GROUP BY k`
where t has 256 buckets on k, d is a small dimension table
(broadcast).

- Fragment 0 (kSource): TableScan(d), output via broadcast.
- Fragment 1 (kSource, groupedNodes={scan_t: hive256}):
  TableScan(t) → HashJoin ← Exchange(d) → Aggregation(k).

Same structure as example 9. The broadcast join enriches each row
from t with data from d. The aggregation downstream benefits from
per-group processing — its hash table is cleared between groups.
The join key (x) differs from the bucket key (k), but that doesn't
matter: grouping is on k (the bucket key), and the broadcast join
just passes data through without affecting the grouping.

#### Generalized partitioning

The `PartitionType`-based design handles Hive's simple `bucket % N` natively, but also generalizes to more complex partitioning schemes. This section walks through Hive partition-aware grouping and Iceberg as concrete examples.

**Hive partition-aware grouping.** When a Hive table is both partitioned by `ds` and bucketed by `userid`, and the query joins on both `ds` and `userid`, the unit of processing should be a single `(partition, bucket)` pair — not a bucket across all partitions. This keeps memory proportional to per-partition-per-bucket data. The `PartitionType` for this case has `numPartitions() = numPartitions * numBuckets` and maps each split to a `(partition, bucket)` group ID — the `PartitionType` abstracts away the connector-specific mapping.

**Iceberg.** Iceberg tables use a different partitioning model than Hive. Bucketing is one of several partition transforms (`bucket(N, col)`) that can be combined arbitrarily in a partition spec. Each file contains only rows with the same values for all partition transforms — e.g., with `bucket(16, user_id), day(event_time)`, a file contains rows for exactly one bucket and one day. The complete list of files and their partition values is stored in manifest metadata (Avro files), so the connector can discover all files and their partition values without directory listing or file name parsing.

`GroupedSplitSource` is feasible for Iceberg — manifest files provide partition values per-file upfront, so the connector can produce complete groups efficiently. This makes Iceberg a good candidate for full grouped execution (both shuffle avoidance and memory reduction).

Iceberg introduces scenarios not present in basic Hive bucketing:

*Multiple transforms on join keys.* Iceberg allows independent transforms in the same spec: `bucket(16, user_id), bucket(32, event_type)`. Each produces an independent partition value. Files are created at the granularity of the full partition tuple (e.g., `(bucket_a=7, bucket_b=23)`). For a join on `user_id` only, `bucket(16, user_id)` defines the grouping. For a join on both `(user_id, event_type)`, the connector can offer individual transforms or a compound projection with Y = 16 * 32 = 512 groups.

*Value-based transforms.* A join on `event_time` with a spec containing `day(event_time)` can use the day transform for grouping — the number of groups comes from the data (number of distinct days in the manifest metadata). This is the same pattern as Hive's `(partition, bucket)` grouping above.

*Partition evolution.* Iceberg allows changing the partition scheme without rewriting data. A table may have files from `bucket(16, k)` and newer files from `bucket(256, k)` coexisting. The connector handles this internally — it computes the effective group count across all specs (e.g., 16 when 16 divides 256) and presents a single `PartitionType` to the optimizer. Old bucket `i` maps to group `i`, new bucket `j` maps to group `j % 16`. If the group counts are incompatible (e.g., 16 and 100, where neither divides the other), the connector reports no compatible partitioning and the optimizer falls back to shuffle.

*Cost-based decision.* Grouped execution is beneficial only when per-group data is small enough to fit in memory comfortably. With Hive, tables typically have thousands of buckets, so per-group data is manageable. With Iceberg, value-based transforms like `day(event_time)` may produce groups that are too large — a single day's data could be terabytes. In such cases, a hash shuffle into more partitions may be better despite the network cost. The optimizer should estimate per-group data size (using cardinality estimates and group count) and choose grouped execution only when the per-group size is reasonable.

**General framing.** These scenarios are all instances of a general pattern. A table's partition scheme defines an N-dimensional space — each dimension is a transform over a column (bucket, day, identity, truncate, etc.). Each file is a point in this space. For grouped execution, the connector defines a projection from this N-dimensional space to a single dimension `[0, Y)` of group IDs, where Y is the number of groups. The `PartitionType` encodes this projection. Any transform on a join key — not just bucket — can define the grouping. The connector chooses the best projection based on the join keys and the other side's partitioning, via the `TableLayout` API described in "Optimizer: plan construction" above.
