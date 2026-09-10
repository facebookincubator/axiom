# UNION ALL Fragment Planning

## Goal

Pick the cheapest fragment layout for a UNION ALL that:
1. Produces correct results (all rows reach the consumer).
2. Minimizes data movement (don't reshuffle what's already in the
   right shape).

## Planning context: bottom-up plans + top-down requirements

Planning is bottom-up. By the time the optimizer plans UNION ALL, each
leg has already been planned and the following are known:
- The leg's output distribution (gather, hash(K), arbitrary, etc.).
- The leg's intrinsic fragment characteristics — what kind of fragment
  its root would naturally live in (connector-driven kSource, single-task
  kSingle, hash-exchange-consuming kFixed N).
- The leg's estimated cardinality.

Requirements come top-down. The parent (the operator consuming UNION
ALL's output) may pass a desired output distribution — e.g., GROUP BY
on K wants hash(K) input.

UNION ALL planning reconciles these by choosing:
1. **Which legs are co-located** (live in the same fragment as the
   UNION ALL operator). The choice is a tree decision: a leg with no
   `Repartition` between it and the UNION ALL operator is co-located.
2. **What kind of `Repartition`** to insert above each non-co-located
   leg — `arbitrary`, `gather`, or `hash(K)`. (Broadcast is never an
   option — it replicates each row to every consumer task, which would
   multiply UNION ALL counts.)

The parent's desired output distribution is treated as a hint: it is
honored per-leg only when at least one leg already produces it
natively; otherwise the parent inserts its own `Repartition` above the
union. UNION ALL planning does not insert a `Repartition` above the
union itself.

The fragment type of the UNION ALL operator's host fragment is *not* an
input to planning — it's a *consequence* of the co-location choice.
- All co-located legs are scans → kSource fragment.
- Any co-located leg is a hash-exchange consumer → kFixed N fragment
  (kSource scans, if also co-located, adapt to the fixed width).
- All co-located legs are single-task → kSingle fragment.

Some combinations cannot co-locate — for example, a kSingle leg with
any parallel leg, or two kFixed legs with different widths. The
compatibility rules below define which combinations are allowed;
planning must wrap any incompatible leg in a `Repartition` so the
remaining co-located legs share a single fragment type.

## The fragment-compatibility invariant

Every fragment has a single **type**. A fragment is replicated across
its tasks — every task runs the same plan, processing a different
chunk of the input data. So legs co-located in the consumer fragment
must agree on fragment type.

Fragment types:

- **kSource** — task count decided by the runtime, scaled by split
  count. Multiple scans can share one kSource fragment; each task
  receives splits from all of those scans. The optimizer may suggest
  a target width as a cardinality-based hint, but the runtime is not
  bound by it.
- **kFixed N** — exactly N tasks, set in the plan. The width must
  match the `numPartitions` of every hash exchange feeding this
  fragment.
- **kSingle** — exactly 1 task. Forced by a gather exchange or by an
  inherently single-task operator (Values, global aggregation,
  EnforceSingleRow).
- **kCoordinator** — exactly 1 task, on the coordinator node.

Compatibility rules for co-locating legs in one fragment:
- **All kSource legs** → consumer fragment is kSource. Splits from all
  scans get served to the runtime-decided task set.
- **kSource + kFixed N** → consumer fragment is kFixed N. The scan
  commits to N parallelism (loses runtime flexibility — if splits are
  sparse, some of the N tasks may be idle) but its splits serve the
  same N tasks that consume the hash exchange. Co-location works; a
  cost-driven optimizer might wrap the scan in arbitrary anyway when
  its expected parallelism is much smaller than N (future refinement,
  not correctness).
- **All kFixed legs with the same width N** → kFixed N. Each leg's
  hash exchange feeds the same N tasks; each task gets partition i
  from each leg.
- **All kSingle legs** → kSingle.
- **kSingle + any parallel leg** → CANNOT co-locate (correctness
  violation: the single-row leg would be multiplied across the parallel
  tasks). The kSingle leg must be wrapped in `Repartition(arbitrary)`.
- **kFixed N1 + kFixed N2** with N1 ≠ N2 → cannot co-locate (a fragment
  has one numRemotePartitions). One leg must be wrapped. Not produced by the
  optimizer today — every parallel leg ends up at `maxRemotePartitions`;
  covered here for completeness.

Note: An `arbitrary` exchange consumer can adopt any width, so it never
constrains the consumer fragment type — it conforms to whatever the
consumer already is. Same for the producer side: an arbitrary
PartitionedOutput sends to whatever consumer count is requested at
runtime.

## Fragment type vs. output distribution — they're independent

Two separate questions for the UNION ALL fragment (the one containing the
local merge of all legs):

1. **What's the fragment's type?** Driven by what's inside. If the fragment
   contains scans → kSource. If it only contains hash-exchange consumers
   → kFixed. If only a single-task source → kSingle.
2. **What's the fragment's output distribution?** Driven by what the
   parent needs. A `PartitionedOutput[HASH K]` at the root produces
   hash(K) output for the GROUP BY consumer fragment, regardless of
   whether the UNION ALL fragment itself is kSource or kFixed.

A kSource UNION ALL fragment can absolutely produce hash(K) output for a
downstream GROUP BY — the fragment runs connector-driven, locally merges its
legs, and shuffles via a hash PartitionedOutput at the end.

In code, a producer fragment's type is derived from its contents at lowering
time by walking the RelationOp sub-tree down to (but not including) any
inner Repartition or leaf, merging contributions per the compatibility rules
above. The Repartition that caps the fragment carries the wire output via
its `distribution()`; the two are independent.

## Reference examples

Each example states the SQL, leg facts, parent requirement, and the
optimal plan shape per the design.

Convention:
- `t_big`, `t_small` — large/small tables.
- `numWorkers = 4` for all examples unless stated.

### Coverage matrices

Examples are grouped into sections by what they exercise:
- **A** — Parent imposes no distribution requirement (e.g., UNION ALL
  feeds a filter, a project, or another UNION ALL).
- **B** — Parent requires hash(K) (e.g., GROUP BY, hash join).
- **C** — Parent requires gather/single (e.g., final query result,
  ORDER BY at top).
- **D** — UNION ALL as a hash join build (broadcast or shuffled).

Within each section, examples are numbered (A1, A2, …, B1, B2, …).

Coverage organized by leg fragment-type combination (rows) and
parent's distribution requirement (columns).

| Leg combination                | None | hash(K) | gather (single) |
|--------------------------------|------|---------|-----------------|
| All kSource                    | A1   | B1      | C1              |
| All kFixed N (same width)      | A2   | B2      | C3              |
| All kSingle                    | A3   | B3      | C2              |
| kSource + kSingle              | A4   | B4      | C4              |
| kFixed N + kSingle             | A5   | B5      | C5              |
| kSource + kFixed N             | A6   | B6      | C6              |
| kSource + kFixed + kSingle     | A7   | B7      | C7              |

For section B, each example is the canonical case where any kFixed leg
already produces the parent's required key. Three orthogonal variants
apply on top:
- **Wrong key:** kFixed leg produces hash(K') ≠ parent's hash(K) — add
  `Repartition(hash K)` on top of that leg.
- **Mixed match:** some kFixed legs match parent's K, others don't —
  apply the per-leg decision independently.
- **Join instead of GROUP BY:** same plan shape, parent fragment is a
  hash join instead of FINAL aggregation.

These variants don't change the leg-combination structure, so each
gets one note in the "B variations" section rather than separate
examples.

### A. Parent imposes no distribution requirement

**General principle (derived from A1–A7):**

1. **If any parallel leg exists, group all kSingle legs into one
   kSingle sub-fragment and wrap that sub-fragment in
   `Repartition(arbitrary)`.** One arbitrary exchange total, regardless
   of how many kSingle legs there are. Correctness — prevents row
   multiplication.
2. **Co-locate all remaining legs** (the wrapped-kSingle sub-union
   plus all parallel kSource and kFixed N legs). The resulting union
   fragment's type is:
   - `kFixed N` if any co-located leg is kFixed N (kSource scans adapt
     to the fixed width).
   - `kSource` if all co-located legs are kSource.
   - `kSingle` if no parallel legs exist (everything is kSingle, no
     wrapping needed).
3. **No remote exchanges are added** beyond:
   - One arbitrary exchange for the kSingle sub-union (if any).
   - The pre-existing exchanges intrinsic to each leg (e.g., a
     DISTINCT's PARTIAL→FINAL hash exchange).

The principle reduces to a per-leg classifier (kSingle vs. parallel)
plus "co-locate everything compatible". No "main leg" heuristic, no
cardinality-driven choices.

#### A1. All kSource — two scans
```sql
SELECT * FROM t_big UNION ALL SELECT * FROM t_small
```
Legs: kSource + kSource.
**Optimal:** Both scans co-located in one kSource UNION ALL fragment
with a LocalPartition. No remote exchanges added.

#### A2. All kFixed N — two parallel-hash-partitioned legs
```sql
(SELECT DISTINCT a FROM t1) UNION ALL (SELECT DISTINCT b FROM t2)
```
Legs: both kFixed N. Each is hash-partitioned for its own reason
(DISTINCT needs hash for dedup); the keys may or may not be the same.
**Optimal:** Both legs co-located in one kFixed N fragment with both
incoming hash exchanges and a LocalPartition. Each leg's FINAL agg
consumes only its own incoming hash exchange — they don't interfere
even if hashed on different keys. No new exchanges added (the two
PARTIAL→FINAL hash exchanges are intrinsic to each DISTINCT, pre-date
union planning).

#### A3. All kSingle — single-task legs
```sql
VALUES (1) UNION ALL SELECT COUNT(*) FROM t_a UNION ALL VALUES (2)
```
Legs: all kSingle.
**Optimal:** All legs co-located in one kSingle fragment with a
LocalPartition. No exchanges added.

#### A4. kSource + kSingle — scan + Values
```sql
SELECT * FROM t_big UNION ALL VALUES (1, 'x')
```
Legs: kSource + kSingle.
**Optimal:** kSingle group (just Values here) wrapped in arbitrary
to feed the kSource union fragment. **1 new arbitrary exchange.**

#### A5. kFixed N + kSingle — DISTINCT + Values
```sql
SELECT DISTINCT a FROM t_big UNION ALL VALUES (1)
```
Legs: kFixed N + kSingle.
**Optimal:** kSingle group wrapped in arbitrary to feed the kFixed N
union fragment that hosts the DISTINCT's FINAL agg. **1 new arbitrary
exchange** (plus DISTINCT's pre-existing PARTIAL→FINAL hash).

#### A6. kSource + kFixed N — scan + DISTINCT
```sql
SELECT a FROM t_huge UNION ALL SELECT DISTINCT a FROM t_other
```
Legs: kSource + kFixed N.
**Optimal:** Co-locate both in one kFixed N fragment per the
compatibility rules (scan adapts to fixed width, DISTINCT's hash
exchange feeds the same N tasks). LocalPartition merges. No new
exchanges added (DISTINCT's pre-existing hash exchange is the only
remote one).

#### A7. kSource + kFixed + kSingle — all three types
```sql
SELECT a FROM t_big
UNION ALL SELECT DISTINCT a FROM t_other
UNION ALL VALUES (1)
```
Legs: kSource + kFixed N + kSingle.
**Optimal:** kSingle group wrapped in arbitrary. Scan + DISTINCT
co-located in kFixed N union fragment per A6. **1 new arbitrary
exchange** (plus DISTINCT's pre-existing hash).


### B. Parent requires hash(K) on union output

**General principle (derived from B1–B7):**

1. **Apply section A's layout to all legs.**
2. **Substitute hash(K) for arbitrary** in any wrapping A would
   introduce. A wraps kSingle legs in arbitrary when parallel legs
   exist; in B, hash(K) is used instead, since hash output is the
   target anyway and the arbitrary intermediate is unnecessary. The
   wrapped kSingle group becomes
   a kFixed N producer of hash(K) that feeds the union fragment via a
   hash exchange. The union fragment is kFixed N either way (since at
   least one parallel leg exists or the wrap produces one).
3. **Add a hash(K) shuffle above the union output if it isn't already
   hash(K).** The output is hash(K) iff every leg arrived at the union
   as hash(K) on the same K — true when all legs were already
   conforming (B2) or all were wrapped in step 2 (B5: DISTINCT already
   hash(K) + kSingle wrapped in hash(K)). Otherwise (any unwrapped
   non-hash leg co-located in the union — typically a kSource scan), a
   shuffle above the union is required.

**Examples through the principle:**

| Example | Step 2 (wrap kSingles in hash) | Step 3 (shuffle above union) | New hash exchanges |
|---------|------|-------|-----|
| B1 (all scans)              | n/a  | Yes  | 1   |
| B2 (all kFixed already)     | n/a  | No   | 0 (just existing DISTINCT shuffles) |
| B3 (all kSingle, no parallel) | n/a (no wrapping needed) | Yes (single → hash output) | 1 |
| B4 (scan + kSingle)         | Yes (kSingle → hash) | Yes (scan rows still need shuffling) | 2 |
| B5 (kFixed already + kSingle) | Yes (kSingle → hash) | No (all data arrives as hash(K)) | 1 |
| B6 (scan + kFixed already)  | n/a  | Yes (scan rows still need shuffling) | 1 |
| B7 (scan + kFixed already + kSingle) | Yes (kSingle → hash) | Yes (scan rows still need shuffling) | 2 |

The cleanest cases (0 or 1 new shuffle) are B2 (everyone already
hash(K)), B5 (kSingle wrap is the only non-conforming thing). The
expensive cases (2 new shuffles) are when both a kSource scan and a
kSingle are present.

#### B1. All kSource — GROUP BY over two scans
```sql
SELECT k, COUNT(*) FROM (
    SELECT k FROM t_big UNION ALL SELECT k FROM t_small
) GROUP BY k
```
Legs: kSource + kSource. Neither produces hash(k).
**Optimal:** Apply A → both scans co-located in kSource fragment. Add
hash(k) shuffle above the union → output to a kFixed N consumer
fragment running the parent. **1 new hash exchange.**

#### B2. All kFixed N — GROUP BY over two already-hash(k) legs
```sql
SELECT k, COUNT(*) FROM (
    (SELECT DISTINCT k FROM t1) UNION ALL (SELECT DISTINCT k FROM t2)
) GROUP BY k
```
Legs: both kFixed N, both hash(k).
**Optimal:** Apply A → both co-located in kFixed N fragment via their
incoming hash exchanges. Output is already hash(k) (worker `i` has
partition `i` from each leg). No shuffle above the union; the parent
runs in the same fragment. **0 new exchanges** (DISTINCT's hashes are
pre-existing).

#### B3. All kSingle — GROUP BY over single-row legs
```sql
SELECT k, COUNT(*) FROM (
    VALUES (1), (2) UNION ALL SELECT COUNT(*) AS k FROM t_a
) GROUP BY k
```
Legs: all kSingle.
**Optimal:** Apply A → all co-located in kSingle. Add hash(k) shuffle
above the union → output to kFixed N consumer. **1 new hash exchange.**

#### B4. kSource + kSingle — GROUP BY over scan + single-row
```sql
SELECT k, COUNT(*) FROM (
    SELECT k FROM t_big UNION ALL VALUES (1)
) GROUP BY k
```
Legs: kSource + kSingle.
**Optimal:** Apply A with hash substitution → kSingle wrapped in
hash(k); scan stays in the kFixed N union fragment receiving the
kSingle's hash exchange. Union output isn't hash(k) (scan rows mixed
in), so add a hash(k) shuffle above. **2 new hash exchanges.**

#### B5. kFixed N + kSingle — GROUP BY over DISTINCT + single-row
```sql
SELECT k, COUNT(*) FROM (
    SELECT DISTINCT k FROM t_big UNION ALL VALUES (1)
) GROUP BY k
```
Legs: kFixed N (hash k) + kSingle.
**Optimal:** Apply A with hash substitution → kSingle wrapped in
hash(k); DISTINCT keeps its hash(k) as-is. Both feed the kFixed N
union fragment as hash exchanges; output is already hash(k). No
shuffle above. **1 new hash exchange** (the kSingle wrap).

#### B6. kSource + kFixed N — GROUP BY over scan + already-hash(k) leg
```sql
SELECT k, COUNT(*) FROM (
    SELECT k FROM t_big
    UNION ALL
    SELECT DISTINCT k FROM t2
) GROUP BY k
```
Legs: kSource + kFixed N (hash(k)).
**Optimal:** Apply A → scan + DISTINCT co-located in kFixed N. Output
mixes scan rows (not hash) with DISTINCT rows (hash) → not hash(k).
Add hash(k) shuffle above. **1 new hash exchange.**

#### B7. kSource + kFixed + kSingle — GROUP BY over all three types
```sql
SELECT k, COUNT(*) FROM (
    SELECT k FROM t_big
    UNION ALL SELECT DISTINCT k FROM t_other
    UNION ALL VALUES (1)
) GROUP BY k
```
Legs: kSource + kFixed N + kSingle.
**Optimal:** Apply A with hash substitution → kSingle wrapped in
hash(k); scan + DISTINCT co-located in kFixed N. Union output mixes
scan rows in, so add hash(k) shuffle above. **2 new hash exchanges.**

#### B variations

- **Different parent key (B1-style legs, parent wants hash(k1) instead
  of hash(k))**: existing leg partitioning is useless; each leg gets
  `Repartition(hash k1)`. Same shape as the corresponding base B
  example but with extra shuffles where leg-output partitioning was
  wasted.
- **Join instead of GROUP BY (B1-style legs, parent is a hash join
  probe on K)**: same plan shape as the corresponding base B example —
  the consumer fragment is the kFixed N join fragment, otherwise
  identical.

### C. Parent requires gather (single)

**General principle (derived from C1–C7):**

C's parent is kSingle, which is *compatible* with kSingle legs (no
multiplication problem when a kSingle leg sits in a kSingle host).
That changes A's wrapping logic for C: kSingle legs don't need
isolation; they belong in the kSingle final fragment directly.

1. **Parallel legs (kSource, kFixed N):** co-locate them per A's
   compatibility rules into one parallel sub-union fragment (kSource
   if all scans, kFixed N if any kFixed N or mixed). Add a gather
   exchange above this sub-union to feed the kSingle final fragment.
2. **kSingle legs:** co-locate directly in the kSingle final
   fragment — no wrapping, no exchange. They become local sources to
   the LocalPartition that merges the union.
3. **The FINAL operation** (ORDER BY / LIMIT / global agg) runs in
   the kSingle final fragment, alongside the LocalPartition that
   merges the gathered parallel data with the co-located kSingle legs.

**Examples through the principle:**

| Example | Parallel sub-union | kSingle legs co-located in final | New exchanges |
|---------|--------------------|----------------------------------|---------------|
| C1 (all scans)              | kSource (2 scans)            | —    | 1 gather |
| C2 (all kSingle)            | —                            | all  | 0 |
| C3 (all kFixed)             | kFixed N (2 DISTINCTs)       | —    | 1 gather |
| C4 (scan + kSingle)         | kSource (scan)               | yes  | 1 gather |
| C5 (kFixed + kSingle)       | kFixed N (DISTINCT)          | yes  | 1 gather |
| C6 (scan + kFixed)          | kFixed N (scan + DISTINCT)   | —    | 1 gather |
| C7 (all three)              | kFixed N (scan + DISTINCT)   | yes  | 1 gather |

Uniform: 1 new gather for any case with a parallel leg, 0 if all
legs are kSingle. C is genuinely the simplest of the three sections.

#### C1. All kSource — scans feeding ORDER BY
```sql
SELECT * FROM (
    SELECT a FROM t_big UNION ALL SELECT a FROM t_small
) ORDER BY a
```
Legs: kSource + kSource.
**Optimal:** Both scans co-located in kSource fragment per A1. Add a
gather above the union → kSingle final fragment hosts the ORDER BY.
**1 new gather exchange.**

#### C2. All kSingle — single-task legs feeding ORDER BY
```sql
SELECT * FROM (
    VALUES (1), (2) UNION ALL VALUES (3), (4)
) ORDER BY 1
```
Legs: all kSingle.
**Optimal:** All legs co-located in one kSingle fragment per A3. ORDER
BY runs in the same fragment. **No new exchanges.**

#### C3. All kFixed N — DISTINCTs feeding ORDER BY
```sql
SELECT * FROM (
    (SELECT DISTINCT a FROM t1) UNION ALL (SELECT DISTINCT a FROM t2)
) ORDER BY a
```
Legs: both kFixed N.
**Optimal:** Both DISTINCTs co-located in kFixed N fragment per A2.
Add a gather above the union → kSingle final fragment hosts the
ORDER BY. **1 new gather exchange** (plus the two DISTINCTs'
pre-existing hash exchanges).

#### C4. kSource + kSingle — scan + Values feeding ORDER BY
```sql
SELECT * FROM (
    SELECT a FROM t_big UNION ALL VALUES (1)
) ORDER BY a
```
Legs: kSource + kSingle.
**Optimal:** Scan in its own kSource fragment, gather above to kSingle
final fragment. Values co-located in the kSingle final fragment as a
local source. LocalPartition in the final fragment merges gathered
scan data with Values; ORDER BY runs there. **1 new gather exchange.**

#### C5. kFixed N + kSingle — DISTINCT + Values feeding ORDER BY
```sql
SELECT * FROM (
    SELECT DISTINCT a FROM t_big UNION ALL VALUES (1)
) ORDER BY a
```
DISTINCT in its own kFixed N fragment, gather above to kSingle final.
Values co-located in the kSingle final. **1 new gather exchange**
(plus DISTINCT's pre-existing hash).

#### C6. kSource + kFixed N — scan + DISTINCT feeding ORDER BY
```sql
SELECT * FROM (
    SELECT a FROM t_huge UNION ALL SELECT DISTINCT a FROM t_other
) ORDER BY a
```
Scan + DISTINCT co-located in one kFixed N fragment per A6. Gather
above to kSingle final hosting the ORDER BY. **1 new gather exchange.**

#### C7. kSource + kFixed + kSingle — all three feeding ORDER BY
```sql
SELECT * FROM (
    SELECT a FROM t_big
    UNION ALL SELECT DISTINCT a FROM t_other
    UNION ALL VALUES (1)
) ORDER BY a
```
Scan + DISTINCT co-located in kFixed N (parallel sub-union). Gather
above to kSingle final. Values co-located in the kSingle final. **1
new gather exchange.**

### D. UNION ALL as a hash join build

These cases reduce to A or B depending on the join strategy. They are
documented separately as concrete examples for join contexts.

#### D1. UNION ALL as build of a broadcast hash join — A-class
```sql
SELECT * FROM t JOIN (SELECT a FROM t1 UNION ALL SELECT a FROM t2) u
       ON t.a = u.a
-- where the union output is small enough to broadcast
```
Broadcast means a `PartitionedOutput[BROADCAST]` is added above the
union output, replicating it to all probe tasks. UNION ALL itself sees
no distribution requirement — the broadcast is a transformation the
parent applies above the union, regardless of what shape the union
output is. Plan shape follows A.

#### D2. UNION ALL as build of a shuffled hash join — B-class
```sql
SELECT * FROM t JOIN (SELECT a FROM t1 UNION ALL SELECT a FROM t2) u
       ON t.a = u.a
```
Both probe (`t`) and build (the union) get hash-partitioned on `a` for
the shuffled join. From UNION ALL's perspective, the parent (join
build) requires hash(a) — same as B-class queries. Plan shape follows
B1.

## Known gaps

The model above describes the implemented design. The known gaps below
all share one root cause: planners that sit above UNION ALL don't
communicate their distribution intent down to UNION ALL planning, and
UNION ALL planning doesn't recognize the union's output distribution
upward. Hash joins are an exception — they propagate the desired hash
distribution to the build via `forBuild`, which UNION ALL planning
honors. Plans below are correct but include unnecessary shuffles.

- **Aggregations and windows above UNION ALL.** Aggregation and window
  planning don't recognize that a UNION ALL output is already
  hash-partitioned on the grouping/partition keys, so an aggregation
  or window above the union adds a redundant local hash partition.
  Affects the case described by section B.
- **UNION DISTINCT.** Currently implemented as an inline single-step
  Aggregation on top of UNION ALL with a force-shuffle on every leg.
  The semantically equivalent UNION ALL + GROUP BY hits the same
  aggregation gap above. Once aggregation planning propagates
  distribution awareness, UNION DISTINCT can be lowered to UNION ALL +
  GROUP BY and both forms will produce identical plans.
- **ORDER BY / LIMIT / global aggregation above UNION ALL.** These
  planners don't propagate the gather preference to UNION ALL
  planning. UNION ALL falls back to A's rule and wraps kSingle legs in
  `Repartition(arbitrary)` into the parallel sub-union, instead of
  C's optimal layout (kSingle legs as local sources in the kSingle
  final fragment). Affects mixed-leg cases C4, C5, C7 — one extra
  arbitrary exchange per kSingle leg.
