# Apply-over-Aggregate Pushdown Rules

*Companion to `Decorrelate-outer-refs-design.md` (resolves outer
references) and `Decorrelate-design-rationale.md` (alternatives
considered and rejected).*

Iterative decorrelation peels one body operator per step. This doc
covers the case where that operator is an `Aggregate`. It defines
the lift shape for two Apply kinds:

- `kLeft` — correlated scalar / value subqueries
- `kLeftSemiProject` — correlated semi/anti-join projections

Each kind has four sub-cases along two axes — `Apply.filter` null vs
non-null, and `Agg.gby` empty vs non-empty — but the sub-cases share
a single shape per kind, parameterized by those axes.

## Relation to the literature — read before designing rules

The general framing here (Apply as a dependent join, iterative
pushdown, lifting Aggregate via per-`rn` grouping) follows
Neumann & Kemper's unnesting work (HyPer, Umbra, Neumann 2024). But
the **specific lifted shapes in those papers cannot be used here as
written**, because they assume an execution model we don't have.

Standard unnesting rewrites produce plans where L (the outer side)
appears on multiple paths — typically as input to the inner Apply
AND as the outer side of a LEFT JOIN that re-pads outers whose
groups all got filtered out. In HyPer/Umbra, this is safe because:

- The execution engine supports **DAG plans** (a single evaluated
  subtree can have multiple consumers), or
- There is a **materialization primitive** ("evaluate once, read N
  times") that the optimizer inserts.

Either way, L is evaluated once, and downstream operators read the
same rows.

**We have neither.** Plans are strict trees; there is no
materialization operator. Evaluating L twice would be unsafe for
two independent reasons:

- **`rn` mismatch.** `AssignUniqueId` is order-dependent — a second
  scan assigns different `rn` values, so the rejoin matches nothing.
- **Non-determinism.** L can contain `random()`, `now()`, `uuid()`,
  multi-source reads in unspecified order — two evaluations need not
  return identical rows.

So we can't borrow the paper's rewrites for any case that requires
re-referencing L (the multi-row-per-outer + F_post case being the
sharpest example). For those cases, we have to design rewrites that
keep L on a single path through the plan — even when the equivalent
academic shape is simpler.

This constraint shapes most of what follows. When a sub-case looks
unnaturally restricted or uses an in-place trick instead of the
"obvious" rejoin, that's why.

**Forward-looking note.** Even if execution later gains diamond
support — almost certainly via an explicit materialization operator
(the only viable mechanism in distributed execution) — the
tree-shape rewrites in this doc remain the preferred path.
Materialization is not free: memory pressure, pipeline break, extra
shuffle in distributed plans. The mature optimizer pattern (same as
CTE handling) is: use the tree-shape rewrite when it exists;
materialize only when the reuse savings exceed materialization cost.
So a future "rejoin with L via materialization" rule would be added
as a CBO option, not a replacement for the rules below.

### Aside — "why not just follow the paper?"

A common reaction reading the paper is: "the rewrites are clean,
just implement them." That works in HyPer/Umbra because those
systems are engineered around the assumption that **a DAG of
operators with a shared subtree is a small, absorbable cost**:

- Single-node main-memory execution.
- Compressed in-memory temp storage with vectorized re-reads.
- Intermediates in OLAP are often small post-filter / post-agg.

Outside that regime, the assumption breaks down quickly:

- **Pipelining.** A shared subtree forces materialization, which
  blocks downstream operators until the subtree completes. The
  "free" DAG costs latency and breaks streaming execution.
- **Memory pressure.** Materialized intermediates compete with
  hash tables, sorts, and other operators for memory.
- **Distributed execution.** DAGs become either broadcast
  (replicate to all consumers — only cheap for small data) or
  shuffle (network-heavy, often dominates query cost). What was a
  "small constant" in single-node becomes a first-class cost
  driver.

In Axiom's setting (distributed Velox execution, potentially large
intermediates), naively applying paper-style rewrites would produce
plans whose physical realization includes shuffles or broadcasts
that the original SQL didn't need. The paper isn't wrong — it
optimizes for a different execution regime. Following it without
adapting would trade correctness for plans that are slower than the
correlated original.

This is the subtle point: the paper's rewrites are **complete and
efficient for their target system class, but not portable as-is**.
A meaningful chunk of decorrelation engineering in any non-HyPer
system is the work of producing tree-friendly variants that don't
need DAG support, or producing DAG-aware variants that cost the
materialization correctly so the CBO can choose intelligently.

## Assumed model

- Apply is a "true" dependent Join. Body may reference L's columns
  and may produce N rows / N cols per outer; Apply's output is a
  Velox-Join-shaped result.
- Apply has two kinds: `kLeft` and `kLeftSemiProject`.
- Apply carries a `filter` field (the eventual Join's residual
  filter). During iteration, `Apply.filter` accumulates predicates as
  earlier Filter operators are peeled from body.
- Iteration peels one body operator per step. It terminates when
  body has no outer refs left. At terminus, Apply renames to Join.

## How to think about lifting Aggregate above Apply

### The mental model

Conceptually, `Apply over Agg(child, gby, aggs)` runs the Aggregate
**per outer row** — for each L row, evaluate `Agg` over the child rows
in scope for that outer.

The equivalent single-pass shape: **tag each L row with a unique id
`rn`, join L with child once, then aggregate grouped by `rn` (and any
original `gby`)**. Each `rn` group corresponds to one L row's worth
of child rows; aggregating within the group reproduces per-outer
execution.

**Picture 1 — `Apply.filter` is null (the plain case):**

```
- Agg                                   ← lifted from body
    groupingKeys = [rn, gby]            ← rn isolates each L row's data
    aggregates   = aggs
  - Apply (kLeft)
    - body  = child                     ← may still depend on L
    - input = AssignUniqueId(L) → tagged_L
```

One row per outer. Empty-input outers (no child rows) get padded by
`kLeft` → the lifted Agg's aggregates return NULL → need to be
restored to their empty-input value (count → 0). That happens in a
result Project (omitted here; see "the three primitives").

**Picture 2 — `Apply.filter` is non-null:**

`Apply.filter` predicates split into two classes:
- **F_pre** — references only pre-aggregation cols (L.cols, gby
  cols). Stays as `Apply.filter` below the lifted Agg — applied
  before aggregation, becomes the eventual Join.filter at terminus.
- **F_post** — references the aggregate result (or both pre- and
  post-aggregation cols). Can't apply as a Filter above the lifted
  Agg without losing outers (see "Why F_post folds into the
  Project's IF" below). Folds into the result Project as an `IF`
  wrap that nulls the aggregate values when the predicate fails.

```
- Project                               ← single Project; per agg col c:
    result_c = IF(F_post_subst, c_coal, NULL)
                                        ← c_coal := COALESCE(c, empty(c))
                                          (or just c when empty(c) is NULL)
                                          F_post_subst := F_post[c → c_coal]
  - Agg                                 ← lifted from body
      groupingKeys = [rn, gby]
      aggregates   = aggs FILTER (WHERE _include)   ← excludes kLeft pads
    - Apply (kLeft, filter = F_pre)     ← F_pre stays here
      - body  = Project(child, [..., _include := true])
                                        ← marks real rows; NULL on pad
      - input = AssignUniqueId(L) → tagged_L
```

Notation: `c_coal` is shorthand for the `COALESCE(c, empty(c))`
subexpression — used in prose and substitution rules below but not
materialized as a separate column. `F_post_subst` is `F_post` with
every reference to an aggregate column `c` rewritten to `c_coal`, so
`c_coal` appears in both the IF condition and the IF's THEN branch.

The rest of the doc fills in the details: the next subsection
shows WHY the COALESCE wrap is needed (without it, F_post on
NULL-sensitive predicates breaks); "the three primitives" defines
the COALESCE / `_include` / L.cols-carry-through mechanisms; Rule A
spells out the full schema with all axes.

### Examples (for the non-obvious cases)

Picture 1 covers the plain case (e.g.,
`SELECT (SELECT count(*) FROM u WHERE u.a = t.a) FROM t`): no
`Apply.filter`, one row per outer, done. The examples below are
specific SQL queries that hit Picture 2 — `Apply.filter` non-null —
useful to anchor the F_pre / F_post mechanics.

**Example 1 — HAVING plus correlated WHERE:**

```sql
SELECT (SELECT count(*) FROM u WHERE u.a = t.a HAVING count(*) > 5) FROM t
```

`Apply.filter = (count_result > 5)` — F_post (references the agg
result).

For each `t` row: count `u` rows where `u.a = t.a`; if count > 5,
return count, else NULL.

Two valid decorrelated shapes:

- **(a) Aggregate-below-join:** group `u` by `u.a` once, then LEFT
  JOIN with `t` on the key. Efficient when `u` has duplicates per
  `u.a` or many `t` rows share `t.a`.
- **(b) Aggregate-above-join:** tag `t` with `rn`, join `t × u`
  filtered by the correlation, aggregate per `rn`. Efficient when
  the join is selective or the aggregate doesn't reduce cardinality.

Both produce the same answer; choice between them is a cost
decision.

**Example 2 — HAVING-only correlation (motivates the F_post tweak):**

```sql
SELECT (SELECT count(*) FROM u HAVING count(*) > t.a) FROM t
```

`Apply.filter = (count_result > t.a)` — F_post (references both the
agg result and an outer column).

The aggregate is independent of `t`; the correlation lives entirely
in HAVING. This is the case the next subsection picks apart.

### Why F_post evaluates on `c_coal`, not raw `c`

One concrete failure case shows why F_post in Picture 2 references
`c_coal` (the `COALESCE(c, empty(c))` subexpression) and not the
raw aggregate column `c`.

Take this query:

```sql
SELECT (SELECT count(*) FROM u WHERE u.a = t.a HAVING count(*) = 0) FROM t
```

For a `t` row with no matching `u` rows (`u.a = t.a` returns 0
rows), the original semantics:
- Per-outer Agg over 0 rows: `count(*) = 0` (count's empty-input value).
- HAVING: `0 = 0` → true → return 0.

Now lift the Agg above Apply. The 0-match outer hits a `kLeft` pad
→ `_include = NULL` → `FILTER (WHERE _include)` excludes the row
→ `count(*)` over an empty group returns NULL (not 0).

So the lifted Agg's raw `c = NULL` for this outer. If F_post used
`c` directly, the HAVING would be `NULL = 0` → NULL → false → the
result would be NULL. Wrong: original was 0.

The COALESCE wrap restores the empty-input value before F_post
sees it: `c_coal = COALESCE(c, 0) = 0` for the 0-match outer. The
Project's IF then evaluates `IF(c_coal = 0, c_coal, NULL) =
IF(0 = 0, 0, NULL) = 0`. ✓

The general rule: any aggregate with a non-NULL empty-input value
(count, count_if, …) needs F_post to see `c_coal` so NULL-sensitive
predicates (`= 0`, `IS NULL`, `COALESCE(c, -1) < 0`, …) match the
original semantics.

### Shape (a) for equi correlation; shape (b) otherwise

`kLeft` Aggregate peel emits **shape (a)** — aggregate the body once
grouped by the correlation key, then LEFT-join the result back to the
outer (no `AssignUniqueId`) — when the correlation lifts cleanly to
equi keys and the aggregates don't reference the outer (`aggregatePeel`
→ `aggregatePeelEqui` in `DecorrelatePass.cpp`). Otherwise it falls back to
**shape (b)**, the AssignUniqueId per-outer-`rn` form the rest of this
doc describes.

No cardinality estimate is needed for the rewrite itself. Whether the
body *scan* is restricted depends on the join-back's build/probe
orientation, because a dynamic filter only reaches the **probe** side:

- When the cost model builds the **outer** (the smaller side) and probes
  the body aggregate, the DF on the correlation key (the aggregate's
  grouping key) propagates through the `HashAggregation` to the body's
  `TableScan`, restricting it to the outer's keys — q2/q17, whose outers
  are selective (see the dynamic-filter note in
  `TpchV1V2PlanComparison.md`).
- When the **outer** is the larger side, the cost model builds the body
  aggregate, which must scan and aggregate the whole body first; the DF
  then restricts the outer, not the body. To restrict the body here —
  especially when the outer has many rows but few distinct correlation
  keys — an explicit reducing semijoin (push the outer's distinct keys
  into the body before aggregating) is still needed. That reducer is
  deferred; today shape (a) relies on DF, which covers the selective-outer
  case.

Shape (b) remains for non-equi correlation (`u.a > t.a`) and bodies where
the correlation can't be lifted to equi keys above a correlation-free
subtree.

## The three primitives

Every per-case rule below composes these three mechanisms. Defined
once here; the rules reference them.

**1. L.cols carry-through.** The lifted Aggregate's output naturally
contains `rn`, `gby`, and aggregate result columns — but not L's
other columns. For each `L.col`, add an `arbitrary(L.col)` aggregate
whose output column REUSES the original L.col's `Column*`. Since
L.col is constant within each `rn` group, `arbitrary` returns that
value; downstream refs to L.col resolve unchanged.

**2. `_include` marker for outer preservation.** With `kLeft` Apply,
outers whose child produces 0 rows are padded with NULL — those
padded rows would otherwise contribute to the aggregate (e.g.,
`count(*) = 1` instead of `0`). Project `_include := true` on real
body rows; LEFT JOIN padding turns it to NULL. Apply
`FILTER (WHERE _include)` to every aggregate so padded rows are
excluded.

**3. Empty-input value reconstruction.** Per-outer execution of
`count(*)` over 0 rows produces `0`, not NULL. After `_include`
filtering, the aggregate returns NULL for padded outers. The result
Project wraps in `COALESCE(agg, empty(agg))` for aggregates whose
empty-input value is non-NULL (count, count_if, etc., per
`FunctionRegistry::aggregateEmptyResultResolver`). Other aggregates
(sum, max, etc.) leave NULL alone — that's their correct empty-input
value.

## Apply.filter classification: F_pre vs F_post

When `Apply.filter` is non-null, each predicate falls into one of two
classes:

| Class | References | Placement |
|-------|-----------|-----------|
| **F_pre** | L.cols, gby cols (Aggregate's grouping-key outputs), `_include` | Stays on inner Apply's filter (becomes Join.filter at terminus) — applied before aggregation |
| **F_post** | Post-aggregation cols (`gby`, agg results), OR mixed | Folded into the result Project as `IF(F_post_subst, c_coal, NULL)` — applied after aggregation |

Above the lifted `Agg` sits a single Project that both restores
empty-input values and applies the F_post IF wrap:

```
- Project           ← per agg col c:
                       IF(F_post_subst, COALESCE(c, empty(c)), NULL)
                       (the COALESCE is omitted when empty(c) is NULL)
  - Agg (lifted, grouped by rn[, gby])
    - Apply (kLeft, filter = F_pre)
      ...
```

**Why F_post folds into the Project's IF instead of becoming a
Filter node.** In the original tree, F_post was `Apply (kLeft).filter`
sitting above a per-outer Aggregate that produces exactly one row
per outer. If F_post failed, `kLeft` would pad the dropped row back
as NULL — so the outer is always preserved, just with NULL values.

After lifting Aggregate above Apply, the lifted Aggregate produces
one row per outer (via `rn` grouping). There is no `kLeft` above it
to pad. Putting F_post as a Filter node here would drop the row
outright, losing the outer. The IF-in-Project shape reproduces the
original behavior exactly: the row stays in place, and when F_post
fails the per-aggregate values become NULL.

Mixed predicates (e.g., `agg_result > L.col + 5`) count as F_post.
L.cols remain accessible in the result Project via the carry-through
primitive.

## Cases covered

Three axes pick the rule:
- **Apply.kind** — `kLeft` (scalar correlated) or `kLeftSemiProject`
  (EXISTS / IN).
- **`Agg.gby`** — empty (global aggregate) or non-empty.
- **IN-pair** (`Apply.inLhs` / `Apply.inBodyKey`) — only meaningful
  for `kLeftSemiProject`; null means EXISTS-shape, non-null means
  IN-shape with the equi pair `eq(inLhs, inBodyKey)` and `nullAware`
  semantics applied at terminus.

`Apply.filter` (null or non-null; F_pre / F_post split when non-null)
parameterizes within each rule's body, not across rules.

| # | Apply.kind | `Agg.gby` | IN-pair | Rule |
|---|------------|-----------|---------|------|
| 1 | `kLeft` | empty | n/a | A |
| 2 | `kLeft` | non-empty | n/a | A ¹ |
| 3 | `kLeftSemiProject` | empty | null (EXISTS) | **B-EXISTS Path 1** |
| 4 | `kLeftSemiProject` | empty | non-null (IN) | **B-IN Path 1** |
| 5 | `kLeftSemiProject` | non-empty | null (EXISTS) | **B-EXISTS Path 2** |
| 6 | `kLeftSemiProject` | non-empty | non-null (IN) | **B-IN Path 2** |

¹ NYI when `Apply.filter` is non-null (correlated post-aggregate
predicate over a multi-row gby). Design open between window-based
per-rn collapse vs translate-side restriction; see
`Decorrelate-design-rationale.md`.

NYI cases throw at planning time; they don't silently produce wrong
results.

Rule body discussions below refer to the original sub-case
enumeration that splits each row above by `Apply.filter` and (for
Rule B) by EXISTS / IN. The mapping:

- Rule A → sub-cases 1 (filter null, empty gby), 2 (null, non-empty),
  3 (non-null, empty), 4 (non-null, non-empty — NYI).
- Rule B-EXISTS Path 1 → 5a (filter null), 7a (non-null).
- Rule B-EXISTS Path 2 → 6a (null), 8a (non-null).
- Rule B-IN Path 1 → 5b (null), 7b (non-null).
- Rule B-IN Path 2 → 6b (null), 8b (non-null).

**NYI error message format** (deferred cases):

```
VELOX_NYI("Decorrelate Case <N> (<kind>, Apply.filter <null|non-null>, "
          "Agg.gby <empty|non-empty>, IN-pair <null|non-null>) is not "
          "yet implemented");
```

The check fires inside the Aggregate peel before any IR is constructed.

## Rule A — `kLeft`

**Before:**

```
- Apply (kLeft, filter = F)              // F may be null
  - body = Agg(child, gby, aggs)          // gby may be empty
  - input = L
```

**After:**

```
- Project:                              // single Project; per output col:
    for each agg output c:
      c_coal       = COALESCE(c, empty(c))   if empty(c) is non-NULL
                     = c                     otherwise
      F_post_subst = F_post[c → c_coal]
      result_c     = IF(F_post_subst, c_coal, NULL)   if F_post applicable
                     = c_coal                         otherwise
    other cols: pass-through
  - Agg
      groupingKeys = [rn] ++ gby
      aggregates   = aggs with FILTER (WHERE _include)
                  ++ arbitrary(L.col) for each L.col
    - Apply (kLeft, filter = F_pre)          // F_pre may be empty
      - body  = Project(child, [..., _include := true])
      - input = AssignUniqueId(L) → tagged_L
```

Where:

- `F_pre`, `F_post` come from splitting `F` per the classification
  above. When `F` is null, both are empty (the IF wrap drops out;
  the COALESCE wrap still applies).
- `F_post_subst` is `F_post` with each aggregate column ref `c`
  rewritten to `c_coal`. See "Why F_post must evaluate on the
  COALESCE'd value" for why.
- `gby` adds zero or more grouping keys; an outer with 0 child rows
  produces a `(rn, NULL_gby)` pad-row → `_include` filter excludes
  → aggregates NULL → COALESCE restores empty-input values → exactly
  one output row per outer.
- Result Project's IF wrap applies per-aggregate; gby columns pass
  through unchanged (NULL on padded outers, per original `kLeft`
  semantics).

## Rule B — `kLeftSemiProject`

Direction: **`bool_or` recovery** (lifted Aggregate emits
`bool_or(_include)` or `bool_or(combined) FILTER(_include)`; mark IS
the aggregate output, wrapped in `COALESCE(..., false)` for
non-null-aware shapes). Rejected alternatives: a new counting join
kind, and the v1-style `NOT(count == 0)` pattern — both add substrate
or wrappers for no semantic gain.

Recovery only fires when the body Aggregate's user aggregates are
genuinely referenced past the kSemi mark. The aggregate-elision
early-out below catches the more common DISTINCT-shaped bodies
before they reach the per-IN/EXISTS rules.

### Aggregate-elision early-out

Fires at the entry of `aggregatePeelSemi`, before the IN/EXISTS
dispatch. Preconditions:

- `aggregate->groupingKeys()` non-empty (scalar Aggregate is
  Case 5a's fast-path, handled separately — `Aggregate` over an
  empty input emits one row, so dropping it would change EXISTS
  semantics)
- `accumulatedFilter` does not reference any of
  `aggregate->aggregates()` outputs
- `inBodyKey` (for IN) does not reference any of
  `aggregate->aggregates()` outputs

When these hold, the body Aggregate is a pure DISTINCT — kSemi
already dedupes its body, so drop the Aggregate and rewrite a new
`Apply(kLeftSemiProject, body=aggregate->input(), ...)` with
`accumulatedFilter` and `inBodyKey` substituted through the
grouping-key output → grouping-key expression map. The outer driver
then takes the simpler kSemi join-peel path, which the executor
runs natively as `LEFT SEMI (PROJECT)` — no AssignUniqueId, no
two-stage Aggregate, no `bool_or` recovery.

This subsumes Cases 6a (EXISTS, no HAVING) and the IN analogue
where `inBodyKey` is a grouping-key expression with no HAVING, and
also handles HAVING-over-grouping-keys (the HAVING substitutes
through unchanged and rides along as the new Apply's filter, to
be pushed down on the next iteration). The recovery paths below
remain in play for the genuinely-need-the-aggregate cases (Cases
7a/8a with HAVING referencing a user agg, IN where `inBodyKey`
references a user agg, null-aware IN with the equi-pair on an
aggregate output).

## Rule B-EXISTS — `kLeftSemiProject` without IN equi-pair (`Apply.inLhs == nullptr`)

Covers Cases 5a/6a/7a/8a. Mirrors Rule B-IN's structure with the
equi-pair half of `combined` removed and the mark CASE reduced to a
two-state form (no null-aware tri-state — EXISTS has no NULL source).

  combined := COALESCE(F_post_subst, false)   if F_post present
              TRUE                              otherwise

### Path 1 — empty gby (cases 5a, 7a): single aggregate + inline mark

Stage-1 Aggregate per `[rn]` produces one row per outer. Mark =
`combined` directly.

```
Aggregate:
  gby  = [rn]
  aggs = body's user aggs FILTER(_include)
       ++ arbitrary(L.col) for each outer col

Final Project:
  mark := combined
       (= TRUE for 5a; = COALESCE(F_post_subst, false) for 7a)
```

For 5a: mark = TRUE for every outer (matches strict SQL — scalar agg
always emits one row → EXISTS sees one row → TRUE).
For 7a (pad-only outer): user aggs FILTER(_include) evaluate over the
empty set, so `F_post_subst` runs against the SQL "agg over empty"
values (NULL for max, 0 for count) and mark reflects that.

### Path 2 — non-empty gby (case 8a): two aggregates

Case 6a (no F_post, non-empty gby) is caught by the aggregate-elision
early-out above; this path serves the F_post-present case only.

Stage 1 per `[rn, gby...]`; Stage 1.5 Project computes `combined`
per group; Stage 2 per `[rn]` collapses across groups.

```
Stage 1 Aggregate (lifted body):
  gby  = [rn, gby...]
  aggs = body's user aggs FILTER(_include)
       ++ arbitrary(_include)        -- pad marker for Stage 2 FILTER
       ++ arbitrary(L.col)

Stage 1.5 Project (per-group combined bit):
  combined := TRUE | COALESCE(F_post_subst, false)

Stage 2 Aggregate (mark recovery):
  gby  = [rn]
  aggs:
    bool_or(combined) FILTER(_include) AS has_true
    arbitrary(L.col)

Final Project:
  mark := COALESCE(has_true, false)
```

For 6a (no F_post): `combined = TRUE`. `bool_or(TRUE) FILTER(_include)`
returns TRUE if any real group exists, NULL if pad-only outer.
COALESCE → FALSE for pad-only — matches strict SQL "gby on empty
input → 0 groups → FALSE".
For 8a (with F_post): mark = TRUE iff any real group passes HAVING.

### Invariants (both paths)

- Same `_include`-discrimination invariant as B-IN: the FILTER on
  Stage 2's bool_or discards the synthetic pad-only group.
- F_post coalesces to **false** — a HAVING that fails or returns NULL
  on a real row drops that row's contribution to existence.
- No `nullAware` consideration — EXISTS has no equi-pair to source
  NULL from. Mark is unconditionally two-state.

## Rule B-IN — `kLeftSemiProject` with IN equi-pair (`Apply.inLhs` / `Apply.inBodyKey` non-null)

Covers cases 5b/6b/7b/8b uniformly. Two paths, branched on
`aggregate->groupingKeys().empty()`. Both build
the same `combined` per body row:

  combined := [if F_post present: COALESCE(F_post_subst, false) AND]
              eq(inLhs, inBodyKey_subst)

F_post coalesces to **false**: a HAVING that fails or returns NULL on
a body row means the row does not contribute to the IN match.

### Path 1 — empty gby (cases 5b, 7b): single aggregate + inline mark

Stage-1 Aggregate per `[rn]` produces exactly one row per outer.
Mark derived in the final Project via a NULL-aware CASE — no Stage 2.

```
Aggregate:
  gby  = [rn]
  aggs = body's user aggs FILTER(_include)
       ++ arbitrary(_include)        -- pad marker
       ++ arbitrary(L.col)
       ++ arbitrary(inLhs)           [if outer]

Final Project:
  mark := CASE
    WHEN _include IS NULL THEN false   -- empty body → IN = false
    WHEN combined         THEN true
    WHEN combined IS NULL THEN NULL    -- null-aware NULL state
    ELSE                       false
  END
```

### Path 2 — non-empty gby (case 8b, and 6b when `inBodyKey` references a user aggregate): two aggregates

When `inBodyKey` is purely a grouping-key expression and no HAVING is
present, Case 6b is caught by the aggregate-elision early-out above
(IN naturally dedupes its right-hand side). This path serves the
cases where the user aggregates are genuinely referenced —
typically `inBodyKey = max(...)` / `min(...)` / etc., or any
HAVING that references aggregate outputs.

Stage 1 per `[rn, gby...]`; Stage 2 per `[rn]` collapses across gby
groups using two `bool_or`s to preserve null-awareness.

```
Stage 1 Aggregate (lifted body):
  gby  = [rn, gby...]
  aggs = body's user aggs FILTER(_include)
       ++ arbitrary(_include)
       ++ arbitrary(L.col)
       ++ arbitrary(inLhs)

Stage 1.5 Project (per-group combined bit):
  combined := COALESCE(F_post_subst, false) AND eq(inLhs, inBodyKey_subst)

Stage 2 Aggregate (mark recovery):
  gby  = [rn]
  aggs:
    bool_or(combined)          FILTER(_include) AS has_true
    bool_or(combined IS NULL)  FILTER(_include) AS has_null
    arbitrary(L.col), arbitrary(inLhs)

Final Project:
  mark := CASE
    WHEN COALESCE(has_true, false) THEN true
    WHEN has_null                  THEN NULL
    ELSE                                false
  END
```

### Invariants (both paths)

- **`inBodyKey_subst`** uses Case 3's `c → c_coal` substitution when
  `inBodyKey` references a COALESCE-needing aggregate. Same mechanism
  as F_post; not a special case.
- **`FILTER(_include)` on Stage 2 `bool_or`s is load-bearing.** Without
  it, an unmatched outer's pad-row group can synthesize
  `combined = NULL` and emit a spurious `mark = NULL` instead of `false`.
- **Pad-marker carry** (`arbitrary(_include)` in Stage 1): for a real
  `(rn, gby)` group it yields `true`; for a pad-only group it yields
  `NULL`. Stage 2's FILTER consumes this signal.
- **`nullAware = false`** (non-null-aware IN): wrap final mark with
  `COALESCE(mark, false)`. With `nullAware = true`, emit the tri-state
  mark as-is.
- **Case 4-equivalent restriction does NOT apply here.** F_post does
  not need IF-in-Project form because the mark CASE consumes F_post
  directly. F_post + non-empty gby is fine on Path 2.

### Why `inLhs` / `inBodyKey` stay distinct from `Apply.filter`

An earlier framing considered folding the IN equi `eq(inLhs,
inBodyKey)` into `Apply.filter` so the F_pre / F_post split would
handle it uniformly. That doesn't work because **`nullAware` applies
only to the IN equi pair**, not to other filter conjuncts:

- `eq(inLhs, inBodyKey)` → NULL contributes to `mark = NULL` (SQL
  three-valued IN logic).
- Other filter conjuncts → NULL filters the row out (`mark`
  contribution = false), not NULL.

If both lived in `filter`, terminus would need to distinguish "the IN
equi conjunct" from "other conjuncts" — pattern-matching on
`eq(outer_col, body_col)` shape doesn't work because correlated WHERE
predicates have the same shape. The pair MUST stay a distinct field on
Apply to preserve nullAware semantics down to terminus (where it
lowers to `Join.leftKeys` / `Join.rightKeys` + `Join.nullAware`).

## Known design gaps

### Other body operators

The agg rules doc only addresses `Aggregate` in body. Body operators
**Window, Limit, Sort, Join (inside body), UnionAll, Unnest, nested
Apply** are not designed here. Each would need its own peel rule.
Currently NYI'd loudly at the driver.
