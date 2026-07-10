# Decorrelate Design Rationale

*Companion to `Decorrelate-agg-rules.md` and
`Decorrelate-outer-refs-design.md`. Captures alternatives we
considered, paths we rejected, and why. Half the value of this doc
is preserving dead-ends so future readers don't re-walk them.*

The rules docs say WHAT the decorrelate pass does. This doc says
WHY each choice was made — including the rejected alternatives that
look attractive on first read.

## Why we can't just follow the paper

Standard reference: Neumann & Kemper "Unnesting Arbitrary Queries"
(2015) and HyPer / Umbra follow-ups, including Neumann 2024.

The paper's framework (Apply as dependent join, iterative pushdown,
lifting Aggregate via per-`rn` grouping) is sound and we use it. But
the **specific lifted shapes** in the paper aren't usable as written
in our setting. Three independent reasons, in order of severity:

**1. Correctness.** The standard shapes (e.g., LEFT-JOIN-back to L
to re-pad outers whose groups all got filtered) require L to appear
on multiple paths in the plan. We don't have shared scans or a
materialization operator. Re-evaluating L is unsafe:

- **`rn` mismatch.** `AssignUniqueId` is order-dependent; a second
  scan produces different `rn` values; the LEFT JOIN on `rn` would
  match nothing. Every outer would get padded → wrong results.
- **Non-determinism.** L can contain `random()`, `now()`, `uuid()`,
  multi-source reads in unspecified order. Two evaluations need not
  return identical rows.

This is a correctness blocker, not a performance concern.

**2. Distributed-execution cost.** Even with future DAG / materialization
support, the paper's shapes assume shared subtrees are nearly free
— true in HyPer/Umbra (single-node main-memory, compressed in-memory
temp storage), false in distributed execution where DAGs become
broadcast (only cheap for small data) or shuffle (often dominates
query cost).

**3. Per-shape inefficiency.** Even within the paper's target system
class, the canonical rewrites are a baseline. Per-shape shortcuts
(e.g., our IF-in-outer-Project for empty `gby`) can avoid the
materialization entirely. The paper doesn't enumerate these.

So: the framework is portable, the shapes are not. Most of the rules
in `Decorrelate-agg-rules.md` are tree-friendly variants invented to
satisfy reason (1), with awareness of (2) and (3).

## Shapes considered for Apply over Aggregate

### LEFT-JOIN-back to L — rejected (correctness)

The textbook shape: filter the lifted Agg's output with F_post,
then LEFT JOIN to L on `rn` to re-pad outers whose groups all got
dropped. Rejected because it requires re-evaluating L (see
correctness reason above).

Would become viable if/when we add a materialization operator. Even
then, it would be a CBO option, not a default — see "Forward-looking
note" in the rules doc.

### IF-in-outer-Project — kept, scope-limited to empty `gby`

For empty `gby` (global Agg, exactly one row per outer), the
LEFT-JOIN-back rejoin isn't needed: the outer Project wraps each
aggregate column as `IF(F_post_subst, c_coal, NULL)`, which keeps
the row in place and nulls the values when F_post fails. This
matches the original `kLeft` semantics (row preserved, body cols
NULL) exactly because there's always exactly one row per outer to
operate on.

**Breaks for non-empty `gby`.** The lifted Agg now produces N rows
per outer (one per gby group). IF-in-Project would emit all N rows
with selective NULLs — but the original semantics drops failing
rows and pads ONCE if all fail. Wrong row counts.

So this shape stays, but Rule A applies it only to sub-cases 1 and
3 (empty `gby`).

### Window-based per-rn collapse — open candidate for non-empty `gby`

Sketch: tag each row from the lifted Agg with
`survives = F_post AND _include`. Compute
`any_survives = MAX(survives) OVER (PARTITION BY rn)`. Then in the
outer Project:

- Rows with `survives = true` → emit aggregate values.
- For outers with `any_survives = false` → emit one NULL-padded
  row per `rn` (need a tiebreaker to pick exactly one row from each
  failing rn group).

No L re-scan. Costs: a window pass over the lifted Agg output, plus
the tiebreaker logic.

Open issues:

- Exact tiebreaker. Simplest: `ROW_NUMBER() OVER (PARTITION BY rn
  ORDER BY (anything stable))` and keep rn=1. Needs verification
  that this composes cleanly with the outer Project.
- Whether to fold this into Rule A as the canonical non-empty-gby
  variant, or split into a separate rule. Probably separate — the
  shape diverges enough that conflating reads poorly.

### Translate-side restriction — open alternative for non-empty `gby`

Sketch: never let F_post on multi-row gby reach `Apply.filter`.
Translate handles HAVING-with-gby specially: instead of lifting it
as a Filter that the Filter-peel rule will absorb into
`Apply.filter`, keep it as a Filter **under** the Apply (above the
Aggregate but inside body). The Filter-peel rule's preconditions
prevent it from lifting this Filter when gby is non-empty.

Pros: keeps Rule A simple — only the empty-gby case needs IF-in-Project.

Cons: more bookkeeping at translate; the Filter-peel rule needs a
case split; coupling between SQL translation and rule preconditions
that has to be maintained.

Decision: deferred until we have the window-based variant sketched
end-to-end and can compare.

### Per-rn re-aggregate — rejected

After F_post Filter drops failing rows, run another Agg grouped by
`rn` to detect outers with zero survivors. Problem: this collapses
N rows per outer into 1, losing gby differentiation. Only correct
for empty gby — in which case we already have IF-in-Project, which
is cheaper.

## kLeftSemiProject mark derivation — three candidate directions

(Detailed alternatives behind Rule B's placeholder status in the
rules doc.)

### Direction 1 — New `kCountingLeftSemiProject` Apply/Join kind

Add a new Apply kind that emits each L row with a count of matching
body rows. The outer Project computes `mark = (count > 0)`.

Pros: clean separation between substrate and rule; mark derivation
becomes trivial; reusable for COUNT-style anti-join patterns too.

Cons: substrate addition — new Apply kind, new corresponding Join
kind, execution support, costing. Non-trivial.

### Direction 2 — `count` + `NOT(count == 0)` pattern

Lifted Aggregate emits per-outer `count(_include)`. Outer Project
derives `mark = NOT(count == 0)`. No new substrate.

Pros: works with existing kinds; minimal change.

Cons: introduces a count where a boolean would suffice — slightly
wasteful; doesn't generalize as cleanly to mixed semi/anti shapes.

### Direction 3 — `bool_or` pattern (matches current v2 substrate)

Lifted Aggregate emits per-outer `bool_or(_include)`. The aggregate
output IS the mark — no separate derivation. Uses
`AggregateRecovery::makeBoolOr` already present in v2.

Pros: matches existing v2 substrate exactly; no new aggregate
needed; mark is the aggregate's natural output.

Cons: `bool_or` semantics with `_include` (where NULL means "kLeft
pad, not a real match") needs care — `bool_or(NULL, NULL) = NULL`,
which must be coerced to `false` in the outer Project.

### Worked-out shapes for Cases 5 / 6 (filter null), assuming Direction 3

The lifted shape splits along the gby axis. v1 raises `NYI` on this
entire pattern ("Outer-column reference in the aggregate body of an IN
subquery is not supported yet"), so v2 is genuinely new territory
here — no v1 baseline to
match, just SQL correctness.

**Case 5 — gby empty.** Lifted Agg always produces exactly one row
per outer; predicate runs once per outer; mark is the predicate
result directly. One Agg over Apply.

```
- Project: mark = predicate(L.cols, agg_output)
  - Agg
      groupingKeys = [rn]
      aggregates   = aggs FILTER (WHERE _include)
                  ++ arbitrary(L.col) for each L.col
    - Apply (kLeft)
      - body  = Project(child, [..., _include := true])
      - input = AssignUniqueId(L) → tagged_L
```

**Case 6 — gby non-empty.** Lifted Agg produces N rows per outer
(one per gby group). Predicate runs per row giving N booleans per
outer; need to OR them into one mark per outer → extra reduction.
Two Aggs over Apply.

```
- Project: mark = collapsed_mark
  - Agg                                   ← per-outer reduction
      groupingKeys = [rn]
      aggregates   = bool_or(per_group_mark) AS collapsed_mark
                  ++ arbitrary(L.col) for each L.col
    - Project: per_group_mark = predicate(L.cols, agg_output)
      - Agg                               ← per-(rn, gby), like Rule A
          groupingKeys = [rn, gby]
          aggregates   = aggs FILTER (WHERE _include)
                      ++ arbitrary(L.col)
        - Apply (kLeft)
          - body  = Project(child, [..., _include := true])
          - input = tagged_L
```

(Case 5 = Case 6 with the outer Agg collapsed to identity — could
be a single rule that the optimizer simplifies when gby is empty.)

### Streaming / shuffle properties of Case 6's two-Agg shape

The Agg-over-Agg-over-Apply shape streams cleanly under common
physical plans:

- **Inner Agg (`[rn, gby]`).** `AssignUniqueId(L)` assigns rn
  monotonically as L is scanned, so rows arrive in rn order. Within
  each rn, gby values are unordered, but the aggregate only needs
  state for the *current* rn — flush when rn changes. State size =
  max distinct gby values per outer (typically small), not total
  cardinality.
- **Outer Agg (`[rn]`).** Inner Agg emits in rn order, so outer Agg
  holds one `bool_or` accumulator per current rn and flushes at the
  rn boundary. Fully streaming, constant state per outer.

**No shuffle in the common case.** Both Aggs stream as long as L
stays partitioned the way `AssignUniqueId` produced it (rn
contiguous within each partition). The Apply itself doesn't break
rn locality if body's child is broadcast or partition-local.

**Shuffle risk.** If body's child must be shuffled by some key (e.g.,
equi-correlation optimization promoting body to a partitioned
side), rn locality breaks and the inner Agg needs a re-sort or
hash-agg on rn first — costing a shuffle. The shape itself doesn't
force this; physical join planning does.

For the canonical SQL example
(`SELECT t.id FROM t WHERE t.x IN (SELECT max(u.b + t.a) FROM u)`),
the body is a full scan of `u` with no equi-correlation, so the
natural physical plan is `u` broadcast (or replicated) per L
partition → no shuffle, both Aggs stream.

### Cases 7 / 8 (filter non-null) — extension sketch

Fold F_post into the per-(rn, gby) mark predicate: replace
`per_group_mark = predicate(...)` with
`per_group_mark = predicate(...) AND F_post_subst`. The outer
`bool_or` reduction is unchanged. Same streaming properties as
Cases 5/6.

(F_post here references the lifted Agg's outputs, so the same
`c_coal` substitution Rule A uses applies — needs an additional
inner-Project of COALESCE before the per-group mark Project. Not
yet fully sketched.)

### Aggregate-elision early-out (subsumes Cases 5a/6a and the
DISTINCT-shaped IN analogue)

Layered on top of Direction 3. When the body Aggregate's user
aggregates are not referenced by anything that survives the kSemi
mark — i.e. neither the accumulated filter nor `inBodyKey` touches
an aggregate-result column — the Aggregate is a pure DISTINCT and
the kSemi mark already gives one bit per outer regardless. Dropping
the Aggregate and recursing on its input lets the simpler kSemi
join-peel path produce a native `LEFT SEMI (PROJECT)`, with no
AssignUniqueId / two-stage Aggregate / `bool_or` recovery.

This isn't a new direction — it's a precondition check that picks
between "no recovery needed" and "Direction 3 recovery". The
recovery shapes stay valid for the cases that genuinely need the
user aggregates; the early-out just keeps them off the common path.

Worked-out example. Before: `EXISTS (SELECT 1 FROM u WHERE u.a > t.b
GROUP BY u.x)` lifted as AssignUniqueId → NLJ(kLeft) → Aggregate per
[rn] with `bool_or(_include)`. After: NLJ(kLeftSemiProject) directly
over `TableScan(u)` with the gt as join condition. Identical results,
simpler plan, native executor support.

## Decisions made

- **Decorrelate emits Shape (b) only** (aggregate-above-join with
  `rn` grouping). Shape (a) (aggregate-below-join) is a downstream
  rewrite the join-order pass applies when cost-justified. Three
  reasons: (1) shape (b) works for both equi and non-equi
  correlation; (2) choosing between (a) and (b) needs cardinality
  estimates decorrelation doesn't have; (3) one canonical shape
  keeps per-case rules simple. Detail in
  `Decorrelate-agg-rules.md` §"Shape (a) for equi correlation; shape
  (b) otherwise".
- **F_pre vs F_post classification happens at rule application,
  not at translate.** Translate doesn't know which axes the
  surrounding Apply will hit; classification is local to the rule.
- **No `Apply (kInner)` at all.** Translate lowers uncorrelated
  scalar subqueries directly to `Join(kInner, input, EnforceSingleRow(body))`
  — Apply only carries correlated scalar (kLeft) and EXISTS / IN
  (kLeftSemiProject). No iteration step changes kind.
- **L.cols carry-through via `arbitrary(L.col)` with `Column*`
  reuse.** Alternatives considered: (a) add L.cols to
  `groupingKeys` — works but inflates the grouping cardinality
  for no semantic benefit (each rn group already has one L value);
  (b) carry via a side-channel — breaks the principle that
  downstream refs resolve by `Column*`. The `arbitrary` aggregate
  is the cleanest option and is constant-folded inside each `rn`
  group anyway.

## Known design gaps (consolidated)

Two genuine gaps, tracked in the per-rule docs and consolidated here.

### Gap 1 — Cases 5/6 IN-shape (`Apply.inLhs` set, body = Aggregate)

**Where**: `Decorrelate-agg-rules.md` §"Known design gaps".

**What's missing**: Cases 5/6 specify mark derivation via
`bool_or(_include)` for **EXISTS-shape**. For **IN-shape** (where
the Apply carries `inLhs` / `inBodyKey`), the equi predicate
`eq(inLhs, inBodyKey)` needs to be folded into the per-row mark
derivation before `bool_or` collapses. This wasn't worked out in
the rules docs.

**Why `inLhs` / `inBodyKey` stay distinct from `Apply.filter`.** An
attractive simplification considered: fold the IN equi into `filter`
and rely on the existing F_pre / F_post split. It doesn't work
because **`nullAware` applies only to the IN equi pair**, not to
other filter conjuncts:
- `eq(inLhs, inBodyKey)` evaluating NULL → mark NULL (three-valued
  IN logic).
- Other filter conjuncts evaluating NULL → row filtered out (mark
  contribution = false).

If both lived in `filter`, terminus couldn't reliably tell which
conjunct came from IN (pattern-matching `eq(outer_col, body_col)`
collides with correlated WHERE predicates of the same shape). The
pair MUST stay distinct on Apply to preserve nullAware semantics
through to terminus (where they lower to `Join.leftKeys` /
`Join.rightKeys` + `Join.nullAware`).

**Implementation framing.** IN-with-Agg-body is a separate mechanism
parallel to F_post, not a special case of it:
- F_post: substitute accFilter conjuncts referencing agg-result cols
  via `c → c_coal`; fold into the IF-in-outer-Project wrap.
- IN-shape: substitute `inBodyKey` via the same `c → c_coal` rules
  when it references an agg-result col; fold into the per-row mark
  predicate as `_include AND eq(inLhs, inBodyKey_subst)`, then
  `bool_or` per outer.

Both mechanisms share the substitution helper but have different
result-shape concerns (IF-wrap vs bool_or mark derivation).

**Required changes to `aggregatePeelSemi`**:
- Currently drops user aggregates entirely (EXISTS doesn't need
  them). For IN, must keep the aggregates whose results `inBodyKey`
  may reference, so the substitution has targets.
- Add `inBodyKey` resolution against lifted Agg outputs, including
  `c → c_coal` for COALESCE-needing aggregates.
- Build per-row predicate as `_include AND eq(inLhs, inBodyKey_subst)`
  feeding into `bool_or`.
- Preserve nullAware semantics: `bool_or` returns NULL when all
  inputs are NULL on the equi side; the final COALESCE remaps NULL →
  false ONLY when `nullAware == false`. For `nullAware == true`,
  the mark stays NULL for outers with all-NULL equi evaluations.

**Open subtleties**:
- Case 6 IN-shape (gby non-empty): same per-(rn, gby) eval into the
  per-row predicate, then `bool_or` per `rn`.
- Interaction with concurrent F_post conjuncts on accFilter: both
  fold into the per-row predicate via AND; both contribute to the
  set of agg-result substitutions.

**Implementation status**: NYI loud (`VELOX_NYI` at Aggregate peel
when `kSemi + inLhs != nullptr`).

### Gap 2 — Shape C composition with Decorrelate Project peel

**Where**: `Decorrelate-outer-refs-design.md` and
`Decorrelate-project-rules.md` §"Known design gap — Shape C
composition".

**What's missing**: The outer-refs design specifies a Translate-side
fix (alias body when output collides with applyTarget's outputs).
This is design-correct in isolation but **insufficient** in the
presence of Decorrelate's Project peel. Project peel peels the
alias wrap, exposes the un-aliased body, and the inner Apply's
`outputColumns = input.cols ++ originalBody.cols` brings the
duplicate back.

**Required completion**: Project peel's `projectPeelLeft`
must detect collisions in the inner Apply's outputColumns (by
Column* identity OR by name) and apply the same aliasing trick
recursively — wrapping body cols that collide and propagating the
alias through the lifted Project's expression rewrite.

**Open subtleties**:
- Per-col aliasing for multi-col bodies.
- Lifted Project's expr rewrite to honor aliases (via
  `ExprFactory::substitute`).
- Interaction with COALESCE wraps in Aggregate peel (cols may
  already be fresh; don't double-alias).

**Implementation status**: Translate-side fix landed; Decorrelate
completion NYI. Tests like `SELECT (SELECT a) FROM t` still fail
with "Duplicate output column" at PlanConsistencyChecker.

### Out-of-scope absences (not gaps per se — items the design didn't aim to cover)

- **Body operators**: Window, Limit, Sort, Join (in body), UnionAll,
  Unnest, nested Apply — no peel rules designed. Each would need
  its own per-operator design + impl.
- **Aggregate Cases 4, 7, 8** — explicitly deferred in
  `Decorrelate-agg-rules.md`. Design has notes on the approach
  (window-based vs translate-side restriction for Case 4;
  per-(rn, gby) F_post + outer bool_or for 7/8) but specific
  shapes not worked out.

## Sources cited

- Neumann, T., & Kemper, A. (2015). "Unnesting Arbitrary Queries."
  BTW 2015. The canonical reference for the iterative pushdown
  framework we follow.
- Neumann, T. (2024). Follow-up work in the same line (Umbra
  context). To be cited precisely once we locate the exact paper.
- Galindo-Legaria, C., & Joshi, M. (2001). "Orthogonal Optimization
  of Subqueries and Aggregation." SIGMOD 2001. The Apply-operator
  framing from SQL Server's optimizer.
- Elhemali, M., Galindo-Legaria, C., Grabs, T., & Joshi, M. (2007).
  "Execution Strategies for SQL Subqueries." SIGMOD 2007. Cost-based
  subquery execution in SQL Server.
