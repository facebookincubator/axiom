# Decorrelate Join Rules

*Companion to:*
- *`Decorrelate-driver.md` — driver loop, terminus, NYI guards.*
- *`Decorrelate-filter-rules.md` — Filter peel.*
- *`Decorrelate-project-rules.md` — Project peel.*
- *`Decorrelate-agg-rules.md` — Aggregate peel.*
- *`Decorrelate-limit-rules.md` — Limit peel.*

Peels a `Join` from `body`. Body's Join encodes a relational join
between its two sides (`A` = left, `B` = right) under
`(joinKind, leftKeys, rightKeys, filter)`.

## Mental model

`Apply(L, Join(A, B, joinKind, predicate))` evaluates the join per
outer row of `L`. Tree-only execution forbids putting `L` on two
sides of a downstream Join (no DAG support, no materialization
operator — see `Decorrelate-design-rationale.md` §"Why we can't just
follow the paper"), so the textbook "Apply distributes over Join"
rewrite (which duplicates `L` to both sides) is out of bounds.

Instead, **serialize**: compute `A` per outer first, then compute `B`
per `(outer, A)` row. This places `L` only at the bottom of the chain
and preserves tree shape.

```
Apply(L, Join(A, B, joinKind, predicate), outerKind, ...)
↓ joinPeel
[outer envelope: ESR / markColumn / NULL-collapse / IN equi]
  applyB = Apply(applyA, B, kindForB, filter=predicate, keysForB=...)
    applyA = Apply(L, A, kindForA, ...)
```

- `applyA` decorrelates the left side per outer L. Its `outputColumns`
  are `L.cols ++ A.cols ++ [applyA.includeMarker]` (relaxed Apply
  contract).
- `applyB` decorrelates the right side per `(outer, A)` row. Its input
  is `applyA`'s output, so it can resolve any predicate involving
  `L`, `A`, or `B`. `applyB.filter` carries the Join's predicate.
- The outer envelope handles `outerKind`-specific semantics:
  ESR wrap (scalar context), markColumn synthesis (kSemi), post-Filter
  for INNER semantics (drop pad rows), or IN equi check.

Each chained `Apply` is independently decorrelatable by the driver —
they recurse through the normal peel machinery.

## Predicate normalization

`Join.leftKeys[i] = Join.rightKeys[i]` conjuncts and `Join.filter` are
together the body Join's "predicate." Before applying any per-kind
rule, normalize to a single `predicate` expression:

```
predicate = AND(eq(leftKeys[i], rightKeys[i]) for i) AND filter
```

(Or `nullptr` if all components are empty / TRUE.) The normalized
predicate becomes `applyB.filter`. Where the rule needs to split the
predicate (e.g., equi-pair for IN), it re-extracts from the
normalized form.

## Body `joinKind` translation

Each Join kind maps to a chain configuration. `kindForB` controls
whether `applyB` pads on no-match (kLeft pad) or projects a mark
(kLeftSemiProject).

| `joinKind` | `kindForB` | Outer envelope addition |
|---|---|---|
| `kInner` | `kLeft`, or `kLeftSemiProject` under an outer `kLeftSemiProject` | per-outer pad collapse: drop `applyB` pad rows, keeping exactly one pad per outer with no match — see §"INNER pad-row drop" |
| `kLeft` | `kLeft` | none — `kLeft` pad maps to `A LEFT JOIN B`'s pad |
| `kRight` | swap to `kLeft` with sides reversed (`applyA=B, applyB=A`) | none — symmetry |
| `kFull` | NYI | needs both sides' unmatched rows; tree-only can't express without re-evaluating one side. Loud NYI. |
| `kLeftSemiFilter` | `kLeftSemiProject` (apply emits per-A mark) | `Filter(mark)` then drop mark; equivalent to A-rows-with-any-matching-B |
| `kLeftSemiProject` (in body) | `kLeftSemiProject` | mark propagates as a new column; a filter reading that mark gates inclusion and triggers the per-outer pad collapse — see §"outerKind = kLeft (scalar context)" |
| `kAnti` | `kLeftSemiProject` | `Filter(NOT mark OR mark IS NULL)` |

(Filter expressions in the envelope use `Builder::makeBoolean` /
`ExprFactory::makeIsNull` per the existing conventions.)

## Correlation distribution

Per outer `L`, A and B can each be correlated (reference `L` cols) or
not. Combinations and chain effect:

| (A corr?, B corr?) | Chain behavior |
|---|---|
| (T, T) | Both `Apply`s in chain stay correlated; driver decorrelates each via Filter / Project / Agg / Limit peels. |
| (T, F) | `applyA` correlated (driver peels); `applyB` uncorrelated (terminus fast-path → cross-product Join). |
| (F, T) | Rewrite to put correlated side outermost OR keep order with `applyA` uncorrelated (terminus fast-path) and `applyB` correlated. Prefer outermost-correlated for shallower chain; both shapes are semantically equivalent. |
| (F, F) | Outer Apply itself is uncorrelated; driver's terminus fast-path fires before any peel. Join peel never sees this. |

B may reference A cols via the Join's predicate even when B's
relation isn't correlated to `L`; that's handled by `applyB.filter`
referencing input cols.

Before `joinPeel`, `liftCorrelationAboveJoin` moves correlation predicates
above the body Join when doing so leaves both inputs uncorrelated. The body
then remains intact and the normal Apply terminus joins it to L once.
`joinPeel` serializes the body only when correlation remains inside at least
one input after that lift, such as an outer reference in a projected value.

## Outer Apply kind

The outer envelope above depends on the **outer** Apply's kind.

### outerKind = `kLeft` (scalar context)

The outer Apply emits one row per outer (per scalar-subquery
semantics). Body's Join output becomes that row's value(s). Outer's
`includeMarker` is `true` on real body rows, `NULL` on pad.

- Outer's `includeMarker` resolves by body Join kind:
  - Body kInner: `applyA.includeMarker AND applyB.includeMarker` —
    a real body row requires both sides to match; applyB pads
    (unmatched right) represent body rows that don't exist and must
    be excluded.
  - Body kLeft: `applyA.includeMarker` alone — the inner LEFT JOIN's
    right-side pad rows ARE real body rows (left preserved with
    NULL right cols), so applyB's marker must not gate inclusion.
  - Body kLeftSemiProject: `applyB` is itself kLeftSemiProject and has
    no `includeMarker`; it writes the body's mark for every row `applyA`
    produced. With no filter on that mark, `applyA.includeMarker` alone
    gates inclusion. A filter on the mark (`WHERE x IN (...)` inside the
    subquery) selects among body rows, so inclusion becomes
    `COALESCE(applyA.includeMarker AND markFilter, false)` and the chain
    runs the per-outer pad collapse of §"INNER pad-row drop" to keep one
    pad row for an outer whose body rows the filter all rejected.
    `accumulatedFilter` splits by whether a conjunct reads the mark: the
    rest reference only A and outer columns and ride on `applyA.filter`.
  - A null-aware body (`IN` / `NOT IN`) carries the IN equality as its
    single equi-pair, which must be re-hosted as `applyB`'s
    `inLhs` / `inBodyKey` — `Apply.nullAware()` is defined as
    `inLhs != nullptr`, so folding the pair into `applyB.filter` would
    silently drop null-awareness. A body whose IN key reads outer
    columns has its equality demoted into the Join filter by
    `terminusSemi` and so carries no equi-pair; that shape is NYI loud.
- ESR=true (`enforceSingleRow`) handling depends on body Join kind:
  - Body kLeft: leg-wise ESR on `applyA` (`|A| ≤ 1`) and `applyB`
    (per-A `|B| ≤ 1`) enforces the scalar bound; their conjunction is
    exactly "≤1 body row per outer," and the chain already emits one
    row per outer.
  - Body kInner: leg-wise ESR is wrong — an empty side makes the body
    empty (a valid 0-row scalar → NULL), but a per-leg check fires on
    the other side's rows. Run the per-outer pad collapse first, then
    `EnforceDistinct(rn)` over the collapsed stream; see §"INNER
    pad-row drop."
  - Body kLeftSemiProject: with no mark filter, `applyA` carries the ESR
    (`|A| ≤ 1`), since semi projection is one row in, one row out. With
    a mark filter, the legs carry none and `EnforceDistinct(rn)` runs
    over the collapsed stream, as for kInner.

### outerKind = `kLeftSemiProject` EXISTS (inLhs == nullptr)

Outer emits one row per outer with `markColumn = true` iff body's Join
produces ≥1 row.

- Mark composition: chain's `applyB` is kLeftSemiProject → emits its
  own mark per outer. Outer `markColumn` then equals the chained mark
  (possibly composed with applyA's mark if both contribute).
- For an unpredicated kInner body: mark = `applyA.mark AND applyB.mark`
  (∃a ∧ ∃b), since with no predicate the two existences are independent.
- For a kInner body carrying a predicate, that formula is wrong: existence
  is a property of the (a, b) pair. `AssignUniqueId` tags each outer,
  `applyA` is kLeft over A (so an outer with no a row still reaches the
  result), `applyB` is kLeftSemiProject over B carrying the predicate and
  the accumulated filter, and a `bool_or(applyA.mark AND applyB.mark)`
  aggregate grouped by the outer's id answers the mark and emits one row per
  outer. The outer columns use `arbitrary` because the id functionally
  determines them.
- For a kLeft body, the chained `applyB` is kLeft. A real A row makes the
  body exist whether B matches or contributes a NULL-padded row. The outer
  mark is therefore `bool_or(applyA.includeMarker AND accumulatedFilter)`.
  Conjuncts that read only L or A ride on `applyA.filter`. If every conjunct
  can move there, B cannot affect the outer mark and `applyB` is omitted.
  Remaining conjuncts are evaluated in the mark expression so the padded row
  survives long enough to produce false when no body row qualifies.
- For a non-null-aware kLeftSemiProject body, the chained `applyB` reproduces
  the body's mark. The outer mark is
  `bool_or(applyA.includeMarker AND accumulatedFilter)`, where the accumulated
  filter may read that body mark. Conjuncts that read only L or A move to
  `applyA.filter`; if none read the body mark, `applyB` is omitted.
- Body kLeftSemiFilter, null-aware kLeftSemiProject and kAnti: NYI loud.

### outerKind = `kLeftSemiProject` IN (inLhs != nullptr, inBodyKey set)

Outer emits `markColumn = (inLhs IN body's output column inBodyKey)`.
For a kInner body, `applyA` is kLeft so every outer reaches the result, and
`applyB` is kLeftSemiProject carrying the IN operands and the body's join
predicate. This produces one three-valued IN mark per A row. Two `bool_or`
aggregates combine these marks across each outer: the final mark is true if
any A row matched, NULL if only an unknown result exists, and false otherwise.
`applyA`'s include marker excludes its synthetic row when A is empty.
`inBodyKey` must read only B. `applyB` resolves it against its body, so a key
reading A resolves on the input side instead: `terminusSemi` demotes the
equality to a residual comparison and leaves no equi-key, and the leg loses
the null-awareness the mark's unknown case needs. That shape is NYI loud.

Other body Join kinds are NYI loud.

## INNER pad-row drop with outer kLeft preservation

`outerKind=kLeft` over `joinKind=kInner` needs `outer LEFT JOIN body`
semantics per outer L:
- body produces matching (a, b) rows.
- ≥1 matches → emit those rows.
- 0 matches → emit a single NULL-padded row (outer preserved).

The chain's kLeft legs do NOT deliver this directly. Per outer they
emit `Σ over A-rows of max(1, matching B)` rows, tagging each with
`combinedMarker = applyA.includeMarker AND applyB.includeMarker`
(true only on real (a, b) pairs). Two failure modes:

- A real match and a pad both carry the outer, so INNER semantics
  require dropping the pad rows (`combinedMarker` false).
- Dropping *all* pad rows loses the outer entirely when there are 0
  matches. And when one side is empty while the other has >1 rows, the
  legs emit one pad row per surviving row — so a 0-match outer is
  represented by *multiple* pad rows, not one. A bare `EnforceDistinct`
  then raises on those duplicate pads, and a plain projection
  duplicates the outer row. (This is the cross-join bug: `|A|>1,
  |B|=0` yields `|A|` pad rows where semantics demand one.)

**Resolution — per-outer pad collapse.** Restore exactly one pad per
match-less outer while keeping every real row:

1. `rn = AssignUniqueId(L)` at chain bottom (the tagged_L pattern the
   Limit peel uses).
2. Chain produces `(L, rn, a, b, combinedMarker)`.
3. Window over `PARTITION BY rn`: `anyMatch = bool_or(combinedMarker)`
   and `padOrdinal = row_number()`.
4. `Filter(combinedMarker OR (NOT anyMatch AND padOrdinal = 1))` —
   real rows survive; a match-less outer keeps its first row (a pad)
   and drops the rest.

A side being empty is exactly the 0-match case, and the collapse
reduces the duplicate pads to one. A predicate rides in
`applyB.filter`, whether written on the Join node itself
(`JOIN ... ON`) or as a WHERE above it, so `anyMatch` correctly
handles the resulting mixed matches.

### ESR interaction

- **ESR=false**: the Filter above is the whole envelope — real rows
  (possibly many) plus one pad per match-less outer.
- **ESR=true**: wrap the Filter output in `EnforceDistinct(rn)`. A
  match-less outer contributes exactly one (pad) row and never trips
  it; an outer with >1 real matches contributes >1 row and raises
  SQL's "scalar subquery returned multiple rows" error.

`joinKind=kLeft` does NOT use this pass: there `combinedMarker` is
`applyA.includeMarker` alone (applyB pads are real LEFT JOIN output),
so the chain already emits one row per A-row and leg-wise ESR on
`applyA` / `applyB` enforces the scalar bound directly.

## Cases covered

Combinations across (outerKind, joinKind):

| outerKind | joinKind | Status |
|---|---|---|
| kLeft (ESR=true) | kInner cross-join | **in scope** (per-outer pad collapse + `EnforceDistinct(rn)`) |
| kLeft (ESR=true) | kInner with predicate | **in scope** (per-outer pad collapse + `EnforceDistinct(rn)`) |
| kLeft (ESR=true) | kLeft | **in scope** |
| kLeft (ESR=true) | kRight | **in scope** (via swap) |
| kLeft (ESR=true) | kFull | NYI loud |
| kLeft (ESR=true) | kLeftSemiProject | **in scope** |
| kLeft (ESR=true) | kLeftSemiFilter / kAnti | NYI loud |
| kLeft (ESR=false) | kInner cross-join | **in scope** (per-outer pad collapse; see §"INNER pad-row drop") |
| kLeft (ESR=false) | kInner with predicate | **in scope** (per-outer pad collapse; see §"INNER pad-row drop") |
| kLeft (ESR=false) | kLeft / kRight | **in scope** |
| kLeft (ESR=false) | kFull | NYI loud |
| kLeft (ESR=false) | kLeftSemiProject | **in scope** |
| kLeft (ESR=false) | kLeftSemiFilter / kAnti | NYI loud |
| kLeftSemiProject EXISTS | kInner, with or without predicate | **in scope** (mark composes per §"outerKind = kLeftSemiProject EXISTS") |
| kLeftSemiProject EXISTS | kLeft | **in scope** (each left row yields output; reduce by outer id) |
| kLeftSemiProject EXISTS | kLeftSemiProject (not null-aware) | **in scope** (body mark stays available to the accumulated filter) |
| kLeftSemiProject EXISTS | kLeftSemiFilter / null-aware kLeftSemiProject / kAnti | NYI loud |
| kLeftSemiProject IN | kInner | **in scope** (three-valued comparison reduced per outer) |
| kLeftSemiProject IN | other Join kinds | NYI loud |
| any | kFull | NYI loud — needs DAG / re-evaluation of one side |

NYI loud throws `VELOX_NYI` at the dispatch point with the specific
shape in the error message; satisfies the correctness invariant from
`Decorrelate-design-rationale.md`.

## Termination contribution

`joinPeel` is a full-replacement rule: it constructs the chain and
returns the fully-decorrelated subtree, including the outer envelope.
The outer Apply is gone after the rule. Driver does not re-iterate.

Each chained `Apply` is dispatched through the normal driver loop and
hits whatever per-op peels apply to its body (Filter / Project / Agg /
Limit / nested Join).

## What this rule does NOT do

- **Handle `kFull`**: see NYI rationale. Future work; requires DAG
  support or materialization.
- **Reorder for performance**: chain order is `Apply(L, A)` then
  `Apply(prev, B)` in body-Join's natural left-right order. Optimizer
  later may swap if cost-justified; that's not decorrelate's concern.
- **Keep the body Join whole**: the chain Applies `A`, then `B`, so the
  body's own Join never forms as a unit and `L x A` materializes before
  `B` is read. Taking it apart is what lets the chain accept correlation
  on either side — engines that decorrelate the body as a unit reject
  those shapes. A body whose correlation lifts into a join condition
  could be decorrelated once and joined to `L` whole; that path is not
  built.
- **Optimize uncorrelated side**: if A is uncorrelated, `applyA` hits
  terminus fast-path → cross-product Join. No special case in this
  rule.
- **Fuse with surrounding Project peel**: if outer body is
  `Project(Join(...))`, Project peel runs first (driver dispatch by
  body's outermost op) and lifts the Project; `joinPeel` sees the
  cleaned-up Join body.
- **Multi-leaf nested Joins**: a body `Join(Join(A1, A2), B)` decorrelates
  as `joinPeel` over the outer Join; the inner `Join(A1, A2)` becomes
  `applyA`'s body and the driver recurses into `joinPeel` on it. No
  special depth limit; recursion terminates at leaf scans.
