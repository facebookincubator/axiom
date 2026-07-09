# Decorrelate EnforceSingleRow Handling

*Companion to:*
- *`Decorrelate-driver.md` — driver loop, terminus.*
- *`Decorrelate-agg-rules.md` — Aggregate peel (special case: ESR redundancy).*
- *`Decorrelate-design-rationale.md` — why ESR is an Apply flag and not a body operator.*

`Apply.enforceSingleRow` is a flag set by Translate for scalar
subquery contexts where SQL requires the subquery to return 0 or 1
row per outer (`> 1` errors at runtime). The flag rides with Apply
through iteration; this doc covers how it lowers at terminus and
where it interacts with peel rules.

## Why a flag, not a body operator

Earlier designs put `EnforceSingleRow` as a wrapper inside body. That
breaks under iteration: when correlations peel out of body, the
EnforceSingleRow's scope strips from "per outer" to "global" because
body's per-outer evaluation is gone. The assertion then fires on the
wrong cardinality (or doesn't fire at all).

By making ESR an `Apply` flag, the per-outer scope is bound to Apply,
not to body's internal shape. The flag travels through iteration
unchanged; the assertion is enforced at terminus where the per-outer
semantics are explicit.

See `Decorrelate-design-rationale.md` §"Shapes considered for Apply
over Aggregate" for the rejected body-operator alternatives.

## Per-kind lowering at terminus

| `kind`             | `enforceSingleRow` | Terminus shape                                            |
|--------------------|--------------------|-----------------------------------------------------------|
| `kLeft`            | false              | LEFT JOIN                                                 |
| `kLeft`            | true               | LEFT JOIN + per-`rn` `EnforceDistinct` (see below)        |
| `kLeftSemiProject` | (invariant: false) | LEFT SEMI PROJECT (no ESR)                                |

## kLeft + ESR=true — per-`rn` `EnforceDistinct`

Body is per-outer; the assertion needs per-outer scope. Velox's global
`EnforceSingleRow` doesn't suffice — and there's no `single_value()`
aggregate. The substrate already has the right primitive:
`EnforceDistinct` (`v2/Node.h` / lowers to Velox
`EnforceDistinctNode`) — asserts ≤1 row per distinct key.

Lowering shape:

```
- EnforceDistinct (distinctKeys = [rn],
                   errorMessage = "Scalar sub-query has returned multiple rows")
  - LEFT JOIN
      filter = Apply.filter
    - input = AssignUniqueId(L) → tagged_L (adds `rn`)
    - body (already simplified by iteration)
```

Per-outer cardinality at the LEFT JOIN's output:
- N = 0 matching body rows → LEFT JOIN pads 1 row (body cols NULL) →
  EnforceDistinct sees 1 row per `rn` → passes. ✓ SQL "scalar
  subquery returns NULL when 0 rows".
- N = 1 matching body row → 1 row per `rn` → passes. ✓
- N > 1 matching body rows → > 1 row per `rn` → runtime error. ✓
  SQL "scalar subquery errors when > 1 row".

No per-rn aggregate synthesis needed. `EnforceDistinct` is a
pass-through node (its output columns equal its input's pointer-for-
pointer), so the parent expression that referenced the scalar
subquery's value resolves directly to body's output column appearing
in the LEFT JOIN's output.

**Implementation note**: in practice a final `Project` above
`EnforceDistinct` is needed to strip `rn` from the output schema —
`EnforceDistinct` requires `rn` in its input (it's the distinct
key), and pass-through preserves it, so the consumer sees `rn`
unless we project it out. The Project is purely a column-prune; it
adds no expressions. Output schema = `apply.outputColumns` =
`input.cols ++ body.cols`.

## Interaction with peel rules — redundancy detection

When body contains an `Aggregate` with empty `gby`, the lifted Agg
naturally produces exactly one row per outer (per-`rn` group). The
ESR assertion is then *redundant* — Aggregate already enforces ≤1
row per outer via grouping.

**Translate should not set ESR=true when body has a global
Aggregate.** Specifically, if the SQL is
`SELECT (SELECT max(x) FROM ...) FROM t`, the inner `max(x)` is a
global Agg → always 1 row per outer → ESR is unneeded. Translate
emits `Apply.enforceSingleRow=false`.

**Aggregate peel preserves ESR.** If translate set ESR=true (e.g.,
because the SQL form looked scalar but body wasn't initially analyzed
as having a global Agg), the Aggregate rule peels normally. At
terminus, both the lifted Agg's per-`rn` grouping AND the
`EnforceDistinct([rn])` wrap apply. EnforceDistinct becomes a no-op
(each per-`rn` group already has 1 row from the lifted Agg). Correct
but redundant; downstream optimization can elide.

**Filter / Project peel don't interact with ESR.** Both preserve
per-outer cardinality (Filter peel: predicates absorbed into
Apply.filter, applied at the join boundary before ESR; Project peel:
no cardinality change). ESR rides through unchanged.

## What this rule does NOT do

- **Detect mid-iteration redundancy.** If body becomes an Aggregate
  during iteration (peel reveals it), we don't drop ESR=true at that
  point. The redundant wrap is left for downstream optimization.
- **Lower ESR before terminus.** The flag stays on Apply until
  terminus; no per-iteration wrap.
- **Handle EnforceSingleRow node inside body.** Under the relaxed
  Apply contract, body doesn't contain EnforceSingleRow nodes —
  translate uses the flag instead. If iteration somehow encounters an
  EnforceSingleRow inside body (e.g., from a sub-Apply that hasn't
  been driven yet), that's the inner Apply's concern, not this
  doc's.
