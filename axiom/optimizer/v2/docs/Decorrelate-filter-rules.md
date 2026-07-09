# Decorrelate Filter Rules

*Companion to:*
- *`Decorrelate-driver.md` — driver loop, terminus, NYI guards.*
- *`Decorrelate-agg-rules.md` — F_pre / F_post classification at Aggregate lift.*

Peels a `Filter` from `body` by absorbing its predicates into
`Apply.filter`. Uniform across `Apply.kind` (kLeft / kLeftSemiProject).

## Mental model

```
Apply (kind, filter = F_existing, body = Filter(child, P))
  ↓
Apply (kind, filter = F_existing AND P, body = child)
```

Filter disappears from body; its predicates land in `Apply.filter`.
At terminus, `Apply.filter` becomes `Join.filter`.

## Rule

**Before:**

```
- Apply (kind, filter = F_existing, body = Filter(child, P))
  - input = L
```

**After:**

```
- Apply (kind, filter = F_existing AND P, body = child)
  - input = L
```

Where:
- `F_existing AND P` is the conjunction of the pre-existing
  `Apply.filter` (null treated as TRUE) with the Filter's predicate.
  Implementation: use a helper that ANDs nullable filters and
  collapses TRUE.
- `kind` is preserved.
- `Apply.input`, `Apply.enforceSingleRow`, `Apply.markColumn`,
  `Apply.inLhs`, `Apply.inBodyKey` are all unchanged.
- No new node is introduced above Apply. The Filter is gone.

## Why uniform across kinds (and across F_pre / F_post)

**No kind-based case analysis.** `kLeft` and `kLeftSemiProject` both
treat `Apply.filter` the same way at terminus — it becomes
`Join.filter`. So absorbing predicates uniformly is sound.

**No correlated / non-correlated split.** Both correlated predicates
(reference outer cols) and non-correlated predicates (reference only
body cols) absorb into `Apply.filter`. At terminus, `Join.filter`
applies them all — semantically identical to the original
`Apply(body=Filter(...))`. The optimizer's later pushdown pass moves
non-correlated predicates into the scan side; that's not
decorrelate's concern.

**No F_pre / F_post split at this rule.** The Aggregate peel rule
splits `Apply.filter` into F_pre (applied below the lifted Agg) and
F_post (folded into the result Project's IF wrap) at *lift time*. The
Filter rule has no Aggregate in view yet — it just accumulates
predicates and lets the eventual Aggregate rule classify them.

## Soundness under `enforceSingleRow`

`Apply.enforceSingleRow == true` (only meaningful for `kLeft`; the
invariant forbids it for `kLeftSemiProject`): predicate absorption is
sound because the per-`rn` single-row assertion at terminus operates
on the post-filter rows:

- Before: per L row, body produces filtered child rows; ESR asserts ≤1.
- After: per L row, body produces all child rows; `Join.filter`
  selects matching rows at the join boundary; per-`rn` aggregate
  asserts ≤1 on the matching rows.

Same cardinality per outer in both shapes.

## Termination contribution

If the Filter carried the only remaining outer reference in body
(e.g., body was `Filter(Scan(u), u.a = t.a)`), absorbing it into
`Apply.filter` empties `correlationColumns` on the next recompute →
driver hits terminus. This is the common shape for simple correlated
scalar subqueries.

If body has more correlated operators below the Filter, the driver
continues peeling them.

## What this rule does NOT do

- **Reorder predicates relative to Aggregate.** F_pre / F_post
  classification happens later, at Aggregate lift.
- **Detect cardinality-changing implications.** Predicates that would
  cause `Apply.enforceSingleRow == true` to misfire are caught at
  terminus, not here.
- **Push predicates further down into body.** The Filter peel raises
  predicates UP into `Apply.filter`. Predicates already inside body
  (e.g., a `Filter(Scan, ...)` further down) get peeled in subsequent
  driver iterations when the outer Filter is gone and the inner one
  becomes the new outermost body operator.
