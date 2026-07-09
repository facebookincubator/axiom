# Decorrelate Driver

*Companion to:*
- *`Decorrelate-filter-rules.md` — Filter peel.*
- *`Decorrelate-project-rules.md` — Project peel.*
- *`Decorrelate-agg-rules.md` — Aggregate peel.*
- *`Decorrelate-limit-rules.md` — Limit peel.*
- *`Decorrelate-join-rules.md` — Join peel.*
- *`Decorrelate-enforce-single-row.md` — ESR handling at terminus.*
- *`Decorrelate-design-rationale.md` — alternatives considered, rejected paths.*

The driver is the loop that turns each `Apply` node into a `Join`. The
per-op rules (Filter, Project, Aggregate, Limit, Join, AssignUniqueId)
describe HOW to peel one body operator; the driver describes WHICH
rule fires WHEN, how state is maintained between peels, and when
iteration terminates.

## Mental model

```
Apply (kLeft, body = some_subtree)
  ↓ peel one body operator (Filter / Project / Aggregate)
Apply (kLeft, body = smaller_subtree)
  ↓ peel another
...
  ↓ terminus: body has no outer refs
Join (kLeft, right = simplified_subtree)
```

Apply's state, as the driver sees it (see `Node.h` for full
invariants):

- `input` — the outer (correlated) side. Stable across iterations.
- `body` — the (sub)query tree being decorrelated. Shrinks per peel.
- `kind` — what the subquery context expects per outer row. Stable
  across iterations.
   - `kLeft` — correlated scalar / lateral subquery: emit one row per
     `(outer, body-match)` and a NULL-padded row for outers with no
     match.
   - `kLeftSemiProject` — `EXISTS` / `IN`: emit one row per outer
     with a fresh BOOLEAN `markColumn` (true iff any body row matches,
     NULL-aware for IN).
- `correlationColumns` — `input` columns still referenced inside
  `body`. Recomputed from `body` each iteration; emptying it
  triggers terminus.
- `filter` — residual predicates that ride with Apply through
  iteration and become `Join.filter` at terminus. May grow when a
  Filter peel absorbs predicates from `body`.
- `enforceSingleRow` — scalar-subquery cardinality flag. Stable;
  honored at terminus per `Decorrelate-enforce-single-row.md`.
- `markColumn` — `kLeftSemiProject` mark output. Stable.
- `includeMarker` — `kLeft` pad-row marker. Stable.
- `inLhs` / `inBodyKey` — IN equi-pair (kLeftSemiProject only).
  Stable except under Project peel, which may substitute Project's
  exprs into `inBodyKey` (Rule B).
- `outputColumns` — Apply's external schema. Stable.

Per peel, `body` shrinks and `filter` may grow; the surrounding tree
may gain operators above Apply (the peeled operator lands there for
some rules).

## Driver loop

```
driver(apply):
  loop:
    recompute apply.correlationColumns from apply.body
    if apply.correlationColumns is empty:
      return terminus(apply)              // Apply → Join + optional ESR
    rule = dispatchByBodyOperator(apply.body)
    if rule is null:
      VELOX_NYI("driver: no peel rule for body operator X")
    apply = rule.apply(apply)             // peel, returning transformed tree
                                          // (may contain new Apply nodes;
                                          //  rule may itself throw NYI for
                                          //  subcases it doesn't handle)
```

The driver is invoked once per `Apply` node in the plan. When a peel
rule (notably Aggregate) produces a tree containing a NEW inner Apply,
the driver is recursively invoked on that inner Apply.

## Per-peel dispatch

Dispatch by `body`'s outermost operator:

| Body operator     | Rule                                              |
|-------------------|---------------------------------------------------|
| `Filter`          | `Decorrelate-filter-rules.md`                     |
| `Project`         | `Decorrelate-project-rules.md`                    |
| `Aggregate`       | `Decorrelate-agg-rules.md`                        |
| `Limit`           | `Decorrelate-limit-rules.md`                      |
| `Join`            | `Decorrelate-join-rules.md`                       |
| `AssignUniqueId`  | lift above Apply; recurse on inner Apply over child |
| anything else     | `VELOX_NYI` ("correlated reference inside …")     |

There is no leaf fast-path: when body is `Scan` / `Values` (or any
node that doesn't actually reference outer columns), the next
`recompute` finds an empty correlation set and terminus fires before
dispatch — no peel is consulted.

`EnforceSingleRow` does not appear as a body operator — it's the
`Apply.enforceSingleRow` flag, handled at terminus (see
`Decorrelate-enforce-single-row.md`).

## `correlationColumns` recomputation

After each peel, recompute `apply.correlationColumns` by walking the
new `body` and collecting references to `apply.input`'s columns.

**Recompute, not incremental.** A peel may absorb body's only reference
to an outer col (drop the ref) OR may peel one of several occurrences
(ref still in body via the other occurrences). Subtracting a ref per
peel would be wrong; the walk is the only correct mechanism.

**Refs in `apply.filter` don't count.** Those are at the boundary —
they become `Join.filter` at terminus and don't block iteration. Only
refs INSIDE `body` block terminus.

TODO: cache correlation refs per body node to avoid walking the full
body on each peel. Optimization, not correctness — defer until profile
shows it matters.

## Terminus

`correlationColumns` empty → terminus:

Translate `Apply` → `Join`, dispatched on `apply.kind`:

- **`kLeft`**: wrap body with an `_include := true` marker Project,
  then LEFT JOIN with `Apply.filter` as `Join.filter`. If
  `enforceSingleRow=true`, additionally tag input with
  `AssignUniqueId` and wrap the LEFT JOIN in `EnforceDistinct` on
  that id column; a final Project strips the id. See
  `Decorrelate-enforce-single-row.md`.
- **`kLeftSemiProject`**: LEFT SEMI PROJECT with `markColumn` as the
  mark output. For IN, the `(inLhs, inBodyKey)` equi-pair becomes
  Join equi-keys; `Apply.filter` becomes `Join.filter`.
  `enforceSingleRow` is invariant false for this kind.

**Non-correlated Apply hits this directly.** When translate emits an
uncorrelated `kLeftSemiProject` Apply (uncorrelated `EXISTS` / `IN`),
the first `recompute` finds an empty correlation set → straight to
terminus without any peel rule firing.

## NYI from rules

Rules may throw `VELOX_NYI` for sub-cases they don't yet handle —
those are rule-local limitations, not driver concerns. The driver
just propagates the error. Examples: Aggregate Case 4 (kLeft +
non-empty F_post + non-empty gby — see `Decorrelate-agg-rules.md`);
Aggregate over `GROUPING SETS`; kSemi+IN over Limit with count ≥ 1;
Join body of kind `kFull`, and
various outer/body-kind combinations under `joinPeel`; pure-outer
aggregates (args reference only outer columns) — declined loud
pending the pure-outer-aggregate rewrite.

## Order of operations subtleties

- **Recompute BEFORE checking terminus.** A peel may have just dropped
  the last outer ref; without recomputing first, the driver would
  attempt another peel on a body that's now ready for terminus.
- **Recursive Apply.** Project / Aggregate / Limit / Join /
  AssignUniqueId peels each emit a tree containing a fresh inner
  Apply whose body is the peeled operator's child (or, for Join,
  a chained pair of Apply nodes over the two sides). The peel calls
  `rewrite` on that inner Apply to re-enter the driver; each step
  strictly shrinks body, so iteration converges. Filter peel is the
  exception — it mutates `body` and `accumulatedFilter` in place
  inside the driver loop and continues, with no inner Apply.
- **Filter peel may trigger terminus.** If Filter was the last body
  operator carrying an outer ref, peeling it empties
  `correlationColumns` → terminus on the next iteration.

## What the driver does NOT do

- **Decide F_pre vs F_post.** That's the Aggregate rule's local concern
  at lift time, not the driver's. The driver just hands the Filter rule
  the Filter; it absorbs predicates uniformly into `Apply.filter`.
- **Optimize peel order across multiple options.** Dispatch is purely
  by body's outermost operator. No cost-based choice.
- **Validate body shape post-rule.** Each rule is responsible for
  producing a well-formed result; the driver re-enters the loop and
  re-recomputes from there.
