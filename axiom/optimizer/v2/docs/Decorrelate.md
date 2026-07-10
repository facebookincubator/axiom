# v2 Decorrelate — Overview

*Overview of the decorrelate pass, pointing at the doc family below. The
design rationale, alternatives considered, and rejected paths live in
`Decorrelate-design-rationale.md`.*

## Doc family

Specialized docs cover separate concerns; each is short and focused:

- **`Decorrelate-driver.md`** — driver loop, per-peel dispatch,
  `correlationColumns` recomputation, terminus detection.
- **`Decorrelate-filter-rules.md`** — Filter peel (uniformly absorbs
  predicates into `Apply.filter`).
- **`Decorrelate-project-rules.md`** — Project peel (Rule A lifts above
  Apply for kLeft; Rule B substitutes into mark predicate for
  kLeftSemiProject).
- **`Decorrelate-agg-rules.md`** — Aggregate peel. Case 4 (kLeft +
  non-empty F_post + non-empty gby) NYI.
- **`Decorrelate-enforce-single-row.md`** — `Apply.enforceSingleRow`
  lowering at terminus (kLeft only: `AssignUniqueId` + LEFT JOIN +
  `EnforceDistinct` on the id).
- **`Decorrelate-design-rationale.md`** — alternatives considered,
  rejected paths, why-we-can't-just-follow-the-paper.
- **`Decorrelate-outer-refs-design.md`** — outer-reference resolution
  (companion concern).

## What decorrelate does

Turns each `Apply` (a "true dependent join" produced by translate) into
a `Join` (or a richer post-Join structure for some cases) by iteratively
peeling correlations out of `Apply.body`. Runs after translate, before
any later pass.

## Algorithmic heritage

Framework derived from Neumann (2024) "Improving Unnesting of Complex
Queries" (top-down) and Neumann & Kemper (2015) "Unnesting Arbitrary
Queries" (bottom-up).

The **iterative-pushdown framework** is from that line. The **specific
lifted shapes** are designed from scratch — the paper's shapes assume
DAG execution / materialization that we don't have, which makes them
unsafe (correctness) or inefficient (distributed cost) in our setting.
See `Decorrelate-design-rationale.md` §"Why we can't just follow the
paper" for the full constraint analysis.

## Apply IR shape

Authoritative contract: `v2/Node.h` `Apply` class doc. Summary:

- 11 fields: `input`, `body`, `correlationColumns`, `kind`, `filter`,
  `enforceSingleRow`, `markColumn`, `inLhs`, `inBodyKey`,
  `includeMarker`, `outputColumns`.
- `kind ∈ {kLeft, kLeftSemiProject}`. Uncorrelated scalars are lowered
  directly by translate to `Join(kInner, input, EnforceSingleRow(body))`
  — they do not reach Apply.
- `filter`: residual predicates that ride with Apply through iteration;
  become `Join.filter` at terminus.
- `enforceSingleRow`: scalar-context assertion flag. Lowered at terminus
  per ESR doc. Invariant: false for `kLeftSemiProject`.
- `correlationColumns`: `input` cols the current `body` still references.
  Recomputed by driver after every peel; empty → terminus.
- `markColumn`: non-null iff `kLeftSemiProject`; BOOLEAN; holds the
  mark output.
- `inLhs` / `inBodyKey`: IN-only equi pair; both null for scalar /
  EXISTS.
- `includeMarker`: pad-row marker column for `kLeft` (non-null iff
  `kind == kLeft`); reads `true` on real body rows, NULL on pad rows
  after terminus.

## Driver / peel-rule mental model

```
driver(apply):
  loop:
    recompute correlationColumns from body
    if empty → terminus (Apply → Join, + optional ESR wrap)
    rule = dispatch(body's outermost operator)
    apply = rule.apply(apply)   // peel, possibly NYI
```

Each peel rule produces a transformed tree (often containing a new
inner Apply); driver re-enters on that.

See `Decorrelate-driver.md` for the full driver spec.
