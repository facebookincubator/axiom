# Decorrelate Project Rules

*Companion to:*
- *`Decorrelate-driver.md` — driver loop, terminus, NYI guards.*
- *`Decorrelate-agg-rules.md` — Aggregate peel.*

Peels a `Project` from `body`. Shape depends on `Apply.kind`:

- **kLeft** — Project lifts above Apply (body cols flow through
  Apply.outputColumns; Project's expressions remain unchanged). Rule A.
- **kLeftSemiProject** — Project dissolves into the mark predicate
  (`inBodyKey` substituted via Project's expressions; body becomes
  Project's child; no Project above Apply). Rule B.

## Mental model

Project's role in a correlated body: transform child rows into new
output cols, possibly referencing outer (L) cols inside expressions.
The peel pulls Project out of body — *where* depends on kind because
body cols flow through differently per kind.

```
kLeft:
  Apply (kind, body = Project(child, exprs))
    ↓
  Project(exprs)
    └ Apply (kind, body = child)

kLeftSemiProject:
  Apply (kSemi, body = Project(child, exprs), inBodyKey = c)
    ↓ (substitute c → expr_for_c)
  Apply (kSemi, body = child, inBodyKey = expr_for_c)
```

## Rule A — `kLeft`

**Before:**

```
- Apply (kind, filter = F, body = Project(child, exprs → out_cols))
  - input = L
```

**After:**

```
- Project: out_cols computed via exprs
                  // exprs reference body cols (now child cols) and
                  // L cols, all visible in the new Apply.outputColumns
  - Apply (kind, filter = F, body = child)
    - input = L
```

Where:
- `Apply.outputColumns` shifts from `input.cols ++ Project's out_cols`
  to `input.cols ++ child.cols` (the relaxed contract: body's cols
  pass through).
- The lifted Project above Apply reproduces the original Apply's
  outputs: `input.cols ++ out_cols`, with `out_cols` computed from
  exprs that reference child cols and L cols (all in the new Apply's
  outputColumns).
- Apply's `kind`, `filter`, `enforceSingleRow`, `markColumn`, `inLhs`,
  `inBodyKey` are unchanged.

**Correlated Project benefit:** if exprs reference L cols, those refs
are now above Apply (not in body). `correlationColumns` recomputation
sees only refs in the new body (`child`). If Project was the last
carrier of correlations, the next driver iteration hits terminus.

## Rule B — `kLeftSemiProject`

The Project's output cols feed into mark derivation, specifically the
IN equi-pair `(inLhs, inBodyKey)`. Lifting Project above Apply doesn't
work because `kLeftSemiProject.outputColumns` is
`input.cols ++ markColumn` — body cols (Project's outputs) aren't
visible above Apply.

**Before:**

```
- Apply (kSemi, filter = F, body = Project(child, exprs → out_cols),
         inLhs = lhs_expr, inBodyKey = bk_col)
  - input = L
```

where `bk_col ∈ out_cols` (i.e., `inBodyKey` references one of
Project's output cols).

**After:**

```
- Apply (kSemi, filter = F, body = child,
         inLhs = lhs_expr, inBodyKey = bk_expr)
  - input = L
```

where `bk_expr` is the Project's expression that produced `bk_col` —
substituted in place. `bk_expr` references child cols (and possibly L
cols); both are valid at the new Apply boundary.

For EXISTS (`inLhs == nullptr`, `inBodyKey == nullptr`), there's
nothing to substitute — body just becomes `child`, no expression
rewrite needed.

If `Apply.filter` references any of Project's output cols, substitute
them too via the same `out_col → expr` map.

**Correlated Project benefit (same as Rule A):** Project's L refs end
up in substituted expressions, possibly raising correlation into
`inBodyKey` or `filter` (both at Apply boundary). Body
(`child`) becomes simpler. `correlationColumns` recomputation may
empty out.

## Column identity / aliasing

The peel tracks column identity across the lift / substitute:

- For Rule A: `newBody`'s `outputColumns` flow into the inner Apply's
  `outputColumns` by identity (body cols that alias an input col
  collapse to a single slot). The lifted Project reproduces the
  original output schema, wrapping each of the original Project's exprs
  in `IF(includeMarker, expr, NULL)` so they read NULL on pad rows
  post-Join.
- For Rule B: the substitution `bk_col → bk_expr` is a Column-ref
  rewrite. If `bk_expr` references other Project output cols (rare —
  would require Project chaining we don't normally see), the
  substitution recurses.

The rule describes the lift / substitute *conceptually*; the
implementation mints nodes/columns via `Builder` and rewrites
expressions via `ExprFactory::substitute`.

## Termination contribution

Project peel removes Project from body uniformly. Body shrinks by one
operator. If Project was the only carrier of correlation (its exprs
referenced L cols and nothing below did), the next iteration hits
terminus.

## What this rule does NOT do

- **Push Project below other body operators.** Peel only lifts /
  dissolves; no internal reorganization of body.
- **Combine adjacent Projects.** A Project above the lifted Apply and
  another Project further above are not fused. Downstream
  constant-folding / project-merge passes handle that.
- **Handle Project chaining inside body.** If body =
  `Project(Project(child, ...), ...)`, the outer Project peels first
  (this rule), then the inner Project peels on the next iteration.

## Known design gap — Shape C composition

`Decorrelate-outer-refs-design.md` specifies the **Shape C fix in
Translate**: when a scalar subquery's body output column is the same
(or same-named-as) an outer input column (e.g., `SELECT (SELECT a)
FROM t`), Translate wraps body in `Project(body, [body.col],
[fresh_alias])` so `Apply.outputColumns` doesn't have duplicate
names.

What the design did NOT anticipate: **Project peel undoes the alias
wrap.** Rule A peels the alias Project, exposing the un-aliased
original body underneath. The resulting inner Apply has
`outputColumns = input.cols ++ originalBody.cols`, which once again
contains the duplicate. The lifted Project / terminus chain
propagates the duplicate to a Velox Project, which fails
`PlanConsistencyChecker` at emit time.

To close: Rule A's `projectPeelLeft` needs to detect when
`newBody.outputColumns` would collide (by name) with
`input.outputColumns`, and apply the same aliasing trick recursively
when constructing the inner Apply. This is non-trivial because:

- Each colliding body col needs its own alias.
- The lifted Project above must restore body's original output
  schema using the aliased cols (via `replaceInputs` substitution,
  similar to F_post handling).
- Multi-col body bodies need parallel aliases.

For now: Translate's Shape C fix is design-correct but partial; the
Decorrelate completion is NYI. Tests like `SELECT (SELECT a) FROM
t`, `SELECT (SELECT t.a) FROM t` still fail loudly with "Duplicate
output column" at emit-time PlanConsistencyChecker.

## NYI / edge cases

- **`bk_expr` is itself a Project of another Project**: substitution
  recurses cleanly in principle; flag if implementation hits an
  unexpected nesting depth.
- **`bk_expr` references L cols only** (the `t.x IN (SELECT t.y FROM
  u)` case): after substitution, `inBodyKey` references L cols, making
  the mark predicate `(L_col = L_col)` — trivially true. Translate
  should catch this earlier; flag if it reaches the peel.
- **Project's output col referenced both by `inBodyKey` AND by
  `Apply.filter`**: substitute consistently in both places via the
  same map.
