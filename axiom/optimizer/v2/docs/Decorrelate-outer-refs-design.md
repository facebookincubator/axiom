# Decorrelate — Outer-Reference Resolution Design

## The gap this addresses

Today's `v2/DecorrelatePass.cpp` pulls correlation **predicates** out (via
`PushDown.runFilter`) and builds a recovery skeleton for **Aggregate**
body shapes. It does NOT resolve outer-column references that appear in
body **expressions** outside of those two contexts:

- `Project` expression list (e.g., `SELECT outer_a + 1`)
- `Aggregate` function arguments (e.g., `SELECT max(u.a + t.b) FROM u`)
- `Aggregate` FILTER condition (e.g., `SELECT count(*) FILTER (WHERE outer_x)`)
- `Sort` key expressions
- `Window` partition / order / arg expressions
- `Join` ON-condition subterms
- `Unnest` expressions

These outer references survive into the body's expressions after
decorrelate runs. At Velox emit time, the body becomes a self-contained
subtree (the right side of `Apply→Join`), and Velox resolves column
references by NAME against each operator's input schema. An outer-ref
that's not in the body's input schema produces:

- `Field not found: <name>. Available fields are: …` (Velox can't
  resolve the reference)
- or, masked today by an aggressive `Project` passthrough that leaks
  outer columns up the body chain so they appear in `body.outputColumns`
  by coincidence — which in turn causes
  `Duplicate column name found on join's left and right sides: <name>`
  when the leaked outer column collides with L's column at the
  collapsed Join.

Five tests in the SQL suite hit this category today. More will appear
as we implement more body shapes (Window, Join in body, etc.) without
fixing the underlying gap.

## Scope of this design

Specify the algorithm — including IR shapes, column lifecycle, and
schema invariants — for decorrelating any correlated `Apply` whose body
references outer columns in any expression position. Verify the algorithm
against concrete walkthroughs of representative body shapes before
implementing.

Out of scope: NYI per-operator body handlers (`Window`, `Limit`, `Sort`,
`Unnest`, multi-leaf `Join` / `UnionAll`) — each will follow the same
algorithm when added; specifics are left to those handlers.

## Mechanism

The paper's algorithm (Neumann 2024, §3.3): **D-join push-through with
expression substitution**. The dependent join (`L ⋈D body`) pushes down
through each body operator, and at each level the operator's expressions
that reference outer columns get substituted to reference columns the
body now produces (post-push).

At the leaves, the D-join becomes a regular relational join with L.
Above the leaves, every operator sees L's columns in its input schema
and outer-ref expressions resolve naturally.

### v2 instantiation

A single per-Apply tagged outer node:

```
tagged_L = AssignUniqueId(L)
```

is built once at `Decorrelator::rewriteApply` for correlated Applies.
It carries L's columns plus an `__rownum` id (BIGINT, unique per L row),
serving two roles:

1. The "L side" of the materialization join at the body leaf
2. The grouping key for the per-outer collapse at Apply collapse

The body is then rewritten via two coordinated passes:

**Pass 1 — PushDown** (current behavior, mostly unchanged):
- Walks the body bottom-up
- `Filter`: pulls correlated predicates up into `correlationFilter`;
  non-correlated predicates either stay as Filter (current) OR rewrite
  to `Project(p AS _kept)` (current)
- `Aggregate`: builds the recovery skeleton (current — to be extended
  to use the shared `tagged_L` from above rather than building its own)
- `Project`: rebuilds with passed-through schema (current)

Returns `(body_pushed, correlationFilter, kept, recovered)`.

**Pass 2 — leaf wrap + Apply collapse** (new, non-recovered correlated only):
- Walks `body_pushed` to find its single leaf (`Scan` / `Values`)
- Replaces the leaf with
  `Join(tagged_L, Project(leaf, _include := true), kLeft, filter=correlationFilter)`
- Walks back up rebuilding each ancestor operator, extending each
  `Project`'s output schema to include the new columns (`L.cols`,
  `__rownum`, `_include`) so they survive to the top
- At Apply collapse, builds a per-outer `Aggregate` (GROUP BY `[__rownum, L.cols]`):
  - `kLeft` scalar: `arbitrary(IF(_include AND AND(kept), apply.resultColumn, NULL))`
  - `kLeftSemiProject`: `bool_or(_include AND AND(kept))`
- Final `Project` shapes the collapsed output to `apply.outputColumns`

Multi-leaf body (`Join`, `UnionAll`, nested `Apply` in body): NYI —
sharing `tagged_L` across multiple leaves would create a DAG that v2
IR doesn't support; cloning would produce distinct ids and break the
per-outer collapse.

### Substitution map (Column*-identity in v2)

The paper's substitution map is `outer_column → body_visible_expr`. In
v2 IR, columns are identified by `ColumnCP` pointers, and Velox emit
resolves references by NAME against each operator's input schema.

**Key insight:** v2's leaf wrap uses `tagged_L` whose `outputColumns`
include L's columns by the SAME `ColumnCP` identities. After the leaf
wrap, the body's leaf is `Join(tagged_L, …)` whose outputs include
those exact L columns by name. Expressions higher up in the body that
reference outer L columns (by Column* / by name) resolve against this
joined input schema.

Substitution is therefore **identity at the Column* level** — no
expression rewriting is needed for normal correlated references. The
work is the schema wrapping (leaf wrap), not the expression walk.

**Exception — naming collisions:** if `L.outputColumns` and `body's
leaf.outputColumns` share a column name (different `Column*`, same
name), `Join(L, leaf)` produces a duplicate name and Velox rejects.
This requires renaming, which IS substitution: replace body-side
column refs by name with a fresh alias.

In practice, naming collisions arise from translate's handling of
identity-passthrough subqueries: `(SELECT a) FROM t` translates body
to `Project(NoFromValues, [outer_a], [outer_a])` where the inner
Project's output column IS the outer's `t.a` Column* (translate's
identity-Project elision). Apply's `resultColumn` = outer `t.a`,
which then appears in Apply.outputColumns twice (once from L, once
as resultColumn).

This is a TRANSLATE-side issue that should be fixed independently:
when `liftSubquery`'s body output column is already in `applyTarget`'s
output schema, wrap the body in an aliasing Project that exposes a
fresh column. With that translate fix, naming collisions don't reach
Decorrelate. (See "Open issue" below.)

## Walkthroughs

For each shape: lp form → v2 translate output → Decorrelate steps →
Velox-emit-able plan. Verify schema correctness at every step.

Notation:
- `t` = outer relation with column `t.a`
- `u` = inner relation with column `u.a` (and possibly more)
- Column identities written as `t.a`, `u.a` (the actual ColumnCP); names
  given when distinct from identity
- `Apply.outputColumns = input.outputColumns + apply.resultColumn` (per
  v2 IR contract)

### Shape A — uncorrelated scalar (baseline; no change)

```sql
SELECT (SELECT max(u.a) FROM u) FROM t
```

lp body: `Aggregate(Scan(u), [max(u.a) AS m])`.

v2 translate emits `Join(kInner, t, EnforceSingleRow(Aggregate(...)))`
directly — uncorrelated scalars bypass Apply entirely. No decorrelate
work needed.

✓ Already works. No change.

### Shape B — correlated scalar over Filter(Scan), no outer ref in expressions

```sql
SELECT (SELECT u.b FROM u WHERE u.a = t.a) FROM t
```

lp body: `Project([u.b], Filter(u.a = t.a, Scan(u)))`.

v2 translate: `Apply(kLeft, t, body, correlations=[t.a], resultColumn=u.b)`.

**PushDown pass** (mostly current behavior):
- `runProject([u.b], …)` — recurses into Filter
- `runFilter(u.a = t.a, …)` — predicate references outer t.a → pulled into `correlationFilter`. Recurses into Scan(u). Returns `(Scan(u), u.a=t.a, {}, false)`. Filter handler returns `(Scan(u), u.a=t.a, {}, false)` (predicates all pulled, no Filter rebuilt).
- `runProject` returns `(Project(Scan(u), [u.b], [u.b]), u.a=t.a, {}, false)`.

**Apply collapse** — non-recovered correlated, new path:
- `tagged_L = AssignUniqueId(t)`, output `[t.a, __rownum]`
- `injectLeafWrap` walks body, finds Scan(u) leaf:
  - `marked_u = Project(u, [u.a, _include := true])`, output `[u.a, _include]`
  - `wrap = Join(tagged_L, marked_u, kLeft, filter=u.a=t.a)`, output `[t.a, __rownum, u.a, _include]`
- Walking up: `Project([u.b], wrap)` — but wait, `u.b` referenced; need it in scope. The original body had `Project(Filter(Scan(u)), [u.b])`, so the leaf is Scan(u) which has `[u.a, u.b]` (both cols). After leaf wrap, `wrap.outputColumns = [t.a, __rownum, u.a, u.b, _include]`. Project([u.b]) above resolves.
  - Project's outputs need to passthrough `__rownum, t.a, _include` so they survive to Apply collapse: outputs become `[u.b, __rownum, t.a, _include]`. Exprs: `[u.b, __rownum, t.a, _include]` (all passthrough columns).
- Per-outer Aggregate: GROUP BY `[__rownum, t.a]`, agg `arbitrary(IF(_include, u.b, NULL))`.
  - Output: `[__rownum, t.a, u.b]`.
- Final Project: `[t.a, u.b]` matching `apply.outputColumns`.

Velox emit-time schema check:
- Scan(u): `[u.a, u.b]` ✓
- Project(marker): input `[u.a, u.b]`, output `[u.a, u.b, _include]` ✓
- Join(kLeft, tagged_L, marked_u, filter=u.a=t.a):
  - tagged_L (left) cols: `[t.a, __rownum]` (names: "a", "__rownum")
  - marked_u (right) cols: `[u.a, u.b, _include]` (names: "a", "b", "_include")
  - **NAME COLLISION**: both sides have column named "a" (t.a is "a", u.a is "a")
  - Velox rejects ✗

This shape hits the duplicate-name problem at the leaf wrap. Need to
rename one side's "a" before the join.

**Resolution:** at leaf wrap construction, detect name collisions
between `tagged_L.outputColumns` and `marked_leaf.outputColumns`. For
each collision, alias one side via a renaming Project. By convention,
rename body-side (leaf side) — the outer's names are user-facing and
shouldn't be modified.

Continuing with renaming applied:
- `marked_u_renamed = Project(marked_u, [u.a AS u_a_renamed, u.b, _include])`, output `[u_a_renamed, u.b, _include]`
- BUT: the `u.a` in the `u.a = t.a` filter is the SAME Column* as `marked_u`'s u.a, and Velox resolves by name — after rename, the filter's "u.a" would not resolve. Need to substitute.
- This is the substitution map at work: rename `u.a` → `u_a_renamed` in the join filter expression.

This case needs both:
1. The leaf-wrap renaming Project on the body side
2. An expression-level substitution of body-side column refs that got renamed (applied to the join filter)

The expression substitution is narrowly scoped: it only affects expressions that reference body-side columns that were renamed during leaf wrap. Other expressions (referencing outer columns, referencing body columns that weren't renamed) pass through unchanged.

### Shape C — `(SELECT a) FROM t` (the failing l317)

```sql
SELECT (SELECT a) FROM t
```

lp body: `Project(NoFromValues, [outer_t.a])` — the inner Project's
single expression resolves to outer's t.a; translate's identity-Project
elision makes Project's outputColumn the SAME `t.a` Column*.

v2 translate: `Apply(kLeft, t, body=Project(NoFromValues, [t.a], [t.a]), correlations=[t.a], resultColumn=t.a)`.

**The original sin** — `resultColumn = t.a` is the SAME column already
in `applyTarget=t`'s outputColumns. So `apply.outputColumns = [t.a, t.a]`
(duplicate). This is a translate-side bug that no amount of Decorrelate
work can paper over — Apply's claimed output schema is malformed at
construction time.

**Translate-side fix** (separate from Decorrelate):

In `liftSubquery`, after computing `resultColumn = bodyOut[0]`, check
if `resultColumn` is already in `applyTarget->outputColumns()`. If so,
allocate a fresh aliasing column and wrap body in
`Project(body, [bodyOut[0]], [fresh_alias])`. Apply's `resultColumn`
becomes `fresh_alias`.

With this fix, `apply.outputColumns = [t.a, fresh_alias_for_a]`, no
duplicate. The Decorrelate algorithm proceeds normally.

**Known design gap:**
The Translate-side fix is design-correct but **not sufficient**.
Decorrelate's Project peel (Rule A in
`Decorrelate-project-rules.md`) peels the alias wrap, exposing the
un-aliased originalBody underneath. The resulting inner Apply has
`outputColumns = input.cols ++ originalBody.cols` = duplicate. The
terminus / lifted-Project chain propagates the duplicate to a
Velox Project that fails `PlanConsistencyChecker`.

Full Shape C correctness needs Decorrelate's Project peel to apply
the same aliasing trick recursively when constructing the inner
Apply. See `Decorrelate-project-rules.md` §"Known design gap —
Shape C composition" for the proposed completion.

**Decorrelate after translate fix:**

Body = `Project(Project(NoFromValues, [t.a], [t.a]), [t.a], [fresh_a])`.

- PushDown: nested Projects, both pass-through (no Filter to pull). Returns body unchanged.
- Apply collapse non-recovered:
  - `tagged_L = AssignUniqueId(t)` → `[t.a, __rownum]`
  - `injectLeafWrap` walks body, finds NoFromValues leaf:
    - `marked_leaf = Project(NoFromValues, [_include := true])`, output `[_include]`
    - `wrap = Join(tagged_L, marked_leaf, kLeft, filter=null)`, output `[t.a, __rownum, _include]`
    - No name collision (NoFromValues contributes no columns named "a")
  - Walking up: inner Project `[t.a]` over wrap — resolves ✓. Extends output schema: `[t.a, __rownum, _include]` (no addition since t.a, __rownum, _include all present).
  - Outer Project `[fresh_a]` over inner — resolves (fresh_a = expr referencing t.a via the inner Project's t.a output). Extends output: `[fresh_a, __rownum, t.a, _include]`.
- Per-outer Aggregate: GROUP BY `[__rownum, t.a]`, agg `arbitrary(IF(_include, fresh_a, NULL))`, output `[__rownum, t.a, fresh_a]`.
- Final Project: `[t.a, fresh_a]` matching `apply.outputColumns`.

Velox schema check at each operator: ✓ no duplicates, all refs resolve.

### Shape D — correlated semi (EXISTS) over Filter(Scan)

```sql
SELECT EXISTS (SELECT 1 FROM u WHERE u.a = t.a) FROM t
```

lp body: `Project([1], Filter(u.a = t.a, Scan(u)))` (or similar — the
projected value is irrelevant for EXISTS).

v2 translate: `Apply(kLeftSemiProject, t, body, correlations=[t.a], resultColumn=fresh_mark)`.

**Decorrelate:**

- PushDown pulls `u.a = t.a` to correlationFilter, returns body's Project unchanged.
- Apply collapse non-recovered semi:
  - `tagged_L = AssignUniqueId(t)`, `wrap = Join(tagged_L, marked_u, kLeft, filter=u.a=t.a)`.
  - (Same name-collision issue as Shape B if u.a and t.a both named "a" — needs rename.)
  - Per-outer Aggregate: GROUP BY `[__rownum, t.a]`, agg `bool_or(_include)`, output `[__rownum, t.a, fresh_mark]`.
  - Final Project: `[t.a, fresh_mark]` matching `apply.outputColumns`.

Verifies: for unmatched outer rows, the LEFT JOIN pads `_include = NULL`,
`bool_or(NULL) = NULL` which resolves to false in BOOLEAN context. Mark
is false. ✓

For matched outer rows, `_include = true`, `bool_or(true) = true`. Mark
is true. ✓

### Shape E — correlated scalar over Aggregate(Filter(Scan)) — count(*)

```sql
SELECT (SELECT count(*) FROM u WHERE u.a = t.a) FROM t
```

Currently working. Recovery path. Walkthrough for completeness, to
verify the new design keeps it working:

lp body: `Aggregate([count(*)], Filter(u.a = t.a, Scan(u)))`.

v2 translate: `Apply(kLeft, t, body, correlations=[t.a], resultColumn=count_result)`.

**Decorrelate** (uses recovery, sharing tagged_L):

- `tagged_L = AssignUniqueId(t)` at Apply level (already in place).
- PushDown.runAggregate:
  - Recurses into Filter → pulls `u.a = t.a` into `correlationFilter`. Returns `(Scan(u), u.a=t.a, {}, false)`.
  - Builds recovery using `tagged_L` (no longer builds its own):
    - `marked_u = Project(u, _include := true)`
    - `joined = Join(tagged_L, marked_u, kLeft, filter=u.a=t.a)` — **same name-collision issue**.
    - Recovery Aggregate: GROUP BY `[__rownum, t.a]`, agg `count() FILTER (_include) AS count_result`, output `[__rownum, t.a, count_result, _matched]`.
  - Returns `(recovered, null, {}, true)`.
- Apply collapse recovered (existing path, mostly): final Project `[t.a, count_result]` matching `apply.outputColumns`. ✓

Name-collision concern same as Shapes B/D — the recovery's LEFT JOIN
needs the rename too. Currently masked because t and u happen to have
different actual column identities in the test set; would surface for
queries where they share names.

### Shape F — correlated scalar over Filter(Aggregate(...)) — HAVING

```sql
SELECT (SELECT count(*) FROM u WHERE u.a = t.a HAVING count(*) > 0) FROM t
```

Currently working. Recovery + post-aggregate Filter folded as IF.

(Mostly same as Shape E, with the addition of the post-aggregate
Filter rewritten to `IF(having_predicate, count_result, NULL)` at the
final Project. No new issues for this design.)

### Shape G — correlated scalar over Project(Aggregate(...)) with outer ref in Project expr

```sql
SELECT (SELECT count(*) + t.a FROM u WHERE u.a = t.a) FROM t
```

This is the next layer of the structural gap — currently fails because
the Project's expression `count(*) + t.a` references outer `t.a` which
isn't in the body's input scope after recovery.

lp body: `Project([count_star + t.a], Aggregate([count(*)], Filter(u.a = t.a, Scan(u))))`.

v2 translate: `Apply(kLeft, t, body, correlations=[t.a], resultColumn=sum_result)`.

**Decorrelate:**

- PushDown:
  - runProject recurses into Aggregate.
  - runAggregate builds recovery (Shape E behavior). Returns `(recovered, null, {}, true)`. `recovered.outputColumns = [__rownum, t.a, count_result, _matched]`.
  - Back in runProject: `Project([count_star + t.a], recovered)`. The expression references `t.a` — does it resolve against `recovered`?
    - `recovered.outputColumns` includes `t.a` (from `tagged_L`'s groupingKeys passthrough). ✓
  - Project's outputs need passthrough so downstream (Apply collapse) gets `__rownum`, `_matched`, `t.a`: outputs become `[sum_result, __rownum, t.a, _matched]`.
- Apply collapse recovered:
  - Final Project: `[t.a, sum_result]` matching `apply.outputColumns`. ✓

Velox schema check:
- recovered output: `[__rownum, t.a, count_result, _matched]` — t.a is in scope.
- Project: expr `count_star + t.a`, input has both — ✓.

Shape G ACTUALLY WORKS today via the recovery path + Project passthrough, BECAUSE the recovery's groupingKeys carry L.cols (`t.a` is in `tagged_L.outputColumns` which the recovery joins+groups by). The passthrough then propagates `t.a` to the Project's output, and Apply collapse uses it.

The failure is in shape C — non-Aggregate body. The recovery doesn't fire there.

## Summary of decisions

1. **Tagged_L is shared** between Aggregate-recovery and (when added) leaf wrap. Built once at Apply level.
2. **Leaf wrap** for non-recovered correlated bodies: replace single leaf with `Join(tagged_L, marked_leaf, kLeft, filter=correlationFilter)`. NYI multi-leaf bodies (substrate gap: needs DAG support).
3. **Naming-collision rename** at leaf wrap (and at recovery LEFT JOIN): when L.outputColumns and body-side outputColumns share a column name (different ColumnCP), insert an aliasing Project on the body side and substitute body-side column refs in any expression that survives past the rename point (notably the join filter).
4. **Project passthrough through body chain**: must extend Project's output schema with the new columns introduced at the leaf wrap (`__rownum`, `L.cols`, `_include`) so they survive to Apply collapse. Same mechanism used by the current passthrough; the change is making it bounded (only the recovery / leaf-wrap-introduced columns, not arbitrary inner columns).
5. **Per-outer Aggregate at Apply collapse** for non-recovered:
   - `kLeft` scalar: `arbitrary(IF(_include AND AND(kept), apply.resultColumn, NULL))`
   - `kLeftSemiProject`: `bool_or(_include AND AND(kept))`
6. **Translate-side fix for identity-projection collision** (`(SELECT a) FROM t`): in `liftSubquery`, when `bodyOut[0]` is already in `applyTarget->outputColumns()`, wrap body in an aliasing Project and use the alias as `resultColumn`. This eliminates the Apply.outputColumns duplicate independent of any Decorrelate work.

## Open issue — translate-side fix vs Decorrelate-side rename

The naming collision arises in two places:
- Apply.outputColumns has L.cols + resultColumn, where resultColumn IS an outer column (Shape C). Fix: translate-side rename in `liftSubquery`.
- Leaf wrap / recovery LEFT JOIN has `tagged_L.cols` + `body_side.cols` with shared names (Shapes B, D, E). Fix: Decorrelate-side rename at join construction.

These are independent and BOTH need fixing. The translate-side fix is small and tractable. The Decorrelate-side rename is a slightly bigger change because it requires substitution into the join filter (only column refs that got renamed need substituting — narrow scope).

## Implementation order

1. Translate-side rename in `liftSubquery` (Shape C). Independent, no Decorrelate change. ~10 lines + comment.
2. Decorrelate-side rename at recovery LEFT JOIN (affects existing recovery; verifies the rename mechanism doesn't regress current 632/50). ~30 lines.
3. Hoist `tagged_L` construction to Apply level; pass to PushDown (already attempted; verified to maintain 632/50 in isolation). ~20 lines.
4. Apply-collapse non-recovered leaf-wrap path + per-outer Aggregate (Shapes B, D). ~100 lines.
5. Bounded passthrough in Project handler (only `tagged_L.outputColumns + _include`, not arbitrary). ~20 lines.
6. Decorrelate-side rename at leaf wrap, with filter-expression substitution (Shapes B, D, E refinement). ~40 lines.

Each step verified to maintain or improve the test pass count before
moving to the next. Total: ~220 lines net, in 6 commits.

## What this design does NOT solve

- **Multi-leaf bodies** (Join, UnionAll, nested Apply in body): NYI per
  substrate gap. Solving requires v2 IR DAG support OR per-leaf cloned
  `tagged_L` (with the cloned ids breaking per-outer aggregation —
  acceptable only if the body shape has no Aggregate above).
- **Window in correlated body**: own per-operator recovery, paper §4.4.
  Outer-ref resolution above follows the same leaf-wrap mechanism once
  the Window handler is added.
- **Sort + Limit in correlated body**: per-outer ORDER BY / LIMIT needs
  window-function rewrite, paper §4.4.
- **Outer refs in `Aggregate` FILTER condition** (e.g., `count(*)
  FILTER (WHERE outer_x > 0)`): the Filter condition is on a body
  aggregate. After leaf wrap or recovery, outer cols are visible in
  the input — should work, but needs walkthrough to verify.
