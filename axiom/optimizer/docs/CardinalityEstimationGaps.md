# Cardinality Estimation: Known Gaps

This document catalogs known flaws and missing pieces in how the optimizer
estimates statistics, and the direction we intend to take for each. It is a
companion to [CardinalityEstimation.md](CardinalityEstimation.md), which
describes how estimation works today; this doc is about where it is wrong and
what "right" looks like.

Most gaps are presented as **Current** (what the code does), **Why it's wrong**,
and **Target** (the intended design); gap 5 is a structural constraint and is
framed differently. Two mature engines solve these problems and are used as
reference throughout: Presto and DuckDB. Their counterparts are summarized at the
end.

## 1. Statistics are stored on the expression, not per operator

**Current.** Every `Expr` carries a `Value` holding statistical estimates
(`cardinality`/NDV, `min`/`max`, `nullFraction`). A `Column`/`Call` node is
deduplicated, so it has exactly one `Value`, fixed at query-graph construction.
Per-operator narrowing lives separately, in each operator's constraint map
(`constraints_`), keyed by expression id.

**Why it's wrong.** Statistics are position-dependent: the same column has a
different NDV before and after a selective filter. A single fixed `Value` on the
shared `Expr` cannot represent that — only the per-operator constraint map can.
So `Expr::value()` is, at best, the original (leaf) estimate, and is stale
anywhere downstream of a filter or join.

**Target.** An expression is position-independent (what it computes plus its
type); its statistics are position-dependent and belong in the per-operator
stats layer. `Expr` should carry only `type` (a `Literal` additionally carries
its constant value, from which its stats are trivially derivable); all
estimates are derived per operator. This is the generalized form of the existing
`Schema.h` TODO ("separate schema facts from statistical estimates") and matches
both reference engines, which compute per-node statistics from input statistics
rather than storing them on the expression.

## 2. Computed-expression NDV is fabricated

**Current.** A function call's result NDV is taken as the maximum of its
argument NDVs (`estimateCallCardinality` in `ToGraph`).

**Why it's wrong.** `max-of-args` is not a sound bound. For a deterministic
function, each distinct combination of argument values maps to at most one
result, so the result NDV is bounded by the *product* of the argument NDVs, not
the maximum. `max` equals that product only when the arguments are fully
dependent (one functionally determines the others); when they are independent it
under-counts. It is also far too loose for small-codomain functions
(`mod(x, 3)` has at most a handful of distinct values regardless of `x`'s NDV) —
the under-count there inflates equality selectivity (`1/NDV`) and badly
under-estimates row counts.

**Target.** For a deterministic call, default the result NDV to the
**saturating product** of the argument NDVs (capped at the row count where
consumed). This is the sound upper bound; it assumes argument independence and is
loose under correlation, which is acceptable for a bound. Independently, the
result **type** already caps NDV regardless of function — boolean ≤ 2,
`tinyint` ≤ 256, etc. — applied by `clampCardinality`; this is not a per-function
concern. Layer **per-function overrides** on top only for functions whose
codomain is known more precisely than the type (e.g. `mod` ≤ the modulus
magnitude). The handful of
non-deterministic functions (`rand`/`random`, `uuid`, `shuffle`) get explicit
rules too (`rand`/`uuid` → unique per row, i.e. NDV = row count; `random(n)` →
≤ `n`; `shuffle` → input NDV), so no expression is ever left unknown.

## 3. Cost-relevant consumers read the fixed, stale value

**Current.** Some estimators read the per-operator constraint map (filter
selectivity, aggregation group count), but several read `expr->value()`
directly — most importantly **join fanout** (`QueryGraph` `joinKeyFanout`), and
also order/index-lookup selectivity and unnest fanout. (Join *sampling* also
reads the fixed NDV, but only to choose its sampling fraction; its fanout is
measured from data, not estimated, so a stale NDV there only affects sample
size.)

**Why it's wrong.** Reading the fixed `Value` ignores upstream narrowing. A join
on a heavily filtered column is costed with the column's *original* (pre-filter)
NDV, so its fanout is systematically wrong. This is a direct consequence of
gap 1.

**Target.** Once statistics live in the per-operator layer (gap 1), all
cost-relevant consumers — join fanout included — must read the flow-adjusted
statistics for the point in the plan where they apply, not the fixed expression
value.

## 4. Aggregate-output NDV is fabricated as 1

**Current.** An aggregate result column (`sum`, `count`, …) gets its `Value`
from `toConstantValue`, i.e. NDV 1. `Aggregation::initConstraints` only caps it
at the group count (`min(1, groupCount) = 1`), so it stays 1.

**Why it's wrong.** An aggregate produces one value per group, so its output
column generally has many distinct values, not one. A downstream join keyed on
an aggregate output therefore sees NDV 1 and computes a wildly wrong fanout.

**Target.** An aggregate emits one value per group, so its output NDV is bounded
by the group count: default to `min(groupCount, …)` instead of 1. Refine per
aggregate where the codomain is known (`min(x)`/`max(x)` ≤ `min(groupCount,
NDV(x))` and carry `x`'s range; `count` is a non-negative integer in
`[0, maxRowsPerGroup]`, NDV bounded by the number of distinct group sizes;
`avg(x)` range ⊆ `[min(x), max(x)]`). This is the aggregate counterpart of the
per-function refinement in gap 2.

## 5. An unknown cardinality forces a fallback to non-cost-based order

This is a deliberate design choice that is **demanding to live with**: Axiom
ranks complete plans by total cost, so a single unknown cost anywhere in a
derived table (DT) discards cost-based ordering for the *whole* DT — joins,
aggregation, and order-by fall back together — and the unknown propagates to
consuming DTs. The coarse granularity of that fallback is the gap.

**Behavior.** A DT's plan candidates are ranked by total plan cost, computed over
the complete plan for that DT — the joins plus post-processing (aggregation,
order-by) attached before a candidate enters the cost-ranked set. If any cost is
unknown, the candidates are incomparable and the DT is re-enumerated in syntactic
join order (`makeJoinsWithSyntacticFallback`). That DT's unknown output
cardinality then flows up to consuming DTs, which fall back too; input DTs are
unaffected. Today the trigger is a genuinely-missing base-table statistic (e.g. a
column with no NDV).

**Why it's demanding.** One unknown anywhere in the DT costs the whole DT its
cost-based plan, so every estimate the DT touches must be defined or join
ordering, aggregation, and the rest fall back together. Other engines fall back
at a finer grain: Presto's cost-based reordering operates on a `JoinNode`
multi-join *subtree*, so an aggregation above it — even if uncostable — does not
affect the subtree's join order; DuckDB substitutes a default (`input/2`) for a
computed grouping key rather than going unknown. Axiom's DT bundles the joins
*together with* the aggregation, which is intentional for the multi-worker case
(comparing two join distributions needs to know whether the aggregation can reuse
the partitioning to skip a shuffle) — but it makes the fallback coarse.

**Target.** Fall back at a finer grain — cost and rank sub-plans so an unknown
forces only the affected piece off cost-based ordering — and/or substitute
bounded defaults so unknowns are rare, making the optimizer tolerant of an
occasional unknown instead of discarding the whole DT.

## 6. Shuffle cost is included when planning single-node

**Current.** `PlanSet::addPlan` computes a shuffle margin
`mul(cardinality, shuffleCost(columns))` for every candidate and folds it into
the `bestCostWithShuffle` pruning cutoff, regardless of worker count.
Repartitions, however, are only inserted in multi-worker planning (every
`Repartition` is gated on `!isSingleWorker_`), and `best()` already guards
distribution handling with `!isSingleWorker()`.

**Why it's wrong.** A single-node plan never shuffles, so its cost should not
contain a shuffle term. Today the shuffle margin still inflates the
`bestCostWithShuffle` cutoff (the distribution-divergent comparison branches that
also use it never fire single-node, since all candidates share one
distribution), so single-node pruning uses a cutoff loosened by a shuffle that
will never happen.

**Target.** Gate the shuffle margin on multi-worker planning: single-node
candidates carry no shuffle cost, so their plan costs and pruning cutoff reflect
only work that actually runs.

## Reference: how Presto and DuckDB estimate this

Both engines estimate statistics only for a small **whitelist** of functions and
fall back to "unknown" (Presto) or a min/max-only/heuristic estimate (DuckDB)
elsewhere. The whitelists and their derivations are below.

### Presto

Source: `presto-main-base/.../cost/ScalarStatsCalculator.java` (expressions) and
`.../cost/AggregationStatsRule.java` (aggregation). The whitelist is a set of
**operators**, not named functions — a named function gets statistics only if it
constant-folds (then NDV 1); otherwise the result is `unknown`.

| Whitelisted | Result statistics |
|---|---|
| `negate` (unary `-`) | range flipped to `[-high, -low]`; NDV and null fraction preserved (`computeNegationStatistics`). |
| arithmetic `+ − × ÷ %` (`OperatorType.isArithmeticOperator` = ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULUS) | **NDV = `min(leftNDV × rightNDV, rows)`** (product, capped at row count); null fraction `nl + nr − nl·nr`. Range per operator: general case = min/max of the four corner combinations of the operand ranges; `%` → `[−d, d]` where `d = max(|rl|, |rh|)`, clamped by the dividend's sign; `÷` with a divisor range spanning 0 → `[−∞, +∞]` (`computeArithmeticBinaryStatistics`). |
| `cast` | pass through source stats; for an integral target, round low/high and cap NDV at `high − low + 1` (`computeCastStatistics`). |
| `coalesce` | fold operands left-to-right: range union; NDV = `left.NDV + min(right.NDV, rows × left.nullFraction)`; null fraction `left.nf × right.nf` (`estimateCoalesce`). |
| any function that constant-folds | becomes a constant → NDV 1. |
| everything else (generic functions) | **`unknown`**. |

Aggregation (`AggregationStatsRule`):
- Group count = `min(∏(grouping-key NDV + nullRow), input rows)` — product of
  grouping-key NDVs (plus 1 per nullable key) capped at input rows. No grouping
  keys → 1.
- Grouping-key output NDV preserved; null fraction reset.
- **Aggregate-result NDV → `unknown`** (`estimateAggregationStats` is a stub;
  `min`/`max`/`count`/`sum` are noted as TODO).
- Join reordering (`ReorderJoins`) is a separate rule scoped to a `JoinNode`
  multi-join *subtree*, with the aggregation outside it. So an unknown
  aggregation cardinality does not enter the subtree's cost, and the subtree
  still reorders from its (known) base-column stats. Presto is not magic about
  unknown, though: when a cost in the subtree itself *is* unknown
  (`cost.hasUnknownComponents()`), the enumeration returns `UNKNOWN_COST_RESULT`
  and falls back to heuristic order (`EliminateCrossJoins`) — the same
  fall-back-on-unknown as Axiom, just at a finer granularity (see gap 5).

### DuckDB

DuckDB keeps two layers: per-expression `BaseStatistics` (min/max, null-ness, and
a leaf distinct-count) used for pruning/folding, and separate join-order
cardinality estimation. **NDV is never propagated through functions or
arithmetic** — only min/max/null are; NDV is meaningful only at leaves.

Whitelisted via a per-function `function_statistics_t` callback (all derive
min/max/null, **none set NDV**):

| Whitelisted (callback) | Result statistics |
|---|---|
| `date_part` (incl. `year`), `date_trunc` | min/max of the extracted part from the source range. |
| `abs` | non-negative range from the source range. |
| arithmetic `+ − *` | interval arithmetic on min/max; unknown on overflow / missing bounds. |
| string functions: `length`, `substring`, `upper`, `lower`, `instr`, `like`, `ilike` | min/max / null derived per function. |
| struct/list `extract`, `pack` | field/element stats. |
| `cast` | pushed through when order-preserving. |
| `coalesce`, `case` | union of branch ranges + null-ness. |
| no callback | monotone corner-evaluation for a numeric, monotonicity-annotated function; otherwise **unknown**. |

Aggregation (`RelationStatisticsHelper::ExtractAggregationStats`): handles only
grouping keys that are columns coming directly from a scan, which carry the
scanned column's NDV; a
**computed** grouping key is skipped, and the group count falls back to
`input_cardinality / 2`. So a computed grouping key never collapses estimation —
it degrades to a heuristic.

### Takeaways for Axiom

1. Both engines **selectively** estimate stats for a small whitelist; for an
   expression outside it the fallback is `unknown` (Presto) or min/max-only with
   no NDV (DuckDB). Neither pretends to cost-optimize on an unknown.
2. When a *needed* statistic is unknown, every engine — including Axiom — falls
   back to a non-cost-based order. The differences are the **granularity** of
   that fallback and whether the engine **bails or substitutes**:
   - Presto: cost-based reordering is scoped to a `JoinNode` multi-join subtree;
     an unknown cost there yields `UNKNOWN_COST_RESULT` → heuristic order
     (`EliminateCrossJoins`), localized to that subtree. An aggregation *above*
     the subtree is out of scope, so agg-over-join still cost-reorders;
     join-over-agg (the agg's unknown is a join input) falls back.
   - DuckDB: **substitutes** a heuristic estimate (`input/2`) for a computed
     grouping key, so it keeps cost-optimizing rather than bailing.
   - Axiom: the fallback unit is the whole **DT**, which bundles the joins with
     the post-processing aggregation, so an unknown aggregation cardinality drags
     the DT's joins to syntactic order and propagates up to consuming DTs
     (gap 5).
3. Presto derives **NDV** for arithmetic (product, capped at rows); DuckDB
   derives only **min/max**, never NDV.
4. Both keep statistics in a per-node / per-operator structure, not fixed on the
   expression.

In order of importance, **gap 1** comes first: moving statistics into the
per-operator layer so they track the plan. It is the root — a single fixed value
on the `Expr` is wrong wherever a column flows past a filter or join, and it is
what makes the stale reads in gap 3 possible. Refining the per-expression and
per-aggregate estimates (gaps 2 and 4) is next: a better formula only pays off
once the statistics it feeds actually flow.
