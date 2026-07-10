# TPC-H Plan Comparison: v1 vs v2 — active gaps

*Query-by-query comparison of the executable plans v1 and v2 produce for the 22 TPC-H queries, trimmed to the queries where v2 still differs. Queries where v2 matches v1 (q1, q4, q6, q7, q12, q14, q18, q21, q22 — locked with matchers in `TpchPlanTest`; q18 matches in substance via dynamic filtering, see the DF table; q5 matches v1 in substance; q7's v2 plan is optimal and matches v1's B&B plan — v1's default greedy fallback regresses it ~12x, which is why the baseline forces B&B, see Methodology). Query texts live in `axiom/optimizer/tests/tpch/queries/`.*

**Scope.** Plan shape (operators, join order, join type, build/probe side). Where a row says **(measured)**, it carries runtime row counts from `EXPLAIN ANALYZE` on sf1; the rest is structural reasoning, not benchmarked.

## Caveat: locked (TestConnector) plans can differ from Hive plans

`TpchPlanTest` plans against `TestConnector::addTpchTables`, which injects **exact, synthetic** TPC-H stats. Real Hive stats differ (HLL-estimated NDVs, and no NDV at all for computed columns), and the join order can flip between the two. So a green matcher reflects the optimizer's behavior under clean stats — **not necessarily the plan produced on real Hive data.** Each query here is therefore also cross-checked against Hive via the CLI.

**q15 is a verified — but benign — example.** With exact stats v2 applies the selective `total_revenue = max` filter first (`supplier ⋈ (revenue ⋈ max)`); on Hive it joins `supplier` first (`(revenue ⋈ supplier) ⋈ max`). Verified by injecting Hive's `lineitem` `SHOW STATS` values into `TestConnector` — the locked plan flips to exactly the Hive order, so stats are the cause. Mechanism (inferred): Hive's `SHOW STATS` reports the computed `total_revenue` NDV as 1, and Axiom turns a missing/underivable NDV into 1, collapsing the `= max` selectivity to ~1 (filter removes nothing) so the max join no longer looks worth applying first.

**The two q15 orders are runtime-equivalent** (measured, sf1 `EXPLAIN ANALYZE`): the dominant cost is the revenue aggregation — scan+filter ~226k `lineitem` rows, aggregate to 10k — computed twice either way. The reordered joins are trivial: the `= max` equality plus dynamic filtering of the 1-row `max` collapses the supplier-side join to **1 row** regardless of order, so "supplier-first" does not materialize a 10k intermediate. So the flip is a near-tie tipped by a small estimate difference, not a quality regression. It illustrates that locked plans can diverge from Hive, but here the divergence is immaterial.

## Methodology

**Always compare against v1's *best* plan, not its default.** v1 falls back to a greedy heuristic at >=5 tables; that greedy plan can be much worse than the branch-and-bound plan v1 is capable of (q7: ~12x). Comparing v2 against v1-greedy flatters v2. So **disable the greedy fallback for every v1 run** (`SET SESSION optimizer.greedy_join_threshold = 2147483647;`) and treat that B&B plan as the bar. The `measure_baseline.py` script does this automatically; do it by hand for manual runs.

Both optimizers are driven through the Axiom CLI against on-disk TPC-H Parquet:

```bash
# v1 — greedy fallback disabled so the reference is v1's branch-and-bound plan
# (its best), not the greedy heuristic that kicks in at >=5 tables. Both engines
# use the same worker/driver count: 1/1 for the single-node baseline shown here.
SET SESSION optimizer.greedy_join_threshold = 2147483647;
EXPLAIN (TYPE EXECUTABLE) <query>   --data_path <tpch> --num_workers 1 --num_drivers 1
# v2
EXPLAIN (TYPE EXECUTABLE) <query>   --data_path <tpch> --v2 --num_workers 1 --num_drivers 1
# plan quality: runtime row counts + per-operator CPU:
EXPLAIN ANALYZE <query>   --data_path <tpch> [--v2] --num_workers 1 --num_drivers 1 --repeat 6 --print_timing
# optimizer speed: the Optimizing: phase from --print_timing (plain EXPLAIN, no execute):
EXPLAIN <query>           --data_path <tpch> [--v2] --num_workers 1 --num_drivers 1 --repeat 6 --print_timing
```

`EXPLAIN (TYPE EXECUTABLE)` is the only EXPLAIN form that works under both optimizers — `TYPE GRAPH`/`OPTIMIZED`/`IO` introspect v1-only state and error under `--v2`. For the single-node baseline both engines run one fragment / one driver, so the diff reflects optimizer choices rather than parallelism; the multi-node baseline runs both at the same worker count (see Multi-node). All v1 runs disable the greedy join-order fallback, so the baseline is the plan v1's branch-and-bound considers best. The reference is the optimal plan, not v1: where v1's greedy fallback would differ from its own B&B plan, comparing against greedy would compare against a known-suboptimal heuristic (q7 was exactly this — v1's default greedy plan is ~12x heavier than the B&B plan v2 also finds).

Verified CLI facts that the procedure below depends on:
- **v2 honors `--num_workers`/`--num_drivers`** and emits distributed multi-fragment plans at more than one worker, as does v1; run both at the same count. Multi-fragment `EXPLAIN ANALYZE` works for both.
- **`--repeat N` re-runs the full pipeline** (re-parse, re-optimize, re-execute) N times with **no warmup** — the first iteration is cold and printed like the rest, so discard it.
- **`--print_timing`** prints `Parsing: | Optimizing: | Executing: | Total:` per run; `Optimizing:` is the optimize-phase wall time (the optimizer-speed metric).
- **`EXPLAIN ANALYZE` prints no total CPU** — only per-operator `Cpu time:`. The plan-quality metric is their **sum**.
- Always build `@mode/opt` (debug timings are untrustworthy) and run on a **quiet machine** — timing is load-sensitive.

## Deciding whether a change is good: measure both axes, don't speculate

A cost-model change can shift a query off its locked matcher. A shape diff alone — different join order or build side — does **not** establish a regression: TPC-H join orders are frequently near-ties, and a locked matcher captures *one* optimal plan, not the only one. v1 is the bar on **two axes — plan quality and optimizer speed** (modularity must not silently cost plan quality or planning speed). Decide by **measuring** both, never by eyeballing the shape or reasoning alone.

**The trigger is `TpchPlanTest`.** Its `PlanMatcher` assertions fail when a query's plan *shape* changes (operators, join order, join type, build/probe side), so a cost-model change tells you which queries to measure — no separate plan-dump gate is needed. One caveat: `TpchPlanTest` plans against TestConnector's injected stats, so a green matcher does **not** guarantee the plan is unchanged on real Hive stats (see the caveat above); a cost-model change warrants a real-data spot-check via Steps 1–2 even when the matchers stay green.

**Step 1 — plan quality.** `EXPLAIN ANALYZE` on real data at **sf1 and sf10** (sf1 alone misleads — scale-sensitive effects only surface at sf10). Use `measure_baseline.py` (`measure` gathers samples, `present` formats them): it loops the runs, discards the cold first, and reports the **median** total CPU — the sum of per-operator `Cpu time` — with a trimmed noise **band%**. Compare v1 vs v2:
   - **Dominant-operator reduction** first — does the big-table scan keep its dynamic-filter reduction (e.g. `lineitem` 6M→Xk)? Losing it is the clearest regression.
   - **Median total CPU**. A Δ% larger than the band is a real difference; within the band it is a near-tie.

**Always check `band%` before trusting any delta.** It should be small (single digits). A large band (say >15%) means the measurement is unreliable — typically machine contention. Don't read a Δ% off a noisy row: re-measure on a quiet machine first. The script trims one hi/lo sample to absorb an occasional outlier, but a persistently large band still means something is off — investigate before drawing a conclusion.

**Step 2 — optimizer speed.** The `Optimizing:` wall time from `--print_timing` (the same `measure_baseline.py` run reports it), median + band, v1 vs v2. Largely scale-independent — sf1 is enough. For join-count stress and the enumerator fallback knobs (DPhyp budget, v1 greedy threshold) see [JoinEnumerationScaling.md](JoinEnumerationScaling.md).

**Step 3 — classify.** A **regression** measurably raises cost on either axis — most importantly, plan quality losing a dominant-table reduction. The rule is *no unexplained gap and no steep one*: a near-tie (overlapping bands) is not a regression; a real gap must be root-caused (the shape that `TpchPlanTest` flagged, and the plan itself, point at the cause), quantified, and dispositioned — close it, or defer with a stated reason; a steep gap is rework, not a waiver. Never revert-to-green, weaken the matcher, or fit the cost logic to the numbers.

**Non-estimable queries** — a filter with no usable selectivity (e.g. q9's `p_name like '%green%'`) — have no stable optimal plan; their numbers are informational, not pass/fail. Judge the estimable twin (q9_alt) instead, and carry no shape matcher for them (mirroring v1's `TpchPlanTest`).

**On commit**, update the per-query comparison below and the checked-in baselines for any query whose plan changed — with the measured numbers, not the shape diff.

## Cosmetic differences (present everywhere — ignore)

- v2 prints no per-node row estimates in `EXPLAIN (TYPE EXECUTABLE)`.
- Temporary projection names differ (`__p40` vs `dt1.__p43`).

## Classification

Legend: 🟡 minor difference, both plausible · 🟠 build/probe-side difference · 🔴 v2 produces a clearly worse shape. **Gaps** column cross-references the numbered gaps below; **M** = see Measured cost gaps; **DF** = depends on dynamic-filter pushdown (see the dynamic-filter table).

| Q | Verdict | Gaps | Notes |
|---|---------|------|-------|
| q13 | 🟡 | 2 | Right outer join + nested aggregation; build side and structure match v1. Two cosmetic rename Projects remain (identity `count` → `count` + trailing `count` → `c_count`). Shape locked with a matcher. |
| q20 | 🟡 | 1 | **(measured, sf1)** The availqty subquery decorrelates to shape (a) — `lineitem` grouped by `(l_partkey, l_suppkey)` and joined back to the forest partsupps — so the composite-key dynamic filter reduces the `lineitem` scan to **9,741** rows (aggregation → 5,843 groups); no `AssignUniqueId`, no blow-up. Residual gap vs v1: v2 does not push the outer `s_suppkey IN` keys into the partsupp subquery, so v1 reduces `partsupp` `800k → 32,960` (by supplier) → `342` (forest) and aggregates **433** `lineitem` rows, vs v2's `8,508` forest partsupps and `9,741` `lineitem` rows. Small and scan-bounded — both read `lineitem` and `partsupp` in full (~5–10% at sf10). Shape locked with a matcher. |

## Measured cost gaps (v2 vs v1-B&B)

From the checked-in baseline (`axiom/optimizer/tests/tpch/baseline.md`: total CPU, sf1/sf10, v1 greedy disabled, median of warm runs). A query is a **true regression** only where v2 is slower *and* the Δ% exceeds the noise band at **both** scales. Cross-referenced as **M** in the Classification table.

| Q | Δ (sf1 / sf10) | status |
|---|---|---|
| q2 | +13% / +39% | **Known.** v2's subquery `partsupp` reduces only `800k → 158,960` (supplier-side DF); v1 reaches `642` via a reducing semijoin (magic-set) v2 lacks. |

**v2 wins (faster beyond the band, both scales):** q18 (−24% / −24%), q10 (−17% / −20%), q21 (−10% / −9%), q9 (−17% / −15%, informational — non-estimable). On the optimizer-speed axis v2 is faster on **every** query (−47% to −95%).

q2 awaits semijoin reduction, not to be waved through (modularity must not silently cost plan quality).

## Multi-node (4 workers / 4 drivers)

v2 emits distributed multi-fragment plans that honor `--num_workers`/`--num_drivers`, so both engines were measured at 4 workers / 4 drivers, sf1 and sf10, same procedure as the single-node baseline (`measure_baseline.py --num-workers 4 --num-drivers 4`). Numbers are the checked-in `axiom/optimizer/tests/tpch/baseline_4w.md`; its Callouts section lists the regressions and unreliable (wide-band) rows.

**Optimizer speed** is unchanged from single-node — v2 is faster on every query (−44% to −92%).

**Plan quality** is parity or a v2 win on most queries (q16 −20% / −28%, q18 −57% / −35%, q10 −39% / −12%, q21 −33% / −27%). Regressions beyond the noise band:

- **q17 / q9_alt — dynamic filter lost across a repartition.** A reduction a probe-side dynamic filter provides single-node is lost once the probe is partitioned (the DF does not cross the exchange). q17 sf10 **+286%** (subquery `lineitem` runs 59,986,052 rows vs 61,385 single-node, verified), q17 sf1 +31%, q9_alt sf10 +22%. Resolved by distributed dynamic-filter pushdown (plan of record); see the Dynamic-filter dependence table below.
- **q2 +42% / +69%** — the single-node semijoin-reduction gap (see Measured cost gaps), wider under partitioning.

q20 (+13% / +11%) is within noise. **q11 is borderline / noise-dominated**, not a firm regression: sf1 +15% (6% band, right at the regression threshold), sf10 −5% within a wide 24% band. Latent risk if it firms up: v2 emits a pre-shuffle `PARTIAL` aggregation on the near-unique `ps_partkey` that reduces almost nothing yet adds cost, where v1 skips the partial and aggregates once after the `HASH(ps_partkey)` repartition (Velox's adaptive abandon-partial-aggregation does not rescue it — the `abandon_partial_aggregation_min_rows` gate, default 100k per driver per partial-flush, is not reached at ~20k rows/driver). Fix if it proves real: skip the pre-shuffle partial when the grouping key's estimated NDV ≈ input cardinality. Other bands too wide to read: q12 sf1 23%, q3 sf10 15% — measurement noise, not real differences.

## Dynamic-filter dependence (distributed-exec risk)

Several v2 plans match v1's reductions by relying on Velox's **dynamic filtering** — a hash-join build pushes a filter into the probe-side `TableScan` at runtime — rather than on a static reducing semijoin. **The plan of record is to build dynamic-filter pushdown for distributed execution** (single-node already works); these plans are therefore considered good, and this table tracks which queries depend on that work. The dependence is real: today's dynamic filters do not cross exchanges, so a partitioned probe loses the reduction (broadcast/replicated-build joins keep it in the probe's fragment and are unaffected). Now that v2 emits multi-fragment plans, this regresses at more than one worker — **measured** at 4 workers, q17's correlated-subquery `lineitem` scan runs the full table (+283% sf10) instead of the single-node 60M→61k reduction; see Multi-node. Until distributed DF pushdown lands, the reduction holds only single-node.

> **Note — a dynamic filter crosses an aggregation on its grouping key.** Some reductions below run *through* an aggregation: a join whose key is an aggregate's grouping key pushes its dynamic filter past the `HashAggregation` to the upstream `TableScan` (the scan shows `Raw Input` = full table but `Input` = only the join's keys' rows). This is how shape-(a) decorrelation restricts a correlated subquery's scan with no explicit reducing semijoin — but only when the cost model builds the outer side (a DF reaches only the **probe**), e.g. q2/q17 with their selective outers. When the outer is the larger (probe) side the body aggregate is built and fully scanned, and an explicit reducing semijoin would be needed instead. Mechanism (Velox): `HashAggregation` exposes grouping keys as identity projections (`HashAggregation.cpp`); `Driver::pushdownFilters` passes a filter through any identity-projection operator without checking its `canAddDynamicFilter()` (that is consulted only at the accepting `TableScan`); a plain aggregation is not a pipeline boundary (`LocalPlanner.cpp`). So "a DF can't cross an aggregation" is **false** for a grouping-key filter.

Measured single-node, sf1, `EXPLAIN ANALYZE`:

| Q | reduced scan | raw → out | filter source |
|---|---|---|---|
| q2  | `partsupp` (subquery) | 800k → 158,960 (v1: 642) | supplier-side DF only; v2 misses the part-filter reduction v1 gets via a semijoin — see Measured cost gaps (q2) |
| q5  | `lineitem` | 6M → 184k | `orders[date,ASIA]` build (orderkey) + `supplier[ASIA]` build (suppkey) |
| q8  | `lineitem` | 6M → 44k | `part[p_type]` build |
| q9  | `lineitem` | 6M → 319k | `part[like '%green%']` build |
| q10 | `lineitem`; `customer` | 6M → 115k; 150k → 38k | `orders[date]` build |
| q11 | `partsupp` | 800k → 32k | `supplier ⋈ nation[GERMANY]` build |
| q12 | `orders` | 1.5M → 29k | `lineitem[filters]` build |
| q17 | `lineitem` (subquery) | 6M → 6,088 | decorrelation join-back (`l_partkey = p_partkey`), through the subquery aggregation; part keys from `Brand#23, MED BOX` |
| q18 | `lineitem` (main) | 6M → 399 | the `having` `LEFT SEMI` big-orders build (orderkeys with `sum(l_quantity)>300`) |
| q20 | `lineitem` (subquery) | 6M → 9,741 | decorrelation join-back (`(l_partkey, l_suppkey) = partsupp PK`), composite key through the subquery aggregation |

## Gaps

### 1. Outer IN keys not pushed into the correlated subquery (q20)

The availqty subquery decorrelates to shape (a) — grouped by the partsupp PK + join-back, no `AssignUniqueId` (`DecorrelatePass.cpp`, `aggregatePeelEqui`) — and the composite-key dynamic filter from the join-back reduces the `lineitem` scan through the `HashAggregation` grouping keys: 6M → **9,741** rows at sf1, aggregation → 5,843 groups. There is no per-row fanout (one group per partsupp, as v1) and no re-aggregation blow-up.

The residual gap is a **magic-set reduction** v1 performs and v2 does not. v1 pushes the outer `s_suppkey IN` membership into the partsupp subquery as a reducing semijoin (`partsupp ⋉ supplier`, `800k → 32,960`), which cascades to forest → 342 and `lineitem`-into-aggregate → 433. v2 reduces `partsupp` only by the forest filter (→ 8,508) and aggregates 9,741 `lineitem` rows. The cost is small and scan-bounded — both read `lineitem` and `partsupp` in full (~5–10% at sf10).

v2 cannot express the reduction with dynamic filtering: a DF reaches only a **probe**-side scan, but the reduction target (`partsupp`) sits on the **build** side of the decorrelation join-back, so no existing join's DF can reach it. Restricting it needs an explicit plan-time reducing semijoin (what v1 inserts), which v2 defers. This is a case beyond the usual DF-vs-semijoin dichotomy — a join exists and the correlation is equi, but the target is not probe-reachable, so neither "DF where a join exists" nor the non-equi shape-(c) construction applies. Decorrelation framework in [Decorrelate.md](Decorrelate.md).

### 2. Residual rename Projects from a subquery aggregate alias (q13)

q13's inner aggregate (`count(o_orderkey)`, aliased `c_count` by the subquery) emits its output under the natural name `count` instead of `c_count`. Because the rename never folds into the aggregate, v2 carries `count` through the rest of the plan and leaves **two** cosmetic Project nodes v1 does not have: a redundant **identity** Project (`count` → `count`) just above the inner aggregation, and a **trailing** Project (`count` → `c_count`) after the outer `OrderBy`. v1 instead renames `count` → `c_count` in its single pre-aggregation Project and needs no trailing node.

Both are no-ops for cost and correctness — the join order, build/probe sides, and the two aggregations all match v1. Fix: carry the `c_count` alias on the inner aggregation's output so both Projects disappear. Tracked by a `TODO` on `TpchPlanTest.q13`.

### 3. q9 — `like`-selectivity is mis-estimated but does not change the sf1 plan

Not an active gap at sf1: v2 (and v1) join `lineitem ⋈ part` first, so the selective `part` filter reduces `lineitem` to the green-parts subset early. v2 scans 6M → 319k `lineitem` rows via the `part` dynamic filter (sf1, EXPLAIN ANALYZE) — the optimal reduction. The order is robust to the `like` estimate because `part` (200k) is far smaller than `lineitem` (6M), so building `part` and probing `lineitem` is correct by size regardless of the estimate. Both the `like` form and an estimable-filter variant (`p_size <= 3`) are locked in `TpchPlanTest`; with the accurate estimate the planner additionally makes `partsupp` the top probe, a cost-driven refinement of the top-level shape that the `like` estimate does not reach.

The underlying gap — the `like` estimator defaults to non-selective (~80%, `Selectivity::likelyTrue()`; see [FilterSelectivity.md](../../docs/FilterSelectivity.md)) vs the true ~5% — only manifests at sf0.1, where the FK-key mismatch inflates `lineitem ⋈ supplier` selectivity enough to flip the order to the worse supplier-first plan. At sf1 it does not reproduce.

## Reproduction

```bash
CLI=$(buck build @mode/opt axiom/cli:cli --show-simple-output)
QDIR=axiom/optimizer/tests/tpch/queries
DATA=<path-to-tpch-parquet>            # one directory per table, e.g. ~/tpch/sf1

for n in $(seq 1 22); do
  Q=$(cat "$QDIR/q$n.sql")
  printf 'SET SESSION optimizer.greedy_join_threshold = 2147483647;\nEXPLAIN (TYPE EXECUTABLE)\n%s' "$Q" | "$CLI" --data_path "$DATA" --num_workers 1 --num_drivers 1 --query "" > v1/q$n.txt
  printf 'EXPLAIN (TYPE EXECUTABLE)\n%s' "$Q" | "$CLI" --data_path "$DATA" --v2 --query ""                    > v2/q$n.txt
done
```
