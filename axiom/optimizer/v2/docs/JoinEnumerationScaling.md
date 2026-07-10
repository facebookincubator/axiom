# Join Enumeration Scaling: B&B (v1) vs DPhyp (v2)

*How the two join-order enumerators scale on a same-key join chain, why both
need a greedy (GOO) fallback, and where each fallback starts to matter.*

## The pathology

The stress case is an N-table chain joined entirely on **one key**:

```sql
SELECT count(*) FROM t1, ..., tN
WHERE t1.a = t2.a AND t2.a = t3.a AND ... AND t{N-1}.a = tN.a
```

All equi-predicates collapse into a single equivalence class, so transitive
closure turns the join graph into a **complete graph (clique)**, and every one
of the N! join orders has the **same estimated cost**. Tables must carry stats
(row count + column NDV); without NDV the join cost is unknown and the optimizer
falls back to syntactic order with no enumeration at all.

This is the worst case for both enumerators, for different reasons:

- **v1 branch-and-bound** prunes a partial plan once its cost exceeds the best
  complete plan (`isOverBest()`). When every order ties, no partial plan is ever
  worse than the best, so **nothing prunes** and B&B walks the whole search space
  (orderings × join-method × build-side at each level) — super-factorial. It is
  the *tie* that disables pruning, not the raw ordering count, which is why even
  N=6 hangs.
- **v2 DPhyp** (Moerkotte/Neumann 2006) is a memoized bottom-up DP over connected
  subgraph / complement pairs. It has no pruning to disable, so ties don't hurt
  it — but on a clique the number of csg-cmp pairs is `3^N - 2^(N+1) + 1` ≈ **3^N**
  (vs polynomial ~O(N^3) on a plain chain). Exponential, but far below N!.

## Fallbacks

Both pipelines bound the worst case by switching to greedy operator ordering
(GOO) instead of exhaustive enumeration:

- **v1:** `optimizer.greedy_join_threshold` (default 5). A join cluster with
  `>= threshold` tables uses greedy instead of B&B. Set to `INT_MAX` to force
  B&B always (what `TpchPlanTest` does to lock golden plans).
- **v2:** `optimizer.dphyp_enumeration_budget` (default 100000). DPhyp bails to a
  per-component DP/GOO fallback once it emits this many csg-cmp pairs. Set to `0`
  for unlimited (full DPhyp, never falls back).

## Methodology

Driven through the Axiom CLI against the TestConnector with injected stats
(`t1..t20`, each 100k rows, column `a` NDV 100k). `Optimizing:` is the optimize
phase from `--print_timing`; `@mode/opt`, devserver — small per-N differences are
machine/build variance.

```bash
buck2 build @mode/opt fbcode//axiom/cli:cli

# Generate test-connector tables with stats: t1..t20, each 100k rows, column `a`
# with NDV 100k, plus a repro.properties so the CLI's test connector loads them.
# The stats are essential — without NDV the join cost is unknown and v1 falls
# back to syntactic order (no enumeration, no blowup).
mkdir -p /tmp/chain
python3 - <<'PY'
import json, os
d = "/tmp/chain"
for i in range(1, 21):
    json.dump(
        {"name": f"t{i}", "numRows": 100000,
         "columns": [{"name": "a", "type": "BIGINT", "numDistinct": 100000,
                      "low": 1, "high": 100000, "nullFraction": 0.0}]},
        open(os.path.join(d, f"t{i}.json"), "w"), indent=2)
open(os.path.join(d, "repro.properties"), "w").write(
    "connector.name=test\ntables=*.json\n")
PY

# Run a same-key chain of N tables under v1 or v2. v1 uses EXPLAIN (TYPE
# OPTIMIZED); v2 uses plain EXPLAIN (v2 rejects TYPE OPTIMIZED). `timeout` bounds
# the run because v1 hangs from N=6 once its greedy fallback is off.
run_chain() {  # $1 = N, $2 = v1|v2
  local N=$1 mode=$2 from="t1" where="t1.a = t2.a"
  for i in $(seq 2 "$N"); do from="$from, t$i"; done
  for i in $(seq 3 "$N"); do where="$where AND t$((i-1)).a = t$i.a"; done
  local explain="EXPLAIN (TYPE OPTIMIZED)" flag=""
  [ "$mode" = v2 ] && explain="EXPLAIN" flag="--v2"
  timeout 120 buck2 run @mode/opt fbcode//axiom/cli:cli -- $flag \
    --etc_dir /tmp/chain --catalog repro --num_workers 1 --num_drivers 1 \
    --print_timing \
    --query "$explain SELECT count(*) FROM $from WHERE $where" 2>&1 \
    | grep -oE "Optimizing: [0-9.]+[a-z]*"
}
for N in 3 4 5 6 8 10 14 20; do echo "v1 N=$N: $(run_chain $N v1)"; done
for N in 3 4 5 6 8 10 14 20; do echo "v2 N=$N: $(run_chain $N v2)"; done

# To disable the fallbacks (raw enumeration), prepend SET SESSION statements to
# the --query so they run before the EXPLAIN:
#   v1 force B&B:        SET SESSION optimizer.greedy_join_threshold = 2147483647;
#   v2 force full DPhyp: SET SESSION optimizer.dphyp_enumeration_budget = 0;
```

## Results — with fallbacks (default)

Both degrade gracefully; neither hangs.

| N  | v1 (B&B + GOO) | v2 (DPhyp + budget) |
|----|----------------|---------------------|
| 3  | 0.93 ms        | 0.33 ms             |
| 4  | 3.18 ms        | 0.38 ms             |
| 5  | 1.35 ms (GOO)  | 0.41 ms             |
| 6  | 1.96 ms (GOO)  | 0.53 ms             |
| 8  | 4.36 ms (GOO)  | 1.86 ms             |
| 10 | 10.0 ms (GOO)  | 15.8 ms             |
| 14 | 42.2 ms (GOO)  | 80.4 ms (GOO)       |
| 20 | 211 ms (GOO)   | 89.7 ms (GOO)       |

`(GOO)` marks the greedy fallback; unmarked cells are full enumeration — B&B for
v1, DPhyp for v2 (v1 falls back past its greedy threshold of 5, v2 past its DPhyp
budget). This is why v2 reads slower at N=10 and N=14: v1 has already dropped to
cheap greedy, while v2 is still enumerating (full DPhyp at N=10; the whole budget's
worth of pairs before bailing at N=14).

- v1 crosses to GOO at N=5 (default threshold), so N=5 drops from B&B's 133 ms to
  1.35 ms and N=6 never hangs.
- v2 plateaus (~80→90 ms past N=14): the budget cap is hit and it falls back to
  GOO. Below the cap (N=10, `3^10` ≈ 59k < 100k) it runs full DPhyp.

## Results — fallbacks disabled (raw enumeration)

`greedy_join_threshold = INT_MAX` (force B&B) and `dphyp_enumeration_budget = 0`
(unlimited DPhyp). This isolates the two enumeration algorithms.

| N  | v1 B&B (no greedy)   | v2 DPhyp (no budget) |
|----|----------------------|----------------------|
| 4  | 2.93 ms              | 0.35 ms              |
| 5  | 129 ms               | 0.40 ms              |
| 6  | **hang (>90 s)**     | 0.51 ms              |
| 8  | —                    | 1.75 ms              |
| 10 | —                    | 15.8 ms              |
| 12 | —                    | 172 ms               |
| 14 | —                    | 1.99 s               |
| 16 | —                    | **22.9 s**           |

- **B&B (~N!):** ~44x per added table (2.9 ms → 129 ms), then a cliff — N=6
  already hangs. Intractable at **N=6**.
- **DPhyp (~3^N):** clean ~3^2 ≈ 9-11x per +2 tables (15.8 ms → 172 ms → 1.99 s →
  22.9 s). Intractable around **N=16-17**.

## Takeaways

- Both enumerators are exponential on a same-key clique, but DPhyp's `3^N` buys
  ~10 more tables than B&B's `N!` before becoming intractable (N≈16 vs N≈6), and
  DPhyp has no tie-induced pruning cliff — it is a smooth, memoized `3^N`.
- Neither is safe unbounded, which is why both have a GOO fallback. The v1 greedy
  fallback is essential from N=6; the v2 budget fallback only matters from ~N=14.
