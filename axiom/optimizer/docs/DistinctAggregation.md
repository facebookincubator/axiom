# Distinct Aggregation

## Overview

Aggregation over distinct arguments (`count(DISTINCT x)`, `sum(DISTINCT y)`)
requires deduplication before computing the aggregate result. The naive approach
shuffles by grouping keys, then runs a single-step aggregation that tracks
distinct
values. This, however, does not scale because it cannot take advantage of partial
aggregation to reduce shuffled data, and has high memory pressure due to keeping
per-group distinct rows of inputs.

Axiom currently implements two transformations that enable distributed execution with
partial aggregation:

| Approach | Description | When eligible |
|----------|-------------|---------------|
| **SingleDistinctToGroupBy** | Inner GROUP BY deduplicates, outer AGG computes | All aggs DISTINCT, same column args, no filters |
| **MarkDistinct** | MarkDistinct tags first-seen rows, AGG uses masks | No filter on any DISTINCT agg |

When both approaches are eligible, the optimizer generates candidate plans for
each, computes their costs, and picks the cheapest via
`std::map<PlanCost, RelationOpPtr>`. When neither applies (e.g., DISTINCT
aggregate with a FILTER clause), the optimizer fails with an error of unsupported query shape.

## SingleDistinctToGroupBy

### Transformation

When all aggregates are DISTINCT with the same set of column arguments and no
filters, the query can be rewritten as a two-level aggregation:

```sql
SELECT g, count(DISTINCT c), sum(DISTINCT c) FROM t GROUP BY g
```

```text
Aggregation [Final] (group by g) (count(c), sum(c))
  Shuffle (hash by g)
    Aggregation [Partial] (group by g) (count(c), sum(c))
      Aggregation [Final] (group by g, c)        -- dedup
        Shuffle (hash by g, c)
          Aggregation [Partial] (group by g, c)   -- dedup
            TableScan(t)
```

The inner `GROUP BY (groupingKeys ∪ distinctArgs)` deduplicates rows. The outer
aggregation computes the functions without DISTINCT on the deduplicated data.
Both levels can use partial/final to reduce data early. The optimizer makes
the decision between partial/final aggregation and single-step aggregation
based on the estimated cost, via `makeSplitOrSingleAggregationPlan`.

### Eligibility

`canMakeDistinctToGroupByPlan` checks:
1. **All aggregates are DISTINCT.** A mix of DISTINCT and non-DISTINCT
   disqualifies this approach.
2. **Same column arguments.** All aggregates must have the same set of column
   args (compared as a set, ignoring order and duplicates). Literal arguments
   are excluded since they don't affect deduplication.
3. **No filters.** No aggregate may have a `condition()` (FILTER clause).

### Example: DISTINCT args overlap with grouping keys

```sql
SELECT b, covar_pop(DISTINCT b, c) FROM t GROUP BY b
```

The inner aggregation deduplicates on `(b, c)`, then the outer aggregation groups by `(b)` and computes `covar_pop(b, c)` without DISTINCT.

### Example: Literal arguments

```sql
SELECT a, max_by(DISTINCT b, 1), min_by(DISTINCT b, 2) FROM t GROUP BY a
```

Literals (`1`, `2`) are skipped during column arg extraction. Both aggregates
have the same column arg set `{b}`, so inner GROUP BY keys are `(a, b)`. The outer aggregation then groups by `(a)` and computes `max_by(b, 1), min_by(b, 2)` without DISTINCT.

## MarkDistinct

### Transformation

When there are multiple sets of distinct arguments, or a mix of DISTINCT and
non-DISTINCT aggregates, the MarkDistinct approach handles each distinct arg set
independently by inserting a `MarkDistinct` node that produces a boolean marker
column. DISTINCT aggregates are then rewritten as masked non-DISTINCT aggregates
filtered by the marker.

```sql
SELECT g, count(DISTINCT c0), sum(c0), avg(DISTINCT c1) FROM t GROUP BY g
```

```text
Aggregation [Final] (group by g) (count(c0) mask m0, sum(c0), avg(c1) mask m1)
  Shuffle (hash by g)
    Aggregation [Partial] (group by g) (count(c0) mask m0, sum(c0), avg(c1) mask m1)
      MarkDistinct (by g, c1) → m1
        Shuffle (hash by g, c1)
          MarkDistinct (by g, c0) → m0
            Shuffle (hash by g, c0)
              TableScan(t)
```

Each `MarkDistinct` node marks the first occurrence of each unique combination
of distinct keys (grouping keys + distinct args) with `true` in its marker
column. The subsequent aggregation uses these markers as FILTER masks, so each
row contributes to a DISTINCT aggregate only once per distinct-key combination.

### Eligibility

`canMakeMarkDistinctPlan` checks: no DISTINCT aggregate has a filter
(`condition()`). Non-DISTINCT aggregates may have filters.

### Cardinality Estimation of MarkDistinct RelationOp

```cpp
struct MarkDistinct : public RelationOp {
  MarkDistinct(RelationOpPtr input, ColumnCP markerColumn, ExprVector distinctKeys);

  ColumnCP markerColumn() const;
  const ExprVector& distinctKeys() const;
};
```

- **Output columns**: all input columns plus the boolean marker column.
- **Fanout**: 1 (cardinality-neutral — does not filter rows).
- **Cost**: `Costs::hashTableCost(inputCardinality)` — models the hash table
  overhead of tracking seen distinct-key combinations.

### Example: Shared marker columns

```sql
SELECT b, count(DISTINCT c), covar_pop(DISTINCT b, c) FROM t GROUP BY b
```

Both aggregates have the same distinct arg set minus grouping keys: `{c}` (since
`b` is a grouping key). They share a single marker column `m0`:

```text
Aggregation (group by b) (count(c) mask m0, covar_pop(b, c) mask m0)
  MarkDistinct (by b, c) → m0
    Shuffle (hash by b, c)
      TableScan(t)
```

### Example: All distinct args are grouping keys

```sql
SELECT a, count(DISTINCT a), sum(DISTINCT b) FROM t GROUP BY a
```

For `count(DISTINCT a)`: the distinct arg set minus grouping keys is empty
(`{a} - {a} = {}`). No marker is needed — `count(DISTINCT a)` passes through
as-is. For `sum(DISTINCT b)`: the set is `{b}`, so a marker `m0` is created:

```text
Aggregation (group by a) (count(DISTINCT a), sum(b) mask m0)
  MarkDistinct (by a, b) → m0
    Shuffle (hash by a, b)
      TableScan(t)
```

## Cost-Based Selection

### Entry Point

`Optimization::addAggregation` orchestrates the strategy selection:

1. **Short-circuit**: single-worker/single-driver or pre-grouped keys → simple
   `kSingle` aggregation (supports DISTINCT natively).
2. Detects `hasDistinct` by scanning aggregates.
3. If `hasDistinct`:
   - Creates `std::map<PlanCost, RelationOpPtr> candidatePlans`.
   - If `canMakeDistinctToGroupByPlan` → generates candidate via
     `makeDistinctToGroupByPlan`.
   - If `canMakeMarkDistinctPlan` → generates candidate via
     `makeDistinctToMarkDistinctPlan`.
   - Picks `candidatePlans.begin()` (lowest cost).
4. Falls through to normal aggregation planning for non-distinct cases.

### PlanCost Comparison

```cpp
struct PlanCost {
  float cost{0};
  float cardinality{1};

  bool operator<(const PlanCost& other) const {
    if (cost != other.cost) return cost < other.cost;
    return cardinality < other.cardinality;
  }
};
```

When costs are equal, lower cardinality wins. If both are equal, the first
inserted plan wins (GroupBy is inserted first).

### When each approach wins

- **SingleDistinctToGroupBy**: Best when the duplicate ratio of
  `(groupingKeys + distinctArgs)` is high, so the inner GROUP BY aggressively
  reduces data. The inner partial aggregation sends far fewer rows through the
  shuffle.
- **MarkDistinct**: Best when the duplicate ratio is low, making the inner
  GROUP BY in SingleDistinctToGroupBy ineffective. MarkDistinct avoids the
  two-level aggregation overhead and adds no data amplification — it only
  appends a boolean column.

## Current Limitations

- **DISTINCT + FILTER**: `count(DISTINCT b) FILTER (WHERE c > 0)` is not
  supported. Neither approach is eligible (both check for no filter on DISTINCT
  aggregates), and the optimizer fails with an error.
- **DISTINCT + ORDER BY**: Supported only when ORDER BY keys are a subset of
  the distinct args. The outer aggregation uses single-step (not partial/final)
  since partial aggregation cannot preserve global ordering.

## Testing

### Optimizer Plan Tests

`AggregationPlanTest.cpp` in `axiom/optimizer/tests/` (extends
`HiveQueriesTestBase`). Parses SQL via `PlanBuilder`, optimizes with
`planVelox()`, and verifies distributed plans with
`AXIOM_ASSERT_DISTRIBUTED_PLAN` and `PlanMatcherBuilder`.

`PlanMatcherBuilder` matchers: `partialAggregation()`, `finalAggregation()`,
`singleAggregation()`, `markDistinct()`, `shuffle()`, `localPartition()` in
`PlanMatcher.h/cpp`.

**SingleDistinctToGroupBy tests** (`singleDistinctToGroupBy`):
- Global aggregation with multiple DISTINCT aggregates on same columns.
- Single DISTINCT aggregate with grouping keys.
- Multiple DISTINCT aggregates on same column set.
- Expression-based grouping keys and distinct args.
- Same distinct arg set with different order and duplicates.
- DISTINCT argument overlap with grouping keys.
- DISTINCT with ORDER BY.
- DISTINCT with literal args (literals skipped in inner GROUP BY keys).
- DISTINCT aggregate where all arguments are literals.

**MarkDistinct tests** (`multipleDistinctToMarkDistinct`):
- Multiple DISTINCT aggregates with different argument sets.
- Global aggregation with multiple DISTINCT sets.
- Mix of DISTINCT with different args and non-DISTINCT.
- DISTINCT with ORDER BY uses single aggregation step.
- Shared marker columns for same distinct arg set (minus grouping keys).
- DISTINCT args overlapping with grouping keys are deduplicated.
- Multi-argument DISTINCT aggregates with different arg sets.
- DISTINCT aggregate with literal args alongside different DISTINCT sets.
- DISTINCT aggregate whose column args are all grouping keys (no marker).
- DISTINCT aggregate with all-literal args (no marker).

**Unsupported cases** (`unsupportedAggregationOverDistinct`):
- DISTINCT aggregate with a filter condition → error.

Target: `buck test fbcode//axiom/optimizer/tests:aggregation_plan`

### End-to-End Tests

Distinct aggregation test queries in `axiom/optimizer/tests/sql/aggregation.sql`. The `SqlTest`
harness parses SQL, runs through the full Axiom pipeline (parse → optimize →
execute via `LocalRunner`), and compares results against DuckDB.

### Verification Commands

```bash
buck test fbcode//axiom/optimizer/tests:aggregation_plan
buck test fbcode//axiom/optimizer/tests:sql
buck build fbcode//axiom/...
```
