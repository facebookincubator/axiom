# Payload NDV Scaling: Coupon Collector vs Linear

This document explains why the coupon collector formula is preferred over
linear scaling for estimating the number of distinct values (NDV) surviving
a row-level filter. The same formula applies wherever a subset of rows is
selected and we need to estimate how many distinct values of an unrelated
column remain — including join payloads and partial aggregations.

## The Problem

Given a column with `d` distinct values across `N` rows, if we keep a
fraction `s` of the rows, how many distinct values survive?

This arises in two settings:

- **Joins:** The join filters rows by key. For payload columns (unrelated
  to the join key), we need to estimate how many distinct payload values
  remain after the join discards non-matching rows.
- **Partial aggregations:** A partial aggregation processes a subset of
  the input rows. We need to estimate how many distinct group-by key
  values appear in that subset.

In both cases, the selection is effectively random with respect to the
column whose NDV we're estimating — whether a row survives depends on
something independent of the payload / group-by value.

## Linear Scaling

The simple approach:

```
ndv_output = d × s
```

This assumes that keeping fraction `s` of the rows keeps fraction `s` of
the distinct values.

**When it's correct:** Only when each distinct value appears exactly once
(`N = d`). Removing a row removes a value; keeping fraction `s` of the
rows keeps exactly `s × d` values.

**When it breaks down:** When values have duplicates (`N > d`). A value
with 10 rows has 10 independent chances to survive — removing some rows
doesn't remove the value unless ALL its rows are removed.

## Coupon Collector Formula

Under uniform distribution, each distinct value appears `f = N/d` times.
The probability that a specific value is completely absent from a sample
of `s × N` rows is:

```
P(absent) ≈ (1 - s)^f = (1 - s)^(N/d)
```

So the expected number of surviving distinct values is:

```
ndv_output = d × (1 - (1 - s)^(N/d))
```

We define this as `sampledNdv(c, s)` where `c` is the column and `s` is
the selectivity:

```
sampledNdv(c, s) = ndv(c) × (1 - (1 - s) ^ (N / ndv(c)))
```

where `N` is the row count of the side column `c` comes from.

## Properties

| Property | Holds? |
|---|---|
| s = 0 → ndv = 0 | ✓ |
| s = 1 → ndv = d | ✓ |
| N/d = 1 → degenerates to d × s (linear) | ✓ |
| N/d ≫ 1 → ndv ≈ d (most values survive) | ✓ |
| Monotonically increasing in s | ✓ |

The formula degenerates to linear scaling when there are no duplicates,
so it is never worse than linear. When duplicates exist, it is strictly
more accurate.

## Numerical Comparison

**High duplicates: d = 100, N = 1000 (10 rows per value), s = 0.5**

- Linear: 100 × 0.5 = **50**
- Coupon: 100 × (1 - 0.5^10) = 100 × 0.999 = **99.9**

Each value has 10 rows. The probability that ALL 10 rows of a given
value are removed is 0.5^10 ≈ 0.001. Nearly every value survives.
Linear underestimates by 2×.

**Moderate duplicates: d = 500, N = 1000 (2 rows per value), s = 0.5**

- Linear: 500 × 0.5 = **250**
- Coupon: 500 × (1 - 0.5^2) = 500 × 0.75 = **375**

With just 2 rows per value, each value has a 25% chance of being
completely removed. 75% survive. Linear underestimates by 1.5×.

**No duplicates: d = 1000, N = 1000 (1 row per value), s = 0.5**

- Linear: 1000 × 0.5 = **500**
- Coupon: 1000 × (1 - 0.5^1) = 1000 × 0.5 = **500**

Identical. No duplicates means each removed row removes exactly one
distinct value.

**Low-cardinality column: d = 5, N = 1,000,000, s = 0.1**

- Linear: 5 × 0.1 = **0.5** → max(1, 0.5) = 1
- Coupon: 5 × (1 - 0.9^200000) ≈ **5**

A status column with 5 values and a million rows. Even keeping only 10%
of rows, all 5 values remain. Linear estimates 1 distinct value — a
severe underestimate that distorts downstream cost calculations.

## Impact on Query Optimization

Underestimating NDV with linear scaling cascades through the optimizer:

| Downstream Operator | Effect of NDV Underestimate |
|---|---|
| Join (as key) | Inflated fanout → pessimistic join cost → suboptimal join order |
| Group By | Underestimated output rows → optimistic aggregation cost |
| Equality filter | Overestimated selectivity (1/ndv higher) → inflated filter output |

All three biases can lead to poor query plans.

## When Each Model Applies

The key distinction is **by-group** vs **by-row** selection:

**By-group (keys):** The join or aggregation keeps/removes entire groups
of rows sharing the same value. If a key value matches, ALL rows with
that key are affected. Specialized formulas apply:

- Join keys: `minNdv`, `ndv - minNdv`
- These are exact under uniform distribution

**By-row (payloads):** From a payload column's perspective, the selection
is random — whether a row survives depends on its key, which is
independent of its payload values. The coupon collector formula applies:

- Join payloads: `sampledNdv(c, selectivity)`
- Partial aggregation inputs: `sampledNdv(c, fraction)`

## Application: Joins

For join payloads, `s` is the selectivity for the relevant side:

| Join Type | Left Payloads | Right Payloads |
|---|---|---|
| Inner | sampledNdv(c, leftSelectivity) | sampledNdv(c, rightSelectivity) |
| Left | input (s = 1) | sampledNdv(c, rightSelectivity) |
| Right | sampledNdv(c, leftSelectivity) | input (s = 1) |
| Left Semi Filter | sampledNdv(c, leftSelectivity) | — |
| Anti | sampledNdv(c, antiSelectivity) | — |

See [JoinEstimation.md](JoinEstimation.md) for the complete set of
formulas across all join types.

## Application: Partial Aggregations

A partial aggregation processes a fraction of the input rows (e.g., one
partition or one batch). The group-by key column behaves like a payload
column from a sampling perspective — the subset of rows is determined by
partitioning, not by group-by key values.

If a partial aggregation processes fraction `s` of the input:

```
estimated groups = sampledNdv(groupByKey, s)
```

This estimates how many distinct group-by key values appear in that
partition, which determines the number of output rows from the partial
aggregation.

The same formula applies when estimating the output cardinality of a
partial aggregation that receives a fraction of the rows after a filter
or join.

## Related Documentation

- [JoinEstimation.md](JoinEstimation.md) — full derivations of join
  cardinality and constraint propagation using `sampledNdv`
- [JoinEstimationQuickRef.md](JoinEstimationQuickRef.md) — compact
  cheat-sheet of all join cardinality and constraint formulas
- [CardinalityEstimation.md](CardinalityEstimation.md) — cardinality
  estimation for all operators (not just joins)
