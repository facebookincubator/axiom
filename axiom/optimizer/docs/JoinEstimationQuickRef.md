# Join Estimation Quick Reference

Quick reference for join result cardinality and constraint propagation
formulas across all join types. For full derivations and examples, see
[JoinEstimation.md](JoinEstimation.md).

## Notation

| Term | Definition |
|---|---|
| \|left\|, \|right\| | Row counts of the left and right sides |
| ndv(c) | Number of distinct non-null values of column c |
| fanout | Expected right matches per left row: \|right\| / max(ndv(leftKey), ndv(rightKey)). For joins without equi-keys (cross joins), fanout = \|right\| |
| rlFanout | Expected left matches per right row: \|left\| / max(ndv(leftKey), ndv(rightKey)). For cross joins, rlFanout = \|left\| |
| filterSelectivity | Selectivity of non-equi filter conditions (1.0 if none) |
| minNdv | min(ndv(leftKey), ndv(rightKey)) — overlapping distinct key values |
| leftSelectivity | min(1, fanout) × filterSelectivity — fraction of left rows that match |
| rightSelectivity | min(1, rlFanout) × filterSelectivity — fraction of right rows that match |
| antiSelectivity | max(0, 1 - fanout × filterSelectivity) — fraction of left rows with no match |
| leftNullFraction | max(0, 1 - ndv(leftKey) / ndv(rightKey)) — fraction of left rows without a right match |
| rightNullFraction | max(0, 1 - ndv(rightKey) / ndv(leftKey)) — fraction of right rows without a left match |
| sampledNdv(c, s)* | ndv(c) × (1 - (1 - s)^(N / ndv(c))) — distinct values surviving selection of fraction s of N rows, where N = \|left\| or \|right\| depending on which side column c comes from |
| input | Value preserved from the input constraint unchanged |
| intersect(L, R) | Intersection of left and right key ranges |

\* Uses the coupon collector formula rather than linear scaling (`d × s`).
Linear scaling underestimates NDV when values have duplicates (`N > d`).
See [PayloadNdvScaling.md](PayloadNdvScaling.md) for derivation and
numerical comparison.

## Result Cardinality

| Join Type | Result Cardinality |
|---|---|
| Inner | \|left\| × fanout × filterSelectivity |
| Left | \|left\| × max(1, fanout × filterSelectivity) |
| Right | \|right\| × max(1, rlFanout × filterSelectivity) |
| Full Outer | \|left\| × max(1, fanout × filterSelectivity) + \|right\| × max(0, 1 - rlFanout × filterSelectivity) |
| Left Semi Filter | \|left\| × min(1, fanout) × filterSelectivity |
| Left Semi Project | \|left\| |
| Right Semi Filter | \|right\| × min(1, rlFanout) × filterSelectivity |
| Right Semi Project | \|right\| |
| Anti | \|left\| × max(0, 1 - fanout × filterSelectivity) |

## Constraint Propagation

### Inner Join

```
resultCardinality = |left| × fanout × filterSelectivity
```

| Column | cardinality | nullFraction | range |
|---|---|---|---|
| Keys (both) | minNdv | 0 | intersect(L, R) |
| Left Payloads | max(1, sampledNdv(c, leftSelectivity)) | input | input |
| Right Payloads | max(1, sampledNdv(c, rightSelectivity)) | input | input |

### Left Join

```
resultCardinality = |left| × max(1, fanout × filterSelectivity)
```

| Column | cardinality | nullFraction | range |
|---|---|---|---|
| Left (all) | input | input | input |
| Right Keys | minNdv | rightNullFraction | intersect(L, R) |
| Right Payloads | max(1, sampledNdv(c, rightSelectivity)) | rightNullFraction | input |

### Right Join

```
resultCardinality = |right| × max(1, rlFanout × filterSelectivity)
```

| Column | cardinality | nullFraction | range |
|---|---|---|---|
| Right (all) | input | input | input |
| Left Keys | minNdv | leftNullFraction | intersect(L, R) |
| Left Payloads | max(1, sampledNdv(c, leftSelectivity)) | leftNullFraction | input |

### Full Outer Join

```
resultCardinality = |left| × max(1, fanout × filterSelectivity)
                  + |right| × max(0, 1 - rlFanout × filterSelectivity)
```

| Column | cardinality | nullFraction | range |
|---|---|---|---|
| Left Keys | minNdv | leftNullFraction | intersect(L, R) |
| Left Payloads | max(1, sampledNdv(c, leftSelectivity)) | leftNullFraction | input |
| Right Keys | minNdv | rightNullFraction | intersect(L, R) |
| Right Payloads | max(1, sampledNdv(c, rightSelectivity)) | rightNullFraction | input |

### Left Semi Filter

```
resultCardinality = |left| × min(1, fanout) × filterSelectivity
```

| Column | cardinality | nullFraction | range | trueFraction |
|---|---|---|---|---|
| Left Keys | minNdv | 0 | intersect(L, R) | — |
| Left Payloads | max(1, sampledNdv(c, leftSelectivity)) | input | input | — |

### Left Semi Project

```
resultCardinality = |left|
```

| Column | cardinality | nullFraction | range | trueFraction |
|---|---|---|---|---|
| Left (all) | input | input | input | — |
| Mark | — | 0 | — | min(1, fanout) × filterSelectivity |

### Right Semi Filter

```
resultCardinality = |right| × min(1, rlFanout) × filterSelectivity
```

| Column | cardinality | nullFraction | range | trueFraction |
|---|---|---|---|---|
| Right Keys | minNdv | 0 | intersect(L, R) | — |
| Right Payloads | max(1, sampledNdv(c, rightSelectivity)) | input | input | — |

### Right Semi Project

```
resultCardinality = |right|
```

| Column | cardinality | nullFraction | range | trueFraction |
|---|---|---|---|---|
| Right (all) | input | input | input | — |
| Mark | — | 0 | — | min(1, rlFanout) × filterSelectivity |

### Anti Join

```
resultCardinality = |left| × max(0, 1 - fanout × filterSelectivity)
```

| Column | cardinality | nullFraction | range |
|---|---|---|---|
| Left Keys | max(1, ndv(leftKey) - minNdv) | input | input |
| Left Payloads | max(1, sampledNdv(c, antiSelectivity)) | input | input |

## Invariants

**Symmetry:**

- Inner Join is fully symmetric: \|left\| × fanout = \|right\| × rlFanout
- Left Join(L, R) = Right Join(R, L) with sides flipped
- Left Semi Filter(L, R) = Right Semi Filter(R, L) with sides flipped
- Left Semi Project(L, R) = Right Semi Project(R, L) with sides flipped

**Cardinality bounds:**

- Inner ≤ \|left\| × \|right\| (at most the cross product)
- Left Join ≥ \|left\| (all left rows preserved)
- Right Join ≥ \|right\| (all right rows preserved)
- Full Outer ≥ max(\|left\|, \|right\|)
- Left Semi Filter ≤ \|left\| (subset of left rows)
- Anti ≤ \|left\| (subset of left rows)

**Complementarity:**

- Left Semi Filter + Anti partition the left side
  (exact when filterSelectivity = 1):
  resultCard(Left Semi Filter) + resultCard(Anti) = \|left\|
- Anti key ndv + Semi Filter key ndv = ndv(leftKey):
  (ndv(leftKey) - minNdv) + minNdv = ndv(leftKey)

**Decomposition:**

- Full Outer = Left Join + unmatched right rows:
  resultCard(Full Outer) = resultCard(Left Join)
  + \|right\| × max(0, 1 - rlFanout × filterSelectivity)

**Semi Project ↔ Semi Filter:**

- Semi Project mark trueFraction = resultCard(Semi Filter) / \|input\|:
  trueFraction = min(1, fanout) × filterSelectivity
  = resultCard(Left Semi Filter) / \|left\|

**Constraint relationships:**

- Left Semi Filter keys = Inner Join keys
  (same minNdv, nullFraction = 0, range = intersect)
- Left Semi Filter constraints are a subset of Inner Join constraints
  (semi outputs only left columns; inner outputs both sides)

## Related Documentation

- See [JoinEstimation.md](JoinEstimation.md) for full derivations, worked
  examples, and constraint propagation details.
- See [CardinalityEstimation.md](CardinalityEstimation.md) for cardinality
  estimation of all operators (not just joins).
- See [PayloadNdvScaling.md](PayloadNdvScaling.md) for why the coupon
  collector formula is preferred over linear scaling.
- See [FilterSelectivity.md](FilterSelectivity.md) for filter selectivity
  estimation details.
