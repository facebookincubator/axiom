# Join Estimation

This document describes how output cardinality and column constraints are
estimated for join operators in the Axiom optimizer. It covers the inputs
needed for estimation, derived quantities, join-type adjustment, and
per-type formulas for both result cardinality and constraint propagation.

For a compact cheat-sheet of all formulas, see
[JoinEstimationQuickRef.md](JoinEstimationQuickRef.md).

## Inputs

A join has a left side and a right side, zero or more equality keys
(`leftKeys[i] = rightKeys[i]`), and optional non-equi filter expressions.
Estimation requires three inputs:

- **fanout** — expected number of right matches per left row based on
  equi-keys only. Does not include join-type adjustment or filter
  selectivity. See [Fanout Estimation](#fanout-estimation) for how this
  is computed. For joins without equi-keys (cross joins),
  `fanout = |right|` — every left row is paired with every right row.

- **rlFanout** — expected number of left matches per right row based on
  equi-keys only. Symmetric to `fanout`. For cross joins,
  `rlFanout = |left|`.

- **filterSelectivity** — selectivity of non-equi filter conditions
  (1.0 if none). See [FilterSelectivity.md](FilterSelectivity.md) for
  details on how filter selectivity is estimated.

Additional inputs from column statistics:

- **ndv(c)** — number of distinct values of column c from input constraints
- **|left|**, **|right|** — row counts of the left and right sides

## Derived Quantities

- `minNdv` — `min(ndv(leftKey), ndv(rightKey))` after equality constraint.
  Assumes full range overlap; when ranges partially overlap, the range-aware
  computation is more precise (estimates how many distinct values from each
  side fall in the overlapping range and takes the minimum of those)
- `leftNullFraction` — `max(0, 1 - ndv(leftKey) / ndv(rightKey))`:
  fraction of left rows without a right match
- `rightNullFraction` — `max(0, 1 - ndv(rightKey) / ndv(leftKey))`:
  fraction of right rows without a left match
- `leftSelectivity` — `min(1, fanout) × filterSelectivity`:
  fraction of left rows that survive the join. `min(1, fanout)` captures
  the equi-key matching: when `fanout >= 1` every left row finds at least
  one match; when `fanout < 1` only that fraction of left rows find a
  match. Multiplying by `filterSelectivity` accounts for additional rows
  eliminated by non-equi filter conditions
- `rightSelectivity` — `min(1, rlFanout) × filterSelectivity`:
  symmetric formula for the right side
- `antiSelectivity` — `max(0, 1 - fanout × filterSelectivity)`:
  fraction of left rows that do NOT match any right row, used for anti
  joins (NOT EXISTS)
- `sampledNdv(c, s)` — expected number of distinct values of column `c`
  surviving a random selection of fraction `s` of the rows (see below)

**Payload NDV scaling.** For key columns, the join keeps or removes
entire groups of rows sharing a key value — specialized formulas
(`minNdv`, `ndv - minNdv`) apply. For payload columns, the join acts as
a row-level filter: whether a row survives depends on its key, which is
independent of its payload values. The number of distinct payload values
surviving a random selection of `s × N` rows from `N` rows containing
`d` distinct values (uniform distribution, `N/d` rows per value) is:

```
sampledNdv(c, s) = d × (1 - (1 - s) ^ (N / d))
```

where `d = ndv(c)`, `N` is the row count of the side column `c` comes
from, and `s` is the selectivity for that side.

When `N/d = 1` (every value appears once), this degenerates to `d × s`
(linear scaling). When `N/d ≫ 1` (many duplicates per value), even a
small fraction of rows retains most distinct values — each value has
`N/d` independent chances to appear in the sample.

See [PayloadNdvScaling.md](PayloadNdvScaling.md) for derivation and
numerical comparison of coupon collector vs linear scaling.

## Join-Type Adjustment

Raw fanout reflects only equi-key matching and doesn't account for join
semantics (e.g., a left join must preserve all left rows even if they have
no match, a semi join must deduplicate). The adjustment enforces these
guarantees.

Filter selectivity is applied to raw fanouts before join-type adjustment.
The adjusted fanout determines output cardinality:

```
resultCardinality = |left| × adjustedFanout
```

| Join Type | Adjusted Fanout | Rationale |
|-----------|----------------|-----------|
| Inner | `fanout` | No adjustment |
| Left | `max(1, fanout)` | Every left row produces ≥1 output |
| Right | `max(1, rlFanout) × \|right\| / \|left\|` | Every right row produces ≥1 output |
| Full | `max(max(1, fanout), \|right\| / \|left\|)` | Both sides preserved |
| Left Semi Filter | `min(1, fanout)` | At most 1 output per left row |
| Left Semi Project | `1` | Always 1 output per left row (with mark column) |
| Right Semi Filter | `min(1, rlFanout) × \|right\| / \|left\|` | At most 1 output per right row |
| Right Semi Project | `\|right\| / \|left\|` | Always 1 output per right row |
| Anti | `max(0, 1 - fanout)` | Left rows without matches |

All fanout values in the table above are after multiplying by
`filterSelectivity`. For example, a left join with raw `fanout = 2` and
`filterSelectivity = 0.3` gives `max(1, 2 × 0.3) = max(1, 0.6) = 1`,
preserving the guarantee that every left row produces at least 1 output.

## Output Columns by Join Type

| Join Type           | Left Keys | Left Payloads | Right Keys | Right Payloads | Mark |
|---------------------|:---------:|:-------------:|:----------:|:--------------:|:----:|
| Inner               | ✓         | ✓             | ✓          | ✓              |      |
| Left                | ✓         | ✓             | ✓          | ✓              |      |
| Right               | ✓         | ✓             | ✓          | ✓              |      |
| Full                | ✓         | ✓             | ✓          | ✓              |      |
| Left Semi Filter    | ✓         | ✓             |            |                |      |
| Left Semi Project   | ✓         | ✓             |            |                | ✓    |
| Right Semi Filter   |           |               | ✓          | ✓              |      |
| Right Semi Project  |           |               | ✓          | ✓              | ✓    |
| Anti                | ✓         | ✓             |            |                |      |

## Example Tables

The following tables are used in all examples below:

```
t(a, b): 1000 rows
  a: ndv=100, 10% nulls, range=[1, 200]
  b: ndv=500

u(x, y): 50 rows
  x: ndv=50, range=[50, 150]
  y: ndv=40

v(p, q): 500 rows
  p: ndv=50, range=[1, 100]
  q: ndv=200
```

Derived values:

```
t JOIN u ON a = x:
  fanout   = |u| / max(ndv(a), ndv(x)) = 50 / 100 = 0.5
  rlFanout = |t| / max(ndv(a), ndv(x)) = 1000 / 100 = 10

t JOIN v ON a = p:
  fanout   = |v| / max(ndv(a), ndv(p)) = 500 / 100 = 5
  rlFanout = |t| / max(ndv(a), ndv(p)) = 1000 / 100 = 10
```

## Join Types

### Inner Join

Both sides must match on equality keys. NULL keys are eliminated.

**Output cardinality:**

```
resultCardinality = |left| × fanout × filterSelectivity
```

This is the total number of (left row, right row) pairs that match on
both the equality keys and the non-equi filter. Equivalently:

```
resultCardinality = |left| × |right| / max(ndv(leftKey), ndv(rightKey)) × filterSelectivity
```

| Column | cardinality | nullFraction | range |
|---|---|---|---|
| Keys (both) | minNdv | 0 | intersect(L, R) |
| Left Payloads | max(1, sampledNdv(c, leftSelectivity)) | input | input |
| Right Payloads | max(1, sampledNdv(c, rightSelectivity)) | input | input |

**Keys:** After the equi-join `leftKey = rightKey`, only values present on
both sides survive. Left and right keys receive the same constraints
since they represent the same set of matched values. Under the uniform
distribution assumption, the smaller set is a subset of the larger, so
the number of surviving distinct values is
`min(ndv(leftKey), ndv(rightKey))`. `NULL = NULL` is false in SQL, so
all NULL keys are eliminated: nullFraction becomes 0. The output
range is the intersection of both input ranges since only values in the
overlapping range can match.

**Payloads:** The join filters rows by key matching, which is independent
of payload values — `sampledNdv` applies. NULLs and non-NULLs are
filtered at the same rate, so nullFraction is unchanged. Similarly,
min/max don't shift since the filter is unrelated to payload values.

**Example:**

```
SELECT * FROM t INNER JOIN u ON a = x

|left|  = 1000
|right| = 50

resultCardinality = 1000 × 0.5 = 500

leftSelectivity  = min(1, 0.5) × 1.0 = 0.5
rightSelectivity = min(1, 10) × 1.0 = 1.0

Input constraints:
  a: ndv=100, 10% nulls, range=[1,200]
  b: ndv=500, no nulls, no range
  x: ndv=50, no nulls, range=[50,150]
  y: ndv=40, no nulls, no range

Output constraints:
  a: ndv=50, no nulls, range=[50,150]
  b: ndv=375, no nulls, no range
  x: <same as a>
  y: <same as input>
```

Key ranges narrow to the intersection of input ranges: `a` had [1,200]
and `x` had [50,150], so both output as [50,150].

Fanout = 0.5 means only half the left key values exist on the right,
so half the left rows are eliminated. Left payload `b`:
`sampledNdv(b, 0.5) = 500 × (1 - 0.5^(1000/500)) = 500 × 0.75 = 375`.
With 2 rows per distinct value (`1000/500`), even keeping half the rows
retains 75% of distinct values. `rlFanout = 10` means every right row
matches, so `rightSelectivity = 1.0` and right payload `y` keeps its
original ndv of 40.

When `fanout >= 1` (e.g. `t JOIN v` with `fanout = 5`), all left rows
match and `leftSelectivity = 1.0` — left payload ndv is unchanged.

### Left Join

All left rows are preserved. Unmatched left rows produce NULLs for right
columns. Left side is not optional; right side is optional.

**Output cardinality:**

```
resultCardinality = |left| × max(1, fanout × filterSelectivity)
```

The `max(1, ...)` ensures every left row produces at least one output
row. When `fanout × filterSelectivity >= 1`, each left row matches one
or more right rows. When `fanout × filterSelectivity < 1`, unmatched
left rows still produce output (with NULLs on the right), so the
minimum fanout is 1.

| Column | cardinality | nullFraction | range |
|---|---|---|---|
| Left (all) | input | input | input |
| Right Keys | minNdv | rightNullFraction | intersect(L, R) |
| Right Payloads | max(1, sampledNdv(c, rightSelectivity)) | rightNullFraction | input |

**Left side:** Completely unchanged. All left rows survive regardless of
whether they find a match, so all left column constraints (keys and
payloads) are preserved from input.

**Right keys:** Only values present on both sides appear as non-NULL.
The ndv is `min(ndv(leftKey), ndv(rightKey))` and the range narrows to
the intersection. Unmatched left rows produce NULLs for right keys, so
`nullFraction = rightNullFraction = max(0, 1 - ndv(rightKey) / ndv(leftKey))`.

**Right payloads:** Only right rows that match a left key appear in the
output (unmatched left rows produce NULLs for right columns).
`sampledNdv` with `rightSelectivity` gives the ndv of the matched
portion. NullFraction is set to `rightNullFraction` — the same fraction
as right keys, since the same unmatched left rows produce NULLs across
all right columns.

**Example:**

```
SELECT * FROM t LEFT JOIN u ON a = x

|left|  = 1000
|right| = 50

fanout = 0.5, adjustedFanout = max(1, 0.5) = 1
resultCardinality = 1000 × 1 = 1000

rightNullFraction = max(0, 1 - ndv(x)/ndv(a)) = 1 - 50/100 = 0.5

Input constraints:
  a: ndv=100, 10% nulls, range=[1,200]
  b: ndv=500, no nulls, no range
  x: ndv=50, no nulls, range=[50,150]
  y: ndv=40, no nulls, no range

Output constraints:
  a: <same as input>
  b: <same as input>
  x: ndv=50, 50% nulls, range=[50,150]
  y: ndv=40, 50% nulls, no range
```

Half the left key values (50 out of 100) don't exist on the right, so
half the output rows have NULLs for all right columns. The left side is
entirely unchanged — the 10% nulls in `a` are preserved since NULL left
keys survive the join (they simply don't match any right row).

Note: even with `t LEFT JOIN v` where `fanout = 5`, the
`rightNullFraction` is still 0.5 (since `ndv(p) = 50 < ndv(a) = 100`).
High fanout means matching rows match many times, but it doesn't mean
all left key values find a match.

### Right Join

All right rows are preserved. Unmatched right rows produce NULLs for left
columns. Right side is not optional; left side is optional.

**Output cardinality:**

```
resultCardinality = |right| × max(1, rlFanout × filterSelectivity)
```

The `max(1, ...)` ensures every right row produces at least one output
row. When `rlFanout × filterSelectivity >= 1`, each right row matches one
or more left rows. When `rlFanout × filterSelectivity < 1`, unmatched
right rows still produce output (with NULLs on the left), so the
minimum reverse fanout is 1.

| Column | cardinality | nullFraction | range |
|---|---|---|---|
| Right (all) | input | input | input |
| Left Keys | minNdv | leftNullFraction | intersect(L, R) |
| Left Payloads | max(1, sampledNdv(c, leftSelectivity)) | leftNullFraction | input |

**Right side:** Completely unchanged. All right rows survive regardless of
whether they find a match, so all right column constraints (keys and
payloads) are preserved from input.

**Left keys:** Only values present on both sides appear as non-NULL.
The ndv is `min(ndv(leftKey), ndv(rightKey))` and the range narrows to
the intersection. Unmatched right rows produce NULLs for left keys, so
`nullFraction = leftNullFraction = max(0, 1 - ndv(leftKey) / ndv(rightKey))`.

**Left payloads:** Only left rows that match a right key appear in the
output (unmatched right rows produce NULLs for left columns).
`sampledNdv` with `leftSelectivity` gives the ndv of the matched
portion. NullFraction is set to `leftNullFraction` — the same fraction
as left keys, since the same unmatched right rows produce NULLs across
all left columns.

**Example:**

```
SELECT * FROM t RIGHT JOIN u ON a = x

|left|  = 1000
|right| = 50

fanout = 0.5, rlFanout = 10
resultCardinality = 50 × max(1, 10) = 500

leftNullFraction = max(0, 1 - ndv(a)/ndv(x)) = max(0, 1 - 100/50) = 0

Input constraints:
  a: ndv=100, 10% nulls, range=[1,200]
  b: ndv=500, no nulls, no range
  x: ndv=50, no nulls, range=[50,150]
  y: ndv=40, no nulls, no range

Output constraints:
  a: ndv=50, no nulls, range=[50,150]
  b: ndv=375, no nulls, no range
  x: <same as input>
  y: <same as input>
```

Every right key value exists on the left (`ndv(a) = 100 >= ndv(x) = 50`),
so `leftNullFraction = 0` — no output rows need NULLs for left columns.
Each of the 50 right rows matches ~10 left rows, producing 500 output
rows. Left payload `b`:
`sampledNdv(b, 0.5) = 500 × (1 - 0.5^(1000/500)) = 500 × 0.75 = 375`.

This is equivalent to `SELECT * FROM u LEFT JOIN t ON x = a` with left
and right columns swapped.

### Full Outer Join

All rows from both sides are preserved. Unmatched left rows produce NULLs
for right columns; unmatched right rows produce NULLs for left columns.
Both sides are optional.

**Output cardinality:**

```
matchedRows = |left| × fanout × filterSelectivity
unmatchedLeft  = |left| × max(0, 1 - fanout × filterSelectivity)
unmatchedRight = |right| × max(0, 1 - rlFanout × filterSelectivity)
resultCardinality = matchedRows + unmatchedLeft + unmatchedRight
```

`matchedRows` counts pairs where both sides match (same as inner join).
`unmatchedLeft` counts left rows with no right match — each produces one
output row with NULLs for all right columns. `unmatchedRight` counts right
rows with no left match — each produces one output row with NULLs for all
left columns.

Equivalently:

```
resultCardinality = |left| × max(1, fanout × filterSelectivity)
                  + |right| × max(0, 1 - rlFanout × filterSelectivity)
```

The first term is the left join cardinality (matched + unmatched left rows).
The second term adds the unmatched right rows that the left join doesn't
account for.

| Column | cardinality | nullFraction | range |
|---|---|---|---|
| Left Keys | minNdv | leftNullFraction | intersect(L, R) |
| Left Payloads | max(1, sampledNdv(c, leftSelectivity)) | leftNullFraction | input |
| Right Keys | minNdv | rightNullFraction | intersect(L, R) |
| Right Payloads | max(1, sampledNdv(c, rightSelectivity)) | rightNullFraction | input |

**Left keys:** Only values present on both sides appear as non-NULL.
The ndv is `min(ndv(leftKey), ndv(rightKey))` and the range narrows to
the intersection. Unmatched right rows produce NULLs for left keys, so
`nullFraction = leftNullFraction = max(0, 1 - ndv(leftKey) / ndv(rightKey))`.

**Left payloads:** Only left rows that match a right key contribute
non-NULL values. Unmatched right rows produce NULLs for all left columns.
`sampledNdv` with `leftSelectivity` gives the ndv of the matched
portion. The nullFraction is `leftNullFraction` — the same fraction as
left keys, since the same unmatched right rows produce NULLs across all
left columns.

**Right keys:** Symmetric to left keys. The ndv is
`min(ndv(leftKey), ndv(rightKey))` and the range narrows to the
intersection. Unmatched left rows produce NULLs for right keys, so
`nullFraction = rightNullFraction = max(0, 1 - ndv(rightKey) / ndv(leftKey))`.

**Right payloads:** Symmetric to left payloads. Only right rows that
match a left key contribute non-NULL values. Unmatched left rows produce
NULLs for all right columns. `sampledNdv` with `rightSelectivity` gives
the ndv of the matched portion. The nullFraction is `rightNullFraction`.

**Example:**

```
SELECT * FROM t FULL JOIN u ON a = x

|left|  = 1000
|right| = 50

fanout = 0.5, rlFanout = 10
matchedRows    = 1000 × 0.5 = 500
unmatchedLeft  = 1000 × max(0, 1 - 0.5) = 500
unmatchedRight = 50 × max(0, 1 - 10) = 0
resultCardinality = 500 + 500 + 0 = 1000

leftNullFraction  = max(0, 1 - ndv(a)/ndv(x)) = max(0, 1 - 100/50) = 0
rightNullFraction = max(0, 1 - ndv(x)/ndv(a)) = 1 - 50/100 = 0.5

Input constraints:
  a: ndv=100, 10% nulls, range=[1,200]
  b: ndv=500, no nulls, no range
  x: ndv=50, no nulls, range=[50,150]
  y: ndv=40, no nulls, no range

Output constraints:
  a: ndv=50, no nulls, range=[50,150]
  b: ndv=375, no nulls, no range
  x: ndv=50, 50% nulls, range=[50,150]
  y: ndv=40, 50% nulls, no range
```

The right side has fewer distinct key values (`ndv(x) = 50 < ndv(a) = 100`),
so all right key values exist on the left (`leftNullFraction = 0`) — no
output rows need NULLs for left columns. But half the left key values
(50 out of 100) don't exist on the right, so `rightNullFraction = 0.5` —
half the output rows have NULLs for all right columns.

Since every right row finds a match (`rlFanout = 10 >= 1`), there are no
unmatched right rows. The result is the same as `t LEFT JOIN u ON a = x`.

In general, when one side's keys are a subset of the other side's keys, the
full join degenerates to a left or right join. When both sides have keys
not present on the other, both `leftNullFraction` and `rightNullFraction`
are positive and the result is strictly larger than either a left or right
join alone.

### Left Semi Filter

Returns left rows that have at least one match on the right. Each left row
appears at most once regardless of how many right rows match. Only left
columns are output; right columns are not included.

**Output cardinality:**

```
resultCardinality = |left| × min(1, fanout) × filterSelectivity
```

`min(1, fanout)` is the fraction of left rows whose key exists on the
right. When `fanout >= 1` every left row finds at least one match and
`min(1, fanout) = 1`, so selectivity comes only from the non-equi filter.
When `fanout < 1`, some left key values don't exist on the right and
the corresponding rows are eliminated.

| Column | cardinality | nullFraction | range | trueFraction |
|---|---|---|---|---|
| Left Keys | minNdv | 0 | intersect(L, R) | — |
| Left Payloads | max(1, sampledNdv(c, leftSelectivity)) | input | input | — |

**Keys:** Same as inner join. After the equi-key match, only values
present on both sides survive. The ndv is
`min(ndv(leftKey), ndv(rightKey))`, the range narrows to the
intersection, and nullFraction becomes 0 (NULL keys never match).

**Payloads:** The join acts as a filter on left rows — it eliminates
rows whose key has no match, but doesn't duplicate any row.
`sampledNdv` with `leftSelectivity` applies. NullFraction and range are
unchanged.

**Example:**

```
SELECT a, b FROM t WHERE EXISTS (SELECT 1 FROM u WHERE a = x)

|left|  = 1000
|right| = 50

fanout = 0.5, filterSelectivity = 1.0
leftSelectivity = min(1, 0.5) × 1.0 = 0.5
resultCardinality = 1000 × 0.5 = 500

Input constraints:
  a: ndv=100, 10% nulls, range=[1,200]
  b: ndv=500, no nulls, no range

Output constraints:
  a: ndv=50, no nulls, range=[50,150]
  b: ndv=375, no nulls, no range
```

Half the left key values (50 out of 100) exist on the right, so half the
left rows survive. Left key `a` loses its NULLs (`NULL = x` is never
true) and its ndv drops to 50. Payload `b`:
`sampledNdv(b, 0.5) = 500 × (1 - 0.5^(1000/500)) = 500 × 0.75 = 375`.

### Left Semi Project

Returns all left rows with an additional boolean mark column indicating
whether each row has at least one match on the right. Only left columns
plus the mark column are output; right columns are not included.

**Output cardinality:**

```
resultCardinality = |left|
```

Every left row survives — matched rows get `mark = true`, unmatched rows
get `mark = false`. The output cardinality equals the left input
cardinality.

| Column | cardinality | nullFraction | range | trueFraction |
|---|---|---|---|---|
| Left (all) | input | input | input | — |
| Mark | — | 0 | — | min(1, fanout) × filterSelectivity |

**Left (all):** Completely unchanged. All left rows survive, so all
column constraints are preserved from input. Unlike Left Semi Filter,
no rows are eliminated, so no scaling is needed.

**Mark:** A non-nullable boolean. Its `trueFraction` is
`min(1, fanout) × filterSelectivity` — the fraction of left rows that
found at least one match after applying both equi-key and non-equi
filter conditions. This is the same as the selectivity of the
corresponding Left Semi Filter: filtering on `mark = true` yields
exactly the Left Semi Filter result.

**Example:**

```
SELECT a, b, EXISTS (SELECT 1 FROM u WHERE a = x) as mark FROM t

|left|  = 1000
|right| = 50

fanout = 0.5, filterSelectivity = 1.0
resultCardinality = 1000

trueFraction = min(1, 0.5) × 1.0 = 0.5

Input constraints:
  a: ndv=100, 10% nulls, range=[1,200]
  b: ndv=500, no nulls, no range

Output constraints:
  a: <same as input>
  b: <same as input>
  mark: trueFraction=0.5, no nulls
```

All 1000 left rows appear in the output. Half have `mark = true`
(their key exists on the right) and half have `mark = false`.
Left columns `a` and `b` retain their original constraints since no
rows are filtered.

### Right Semi Filter

Returns right rows that have at least one match on the left. Each right
row appears at most once regardless of how many left rows match. Only right
columns are output; left columns are not included.

This is the mirror of Left Semi Filter with left and right swapped.

**Output cardinality:**

```
resultCardinality = |right| × min(1, rlFanout) × filterSelectivity
```

`min(1, rlFanout)` is the fraction of right rows whose key exists on the
left. When `rlFanout >= 1` every right row finds at least one match.
When `rlFanout < 1`, some right key values don't exist on the left and
the corresponding rows are eliminated.

| Column | cardinality | nullFraction | range | trueFraction |
|---|---|---|---|---|
| Right Keys | minNdv | 0 | intersect(L, R) | — |
| Right Payloads | max(1, sampledNdv(c, rightSelectivity)) | input | input | — |

**Keys:** Same as inner join. After the equi-key match, only values
present on both sides survive. The ndv is
`min(ndv(leftKey), ndv(rightKey))`, the range narrows to the
intersection, and nullFraction becomes 0.

**Payloads:** The join acts as a filter on right rows — it eliminates
rows whose key has no match, but doesn't duplicate any row.
`sampledNdv` with `rightSelectivity` applies. NullFraction and range
are unchanged.

**Example:**

```
SELECT x, y FROM u WHERE EXISTS (SELECT 1 FROM t WHERE x = a)

|left|  = 1000
|right| = 50

rlFanout = 10, filterSelectivity = 1.0
rightSelectivity = min(1, 10) × 1.0 = 1.0
resultCardinality = 50 × 1.0 = 50

Input constraints:
  x: ndv=50, no nulls, range=[50,150]
  y: ndv=40, no nulls, no range

Output constraints:
  x: ndv=50, no nulls, range=[50,150]
  y: ndv=40, no nulls, no range
```

Every right key value exists on the left (`rlFanout = 10 >= 1`), so all
50 right rows survive. Right columns are unchanged — the semi join is a
no-op because every right key has at least one match on the left.

### Right Semi Project

Returns all right rows with an additional boolean mark column indicating
whether each row has at least one match on the left. Only right columns
plus the mark column are output; left columns are not included.

This is the mirror of Left Semi Project with left and right swapped.

**Output cardinality:**

```
resultCardinality = |right|
```

Every right row survives — matched rows get `mark = true`, unmatched rows
get `mark = false`.

| Column | cardinality | nullFraction | range | trueFraction |
|---|---|---|---|---|
| Right (all) | input | input | input | — |
| Mark | — | 0 | — | min(1, rlFanout) × filterSelectivity |

**Right (all):** Completely unchanged. All right rows survive, so all
column constraints are preserved from input. Unlike Right Semi Filter,
no rows are eliminated, so no scaling is needed.

**Mark:** A non-nullable boolean. Its `trueFraction` is
`min(1, rlFanout) × filterSelectivity` — the fraction of right rows
that found at least one match after applying both equi-key and non-equi
filter conditions. This is the same as the selectivity of the
corresponding Right Semi Filter: filtering on `mark = true` yields
exactly the Right Semi Filter result.

**Example:**

```
SELECT x, y, EXISTS (SELECT 1 FROM t WHERE x = a) as mark FROM u

|left|  = 1000
|right| = 50

rlFanout = 10, filterSelectivity = 1.0
resultCardinality = 50

trueFraction = min(1, 10) × 1.0 = 1.0

Input constraints:
  x: ndv=50, no nulls, range=[50,150]
  y: ndv=40, no nulls, no range

Output constraints:
  x: <same as input>
  y: <same as input>
  mark: trueFraction=1.0, no nulls
```

All 50 right rows appear in the output. Since `rlFanout = 10 >= 1`,
every right row finds a match, so `trueFraction = 1.0` — all mark
values are true. Right columns retain their original constraints.

### Anti Join

Returns left rows that have NO match on the right. Each left row that
doesn't match any right row (on both equi-keys and the non-equi filter)
appears exactly once in the output. Only left columns are output; right
columns are not included.

This is the complement of Left Semi Filter: semi filter returns the rows
that match, anti join returns the rows that don't.

**Output cardinality:**

```
resultCardinality = |left| × antiSelectivity
                  = |left| × max(0, 1 - fanout × filterSelectivity)
```

`antiSelectivity` is the fraction of left rows that do NOT find a match
on the right. When `fanout < 1`, some left key values don't exist on the
right and the corresponding rows survive. When `fanout >= 1`, the
estimate assumes all left key values have matches, giving
`antiSelectivity = 0`.

| Column | cardinality | nullFraction | range |
|---|---|---|---|
| Left Keys | max(1, ndv(leftKey) - minNdv) | input | input |
| Left Payloads | max(1, sampledNdv(c, antiSelectivity)) | input | input |

**Keys:** The anti join returns left rows whose key value does NOT exist
on the right. Under uniform distribution, the number of left key values
present on the right is `minNdv = min(ndv(leftKey), ndv(rightKey))`,
so the number of non-matching left key values is
`ndv(leftKey) - minNdv`. NULL left keys always survive since
`NULL = rightKey` is never true. The range is preserved from input —
non-matching values can appear anywhere in the left range, not just
outside the intersection.

Unlike semi join where `nullFraction = 0` (NULLs are eliminated because
they never match), anti join preserves NULLs (they never match, so they
always survive). The nullFraction is preserved from input as an
approximation; the actual fraction may be slightly higher since
matching non-NULL rows are removed while all NULL rows survive.

**Payloads:** The anti join acts as a filter on left rows — it eliminates
rows whose key has a match, but doesn't duplicate any row.
`sampledNdv` with `antiSelectivity` applies. NullFraction and range are
unchanged.

**Example:**

```
SELECT a, b FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE a = x)

|left|  = 1000
|right| = 50

fanout = 0.5, filterSelectivity = 1.0
antiSelectivity = max(0, 1 - 0.5 × 1.0) = 0.5
resultCardinality = 1000 × 0.5 = 500

minNdv = min(100, 50) = 50

Input constraints:
  a: ndv=100, 10% nulls, range=[1,200]
  b: ndv=500, no nulls, no range

Output constraints:
  a: ndv=50, 10% nulls, range=[1,200]
  b: ndv=375, no nulls, no range
```

Half the left key values (50 out of 100) exist on the right, so the
other half don't — those 50 non-matching key values survive along with
their rows. Left key `a` retains its NULLs (NULL keys never match, so
they always pass the anti join) and its range stays [1,200] since
non-matching values span the full left range. Payload `b`:
`sampledNdv(b, 0.5) = 500 × (1 - 0.5^(1000/500)) = 500 × 0.75 = 375`.

Contrast with the Left Semi Filter example: semi filter returns the
500 matching rows with `a: ndv=50, no nulls, range=[50,150]` (keys
narrowed to the intersection, NULLs eliminated). Anti join returns the
other 500 rows with `a: ndv=50, 10% nulls, range=[1,200]` (keys outside
the intersection, NULLs preserved, full left range retained).

## Fanout Estimation

### Fanout Formula

Fanout is the expected number of right-side matches per left row. For an
equi-join `left.a = right.x`, let:

- `d_L = ndv(leftKey)`, `d_R = ndv(rightKey)` — distinct key values on each side
- `|right|` — row count of the right side

Under the **uniform distribution** assumption, each distinct right key value
appears `|right| / d_R` times. So if a left row's key value `v` exists on
the right side, it matches `|right| / d_R` rows.

But not every left key value necessarily exists on the right side. Under the
**containment** assumption, the smaller set of distinct values is contained
within the larger set. The overlap is `min(d_L, d_R)` distinct values, so
the probability that a given left key value has a match on the right is:

```
P(match) = min(d_L, d_R) / d_L
```

Combining:

```
fanout = P(match) × matches_per_hit
       = min(d_L, d_R) / d_L  ×  |right| / d_R
```

When `d_L ≥ d_R`: `min(d_L, d_R) = d_R`, so
`fanout = (d_R / d_L) × (|right| / d_R) = |right| / d_L`.

When `d_R ≥ d_L`: `min(d_L, d_R) = d_L`, so
`fanout = (d_L / d_L) × (|right| / d_R) = |right| / d_R`.

Both cases simplify to:

```
fanout = |right| / max(d_L, d_R)
```

The symmetric reverse fanout is `rlFanout = |left| / max(d_L, d_R)`.

### Implementation

Fanout estimation happens during query graph construction (`ToGraph`), before
the optimizer builds `Join` operators. Join edges estimate fanout via
`JoinEdge::guessFanout()`, which sets both `lrFanout` (left-to-right) and
`rlFanout` (right-to-left).

#### Step 1: Early Exit

If no left table or no join keys:
```
lrFanout = 1.1
rlFanout = 1
```

The slight asymmetry breaks ties in join ordering.

#### Step 2: Compute Fanout and Uniqueness

Call `joinFanout(table, keys, otherKeys)` on both sides. This computes:
- `fanout`: `estimateFanout(|table|, keys, otherKeys)` using
  `|table| / max(ndv(thisKey), ndv(otherKey))`.
- `unique`: for BaseTable, `SchemaTable::isUnique(keys)` checks if the key
  columns match a uniqueness constraint on any index. For DerivedTable with
  an aggregation, unique is true when keys cover all grouping keys.
  Otherwise false.

#### Step 3: Apply Fanouts

If one side has unique keys (PK-FK join), the unique side has fanout ≤ 1
by definition:
```
if leftUnique:
    rlFanout = baseSelectivity(leftTable)
    lrFanout = |right| / |left| × baseSelectivity(rightTable)
else if rightUnique:
    lrFanout = baseSelectivity(rightTable)
    rlFanout = |left| / |right| × baseSelectivity(leftTable)
```

This reflects that a PK-FK join produces at most 1 match on the PK side,
while the FK side fanout is proportional to the table cardinality ratio.
When both sides are unique (1:1 join), leftUnique takes precedence.

Otherwise, if historical sample data is available from
`VeloxHistory::sampleJoin()`:
```
lrFanout = sampleRight × baseSelectivity(rightTable)
rlFanout = sampleLeft × baseSelectivity(leftTable)
```

Otherwise, use the computed fanouts:
```
lrFanout = right.fanout × baseSelectivity(rightTable)
rlFanout = left.fanout × baseSelectivity(leftTable)
```

`baseSelectivity(table)` returns the fraction of rows selected by non-join
filters on the table (1.0 for non-BaseTable inputs).

## Related Documentation

- See [JoinEstimationQuickRef.md](JoinEstimationQuickRef.md) for a compact
  cheat-sheet of join cardinality and constraint propagation formulas.
- See [CardinalityEstimation.md](CardinalityEstimation.md) for cardinality
  estimation of all operators (not just joins).
- See [PayloadNdvScaling.md](PayloadNdvScaling.md) for why the coupon
  collector formula is preferred over linear scaling.
- See [FilterSelectivity.md](FilterSelectivity.md) for filter selectivity
  estimation details.
