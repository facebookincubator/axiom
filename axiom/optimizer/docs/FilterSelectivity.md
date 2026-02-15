# Filter Selectivity Estimation

This document describes how filter selectivity is estimated in the Axiom
optimizer. Selectivity estimation is used for cost-based optimization to
predict the number of rows that will pass through a filter, enabling better
join ordering and operation placement decisions.

## Introduction

Filter selectivity is expressed as a `Selectivity` struct with two components:

- **trueFraction**: Probability that the filter evaluates to TRUE (row passes)
- **nullFraction**: Probability that the filter evaluates to NULL

The remaining probability `(1 - trueFraction - nullFraction)` represents rows
where the filter evaluates to FALSE. The invariant `trueFraction + nullFraction
<= 1` must always hold.

Selectivity estimation relies on column constraints — statistical properties
maintained for each column:

- **min** / **max**: The range of values in the column.
- **cardinality**: The number of distinct values.
- **nullFraction**: The fraction of values that are NULL.

These constraints are initially derived from table statistics (e.g., Hive
metastore) and are progressively refined as the optimizer processes filter
expressions (see [Constraint Propagation](#constraint-propagation) below).

## Expression Types

The optimizer recognizes and handles the following expression types:

| Expression | Description |
|------------|-------------|
| `NOT` | Logical negation |
| `AND` | Logical conjunction |
| `OR` | Logical disjunction |
| `eq`, `lt`, `lte`, `gt`, `gte` | Comparison operators |
| `IN` | Membership test |
| `IS NULL` | Null check |
| Boolean columns | Direct use of boolean columns as predicates |
| Boolean literals | `TRUE`, `FALSE`, `NULL` as filter conditions |

The following expressions are **not yet supported** and receive default selectivity (0.8):

| Expression | Description |
|------------|-------------|
| `<>` (neq) | Not-equal comparison (use `NOT(x = value)` for better estimates) |
| `BETWEEN` | Range membership (use `x >= lower AND x <= upper` for better estimates) |

### Logical Operators

#### NOT

Inverts true and false fractions while preserving the null fraction.

```
selectivity(NOT expr) = {falseFraction(expr), nullFraction(expr)}
```

Note that `falseFraction = 1 - trueFraction - nullFraction`, so this differs from
simply inverting the true fraction when nullFraction is non-zero.

**Example:** For `NOT(x > 10)` where `x > 10` has selectivity 0.5 (no nulls):
- `NOT(x > 10)` has selectivity 1 - 0.5 = 0.5

Summary: 50% of rows pass, 50% do not pass.

**Example with nulls:** For `NOT(x > 10)` where `x > 10` has trueFraction = 0.5,
nullFraction = 0.1 (50% true, 10% null, 40% false):
- `falseFraction = 1 - 0.5 - 0.1 = 0.4`
- `NOT(x > 10)` has selectivity {0.4, 0.1}

Summary: 40% of rows pass, 50% do not pass, 10% return NULL.

#### AND (Conjunction)

For a conjunction of conditions, the result is TRUE only if all conditions are
TRUE, and NULL if any is NULL and none is FALSE.

```
P(TRUE) = ∏ trueFraction(i)
P(NULL) = ∏ (trueFraction(i) + nullFraction(i)) - P(TRUE)
```

This assumes independence between conditions. When conditions share the same
left-hand side column (e.g., `x > 5 AND x < 10`), they are grouped and processed
together using range selectivity to avoid underestimating selectivity. Only
comparisons with a literal on the right-hand side are grouped this way.
Column-vs-column comparisons (e.g., `a = b`) are evaluated individually using
cardinality-based selectivity (see
[Cardinality-Based Selectivity](#cardinality-based-selectivity) below).

**Note:** Expressions are normalized during planning, so `5 < x` is always
converted to `x > 5`. This ensures the column is consistently on the left-hand
side, enabling proper grouping of range conditions on the same column.

**Example:** For `x > 10 AND y = 'foo'` where `x > 10` has selectivity 0.5 and
`y = 'foo'` has selectivity 0.4:
- `P(TRUE) = 0.5 × 0.4 = 0.2` (20% of rows pass both conditions)

**Example with nulls:** For `a > 5 AND b < 10` where:
- `a > 5` has trueFraction = 0.5, nullFraction = 0.1 (50% true, 10% null, 40% false)
- `b < 10` has trueFraction = 0.4, nullFraction = 0.2 (40% true, 20% null, 40% false)

The result is:
- `P(TRUE) = 0.5 × 0.4 = 0.2` — Both conditions must be TRUE
- `P(not-FALSE) = (0.5 + 0.1) × (0.4 + 0.2) = 0.36` — Neither condition is FALSE
- `P(NULL) = 0.36 - 0.2 = 0.16` — At least one is NULL and neither is FALSE
- `P(FALSE) = 1 - 0.2 - 0.16 = 0.64` — At least one condition is FALSE

Summary: 20% of rows pass, 64% do not pass, 16% return NULL.

#### OR (Disjunction)

For a disjunction, the result is TRUE if any condition is TRUE, and FALSE only
if all are FALSE.

```
P(TRUE) = 1 - ∏ (1 - trueFraction(i))
P(FALSE) = ∏ (1 - trueFraction(i) - nullFraction(i))
P(NULL) = 1 - P(TRUE) - P(FALSE)
```

**Example:** For `x > 10 OR y = 'foo'` where `x > 10` has selectivity 0.5 and
`y = 'foo'` has selectivity 0.4 (no nulls):
- `P(FALSE) = (1 - 0.5) × (1 - 0.4) = 0.5 × 0.6 = 0.3` — Both conditions are FALSE
- `P(TRUE) = 1 - 0.3 = 0.7` — At least one condition is TRUE

Summary: 70% of rows pass, 30% do not pass.

**Example with nulls:** For `a > 5 OR b < 10` where:
- `a > 5` has trueFraction = 0.5, nullFraction = 0.1 (50% true, 10% null, 40% false)
- `b < 10` has trueFraction = 0.4, nullFraction = 0.2 (40% true, 20% null, 40% false)

The result is:
- `P(FALSE) = (1 - 0.5 - 0.1) × (1 - 0.4 - 0.2) = 0.4 × 0.4 = 0.16` — Both are FALSE
- `P(not-TRUE) = (1 - 0.5) × (1 - 0.4) = 0.5 × 0.6 = 0.3` — Neither is TRUE
- `P(NULL) = 0.3 - 0.16 = 0.14` — At least one is NULL and neither is TRUE
- `P(TRUE) = 1 - 0.16 - 0.14 = 0.7` — At least one condition is TRUE

Summary: 70% of rows pass, 16% do not pass, 14% return NULL.

### Comparison Operators

The optimizer handles: `eq`, `lt`, `lte`, `gt`, `gte`.

#### Column vs Literal Comparison

When comparing a column to a literal (e.g., `x = 5`, `y < 100`), the optimizer
uses **range selectivity**. The formula depends on the comparison type:

**For equality (`x = 5`):**
```
selectivity = 1 / cardinality
```

This assumes uniform distribution: if a column has N distinct values, the
probability of matching any specific value is 1/N.

**For range conditions (`x > 5`, `x < 100`, `x >= 5`, `x <= 100`):**

For a column with known bounds `[min, max]`:

```
selectivity = (effectiveUpper - effectiveLower) / (max - min)
```

where:
- `effectiveLower = max(min, literal)` — The effective lower bound is the
  larger of the column's minimum value and the literal from the comparison.
  For example, if a column has values in `[0, 100]` and the filter is `x > 30`,
  the effective lower bound is 30 (from the literal), not 0 (from the column).
- `effectiveUpper = min(max, literal)` — The effective upper bound is the
  smaller of the column's maximum value and the literal from the comparison.
  For example, if a column has values in `[0, 100]` and the filter is `x < 80`,
  the effective upper bound is 80 (from the literal), not 100 (from the column).

If no literal bound is specified for a direction (e.g., `x > 30` has no upper
bound), the column's bound is used as the effective bound.

All selectivity results are scaled by `(1 - nullFraction)` since NULLs cannot
satisfy comparisons.

**Strict inequality adjustment for integer types:** For integer types (TINYINT,
SMALLINT, INTEGER, BIGINT, HUGEINT), strict inequality bounds are adjusted to
inclusive bounds:
- `x > 5` becomes `x >= 6` (lower bound incremented by 1)
- `x < 5` becomes `x <= 4` (upper bound decremented by 1)

This produces exact selectivity for integer ranges (e.g., `x > 5 AND x < 10` on
integers gives the count of `{6, 7, 8, 9}` rather than a continuous
approximation). Non-integer types (DOUBLE, VARCHAR) keep strict bounds unchanged.

If the literal falls outside the column's range, the result is an empty
intersection with zero selectivity (before null scaling). For example:
- `x > 50` on a column with range `[0, 30]`: `effectiveLower = 50`,
  `effectiveUpper = 30`, so `effectiveLower > effectiveUpper` → selectivity = 0
- `x < 10` on a column with range `[20, 100]`: `effectiveLower = 20`,
  `effectiveUpper = 10`, so `effectiveLower > effectiveUpper` → selectivity = 0

#### Column vs Column Comparison

When comparing two columns (e.g., `a = b`, `a < b`), the optimizer uses
**column comparison selectivity** which considers the ranges and cardinalities
of both columns.

For equality (`a = b`):
- Compute the overlap of the two column ranges
- Estimate the number of distinct values in the overlap from each side:
  - `n_a = cardinality_a × (overlap_size / range_a)` — distinct values from `a` in the overlap
  - `n_b = cardinality_b × (overlap_size / range_b)` — distinct values from `b` in the overlap
- Probability = min(n_a, n_b) / (cardinality_a × cardinality_b)

For inequality (`a < b`):
- If ranges don't overlap: P = 0 or 1 depending on relative positions
- If ranges overlap: Integrate probability using uniform distribution assumption

When min/max bounds are not available, falls back to cardinality-based estimation
(see [Cardinality-Based Selectivity](#cardinality-based-selectivity) below).

> **TODO:** Handle `<>` (not-equal) operator. Currently treated as unknown function
> with default selectivity of 0.8. Could be normalized to `NOT(eq)` for better
> estimates. For example, `x <> 5` would become `NOT(x = 5)` and get selectivity
> of `1 - 1/cardinality` instead of 0.8.

> **TODO:** Handle `BETWEEN` operator. Currently not supported. Could be normalized
> to `expr >= lower AND expr <= upper` for range selectivity estimation.

### IN Operator

The IN operator is handled as a range condition. Selectivity is computed as:

```
selectivity = |IN list| / cardinality
```

If the IN list contains non-literal values (e.g., column references), a default
selectivity of 0.5 is used.

If the IN list contains any NULL literals, zero selectivity is returned (comparing
with NULL always yields NULL).

The IN list is pruned based on known column bounds before computing selectivity.
Contradictions (empty list after pruning) result in near-zero selectivity
(`likelyZero`) since column bounds are estimates.

### IS NULL

The IS NULL predicate returns TRUE when its argument is NULL:

```
selectivity(expr IS NULL) = {nullFraction(expr), 0.0}
```

### Boolean Columns

For boolean columns used directly as filter conditions:
- If `trueFraction` is known from statistics, use it
- Otherwise, default to 0.8

### Boolean Literals

Boolean literals as filter conditions should be eliminated during optimization:
- `WHERE TRUE` → filter is removed (always passes)
- `WHERE FALSE` or `WHERE NULL` → subtree is replaced with empty set

If these reach selectivity estimation (fallback), the following values are used:
- NULL: `{0.0, 1.0}`
- FALSE: `{0.0, 0.0}`
- TRUE: `{1.0, 0.0}`

### Unknown Functions and Special Forms

Functions and special forms not explicitly handled by the optimizer (e.g., CAST,
COALESCE, CASE) receive default selectivity: `{0.8, 0.0}`.

## Mathematical Formulas

### NULL Handling

For comparisons involving two expressions, the probability that either is NULL:

```
P(either null) = P(a null) + P(b null) - P(a null) × P(b null)
```

All comparison selectivities are scaled by `(1 - P(either null))` since NULL
comparisons yield NULL, not TRUE.

### Range Selectivity for Numeric Types

Given a column with range `[min, max]` and a query range `[lower, upper]`:

```
effectiveLower = max(min, lower)  // Use column min if query has no lower bound
effectiveUpper = min(max, upper)  // Use column max if query has no upper bound

if effectiveLower > effectiveUpper:
    selectivity = 0.0  // Empty intersection
else:
    // For discrete types (integers):
    selectivity = (effectiveUpper - effectiveLower + 1) / (max - min + 1)
    // For continuous types (REAL, DOUBLE):
    selectivity = (effectiveUpper - effectiveLower) / (max - min)
```

**Example:** For `x > 30 AND x < 80` on a column with range `[0, 100]` and
nullFraction = 0.1:
- effectiveLower = max(0, 30) = 30
- effectiveUpper = min(100, 80) = 80
- The query matches values in `[30, 80]`, which covers 50 out of 100 possible
  values, so selectivity is 50%.
- 10% of values are NULL and cannot satisfy the filter, so the final
  selectivity = 0.5 × (1 - 0.1) = 0.45.

Summary: 45% of rows pass, 45% do not pass, 10% return NULL.

**Example (empty intersection):** For `x > 50` on a column with range `[0, 30]`:
- The column's maximum value is 30, but the filter requires values greater than
  50. No values can satisfy this condition, so selectivity is 0%.

Summary: 0% of rows pass.

### Range Selectivity for VARCHAR

For string columns, selectivity is estimated using the ASCII value of the first
character. This provides a coarse approximation:

```
charValue(str) = ASCII value of first character, or 0 if empty
selectivity = (charValue(upper) - charValue(lower) + 1) / (charValue(max) - charValue(min) + 1)
```

**Example:** For `city >= 'Chicago' AND city < 'Green Bay'` on a column with
range `['Albuquerque', 'Zurich']` and nullFraction = 0.1:
- Only the first character of each string is used. There are 26 possible
  first-character buckets from 'A' to 'Z'. The query matches 5 of them
  (C, D, E, F, G), so selectivity is 5 / 26 = 19.2%.
- 10% of values are NULL and cannot satisfy the filter, so the final
  selectivity = 0.192 × (1 - 0.1) = 0.173.

Summary: 17.3% of rows pass, 72.7% do not pass, 10% return NULL.

**Example (same first character):** For `city >= 'Boston' AND city < 'Bristol'`
on a column with range `['Albuquerque', 'Zurich']`:
- Both bounds map to the same first-character bucket ('B'). There are 26 buckets from 'A' to 'Z',
  and the query matches 1 of them (B), so selectivity is 1 / 26 = 3.8%.

### Column Equality (a = b)

When both columns have known ranges and cardinalities:

```
overlap_low = max(a.min, b.min)
overlap_high = min(a.max, b.max)
overlap_size = max(0, overlap_high - overlap_low)

n_a = a.cardinality × (overlap_size / a.range)  // Values from a in overlap
n_b = b.cardinality × (overlap_size / b.range)  // Values from b in overlap
matching = min(n_a, n_b)                         // Matching pairs

P(a = b) = matching / (a.cardinality × b.cardinality)
```

**Example:** For `a = b` where column `a` has range `[1000, 2000]` with 100
distinct values and column `b` has range `[1500, 2500]` with 100 distinct values:
- The two ranges overlap in `[1500, 2000]`, which is 500 out of each column's
  range of 1000. So roughly 50 of `a`'s 100 distinct values and 50 of `b`'s
  100 distinct values fall in the overlap.
- The probability that a randomly chosen value from `a` matches a randomly
  chosen value from `b` is 50 / (100 × 100) = 0.5%.

Summary: 0.5% of row pairs have matching values.

**Example (no overlap):** For `a = b` where column `a` has range `[100, 200]`
and column `b` has range `[300, 400]`:
- The ranges do not overlap, so no values from `a` can equal any value from
  `b`. Selectivity is 0%.

### Column Inequality (a < b)

For overlapping ranges:

```
below_b = portion of a's range below b.min
overlap_integral = average P(b > x) for x in overlap region
P(a < b) = (below_b + overlap_integral) / a.range
```

The overlap integral assumes uniform distribution:

```
overlap_integral = overlap_size × (2×b.max - overlap_high - overlap_low) / (2 × b.range)
```

**Example (no overlap, a below b):** For `a < b` where column `a` has range
`[100, 200]` and column `b` has range `[300, 400]`:
- Every value of `a` is less than every value of `b`, so selectivity is 100%.

**Example (no overlap, a above b):** For `a < b` where column `a` has range
`[300, 400]` and column `b` has range `[100, 200]`:
- Every value of `a` is greater than every value of `b`, so selectivity is 0%.

**Example (partial overlap):** For `a < b` where column `a` has range
`[1000, 2000]` and column `b` has range `[1500, 2500]`:
- Values of `a` below 1500 are definitely less than all values of `b`. That's
  500 out of `a`'s range of 1000, contributing 50%.
- Values of `a` in the overlap region `[1500, 2000]` are less than some but not
  all values of `b`. On average, a value in the middle of the overlap is less
  than about 75% of `b`'s values (since `b` extends to 2500). This contributes
  another 37.5%.
- Total selectivity: 50% + 37.5% = 87.5%.

Summary: 87.5% of row pairs satisfy `a < b`.

### Cardinality-Based Selectivity

When min/max bounds are not available, selectivity is estimated using only
cardinalities:

For equality:
```
P(a = b) = min(a.cardinality, b.cardinality) / (a.cardinality × b.cardinality)
```

For inequalities:
```
P(a < b) = P(a > b) = 0.5 × (1 - nullFraction)
```

(Floored at 1% when nullFraction ≈ 1.0.)

**Example (equality):** For `a = b` where column `a` has 50 distinct values and
column `b` has 200 distinct values (no range info):
- min(50, 200) / (50 × 200) = 50 / 10,000 = 0.5%.
- Intuitively, each of `a`'s 50 values has a 1-in-200 chance of matching a
  randomly chosen `b` value.

**Example (inequality):** For `a < b` where neither column has min/max bounds:
- Without range information, the optimizer cannot determine whether `a` tends to
  be smaller or larger than `b`, so it assumes 50% selectivity.

## Magic Numbers

The `Selectivity` struct provides factory methods for common estimation patterns
using the following public constants:

| Constant | Value | Factory Method | Rationale |
|----------|-------|----------------|-----------|
| `kLikelyTrue` | **0.8** | `likelyTrue()` | Conservative estimate that most predicates pass most rows |
| `kUnknown` | **0.5** | `unknown(nullFraction)` | Equal probability assumption when direction is unknown |
| `kLikelyZero` | **0.01** | `likelyZero(nullFraction)` | Near-zero estimate for likely contradictions based on statistics |
| `kNoRange` | **0.1** | `noRange(nullFraction)` | Conservative low estimate for types without range semantics |

Additionally, `Selectivity::zero(nullFraction)` returns true zero selectivity
for provably impossible conditions (e.g., comparisons with NULL literals).

### Examples

**80% — Unknown functions and operators** (`Selectivity::likelyTrue()`):
```sql
WHERE is_active                      -- boolean column without statistics
WHERE custom_udf(x)                  -- user-defined function as filter condition
WHERE x <> 5                         -- neq is not specially handled (use NOT(x = 5) for better estimates)
```

**50% — Unknown direction or values** (`Selectivity::unknown(nullFraction)`):
```sql
WHERE a < b                          -- column vs column, no min/max available
```

**10% — Types without meaningful range semantics** (`Selectivity::noRange(nullFraction)`):
```sql
WHERE array_column = ARRAY[1, 2, 3]  -- ARRAY type
WHERE json_data['key'] = 'value'     -- MAP type
WHERE blob_column = x'DEADBEEF'      -- VARBINARY type
WHERE COALESCE(x, 0) > 10            -- function expression with no min/max stats
```

**1% — Likely contradictions** (`Selectivity::likelyZero(nullFraction)`):
```sql
WHERE x > 10 AND x < 5              -- empty range based on statistics
WHERE x IN (1, 2, 3) AND x = 5      -- equality not in IN list
WHERE x = 100                       -- value outside estimated column range [0, 24]
```

These conditions appear contradictory based on column statistics, but since
statistics are estimates, a small non-zero selectivity is used instead of zero.

### Type-Specific Cardinality Limits

When adding constraints, cardinality is capped based on type:

| Type | Maximum Cardinality | Rationale |
|------|---------------------|-----------|
| BOOLEAN | 2 | Only TRUE and FALSE |
| TINYINT | 256 | 8-bit integer range |
| SMALLINT | 65,536 | 16-bit integer range |

## Constraint Propagation

In addition to estimating selectivity, the optimizer propagates constraints
derived from filter expressions to refine column statistics. This enables
downstream operations (e.g., joins, aggregations) to use tighter bounds and
produce better cost estimates.

Each constraint is a refined `Value` with updated min, max, cardinality, and
nullFraction. The nullFraction is always set to 0 because rows that pass a
comparison cannot be NULL (comparisons with NULL return NULL/unknown, which is
treated as FALSE in WHERE clauses).

The following constraint types are supported:

**Equality** (`x = 5`):
- min = max = literal value
- cardinality = 1

**Range** (`x > 5 AND x < 100`):
- min and max are tightened to the intersection of the existing constraint bounds
  and the filter bounds
- cardinality is scaled proportionally by the selectivity

**IN list** (`x IN (1, 2, 3)`):
- min and max are set from the smallest and largest values in the IN list
- cardinality = number of elements in the list

**Column equality** (`a = b`):
- Both columns receive the intersection range as their new constraint
- Cardinality is set to `min(matching values from a, matching values from b)`

## Contradictory Conditions

The optimizer detects contradictory conditions. The selectivity depends on
whether the contradiction is provable or based on estimates:

**Provably zero** (`Selectivity::zero`) — conditions that are guaranteed to match
no rows regardless of statistics:
- Any comparison with NULL literal (e.g., `x = NULL`, `x > NULL`)
- IN list containing NULL elements

**Likely zero** (`Selectivity::likelyZero`) — conditions that appear contradictory
based on column statistics, but statistics may be stale or approximate:
- `x = 5 AND x = 6` (conflicting equalities)
- `x > 10 AND x < 5` (empty range)
- `x IN (1, 2, 3) AND x = 5` (equality not in IN list)
- Equality value outside estimated column range
- Empty IN list after pruning to effective range

## Assumptions and Limitations

1. **Independence**: Conditions are assumed to be independent unless they share
   the same left-hand side column.

2. **Uniform Distribution**: Values are assumed to be uniformly distributed
   within their ranges.

3. **Aligned Values**: For column comparisons, distinct values in the overlap
   region are assumed to "align" (occur at the same positions).

4. **First Character for VARCHAR**: String range selectivity uses only the first
   character, which is a coarse approximation.

5. **No Correlation**: Cross-column correlations (e.g., city and country) are
   not modeled.

6. **Static Statistics**: Selectivity uses statistics from the original table
   scans; intermediate result statistics are estimated.

## Related Documentation

- See [CardinalityEstimation.md](CardinalityEstimation.md) for cardinality
  estimation of all operators, including how filter selectivity feeds into
  output cardinality.
- See [JoinEstimation.md](JoinEstimation.md) for join cardinality and
  constraint propagation, which uses filter selectivity for non-equi
  filter conditions.
- See [JoinEstimationQuickRef.md](JoinEstimationQuickRef.md) for a compact
  cheat-sheet of join cardinality and constraint propagation formulas.
