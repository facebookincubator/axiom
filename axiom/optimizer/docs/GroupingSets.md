# Grouping Sets, GroupId, and GROUPING()

This document describes how Axiom handles queries with multiple grouping
sets — GROUP BY ROLLUP, CUBE, GROUPING SETS — and the companion GROUPING()
function that disambiguates rolled-up NULLs from real NULLs.

## Grouping Set Expansion

SQL provides three shorthands for requesting multiple aggregation levels in
a single query. The parser expands each into an explicit list of grouping
sets before any planning happens:

```sql
-- ROLLUP drops columns right to left (hierarchical subtotals)
GROUP BY ROLLUP(a, b)
-- expands to: GROUPING SETS ((a, b), (a), ())

-- CUBE generates all 2^n combinations
GROUP BY CUBE(a, b)
-- expands to: GROUPING SETS ((a, b), (a), (b), ())

-- GROUPING SETS passes through as-is
GROUP BY GROUPING SETS ((a), (b))
-- stays: GROUPING SETS ((a), (b))
```

Mixed forms are also supported. `GROUP BY a, ROLLUP(b)` computes the
Cartesian product of the plain key and the rollup expansion, producing
`GROUPING SETS ((a, b), (a))`.

Duplicate grouping sets are preserved per the SQL standard — each set
produces its own row group. `GROUP BY DISTINCT` removes duplicates after
expansion.

The expansion happens in `GroupByPlanner::expandGroupingSets()`, which
delegates to `expandRollup()`, `expandCube()`, and
`crossProductGroupingSets()`.

## GroupId Operator

After expansion, the optimizer needs to produce all subtotals in a single
pass. The naive approach — run a separate GROUP BY for each set and UNION
the results — scans the input once per set. For CUBE with N columns that
means 2^N scans, which doesn't scale.

Instead, Axiom inserts a GroupId operator that duplicates each input row
once per grouping set, NULLing out columns not in the active set and
tagging each copy with a sequential set ID. A single aggregation then
groups by all keys plus the set ID, producing detail rows, subtotals, and
grand totals in one pass.

Consider `SELECT a, sum(b) FROM t GROUP BY ROLLUP(a)` with two rows:

```
Input:     a=1, b=10
           a=2, b=20

GroupId output (2 rows × 2 sets = 4 rows):
           a=1,    b=10, set_id=0    ← set (a): a kept
           a=NULL, b=10, set_id=1    ← set (): a NULLed
           a=2,    b=20, set_id=0
           a=NULL, b=20, set_id=1

Aggregation (GROUP BY a, set_id):
           a=1,    sum=10, set_id=0  ← detail
           a=2,    sum=20, set_id=0  ← detail
           a=NULL, sum=30, set_id=1  ← grand total
```

### Where GroupId Lives

GroupId is a physical-only operator. The logical plan carries grouping sets
on AggregateNode — no GroupIdNode in the logical plan. This keeps optimizer
rules simple: they see AggregateNode with a `groupingSets` field and don't
need to reason about row duplication.

GroupId is introduced during optimization by
`AggregationPlanner::addGroupingSetsAggregation()`. For a plain GROUP BY
(no ROLLUP/CUBE/GROUPING SETS), none of this code runs — a regular
aggregation is planned as before.

## GROUPING() Function

GROUPING() tells you whether a NULL in the result came from the data or
from rolling up a grouping set. Without it, a grand total row
(`a=NULL, sum=30`) is indistinguishable from a row where `a` is genuinely
NULL.

```sql
SELECT a, GROUPING(a), sum(b) FROM t GROUP BY ROLLUP(a)

a      grp  sum
1      0    10     ← a present (0)
2      0    20     ← a present (0)
NULL   1    30     ← a rolled up (1)
```

For multiple columns, GROUPING(a, b) returns a bitmask with the leftmost
argument as the most significant bit.

### How It Works

ExpressionPlanner emits `Call("grouping", [Col("a"), Col("b")])` — a
regular function call used as a marker. GroupByPlanner finds these by name
after grouping set deduplication and rewrites inline to:

```
element_at(
    array[bitmask_for_set_0, bitmask_for_set_1, ...],
    $grouping_set_id + 1
)
```

The bitmask for each set is precomputed at plan time. The result is built
entirely from existing `lp::` primitives (Lit, Col, Call). No new Velox
types or APIs needed.

By the time the plan reaches the optimizer, GROUPING() has been replaced
with standard function calls. The optimizer and Velox have no awareness
of GROUPING().

### Dialect Independence

Only the SQL parser layer is dialect-specific:

- **ExpressionPlanner** (in `axiom/sql/presto/`) creates
  `Call("grouping", args)` from the Presto GROUPING() AST node.
- **GroupByPlanner** (in `axiom/sql/presto/`) resolves the marker and
  builds the element_at expression using `"$grouping_set_id"` as the
  GroupId column name.

Adding GROUPING() support for a new dialect requires only parser-layer
changes. The optimizer and Velox require zero modifications.

### Validation

ExpressionPlanner validates GROUPING() via the `ExprOptions.allowGrouping`
flag, which defaults to `false`. Only the GROUP BY path (SELECT, HAVING,
ORDER BY) sets it to `true`. This means GROUPING() is automatically
rejected in all other contexts (WHERE, JOIN ON, standalone queries without
GROUP BY) without needing per-context checks.

GroupByPlanner validates GROUPING() usage at plan time:

- GROUPING() with plain GROUP BY (no ROLLUP/CUBE/GROUPING SETS) returns
  0. Every column is always present in the single grouping set.
- GROUPING() without any GROUP BY clause is rejected (allowGrouping is
  false by default).
- GROUPING() referencing a column not in the grouping keys is rejected.
- GROUPING() arguments must be column references (not expressions).
- GROUPING() inside aggregate function arguments, FILTER, or ORDER BY
  is rejected.

## Unsupported Combinations

> **Not yet implemented.** The following query shapes are rejected at plan
> time with user-facing errors.

- `count(DISTINCT x)` with ROLLUP/CUBE/GROUPING SETS — requires
  integrating `transformDistinctToGroupBy` with GroupId.
- `array_agg(x ORDER BY x)` with a global (empty) grouping set
  ([velox#17312](https://github.com/facebookincubator/velox/issues/17312)).
- HAVING filter pushdown between GroupId and Aggregation is disabled when
  grouping sets are present. Queries pass but the aggregation is
  unoptimized.
