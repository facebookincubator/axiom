# Memoization of Partial Plans

> **Legacy.** This document describes the v1 optimizer, built on `DerivedTable`, which is being retired. For the current optimizer, see the [pass cheat sheet](../OptimizerPasses.md).

This document describes the memoization mechanism used during plan enumeration
to avoid redundant work.

## Overview

The optimizer explores different join orderings to find the lowest-cost physical
plan. During this exploration, the same subset of tables with the same output
requirements may appear in multiple branches of the search. The `Memo` cache
stores previously computed plans so they can be reused.

## MemoKey

A `MemoKey` identifies a planning subproblem. It has four fields:

| Field | Type | Description |
|-------|------|-------------|
| `firstTable` | `PlanObjectCP` | The primary table (anchor) of the subplan. Must be a member of `tables`. |
| `columns` | `PlanObjectSet` | The set of output columns required from this subplan. |
| `tables` | `PlanObjectSet` | All tables that must be joined together in this subplan. |
| `existences` | `vector<PlanObjectSet>` | Optional existence (semijoin) constraints — see below. |

The key captures the **problem specification**: "produce these columns by
combining these tables." It does not describe *how* to combine them — that is
what the cached plans represent.

### Hash and Equality

`MemoKey::hash()` is computed from `tables` and `existences` only
(order-independent). It deliberately excludes `firstTable` and `columns` for
efficiency, accepting more hash collisions in exchange for a cheaper hash.
`operator==` compares all four fields to resolve collisions.

## Memo

The `Memo` class wraps `folly::F14FastMap<MemoKey, PlanSet>`. Each entry maps a
`MemoKey` to a `PlanSet` — a collection of physical plans (operator trees) that
satisfy the key's requirements. Different plans in a `PlanSet` may use different
join orders, join methods (hash vs. index), or data distributions.

## How It Works

The main consumer is `Optimization::makeDtPlan(dt, key, ...)`:

```
makeDtPlan(dt, key, distribution, existsFanout, needsShuffle)
    │
    ├─► memo_.find(key)
    │     │
    │     ├─► Cache hit → skip to step 3
    │     │
    │     └─► Cache miss:
    │           1. Create a temporary DerivedTable
    │           2. Import tables, joins, and existences from 'dt' into it
    │           3. Call makeJoins() to enumerate join orderings
    │           4. Store resulting plans: memo_.insert(key, plans)
    │
    └─► Return plans->best(distribution, needsShuffle)
```

The `dt` parameter is the outer (super) DerivedTable that owns the tables and
joins. On a cache miss, `tmpDt->import(dt, key.tables, ...)` copies the relevant
subset of tables and joins from `dt` into a temporary DerivedTable, then runs
join enumeration (`makeJoins`) to produce alternative physical plans. These
plans are full operator trees containing scans, joins, projections,
aggregations, etc. The result is stored in the memo for reuse.

On a cache hit, the previously computed plans are returned directly.

`best()` selects the lowest-cost plan from the set, optionally matching a
desired data distribution.

## Existences in the MemoKey

Existences are **not** part of the logical query specification — they are an
optimization strategy. An existence represents a decision to push a semijoin
from the outer query *into* a subplan to filter rows early (see
[Existence Pushdown](ExistencePushdown.md)).

For example, given:

```sql
SELECT t.a, dt.cnt
FROM t
JOIN (SELECT x, COUNT(*) AS cnt FROM u GROUP BY x) dt ON t.a = dt.x
```

The optimizer may decide to push a semijoin on `t` inside `dt`'s subplan to
reduce rows before the GROUP BY. This decision is made *before* the MemoKey is
constructed (by `findReducingJoins` in `placeDerivedTable`). The existences
field records this decision so the memo distinguishes plans built with different
optimization strategies:

- `MemoKey{tables={dt}, existences=[]}` → plan `dt` without early filtering.
- `MemoKey{tables={dt}, existences=[{t}]}` → plan `dt` with a semijoin on `t`.

These are different subproblems because the resulting operator trees are
structurally different.

### Cross-boundary Nature of Existences

Existences involve tables that are **outside** the current subproblem's scope.
When `makeJoins` runs on a subplan with tables `{dt}`, it only sees joins
*within* that DerivedTable. Table `t` belongs to the outer DerivedTable and
would never be considered during normal join enumeration.

The existence mechanism bridges this boundary: `addExistences` imports the
cross-boundary join into the child DerivedTable as a semijoin edge. After
import, `makeJoins` sees the semijoin and incorporates it into join ordering
like any other join.

## Call Sites

`MemoKey::create` is called in several places in `v1/Optimization.cpp`:

| Context | firstTable | tables | existences |
|---------|-----------|--------|------------|
| Hash join build side | Build anchor | Build tables | From candidate |
| Hash join probe side | Probe anchor | Probe tables | From candidate |
| Single-table build | Table | {table} | From candidate |
| Single-row DT | Subquery DT | {subquery} | Empty |
| `placeDerivedTable` (initial) | DT | {DT} | Empty |
| `placeDerivedTable` (reducing) | DT | Bushy tables or {DT} | From `findReducingJoins` |
| UNION ALL child | Child DT | {child} | Empty |
