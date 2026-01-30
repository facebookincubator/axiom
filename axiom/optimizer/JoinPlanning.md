# Join Planning: Control Flow and State Management

This document describes the control flow and state management in
`Optimization::makeJoins`, the core algorithm for join order enumeration.

## Overview

There are **two overloads** of `makeJoins`:

1. **`makeJoins(PlanState& state)`** - Entry point that initiates planning from
   starting tables
2. **`makeJoins(RelationOpPtr plan, PlanState& state)`** - Recursive function
   that builds the plan incrementally

## State Management via `PlanStateSaver`

The key to understanding state management is the **`PlanStateSaver`** class
(RAII guard):

```cpp
struct PlanStateSaver {
  explicit PlanStateSaver(PlanState& state)
      : state_(state),
        placed_(state.placed),      // Copies current placed set
        columns_(state.columns),    // Copies current columns set
        cost_(state.cost),          // Copies current cost
        numPlaced_(...) {}

  ~PlanStateSaver() {
    state_.placed = std::move(placed_);   // Restores on destruction
    state_.columns = std::move(columns_);
    state_.cost = cost_;
    ...
  }
};
```

This enables **backtracking**: the optimizer can try different plans, modify
state, and automatically restore it when the scope exits.

## Control Flow Diagram

```
makeJoins(state)                         [Entry Point]
    │
    ├─► Determine starting tables (syntactic order or all start tables)
    │
    └─► For each starting table:
            │
            ├─► PlanStateSaver save(state)    ◄── Save state
            ├─► state.placed.add(table)
            ├─► state.columns.unionObjects(columns)
            ├─► Create scan operation
            ├─► state.addCost(*scan)
            └─► makeJoins(scan, state)        ◄── Recurse
                    │                              (state restored on return)
                    ▼
        ┌─────────────────────────────────────────────────────┐
        │  makeJoins(plan, state)              [Recursive]    │
        │                                                     │
        │  1. Check cutoff: if cost > best, return early      │
        │                                                     │
        │  2. Place conjuncts (filters) if possible           │
        │     └─► If placed, return (recursion continues      │
        │         inside placeConjuncts)                      │
        │                                                     │
        │  3. Get next join candidates                        │
        │     └─► nextJoins(state) returns joinable tables    │
        │                                                     │
        │  4. If no candidates (all tables placed):           │
        │     ├─► Place remaining conjuncts                   │
        │     ├─► Place single-row derived tables             │
        │     ├─► Add postprocessing (GROUP BY, ORDER BY...)  │
        │     ├─► state.plans.addPlan(plan, state)  ◄── SAVE  │
        │     └─► return                                      │
        │                                                     │
        │  5. For each candidate, generate NextJoin options:  │
        │     └─► addJoin(candidate, plan, state, nextJoins)  │
        │         ├─► Try index join                          │
        │         ├─► Try hash join                           │
        │         └─► Each creates NextJoin with snapshot of: │
        │             - placed, columns, cost, plan           │
        │                                                     │
        │  6. tryNextJoins(state, nextJoins):                 │
        │     └─► For each NextJoin:                          │
        │         ├─► PlanStateSaver save(state)              │
        │         ├─► state.placed = next.placed              │
        │         ├─► state.columns = next.columns            │
        │         ├─► state.cost = next.cost                  │
        │         └─► makeJoins(next.plan, state) ◄── RECURSE │
        └─────────────────────────────────────────────────────┘
```

## Key State Fields in `PlanState`

| Field | Purpose |
|-------|---------|
| `placed` | Set of objects (tables, conjuncts, etc.) already in the plan |
| `columns` | Set of columns available from placed tables |
| `cost` | Accumulated cost of the partial plan |
| `plans` | Collection of completed plans (output) |
| `dt` | The derived table being planned |
| `targetExprs` | Expressions needed in final output |

## State Transitions

### Placing a table scan

```cpp
state.placed.add(table);
state.columns.unionObjects(columns);
state.addCost(*scan);
```

### Placing a join (via `NextJoin`)

`NextJoin` captures a **snapshot** of `placed`, `columns`, `cost` after the
join. `tryNextJoins` restores state from the snapshot before recursing.

### Placing a filter

```cpp
state.placed.add(filter);
state.addCost(*filter);
```

## Backtracking Pattern

The optimizer explores multiple paths (different join orders, methods) using
this pattern:

```cpp
for (auto& next : nextJoins) {
    PlanStateSaver save(state);      // Save current state
    state.placed = next.placed;       // Apply this branch's state
    state.columns = next.columns;
    state.cost = next.cost;
    makeJoins(next.plan, state);      // Explore this path
}                                     // ~PlanStateSaver restores state
```

This allows the optimizer to try all combinations and keep the best plans in
`state.plans`.

## Summary

- **`PlanStateSaver`** provides automatic state save/restore (RAII)
- **`NextJoin`** captures state snapshots for deferred exploration
- **Recursion** explores the search space depth-first
- **Cutoff** (`state.isOverBest()`) prunes unpromising branches
- **`state.plans`** accumulates interesting completed plans
