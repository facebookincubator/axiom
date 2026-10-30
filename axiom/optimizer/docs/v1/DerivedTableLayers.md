# DerivedTable Layers

> **Legacy.** This document describes the v1 optimizer, built on `DerivedTable`, which is being retired. For the current optimizer, see the [pass cheat sheet](../OptimizerPasses.md).

A DerivedTable (DT) represents a query block. Internally it has a layered
structure that mirrors SQL's logical order of operations. Each layer can
consume columns from lower layers and produce new columns.

## Layers (bottom-up)

| Layer | Fields | Produces columns? | Column `relation_` |
|-------|--------|-------------------|---------------------|
| 1. Base Tables | `tables`, `tableSet` | Yes | base table or sub-DT |
| 2. Joins | `joins` (JoinEdge) | Yes | this DT |
| 3. Filters | `conjuncts` | No | — |
| 4. Aggregation | `aggregation` | Yes (replaces) | this DT |
| 5. Having | `having` | No | — |
| 6. Window | `windowPlan` | Yes | this DT |
| 7. Projection | `exprs`, `columns` | Yes | this DT |
| 8. Order By | `orderKeys`, `orderTypes` | No | — |
| 9. Output | `outputColumns` | No | — |

## Column Ownership

Columns with `relation_ == thisDt` are produced by the Joins, Aggregation,
Window, and Projection layers:

- **Joins:** Columns stored on JoinEdge: `leftColumns`, `rightColumns`
  (nullable columns from outer joins), `markColumn` (semi-join existence
  flag), `rowNumberColumn` (unique ID for decorrelation).
- **Aggregation:** `aggregation->columns()` and
  `aggregation->intermediateColumns()`. Grouping key columns in
  `intermediateColumns()` share the same Column objects as `columns()`.
  Aggregation replaces the available column set — columns from lower
  layers are no longer directly accessible.
- **Window:** `windowPlan->columns()`.
- **Projection:** `columns` (the DT's output columns).

## Dependency Rules

Dependencies flow upward. Each layer's expressions may reference columns
produced by lower layers but never by higher layers. `checkConsistency`
validates these rules.

- **Filters:** `conjuncts` reference columns from Base Tables and Joins.
- **Aggregation:** `groupingKeys` and `aggregates` reference columns from
  Base Tables and Joins.
- **Having:** `having` references Aggregation output columns.
- **Window:** `partitionKeys`, `orderKeys`, and `args` reference columns
  available after Aggregation (or from Base Tables and Joins if there is
  no aggregation). Within a `WindowPlan`,
  dependent window functions (function B referencing function A's output)
  are ordered so that A appears before B.
- **Projection:** `exprs` may reference any available column.
- **Order By:** `orderKeys` may reference any available column.

## Flattening

When a DT is inlined into another (e.g., UNION ALL collapses to one child,
or a wrapper DT contains a single child with no additional operations),
the inner DT's fields are copied into the outer DT.
Columns with `relation_` pointing to the inner DT become dangling —
they reference an object that is neither `this` nor in `tableSet`.

`DerivedTableFlattener::reconstructColumns` fixes these references by
processing layers bottom-up:

1. For column-producing layers (Joins, Aggregation, Window): recreate
   columns with `relation_ == innerDt` so they reference the outer DT.
   Register the old-to-new mapping.
2. For expression-only layers (Filters, Having, Projection, Order By):
   rewrite expressions using the accumulated column mapping.

See `v1/DerivedTableFlattener.h` for the API.
