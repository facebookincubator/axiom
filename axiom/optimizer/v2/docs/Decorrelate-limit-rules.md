# Decorrelate Limit Rules

*Companion to:*
- *`Decorrelate-driver.md` — driver loop, terminus, NYI guards.*
- *`Decorrelate-filter-rules.md` — Filter peel.*
- *`Decorrelate-project-rules.md` — Project peel.*
- *`Decorrelate-agg-rules.md` — Aggregate peel.*

Peels a `Limit` from `body`. Behavior depends on `Apply.kind` and
`Limit.count` (offset must be 0; non-zero is rejected with a
`VELOX_USER_CHECK`).

## Cases

`count = 0` does not reach this rule: Translate rewrites `LIMIT 0` to
`Values(empty)` upstream (`translateLimit`). The peel asserts `count
>= 1`.

| `Apply.kind` | Rewrite |
|---|---|
| `kLeftSemiProject` EXISTS (`inLhs == nullptr`) | Drop `Limit`. `∃ row in first N` ↔ `∃ row` for `N >= 1`. Recurse with body = `Limit.input`. |
| `kLeft` | Per-outer LIMIT via window. See "kLeft + count >= 1" below. |
| `kLeftSemiProject` IN | **NYI.** Per-outer LIMIT must precede the IN equi check at terminus; needs `terminusSemi` restructuring. |

## kLeft + count >= 1

Shape:

```
- Project (alias inner includeMarker → outer includeMarker, drop rn / row_number)
  - Filter (row_number <= count)
    - Window (partition by rn, row_number AS __limit_rn)
      - decorrelate(
          Apply (kLeft, fresh includeMarker, accumulatedFilter)
            - body  = Limit.input
            - input = AssignUniqueId(apply.input, rn)
        )
```

Where:

- `rn` is a fresh BIGINT column minted on `apply.input` via
  `AssignUniqueId`. It serves as the window partition key, ensuring
  `row_number()` resets per outer row.
- The inner `Apply` is a normal correlated `kLeft` with a fresh
  `includeMarker`. The driver recurses on it; at terminus it becomes a
  LEFT JOIN with `_include` materialized on the right side as `true`.
- `row_number() OVER (PARTITION BY rn)` numbers body rows per outer
  starting at 1. With no `ORDER BY`, the row pick is non-deterministic —
  matching SQL `LIMIT` semantics without explicit ordering.
- `Filter (__limit_rn <= count)` keeps only the first `count` body rows
  per outer.
- The top `Project` reshapes to `apply.outputColumns`, sourcing the
  outer `includeMarker` from the inner one (per-outer LIMIT keeps the
  real-vs-pad signal: an outer with no body matches survives via the
  inner `kLeft` pad with `includeMarker = NULL`).

## Why kSemi+IN+count>=1 is NYI

For IN, `Apply.inBodyKey` becomes a `Join.rightKey` at
`terminusSemi`, and the IN equi check is computed at the Join
boundary. Per-outer LIMIT must apply to body rows **before** the equi
check (`IN matches if any of first N body rows = lhs`). The current
`terminusSemi` collapses IN equi into the Join in one shot — there is
no insertion point for a per-outer LIMIT between body emission and
equi evaluation. A clean rewrite would restructure the IN path to
expose body rows individually (e.g., split equi pair into existence
mark + post-mark predicate), then apply per-outer LIMIT via the same
window mechanism as kLeft.

## Termination contribution

`Limit` peels in one step. The recursive rewrite on the inner Apply
continues peeling Limit's child operators (Filter, Project, Aggregate,
…) and terminates at the inner Apply's terminus. Limit peel does not
re-enter the driver iteration loop above itself.

## What this rule does NOT do

- **Handle `OFFSET`.** Non-zero `Limit.offset` raises `VELOX_USER_CHECK`.
  Per-outer OFFSET via window's `row_number > offset AND row_number <=
  offset + count` is a small extension when needed.
- **Honor body's intrinsic ORDER BY.** Without an explicit `Sort`
  inside body, `row_number()` has no `ORDER BY` and the row pick is
  non-deterministic — matching SQL `LIMIT` semantics. When `Sort`
  becomes peelable, its keys can flow into the Window's `orderKeys`.
- **Optimize `count = 1` to `EnforceDistinct`.** Could fire a tighter
  rewrite for the common scalar `LIMIT 1` shape; for now the general
  Window+Filter handles it.
