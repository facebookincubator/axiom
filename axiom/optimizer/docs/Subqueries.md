# Subquery Implementation in Axiom Optimizer

This document describes how subqueries are implemented in the Axiom optimizer,
including the data structures, processing flow, optimization techniques, and
current limitations.

## Overview

The Axiom optimizer converts logical plan subquery expressions into
`DerivedTable` nodes connected via specialized `JoinEdge` objects. The
processing happens during the query graph construction phase (`ToGraph`), with
support for three main subquery types:

- **Scalar subqueries**: `x = <subquery>`, `x < (SELECT ...)`, etc.
- **IN subqueries**: `x IN <subquery>`, `x NOT IN <subquery>`
- **EXISTS subqueries**: `EXISTS <subquery>`, `NOT EXISTS <subquery>`

Each type supports both **correlated** and **uncorrelated** variants.

> **Note:** Currently, subqueries are supported only in filter predicates (WHERE
> clause). Subqueries in projections (SELECT list) are not supported yet.

## Key Source Files

| File | Purpose |
|------|---------|
| `ToGraph.cpp` | Core subquery processing (`processSubqueries`, `translateSubquery`) |
| `ToGraph.h` | Interface definitions and correlation state fields |
| `QueryGraph.h` | `JoinEdge` representation including `makeExists` factory |
| `DerivedTable.h` | Query graph nodes, `singleRowDts` for non-correlated scalars |
| `tests/SubqueryTest.cpp` | Test suite demonstrating expected behavior |

## Data Structures

### Subqueries Struct

Subqueries found in a predicate are categorized during extraction:

```cpp
struct Subqueries {
  std::vector<lp::SubqueryExprPtr> scalars;    // x = <subquery>
  std::vector<lp::ExprPtr> inPredicates;       // x IN <subquery>
  std::vector<lp::ExprPtr> exists;             // EXISTS <subquery>

  bool empty() const {
    return scalars.empty() && inPredicates.empty() && exists.empty();
  }
};
```

### Correlation State (ToGraph.h, lines 428-440)

The `ToGraph` class maintains state for handling correlated subqueries:

```cpp
// Symbols from the 'outer' query. Used when processing correlated subqueries.
const folly::F14FastMap<std::string, ExprCP>* correlations_;

// True if expression is allowed to reference symbols from the 'outer' query.
bool allowCorrelations_{false};

// Filter conjuncts found in a subquery that reference symbols from the
// 'outer' query.
ExprVector correlatedConjuncts_;

// Maps an expression that contains a subquery to a column or constant that
// should be used instead. Populated in 'processSubqueries()'.
folly::F14FastMap<logical_plan::ExprPtr, ExprCP> subqueries_;
```

### Mark Columns

Semi-joins use boolean "mark" columns to track row membership:

```cpp
auto* mark = toName(fmt::format("__mark{}", markCounter_++));
auto* markColumn = make<Column>(mark, currentDt_, Value{toType(velox::BOOLEAN()), 2});
```

## Processing Flow

### Entry Point: `processSubqueries()`

```
Filter predicate received in addFilter()
            │
            ▼
extractSubqueries(predicate, subqueries)
    - Identifies scalar, IN, and EXISTS subqueries recursively
            │
            ▼
For each subquery type:
            │
    ┌───────┴────────┬─────────────────┐
    ▼                ▼                 ▼
  Scalars           IN              EXISTS
```

### Subquery Translation

`translateSubquery()` creates an isolated scope for the subquery:

```cpp
DerivedTableP ToGraph::translateSubquery(
    const logical_plan::LogicalPlanNode& node,
    bool finalize) {
  // Save outer query symbol map.
  auto originalRenames = std::move(renames_);
  renames_.clear();

  // Enable correlation - inner query can see outer symbols.
  correlations_ = &originalRenames;
  SCOPE_EXIT { correlations_ = nullptr; };

  // Create new DerivedTable for subquery.
  auto* outerDt = std::exchange(currentDt_, newDt());
  makeQueryGraph(node, kAllAllowedInDt);  // Recursive query graph build
  auto* subqueryDt = currentDt_;

  // Restore outer context.
  currentDt_ = outerDt;
  // ...
  return subqueryDt;
}
```

## Subquery Type Details

### Scalar Subqueries

**Uncorrelated scalar subqueries** can be:
1. **Constant folded** if the subquery is foldable (see Constant Folding below)
2. Converted to a **cross-join** with the outer query

**Correlated scalar subqueries** create a **LEFT join** with correlation
equality conditions:

```cpp
// Add LEFT join.
auto* join = make<JoinEdge>(
    leftTable, subqueryDt, JoinEdge::Spec{.rightOptional = true});
for (auto i = 0; i < leftKeys.size(); ++i) {
  join->addEquality(leftKeys[i], rightKeys[i]);
}
currentDt_->joins.push_back(join);
subqueries_.emplace(subquery, subqueryDt->columns.back());
```

### IN Subqueries

IN subqueries create **semi-joins** with a mark column:

```cpp
// x IN <subquery>
auto* edge = JoinEdge::makeExists(leftTable, subqueryDt, markColumn);
currentDt_->joins.push_back(edge);
edge->addEquality(leftKey, subqueryDt->columns.front());
subqueries_.emplace(expr, markColumn);
```

The resulting join types are:
- `kLeftSemiFilter` for `IN`
- `kAnti` for `NOT IN`

### EXISTS Subqueries

**Uncorrelated EXISTS** is transformed to check if any rows exist:

```sql
-- Original:
SELECT * FROM region WHERE EXISTS (SELECT 1 FROM nation)

-- Transformed to:
SELECT * FROM region
CROSS JOIN (
  SELECT NOT(count = 0) as exists_flag
  FROM (SELECT 1 FROM nation LIMIT 1) AGGREGATE count(*)
)
WHERE exists_flag
```

**Correlated EXISTS** creates a semi-join with mark column:

```cpp
auto* existsEdge = JoinEdge::makeExists(
    leftTable, subqueryDt, markColumn, std::move(filter));
currentDt_->joins.push_back(existsEdge);

for (auto i = 0; i < leftKeys.size(); ++i) {
  existsEdge->addEquality(leftKeys[i], rightKeys[i]);
}

subqueries_.emplace(expr, markColumn);
```

## JoinEdge Representation

`JoinEdge::makeExists` creates a specialized join edge:

```cpp
static JoinEdge* makeExists(
    PlanObjectCP leftTable,
    PlanObjectCP rightTable,
    ColumnCP markColumn = nullptr,
    ExprVector filter = {}) {
  return make<JoinEdge>(
      leftTable,
      rightTable,
      Spec{
          .filter = std::move(filter),
          .rightExists = true,
          .markColumn = markColumn,
      });
}
```

Join types used for subqueries:
- `kLeftSemiFilter` for EXISTS/IN (keeps matching rows)
- `kAnti` for NOT EXISTS/NOT IN (filters out matching rows)
- `kLeft` with `rightOptional = true` for correlated scalars

## Constant Folding Optimization

The optimizer can evaluate certain scalar subqueries at compile time using
`tryFoldConstantDt()`. This optimization relies on **discrete values** provided
by the connector to enumerate all possible values for certain columns.

### Conditions for Constant Folding

1. Single table with global aggregation (no GROUP BY)
2. Aggregate functions ignore duplicates (e.g., `max`, `min`) or use `DISTINCT`
3. Aggregation uses only columns with discrete predicates (the connector must
   provide the complete set of possible values for these columns)

### Example

```sql
-- Before:
SELECT * FROM t WHERE ds = (SELECT max(ds) FROM t)

-- After constant folding (if discrete values are known):
SELECT * FROM t WHERE ds = '2025-11-03'
```

### Implementation

```cpp
lp::ValuesNodePtr tryFoldConstantDt(DerivedTableP dt, velox::memory::MemoryPool* pool) {
  // Check preconditions...
  if (!dt->hasAggregation() || !dt->aggregation->groupingKeys().empty()) {
    return nullptr;
  }

  for (const auto* agg : aggPlan->aggregates()) {
    if (!agg->functions().contains(FunctionSet::kIgnoreDuplicatesAggregate) &&
        !agg->isDistinct()) {
      return nullptr;
    }
  }

  // Execute plan and return constant result.
  auto veloxPlan = queryCtx()->optimization()->toVeloxPlan(plan);
  auto results = runConstantPlan(veloxPlan, pool);
  return std::make_shared<lp::ValuesNode>(dt->cname, std::move(results));
}
```

## SQL Examples and Expected Plans

### Scalar Subquery

```sql
SELECT * FROM nation
WHERE n_regionkey = (SELECT r_regionkey FROM region WHERE r_name LIKE 'AF%')

-- Plan: nation → HashJoin(kInner) → region
```

### IN Subquery

```sql
SELECT * FROM nation
WHERE n_regionkey IN (SELECT r_regionkey FROM region WHERE r_name > 'ASIA')

-- Plan: nation → HashJoin(kLeftSemiFilter) → region
```

### NOT IN Subquery

```sql
SELECT * FROM nation
WHERE n_regionkey NOT IN (SELECT r_regionkey FROM region WHERE r_name > 'ASIA')

-- Plan: nation → HashJoin(kAnti) → region
```

### Correlated EXISTS

```sql
SELECT * FROM nation
WHERE EXISTS (SELECT 1 FROM region WHERE r_regionkey = n_regionkey)

-- Plan: nation → HashJoin(kLeftSemiFilter) → region
```

### Correlated EXISTS with Non-Equality

```sql
SELECT * FROM nation
WHERE EXISTS (SELECT 1 FROM region WHERE r_regionkey > n_regionkey)

-- Plan: nation → NestedLoopJoin(kLeftSemiProject) → region → Filter → Project
```

### Uncorrelated EXISTS

```sql
SELECT * FROM region WHERE EXISTS (SELECT 1 FROM nation)

-- Plan:
-- region → NestedLoopJoin(kInner) → (
--   nation → Limit(1) → Aggregate(count(*)) → Filter(NOT(count == 0)) → Project
-- )
```

## Limitations and TODOs

### Current Limitations

1. **Subqueries only supported in filters**
   - Subqueries can only appear in WHERE clause predicates
   - Subqueries in SELECT list (projections) are not supported yet

2. **Multi-table IN expressions not supported**
   ```sql
   -- Not supported:
   WHERE (a, b) IN (SELECT x, y FROM t)
   ```

3. **Correlated conjuncts referencing multiple outer tables**
   ```sql
   -- Not supported:
   WHERE EXISTS (SELECT 1 FROM t WHERE t.a = outer1.x AND t.b = outer2.y)
   ```
   Note: This limitation applies only when outer1 and outer2 are different
   base tables, not aliases of the same derived table.

4. **Multi-table expression in IN predicate left-hand side**
   ```cpp
   VELOX_CHECK_NOT_NULL(
       leftTable,
       "<expr> IN <subquery> with multi-table <expr> is not supported yet");
   ```

### Known TODOs (from source code)

1. Handle the case when scalar subquery returns no rows (line 2104):
   - Should fail if used in comparison (`x = <subquery>`)
   - Could constant fold if used in IN list

2. Support `SELECT COUNT(1) FROM (SELECT DISTINCT x FROM t)` for constant
   folding (line 274)

3. Support ORDER BY and LIMIT in constant foldable subqueries (lines 278-283)

## Test Coverage

The `SubqueryTest.cpp` file provides comprehensive test coverage:

| Test Case | Description |
|-----------|-------------|
| `scalar` | Uncorrelated `=`, `IN`, `NOT IN` with type coercion |
| `foldable` | Constant folding of `max`/`min` aggregates |
| `correlatedExists` | Equality conditions, non-equality conditions, multiple outer tables |
| `uncorrelatedExists` | `COUNT(*)` wrapper, `NOT EXISTS` |

## Architectural Notes

1. **DerivedTable Container**: Each subquery gets its own isolated `DerivedTable`
   node, enabling independent optimization.

2. **Join Edge Reuse**: Subqueries leverage the existing join planning
   infrastructure, enabling cost-based join ordering to consider subquery joins.

3. **Symbol Map Correlation**: Supports arbitrary nested correlation depths
   through the `correlations_` pointer chain.

4. **Mark Columns**: Simplify semi-join implementation by producing boolean
   results without materializing full subquery results.

5. **Early Constant Folding**: Reduces plan complexity and improves cardinality
   estimates by evaluating foldable subqueries at planning time.

## Related Documentation

- See [JoinPlanning.md](JoinPlanning.md) for details on how joins (including
  subquery joins) are planned and optimized.
