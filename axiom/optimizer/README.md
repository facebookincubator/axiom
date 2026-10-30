# Optimizer

The optimizer turns a logical plan (`logical_plan::LogicalPlanNode`) into a
`MultiFragmentPlan` of Velox plan nodes. It translates the logical plan into
the Node IR, runs a sequence of passes over that IR, chooses join order and
data distribution, and emits executable fragments. The
[pass cheat sheet](docs/OptimizerPasses.md) maps each optimization to the pass
that owns it.

The optimizer's code is in this directory. The previous optimizer, built on
`DerivedTable`, is legacy; its code is under `v1/` and its design is
documented under [docs/v1](docs/v1/Overview.md).

## Documentation

Marks: [proposal] describes a design that is not built; [needs v2 update] still describes parts of the legacy v1 optimizer.

Passes and IR:
- [Optimizer Passes](docs/OptimizerPasses.md) - Pipeline, pass order, and what each pass owns
- [Expression Evaluation](docs/ExpressionEvaluation.md) - Where the optimizer may evaluate an expression, and what moving one preserves
- [Decorrelate](docs/Decorrelate.md) - Removing correlated subqueries; links the rule-specific documents
- [Fixed Point](docs/FixedPoint.md) - Loops: recursive queries and repeated graph paths, from the logical plan to Velox; why patterns and loops are separate nodes
- [Graph Queries](docs/GraphTable.md) - GRAPH_TABLE pattern matching: the logical plan node and how optimizer v2 plans it
  - [Fixed-length patterns](docs/GraphTable-lowering.md) - Planned as joins
  - [Repeated patterns](docs/GraphTable-repetition.md) - Planned as loops
- [proposal] [Maps and Arrays as Tables](docs/MapsAndArraysAsTables.md) - Treating map and array columns as relations
- [Grouping Sets](docs/GroupingSets.md) - Grouping sets, GroupId, and GROUPING()

Estimation:
- [needs v2 update] [Cardinality Estimation](docs/CardinalityEstimation.md) - How output cardinality is estimated for each operator
- [needs v2 update] [Filter Selectivity](docs/FilterSelectivity.md) - How filter selectivity is estimated
- [needs v2 update] [Join Estimation](docs/JoinEstimation.md) and its [quick reference](docs/JoinEstimationQuickRef.md) - Join cardinality estimation
- [Payload NDV Scaling](docs/PayloadNdvScaling.md) - Estimating how many distinct values survive a row filter
- [needs v2 update] [Filtered Table Stats](docs/FilteredTableStats.md) - Connector-level statistics after filters

Join enumeration and distributed execution:
- [Join Enumeration Scaling](docs/JoinEnumerationScaling.md) - How join enumeration scales on same-key join chains, and its greedy fallback
- [needs v2 update] [Distributed Execution](docs/DistributedExecution.md) - How plans are split into fragments, scheduled, and wired
  - [needs v2 update] [UNION ALL Planning](docs/UnionAllPlanning.md) - Fragment placement for UNION ALL legs
  - [proposal] [Distributed Dynamic Filters](docs/DistributedDynamicFilters.md) - Dynamic filters across fragments

Testing and debugging:
- [Testing](docs/Testing.md) - PlanMatcher for plan shape, SqlTest for correctness, test coverage guidelines
- [needs v2 update] [Debugging Tips](docs/DebuggingTips.md) - Using the CLI, generating TPC-H data, speeding up test runs, adding debug logging
- [Visualization CLI](docs/QueryGraphviz.md) - Diagrams of logical and distributed plans from SQL
- [TPC-H Plan Comparison](docs/TpchV1V2PlanComparison.md) - How v2's TPC-H plans compare with v1's best plans, and how to measure a plan change

## Terminology

Fanout
: Join fanout or one-to-many join refers to a situation where one row in a table joins to multiple rows in another table. This means the number of rows in the joined result set can be greater than the number of rows in the primary (left) table.

Outer join reduction
: Replacing an outer join with one that preserves fewer inputs.

The possible reductions are:

- Full join to left or right join.
- Left or right join to inner join.

A reduction applies when a predicate above the join rejects its NULL-padded
rows. For example, this query:

```sql
SELECT *
FROM t
LEFT JOIN u ON t.k = u.k
WHERE u.v > 0
```

can run as:

```sql
SELECT *
FROM t
INNER JOIN u ON t.k = u.k
WHERE u.v > 0
```

The filter rejects every row whose `u` side is NULL-padded, so changing the
join type does not change the result.

Theta join
: A join whose condition relates the two inputs with something other than key equality, e.g. `t.a < u.b`. With no equi-keys to hash on, it runs as a nested loop join. In a distributed plan, one input is broadcast; a full join gathers both inputs onto one task.

Conjunction
: A statement formed by adding two statements with the connector AND. Individual statements are called conjuncts.

Disjunction
: A statement formed by adding two statements with the connector OR. Individual statements are called disjuncts.

Negation
: A statement formed by adding a NOT to a statement.

> [!NOTE]
> De Morgan's Laws:
> * Negation of a conjunction: The negation of an AND statement is equivalent to the OR of the individual negations.
>   * not(a AND b) <==> not(a) OR not(b)
> * Negation of a disjunction: The negation of an OR statement is equivalent to the AND of the individual negations.
>   * not(a OR b) <==> not(a) AND not(b)

## Abbreviations

XxxP
: Raw pointer to Xxx: XxxP := Xxx*

XxxCP
: Raw pointer to constant Xxx: XxxCP := const Xxx*. Remember to place 'const' *after* the type: XxxCP const variableName. (const XxxCP doesn't produce the desired effect.)

XxxVector
: Standard vector of raw pointers to Xxx allocated from the arena: XxxVector := `QGVector<XxxP>`.

CPSpan<T>
: A view on an array of const raw pointers `CPSpan<T> := std::span<const T* const>`;

LRFanout
: Left-to-Right Fanout. The average number of right side rows selected for one row on the left.

RLFanout
: Right-to-Left Fanout. The average number of left side rows selected for one row on the right.

## Memory Management

Node IR nodes and expressions are allocated from the arena of the optimization's `QueryGraphContext` and have the lifetime of the optimization. They are dropped wholesale at the end without invoking destructors. The candidate plans DPhyp builds during join enumeration (`MemoOp`) are allocated with malloc and owned by the memo through `std::unique_ptr`.

QGAllocator
: STL compatible allocator that manages std:: containers allocated in the QueryGraphContext arena. Use `make<T>(<args>)` to allocate a new object of type T from the arena.

Name
: Pointer to an arena allocated interned copy of a null terminated string. Used for identifiers. Allows comparing strings by comparing pointers. `Name := const char*`. Use `toName(<string>)` to convert an external string.
