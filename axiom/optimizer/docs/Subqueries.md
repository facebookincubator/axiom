# Subquery Implementation in Axiom Optimizer

This document describes how subqueries are implemented in the Axiom optimizer,
including the data structures, processing flow, optimization techniques, and
current limitations.

## Introduction to Subqueries

A **subquery** (also called a nested query or inner query) is a SQL query
embedded within another query. Subqueries appear inside parentheses and can be
used in various parts of a SQL statement, including the SELECT list, WHERE
clause, and FROM clause.

### Types of Subqueries

There are three main types of subqueries based on what they return:

**Scalar Subqueries** return a single value (one row, one column). They can be
used anywhere a single value is expected:

```sql
SELECT * FROM orders
WHERE order_total > (SELECT AVG(order_total) FROM orders)
```

**IN Subqueries** return a set of values and are used with the IN or NOT IN
operators to test set membership:

```sql
SELECT * FROM customers
WHERE customer_id IN (SELECT customer_id FROM orders WHERE year = 2024)
```

**EXISTS Subqueries** test whether a subquery returns any rows at all. They
return true if the subquery produces at least one row:

```sql
SELECT * FROM customers c
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id)
```

### Correlated vs. Uncorrelated Subqueries

The most important distinction for query optimization is whether a subquery is
**correlated** or **uncorrelated**.

#### Uncorrelated Subqueries

An **uncorrelated subquery** is independent of the outer query. It can be
executed once, and its result reused for every row of the outer query:

```sql
-- Uncorrelated: the subquery references only the 'daily_rates' table,
-- not the 'orders' table from the outer query
SELECT * FROM orders
WHERE order_total > (SELECT min_order_amount FROM daily_rates WHERE rate_date = '2024-01-15')
```

In this example, the subquery computes a single value from `daily_rates` that
doesn't depend on which row of `orders` is being evaluated. The subquery can
be executed once and its result reused for all rows of the outer query.

#### Correlated Subqueries

A **correlated subquery** references columns from the outer query. It must be
conceptually re-evaluated for each row of the outer query because the result
depends on values from that row:

```sql
-- Correlated: the subquery references 'c.customer_id' from the outer query
SELECT * FROM customers c
WHERE EXISTS (
    SELECT 1 FROM orders o
    WHERE o.customer_id = c.customer_id
)
```

Here, the subquery checks if orders exist *for each specific customer*. The
reference to `c.customer_id` creates a **correlation** between the inner and
outer queries.

#### Why the Distinction Matters

The difference has significant performance implications:

- **Uncorrelated subqueries** can often be executed once and their results
  cached or constant-folded, making them efficient.

- **Correlated subqueries** naively require re-execution for each outer row,
  which can be extremely slow. However, optimizers use a technique called
  **decorrelation** to transform correlated subqueries into joins, which are
  much more efficient.

For example, the correlated EXISTS query above can be transformed into a
semi-join:

```sql
SELECT c.* FROM customers c
LEFT SEMI JOIN orders o ON o.customer_id = c.customer_id
```

This join-based execution is typically orders of magnitude faster than
repeatedly executing the subquery.

### Basic Decorrelation via Joins

The most common and efficient decorrelation technique converts correlated
subqueries into joins. This works when the correlation condition can be
"pulled up" into a join predicate.

#### Equality Correlations

When the correlation is an equality condition, it becomes a join key:

```sql
-- Original: correlated EXISTS
SELECT * FROM customers c
WHERE EXISTS (
    SELECT 1 FROM orders o
    WHERE o.customer_id = c.customer_id
)

-- Decorrelated: semi-join on equality
SELECT c.*
FROM customers c
LEFT SEMI JOIN orders o ON o.customer_id = c.customer_id
```

This transformation works for:
- **EXISTS** → Semi-join
- **NOT EXISTS** → Anti-join
- **IN** → Semi-join (null-aware)
- **NOT IN** → Anti-join (null-aware)
- **Scalar subqueries** → Left join

#### Multiple Equality Correlations

Multiple equality conditions become multiple join keys:

```sql
-- Original: correlation on two columns
SELECT * FROM customers c
WHERE EXISTS (
    SELECT 1 FROM orders o
    WHERE o.customer_id = c.customer_id
      AND o.region = c.region
)

-- Decorrelated: semi-join with composite key
SELECT c.*
FROM customers c
LEFT SEMI JOIN orders o
    ON o.customer_id = c.customer_id
    AND o.region = c.region
```

#### Correlated Scalar Subqueries with Aggregation

When a correlated scalar subquery contains aggregation, the correlation key is
added as a grouping key:

```sql
-- Original: correlated scalar with aggregation
SELECT c.*, (
    SELECT SUM(o.order_total)
    FROM orders o
    WHERE o.customer_id = c.customer_id
) AS total_orders
FROM customers c

-- Decorrelated: left join with grouped subquery
SELECT c.*, subq.total_orders
FROM customers c
LEFT JOIN (
    SELECT customer_id, SUM(order_total) AS total_orders
    FROM orders
    GROUP BY customer_id
) subq ON subq.customer_id = c.customer_id
```

The correlation column (`customer_id`) becomes a grouping key, allowing the
subquery to be executed once for all groups rather than once per outer row.

### Supported Patterns for Basic Decorrelation

Basic join-based decorrelation works when the correlated subquery can be
decomposed into:

1. An **uncorrelated subquery** (the inner query without correlation)
2. A **correlated filter** on top, consisting of one or more conjuncts
3. Each conjunct is an **equality condition** of the form `f(outer) = g(inner)`,
   where `f(outer)` references only outer columns and `g(inner)` references
   only inner columns

For example:

```sql
-- Supported: correlation filter is a conjunction of equalities
SELECT * FROM customers c
WHERE EXISTS (
    SELECT 1 FROM orders o
    WHERE o.customer_id = c.customer_id    -- f(outer) = g(inner)
      AND o.region = c.region              -- f(outer) = g(inner)
)
```

The expressions `f(outer)` and `g(inner)` can be arbitrarily complex, as long
as they reference only one side:

```sql
-- Supported: complex expressions on each side
SELECT * FROM t
WHERE EXISTS (
    SELECT 1 FROM u
    WHERE u.x + u.y = t.a * t.b            -- g(inner) = f(outer)
)
```

### Limitations of Basic Decorrelation

Basic decorrelation fails when the correlation doesn't fit the
`f(outer) = g(inner)` pattern, or when the correlation cannot be pulled up
into a filter (e.g., it appears inside an aggregation or below a LIMIT):

1. **Non-equality correlation**: Range predicates or other comparisons cannot
   be used as join keys directly.

   ```sql
   -- Not supported by basic decorrelation: inequality
   SELECT * FROM customers c
   WHERE EXISTS (
       SELECT 1 FROM orders o
       WHERE o.order_total > c.credit_limit
   )
   ```

2. **Mixed references in expressions**: When outer and inner references appear
   in the same expression, it cannot be split into separate join keys.

   ```sql
   -- Not supported: outer and inner mixed in same expression
   SELECT * FROM t
   WHERE (SELECT max(y) FROM u WHERE u.x[t.a] = 1) IS NULL
   ```

   Here, `u.x[t.a]` contains both inner (`u.x`) and outer (`t.a`) references,
   so it cannot be decomposed into `f(outer) = g(inner)`.

3. **Correlation inside aggregation**: When the correlation appears inside an
   aggregate function rather than in the WHERE clause, it cannot be converted
   to a join key.

   ```sql
   -- Not supported: correlation inside COUNT
   SELECT * FROM customers c
   WHERE (SELECT COUNT(o.order_id + c.customer_id) FROM orders o) > 10
   ```

4. **Correlation with LIMIT**: When a correlated subquery uses LIMIT to
   restrict results per outer row, decorrelation is not straightforward because
   the limit applies independently for each correlated group.

   ```sql
   -- Not supported: LIMIT 5 applies per customer
   SELECT c.*, (
       SELECT SUM(order_total) FROM (
           SELECT order_total FROM orders o
           WHERE o.customer_id = c.customer_id
           ORDER BY o.order_date DESC
           LIMIT 5
       )
   ) AS recent_orders_total
   FROM customers c
   ```

### Alternative Decorrelation Techniques

When basic join-based decorrelation doesn't apply, alternative techniques
can be used.

#### Window Functions for Range Predicates

When the correlation involves a range comparison (`<`, `<=`, `>`, `>=`), window
functions can provide efficient decorrelation. The key insight is that for a
predicate like `t.a < u.x`, the subquery result for a given `t.a` value depends
only on `u` rows where `u.x > t.a`. By sorting all keys and computing a running
aggregate, we can evaluate the subquery for all outer rows in a single pass.

```sql
-- Original: correlated subquery with range predicate
SELECT * FROM t
WHERE (SELECT max(y) FROM u WHERE t.a < u.x) IS NULL
```

The decorrelation works as follows:

1. **Combine keys**: Union distinct `t.a` values with all `u.x` values, tagging
   each row with an `is_outer` flag
2. **Sort**: Order by key (descending for `<`), with secondary sort on
   `is_outer` to control boundary inclusion
3. **Running aggregation**: Compute cumulative `max(y)` over the sorted rows,
   considering only inner (`u`) rows in the aggregation
4. **Extract results**: For each outer (`t`) row, the running aggregate at that
   point is the subquery result
5. **Join back**: Equi-join with original `t` to get final results

```sql
-- Decorrelated using window function
WITH combined AS (
    SELECT DISTINCT a AS key, TRUE AS is_outer, NULL::BIGINT AS y FROM t
    UNION ALL
    SELECT x AS key, FALSE AS is_outer, y FROM u
),
with_running_agg AS (
    SELECT key, is_outer,
        max(CASE WHEN NOT is_outer THEN y END)
            OVER (ORDER BY key DESC, is_outer DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_y
    FROM combined
),
agg_per_key AS (
    SELECT key, max_y FROM with_running_agg WHERE is_outer
)
SELECT t.* FROM t
JOIN agg_per_key ON t.a = agg_per_key.key
WHERE agg_per_key.max_y IS NULL
```

The sort order and boundary handling depends on the comparison operator:

| Condition | Primary Sort | Secondary Sort | Boundary |
|-----------|--------------|----------------|----------|
| `t.a < u.x` | `key DESC` | `is_outer DESC` | Excludes boundary |
| `t.a <= u.x` | `key DESC` | `is_outer ASC` | Includes boundary |
| `t.a > u.x` | `key ASC` | `is_outer DESC` | Excludes boundary |
| `t.a >= u.x` | `key ASC` | `is_outer ASC` | Includes boundary |

**Note**: This approach requires a global sort across all combined keys, which
limits its applicability in distributed execution. However, it efficiently
handles range predicates that basic join-based decorrelation cannot support.

#### Assign-Unique-ID + Cross Join + Streaming Aggregation

For correlated subqueries that cannot be transformed into simple joins or
window functions, a more general technique uses three steps:

1. **Assign unique IDs** to outer query rows
2. **Left join** (with non-equality condition) outer rows with the subquery's
   base table
3. **Aggregate** using the unique ID as a grouping key

```sql
-- Original: correlated subquery with complex correlation
SELECT * FROM t
WHERE (SELECT count(*) FROM u WHERE u.x[t.a] = 1) > 10
```

This is rewritten into the following plan:

```
Filter: cnt > 10
  └─ StreamingAgg(groupBy=[t_uniq]): count(u_marker) AS cnt, arbitrary(t.*) AS t
       └─ LeftJoin on u.x[t.a] = 1
            ├─ AssignUniqueId → t_uniq
            │    └─ Scan(t)
            └─ Project: *, TRUE AS u_marker
                 └─ Scan(u)
```

The `AssignUniqueId` operator assigns unique identifiers locally on each node
(using node ID + sequence number), avoiding the need to gather all outer rows
to a single node. The left join with a non-equality condition is effectively a
nested-loop join that evaluates the predicate for each `(t, u)` pair. The
streaming aggregation groups results by the unique ID, effectively
reconstructing the per-row subquery semantics in a set-based manner.

**Note**: The `arbitrary(t.*)` preserves all columns from `t` through the aggregation.
Since all rows with the same `t_uniq` have identical `t` column values, `arbitrary`
safely picks any one of them.

**Note**: The `u_marker` column ensures correct results for aggregations that don't
ignore NULLs (like `count`). When the left join finds no matches, it emits a row
with NULL for `u_marker`, and `count(u_marker)` correctly returns 0. For
NULL-ignoring aggregations (`min`, `max`, `sum`, `avg`), the marker is not needed.
For other aggregations, the general form `agg(...) FILTER (WHERE u_marker)` may
be required to exclude non-matching rows.

This technique is more general but can be expensive because it performs a
nested-loop join. It works well when:

- The inner table (`u`) is small enough to broadcast to all nodes
- The correlation condition cannot be expressed as a simple equality
- Other decorrelation techniques don't apply

#### Distinct Correlation Values (DELIM)

The techniques above (basic decorrelation, window functions, and assign-unique-id)
all require the subquery to be "flattenable" into a LEFT JOIN + aggregation. However,
some subqueries have internal structure that cannot be flattened:

```sql
-- Subquery with internal GROUP BY / HAVING
SELECT * FROM t
WHERE EXISTS (
    SELECT 1 FROM u
    WHERE u.x[t.a] = 1
    GROUP BY u.category
    HAVING count(*) > 10
)

-- LATERAL join returning multiple rows
SELECT t.*, sub.* FROM t,
LATERAL (SELECT * FROM u WHERE u.x[t.a] = 1 LIMIT 5) AS sub
```

The **Distinct Correlation Values** approach (also known as "DELIM" in DuckDB,
based on the paper "Unnesting Arbitrary Queries" by Neumann and Kemper, BTW 2015)
handles these cases by running the subquery as a "black box" for each distinct
correlation value:

1. **Collect** distinct values of correlation columns from outer table
2. **Execute** the parameterized inner subquery for each distinct value
3. **Join** results back to outer table

```sql
-- Original
SELECT * FROM t
WHERE (SELECT max(y) FROM u WHERE u.x[t.a] = 1) IS NULL

-- DELIM approach
WITH distinct_keys AS (
    SELECT DISTINCT a FROM t
),
agg AS (
    SELECT dk.a AS key, (
        SELECT max(u.y) FROM u WHERE u.x[dk.a] = 1
    ) AS max_y
    FROM distinct_keys dk
)
SELECT t.*
FROM t LEFT JOIN agg ON t.a = agg.key
WHERE agg.max_y IS NULL
```

The corresponding plan uses a LATERAL join to parameterize the subquery:

```
Filter: max_y IS NULL
  └─ LeftJoin on t.a = key
       ├─ Scan(t)
       └─ LeftJoin LATERAL on TRUE
            ├─ Agg(groupBy=[a]): (distinct correlation values)
            │    └─ Scan(t)
            └─ Agg: max(y)
                 └─ Filter: x[dk.a] = 1
                      └─ Scan(u)
```

**When DELIM is more efficient than Assign-Unique-ID:**
- When `|distinct correlation values| << |outer rows|` — DELIM executes the
  subquery once per distinct value, not once per outer row
- When the inner subquery is complex (has GROUP BY, HAVING, nested subqueries)

**Implementation requirement:** DELIM requires LATERAL join support in the
execution engine — the ability to execute a subquery parameterized by values
from the outer side.

## Supported Subquery Shapes in Axiom

The Axiom optimizer supports three types of subqueries—scalar, IN, and
EXISTS—in both correlated and uncorrelated forms. Subqueries can appear in:

- **WHERE clauses** (filter predicates)
- **INNER JOIN ON clauses** (processed as cross join + filter)
- **SELECT lists** (projections)
- **GROUP BY keys** (grouping expressions)

**Correlation types:**
- **Equality correlations** of the form `f(outer) = g(inner)` become join keys
  and enable efficient hash join execution.
- **Non-equality correlations** (e.g., `outer.a > inner.b`) become join filters
  and require nested-loop join execution.

This section describes each supported shape and how it is transformed
into an equivalent join-based query.

### Scalar Subqueries

A scalar subquery returns exactly one row with one column. It can be used in
any expression context where a single value is expected.

#### Uncorrelated Scalar Subquery in Filter

When the subquery doesn't reference the outer query, it can be evaluated
independently. A scalar subquery must return exactly one row; if it returns
zero or more than one row, the query fails with an error.

```sql
-- Original
SELECT * FROM orders
WHERE order_total > (SELECT min_order_amount FROM daily_rates WHERE rate_date = '2024-01-15')

-- Rewritten as
SELECT * FROM orders
CROSS JOIN (
    EnforceSingleRow(
        SELECT min_order_amount FROM daily_rates WHERE rate_date = '2024-01-15'
    )
) AS subq
WHERE order_total > subq.min_order_amount
```

The `EnforceSingleRow` operator validates that the subquery returns exactly one
row, failing with an error otherwise.

If the subquery result can be computed at planning time (see "Constant Folding"
below), it is replaced with a literal constant, eliminating the join entirely.

#### Uncorrelated Scalar Subquery in Projection

When a scalar subquery appears in the SELECT list, it is also cross-joined.

```sql
-- Original
SELECT r_name, (SELECT count(*) FROM nation) AS total_nations
FROM region

-- Rewritten as
SELECT r_name, subq.cnt AS total_nations
FROM region
CROSS JOIN (SELECT count(*) AS cnt FROM nation) AS subq
```

**Note**: `EnforceSingleRow` is only needed when the subquery does not naturally
return exactly one row. In this example, `count(*)` is a global aggregation that
always produces one row, so `EnforceSingleRow` is not required.

#### Uncorrelated Scalar Subquery in GROUP BY

Scalar subqueries can also appear in GROUP BY keys. Like other uses, the subquery
is cross-joined and its result becomes available as a grouping expression.

```sql
-- Original
SELECT sum(n_nationkey)
FROM nation
GROUP BY (SELECT max(r_regionkey) FROM region)

-- Rewritten as
SELECT sum(n_nationkey)
FROM nation
CROSS JOIN (
    EnforceSingleRow(SELECT max(r_regionkey) FROM region)
) AS subq
GROUP BY subq.max_r_regionkey
```

This is particularly useful when grouping by a dynamically computed value that
depends on data from another table.

#### Correlated Scalar Subquery with Aggregation

Correlated scalar subqueries reference columns from the outer query. When the
subquery contains aggregation, the correlation key is added as a grouping key,
which naturally produces one row per correlation value and enables decorrelation.

```sql
-- Original
SELECT * FROM region r
WHERE r.r_regionkey < (
    SELECT max(n.n_regionkey)
    FROM nation n
    WHERE n.n_regionkey = r.r_regionkey
)

-- Rewritten as
SELECT r.*
FROM region r
LEFT JOIN (
    SELECT n_regionkey, max(n_regionkey) AS max_key
    FROM nation
    GROUP BY n_regionkey
) AS subq ON subq.n_regionkey = r.r_regionkey
WHERE r.r_regionkey < subq.max_key
```

The correlation column (`n_regionkey`) becomes a grouping key in the subquery,
ensuring exactly one row per correlation value. The LEFT JOIN preserves outer
rows that have no matches (producing NULL for the subquery result).

For aggregations where `agg(<empty set>) == NULL` (such as `min`, `max`, `sum`,
`avg`), this NULL correctly represents "no matching rows".

For "counting-like" aggregations like `count(*)`, `count_if`, or `approx_distinct`
that return a non-NULL value for empty sets (e.g., 0), the optimizer wraps
the aggregate result with `COALESCE(agg_result, <empty_input_value>)`. This
replaces the NULL from the LEFT JOIN (no matches) with the correct value.

This COALESCE wrapping is safe because these aggregates never return NULL for
non-empty input. The `FunctionRegistry::aggregateResultForEmptyInput()` method
identifies such aggregates and provides their empty-input result.

#### Correlated Scalar Subquery without Aggregation

When a correlated scalar subquery does not contain aggregation, it must still
return at most one row per correlation value. Axiom uses the `EnforceDistinct`
operator to validate this constraint at runtime.

```sql
-- Original
SELECT *,
       (SELECT u.c FROM u WHERE u.a = t.a AND u.b > t.b) AS subq_result
FROM t
```

```
-- Rewritten plan
EnforceDistinct(keys=[__rownum], error="Scalar sub-query has returned multiple rows")
  └─ LeftJoin(on: u.a = t.a, filter: u.b > t.b)
       ├─ AssignUniqueId(__rownum)
       │    └─ Scan(t)
       └─ Scan(u)
```

The decorrelation works as follows:

1. **AssignUniqueId**: Assigns a unique identifier (`__rownum`) to each outer
   row. This ID is used to detect when multiple subquery rows match the same
   outer row.

2. **LEFT JOIN**: Joins the outer table with the subquery's base table using
   equality correlation conditions as join keys and non-equality conditions
   as the join filter.

3. **EnforceDistinct**: Validates that each unique outer row ID appears at
   most once in the join result. If the same `__rownum` appears multiple times
   (meaning the subquery returned multiple rows for that outer row), the
   operator throws a runtime error: "Scalar sub-query has returned multiple rows".

Since the join preserves the ordering of the left side rows (rows with the same
`__rownum` are consecutive), the streaming version of EnforceDistinct can be used.
This requires only O(1) memory as it only needs to compare adjacent rows.

**Note**: In this example, the equality condition `u.a = t.a` is used as the
hash join key, while the inequality `u.b > t.b` becomes the join filter. When
a correlation has only non-equality conditions (no equalities), a nested-loop
join is used instead.

#### Correlated Scalar Subquery in Projection

Correlated scalar subqueries in the SELECT list are decorrelated the same way
as in filters. With aggregation, the correlation key becomes a GROUP BY key:

```sql
-- Original
SELECT r_name,
       (SELECT max(n_nationkey) FROM nation WHERE n_regionkey = r_regionkey) AS max_nation
FROM region

-- Rewritten as
SELECT r.r_name, subq.max_nation
FROM region r
LEFT JOIN (
    SELECT n_regionkey, max(n_nationkey) AS max_nation
    FROM nation
    GROUP BY n_regionkey
) AS subq ON subq.n_regionkey = r.r_regionkey
```

Without aggregation, the AssignUniqueId + EnforceDistinct pattern is used:

```sql
-- Original
SELECT a, (SELECT c FROM u WHERE u.a = t.a) AS subq_result
FROM t
```

```
-- Rewritten plan
EnforceDistinct(keys=[__rownum], error="Scalar sub-query has returned multiple rows")
  └─ LeftJoin(on: u.a = t.a)
       ├─ AssignUniqueId(__rownum)
       │    └─ Scan(t)
       └─ Scan(u)
```

### IN Subqueries

IN subqueries test whether a value exists in a set returned by the subquery.
They are transformed into semi-joins.

#### Uncorrelated IN Subquery

```sql
-- Original
SELECT * FROM nation
WHERE n_regionkey IN (SELECT r_regionkey FROM region WHERE r_name > 'ASIA')

-- Rewritten as
SELECT n.*
FROM nation n
LEFT SEMI JOIN (
    SELECT r_regionkey FROM region WHERE r_name > 'ASIA'
) AS subq ON n.n_regionkey = subq.r_regionkey
```

The semi-join returns each nation row at most once if a matching region exists.
This uses **null-aware** semantics to correctly handle NULL values in the IN
comparison.

#### Uncorrelated NOT IN Subquery

NOT IN is transformed into an anti-join.

```sql
-- Original
SELECT * FROM nation
WHERE n_regionkey NOT IN (SELECT r_regionkey FROM region WHERE r_name > 'ASIA')

-- Rewritten as
SELECT n.*
FROM nation n
ANTI JOIN (
    SELECT r_regionkey FROM region WHERE r_name > 'ASIA'
) AS subq ON n.n_regionkey = subq.r_regionkey
```

The anti-join returns nation rows that have no matching region. Null-aware
semantics ensure correct three-valued logic for NOT IN with NULLs.

#### Correlated IN Subquery

When the IN subquery references outer columns, correlation conditions become
additional join keys.

```sql
-- Original: Find nations where another nation in the same region has name > 'M'
SELECT * FROM nation n1
WHERE n1.n_nationkey IN (
    SELECT n2.n_nationkey
    FROM nation n2
    WHERE n2.n_regionkey = n1.n_regionkey
    AND n2.n_name > 'M'
)

-- Rewritten as
SELECT n1.*
FROM nation n1
LEFT SEMI JOIN nation n2
    ON n2.n_nationkey = n1.n_nationkey   -- IN equality
    AND n2.n_regionkey = n1.n_regionkey  -- correlation equality
WHERE n2.n_name > 'M'
```

Both the IN condition and the correlation condition become join keys.

#### IN Subquery in Projection

When IN appears in the SELECT list rather than WHERE, it produces a boolean
column indicating membership.

```sql
-- Original
SELECT n_name,
       n_regionkey IN (SELECT r_regionkey FROM region WHERE r_name > 'ASIA') AS in_large_region
FROM nation

-- Rewritten using a mark join
SELECT n.n_name, subq.__mark AS in_large_region
FROM nation n
LEFT SEMI JOIN (PROJECT) (
    SELECT r_regionkey FROM region WHERE r_name > 'ASIA'
) AS subq ON n.n_regionkey = subq.r_regionkey
```

The semi-join produces a boolean "mark" column indicating whether each nation
row had a match.

### EXISTS Subqueries

EXISTS tests whether a subquery returns any rows at all.

#### Uncorrelated EXISTS

When EXISTS doesn't reference the outer query, it's transformed into a
cross-join with a subquery that uses HAVING to return one row if any rows
exist, or zero rows otherwise.

```sql
-- Original
SELECT * FROM region
WHERE EXISTS (SELECT * FROM nation)

-- Rewritten as
SELECT r.*
FROM region r
CROSS JOIN (
    SELECT 1
    FROM (SELECT * FROM nation LIMIT 1)
    HAVING count(*) > 0
) AS subq
```

The inner `LIMIT 1` avoids scanning the entire nation table—we only need
to know if at least one row exists. The HAVING clause filters out the subquery
result if no rows exist, causing the cross-join to produce no rows.

#### Uncorrelated NOT EXISTS

```sql
-- Original
SELECT * FROM region
WHERE NOT EXISTS (SELECT 1 FROM nation)

-- Rewritten as
SELECT r.*
FROM region r
CROSS JOIN (
    SELECT 1
    FROM (SELECT 1 FROM nation LIMIT 1)
    HAVING count(*) = 0
) AS subq
```

#### Correlated EXISTS with Equality Condition

The most common case: EXISTS with an equality correlation condition becomes a
semi-join.

```sql
-- Original
SELECT * FROM customer c
WHERE EXISTS (
    SELECT 1 FROM orders o
    WHERE o.o_custkey = c.c_custkey
)

-- Rewritten as
SELECT c.*
FROM customer c
LEFT SEMI JOIN orders o ON o.o_custkey = c.c_custkey
```

The correlation equality becomes the join key, enabling efficient hash-based
execution.

#### Correlated NOT EXISTS

NOT EXISTS becomes an anti-join.

```sql
-- Original
SELECT * FROM customer c
WHERE NOT EXISTS (
    SELECT 1 FROM orders o
    WHERE o.o_custkey = c.c_custkey
)

-- Rewritten as
SELECT c.*
FROM customer c
ANTI JOIN orders o ON o.o_custkey = c.c_custkey
```

This returns customers with no orders.

#### Correlated EXISTS with Non-Equality Condition

When the correlation uses inequality or other non-equality conditions, a
nested-loop join with a filter is used instead of a hash join.

```sql
-- Original
SELECT * FROM nation
WHERE EXISTS (SELECT 1 FROM region WHERE r_regionkey > n_regionkey)

-- Rewritten as
SELECT n.*
FROM nation n
LEFT SEMI JOIN (NESTED LOOP) region r
WHERE r.r_regionkey > n.n_regionkey
```

Non-equality conditions cannot be used as hash join keys, so the optimizer
falls back to nested-loop execution with the condition applied as a filter.

#### Correlated EXISTS in Projection

When EXISTS appears in the SELECT list, it produces a boolean column.

```sql
-- Original
SELECT c.c_name,
       EXISTS (SELECT 1 FROM orders WHERE o_custkey = c.c_custkey) AS has_orders
FROM customer c

-- Rewritten using a mark join
SELECT c.c_name, subq.__mark AS has_orders
FROM customer c
LEFT SEMI JOIN (PROJECT) orders o ON o.o_custkey = c.c_custkey
```

### Constant Folding Optimization

Certain uncorrelated scalar subqueries can be evaluated at query planning time,
replacing the subquery with a literal constant. This is called **constant
folding**.

#### When Constant Folding Applies

Constant folding is possible when:

1. The subquery reads from a single table
2. The subquery performs a global aggregation (no GROUP BY)
3. The aggregate functions either ignore duplicates (`MIN`, `MAX`,
   `ARBITRARY`) or use DISTINCT
4. The aggregated columns have a known, finite set of possible values
   (provided by the connector as "discrete values")

#### Example

```sql
-- Original
SELECT * FROM events
WHERE ds = (SELECT max(ds) FROM events)

-- After constant folding (connector provides discrete ds values)
SELECT * FROM events
WHERE ds = '2024-01-15'
```

The optimizer evaluates `max(ds)` using the discrete values provided by the
connector (e.g., all known partition dates) and substitutes the result
directly.

### Summary of Transformations

#### Uncorrelated Subqueries

| Subquery Type | Location | Rewritten As |
|---------------|----------|--------------|
| Scalar | Filter/Projection/GROUP BY | Cross join with EnforceSingleRow |
| IN | Filter | Semi-join (null-aware) |
| IN | Projection | Mark semi-join (null-aware) |
| NOT IN | Filter | Anti-join (null-aware) |
| NOT IN | Projection | Mark semi-join (null-aware) |
| EXISTS | Filter | Cross join + HAVING count check |
| EXISTS | Projection | Cross join + count > 0 |
| NOT EXISTS | Filter | Cross join + HAVING count check |
| NOT EXISTS | Projection | Cross join + count = 0 |

#### Correlated Subqueries

| Subquery Type | Location | Rewritten As |
|---------------|----------|--------------|
| Scalar (with agg) | Filter/Projection/GROUP BY | Left join + GROUP BY correlation key |
| Scalar (without agg) | Filter/Projection/GROUP BY | AssignUniqueId + Left join + EnforceDistinct |
| IN | Filter | Semi-join with correlation keys |
| IN | Projection | Mark semi-join with correlation keys |
| NOT IN | Filter | Anti-join with correlation keys |
| NOT IN | Projection | Mark anti-join with correlation keys |
| EXISTS (equality) | Filter | Semi-join |
| EXISTS (non-equality) | Filter | Nested-loop semi-join |
| EXISTS | Projection | Mark semi-join |
| NOT EXISTS (equality) | Filter | Anti-join |
| NOT EXISTS (non-equality) | Filter | Nested-loop semi-join + NOT |
| NOT EXISTS | Projection | Mark semi-join + NOT |

## Key Source Files

| File | Purpose |
|------|---------|
| `ToGraph.cpp` | Core subquery processing (`processSubqueries`, `translateSubquery`, `extractDecorrelatedJoin`) |
| `ToGraph.h` | Interface definitions, correlation state fields, `DecorrelatedJoin` struct |
| `QueryGraph.h` | `JoinEdge` representation including `makeExists` factory |
| `DerivedTable.h` | Query graph nodes, `singleRowDts` for non-correlated scalars |
| `tests/SubqueryTest.cpp` | Test suite for scalar, EXISTS, and uncorrelated subqueries |
| `tests/JoinTest.cpp` | Test suite including correlated IN subqueries |

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

### DecorrelatedJoin Struct

When processing correlated subqueries, correlation conjuncts are extracted into
a common structure:

```cpp
struct DecorrelatedJoin {
  PlanObjectSet leftTables;     // Outer tables referenced by correlation
  ExprVector leftKeys;          // Keys from outer query (for equalities)
  ExprVector rightKeys;         // Keys from subquery (for equalities)
  ExprVector nonEquiConjuncts;  // Non-equality correlation conditions
};
```

The `extractDecorrelatedJoin()` helper processes `correlatedConjuncts_` and
separates equality conditions (which become join keys) from non-equality
conditions (which become join filters).

### JoinEdge Fields for Subqueries

Subquery decorrelation uses various `JoinEdge::Spec` fields to represent
different subquery semantics. Most fields (`filter`, `rightOptional`,
`rightExists`, `rightNotExists`, `nullAwareIn`, `markColumn`) are also used
for regular joins. The `rowNumberColumn` and `multipleMatchesError` fields
are used exclusively for subqueries.

```cpp
struct Spec {
  /// Filter conjuncts to be applied after the join. Only for non-inner joins.
  /// Used for non-equality correlation conditions (e.g., u.b > t.b).
  ExprVector filter;

  /// True for LEFT and FULL OUTER JOIN. The output may have no match on the
  /// right side. Set for correlated scalar subqueries.
  bool rightOptional{false};

  /// True for EXISTS subquery. Mutually exclusive with 'rightNotExists'.
  bool rightExists{false};

  /// True for NOT EXISTS subquery. Mutually exclusive with 'rightExists'.
  bool rightNotExists{false};

  /// When true, the join semantic is IN / NOT IN. When false, the join
  /// semantic is EXISTS / NOT EXISTS. Applies to semi and anti joins.
  bool nullAwareIn{false};

  /// Marker column produced by 'exists' or 'not exists' join. If set, the
  /// 'rightExists' must be true. Used when EXISTS/IN appears in the SELECT
  /// list rather than WHERE clause.
  ColumnCP markColumn{nullptr};

  /// Row number column to be assigned to the non-optional (probe) side using
  /// AssignUniqueId. Used for decorrelating scalar subqueries with non-equi
  /// correlation conditions and scalar subqueries without aggregation. The
  /// join output will be clustered on this column.
  ColumnCP rowNumberColumn{nullptr};

  /// When set, validate at runtime during query execution that the join
  /// produces at most one match for each left-side row. Throws with this
  /// error message if multiple matches are found. Used for decorrelating
  /// scalar subqueries without aggregation. Requires 'rowNumberColumn' to
  /// be set.
  Name multipleMatchesError{nullptr};
};
```

**Field usage by subquery type:**

| Subquery Type | Join Kind | Key Spec Fields |
|---------------|-----------|-----------------|
| EXISTS | Semi join | `rightExists = true` |
| NOT EXISTS | Anti join | `rightNotExists = true` |
| IN | Semi join (null-aware) | `rightExists = true`, `nullAwareIn = true` |
| NOT IN | Anti join (null-aware) | `rightNotExists = true`, `nullAwareIn = true` |
| EXISTS/IN in SELECT | Mark join | `markColumn` set |
| Scalar (with agg, equi-only) | Left join | `rightOptional = true` |
| Scalar (with agg, non-equi) | Left join | `rightOptional = true`, `rowNumberColumn` |
| Scalar (without agg) | Left join + EnforceDistinct | `rightOptional = true`, `rowNumberColumn`, `multipleMatchesError` |

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

## Limitations and TODOs

### Current Limitations

1. **Multi-table IN expressions not supported**
   ```sql
   -- Not supported:
   WHERE (a, b) IN (SELECT x, y FROM t)
   ```

2. **Correlated conjuncts referencing multiple outer tables**
   ```sql
   -- Not supported:
   WHERE EXISTS (SELECT 1 FROM t WHERE t.a = outer1.x AND t.b = outer2.y)
   ```
   Note: This limitation applies only when outer1 and outer2 are different
   base tables, not aliases of the same derived table.

3. **Multi-table expression in IN predicate left-hand side**
   ```cpp
   VELOX_CHECK_NOT_NULL(
       leftTable,
       "<expr> IN <subquery> with multi-table <expr> is not supported yet");
   ```

## Architectural Notes

1. **DerivedTable Container**: Each subquery gets its own isolated `DerivedTable`
   node, enabling independent optimization.

2. **Join Edge Reuse**: Subqueries leverage the existing join planning
   infrastructure, enabling cost-based join ordering to consider subquery joins.

3. **Symbol Map Correlation**: Supports arbitrary nested correlation depths
   through the `correlations_` pointer chain.

4. **Mark Columns**: Boolean columns that indicate whether a row has a match in
   the subquery. Used for IN/EXISTS expressions in projections (SELECT list)
   and for null-aware IN/NOT IN semantics.

5. **Early Constant Folding**: Reduces plan complexity and improves cardinality
   estimates by evaluating foldable subqueries at planning time.

## Related Documentation

- See [JoinPlanning.md](JoinPlanning.md) for details on how joins (including
  subquery joins) are planned and optimized.
