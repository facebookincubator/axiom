# History-Based Optimization (HBO) Design Proposal

*February 17, 2026*

## Overview

### Why HBO?

The optimizer estimates cardinalities using table statistics and heuristics
(see [CardinalityEstimation.md](CardinalityEstimation.md)). These estimates can
be inaccurate when:

- **Correlation between columns** — predicates on correlated columns compound
  estimation errors (e.g., `city = 'Seattle' AND state = 'WA'`)
- **Complex expressions** — UDFs, LIKE patterns, and multi-column predicates
  are difficult to estimate accurately
- **Join fanout** — estimating output of joins depends on data distribution
  that statistics may not capture
- **Statistics are stale or missing** — tables may have outdated row counts or
  lack NDV information

History-Based Optimization addresses these gaps by recording actual execution
statistics and using them for future queries with matching plan fragments.

### Primary Use Cases

**Scheduled pipelines.** The primary use case for HBO is the repeated
execution of scheduled data pipelines where only the date/timestamp
partition (`ds`, `ts`) changes between runs. These queries run daily or
hourly on similar data, so historical cardinalities from yesterday's run
are excellent predictors for today's run. Partition parameterization ensures
that `ds = '2024-01-01'` and `ds = '2024-01-02'` match the same history
entry.

Occasionally, pipeline queries receive minor tweaks — an additional filter,
a new column in the SELECT list, a changed aggregate. HBO should handle
these gracefully:
- Adding a SELECT column → same key (payload excluded)
- Adding a filter predicate → predicate relaxation (history from the base query
  still helps)
- Changing an aggregate function → same key (aggregates are payload)

**Debugging scheduled pipelines.** Engineers debugging pipeline issues often
run ad-hoc queries that represent a subset or minor variation of the
scheduled query — for example, adding a `LIMIT`, filtering to a specific
region, or selecting only a few columns to inspect the data. These ad-hoc
queries should benefit from the history accumulated by the scheduled
pipeline runs:
- Sub-tree reuse: an ad-hoc query that includes the same filtered scan or
  join sub-tree as the pipeline query matches that sub-tree's history.
- Payload independence: different SELECT columns in the ad-hoc query still
  match the pipeline's history entries for cardinality.
- Predicate relaxation: the ad-hoc query's predicates are a superset of (or
    overlap with) the pipeline's predicates.

### Document Outline

**[Architecture](#architecture)** describes the two main flows — recording
and retrieval — and defines what statistics are collected per operator.
**[Canonical Key Generation](#canonical-key-generation)** is the core of the
design: it specifies which operators are included in the key, how expressions
and columns are encoded, and which operators are transparent.
**[Statistics Storage](#statistics-storage)** covers what is intentionally
*not* stored and the pluggable storage backend.
**[Recording and Retrieval](#recording-and-retrieval)** details the top-down
retrieval algorithm, the dual-key lookup strategy, and validation via catalog
metadata.
**[Connector Integration](#connector-integration)** and
**[Testing Strategy](#testing-strategy)** outline the required connector APIs
and the verification approach.
**[Open Questions](#open-questions)** lists unresolved design decisions.
Finally, **[Reference: Presto HBO](#reference-presto-hbo)** documents Presto's
approach in detail and compares it with our design.

## Architecture

**Control vs. payload columns.** Every column in a query plan is either a
*control* column or a *payload* column. **Control columns** affect output
cardinality: join keys, group-by keys, filter predicates, and partition keys.
**Payload columns** do not: SELECT expressions, aggregate functions, and
projected columns that no downstream operator filters on. This distinction is
central to the design — canonical keys include control columns and exclude
payload columns, enabling broad history reuse across queries that differ only
in which columns they select or how they aggregate. Output data size, which
does depend on payload columns, is handled via a dual-key approach described
in [Statistics Collected](#statistics-collected).

HBO consists of two main flows:

1. **Recording** — After query execution, task statistics (cardinality,
   NULL key counts) are extracted and stored in the `History` component
   keyed by the canonical representation of each operator.

   **RelationOp → PlanNode mapping.** The optimizer works with `RelationOp`
   trees, but execution happens on Velox `PlanNode` trees. Canonical keys are
   generated from `RelationOp` nodes; execution stats are collected from
   `PlanNode` nodes. The `ToVelox` translation step — which runs **after
   optimization is complete and the plan is stable** — builds a mapping from
   `PlanNodeId` → `historyKey` (the canonical key of the corresponding
   `RelationOp`). After execution, `VeloxHistory::recordVeloxExecution()` uses
   this mapping to associate observed stats back to the correct history keys.

   The canonical key is critical: it determines when two operators are
   considered "the same" for history matching. The key must be:
   - **Stable** — Equivalent operators produce identical keys when queries are re-optimized
     - Alias-independent: `SELECT * FROM t AS x` matches `SELECT * FROM t AS y`
     - Order-independent: `a > 1 AND b < 2` matches `b < 2 AND a > 1`
   - **Parameterizable** — Partition values can be abstracted (e.g., `ds = '2024-01-01'` → `ds = ?`)

   See [Canonical Key Generation](#canonical-key-generation) for detailed discussion.

2. **Retrieval** — During optimization, the optimizer generates canonical keys
   for operators and looks up historical statistics. If found, these statistics
   override the cardinality estimates derived from table statistics and
   heuristics.

```
+---------------+                   +---------------+
|               |    retrieval      |               |
|  Optimization | ----------------> |    History    |
|               |                   |               |
+---------------+                   +---------------+
                                           ^
                                           |
+---------------+                          |
|               |      recording           |
|   Execution   | -------------------------+
|               |
+---------------+
```

### Statistics Collected

Each history entry is keyed by the operator's canonical key. HBO uses two
keys per operator to balance reuse breadth with data size accuracy:

**Control-only key** (excludes payload — broad reuse):

| Field | Description |
|-------|-------------|
| Output cardinality | Number of rows produced by the operator |

**Full key** (includes payload — exact match):

| Field | Description |
|-------|-------------|
| Output cardinality | Number of rows produced by the operator |
| Output data size | Total bytes produced by the operator |

**Join-specific fields.** Joins additionally store NULL key counts for
build and probe sides.

| Field | Purpose |
|-------|---------|
| Build-side NULL key count | NULLs hash to the same node; high counts trigger optimization (randomization) |
| Probe-side NULL key count | Same |

**Why store NULL key counts?** NULL keys all hash to the same value, causing
memory overload during shuffle. Inner joins can drop NULL keys before shuffle
(NULLs never match), but outer joins must preserve them. High NULL key counts
can trigger optimizations that randomize NULL keys to spread them across
nodes.[^1]

[^1]: Presto implements this in `RandomizeNullKeyInOuterJoin`, which replaces
`key` with `COALESCE(key, Randomize(key))`.

## Canonical Key Generation

The canonical key determines when two sub-trees (rooted at an operator) are
considered "the same" for history matching. Getting this right is critical for
HBO effectiveness.

### What Makes HBO Effective?

HBO records exact statistics from query execution — these are accurate by
definition. The challenge is that we want HBO to help optimize queries that are
*similar* to ones that ran before, not just *identical* queries.

**Example 1: Different partition, same query shape**

- `SELECT * FROM orders WHERE ds = '2024-01-01'` ran yesterday
- `SELECT * FROM orders WHERE ds = '2024-01-02'` runs today

These queries scan different partitions but likely have similar cardinalities.
If we can match them to the same history entry, HBO provides value.

**Example 2: Sub-tree from a previous query**

- Yesterday: `SELECT * FROM orders WHERE ds = '2024-01-01' AND region = 'US'`
- Today: `SELECT o.*, c.name FROM orders o JOIN customers c ON o.cust_id = c.id
         WHERE o.ds = '2024-01-02' AND o.region = 'US'`

The second query includes a filtered `orders` scan similar to what ran before.
Even though the queries are different, the sub-tree cardinality can be reused.

**Example 3: Same join, different payload columns**

- Yesterday: `SELECT o.id, o.total FROM orders o JOIN customers c ON o.cust_id = c.id`
- Today: `SELECT o.id, c.name, c.email FROM orders o JOIN customers c ON o.cust_id = c.id`

The join has the same tables and keys, only the projected columns differ. The
join cardinality and fanout are determined by the join condition, not the
columns selected. These should match the same history entry.

**Example 4: Same grouping keys, different aggregates**

- Yesterday: `SELECT region, SUM(revenue) FROM sales GROUP BY region`
- Today: `SELECT region, AVG(revenue), COUNT(*) FROM sales GROUP BY region`

The output cardinality is determined by the grouping keys (`region`), not the
aggregate functions. These should match the same history entry.

**Example 5: Additional filter conjuncts**

- Yesterday: `SELECT * FROM orders WHERE region = 'US'`
- Today: `SELECT * FROM orders WHERE region = 'US' AND status = 'shipped'`

These queries have different predicates, but we can still leverage history:
use the recorded cardinality for `region = 'US'` (say, 10,000 rows), then
apply the standard selectivity estimate for `status = 'shipped'`. This gives
a hybrid approach: historical accuracy for known predicates, standard
estimation for new ones.

### Patterns HBO Must Handle

**Parameterization patterns (should match):**
- Partition column values: `ds = '2024-01-01'` ↔ `ds = '2024-01-02'`

**Equivalence patterns (should match):**
- Table alias independence: `FROM t AS x` ↔ `FROM t AS y`
- Predicate reordering: `a > 1 AND b < 2` ↔ `b < 2 AND a > 1`
- Commutative expressions: `a + b` ↔ `b + a`

**Payload independence (should match):**
- Same join, different SELECT columns
- Same grouping keys, different aggregate functions

**Sub-tree reuse (should match):**
- Same filtered scan appears in different queries
- Same join sub-tree appears in larger queries

**Hybrid approach (partial match):**
- Base predicates match history; new predicates use standard estimation

**What does this imply for key design?**

These patterns have implications for how we design canonical keys, what
statistics we store, and how we retrieve them:

- **Key design** — points 1–4 below and [Key Format Specification](#key-format-specification)
- **Statistics storage** — [Statistics Collected](#statistics-collected) and [Statistics Storage](#statistics-storage)
- **Retrieval algorithm** — [Retrieval with Validation](#retrieval-with-validation)

1. **Store per sub-tree, not per query.** To enable sub-tree reuse (patterns
   from different queries matching), history must be keyed by sub-tree, not
   by the full query. Each operator's key includes its children recursively.

2. **Exclude payload from keys.** Join cardinality depends on join condition,
   not SELECT columns. Aggregation cardinality depends on GROUP BY, not
   aggregate functions. Keys must exclude "payload" that doesn't affect
   cardinality. This maximizes reuse across many new queries.

3. **Parameterize partition columns.** To match queries on different partitions,
   replace partition literals with placeholders (`ds = '2024-01-01'` → `ds = ?`).

4. **Normalize equivalent representations.** Sort predicates, use canonical
   column names (not aliases), rely on existing expression canonicalization.

### Key Format Specification

Keys are generated from the **physical optimized plan**. Each operator's key
embeds its children's keys recursively, forming a sub-tree representation.

HBO generates **two keys** per operator (see [Statistics Collected](#statistics-collected)):

- **Control-only key** — includes only cardinality-affecting information:
  join keys, group-by keys, filter predicates, partition columns. Payload
  columns (SELECT expressions, aggregate functions) are excluded. The
  operator patterns below define this key.
- **Full key** — extends the control-only key by appending payload columns
  (output column list, aggregate functions, projected expressions). This key
  is used to store output data size, which depends on which columns are
  produced.

The rest of this section specifies the **control-only key** patterns. The full
key appends payload information to the same structure.

There are three categories of operators for key generation:

1. **Cardinality-affecting** — always included in the key because they change
   the number of output rows (TableScan, Filter, Join, Aggregation, Limit,
   TopN, UnionAll, Unnest, Values).
2. **Conditionally included** — do not change row count, but included when
   they reference control columns (Project).
3. **Always transparent** — never included; key generation passes through
   to the child (Repartition, HashBuild, EnforceSingleRow, etc.).

#### Cardinality-Affecting Operators

**TableScan**

```text
scan(<connector>.<schema>.<table>, f:<pred1>, f:<pred2>, ...)
```

- Fully qualified table name: `connector.schema.table`
- Filter predicates split into individual conjuncts (`a > 1 AND b < 2`
  becomes two separate entries `f:a>1`, `f:b<2`), each sorted
  lexicographically by canonical expression string
- Partition predicates parameterized: `f:ds=?`
- Non-partition predicates exact: `f:region='US'`
- Index lookup keys included: `k:<key1>, k:<key2>`

Examples:
```text
scan(hive.prod.orders, f:ds=?, f:region='US')
scan(hive.prod.orders, f:ds=?, k:order_id)
```

**Filter**

```text
filter(f:<pred1>, f:<pred2>, ..., <child>)
```

- Predicates split into individual conjuncts and sorted lexicographically
- Child key embedded recursively

Example:
```text
filter(f:total>1000, scan(hive.prod.orders, f:ds=?))
```

**Join**

```text
join(<type>, on:<left1>=<right1>,<left2>=<right2>, <left>, <right>)
```

- Join type: `INNER`, `LEFT`, `RIGHT`, `FULL`, `SEMI`, `ANTI`
- Join key pairs sorted by left column name
- Non-equality join filter split into individual conjuncts and sorted: `jf:<pred1>, jf:<pred2>`
- Null-aware flag appended when true: `null_aware`
- **INNER join normalization:** For INNER joins, sort the two children
  deterministically (by their key strings) so that `A ⋈ B` and `B ⋈ A`
  produce the same key. For LEFT/RIGHT/SEMI/ANTI, preserve left/right
  ordering since the sides are not interchangeable.
- Join method (HASH/MERGE/CROSS) is **excluded** — it is a physical choice
  that does not affect cardinality.

Examples:
```text
join(INNER, on:id=cust_id,
  scan(hive.prod.customers),
  scan(hive.prod.orders, f:ds=?))

join(LEFT, on:id=order_id,
  scan(hive.prod.customers),
  scan(hive.prod.orders, f:ds=?))

join(ANTI, on:id=cust_id, null_aware,
  scan(hive.prod.customers),
  scan(hive.prod.blacklist))
```

Note: In the INNER join example, children are sorted alphabetically
(`customers` before `orders`), regardless of which side was originally
left or right. The join key pairs are adjusted to match: `id` (from
`customers`, now left) appears before `cust_id` (from `orders`, now right).

**Aggregation**

```text
agg(by:<key1>,<key2>, step:<step>, <child>)
```

- Grouping keys sorted lexicographically
- Aggregation step: `PARTIAL`, `FINAL`, `SINGLE`, `INTERMEDIATE`
  (different steps have different cardinality characteristics)
- Aggregate functions **excluded** from cardinality key (output cardinality
  depends on grouping keys, not aggregate functions)

Examples:
```text
agg(by:region,status, step:SINGLE,
  scan(hive.prod.orders, f:ds=?))

agg(by:region, step:FINAL,
  agg(by:region, step:PARTIAL,
    scan(hive.prod.orders, f:ds=?)))
```

**Limit**

```text
limit(<count>, offset:<offset>, <child>)
```

- Include count and offset (both affect output cardinality)
- Offset omitted when zero: `limit(100, <child>)`

#### Conditionally Included Operators

These operators do not change the number of output rows. They are included
in the key only when they reference control columns; otherwise they are
transparent (skipped).

**Project**

Project does not change row count but may compute control columns. Only
non-identity projections (computed expressions) matter — columns simply passed
through from the child are ignored. The operator is included in the key when
any computed expression produces a control column (used by a downstream filter,
join key, or group-by key); transparent when all computed expressions are
payload.

```text
project(expr:<expr1>,<expr2>, <child>)
```

- Only computed expressions that produce control columns are listed in the
  control-only key
- All computed expressions are listed in the full key
- Identity projections (pass-through columns) are never listed
- Transparent (skipped) when all computed expressions are payload

Note: Project affects output data size and per-column statistics even though
it doesn't affect cardinality. Output data size is handled via the dual-key
approach (see [Statistics Collected](#statistics-collected)). See
[Open Questions](#open-questions) for discussion of per-column constraints.

**TopN**

TopN limits output to the first N rows according to a sort order:

```text
topn(<count>, sort:<key1>,<key2>, <child>)
```

- Count determines the maximum output cardinality
- Sort keys follow the same control vs. payload rule: included in the
  control-only key only if they are control columns; always included in the
  full key

**UnionAll**

```text
union(<child1>, <child2>, ...)
```

- Children sorted deterministically (by their key strings) to normalize
  ordering — `a UNION ALL b` and `b UNION ALL a` produce the same key

Note: UNION ALL cardinality equals the sum of its children's cardinalities,
so matching individual branches (via top-down recursion) gives the same
result as matching the union as a whole. Sorting is still valuable because
it allows exact key matching when the same set of branches appears in a
different order.

**Unnest**

```text
unnest(<expr1>,<expr2>, <child>)
```

- Unnest expressions sorted lexicographically
- Ordinality column excluded (doesn't affect cardinality)

**Values**

```text
values(<hash>)
```

- Encodes the content via order-independent hashing: each row is hashed
  independently, then row hashes are combined using `bits::commutativeHashMix`.
  This ensures that VALUES clauses with the same rows in different order
  produce the same key, while clauses with different content produce different
  keys — important when a filter sits on top.

#### Always Transparent Operators

These operators are never included in either key. Key generation passes
through to the child.

| Operator | Reason |
|----------|--------|
| **Repartition** | Changes data distribution, not content |
| **HashBuild** | Build side of join; keyed via the Join operator |
| **EnforceSingleRow** | Runtime validation only |
| **EnforceDistinct** | Runtime validation only |
| **OrderBy** (no LIMIT) | Reorders rows without changing row count or data size — even when sorting keys are control columns, the sort order does not affect downstream cardinality |
| **AssignUniqueId** | Adds a synthetic column; unlikely to be a control column |
| **TableWrite** | Terminal operator, not relevant for read-path HBO (see [Open Questions](#open-questions) for write-path optimization) |

### Expression Encoding

The key format specification above defines the structure of keys per operator,
but leaves open the question of how individual **expressions** are encoded
within those keys. Expressions appear in multiple key components:

| Key Component | Example Expressions |
|---------------|-------------------|
| TableScan filter predicates | `region = 'US'`, `YEAR(created_at) = 2024` |
| Filter predicates | `total > 1000`, `length(name) > 10` |
| Join equality keys | `cust_id = id`, `YEAR(a.date) = b.year` |
| Join non-equality filters | `a.price > b.min_price` |
| Aggregation grouping keys | `region`, `YEAR(created_at)` |
| Unnest expressions | `tags`, `TRANSFORM(items, x -> x.id)` |

**Column references.** The existing `historyKey()` implementations use
`expr->toString()` with `cnamesInExpr` set to `false` (via `ScopedVarSetter`),
which strips table correlation prefixes and returns just `Column::name_`.

This is stable for **base table columns** — `name_` comes from the schema
(e.g., `id`, `region`, `ds`). These don't change across compilations.

However, **computed columns** get generated names like `__p{expr_id}`, where
`expr_id` comes from a sequential counter (`queryCtx()->newId(this)`). These
IDs depend on object creation order and are **NOT stable** across compilations.
The same query compiled twice may assign different IDs if optimizer passes
create objects in a different order. Minor query changes (e.g., adding a column
to SELECT) shift IDs for all subsequent objects.

**Ambiguity above joins.** With `cnamesInExpr = false`, both sides of a join
may have an `id` column, producing identical strings. However, the existing
`joinKeysString()` helper pairs left and right keys positionally (`leftText =
rightText`), so they are disambiguated by position in the key format. The risk
is limited to expressions in filter or project above a join.

**Proposed resolution: bottom-up hash IDs for columns.** As key generation
walks the plan bottom-up (children first, which is already how `historyKey()`
works), each operator assigns stable hash-based IDs to its output columns:

1. **TableScan**: Each output column gets
   `hash(schemaTable.name + "." + schemaColumn.name)` — e.g.,
   `hash("orders.region")`. Derived from the schema, completely stable.

2. **Project**: Each output column gets `hash(defining_expression)`, where the
   expression hash uses the hash IDs already assigned to its input columns by
   the operator below. E.g., `__p41 = YEAR(created_at)` gets
   `hash("year", hash("orders.created_at"))`.

3. **Filter, Join, Aggregation** (any operator above): When referencing a
   column in an expression, that column already carries a stable hash ID
   assigned by the producing operator below. No backtracking needed.

Hash IDs propagate upward naturally through the plan tree. Each operator only
needs its own inputs (which already have hash IDs) and its own expressions.
This is purely local — no tracing through the plan.

**Literals.** Filter predicates contain literal values: `f:region='US'`. These
must be encoded faithfully — `region = 'US'` and `region = 'EU'` are different
predicates with different selectivities. For partition columns, literals are
parameterized (`ds = ?`), but for non-partition literals, the actual value must
appear in the key.

**Function calls.** Predicates and grouping keys can contain function calls:

```text
f:YEAR(created_at)=2024
by:YEAR(created_at)
```

The function name and arguments must be encoded. `GROUP BY YEAR(created_at)`
and `GROUP BY MONTH(created_at)` have very different NDVs and must produce
different keys.

**Nested expressions.** Expressions can be arbitrarily nested:

```text
f:CAST(total / quantity AS DOUBLE) > 100.0
by:CONCAT(first_name, ' ', last_name)
```

The key must capture the full expression tree, not just the outermost function.

**Commutative operators.** `a + b` and `b + a` are semantically equivalent.
For commutative operators (`+`, `*`, `AND`, `OR`, `=`), we should sort
operands to normalize the representation. `ToGraph` already canonicalizes
comparisons (column op constant, smaller plan object ID on left), but this
may not cover all commutative functions.

**Values content.** See the [Values](#values) operator description for
details on order-independent hashing of row content.

#### Encoding Approach

Expressions are hashed for key generation. Expressions tend to be long and
verbose in text form, making hashing the natural choice for compact, fixed-size
keys. The hash is computed by recursively walking the expression tree:

- **Column reference** → hash of the column identifier
- **Literal** → hash of the typed value
- **Function call** → hash combining function name and argument hashes
- **Cast** → hash combining target type and argument hash
- **Commutative operators** (`+`, `*`, `AND`, `OR`, `=`) → combine operand
  hashes using `bits::commutativeHashMix` so that `a + b` and `b + a` produce
  the same hash. `ToGraph` already canonicalizes comparisons; we extend this
  to all commutative functions.

For **Values**, use order-independent hashing: hash each row independently,
then combine row hashes using `bits::commutativeHashMix` (already used in Velox
for MAP hashing in `Variant.cpp`). This produces `values(<hash>)` where the
hash is stable regardless of row ordering.

A mapping from hash to expression text can be stored separately for debugging
and diagnostics.

**Correctness note.** A hash collision (two different expressions producing the
same hash) would give wrong cardinality estimates. A false miss (equivalent
expressions producing different hashes) only loses reuse opportunity. We should
err toward distinct hashes — avoid lossy normalization.

**Existing canonicalization.** `ToGraph` already canonicalizes comparison
expressions. We should leverage this rather than re-implementing equivalence
rules. Since keys are generated from the physical plan, expressions have
already been through optimization and canonicalization.

## Statistics Storage

### What We Do NOT Store

| Metric | Why NOT Store |
|--------|---------------|
| CPU time | Varies with hardware, load, concurrent queries |
| Memory | Depends on runtime factors (spilling, buffer sizes) |
| Wall time | Includes I/O wait, affected by cluster state |

### Storage Backend

The storage layer uses a pluggable interface to decouple the optimizer from
any specific persistence mechanism:

- **File-based** — for testing and single-node debugging. Reads/writes a local
  JSON or binary file.
- **Remote** — for production. Backed by a configurable remote store
  (e.g., ZippyDB). Shared across sessions.

## Recording and Retrieval

### Recording

After query execution, walk the physical plan and store the observed output
cardinality for each operator, keyed by its canonical key. For the full key
(including payload), additionally store the output data size.

For joins, additionally record NULL key counts for build and probe sides.

### Retrieval with Validation

The optimizer traverses the plan tree **top-down**, looking for the largest
subtree that has a history match. Larger subtrees capture more cross-operator
effects (e.g., correlated predicates, join fanout) and produce better estimates
than composing statistics from smaller fragments.

**Top-down tree matching algorithm:**

1. Generate the canonical key for the current operator (which includes its
   full subtree recursively).
2. Look up the key in history using dual-key lookup:
   - **Full key** (includes payload columns): If hit, use both historical
     cardinality and output data size — this is the most precise match.
   - **Control-only key** (excludes payload): If full key misses, try the
     control-only key. On hit, use historical cardinality (accurate);
     compute data size approximately from cardinality × average row size.
   - **Both miss**: Recurse into children. Each child becomes a new root for
     the same top-down search.
   - **Hit (either key):** Use the historical statistics. Do not recurse
     into children — the subtree-level stats already account for everything
     below.
3. **Validate** the match before using it. At the scan level, compare the
   current partition-level statistics (row count from the metastore) against
   the row count recorded with the historical entry. For partitioned tables
   (e.g., `ds`-partitioned), this means comparing the size of the partition
   being scanned against the size of the partition from the historical run —
   not the table-level row count, which changes daily as new partitions
   arrive. If the partition size differs significantly (e.g., beyond a
   configurable threshold), reject the entry — the historical cardinalities
   may not apply to a partition of a very different size. This also catches
   schema changes, since adding or dropping columns typically changes
   partition-level statistics. For non-scan operators, the key itself encodes
   the full subtree structure; a key match implies structural equivalence.
4. At scan nodes, if the exact key misses, try **predicate relaxation**: remove
   predicates one at a time to find a subset match, then apply estimated
   selectivity for the unmatched predicates. The same applies to filter
   predicates in Filter operators and join conditions in outer joins.

**Example:**

```text
         Join (A ⋈ B)         ← try key for full join subtree first
        /            \
  Scan A              Scan B   ← on miss, try each scan independently
  (f:region=US        (f:year=2024)
   f:status=shipped)
```

- First, try the key for the entire join subtree. If history has seen this
  exact join with these exact filtered scans, we get the best estimate.
- On miss, recurse: try Scan A and Scan B independently. Each may have
  history from a different query that included the same filtered scan.
- If Scan A misses on `{region=US, status=shipped}`, try relaxing:
  remove `status=shipped`, look up `{region=US}`. On hit, apply:

  `cardinality = history(region=US) × selectivity(status=shipped)`

**Why top-down?** A join subtree captures correlation between the join
condition and the filters on each side. If we only match individual scans
and compose their cardinalities through the join, we lose this information
and fall back to the same independence assumptions that make standard
estimation inaccurate.

**Predicate relaxation details:**

The number of predicate subsets is exponential (2^n), so we bound the search:
- Only try removing 1 or 2 predicates (not arbitrary subsets)
- Prioritize removing predicates with readily available selectivity estimates
- Stop on first match (largest subset wins)

## Connector Integration

### Partition Column Detection

Connectors provide APIs to identify partition keys for a table. The canonical
key generator uses this to parameterize partition column predicates — replacing
literal values with `?` so that queries differing only in partition values
(e.g., different `ds` values) share the same history key.

## Testing Strategy

### Key Stability Tests

Verify that structurally equivalent queries produce identical canonical keys:

- **Alias independence** — `SELECT * FROM orders t1 WHERE t1.id > 5` and
  `SELECT * FROM orders t2 WHERE t2.id > 5` must produce the same key.
- **Predicate ordering** — `WHERE a > 1 AND b < 2` and `WHERE b < 2 AND a > 1`
  must produce the same key.
- **Partition parameterization** — `WHERE ds = '2024-01-01'` and
  `WHERE ds = '2024-01-02'` must produce the same key (with partition
  parameterization enabled), but different keys with exact matching.
- **Payload independence** — queries with different SELECT columns but identical
  predicates, joins, and group-by keys must produce the same control-only key.
- **INNER join commutativity** — swapped join sides must produce the same key.
- **UNION ALL commutativity** — reordered children must produce the same key.

### HBO Round-Trip Tests

Verify that recording and retrieval work end-to-end:

- Record observed cardinality for a plan, then retrieve it for the same plan
  and verify the historical cardinality is returned.
- Record for one partition value, retrieve for a different partition value,
  and verify the match (with partition parameterization).
- Record for a query with predicates A and B, retrieve for a query with
  predicates A, B, and C, and verify predicate relaxation finds the subset
  match.

## Open Questions

1. **Per-column constraints.** Operators above a matched subtree need per-column
   constraints — NDV, null fraction, min/max — to estimate their own
   cardinalities (see [CardinalityEstimation.md](CardinalityEstimation.md)).
   For example, a `GROUP BY status` above a matched scan needs `ndv(status)`;
   a `JOIN ON cust_id = id` needs `ndv(cust_id)` and `nullFraction(cust_id)`.
   Without these, the optimizer falls back to base table statistics, losing
   some of HBO's accuracy benefit. Per-column stats (NDV, null fraction) are
   not free to collect at execution time (unlike cardinality, which is
   available from task execution stats).

   **How Presto handles this:** Presto does NOT store per-column stats in
   history either. When HBO provides historical cardinality for a matched
   subtree, Presto re-derives per-column constraints using the optimizer's
   normal constraint propagation rules (filter narrowing, join NDV scaling,
   etc.) with the historical cardinality substituted in. This is pragmatic:
   per-column stats remain consistent with the optimizer's model, and
   historical cardinality provides a better starting point than table-level
   estimates.

   We can follow the same approach. Options for further improvement:
   - Collect only for a sample of queries
   - Collect only for columns that appear as join keys or group-by keys
   - Derive from the optimizer's own constraint propagation at record time
     (these are estimates, not actuals, but may be better than base table stats
     for correlated data)

2. **Range predicates:** Should `x > 100` ever be parameterized? (Presto keeps
   as-is)

3. **Partial HBO and mixed statistics.** When HBO provides accurate historical
   cardinalities for some operators but the optimizer relies on estimates for
   others, the mix could in theory distort relative cost comparisons — the
   corrected plan may look disproportionately expensive relative to an
   uncorrected alternative with an underestimate.

   This is expected to be largely **self-correcting** for recurring queries:
   after executing the chosen plan, accurate stats are recorded for its
   operators. On the next run, the optimizer has accurate data for the
   previously chosen plan and may now prefer an alternative — at which point
   that alternative's stats are recorded too. However, for queries with many
   joins, the number of candidate plans is combinatorial and full history
   coverage may take many executions.

   The remaining risk is when the chosen plan **fails** (OOM, timeout) before
   stats can be recorded. In that case, the self-correcting loop is broken —
   the same bad choice repeats. A mitigation is to collect and record partial
   stats even from failed executions, so that subsequent runs have better data
   to work with.

4. **Write-path optimization.** The current design focuses on read-path
   optimization (cardinality estimation for query plan selection). For write
   queries (INSERT, CTAS), historical stats from the TableWrite operator
   (output file count, file sizes, number of writers) could inform write-path
   decisions such as scaled writer optimization. The HBO infrastructure (key
   generation, storage, retrieval) could be reused for this purpose.

## Reference: Presto HBO

### Key Generation Pipeline

Presto generates canonical keys through a three-stage pipeline:

1. **Canonicalization** (`CanonicalPlanGenerator`) — Traverses the plan tree
   and creates a canonical version of each node. Variables are renamed to
   positional names. Expressions are inlined and canonicalized via
   `CanonicalRowExpressionRewriter`.

2. **JSON Serialization** — The canonical plan is serialized to JSON with
   deterministic settings (sorted map keys, alphabetical properties).

3. **SHA256 Hashing** (`CachingPlanCanonicalInfoProvider`) — The JSON string
   is hashed to produce a fixed-size key for storage and lookup.

### Expression Encoding

Presto serializes the **entire plan node** to JSON, including all expressions
and output variables. Each expression type is handled:

- **Column references** — Renamed to canonical variable names during step 1
- **Literals** — Serialized as-is (JSON representation)
- **Function calls** — Full expression tree: function name, arguments, return
  type, recursively
- **Join keys** — Sorted lexicographically (`left.compareTo(right)`) to
  normalize `cust_id = id` vs `id = cust_id`
- **Join/filter predicates** — Sorted by their JSON string representation
- **VALUES content** — Full row content serialized and included
- **Commutative ops** — Handled by `CanonicalRowExpressionRewriter`

### Column Naming

Presto's `CanonicalPlanGenerator` renames all variables to deterministic
canonical names using a two-step approach:

1. **Table columns** get names derived from `ColumnHandle.toString()` — a
   connector-specific string that identifies the column in the source system
   (e.g., `hive:orders:region`). This is stable across SQL alias changes.

2. **Computed columns** get names derived from their expression structure.
   A sequential counter resolves collisions (`col`, `col_0`, `col_1`).
   Deterministic sorting ensures the counter increments identically for
   structurally identical plans.

**Limitations.** Presto's naming guarantees that *structurally identical* plans
produce identical keys. But it does NOT provide stability across plan changes —
if you add a column to SELECT, the plan structure changes, the counter
increments differently, and canonical names shift. Presto mitigates this by
keying per subtree rather than per query, so unchanged subtrees keep their
keys.

| Aspect | Presto |
|--------|--------|
| Table columns | `ColumnHandle.toString()` |
| Computed columns | Expression structure + collision counter |
| Stability across identical plans | ✅ |
| Stability across minor query changes | ❌ (counter shifts) |

### Canonicalization Strategies

Presto defines four canonicalization strategies with increasing levels of
generalization. Each strategy controls which constants are replaced with
placeholders before hashing:

| Strategy | What it parameterizes | Example |
|----------|----------------------|---------|
| `DEFAULT` | Nothing — exact match only | `WHERE id = 1` ≠ `WHERE id = 2` |
| `CONNECTOR` | Partition column predicates (connector-aware) | `WHERE ds = '2024-01-01'` ≡ `WHERE ds = '2024-01-02'` (if `ds` is partitioned) |
| `IGNORE_SAFE_CONSTANTS` | Above + constants in Project | `SELECT x / 2` ≡ `SELECT x / 4`; `WHERE x > 100` ≠ `WHERE x > 200` |
| `IGNORE_SCAN_CONSTANTS` | Above + all scan predicate constants | `WHERE id = 1` ≡ `WHERE id = 1000` (even non-partition columns) |

**Limitation of `IGNORE_SAFE_CONSTANTS`.** Presto assumes constants in Project
don't affect cardinality. This is incorrect when a projected expression flows
into a predicate above. For example, `Project: x = a + 10` followed by
`Filter: x > 0` is effectively `a > -10`. Replacing `10` with `?` loses this
relationship — `a + 1000` filtered by `x > 0` (i.e., `a > -1000`) would match
the same key despite very different cardinality.

**Multi-strategy fallback.** HBO tries strategies in accuracy order (most
accurate first). For each plan node, it generates keys for all configured
strategies and looks up the most accurate match first, falling back to
looser strategies only if no match is found. This balances precision
(prefer exact matches) with coverage (fall back to approximate matches).

**Relevance to Axiom.** Our design already incorporates the key ideas:

- **Partition parameterization** — equivalent to `CONNECTOR` / `IGNORE_SCAN_CONSTANTS`
  for partition columns. We replace partition literals with `?`.
- **Payload exclusion** — subsumes `IGNORE_SAFE_CONSTANTS` for Project
  constants, since payload columns (including projected expressions) are
  excluded from the control-only key entirely.
- **Predicate relaxation** — addresses a different dimension than Presto's
  `IGNORE_SCAN_CONSTANTS`. Presto parameterizes predicate constants (replacing
  values with `?`), treating `WHERE x > 100` and `WHERE x > 1000` as
  equivalent. Axiom keeps predicate values in the key but tries removing entire
  predicates on miss, applying estimated selectivity for the unmatched ones.
  This is more conservative (Axiom doesn't equate different filter values) but
  broader (Axiom can match a subset of predicates).

  Conversely, Presto's `IGNORE_SAFE_CONSTANTS` gives broader full-key matching
  for Project constants: `SELECT a + 10` and `SELECT a + 1` share the same
  Presto key (constant replaced with `?`), reusing both cardinality and data
  size. In Axiom, the control-only key matches these (payload excluded), but
  the full keys differ — so data size is not reused. This is a tradeoff:
  Presto matches more broadly on projection constants; Axiom matches more
  broadly on predicate subsets.

The multi-strategy fallback pattern is worth considering if we find that
exact-match hit rates are too low in practice.

**Connection to dual-key.** Our dual-key approach (full key → control-only key)
follows the same pattern: try the most specific key first, fall back to a
broader key on miss. Presto relaxes along the **predicate constant** dimension
(replace values with `?`); our dual-key relaxes along the **payload column**
dimension (drop SELECT/aggregate columns). Our predicate relaxation (removing
entire predicates on miss) is a third dimension of fallback. All three
dimensions could be composed for maximum coverage.

### Evaluation and Comparison

| Feature | Presto | Axiom (Proposed) |
|---------|--------|------------------|
| Partition parameterization | ✅ | ✅ |
| Sub-tree reuse | ✅ | ✅ |
| Payload exclusion | ❌ (output variables, aggregates included in key) | ✅ (control-only key excludes payload) |
| Output data size | ✅ (key includes payload → stored size is exact) | ✅ via dual-key (full key stores exact size; control-only key stores cardinality only) |
| Predicate relaxation | ❌ (SHA256 hash is atomic → cannot remove predicates from a hash) | ✅ (on miss, generate candidate keys with fewer predicates and look up each) |
| Column naming stability | ❌ across plan changes (counter shifts) | ✅ (bottom-up hash IDs are local) |
| Join-specific stats | ✅ NULL key counts, total key counts | ✅ NULL key counts |
| Aggregation stats | ✅ Input/output cardinalities | ✅ Output cardinality (input cardinality = child's output) |
| Persistence | ZippyDB | Pluggable: file-based for testing, remote for prod |

### How Our Design Diverges

**Payload exclusion.** The primary design difference is **what goes into the
key**. Presto includes everything — output variables, aggregate functions,
projected columns — in the canonical plan before hashing. Our design excludes
payload (SELECT columns, aggregate functions) from the control-only key. This
is orthogonal to the key format: we could hash the key and still exclude
payload. The benefit is broader history reuse across queries with different
projections. The dual-key approach recovers Presto's advantage for output data
size: the full key (including payload) stores exact data size for queries
with matching projections.

**Predicate relaxation.** Presto hashes the entire canonical plan with SHA256,
producing an atomic key. If a query has an additional predicate not seen in
history, the hash is completely different — there is no way to remove predicates
from a hash. Our design generates multiple candidate keys at lookup time
(removing predicates one at a time) and looks up each candidate. This works
with any key format, including hashing — the candidates are generated before
hashing.

**Column naming.** Presto assigns text-based canonical names with a sequential
collision counter, which is stable for identical plans but shifts when the
plan changes. Our bottom-up hash ID approach assigns each column a hash
based on its semantic identity (schema column or defining expression),
making it immune to changes in unrelated parts of the query.

The key format itself (text vs hash) is an implementation detail. We can
store both a hash (for efficient lookups) and text (for debugging) if needed.
