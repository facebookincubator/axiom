# Cardinality Estimation

This document describes how output cardinality is estimated for relational
operators in the Axiom optimizer. Cardinality estimates drive cost-based
optimization decisions such as join ordering and operator placement.

Accurate cardinality estimation depends on column statistics that are
propagated through the plan as constraints. Initial constraints come from
the connector (e.g., table row count, per-column distinct value counts,
null fractions, and min/max ranges from the metastore). Each operator
reads input constraints to estimate its output cardinality, then updates
constraints for downstream operators. For example, a filter narrows the
range and scales down ndv; a downstream join then uses these refined
statistics rather than the original base table statistics.

Column statistics are represented as `Value` objects in each operator's
constraint map (`constraints_`):

- **cardinality**: Number of distinct non-null values (ndv)
- **nullFraction**: Fraction of values that are NULL
- **min**, **max**: Value range bounds
- **trueFraction**: Fraction of values that are TRUE (for boolean columns)

Output cardinality is represented through the `Cost` struct:

- **inputCardinality**: Number of input rows (1 for leaf nodes, otherwise
  the result cardinality of the input operator)
- **fanout**: Ratio of output rows to input rows
- **resultCardinality**: `max(1, fanout × inputCardinality)`

For leaf nodes (e.g., TableScan), `inputCardinality` is 1 and `fanout`
equals the actual row count. For non-leaf nodes, `fanout` represents the
change in cardinality.

## Documentation Map

| Document | Description |
|----------|-------------|
| **CardinalityEstimation.md** (this doc) | Overview of cardinality and constraint estimation for all operators |
| [JoinEstimation.md](JoinEstimation.md) | Detailed join cardinality and constraint propagation for all join types, with worked examples |
| [JoinEstimationQuickRef.md](JoinEstimationQuickRef.md) | Compact cheat-sheet of all join formulas and constraint tables |
| [FilterSelectivity.md](FilterSelectivity.md) | How filter selectivity is estimated from expressions (comparisons, logical operators, IN, IS NULL) |
| [PayloadNdvScaling.md](PayloadNdvScaling.md) | Why the coupon collector formula is preferred over linear scaling for NDV estimation |
| [JoinPlanning.md](JoinPlanning.md) | Join order enumeration: control flow, backtracking, and state management |

**Reading order:** Start here for the overall picture, then
JoinEstimation.md for the most complex operator. Use
JoinEstimationQuickRef.md as a desk reference while reading code.
FilterSelectivity.md and PayloadNdvScaling.md are deep dives into
specific topics.

## Architecture

Cardinality estimation happens in two phases:

**Query graph construction (ToGraph)** builds the query graph from the
parsed SQL. During this phase:
- Base table filter selectivities are estimated (optionally via sampling).
  See [Sampling](#sampling) for details.
- Join edges are created and their raw fanouts are estimated via
  `guessFanout` (using index statistics, sampling, or constraint-based
  estimates). See [Fanout Estimation](JoinEstimation.md#fanout-estimation)
  for details.

**Plan optimization (Optimization)** enumerates join orders and constructs
the operator tree. Each operator computes its own fanout during construction
from input constraints (e.g., Filter computes selectivity, Aggregation
estimates group count, Limit divides limit by input row count). Join
operators receive raw fanouts from the join edges computed during ToGraph
and apply join-type adjustment and filter selectivity internally.

Each operator stores its output constraints in a constraint map
(`constraints_`), as described above.

**Known issue:** Base table filter selectivities are estimated during
ToGraph, before the full query graph is constructed and all filters have
been pushed down to base tables. This means the selectivity estimate may
not account for all applicable filters, leading to less accurate cardinality
estimates.

## Viewing Estimates

At the end of query graph construction, the optimizer builds an initial plan
for each derived table (DT). This initial plan provides cardinality estimates
for the DT and constraints for its output columns.

The `EXPLAIN (TYPE GRAPH)` command shows cardinality estimates and join fanouts
for a query plan. This is useful for understanding how the optimizer estimates
row counts and for diagnosing unexpected join orders or performance issues.

## Operator-Specific Estimation

| Operator | Cardinality | Constraints |
|----------|-------------|-------------|
| **TableScan** | `table.cardinality × filterSelectivity` | Base table statistics; narrowed by pushed-down filters |
| **Filter** | Reduces by selectivity | Filtered column: range narrowed, ndv scaled by predicate; other columns: `sampledNdv(c, selectivity)` |
| **Project** | Neutral | Adds computed columns with derived constraints |
| **Join** | Fanout from join edge; depends on type | See [JoinEstimation.md](JoinEstimation.md) |
| **Aggregation** | Reduces to group count | Keys clamped to group count; aggregates use own Value |
| **Limit** | Reduces to `limit` rows | Cardinalities scaled by `sampledNdv(c, limit / \|input\|)` |
| **OrderBy** | Neutral; reduces to `limit` with LIMIT | With LIMIT, cardinalities scaled by `sampledNdv` |
| **UnionAll** | Sum of inputs | Cardinalities summed, null fractions averaged, ranges unioned |
| **Values** | Literal row count | From literal values |
| **Unnest** | Expands by heuristic fanout of 10 | Pass-through |
| **AssignUniqueId** | Neutral | Adds unique ID column (ndv = rowCount, no nulls) |

All other operators are cardinality-neutral (`fanout = 1`) and pass
constraints through unchanged.

### TableScan

For a table scan with filters:

```
cardinality = table.cardinality × filterSelectivity
```

Where:
- `table.cardinality` is the total row count from table metadata
- `filterSelectivity` is computed using filter selectivity estimation
  (see [FilterSelectivity.md](FilterSelectivity.md))

For index lookups with keys, the fanout is provided by the join edge planning.

### Filter

Filter cardinality uses constraint-based selectivity estimation:

```
fanout = selectivity.trueFraction
```

The filter selectivity is computed by analyzing the filter expressions as
described in [FilterSelectivity.md](FilterSelectivity.md).

Constraints: for the filtered column, cardinality and range are narrowed based
on the predicate (e.g., a range predicate narrows the range and scales ndv
proportionally). For other columns, under the independence assumption, the
filter acts like random sampling with fraction s = selectivity, so
cardinalities are scaled by `sampledNdv(c, selectivity)`.

### Join

Join cardinality depends on the join type (inner, left, right, full, semi,
anti) and the raw fanout from join edge estimation. The Join operator
takes two raw fanout parameters (`fanout` and `rlFanout`), applies filter
selectivity and join-type adjustment internally, and computes the final
output cardinality.

See [JoinEstimation.md](JoinEstimation.md) for full details on fanout
computation, join-type adjustment, join edge fanout calculation, and
constraint propagation for all join types. See
[JoinEstimationQuickRef.md](JoinEstimationQuickRef.md) for a compact
cheat-sheet.

### Aggregation

Aggregation cardinality depends on whether there are grouping keys.

**Global aggregation** (no grouping keys) produces exactly one output row:

```
fanout = 1 / inputCardinality
```

**Grouped aggregation** estimates the number of output groups from key
cardinalities:

```
fanout = min(inputCardinality, numGroups) / inputCardinality
```

For each grouping key, the cardinality is looked up from the input
operator's constraint map.

Single key from one table:
```
groupCardinality = min(key.cardinality, table.cardinality)
```

Multiple keys from the same table:
```
groupCardinality = saturatingProduct(table.cardinality, [key1.card, key2.card, ...])
```

Keys from multiple tables:
```
combinedMax = max(3 × maxTableCardinality, 1e10)
numGroups = saturatingProduct(combinedMax, [group1.card, group2.card, ...])
```

The **saturating product** prevents unrealistic cardinality explosion:
`saturatingProduct(max, [n1, n2, ...]) = max × P / (max + P)` where
`P = n1 × n2 × ...`. This behaves like multiplication when far from max,
but asymptotically approaches max as the product increases.

**Partial aggregation** accounts for expected reduction using the coupon
collector formula:

```
initialDistincts = d × (1 - (1 - 1/d)^n)
partialFanout = initialDistincts / inputCardinality
```

where `d` is total distinct values (numGroups) and `n` is the number of
input rows. If partial aggregation doesn't reduce sufficiently (below a
configured minimum reduction threshold), it's treated as pass-through with
`fanout = 1`.

Constraints: key cardinalities clamped to group count; aggregate columns use
their own Value (since aggregates produce new values).

### Limit

Limit reduces output to at most `limit` rows:

```
if inputCardinality <= limit:
    fanout = 1
else:
    fanout = limit / inputCardinality
```

Constraints: cardinalities scaled by `sampledNdv(c, limit / |input|)` —
keeping N of M rows is equivalent to sampling with fraction `s = N/M`.
NullFraction and range are unchanged.

### OrderBy

OrderBy is cardinality-neutral unless it has a LIMIT clause:

```
if limit == -1 (no limit):
    fanout = 1
else if inputCardinality <= limit:
    fanout = 1  (limit is no-op)
else:
    fanout = limit / inputCardinality
```

Constraints: without LIMIT, pass-through. With LIMIT, cardinalities scaled
by `sampledNdv(c, limit / |input|)` — keeping N of M rows is equivalent to
sampling with fraction `s = N/M`. NullFraction and range are unchanged.

### UnionAll

UnionAll combines all inputs:

```
inputCardinality = Σ (input[i].inputCardinality × input[i].fanout)
fanout = 1
```

Constraints: cardinalities (NDVs) summed, null fractions weighted-averaged,
ranges unioned across all inputs.

**Why sum NDVs?** For a column present in all inputs, the true output NDV is
the size of the set union: `|S_1 ∪ S_2 ∪ ...| = Σ ndv_i − (overlap)`. This
lies in the range `[max(ndv_i), Σ ndv_i]`. Summing assumes zero overlap, which
follows from the optimizer's global independence assumption: model each input as
drawing distinct values uniformly from a large domain D. The expected output NDV
is `D × (1 − Π(1 − ndv_i / D))`, which simplifies to `Σ ndv_i` when
D >> ndv_i. This is the same independence assumption used for filter
selectivity, join key matching, and aggregation group counts. It is also exact
for the common case where UnionAll combines data from disjoint sources (e.g.,
different partitions or time ranges with non-overlapping keys).

### Values

Values cardinality is the literal count of rows:

```
cardinality = number of rows in the Values data
```

### Unnest

Unnest expands array/map columns into rows:

```
fanout = 10
```

This is a heuristic for average array/map size.

TODO: Compute fanout from unnest expression array size statistics when available.

## Assumptions and Limitations

1. **Independence**: Columns are assumed to be independently distributed unless
   explicitly correlated through join keys.

2. **Uniform distribution**: Values are assumed uniformly distributed within
   their cardinality range.

3. **No skew modeling**: Data skew (hot keys, Zipf distributions) is not
   modeled.

4. **Static statistics**: Estimates are based on base table statistics; runtime
   selectivity changes are not predicted. See [Historical Data](#historical-data)
   for how past execution statistics can mitigate this.

5. **Unnest heuristic**: Array/map unnest uses a hardcoded fanout of 10
   regardless of actual array sizes.

## Sampling

The optimizer can sample actual data to improve cardinality estimates beyond
what constraint-based estimation provides. Both forms of sampling require
connector cooperation — the connector must implement the relevant data access
methods (e.g., `Layout::sample` for filter sampling, table scans for join
sampling).

### Filter Sampling (`sampleFilters`)

When enabled, `VeloxHistory::estimateLeafSelectivity` samples 1% of a base
table's rows to estimate filter selectivity. This applies only to filters
directly on base tables (pushed-down predicates); all other filters use
constraint-based estimation.

The optimizer calls the connector's `Layout::sample` API, passing the table
handle, filters, and sampling percentage (hard-coded to 1%). The connector
reads a sample of rows, applies the filters, and returns a pair of counts: the
total number of rows in the sample (numSampled) and the number of rows that
passed the filters (numMatching).

The selectivity is then:
`filterSelectivity = max(Selectivity::kLikelyTrue, numMatching) / numSampled`.
The `kLikelyTrue` (0.8) floor prevents zero selectivity, which would make all
downstream cardinality estimates zero. If no rows were sampled (empty table),
defaults to 1. Results are cached by table handle string to avoid repeated I/O.

When `sampleFilters` is disabled, the constraint-based estimate is used as
`filterSelectivity`.

### Join Sampling (`sampleJoins`)

When enabled, `VeloxHistory::sampleJoin` estimates join fanout by sampling
actual data from both sides of the join.

A naive approach would be to sample rows randomly from each side and run an
actual join on the samples. This doesn't work well because random sampling on
each side independently is unlikely to produce matching keys — a key present in
the left sample may not appear in the right sample, leading to a severe
underestimate of fanout. Running an actual join on full data would be too
expensive since the optimizer needs fanout estimates to decide join order in the
first place.

Instead, the optimizer uses hash-based sampling: for each row, it hashes the
join keys and keeps the row only if the hash falls within a deterministic range
(`(hash % mod) < limit`). Because the same key always produces the same hash,
matching keys on both sides are guaranteed to fall in the same range — ensuring
they are either both sampled or both excluded.

The process (`JoinSample.cpp`):
- For each side, build a pipeline that scans the base table, hashes join keys
  into a single `int64` value per row, and keeps only rows whose hash falls
  within the sample range (`(hash % mod) < limit`). Both pipelines run in
  parallel.
- Each pipeline produces a frequency map: hash value → occurrence count.
- The fanout from left to right is the average number of right-side matches
  per distinct left-side key (and symmetrically for right to left).

Sample size is controlled by table size and key cardinality (both derived from
column metadata provided by the connector). All rows are always scanned and
hashed, but the hash filter controls what fraction of keys is collected into
the frequency map:
- If both tables have < 10,000 rows, all keys are kept (the filter is a
  no-op).
- If both sides have high key cardinality (> 10,000 distinct values), the
  fraction is chosen to collect roughly 10,000 keys from the smaller table:
  `fraction = max(2, 10,000² / min(leftRows, rightRows))`. The filter then
  keeps keys where `(hash % 10,000) < fraction`.
- Otherwise, sampling is skipped and the optimizer falls back to
  constraint-based estimation.

Results are cached by join edge key to avoid repeated sampling.

## Historical Data

Constraint-based cardinality estimates are notoriously inaccurate in practice.
The independence assumption, uniform distribution, and lack of skew modeling
mean that estimates can be off by orders of magnitude, especially for
multi-join queries where errors compound. Sampling improves accuracy but is
expensive — it requires reading actual data during planning, adding latency
before the query starts executing.

However, many production queries run repeatedly with similar data. The
optimizer can leverage actual execution statistics from past runs to improve
estimates without the cost of sampling at planning time. When historical data
is available, it takes precedence over both constraint-based estimates and
sampling.

## Related Documentation

- See [JoinEstimation.md](JoinEstimation.md) for full derivations of join
  cardinality and constraint propagation.
- See [JoinEstimationQuickRef.md](JoinEstimationQuickRef.md) for a compact
  cheat-sheet of join cardinality and constraint propagation formulas.
- See [PayloadNdvScaling.md](PayloadNdvScaling.md) for why the coupon
  collector formula is preferred over linear scaling.
- See [FilterSelectivity.md](FilterSelectivity.md) for filter selectivity
  estimation details.
