# Debugging Tips

Tips for debugging the Axiom optimizer.

## 1. Using the CLI to Get a Query Plan

Use the `axiom_sql` CLI to run queries against TPC-H data and view their plans.

**Buck (Meta-internal):**

```bash
buck run axiom/cli:cli -- \
  --data_path /home/$USER/tpch/sf0.1/ \
  --num-workers 1 \
  --num-drivers 1 \
  --debug \
  --query "EXPLAIN SELECT * FROM lineitem LIMIT 10"
```

**CMake (OSS):**

```bash
./build/axiom/cli/axiom_sql \
  --data_path /home/$USER/tpch/sf0.1/ \
  --num-workers 1 \
  --num-drivers 1 \
  --debug \
  --query "EXPLAIN SELECT * FROM lineitem LIMIT 10"
```

> **Note:** The remaining examples in this document use Buck commands. For CMake
> builds, replace `buck run <target> --` with the binary path under your build
> directory (e.g., `./build/axiom/cli/axiom_tpchgen`) and `buck test <target>`
> with `ctest` or the test binary directly.

### Key flags

| Flag | Description |
|------|-------------|
| `--data_path` | Path to TPC-H data directory |
| `--data_format` | Data format: `parquet` or `dwrf` (default: `parquet`) |
| `--num-workers` | Number of workers (use 1 for single-node plans) |
| `--num-drivers` | Number of drivers per worker (use 1 for single-threaded plans) |
| `--debug` | Enable glog output to stderr (disabled by default) |
| `--query` | SQL query to execute (use `EXPLAIN` prefix to see the plan). Multiple queries can be separated by `;` |

### EXPLAIN variants

The CLI uses the v2 optimizer by default; `--v1` selects v1. `EXPLAIN (TYPE
<type>) <query>` shows the query at a stage of planning:

| Command | Output |
|---------|--------|
| `EXPLAIN (TYPE LOGICAL) <query>` | The logical plan the parser produces, before optimization |
| `EXPLAIN (TYPE OPTIMIZED) <query>` | The optimizer's Node IR after its last pass, `PLAN_PHYSICAL`, with estimates |
| `EXPLAIN (TYPE OPTIMIZED WITH (last_pass = '<pass>')) <query>` | The Node IR after the named pass |
| `EXPLAIN <query>` | The distributed Velox plan. Same as `EXPLAIN (TYPE EXECUTABLE)` and `EXPLAIN (TYPE DISTRIBUTED)` |
| `EXPLAIN (TYPE IO) <query>` | JSON listing the tables the query reads and writes, with constraints on their partition columns |
| `EXPLAIN (TYPE VALIDATE) <query>` | Succeeds when the query parses and resolves |
| `EXPLAIN ANALYZE <query>` | Runs the query and shows the Velox plan with runtime statistics (row counts, timings, custom operator stats). Use `--debug` for custom operator stats |

The v2 passes, in the order `Optimizer::planTo` (`v2/Optimize.cpp`) runs them:

1. `TRANSLATE`
2. `DECORRELATE`
3. `LIMIT_AND_ORDER`
4. `PUSHDOWN_AND_PRUNE`
5. `FOLD_METADATA_AGGREGATE`
6. `CONNECTOR_PUSHDOWN`
7. `ESTIMATE_LEAF_STATS` — estimates appear from here on.
8. `PLAN_PHYSICAL`

To find the pass that introduced a node or rewrite, compare the IR after
adjacent passes. [Optimizer v2 Pass Cheat Sheet](OptimizerPasses.md) describes
what each pass does.

Under v1:

- `TYPE OPTIMIZED` prints the output of `Optimization::bestPlan`.
- `TYPE GRAPH` prints the query graph `ToGraph` builds. v2 rejects it.
- `last_pass` is not supported.

### Plan summary

`EXPLAIN (TYPE EXECUTABLE WITH (detail = 'summary')) <query>` prints the shape
of the distributed plan instead of its operators: one indented tree of its
scans, joins, aggregations, exchanges and other control nodes, root on top. At
an exchange the tree continues into the fragment it reads.

```
Gather F2 (FIXED, 4 workers) → F1
  Agg (FINAL) 1 key
      Estimate: 25 rows
    Shuffle F3 (SOURCE) → F2
        Estimate: 25 rows
      Agg (PARTIAL) 1 key
          Estimate: 25 rows
        HashJoin (INNER) 1 key
            Estimate: 15,000 rows
          Scan customer
              Estimate: 15,000 rows
          Broadcast F4 (SOURCE) → F3
              Estimate: 25 rows
            Scan nation
                Estimate: 25 rows
```

This is `SELECT n_name, count(*) FROM customer, nation WHERE c_nationkey =
n_nationkey GROUP BY n_name` on TPC-H scale factor 0.1 with `--num_workers 4`.
`estimates = 'false'` leaves out the estimate lines.

`FORMAT JSON` gives the same nodes per fragment, with where each fragment's
output goes:

```json
{
  "fragments": [
    {
      "id": 2,
      "type": "SOURCE",
      "tree": [
        {
          "kind": "Scan",
          "nodeId": "0",
          "table": "lineitem",
          "estimate": {
            "rawInputRows": 600572,
            "rawInputBytesPerRow": 16,
            "splits": 1,
            "outputRows": 490263,
            "outputBytesPerRow": 16
          }
        }
      ],
      "output": {"nodeId": "1", "consumerFragmentId": 1}
    },
    {
      "id": 1,
      "type": "SINGLE",
      "tree": [
        {
          "kind": "Exchange",
          "nodeId": "2",
          "distribution": "gather",
          "producerFragmentId": 2
        }
      ]
    }
  ]
}
```

This is the output for `SELECT l_orderkey, l_quantity FROM lineitem WHERE
l_quantity > 10` on the same data, reformatted for reading. The fields are
defined in `MultiFragmentPlanPrinter::toSummaryJson`
(`axiom/optimizer/MultiFragmentPlanPrinter.h`).

### TPC-H Data Directories

Two pre-generated TPC-H datasets are available:

| Path | Scale Factor | Size | Use Case |
|------|--------------|------|----------|
| `/home/$USER/tpch/sf0.1/` | 0.1 | ~60K lineitem rows | Fast iteration, debugging |
| `/home/$USER/tpch/sf1/` | 1.0 | ~6M lineitem rows | Larger-scale testing |

Tests use `sf0.1` because they regenerate data on the fly and cannot
afford larger scales.  Note that query plans may differ across scale
factors because the optimizer uses data statistics (row counts,
cardinalities) that change with scale.

### Generating TPC-H Data

If you don't have TPC-H data, generate it using the `axiom/cli:tpchgen` CLI:

```bash
buck run axiom/cli:tpchgen -- \
  --data_path /home/$USER/tpch/sf0.1 --sf 0.1
```

To generate data in DWRF format:

```bash
buck run axiom/cli:tpchgen -- \
  --data_path /home/$USER/tpch/sf0.1-dwrf --sf 0.1 \
  --data_format dwrf --compression zstd
```

| Flag | Default | Description |
|------|---------|-------------|
| `--data_path` | (required) | Output directory for TPC-H data |
| `--sf` | `0.1` | TPC-H scale factor (e.g., 0.1, 1, 10) |
| `--data_format` | `parquet` | Data format: `parquet` or `dwrf` |
| `--compression` | `none` | Compression: `none`, `snappy`, `zlib`, `zstd`, `lz4`, `gzip`. Not all options work with all formats, e.g. DWRF doesn't support `snappy`. |

**Note:** When querying data generated with a non-default `--data_format`,
specify the same format in the query CLI. For example, to query DWRF data:

```bash
buck run axiom/cli:cli -- \
  --data_path /home/$USER/tpch/sf0.1-dwrf \
  --data_format dwrf \
  --query "SELECT count(*) FROM lineitem"
```

### Example: View TPC-H q5 plan

```bash
buck run axiom/cli:cli -- \
  --data_path /home/$USER/tpch/sf0.1/ \
  --num-workers 1 \
  --num-drivers 1 \
  --query "EXPLAIN $(cat axiom/optimizer/tests/tpch/queries/q5.sql)"
```

To view the optimizer's IR after a pass instead:

```bash
buck run axiom/cli:cli -- \
  --data_path /home/$USER/tpch/sf0.1/ \
  --num-workers 1 \
  --num-drivers 1 \
  --query "EXPLAIN (TYPE OPTIMIZED WITH (last_pass = 'pushdown_and_prune')) $(cat axiom/optimizer/tests/tpch/queries/q5.sql)"
```

## 2. Adding Debug Logging

For temporary debugging, use `LOG(ERROR)` to print diagnostic
information:

```cpp
LOG(ERROR) << "reducingJoins: fanout=" << fanout
           << " existences.size()=" << existences.size();
```

`LOG(ERROR)` is used instead of `LOG(INFO)` because error-level logs
are always visible regardless of logging configuration.

**Note:** By default, `buck test` only shows logs for failing tests.
To see logs for passing tests, use `--print-passing-details`:

```bash
buck test axiom/optimizer/v2/tests:tpch_plan -- q05 --print-passing-details
```

**Warning:** Remove all debug logging before committing!

### Printing the IR after each pass

To see how the v2 optimizer transforms a test query, set `afterPass_` in a
test built on `QueryTestBase`. Every `planVelox` call in that test, including
the ones behind `toSingleNodePlan`, then calls it after each pass that runs:

```cpp
afterPass_ = [](v2::Optimizer::Pass pass, v2::NodeCP root) {
  LOG(ERROR) << "After " << v2::Optimizer::toName(pass) << ":\n"
             << root->toString();
};
```

Run the v2 instance with `--print-passing-details` to see the output. A pass
that is skipped, such as `CONNECTOR_PUSHDOWN` when no connector supports
pushdown, is not reported. This is the test-side counterpart of
`EXPLAIN (TYPE OPTIMIZED WITH (last_pass = '<pass>'))`, and it shows every
stage in one run.

## 3. TPC-H Query Tests

Two tests cover the 22 TPC-H queries:

- **`axiom/optimizer/v2/tests:tpch_plan`** asserts the single-node plan shape
  the v2 optimizer produces. It injects statistics through
  `TestConnector::addTpchTables`, so it generates no data and plans at any
  scale instantly. Queries whose v2 plan does not yet match v1's assert only
  that planning succeeds; `TpchV1V2PlanComparison.md` tracks the gaps.
- **`axiom/optimizer/tests:tpch_result`** runs each query on generated data
  under both optimizers and compares the results with a reference plan.

### Running TPC-H tests

```bash
# Plan shapes under v2.
buck test axiom/optimizer/v2/tests:tpch_plan

# One query.
buck test axiom/optimizer/v2/tests:tpch_plan -- q05

# Results under both optimizers.
buck test axiom/optimizer/tests:tpch_result
```

### v1 plan tests and snapshots

`axiom/optimizer/v1/tests:tpch_plan` asserts the v1 plan shapes. Its
`DISABLED_makePlans` regenerates the v1 plan snapshots in the `tpch/plans`
directory, which are for reviewing plan shapes and are not used by the tests.
Each `.plans` file contains:

- **Query graph** — `DerivedTable::toString()` output showing the parsed query structure with tables, joins, filters, and aggregations
- **Optimized plan (oneline)** — `RelationOp::toOneline()` output, a compact one-liner showing the join tree structure
- **Optimized plan** — `RelationOp::toString()` output, the full optimized logical plan with all operators
- **Executable Velox plan** — `MultiFragmentPlan::toString()` output, the distributed execution plan with Velox operators

To regenerate them, run from the `fbcode` directory:

```bash
cd fbcode
buck run axiom/optimizer/v1/tests:tpch_plan -- \
  --gtest_filter="TpchPlanTest.DISABLED_makePlans" \
  --gtest_also_run_disabled_tests
```

## 4. Using the CLI with Custom Tables

Use `--init` to create in-memory tables with specific stats for testing
optimizer behavior outside of TPC-H:

```bash
buck run fbcode//axiom/cli:cli -- \
  --num_workers 1 --num_drivers 1 \
  --init /path/to/init.sql \
  --query "EXPLAIN SELECT ..."
```

Example init file:
```sql
use test.default;
create table t as select * from unnest(sequence(1, 100), sequence(1, 100)) as t(a, b);
create table u as select * from unnest(sequence(1, 10000), sequence(1, 10000)) as t(x, y);
```

Use `--num_workers 1 --num_drivers 1` to produce single-node plans matching
`toSingleNodePlan` in C++ tests. Without these flags, the CLI produces
distributed multi-fragment plans with `LocalPartition` nodes.
