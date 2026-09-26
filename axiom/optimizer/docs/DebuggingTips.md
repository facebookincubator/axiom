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

The `EXPLAIN` command supports several output types via `EXPLAIN (TYPE <type>) <query>`:

| Command | Output | Description |
|---------|--------|-------------|
| `EXPLAIN <query>` | Velox plan | The default output — a distributed execution plan with Velox operators. Same as `EXPLAIN (TYPE EXECUTABLE)` |
| `EXPLAIN (TYPE LOGICAL) <query>` | Logical plan | The unoptimized logical plan tree (the output of `PrestoParser`) |
| `EXPLAIN (TYPE GRAPH) <query>` | Query graph | The output of `ToGraph` — parsed query structure with tables, joins, filters, and aggregations |
| `EXPLAIN (TYPE OPTIMIZED) <query>` | Physical plan | The output of `Optimization::bestPlan` — the optimized logical plan before translation to Velox |
| `EXPLAIN (TYPE EXECUTABLE) <query>` | Velox plan | The distributed execution plan with Velox operators (same as default `EXPLAIN`) |
| `EXPLAIN (TYPE DISTRIBUTED) <query>` | Velox plan | Alias for `EXECUTABLE` |
| `EXPLAIN ANALYZE <query>` | Velox plan + stats | Runs the query and shows the execution plan with runtime statistics (row counts, timings, custom operator stats). Use `--debug` flag for custom operator stats |

The types correspond to stages of the query compilation pipeline:
1. **LOGICAL** — SQL parsed into a logical plan tree
2. **GRAPH** — Logical plan converted into a query graph (input to the optimizer)
3. **OPTIMIZED** — Optimizer produces the best physical plan
4. **EXECUTABLE** — Physical plan translated to Velox operators for execution

For the v2 pass order and the `last_pass` setting used to inspect intermediate
IR, see [Optimizer v2 Pass Cheat Sheet](../v2/docs/OptimizerPasses.md).

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

To view the query graph instead:

```bash
buck run axiom/cli:cli -- \
  --data_path /home/$USER/tpch/sf0.1/ \
  --num-workers 1 \
  --num-drivers 1 \
  --query "EXPLAIN (type graph) $(cat axiom/optimizer/tests/tpch/queries/q5.sql)"
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
buck test axiom/optimizer/tests:tpch_plan -- q5 --print-passing-details
```

**Warning:** Remove all debug logging before committing!

## 3. TPC-H Query Tests

`TpchPlanTest` tests all 22 TPC-H queries. For each query, the test verifies:

1. **Query results** — `checkTpchSql(n)` executes the query and compares results against a reference Velox plan
2. **Single-node plan shape** — `AXIOM_ASSERT_PLAN(plan, matcher)` validates the optimized plan structure matches expected patterns
3. **Multi-node plan generation** — `ASSERT_NO_THROW(planVelox(parseTpchSql(n)))` confirms the optimizer can successfully generate a distributed plan

### Running TPC-H tests

```bash
# Run all 22 TPC-H query tests
buck test axiom/optimizer/tests:tpch_plan

# Run a specific query test
buck test axiom/optimizer/tests:tpch_plan -- q5
```

### Regenerating plan files

The `tpch/plans` directory is checked into the repo and contains a snapshot of the TPC-H single-node plans. These files are useful for reviewing expected plan shapes, but they are not used directly by the tests.

`DISABLED_makePlans` generates files in the `tpch/plans` directory.
Each `.plans` file contains:

- **Query graph** — `DerivedTable::toString()` output showing the parsed query structure with tables, joins, filters, and aggregations
- **Optimized plan (oneline)** — `RelationOp::toOneline()` output, a compact one-liner showing the join tree structure
- **Optimized plan** — `RelationOp::toString()` output, the full optimized logical plan with all operators
- **Executable Velox plan** — `MultiFragmentPlan::toString()` output, the distributed execution plan with Velox operators

To regenerate all plan files after changing the optimizer, run from the `fbcode` directory:

```bash
cd fbcode
buck run axiom/optimizer/tests:tpch_plan -- \
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
