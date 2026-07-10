# Testing

Guidelines and tools for testing the Axiom optimizer: the two testing dimensions (plan optimality and correctness), the **PlanMatcher** framework for verifying plan shape, the **SqlTest** framework for verifying correctness against DuckDB, and how to verify expected plans and review test coverage.

### The Two Testing Dimensions

The optimizer has two goals:

1. **Produce optimal plans** — Plans that avoid redundant work and aim to minimize CPU, memory, and network. (e.g., Did predicate pushdown move the filter below the join? Did column pruning remove unused columns? Is the smaller side of the join used to build the hash table? Are there no redundant shuffles?)
2. **Produce correct plans** — Plans that when executed produce correct results.

These goals map to two different testing frameworks:

| What to verify | Framework | Location |
|---|---|---|
| Plan optimality | **PlanMatcher** in unit tests | `axiom/optimizer/tests/*Test.cpp` |
| Plan correctness | **.sql files** + **SqlTest** | `axiom/optimizer/tests/sql/*.sql` |

**Guideline:** Plan optimality tests go into unit tests using PlanMatcher. Correctness verification goes into `.sql` files run by SqlTest.

Why separate optimality and correctness tests? PlanMatcher tests are precise and tell you exactly which optimization broke. But they don't tell you whether the plan produces the right answer. `.sql` tests verify end-to-end correctness by comparing Axiom's results against DuckDB, but they don't tell you what the plan looks like. You need both.

**Guideline:** Test both single-node and distributed plans. The optimizer may produce different plans depending on the number of workers (e.g., single-node plans skip shuffles, distributed plans split aggregations into partial/final). When relevant, also test single-node multi-threaded plans, as local parallelism introduces local partitioning that can expose different issues.

---

### PlanMatcher: Verifying Plan Shape

PlanMatcher provides a fluent builder API for asserting the structure of an optimized plan. You describe the expected plan from leaf to root (from scan upward), and use `AXIOM_ASSERT_PLAN` to match it against the actual plan. On mismatch, the full plan is printed automatically for debugging.

#### Matching granularity

Matchers can verify as much or as little as needed. Each builder method can be called with no arguments (match node type only) or with arguments (match details like expressions, table names, grouping keys, etc.):

```cpp
// Coarse: just verify the plan shape (node types only)
auto coarse = PlanMatcherBuilder()
    .tableScan()
    .filter()
    .singleAggregation()
    .build();

// Detailed: verify specific expressions and table names
auto detailed = PlanMatcherBuilder()
    .tableScan("orders")
    .filter("o_custkey < 100")
    .singleAggregation({"o_custkey"}, {"sum(o_totalprice) as total"})
    .filter("total > 200.0")
    .build();
```

**How much to check?** There is no hard-and-fast rule here, but here's a way to think about it. Match what the test is about. If you're testing predicate pushdown, verify the filter expressions and their position in the plan, but you don't need to spell out every aggregation detail. If you're testing join reordering, verify the join structure and which table is on the build side. Over-specifying makes tests brittle and hard to read — they break when unrelated optimizations change and it's difficult to see what the test is actually verifying. Under-specifying makes tests weak — they pass even when the optimization is wrong. Too-narrow tests have their own problem: you need a lot of them, and the sheer volume makes it hard to reason about what's actually covered.

Node-only matching (no arguments) should be reserved for local development and debugging. Checked-in tests should verify the details relevant to the optimization being tested.

#### Test comments

Test comments should describe **what behavior the test verifies**, not the history that led to writing it or how the optimizer happens to implement it today.

```cpp
// ❌ Avoid — historical / implementation detail.
// Regression: outer filter pushed into a UNION DISTINCT leg that becomes
// zero-rows previously failed in checkConsistency with "Size mismatch
// between joinOrder and tables" (clearState reset tables but not
// joinOrder, leaving stale entries when the leg DT was reused).
TEST_F(SetTest, ...) { ... }

// ✅ Prefer — describe the behavior under test.
// UNION DISTINCT with an outer filter that pushes into the legs and
// reduces one to zero rows: dedup still applies to the surviving leg.
TEST_F(SetTest, ...) { ... }
```

The bug story rots: original failure modes change, internal class names get renamed, line numbers shift. The behavior the test verifies stays stable — that's what future readers (and you, six months later) need to understand the test.

The bug story belongs in the commit message and the diff/PR description, where readers know to look for historical context. Don't duplicate it into the test code.

#### Alias propagation

The optimizer generates internal column names (e.g., `dt3.__p12`) that are implementation details subject to change without notice. PlanMatcher handles this via **alias capture and propagation**: you assign test-defined aliases in aggregations and projects, then reference them in subsequent matchers. The test fully controls these names, making them independent of optimizer internals. The framework rewrites your aliases to actual column names during matching.

```cpp
auto matcher = PlanMatcherBuilder()
    .tableScan()
    .singleAggregation({}, {"count(*) as c"})  // captures alias 'c'
    .filter("c > 0")                           // 'c' is rewritten to actual column name
    .build();
```

This works across `project`, `singleAggregation`, `partialAggregation`, `finalAggregation`, and `assignUniqueId`, but alias propagation is not yet implemented for all matchers. If you run into a matcher that doesn't support it, please help extend the framework — contributions welcome! See `axiom/optimizer/tests/PlanMatcher.h` for detailed documentation.

#### Expression syntax

All expressions use DuckDB SQL syntax. Both function-call form (`le(a, b)`) and operator form (`a <= b`) work, but the operator form should be preferred for readability.

**`IN` operator gotcha:** The operator form `a IN (b, c)` works only when all IN-list values are constants. DuckDB rejects non-constant IN lists, so `a IN (col, 2)` fails to parse. In that case, use the quoted function-call form `"in"(a, col, 2)` (quotes are needed because `in` is a keyword):

```cpp
// ✅ Works — all constants.
.filter("n_regionkey IN (1, 2)")

// ❌ Fails — DuckDB requires constant IN list values.
.filter("n_regionkey IN (max_key, 2)")

// ✅ Works — quoted function-call form bypasses the keyword issue.
.filter("\"in\"(n_regionkey, max_key, 2)")
```

#### Controlling cost-based decisions with `setStats`

The optimizer makes cost-based decisions (join ordering, broadcast vs. shuffle, etc.) based on table and column statistics. To make these decisions deterministic in tests, use `TestConnector::setStats` to set row counts and column statistics without adding actual data:

```cpp
testConnector_->addTable("small_table", ROW({"a", "b"}, BIGINT()))
    ->setStats(
        100,
        {{"a", {.min = 1LL, .max = 100LL, .numDistinct = 50}},
         {"b", {.min = 10LL, .max = 500LL, .numDistinct = 100}}});

testConnector_->addTable("large_table", ROW({"x", "y"}, BIGINT()))
    ->setStats(
        10'000,
        {{"x", {.min = 1LL, .max = 50'000LL, .numDistinct = 5'000}},
         {"y", {.min = 100LL, .max = 10'000LL, .numDistinct = 8'000}}});
```

With these statistics, the optimizer will reliably choose to broadcast `small_table` and use `large_table` as the probe side. Available `ColumnStatistics` fields include `min`, `max`, `numDistinct`, `nullPct`, and `maxLength`.

Note: `setStats` provides statistics only — no actual data. These tables can be used in PlanMatcher tests (plan shape verification) but not in SqlTest (correctness verification, which requires actual data).

#### Plan well-formedness

PlanMatcher tests verify plan structure but do not ensure the plan is executable or produces correct results. However, `PlanConsistencyChecker` (`velox/core/PlanConsistencyChecker.h`) runs automatically on every plan Axiom produces and verifies that expressions in Filter, Project, and other nodes reference input columns with valid names and types. This catches malformed plans early — Velox `PlanNode` constructors lack sanity checks. We cannot freely add them because Velox is widely used and existing production applications may create technically malformed plans that happen to execute correctly. Without `PlanConsistencyChecker`, malformed plans would only surface as cryptic execution failures. If you encounter a cryptic execution failure caused by a malformed plan, consider extending `PlanConsistencyChecker` to catch that class of error.

#### Distributed plan matchers

Distributed plans are split into fragments that run on different nodes, connected by shuffles (data exchanges). A Velox plan describes only a single fragment — it cannot represent a multi-fragment plan. Axiom represents distributed plans as a `MultiFragmentPlan`: a collection of per-fragment Velox plans connected by exchanges. PlanMatcher allows you to verify the full distributed plan in a single matcher by representing fragment boundaries with `.shuffle()`, `.shuffleMerge()`, `.broadcast()`, and `.gather()`. You can also specify shuffle partition keys.

Use `AXIOM_ASSERT_DISTRIBUTED_PLAN` instead of `AXIOM_ASSERT_PLAN` for distributed plans. The framework validates the full fragment topology: producer side expects a `PartitionedOutput` node, consumer side expects an `Exchange` or `MergeExchange` node.

```cpp
// Distributed aggregation: partial agg → shuffle by key → final agg
auto matcher = PlanMatcherBuilder()
    .tableScan()
    .partialAggregation({"k"}, {"sum(v)"})
    .shuffle({"k"})
    .finalAggregation()
    .build();

AXIOM_ASSERT_DISTRIBUTED_PLAN(distributedPlan.plan, matcher);
```

`matchScan` is a helper defined in `QueryTestBase.h` to reduce boilerplate:

```cpp
static PlanMatcherBuilder matchScan(const std::string& tableName) {
    return PlanMatcherBuilder().tableScan(
        SchemaTableName{"default", tableName}.toString());
}
```

```cpp
// Broadcast join: small table is broadcast to all nodes
auto matcher = matchScan("large_table")
    .hashJoin(
        matchScan("small_table").broadcast().build(),
        core::JoinType::kInner)
    .build();
```

**Cross-node exchanges** represent fragment boundaries where data is transferred between nodes:
- `.shuffle({"key"})` — hash-partitioned exchange across nodes by specified keys.
- `.shuffleMerge()` — ordered exchange that preserves sort order (used with ORDER BY).
- `.broadcast()` — sends a full copy of data to all nodes (used for small tables in joins).
- `.gather()` — collects all data to a single node (used for final result output).

**Local exchanges** repartition data within a single node across pipelines. These are used to parallelize execution within a fragment (e.g., multiple threads processing different partitions of the same data on one node):
- `.localPartition({"key"})` — hash-partitions data across local pipelines by specified keys.
- `.localGather()` — collects data from multiple local pipelines into one.
- `.localMerge()` — merges sorted data from multiple local pipelines preserving order.

#### Reading matcher failures

When a matcher fails, the output includes the specific mismatch, a scoped trace showing which node failed, and a full dump of the actual plan. Here are two examples.

**Example 1: Wrong plan structure.** The test expects a Filter node above a TableScan, but the optimizer pushed the filter into the scan, so the root is a TableScan, not a Filter:

```
fbcode/axiom/optimizer/tests/PlanMatcher.cpp:64: Failure
Value of: specificNode != nullptr
  Actual: false
Expected: true
Expected facebook::velox::core::FilterNode, but got -- TableScan[0]

fbcode/axiom/optimizer/tests/FilterPushdownTest.cpp:105: Failure
Value of: (matcher)->match(_axiom_plan_)
  Actual: false
Expected: true
-- TableScan[0][table: nation, range filters: [(n_nationkey,
  BigintRange: [-9223372036854775808, 9] no nulls)], ...] -> n_nationkey:BIGINT, ...
```

How to read:
- `Expected facebook::velox::core::FilterNode, but got -- TableScan[0]` — the matcher expected a `FilterNode` but found a `TableScan[0]`. The plan structure is different from expected.
- The second block prints the full actual plan so you can see what the optimizer produced.

**Example 2: Wrong node property.** The test expects `n_nationkey < 20`, but the optimizer produced `n_nationkey < 10`:

```
fbcode/axiom/optimizer/tests/PlanMatcher.cpp:227: Failure
Value of: filter->testingEquals(*expected)
  Actual: false
Expected: true
Expected filter on n_nationkey: BigintRange: [-9223372036854775808, 19] no nulls,
  but got BigintRange: [-9223372036854775808, 9] no nulls
Google Test trace:
HiveScanMatcher: -- TableScan[0][table: nation, range filters: [(n_nationkey,
  BigintRange: [-9223372036854775808, 9] no nulls)], ...] -> n_nationkey:BIGINT, ...

fbcode/axiom/optimizer/tests/FilterPushdownTest.cpp:89: Failure
Value of: (matcher)->match(_axiom_plan_)
  Actual: false
Expected: true
-- TableScan[0][table: nation, range filters: [(n_nationkey,
  BigintRange: [-9223372036854775808, 9] no nulls)], ...] -> n_nationkey:BIGINT, ...
```

How to read:
- `Expected filter on n_nationkey: ... 19 ... but got ... 9` — the filter value is wrong. Expected `19` (< 20), got `9` (< 10).
- `HiveScanMatcher: -- TableScan[0][...]` — the Google Test trace identifies which plan node hit the mismatch. This is especially useful when the plan has multiple nodes of the same type.
- The second block prints the full actual plan.

---

### SqlTest: Verifying Correctness

SqlTest runs SQL queries through the full Axiom pipeline (parse → optimize → execute) and compares results against DuckDB. Each query runs under both the v1 and v2 optimizers, so one `.sql` file exercises both.

Tests are written as plain `.sql` files. Queries are separated by `----`. Comment lines (`-- ...`) before the SQL can contain annotations that control how the query is verified. Unrecognized comments are ignored. Comments after the SQL starts are treated as part of the SQL body.

**Annotations:**

| Annotation | Behavior |
|---|---|
| *(none)* | Default: run query in Axiom and DuckDB, compare results ignoring row order and output column names. |
| `-- ordered` | Compare results preserving row order. Use for queries with ORDER BY. |
| `-- count N` | Only check that the result has exactly N rows (no data comparison). Useful for verifying empty results (`-- count 0`) or queries with non-deterministic output (e.g., `random()`, `uuid()`, `LIMIT` without `ORDER BY`). |
| `-- error: message` | Expect the query to fail under both optimizers with an error containing `message`. Shorthand for `error_v1` + `error_v2`; cannot be combined with either. |
| `-- error_v1: message` | Expect the query to fail under the v1 optimizer with an error containing `message`. v2 is verified normally unless it also carries `error_v2`. |
| `-- error_v2: message` | Expect the query to fail under the v2 optimizer with an error containing `message`. v1 is verified normally unless it also carries `error_v1`. |
| `-- duckdb: sql` | Use a different SQL for the DuckDB comparison (when Presto and DuckDB syntax differs). |
| `-- columns` | Additionally verify that column names match between Axiom and DuckDB. |
| `-- disabled` | Skip this query entirely. For use during local development only — do not commit disabled queries. |

Annotations can be combined. For example, `-- ordered` and `-- columns` can be used together.

Use `error_v1`/`error_v2` when the two optimizers legitimately diverge — typically a v1 limitation that v2 fixes (or vice versa). The optimizer without an error annotation runs and is verified normally, so the same query can assert a v1 error and check the v2 result.

**Example:**

```sql
SELECT a, b FROM t WHERE a > 1
----
-- ordered
SELECT a, b FROM t ORDER BY b DESC
----
-- count 15
SELECT * FROM t
----
-- error: division by zero
SELECT a / 0 FROM t
----
-- duckdb: SELECT NULL
SELECT element_at(filter(ARRAY[1, 2, 3], x -> x > 5), 1)
----
-- columns
SELECT *, sum(b) OVER (PARTITION BY a) AS total_b FROM t
```

**`-- duckdb:` is for syntax/semantic differences only.** If the same SQL text would parse and produce the right answer in DuckDB, omit the annotation — duplicating the query is noise. Legitimate uses:

- DuckDB doesn't support a Presto construct.
- DuckDB has different semantics than Presto for the same syntax (e.g., DuckDB silently ignores the ALL keyword in `EXCEPT ALL` / `INTERSECT ALL` and returns DISTINCT results — see `set.sql`).
- Express the expected result via a simpler reference query that's easier to reason about than the query under test.

```sql
-- ❌ Avoid — `-- duckdb:` text is identical to the query.
-- duckdb: SELECT a, b FROM t WHERE a > 1
SELECT a, b FROM t WHERE a > 1

-- ✅ Use when DuckDB's semantics differ; hardcode the expected result.
-- duckdb: VALUES (1), (1), (1), (2), (2), (2), (2), (2)
SELECT a FROM t WHERE a < 3 EXCEPT ALL ...
```

**Test data setup:**

Test tables are declared at the top of the `.sql` file via setup directives. Statements within a setup block (or referenced setup file) are separated by `----`, the same separator used between queries.

| Directive | Behavior |
|---|---|
| `-- setup_file: relative.sql` | Splice in the contents of another `.sql` file as setup statements. Path is relative to the directory of the file containing the directive. |
| `-- setup` … `-- end_setup` | Inline block of setup statements. |

Both forms can appear any number of times, in any order, before the first query. Setup directives appearing after a query has started are rejected — install all tables first, then write queries.

Setup statements run via Axiom's PrestoParser:

- `CREATE TABLE name(col TYPE, ...)` registers an empty table.
- `INSERT INTO name VALUES (...), ...`, `INSERT INTO name SELECT ...`, and `CREATE TABLE name AS SELECT ...` install rows. Each `INSERT` produces one TestConnector split — use multiple `INSERT`s when you want a multi-split table.
- All statements are also mirrored into DuckDB so result comparison works against the same data. There is no `-- duckdb:` override for setup statements; setup SQL must be valid in both Presto and DuckDB. Stick to portable types (`BIGINT`, `DOUBLE`, `VARCHAR`, `BOOLEAN`, `DATE`, …) and literal `VALUES`.

Most files share the standard table `t` via the project-wide setup file:

```sql
-- setup_file: common_setup.sql

SELECT * FROM t WHERE a > 1
```

Files that need additional tables can declare them inline alongside the include:

```sql
-- setup_file: common_setup.sql
-- setup
CREATE TABLE u(a BIGINT)
----
INSERT INTO u VALUES (1), (2), (3)
-- end_setup

SELECT * FROM t JOIN u ON t.a = u.a
```

Each query becomes a separate gtest per optimizer, in the suites `V1/SqlTest_<file>` and `V2/SqlTest_<file>` (e.g., `V1/SqlTest_basic.l22` and `V2/SqlTest_basic.l22` for the query at line 22 of `basic.sql`); each also runs under cost-based and syntactic join ordering. Run them with:
```bash
buck test fbcode//axiom/optimizer/tests:sql
# only the v2 optimizer:
buck test fbcode//axiom/optimizer/tests:sql -- V2/
# one file:
buck test fbcode//axiom/optimizer/tests:sql -- basic
```

To add a new correctness test, append a query to the appropriate `.sql` file in `axiom/optimizer/tests/sql/`. No C++ changes needed. A few of the files:

- `basic.sql` — scratch file for local development and quick experiments. Not a dumping ground for checked-in tests.
- `join.sql` — joins.
- `subquery.sql` — EXISTS and other subquery patterns.
- `window.sql` — window functions (row_number, rank, frames, etc.).
- ... and others; see `tests/sql/` for the full list.

Choose the file that best matches the SQL feature being tested. If your tests don't fit any existing file, create a new `.sql` file and register it with a `registerQueryFile` call in `SqlTest.cpp`.

---

### Verifying Expected Plans

When writing PlanMatcher tests, you must verify that the expected plan is actually optimal. An incorrect expected plan bakes a bug into the test. A good way to do this is to ask Claude to review the plans from first principles:

> Would you check expected plans from first principles? Are these optimal and correct?

Claude will analyze the SQL, schema, and statistics, then reason about predicate pushdown, join ordering, aggregation splitting, column pruning, etc. For more targeted review, you can also point to a specific test case.

---

### Test Coverage

Before writing tests, plan what scenarios to cover. After writing, review to identify gaps, duplication, or redundancy. Ask Claude:

> Would you review tests for coverage completeness? Are there any gaps, duplication, redundancy?

Claude will analyze the tests and flag missing edge cases, tests that verify the same thing, and tests subsumed by more comprehensive ones.

---

### Summary

- **Plan shape → unit test + PlanMatcher**: Precise, tells you what broke.
- **Correctness → .sql + SqlTest**: End-to-end, compares against DuckDB, easy to write.
- **Use both**: New optimizations should have PlanMatcher tests for plan shape AND `.sql` tests for correctness.
- **Use Claude** to sanity-check your expected plans and review test coverage.
