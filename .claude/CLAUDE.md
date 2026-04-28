# CLAUDE.md

Guidance for Claude Code when working in the Axiom repository.

## Overview

Axiom provides reusable SQL query processing components compatible with
[Velox](https://github.com/facebookincubator/velox). Licensed under
Apache 2.0.

## Build

```bash
make debug    # debug build
make release  # optimized build
```

## Testing

```bash
make unittest                    # run all tests
cd _build/debug && ctest -j 8   # run all tests in parallel
ctest -R AggregationParserTest   # run tests matching a pattern
```

Test files live in `tests/` subdirectories alongside source. Place new
tests next to related existing tests, not at the end of the file. Group
tests by topic (e.g., place `tryCast` next to `types`, `notBetween` next
to `ifClause` which uses `between`).

Prefer gtest container matchers over individual assertions:

```cpp
// ❌ Avoid - multiple individual assertions
EXPECT_EQ(result.size(), 3);
EXPECT_EQ(result[0], "a");
EXPECT_EQ(result[1], "b");
EXPECT_EQ(result[2], "c");

// ✅ Prefer - single matcher assertion
EXPECT_THAT(result, testing::ElementsAre("a", "b", "c"));
```

Common matchers:
- `ElementsAre(...)` - exact ordered match
- `UnorderedElementsAre(...)` - exact unordered match
- `Contains(...)` - at least one element matches
- `IsEmpty()` - collection is empty
- `SizeIs(n)` - collection has n elements

Requires `#include <gmock/gmock.h>`.

### Verifying bug fixes

When fixing a bug, write the test **first** and confirm it reproduces the issue before applying the fix. If the test was written after the fix, temporarily revert the fix and verify the test fails without it. A test that passes with and without the fix proves nothing.

### Verifying changes empirically

Do not rely solely on static code analysis (reading code and reasoning about behavior). Always verify changes by running the code:
- Add temporary print statements to confirm the code path is exercised.
- Write a small test to observe actual behavior.
- Remove debug logging before committing.

## Formatting

```bash
make format  # format all changed files
```

## Coding Style

Axiom follows the Velox coding style. See
[CODING_STYLE.md](https://github.com/facebookincubator/velox/blob/main/CODING_STYLE.md)
for the complete guide. Key rules are summarized below.

### Comments

- Use `///` for public API documentation (classes, public methods, public members).
- Use `//` for private/protected members and comments inside code blocks.
- Start comments with active verbs, not "This class…" or "This method…".
  - ❌ `/// This class builds query plans.`
  - ✅ `/// Builds query plans.`
- Comments should be full English sentences starting with a capital letter and ending with a period.
- Comment every class, every non-trivial method, every member variable.
- Do not restate the variable name. Either explain the semantic meaning or omit the comment.
  - ❌ `// A simple counter.` above `size_t count_{0};`
- Avoid redundant comments that repeat what the code already says. Comments should explain *why*, not *what*.
- Use `// TODO: Description.` for future work. Do not include author's username.
- Do not duplicate comments between `.h` and `.cpp`. Document the function in the header; the implementation should not repeat the same comment. Duplicated comments diverge over time.

### Naming Conventions

- **PascalCase** for types and file names.
- **camelCase** for functions, member and local variables.
- **camelCase_** for private and protected member variables.
- **snake_case** for namespace names and build targets.
- **UPPER_SNAKE_CASE** for macros.
- **kPascalCase** for static constants and enumerators.
- Do not abbreviate. Use full, descriptive names. Well-established abbreviations (`id`, `url`, `sql`, `expr`) are acceptable.
- Prefer `numXxx` over `xxxCount` (e.g. `numRows`, `numKeys`).
- Never name a file or class `*Utils`, `*Helpers`, or `*Common`. These generic
  names attract unrelated functions over time and lose cohesion. Name files and
  classes after the concept they represent. Use a class with static methods to
  group related operations, and shorten method names since the class name
  provides context.
- Type aliases defined elsewhere (e.g., `ExprCP`, `ColumnCP`) should be used as-is.

### Asserts and CHECKs

- Use `VELOX_CHECK_*` for internal errors, `VELOX_USER_CHECK_*` for user errors.
- Prefer two-argument forms: `VELOX_CHECK_LT(idx, size)` over `VELOX_CHECK(idx < size)`.
- Use `VELOX_FAIL()` / `VELOX_USER_FAIL()` to throw unconditionally.
- Use `VELOX_UNREACHABLE()` for impossible branches, `VELOX_NYI()` for unimplemented paths.
- Put runtime information (names, values, types) at the **end** of error messages, after the static description.
  - ❌ `VELOX_USER_FAIL("Column '{}' is ambiguous", name);`
  - ✅ `VELOX_USER_FAIL("Column is ambiguous: {}", name);`
- Use precise terminology when describing errors:
  - **crash**: unrecoverable error — segfault, `VELOX_UNREACHABLE()`, abort, or similar.
  - **fail**: controlled error — `VELOX_CHECK`, `VELOX_USER_CHECK`, thrown exception, or returned error code.
  - Do not use "crash" when the code throws a catchable exception.

### Variables

- Prefer value types, then `std::optional`, then `std::unique_ptr`.
- Prefer `std::string_view` over `const std::string&` for function parameters.
- Use uniform initialization: `size_t size{0}` over `size_t size = 0`.
- Declare variables in the smallest scope, as close to usage as possible.
- Use digit separators (`'`) for numeric literals with 4 or more digits: `10'000`, not `10000`.
- Use trailing commas in multi-line initializer lists and enum definitions.
  This produces cleaner diffs when items are added or reordered.

### API Design

- Keep the public API surface small.
- Prefer free functions in `.cpp` (anonymous namespace) over private/static class methods.
- Define free functions close to where they are used, not grouped together at the top or bottom of the file.
- Keep method implementations in `.cpp` except for trivial one-liners.
- Avoid default arguments when all callers can pass values explicitly.
- Never use `friend`, `FRIEND_TEST`, or any friend declarations. If a test needs access to private members, redesign the API or test through public methods instead.

## Commit Messages

Use conventional commit format:

```
feat(optimizer): Add support for window functions
fix: Validate HAVING column references in SQL parser
refactor(parser): Extract GroupByPlanner class
```

Format: `type(scope): Description`
- Types: `feat`, `fix`, `refactor`, `test`, `docs`
- Scope is optional, use for subsystem clarity (e.g., `parser`, `optimizer`)
- Description starts with a capital letter, no trailing period

## Common Mistakes

These are frequently violated rules. Check every new or modified line against
this list before finishing.

### Bug fixes without a failing test first

When fixing a bug, write the test **first**, run it, and confirm it **fails**
before applying the fix. Then apply the fix and confirm the test passes. If you
wrote the test after the fix, temporarily revert the fix and verify the test
fails. A test that passes both with and without the fix proves nothing. This is
the single most important workflow rule for bug fixes.

### `///` vs `//` — wrong comment style

`///` is **only** for public API: public classes, public methods, public member
variables. Everything else uses `//`: private/protected members, anonymous
namespace functions and types, comments inside function bodies.

```cpp
// ❌ Wrong — anonymous-namespace function is not public API.
namespace {
/// Returns true if 'a' is a prefix of 'b'.
bool isPrefix(const ExprVector& a, const ExprVector& b);
} // namespace

// ✅ Correct.
namespace {
// Returns true if 'a' is a prefix of 'b'.
bool isPrefix(const ExprVector& a, const ExprVector& b);
} // namespace
```

### Generic file and class names (`*Utils`, `*Helpers`, `*Common`)

Never name a file or class `*Utils`, `*Helpers`, or `*Common`. These generic
names attract unrelated functions over time and lose cohesion. Instead, name
files and classes after the concept they represent.

When extracting shared functions, ask: "What do these functions have in common
beyond being useful?" The answer is the name. Use a class with static methods
to group related operations, and shorten method names since the class name
provides context.

```cpp
// ❌ Wrong — generic name, will attract unrelated functions.
class ParserUtils {
  static std::vector<size_t> widenProjectionsForSort(...);
  static void sortAndTrimProjections(...);
};

// ✅ Correct — named after the concept (sorting + projections).
class SortProjection {
  static std::vector<size_t> widenProjections(...);
  static void sortAndTrim(...);
};
```

### One-letter and abbreviated variable names

Do not abbreviate. Use full, descriptive names. Loop indices (`i`, `j`) are
acceptable. Everything else — function parameters, lambda parameters, local
variables — must be descriptive.

```cpp
// ❌ Wrong — one-letter names and abbreviations.
bool sameKeys(const ExprVector& a, const ExprVector& b);
std::sort(groups.begin(), groups.end(), [](const auto& a, const auto& b) { ... });
auto f = [](WindowFunctionCP f) { return f->frame().type == WindowType::kRows; };

// ✅ Correct — descriptive names.
bool sameKeys(const ExprVector& lhs, const ExprVector& rhs);
std::sort(groups.begin(), groups.end(), [](const auto& lhs, const auto& rhs) { ... });
auto f = [](WindowFunctionCP func) { return func->frame().type == WindowType::kRows; };
```

### Undocumented APIs in headers

Every class, every non-trivial method declaration, and every member variable in
a `.h` file must have a comment. Trivial one-liner getters may be left
undocumented if the name is self-explanatory.

```cpp
// ❌ Wrong — no comment on method declaration.
  ExprCP translateWindowExpr(const logical_plan::WindowExpr* window);

// ✅ Correct.
  // Translates a logical WindowExpr to a QueryGraph WindowFunction.
  ExprCP translateWindowExpr(const logical_plan::WindowExpr* window);
```

### Non-trivial implementations in headers

Keep method implementations in `.cpp` except for trivial one-liners. If a
method body has more than one statement, it belongs in the `.cpp` file.

```cpp
// ❌ Wrong — multi-statement body in header.
  const WindowPlan* withFunctions(
      const QGVector<WindowFunctionCP>& functions,
      const ColumnVector& columns) const {
    auto merged = functions_;
    merged.insert(merged.end(), functions.begin(), functions.end());
    return make<WindowPlan>(std::move(merged), columns_);
  }

// ✅ Correct — declaration only in header, implementation in .cpp.
  const WindowPlan* withFunctions(
      const QGVector<WindowFunctionCP>& functions,
      const ColumnVector& columns) const;
```

### `goto` statements

Never use `goto`. Restructure the code using early returns, helper functions, or
duplicated code paths instead. `goto` makes control flow hard to follow and is
never necessary in C++.

### Fitting tests to buggy code

When a test fails, **never** update the test expectation to match what the code
produces without first verifying that the code is correct. The default
assumption should be that the code is wrong, not the test.

For optimizer plan tests:
1. Derive the expected plan from query semantics before running the test.
2. When a test fails, investigate the mismatch — do not blindly copy the actual
   output into the expected.
3. Flag suspicious patterns: redundant operators, two gathers with no partition
   keys, extra columns flowing through unnecessary nodes.

### Static analysis instead of CLI experiments

When working on the optimizer, do not spend time reading code and manually
tracing through logic to predict output (cardinalities, plans, column indices).
Instead, run a quick CLI experiment:

```bash
buck run axiom/cli:cli -- --num_workers 1 --num_drivers 1 \
  --query "EXPLAIN (type optimized) SELECT ..."
```

Use `--init` with the test connector to create tables with specific data:

```sql
use test.default;
create table t as select * from unnest(sequence(1, 100)) as t(a);
```

A 10-second CLI run is more reliable than 10 minutes of static analysis.

### Silently simplifying an approved plan

Never silently simplify or skip parts of an approved implementation plan. If a
step turns out to be harder than expected, or you want to defer it, say so
explicitly and get approval before proceeding with a reduced scope. Reporting
"done" when a key piece was dropped is worse than asking for help.

### Working around infrastructure bugs

When you discover a bug in test infrastructure, shared helpers, or common
utilities, do **not** silently work around it. Stop, report the finding, and
discuss whether to fix the root cause or work around it. Workarounds accumulate
into technical debt and mask real problems.

### Verify causation before asserting it

When investigating a test failure or regression, do not attribute it to a
specific commit based on the commit message alone. Verify empirically by
checking out the parent commit and running the test. Incorrect attribution
leads to wrong fixes — e.g., updating test expectations when the real problem
is a bug introduced by a different commit.

## Axiom-Specific Patterns

### Dialect Independence

- Only the SQL parser is dialect-specific. The logical plan, optimizer,
  PlanBuilder, and runner are dialect-agnostic — do not introduce
  dialect-specific terms in these layers.

## Directory Structure

| Directory | Description |
|-----------|-------------|
| `axiom/sql/` | SQL parser (Presto dialect) |
| `axiom/logical_plan/` | Logical plan representation |
| `axiom/optimizer/` | Cost-based query optimizer |
| `axiom/runner/` | Multi-stage Velox execution |
| `axiom/connectors/` | Velox connector extensions |
| `axiom/common/` | Shared utilities |
| `axiom/cli/` | Command-line SQL interface |

## Key Documentation

- [SQL Parser](../axiom/sql/presto/README.md) — Architecture, scoping, PlanBuilder design, testing conventions.
- [Optimizer](../axiom/optimizer/README.md) — Query graph data structure and terminology.
- [Testing](../axiom/optimizer/docs/Testing.md) — PlanMatcher, SqlTest, and test organization guidelines.
