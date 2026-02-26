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

### Naming Conventions

- **PascalCase** for types and file names.
- **camelCase** for functions, member and local variables.
- **camelCase_** for private and protected member variables.
- **snake_case** for namespace names and build targets.
- **UPPER_SNAKE_CASE** for macros.
- **kPascalCase** for static constants and enumerators.
- Do not abbreviate. Use full, descriptive names. Well-established abbreviations (`id`, `url`, `sql`, `expr`) are acceptable.
- Prefer `numXxx` over `xxxCount` (e.g. `numRows`, `numKeys`).
- Type aliases defined elsewhere (e.g., `ExprCP`, `ColumnCP`) should be used as-is.

### Asserts and CHECKs

- Use `VELOX_CHECK_*` for internal errors, `VELOX_USER_CHECK_*` for user errors.
- Prefer two-argument forms: `VELOX_CHECK_LT(idx, size)` over `VELOX_CHECK(idx < size)`.
- Use `VELOX_FAIL()` / `VELOX_USER_FAIL()` to throw unconditionally.
- Use `VELOX_UNREACHABLE()` for impossible branches, `VELOX_NYI()` for unimplemented paths.
- Put runtime information (names, values, types) at the **end** of error messages, after the static description.
  - ❌ `VELOX_USER_FAIL("Column '{}' is ambiguous", name);`
  - ✅ `VELOX_USER_FAIL("Column is ambiguous: {}", name);`

### Variables

- Prefer value types, then `std::optional`, then `std::unique_ptr`.
- Prefer `std::string_view` over `const std::string&` for function parameters.
- Use uniform initialization: `size_t size{0}` over `size_t size = 0`.
- Declare variables in the smallest scope, as close to usage as possible.
- Use digit separators (`'`) for numeric literals with 4 or more digits: `10'000`, not `10000`.

### API Design

- Keep the public API surface small.
- Prefer free functions in `.cpp` (anonymous namespace) over private/static class methods.
- Keep method implementations in `.cpp` except for trivial one-liners.
- Avoid default arguments when all callers can pass values explicitly.

## Common Mistakes

These are frequently violated rules. Check every new or modified line against
this list before finishing.

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

// ❌ Wrong — private method is not public API.
 private:
  /// Recursively replaces window function references.
  ExprCP resolveWindowRefs(ExprCP expr) const;

// ✅ Correct.
 private:
  // Recursively replaces window function references.
  ExprCP resolveWindowRefs(ExprCP expr) const;
```

### One-letter and abbreviated variable names

Do not abbreviate. Use full, descriptive names. Loop indices (`i`, `j`) are
acceptable. Everything else — function parameters, lambda parameters, local
variables — must be descriptive.

```cpp
// ❌ Wrong — one-letter names and abbreviations.
bool sameKeys(const ExprVector& a, const ExprVector& b);
std::sort(groups.begin(), groups.end(), [](const auto& a, const auto& b) { ... });
for (auto* wf : group.functions) { ... }
for (size_t gi = 0; gi < groups.size(); ++gi) { ... }
auto f = [](WindowFunctionCP f) { return f->frame().type == WindowType::kRows; };

// ✅ Correct — descriptive names.
bool sameKeys(const ExprVector& lhs, const ExprVector& rhs);
std::sort(groups.begin(), groups.end(), [](const auto& lhs, const auto& rhs) { ... });
for (auto* windowFunc : group.functions) { ... }
for (size_t groupIndex = 0; groupIndex < groups.size(); ++groupIndex) { ... }
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
