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

Test comments should describe **what behavior the test verifies**, not the history that led to writing it or how the implementation happens to work today. The bug story belongs in the commit message and diff/PR description; don't duplicate it into the test code.

A new case of a behavior that already has a test extends that test. A new
stat, field or variant goes into the existing test, which then asserts all of
the behavior's outputs, including the new one.

When a test already runs the code through its real path, such as an
end-to-end or query test, extend that test. Inputs built by hand can describe
states the real path never produces, and a test on them pins behavior that
does not exist.

A bug fix in code that has no tests adds tests for the whole behavior that
code implements. The bug shows that coverage is missing, and one case for the
bug leaves the rest of the behavior untested.

### Query names and durability

Replace production table, column, CTE, and alias names with generic names.
Use `t`, `u`, and `v` consistently for tables, CTEs, and relation aliases, and
simple column names such as `a`, `b`, and `c`.

Aim for tests resilient to future optimizations. Choose queries and data that
keep the behavior under test observable after predictable simplifications,
such as constant folding, expression deduplication, and singleton UNNEST
elimination. Avoid relying on an unrelated missed optimization to reach the
bug; choose another query.
Assert results and invariants, and match only plan details central to the
optimization being tested.

### Assertions

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
- State an invariant once, where it is owned: the type's doc, the component that guarantees it, or "Optimizer Principles" below for canonical forms. A comment at a use site explains only what is particular to that code. It does not restate or paraphrase a property owned elsewhere — a canonical form, an IR invariant, another component's guarantee. A local statement of a general fact reads as specific to that site, so readers conclude other sites differ or that this code has a limitation, and chase it.
  - ❌ `// Returns the keys only when every key is one of the state's columns.` — reads as a limitation of this function; it is a fact about canonical form.
  - ✅ `// The anchor is placed by the same keys, so they must read only the state's columns.` — this function's own reason.
- Don't leave reasoning-scaffolding comments in committed code. Comments that capture the thought process — naming a sibling function, citing an example from a design discussion ("e.g. for COUNT-style aggregates"), framing the code defensively against an alternative path that was considered and rejected — are useful working notes while writing but rot quickly: they couple to other functions' internals and to ephemeral conversation context that a fresh reader doesn't share. Before committing, delete any comment that names another function and contrasts it ("X does Y; we do Z"), cites a specific operator/aggregate from discussion, describes a path you didn't take, or reads as a running narrative. Keep only comments that describe an invariant this code maintains or a non-obvious constraint particular to it. A quick check when re-reading a diff: a comment carrying "rather than", "instead of", "would otherwise" or "unreachable", or naming another function in order to contrast with it, is almost always scaffolding.
- After trimming, re-read the function as a fresh reader and check the *other* failure mode: stripping every comment can leave branches whose intent is no longer visible. A silent `else` to a non-trivial `if/else-if` chain, a state mutation whose justification depends on a downstream call, a deliberate-looking "do nothing" — these usually need a one-line `// why` even after every scaffolding comment is gone. Brevity is not "zero comments"; it is "no comment that doesn't earn its line."

#### Public class docs — readability

The above rules are mechanical (form). Class-level documentation has an
additional readability requirement, since header docs are read cold by
people who don't know the surrounding design.

**For non-trivial classes** — defined as ANY of:
- Has multi-state behavior (an enum field, mode flag, or `kind` tag the implementation dispatches on)
- Carries invariants beyond defaults (constructor assertions, ordering requirements, lifecycle rules)
- Participates in a larger protocol or algorithm with other classes
- Is a factory / builder / registry / context object callers compose

— the class-level doc MUST include:
- **(a)** ONE sentence stating what the class is (lead, not buried)
- **(b)** ONE concrete usage example (typically a snippet showing construction + the most common operation, OR a sketch of how it slots into the larger algorithm)
- **(c)** IF the class has invariants: a checklist at the end mirroring what the constructor asserts

**For trivial classes** (pure data carriers with self-explanatory fields,
single-method helpers whose method name conveys purpose, typedef-style
aliases, simple enums): ONE sentence stating what it is. No example or
invariants checklist required.

**When in doubt:** would a reader who hasn't seen the rest of the file
get stuck before reaching the second paragraph? If yes, the class is
non-trivial.

**Failure modes the mechanical rules don't catch** (apply
would-I-say-this-aloud test to each):
- Per-kind / per-mode behavior described as prose instead of a scannable list — readers can't compare cases at a glance.
- Invariants buried in narrative ("if X is set then Y must also be set, except when Z, in which case W") — restate as a checklist.
- Jargon used without definition ("dependent join", "universal", "DT-form") — define inline at first use or replace with plain terms.
- Modeling-rationale fine-print (why the field is split into N components) added without a one-line summary of what the field IS — give the "what" before the "why."
- Edit accretion: a doc that started small and grew through edits without restructuring. Periodically re-read public docs cold and refactor if they no longer read as one piece.

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

### Types

- Use the short `ROW` forms rather than spelling out parallel vectors:
  - One field: `ROW("a", BIGINT())`, not `ROW({"a"}, {BIGINT()})`.
  - Fields sharing a type: `ROW({"a", "b"}, BIGINT())`, not
    `ROW({"a", "b"}, {BIGINT(), BIGINT()})`.
  - Mixed types keep the two-vector form: `ROW({"a", "b"}, {BIGINT(),
    VARCHAR()})`.

### Enums

Every enum declared in a header gets name mapping, added with the enum itself.
It is not the first caller's job to add it. Use `AXIOM_DECLARE_ENUM_NAME` /
`AXIOM_DEFINE_ENUM_NAME` for a namespace-scope enum and the `_EMBEDDED_`
variants for one nested in a class — the two are the same rule, and both
macros are in `axiom/common/Enums.h`.

The declaration goes in the header next to the enum, the definition in the
`.cpp` next to a `Names()` function in an anonymous namespace mapping each
enumerator to its name:

```cpp
// Schema.h
enum class Kind { kUnspecified, kPartitioned, ... };

AXIOM_DECLARE_EMBEDDED_ENUM_NAME(Kind);
```

```cpp
// Schema.cpp
namespace {
const auto& distributionKindNames() {
  static const folly::F14FastMap<Distribution::Kind, std::string_view> kNames =
      {
          {Distribution::Kind::kUnspecified, "UNSPECIFIED"},
          {Distribution::Kind::kPartitioned, "PARTITIONED"},
      };
  return kNames;
}
} // namespace

AXIOM_DEFINE_EMBEDDED_ENUM_NAME(Distribution, Kind, distributionKindNames);
```

A `DECLARE` without its `DEFINE` compiles and links until someone calls
`toName`, so a missing definition is invisible until it is not.

### API Design

Read an API's documented contract before using it. Document preconditions,
guarantees, ownership, and optional-value semantics at the API declaration.
When a caller needs an undocumented guarantee, establish it at the owning API
before relying on it. Producers enforce guarantees; callers handle states the
contract permits. Implementation behavior alone does not establish a contract.

- Keep the public API surface small.
- Prefer free functions in `.cpp` (anonymous namespace) over private/static methods declared in the header. The goal is keeping the header small: fewer things in the header means the header changes less often, so fewer downstream translation units recompile. Private members of a file-local class defined entirely in the `.cpp` already satisfy this — the rule doesn't apply to them.
- Define free functions close to where they are used, not grouped together at the top or bottom of the file.
- Keep method implementations in `.cpp` except for trivial one-liners.
- Avoid default arguments when all callers can pass values explicitly.
- Never use `friend`, `FRIEND_TEST`, or any friend declarations. If a test needs access to private members, redesign the API or test through public methods instead.
- Never declare a free function in a header. A helper that more than one translation unit needs goes on the class that owns the concept, as a static method — `Join::preservedSides(joinType)`, not `preservedSides(joinType)` at namespace scope. A free function already sitting in a header is not license to add another.

### IR shape

The optimizer IR is a tree — the `Node` docblock states the invariant and what `Builder`'s interning does. Write passes to match it:

- Recurse over inputs directly; each node is reached once.
- Build test plans through `Builder` from a query, so they have the shape the IR produces.

A node reached from two parents is a bug. Report it rather than working around it.

### Optimizer passes

Passes do not depend on each other. A pass must not include another pass's header, or a header that is one pass's implementation detail, to reuse a helper — even a pure one.

A pass-to-pass include makes the pipeline order load-bearing in the type system and turns one pass's internals into another's contract. Two passes that must agree on a fact are also a drift hazard: two copies of the same rule diverge, and a divergence in a soundness rule is a wrong-results bug.

When two passes need the same derived fact, it has to live somewhere neither of them owns. If the fact is a property of a node, the IR layer is the natural home — on the node class, alongside the other derivations. If it is not, it belongs in a component of its own. That component has to stand on its own terms — a coherent thing with a reason to exist, not a home invented to hold a shared helper — and it has to make sense for each pass to depend on it. `Builder` and `ExprFactory` are components of that kind: constructing nodes and constructing expressions are concerns in their own right, and every pass has a reason to depend on them.

A need to share that has no home yet is a signal, not a placement puzzle. If neither the IR nor an existing component fits, the decomposition does not express something the code needs — two passes have grown a common concept that belongs to neither. Work out what that concept is before writing anything.

## Commit Messages

Draft commit messages with the `write-commit-message` skill
(`.claude/skills/write-commit-message/`). It holds the rules for the
summary body and the check to run before showing a draft.

Use conventional commit prefixes with `[Project]` tags:

```
[Axiom] feat(optimizer): Add support for window functions
[Axiom] fix: Validate HAVING column references in SQL parser
[Axiom] refactor(parser): Extract GroupByPlanner class
```

Format: `[Project] type(scope): Description`
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

### Duplicating existing logic

Before writing a helper or a block of more than a few lines, find where the
concept already lives and call it. Search for the data being computed, not
only for a function name. When the existing code is private to another file
or class, move it to the class that owns the concept and call it from both
places.

This applies to tests. A test helper calls the production code for anything
production already computes, and a new check reuses the helpers and fixtures
its test file already has.

Copies drift apart. A drifted copy of a rule in production code is a
wrong-results bug, and a drifted copy in a test checks something production
no longer does.

### Copying a pattern without judging it

An existing instance of a pattern is not a reason to repeat it. Before copying
one, ask whether it is a good pattern. Compatibility shims, fallbacks and
workarounds are usually one-offs. One may have been added for a reason
particular to its site, or added without much thought. Its reason may also be
long gone, leaving the pattern in place. Start by working out how your code can
do without the pattern. Copy it only when you cannot, and then justify your
instance on its own terms. Another instance is never the justification.

### One-letter and abbreviated variable names

Do not abbreviate. Use full, descriptive names. Loop indices (`i`, `j`) and
standard iterator/comparison names (`it`, `lhs`, `rhs`) are
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

Keep method implementations in `.cpp`. A body stays in the header only when it
needs no types the header does not already have and is short enough that it
will not churn — a getter, or a two-line check-and-set. Anything that pulls in
an include, or that a reader has to scroll through, moves. Statement count is
not the test: moving a two-line guard to the `.cpp` costs a declaration, a
definition and a jump between files, and saves nothing.

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

### Leaving incremental scaffolding in the final code

Before declaring a multi-step change done, re-read the modified files as a
fresh reader and remove scaffolding from the editing process: dead
variables/helpers/fields, stale comments describing an earlier version of
the code, defensive checks that no longer guard anything, organization
reflecting edit order, and naming inherited from prior iterations. Refactor
as if you were writing the final form from scratch — the reader has only
the code, not your editing history.

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

### Optimizer Principles

Goal and models:

Optimize as well as the available information, models, and search budget allow.
Producing the globally best plan for every query is not a requirement.

- Preserve query semantics using explicit, verifiable invariants. Statistical
  estimates do not provide correctness guarantees.
- Use coherent cardinality and cost models. Document their assumptions and
  limitations, and verify their predictions against observed behavior. Costs
  must account consistently for the work each candidate requires.
- Choose the best candidate considered under those models. Investigate a poor
  plan for missing information, model errors, bugs, or unexplored alternatives.

Build general capabilities:

- A production query motivates a change and serves as its test. It does not
  define the change's scope. Design the capability for every operator and plan
  shape it applies to, rather than the least code that makes the motivating
  query plan well.
- Choose the design that handles the general case cleanly over the smallest
  diff. A narrow fix leaves sibling cases broken for someone else to find.
- An investigation can uncover a bug or gap worth fixing even when the
  motivating query's plan stays unchanged. Evaluate the fix by the correctness,
  estimates, or cost modeling it improves across the cases it applies to, and
  report its effect on the motivating query separately.

Invariants. These hold across the optimizer. Rely on them. Do not write code that guards
against their violation, and do not ask for such code in designs or reviews. A
violation is a bug in the code that owns the invariant; fix it there.

- `Builder::canonicalizeCall` (`axiom/optimizer/Builder.h`) puts every
  binary call in `reversibleFunctions_` into canonical order when it is built:
  a literal goes on the right; otherwise the lower-id expression goes on the
  left. Match `column = literal`; do not also check `literal = column`.
- Translate rewrites a single-element `IN` as equality. Treat `x IN (c)` as
  `x = c`.
- Before handling an alternative form of an expression, find the normalization
  that produces the form you rely on.
- Canonical forms decide how much work the optimizer does, not whether its
  result is correct. Code normalizes once, where a fact becomes known: when an
  expression is built (argument order, `IN` lists), or when a join makes
  columns equal (conjuncts above it read one of the equal columns and are
  simplified using what the join guarantees, such as non-null keys). Later
  rules match only the canonical form instead of re-deriving every equivalent
  form on every expression in every pass. So a non-canonical input costs a
  missed optimization, never a wrong result: a rule that removes or weakens a
  condition must be correct for any input. If a non-canonical form reaches your
  code, fix the normalization to recover the optimization.
- If `a = b`, the optimizer may substitute one for the other. `-0.0` vs `0.0`
  and different zones of equal `TIMESTAMP WITH TIME ZONE` values are
  representation differences, not correctness issues. Do not add per-type
  guards to substitutions, and do not raise these cases in designs or reviews.

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
- [Optimizer](../axiom/optimizer/README.md) — Optimizer overview, documentation index, and terminology.
- [Testing](../axiom/optimizer/docs/Testing.md) — PlanMatcher, SqlTest, and test organization guidelines.
