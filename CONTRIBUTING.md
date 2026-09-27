# Contributing

Axiom is a set of composable libraries for Compute frontend in the Velox ecosystem.
Please refer to the contribution guidelines
[here](https://github.com/facebookincubator/velox/blob/main/CONTRIBUTING.md)

## CMake guidelines

There are 4 top-level libraries in Axiom. Each of them is a separate CMake target.

- optimizer
- runner
- logical_plan
- connectors

Connectors library is special as it inludes a set of plugins  / connectors. Each
of the connectors is a separate CMake target.

- connectors/tpch
- connectors/hive

Avoid introducing new CMake targets unless there is a clear reason to do so.

### Tests and test-support libraries

Two options gate what a build produces:

- `AXIOM_BUILD_TESTING` (on by default) builds Axiom's test suites and registers
  them with ctest.
- `AXIOM_BUILD_TEST_UTILS` (off by default) builds the support libraries that
  test directories define, without the suites. A project that embeds Axiom and
  links those libraries configures `-DAXIOM_BUILD_TEST_UTILS=ON
  -DAXIOM_BUILD_TESTING=OFF` and adds no `axiom/*/tests` directory of its own.

Two rules keep that working:

- A `tests` directory that defines a library is added by its parent under
  `if(AXIOM_BUILD_TESTING OR AXIOM_BUILD_TEST_UTILS)`. A directory holding only
  suites keeps the plain `if(AXIOM_BUILD_TESTING)`.
- Inside a `tests` directory, `add_library` sits outside any guard, while the
  executables and their `add_test` calls sit behind `if(AXIOM_BUILD_TESTING)`.

Adding an `add_library` to a suites-only test directory means its parent needs
the `OR` too. Nothing fails if you forget: the library is quietly unavailable to
consumers.

Link gtest through `GTest::gtest`, `GTest::gtest_main` and `GTest::gmock`. The
bare names can resolve to a system gtest whose headers do not match the bundled
one.

## Design guidelines

### Keep Axiom generic

Axiom is designed to work with different SQL dialects, not just Presto. Avoid
hard-coding function names, type names, or dialect-specific behavior directly
in the optimizer or runner. Instead, use the `FunctionRegistry` to register
well-known functions by their semantic role and look them up at optimization
time.

```cpp
// ❌ Wrong — hard-coded function name.
if (name == "row_number") {
  return RankFunction::kRowNumber;
}

// ✅ Correct — use registered name from FunctionRegistry.
const auto* registry = FunctionRegistry::instance();
if (registry->rowNumber().has_value() && name == *registry->rowNumber()) {
  return RankFunction::kRowNumber;
}
```

When adding a new optimization that depends on recognizing a specific function,
add a registration method to `FunctionRegistry` (e.g., `registerRowNumber`) and
register the function in `registerPrestoFunctions` (or the equivalent for other
dialects). This ensures the optimization works regardless of the function name
used by a particular dialect.
