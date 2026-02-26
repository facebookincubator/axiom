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
