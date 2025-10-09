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
