Verax is an experimental frontend for the Velox ecosystem.

## License

Verax is licensed under the Apache 2.0 License. A copy of the license
[can be found here.](LICENSE)

## Getting started

[Optimizer](axiom/optimizer/README.md)

## Advance Velox Version
Verax integrates Velox as a Git submodule, referencing a specific commit of the Velox repository. Advance Velox when your changes depend on code in Velox that is not available in the current commit. To update the Velox version, follow these steps:
* `git -C velox checkout main`
* `git -C velox pull`
* `git add velox`
* Build and run tests to ensure everything works.
* Submit a PR, get it approved and merged.
