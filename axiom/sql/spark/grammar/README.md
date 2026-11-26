<!--
Copyright (c) Meta Platforms, Inc. and its affiliates.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Spark SQL Grammar

`SqlBaseLexer.g4` and `SqlBaseParser.g4` are Apache Spark 3.4.0's grammar
([lexer](https://github.com/apache/spark/blob/v3.4.0/sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBaseLexer.g4),
[parser](https://github.com/apache/spark/blob/v3.4.0/sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBaseParser.g4)),
the version of the Spark Connect client Collagen serves, with these changes to
compile to C++:

- The `@members` blocks are written in C++.
- Rule `namespace` is `namespaceKeyword`, label `operator` is `op`, and token
  `NULL` is `NULL_LITERAL`: the original names are C++ keywords or macros.

The lexer matches keywords in uppercase; feed it through
`axiom/sql/UpperCaseInputStream.h`. Include the generated headers
through `SparkGrammar.h`, which keeps glog's `CHECK` macro from breaking the
generated `CHECK()` accessor.

## Generated Files

The ANTLR4 generated code is checked into this directory, and the CMake build
uses it directly. After changing the grammar, run `gen.sh` to regenerate it.
