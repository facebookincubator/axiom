<!--
Copyright (c) Facebook, Inc. and its affiliates.

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

# Presto SQL Parser

The SQL parser here is almost an exact copy of the Presto Java SQL parser but with a few changes to make it work in C++ due to C++ keywords. The original Presto Java grammar is located in [Presto SQL grammar](https://github.com/prestodb/presto/blob/master/presto-parser/src/main/antlr4/com/facebook/presto/sql/parser/SqlBase.g4).

The generated visitor APIs are meant to be implemented in order to create the AST classes representing the user defined ANTLR4 grammar.

For now, the generated code is currently checked in by generating the ANTLR4 code and then copying the generated code into the `axio/sql/presto` directory using BUCK. Soon, the ANTLR4 generated code will be automatically done as part of the CMAKE build process (see https://github.com/antlr/antlr4/tree/dev/runtime/Cpp). In this way, all the user has to do is update the .g4 file, and the build process will take care of generating the antlr4 code. The user will then need to implement the new visitor APIs and create the respective AST classes.
