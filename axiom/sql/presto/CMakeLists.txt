# Copyright (c) Meta Platforms, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(grammar)

add_library(axiom_sql_presto_error PrestoSqlError.cpp)
target_link_libraries(axiom_sql_presto_error Folly::folly velox_exception fmt::fmt)

add_subdirectory(ast)
add_subdirectory(example)

add_library(
  axiom_sql_presto_parser
  ColumnsExpansion.cpp
  ExpressionPlanner.cpp
  GroupByPlanner.cpp
  SortProjection.cpp
  PrestoParser.cpp
  ShowStatsBuilder.cpp
  SqlStatement.cpp
  TableVisitor.cpp
)

target_link_libraries(
  axiom_sql_presto_parser
  axiom_logical_plan_builder
  axiom_sql_presto_ast
  axiom_sql_presto_error
  velox_common_base
  velox_presto_types
  antlr4_shared
  re2::re2
)

add_subdirectory(tests)
