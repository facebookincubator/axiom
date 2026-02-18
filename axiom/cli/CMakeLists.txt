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

add_library(
  axiom_cli
  Connectors.cpp
  Console.cpp
  SqlQueryRunner.cpp
  ResultPrinter.cpp
  StdinReader.cpp
  Timing.cpp
  linenoise/linenoise.c
)

target_link_libraries(
  axiom_cli
  axiom_runner_local_runner
  axiom_runner_multifragment_plan
  axiom_optimizer
  axiom_hive_connector_metadata
  axiom_tpch_connector_metadata
  axiom_test_connector
  axiom_sql_presto_parser
  velox_exec_test_lib
  velox_dwio_common
  velox_dwio_parquet_reader
  velox_dwio_native_parquet_reader
  velox_parse_parser
  velox_parse_expression
  velox_parse_utils
)

add_executable(axiom_sql AxiomSql.cpp)

target_link_libraries(axiom_sql axiom_cli)

add_executable(axiom_graphviz QueryGraphviz.cpp)

target_link_libraries(axiom_graphviz axiom_cli)

add_executable(axiom_tpchgen GenerateTpchData.cpp)

target_link_libraries(
  axiom_tpchgen
  axiom_cli
  axiom_optimizer_tests_tpch_data_generator
  velox_exec_test_lib
)

add_subdirectory(tests)
