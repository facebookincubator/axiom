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

add_library(axiom_optimizer_tests_parquet_tpch ParquetTpchTest.cpp)

target_link_libraries(
  axiom_optimizer_tests_parquet_tpch
  axiom_logical_plan_builder
  axiom_optimizer
  velox_tpch_gen
  axiom_tpch_connector_metadata
  axiom_hive_connector_metadata
  axiom_runner_tests_utils
  velox_exec_test_lib
  velox_dwio_parquet_reader
  velox_dwio_native_parquet_reader
  gtest
  gtest_main
)

add_library(axiom_optimizer_tests_query_test_base QueryTestBase.cpp)

target_link_libraries(
  axiom_optimizer_tests_query_test_base
  axiom_optimizer
  velox_tpch_gen
  axiom_hive_connector_metadata
  axiom_runner_tests_utils
  velox_exec_test_lib
  velox_dwio_parquet_reader
  velox_dwio_native_parquet_reader
  gtest
  gtest_main
)

add_library(axiom_optimizer_tests_plan_matcher PlanMatcher.cpp)

target_link_libraries(
  axiom_optimizer_tests_plan_matcher
  velox_hive_connector
  velox_parse_parser
  GTest::gmock
  glog::glog
  GTest::gtest
  GTest::gtest_main
)

add_library(axiom_optimizer_tests_hive_queries_test_base HiveQueriesTestBase.cpp)

target_link_libraries(
  axiom_optimizer_tests_hive_queries_test_base
  axiom_optimizer_tests_parquet_tpch
  axiom_optimizer_tests_plan_matcher
  axiom_optimizer_tests_query_test_base
  axiom_sql_presto_parser
  axiom_runner_multifragment_plan
  axiom_runner_tests_utils
)

add_executable(axiom_optimizer_tpch_plan_test TpchPlanTest.cpp)

add_test(
  NAME axiom_optimizer_tpch_plan_test
  COMMAND axiom_optimizer_tpch_plan_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(
  axiom_optimizer_tpch_plan_test
  velox_dwio_common_test_utils
  axiom_optimizer_tests_parquet_tpch
  axiom_optimizer_tests_query_test_base
  axiom_optimizer_tests_hive_queries_test_base
  axiom_runner_tests_utils
)

add_executable(
  axiom_optimizer_tests
  AggregationPlanTest.cpp
  DerivedTablePrinterTest.cpp
  HiveAggregationQueriesTest.cpp
  HiveLimitQueriesTest.cpp
  HiveQueriesTest.cpp
  PrecomputeProjectionTest.cpp
  PlanTest.cpp
  RelationOpPrinterTest.cpp
  UnnestTest.cpp
  ParquetTpchTest.cpp
  SyntacticJoinOrderTest.cpp
  SubfieldTest.cpp
  FeatureGen.cpp
  Genies.cpp
  TestConnectorQueryTest.cpp
  WriteTest.cpp
)

add_test(axiom_optimizer_tests axiom_optimizer_tests)

target_link_libraries(
  axiom_optimizer_tests
  axiom_optimizer_test_utils
  axiom_optimizer
  axiom_logical_plan_builder
  axiom_optimizer_tests_hive_queries_test_base
  axiom_optimizer_tests_parquet_tpch
  axiom_optimizer_tests_plan_matcher
  axiom_optimizer_tests_query_test_base
  axiom_runner_tests_utils
  axiom_test_connector
  GTest::gmock
  glog::glog
  GTest::gtest
  GTest::gtest_main
)

add_executable(axiom_sql AxiomSql.cpp)

target_link_libraries(
  axiom_sql
  axiom_runner_local_runner
  axiom_runner_multifragment_plan
  axiom_optimizer
  axiom_hive_connector_metadata
  axiom_tpch_connector_metadata
  axiom_sql_presto_parser
  ${READLINE_LIBRARIES}
  velox_query_benchmark
  velox_exec_test_lib
  velox_dwio_common
  velox_dwio_parquet_reader
  velox_dwio_native_parquet_reader
  velox_parse_parser
  velox_parse_expression
  velox_parse_utils
)

add_subdirectory(utils)

add_executable(axiom_model_test ModelTest.cpp)

add_test(axiom_model_test axiom_model_test)

target_link_libraries(
  axiom_model_test
  axiom_optimizer
  Boost::headers
  glog::glog
  GTest::gtest
  GTest::gtest_main
  pthread
)
