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

velox_add_library(velox_optimizer_tests_parquet_tpch ParquetTpchTest.cpp)

velox_link_libraries(
  velox_optimizer_tests_parquet_tpch
  velox_fe_logical_plan_builder
  velox_verax
  velox_schema_resolver
  velox_tpch_gen
  velox_tpch_connector_metadata
  velox_hive_connector_metadata
  axiom_runner_tests_utils
  velox_exec_test_lib
  velox_dwio_parquet_reader
  velox_dwio_native_parquet_reader
  gtest
  gtest_main)

velox_add_library(velox_optimizer_tests_query_test_base QueryTestBase.cpp)

velox_link_libraries(
  velox_optimizer_tests_query_test_base
  velox_verax
  velox_schema_resolver
  velox_tpch_gen
  velox_hive_connector_metadata
  axiom_runner_tests_utils
  velox_exec_test_lib
  velox_dwio_parquet_reader
  velox_dwio_native_parquet_reader
  gtest
  gtest_main)

velox_add_library(velox_optimizer_tests_presto_parser PrestoParser.cpp)

velox_link_libraries(
  velox_optimizer_tests_presto_parser velox_fe_logical_plan_builder
  axiom_sql_presto_ast antlr4_shared)

velox_add_library(velox_optimizer_tests_duck_parser DuckParser.cpp)

velox_link_libraries(
  velox_optimizer_tests_duck_parser velox_fe_logical_plan_builder
  velox_parse_parser velox_vector)

velox_add_library(velox_optimizer_tests_plan_matcher PlanMatcher.cpp
                  LogicalPlanMatcher.cpp)

velox_link_libraries(
  velox_optimizer_tests_plan_matcher
  velox_hive_connector
  velox_parse_parser
  GTest::gmock
  glog::glog
  GTest::gtest
  GTest::gtest_main)

velox_add_library(velox_optimizer_tests_hive_queries_test_base
                  HiveQueriesTestBase.cpp)

velox_link_libraries(
  velox_optimizer_tests_hive_queries_test_base
  velox_optimizer_tests_parquet_tpch
  velox_optimizer_tests_plan_matcher
  velox_optimizer_tests_query_test_base
  velox_optimizer_tests_presto_parser
  axiom_runner_multifragment_plan
  axiom_runner_tests_utils)

add_executable(velox_optimizer_tests_tpch_plan TpchPlanTest.cpp)

add_test(
  NAME velox_optimizer_tests_tpch_plan
  COMMAND velox_optimizer_tests_tpch_plan
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(
  velox_optimizer_tests_tpch_plan
  velox_dwio_common_test_utils
  velox_optimizer_tests_parquet_tpch
  velox_optimizer_tests_query_test_base
  velox_optimizer_tests_hive_queries_test_base
  axiom_runner_tests_utils)

add_executable(
  velox_plan_test
  HiveLimitQueriesTest.cpp
  HiveQueriesTest.cpp
  PlanTest.cpp
  ParquetTpchTest.cpp
  QueryTestBase.cpp
  SubfieldTest.cpp
  FeatureGen.cpp
  Genies.cpp
  PrestoParserTest.cpp
  TestConnectorQueryTest.cpp
  SchemaUtilsTest.cpp
  SchemaResolverTest.cpp)

add_test(velox_plan_test velox_plan_test)

target_link_libraries(
  velox_plan_test
  velox_optimizer_test_utils
  velox_verax
  velox_fe_logical_plan_builder
  velox_optimizer_tests_hive_queries_test_base
  velox_optimizer_tests_parquet_tpch
  velox_optimizer_tests_plan_matcher
  velox_optimizer_tests_query_test_base
  axiom_runner_tests_utils
  velox_test_connector
  GTest::gmock
  glog::glog
  GTest::gtest
  GTest::gtest_main)

add_executable(velox_sql VeloxSql.cpp)

target_link_libraries(
  velox_sql
  axiom_runner_local_runner
  axiom_runner_multifragment_plan
  velox_verax
  velox_schema_resolver
  velox_hive_connector_metadata
  velox_tpch_connector_metadata
  velox_optimizer_tests_presto_parser
  velox_query_benchmark
  velox_exec_test_lib
  velox_dwio_common
  velox_dwio_parquet_reader
  velox_dwio_native_parquet_reader
  velox_parse_parser
  velox_parse_expression
  velox_parse_utils)

add_subdirectory(utils)

add_executable(velox_model_test ModelTest.cpp)

add_test(velox_model_test velox_model_test)

target_link_libraries(
  velox_model_test
  velox_md_model
  Boost::headers
  glog::glog
  GTest::gtest
  GTest::gtest_main
  pthread)
