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
  velox_connector_split_source
  velox_hive_connector_metadata
  velox_exec_runner_test_util
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
  velox_connector_split_source
  velox_hive_connector_metadata
  velox_exec_runner_test_util
  velox_exec_test_lib
  velox_dwio_parquet_reader
  velox_dwio_native_parquet_reader
  gtest
  gtest_main)

velox_add_library(velox_optimizer_tests_query_sql_parser QuerySqlParser.cpp)

velox_link_libraries(
  velox_optimizer_tests_query_sql_parser
  velox_fe_logical_plan_builder
  velox_parse_parser
  velox_vector)

add_executable(velox_optimizer_tests_tpch_plan TpchPlanTest.cpp)

add_test(velox_optimizer_tests_tpch_plan velox_optimizer_tests_tpch_plan)

target_link_libraries(
  velox_optimizer_tests_tpch_plan
  velox_dwio_common_test_utils
  velox_optimizer_tests_parquet_tpch
  velox_optimizer_tests_query_sql_parser
  velox_optimizer_tests_query_test_base)

add_executable(
  velox_plan_test
  HiveLimitQueriesTest.cpp
  PlanMatcher.cpp
  PlanTest.cpp
  Tpch.cpp
  ParquetTpchTest.cpp
  QueryTestBase.cpp
  LogicalSubfieldTest.cpp
  FeatureGen.cpp
  Genies.cpp
  SchemaResolverTest.cpp)

add_test(velox_plan_test velox_plan_test)

target_link_libraries(
  velox_plan_test
  velox_optimizer_test_utils
  velox_verax
  velox_fe_logical_plan_builder
  velox_optimizer_tests_parquet_tpch
  velox_optimizer_tests_query_test_base
  velox_test_connector
  GTest::gmock
  glog::glog
  GTest::gtest
  GTest::gtest_main)

add_executable(velox_sql VeloxSql.cpp)

target_link_libraries(
  velox_sql
  velox_local_runner
  velox_verax
  velox_schema_resolver
  velox_connector_split_source
  velox_hive_connector_metadata
  velox_optimizer_tests_query_sql_parser
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
