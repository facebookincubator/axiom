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

add_executable(
  axiom_system_connector_test
  CoordinatorSchedulingTest.cpp
  InformationSchemaTest.cpp
  SystemConnectorMetadataTest.cpp
  SystemConnectorSerDeTest.cpp
)

add_test(axiom_system_connector_test axiom_system_connector_test)

target_link_libraries(
  axiom_system_connector_test
  axiom_system_connector
  axiom_connectors
  axiom_tpch_connector_metadata
  axiom_optimizer_tests_query_test_base
  axiom_optimizer_tests_plan_matcher
  axiom_runner_tests_utils
  velox_connector
  velox_exec
  velox_exec_test_lib
  velox_dwio_common_test_utils
  velox_expression
  velox_function_registry
  velox_presto_types
  velox_tpch_connector
  velox_vector_test_lib
  GTest::gmock
  GTest::gtest
  GTest::gtest_main
)
