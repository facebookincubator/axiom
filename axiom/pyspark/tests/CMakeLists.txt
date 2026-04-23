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
  collagen_spark_velox_converter_test
  SparkToAxiomLambdaTest.cpp
  SparkToAxiomSetOperationTest.cpp
  SparkToAxiomWithColumnsRenamedTest.cpp
  SparkToAxiomWithColumnsTest.cpp
  SparkVeloxConverterTest.cpp
)

target_link_libraries(
  collagen_spark_velox_converter_test
  collagen_converter
  collagen_spark_velox_converter
  collagen_proto
  axiom_logical_plan
  axiom_test_connector
  velox_connector_registry
  velox_functions_prestosql
  velox_memory
  velox_type
  GTest::gtest
  GTest::gtest_main
)

add_test(NAME collagen_spark_velox_converter_test COMMAND collagen_spark_velox_converter_test)
