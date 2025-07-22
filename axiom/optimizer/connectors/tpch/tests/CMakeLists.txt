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
add_executable(velox_tpch_connector_metadata_test TpchConnectorMetadataTest.cpp)

add_test(velox_tpch_connector_metadata_test velox_tpch_connector_metadata_test)

target_link_libraries(
  velox_tpch_connector_metadata_test
  velox_tpch_connector_metadata
  velox_tpch_connector
  velox_exec_runner_test_util
  velox_vector_fuzzer
  velox_vector_test_lib
  velox_schema_resolver
  velox_exec
  velox_exec_test_lib
  GTest::gtest
  GTest::gtest_main)
