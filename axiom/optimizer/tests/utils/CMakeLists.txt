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

add_library(velox_optimizer_test_utils DfFunctions.cpp RowFunctions.cpp)

target_link_libraries(
  velox_optimizer_test_utils
  velox_verax
  velox_fe_logical_plan_builder
  velox_test_connector
  velox_schema_resolver
  velox_tpch_gen
  velox_connector_split_source
  velox_hive_connector_metadata
  velox_exec_runner_test_util
  velox_exec_test_lib
  velox_dwio_parquet_reader
  velox_dwio_native_parquet_reader)
