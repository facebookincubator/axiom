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

add_subdirectory(third-party/protos)
add_subdirectory(runners)

if(AXIOM_BUILD_TESTING)
  add_subdirectory(tests)
endif()

add_library(collagen_arrow ArrowIpcLib.cpp)

target_link_libraries(collagen_arrow velox_arrow_bridge velox_vector velox_memory Arrow::arrow)

add_library(collagen_spark_velox_converter SparkVeloxConverter.cpp)

target_link_libraries(
  collagen_spark_velox_converter
  axiom_logical_plan
  collagen_proto
  velox_type
  Folly::folly
)

add_library(collagen_converter Optimizer.cpp SparkPlanPrinter.cpp SparkToAxiom.cpp)

target_link_libraries(
  collagen_converter
  collagen_arrow
  collagen_spark_velox_converter
  collagen_proto
  axiom_common
  axiom_connectors
  axiom_logical_plan
  axiom_optimizer
  velox_exec
  velox_expression
  velox_function_registry
  velox_memory
  velox_type
  velox_vector
  Folly::folly
)

add_library(collagen_service CollagenService.cpp)

target_link_libraries(
  collagen_service
  collagen_arrow
  collagen_converter
  collagen_proto
  collagen_runners
  axiom_optimizer
  velox_memory
  Folly::folly
  gRPC::grpc++
)

add_library(collagen_main CollagenMain.cpp)

target_link_libraries(
  collagen_main
  collagen_service
  collagen_runners
  axiom_connectors
  axiom_hive_connector_metadata
  axiom_tpch_connector_metadata
  axiom_test_connector
  axiom_optimizer
  velox_connector_registry
  velox_hive_connector
  velox_tpch_connector
  velox_dwio_common
  velox_dwio_dwrf_reader
  velox_dwio_dwrf_writer
  velox_dwio_parquet_reader
  velox_functions_prestosql
  velox_aggregates
  velox_presto_serializer
  velox_memory
  Folly::folly
  gRPC::grpc++
  gflags::gflags
)

add_executable(collagen_server CollagenServer.cpp)

target_link_libraries(collagen_server collagen_main Folly::folly gflags::gflags)
