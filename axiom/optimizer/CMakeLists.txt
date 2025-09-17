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

if(AXIOM_BUILD_TESTING)
  add_subdirectory(tests)
endif()

add_library(axiom_model Model.cpp)

target_link_libraries(
  axiom_model velox_exception)

add_library(axiom_schema_resolver SchemaResolver.cpp SchemaUtils.cpp)

target_link_libraries(
  axiom_schema_resolver axiom_connector_metadata velox_exception velox_vector)

add_library(
  axiom_optimizer
  DerivedTable.cpp
  DerivedTablePrinter.cpp
  ToGraph.cpp
  Subfields.cpp
  Optimization.cpp
  Plan.cpp
  BitSet.cpp
  ParallelExpr.cpp
  PlanObject.cpp
  Schema.cpp
  QueryGraph.cpp
  QueryGraphContext.cpp
  Cost.cpp
  FunctionRegistry.cpp
  PlanUtils.cpp
  RelationOp.cpp
  ToVelox.cpp
  VeloxHistory.cpp
  JoinSample.cpp)

add_dependencies(axiom_optimizer velox_hive_connector)

target_link_libraries(
  axiom_optimizer
  velox_core
  axiom_connector_metadata
  axiom_logical_plan
  axiom_model
  axiom_runner_multifragment_plan
  velox_connector
  axiom_schema_resolver)
