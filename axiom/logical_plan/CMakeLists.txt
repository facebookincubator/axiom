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

velox_add_library(
  velox_fe_logical_plan
  Expr.cpp
  ExprPrinter.cpp
  LogicalPlanNode.cpp
  PlanPrinter.cpp
  ExprApi.cpp)

velox_link_libraries(velox_fe_logical_plan velox_type)

velox_add_library(velox_fe_logical_plan_builder PlanBuilder.cpp
                  NameAllocator.cpp NameMappings.cpp)

velox_link_libraries(
  velox_fe_logical_plan_builder
  velox_fe_logical_plan
  velox_connector_metadata
  velox_exec
  velox_function_registry
  velox_functions_prestosql
  velox_aggregates
  velox_parse_expression
  velox_parse_parser
  velox_parse_utils)

if(AXIOM_BUILD_TESTING)
  add_subdirectory(tests)
endif()
