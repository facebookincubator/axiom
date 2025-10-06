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

add_library(
  axiom_logical_plan
  Expr.cpp
  ExprPrinter.cpp
  LogicalPlanNode.cpp
  PlanPrinter.cpp
  ExprApi.cpp
)

target_link_libraries(axiom_logical_plan velox_type)

add_library(axiom_logical_plan_builder PlanBuilder.cpp NameAllocator.cpp NameMappings.cpp)

target_link_libraries(
  axiom_logical_plan_builder
  axiom_logical_plan
  axiom_connectors
  velox_exec
  velox_function_registry
  velox_functions_prestosql
  velox_aggregates
  velox_parse_expression
  velox_parse_parser
  velox_parse_utils
)

if(AXIOM_BUILD_TESTING)
  add_subdirectory(tests)
endif()
