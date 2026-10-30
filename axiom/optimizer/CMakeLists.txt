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

if(AXIOM_BUILD_TESTING OR AXIOM_BUILD_TEST_UTILS)
  add_subdirectory(tests)
endif()

add_library(
  axiom_optimizer_multifragment_plan
  MultiFragmentPlan.cpp
  MultiFragmentPlanPrinter.cpp
  NodePrediction.cpp
)

target_link_libraries(
  axiom_optimizer_multifragment_plan
  velox_common_base
  velox_memory
  velox_core
  Folly::folly
  fmt::fmt
)

add_library(
  axiom_optimizer_core
  BitSet.cpp
  ConstantExprEvaluator.cpp
  ConstantFold.cpp
  Cost.cpp
  Domain.cpp
  ExplainIo.cpp
  ExprEstimator.cpp
  Filters.cpp
  FunctionRegistry.cpp
  JoinConstraints.cpp
  OptimizerOptions.cpp
  PlanAndStats.cpp
  PlanObject.cpp
  PlanUtils.cpp
  QueryGraph.cpp
  QueryGraphContext.cpp
  Schema.cpp
  SelectivityEngine.cpp
  StatsFilterSelectivityEstimator.cpp
  ToSubfield.cpp
  UnionConstraints.cpp
  WriteStatsBuilder.cpp
)

target_link_libraries(
  axiom_optimizer_core
  velox_core
  velox_exec
  axiom_common
  axiom_connectors
  axiom_logical_plan
  axiom_optimizer_multifragment_plan
  velox_async_rpc_function_registry
  velox_connector
  velox_function_registry
)

add_subdirectory(v1)
add_subdirectory(v2)
