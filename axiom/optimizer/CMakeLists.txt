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

add_library(
  axiom_optimizer
  BitSet.cpp
  ConstantExprEvaluator.cpp
  Cost.cpp
  DerivedTable.cpp
  DerivedTablePrinter.cpp
  FunctionRegistry.cpp
  JoinSample.cpp
  Model.cpp
  Optimization.cpp
  ParallelExpr.cpp
  Plan.cpp
  PlanObject.cpp
  PlanUtils.cpp
  PrecomputeProjection.cpp
  QueryGraph.cpp
  QueryGraphContext.cpp
  RelationOp.cpp
  Schema.cpp
  Subfields.cpp
  ToGraph.cpp
  ToVelox.cpp
  VeloxHistory.cpp
)

target_link_libraries(
  axiom_optimizer
  velox_core
  axiom_common
  axiom_connectors
  axiom_logical_plan
  axiom_runner_multifragment_plan
  velox_connector
)
