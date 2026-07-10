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
  axiom_optimizer_v2
  AggregateRecovery.cpp
  AlgebraicProperties.cpp
  Builder.cpp
  CostModel.cpp
  DecorrelatePass.cpp
  DPhyp.cpp
  EmitPass.cpp
  EstimateLeafStatsPass.cpp
  EstimateProvider.cpp
  ExpandAggregatePass.cpp
  ExprEmitter.cpp
  ExprFactory.cpp
  ExprSimplifier.cpp
  HypergraphBuilder.cpp
  JoinCondition.cpp
  JoinFanout.cpp
  JoinHypergraph.cpp
  JoinTreeEmitter.cpp
  LimitAndOrderPass.cpp
  MemoOp.cpp
  Node.cpp
  NodePrinter.cpp
  Optimize.cpp
  PhysicalProperties.cpp
  PlanPhysicalPass.cpp
  PlanSet.cpp
  PrecomputeProjectionsPass.cpp
  PushdownAndPrunePass.cpp
  ScanHandle.cpp
  TranslatePass.cpp
)

target_link_libraries(
  axiom_optimizer_v2
  axiom_common
  axiom_connectors
  axiom_logical_plan
  axiom_optimizer
  velox_common_base
  velox_core
  velox_exec
  velox_expression
  velox_vector
)
