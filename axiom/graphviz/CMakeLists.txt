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
  axiom_graphviz_printers
  DerivedTableDotPrinter.cpp
  LogicalPlanDotPrinter.cpp
  MultiFragmentPlanDotPrinter.cpp
)

target_link_libraries(
  axiom_graphviz_printers
  axiom_logical_plan
  axiom_optimizer_core
  axiom_optimizer_v1
  axiom_optimizer_multifragment_plan
  velox_core
)
