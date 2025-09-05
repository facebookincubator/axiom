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

add_executable(
  axiom_lp_tests
  NameAllocatorTest.cpp
  NameMappingsTest.cpp
  ExprApiTest.cpp
  PlanBuilderTest.cpp
  PlanPrinterTest.cpp)

add_test(axiom_lp_tests axiom_lp_tests)

target_link_libraries(
  axiom_lp_tests
  velox_fe_logical_plan_builder
  velox_fe_logical_plan
  velox_test_connector
  GTest::gtest
  GTest::gtest_main
  GTest::gmock)
