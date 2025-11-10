/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <iostream>
#include "axiom/optimizer/tests/PlanMatcherGenerator.h"

/// Example usage of generatePlanMatcherCode
///
/// This example shows how to use the PlanMatcherGenerator to convert a
/// PlanNode tree into C++ code that constructs an equivalent PlanMatcher.
///
/// Usage:
///   // Given a PlanNode tree:
///   auto plan = ...; // your PlanNode tree
///
///   // Generate the matcher code:
///   std::string matcherCode =
///   facebook::velox::core::generatePlanMatcherCode(plan);
///
///   // Print or save the generated code:
///   std::cout << matcherCode << std::endl;
///
/// The generated code will look something like:
///
///   auto builder = core::PlanMatcherBuilder()
///     .tableScan("my_table")
///     .filter("age > 18")
///     .project({"name", "age * 2 AS double_age"})
///     .singleAggregation({"name"}, {"count(*) AS cnt"})
///     .build();
///
/// You can then copy this code into your test file and use it to match plans.

int main() {
  std::cout << "PlanMatcherGenerator - Example Usage" << std::endl;
  std::cout << "=====================================" << std::endl;
  std::cout << std::endl;
  std::cout << "To use the PlanMatcherGenerator:" << std::endl;
  std::cout << std::endl;
  std::cout << "1. Include the header:" << std::endl;
  std::cout << "   #include \"axiom/optimizer/tests/PlanMatcherGenerator.h\""
            << std::endl;
  std::cout << std::endl;
  std::cout << "2. Call the function with your PlanNode:" << std::endl;
  std::cout << "   auto plan = ...; // your PlanNode tree" << std::endl;
  std::cout
      << "   std::string code = facebook::velox::core::generatePlanMatcherCode(plan);"
      << std::endl;
  std::cout << std::endl;
  std::cout << "3. Use the generated code in your tests:" << std::endl;
  std::cout << "   std::cout << code << std::endl;" << std::endl;
  std::cout << std::endl;
  std::cout << "Example output:" << std::endl;
  std::cout << "  auto matcher = core::PlanMatcherBuilder()" << std::endl;
  std::cout << "    .tableScan(\"employees\")" << std::endl;
  std::cout << "    .filter(\"salary > 50000\")" << std::endl;
  std::cout << "    .project({\"name\", \"salary\"})" << std::endl;
  std::cout << "    .build();" << std::endl;
  std::cout << std::endl;

  return 0;
}
