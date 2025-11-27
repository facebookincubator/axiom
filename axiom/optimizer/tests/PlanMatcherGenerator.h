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

#pragma once

#include <string>
#include "velox/core/PlanNode.h"

namespace facebook::velox::core {

/// Generates C++ code that constructs a PlanMatcher tree matching the given
/// PlanNode tree.
///
/// @param planNode The PlanNode tree to generate matcher code for.
/// @param builderVarName The name of the PlanMatcherBuilder variable to use
///                       in the generated code (default: "builder").
/// @return A string containing C++ code that uses PlanMatcherBuilder to
///         construct a matcher tree.
///
/// Example:
///   auto plan = ...; // some PlanNode tree
///   std::string code = generatePlanMatcherCode(plan);
///   // code will contain something like:
///   // core::PlanMatcherBuilder()
///   //   .tableScan("t")
///   //   .filter("a > 10")
///   //   .project({"a", "b"})
///   //   .build();
std::string generatePlanMatcherCode(
    const PlanNodePtr& planNode,
    const std::string& builderVarName = "builder");

} // namespace facebook::velox::core
