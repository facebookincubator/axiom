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

#include <ostream>
#include "axiom/logical_plan/LogicalPlanNode.h"

namespace facebook::axiom::logical_plan {

/// Generates DOT (Graphviz) representation of a logical plan tree.
/// The output can be rendered to SVG using the 'dot' command:
///   dot -Tsvg output.dot -o output.svg
class LogicalPlanDotPrinter {
 public:
  /// Prints the logical plan tree rooted at 'root' in DOT format.
  /// The tree flows top-down with the root at the top.
  static void print(const LogicalPlanNode& root, std::ostream& out);
};

} // namespace facebook::axiom::logical_plan
