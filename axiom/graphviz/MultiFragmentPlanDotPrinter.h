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
#include "axiom/optimizer/MultiFragmentPlan.h"
#include "axiom/optimizer/NodePrediction.h"

namespace facebook::axiom::graphviz {

/// Generates a DOT language representation of a MultiFragmentPlan for
/// visualization with Graphviz.
///
/// Each fragment is rendered as a labeled box containing:
///   - The fragment's output distribution (e.g. hash[k1, k2], broadcast,
///     arbitrary, gather) read from the root PartitionedOutputNode.
///   - The fragment header: task prefix, FragmentType, and task width.
///   - The fragment's Velox plan tree, indented to preserve structure,
///     with node type names only. ProjectNode nodes and the root
///     PartitionedOutputNode are skipped as structural noise.
///
/// Edges between fragments connect each producer fragment to its consumer
/// fragment. The consumer's Exchange row in the body is annotated with the
/// producer's task prefix so the reader can match incoming edges to the
/// receiving Exchange.
///
/// Usage:
///   std::ofstream file("plan.dot");
///   MultiFragmentPlanDotPrinter::print(plan, file);
///
/// Then render with: dot -Tsvg plan.dot -o plan.svg
class MultiFragmentPlanDotPrinter {
 public:
  /// @param prediction Per-node cardinality estimates. When a
  /// fragment's root has an entry, the estimated row count is appended to
  /// the fragment's "out:" row.
  static void print(
      const optimizer::MultiFragmentPlan& plan,
      const optimizer::NodePredictionMap& prediction,
      std::ostream& out);
};

} // namespace facebook::axiom::graphviz
