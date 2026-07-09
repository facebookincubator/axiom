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

#include "axiom/logical_plan/LogicalPlanNode.h"
#include "axiom/optimizer/Schema.h"
#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/Node.h"
#include "velox/core/Expressions.h"

namespace facebook::axiom::optimizer::v2 {

/// Translates a logical plan into the tree-IR.
class TranslatePass {
 public:
  /// Output of `run`. `outputColumns` and `outputNames` describe the
  /// user-visible output layout, aligned 1:1 with each other. `outputColumns`
  /// may contain duplicate `Column*` entries (e.g., `SELECT v, v`).
  struct Result {
    NodeCP root;
    ColumnVector outputColumns;
    std::vector<std::string> outputNames;
  };

  /// Translates 'plan' into a tree-IR plus its user-visible output layout.
  /// Looks up base tables via 'schema' for Scan nodes. Uses 'evaluator' to
  /// constant-fold call expressions whose arguments are all literals. Uses
  /// 'builder' to hash-cons tree-IR nodes; share a single Builder across
  /// translate and downstream rewrites so identical sub-trees dedup across
  /// passes. Throws VELOX_NYI for logical_plan node or expression types not
  /// yet supported.
  ///
  /// 'schema' must outlive the returned IR (and any Velox plan emitted from
  /// it). The IR's `BaseTable` nodes hold raw pointers to `TableLayout`s
  /// owned by `schema`'s connector::Table entries; dropping `schema` before
  /// emit leaves those pointers dangling.
  ///
  /// 'evaluator' and the memory pool it owns must outlive the returned IR
  /// and any Velox plan derived from it. Compilation of foldable expressions
  /// can allocate vectors on the evaluator's pool that are then referenced
  /// from downstream `emit` outputs.
  static Result run(
      const logical_plan::LogicalPlanNode& plan,
      optimizer::Schema& schema,
      velox::core::ExpressionEvaluator& evaluator,
      Builder& builder);
};

} // namespace facebook::axiom::optimizer::v2
