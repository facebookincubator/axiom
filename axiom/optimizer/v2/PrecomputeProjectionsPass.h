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

#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/Node.h"

namespace facebook::axiom::optimizer::v2 {

/// Lifts compound expressions into `Project`s wherever Velox requires a column
/// or literal.
class PrecomputeProjectionsPass {
 public:
  /// Returns the tree rooted at 'node' rewritten so every position where Velox
  /// demands a `FieldAccessTypedExpr` (or, where allowed, a constant) carries a
  /// `Column` (or `Literal`) instead of a compound expression. Such compound
  /// expressions are lifted into a `Project` inserted between the consumer and
  /// its input, and the consumer is rebuilt to reference the projected column.
  /// Affected consumer kinds and positions:
  ///   - Aggregate: grouping keys, aggregate args, FILTER mask, ORDER BY keys
  ///   - Window:    partition keys, order keys, function args
  ///   - Sort:      order keys
  ///   - Unnest:    unnest expressions
  /// Returns the original tree unchanged when no lifting is required.
  static NodeCP run(NodeCP node, Builder& builder);
};

} // namespace facebook::axiom::optimizer::v2
