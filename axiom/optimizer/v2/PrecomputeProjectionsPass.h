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

/// Moves expressions a consumer references into a `Project` over its input.
class PrecomputeProjectionsPass {
 public:
  /// Returns the tree rooted at 'node' rewritten so the expressions listed
  /// below are computed by a `Project` inserted between the consumer and its
  /// input, with the consumer rebuilt to reference the projected column.
  ///
  /// Most positions are moved because Velox demands a `FieldAccessTypedExpr`
  /// (or, where allowed, a constant) there:
  ///   - Aggregate: grouping keys, aggregate args, FILTER mask, ORDER BY keys
  ///   - Window: partition keys, order keys, function args, frame bounds
  ///   - Sort, TopN: order keys
  ///   - RowNumber: partition keys
  ///   - TopNRowNumber: partition keys, order keys
  ///   - Unnest: unnest expressions
  ///   - Join: join keys
  ///
  /// A `UnionAll` is an exception: its legs are aligned to the union's output
  /// columns because Velox's `LocalPartition` requires one shared output
  /// `RowType`.
  ///
  /// Returns the original tree unchanged when nothing needs to move.
  static NodeCP run(NodeCP node, Builder& builder);
};

} // namespace facebook::axiom::optimizer::v2
