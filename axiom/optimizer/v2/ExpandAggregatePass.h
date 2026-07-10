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

/// Lowers DISTINCT aggregates into a shape Velox can execute.
class ExpandAggregatePass {
 public:
  /// Returns `root` with each DISTINCT aggregate lowered to
  /// `Aggregate(MarkDistinct(input, ...))`, where `MarkDistinct` emits a
  /// boolean marker per unique `(groupingKeys, args)` tuple and each
  /// `agg(DISTINCT x)` is rewritten to `agg(x) FILTER (marker)`. Aggregates
  /// that share one distinct signature keep native Velox distinct instead. The
  /// group-id column of a grouping-set aggregate is already one of the grouping
  /// keys (grouping sets are lowered to `GroupId` in translate), so distinct
  /// dedup is per grouping set. Other node shapes pass through unchanged. Runs
  /// after `PlanPhysicalPass` and `PrecomputeProjectionsPass`, before
  /// `EmitPass`.
  static NodeCP run(NodeCP root, Builder& builder);
};

} // namespace facebook::axiom::optimizer::v2
