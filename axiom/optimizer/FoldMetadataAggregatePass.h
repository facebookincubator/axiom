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

#include "axiom/optimizer/Builder.h"
#include "axiom/optimizer/OptimizerSession.h"

namespace facebook::axiom::optimizer {

/// Resolves metadata aggregates (an `optimizer::Aggregate` with a
/// `specialKind`). For an aggregation whose aggregates are all metadata
/// aggregates, directly over a `Scan`, it asks the connector for the counts
/// (`TableLayout::co_metadataCounts`) and, on success, replaces the aggregation
/// with a `Values` node. The connector decides whether it can group the counts
/// by the grouping keys and declines otherwise. A scan sampled with
/// TABLESAMPLE SYSTEM (`BaseTable::sampledPercentage`) is not folded: that
/// sampling happens at split generation, where the connector cannot see it.
/// An aggregate with a FILTER is not folded. When the fold does not apply,
/// each metadata aggregate that allows a fallback is replaced by the
/// registered count over its argument and FILTER; any other is an error.
///
/// Runs after `PushdownAndPrunePass`, which guarantees a `Scan` carries all of
/// its filters (no `Filter` node sits directly over it). Every metadata
/// aggregate is either folded, replaced by a count, or rejected here, so
/// none reaches emit, where the kind name is not a Velox aggregate.
class FoldMetadataAggregatePass {
 public:
  static NodeCP run(
      NodeCP root,
      Builder& builder,
      const OptimizerSession& session,
      velox::core::ExpressionEvaluator& evaluator);
};

} // namespace facebook::axiom::optimizer
