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

#include "axiom/optimizer/Node.h"
#include "axiom/optimizer/OptimizerSession.h"
#include "velox/core/QueryCtx.h"

namespace facebook::axiom::optimizer {

class Builder;

/// Applies connector filtered-table statistics and removes proven-empty
/// subtrees.
class EstimateLeafStatsPass {
 public:
  /// For each Scan reachable from 'root', calls
  /// `TableLayout::co_estimateStats` with the handle its `Scan` points at, and
  /// writes the post-filter row count into `BaseTable::filteredCardinality` and
  /// the per-column min/max/ndv into each `Column::value()`, in place;
  /// join-order planning then reads these via `EstimateProvider`. Runs after
  /// filter pushdown and before join-order planning. The caller gates this on
  /// the `useFilteredTableStats` option; when it does not run, or a connector
  /// returns no stats for a table, that table's `filteredCardinality` stays 0
  /// and `EstimateProvider` falls back to constraint-based selectivity.
  /// When connector metadata proves that a scan is empty, propagates that fact
  /// upward and materializes Values where the fact cannot propagate farther or
  /// at the root. 'session' supplies connector sessions.
  static NodeCP run(
      NodeCP root,
      ColumnVector& outputColumns,
      Builder& builder,
      velox::core::ExpressionEvaluator& evaluator,
      const std::shared_ptr<velox::core::QueryCtx>& queryCtx,
      const OptimizerSession& session);
};

} // namespace facebook::axiom::optimizer
