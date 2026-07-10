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

#include "axiom/optimizer/v2/ScanHandle.h"

namespace facebook::axiom::optimizer::v2 {

/// Annotates base tables with connector filtered-table statistics.
class EstimateLeafStatsPass {
 public:
  /// For each base table reachable from 'root', builds (and caches in
  /// 'scanHandles') the connector handle, calls
  /// `TableLayout::co_estimateStats`, and writes the post-filter row count into
  /// `BaseTable::filteredCardinality` and the per-column min/max/ndv into each
  /// `Column::value()`, in place; join-order planning then reads these via
  /// `EstimateProvider`. Runs after filter pushdown and before join-order
  /// planning. The caller gates this on the `useFilteredTableStats` option;
  /// when it does not run, or a connector returns no stats for a table, that
  /// table's `filteredCardinality` stays 0 and `EstimateProvider` falls back to
  /// constraint-based selectivity. 'session' supplies connector sessions;
  /// 'evaluator' constant-folds filters during handle construction.
  static void run(
      NodeCP root,
      const OptimizerSession& session,
      velox::core::ExpressionEvaluator& evaluator,
      ScanHandleCache& scanHandles);
};

} // namespace facebook::axiom::optimizer::v2
