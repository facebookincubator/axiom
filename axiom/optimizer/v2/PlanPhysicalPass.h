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

#include "axiom/optimizer/OptimizerOptions.h"
#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/Node.h"

namespace facebook::axiom::optimizer::v2 {

/// Turns the logical tree into a distributed physical plan.
class PlanPhysicalPass {
 public:
  /// Returns the distributed physical plan for `root`. In one bottom-up walk it
  /// chooses cost-based join order (DPhyp over maximal chains of reorderable
  /// equi-joins) and the distribution of aggregations and order/limit
  /// operators, inserting remote exchanges so each operator's input satisfies
  /// its required partitioning.
  ///
  /// Cross joins, theta and decorrelated-subquery joins, and joins whose keys
  /// do not resolve to leaf columns keep their written shape; at
  /// `numWorkers > 1` they still get a valid distributed input combination
  /// (broadcast build).
  ///
  /// When `options.syntacticJoinOrder` is true, cost-based reordering is
  /// disabled and every join keeps the order written in the query. The same
  /// query-order fallback also kicks in per cluster when DPhyp cannot cost a
  /// plan (a relation or join key lacked statistics).
  /// `options.dphypEnumerationBudget` caps DPhyp's enumeration before it falls
  /// back to greedy join ordering; <= 0 means unlimited. `numWorkers` is the
  /// target task count; when > 1 the walk generates remote-exchange candidates
  /// so operators co-partition, gather, or broadcast their inputs. `numDrivers`
  /// is the per-worker driver (local pipeline) count; when > 1, an aggregate
  /// whose input is already co-located on its grouping keys still needs a local
  /// exchange to bring each group to one driver, so it two-stages (partial →
  /// local exchange → final). `numWorkers` and `numDrivers` are per-plan
  /// properties, not `OptimizerOptions` fields, so they are passed separately.
  static NodeCP run(
      NodeCP root,
      Builder& builder,
      const OptimizerOptions& options,
      int32_t numWorkers,
      int32_t numDrivers);
};

} // namespace facebook::axiom::optimizer::v2
