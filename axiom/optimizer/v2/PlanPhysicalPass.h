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

namespace facebook::velox::core {
class ExpressionEvaluator;
} // namespace facebook::velox::core

namespace facebook::axiom::optimizer::v2 {

/// Turns the logical tree into a distributed physical plan.
class PlanPhysicalPass {
 public:
  /// Returns the distributed physical plan for `root`. In one bottom-up walk it
  /// chooses cost-based join order (DPhyp over maximal chains of reorderable
  /// equi-joins) and the distribution of aggregations and order/limit
  /// operators, inserting global and driver-scoped exchanges so each
  /// operator's input satisfies its required partitioning.
  ///
  /// Automatic fallbacks preserve operators that cannot be safely reordered.
  /// When DPhyp cannot cost an inner-join cluster, the fallback retains
  /// relation order except that it takes an equi-joinable relation before
  /// introducing a cross join. Available size estimates choose the build side.
  /// At `numWorkers > 1`, broadcast, repartition, or gather exchanges provide a
  /// valid distributed input combination.
  ///
  /// When `options.syntacticJoinOrder` is true, cost-based reordering is
  /// disabled and every join keeps both the tree and input orientation written
  /// in the query.
  /// `options.dphypEnumerationBudget` caps DPhyp's enumeration before it falls
  /// back to greedy join ordering; <= 0 means unlimited. `numWorkers` is the
  /// target task count; when > 1 the walk generates global-exchange candidates
  /// so operators co-partition, gather, or broadcast their inputs. `numDrivers`
  /// is the per-worker driver (local pipeline) count; when > 1 the walk inserts
  /// driver-scoped hash, connector, or gather exchanges where an operator
  /// requires co-location within a task. `numWorkers` and `numDrivers` are
  /// per-plan properties, not `OptimizerOptions` fields, so they are passed
  /// separately. `evaluator` folds constants exposed while projection
  /// expressions merge.
  static NodeCP run(
      NodeCP root,
      Builder& builder,
      velox::core::ExpressionEvaluator& evaluator,
      const OptimizerOptions& options,
      int32_t numWorkers,
      int32_t numDrivers);
};

} // namespace facebook::axiom::optimizer::v2
