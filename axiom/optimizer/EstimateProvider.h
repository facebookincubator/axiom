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

#include <optional>

#include <folly/container/F14Map.h>

#include "axiom/optimizer/Filters.h"
#include "axiom/optimizer/Node.h"

namespace facebook::axiom::optimizer {

/// Cardinality and per-column constraints of an IR subtree whose shape is
/// fixed (a join-cluster leaf or barrier subtree, not a set reordered by
/// DPhyp). `cardinality` is the estimated row count.
/// `constraints` holds the refined per-column `Value` (ndv, min/max,
/// null/true fraction) keyed by `expr->id()`, so a downstream join's fanout
/// uses post-filter NDV rather than base-table NDV. Columns absent from
/// `constraints` fall back to `Column::value()`.
struct Estimate {
  /// Estimated row count; nullopt when unknown, at least 1 when known. A
  /// missing input statistic propagates rather than being replaced by a
  /// concrete fallback.
  std::optional<float> cardinality;

  /// Refined per-column `Value` (ndv, min/max, null/true fraction) keyed by
  /// `expr->id()`. Columns absent here fall back to `Column::value()`.
  ConstraintMap constraints;
};

/// Bottom-up cardinality + constraint estimator over the tree IR, memoized by
/// node, using the shared `conjunctsSelectivity` / `JoinConstraints`
/// primitives.
///
/// Derives each node from its inputs and a `Values` from its rows. Two leaves
/// take facts from outside: a `Scan` from the connector, written onto its
/// `BaseTable` and columns, and a `WorkingTable` from its loop's state,
/// through `estimateLoopState`.
///
/// One instance can serve every reader of the same IR (it is keyed by
/// `NodeCP`, which is globally stable), so each node is estimated once. Its
/// memo stays valid while the statistics of the nodes it read do not change,
/// including a loop's state: a read of the state estimated before
/// `estimateLoopState` stays unknown.
///
/// Usage: `const Estimate& e = estimateProvider.estimate(node);`
class EstimateProvider {
 public:
  /// Returns the memoized estimate for the subtree rooted at `node`,
  /// computing it (and its inputs) on first request.
  const Estimate& estimate(NodeCP node);

  /// Returns the estimated state of `loop`, computing it on the first call and
  /// keeping it for the nodes that return the state.
  ///
  /// A loop's state is the rows one iteration hands to the next. The anchor's
  /// rows feed the first step, and each step's output feeds the next step. For
  /// example, in k-means the state is the centroids: the anchor picks the
  /// initial ones, and each step computes new centroids from the old.
  ///
  /// One estimate describes the state in every iteration:
  /// 1. Start from the estimate of `anchor`, the loop's anchor, placed or not:
  ///    placement does not change its estimate.
  /// 2. Estimate the step three times, each time reading the previous output.
  /// 3. If every output's row count is known and at most the anchor's, the
  ///    state keeps the anchor's row count. Each column combines its
  ///    statistics across the anchor and the outputs as a union's column does,
  ///    except its NDV, which is the largest of them. Otherwise the state is
  ///    unknown.
  ///
  /// The loop's latest-delta reads return the state, and so does `loop` in
  /// replace mode. Estimating `loop` computes its state from its own anchor. A
  /// read cannot reach its loop, so call this before estimating any read of
  /// the loop's state, including placed copies.
  const Estimate& estimateLoopState(const FixedPoint& loop, NodeCP anchor);

 private:
  // Computes the estimate for `node` from its inputs' estimates.
  Estimate compute(NodeCP node);

  // Returns the state of the loop whose first state column is 'key', from
  // this provider or its parents, or null when none has it.
  const Estimate* findLoopState(ColumnCP key) const;

  // Node map for pointer-stable references across recursive insertion.
  folly::F14NodeMap<NodeCP, Estimate> byNode_;

  // Each loop's estimated state (see `estimateLoopState`), over the loop's
  // state columns. These are the loop's output columns, and every read of the
  // state carries them as `stateColumns`. They belong to one loop, and its
  // placed copies keep them, so the first of them is the key. Node map for
  // pointer-stable references across nested insertion.
  folly::F14NodeMap<ColumnCP, Estimate> byLoop_;

  // Set on the scratch provider that estimates one round of a loop's step:
  // the provider estimating the loop, which estimates every node that reads no
  // state and holds the state of each enclosing loop.
  EstimateProvider* parent_{nullptr};
};

} // namespace facebook::axiom::optimizer
