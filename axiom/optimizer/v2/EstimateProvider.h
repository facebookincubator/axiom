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
#include "axiom/optimizer/v2/Node.h"

namespace facebook::axiom::optimizer::v2 {

/// Cardinality and per-column constraints of an IR subtree whose shape is
/// fixed (a join-cluster leaf or barrier subtree, not a set reordered by
/// DPhyp). `cardinality` is the estimated row count; `constraints` holds the
/// refined per-column `Value` (ndv, min/max, null/true fraction) keyed by
/// `expr->id()`, so a downstream join's fanout uses post-filter NDV rather
/// than base-table NDV. Columns absent from `constraints` fall back to
/// `Column::value()`.
struct Estimate {
  /// Estimated row count; nullopt when unknown. A missing input statistic
  /// propagates rather than being replaced by a concrete fallback.
  std::optional<float> cardinality;

  /// Refined per-column `Value` (ndv, min/max, null/true fraction) keyed by
  /// `expr->id()`. Columns absent here fall back to `Column::value()`.
  ConstraintMap constraints;
};

/// Bottom-up cardinality + constraint estimator over the tree IR, memoized by
/// node, using the shared `conjunctsSelectivity` / `JoinConstraints`
/// primitives.
///
/// One instance is shared across all join clusters of a query (it is keyed
/// by `NodeCP`, which is globally stable), so a leaf subtree — or a
/// hash-consed duplicate — is estimated exactly once, and a barrier-subtree
/// leaf reuses the estimates of the inner cluster it wraps.
///
/// Usage: `const Estimate& e = estimateProvider.estimate(node);`
class EstimateProvider {
 public:
  /// Returns the memoized estimate for the subtree rooted at `node`,
  /// computing it (and its inputs) on first request.
  const Estimate& estimate(NodeCP node);

 private:
  // Computes the estimate for `node` from its inputs' estimates.
  Estimate compute(NodeCP node);

  // Node map for pointer-stable references across recursive insertion.
  folly::F14NodeMap<NodeCP, Estimate> byNode_;
};

} // namespace facebook::axiom::optimizer::v2
