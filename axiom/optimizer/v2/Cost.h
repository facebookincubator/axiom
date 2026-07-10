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

namespace facebook::axiom::optimizer::v2 {

/// Default per-input-row fanout assumed for an UNNEST when per-array
/// size statistics are unavailable. The cardinality of an Unnest
/// relation and the cost of its cross-join-unnest edge must use the
/// same value so the two estimates agree.
constexpr float kDefaultUnnestFanout = 10;

/// Cost of a candidate physical plan or subplan, used to score memo
/// entries during cost-based planning. The comparable score lives in
/// 'cost'; 'cardinality' propagates into the cost of consumers above.
struct Cost {
  /// Total estimated cost, or nullopt when unknown. Lower is better; used to
  /// compare memo entries with equivalent logical content. Unknown propagates:
  /// a cost derived from an unknown cardinality is itself unknown, so candidate
  /// ranking must treat unknown as not-comparable rather than substituting a
  /// concrete fallback.
  std::optional<float> cost;

  /// Estimated output row count, or nullopt when unknown (a missing
  /// input statistic propagates rather than being replaced by a concrete
  /// fallback). Feeds into 'cost' of consumers that scan or process this
  /// subplan's output.
  std::optional<float> cardinality;
};

} // namespace facebook::axiom::optimizer::v2
