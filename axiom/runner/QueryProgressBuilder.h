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

#include <cstdint>
#include <string_view>
#include <vector>

#include "axiom/runner/QueryProgress.h"

namespace facebook::velox::exec {
struct TaskStats;
} // namespace facebook::velox::exec

namespace facebook::axiom::optimizer {
struct ExecutableFragment;
} // namespace facebook::axiom::optimizer

namespace facebook::axiom::runner {

/// Stateless functions that build a QueryProgress from a runner's plan
/// fragments and a task-stats snapshot. Derive the stage topology once, then
/// combine it with each stats snapshot:
///
/// @code
///   auto stages = QueryProgressBuilder::toStageTopology(runner.fragments());
///   auto progress = QueryProgressBuilder::computeProgress(
///       stages, runner.stats(), queryId, wallMicros, finished);
/// @endcode
class QueryProgressBuilder {
 public:
  /// One stage's topology: the indexes of the stages that feed it over an
  /// exchange.
  struct StageTopology {
    /// Indexes of the stages feeding this one over an exchange, by position in
    /// the returned vector (which matches stats()); empty for a leaf.
    std::vector<int32_t> producers;
  };

  /// Converts the plan's fragments to per-stage topology, where stage i
  /// corresponds to fragments[i] and to stats()[i]. Every exchange input must
  /// name a known producer fragment; an unresolved prefix throws.
  static std::vector<StageTopology> toStageTopology(
      const std::vector<facebook::axiom::optimizer::ExecutableFragment>&
          fragments);

  /// Combines the stage topology with a task-stats snapshot into a
  /// QueryProgress. `stages` is positionally 1:1 with `taskStats` by fragment
  /// order; a size mismatch throws. `wallMicros` is elapsed time since query
  /// start. `finished` is whether the runner has completed; on a clean finish
  /// the counters are normalized to a consistent completed frame.
  static QueryProgress computeProgress(
      const std::vector<StageTopology>& stages,
      const std::vector<facebook::velox::exec::TaskStats>& taskStats,
      std::string_view queryId,
      int64_t wallMicros,
      bool finished);
};

} // namespace facebook::axiom::runner
