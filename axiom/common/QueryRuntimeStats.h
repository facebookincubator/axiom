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

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

#include <folly/dynamic.h>

#include "axiom/common/RuntimeStatsSink.h"

namespace facebook::axiom {

/// Per-query collector of runtime metrics from Axiom's pipeline (parser,
/// optimizer, split manager, connectors, runner). Metrics are grouped by
/// component: identically-named metrics from different components stay
/// distinct, while metrics within a component aggregate. Components record
/// through an isolated RuntimeStatsSink and never touch the collector or one
/// another. Thread-safe: many components and threads may record concurrently.
///
/// Example:
///   QueryRuntimeStats stats;
///   auto sink = stats.sinkFor(optimizer::kStatsComponent);
///   sink.recordCount("axiom-candidatesPruned", 3);
///   auto snapshot = stats.toMap();
class QueryRuntimeStats {
 public:
  /// Returns 'stats' sink for 'component', or a sink over a throwaway map when
  /// 'stats' is null.
  static RuntimeStatsSink sinkOrThrowaway(
      const std::shared_ptr<QueryRuntimeStats>& stats,
      std::string_view component);

  /// Returns a write-only sink for 'component', creating its view on first use.
  RuntimeStatsSink sinkFor(std::string_view component);

  /// Returns a snapshot of all metrics across components. Each key is the
  /// metric name qualified by its component id ("<component>/<name>"), so a
  /// name emitted by more than one component stays a distinct entry. Safe to
  /// call during execution; reflects everything recorded so far.
  std::unordered_map<std::string, velox::RuntimeMetric> toMap() const;

  /// Serializes the per-query totals to folly::dynamic for logging, keyed by
  /// the bare metric name. Format:
  /// {"name": {"sum": N, "count": N, "min": N, "max": N, "unit": "..."}}
  folly::dynamic toDynamic() const;

  /// Returns each metric summed across all components, keyed by the bare metric
  /// name (unlike toMap(), which keys per component). Components emitting the
  /// same name must use the same unit; a mismatch fails at report time via a
  /// VELOX_CHECK in RuntimeMetric::merge.
  std::unordered_map<std::string, velox::RuntimeMetric> totalsByName() const;

 private:
  // Component id -> that component's metric map. shared_ptr so a
  // RuntimeStatsSink co-owns its map independently of the collector's lifetime.
  folly::ConcurrentHashMap<std::string, std::shared_ptr<RuntimeMetricMap>>
      components_;
};

} // namespace facebook::axiom
