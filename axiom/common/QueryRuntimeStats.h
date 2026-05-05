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

#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/container/F14Map.h>
#include <folly/dynamic.h>
#include <chrono>
#include <string>
#include <string_view>

#include "velox/common/base/RuntimeMetrics.h"

namespace facebook::axiom {

/// Accumulates runtime metrics from Axiom's query pipeline (parser, optimizer,
/// split manager, runner). Each metric is a velox::RuntimeMetric with
/// sum/count/min/max. Thread-safe — multiple pipeline stages may record
/// concurrently.
class QueryRuntimeStats {
 public:
  // Parser.
  static constexpr std::string_view kParseWallNanos{"axiom-parseWallNanos"};

  // Optimizer.
  static constexpr std::string_view kOptimizeWallNanos{
      "axiom-optimizeWallNanos"};
  static constexpr std::string_view kOptimizeToGraphWallNanos{
      "axiom-optimizeToGraphWallNanos"};
  static constexpr std::string_view kOptimizeBestPlanWallNanos{
      "axiom-optimizeBestPlanWallNanos"};
  static constexpr std::string_view kOptimizeToVeloxWallNanos{
      "axiom-optimizeToVeloxWallNanos"};

  // Split manager.
  static constexpr std::string_view kListPartitionsWallNanos{
      "axiom-listPartitionsWallNanos"};
  static constexpr std::string_view kListPartitionsCount{
      "axiom-listPartitionsCount"};
  static constexpr std::string_view kGetSplitsWallNanos{
      "axiom-getSplitsWallNanos"};
  static constexpr std::string_view kGetSplitsCount{"axiom-getSplitsCount"};

  // Permission check.
  static constexpr std::string_view kPermissionCheckWallNanos{
      "axiom-permissionCheckWallNanos"};

  // Connector.
  static constexpr std::string_view kEstimateStatsWallNanos{
      "axiom-estimateStatsWallNanos"};

  // Execution.
  static constexpr std::string_view kExecuteWallNanos{"axiom-executeWallNanos"};

  /// Records a wall-clock duration under the given metric name.
  void recordTiming(std::string_view name, std::chrono::nanoseconds duration);

  /// Records a count (e.g., number of partitions or splits).
  void recordCount(std::string_view name, int64_t value);

  /// Merges a pre-aggregated metric into this recorder.
  void merge(std::string_view name, const velox::RuntimeMetric& metric);

  /// Returns a snapshot of all recorded metrics.
  folly::F14FastMap<std::string, velox::RuntimeMetric> toMap() const;

  /// Serializes all metrics to folly::dynamic for Scribe logging. Format:
  /// {"metricName": {"sum": N, "count": N, "min": N, "max": N, "unit": "..."}}
  folly::dynamic toDynamic() const;

 private:
  folly::ConcurrentHashMap<std::string, velox::RuntimeMetric> metrics_;
};

/// RAII timer that records a wall-clock duration into QueryRuntimeStats on
/// destruction. Guarantees the metric is recorded even if the timed scope
/// exits via exception.
class ScopedRuntimeStatsTimer {
 public:
  ScopedRuntimeStatsTimer(QueryRuntimeStats& stats, std::string_view metricName)
      : stats_(stats),
        metricName_(metricName),
        start_(std::chrono::steady_clock::now()) {}

  ~ScopedRuntimeStatsTimer() {
    stats_.recordTiming(metricName_, std::chrono::steady_clock::now() - start_);
  }

  ScopedRuntimeStatsTimer(const ScopedRuntimeStatsTimer&) = delete;
  ScopedRuntimeStatsTimer& operator=(const ScopedRuntimeStatsTimer&) = delete;

 private:
  QueryRuntimeStats& stats_;
  std::string_view metricName_;
  std::chrono::steady_clock::time_point start_;
};

/// Merges 'metric' into 'map' under 'name' using CAS-based optimistic locking.
/// Shared by QueryRuntimeStats and SchedulerStatsRecorder.
void mergeRuntimeMetric(
    folly::ConcurrentHashMap<std::string, velox::RuntimeMetric>& map,
    const std::string& name,
    const velox::RuntimeMetric& metric);

} // namespace facebook::axiom
