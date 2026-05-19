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

#include <chrono>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>

#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/dynamic.h>

#include "velox/common/base/RuntimeMetrics.h"
#include "velox/common/process/ProcessBase.h"

namespace facebook::axiom {

/// Accumulates runtime metrics from Axiom's query pipeline (parser, optimizer,
/// split manager, runner). Each metric is a velox::RuntimeMetric with
/// sum/count/min/max. Thread-safe — multiple pipeline stages may record
/// concurrently.
class QueryRuntimeStats {
 public:
  // Parser.
  static constexpr std::string_view kParseWallNanos{"axiom-parseWallNanos"};
  static constexpr std::string_view kParseCpuNanos{"axiom-parseCpuNanos"};

  // Optimizer.
  static constexpr std::string_view kOptimizeWallNanos{
      "axiom-optimizeWallNanos"};
  static constexpr std::string_view kOptimizeCpuNanos{"axiom-optimizeCpuNanos"};
  static constexpr std::string_view kOptimizeToGraphWallNanos{
      "axiom-optimizeToGraphWallNanos"};
  static constexpr std::string_view kOptimizeToGraphCpuNanos{
      "axiom-optimizeToGraphCpuNanos"};
  static constexpr std::string_view kOptimizeBestPlanWallNanos{
      "axiom-optimizeBestPlanWallNanos"};
  static constexpr std::string_view kOptimizeBestPlanCpuNanos{
      "axiom-optimizeBestPlanCpuNanos"};
  static constexpr std::string_view kOptimizeToVeloxWallNanos{
      "axiom-optimizeToVeloxWallNanos"};
  static constexpr std::string_view kOptimizeToVeloxCpuNanos{
      "axiom-optimizeToVeloxCpuNanos"};

  // Split manager.
  static constexpr std::string_view kListPartitionsWallNanos{
      "axiom-listPartitionsWallNanos"};
  static constexpr std::string_view kListPartitionsCpuNanos{
      "axiom-listPartitionsCpuNanos"};
  static constexpr std::string_view kListPartitionsCount{
      "axiom-listPartitionsCount"};
  static constexpr std::string_view kGetSplitsWallNanos{
      "axiom-getSplitsWallNanos"};
  static constexpr std::string_view kGetSplitsCpuNanos{
      "axiom-getSplitsCpuNanos"};
  static constexpr std::string_view kGetSplitsCount{"axiom-getSplitsCount"};

  // Permission check.
  static constexpr std::string_view kPermissionCheckWallNanos{
      "axiom-permissionCheckWallNanos"};
  static constexpr std::string_view kPermissionCheckCpuNanos{
      "axiom-permissionCheckCpuNanos"};

  // Connector.
  static constexpr std::string_view kFindTableWallNanos{
      "axiom-findTableWallNanos"};
  static constexpr std::string_view kFindTableCpuNanos{
      "axiom-findTableCpuNanos"};
  static constexpr std::string_view kEstimateStatsWallNanos{
      "axiom-estimateStatsWallNanos"};
  static constexpr std::string_view kEstimateStatsCpuNanos{
      "axiom-estimateStatsCpuNanos"};

  // Execution.
  static constexpr std::string_view kExecuteWallNanos{"axiom-executeWallNanos"};
  static constexpr std::string_view kExecuteCpuNanos{"axiom-executeCpuNanos"};

  /// Records a wall-clock duration under the given metric name.
  void recordTiming(std::string_view name, std::chrono::nanoseconds duration);

  /// Records a count (e.g., number of partitions or splits).
  void recordCount(std::string_view name, int64_t value);

  /// Merges a pre-aggregated metric into this recorder.
  void merge(std::string_view name, const velox::RuntimeMetric& metric);

  /// Returns a snapshot of all recorded metrics.
  std::unordered_map<std::string, velox::RuntimeMetric> toMap() const;

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

/// RAII timer that records both wall-clock and thread CPU durations into
/// QueryRuntimeStats on destruction. CPU time is only recorded if the
/// destructor runs on the same thread as the constructor — coroutine
/// suspension may resume on a different thread, making per-thread CPU
/// measurement invalid. Wall time is always recorded.
class ScopedCpuWallStatsTimer {
 public:
  ScopedCpuWallStatsTimer(
      QueryRuntimeStats& stats,
      std::string_view wallMetricName,
      std::string_view cpuMetricName)
      : stats_(stats),
        wallMetricName_(wallMetricName),
        cpuMetricName_(cpuMetricName),
        wallStart_(std::chrono::steady_clock::now()),
        cpuStart_(velox::process::threadCpuNanos()),
        startThreadId_(std::this_thread::get_id()) {}

  ~ScopedCpuWallStatsTimer();

  ScopedCpuWallStatsTimer(const ScopedCpuWallStatsTimer&) = delete;
  ScopedCpuWallStatsTimer& operator=(const ScopedCpuWallStatsTimer&) = delete;
  ScopedCpuWallStatsTimer(ScopedCpuWallStatsTimer&&) = delete;
  ScopedCpuWallStatsTimer& operator=(ScopedCpuWallStatsTimer&&) = delete;

 private:
  QueryRuntimeStats& stats_;
  std::string_view wallMetricName_;
  std::string_view cpuMetricName_;
  std::chrono::steady_clock::time_point wallStart_;
  uint64_t cpuStart_;
  std::thread::id startThreadId_;
};

/// Records thread CPU time elapsed since 'cpuStart' if still on the same
/// thread ('startThreadId'). Skips recording when the thread switched (e.g.
/// after co_await) to avoid invalid per-thread CPU measurements.
inline void recordCpuIfSameThread(
    QueryRuntimeStats& stats,
    std::string_view metricName,
    uint64_t cpuStart,
    std::thread::id startThreadId) {
  if (std::this_thread::get_id() == startThreadId) {
    stats.recordTiming(
        metricName,
        std::chrono::nanoseconds(velox::process::threadCpuNanos() - cpuStart));
  }
}

/// Merges 'metric' into 'map' under 'name' using CAS-based optimistic locking.
/// Shared by QueryRuntimeStats and SchedulerStatsRecorder.
void mergeRuntimeMetric(
    folly::ConcurrentHashMap<std::string, velox::RuntimeMetric>& map,
    const std::string& name,
    const velox::RuntimeMetric& metric);

} // namespace facebook::axiom
