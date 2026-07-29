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
#include <memory>
#include <string>
#include <string_view>
#include <thread>

#include <folly/concurrency/ConcurrentHashMap.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/common/process/ProcessBase.h"

namespace facebook::axiom {

/// Map of metric name to its aggregated velox::RuntimeMetric. Backs one
/// component's isolated view within a QueryRuntimeStats collector.
using RuntimeMetricMap =
    folly::ConcurrentHashMap<std::string, velox::RuntimeMetric>;

/// Merges 'metric' into 'map' under 'name' using CAS-based optimistic locking.
void mergeRuntimeMetric(
    RuntimeMetricMap& map,
    const std::string& name,
    const velox::RuntimeMetric& metric);

/// Write-only handle to one component's metrics within a per-query
/// QueryRuntimeStats collector. A component records through its own sink and
/// cannot reach the collector or another component's metrics. Cheap to copy
/// (holds a shared handle to one component's map). Obtain via
/// QueryRuntimeStats::sinkFor().
///
/// Example:
///   auto sink = stats.sinkFor(optimizer::kStatsComponent);
///   sink.recordCount("axiom-candidatesPruned", 3);
class RuntimeStatsSink {
 public:
  /// Binds the sink to 'metrics', one component's metric map. Prefer
  /// QueryRuntimeStats::sinkFor() to obtain a sink.
  explicit RuntimeStatsSink(std::shared_ptr<RuntimeMetricMap> metrics)
      : metrics_{std::move(metrics)} {
    VELOX_CHECK_NOT_NULL(metrics_);
  }

  /// Returns a sink over a fresh throwaway map, for callers with no collector.
  static RuntimeStatsSink throwaway();

  /// Records a wall-clock duration under 'name'.
  void recordTiming(std::string_view name, std::chrono::nanoseconds duration)
      const;

  /// Records a count (e.g. number of partitions or splits) under 'name'.
  void recordCount(std::string_view name, int64_t value) const;

  /// Merges a pre-aggregated metric into 'name'.
  void merge(std::string_view name, const velox::RuntimeMetric& metric) const;

 private:
  std::shared_ptr<RuntimeMetricMap> metrics_;
};

/// RAII timer that records both wall-clock and thread CPU durations into a
/// RuntimeStatsSink on destruction. CPU time is only recorded if the destructor
/// runs on the same thread as the constructor — coroutine suspension may resume
/// on a different thread, making per-thread CPU measurement invalid. Wall time
/// is always recorded.
///
/// Example:
///   {
///     ScopedCpuWallStatsTimer timer(
///         stats.sinkFor(optimizer::kStatsComponent),
///         "axiom-optimizeWallNanos",
///         "axiom-optimizeCpuNanos");
///     // ... work to time ...
///   }
class ScopedCpuWallStatsTimer {
 public:
  ScopedCpuWallStatsTimer(
      RuntimeStatsSink sink,
      std::string_view wallMetricName,
      std::string_view cpuMetricName)
      : sink_(std::move(sink)),
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
  RuntimeStatsSink sink_;
  std::string_view wallMetricName_;
  std::string_view cpuMetricName_;
  std::chrono::steady_clock::time_point wallStart_;
  uint64_t cpuStart_;
  std::thread::id startThreadId_;
};

/// Records thread CPU time elapsed since 'cpuStart' into 'sink' under
/// 'metricName' only when still on the constructing thread ('startThreadId'). A
/// thread switch (e.g. after co_await) makes per-thread CPU measurement
/// invalid, so recording is skipped.
inline void recordCpuIfSameThread(
    const RuntimeStatsSink& sink,
    std::string_view metricName,
    uint64_t cpuStart,
    std::thread::id startThreadId) {
  if (std::this_thread::get_id() == startThreadId) {
    sink.recordTiming(
        metricName,
        std::chrono::nanoseconds(velox::process::threadCpuNanos() - cpuStart));
  }
}

} // namespace facebook::axiom
