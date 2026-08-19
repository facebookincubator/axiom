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

#include "axiom/common/QueryRuntimeStats.h"

#include <fmt/format.h>

namespace facebook::axiom {

namespace {
std::string_view unitToString(velox::RuntimeCounter::Unit unit) {
  switch (unit) {
    case velox::RuntimeCounter::Unit::kNanos:
      return "NANO";
    case velox::RuntimeCounter::Unit::kBytes:
      return "BYTE";
    case velox::RuntimeCounter::Unit::kNone:
      return "NONE";
  }
  return "NONE";
}
} // namespace

folly::dynamic QueryRuntimeStats::toDynamic() const {
  folly::dynamic result = folly::dynamic::object();
  for (const auto& [name, metric] : runtimeStats()) {
    folly::dynamic entry = folly::dynamic::object();
    entry["name"] = name;
    entry["sum"] = metric.sum;
    entry["count"] = static_cast<int64_t>(metric.count);
    entry["min"] = metric.min;
    entry["max"] = metric.max;
    entry["unit"] = unitToString(metric.unit);
    result[name] = std::move(entry);
  }
  return result;
}

ScopedCpuWallStatsTimer::~ScopedCpuWallStatsTimer() {
  if (std::this_thread::get_id() == startThreadId_) {
    auto cpuElapsed = velox::process::threadCpuNanos() - cpuStart_;
    stats_.addTiming(cpuMetricName_, std::chrono::nanoseconds(cpuElapsed));
  }
  auto wallElapsed = std::chrono::steady_clock::now() - wallStart_;
  stats_.addTiming(wallMetricName_, wallElapsed);
}

void PrefixedRuntimeStatWriter::addRuntimeStat(
    std::string_view name,
    const velox::RuntimeCounter& value) {
  writer_.addRuntimeStat(prefixed(name), value);
}

void PrefixedRuntimeStatWriter::setRuntimeStat(
    std::string_view name,
    const velox::RuntimeMetric& metric) {
  writer_.setRuntimeStat(prefixed(name), metric);
}

std::string PrefixedRuntimeStatWriter::prefixed(std::string_view name) const {
  return fmt::format("{}-{}", prefix_, name);
}

} // namespace facebook::axiom
