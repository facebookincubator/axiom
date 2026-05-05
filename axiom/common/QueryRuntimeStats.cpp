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

namespace facebook::axiom {

namespace {
bool runtimeMetricEquals(
    const velox::RuntimeMetric& lhs,
    const velox::RuntimeMetric& rhs) {
  return lhs.sum == rhs.sum && lhs.count == rhs.count && lhs.min == rhs.min &&
      lhs.max == rhs.max && lhs.unit == rhs.unit;
}

std::string_view unitToString(velox::RuntimeCounter::Unit unit) {
  switch (unit) {
    case velox::RuntimeCounter::Unit::kNanos:
      return "NANO";
    case velox::RuntimeCounter::Unit::kBytes:
      return "BYTE";
    case velox::RuntimeCounter::Unit::kNone:
      return "NONE";
  }
  VELOX_UNREACHABLE();
}
} // namespace

void mergeRuntimeMetric(
    folly::ConcurrentHashMap<std::string, velox::RuntimeMetric>& map,
    const std::string& name,
    const velox::RuntimeMetric& metric) {
  auto [it, inserted] = map.try_emplace(name, metric);
  if (inserted) {
    return;
  }
  while (true) {
    auto current = it->second;
    auto updated = current;
    updated.merge(metric);
    auto result = map.assign_if(
        name,
        std::move(updated),
        [&current](const velox::RuntimeMetric& existing) {
          return runtimeMetricEquals(existing, current);
        });
    if (result.has_value()) {
      return;
    }
    it = map.find(name);
    if (it == map.cend()) {
      std::tie(it, inserted) = map.try_emplace(name, metric);
      if (inserted) {
        return;
      }
    }
  }
}

void QueryRuntimeStats::recordTiming(
    std::string_view name,
    std::chrono::nanoseconds duration) {
  mergeRuntimeMetric(
      metrics_,
      std::string(name),
      velox::RuntimeMetric(
          duration.count(), velox::RuntimeCounter::Unit::kNanos));
}

void QueryRuntimeStats::recordCount(std::string_view name, int64_t value) {
  mergeRuntimeMetric(
      metrics_,
      std::string(name),
      velox::RuntimeMetric(value, velox::RuntimeCounter::Unit::kNone));
}

void QueryRuntimeStats::merge(
    std::string_view name,
    const velox::RuntimeMetric& metric) {
  mergeRuntimeMetric(metrics_, std::string(name), metric);
}

folly::F14FastMap<std::string, velox::RuntimeMetric> QueryRuntimeStats::toMap()
    const {
  folly::F14FastMap<std::string, velox::RuntimeMetric> result;
  for (const auto& [name, metric] : metrics_) {
    result.emplace(name, metric);
  }
  return result;
}

folly::dynamic QueryRuntimeStats::toDynamic() const {
  folly::dynamic result = folly::dynamic::object();
  for (const auto& [name, metric] : metrics_) {
    folly::dynamic entry = folly::dynamic::object();
    entry["sum"] = metric.sum;
    entry["count"] = static_cast<int64_t>(metric.count);
    entry["min"] = metric.min;
    entry["max"] = metric.max;
    entry["unit"] = unitToString(metric.unit);
    result[name] = std::move(entry);
  }
  return result;
}

} // namespace facebook::axiom
