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
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include <folly/Synchronized.h>
#include <folly/container/F14Map.h>

#include "velox/common/base/ConcurrentRuntimeStatWriter.h"
#include "velox/common/base/RuntimeMetrics.h"

namespace facebook::axiom {

/// Aggregates a query's runtime metrics. Components record through
/// writerFor() and are keyed <component>/<name>; connectors record through
/// writerForConnector() and are keyed connector/<connectorId>/<name>. The two
/// live in separate namespaces, so a catalog named after a component is not a
/// collision. Thread-safe: concurrent recording is safe.
///
/// Each records into the handle it was given, so a metric is always filed
/// under whoever measured it. An application hands a component its handle and
/// takes one of its own for the phases it times:
///
///   OptimizerSession session{..., stats.writerFor(kOptimizer), ...};
///   session.statsWriter().addTiming(OptimizerMetrics::kEstimateStatsWallNanos,
///   elapsed);
///   // toMap() holds "optimizer/estimateStatsWallNanos".
///
/// Rejecting '/' in an id keeps the two namespaces separate, and leaves a '/'
/// inside 'name' unambiguous.
class QueryRuntimeStats {
 public:
  /// The namespace connector metrics are keyed under, so a catalog cannot
  /// take a component's name.
  static constexpr std::string_view kConnectorNamespace{"connector"};

  /// Returns the write handle for component 'componentId', creating
  /// it on first use. The same id returns the same handle, valid for this
  /// object's lifetime because handles are never erased; keep it that way, an
  /// erase or reset would dangle every outstanding reference. Throws if
  /// 'componentId' is empty, contains '/', or equals kConnectorNamespace.
  velox::BaseRuntimeStatWriter& writerFor(std::string_view componentId);

  /// Returns the write handle for connector 'connectorId', keyed under
  /// connector/, with the same creation, lifetime and id checks as writerFor().
  velox::BaseRuntimeStatWriter& writerForConnector(
      std::string_view connectorId);

  /// Returns the metric 'id' recorded under 'name', or std::nullopt if it
  /// never recorded it. Copies that id's whole metric map on every call, so
  /// this is for tests and one-off inspection; read many metrics with toMap()
  /// instead. A connector's 'id' is connector/<connectorId>.
  std::optional<velox::RuntimeMetric> find(
      std::string_view id,
      std::string_view name) const;

  /// What separates the id from the name in a key. kSlash spells a connector
  /// key connector/prism/listDirectoryCalls; kDash spells the same key
  /// connector-prism-listDirectoryCalls, which is how sinks publish it.
  enum class KeySeparator { kSlash, kDash };

  /// Returns a snapshot of all recorded metrics, keyed by 'separator'.
  std::unordered_map<std::string, velox::RuntimeMetric> toMap(
      KeySeparator separator) const;

  /// Returns the key toMap() emits for 'id' and 'name'. Every separator in
  /// 'id' becomes 'separator', while 'name' is passed through, so a '/' chosen
  /// for a metric name survives.
  static std::string qualifiedKey(
      std::string_view id,
      std::string_view name,
      KeySeparator separator);

 private:
  velox::BaseRuntimeStatWriter& writerForId(std::string_view id);

  // The writer is held by pointer, not by value: the writerFor* methods return
  // references that must survive later insertions into the map.
  folly::Synchronized<folly::F14FastMap<
      std::string,
      std::unique_ptr<velox::ConcurrentRuntimeStatWriter>>>
      buckets_;
};

} // namespace facebook::axiom
