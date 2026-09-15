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

#include "axiom/cli/common/ComponentMetrics.h"

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
/// Every id rejects '/'. A component id also rejects '-', since kDash rewrites
/// a connector id's '/' to one. A '/' inside 'name' stays unambiguous.
class QueryRuntimeStats {
 public:
  /// Returns the write handle for component 'componentId', creating it on
  /// first use. The same id returns the same handle, and the handle owns the
  /// writer, so a session given one stays usable however long it is held.
  /// Throws if 'componentId' is empty, contains '/' or '-', or equals
  /// ComponentMetrics::kConnector.
  std::shared_ptr<velox::BaseRuntimeStatWriter> writerFor(
      std::string_view componentId);

  /// Returns the write handle for connector 'connectorId', keyed under
  /// connector/, with the same creation and ownership as writerFor(). Throws
  /// if 'connectorId' is empty or contains '/'; a '-' is allowed, since
  /// catalogs carry one.
  std::shared_ptr<velox::BaseRuntimeStatWriter> writerForConnector(
      std::string_view connectorId);

  /// What separates the id from the name in a key. kSlash spells a connector
  /// key connector/hive/listDirectoryCalls; kDash spells the same key
  /// connector-hive-listDirectoryCalls, which is how sinks publish it.
  enum class KeySeparator { kSlash, kDash };

  /// Returns a snapshot of all recorded metrics, keyed by 'separator'. Two
  /// producers can qualify to one key, in which case one is kept and the
  /// collision is logged; which one is unspecified.
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
  std::shared_ptr<velox::ConcurrentRuntimeStatWriter> writerForId(
      std::string_view id);

  // Shared, not owned solely here: a handed-out handle keeps its writer alive
  // independently of this map.
  folly::Synchronized<folly::F14FastMap<
      std::string,
      std::shared_ptr<velox::ConcurrentRuntimeStatWriter>>>
      buckets_;
};

} // namespace facebook::axiom
