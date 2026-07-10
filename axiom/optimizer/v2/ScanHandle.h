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

#include "axiom/optimizer/OptimizerSession.h"
#include "axiom/optimizer/v2/Node.h"
#include "velox/connectors/Connector.h"
#include "velox/core/Expressions.h"

namespace facebook::axiom::optimizer::v2 {

/// Connector access for a scanned leaf table, built once from a `Scan` and
/// reused by both the filtered-stats pass (`estimateLeafStats`) and `emit`.
/// Building the handle calls into the connector to push filters down, so it
/// must not be repeated per consumer.
struct ScanHandle {
  /// Builds the connector handle for `scan`'s base table. Calls into the
  /// connector to push filters down.
  static ScanHandle build(
      const Scan& scan,
      const OptimizerSession& session,
      velox::core::ExpressionEvaluator& evaluator);

  /// Table handle with the accepted filters pushed into the connector.
  velox::connector::ConnectorTableHandlePtr tableHandle;

  /// Column handles for the connector read schema: the scan's consumer output
  /// columns first (in `scan->outputColumns()` order), then `filterOnlyColumns`
  /// (aligned with the tail).
  std::vector<velox::connector::ColumnHandlePtr> columnHandles;

  /// Columns referenced only by filters and not in the scan output. Read by the
  /// connector but, unless referenced by a rejected filter, projected away.
  ColumnVector filterOnlyColumns;

  /// Filters the connector could not push down; applied as a `Filter` above the
  /// `TableScan` at emit time.
  std::vector<velox::core::TypedExprPtr> rejectedFilters;

  /// All scan filters as `TypedExpr` (schema-named), in the order passed to
  /// `createTableHandle`. Passed to `co_estimateStats` so the connector can
  /// report which conjuncts it could not account for.
  std::vector<velox::core::TypedExprPtr> filterConjuncts;
};

/// Caches one `ScanHandle` per base table, keyed by base-table id. `getOrBuild`
/// is the writing path; `find` is read-only.
///
///   ScanHandleCache cache;
///   const ScanHandle& handle = cache.getOrBuild(scan, session, evaluator);
///   ...
///   const ScanHandle* cached = cache.find(scan);
class ScanHandleCache {
 public:
  /// Returns the cached `ScanHandle` for `scan`'s base table, building and
  /// caching it on first access.
  const ScanHandle& getOrBuild(
      const Scan& scan,
      const OptimizerSession& session,
      velox::core::ExpressionEvaluator& evaluator);

  /// Returns the cached handle for `scan`'s base table, or nullptr if none was
  /// built.
  const ScanHandle* find(const Scan& scan) const;

 private:
  folly::F14FastMap<int32_t, ScanHandle> byBaseTableId_;
};

} // namespace facebook::axiom::optimizer::v2
