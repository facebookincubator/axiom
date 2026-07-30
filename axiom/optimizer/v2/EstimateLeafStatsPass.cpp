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

#include "axiom/optimizer/v2/EstimateLeafStatsPass.h"

#include "folly/coro/BlockingWait.h"
#include "folly/coro/Collect.h"

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/optimizer/Filters.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/QueryGraphContext.h"
#include "axiom/optimizer/Schema.h"
#include "axiom/optimizer/StatsFilterSelectivityEstimator.h"

namespace facebook::axiom::optimizer::v2 {

namespace {

// Collects the Scan nodes reachable from 'node'.
void collectScans(NodeCP node, std::vector<ScanCP>& scans) {
  if (node->is(NodeType::kScan)) {
    scans.push_back(node->as<Scan>());
    return;
  }

  for (NodeCP input : node->inputs()) {
    collectScans(input, scans);
  }
}

// Overwrites 'column''s Value with connector-provided per-column statistics.
void applyColumnStats(
    ColumnCP column,
    const connector::ColumnStatistics& stats) {
  const auto& existing = column->value();
  // Propagate an absent NDV as unknown (nullopt) rather than fabricating 1;
  // downstream fanout estimation treats unknown NDV as unknown selectivity.
  const std::optional<float> numDistinct = stats.numDistinct.has_value()
      ? std::optional<float>{std::max<float>(1, *stats.numDistinct)}
      : std::nullopt;
  Value value(existing.type, numDistinct);
  value.min =
      stats.min.has_value() ? registerVariant(stats.min.value()) : nullptr;
  value.max =
      stats.max.has_value() ? registerVariant(stats.max.value()) : nullptr;
  value.nullFraction = stats.nullPct / 100.0f;
  value.nullable = !stats.nonNull;
  const_cast<Value&>(existing) = value;
}

// Applies one base table's connector stats result. Sets filteredCardinality to
// the connector's post-filter row count, scaled by the optimizer's own
// selectivity for any filters the connector rejected. A nullopt result (the
// connector does not support stats) leaves filteredCardinality at 0 so
// downstream estimation falls back to constraint-based selectivity. Rejected
// filters scale only the row count; per-column constraints are re-derived
// downstream.
void applyFilteredStats(
    const Scan& scan,
    const std::vector<ColumnCP>& statColumns,
    const std::optional<connector::FilteredTableStats>& stats,
    const ExprVector& rejectedFilters) {
  if (!stats.has_value()) {
    return;
  }

  auto* baseTable = const_cast<BaseTable*>(scan.baseTable());

  if (!stats->columnStats.empty()) {
    VELOX_CHECK_EQ(stats->columnStats.size(), statColumns.size());
    for (size_t i = 0; i < stats->columnStats.size(); ++i) {
      applyColumnStats(statColumns[i], stats->columnStats[i]);
    }
  }

  // Bytes read from storage are fixed by partition pruning, already reflected
  // in stats->dataSize, so they are not scaled by row-level filter selectivity.
  // Only refine when the connector estimated a size; otherwise keep the
  // whole-table seed from schemaTable->dataSize.
  if (auto dataSize = toSignedByteCount(stats->dataSize)) {
    baseTable->filteredDataSize = dataSize;
  }

  // The connector accounted for its accepted filters in numRows. Post-apply
  // selectivity for the filters createTableHandle rejected (kept as ExprCP).
  if (rejectedFilters.empty()) {
    baseTable->filteredCardinality = std::max<float>(1, stats->numRows);
    return;
  }

  ConstraintMap constraints;
  const auto selectivity = conjunctsSelectivity(
      constraints,
      {rejectedFilters.data(), rejectedFilters.size()},
      /*updateConstraints=*/false);
  // When the rejected filters' selectivity is unknown, the post-filter row
  // count is unknown too: leave `filteredCardinality` unset so downstream
  // estimation propagates the unknown rather than fabricating a fraction.
  if (!selectivity.has_value()) {
    baseTable->filteredCardinality = std::nullopt;
    return;
  }
  baseTable->filteredCardinality =
      std::max<float>(1, stats->numRows * selectivity->trueFraction);
}

} // namespace

void EstimateLeafStatsPass::run(
    NodeCP root,
    const OptimizerSession& session,
    velox::core::ExpressionEvaluator& evaluator,
    ScanHandleCache& scanHandles) {
  std::vector<ScanCP> scans;
  collectScans(root, scans);

  // One stats request per distinct base table.
  struct TableTask {
    ScanCP scan;
    std::vector<ColumnCP> statColumns;
    // Copied from the cached ScanHandle: a reference would dangle when a later
    // getOrBuild rehashes the ScanHandleCache and moves the ScanHandle.
    ExprVector rejectedExprs;
  };
  std::vector<TableTask> tasks;
  std::vector<folly::coro::Task<std::optional<connector::FilteredTableStats>>>
      requests;
  folly::F14FastSet<int32_t> seen;

  // Shared helper offered to each connector's co_estimateStats. Outlives the
  // coroutines below, which run under blockingWait before this returns.
  StatsFilterSelectivityEstimator estimator;

  for (ScanCP scan : scans) {
    const auto* baseTable = scan->baseTable();
    if (!seen.insert(baseTable->id()).second) {
      continue;
    }
    const ScanHandle& handle =
        scanHandles.getOrBuild(*scan, session, evaluator);

    const auto* layout = baseTable->schemaTable->columnGroups[0]->layout;
    auto connectorSession =
        session.toConnectorSession(layout->connector()->connectorId());

    // Request stats for the top-level columns of the read schema. Subfield
    // columns have no connector-level statistics.
    std::vector<ColumnCP> statColumns;
    std::vector<std::string> columnNames;
    const auto addColumn = [&](ColumnCP column) {
      if (column->topColumn() == nullptr) {
        statColumns.push_back(column);
        columnNames.emplace_back(column->name());
      }
    };
    for (ColumnCP column : scan->outputColumns()) {
      addColumn(column);
    }
    for (ColumnCP column : handle.filterOnlyColumns) {
      addColumn(column);
    }

    tasks.push_back(
        TableTask{scan, std::move(statColumns), handle.rejectedExprs});
    requests.push_back(layout->co_estimateStats(
        std::move(connectorSession),
        handle.tableHandle,
        std::move(columnNames),
        estimator));
  }

  if (requests.empty()) {
    return;
  }

  // No optimizer-time executor is available, so the requests run inline. They
  // are still launched together so a connector that suspends on I/O can
  // overlap them.
  auto results = folly::coro::blockingWait(
      folly::coro::collectAllRange(std::move(requests)));

  for (size_t i = 0; i < tasks.size(); ++i) {
    applyFilteredStats(
        *tasks[i].scan,
        tasks[i].statColumns,
        results[i],
        tasks[i].rejectedExprs);
  }
}

} // namespace facebook::axiom::optimizer::v2
