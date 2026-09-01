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
#include "axiom/optimizer/v2/ScanHandle.h"

#include "folly/coro/BlockingWait.h"
#include "folly/coro/Collect.h"

#include "axiom/connectors/ConnectorMetadata.h"
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
  const_cast<Value&>(existing) =
      Value::fromColumnStatistics(existing.type, stats);
}

// Applies one base table's connector stats result. Sets filteredCardinality to
// the connector's post-filter row count. A nullopt result (the connector does
// not support stats) leaves filteredCardinality at 0 so downstream estimation
// falls back to constraint-based selectivity.
void applyFilteredStats(
    const Scan& scan,
    const std::vector<ColumnCP>& statColumns,
    const std::optional<connector::FilteredTableStats>& stats) {
  if (!stats.has_value()) {
    return;
  }

  auto* baseTable = const_cast<BaseTable*>(scan.baseTable());

  baseTable->numRawInputRows = stats->numRawInputRows;

  if (!stats->columnStats.empty()) {
    VELOX_CHECK_EQ(stats->columnStats.size(), statColumns.size());
    for (size_t i = 0; i < stats->columnStats.size(); ++i) {
      applyColumnStats(statColumns[i], stats->columnStats[i]);
    }
  }

  // numRows is post-filter for the filters the connector took; the refused
  // ones are estimated at the Filter above the scan.
  baseTable->filteredCardinality = std::max<float>(1, stats->numRows);
}

} // namespace

void EstimateLeafStatsPass::run(NodeCP root, const OptimizerSession& session) {
  std::vector<ScanCP> scans;
  collectScans(root, scans);

  // One stats request per distinct base table.
  struct TableTask {
    ScanCP scan;
    std::vector<ColumnCP> statColumns;
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
    const ScanHandle* handle = scan->scanHandle();
    VELOX_CHECK_NOT_NULL(
        handle, "Filtered-table stats need the connector's read handle");

    const auto* layout = baseTable->schemaTable->columnGroups[0]->layout;
    auto connectorSession =
        session.toConnectorSession(layout->connector()->connectorId());

    // Subfield columns have no connector-level statistics.
    std::vector<ColumnCP> statColumns;
    std::vector<std::string> columnNames;
    for (ColumnCP column : scan->outputColumns()) {
      if (column->topColumn() == nullptr) {
        statColumns.push_back(column);
        columnNames.emplace_back(column->name());
      }
    }

    tasks.push_back(TableTask{scan, std::move(statColumns)});
    requests.push_back(layout->co_estimateStats(
        std::move(connectorSession),
        handle->tableHandle,
        handle->partitionSelection,
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
    applyFilteredStats(*tasks[i].scan, tasks[i].statColumns, results[i]);
  }
}

} // namespace facebook::axiom::optimizer::v2
