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

#include "axiom/connectors/system/SystemConnectorMetadata.h"

#include "axiom/connectors/system/SystemConnector.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::connector::system {

velox::RowTypePtr queriesTableSchema() {
  static auto schema = velox::ROW(
      {{"query_id", velox::VARCHAR()},
       {"state", velox::VARCHAR()},
       {"query", velox::VARCHAR()},
       {"catalog", velox::VARCHAR()},
       {"schema", velox::VARCHAR()},
       {"user", velox::VARCHAR()},
       {"source", velox::VARCHAR()},
       {"query_type", velox::VARCHAR()},
       {"planning_time_ms", velox::BIGINT()},
       {"optimization_time_ms", velox::BIGINT()},
       {"queue_time_ms", velox::BIGINT()},
       {"execution_time_ms", velox::BIGINT()},
       {"elapsed_time_ms", velox::BIGINT()},
       {"cpu_time_ms", velox::BIGINT()},
       {"wall_time_ms", velox::BIGINT()},
       {"total_splits", velox::BIGINT()},
       {"queued_splits", velox::BIGINT()},
       {"running_splits", velox::BIGINT()},
       {"finished_splits", velox::BIGINT()},
       {"output_rows", velox::BIGINT()},
       {"output_bytes", velox::BIGINT()},
       {"processed_rows", velox::BIGINT()},
       {"processed_bytes", velox::BIGINT()},
       {"written_rows", velox::BIGINT()},
       {"written_bytes", velox::BIGINT()},
       {"peak_memory_bytes", velox::BIGINT()},
       {"spilled_bytes", velox::BIGINT()},
       {"create_time", velox::TIMESTAMP()},
       {"start_time", velox::TIMESTAMP()},
       {"end_time", velox::TIMESTAMP()}});
  return schema;
}

// ===================== SystemTableLayout =====================

velox::connector::ColumnHandlePtr SystemTableLayout::createColumnHandle(
    const ConnectorSessionPtr& /*session*/,
    const std::string& columnName,
    std::vector<velox::common::Subfield> /*subfields*/,
    std::optional<velox::TypePtr> /*castToType*/,
    SubfieldMapping /*subfieldMapping*/) const {
  auto column = findColumn(columnName);
  VELOX_CHECK_NOT_NULL(
      column, "Column '{}' not found in system.runtime.queries", columnName);
  return std::make_shared<SystemColumnHandle>(columnName);
}

velox::connector::ConnectorTableHandlePtr SystemTableLayout::createTableHandle(
    const ConnectorSessionPtr& /*session*/,
    std::vector<velox::connector::ColumnHandlePtr> columnHandles,
    velox::core::ExpressionEvaluator& /*evaluator*/,
    std::vector<velox::core::TypedExprPtr> filters,
    std::vector<velox::core::TypedExprPtr>& rejectedFilters,
    velox::RowTypePtr /*dataColumns*/,
    std::optional<LookupKeys> /*lookupKeys*/) const {
  // System connector does not support filter pushdown.
  rejectedFilters = std::move(filters);
  return std::make_shared<SystemTableHandle>(
      connectorId(), *this, std::move(columnHandles));
}

// ===================== SystemTable =====================

SystemTable::SystemTable(velox::connector::Connector* connector)
    : Table(
          SchemaTableName{
              std::string(SystemConnectorMetadata::kDefaultSchema),
              "queries"},
          makeColumns(queriesTableSchema())) {
  layout_ = std::make_unique<SystemTableLayout>(this, connector, allColumns());
  layouts_.push_back(layout_.get());
}

// ===================== SystemSplitSource / SystemSplitManager
// =====================

std::vector<SplitSource::SplitAndGroup> SystemSplitSource::getSplits(
    uint64_t /*targetBytes*/) {
  std::vector<SplitAndGroup> result;
  if (!done_) {
    result.push_back(
        {std::make_shared<SystemSplit>(connectorId_), kUngroupedGroupId});
    done_ = true;
  }
  if (result.empty()) {
    // Signal end-of-splits.
    result.push_back({nullptr, kUngroupedGroupId});
  }
  return result;
}

std::vector<PartitionHandlePtr> SystemSplitManager::listPartitions(
    const ConnectorSessionPtr& /*session*/,
    const velox::connector::ConnectorTableHandlePtr& /*tableHandle*/) {
  return {std::make_shared<PartitionHandle>()};
}

std::shared_ptr<SplitSource> SystemSplitManager::getSplitSource(
    const ConnectorSessionPtr& /*session*/,
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    const std::vector<PartitionHandlePtr>& /*partitions*/,
    SplitOptions /*options*/) {
  return std::make_shared<SystemSplitSource>(tableHandle->connectorId());
}

// ===================== SystemConnectorMetadata =====================

TablePtr SystemConnectorMetadata::findTable(const SchemaTableName& tableName) {
  if (tableName.schema != kDefaultSchema || tableName.table != "queries") {
    return nullptr;
  }
  if (!queriesTable_) {
    queriesTable_ = std::make_shared<SystemTable>(connector_);
  }
  return queriesTable_;
}

} // namespace facebook::axiom::connector::system
