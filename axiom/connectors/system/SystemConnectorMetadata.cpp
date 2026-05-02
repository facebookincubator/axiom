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

const velox::RowTypePtr& queriesTableSchema() {
  static auto kSchema = velox::ROW({
      {"query_id", velox::VARCHAR()},
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
      {"end_time", velox::TIMESTAMP()},
  });
  return kSchema;
}

const velox::RowTypePtr& sessionPropertiesTableSchema() {
  static auto kSchema = velox::ROW({
      {"component", velox::VARCHAR()},
      {"name", velox::VARCHAR()},
      {"type", velox::VARCHAR()},
      {"default_value", velox::VARCHAR()},
      {"current_value", velox::VARCHAR()},
      {"description", velox::VARCHAR()},
  });
  return kSchema;
}

const velox::RowTypePtr& functionsTableSchema() {
  static auto kSchema = velox::ROW({
      {"name", velox::VARCHAR()},
      {"kind", velox::VARCHAR()},
      {"return_type", velox::VARCHAR()},
      {"argument_types", velox::ARRAY(velox::VARCHAR())},
      {"is_variadic", velox::BOOLEAN()},
      {"owner", velox::VARCHAR()},
      {"properties", velox::VARCHAR()},
  });
  return kSchema;
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
      connectorId(), table().name(), std::move(columnHandles));
}

// ===================== SystemTable =====================

SystemTable::SystemTable(
    const SchemaTableName& tableName,
    const velox::RowTypePtr& schema,
    velox::connector::Connector* connector)
    : Table(tableName, makeColumns(schema)) {
  layout_ = std::make_unique<SystemTableLayout>(this, connector, allColumns());
  layouts_.push_back(layout_.get());
}

// ===================== SystemSplitSource / SystemSplitManager
// =====================

folly::coro::Task<SplitBatch> SystemSplitSource::co_getSplits(
    uint32_t /*maxSplitCount*/,
    int32_t /*bucket*/) {
  SplitBatch batch;
  if (!done_) {
    batch.splits.push_back(std::make_shared<SystemSplit>(connectorId_));
    done_ = true;
  }
  batch.noMoreSplits = true;
  co_return batch;
}

folly::coro::Task<std::vector<PartitionHandlePtr>>
SystemSplitManager::co_listPartitions(
    const ConnectorSessionPtr& /*session*/,
    const velox::connector::ConnectorTableHandlePtr& /*tableHandle*/) {
  co_return std::vector<PartitionHandlePtr>{
      std::make_shared<PartitionHandle>()};
}

std::shared_ptr<SplitSource> SystemSplitManager::getSplitSource(
    const ConnectorSessionPtr& /*session*/,
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    const std::vector<PartitionHandlePtr>& /*partitions*/) {
  return std::make_shared<SystemSplitSource>(tableHandle->connectorId());
}

// ===================== SystemConnectorMetadata =====================

TablePtr SystemConnectorMetadata::findTable(const SchemaTableName& tableName) {
  if (tableName == kQueriesTable) {
    if (!queriesTable_) {
      queriesTable_ = std::make_shared<SystemTable>(
          tableName, queriesTableSchema(), connector_);
    }
    return queriesTable_;
  }

  if (tableName == kSessionPropertiesTable) {
    if (!sessionPropertiesTable_) {
      sessionPropertiesTable_ = std::make_shared<SystemTable>(
          tableName, sessionPropertiesTableSchema(), connector_);
    }
    return sessionPropertiesTable_;
  }

  if (tableName == kFunctionsTable) {
    if (!functionsTable_) {
      functionsTable_ = std::make_shared<SystemTable>(
          tableName, functionsTableSchema(), connector_);
    }
    return functionsTable_;
  }

  return nullptr;
}

} // namespace facebook::axiom::connector::system
