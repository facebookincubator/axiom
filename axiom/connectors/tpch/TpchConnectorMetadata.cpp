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

#include "axiom/connectors/tpch/TpchConnectorMetadata.h"

#include "velox/connectors/Connector.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/connectors/tpch/TpchConnectorSplit.h"
#include "velox/tpch/gen/TpchGen.h"

namespace facebook::axiom::connector::tpch {

namespace {

bool isValidTpchTableName(const std::string& tableName) {
  for (auto tpchTable : velox::tpch::tables) {
    if (tableName == velox::tpch::toTableName(tpchTable)) {
      return true;
    }
  }
  return false;
}

constexpr const char* kTiny = "tiny";

bool isValidTpchSchema(std::string_view schema) {
  if (schema == kTiny) {
    return true;
  }

  if (schema.length() <= 2 || !schema.starts_with("sf")) {
    return false;
  }

  bool nonzero = false;
  bool hasDot = false;
  for (char c : schema.substr(2)) {
    if (c == '.') {
      if (hasDot) {
        return false;
      }
      hasDot = true;
      continue;
    }
    if (!std::isdigit(c)) {
      return false;
    }
    if (c != '0') {
      nonzero = true;
    }
  }
  return nonzero;
}

double getScaleFactor(const std::string& schema) {
  VELOX_CHECK(isValidTpchSchema(schema), "invalid TPCH schema {}", schema);
  if (schema == kTiny) {
    return 0.01;
  }
  return folly::to<double>(schema.substr(2));
}

} // namespace

folly::coro::Task<std::vector<PartitionHandlePtr>>
TpchSplitManager::co_listPartitions(
    const connector::ConnectorSessionPtr& /*session*/,
    const velox::connector::ConnectorTableHandlePtr& /*tableHandle*/) {
  co_return std::vector<PartitionHandlePtr>{
      std::make_shared<connector::PartitionHandle>()};
}

std::shared_ptr<SplitSource> TpchSplitManager::getSplitSource(
    const connector::ConnectorSessionPtr& /*session*/,
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    const std::vector<PartitionHandlePtr>& /*partitions*/) {
  auto* tpchTableHandle =
      dynamic_cast<const velox::connector::tpch::TpchTableHandle*>(
          tableHandle.get());
  VELOX_CHECK_NOT_NULL(
      tpchTableHandle, "Expected TpchTableHandle for TPCH connector");

  return std::make_shared<TpchSplitSource>(
      tpchTableHandle->getTable(),
      tpchTableHandle->getScaleFactor(),
      tpchTableHandle->connectorId());
}

folly::coro::Task<SplitBatch> TpchSplitSource::co_getSplits(
    uint32_t maxSplitCount,
    int32_t /*bucket*/) {
  if (numSplits_ < 0) {
    auto rowType = velox::tpch::getTableSchema(table_);
    size_t rowSize = rowType->children().size() * 10;
    const auto totalRows = velox::tpch::getRowCount(table_, scaleFactor_);
    const auto rowsPerSplit = kFileBytesPerSplit / rowSize;
    numSplits_ = (totalRows + rowsPerSplit - 1) / rowsPerSplit;
  }

  SplitBatch batch;
  auto end =
      std::min(nextSplitIdx_ + static_cast<int64_t>(maxSplitCount), numSplits_);
  for (auto i = nextSplitIdx_; i < end; ++i) {
    batch.splits.push_back(
        std::make_shared<velox::connector::tpch::TpchConnectorSplit>(
            connectorId_, numSplits_, i));
  }
  nextSplitIdx_ = end;
  batch.noMoreSplits = (nextSplitIdx_ >= numSplits_);
  co_return batch;
}

TpchConnectorMetadata::TpchConnectorMetadata(
    velox::connector::tpch::TpchConnector* tpchConnector)
    : tpchConnector_(tpchConnector), splitManager_(this) {
  VELOX_CHECK_NOT_NULL(tpchConnector);
}

velox::connector::ColumnHandlePtr TpchTableLayout::createColumnHandle(
    const ConnectorSessionPtr& /*session*/,
    const std::string& columnName,
    std::vector<velox::common::Subfield> /*subfields*/,
    std::optional<velox::TypePtr> /*castToType*/,
    SubfieldMapping /*subfieldMapping*/) const {
  return std::make_shared<velox::connector::tpch::TpchColumnHandle>(columnName);
}

velox::connector::ConnectorTableHandlePtr TpchTableLayout::createTableHandle(
    const ConnectorSessionPtr& /*session*/,
    std::vector<velox::connector::ColumnHandlePtr> /*columnHandles*/,
    velox::core::ExpressionEvaluator& /*evaluator*/,
    std::vector<velox::core::TypedExprPtr> filters,
    std::vector<velox::core::TypedExprPtr>& /*rejectedFilters*/,
    velox::RowTypePtr /*dataColumns*/,
    std::optional<LookupKeys> /*lookupKeys*/) const {
  velox::core::TypedExprPtr filterExpression;
  for (auto& filter : filters) {
    if (!filterExpression) {
      filterExpression = filter;
    } else {
      filterExpression = std::make_shared<velox::core::CallTypedExpr>(
          velox::BOOLEAN(),
          std::vector<velox::core::TypedExprPtr>{
              std::move(filterExpression), std::move(filter)},
          "and");
    }
  }

  return std::make_shared<velox::connector::tpch::TpchTableHandle>(
      connector()->connectorId(),
      getTpchTable(),
      getScaleFactor(),
      std::move(filterExpression));
}

void TpchTable::makeDefaultLayout(TpchConnectorMetadata& metadata) {
  VELOX_CHECK_EQ(0, exportedLayouts_.size());

  auto layout = std::make_unique<TpchTableLayout>(
      name().table,
      this,
      metadata.tpchConnector(),
      allColumns(),
      std::vector<const Column*>{},
      std::vector<const Column*>{},
      std::vector<SortOrder>{},
      std::vector<const Column*>{},
      tpchTable_,
      scaleFactor_);
  exportedLayouts_.push_back(layout.get());
  layouts_.push_back(std::move(layout));
}

TablePtr TpchConnectorMetadata::findTable(const SchemaTableName& tableName) {
  if (!isValidTpchTableName(tableName.table) ||
      (!tableName.schema.empty() && !isValidTpchSchema(tableName.schema))) {
    return nullptr;
  }

  velox::tpch::Table tpchTable = velox::tpch::fromTableName(tableName.table);
  const auto schema = tableName.schema.empty() ? kTiny : tableName.schema;
  const auto scaleFactor = getScaleFactor(schema);

  const auto tableType = velox::tpch::getTableSchema(tpchTable);
  const auto numRows = velox::tpch::getRowCount(tpchTable, scaleFactor);

  auto table = std::make_shared<TpchTable>(
      SchemaTableName{schema, tableName.table},
      tableType,
      tpchTable,
      scaleFactor,
      numRows);

  table->makeDefaultLayout(*this);

  return table;
}

std::vector<std::string> TpchConnectorMetadata::listSchemaNames(
    const ConnectorSessionPtr& /*session*/) {
  return {
      kTiny,
      "sf1",
      "sf100",
      "sf300",
      "sf1000",
      "sf3000",
      "sf10000",
      "sf30000",
      "sf100000",
  };
}

bool TpchConnectorMetadata::schemaExists(
    const ConnectorSessionPtr& /*session*/,
    const std::string& schemaName) {
  return isValidTpchSchema(schemaName);
}

ViewPtr TpchConnectorMetadata::findView(const SchemaTableName& tableName) {
  if (!tableName.schema.empty() && !isValidTpchSchema(tableName.schema)) {
    return nullptr;
  }

  auto it = views_.find(tableName);
  if (it == views_.end()) {
    return nullptr;
  }

  return std::make_shared<View>(it->first, it->second.type, it->second.text);
}

void TpchConnectorMetadata::createView(
    const SchemaTableName& viewName,
    velox::RowTypePtr type,
    std::string_view text) {
  VELOX_USER_CHECK(!viewName.table.empty(), "View name cannot be empty");
  if (!viewName.schema.empty()) {
    VELOX_USER_CHECK(
        isValidTpchSchema(viewName.schema),
        "Invalid view schema: {}",
        viewName.schema);
  }

  auto ok =
      views_
          .emplace(viewName, ViewDefinition{std::move(type), std::string(text)})
          .second;
  VELOX_CHECK(ok, "View already exists: {}", viewName.toString());
}

bool TpchConnectorMetadata::dropView(const SchemaTableName& viewName) {
  return views_.erase(viewName) == 1;
}

} // namespace facebook::axiom::connector::tpch
