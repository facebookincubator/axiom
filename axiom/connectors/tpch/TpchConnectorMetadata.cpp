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

#include "axiom/optimizer/SchemaUtils.h"
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

bool isValidTpchSchema(const std::string& schema) {
  if (schema == "tiny") {
    return true;
  }
  if (schema.length() <= 2 || !schema.starts_with("sf")) {
    return false;
  }
  bool nonzero = false;
  for (char c : schema.substr(2)) {
    if (!std::isdigit(c)) {
      return false;
    }
    if (c != '0') {
      nonzero = true;
    }
  }
  return nonzero;
}

} // namespace

double getScaleFactor(const std::string& schema) {
  VELOX_CHECK(isValidTpchSchema(schema), "invalid TPCH schema {}", schema);
  if (schema == "tiny") {
    return 0.01;
  }
  return folly::to<double>(schema.substr(2));
}

std::vector<PartitionHandlePtr> TpchSplitManager::listPartitions(
    const velox::connector::ConnectorTableHandlePtr& /*tableHandle*/) {
  return {std::make_shared<connector::PartitionHandle>()};
}

std::shared_ptr<SplitSource> TpchSplitManager::getSplitSource(
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    const std::vector<PartitionHandlePtr>& /*partitions*/,
    SplitOptions options) {
  auto* tpchTableHandle =
      dynamic_cast<const velox::connector::tpch::TpchTableHandle*>(
          tableHandle.get());
  VELOX_CHECK_NOT_NULL(
      tpchTableHandle, "Expected TpchTableHandle for TPCH connector");

  return std::make_shared<TpchSplitSource>(
      tpchTableHandle->getTable(),
      tpchTableHandle->getScaleFactor(),
      tpchTableHandle->connectorId(),
      options);
}

std::vector<SplitSource::SplitAndGroup> TpchSplitSource::getSplits(
    uint64_t targetBytes) {
  std::vector<SplitAndGroup> result;

  if (splits_.empty()) {
    // Generate splits if not already done
    auto rowType = velox::tpch::getTableSchema(table_);
    size_t rowSize = 0;
    for (auto i = 0; i < rowType->children().size(); i++) {
      // TODO: use actual size
      rowSize += 10;
    }
    const auto totalRows = velox::tpch::getRowCount(table_, scaleFactor_);
    const auto rowsPerSplit = options_.fileBytesPerSplit / rowSize;
    const auto numSplits = (totalRows + rowsPerSplit - 1) / rowsPerSplit;

    // TODO: adjust numSplits based on options_.targetSplitCount
    for (int64_t i = 0; i < numSplits; ++i) {
      splits_.push_back(
          std::make_shared<velox::connector::tpch::TpchConnectorSplit>(
              connectorId_, numSplits, i));
    }
  }

  if (currentSplit_ >= splits_.size()) {
    result.push_back(kNoMoreSplits);
    return result;
  }

  uint64_t bytes = 0;
  while (currentSplit_ < splits_.size()) {
    auto split = splits_[currentSplit_++];
    result.emplace_back(SplitAndGroup{split, 0});

    // TODO: use a more accurate size for the split
    bytes += options_.fileBytesPerSplit;

    if (bytes > targetBytes) {
      break;
    }
  }

  if (result.empty()) {
    result.push_back(SplitSource::SplitAndGroup{nullptr, 0});
  }

  return result;
}

TpchConnectorMetadata::TpchConnectorMetadata(
    velox::connector::tpch::TpchConnector* tpchConnector)
    : tpchConnector_(tpchConnector), splitManager_(this) {
  VELOX_CHECK_NOT_NULL(tpchConnector);
}

velox::connector::ColumnHandlePtr TpchConnectorMetadata::createColumnHandle(
    const TableLayout& layoutData,
    const std::string& columnName,
    std::vector<velox::common::Subfield> subfields,
    std::optional<velox::TypePtr> castToType,
    SubfieldMapping subfieldMapping) {
  return std::make_shared<velox::connector::tpch::TpchColumnHandle>(columnName);
}

velox::connector::ConnectorTableHandlePtr
TpchConnectorMetadata::createTableHandle(
    const TableLayout& layout,
    std::vector<velox::connector::ColumnHandlePtr> /*columnHandles*/,
    velox::core::ExpressionEvaluator& /*evaluator*/,
    std::vector<velox::core::TypedExprPtr> filters,
    std::vector<velox::core::TypedExprPtr>& /*rejectedFilters*/,
    velox::RowTypePtr /*dataColumns*/,
    std::optional<LookupKeys> /*lookupKeys*/) {
  auto* tpchLayout = dynamic_cast<const TpchTableLayout*>(&layout);
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
      tpchConnector_->connectorId(),
      tpchLayout->getTpchTable(),
      tpchLayout->getScaleFactor(),
      std::move(filterExpression));
}

TablePtr TpchConnectorMetadata::loadTable(
    const std::optional<std::string>& ns,
    const std::string& name) {
  velox::tpch::Table tpchTable = velox::tpch::fromTableName(name);
  const auto schema = ns.value_or("tiny");
  const auto scaleFactor = getScaleFactor(schema);

  const auto tableName = fmt::format("{}.{}", schema, name);
  const auto tableType = velox::tpch::getTableSchema(tpchTable);
  const auto numRows = velox::tpch::getRowCount(tpchTable, scaleFactor);

  auto table =
      std::make_shared<TpchTable>(tableName, tableType, tpchTable, scaleFactor);
  table->numRows_ = numRows;

  for (auto i = 0; i < tableType->size(); ++i) {
    const auto columnName = tableType->nameOf(i);
    const auto columnType = tableType->childAt(i);
    table->columns()[columnName] =
        std::make_unique<Column>(columnName, columnType);
  }

  table->makeDefaultLayout(*this, scaleFactor);

  return table;
}

std::pair<int64_t, int64_t> TpchTableLayout::sample(
    const velox::connector::ConnectorTableHandlePtr& handle,
    float pct,
    const std::vector<velox::core::TypedExprPtr>& /* extraFilters */,
    velox::RowTypePtr /* outputType */,
    const std::vector<velox::common::Subfield>& /* fields */,
    velox::HashStringAllocator* /* allocator */,
    std::vector<ColumnStatistics>* /* statistics */) const {
  // TODO Add support for filter in 'handle' and 'extraFilters'.
  const auto totalRows = velox::tpch::getRowCount(tpchTable_, scaleFactor_);
  return std::pair(totalRows, totalRows);
}

void TpchTable::makeDefaultLayout(
    TpchConnectorMetadata& metadata,
    double scaleFactor) {
  std::vector<const Column*> columns;
  for (auto i = 0; i < type_->size(); ++i) {
    auto name = type_->nameOf(i);
    columns.push_back(columns_[name].get());
  }
  auto* connector = metadata.tpchConnector();
  std::vector<const Column*> empty;
  auto layout = std::make_unique<TpchTableLayout>(
      name_,
      this,
      connector,
      std::move(columns),
      empty,
      empty,
      std::vector<SortOrder>{},
      empty,
      tpchTable_,
      scaleFactor);
  exportedLayouts_.push_back(layout.get());
  layouts_.push_back(std::move(layout));
}

const folly::F14FastMap<std::string, const Column*>& TpchTable::columnMap()
    const {
  std::lock_guard<std::mutex> l(mutex_);
  if (columns_.empty()) {
    return exportedColumns_;
  }
  for (auto& pair : columns_) {
    exportedColumns_[pair.first] = pair.second.get();
  }
  return exportedColumns_;
}

TablePtr TpchConnectorMetadata::findTable(const std::string& name) {
  axiom::optimizer::TableNameParser parser(name);
  if (!parser.valid() || !isValidTpchTableName(parser.table()) ||
      (parser.schema().has_value() &&
       !isValidTpchSchema(parser.schema().value()))) {
    return nullptr;
  }
  return loadTable(parser.schema(), parser.table());
}

} // namespace facebook::axiom::connector::tpch
