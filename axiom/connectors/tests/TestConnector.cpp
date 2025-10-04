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

#include "axiom/connectors/tests/TestConnector.h"

namespace facebook::axiom::connector {

TestTable::TestTable(
    const std::string& name,
    const velox::RowTypePtr& schema,
    TestConnector* connector)
    : Table(name, schema), connector_(connector) {
  exportedColumns_.reserve(schema->size());

  std::vector<const Column*> columnVector;
  columnVector.reserve(schema->size());

  for (auto i = 0; i < schema->size(); ++i) {
    const auto& columnName = schema->nameOf(i);
    const auto& columnType = schema->childAt(i);
    VELOX_CHECK(
        !columnName.empty(), "column {} in table {} has empty name", i, name_);
    exportedColumns_.emplace_back(
        std::make_unique<Column>(columnName, columnType));
    columnVector.emplace_back(exportedColumns_.back().get());
    auto [_, ok] = columns_.emplace(columnName, exportedColumns_.back().get());
    VELOX_CHECK(
        ok, "duplicate column name '{}' in table {}", columnName, name_);
  }

  auto layout =
      std::make_unique<TestTableLayout>(name_, this, connector_, columnVector);
  layouts_.push_back(layout.get());
  exportedLayouts_.push_back(std::move(layout));
  pool_ = velox::memory::memoryManager()->addLeafPool(name_ + "_table");
}

std::vector<SplitSource::SplitAndGroup> TestSplitSource::getSplits(uint64_t) {
  std::vector<SplitAndGroup> result;
  if (currentPartition_ >= partitions_.size()) {
    result.push_back({nullptr, kUngroupedGroupId});
  } else {
    result.push_back(
        {std::make_shared<velox::connector::ConnectorSplit>(connectorId_),
         kUngroupedGroupId});
  }
  currentPartition_++;
  return result;
}

std::vector<PartitionHandlePtr> TestSplitManager::listPartitions(
    const velox::connector::ConnectorTableHandlePtr&) {
  return {std::make_shared<PartitionHandle>()};
}

std::shared_ptr<SplitSource> TestSplitManager::getSplitSource(
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    const std::vector<PartitionHandlePtr>& partitions,
    SplitOptions) {
  return std::make_shared<TestSplitSource>(
      tableHandle->connectorId(), partitions);
}

std::shared_ptr<Table> TestConnectorMetadata::findTableInternal(
    std::string_view name) {
  auto it = tables_.find(name);
  return it != tables_.end() ? it->second : nullptr;
}

TablePtr TestConnectorMetadata::findTable(std::string_view name) {
  return findTableInternal(name);
}

velox::connector::ColumnHandlePtr TestConnectorMetadata::createColumnHandle(
    const TableLayout& layout,
    const std::string& columnName,
    std::vector<velox::common::Subfield>,
    std::optional<velox::TypePtr> castToType,
    SubfieldMapping) {
  auto column = layout.findColumn(columnName);
  VELOX_CHECK_NOT_NULL(
      column, "Column {} not found in table {}", columnName, layout.name());
  return std::make_shared<TestColumnHandle>(
      columnName, castToType.value_or(column->type()));
}

velox::connector::ConnectorTableHandlePtr
TestConnectorMetadata::createTableHandle(
    const TableLayout& layout,
    std::vector<velox::connector::ColumnHandlePtr> columnHandles,
    velox::core::ExpressionEvaluator& /* evaluator */,
    std::vector<velox::core::TypedExprPtr> filters,
    std::vector<velox::core::TypedExprPtr>& rejectedFilters,
    velox::RowTypePtr /* dataColumns */,
    std::optional<LookupKeys>) {
  rejectedFilters = std::move(filters);
  return std::make_shared<TestTableHandle>(layout, std::move(columnHandles));
}

std::shared_ptr<TestTable> TestConnectorMetadata::addTable(
    const std::string& name,
    const velox::RowTypePtr& schema) {
  auto table = std::make_shared<TestTable>(name, schema, connector_);
  auto [it, ok] = tables_.emplace(name, std::move(table));
  VELOX_CHECK(ok, "table {} already exists", name);
  return it->second;
}

void TestConnectorMetadata::appendData(
    std::string_view name,
    const velox::RowVectorPtr& data) {
  auto it = tables_.find(name);
  VELOX_CHECK(it != tables_.end(), "no table {} exists", name);
  it->second->addData(data);
}

TestDataSource::TestDataSource(
    const velox::RowTypePtr& outputType,
    const velox::connector::ColumnHandleMap& handles,
    TablePtr table,
    velox::memory::MemoryPool* pool)
    : outputType_(outputType), pool_(pool) {
  auto maybeTable = std::dynamic_pointer_cast<const TestTable>(table);
  VELOX_CHECK(maybeTable, "table {} not a TestTable", table->name());
  data_ = maybeTable->data();

  auto tableType = table->type();
  outputMappings_.reserve(outputType_->size());
  for (const auto& name : outputType->names()) {
    VELOX_CHECK(
        handles.contains(name),
        "no handle for output column {} for table {}",
        name,
        table->name());
    auto handle = handles.find(name)->second;

    const auto idx = tableType->getChildIdxIfExists(handle->name());
    VELOX_CHECK(
        idx.has_value(),
        "column '{}' not found in table '{}'.",
        handle->name(),
        table->name());
    outputMappings_.emplace_back(idx.value());
  }
}

void TestDataSource::addSplit(
    std::shared_ptr<velox::connector::ConnectorSplit> split) {
  split_ = std::move(split);
}

std::optional<velox::RowVectorPtr> TestDataSource::next(
    uint64_t,
    velox::ContinueFuture&) {
  VELOX_CHECK(split_, "no split added to DataSource");
  if (data_.size() <= idx_) {
    return nullptr;
  }
  auto vector = data_[idx_++];

  completedRows_ += vector->size();
  completedBytes_ += vector->retainedSize();

  std::vector<velox::VectorPtr> children;
  children.reserve(outputMappings_.size());
  for (const auto idx : outputMappings_) {
    children.emplace_back(vector->childAt(idx));
  }

  return std::make_shared<velox::RowVector>(
      pool_, outputType_, nullptr, vector->size(), std::move(children));
}

void TestDataSource::addDynamicFilter(
    velox::column_index_t,
    const std::shared_ptr<velox::common::Filter>&) {
  VELOX_NYI("TestDataSource does not support dynamic filters");
}

std::unique_ptr<velox::connector::DataSource> TestConnector::createDataSource(
    const velox::RowTypePtr& outputType,
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    const velox::connector::ColumnHandleMap& columnHandles,
    velox::connector::ConnectorQueryCtx* connectorQueryCtx) {
  auto table = metadata_->findTable(tableHandle->name());
  VELOX_CHECK(
      table,
      "cannot create data source for nonexistent table {}",
      tableHandle->name());
  return std::make_unique<TestDataSource>(
      outputType, columnHandles, table, connectorQueryCtx->memoryPool());
}

std::unique_ptr<velox::connector::DataSink> TestConnector::createDataSink(
    velox::RowTypePtr,
    velox::connector::ConnectorInsertTableHandlePtr tableHandle,
    velox::connector::ConnectorQueryCtx*,
    velox::connector::CommitStrategy) {
  VELOX_CHECK(tableHandle, "table handle must be non-null");
  auto table = metadata_->findTableInternal(tableHandle->toString());
  VELOX_CHECK(
      table,
      "cannot create data sink for nonexistent table {}",
      tableHandle->toString());
  return std::make_unique<TestDataSink>(table);
}

std::shared_ptr<TestTable> TestConnector::addTable(
    const std::string& name,
    const velox::RowTypePtr& schema) {
  return metadata_->addTable(name, schema);
}

void TestConnector::appendData(
    std::string_view name,
    const velox::RowVectorPtr& data) {
  metadata_->appendData(name, data);
}

std::shared_ptr<velox::connector::Connector> TestConnectorFactory::newConnector(
    const std::string& id,
    std::shared_ptr<const velox::config::ConfigBase> config,
    folly::Executor*,
    folly::Executor*) {
  return std::make_shared<TestConnector>(id, std::move(config));
}

void TestDataSink::appendData(velox::RowVectorPtr vector) {
  if (vector) {
    table_->addData(vector);
  }
}

} // namespace facebook::axiom::connector
