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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include "axiom/optimizer/connectors/tests/TestConnector.h"

namespace facebook::velox::connector {

std::pair<int64_t, int64_t> TestTableLayout::sample(
    const connector::ConnectorTableHandlePtr&,
    float,
    std::vector<core::TypedExprPtr>,
    RowTypePtr,
    const std::vector<common::Subfield>&,
    HashStringAllocator*,
    std::vector<ColumnStatistics>*) const {
  return std::make_pair(1'000, 1'000);
}

TestTable::TestTable(
    const std::string& name,
    const RowTypePtr& schema,
    TestConnector* connector,
    uint64_t rows)
    : Table(name), connector_(connector), rows_(rows) {
  type_ = schema;
  for (auto i = 0; i < schema->size(); ++i) {
    auto columnName = schema->nameOf(i);
    auto columnType = schema->childAt(i);
    auto column = std::make_unique<Column>(columnName, columnType);
    columns_[columnName] = column.get();
    exportedColumns_.push_back(std::move(column));
  }
  auto layout = std::make_unique<TestTableLayout>(
      name_, this, connector_, getColumnVector());
  layouts_.push_back(layout.get());
  exportedLayouts_.push_back(std::move(layout));
}

std::vector<const Column*> TestTable::getColumnVector() const {
  std::vector<const Column*> result;
  result.reserve(columns_.size());
  for (const auto& [name, column] : columns_) {
    result.push_back(column);
  }
  return result;
}

std::vector<SplitSource::SplitAndGroup> TestSplitSource::getSplits(uint64_t) {
  if (currentPartition_ >= partitions_.size()) {
    return {};
  }
  std::vector<SplitAndGroup> result;
  result.push_back(
      {std::make_shared<TestSplit>(connectorId_), kUngroupedGroupId});
  currentPartition_++;
  return result;
}

std::vector<std::shared_ptr<const PartitionHandle>>
TestSplitManager::listPartitions(const ConnectorTableHandlePtr&) {
  return {std::make_shared<PartitionHandle>()};
}

std::shared_ptr<SplitSource> TestSplitManager::getSplitSource(
    const ConnectorTableHandlePtr& tableHandle,
    std::vector<std::shared_ptr<const PartitionHandle>> partitions,
    SplitOptions) {
  return std::make_shared<TestSplitSource>(
      tableHandle->connectorId(), std::move(partitions));
}

const Table* TestConnectorMetadata::findTable(const std::string& name) {
  auto it = tables_.find(name);
  VELOX_USER_CHECK(it != tables_.end(), "Test table not found: {}", name);
  return it->second.get();
}

ColumnHandlePtr TestConnectorMetadata::createColumnHandle(
    const TableLayout& layout,
    const std::string& columnName,
    std::vector<common::Subfield>,
    std::optional<TypePtr> castToType,
    SubfieldMapping) {
  auto column = layout.findColumn(columnName);
  VELOX_CHECK_NOT_NULL(
      column, "Column {} not found in layout {}", columnName, layout.name());
  return std::make_shared<TestColumnHandle>(
      columnName, castToType.value_or(column->type()));
}

ConnectorTableHandlePtr TestConnectorMetadata::createTableHandle(
    const TableLayout& layout,
    std::vector<ColumnHandlePtr> columnHandles,
    core::ExpressionEvaluator& /* evaluator */,
    std::vector<core::TypedExprPtr> filters,
    std::vector<core::TypedExprPtr>& rejectedFilters,
    RowTypePtr /* dataColumns */,
    std::optional<LookupKeys>) {
  auto connector = dynamic_cast<TestConnector*>(layout.connector());
  VELOX_CHECK(connector);
  std::vector<core::TypedExprPtr> pushdown;
  if (connector->pushdownFilters()) {
    pushdown = std::move(filters);
  } else {
    rejectedFilters = std::move(filters);
  }
  return std::make_shared<TestTableHandle>(layout, columnHandles, pushdown);
}

void TestConnectorMetadata::addTable(
    const std::string& name,
    const RowTypePtr& schema,
    TestConnector* connector,
    uint64_t rows) {
  auto table = std::make_unique<TestTable>(name, schema, connector, rows);
  tables_.emplace(name, std::move(table));
}

TestDataSource::TestDataSource(
    const RowTypePtr& outputType,
    const ConnectorTableHandlePtr& tableHandle)
    : outputType_(outputType), tableHandle_(tableHandle) {
  pool_ =
      memory::memoryManager()->addLeafPool(tableHandle_->name() + "_source");
  std::vector<VectorPtr> children;
  for (auto i = 0; i < outputType->size(); ++i) {
    auto type = outputType->childAt(i);
    children.push_back(BaseVector::createNullConstant(type, 1, pool_.get()));
  }
  data_ = std::make_shared<RowVector>(
      pool_.get(), outputType, BufferPtr(nullptr), 1, std::move(children));
}

void TestDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  split_ = std::move(split);
  hasData_ = true;
}

std::optional<RowVectorPtr> TestDataSource::next(
    uint64_t,
    velox::ContinueFuture&) {
  if (!hasData_) {
    return std::nullopt;
  }
  hasData_ = false;
  return data_;
}

void TestDataSource::addDynamicFilter(
    column_index_t,
    const std::shared_ptr<common::Filter>&) {}

memory::MemoryPool* TestDataSource::pool() const {
  return pool_.get();
}

std::unique_ptr<DataSource> TestConnector::createDataSource(
    const RowTypePtr& outputType,
    const ConnectorTableHandlePtr& tableHandle,
    const ColumnHandleMap&,
    ConnectorQueryCtx*) {
  return std::make_unique<TestDataSource>(outputType, tableHandle);
}

std::unique_ptr<DataSink> TestConnector::createDataSink(
    RowTypePtr,
    ConnectorInsertTableHandlePtr,
    ConnectorQueryCtx*,
    CommitStrategy) {
  return std::make_unique<TestDataSink>();
}

void TestConnector::addTable(
    const std::string& name,
    const RowTypePtr& schema,
    uint64_t rows) {
  metadata_->addTable(name, schema, this, rows);
}

void TestConnector::addTable(
    const std::string& name,
    const RowTypePtr& schema) {
  metadata_->addTable(name, schema, this, /*rows=*/1000);
}

void TestConnector::addTable(const std::string& name) {
  metadata_->addTable(name, /*schema=*/ROW({}, {}), this, /*rows=*/1000);
}

} // namespace facebook::velox::connector
