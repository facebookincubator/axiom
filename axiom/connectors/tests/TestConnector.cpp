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
#include "velox/exec/TableWriter.h"
#include "velox/tpch/gen/TpchGen.h"

#include "velox/common/serialization/DeserializationRegistry.h"
#include "velox/core/ITypedExpr.h"

namespace facebook::axiom::connector {

namespace {
std::vector<std::unique_ptr<const Column>> appendHiddenColumns(
    std::vector<std::unique_ptr<const Column>> columns,
    const velox::RowTypePtr& hiddenColumns) {
  for (auto i = 0; i < hiddenColumns->size(); ++i) {
    columns.emplace_back(
        std::make_unique<const Column>(
            hiddenColumns->nameOf(i),
            hiddenColumns->childAt(i),
            /*hidden=*/true));
  }
  return columns;
}
} // namespace

TestTable::TestTable(
    const std::string& name,
    const velox::RowTypePtr& schema,
    const velox::RowTypePtr& hiddenColumns,
    TestConnector* connector)
    : Table(name, appendHiddenColumns(makeColumns(schema), hiddenColumns)),
      connector_(connector) {
  exportedLayout_ =
      std::make_unique<TestTableLayout>(name, this, connector_, allColumns());
  layouts_.push_back(exportedLayout_.get());
  pool_ = velox::memory::memoryManager()->addLeafPool(name + "_table");
}

void TestTable::setStats(
    uint64_t numRows,
    const std::unordered_map<std::string, ColumnStatistics>& columnStats) {
  VELOX_CHECK_EQ(
      dataRows_,
      0,
      "Cannot use both setStats and addData on table '{}'.",
      name());
  numRows_ = numRows;

  // Set or clear stats for all columns.
  for (const auto& [name, column] : columnMap()) {
    auto statsIt = columnStats.find(name);
    if (statsIt != columnStats.end()) {
      auto stats = std::make_unique<ColumnStatistics>(statsIt->second);
      if (stats->numDistinct.has_value()) {
        VELOX_CHECK_LE(
            stats->numDistinct.value(), numRows, "Column '{}'", name);
      }
      if (stats->nonNull) {
        VELOX_CHECK_EQ(stats->nullPct, 0, "Column '{}'", name);
      }
      if (stats->min.has_value() && stats->max.has_value()) {
        VELOX_CHECK(
            !(stats->max.value() < stats->min.value()),
            "Column '{}': min must not exceed max ({} vs. {})",
            name,
            stats->min.value().toJsonUnsafe(),
            stats->max.value().toJsonUnsafe());
      }
      const_cast<Column*>(column)->setStats(std::move(stats));
    } else {
      // Clear stats for columns not in columnStats.
      const_cast<Column*>(column)->setStats(
          std::make_unique<ColumnStatistics>());
    }
  }
}

void TestTable::addData(const velox::RowVectorPtr& data) {
  VELOX_CHECK_EQ(
      numRows_,
      0,
      "Cannot use both setStats and addData on table '{}'.",
      name());
  VELOX_CHECK(
      data->type()->equivalent(*type()),
      "appended data type {} must match table type {}",
      data->type(),
      type());
  VELOX_CHECK_GT(data->size(), 0, "Cannot append empty RowVector");
  auto copy = std::dynamic_pointer_cast<velox::RowVector>(
      velox::BaseVector::copy(*data, pool_.get()));
  data_.push_back(copy);
  dataRows_ += data->size();
}

std::vector<SplitSource::SplitAndGroup> TestSplitSource::getSplits(uint64_t) {
  std::vector<SplitAndGroup> result;
  if (!done_) {
    for (size_t i = 0; i < splitCount_; ++i) {
      result.push_back(
          {std::make_shared<TestConnectorSplit>(connectorId_, i),
           kUngroupedGroupId});
    }
    done_ = true;
  }
  if (result.empty()) {
    result.push_back({nullptr, kUngroupedGroupId});
  }
  return result;
}

std::vector<PartitionHandlePtr> TestSplitManager::listPartitions(
    const ConnectorSessionPtr& session,
    const velox::connector::ConnectorTableHandlePtr&) {
  return {std::make_shared<PartitionHandle>()};
}

std::shared_ptr<SplitSource> TestSplitManager::getSplitSource(
    const ConnectorSessionPtr& session,
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    const std::vector<PartitionHandlePtr>&,
    SplitOptions) {
  auto table = metadata_->findTable(tableHandle->name());
  VELOX_CHECK_NOT_NULL(table);
  auto maybeTable = std::dynamic_pointer_cast<const TestTable>(table);
  VELOX_CHECK(maybeTable, "Table is not a TestTable: {}", table->name());
  return std::make_shared<TestSplitSource>(
      tableHandle->connectorId(), maybeTable->data().size());
}

std::shared_ptr<Table> TestConnectorMetadata::findTableInternal(
    std::string_view name) {
  auto it = tables_.find(name);
  return it != tables_.end() ? it->second : nullptr;
}

TablePtr TestConnectorMetadata::findTable(std::string_view name) {
  return findTableInternal(name);
}

namespace {
class TestDiscretePredicates : public DiscretePredicates {
 public:
  TestDiscretePredicates(
      std::vector<const Column*> columns,
      std::vector<velox::Variant> values)
      : DiscretePredicates(std::move(columns)), values_{std::move(values)} {}

  std::vector<velox::Variant> next() override {
    if (atEnd_) {
      return {};
    }

    atEnd_ = true;

    return std::move(values_);
  }

 private:
  bool atEnd_{false};
  std::vector<velox::Variant> values_;
};
} // namespace

void TestTableLayout::setDiscreteValues(
    const std::vector<std::string>& columnNames,
    const std::vector<velox::Variant>& values) {
  VELOX_CHECK(!columnNames.empty());

  for (const auto& value : values) {
    VELOX_CHECK_EQ(velox::TypeKind::ROW, value.kind());
    VELOX_CHECK_EQ(columnNames.size(), value.row().size());
  }

  std::vector<const Column*> columns;
  columns.reserve(columnNames.size());
  for (const auto& columnName : columnNames) {
    auto column = findColumn(columnName);
    VELOX_CHECK_NOT_NULL(
        column, "Column not found: {} in {}", columnName, name());
    columns.emplace_back(column);
  }

  discreteValueColumns_ = std::move(columns);
  discreteValues_ = values;
}

std::span<const Column* const> TestTableLayout::discretePredicateColumns()
    const {
  return discreteValueColumns_;
}

std::unique_ptr<DiscretePredicates> TestTableLayout::discretePredicates(
    [[maybe_unused]] const std::vector<const Column*>& columns) const {
  if (discreteValueColumns_.empty()) {
    return nullptr;
  }

  // TODO Add logic to prune 'discreteValues_' based on 'columns'.

  return std::make_unique<TestDiscretePredicates>(
      discreteValueColumns_, discreteValues_);
}

velox::connector::ColumnHandlePtr TestTableLayout::createColumnHandle(
    const ConnectorSessionPtr& session,
    const std::string& columnName,
    std::vector<velox::common::Subfield> subfields,
    std::optional<velox::TypePtr> castToType,
    SubfieldMapping subfieldMapping) const {
  auto column = findColumn(columnName);
  VELOX_CHECK_NOT_NULL(
      column, "Column {} not found in table {}", columnName, name());
  return std::make_shared<TestColumnHandle>(
      columnName, castToType.value_or(column->type()));
}

velox::connector::ConnectorTableHandlePtr TestTableLayout::createTableHandle(
    const ConnectorSessionPtr& session,
    std::vector<velox::connector::ColumnHandlePtr> columnHandles,
    velox::core::ExpressionEvaluator& /* evaluator */,
    std::vector<velox::core::TypedExprPtr> filters,
    std::vector<velox::core::TypedExprPtr>& rejectedFilters,
    velox::RowTypePtr /* dataColumns */,
    std::optional<LookupKeys> lookupKeys) const {
  rejectedFilters = std::move(filters);
  return std::make_shared<TestTableHandle>(
      connector()->connectorId(), table().name(), std::move(columnHandles));
}

std::shared_ptr<TestTable> TestConnectorMetadata::addTable(
    const std::string& name,
    const velox::RowTypePtr& schema,
    const velox::RowTypePtr& hiddenColumns) {
  auto table =
      std::make_shared<TestTable>(name, schema, hiddenColumns, connector_);
  auto [it, ok] = tables_.emplace(name, std::move(table));
  VELOX_CHECK(ok, "Table already exists: {}", name);
  return it->second;
}

TablePtr TestConnectorMetadata::createTable(
    const ConnectorSessionPtr& /*session*/,
    const std::string& tableName,
    const velox::RowTypePtr& rowType,
    const folly::F14FastMap<std::string, velox::Variant>& /*options*/) {
  return addTable(tableName, rowType, velox::ROW({}));
}

ConnectorWriteHandlePtr TestConnectorMetadata::beginWrite(
    const ConnectorSessionPtr& /*session*/,
    const TablePtr& table,
    WriteKind /*kind*/) {
  auto insertHandle = std::make_shared<TestInsertTableHandle>(table->name());
  return std::make_shared<ConnectorWriteHandle>(
      std::move(insertHandle),
      velox::exec::TableWriteTraits::outputType(std::nullopt));
}

RowsFuture TestConnectorMetadata::finishWrite(
    const ConnectorSessionPtr& /*session*/,
    const ConnectorWriteHandlePtr& /*handle*/,
    const std::vector<velox::RowVectorPtr>& writeResults) {
  int64_t rows = 0;
  velox::DecodedVector decoded;
  for (const auto& result : writeResults) {
    decoded.decode(*result->childAt(0));
    for (velox::vector_size_t i = 0; i < decoded.size(); ++i) {
      if (!decoded.isNullAt(i)) {
        rows += decoded.valueAt<int64_t>(i);
      }
    }
  }
  return folly::makeFuture(rows);
}

bool TestConnectorMetadata::dropTable(
    const ConnectorSessionPtr& /* session */,
    std::string_view tableName,
    bool ifExists) {
  const bool dropped = tables_.erase(tableName) == 1;
  if (!ifExists) {
    VELOX_USER_CHECK(dropped, "Table doesn't exist: {}", tableName);
  }

  return dropped;
}

void TestConnectorMetadata::appendData(
    std::string_view name,
    const velox::RowVectorPtr& data) {
  auto it = tables_.find(name);
  VELOX_CHECK(it != tables_.end(), "Table doesn't exist: {}", name);
  it->second->addData(data);
}

void TestConnectorMetadata::setDiscreteValues(
    const std::string& name,
    const std::vector<std::string>& columnNames,
    const std::vector<velox::Variant>& values) {
  auto it = tables_.find(name);
  VELOX_CHECK(it != tables_.end(), "Table doesn't exist: {}", name);

  it->second->mutableLayout()->setDiscreteValues(columnNames, values);
}

void TestConnectorMetadata::setStats(
    const std::string& tableName,
    uint64_t numRows,
    const std::unordered_map<std::string, ColumnStatistics>& columnStats) {
  auto it = tables_.find(tableName);
  VELOX_CHECK(it != tables_.end(), "Table doesn't exist: {}", tableName);
  it->second->setStats(numRows, columnStats);
}

TestDataSource::TestDataSource(
    const velox::RowTypePtr& outputType,
    const velox::connector::ColumnHandleMap& handles,
    TablePtr table,
    velox::memory::MemoryPool* pool)
    : outputType_(outputType), pool_(pool) {
  auto maybeTable = std::dynamic_pointer_cast<const TestTable>(table);
  VELOX_CHECK(maybeTable, "Table is not a TestTable: {}", table->name());
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
  split_ = std::dynamic_pointer_cast<TestConnectorSplit>(split);
  VELOX_CHECK(split_, "Expected TestConnectorSplit");
  more_ = true;
}

std::optional<velox::RowVectorPtr> TestDataSource::next(
    uint64_t,
    velox::ContinueFuture&) {
  VELOX_CHECK(split_, "no split added to DataSource");

  if (!more_) {
    return nullptr;
  }
  more_ = false;

  VELOX_CHECK_LT(split_->index(), data_.size(), "split index out of bounds");
  auto vector = data_[split_->index()];

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
    const velox::RowTypePtr& schema,
    const velox::RowTypePtr& hiddenColumns) {
  return metadata_->addTable(name, schema, hiddenColumns);
}

bool TestConnector::dropTableIfExists(const std::string& name) {
  return metadata_->dropTableIfExists(name);
}

void TestConnector::addTpchTables() {
  for (auto table : velox::tpch::tables) {
    addTable(
        std::string(velox::tpch::toTableName(table)),
        velox::tpch::getTableSchema(table));
  }
}

void TestConnector::appendData(
    std::string_view name,
    const velox::RowVectorPtr& data) {
  metadata_->appendData(name, data);
}

void TestConnector::setDiscreteValues(
    const std::string& name,
    const std::vector<std::string>& columnNames,
    const std::vector<velox::Variant>& values) {
  metadata_->setDiscreteValues(name, columnNames, values);
}

void TestConnector::setStats(
    const std::string& tableName,
    uint64_t numRows,
    const std::unordered_map<std::string, ColumnStatistics>& columnStats) {
  metadata_->setStats(tableName, numRows, columnStats);
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

folly::dynamic TestColumnHandle::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = TestColumnHandle::getClassName();
  obj["columnName"] = name_;
  obj["type"] = type_->serialize();
  return obj;
}

std::shared_ptr<TestColumnHandle> TestColumnHandle::create(
    const folly::dynamic& obj,
    void* context) {
  auto name = obj["columnName"].asString();
  auto type =
      velox::ISerializable::deserialize<velox::Type>(obj["type"], context);
  return std::make_shared<TestColumnHandle>(name, type);
}

void TestColumnHandle::registerSerDe() {
  velox::registerDeserializerWithContext<TestColumnHandle>();
}

folly::dynamic TestConnectorSplit::serialize() const {
  auto obj = serializeBase(TestConnectorSplit::getClassName());
  obj["index"] = index_;
  return obj;
}

std::shared_ptr<TestConnectorSplit> TestConnectorSplit::create(
    const folly::dynamic& obj) {
  auto connectorId = obj["connectorId"].asString();
  auto index = static_cast<size_t>(obj["index"].asInt());
  return std::make_shared<TestConnectorSplit>(connectorId, index);
}

void TestConnectorSplit::registerSerDe() {
  velox::registerDeserializer<TestConnectorSplit>();
}

folly::dynamic TestTableHandle::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = TestTableHandle::getClassName();
  obj["connectorId"] = connectorId();
  obj["tableName"] = tableName_;
  folly::dynamic columnHandlesArray = folly::dynamic::array;
  for (const auto& handle : columnHandles_) {
    columnHandlesArray.push_back(handle->serialize());
  }
  obj["columnHandles"] = columnHandlesArray;
  folly::dynamic filtersArray = folly::dynamic::array;
  for (const auto& filter : filters_) {
    filtersArray.push_back(filter->serialize());
  }
  obj["filters"] = filtersArray;
  return obj;
}

velox::connector::ConnectorTableHandlePtr TestTableHandle::create(
    const folly::dynamic& obj,
    void* context) {
  auto connectorId = obj["connectorId"].asString();
  auto tableName = obj["tableName"].asString();

  std::vector<velox::connector::ColumnHandlePtr> columnHandles;
  for (const auto& handleObj : obj["columnHandles"]) {
    columnHandles.push_back(
        velox::ISerializable::deserialize<velox::connector::ColumnHandle>(
            handleObj, context));
  }

  std::vector<velox::core::TypedExprPtr> filters;
  for (const auto& filterObj : obj["filters"]) {
    filters.push_back(
        velox::ISerializable::deserialize<velox::core::ITypedExpr>(
            filterObj, context));
  }

  return std::make_shared<TestTableHandle>(
      connectorId, tableName, std::move(columnHandles), std::move(filters));
}

void TestTableHandle::registerSerDe() {
  velox::registerDeserializerWithContext<TestTableHandle>();
}

} // namespace facebook::axiom::connector
