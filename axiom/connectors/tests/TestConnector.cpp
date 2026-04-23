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

#include <algorithm>

#include "velox/exec/TableWriter.h"
#include "velox/tpch/gen/TpchGen.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::axiom::connector {

namespace {

// Extracts column names to include in EXPLAIN IO output from the options map.
folly::F14FastSet<std::string> extractExplainIoColumns(
    const folly::F14FastMap<std::string, velox::Variant>& options) {
  auto it = options.find(std::string{TestConnectorMetadata::kExplainIo});
  if (it == options.end()) {
    return {};
  }
  auto names = it->second.array<std::string>();
  return {names.begin(), names.end()};
}

// Creates Column objects for a schema, marking specified columns with
// includeInExplainIo.
std::vector<std::unique_ptr<const Column>> makeColumnsWithExplainIo(
    const velox::RowTypePtr& schema,
    const folly::F14FastSet<std::string>& explainIoColumns) {
  std::vector<std::unique_ptr<const Column>> columns;
  columns.reserve(schema->size());
  for (auto i = 0; i < schema->size(); ++i) {
    columns.push_back(
        std::make_unique<const Column>(
            schema->nameOf(i),
            schema->childAt(i),
            /*hidden=*/false,
            /*includeInExplainIo=*/
            explainIoColumns.contains(schema->nameOf(i))));
  }
  return columns;
}

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

namespace {
std::vector<std::unique_ptr<const Column>> makeTestTableColumns(
    const velox::RowTypePtr& schema,
    const velox::RowTypePtr& hiddenColumns,
    const folly::F14FastMap<std::string, velox::Variant>& options) {
  return appendHiddenColumns(
      makeColumnsWithExplainIo(schema, extractExplainIoColumns(options)),
      hiddenColumns);
}
} // namespace

TestTable::TestTable(
    SchemaTableName name,
    const velox::RowTypePtr& schema,
    const velox::RowTypePtr& hiddenColumns,
    TestConnector* connector,
    folly::F14FastMap<std::string, velox::Variant> options)
    : Table(
          std::move(name),
          makeTestTableColumns(schema, hiddenColumns, options),
          options),
      connector_(connector) {
  const auto& label = this->name().table;
  exportedLayout_ =
      std::make_unique<TestTableLayout>(label, this, connector_, allColumns());
  layouts_.push_back(exportedLayout_.get());
  pool_ = velox::memory::memoryManager()->addLeafPool(label + "_table");
  columnTrackers_.resize(schema->size());
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

void TestTable::addData(
    const velox::RowVectorPtr& data,
    bool collectColumnStatistics) {
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

  if (!collectColumnStatistics) {
    return;
  }

  // Compute per-column statistics incrementally.
  const auto& rowType = type();
  for (auto i = 0; i < data->childrenSize(); ++i) {
    auto& tracker = columnTrackers_[i];
    tracker.append(*data->childAt(i));

    const auto& columnName = rowType->nameOf(i);
    const_cast<Column*>(columnMap().at(columnName))
        ->setStats(
            tracker.toColumnStatistics(dataRows_, data->childAt(i)->type()));
  }
}

void TestTable::ColumnTracker::append(const velox::BaseVector& vector) {
  const auto& childType = vector.type();

  for (auto i = 0; i < vector.size(); ++i) {
    if (vector.isNullAt(i)) {
      ++nullCount;
      continue;
    }

    if (childType->isPrimitiveType()) {
      distinctHashes.insert(vector.hashValueAt(i));
      auto value = vector.variantAt(i);
      if (!min.has_value() || value < min.value()) {
        min = value;
      }
      if (!max.has_value() || max.value() < value) {
        max = value;
      }
    }

    auto addLength = [&](int32_t length) {
      totalLength += length;
      maxLength = std::max(maxLength, length);
    };

    if (childType->isVarchar() || childType->isVarbinary()) {
      auto value =
          vector.as<velox::SimpleVector<velox::StringView>>()->valueAt(i);
      addLength(static_cast<int32_t>(value.size()));
    } else if (childType->isArray()) {
      auto* arrayVector = vector.wrappedVector()->as<velox::ArrayVector>();
      addLength(arrayVector->sizeAt(vector.wrappedIndex(i)));
    } else if (childType->isMap()) {
      auto* mapVector = vector.wrappedVector()->as<velox::MapVector>();
      addLength(mapVector->sizeAt(vector.wrappedIndex(i)));
    }
  }
}

std::unique_ptr<ColumnStatistics> TestTable::ColumnTracker::toColumnStatistics(
    uint64_t totalRows,
    const velox::TypePtr& type) const {
  auto stats = std::make_unique<ColumnStatistics>();
  stats->numValues = totalRows - nullCount;
  stats->nonNull = (nullCount == 0);
  stats->nullPct = totalRows > 0 ? 100.0f * nullCount / totalRows : 0;

  if (type->isPrimitiveType()) {
    stats->numDistinct = distinctHashes.size();
    stats->min = min;
    stats->max = max;
  }

  if (type->isVarchar() || type->isVarbinary() || type->isArray() ||
      type->isMap()) {
    stats->maxLength = maxLength;
    auto numNonNull = totalRows - nullCount;
    if (numNonNull > 0) {
      stats->avgLength = totalLength / numNonNull;
    }
  }

  return stats;
}

folly::coro::Task<SplitBatch> TestSplitSource::co_getSplits(
    uint32_t maxSplitCount,
    int32_t /*bucket*/) {
  SplitBatch batch;
  auto end =
      std::min(nextIndex_ + static_cast<size_t>(maxSplitCount), splitCount_);
  for (auto i = nextIndex_; i < end; ++i) {
    batch.splits.push_back(
        std::make_shared<TestConnectorSplit>(connectorId_, i));
  }
  nextIndex_ = end;
  batch.noMoreSplits = (nextIndex_ >= splitCount_);
  co_return batch;
}

folly::coro::Task<std::vector<PartitionHandlePtr>>
TestSplitManager::co_listPartitions(
    const ConnectorSessionPtr& /*session*/,
    const velox::connector::ConnectorTableHandlePtr&) {
  co_return std::vector<PartitionHandlePtr>{
      std::make_shared<PartitionHandle>()};
}

std::shared_ptr<SplitSource> TestSplitManager::getSplitSource(
    const ConnectorSessionPtr& /*session*/,
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    const std::vector<PartitionHandlePtr>&,
    SplitOptions) {
  auto maybeTableHandle =
      std::dynamic_pointer_cast<const TestTableHandle>(tableHandle);
  VELOX_CHECK(maybeTableHandle, "Expected TestTableHandle");
  return std::make_shared<TestSplitSource>(
      tableHandle->connectorId(), maybeTableHandle->size());
}

std::shared_ptr<Table> TestConnectorMetadata::findTableInternal(
    const SchemaTableName& tableName) {
  auto it = tables_.find(tableName);
  if (it == tables_.end()) {
    return nullptr;
  }
  return it->second;
}

TablePtr TestConnectorMetadata::findTable(const SchemaTableName& tableName) {
  return findTableInternal(tableName);
}

ViewPtr TestConnectorMetadata::findView(const SchemaTableName& tableName) {
  auto it = views_.find(tableName);
  if (it == views_.end()) {
    return nullptr;
  }
  return std::make_shared<View>(it->first, it->second.type, it->second.text);
}

void TestConnectorMetadata::createView(
    const SchemaTableName& viewName,
    velox::RowTypePtr type,
    std::string_view text) {
  auto [_, inserted] = views_.emplace(
      viewName, ViewDefinition{std::move(type), std::string(text)});
  VELOX_CHECK(inserted, "View already exists: {}", viewName.toString());
}

bool TestConnectorMetadata::dropView(const SchemaTableName& viewName) {
  return views_.erase(viewName) == 1;
}

std::vector<std::string> TestConnectorMetadata::listSchemaNames(
    const ConnectorSessionPtr& /*session*/) {
  return {schemas_.begin(), schemas_.end()};
}

bool TestConnectorMetadata::schemaExists(
    const ConnectorSessionPtr& /*session*/,
    const std::string& schemaName) {
  return schemas_.contains(schemaName);
}

void TestConnectorMetadata::createSchema(
    const ConnectorSessionPtr& /*session*/,
    const std::string& schemaName,
    bool ifNotExists,
    const folly::F14FastMap<std::string, velox::Variant>& /*properties*/) {
  auto [_, inserted] = schemas_.insert(schemaName);
  VELOX_USER_CHECK(
      inserted || ifNotExists, "Schema already exists: {}", schemaName);
}

void TestConnectorMetadata::dropSchema(
    const ConnectorSessionPtr& /*session*/,
    const std::string& schemaName,
    bool ifExists) {
  VELOX_USER_CHECK_NE(
      schemaName,
      std::string(kDefaultSchema),
      "Cannot drop the default schema");
  auto erased = schemas_.erase(schemaName);
  VELOX_USER_CHECK(
      erased == 1 || ifExists, "Schema does not exist: {}", schemaName);
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
        column, "Column not found: {} in {}", columnName, label());
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
    const ConnectorSessionPtr& /*session*/,
    const std::string& columnName,
    std::vector<velox::common::Subfield> /*subfields*/,
    std::optional<velox::TypePtr> castToType,
    SubfieldMapping /*subfieldMapping*/) const {
  auto column = findColumn(columnName);
  VELOX_CHECK_NOT_NULL(
      column, "Column {} not found in table {}", columnName, label());
  return std::make_shared<TestColumnHandle>(
      columnName, castToType.value_or(column->type()));
}

velox::connector::ConnectorTableHandlePtr TestTableLayout::createTableHandle(
    const ConnectorSessionPtr& /*session*/,
    std::vector<velox::connector::ColumnHandlePtr> columnHandles,
    velox::core::ExpressionEvaluator& /* evaluator */,
    std::vector<velox::core::TypedExprPtr> filters,
    std::vector<velox::core::TypedExprPtr>& rejectedFilters,
    velox::RowTypePtr /* dataColumns */,
    std::optional<LookupKeys> /*lookupKeys*/) const {
  rejectedFilters = std::move(filters);
  return std::make_shared<TestTableHandle>(*this, std::move(columnHandles));
}

std::shared_ptr<TestTable> TestConnectorMetadata::addTable(
    SchemaTableName tableName,
    const velox::RowTypePtr& schema,
    const velox::RowTypePtr& hiddenColumns) {
  auto table = std::make_shared<TestTable>(
      tableName,
      schema,
      hiddenColumns,
      connector_,
      folly::F14FastMap<std::string, velox::Variant>{});
  auto [it, ok] = tables_.emplace(std::move(tableName), std::move(table));
  VELOX_CHECK(ok, "Table already exists: {}", it->first.toString());
  return it->second;
}

TablePtr TestConnectorMetadata::createTable(
    const ConnectorSessionPtr& /*session*/,
    const SchemaTableName& tableName,
    const velox::RowTypePtr& rowType,
    const folly::F14FastMap<std::string, velox::Variant>& options,
    bool explain) {
  for (const auto& [key, value] : options) {
    VELOX_USER_CHECK(
        key == kHidden || key == kExplainIo,
        "TestConnector does not support CREATE TABLE property: {}",
        key);
  }
  VELOX_USER_CHECK(
      schemas_.contains(tableName.schema),
      "Schema does not exist: {}",
      tableName.schema);

  // Parse optional 'hidden' property to add hidden VARCHAR columns.
  // Hidden columns are not part of the schema — they are created implicitly.
  velox::RowTypePtr hiddenColumns = velox::ROW({});
  auto hiddenIt = options.find(std::string{kHidden});
  if (hiddenIt != options.end()) {
    auto hiddenNames = hiddenIt->second.array<std::string>();
    folly::F14FastSet<std::string> seen;
    for (const auto& name : hiddenNames) {
      VELOX_USER_CHECK(!name.empty(), "Hidden column name cannot be empty");
      VELOX_USER_CHECK(
          seen.insert(name).second, "Duplicate hidden column: {}", name);
      VELOX_USER_CHECK(
          !rowType->containsChild(name),
          "Hidden column conflicts with schema column: {}",
          name);
    }

    hiddenColumns = velox::ROW(std::move(hiddenNames), velox::VARCHAR());
  }

  auto table = std::make_shared<TestTable>(
      tableName, rowType, hiddenColumns, connector_, options);
  if (explain) {
    return table;
  }
  auto [it, ok] = tables_.emplace(tableName, std::move(table));
  VELOX_CHECK(ok, "Table already exists: {}", tableName.toString());
  return it->second;
}

ConnectorWriteHandlePtr TestConnectorMetadata::beginWrite(
    const ConnectorSessionPtr& /*session*/,
    const TablePtr& table,
    WriteKind /*kind*/,
    bool /*explain*/) {
  auto insertHandle = std::make_shared<TestInsertTableHandle>(table->name());
  return std::make_shared<ConnectorWriteHandle>(
      std::move(insertHandle),
      velox::exec::TableWriteTraits::outputType(std::nullopt));
}

RowsFuture TestConnectorMetadata::finishWrite(
    const ConnectorSessionPtr& /*session*/,
    const ConnectorWriteHandlePtr& /*handle*/,
    const std::vector<velox::RowVectorPtr>& writeResults,
    velox::RowVectorPtr /*groupingKeys*/,
    std::vector<std::vector<ColumnStatistics>> /*groupStats*/) {
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
    const SchemaTableName& tableName,
    bool ifExists) {
  const bool dropped = tables_.erase(tableName) == 1;
  if (!ifExists) {
    VELOX_USER_CHECK(dropped, "Table doesn't exist: {}", tableName.toString());
  }

  return dropped;
}

std::optional<bool> TestConnectorMetadata::addColumn(
    const ConnectorSessionPtr& /* session */,
    const SchemaTableName& tableName,
    const std::string& columnName,
    const velox::TypePtr& columnType,
    bool ifTableExists,
    bool ifNotExists) {
  auto it = tables_.find(tableName);
  if (it == tables_.end()) {
    if (ifTableExists) {
      return std::nullopt;
    }
    VELOX_USER_FAIL("Table does not exist: {}", tableName.toString());
  }

  auto existingType = it->second->type();
  if (existingType->containsChild(columnName)) {
    if (ifNotExists) {
      return false;
    }
    VELOX_USER_FAIL(
        "Column already exists in table {}: {}",
        tableName.toString(),
        columnName);
  }

  VELOX_CHECK(
      it->second->data().empty(),
      "Cannot add column to table '{}' that already has data",
      tableName.toString());

  auto names = existingType->names();
  auto types = existingType->children();
  names.push_back(columnName);
  types.push_back(columnType);
  auto newType = velox::ROW(std::move(names), std::move(types));

  std::vector<std::string> hiddenNames;
  for (const auto* col : it->second->allColumns()) {
    if (col->hidden()) {
      hiddenNames.push_back(col->name());
    }
  }
  auto hiddenColumns = velox::ROW(std::move(hiddenNames), velox::VARCHAR());

  auto savedOptions = it->second->options();
  tables_.erase(it);

  auto newTable = std::make_shared<TestTable>(
      tableName,
      std::move(newType),
      std::move(hiddenColumns),
      connector_,
      std::move(savedOptions));
  tables_[tableName] = std::move(newTable);
  return true;
}

void TestConnectorMetadata::appendData(
    const SchemaTableName& tableName,
    const velox::RowVectorPtr& data) {
  auto it = tables_.find(tableName);
  VELOX_CHECK(
      it != tables_.end(), "Table doesn't exist: {}", tableName.toString());
  it->second->addData(data);
}

void TestConnectorMetadata::setDiscreteValues(
    const SchemaTableName& tableName,
    const std::vector<std::string>& columnNames,
    const std::vector<velox::Variant>& values) {
  auto it = tables_.find(tableName);
  VELOX_CHECK(
      it != tables_.end(), "Table doesn't exist: {}", tableName.toString());

  it->second->mutableLayout()->setDiscreteValues(columnNames, values);
}

void TestConnectorMetadata::setStats(
    const SchemaTableName& tableName,
    uint64_t numRows,
    const std::unordered_map<std::string, ColumnStatistics>& columnStats) {
  auto it = tables_.find(tableName);
  VELOX_CHECK(
      it != tables_.end(), "Table doesn't exist: {}", tableName.toString());
  it->second->setStats(numRows, columnStats);
}

TestDataSource::TestDataSource(
    const velox::RowTypePtr& outputType,
    const velox::connector::ColumnHandleMap& handles,
    TablePtr table,
    velox::memory::MemoryPool* pool)
    : outputType_(outputType), pool_(pool) {
  auto maybeTable = std::dynamic_pointer_cast<const TestTable>(table);
  VELOX_CHECK(
      maybeTable, "Table is not a TestTable: {}", table->name().toString());
  data_ = maybeTable->data();

  auto tableType = table->type();
  outputMappings_.reserve(outputType_->size());
  for (const auto& name : outputType->names()) {
    VELOX_CHECK(
        handles.contains(name),
        "no handle for output column {} for table {}",
        name,
        table->name().toString());
    auto handle = handles.find(name)->second;

    const auto idx = tableType->getChildIdxIfExists(handle->name());
    VELOX_CHECK(
        idx.has_value(),
        "column '{}' not found in table '{}'.",
        handle->name(),
        table->name().toString());
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
  auto* testHandle = dynamic_cast<const TestTableHandle*>(tableHandle.get());
  VELOX_CHECK_NOT_NULL(
      testHandle, "Expected TestTableHandle, got: {}", tableHandle->name());
  auto table = metadata_->findTable(testHandle->schemaTableName());
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
    velox::connector::ConnectorQueryCtx* connectorQueryCtx,
    velox::connector::CommitStrategy) {
  VELOX_CHECK(tableHandle, "table handle must be non-null");
  auto* testHandle =
      dynamic_cast<const TestInsertTableHandle*>(tableHandle.get());
  VELOX_CHECK_NOT_NULL(testHandle, "Expected TestInsertTableHandle");
  auto table = metadata_->findTableInternal(testHandle->tableName());
  VELOX_CHECK(
      table,
      "cannot create data sink for nonexistent table {}",
      testHandle->tableName().toString());
  auto collectColumnStatistics =
      connectorQueryCtx->sessionProperties()->get<bool>(
          TestConfigProvider::kCollectColumnStatistics, true);
  return std::make_unique<TestDataSink>(table, collectColumnStatistics);
}

std::shared_ptr<TestTable> TestConnector::addTable(
    SchemaTableName tableName,
    const velox::RowTypePtr& schema,
    const velox::RowTypePtr& hiddenColumns) {
  return metadata_->addTable(std::move(tableName), schema, hiddenColumns);
}

bool TestConnector::dropTableIfExists(const SchemaTableName& name) {
  return metadata_->dropTableIfExists(name);
}

void TestConnector::registerSerDe() {
  static bool registered = false;
  if (registered) {
    return;
  }
  TestTableHandle::registerSerDe();
  TestColumnHandle::registerSerDe();
  TestConnectorSplit::registerSerDe();
  registered = true;
}

void TestConnector::addTpchTables() {
  for (auto table : velox::tpch::tables) {
    addTable(
        std::string(velox::tpch::toTableName(table)),
        velox::tpch::getTableSchema(table));
  }
}

void TestConnector::createView(
    const SchemaTableName& viewName,
    velox::RowTypePtr type,
    std::string_view text) {
  metadata_->createView(viewName, std::move(type), text);
}

bool TestConnector::dropView(const SchemaTableName& viewName) {
  return metadata_->dropView(viewName);
}

void TestConnector::appendData(
    const SchemaTableName& tableName,
    const velox::RowVectorPtr& data) {
  metadata_->appendData(tableName, data);
}

void TestConnector::setDiscreteValues(
    const SchemaTableName& tableName,
    const std::vector<std::string>& columnNames,
    const std::vector<velox::Variant>& values) {
  metadata_->setDiscreteValues(tableName, columnNames, values);
}

void TestConnector::setStats(
    const SchemaTableName& tableName,
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
    table_->addData(vector, collectColumnStatistics_);
  }
}

} // namespace facebook::axiom::connector
