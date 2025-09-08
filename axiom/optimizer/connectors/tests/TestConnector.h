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

#pragma once

#include "axiom/optimizer/connectors/ConnectorMetadata.h"

namespace facebook::velox::connector {

class TestConnector;

/// The Table and Connector objects to which this layout correspond
/// are specified explicitly at init time. The sample API is
/// overridden to provide placeholder counts.
class TestTableLayout : public TableLayout {
 public:
  TestTableLayout(
      const std::string& name,
      Table* table,
      connector::Connector* connector,
      std::vector<const Column*> columns)
      : TableLayout(
            name,
            table,
            connector,
            std::move(columns),
            /*partitionColumns=*/{},
            /*orderColumns=*/{},
            /*sortOrder=*/{},
            /*lookupKeys=*/{},
            /*supportsScan=*/true) {}

  std::pair<int64_t, int64_t> sample(
      const connector::ConnectorTableHandlePtr&,
      float,
      const std::vector<core::TypedExprPtr>&,
      RowTypePtr,
      const std::vector<common::Subfield>&,
      HashStringAllocator*,
      std::vector<ColumnStatistics>*) const override {
    return std::make_pair(1'000, 1'000);
  }
};

/// RowVectors are appended using the addData() interface and the vector
/// of RowVectors are retrieved using the data() interface. Appended
/// data is copied inside an internal memory pool associated with
/// the table. Row count is determined dynamically using a summation
/// of row counts for RowVectors currently stored within the table.
class TestTable : public Table {
 public:
  TestTable(
      const std::string& name,
      const RowTypePtr& schema,
      TestConnector* connector);

  const std::unordered_map<std::string, const Column*>& columnMap()
      const override {
    return columns_;
  }

  const std::vector<const TableLayout*>& layouts() const override {
    return layouts_;
  }

  uint64_t numRows() const override {
    uint64_t rows = 0;
    for (const auto& vector : data_) {
      rows += vector->size();
    }
    return rows;
  }

  const std::vector<RowVectorPtr>& data() const {
    return data_;
  }

  /// Copy the specified RowVector into the internal data of the
  /// table. The underlying types of the columns must match the
  /// schema specified during initial table creation.
  /// Data is copied on append so that vectors from temporary
  /// memory pools can be appended. These copies are allocated
  /// via the TestTable internal memory pool.
  void addData(const RowVectorPtr& data) {
    VELOX_CHECK(
        data->type()->equivalent(*type()),
        "appended data type {} must match table type {}",
        data->type(),
        type());
    VELOX_CHECK(data->size() > 0, "cannot append empty RowVector");
    auto copy = std::dynamic_pointer_cast<RowVector>(
        BaseVector::copy(*data, pool_.get()));
    data_.push_back(copy);
  }

 private:
  connector::Connector* connector_;
  std::unordered_map<std::string, const Column*> columns_;
  std::vector<std::unique_ptr<Column>> exportedColumns_;
  std::vector<const TableLayout*> layouts_;
  std::vector<std::unique_ptr<TableLayout>> exportedLayouts_;
  std::shared_ptr<memory::MemoryPool> pool_;
  std::vector<RowVectorPtr> data_;
};

/// SplitSource generated via the TestSplitManager embedded in the
/// TestConnector. Generates one default-initialized ConnectorSplit
/// for each partition provided at initialization time. targetBytes
/// are ignored when retrieving splits, each new call to getSplits
/// returns just one ConnectorSplit.
class TestSplitSource : public SplitSource {
 public:
  TestSplitSource(
      const std::string& connectorId,
      const std::vector<PartitionHandlePtr>& partitions)
      : connectorId_(connectorId),
        partitions_(partitions),
        currentPartition_(0) {}

  std::vector<SplitAndGroup> getSplits(uint64_t targetBytes) override;

 private:
  const std::string connectorId_;
  const std::vector<PartitionHandlePtr> partitions_;
  size_t currentPartition_;
};

/// SplitManager embedded in the TestConnector. Returns one
/// default-initialized PartitionHandle upon call to listPartitions.
/// Generates a TestSplitSource containing the provided partition
/// handles upon call to getSplitSource.
class TestSplitManager : public ConnectorSplitManager {
 public:
  std::vector<PartitionHandlePtr> listPartitions(
      const ConnectorTableHandlePtr& tableHandle) override;

  std::shared_ptr<SplitSource> getSplitSource(
      const ConnectorTableHandlePtr& tableHandle,
      const std::vector<PartitionHandlePtr>& partitions,
      SplitOptions options = {}) override;
};

class TestColumnHandle : public ColumnHandle {
 public:
  TestColumnHandle(const std::string& name, const TypePtr& type)
      : name_(name), type_(type) {}

  const std::string& name() const override {
    return name_;
  }

  const TypePtr& type() const {
    return type_;
  }

 private:
  const std::string name_;
  const TypePtr type_;
};

/// The layout corresponding to the handle is provided at
/// initialization time.
class TestTableHandle : public ConnectorTableHandle {
 public:
  TestTableHandle(
      const TableLayout& layout,
      std::vector<ColumnHandlePtr> columnHandles,
      std::vector<core::TypedExprPtr> filters = {})
      : ConnectorTableHandle(layout.connector()->connectorId()),
        layout_(layout),
        columnHandles_(std::move(columnHandles)),
        filters_(std::move(filters)) {}

  const std::string& name() const override {
    return layout_.table().name();
  }

  std::string toString() const override {
    return name();
  }

  const TableLayout& layout() const {
    return layout_;
  }

  const std::vector<core::TypedExprPtr>& filters() const {
    return filters_;
  }

  const std::vector<ColumnHandlePtr>& columnHandles() const {
    return columnHandles_;
  }

 private:
  const TableLayout& layout_;
  const std::vector<ColumnHandlePtr> columnHandles_;
  const std::vector<core::TypedExprPtr> filters_;
};

/// The TestInsertTableHandle should be populated using the table
/// name as the name parameter so that lookups can be performed
/// against the ConnectorMetadata table map.
class TestInsertTableHandle : public ConnectorInsertTableHandle {
 public:
  explicit TestInsertTableHandle(const std::string& name) : name_(name) {}

  std::string toString() const override {
    return name_;
  }

 private:
  const std::string name_;
};

/// Contains an in-memory map of TestTables inserted via the createTable
/// API. Tables are retrieved by name using the findTable API. The
/// splitManager API returns a TestSplitManager. createColumnHandle
/// returns a TestColumnHandle for the specified layout and column.
/// createTableHandle returns a TestTableHandle for the specified
/// layout. Filter pushdown is not supported.
class TestConnectorMetadata : public ConnectorMetadata {
 public:
  explicit TestConnectorMetadata(TestConnector* connector)
      : connector_(connector),
        splitManager_(std::make_unique<TestSplitManager>()) {}

  void initialize() override {}

  TablePtr findTable(const std::string& name) override;

  /// Non-interface method which supplies a non-const Table reference
  /// which is capable of performing writes to the underlying table.
  std::shared_ptr<Table> findTableInternal(const std::string& name);

  ConnectorSplitManager* splitManager() override {
    return splitManager_.get();
  }

  ColumnHandlePtr createColumnHandle(
      const TableLayout& layout,
      const std::string& columnName,
      std::vector<common::Subfield> subfields = {},
      std::optional<TypePtr> castToType = std::nullopt,
      SubfieldMapping subfieldMapping = {}) override;

  ConnectorTableHandlePtr createTableHandle(
      const TableLayout& layout,
      std::vector<ColumnHandlePtr> columnHandles,
      core::ExpressionEvaluator& evaluator,
      std::vector<core::TypedExprPtr> filters,
      std::vector<core::TypedExprPtr>& rejectedFilters,
      RowTypePtr dataColumns,
      std::optional<LookupKeys>) override;

  void createTable(
      const std::string& tableName,
      const RowTypePtr& rowType,
      const std::unordered_map<std::string, std::string>& options,
      const ConnectorSessionPtr& session,
      bool errorIfExists = true,
      TableKind tableKind = TableKind::kTable) override {
    VELOX_UNSUPPORTED();
  }

  ConnectorInsertTableHandlePtr createInsertTableHandle(
      const TableLayout& layout,
      const RowTypePtr& rowType,
      const std::unordered_map<std::string, std::string>& options,
      WriteKind kind,
      const ConnectorSessionPtr& session) override {
    VELOX_UNSUPPORTED();
  }

  WritePartitionInfo writePartitionInfo(
      const ConnectorInsertTableHandlePtr& handle) override {
    VELOX_UNSUPPORTED();
  }

  void finishWrite(
      const TableLayout& layout,
      const ConnectorInsertTableHandlePtr& handle,
      const std::vector<RowVectorPtr>& writerResult,
      WriteKind kind,
      const ConnectorSessionPtr& session) override {
    VELOX_UNSUPPORTED();
  }

  std::vector<ColumnHandlePtr> rowIdHandles(
      const TableLayout& layout,
      WriteKind kind) override {
    VELOX_UNSUPPORTED();
  }

  /// Create and return a TestTable with the specified name and schema in the
  /// in-memory map maintained in the connector metadata. If the table already
  /// exists, an error is thrown.
  std::shared_ptr<TestTable> createTable(
      const std::string& name,
      const RowTypePtr& schema);

  /// Add data rows to the specified table. This data is returned via the
  /// DataSource corresponding to this table. The data is copied
  /// into the internal memory pool associated with the table.
  void appendData(const std::string& name, const RowVectorPtr& data);

 private:
  TestConnector* connector_;
  std::unordered_map<std::string, std::shared_ptr<TestTable>> tables_;
  std::unique_ptr<TestSplitManager> splitManager_;
};

/// At DataSource creation time, the data contained in the corresponding Table
/// object is retrieved and cached. On each call to next(), one RowVectorPtr
/// returned to the caller, followed by nullptr once data is exhausted.
/// Runtime stats are not populated for the data source.
class TestDataSource : public DataSource {
 public:
  TestDataSource(
      const RowTypePtr& outputType,
      const ColumnHandleMap& handles,
      TablePtr table,
      memory::MemoryPool* pool);

  void addSplit(std::shared_ptr<ConnectorSplit> split) override;

  std::optional<RowVectorPtr> next(uint64_t size, velox::ContinueFuture& future)
      override;

  void addDynamicFilter(
      column_index_t outputChannel,
      const std::shared_ptr<common::Filter>& filter) override;

  uint64_t getCompletedBytes() override {
    return completedBytes_;
  }

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override {
    return {};
  }

 private:
  const RowTypePtr outputType_;
  velox::memory::MemoryPool* pool_;
  std::shared_ptr<ConnectorSplit> split_;
  std::vector<RowVectorPtr> data_;
  std::vector<column_index_t> outputMappings_;
  uint64_t completedBytes_{0};
  uint64_t completedRows_{0};
  uint64_t idx_{0};
};

/// Contains an embedded TestConnectorMetadata to which TestTables are
/// added at runtime using the createTable API. Data is appended to a
/// TestTable via the appendData method. createDataSource creates a
/// TestDataSource object which returns appended data. createDataSink
/// creates a TestDataSink object which appends additional data to
/// the associated table.
class TestConnector : public Connector {
 public:
  explicit TestConnector(const std::string& id)
      : Connector(id),
        metadata_{std::make_unique<TestConnectorMetadata>(this)} {}

  ConnectorMetadata* metadata() const override {
    return metadata_.get();
  }

  bool supportsSplitPreload() const override {
    return true;
  }

  bool canAddDynamicFilter() const override {
    return false;
  }

  std::unique_ptr<DataSource> createDataSource(
      const RowTypePtr& outputType,
      const ConnectorTableHandlePtr& tableHandle,
      const ColumnHandleMap& columnHandles,
      ConnectorQueryCtx* connectorQueryCtx) override;

  std::unique_ptr<DataSink> createDataSink(
      RowTypePtr inputType,
      ConnectorInsertTableHandlePtr connectorInsertTableHandle,
      ConnectorQueryCtx* connectorQueryCtx,
      CommitStrategy commitStrategy) override;

  /// Add a TestTable with the specified name and schema to the
  /// TestConnectorMetadata corresponding to this connector.
  std::shared_ptr<TestTable> createTable(
      const std::string& name,
      const RowTypePtr& schema = ROW({}, {}));

  /// Add data rows to the specified table. This data is returned via the
  /// DataSource corresponding to this table. Appended data is copied
  /// to the internal memory pool of the associated table.
  void appendData(const std::string& name, const RowVectorPtr& data);

 private:
  const std::unique_ptr<TestConnectorMetadata> metadata_;
};

/// The ConnectorFactory for the TestConnector can be configured with
/// any desired connector name in order to inject the TestConnector
/// into workflows which generate connectors using factory interfaces.
class TestConnectorFactory : public ConnectorFactory {
 public:
  explicit TestConnectorFactory(const char* name) : ConnectorFactory(name) {}

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config = nullptr,
      folly::Executor* ioExecutor = nullptr,
      folly::Executor* cpuExecutor = nullptr) override;
};

/// Data appended to the sink is copied to the internal data vector
/// contained in the corresponding table.
class TestDataSink : public DataSink {
 public:
  explicit TestDataSink(std::shared_ptr<Table> table) {
    table_ = std::dynamic_pointer_cast<TestTable>(table);
    VELOX_CHECK(table_, "table {} not a TestTable", table->name());
  }

  /// Data is copied to the memory pool internal to the
  /// corresponding Table object and appended to the Table's
  /// data buffer.
  void appendData(RowVectorPtr vector) override;

  /// Data append is completed inside appendData, so the finish()
  /// interface is treated as a no-op.
  bool finish() override {
    return true;
  }

  std::vector<std::string> close() override {
    return {};
  }

  void abort() override {}

  Stats stats() const override {
    return {};
  }

 private:
  std::shared_ptr<TestTable> table_;
};

} // namespace facebook::velox::connector
