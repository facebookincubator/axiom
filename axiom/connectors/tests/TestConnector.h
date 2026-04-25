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

#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>
#include "axiom/common/SchemaTableName.h"
#include "axiom/connectors/ConnectorMetadata.h"
#include "velox/core/ITypedExpr.h"

namespace facebook::axiom::connector {

class TestConnector;

/// The Table and Connector objects to which this layout correspond
/// are specified explicitly at init time.
class TestTableLayout : public TableLayout {
 public:
  TestTableLayout(
      const std::string& label,
      Table* table,
      velox::connector::Connector* connector,
      std::vector<const Column*> columns)
      : TableLayout(
            label,
            table,
            connector,
            std::move(columns),
            /*partitionColumns=*/{},
            /*orderColumns=*/{},
            /*sortOrder=*/{},
            /*lookupKeys=*/{},
            /*supportsScan=*/true) {}

  /// Records discrete values to use in 'discretePredicateColumns' and
  /// 'discretePredicates' APIs. If called repeatedly, overwrites previous
  /// values.
  void setDiscreteValues(
      const std::vector<std::string>& columnNames,
      const std::vector<velox::Variant>& values);

  std::span<const Column* const> discretePredicateColumns() const override;

  std::unique_ptr<DiscretePredicates> discretePredicates(
      const std::vector<const Column*>& columns) const override;

  velox::connector::ColumnHandlePtr createColumnHandle(
      const ConnectorSessionPtr& session,
      const std::string& columnName,
      std::vector<velox::common::Subfield> subfields,
      std::optional<velox::TypePtr> castToType,
      SubfieldMapping subfieldMapping) const override;

  velox::connector::ConnectorTableHandlePtr createTableHandle(
      const ConnectorSessionPtr& session,
      std::vector<velox::connector::ColumnHandlePtr> columnHandles,
      velox::core::ExpressionEvaluator& evaluator,
      std::vector<velox::core::TypedExprPtr> filters,
      std::vector<velox::core::TypedExprPtr>& rejectedFilters,
      velox::RowTypePtr dataColumns,
      std::optional<LookupKeys> lookupKeys) const override;

 private:
  std::vector<const Column*> discreteValueColumns_;
  std::vector<velox::Variant> discreteValues_;
};

/// RowVectors are appended using the addData() interface and the vector
/// of RowVectors are retrieved using the data() interface. Appended
/// data is copied inside an internal memory pool associated with
/// the table. Row count is determined dynamically using a summation
/// of row counts for RowVectors currently stored within the table.
class TestTable : public Table {
 public:
  TestTable(
      SchemaTableName name,
      const velox::RowTypePtr& schema,
      const velox::RowTypePtr& hiddenColumns,
      TestConnector* connector,
      folly::F14FastMap<std::string, velox::Variant> options);

  const std::vector<const TableLayout*>& layouts() const override {
    return layouts_;
  }

  uint64_t numRows() const override {
    return data_.empty() ? numRows_ : dataRows_;
  }

  const std::vector<velox::RowVectorPtr>& data() const {
    return data_;
  }

  /// Appends a RowVector to the table's data. Each appended vector generates
  /// a separate TestConnectorSplit. Data is copied into the table's internal
  /// memory pool. When 'collectColumnStatistics' is true, computes per-column
  /// statistics incrementally (numDistinct, min/max, nullPct, maxLength).
  /// Cannot be combined with setStats on the same table.
  void addData(
      const velox::RowVectorPtr& data,
      bool collectColumnStatistics = true);

  TestTableLayout* mutableLayout() {
    return exportedLayout_.get();
  }

  /// Sets row count and column statistics without adding actual data.
  /// Use this to create tables with controlled statistics for optimizer
  /// testing. Cannot be combined with addData on the same table.
  void setStats(
      uint64_t numRows,
      const std::unordered_map<std::string, ColumnStatistics>& columnStats);

 private:
  // Per-column state for incremental stat computation during addData.
  struct ColumnTracker {
    // Updates tracker state with values from 'vector'.
    void append(const velox::BaseVector& vector);

    // Builds ColumnStatistics from accumulated state.
    std::unique_ptr<ColumnStatistics> toColumnStatistics(
        uint64_t totalRows,
        const velox::TypePtr& type) const;

    folly::F14FastSet<uint64_t> distinctHashes;
    uint64_t nullCount{0};
    std::optional<velox::Variant> min;
    std::optional<velox::Variant> max;
    int64_t totalLength{0};
    int32_t maxLength{0};
  };

  velox::connector::Connector* connector_;
  std::vector<const TableLayout*> layouts_;
  std::unique_ptr<TestTableLayout> exportedLayout_;
  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::vector<velox::RowVectorPtr> data_;
  uint64_t numRows_{0};
  uint64_t dataRows_{0};
  std::vector<ColumnTracker> columnTrackers_;
};

/// SplitSource generated via the TestSplitManager embedded in the
/// TestConnector. Generates one TestConnectorSplit for each RowVector
/// in the table's data_ vector.
class TestSplitSource : public SplitSource {
 public:
  TestSplitSource(const std::string& connectorId, size_t splitCount)
      : connectorId_(connectorId), splitCount_(splitCount) {}

  folly::coro::Task<SplitBatch> co_getSplits(
      uint32_t maxSplitCount,
      int32_t /*bucket*/) override;

 private:
  const std::string connectorId_;
  const size_t splitCount_;
  size_t nextIndex_{0};
};

/// SplitManager embedded in the TestConnector. Returns one
/// default-initialized PartitionHandle upon call to co_listPartitions.
/// Generates a TestSplitSource that produces one TestConnectorSplit
/// per RowVector in the table's data_ vector.
class TestSplitManager : public ConnectorSplitManager {
 public:
  folly::coro::Task<std::vector<PartitionHandlePtr>> co_listPartitions(
      const ConnectorSessionPtr& session,
      const velox::connector::ConnectorTableHandlePtr& tableHandle) override;

  std::shared_ptr<SplitSource> getSplitSource(
      const ConnectorSessionPtr& session,
      const velox::connector::ConnectorTableHandlePtr& tableHandle,
      const std::vector<PartitionHandlePtr>& partitions,
      SplitOptions options = {}) override;
};

class TestColumnHandle : public velox::connector::ColumnHandle {
 public:
  TestColumnHandle(const std::string& name, const velox::TypePtr& type)
      : name_(name), type_(type) {}

  const std::string& name() const override {
    return name_;
  }

  const velox::TypePtr& type() const {
    return type_;
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = TestColumnHandle::getClassName();
    obj["columnName"] = name_;
    obj["columnType"] = type_->serialize();
    return obj;
  }

  static std::shared_ptr<TestColumnHandle> create(const folly::dynamic& obj) {
    auto name = obj["columnName"].asString();
    auto type = velox::Type::create(obj["columnType"]);
    return std::make_shared<TestColumnHandle>(name, type);
  }

  static void registerSerDe() {
    velox::registerDeserializer<TestColumnHandle>();
  }

  VELOX_DEFINE_CLASS_NAME(TestColumnHandle)

 private:
  const std::string name_;
  const velox::TypePtr type_;
};

/// Connector split containing an index into the data_ vector of the table.
/// Each split yields the RowVector at the corresponding index in the table
/// data.
class TestConnectorSplit : public velox::connector::ConnectorSplit {
 public:
  TestConnectorSplit(const std::string& connectorId, size_t index)
      : ConnectorSplit(connectorId), index_(index) {}

  size_t index() const {
    return index_;
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = TestConnectorSplit::getClassName();
    obj["connectorId"] = connectorId;
    obj["index"] = index_;
    return obj;
  }

  static std::shared_ptr<TestConnectorSplit> create(const folly::dynamic& obj) {
    auto connectorId = obj["connectorId"].asString();
    auto index = obj["index"].asInt();
    return std::make_shared<TestConnectorSplit>(connectorId, index);
  }

  static void registerSerDe() {
    velox::registerDeserializer<TestConnectorSplit>();
  }

  VELOX_DEFINE_CLASS_NAME(TestConnectorSplit)

 private:
  const size_t index_;
};

/// The layout corresponding to the handle is provided at
/// initialization time.
class TestTableHandle : public velox::connector::ConnectorTableHandle {
 public:
  TestTableHandle(
      const std::string& connectorId,
      const SchemaTableName& name,
      int64_t size,
      std::vector<velox::connector::ColumnHandlePtr> columnHandles,
      std::vector<velox::core::TypedExprPtr> filters = {})
      : ConnectorTableHandle(connectorId),
        name_(name),
        size_(size),
        columnHandles_(std::move(columnHandles)),
        filters_(std::move(filters)),
        nameString_(name.toString()) {}

  TestTableHandle(
      const TableLayout& layout,
      std::vector<velox::connector::ColumnHandlePtr> columnHandles,
      std::vector<velox::core::TypedExprPtr> filters = {})
      : TestTableHandle(
            layout.connector()->connectorId(),
            layout.table().name(),
            getTableSize(layout),
            std::move(columnHandles),
            std::move(filters)) {}

  static int64_t getTableSize(const TableLayout& layout) {
    auto& table = dynamic_cast<const TestTable&>(layout.table());
    return table.data().size();
  }

  const SchemaTableName& schemaTableName() const {
    return name_;
  }

  const std::string& name() const override {
    return nameString_;
  }

  int64_t size() const {
    return size_;
  }

  std::string toString() const override {
    return name();
  }

  const std::vector<velox::core::TypedExprPtr>& filters() const {
    return filters_;
  }

  const std::vector<velox::connector::ColumnHandlePtr>& columnHandles() const {
    return columnHandles_;
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = TestTableHandle::getClassName();
    obj["connectorId"] = connectorId();
    obj["schemaName"] = name_.schema;
    obj["tableName"] = name_.table;
    obj["size"] = size_;
    folly::dynamic columns = folly::dynamic::array;
    for (const auto& handle : columnHandles_) {
      columns.push_back(handle->serialize());
    }
    obj["columnHandles"] = std::move(columns);
    folly::dynamic filterArray = folly::dynamic::array;
    for (const auto& filter : filters_) {
      filterArray.push_back(filter->serialize());
    }
    obj["filters"] = std::move(filterArray);
    return obj;
  }

  static velox::connector::ConnectorTableHandlePtr create(
      const folly::dynamic& obj,
      void* context) {
    auto connectorId = obj["connectorId"].asString();
    auto schema = obj["schemaName"].asString();
    auto tableName = obj["tableName"].asString();
    auto size = obj["size"].asInt();
    std::vector<velox::connector::ColumnHandlePtr> columnHandles;
    if (obj.count("columnHandles")) {
      for (const auto& col : obj["columnHandles"]) {
        columnHandles.push_back(
            velox::ISerializable::deserialize<velox::connector::ColumnHandle>(
                col));
      }
    }
    std::vector<velox::core::TypedExprPtr> filters;
    if (obj.count("filters")) {
      for (const auto& f : obj["filters"]) {
        filters.push_back(
            velox::ISerializable::deserialize<velox::core::ITypedExpr>(
                f, context));
      }
    }
    return std::make_shared<TestTableHandle>(
        connectorId,
        SchemaTableName(schema, tableName),
        size,
        std::move(columnHandles),
        std::move(filters));
  }

  static void registerSerDe() {
    velox::registerDeserializerWithContext<TestTableHandle>();
  }

  VELOX_DEFINE_CLASS_NAME(TestTableHandle)

 private:
  const SchemaTableName name_;
  const int64_t size_;
  const std::vector<velox::connector::ColumnHandlePtr> columnHandles_;
  const std::vector<velox::core::TypedExprPtr> filters_;
  const std::string nameString_;
};

/// The TestInsertTableHandle should be populated using the table
/// name as the name parameter so that lookups can be performed
/// against the ConnectorMetadata table map.
class TestInsertTableHandle
    : public velox::connector::ConnectorInsertTableHandle {
 public:
  explicit TestInsertTableHandle(SchemaTableName tableName)
      : tableName_(std::move(tableName)) {}

  const SchemaTableName& tableName() const {
    return tableName_;
  }

  std::string toString() const override {
    return tableName_.toString();
  }

 private:
  const SchemaTableName tableName_;
};

/// Contains an in-memory map of TestTables inserted via the addTable
/// API. Tables are retrieved by name using the findTable API. The
/// splitManager API returns a TestSplitManager. createColumnHandle
/// returns a TestColumnHandle for the specified layout and column.
/// createTableHandle returns a TestTableHandle for the specified
/// layout. Filter pushdown is not supported.
class TestConnectorMetadata : public ConnectorMetadata {
 public:
  static constexpr std::string_view kDefaultSchema = "default";

  /// CREATE TABLE property to mark columns as hidden.
  /// Example: WITH (hidden = ARRAY['col1', 'col2']).
  static constexpr std::string_view kHidden = "hidden";

  /// CREATE TABLE property to mark columns for EXPLAIN IO output.
  /// Example: WITH (explain_io = ARRAY['ds']).
  static constexpr std::string_view kExplainIo = "explain_io";

  explicit TestConnectorMetadata(TestConnector* connector)
      : connector_(connector),
        splitManager_(std::make_unique<TestSplitManager>()) {}

  TablePtr findTable(const SchemaTableName& tableName) override;

  /// Non-interface method which supplies a non-const Table reference
  /// which is capable of performing writes to the underlying table.
  std::shared_ptr<Table> findTableInternal(const SchemaTableName& tableName);

  ConnectorSplitManager* splitManager() override {
    return splitManager_.get();
  }

  /// Creates and returns a TestTable with the specified name and schema in the
  /// in-memory map maintained in the connector metadata. Throws if the table
  /// already exists.
  std::shared_ptr<TestTable> addTable(
      SchemaTableName tableName,
      const velox::RowTypePtr& schema,
      const velox::RowTypePtr& hiddenColumns);

  /// Appends data to the table with the specified name.
  void appendData(
      const SchemaTableName& tableName,
      const velox::RowVectorPtr& data);

  void setDiscreteValues(
      const SchemaTableName& tableName,
      const std::vector<std::string>& columnNames,
      const std::vector<velox::Variant>& values);

  /// See TestTable::setStats.
  void setStats(
      const SchemaTableName& tableName,
      uint64_t numRows,
      const std::unordered_map<std::string, ColumnStatistics>& columnStats);

  TablePtr createTable(
      const ConnectorSessionPtr& session,
      const SchemaTableName& tableName,
      const velox::RowTypePtr& rowType,
      const folly::F14FastMap<std::string, velox::Variant>& options,
      bool explain) override;

  ConnectorWriteHandlePtr beginWrite(
      const ConnectorSessionPtr& session,
      const TablePtr& table,
      WriteKind kind,
      bool explain) override;

  RowsFuture finishWrite(
      const ConnectorSessionPtr& session,
      const ConnectorWriteHandlePtr& handle,
      const std::vector<velox::RowVectorPtr>& writeResults,
      velox::RowVectorPtr groupingKeys,
      std::vector<std::vector<ColumnStatistics>> groupStats) override;

  bool dropTable(
      const ConnectorSessionPtr& session,
      const SchemaTableName& tableName,
      bool ifExists) override;

  /// Shortcut for dropTable(session, tableName, true).
  bool dropTableIfExists(const SchemaTableName& tableName) {
    return dropTable(nullptr, tableName, true);
  }

  ViewPtr findView(const SchemaTableName& tableName) override;

  /// Register a view with the given name, output schema, and SQL text.
  void createView(
      const SchemaTableName& viewName,
      velox::RowTypePtr type,
      std::string_view text);

  /// Remove a view by name. Returns true if the view existed.
  bool dropView(const SchemaTableName& viewName);

  std::vector<std::string> listSchemaNames(
      const ConnectorSessionPtr& session) override;

  bool schemaExists(
      const ConnectorSessionPtr& session,
      const std::string& schemaName) override;

  std::vector<std::string> listTableNames(
      const ConnectorSessionPtr& session,
      const std::string& schemaName) override;

  void createSchema(
      const ConnectorSessionPtr& session,
      const std::string& schemaName,
      bool ifNotExists,
      const folly::F14FastMap<std::string, velox::Variant>& properties)
      override;

  void dropSchema(
      const ConnectorSessionPtr& session,
      const std::string& schemaName,
      bool ifExists) override;

 private:
  TestConnector* connector_;
  folly::F14FastMap<SchemaTableName, std::shared_ptr<TestTable>> tables_;
  std::unique_ptr<TestSplitManager> splitManager_;

  struct ViewDefinition {
    velox::RowTypePtr type;
    std::string text;
  };
  folly::F14FastMap<SchemaTableName, ViewDefinition> views_;

  folly::F14FastSet<std::string> schemas_{"default"};
};

/// At DataSource creation time, the data contained in the corresponding Table
/// object is retrieved and cached. On each call to next(), one RowVectorPtr
/// returned to the caller, followed by nullptr once data is exhausted.
/// Runtime stats are not populated for the data source.
class TestDataSource : public velox::connector::DataSource {
 public:
  TestDataSource(
      const velox::RowTypePtr& outputType,
      const velox::connector::ColumnHandleMap& handles,
      TablePtr table,
      velox::memory::MemoryPool* pool);

  void addSplit(
      std::shared_ptr<velox::connector::ConnectorSplit> split) override;

  std::optional<velox::RowVectorPtr> next(
      uint64_t size,
      velox::ContinueFuture& future) override;

  void addDynamicFilter(
      velox::column_index_t outputChannel,
      const std::shared_ptr<velox::common::Filter>& filter) override;

  uint64_t getCompletedBytes() override {
    return completedBytes_;
  }

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  std::unordered_map<std::string, velox::RuntimeMetric> getRuntimeStats()
      override {
    return {};
  }

 private:
  const velox::RowTypePtr outputType_;
  velox::memory::MemoryPool* pool_;
  std::shared_ptr<TestConnectorSplit> split_;
  std::vector<velox::RowVectorPtr> data_;
  std::vector<velox::column_index_t> outputMappings_;
  uint64_t completedBytes_{0};
  uint64_t completedRows_{0};
  bool more_{false};
};

/// ConfigProvider for TestConnector session properties.
class TestConfigProvider : public velox::config::ConfigProvider {
 public:
  static constexpr const char* kCollectColumnStatistics =
      "collect_column_statistics";

  std::vector<velox::config::ConfigProperty> properties() const override {
    return {
        {kCollectColumnStatistics,
         velox::config::ConfigPropertyType::kBoolean,
         "true",
         "Collect per-column statistics when writing data to a table."},
    };
  }

  std::string normalize(std::string_view /*name*/, std::string_view value)
      const override {
    return std::string(value);
  }
};

/// Contains an embedded TestConnectorMetadata to which TestTables are
/// added at runtime using the addTable API. Data is appended to a
/// TestTable via the appendData method. createDataSource creates a
/// TestDataSource object which returns appended data. createDataSink
/// creates a TestDataSink object which appends additional data to
/// the associated table.
class TestConnector : public velox::connector::Connector {
 public:
  static constexpr std::string_view kDefaultSchema =
      TestConnectorMetadata::kDefaultSchema;

  explicit TestConnector(
      const std::string& id,
      std::shared_ptr<const velox::config::ConfigBase> config = nullptr)
      : Connector(id, std::move(config)),
        metadata_{std::make_shared<TestConnectorMetadata>(this)} {
    registerSerDe();
    ConnectorMetadata::registerMetadata(id, metadata_);
  }

  ~TestConnector() override {
    ConnectorMetadata::unregisterMetadata(connectorId());
  }

  const velox::config::ConfigProvider* configProvider() const override {
    static const TestConfigProvider kProvider;
    return &kProvider;
  }

  bool supportsSplitPreload() const override {
    return true;
  }

  bool canAddDynamicFilter() const override {
    return false;
  }

  std::unique_ptr<velox::connector::DataSource> createDataSource(
      const velox::RowTypePtr& outputType,
      const velox::connector::ConnectorTableHandlePtr& tableHandle,
      const velox::connector::ColumnHandleMap& columnHandles,
      velox::connector::ConnectorQueryCtx* connectorQueryCtx) override;

  std::unique_ptr<velox::connector::DataSink> createDataSink(
      velox::RowTypePtr inputType,
      velox::connector::ConnectorInsertTableHandlePtr
          connectorInsertTableHandle,
      velox::connector::ConnectorQueryCtx* connectorQueryCtx,
      velox::connector::CommitStrategy commitStrategy) override;

  /// Adds a TestTable with the specified name and schema. Throws if a table
  /// with the same name already exists.
  std::shared_ptr<TestTable> addTable(
      SchemaTableName tableName,
      const velox::RowTypePtr& schema,
      const velox::RowTypePtr& hiddenColumns = velox::ROW({}));

  /// Convenience overload that uses kDefaultSchema as the schema.
  std::shared_ptr<TestTable> addTable(
      const std::string& name,
      const velox::RowTypePtr& schema,
      const velox::RowTypePtr& hiddenColumns = velox::ROW({})) {
    return addTable({std::string(kDefaultSchema), name}, schema, hiddenColumns);
  }

  /// Appends data to the table with the specified name.
  void appendData(
      const SchemaTableName& tableName,
      const velox::RowVectorPtr& data);

  /// Convenience overload that uses kDefaultSchema as the schema.
  void appendData(std::string_view name, const velox::RowVectorPtr& data) {
    appendData({std::string(kDefaultSchema), std::string(name)}, data);
  }

  void setDiscreteValues(
      const SchemaTableName& tableName,
      const std::vector<std::string>& columnNames,
      const std::vector<velox::Variant>& values);

  /// Convenience overload that uses kDefaultSchema as the schema.
  void setDiscreteValues(
      const std::string& name,
      const std::vector<std::string>& columnNames,
      const std::vector<velox::Variant>& values) {
    setDiscreteValues({std::string(kDefaultSchema), name}, columnNames, values);
  }

  /// Sets statistics for the table with the specified name.
  void setStats(
      const SchemaTableName& tableName,
      uint64_t numRows,
      const std::unordered_map<std::string, ColumnStatistics>& columnStats);

  /// Convenience overload that uses kDefaultSchema as the schema.
  void setStats(
      const std::string& tableName,
      uint64_t numRows,
      const std::unordered_map<std::string, ColumnStatistics>& columnStats) {
    setStats({std::string(kDefaultSchema), tableName}, numRows, columnStats);
  }

  bool dropTableIfExists(const SchemaTableName& name);

  bool dropTableIfExists(std::string_view tableName) {
    return dropTableIfExists(
        {std::string(kDefaultSchema), std::string(tableName)});
  }

  static void registerSerDe();

  void addTpchTables();

  /// Register a view with the given name, output schema, and SQL text.
  void createView(
      const SchemaTableName& viewName,
      velox::RowTypePtr type,
      std::string_view text);

  /// Remove a view by name. Returns true if the view existed.
  bool dropView(const SchemaTableName& viewName);

 private:
  const std::shared_ptr<TestConnectorMetadata> metadata_;
};

/// The ConnectorFactory for the TestConnector can be configured with
/// any desired connector name in order to inject the TestConnector
/// into workflows which generate connectors using factory interfaces.
class TestConnectorFactory : public velox::connector::ConnectorFactory {
 public:
  explicit TestConnectorFactory(const char* name) : ConnectorFactory(name) {}

  std::shared_ptr<velox::connector::Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const velox::config::ConfigBase> config = nullptr,
      folly::Executor* ioExecutor = nullptr,
      folly::Executor* cpuExecutor = nullptr) override;
};

/// Data appended to the sink is copied to the internal data vector
/// contained in the corresponding table.
class TestDataSink : public velox::connector::DataSink {
 public:
  TestDataSink(std::shared_ptr<Table> table, bool collectStats)
      : collectColumnStatistics_(collectStats) {
    table_ = std::dynamic_pointer_cast<TestTable>(table);
    VELOX_CHECK(table_, "table {} not a TestTable", table->name().toString());
  }

  /// Data is copied to the memory pool internal to the
  /// corresponding Table object and appended to the Table's
  /// data buffer.
  void appendData(velox::RowVectorPtr vector) override;

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
  bool collectColumnStatistics_;
};

} // namespace facebook::axiom::connector
