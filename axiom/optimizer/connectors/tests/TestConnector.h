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

// Forward declarations
class TestConnector;

class TestTableLayout : public TableLayout {
 public:
  TestTableLayout(
      const std::string& name,
      const Table* table,
      connector::Connector* connector,
      std::vector<const Column*> columns)
      : TableLayout(
            name,
            table,
            connector,
            std::move(columns),
            {}, // partitionColumns
            {}, // orderColumns
            {}, // sortOrder
            {}, // lookupKeys
            true) {} // supportsScan

  const std::vector<const Column*>& columns() const {
    return TableLayout::columns();
  }

  std::pair<int64_t, int64_t> sample(
      const connector::ConnectorTableHandlePtr& handle,
      float pct,
      std::vector<core::TypedExprPtr> extraFilters,
      RowTypePtr outputType = nullptr,
      const std::vector<common::Subfield>& fields = {},
      HashStringAllocator* allocator = nullptr,
      std::vector<ColumnStatistics>* statistics = nullptr) const override;
};

class TestTable : public Table {
 public:
  TestTable(
      const std::string& name,
      const RowTypePtr& schema,
      TestConnector* connector,
      uint64_t rows);

  const std::unordered_map<std::string, const Column*>& columnMap()
      const override {
    return columns_;
  }

  const std::vector<const TableLayout*>& layouts() const override {
    return layouts_;
  }

  uint64_t numRows() const override {
    return rows_;
  }

 private:
  std::vector<const Column*> getColumnVector() const;

  connector::Connector* connector_;
  uint64_t rows_;
  std::unordered_map<std::string, const Column*> columns_;
  std::vector<std::unique_ptr<Column>> exportedColumns_;
  std::vector<const TableLayout*> layouts_;
  std::vector<std::unique_ptr<TableLayout>> exportedLayouts_;
};

class TestSplit : public ConnectorSplit {
 public:
  explicit TestSplit(const std::string& connectorId)
      : ConnectorSplit(connectorId) {}
};

class TestSplitSource : public SplitSource {
 public:
  TestSplitSource(
      const std::string& connectorId,
      const std::vector<std::shared_ptr<const PartitionHandle>>& partitions)
      : connectorId_(connectorId),
        partitions_(partitions),
        currentPartition_(0) {}

  std::vector<SplitAndGroup> getSplits(uint64_t targetBytes) override;

 private:
  std::string connectorId_;
  std::vector<std::shared_ptr<const PartitionHandle>> partitions_;
  size_t currentPartition_;
};

class TestSplitManager : public ConnectorSplitManager {
 public:
  std::vector<std::shared_ptr<const PartitionHandle>> listPartitions(
      const ConnectorTableHandlePtr& tableHandle) override;

  std::shared_ptr<SplitSource> getSplitSource(
      const ConnectorTableHandlePtr& tableHandle,
      std::vector<std::shared_ptr<const PartitionHandle>> partitions,
      SplitOptions options = {}) override;
};

class TestColumnHandle : public ColumnHandle {
 public:
  TestColumnHandle(const std::string& name, TypePtr type)
      : name_(name), type_(std::move(type)) {}

  const std::string& name() const override {
    return name_;
  }

  const TypePtr& type() const {
    return type_;
  }

 private:
  std::string name_;
  TypePtr type_;
};

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
    return layout_.table()->name();
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
  std::vector<ColumnHandlePtr> columnHandles_;
  std::vector<core::TypedExprPtr> filters_;
};

class TestConnectorMetadata : public ConnectorMetadata {
 public:
  explicit TestConnectorMetadata(TestConnector* connector)
      : connector_(connector),
        splitManager_(std::make_unique<TestSplitManager>()) {}

  void initialize() override {}

  const Table* findTable(const std::string& name) override;

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
      RowTypePtr dataColumns = nullptr,
      std::optional<LookupKeys> = std::nullopt) override;

  void addTable(
      const std::string& name,
      const RowTypePtr& schema,
      TestConnector* connector,
      uint64_t rows);

 private:
  TestConnector* connector_;
  std::unordered_map<std::string, std::unique_ptr<TestTable>> tables_;
  std::unique_ptr<TestSplitManager> splitManager_;
};

class TestDataSource : public DataSource {
 public:
  TestDataSource(
      const RowTypePtr& outputType,
      const ConnectorTableHandlePtr& tableHandle);

  ~TestDataSource() override {
    data_.reset();
  }

  void addSplit(std::shared_ptr<ConnectorSplit> split) override;

  std::optional<RowVectorPtr> next(uint64_t size, velox::ContinueFuture& future)
      override;

  void addDynamicFilter(
      column_index_t outputChannel,
      const std::shared_ptr<common::Filter>& filter) override;

  uint64_t getCompletedBytes() override {
    return 0;
  }

  uint64_t getCompletedRows() override {
    return 0;
  }

  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override {
    return {};
  }

 private:
  memory::MemoryPool* pool() const;

 private:
  RowTypePtr outputType_;
  ConnectorTableHandlePtr tableHandle_;
  std::shared_ptr<ConnectorSplit> split_;
  RowVectorPtr data_;
  std::shared_ptr<memory::MemoryPool> pool_;
  bool hasData_{false};
};

class TestConnector : public Connector {
 public:
  explicit TestConnector(const std::string& id)
      : Connector(id),
        metadata_{std::make_unique<TestConnectorMetadata>(this)} {}

  ConnectorMetadata* metadata() const override {
    return metadata_.get();
  }

  bool supportsSplitPreload() override {
    return true;
  }

  bool canAddDynamicFilter() const override {
    return true;
  }

  void setPushdownFilters(bool enabled) {
    pushdownFilters_ = enabled;
  }

  bool pushdownFilters() const {
    return pushdownFilters_;
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

  void
  addTable(const std::string& name, const RowTypePtr& schema, uint64_t rows);

  void addTable(const std::string& name, const RowTypePtr& schema);

  void addTable(const std::string& name);

 private:
  const std::unique_ptr<TestConnectorMetadata> metadata_;
  bool pushdownFilters_{false};
};

class TestDataSink : public DataSink {
 public:
  void appendData(RowVectorPtr) override {}
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
};

} // namespace facebook::velox::connector
