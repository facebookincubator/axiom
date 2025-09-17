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

#include "axiom/connectors/ConnectorMetadata.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/core/QueryCtx.h"
#include "velox/tpch/gen/TpchGen.h"

namespace facebook::axiom::connector::tpch {

static const SplitSource::SplitAndGroup kNoMoreSplits{nullptr, 0};

class TpchConnectorMetadata;

class TpchSplitSource : public SplitSource {
 public:
  TpchSplitSource(
      velox::tpch::Table table,
      double scaleFactor,
      const std::string& connectorId,
      SplitOptions options)
      : options_(options),
        table_(table),
        scaleFactor_(scaleFactor),
        connectorId_(connectorId) {}

  std::vector<SplitSource::SplitAndGroup> getSplits(
      uint64_t targetBytes) override;

 private:
  const SplitOptions options_;
  const velox::tpch::Table table_;
  const double scaleFactor_;
  const std::string connectorId_;
  std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> splits_;
  int32_t currentSplit_{0};
};

class TpchSplitManager : public ConnectorSplitManager {
 public:
  TpchSplitManager(TpchConnectorMetadata* /* metadata */) {}

  std::vector<PartitionHandlePtr> listPartitions(
      const velox::connector::ConnectorTableHandlePtr& tableHandle) override;

  std::shared_ptr<SplitSource> getSplitSource(
      const velox::connector::ConnectorTableHandlePtr& tableHandle,
      const std::vector<PartitionHandlePtr>& partitions,
      SplitOptions options = {}) override;
};

/// A TableLayout for TPCH tables. Implements sampling by generating TPCH data.
class TpchTableLayout : public TableLayout {
 public:
  TpchTableLayout(
      const std::string& name,
      const Table* table,
      velox::connector::Connector* connector,
      std::vector<const Column*> columns,
      std::vector<const Column*> partitioning,
      std::vector<const Column*> orderColumns,
      std::vector<SortOrder> sortOrder,
      std::vector<const Column*> lookupKeys,
      velox::tpch::Table tpchTable,
      double scaleFactor)
      : TableLayout(
            name,
            table,
            connector,
            std::move(columns),
            std::move(partitioning),
            std::move(orderColumns),
            std::move(sortOrder),
            std::move(lookupKeys),
            true),
        tpchTable_(tpchTable),
        scaleFactor_(scaleFactor) {}

  velox::tpch::Table getTpchTable() const {
    return tpchTable_;
  }

  double getScaleFactor() const {
    return scaleFactor_;
  }

  std::pair<int64_t, int64_t> sample(
      const velox::connector::ConnectorTableHandlePtr& handle,
      float pct,
      const std::vector<velox::core::TypedExprPtr>& extraFilters,
      velox::RowTypePtr outputType = nullptr,
      const std::vector<velox::common::Subfield>& fields = {},
      velox::HashStringAllocator* allocator = nullptr,
      std::vector<ColumnStatistics>* statistics = nullptr) const override;

 private:
  const velox::tpch::Table tpchTable_;
  const double scaleFactor_;
};

class TpchTable : public Table {
 public:
  TpchTable(
      std::string name,
      velox::RowTypePtr type,
      velox::tpch::Table tpchTable,
      double scaleFactor,
      int64_t numRows)
      : Table(std::move(name), std::move(type)),
        tpchTable_(tpchTable),
        scaleFactor_(scaleFactor),
        numRows_{numRows} {}

  folly::F14FastMap<std::string, std::unique_ptr<Column>>& columns() {
    return columns_;
  }

  const std::vector<const TableLayout*>& layouts() const override {
    return exportedLayouts_;
  }

  const folly::F14FastMap<std::string, const Column*>& columnMap()
      const override;

  void makeDefaultLayout(TpchConnectorMetadata& metadata, double scaleFactor);

  uint64_t numRows() const override {
    return numRows_;
  }

  velox::tpch::Table tpchTable() const {
    return tpchTable_;
  }

  double scaleFactor() const {
    return scaleFactor_;
  }

 private:
  mutable std::mutex mutex_;

  folly::F14FastMap<std::string, std::unique_ptr<Column>> columns_;

  mutable folly::F14FastMap<std::string, const Column*> exportedColumns_;

  std::vector<std::unique_ptr<TableLayout>> layouts_;

  std::vector<const TableLayout*> exportedLayouts_;

  const velox::tpch::Table tpchTable_;

  const double scaleFactor_;

  const int64_t numRows_;
};

class TpchConnectorMetadata : public ConnectorMetadata {
 public:
  explicit TpchConnectorMetadata(
      velox::connector::tpch::TpchConnector* tpchConnector);

  void initialize() override {}

  TablePtr findTable(std::string_view name) override;

  ConnectorSplitManager* splitManager() override {
    return &splitManager_;
  }

  velox::connector::ColumnHandlePtr createColumnHandle(
      const TableLayout& layoutData,
      const std::string& columnName,
      std::vector<velox::common::Subfield> subfields = {},
      std::optional<velox::TypePtr> castToType = std::nullopt,
      SubfieldMapping subfieldMapping = {}) override;

  velox::connector::ConnectorTableHandlePtr createTableHandle(
      const TableLayout& layout,
      std::vector<velox::connector::ColumnHandlePtr> columnHandles,
      velox::core::ExpressionEvaluator& evaluator,
      std::vector<velox::core::TypedExprPtr> filters,
      std::vector<velox::core::TypedExprPtr>& rejectedFilters,
      velox::RowTypePtr dataColumns = nullptr,
      std::optional<LookupKeys> = std::nullopt) override;

  void createTable(
      const std::string& tableName,
      const velox::RowTypePtr& rowType,
      const folly::F14FastMap<std::string, std::string>& options,
      const ConnectorSessionPtr& session,
      bool errorIfExists = true,
      TableKind tableKind = TableKind::kTable) override {
    VELOX_UNSUPPORTED();
  }

  velox::connector::ConnectorInsertTableHandlePtr createInsertTableHandle(
      const TableLayout& layout,
      const velox::RowTypePtr& rowType,
      const folly::F14FastMap<std::string, std::string>& options,
      WriteKind kind,
      const ConnectorSessionPtr& session) override {
    VELOX_UNSUPPORTED();
  }

  WritePartitionInfo writePartitionInfo(
      const velox::connector::ConnectorInsertTableHandlePtr& handle) override {
    VELOX_UNSUPPORTED();
  }

  void finishWrite(
      const TableLayout& layout,
      const velox::connector::ConnectorInsertTableHandlePtr& handle,
      const std::vector<velox::RowVectorPtr>& writerResult,
      WriteKind kind,
      const ConnectorSessionPtr& session) override {
    VELOX_UNSUPPORTED();
  }

  std::vector<velox::connector::ColumnHandlePtr> rowIdHandles(
      const TableLayout& layout,
      WriteKind kind) override {
    VELOX_UNSUPPORTED();
  }

  velox::connector::tpch::TpchConnector* tpchConnector() const {
    return tpchConnector_;
  }

 private:
  velox::connector::tpch::TpchConnector* tpchConnector_;
  std::shared_ptr<velox::memory::MemoryPool> rootPool_{
      velox::memory::memoryManager()->addRootPool()};
  TpchSplitManager splitManager_;
};

} // namespace facebook::axiom::connector::tpch
