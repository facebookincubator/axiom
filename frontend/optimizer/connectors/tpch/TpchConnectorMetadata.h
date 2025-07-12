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

#include "optimizer/connectors/ConnectorMetadata.h" //@manual
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/core/QueryCtx.h"
#include "velox/tpch/gen/TpchGen.h"

namespace facebook::velox::connector::tpch {

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
  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits_;
  int32_t currentSplit_{0};
};

class TpchSplitManager : public ConnectorSplitManager {
 public:
  TpchSplitManager(TpchConnectorMetadata* /* metadata */) {}

  std::vector<std::shared_ptr<const PartitionHandle>> listPartitions(
      const ConnectorTableHandlePtr& tableHandle) override;

  std::shared_ptr<SplitSource> getSplitSource(
      const ConnectorTableHandlePtr& tableHandle,
      std::vector<std::shared_ptr<const PartitionHandle>> partitions,
      SplitOptions options = {}) override;
};

/// A TableLayout for TPCH tables. Implements sampling by generating TPCH data.
class TpchTableLayout : public TableLayout {
 public:
  TpchTableLayout(
      const std::string& name,
      const Table* table,
      connector::Connector* connector,
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
      const connector::ConnectorTableHandlePtr& handle,
      float pct,
      std::vector<core::TypedExprPtr> extraFilters,
      RowTypePtr outputType = nullptr,
      const std::vector<common::Subfield>& fields = {},
      HashStringAllocator* allocator = nullptr,
      std::vector<ColumnStatistics>* statistics = nullptr) const override;

 private:
  const velox::tpch::Table tpchTable_;
  const double scaleFactor_;
};

class TpchTable : public Table {
 public:
  TpchTable(
      const std::string& name,
      velox::tpch::Table tpchTable,
      double scaleFactor)
      : Table(name), tpchTable_(tpchTable), scaleFactor_(scaleFactor) {}

  std::unordered_map<std::string, std::unique_ptr<Column>>& columns() {
    return columns_;
  }

  const std::vector<const TableLayout*>& layouts() const override {
    return exportedLayouts_;
  }

  const std::unordered_map<std::string, const Column*>& columnMap()
      const override;

  void setType(const RowTypePtr& type) {
    type_ = type;
  }

  void makeDefaultLayout(TpchConnectorMetadata& metadata);

  uint64_t numRows() const override {
    return numRows_;
  }

  velox::tpch::Table getTpchTable() const {
    return tpchTable_;
  }

  double getScaleFactor() const {
    return scaleFactor_;
  }

 private:
  mutable std::mutex mutex_;

  std::unordered_map<std::string, std::unique_ptr<Column>> columns_;

  mutable std::unordered_map<std::string, const Column*> exportedColumns_;

  std::vector<std::unique_ptr<TableLayout>> layouts_;

  std::vector<const TableLayout*> exportedLayouts_;

  const velox::tpch::Table tpchTable_;

  const double scaleFactor_;

  int64_t numRows_{0};

  friend class TpchConnectorMetadata;
};

class TpchConnectorMetadata : public ConnectorMetadata {
 public:
  TpchConnectorMetadata(
      TpchConnector* tpchConnector,
      double scaleFactor = 0.01);

  void initialize() override;

  const Table* findTable(const std::string& name) override;

  ConnectorSplitManager* splitManager() override {
    ensureInitialized();
    return &splitManager_;
  }

  std::shared_ptr<core::QueryCtx> makeQueryCtx(
      const std::string& queryId) override;

  ColumnHandlePtr createColumnHandle(
      const TableLayout& layoutData,
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

  TpchConnector* tpchConnector() const {
    return tpchConnector_;
  }

  double getScaleFactor() const {
    return scaleFactor_;
  }

  /// Reinitializes the metadata with a new scale factor.
  void reinitialize(double newScaleFactor = 0.01);

  const std::unordered_map<std::string, std::unique_ptr<TpchTable>>& tables()
      const {
    ensureInitialized();
    return tables_;
  }

 private:
  void ensureInitialized() const;
  void makeQueryCtx();
  void initializeTables();
  void loadTable(velox::tpch::Table tpchTable);

  mutable std::mutex mutex_;
  mutable bool initialized_{false};
  TpchConnector* tpchConnector_;
  double scaleFactor_;
  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool()};
  std::shared_ptr<core::QueryCtx> queryCtx_;
  std::unordered_map<std::string, std::unique_ptr<TpchTable>> tables_;
  TpchSplitManager splitManager_;
};

} // namespace facebook::velox::connector::tpch
