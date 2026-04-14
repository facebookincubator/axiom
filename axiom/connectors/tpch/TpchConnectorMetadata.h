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
#include "velox/tpch/gen/TpchGen.h"

namespace facebook::axiom::connector::tpch {

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

  folly::coro::Task<SplitBatch> co_getSplits(
      uint32_t maxSplitCount,
      int32_t /*bucket*/) override;

 private:
  const SplitOptions options_;
  const velox::tpch::Table table_;
  const double scaleFactor_;
  const std::string connectorId_;
  int64_t nextSplitIdx_{0};
  int64_t numSplits_{-1};
};

class TpchSplitManager : public ConnectorSplitManager {
 public:
  explicit TpchSplitManager(TpchConnectorMetadata* /* metadata */) {}

  folly::coro::Task<std::vector<PartitionHandlePtr>> co_listPartitions(
      const ConnectorSessionPtr& session,
      const velox::connector::ConnectorTableHandlePtr& tableHandle) override;

  std::shared_ptr<SplitSource> getSplitSource(
      const ConnectorSessionPtr& session,
      const velox::connector::ConnectorTableHandlePtr& tableHandle,
      const std::vector<PartitionHandlePtr>& partitions,
      SplitOptions options = {}) override;
};

/// A TableLayout for TPCH tables. Implements sampling by generating TPCH data.
class TpchTableLayout : public TableLayout {
 public:
  TpchTableLayout(
      const std::string& label,
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
            label,
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

  bool supportsSampling() const override {
    return false;
  }

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
  const velox::tpch::Table tpchTable_;
  const double scaleFactor_;
};

class TpchTable : public Table {
 public:
  TpchTable(
      SchemaTableName name,
      velox::RowTypePtr type,
      velox::tpch::Table tpchTable,
      double scaleFactor,
      int64_t numRows)
      : Table(std::move(name), makeColumns(type)),
        tpchTable_(tpchTable),
        scaleFactor_(scaleFactor),
        numRows_{numRows} {}

  const std::vector<const TableLayout*>& layouts() const override {
    return exportedLayouts_;
  }

  void makeDefaultLayout(TpchConnectorMetadata& metadata);

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

  TablePtr findTable(const SchemaTableName& tableName) override;

  std::vector<std::string> listSchemaNames(
      const ConnectorSessionPtr& session) override;

  bool schemaExists(
      const ConnectorSessionPtr& session,
      const std::string& schemaName) override;

  ViewPtr findView(const SchemaTableName& tableName) override;

  void createView(
      const SchemaTableName& viewName,
      velox::RowTypePtr type,
      std::string_view text);

  bool dropView(const SchemaTableName& viewName);

  ConnectorSplitManager* splitManager() override {
    return &splitManager_;
  }

  velox::connector::tpch::TpchConnector* tpchConnector() const {
    return tpchConnector_;
  }

 private:
  struct ViewDefinition {
    velox::RowTypePtr type;
    std::string text;
  };

  velox::connector::tpch::TpchConnector* tpchConnector_;
  std::shared_ptr<velox::memory::MemoryPool> rootPool_{
      velox::memory::memoryManager()->addRootPool()};
  TpchSplitManager splitManager_;
  folly::F14FastMap<SchemaTableName, ViewDefinition> views_;
};

} // namespace facebook::axiom::connector::tpch
