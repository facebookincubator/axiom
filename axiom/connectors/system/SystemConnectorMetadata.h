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

namespace facebook::axiom::connector::system {

/// Returns the schema for the system.runtime.queries table.
velox::RowTypePtr queriesTableSchema();

// ===================== Axiom Metadata Layer =====================

/// Table layout for the system.runtime.queries table.
class SystemTableLayout : public TableLayout {
 public:
  SystemTableLayout(
      Table* table,
      velox::connector::Connector* connector,
      std::vector<const Column*> columns)
      : TableLayout(
            "default",
            table,
            connector,
            std::move(columns),
            /*partitionColumns=*/{},
            /*orderColumns=*/{},
            /*sortOrder=*/{},
            /*lookupKeys=*/{},
            /*supportsScan=*/true) {}

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
};

/// Table representing system.runtime.queries.
class SystemTable : public Table {
 public:
  explicit SystemTable(velox::connector::Connector* connector);

  const std::vector<const TableLayout*>& layouts() const override {
    return layouts_;
  }

  uint64_t numRows() const override {
    return 0; // Dynamic, unknown.
  }

 private:
  std::vector<const TableLayout*> layouts_;
  std::unique_ptr<SystemTableLayout> layout_;
};

/// SplitSource that emits exactly one SystemSplit.
class SystemSplitSource : public SplitSource {
 public:
  explicit SystemSplitSource(const std::string& connectorId)
      : connectorId_(connectorId) {}

  folly::coro::Task<SplitBatch> co_getSplits(
      uint32_t maxSplitCount,
      int32_t /*bucket*/) override;

 private:
  const std::string connectorId_;
  bool done_{false};
};

/// SplitManager that returns a single partition and a single split.
class SystemSplitManager : public ConnectorSplitManager {
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

/// Axiom ConnectorMetadata for the system connector.
/// Provides the runtime.queries table and split management.
class SystemConnectorMetadata : public ConnectorMetadata {
 public:
  static constexpr std::string_view kDefaultSchema = "runtime";

  explicit SystemConnectorMetadata(velox::connector::Connector* connector)
      : connector_(connector),
        splitManager_(std::make_unique<SystemSplitManager>()) {}

  TablePtr findTable(const SchemaTableName& tableName) override;

  std::vector<std::string> listSchemaNames(
      const ConnectorSessionPtr& session) override {
    return {std::string(kDefaultSchema)};
  }

  bool schemaExists(
      const ConnectorSessionPtr& session,
      const std::string& schemaName) override {
    return schemaName == kDefaultSchema;
  }

  ConnectorSplitManager* splitManager() override {
    return splitManager_.get();
  }

 private:
  velox::connector::Connector* connector_;
  std::unique_ptr<SystemSplitManager> splitManager_;
  std::shared_ptr<SystemTable> queriesTable_;
};

} // namespace facebook::axiom::connector::system
