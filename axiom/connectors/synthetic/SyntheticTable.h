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
#include "axiom/connectors/synthetic/SyntheticConnector.h"

namespace facebook::axiom::connector::synthetic {

/// Table layout for a synthetic table. Constructed by SyntheticTable; the
/// optimizer calls createTableHandle() to produce the SyntheticTableHandle that
/// drives the scan. createTableHandle() runs the provider's acceptFilters() to
/// split the predicates: accepted ones are stored on the handle for pushdown,
/// the rest go back to the engine to re-apply.
class SyntheticTableLayout : public TableLayout {
 public:
  /// 'connector' executes the scan (SyntheticExecutionConnector).
  /// 'sourceCatalog' is stored on every handle this layout creates so the
  /// execution connector can rebuild the provider.
  SyntheticTableLayout(
      Table* table,
      velox::connector::Connector* connector,
      std::vector<const Column*> columns,
      std::shared_ptr<const SyntheticRowProvider> provider,
      std::string sourceCatalog)
      : TableLayout(
            "default",
            table,
            connector,
            std::move(columns),
            /*partitionColumns=*/{},
            /*orderColumns=*/{},
            /*sortOrder=*/{},
            /*lookupKeys=*/{},
            /*supportsScan=*/true),
        provider_(std::move(provider)),
        sourceCatalog_(std::move(sourceCatalog)) {}

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
  const std::shared_ptr<const SyntheticRowProvider> provider_;
  const std::string sourceCatalog_;
};

/// Table backed by a SyntheticRowProvider. Its schema is the provider's
/// rowType(), and its single layout (SyntheticTableLayout) handles predicate
/// pushdown to the provider. Created by findSyntheticTable() and returned as a
/// TablePtr from a catalog's findTable().
class SyntheticTable : public Table {
 public:
  /// 'sourceCatalog' is the catalog whose metadata rebuilds the provider at
  /// execution time.
  SyntheticTable(
      const SchemaTableName& tableName,
      std::shared_ptr<const SyntheticRowProvider> provider,
      velox::connector::Connector* connector,
      std::string sourceCatalog);

  const std::vector<const TableLayout*>& layouts() const override {
    return layouts_;
  }

  std::optional<uint64_t> numRows() const override {
    return std::nullopt; // Synthesized at query time, unknown.
  }

 private:
  // layouts_ holds a raw pointer into layout_, so layout_ is declared first and
  // must outlive layouts_.
  std::unique_ptr<SyntheticTableLayout> layout_;
  std::vector<const TableLayout*> layouts_;
};

/// SplitSource that emits exactly one SyntheticSplit, since all rows are
/// produced in a single in-memory batch.
class SyntheticSplitSource : public SplitSource {
 public:
  explicit SyntheticSplitSource(std::string connectorId)
      : connectorId_(std::move(connectorId)) {}

  folly::coro::Task<SplitBatch> co_getSplits(uint32_t maxSplitCount) override;

 private:
  const std::string connectorId_;
  bool done_{false};
};

/// SplitManager that returns a single partition and a single split.
class SyntheticSplitManager : public ConnectorSplitManager {
 public:
  folly::coro::Task<std::vector<PartitionHandlePtr>> co_listPartitions(
      const ConnectorSessionPtr& session,
      const velox::connector::ConnectorTableHandlePtr& tableHandle) override;

  std::shared_ptr<SplitSource> getSplitSource(
      const ConnectorSessionPtr& session,
      const velox::connector::ConnectorTableHandlePtr& tableHandle,
      const std::vector<PartitionHandlePtr>& partitions,
      const std::shared_ptr<PartitionType>& partitionType,
      QueryRuntimeStats& runtimeStats) override;
};

} // namespace facebook::axiom::connector::synthetic
