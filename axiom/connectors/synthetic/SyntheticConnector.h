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

#include "axiom/common/SchemaTableName.h"
#include "velox/connectors/Connector.h"
#include "velox/core/ITypedExpr.h"

namespace facebook::axiom::connector::synthetic {

/// Produces the rows of a synthetic relation: a relation synthesized from
/// connector metadata at query time instead of read from storage, such as
/// information_schema.columns or a table's $partitions.
///
/// The provider is the only piece written per relation. The handle, split,
/// data source, and table around it are generic and shared across all synthetic
/// relations.
///
/// A concrete provider implements rowType() and buildRows(), and overrides
/// acceptFilters() to opt into predicate pushdown. A catalog returns it from
/// ConnectorMetadata::findSyntheticProvider().
///
/// A provider must be deterministic and hold no per-scan state. acceptFilters()
/// and buildRows() may run on different provider instances for the same
/// relation, because a distributed scan rebuilds the provider from the
/// serialized handle.
class SyntheticRowProvider {
 public:
  virtual ~SyntheticRowProvider() = default;

  /// Returns the full output schema of the synthetic table. buildRows() must
  /// return a RowVector of exactly this type.
  virtual const velox::RowTypePtr& rowType() const = 0;

  /// Splits 'filters' into the ones the provider will apply itself and the ones
  /// it cannot. Returns the former; appends the latter to 'rejected' for the
  /// engine to re-apply above the scan. Accepting a filter is a promise to
  /// apply it in buildRows(). The default accepts nothing.
  virtual std::vector<velox::core::TypedExprPtr> acceptFilters(
      std::vector<velox::core::TypedExprPtr> filters,
      std::vector<velox::core::TypedExprPtr>& rejected) const;

  /// Materializes all rows matching 'acceptedFilters' as a RowVector of
  /// rowType(). 'acceptedFilters' are exactly the filters returned by a prior
  /// acceptFilters() call for this scan.
  virtual velox::RowVectorPtr buildRows(
      const std::vector<velox::core::TypedExprPtr>& acceptedFilters,
      velox::memory::MemoryPool* pool) const = 0;
};

/// Column handle for a synthetic table.
class SyntheticColumnHandle : public velox::connector::ColumnHandle {
 public:
  explicit SyntheticColumnHandle(std::string name) : name_(std::move(name)) {}

  const std::string& name() const override {
    return name_;
  }

  folly::dynamic serialize() const override;

  static std::shared_ptr<SyntheticColumnHandle> create(
      const folly::dynamic& obj);

  static void registerSerDe() {
    velox::registerDeserializer<SyntheticColumnHandle>();
  }

  VELOX_DEFINE_CLASS_NAME(SyntheticColumnHandle)

 private:
  const std::string name_;
};

/// Table handle for a synthetic table. Carries the schema-qualified name, the
/// selected column handles, and the filters the provider accepted for pushdown
/// (so the DataSource can hand them back to the provider).
class SyntheticTableHandle : public velox::connector::ConnectorTableHandle {
 public:
  /// 'connectorId' is the shared execution connector that runs the scan (see
  /// SyntheticExecutionConnector); 'sourceCatalog' is the catalog whose
  /// metadata produces the rows.
  SyntheticTableHandle(
      std::string_view connectorId,
      SchemaTableName schemaTableName,
      std::vector<velox::connector::ColumnHandlePtr> columnHandles,
      std::vector<velox::core::TypedExprPtr> acceptedFilters,
      std::string sourceCatalog);

  const std::string& name() const override {
    return qualifiedName_;
  }

  std::string toString() const override {
    return name();
  }

  const SchemaTableName& schemaTableName() const {
    return schemaTableName_;
  }

  /// Catalog whose metadata rebuilds the row provider at execution time.
  const std::string& sourceCatalog() const {
    return sourceCatalog_;
  }

  const std::vector<velox::connector::ColumnHandlePtr>& columnHandles() const {
    return columnHandles_;
  }

  const std::vector<velox::core::TypedExprPtr>& acceptedFilters() const {
    return acceptedFilters_;
  }

  folly::dynamic serialize() const override;

  static velox::connector::ConnectorTableHandlePtr create(
      const folly::dynamic& obj,
      void* context);

  static void registerSerDe() {
    velox::registerDeserializerWithContext<SyntheticTableHandle>();
  }

  VELOX_DEFINE_CLASS_NAME(SyntheticTableHandle)

 private:
  const SchemaTableName schemaTableName_;
  const std::string qualifiedName_;
  const std::vector<velox::connector::ColumnHandlePtr> columnHandles_;
  const std::vector<velox::core::TypedExprPtr> acceptedFilters_;
  const std::string sourceCatalog_;
};

/// Split for a synthetic table. A single split covers all rows, which are
/// produced in-memory from the provider.
struct SyntheticSplit : public velox::connector::ConnectorSplit {
  explicit SyntheticSplit(const std::string& connectorId)
      : ConnectorSplit(connectorId) {}

  folly::dynamic serialize() const override;

  static std::shared_ptr<SyntheticSplit> create(const folly::dynamic& obj);

  static void registerSerDe() {
    velox::registerDeserializer<SyntheticSplit>();
  }

  VELOX_DEFINE_CLASS_NAME(SyntheticSplit)
};

/// DataSource for a synthetic table. Builds its rows from a
/// SyntheticRowProvider, which applies the accepted filters, then projects that
/// full-schema output down to the scan's output columns.
///
/// A split must be added before the first next(). The rows are materialized
/// lazily on that first next(), then handed out in size-bounded slices:
///
///   SyntheticDataSource source(outputType, columnHandles, provider, {}, pool);
///   source.addSplit(split);
///   auto batch = source.next(size, future); // repeat until it yields nullptr
class SyntheticDataSource : public velox::connector::DataSource {
 public:
  SyntheticDataSource(
      const velox::RowTypePtr& outputType,
      const velox::connector::ColumnHandleMap& columnHandles,
      std::shared_ptr<const SyntheticRowProvider> provider,
      std::vector<velox::core::TypedExprPtr> acceptedFilters,
      velox::memory::MemoryPool* pool);

  void addSplit(
      std::shared_ptr<velox::connector::ConnectorSplit> split) override;

  std::optional<velox::RowVectorPtr> next(
      uint64_t size,
      velox::ContinueFuture& future) override;

  void addDynamicFilter(
      velox::column_index_t,
      const std::shared_ptr<velox::common::Filter>&) override {}

  uint64_t getCompletedBytes() override {
    return 0;
  }

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  std::unordered_map<std::string, velox::RuntimeMetric> getRuntimeStats()
      override {
    return {};
  }

 private:
  // Materializes the provider's rows and projects them onto outputType_.
  velox::RowVectorPtr buildResults();

  const velox::RowTypePtr outputType_;
  velox::memory::MemoryPool* const pool_;
  const std::shared_ptr<const SyntheticRowProvider> provider_;
  const std::vector<velox::core::TypedExprPtr> acceptedFilters_;
  // Output column index -> provider rowType() index.
  std::vector<velox::column_index_t> outputColumnMappings_;
  std::shared_ptr<SyntheticSplit> split_;
  // Built lazily on the first next() after a split is added; nullptr until
  // then.
  velox::RowVectorPtr results_;
  velox::vector_size_t offset_{0};
  uint64_t completedRows_{0};
};

/// Registers deserializers for SyntheticTableHandle, SyntheticColumnHandle, and
/// SyntheticSplit. A consuming connector must call this once before any
/// serialized synthetic handle or split is deserialized.
void registerSyntheticSerDe();

} // namespace facebook::axiom::connector::synthetic
