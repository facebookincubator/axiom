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

#include <chrono>
#include <optional>

#include "axiom/connectors/ConnectorMetadata.h"
#include "velox/connectors/Connector.h"

namespace facebook::axiom::connector::system {

/// Snapshot of a single query's state, used by the system connector to
/// populate the system.runtime.queries table.
struct QueryInfo {
  std::string queryId;
  std::string state;

  std::string query;
  std::string catalog;
  std::string schema;
  std::string user;
  std::optional<std::string> source;

  std::string queryType;
  int64_t planningTimeMs{0};
  int64_t optimizationTimeMs{0};
  int64_t queueTimeMs{0};
  int64_t executionTimeMs{0};
  int64_t elapsedTimeMs{0};
  int64_t cpuTimeMs{0};
  int64_t wallTimeMs{0};
  int64_t totalSplits{0};
  int64_t queuedSplits{0};
  int64_t runningSplits{0};
  int64_t finishedSplits{0};
  int64_t outputRows{0};
  int64_t outputBytes{0};
  int64_t processedRows{0};
  int64_t processedBytes{0};
  int64_t writtenRows{0};
  int64_t writtenBytes{0};
  int64_t peakMemoryBytes{0};
  int64_t spilledBytes{0};

  // Timestamps. Null (epoch) means "not set".
  std::chrono::system_clock::time_point createTime{};
  std::chrono::system_clock::time_point startTime{};
  std::chrono::system_clock::time_point endTime{};
};

/// Provides query metadata to the system connector without exposing
/// server-layer internals.
class QueryInfoProvider {
 public:
  virtual ~QueryInfoProvider() = default;

  /// Returns a snapshot of all known queries.
  virtual std::vector<QueryInfo> getQueryInfos() const = 0;
};

class SystemConnectorMetadata;

/// Column handle for the system connector. Wraps a column name.
class SystemColumnHandle : public velox::connector::ColumnHandle {
 public:
  explicit SystemColumnHandle(const std::string& name) : name_(name) {}

  const std::string& name() const override {
    return name_;
  }

 private:
  const std::string name_;
};

/// Table handle for the system connector. References the layout
/// and holds the set of column handles for the query.
class SystemTableHandle : public velox::connector::ConnectorTableHandle {
 public:
  SystemTableHandle(
      const std::string& connectorId,
      const TableLayout& layout,
      std::vector<velox::connector::ColumnHandlePtr> columnHandles);

  const std::string& name() const override {
    return name_;
  }

  std::string toString() const override {
    return name();
  }

  const TableLayout& layout() const {
    return layout_;
  }

  const std::vector<velox::connector::ColumnHandlePtr>& columnHandles() const {
    return columnHandles_;
  }

 private:
  const std::string name_;
  const TableLayout& layout_;
  const std::vector<velox::connector::ColumnHandlePtr> columnHandles_;
};

/// Split for the system connector. A single split covers all data since
/// everything is read in-memory from the IQueryManager.
struct SystemSplit : public velox::connector::ConnectorSplit {
  explicit SystemSplit(const std::string& connectorId)
      : ConnectorSplit(connectorId) {}
};

/// DataSource that reads live query state from a QueryInfoProvider.
/// On next(), it enumerates all queries and populates a RowVector
/// with one row per query for the requested columns.
class SystemDataSource : public velox::connector::DataSource {
 public:
  SystemDataSource(
      const velox::RowTypePtr& outputType,
      const velox::connector::ColumnHandleMap& columnHandles,
      const QueryInfoProvider* queryInfoProvider,
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
  velox::RowVectorPtr buildQueryResults();

  const velox::RowTypePtr outputType_;
  const QueryInfoProvider* queryInfoProvider_;
  velox::memory::MemoryPool* pool_;
  std::shared_ptr<SystemSplit> split_;
  // Maps output column index -> index in the full queries schema.
  std::vector<velox::column_index_t> outputColumnMappings_;
  uint64_t completedRows_{0};
  bool needData_{true};
};

/// Velox connector for the system catalog. Creates SystemDataSource
/// instances for reading live query metadata.
class SystemConnector : public velox::connector::Connector {
 public:
  SystemConnector(
      const std::string& id,
      const QueryInfoProvider* queryInfoProvider);

  ~SystemConnector() override = default;

  /// Returns the Axiom ConnectorMetadata for external registration.
  const std::shared_ptr<SystemConnectorMetadata>& metadata() const {
    return metadata_;
  }

  std::unique_ptr<velox::connector::DataSource> createDataSource(
      const velox::RowTypePtr& outputType,
      const velox::connector::ConnectorTableHandlePtr& tableHandle,
      const velox::connector::ColumnHandleMap& columnHandles,
      velox::connector::ConnectorQueryCtx* connectorQueryCtx) override;

  std::unique_ptr<velox::connector::DataSink> createDataSink(
      velox::RowTypePtr,
      velox::connector::ConnectorInsertTableHandlePtr,
      velox::connector::ConnectorQueryCtx*,
      velox::connector::CommitStrategy) override {
    VELOX_UNSUPPORTED("SystemConnector does not support writes");
  }

 private:
  const QueryInfoProvider* queryInfoProvider_;
  std::shared_ptr<SystemConnectorMetadata> metadata_;
};

} // namespace facebook::axiom::connector::system
