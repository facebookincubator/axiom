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

#include "axiom/common/SchemaTableName.h"
#include "velox/connectors/Connector.h"

namespace facebook::axiom::connector::system {

/// Schema and table name constants for the system connector.
static constexpr std::string_view kRuntimeSchema = "runtime";
static constexpr std::string_view kMetadataSchema = "metadata";
inline const SchemaTableName kQueriesTable{
    std::string(kRuntimeSchema),
    "queries"};
inline const SchemaTableName kSessionPropertiesTable{
    std::string(kMetadataSchema),
    "session_properties"};
inline const SchemaTableName kFunctionsTable{
    std::string(kMetadataSchema),
    "functions"};

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

/// Snapshot of a single session property, used by the system connector to
/// populate the system.metadata.session_properties table.
struct SessionPropertyInfo {
  std::string component;
  std::string name;
  std::string type;
  std::string defaultValue;
  std::string currentValue;
  std::string description;
};

/// Provides session property metadata to the system connector.
class SessionPropertiesProvider {
 public:
  virtual ~SessionPropertiesProvider() = default;

  /// Returns all registered session properties with their current values.
  virtual std::vector<SessionPropertyInfo> getSessionProperties() const = 0;
};

/// Column handle for the system connector. Wraps a column name.
class SystemColumnHandle : public velox::connector::ColumnHandle {
 public:
  explicit SystemColumnHandle(const std::string& name) : name_(name) {}

  const std::string& name() const override {
    return name_;
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = SystemColumnHandle::getClassName();
    obj["columnName"] = name_;
    return obj;
  }

  static std::shared_ptr<SystemColumnHandle> create(const folly::dynamic& obj) {
    return std::make_shared<SystemColumnHandle>(obj["columnName"].asString());
  }

  static void registerSerDe() {
    velox::registerDeserializer<SystemColumnHandle>();
  }

  VELOX_DEFINE_CLASS_NAME(SystemColumnHandle)

 private:
  const std::string name_;
};

/// Table handle for the system connector. Holds the schema-qualified table
/// name and the set of column handles for the query.
class SystemTableHandle : public velox::connector::ConnectorTableHandle {
 public:
  SystemTableHandle(
      const std::string& connectorId,
      SchemaTableName schemaTableName,
      std::vector<velox::connector::ColumnHandlePtr> columnHandles);

  const std::string& name() const override {
    return qualifiedName_;
  }

  std::string toString() const override {
    return name();
  }

  const SchemaTableName& schemaTableName() const {
    return schemaTableName_;
  }

  const std::vector<velox::connector::ColumnHandlePtr>& columnHandles() const {
    return columnHandles_;
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = SystemTableHandle::getClassName();
    obj["connectorId"] = connectorId();
    obj["schemaName"] = schemaTableName_.schema;
    obj["tableName"] = schemaTableName_.table;
    folly::dynamic handles = folly::dynamic::array;
    for (const auto& handle : columnHandles_) {
      handles.push_back(handle->serialize());
    }
    obj["columnHandles"] = handles;
    return obj;
  }

  static velox::connector::ConnectorTableHandlePtr create(
      const folly::dynamic& obj,
      void* context);

  static void registerSerDe() {
    velox::registerDeserializerWithContext<SystemTableHandle>();
  }

  VELOX_DEFINE_CLASS_NAME(SystemTableHandle)

 private:
  const SchemaTableName schemaTableName_;
  const std::string qualifiedName_;
  const std::vector<velox::connector::ColumnHandlePtr> columnHandles_;
};

/// Split for the system connector. A single split covers all data since
/// everything is read in-memory from the IQueryManager.
struct SystemSplit : public velox::connector::ConnectorSplit {
  explicit SystemSplit(const std::string& connectorId)
      : ConnectorSplit(connectorId) {}

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = SystemSplit::getClassName();
    obj["connectorId"] = connectorId;
    return obj;
  }

  static std::shared_ptr<SystemSplit> create(const folly::dynamic& obj) {
    return std::make_shared<SystemSplit>(obj["connectorId"].asString());
  }

  static void registerSerDe() {
    velox::registerDeserializer<SystemSplit>();
  }

  VELOX_DEFINE_CLASS_NAME(SystemSplit)
};

/// Velox connector for the system catalog. Creates data source instances
/// for reading live query metadata and session properties.
class SystemConnector : public velox::connector::Connector {
 public:
  SystemConnector(
      const std::string& id,
      const QueryInfoProvider* queryInfoProvider,
      const SessionPropertiesProvider* sessionPropertiesProvider = nullptr);

  ~SystemConnector() override = default;

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

  /// Registers deserialization functions for all system connector types.
  static void registerSerDe() {
    SystemTableHandle::registerSerDe();
    SystemColumnHandle::registerSerDe();
    SystemSplit::registerSerDe();
  }

 private:
  const QueryInfoProvider* queryInfoProvider_;
  const SessionPropertiesProvider* sessionPropertiesProvider_;
};

} // namespace facebook::axiom::connector::system
