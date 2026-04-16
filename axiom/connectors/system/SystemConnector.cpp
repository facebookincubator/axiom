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

#include "axiom/connectors/system/SystemConnector.h"

#include <algorithm>

#include <folly/json/json.h>

#include "axiom/connectors/system/SystemConnectorMetadata.h"
#include "velox/common/base/Exceptions.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::axiom::connector::system {

namespace {

// Indices into the full queries table schema, matching the column order
// returned by queriesTableSchema().
enum class QueryColumn : int32_t {
  kQueryId = 0,
  kState,
  kQuery,
  kCatalog,
  kSchema,
  kUser,
  kSource,
  kQueryType,
  kPlanningTimeMs,
  kOptimizationTimeMs,
  kQueueTimeMs,
  kExecutionTimeMs,
  kElapsedTimeMs,
  kCpuTimeMs,
  kWallTimeMs,
  kTotalSplits,
  kQueuedSplits,
  kRunningSplits,
  kFinishedSplits,
  kOutputRows,
  kOutputBytes,
  kProcessedRows,
  kProcessedBytes,
  kWrittenRows,
  kWrittenBytes,
  kPeakMemoryBytes,
  kSpilledBytes,
  kCreateTime,
  kStartTime,
  kEndTime,
  kNumColumns
};

// Indices into the session properties table schema, matching the column order
// returned by sessionPropertiesTableSchema().
enum class SessionPropertyColumn : int32_t {
  kComponent = 0,
  kName,
  kType,
  kDefaultValue,
  kCurrentValue,
  kDescription,
};

// Indices into the full functions table schema, matching the column order
// returned by functionsTableSchema().
enum class FunctionColumn : int32_t {
  kName = 0,
  kKind,
  kReturnType,
  kArgumentTypes,
  kIsVariadic,
  kOwner,
  kProperties,
};

// Converts a system_clock time_point to a Velox Timestamp.
// Returns a null Timestamp (0, 0) for the epoch (default-constructed
// time_point), which we treat as "not set".
velox::Timestamp toTimestamp(std::chrono::system_clock::time_point tp) {
  auto epoch = tp.time_since_epoch();
  auto secs = std::chrono::duration_cast<std::chrono::seconds>(epoch).count();
  auto nanos =
      std::chrono::duration_cast<std::chrono::nanoseconds>(epoch).count() %
      1'000'000'000;
  return velox::Timestamp(secs, nanos);
}

bool isEpoch(std::chrono::system_clock::time_point tp) {
  return tp == std::chrono::system_clock::time_point{};
}

// ===================== Data Sources (internal) =====================

// Base class for system connector data sources. Handles split management
// and the single-batch read pattern. Subclasses implement buildResults().
class SystemDataSourceBase : public velox::connector::DataSource {
 public:
  SystemDataSourceBase(
      const velox::RowTypePtr& outputType,
      const velox::RowTypePtr& fullSchema,
      const velox::connector::ColumnHandleMap& columnHandles,
      velox::memory::MemoryPool* pool)
      : outputType_(outputType), pool_(pool) {
    outputColumnMappings_.reserve(outputType_->size());
    for (const auto& name : outputType_->names()) {
      VELOX_CHECK(
          columnHandles.contains(name),
          "No handle for output column '{}'",
          name);
      auto handle = columnHandles.find(name)->second;
      auto idx = fullSchema->getChildIdxIfExists(handle->name());
      VELOX_CHECK(idx.has_value(), "Column not found: {}", handle->name());
      outputColumnMappings_.push_back(idx.value());
    }
  }

  void addSplit(
      std::shared_ptr<velox::connector::ConnectorSplit> split) override {
    split_ = std::dynamic_pointer_cast<SystemSplit>(split);
    VELOX_CHECK(split_, "Expected SystemSplit");
    needData_ = true;
  }

  std::optional<velox::RowVectorPtr> next(
      uint64_t /*size*/,
      velox::ContinueFuture& /*future*/) override {
    VELOX_CHECK(split_, "No split added");
    if (!needData_) {
      return nullptr;
    }
    needData_ = false;
    auto result = buildResults();
    if (result) {
      completedRows_ += result->size();
    }
    return result;
  }

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

 protected:
  virtual velox::RowVectorPtr buildResults() = 0;

  template <typename T>
  velox::FlatVectorPtr<T> createFlat(
      const velox::TypePtr& type,
      velox::vector_size_t size) {
    return velox::BaseVector::create<velox::FlatVector<T>>(type, size, pool_);
  }

  const velox::RowTypePtr outputType_;
  velox::memory::MemoryPool* pool_;
  std::vector<velox::column_index_t> outputColumnMappings_;
  uint64_t completedRows_{0};

 private:
  std::shared_ptr<SystemSplit> split_;
  bool needData_{true};
};

// Reads live query state from a QueryInfoProvider.
class SystemDataSource : public SystemDataSourceBase {
 public:
  SystemDataSource(
      const velox::RowTypePtr& outputType,
      const velox::connector::ColumnHandleMap& columnHandles,
      const QueryInfoProvider* queryInfoProvider,
      velox::memory::MemoryPool* pool)
      : SystemDataSourceBase(
            outputType,
            queriesTableSchema(),
            columnHandles,
            pool),
        queryInfoProvider_(queryInfoProvider) {}

 protected:
  velox::RowVectorPtr buildResults() override;

 private:
  const QueryInfoProvider* queryInfoProvider_;
};

// Reads session properties from a SessionPropertiesProvider.
class SessionPropertiesDataSource : public SystemDataSourceBase {
 public:
  SessionPropertiesDataSource(
      const velox::RowTypePtr& outputType,
      const velox::connector::ColumnHandleMap& columnHandles,
      const SessionPropertiesProvider* provider,
      velox::memory::MemoryPool* pool)
      : SystemDataSourceBase(
            outputType,
            sessionPropertiesTableSchema(),
            columnHandles,
            pool),
        provider_(provider) {}

 protected:
  velox::RowVectorPtr buildResults() override;

 private:
  const SessionPropertiesProvider* provider_;
};

// Reads function metadata directly from Velox's global function registries.
class FunctionsDataSource : public SystemDataSourceBase {
 public:
  FunctionsDataSource(
      const velox::RowTypePtr& outputType,
      const velox::connector::ColumnHandleMap& columnHandles,
      velox::memory::MemoryPool* pool)
      : SystemDataSourceBase(
            outputType,
            functionsTableSchema(),
            columnHandles,
            pool) {}

 protected:
  velox::RowVectorPtr buildResults() override;
};

} // namespace

// ===================== SystemTableHandle =====================

SystemTableHandle::SystemTableHandle(
    const std::string& connectorId,
    SchemaTableName schemaTableName,
    std::vector<velox::connector::ColumnHandlePtr> columnHandles)
    : ConnectorTableHandle(connectorId),
      schemaTableName_(std::move(schemaTableName)),
      qualifiedName_(
          fmt::format(
              "{}.{}",
              schemaTableName_.schema,
              schemaTableName_.table)),
      columnHandles_(std::move(columnHandles)) {}

velox::connector::ConnectorTableHandlePtr SystemTableHandle::create(
    const folly::dynamic& obj,
    void* /*context*/) {
  auto connectorId = obj["connectorId"].asString();
  SchemaTableName schemaTableName{
      obj["schemaName"].asString(), obj["tableName"].asString()};
  std::vector<velox::connector::ColumnHandlePtr> handles;
  for (const auto& handleObj : obj["columnHandles"]) {
    handles.push_back(
        velox::ISerializable::deserialize<SystemColumnHandle>(handleObj));
  }
  return std::make_shared<SystemTableHandle>(
      connectorId, std::move(schemaTableName), std::move(handles));
}

// ===================== SystemDataSource =====================

velox::RowVectorPtr SystemDataSource::buildResults() {
  // If no query info provider is available (e.g. CLI mode), return empty
  // results.
  if (!queryInfoProvider_) {
    return velox::RowVector::createEmpty(outputType_, pool_);
  }

  auto queries = queryInfoProvider_->getQueryInfos();
  auto numRows = queries.size();

  if (numRows == 0) {
    return velox::RowVector::createEmpty(outputType_, pool_);
  }

  const auto& fullSchema = queriesTableSchema();

  std::vector<velox::VectorPtr> children;
  children.reserve(outputType_->size());
  for (auto fullIdx : outputColumnMappings_) {
    const auto column = static_cast<QueryColumn>(fullIdx);
    const auto& type = fullSchema->childAt(fullIdx);

    switch (column) {
      // VARCHAR columns
      case QueryColumn::kQueryId: {
        auto flat = createFlat<velox::StringView>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          flat->set(i, velox::StringView(queries[i].queryId));
        }
        children.push_back(std::move(flat));
        break;
      }
      case QueryColumn::kState: {
        auto flat = createFlat<velox::StringView>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          flat->set(i, velox::StringView(queries[i].state));
        }
        children.push_back(std::move(flat));
        break;
      }
      case QueryColumn::kQuery: {
        auto flat = createFlat<velox::StringView>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          flat->set(i, velox::StringView(queries[i].query));
        }
        children.push_back(std::move(flat));
        break;
      }
      case QueryColumn::kCatalog: {
        auto flat = createFlat<velox::StringView>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          flat->set(i, velox::StringView(queries[i].catalog));
        }
        children.push_back(std::move(flat));
        break;
      }
      case QueryColumn::kSchema: {
        auto flat = createFlat<velox::StringView>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          flat->set(i, velox::StringView(queries[i].schema));
        }
        children.push_back(std::move(flat));
        break;
      }
      case QueryColumn::kUser: {
        auto flat = createFlat<velox::StringView>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          flat->set(i, velox::StringView(queries[i].user));
        }
        children.push_back(std::move(flat));
        break;
      }
      case QueryColumn::kSource: {
        auto flat = createFlat<velox::StringView>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          if (queries[i].source.has_value()) {
            flat->set(i, velox::StringView(queries[i].source.value()));
          } else {
            flat->setNull(i, true);
          }
        }
        children.push_back(std::move(flat));
        break;
      }
      case QueryColumn::kQueryType: {
        auto flat = createFlat<velox::StringView>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          flat->set(i, velox::StringView(queries[i].queryType));
        }
        children.push_back(std::move(flat));
        break;
      }

      // BIGINT columns (time in ms)
      case QueryColumn::kPlanningTimeMs: {
        auto flat = createFlat<int64_t>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          flat->set(i, queries[i].planningTimeMs);
        }
        children.push_back(std::move(flat));
        break;
      }
      case QueryColumn::kOptimizationTimeMs: {
        auto flat = createFlat<int64_t>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          flat->set(i, queries[i].optimizationTimeMs);
        }
        children.push_back(std::move(flat));
        break;
      }
      case QueryColumn::kQueueTimeMs: {
        auto flat = createFlat<int64_t>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          flat->set(i, queries[i].queueTimeMs);
        }
        children.push_back(std::move(flat));
        break;
      }
      case QueryColumn::kExecutionTimeMs: {
        auto flat = createFlat<int64_t>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          flat->set(i, queries[i].executionTimeMs);
        }
        children.push_back(std::move(flat));
        break;
      }
      case QueryColumn::kElapsedTimeMs: {
        auto flat = createFlat<int64_t>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          flat->set(i, queries[i].elapsedTimeMs);
        }
        children.push_back(std::move(flat));
        break;
      }
      case QueryColumn::kCpuTimeMs: {
        auto flat = createFlat<int64_t>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          flat->set(i, queries[i].cpuTimeMs);
        }
        children.push_back(std::move(flat));
        break;
      }
      case QueryColumn::kWallTimeMs: {
        auto flat = createFlat<int64_t>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          flat->set(i, queries[i].wallTimeMs);
        }
        children.push_back(std::move(flat));
        break;
      }

      // BIGINT columns (split counts)
      case QueryColumn::kTotalSplits: {
        auto flat = createFlat<int64_t>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          flat->set(i, queries[i].totalSplits);
        }
        children.push_back(std::move(flat));
        break;
      }
      case QueryColumn::kQueuedSplits: {
        auto flat = createFlat<int64_t>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          flat->set(i, queries[i].queuedSplits);
        }
        children.push_back(std::move(flat));
        break;
      }
      case QueryColumn::kRunningSplits: {
        auto flat = createFlat<int64_t>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          flat->set(i, queries[i].runningSplits);
        }
        children.push_back(std::move(flat));
        break;
      }
      case QueryColumn::kFinishedSplits: {
        auto flat = createFlat<int64_t>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          flat->set(i, queries[i].finishedSplits);
        }
        children.push_back(std::move(flat));
        break;
      }

      // BIGINT columns (data volumes)
      case QueryColumn::kOutputRows: {
        auto flat = createFlat<int64_t>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          flat->set(i, queries[i].outputRows);
        }
        children.push_back(std::move(flat));
        break;
      }
      case QueryColumn::kOutputBytes: {
        auto flat = createFlat<int64_t>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          flat->set(i, queries[i].outputBytes);
        }
        children.push_back(std::move(flat));
        break;
      }
      case QueryColumn::kProcessedRows: {
        auto flat = createFlat<int64_t>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          flat->set(i, queries[i].processedRows);
        }
        children.push_back(std::move(flat));
        break;
      }
      case QueryColumn::kProcessedBytes: {
        auto flat = createFlat<int64_t>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          flat->set(i, queries[i].processedBytes);
        }
        children.push_back(std::move(flat));
        break;
      }
      case QueryColumn::kWrittenRows: {
        auto flat = createFlat<int64_t>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          flat->set(i, queries[i].writtenRows);
        }
        children.push_back(std::move(flat));
        break;
      }
      case QueryColumn::kWrittenBytes: {
        auto flat = createFlat<int64_t>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          flat->set(i, queries[i].writtenBytes);
        }
        children.push_back(std::move(flat));
        break;
      }
      case QueryColumn::kPeakMemoryBytes: {
        auto flat = createFlat<int64_t>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          flat->set(i, queries[i].peakMemoryBytes);
        }
        children.push_back(std::move(flat));
        break;
      }
      case QueryColumn::kSpilledBytes: {
        auto flat = createFlat<int64_t>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          flat->set(i, queries[i].spilledBytes);
        }
        children.push_back(std::move(flat));
        break;
      }

      // TIMESTAMP columns
      case QueryColumn::kCreateTime: {
        auto flat = createFlat<velox::Timestamp>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          auto tp = queries[i].createTime;
          if (!isEpoch(tp)) {
            flat->set(i, toTimestamp(tp));
          } else {
            flat->setNull(i, true);
          }
        }
        children.push_back(std::move(flat));
        break;
      }
      case QueryColumn::kStartTime: {
        auto flat = createFlat<velox::Timestamp>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          auto tp = queries[i].startTime;
          if (!isEpoch(tp)) {
            flat->set(i, toTimestamp(tp));
          } else {
            flat->setNull(i, true);
          }
        }
        children.push_back(std::move(flat));
        break;
      }
      case QueryColumn::kEndTime: {
        auto flat = createFlat<velox::Timestamp>(type, numRows);
        for (size_t i = 0; i < numRows; ++i) {
          auto tp = queries[i].endTime;
          if (!isEpoch(tp)) {
            flat->set(i, toTimestamp(tp));
          } else {
            flat->setNull(i, true);
          }
        }
        children.push_back(std::move(flat));
        break;
      }

      default:
        VELOX_FAIL("Unknown system column index: {}", fullIdx);
    }
  }

  return std::make_shared<velox::RowVector>(
      pool_, outputType_, nullptr, numRows, std::move(children));
}

// ===================== SessionPropertiesDataSource =====================

velox::RowVectorPtr SessionPropertiesDataSource::buildResults() {
  if (!provider_) {
    return velox::RowVector::createEmpty(outputType_, pool_);
  }

  auto properties = provider_->getSessionProperties();
  auto numRows = properties.size();

  if (numRows == 0) {
    return velox::RowVector::createEmpty(outputType_, pool_);
  }

  const auto& fullSchema = sessionPropertiesTableSchema();

  auto getValue = [&](size_t row,
                      SessionPropertyColumn column) -> const std::string& {
    switch (column) {
      case SessionPropertyColumn::kComponent:
        return properties[row].component;
      case SessionPropertyColumn::kName:
        return properties[row].name;
      case SessionPropertyColumn::kType:
        return properties[row].type;
      case SessionPropertyColumn::kDefaultValue:
        return properties[row].defaultValue;
      case SessionPropertyColumn::kCurrentValue:
        return properties[row].currentValue;
      case SessionPropertyColumn::kDescription:
        return properties[row].description;
    }
    VELOX_UNREACHABLE();
  };

  std::vector<velox::VectorPtr> children;
  children.reserve(outputType_->size());
  for (auto fullIdx : outputColumnMappings_) {
    auto column = static_cast<SessionPropertyColumn>(fullIdx);
    auto vector =
        createFlat<velox::StringView>(fullSchema->childAt(fullIdx), numRows);
    for (size_t row = 0; row < numRows; ++row) {
      vector->set(row, velox::StringView(getValue(row, column)));
    }
    children.push_back(std::move(vector));
  }

  return std::make_shared<velox::RowVector>(
      pool_, outputType_, nullptr, numRows, std::move(children));
}

// ===================== FunctionsDataSource =====================

namespace {

// Snapshot of a single function signature for building result vectors.
struct FunctionEntry {
  std::string name;
  std::string kind;
  std::string returnType;
  std::vector<std::string> argumentTypes;
  bool isVariadic;
  std::string owner;
  std::string properties;
};

// Serializes a folly::dynamic object to JSON with sorted keys for
// deterministic output.
std::string toSortedJson(const folly::dynamic& obj) {
  folly::json::serialization_opts opts;
  opts.sort_keys = true;
  return folly::json::serialize(obj, opts);
}

// Builds a FunctionEntry from a name, kind, signature, and properties JSON.
FunctionEntry makeFunctionEntry(
    const std::string& name,
    const std::string& kind,
    const velox::exec::FunctionSignature& signature,
    std::string owner,
    std::string properties) {
  std::vector<std::string> argumentTypes;
  argumentTypes.reserve(signature.argumentTypes().size());
  for (const auto& argType : signature.argumentTypes()) {
    argumentTypes.push_back(argType.toString());
  }
  return {
      name,
      kind,
      signature.returnType().toString(),
      std::move(argumentTypes),
      signature.variableArity(),
      std::move(owner),
      std::move(properties),
  };
}

// Reads all function signatures from Velox's global registries. Cached
// after first call since function registrations are immutable after startup.
const std::vector<FunctionEntry>& getAllFunctions() {
  static const auto kFunctions = [] {
    std::vector<FunctionEntry> result;

    // Vector functions — one metadata per registration, multiple signatures.
    velox::exec::vectorFunctionFactories().withRLock(
        [&](const auto& factories) {
          for (const auto& [name, entry] : factories) {
            const auto& metadata = entry.metadata;
            auto properties = toSortedJson(
                folly::dynamic::object("deterministic", metadata.deterministic)(
                    "default_null_behavior", metadata.defaultNullBehavior));
            auto owner = std::string(metadata.owner);
            for (const auto& signature : entry.signatures) {
              result.push_back(makeFunctionEntry(
                  name, "scalar", *signature, owner, properties));
            }
          }
        });

    // Simple functions — per-signature metadata.
    const auto& simpleRegistry = velox::exec::simpleFunctions();
    for (const auto& name : simpleRegistry.getFunctionNames()) {
      for (const auto& [metadata, signature] :
           simpleRegistry.getFunctionSignaturesAndMetadata(name)) {
        auto properties = toSortedJson(
            folly::dynamic::object("deterministic", metadata.deterministic)(
                "default_null_behavior", metadata.defaultNullBehavior));
        result.push_back(makeFunctionEntry(
            name,
            "scalar",
            *signature,
            std::string(metadata.owner),
            properties));
      }
    }

    // Aggregate functions.
    auto allAggregates = velox::exec::getAggregateFunctionSignatures();
    for (const auto& [name, signatures] : allAggregates) {
      auto metadata = velox::exec::getAggregateFunctionMetadata(name);
      auto properties = toSortedJson(
          folly::dynamic::object("order_sensitive", metadata.orderSensitive)(
              "ignore_duplicates", metadata.ignoreDuplicates));
      for (const auto& signature : signatures) {
        result.push_back(
            makeFunctionEntry(name, "aggregate", *signature, "", properties));
      }
    }

    // Window functions (excluding those already counted as aggregates).
    for (const auto& [name, windowEntry] : velox::exec::windowFunctions()) {
      if (allAggregates.contains(name)) {
        continue;
      }
      for (const auto& signature : windowEntry.signatures) {
        result.push_back(
            makeFunctionEntry(name, "window", *signature, "", "{}"));
      }
    }

    return result;
  }();

  return kFunctions;
}

} // namespace

velox::RowVectorPtr FunctionsDataSource::buildResults() {
  const auto& functions = getAllFunctions();
  auto numRows = functions.size();

  if (numRows == 0) {
    return velox::RowVector::createEmpty(outputType_, pool_);
  }

  const auto& fullSchema = functionsTableSchema();

  std::vector<velox::VectorPtr> children;
  children.reserve(outputType_->size());
  for (auto fullIdx : outputColumnMappings_) {
    const auto column = static_cast<FunctionColumn>(fullIdx);
    const auto& type = fullSchema->childAt(fullIdx);

    switch (column) {
      case FunctionColumn::kName:
      case FunctionColumn::kKind:
      case FunctionColumn::kReturnType:
      case FunctionColumn::kOwner:
      case FunctionColumn::kProperties: {
        auto vector = createFlat<velox::StringView>(type, numRows);
        for (size_t row = 0; row < numRows; ++row) {
          const std::string* value;
          switch (column) {
            case FunctionColumn::kName:
              value = &functions[row].name;
              break;
            case FunctionColumn::kKind:
              value = &functions[row].kind;
              break;
            case FunctionColumn::kReturnType:
              value = &functions[row].returnType;
              break;
            case FunctionColumn::kOwner:
              value = &functions[row].owner;
              break;
            case FunctionColumn::kProperties:
              value = &functions[row].properties;
              break;
            default:
              VELOX_UNREACHABLE();
          }
          vector->set(row, velox::StringView(*value));
        }
        children.push_back(std::move(vector));
        break;
      }
      case FunctionColumn::kArgumentTypes: {
        auto offsets = velox::allocateOffsets(numRows, pool_);
        auto sizes = velox::allocateSizes(numRows, pool_);
        auto* rawOffsets = offsets->asMutable<velox::vector_size_t>();
        auto* rawSizes = sizes->asMutable<velox::vector_size_t>();

        velox::vector_size_t totalElements = 0;
        for (size_t row = 0; row < numRows; ++row) {
          totalElements += functions[row].argumentTypes.size();
        }

        auto elements =
            createFlat<velox::StringView>(velox::VARCHAR(), totalElements);

        velox::vector_size_t offset = 0;
        for (size_t row = 0; row < numRows; ++row) {
          rawOffsets[row] = offset;
          rawSizes[row] = functions[row].argumentTypes.size();
          for (const auto& argType : functions[row].argumentTypes) {
            elements->set(offset++, velox::StringView(argType));
          }
        }

        children.push_back(
            std::make_shared<velox::ArrayVector>(
                pool_,
                type,
                nullptr,
                numRows,
                std::move(offsets),
                std::move(sizes),
                std::move(elements)));
        break;
      }
      case FunctionColumn::kIsVariadic: {
        auto vector = createFlat<bool>(type, numRows);
        for (size_t row = 0; row < numRows; ++row) {
          vector->set(row, functions[row].isVariadic);
        }
        children.push_back(std::move(vector));
        break;
      }
    }
  }

  return std::make_shared<velox::RowVector>(
      pool_, outputType_, nullptr, numRows, std::move(children));
}

// ===================== SystemConnector =====================

SystemConnector::SystemConnector(
    const std::string& id,
    const QueryInfoProvider* queryInfoProvider,
    const SessionPropertiesProvider* sessionPropertiesProvider)
    : Connector(id),
      queryInfoProvider_(queryInfoProvider),
      sessionPropertiesProvider_(sessionPropertiesProvider) {}

std::unique_ptr<velox::connector::DataSource> SystemConnector::createDataSource(
    const velox::RowTypePtr& outputType,
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    const velox::connector::ColumnHandleMap& columnHandles,
    velox::connector::ConnectorQueryCtx* connectorQueryCtx) {
  auto* systemHandle =
      dynamic_cast<const SystemTableHandle*>(tableHandle.get());
  VELOX_CHECK_NOT_NULL(systemHandle, "Expected SystemTableHandle");

  const auto& schemaTableName = systemHandle->schemaTableName();

  if (schemaTableName == kSessionPropertiesTable) {
    return std::make_unique<SessionPropertiesDataSource>(
        outputType,
        columnHandles,
        sessionPropertiesProvider_,
        connectorQueryCtx->memoryPool());
  }

  if (schemaTableName == kFunctionsTable) {
    return std::make_unique<FunctionsDataSource>(
        outputType, columnHandles, connectorQueryCtx->memoryPool());
  }

  if (schemaTableName == kQueriesTable) {
    return std::make_unique<SystemDataSource>(
        outputType,
        columnHandles,
        queryInfoProvider_,
        connectorQueryCtx->memoryPool());
  }

  VELOX_FAIL("Unknown system table: {}", systemHandle->name());
}

} // namespace facebook::axiom::connector::system
