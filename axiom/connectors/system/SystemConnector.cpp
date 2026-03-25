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

#include "axiom/connectors/system/SystemConnectorMetadata.h"
#include "velox/common/base/Exceptions.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::axiom::connector::system {

namespace {

/// Indices into the full queries table schema, matching the column order
/// returned by queriesTableSchema().
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

} // namespace

// ===================== SystemTableHandle =====================

SystemTableHandle::SystemTableHandle(
    const std::string& connectorId,
    const TableLayout& layout,
    std::vector<velox::connector::ColumnHandlePtr> columnHandles)
    : ConnectorTableHandle(connectorId),
      name_(layout.table().name().toString()),
      layout_(layout),
      columnHandles_(std::move(columnHandles)) {}

// ===================== SystemDataSource =====================

SystemDataSource::SystemDataSource(
    const velox::RowTypePtr& outputType,
    const velox::connector::ColumnHandleMap& columnHandles,
    const QueryInfoProvider* queryInfoProvider,
    velox::memory::MemoryPool* pool)
    : outputType_(outputType),
      queryInfoProvider_(queryInfoProvider),
      pool_(pool) {
  auto fullSchema = queriesTableSchema();
  outputColumnMappings_.reserve(outputType_->size());
  for (const auto& name : outputType_->names()) {
    VELOX_CHECK(
        columnHandles.contains(name), "No handle for output column '{}'", name);
    auto handle = columnHandles.find(name)->second;
    auto idx = fullSchema->getChildIdxIfExists(handle->name());
    VELOX_CHECK(
        idx.has_value(),
        "Column '{}' not found in system.runtime.queries schema",
        handle->name());
    outputColumnMappings_.push_back(idx.value());
  }
}

void SystemDataSource::addSplit(
    std::shared_ptr<velox::connector::ConnectorSplit> split) {
  split_ = std::dynamic_pointer_cast<SystemSplit>(split);
  VELOX_CHECK(split_, "Expected SystemSplit");
  needData_ = true;
}

std::optional<velox::RowVectorPtr> SystemDataSource::next(
    uint64_t /*size*/,
    velox::ContinueFuture& /*future*/) {
  VELOX_CHECK(split_, "No split added to SystemDataSource");

  if (!needData_) {
    return nullptr;
  }
  needData_ = false;

  return buildQueryResults();
}

velox::RowVectorPtr SystemDataSource::buildQueryResults() {
  // If no query info provider is available (e.g. CLI mode), return empty
  // results.
  if (!queryInfoProvider_) {
    return velox::RowVector::createEmpty(outputType_, pool_);
  }

  auto queries = queryInfoProvider_->getQueryInfos();

  auto numRows = queries.size();
  completedRows_ += numRows;

  if (numRows == 0) {
    return velox::RowVector::createEmpty(outputType_, pool_);
  }

  auto fullSchema = queriesTableSchema();
  auto numFullColumns = static_cast<int32_t>(QueryColumn::kNumColumns);

  // Build all columns of the full schema.
  std::vector<velox::VectorPtr> allColumns(numFullColumns);

  for (size_t outIdx = 0; outIdx < outputColumnMappings_.size(); ++outIdx) {
    auto fullIdx = outputColumnMappings_[outIdx];

    // Skip if already built (possible but unlikely with column pruning).
    if (allColumns[fullIdx]) {
      continue;
    }

    auto col = static_cast<QueryColumn>(fullIdx);
    auto type = fullSchema->childAt(fullIdx);

    switch (col) {
      // VARCHAR columns
      case QueryColumn::kQueryId: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<velox::StringView>();
        for (size_t r = 0; r < numRows; ++r) {
          flat->set(r, velox::StringView(queries[r].queryId));
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }
      case QueryColumn::kState: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<velox::StringView>();
        for (size_t r = 0; r < numRows; ++r) {
          flat->set(r, velox::StringView(queries[r].state));
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }
      case QueryColumn::kQuery: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<velox::StringView>();
        for (size_t r = 0; r < numRows; ++r) {
          flat->set(r, velox::StringView(queries[r].query));
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }
      case QueryColumn::kCatalog: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<velox::StringView>();
        for (size_t r = 0; r < numRows; ++r) {
          flat->set(r, velox::StringView(queries[r].catalog));
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }
      case QueryColumn::kSchema: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<velox::StringView>();
        for (size_t r = 0; r < numRows; ++r) {
          flat->set(r, velox::StringView(queries[r].schema));
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }
      case QueryColumn::kUser: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<velox::StringView>();
        for (size_t r = 0; r < numRows; ++r) {
          flat->set(r, velox::StringView(queries[r].user));
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }
      case QueryColumn::kSource: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<velox::StringView>();
        for (size_t r = 0; r < numRows; ++r) {
          if (queries[r].source.has_value()) {
            flat->set(r, velox::StringView(queries[r].source.value()));
          } else {
            vec->setNull(r, true);
          }
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }
      case QueryColumn::kQueryType: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<velox::StringView>();
        for (size_t r = 0; r < numRows; ++r) {
          flat->set(r, velox::StringView(queries[r].queryType));
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }

      // BIGINT columns (time in ms)
      case QueryColumn::kPlanningTimeMs: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<int64_t>();
        for (size_t r = 0; r < numRows; ++r) {
          flat->set(r, queries[r].planningTimeMs);
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }
      case QueryColumn::kOptimizationTimeMs: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<int64_t>();
        for (size_t r = 0; r < numRows; ++r) {
          flat->set(r, queries[r].optimizationTimeMs);
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }
      case QueryColumn::kQueueTimeMs: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<int64_t>();
        for (size_t r = 0; r < numRows; ++r) {
          flat->set(r, queries[r].queueTimeMs);
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }
      case QueryColumn::kExecutionTimeMs: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<int64_t>();
        for (size_t r = 0; r < numRows; ++r) {
          flat->set(r, queries[r].executionTimeMs);
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }
      case QueryColumn::kElapsedTimeMs: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<int64_t>();
        for (size_t r = 0; r < numRows; ++r) {
          flat->set(r, queries[r].elapsedTimeMs);
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }
      case QueryColumn::kCpuTimeMs: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<int64_t>();
        for (size_t r = 0; r < numRows; ++r) {
          flat->set(r, queries[r].cpuTimeMs);
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }
      case QueryColumn::kWallTimeMs: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<int64_t>();
        for (size_t r = 0; r < numRows; ++r) {
          flat->set(r, queries[r].wallTimeMs);
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }

      // BIGINT columns (split counts)
      case QueryColumn::kTotalSplits: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<int64_t>();
        for (size_t r = 0; r < numRows; ++r) {
          flat->set(r, queries[r].totalSplits);
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }
      case QueryColumn::kQueuedSplits: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<int64_t>();
        for (size_t r = 0; r < numRows; ++r) {
          flat->set(r, queries[r].queuedSplits);
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }
      case QueryColumn::kRunningSplits: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<int64_t>();
        for (size_t r = 0; r < numRows; ++r) {
          flat->set(r, queries[r].runningSplits);
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }
      case QueryColumn::kFinishedSplits: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<int64_t>();
        for (size_t r = 0; r < numRows; ++r) {
          flat->set(r, queries[r].finishedSplits);
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }

      // BIGINT columns (data volumes)
      case QueryColumn::kOutputRows: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<int64_t>();
        for (size_t r = 0; r < numRows; ++r) {
          flat->set(r, queries[r].outputRows);
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }
      case QueryColumn::kOutputBytes: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<int64_t>();
        for (size_t r = 0; r < numRows; ++r) {
          flat->set(r, queries[r].outputBytes);
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }
      case QueryColumn::kProcessedRows: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<int64_t>();
        for (size_t r = 0; r < numRows; ++r) {
          flat->set(r, queries[r].processedRows);
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }
      case QueryColumn::kProcessedBytes: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<int64_t>();
        for (size_t r = 0; r < numRows; ++r) {
          flat->set(r, queries[r].processedBytes);
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }
      case QueryColumn::kWrittenRows: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<int64_t>();
        for (size_t r = 0; r < numRows; ++r) {
          flat->set(r, queries[r].writtenRows);
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }
      case QueryColumn::kWrittenBytes: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<int64_t>();
        for (size_t r = 0; r < numRows; ++r) {
          flat->set(r, queries[r].writtenBytes);
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }
      case QueryColumn::kPeakMemoryBytes: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<int64_t>();
        for (size_t r = 0; r < numRows; ++r) {
          flat->set(r, queries[r].peakMemoryBytes);
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }
      case QueryColumn::kSpilledBytes: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<int64_t>();
        for (size_t r = 0; r < numRows; ++r) {
          flat->set(r, queries[r].spilledBytes);
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }

      // TIMESTAMP columns
      case QueryColumn::kCreateTime: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<velox::Timestamp>();
        for (size_t r = 0; r < numRows; ++r) {
          auto tp = queries[r].createTime;
          if (!isEpoch(tp)) {
            flat->set(r, toTimestamp(tp));
          } else {
            vec->setNull(r, true);
          }
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }
      case QueryColumn::kStartTime: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<velox::Timestamp>();
        for (size_t r = 0; r < numRows; ++r) {
          auto tp = queries[r].startTime;
          if (!isEpoch(tp)) {
            flat->set(r, toTimestamp(tp));
          } else {
            vec->setNull(r, true);
          }
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }
      case QueryColumn::kEndTime: {
        auto vec = velox::BaseVector::create(type, numRows, pool_);
        auto* flat = vec->asFlatVector<velox::Timestamp>();
        for (size_t r = 0; r < numRows; ++r) {
          auto tp = queries[r].endTime;
          if (!isEpoch(tp)) {
            flat->set(r, toTimestamp(tp));
          } else {
            vec->setNull(r, true);
          }
        }
        allColumns[fullIdx] = std::move(vec);
        break;
      }

      default:
        VELOX_FAIL("Unknown system column index: {}", fullIdx);
    }
  }

  // Build output vector with only the requested columns.
  std::vector<velox::VectorPtr> children;
  children.reserve(outputType_->size());
  for (size_t i = 0; i < outputColumnMappings_.size(); ++i) {
    children.push_back(allColumns[outputColumnMappings_[i]]);
  }

  return std::make_shared<velox::RowVector>(
      pool_, outputType_, nullptr, numRows, std::move(children));
}

// ===================== SystemConnector =====================

SystemConnector::SystemConnector(
    const std::string& id,
    const QueryInfoProvider* queryInfoProvider)
    : Connector(id),
      queryInfoProvider_(queryInfoProvider),
      metadata_(std::make_shared<SystemConnectorMetadata>(this)) {}

std::unique_ptr<velox::connector::DataSource> SystemConnector::createDataSource(
    const velox::RowTypePtr& outputType,
    const velox::connector::ConnectorTableHandlePtr& /*tableHandle*/,
    const velox::connector::ColumnHandleMap& columnHandles,
    velox::connector::ConnectorQueryCtx* connectorQueryCtx) {
  return std::make_unique<SystemDataSource>(
      outputType,
      columnHandles,
      queryInfoProvider_,
      connectorQueryCtx->memoryPool());
}

} // namespace facebook::axiom::connector::system
