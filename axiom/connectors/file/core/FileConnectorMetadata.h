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
#include "axiom/connectors/file/core/FileTable.h"
#include "velox/common/memory/Memory.h"

namespace facebook::axiom::connector::file {

/// Emits one FileSplit per file path, in order.
class FileSplitSource : public SplitSource {
 public:
  FileSplitSource(
      const std::string& connectorId,
      std::vector<std::string> filePaths)
      : connectorId_(connectorId), filePaths_(std::move(filePaths)) {}

  folly::coro::Task<SplitBatch> co_getSplits(uint32_t maxSplitCount) override;

 private:
  const std::string connectorId_;
  const std::vector<std::string> filePaths_;
  size_t next_{0};
};

/// Resolves file paths as table names.
class FileConnectorMetadata : public ConnectorMetadata {
 public:
  explicit FileConnectorMetadata(velox::connector::Connector* connector)
      : connector_(connector),
        pool_(velox::memory::memoryManager()->addLeafPool("file_metadata")) {}

  /// Resolves a table from a schema name (format) and file path.
  TablePtr findTable(const SchemaTableName& tableName) override;

  /// Returns all registered file format names.
  std::vector<std::string> listSchemaNames(
      const ConnectorSessionPtr& session) override;

  /// Returns true if a handler exists for the given format name.
  bool schemaExists(
      const ConnectorSessionPtr& session,
      const std::string& schemaName) override;

  /// Returns an empty list; tables are file paths, not enumerable.
  std::vector<std::string> listTableNames(
      const ConnectorSessionPtr& /*session*/,
      const std::string& /*schemaName*/) override {
    return {};
  }

  ConnectorSplitManager* splitManager() override {
    return &splitManager_;
  }

 private:
  /// Manages split generation for file connector queries.
  class SplitManager : public ConnectorSplitManager {
   public:
    folly::coro::Task<std::vector<PartitionHandlePtr>> co_listPartitions(
        const ConnectorSessionPtr& session,
        const velox::connector::ConnectorTableHandlePtr& tableHandle) override;

    std::shared_ptr<SplitSource> getSplitSource(
        const ConnectorSessionPtr& session,
        const velox::connector::ConnectorTableHandlePtr& tableHandle,
        const std::vector<PartitionHandlePtr>& partitions,
        const std::shared_ptr<PartitionType>& partitionType,
        std::optional<double> samplePercentage,
        QueryRuntimeStats& runtimeStats) override;
  };

  velox::connector::Connector* connector_;
  std::shared_ptr<velox::memory::MemoryPool> pool_;
  SplitManager splitManager_;
};

} // namespace facebook::axiom::connector::file
