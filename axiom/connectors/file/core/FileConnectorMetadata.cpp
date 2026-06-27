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

#include "axiom/connectors/file/core/FileConnectorMetadata.h"

#include "axiom/connectors/file/FileHandler.h"
#include "axiom/connectors/file/core/FileTable.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::connector::file {

folly::coro::Task<SplitBatch> FileSplitSource::co_getSplits(
    uint32_t maxSplitCount) {
  SplitBatch batch;
  while (next_ < filePaths_.size() && batch.splits.size() < maxSplitCount) {
    batch.splits.emplace_back(
        std::make_shared<FileSplit>(connectorId_, filePaths_[next_]));
    ++next_;
  }
  batch.noMoreSplits = next_ == filePaths_.size();
  co_return batch;
}

TablePtr FileConnectorMetadata::findTable(const SchemaTableName& tableName) {
  auto& fileHandler = handler(tableName.schema);
  auto [filePath, suffix] = FileTable::parseName(tableName.table);

  velox::RowTypePtr schema;
  if (suffix.empty()) {
    // A directory resolves to the files it contains; take the schema from the
    // first file and assume the rest share it.
    schema = fileHandler.resolve(
        fileHandler.listFiles(filePath).front(), pool_.get());
  } else {
    schema = fileHandler.metadataSchema(suffix);
  }

  return std::make_shared<FileTable>(
      tableName, schema, connector_, filePath, suffix);
}

std::vector<std::string> FileConnectorMetadata::listSchemaNames(
    const ConnectorSessionPtr& /*session*/) {
  return schemas();
}

bool FileConnectorMetadata::schemaExists(
    const ConnectorSessionPtr& /*session*/,
    const std::string& schemaName) {
  return hasHandler(schemaName);
}

folly::coro::Task<std::vector<PartitionHandlePtr>>
FileConnectorMetadata::SplitManager::co_listPartitions(
    const ConnectorSessionPtr& /*session*/,
    const velox::connector::ConnectorTableHandlePtr& /*tableHandle*/) {
  co_return std::vector<PartitionHandlePtr>{
      std::make_shared<PartitionHandle>()};
}

std::shared_ptr<SplitSource>
FileConnectorMetadata::SplitManager::getSplitSource(
    const ConnectorSessionPtr& /*session*/,
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    const std::vector<PartitionHandlePtr>& /*partitions*/,
    const std::shared_ptr<PartitionType>& /*partitionType*/,
    QueryRuntimeStats& /*runtimeStats*/) {
  auto* fileHandle = dynamic_cast<const FileTableHandle*>(tableHandle.get());
  VELOX_CHECK_NOT_NULL(fileHandle, "Expected FileTableHandle");
  auto& fileHandler = handler(fileHandle->schemaTableName().schema);
  return std::make_shared<FileSplitSource>(
      tableHandle->connectorId(),
      fileHandler.listFiles(fileHandle->filePath()));
}

} // namespace facebook::axiom::connector::file
