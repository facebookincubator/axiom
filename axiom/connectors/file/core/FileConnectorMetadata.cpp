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
  std::vector<std::string> filePaths;
  if (suffix.empty()) {
    // Data table: resolve the path to its data files once, here during
    // planning, and thread the list through the table handle to split
    // generation, so the directory is listed — and each entry's format probed —
    // only once per query. A directory takes its schema from the first file;
    // validate every other file against it here so a mismatched directory fails
    // upfront rather than mid-read after emitting rows from earlier files.
    filePaths = fileHandler.listFiles(filePath);
    schema = fileHandler.resolve(filePaths.front(), pool_.get());
    for (size_t i = 1; i < filePaths.size(); ++i) {
      const auto fileType = fileHandler.resolve(filePaths[i], pool_.get());
      VELOX_USER_CHECK(
          *fileType == *schema,
          "Directory file schema does not match the table schema {}, found {}: {}",
          schema->toString(),
          fileType->toString(),
          filePaths[i]);
    }
  } else {
    // Metadata table: validate the suffix BEFORE probing the path. Otherwise an
    // unsupported suffix on a missing or empty directory fails while listing
    // ("No data files found in directory: ...") instead of with the accurate
    // "Unsupported metadata table: ...".
    schema = fileHandler.metadataSchema(suffix);
    filePaths = fileHandler.listFiles(filePath);
  }

  return std::make_shared<FileTable>(
      tableName, schema, connector_, filePath, std::move(filePaths), suffix);
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
    std::optional<double> samplePercentage,
    QueryRuntimeStats& /*runtimeStats*/) {
  VELOX_USER_CHECK(
      !samplePercentage.has_value(),
      "SYSTEM sampling is not supported by this connector");
  auto* fileHandle = dynamic_cast<const FileTableHandle*>(tableHandle.get());
  VELOX_CHECK_NOT_NULL(fileHandle, "Expected FileTableHandle");
  // The file list was resolved once during planning (see findTable); reuse it
  // rather than listing the directory again here.
  return std::make_shared<FileSplitSource>(
      tableHandle->connectorId(), fileHandle->filePaths());
}

} // namespace facebook::axiom::connector::file
