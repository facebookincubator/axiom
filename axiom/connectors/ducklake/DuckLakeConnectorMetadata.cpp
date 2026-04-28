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

#include "axiom/connectors/ducklake/DuckLakeConnectorMetadata.h"

#include <algorithm>
#include <limits>

#include "axiom/connectors/ducklake/DuckLakeMetadataConfig.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/connectors/hive/iceberg/IcebergColumnHandle.h"
#include "velox/connectors/hive/iceberg/IcebergSplit.h"

namespace facebook::axiom::connector::ducklake {

namespace {

template <typename T>
T ceilDivide(T value, T divisor) {
  return (value + divisor - 1) / divisor;
}

velox::connector::hive::HiveColumnHandle::ColumnType columnType(
    const hive::HiveTableLayout& layout,
    const Column* column) {
  if (column->hidden()) {
    return velox::connector::hive::HiveColumnHandle::ColumnType::kSynthesized;
  }

  for (const auto* partitionColumn : layout.hivePartitionColumns()) {
    if (column->name() == partitionColumn->name()) {
      return velox::connector::hive::HiveColumnHandle::ColumnType::
          kPartitionKey;
    }
  }

  return velox::connector::hive::HiveColumnHandle::ColumnType::kRegular;
}

uint64_t splitCountForFile(
    const DuckLakeDataFile& file,
    const SplitOptions& options,
    size_t numFiles) {
  if (options.wholeFile || !file.fileSizeBytes.has_value() ||
      file.fileSizeBytes.value() == 0) {
    return 1;
  }

  auto splits = ceilDivide<uint64_t>(
      file.fileSizeBytes.value(), options.fileBytesPerSplit);
  if (options.targetSplitCount > 0 &&
      splits * numFiles < static_cast<uint64_t>(options.targetSplitCount)) {
    const auto perFile = ceilDivide<uint64_t>(
        static_cast<uint64_t>(options.targetSplitCount), numFiles);
    const auto bytesInSplit =
        ceilDivide<uint64_t>(file.fileSizeBytes.value(), perFile);
    splits = ceilDivide<uint64_t>(
        file.fileSizeBytes.value(),
        std::max<uint64_t>(bytesInSplit, 32ULL << 20U));
  }
  return std::max<uint64_t>(splits, 1);
}

std::optional<velox::FileProperties> fileProperties(
    const DuckLakeDataFile& file) {
  if (!file.fileSizeBytes.has_value()) {
    return std::nullopt;
  }

  VELOX_CHECK_LE(
      file.fileSizeBytes.value(),
      static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
  velox::FileProperties properties;
  properties.fileSize = static_cast<int64_t>(file.fileSizeBytes.value());
  return properties;
}

std::unordered_map<std::string, std::string> infoColumns(
    const DuckLakeDataFile& file) {
  std::unordered_map<std::string, std::string> columns{
      {hive::HiveTable::kPath, file.path},
  };
  if (file.fileSizeBytes.has_value()) {
    columns[hive::HiveTable::kFileSize] =
        std::to_string(file.fileSizeBytes.value());
  }
  return columns;
}

} // namespace

// Enumerates live DuckLake files as Velox Iceberg splits.
class DuckLakeSplitSource : public SplitSource {
 public:
  DuckLakeSplitSource(
      std::vector<DuckLakeDataFile> files,
      std::string connectorId,
      SplitOptions options)
      : files_{std::move(files)},
        connectorId_{std::move(connectorId)},
        options_{options} {}

  folly::coro::Task<SplitBatch> co_getSplits(
      uint32_t maxSplitCount,
      int32_t /*bucket*/) override {
    SplitBatch batch;
    const auto limit = static_cast<size_t>(maxSplitCount);
    while (batch.splits.size() < limit && fileIndex_ < files_.size()) {
      const auto& file = files_[fileIndex_];
      if (splitIndex_ == 0) {
        splitsInCurrentFile_ =
            splitCountForFile(file, options_, files_.size());
      }

      const auto splitSize = file.fileSizeBytes.has_value()
          ? ceilDivide<uint64_t>(
                file.fileSizeBytes.value(), splitsInCurrentFile_)
          : std::numeric_limits<uint64_t>::max();

      while (splitIndex_ < splitsInCurrentFile_ &&
             batch.splits.size() < limit) {
        const auto start = file.fileSizeBytes.has_value()
            ? std::min<uint64_t>(
                  splitIndex_ * splitSize, file.fileSizeBytes.value())
            : 0;
        const auto length = file.fileSizeBytes.has_value()
            ? std::min<uint64_t>(
                  splitSize, file.fileSizeBytes.value() - start)
            : std::numeric_limits<uint64_t>::max();

        batch.splits.push_back(
            std::make_shared<
                velox::connector::hive::iceberg::HiveIcebergSplit>(
                connectorId_,
                file.path,
                velox::dwio::common::FileFormat::PARQUET,
                start,
                length,
                std::unordered_map<std::string, std::optional<std::string>>{},
                std::nullopt,
                std::unordered_map<std::string, std::string>{},
                std::shared_ptr<std::string>{},
                true,
                std::vector<
                    velox::connector::hive::iceberg::IcebergDeleteFile>{},
                infoColumns(file),
                fileProperties(file),
                0));
        ++splitIndex_;
      }

      if (splitIndex_ >= splitsInCurrentFile_) {
        ++fileIndex_;
        splitIndex_ = 0;
        splitsInCurrentFile_ = 0;
      }
    }

    batch.noMoreSplits = (fileIndex_ >= files_.size());
    co_return batch;
  }

 private:
  std::vector<DuckLakeDataFile> files_;
  std::string connectorId_;
  SplitOptions options_;
  size_t fileIndex_{0};
  uint64_t splitIndex_{0};
  uint64_t splitsInCurrentFile_{0};
};

folly::coro::Task<std::vector<PartitionHandlePtr>>
DuckLakeSplitManager::co_listPartitions(
    const ConnectorSessionPtr& /*session*/,
    const velox::connector::ConnectorTableHandlePtr& /*tableHandle*/) {
  std::vector<PartitionHandlePtr> partitions{
      std::make_shared<hive::HivePartitionHandle>(
          folly::F14FastMap<std::string, std::optional<std::string>>{},
          std::nullopt),
  };
  co_return partitions;
}

std::shared_ptr<SplitSource> DuckLakeSplitManager::getSplitSource(
    const ConnectorSessionPtr& /*session*/,
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    const std::vector<PartitionHandlePtr>& /*partitions*/,
    SplitOptions options) {
  auto* hiveTableHandle =
      dynamic_cast<const velox::connector::hive::HiveTableHandle*>(
          tableHandle.get());
  VELOX_CHECK_NOT_NULL(hiveTableHandle);

  const auto schema = hiveTableHandle->dbName().empty()
      ? std::string{"main"}
      : hiveTableHandle->dbName();
  auto table = metadata_->findTable({schema, tableHandle->name()});
  VELOX_CHECK_NOT_NULL(
      table, "Could not find {} in its ConnectorMetadata", tableHandle->name());
  auto* layout = dynamic_cast<const DuckLakeTableLayout*>(table->layouts()[0]);
  VELOX_CHECK_NOT_NULL(layout);

  return std::make_shared<DuckLakeSplitSource>(
      layout->dataFiles(), layout->connectorId(), options);
}

DuckLakeTableLayout::DuckLakeTableLayout(
    const std::string& label,
    const Table* table,
    velox::connector::Connector* connector,
    std::vector<const Column*> columns,
    std::vector<DuckLakeColumnMetadata> duckLakeColumns,
    std::vector<DuckLakeDataFile> dataFiles)
    : hive::HiveTableLayout(
          label,
          table,
          connector,
          std::move(columns),
          std::nullopt,
          {},
          {},
          {},
          {},
          {},
          velox::dwio::common::FileFormat::PARQUET),
      dataFiles_{std::move(dataFiles)} {
  for (const auto& column : duckLakeColumns) {
    VELOX_USER_CHECK_LE(
        column.columnId,
        std::numeric_limits<int32_t>::max(),
        "DuckLake column id is too large for Parquet field id: {}",
        column.columnId);
    fieldIds_.emplace(
        column.name,
        velox::parquet::ParquetFieldId{
            static_cast<int32_t>(column.columnId),
            {},
        });
  }
}

velox::connector::ColumnHandlePtr DuckLakeTableLayout::createColumnHandle(
    const ConnectorSessionPtr& /*session*/,
    const std::string& columnName,
    std::vector<velox::common::Subfield> subfields,
    std::optional<velox::TypePtr> castToType,
    SubfieldMapping subfieldMapping) const {
  VELOX_CHECK(!castToType.has_value());
  VELOX_CHECK(subfieldMapping.empty());

  auto* column = findColumn(columnName);
  VELOX_CHECK_NOT_NULL(
      column, "Column not found: {} in table {}", columnName, table().name());

  const auto handleType = columnType(*this, column);
  if (handleType !=
      velox::connector::hive::HiveColumnHandle::ColumnType::kRegular) {
    return std::make_shared<velox::connector::hive::HiveColumnHandle>(
        columnName,
        handleType,
        column->type(),
        column->type(),
        std::move(subfields));
  }

  auto it = fieldIds_.find(columnName);
  VELOX_CHECK(
      it != fieldIds_.end(),
      "DuckLake column is missing field id: {}",
      columnName);
  return std::make_shared<
      velox::connector::hive::iceberg::IcebergColumnHandle>(
      columnName, handleType, column->type(), it->second, std::move(subfields));
}

DuckLakeTable::DuckLakeTable(DuckLakeTableMetadata metadata)
    : hive::HiveTable(
          std::move(metadata.name),
          std::move(metadata.rowType),
          false,
          true,
          {}),
      numRows_{metadata.numRows} {}

void DuckLakeTable::addLayout(std::unique_ptr<DuckLakeTableLayout> layout) {
  exportedLayouts_.push_back(layout.get());
  layouts_.push_back(std::move(layout));
}

DuckLakeConnectorMetadata::DuckLakeConnectorMetadata(
    velox::connector::hive::iceberg::IcebergConnector* icebergConnector)
    : DuckLakeConnectorMetadata(
          icebergConnector,
          DuckLakeCatalogClient::create(
              DuckLakeMetadataConfig(icebergConnector->connectorConfig())
                  .catalogSpec())) {}

DuckLakeConnectorMetadata::DuckLakeConnectorMetadata(
    velox::connector::hive::iceberg::IcebergConnector* icebergConnector,
    std::unique_ptr<DuckLakeCatalogClient> catalog)
    : splitManager_{this},
      icebergConnector_{icebergConnector},
      catalog_{std::move(catalog)} {
  VELOX_CHECK_NOT_NULL(icebergConnector_);
  VELOX_CHECK_NOT_NULL(catalog_);
}

TablePtr DuckLakeConnectorMetadata::findTable(
    const SchemaTableName& tableName) {
  auto table = catalog_->loadTable(tableName);
  if (!table.has_value()) {
    return nullptr;
  }
  return makeTable(std::move(table.value()));
}

std::vector<std::string> DuckLakeConnectorMetadata::listSchemaNames(
    const ConnectorSessionPtr& /*session*/) {
  return catalog_->listSchemaNames();
}

bool DuckLakeConnectorMetadata::schemaExists(
    const ConnectorSessionPtr& /*session*/,
    const std::string& schemaName) {
  auto schemas = catalog_->listSchemaNames();
  return std::find(schemas.begin(), schemas.end(), schemaName) != schemas.end();
}

TablePtr DuckLakeConnectorMetadata::makeTable(
    DuckLakeTableMetadata tableMetadata) const {
  auto duckLakeColumns = std::move(tableMetadata.columns);
  auto dataFiles = std::move(tableMetadata.dataFiles);
  auto table = std::make_shared<DuckLakeTable>(std::move(tableMetadata));
  table->addLayout(std::make_unique<DuckLakeTableLayout>(
      "ducklake",
      table.get(),
      icebergConnector_,
      table->allColumns(),
      std::move(duckLakeColumns),
      std::move(dataFiles)));
  return table;
}

} // namespace facebook::axiom::connector::ducklake
