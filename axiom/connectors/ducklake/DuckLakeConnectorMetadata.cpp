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

#include <folly/String.h>
#include <algorithm>
#include <limits>
#include <unordered_set>

#include "axiom/connectors/ducklake/DuckLakeMetadataConfig.h"
#include "axiom/connectors/hive/PartitionValue.h"
#include "velox/common/base/Exceptions.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/connectors/hive/iceberg/IcebergColumnHandle.h"
#include "velox/connectors/hive/iceberg/IcebergSplit.h"

namespace facebook::axiom::connector::ducklake {

namespace {

template <typename T>
T ceilDivide(T value, T divisor) {
  return (value + divisor - 1) / divisor;
}

constexpr uint64_t kFileBytesPerSplit{128ULL << 20U};

velox::connector::hive::HiveColumnHandle::ColumnType columnType(
    const hive::HiveTableLayout& layout,
    const Column& column) {
  if (column.hidden()) {
    return velox::connector::hive::HiveColumnHandle::ColumnType::kSynthesized;
  }

  for (const auto* partitionColumn : layout.hivePartitionColumns()) {
    if (column.name() == partitionColumn->name()) {
      return velox::connector::hive::HiveColumnHandle::ColumnType::
          kPartitionKey;
    }
  }

  return velox::connector::hive::HiveColumnHandle::ColumnType::kRegular;
}

uint64_t splitCountForFile(const DuckLakeDataFile& file) {
  if (!file.fileSizeBytes.has_value() || file.fileSizeBytes.value() == 0) {
    return 1;
  }

  auto splits =
      ceilDivide<uint64_t>(file.fileSizeBytes.value(), kFileBytesPerSplit);
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

bool isIdentityPartitionColumn(
    const DuckLakePartitionColumnMetadata& partitionColumn) {
  return partitionColumn.transform == "identity";
}

std::vector<std::string> identityPartitionColumnNames(
    const std::vector<DuckLakePartitionColumnMetadata>& partitionColumns) {
  std::vector<std::string> columnNames;
  std::unordered_set<std::string> seen;
  for (const auto& partitionColumn : partitionColumns) {
    if (!isIdentityPartitionColumn(partitionColumn) ||
        seen.count(partitionColumn.columnName) != 0) {
      continue;
    }
    seen.insert(partitionColumn.columnName);
    columnNames.push_back(partitionColumn.columnName);
  }
  return columnNames;
}

std::vector<const Column*> hivePartitionColumns(
    const DuckLakeTable& table,
    const std::vector<DuckLakePartitionColumnMetadata>& partitionColumns) {
  std::vector<const Column*> columns;
  std::unordered_set<std::string> seen;
  for (const auto& partitionColumn : partitionColumns) {
    if (!isIdentityPartitionColumn(partitionColumn) ||
        seen.count(partitionColumn.columnName) != 0) {
      continue;
    }
    auto* column = table.findColumn(partitionColumn.columnName);
    VELOX_CHECK_NOT_NULL(
        column,
        "DuckLake partition column is missing from the table schema: {}",
        partitionColumn.columnName);
    seen.insert(partitionColumn.columnName);
    columns.push_back(column);
  }
  return columns;
}

velox::connector::Connector* connectorPointer(
    const std::shared_ptr<velox::connector::hive::iceberg::IcebergConnector>&
        icebergConnector) {
  VELOX_CHECK_NOT_NULL(icebergConnector);
  return icebergConnector.get();
}

struct DuckLakePartitionKey {
  std::optional<int64_t> partitionId;
  std::map<int64_t, std::optional<std::string>> partitionValues;
  std::unordered_map<std::string, std::optional<std::string>> partitionKeys;
};

class DuckLakePartitionHandle : public PartitionHandle {
 public:
  explicit DuckLakePartitionHandle(DuckLakePartitionKey partition)
      : partition_{std::move(partition)} {}

  const DuckLakePartitionKey& partition() const {
    return partition_;
  }

 private:
  DuckLakePartitionKey partition_;
};

std::string partitionFingerprint(const DuckLakePartitionKey& partition) {
  std::vector<std::string> parts;
  parts.reserve(partition.partitionValues.size() + 1);
  parts.push_back(
      partition.partitionId.has_value()
          ? fmt::format("id={}", partition.partitionId.value())
          : std::string{"id=null"});
  for (const auto& [index, value] : partition.partitionValues) {
    parts.push_back(
        value.has_value()
            ? fmt::format("{}:{}:{}", index, value->size(), value.value())
            : fmt::format("{}:null", index));
  }
  return folly::join("|", parts);
}

bool partitionMatchesFilters(
    const DuckLakePartitionKey& partition,
    const DuckLakeTableMetadata& tableMetadata,
    const velox::connector::hive::HiveTableHandle& tableHandle) {
  for (const auto& [columnName, value] : partition.partitionKeys) {
    auto filterIt =
        tableHandle.subfieldFilters().find(velox::common::Subfield(columnName));
    if (filterIt == tableHandle.subfieldFilters().end()) {
      continue;
    }

    auto columnType = tableMetadata.rowType->findChild(columnName);
    VELOX_CHECK_NOT_NULL(
        columnType,
        "DuckLake partition column is missing from the table row type: {}",
        columnName);
    if (!hive::testPartitionValue(*filterIt->second, value, *columnType)) {
      return false;
    }
  }
  return true;
}

std::vector<PartitionHandlePtr> makePartitionHandles(
    const DuckLakeTableMetadata& tableMetadata,
    const velox::connector::hive::HiveTableHandle& tableHandle) {
  if (tableMetadata.dataFiles.empty()) {
    if (tableMetadata.partitionColumns.empty()) {
      return {
          std::make_shared<DuckLakePartitionHandle>(DuckLakePartitionKey{})};
    }
    return {};
  }

  std::vector<PartitionHandlePtr> partitions;
  std::unordered_set<std::string> seen;
  for (const auto& file : tableMetadata.dataFiles) {
    DuckLakePartitionKey partition{
        .partitionId = file.partitionId,
        .partitionValues = file.partitionValues,
        .partitionKeys = file.partitionKeys,
    };
    auto fingerprint = partitionFingerprint(partition);
    if (seen.count(fingerprint) != 0) {
      continue;
    }
    seen.insert(std::move(fingerprint));

    if (partitionMatchesFilters(partition, tableMetadata, tableHandle)) {
      partitions.push_back(
          std::make_shared<DuckLakePartitionHandle>(std::move(partition)));
    }
  }
  return partitions;
}

bool fileMatchesPartition(
    const DuckLakeDataFile& file,
    const DuckLakePartitionKey& partition) {
  return file.partitionId == partition.partitionId &&
      file.partitionValues == partition.partitionValues;
}

std::vector<DuckLakeDataFile> selectedDataFiles(
    const std::vector<DuckLakeDataFile>& files,
    const std::vector<PartitionHandlePtr>& partitions) {
  std::vector<DuckLakeDataFile> selectedFiles;
  for (const auto& file : files) {
    for (const auto& partition : partitions) {
      auto* duckLakePartition =
          dynamic_cast<const DuckLakePartitionHandle*>(partition.get());
      VELOX_CHECK_NOT_NULL(duckLakePartition);
      if (fileMatchesPartition(file, duckLakePartition->partition())) {
        selectedFiles.push_back(file);
        break;
      }
    }
  }
  return selectedFiles;
}

TablePtr makeDuckLakeTable(
    DuckLakeTableMetadata tableMetadata,
    const std::shared_ptr<velox::connector::hive::iceberg::IcebergConnector>&
        icebergConnector) {
  VELOX_CHECK_NOT_NULL(icebergConnector);
  auto partitionColumns = tableMetadata.partitionColumns;
  auto duckLakeColumns = std::move(tableMetadata.columns);
  auto dataFiles = std::move(tableMetadata.dataFiles);
  auto table = std::make_shared<DuckLakeTable>(std::move(tableMetadata));
  table->addLayout(
      std::make_unique<DuckLakeTableLayout>(
          "ducklake",
          table.get(),
          icebergConnector,
          table->allColumns(),
          hivePartitionColumns(*table, partitionColumns),
          std::move(duckLakeColumns),
          std::move(dataFiles)));
  return table;
}

} // namespace

// Enumerates live DuckLake files as Velox Iceberg splits.
class DuckLakeSplitSource : public SplitSource {
 public:
  DuckLakeSplitSource(
      std::vector<DuckLakeDataFile> files,
      std::string connectorId)
      : files_{std::move(files)}, connectorId_{std::move(connectorId)} {}

  folly::coro::Task<SplitBatch> co_getSplits(uint32_t maxSplitCount) override {
    SplitBatch batch;
    const auto limit = static_cast<size_t>(maxSplitCount);
    while (batch.splits.size() < limit && fileIndex_ < files_.size()) {
      const auto& file = files_[fileIndex_];
      if (splitIndex_ == 0) {
        splitsInCurrentFile_ = splitCountForFile(file);
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
            ? std::min<uint64_t>(splitSize, file.fileSizeBytes.value() - start)
            : std::numeric_limits<uint64_t>::max();

        batch.splits.push_back(
            std::make_shared<velox::connector::hive::iceberg::HiveIcebergSplit>(
                connectorId_,
                file.path,
                velox::dwio::common::FileFormat::PARQUET,
                start,
                length,
                file.partitionKeys,
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
  size_t fileIndex_{0};
  uint64_t splitIndex_{0};
  uint64_t splitsInCurrentFile_{0};
};

DuckLakeSplitManager::DuckLakeSplitManager(
    std::shared_ptr<DuckLakeCatalogClient> catalog,
    std::shared_ptr<velox::connector::hive::iceberg::IcebergConnector>
        icebergConnector)
    : catalog_{std::move(catalog)},
      icebergConnector_{std::move(icebergConnector)} {
  VELOX_CHECK_NOT_NULL(catalog_);
  VELOX_CHECK_NOT_NULL(icebergConnector_);
}

folly::coro::Task<std::vector<PartitionHandlePtr>>
DuckLakeSplitManager::co_listPartitions(
    const ConnectorSessionPtr& /*session*/,
    const velox::connector::ConnectorTableHandlePtr& tableHandle) {
  auto* hiveTableHandle =
      dynamic_cast<const velox::connector::hive::HiveTableHandle*>(
          tableHandle.get());
  VELOX_CHECK_NOT_NULL(hiveTableHandle);

  const auto schema = hiveTableHandle->dbName().empty()
      ? std::string{"main"}
      : hiveTableHandle->dbName();
  auto tableMetadata = catalog_->loadTable({schema, tableHandle->name()});
  VELOX_CHECK(
      tableMetadata.has_value(),
      "Could not find table in DuckLake catalog: {}.{}",
      schema,
      tableHandle->name());
  co_return makePartitionHandles(tableMetadata.value(), *hiveTableHandle);
}

std::shared_ptr<SplitSource> DuckLakeSplitManager::getSplitSource(
    const ConnectorSessionPtr& /*session*/,
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    const std::vector<PartitionHandlePtr>& partitions) {
  auto* hiveTableHandle =
      dynamic_cast<const velox::connector::hive::HiveTableHandle*>(
          tableHandle.get());
  VELOX_CHECK_NOT_NULL(hiveTableHandle);

  const auto schema = hiveTableHandle->dbName().empty()
      ? std::string{"main"}
      : hiveTableHandle->dbName();
  auto tableMetadata = catalog_->loadTable({schema, tableHandle->name()});
  VELOX_CHECK(
      tableMetadata.has_value(),
      "Could not find table in DuckLake catalog: {}.{}",
      schema,
      tableHandle->name());
  auto table =
      makeDuckLakeTable(std::move(tableMetadata.value()), icebergConnector_);
  auto* layout = dynamic_cast<const DuckLakeTableLayout*>(table->layouts()[0]);
  VELOX_CHECK_NOT_NULL(layout);

  return std::make_shared<DuckLakeSplitSource>(
      selectedDataFiles(layout->dataFiles(), partitions),
      layout->connectorId());
}

DuckLakeTableLayout::DuckLakeTableLayout(
    const std::string& label,
    const Table* table,
    std::shared_ptr<velox::connector::hive::iceberg::IcebergConnector>
        icebergConnector,
    std::vector<const Column*> columns,
    std::vector<const Column*> hivePartitionColumns,
    std::vector<DuckLakeColumnMetadata> duckLakeColumns,
    std::vector<DuckLakeDataFile> dataFiles)
    : hive::HiveTableLayout(
          label,
          table,
          connectorPointer(icebergConnector),
          std::move(columns),
          std::nullopt,
          {},
          {},
          {},
          {},
          std::move(hivePartitionColumns),
          velox::dwio::common::FileFormat::PARQUET),
      icebergConnector_{std::move(icebergConnector)},
      dataFiles_{std::move(dataFiles)} {
  VELOX_CHECK_NOT_NULL(icebergConnector_);
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

  const auto handleType = columnType(*this, *column);
  velox::parquet::ParquetFieldId fieldId{-1, {}};
  if (handleType ==
      velox::connector::hive::HiveColumnHandle::ColumnType::kRegular) {
    auto it = fieldIds_.find(columnName);
    VELOX_CHECK(
        it != fieldIds_.end(),
        "DuckLake column is missing field id: {}",
        columnName);
    fieldId = it->second;
  }

  return std::make_shared<velox::connector::hive::iceberg::IcebergColumnHandle>(
      columnName, handleType, column->type(), fieldId, std::move(subfields));
}

DuckLakeTable::DuckLakeTable(DuckLakeTableMetadata metadata)
    : hive::HiveTable(
          std::move(metadata.name),
          std::move(metadata.rowType),
          false,
          true,
          {},
          identityPartitionColumnNames(metadata.partitionColumns)),
      numRows_{metadata.numRows} {}

void DuckLakeTable::addLayout(std::unique_ptr<DuckLakeTableLayout> layout) {
  exportedLayouts_.push_back(layout.get());
  layouts_.push_back(std::move(layout));
}

DuckLakeConnectorMetadata::DuckLakeConnectorMetadata(
    std::shared_ptr<velox::connector::hive::iceberg::IcebergConnector>
        icebergConnector)
    : icebergConnector_{std::move(icebergConnector)} {
  VELOX_CHECK_NOT_NULL(icebergConnector_);
  catalog_ = DuckLakeCatalogClient::create(
      DuckLakeMetadataConfig(icebergConnector_->connectorConfig())
          .catalogSpec());
  VELOX_CHECK_NOT_NULL(catalog_);
  splitManager_ =
      std::make_unique<DuckLakeSplitManager>(catalog_, icebergConnector_);
}

DuckLakeConnectorMetadata::DuckLakeConnectorMetadata(
    std::shared_ptr<velox::connector::hive::iceberg::IcebergConnector>
        icebergConnector,
    std::shared_ptr<DuckLakeCatalogClient> catalog)
    : icebergConnector_{std::move(icebergConnector)},
      catalog_{std::move(catalog)} {
  VELOX_CHECK_NOT_NULL(icebergConnector_);
  VELOX_CHECK_NOT_NULL(catalog_);
  splitManager_ =
      std::make_unique<DuckLakeSplitManager>(catalog_, icebergConnector_);
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
  return std::ranges::find(schemas, schemaName) != schemas.end();
}

TablePtr DuckLakeConnectorMetadata::makeTable(
    DuckLakeTableMetadata tableMetadata) const {
  return makeDuckLakeTable(std::move(tableMetadata), icebergConnector_);
}

} // namespace facebook::axiom::connector::ducklake
