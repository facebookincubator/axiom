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

#include "axiom/connectors/hive/HiveConnectorMetadata.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/exec/TableWriter.h"
#include "velox/expression/ExprToSubfieldFilter.h"

namespace facebook::axiom::connector::hive {

const PartitionType* HivePartitionType::copartition(
    const PartitionType& other) const {
  if (const auto* otherPartitionType = other.as<HivePartitionType>()) {
    const auto& thisTypes = partitionKeyTypes_;
    const auto& otherTypes = otherPartitionType->partitionKeyTypes_;

    if (thisTypes.size() == otherTypes.size()) {
      for (size_t i = 0; i < thisTypes.size(); ++i) {
        if (!thisTypes[i]->equivalent(*otherTypes[i])) {
          return nullptr;
        }
      }

      if (otherPartitionType->numPartitions_ % numPartitions_ == 0) {
        return this;
      }

      if (numPartitions_ % otherPartitionType->numPartitions_ == 0) {
        return otherPartitionType;
      }
    }
  }
  return nullptr;
}

velox::core::PartitionFunctionSpecPtr HivePartitionType::makeSpec(
    const std::vector<velox::column_index_t>& channels,
    const std::vector<velox::VectorPtr>& constants,
    bool isLocal) const {
  return std::make_shared<velox::connector::hive::HivePartitionFunctionSpec>(
      numPartitions_, channels, constants);
}

std::string HivePartitionType::toString() const {
  return fmt::format("{} Hive buckets", numPartitions_);
}

namespace {
std::vector<velox::TypePtr> extractPartitionKeyTypes(
    const std::vector<const Column*>& partitionedByColumns) {
  std::vector<velox::TypePtr> types;
  types.reserve(partitionedByColumns.size());
  for (const auto* column : partitionedByColumns) {
    types.push_back(column->type());
  }
  return types;
}
} // namespace

HiveTableLayout::HiveTableLayout(
    const std::string& name,
    const Table* table,
    velox::connector::Connector* connector,
    std::vector<const Column*> columns,
    std::optional<int32_t> numPartitions,
    std::vector<const Column*> partitionedByColumns,
    std::vector<const Column*> sortedByColumns,
    std::vector<SortOrder> sortOrder,
    std::vector<const Column*> lookupKeys,
    std::vector<const Column*> hivePartitionedByColumns,
    velox::dwio::common::FileFormat fileFormat)
    : TableLayout(
          name,
          table,
          connector,
          columns,
          partitionedByColumns,
          sortedByColumns,
          sortOrder,
          lookupKeys,
          /*supportsScan=*/true),
      fileFormat_(fileFormat),
      hivePartitionColumns_(hivePartitionedByColumns),
      numBuckets_(numPartitions),
      partitionType_{
          numPartitions.has_value()
              ? std::make_optional<HivePartitionType>(
                    numPartitions.value(),
                    extractPartitionKeyTypes(partitionedByColumns))
              : std::nullopt} {
  VELOX_CHECK_EQ(sortedByColumns.size(), sortOrder.size());
}

namespace {
velox::connector::hive::HiveColumnHandle::ColumnType columnType(
    const HiveTableLayout& layout,
    std::string_view columnName) {
  auto& columns = layout.hivePartitionColumns();
  for (auto& column : columns) {
    if (column->name() == columnName) {
      return velox::connector::hive::HiveColumnHandle::ColumnType::
          kPartitionKey;
    }
  }
  // TODO recognize special names like $path, $bucket etc.
  return velox::connector::hive::HiveColumnHandle::ColumnType::kRegular;
}
} // namespace

velox::connector::ColumnHandlePtr HiveConnectorMetadata::createColumnHandle(
    const ConnectorSessionPtr& session,
    const TableLayout& layout,
    const std::string& columnName,
    std::vector<velox::common::Subfield> subfields,
    std::optional<velox::TypePtr> castToType,
    SubfieldMapping subfieldMapping) {
  // castToType and subfieldMapping are not yet supported.
  VELOX_CHECK(subfieldMapping.empty());
  auto* hiveLayout = reinterpret_cast<const HiveTableLayout*>(&layout);
  auto* column = hiveLayout->findColumn(columnName);
  return std::make_shared<velox::connector::hive::HiveColumnHandle>(
      columnName,
      columnType(*hiveLayout, columnName),
      column->type(),
      column->type(),
      std::move(subfields));
}

velox::connector::ConnectorTableHandlePtr
HiveConnectorMetadata::createTableHandle(
    const ConnectorSessionPtr& session,
    const TableLayout& layout,
    std::vector<velox::connector::ColumnHandlePtr> columnHandles,
    velox::core::ExpressionEvaluator& evaluator,
    std::vector<velox::core::TypedExprPtr> filters,
    std::vector<velox::core::TypedExprPtr>& rejectedFilters,
    velox::RowTypePtr dataColumns,
    std::optional<LookupKeys> lookupKeys) {
  VELOX_CHECK(!lookupKeys.has_value(), "Hive does not support lookup keys");
  auto* hiveLayout = dynamic_cast<const HiveTableLayout*>(&layout);

  std::vector<velox::core::TypedExprPtr> remainingConjuncts;
  velox::common::SubfieldFilters subfieldFilters;
  for (auto& typedExpr : filters) {
    try {
      auto pair = velox::exec::toSubfieldFilter(typedExpr, &evaluator);
      if (!pair.second) {
        remainingConjuncts.push_back(std::move(typedExpr));
        continue;
      }
      auto it = subfieldFilters.find(pair.first);
      if (it != subfieldFilters.end()) {
        auto merged = it->second->mergeWith(pair.second.get());
        subfieldFilters[std::move(pair.first)] = std::move(merged);
      } else {
        subfieldFilters[std::move(pair.first)] = std::move(pair.second);
      }
    } catch (const std::exception&) {
      remainingConjuncts.push_back(std::move(typedExpr));
    }
  }

  velox::core::TypedExprPtr remainingFilter;
  for (const auto& conjunct : remainingConjuncts) {
    if (!remainingFilter) {
      remainingFilter = conjunct;
    } else {
      remainingFilter = std::make_shared<velox::core::CallTypedExpr>(
          velox::BOOLEAN(),
          std::vector<velox::core::TypedExprPtr>{remainingFilter, conjunct},
          "and");
    }
  }
  return std::make_shared<velox::connector::hive::HiveTableHandle>(
      hiveConnector_->connectorId(),
      hiveLayout->table().name(),
      true,
      std::move(subfieldFilters),
      remainingFilter,
      dataColumns ? dataColumns : layout.rowType());
}

namespace {
std::shared_ptr<velox::connector::hive::LocationHandle> makeLocationHandle(
    std::string targetDirectory,
    std::optional<std::string> writeDirectory) {
  return std::make_shared<velox::connector::hive::LocationHandle>(
      targetDirectory,
      writeDirectory.value_or(targetDirectory),
      velox::connector::hive::LocationHandle::TableType::kNew);
}
} // namespace

ConnectorWriteHandlePtr HiveConnectorMetadata::beginWrite(
    const ConnectorSessionPtr& session,
    const TablePtr& table,
    WriteKind kind) {
  ensureInitialized();
  VELOX_CHECK(
      kind == WriteKind::kCreate || kind == WriteKind::kInsert,
      "Only CREATE/INSERT supported, not {}",
      WriteKindName::toName(kind));

  auto* hiveLayout = dynamic_cast<const HiveTableLayout*>(table->layouts()[0]);
  VELOX_CHECK_NOT_NULL(hiveLayout);
  auto storageFormat = hiveLayout->fileFormat();

  std::unordered_map<std::string, std::string> serdeParameters;
  const std::shared_ptr<velox::dwio::common::WriterOptions> writerOptions;

  velox::common::CompressionKind compressionKind;
  auto it =
      hiveLayout->table().options().find(HiveWriteOptions::kCompressionKind);
  if (it != hiveLayout->table().options().end()) {
    compressionKind =
        velox::common::stringToCompressionKind(it->second.value<std::string>());
  } else {
    compressionKind = velox::common::CompressionKind::CompressionKind_ZSTD;
  }

  std::vector<velox::connector::hive::HiveColumnHandlePtr> inputColumns;
  inputColumns.reserve(hiveLayout->rowType()->size());
  for (const auto& name : hiveLayout->rowType()->names()) {
    inputColumns.push_back(std::static_pointer_cast<
                           const velox::connector::hive::HiveColumnHandle>(
        createColumnHandle(session, *hiveLayout, name)));
  }

  std::shared_ptr<const velox::connector::hive::HiveBucketProperty>
      bucketProperty;
  if (hiveLayout->numBuckets().has_value()) {
    std::vector<std::string> names;
    std::vector<velox::TypePtr> types;
    for (auto& column : hiveLayout->partitionColumns()) {
      names.push_back(column->name());
      types.push_back(column->type());
    }
    std::vector<
        std::shared_ptr<const velox::connector::hive::HiveSortingColumn>>
        sortedBy;
    sortedBy.reserve(hiveLayout->orderColumns().size());
    for (auto i = 0; i < hiveLayout->orderColumns().size(); ++i) {
      sortedBy.push_back(
          std::make_shared<velox::connector::hive::HiveSortingColumn>(
              hiveLayout->orderColumns()[i]->name(),
              velox::core::SortOrder(
                  hiveLayout->sortOrder()[i].isAscending,
                  hiveLayout->sortOrder()[i].isNullsFirst)));
    }

    bucketProperty =
        std::make_shared<velox::connector::hive::HiveBucketProperty>(
            velox::connector::hive::HiveBucketProperty::Kind::kHiveCompatible,
            hiveLayout->numBuckets().value(),
            std::move(names),
            std::move(types),
            std::move(sortedBy));
  }

  auto veloxHandle =
      std::make_shared<velox::connector::hive::HiveInsertTableHandle>(
          inputColumns,
          makeLocationHandle(
              tablePath(table->name()), makeStagingDirectory(table->name())),
          storageFormat,
          bucketProperty,
          compressionKind,
          serdeParameters,
          writerOptions);
  return std::make_shared<HiveConnectorWriteHandle>(
      std::move(veloxHandle),
      velox::exec::TableWriteTraits::outputType(std::nullopt),
      table,
      kind);
}

void HiveConnectorMetadata::validateOptions(
    const folly::F14FastMap<std::string, velox::Variant>& options) const {
  static const folly::F14FastSet<std::string_view> kAllowed = {
      HiveWriteOptions::kBucketedBy,
      HiveWriteOptions::kBucketCount,
      HiveWriteOptions::kPartitionedBy,
      HiveWriteOptions::kSortedBy,
      HiveWriteOptions::kFileFormat,
      HiveWriteOptions::kCompressionKind,
  };

  for (auto& pair : options) {
    if (!kAllowed.contains(pair.first)) {
      VELOX_USER_FAIL("Option {} is not supported", pair.first);
    }
  }
}

} // namespace facebook::axiom::connector::hive
