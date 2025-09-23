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
#include "velox/expression/ExprToSubfieldFilter.h"

namespace facebook::axiom::connector::hive {

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

velox::connector::ConnectorInsertTableHandlePtr
HiveConnectorMetadata::createInsertTableHandle(
    const TableLayout& layout,
    const velox::RowTypePtr& rowType,
    const folly::F14FastMap<std::string, std::string>& options,
    WriteKind kind,
    const ConnectorSessionPtr& session) {
  ensureInitialized();
  VELOX_CHECK_EQ(kind, WriteKind::kInsert, "Only insert supported");

  auto* hiveLayout = dynamic_cast<const HiveTableLayout*>(&layout);
  VELOX_CHECK_NOT_NULL(hiveLayout);
  auto storageFormat = hiveLayout->fileFormat();

  std::unordered_map<std::string, std::string> serdeParameters;
  const std::shared_ptr<velox::dwio::common::WriterOptions> writerOptions;

  velox::common::CompressionKind compressionKind;

  auto it = options.find("compression_kind");
  if (it != options.end()) {
    compressionKind = velox::common::stringToCompressionKind(it->second);
  } else {
    it = layout.table().options().find("compression_kind");
    if (it != layout.table().options().end()) {
      compressionKind = velox::common::stringToCompressionKind(it->second);
    } else {
      compressionKind = velox::common::CompressionKind::CompressionKind_ZSTD;
    }
  }

  std::vector<velox::connector::hive::HiveColumnHandlePtr> inputColumns;
  inputColumns.reserve(rowType->size());
  for (const auto& name : rowType->names()) {
    inputColumns.push_back(std::static_pointer_cast<
                           const velox::connector::hive::HiveColumnHandle>(
        createColumnHandle(layout, name)));
  }

  std::shared_ptr<const velox::connector::hive::HiveBucketProperty>
      bucketProperty;
  if (hiveLayout->numBuckets().has_value()) {
    std::vector<std::string> names;
    std::vector<velox::TypePtr> types;
    for (auto& column : layout.partitionColumns()) {
      names.push_back(column->name());
      types.push_back(column->type());
    }
    std::vector<
        std::shared_ptr<const velox::connector::hive::HiveSortingColumn>>
        sortedBy;
    sortedBy.reserve(layout.orderColumns().size());
    for (auto i = 0; i < layout.orderColumns().size(); ++i) {
      sortedBy.push_back(
          std::make_shared<velox::connector::hive::HiveSortingColumn>(
              layout.orderColumns()[i]->name(),
              velox::core::SortOrder(
                  layout.sortOrder()[i].isAscending,
                  layout.sortOrder()[i].isNullsFirst)));
    }

    bucketProperty =
        std::make_shared<velox::connector::hive::HiveBucketProperty>(
            velox::connector::hive::HiveBucketProperty::Kind::kHiveCompatible,
            hiveLayout->numBuckets().value(),
            std::move(names),
            std::move(types),
            std::move(sortedBy));
  }
  return std::make_shared<velox::connector::hive::HiveInsertTableHandle>(
      inputColumns,
      makeLocationHandle(tablePath(layout.table().name()), std::nullopt),
      storageFormat,
      bucketProperty,
      compressionKind,
      serdeParameters,
      writerOptions,
      false);
}

void HiveConnectorMetadata::validateOptions(
    const folly::F14FastMap<std::string, std::string>& options) const {
  static folly::F14FastSet<std::string> allowed = {
      "bucketed_by",
      "sorted_by",
      "bucket_count",
      "partitioned_by",
      "file_format",
      "compression_kind"};
  for (auto& pair : options) {
    if (allowed.find(pair.first) == allowed.end()) {
      VELOX_USER_FAIL("Option {} is not supported", pair.first);
    }
  }
}

} // namespace facebook::axiom::connector::hive
