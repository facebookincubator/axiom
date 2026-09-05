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

#include <boost/algorithm/string/predicate.hpp>
#include <fmt/ranges.h>
#include <folly/Conv.h>
#include <folly/String.h>
#include <algorithm>
#include <utility>
#include "velox/common/Casts.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/HiveConnectorUtil.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/exec/TableWriter.h"
#include "velox/expression/ExprConstants.h"

namespace facebook::axiom::connector::hive {

HivePartitionType::HivePartitionType(
    int32_t numBuckets,
    int32_t numPartitions,
    std::vector<velox::TypePtr> partitionKeyTypes)
    : numBuckets_(numBuckets),
      numPartitions_(numPartitions),
      partitionKeyTypes_(std::move(partitionKeyTypes)) {
  VELOX_CHECK_GT(numBuckets_, 0);
  VELOX_CHECK_GT(numPartitions_, 0);
  VELOX_CHECK_LE(numPartitions_, numBuckets_);
}

std::shared_ptr<PartitionType> HivePartitionType::copartition(
    const PartitionType& other) const {
  const auto* otherPartitionType = other.as<HivePartitionType>();
  if (otherPartitionType == nullptr) {
    return nullptr;
  }
  const auto& thisTypes = partitionKeyTypes_;
  const auto& otherTypes = otherPartitionType->partitionKeyTypes_;
  if (thisTypes.size() != otherTypes.size()) {
    return nullptr;
  }
  for (size_t i = 0; i < thisTypes.size(); ++i) {
    if (!thisTypes[i]->equivalent(*otherTypes[i])) {
      return nullptr;
    }
  }
  const int32_t otherNumBuckets = otherPartitionType->numBuckets_;
  // Both sides route a bucket to bucket % numPartitions, so the coarser of the
  // two counts is what they agree on; keeping the finer would send a key to
  // different tasks on each side.
  const int32_t numPartitions =
      std::min(numPartitions_, otherPartitionType->numPartitions_);
  if (otherNumBuckets % numBuckets_ == 0) {
    return std::make_shared<HivePartitionType>(
        numBuckets_, numPartitions, thisTypes);
  }
  if (numBuckets_ % otherNumBuckets == 0) {
    return std::make_shared<HivePartitionType>(
        otherNumBuckets, numPartitions, otherTypes);
  }
  return nullptr;
}

std::shared_ptr<PartitionType> HivePartitionType::scaleDown(
    int32_t maxPartitions) const {
  VELOX_CHECK_GT(maxPartitions, 0);
  return std::make_shared<HivePartitionType>(
      numBuckets_, std::min(numBuckets_, maxPartitions), partitionKeyTypes_);
}

velox::core::PartitionFunctionSpecPtr HivePartitionType::makeSpec(
    const std::vector<velox::column_index_t>& channels,
    const std::vector<velox::VectorPtr>& constants,
    bool /*isLocal*/) const {
  std::vector<int> bucketToPartition;
  if (numPartitions_ != numBuckets_) {
    bucketToPartition.reserve(numBuckets_);
    for (int32_t bucket = 0; bucket < numBuckets_; ++bucket) {
      bucketToPartition.push_back(mapBucketToPartition(bucket));
    }
  }
  return std::make_shared<velox::connector::hive::HivePartitionFunctionSpec>(
      numBuckets_, std::move(bucketToPartition), channels, constants);
}

std::string HivePartitionType::toString() const {
  if (numPartitions_ == numBuckets_) {
    return fmt::format("{} Hive buckets", numBuckets_);
  }
  return fmt::format(
      "{} Hive buckets -> {} partitions", numBuckets_, numPartitions_);
}

namespace {

// What a Hive-family connector says about a column the table is partitioned
// by.
constexpr std::string_view kPartitionKey = "partition key";

std::vector<std::unique_ptr<const connector::Column>> makeColumns(
    const velox::RowTypePtr& type,
    bool bucketed,
    bool includeHiddenColumns,
    const std::vector<std::string>& partitionColumnNames = {}) {
  const folly::F14FastSet<std::string> partitionColumns(
      partitionColumnNames.begin(), partitionColumnNames.end());

  std::vector<std::unique_ptr<const connector::Column>> columns;
  columns.reserve(type->size() + 2 + (bucketed ? 1 : 0));

  for (auto i = 0; i < type->size(); i++) {
    const bool isPartitionColumn = partitionColumns.contains(type->nameOf(i));
    columns.emplace_back(
        std::make_unique<connector::Column>(
            type->nameOf(i),
            type->childAt(i),
            /*hidden=*/false,
            /*includeInExplainIo=*/isPartitionColumn,
            /*extraInfo=*/
            isPartitionColumn ? std::optional<std::string>(kPartitionKey)
                              : std::nullopt));
  }

  if (includeHiddenColumns) {
    // Add hidden columns.
    columns.emplace_back(
        std::make_unique<connector::Column>(
            HiveTable::kPath, velox::VARCHAR(), /*hidden=*/true));
    columns.emplace_back(
        std::make_unique<connector::Column>(
            HiveTable::kFileSize, velox::BIGINT(), /*hidden=*/true));
    if (bucketed) {
      columns.emplace_back(
          std::make_unique<connector::Column>(
              HiveTable::kBucket, velox::INTEGER(), /*hidden=*/true));
    }
    columns.emplace_back(
        std::make_unique<connector::Column>(
            HiveTable::kRowId, HiveTable::rowIdType(), /*hidden=*/true));
  }

  return columns;
}
} // namespace

HiveTable::HiveTable(
    SchemaTableName name,
    velox::RowTypePtr type,
    bool bucketed,
    bool includeHiddenColumns,
    folly::F14FastMap<std::string, velox::Variant> options,
    std::vector<std::string> partitionColumnNames)
    : Table(
          std::move(name),
          hive::makeColumns(
              type,
              bucketed,
              includeHiddenColumns,
              partitionColumnNames),
          std::move(options)) {}

std::vector<std::string> HiveTable::ioColumnPriority() const {
  const auto* hiveLayout = layouts()[0]->asChecked<HiveTableLayout>();

  bool hasDs = false;
  bool hasTs = false;
  for (const auto* column : hiveLayout->hivePartitionColumns()) {
    hasDs |= column->name() == "ds";
    hasTs |= column->name() == "ts";
  }

  // ts (hourly) is finer-grained than ds (daily), so list it first.
  if (hasTs && hasDs) {
    return {"ts", "ds"};
  }
  if (hasDs) {
    return {"ds"};
  }
  return {};
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
    const std::string& label,
    const Table* table,
    velox::connector::Connector* connector,
    std::vector<const Column*> columns,
    std::optional<int32_t> numPartitions,
    const std::vector<const Column*>& partitionedByColumns,
    const std::vector<const Column*>& sortedByColumns,
    const std::vector<SortOrder>& sortOrder,
    std::vector<const Column*> lookupKeys,
    std::vector<const Column*> hivePartitionedByColumns,
    velox::dwio::common::FileFormat fileFormat)
    : TableLayout(
          label,
          table,
          connector,
          std::move(columns),
          partitionedByColumns,
          sortedByColumns,
          sortOrder,
          std::move(lookupKeys),
          /*supportsScan=*/true),
      fileFormat_(fileFormat),
      hivePartitionColumns_(std::move(hivePartitionedByColumns)),
      numBuckets_(numPartitions),
      partitionType_{
          numPartitions.has_value()
              ? std::make_shared<const HivePartitionType>(
                    numPartitions.value(),
                    extractPartitionKeyTypes(partitionedByColumns))
              : nullptr} {
  VELOX_CHECK_EQ(sortedByColumns.size(), sortOrder.size());
}

// static
velox::Variant HiveTableLayout::partitionValueToVariant(
    std::string_view value,
    const velox::Type& type) {
  switch (type.kind()) {
    case velox::TypeKind::BOOLEAN:
      return velox::Variant(folly::to<bool>(value));
    case velox::TypeKind::TINYINT:
      return velox::Variant(folly::to<int8_t>(value));
    case velox::TypeKind::SMALLINT:
      return velox::Variant(folly::to<int16_t>(value));
    case velox::TypeKind::INTEGER:
      return velox::Variant(folly::to<int32_t>(value));
    case velox::TypeKind::BIGINT:
      return velox::Variant(folly::to<int64_t>(value));
    case velox::TypeKind::VARCHAR:
      return velox::Variant(std::string(value));
    default:
      VELOX_UNREACHABLE(
          "Unsupported partition column type: {}", type.toString());
  }
}

namespace {
velox::connector::hive::HiveColumnHandle::ColumnType columnType(
    const HiveTableLayout& layout,
    const facebook::axiom::connector::Column* column) {
  if (column->name() == HiveTable::kRowId) {
    return velox::connector::hive::HiveColumnHandle::ColumnType::kRowId;
  }

  if (column->hidden()) {
    return velox::connector::hive::HiveColumnHandle::ColumnType::kSynthesized;
  }

  for (const auto& partitionColumn : layout.hivePartitionColumns()) {
    if (column->name() == partitionColumn->name()) {
      return velox::connector::hive::HiveColumnHandle::ColumnType::
          kPartitionKey;
    }
  }

  return velox::connector::hive::HiveColumnHandle::ColumnType::kRegular;
}

/// Adds input fields referenced by the given expression to 'names'.
void extractInputFields(
    const velox::core::TypedExprPtr& expr,
    std::unordered_set<std::string>& names) {
  if (expr->isFieldAccessKind()) {
    if (expr->inputs().empty() || expr->inputs()[0]->isInputKind()) {
      names.emplace(
          expr->asUnchecked<velox::core::FieldAccessTypedExpr>()->name());
    }
    return;
  }

  for (const auto& child : expr->inputs()) {
    extractInputFields(child, names);
  }
}

} // namespace

velox::connector::ColumnHandlePtr HiveTableLayout::createColumnHandle(
    const ConnectorSessionPtr& /*session*/,
    const std::string& columnName,
    std::vector<velox::common::Subfield> subfields,
    std::optional<velox::TypePtr> castToType,
    SubfieldMapping subfieldMapping) const {
  // castToType and subfieldMapping are not yet supported.
  VELOX_CHECK(!castToType.has_value());
  VELOX_CHECK(subfieldMapping.empty());
  auto* column = findColumn(columnName);
  VELOX_CHECK_NOT_NULL(
      column, "Column not found: {} in table {}", columnName, table().name());
  return std::make_shared<velox::connector::hive::HiveColumnHandle>(
      columnName,
      columnType(*this, column),
      column->type(),
      column->type(),
      std::move(subfields));
}

velox::connector::ConnectorTableHandlePtr HiveTableLayout::createTableHandle(
    const ConnectorSessionPtr& session,
    std::vector<velox::connector::ColumnHandlePtr> /*columnHandles*/,
    velox::core::ExpressionEvaluator& evaluator,
    std::vector<velox::core::TypedExprPtr> filters,
    std::vector<int32_t>& /*rejectedFilterIndices*/,
    velox::RowTypePtr dataColumns,
    std::optional<LookupKeys> lookupKeys) const {
  VELOX_CHECK(!lookupKeys.has_value(), "Hive does not support lookup keys");

  std::unordered_set<std::string> filterColumnNames;
  for (const auto& filter : filters) {
    extractInputFields(filter, filterColumnNames);
  }

  std::vector<velox::core::TypedExprPtr> remainingConjuncts;
  velox::common::SubfieldFilters subfieldFilters;
  double sampleRate = 1.0;
  for (auto& typedExpr : filters) {
    auto remaining = velox::connector::hive::extractFiltersFromRemainingFilter(
        typedExpr, &evaluator, subfieldFilters, sampleRate);

    if (remaining != nullptr) {
      remainingConjuncts.push_back(std::move(remaining));
    }
  }

  velox::core::TypedExprPtr remainingFilter;
  if (remainingConjuncts.size() == 1) {
    remainingFilter = std::move(remainingConjuncts[0]);
  } else if (remainingConjuncts.size() > 1) {
    remainingFilter = std::make_shared<velox::core::CallTypedExpr>(
        velox::BOOLEAN(), remainingConjuncts, velox::expression::kAnd);
  }

  std::vector<velox::connector::hive::HiveColumnHandlePtr> filterColumnHandles;
  filterColumnHandles.reserve(filterColumnNames.size());
  for (const auto& name : filterColumnNames) {
    filterColumnHandles.emplace_back(
        std::static_pointer_cast<
            const velox::connector::hive::HiveColumnHandle>(
            createColumnHandle(session, name)));
  }

  const auto& tableName = table().name();
  return std::make_shared<velox::connector::hive::HiveTableHandle>(
      connector()->connectorId(),
      tableName.table,
      std::move(subfieldFilters),
      remainingFilter,
      dataColumns ? dataColumns : rowType(),
      /*indexColumns=*/std::vector<std::string>{},
      serdeParameters(),
      filterColumnHandles,
      sampleRate,
      tableName.schema);
}

std::vector<const Column*> HiveTableLayout::nonPartitionFilterColumns(
    const velox::connector::hive::HiveTableHandle& handle) const {
  std::unordered_set<std::string> filterColumnNames;
  for (const auto& [subfield, filter] : handle.subfieldFilters()) {
    filterColumnNames.emplace(subfield.baseName());
  }
  if (handle.remainingFilter() != nullptr) {
    extractInputFields(handle.remainingFilter(), filterColumnNames);
  }

  folly::F14FastSet<std::string> partitionColumnNames;
  for (const auto* column : hivePartitionColumns_) {
    partitionColumnNames.insert(column->name());
  }

  std::vector<const Column*> filterColumns;
  for (const auto* column : columns()) {
    if (filterColumnNames.contains(column->name()) &&
        !partitionColumnNames.contains(column->name())) {
      filterColumns.push_back(column);
    }
  }
  return filterColumns;
}

std::vector<std::string> HiveTableLayout::withFilterColumns(
    const velox::connector::hive::HiveTableHandle& handle,
    const std::vector<std::string>& requested,
    const std::function<bool(const std::string&)>& hasStats) const {
  std::vector<std::string> columnNames = requested;
  std::unordered_set<std::string> seen{requested.begin(), requested.end()};
  for (const auto* column : nonPartitionFilterColumns(handle)) {
    if (seen.insert(column->name()).second && hasStats(column->name())) {
      columnNames.emplace_back(column->name());
    }
  }
  return columnNames;
}

void trimColumnStats(FilteredTableStats& stats, size_t numColumns) {
  if (stats.columnStats.size() > numColumns) {
    stats.columnStats.erase(
        stats.columnStats.begin() + numColumns, stats.columnStats.end());
  }
}

void HiveTableLayout::foldNonPartitionFilterStats(
    const velox::connector::hive::HiveTableHandle& handle,
    const FilterSelectivityEstimator& estimator,
    FilteredTableStats& stats) const {
  folly::F14FastSet<std::string> partitionColumnNames;
  for (const auto* column : hivePartitionColumns_) {
    partitionColumnNames.insert(column->name());
  }

  // Partition-key subfield filters are already reflected in 'stats' (from
  // partition metadata); only the remaining single-column filters need
  // estimating.
  folly::F14FastMap<std::string, const velox::common::Filter*> dataFilters;
  for (const auto& [subfield, filter] : handle.subfieldFilters()) {
    const auto& name = subfield.baseName();
    if (!partitionColumnNames.contains(name)) {
      dataFilters.emplace(name, filter.get());
    }
  }

  applyFilterEstimates(
      dataFilters, handle.remainingFilter(), columns(), estimator, stats);
}

std::string HiveDeleteWriteHandle::toString() const {
  if (filters_.empty()) {
    return fmt::format("drop all rows of {}", table_->name().toString());
  }

  // SubfieldFilters is unordered; sort by column so the description is stable.
  std::vector<std::pair<std::string_view, const velox::common::Filter*>>
      byColumn;
  byColumn.reserve(filters_.size());
  for (const auto& [subfield, filter] : filters_) {
    byColumn.emplace_back(subfield.baseName(), filter.get());
  }
  std::sort(
      byColumn.begin(),
      byColumn.end(),
      [](const auto& left, const auto& right) {
        return left.first < right.first;
      });

  std::vector<std::string> predicates;
  predicates.reserve(byColumn.size());
  for (const auto& [column, filter] : byColumn) {
    predicates.push_back(fmt::format("{} {}", column, filter->toString()));
  }

  return fmt::format(
      "drop partitions of {} matching {}",
      table_->name().toString(),
      fmt::join(predicates, " AND "));
}

namespace {
std::shared_ptr<velox::connector::hive::LocationHandle> makeLocationHandle(
    const std::string& targetDirectory,
    const std::optional<std::string>& writeDirectory) {
  return std::make_shared<velox::connector::hive::LocationHandle>(
      targetDirectory,
      writeDirectory.value_or(targetDirectory),
      velox::connector::hive::LocationHandle::TableType::kNew);
}

// Returns a handle for removing whole partitions. The partitions are resolved
// at commit by matching the scan's range filters against partition values, so
// every pushed-down filter must be a range filter on a partition column.
// TODO: Support a remaining filter over partition columns alone, which also
// selects whole partitions but cannot be matched this way.
velox::common::SubfieldFilters deleteFilters(
    const HiveTableLayout& layout,
    const velox::connector::ConnectorTableHandlePtr& scanHandle) {
  VELOX_USER_CHECK_NOT_NULL(scanHandle, "DELETE requires a scan of the table");
  const auto* hiveScan =
      scanHandle->asChecked<velox::connector::hive::HiveTableHandle>();

  VELOX_USER_CHECK_NULL(
      hiveScan->remainingFilter(),
      "DELETE supports only range filters on partition columns: {}",
      hiveScan->remainingFilter()->toString());

  folly::F14FastSet<std::string_view> partitionColumns;
  for (const auto* column : layout.hivePartitionColumns()) {
    partitionColumns.insert(column->name());
  }

  velox::common::SubfieldFilters filters;
  for (const auto& [subfield, filter] : hiveScan->subfieldFilters()) {
    VELOX_USER_CHECK(
        partitionColumns.contains(subfield.baseName()),
        "DELETE supports only filters on partition columns: {}",
        subfield.baseName());
    filters.emplace(subfield.clone(), filter->clone());
  }

  return filters;
}

} // namespace

ConnectorWriteHandlePtr HiveConnectorMetadata::makeDeleteWriteHandle(
    const TablePtr& table,
    velox::common::SubfieldFilters filters) const {
  return std::make_shared<HiveDeleteWriteHandle>(table, std::move(filters));
}

ConnectorWriteHandlePtr HiveConnectorMetadata::beginWrite(
    const ConnectorSessionPtr& session,
    const TablePtr& table,
    WriteKind kind,
    const velox::connector::ConnectorTableHandlePtr& scanHandle,
    bool explain) {
  ensureInitialized();
  VELOX_CHECK(
      kind == WriteKind::kCreate || kind == WriteKind::kInsert ||
          kind == WriteKind::kDelete,
      "Only CREATE/INSERT/DELETE supported, not {}",
      WriteKindName::toName(kind));

  const auto* hiveLayout = table->layouts()[0]->asChecked<HiveTableLayout>();

  if (kind == WriteKind::kDelete) {
    return makeDeleteWriteHandle(table, deleteFilters(*hiveLayout, scanHandle));
  }
  auto storageFormat = hiveLayout->fileFormat();

  const auto& serdeParameters = hiveLayout->serdeParameters();

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
    inputColumns.push_back(
        std::static_pointer_cast<
            const velox::connector::hive::HiveColumnHandle>(
            hiveLayout->createColumnHandle(session, name)));
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
    for (size_t i = 0; i < hiveLayout->orderColumns().size(); ++i) {
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
              tablePath(table->name()),
              explain ? std::nullopt : makeStagingDirectory(table->name())),
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

std::vector<SortKey> SortKey::parse(const std::vector<std::string>& entries) {
  std::vector<SortKey> keys;
  keys.reserve(entries.size());
  for (const auto& entry : entries) {
    std::vector<std::string> parts;
    folly::split(' ', entry, parts, /*ignoreEmpty=*/true);
    VELOX_USER_CHECK(
        parts.size() == 1 || parts.size() == 2,
        "Invalid sorted_by entry: {}",
        entry);

    bool ascending = true;
    if (parts.size() == 2) {
      ascending = boost::iequals(parts[1], "ASC");
      VELOX_USER_CHECK(
          ascending || boost::iequals(parts[1], "DESC"),
          "Invalid sort direction: {}",
          parts[1]);
    }

    keys.push_back(
        SortKey{
            .column = parts[0],
            .order = SortOrder{
                .isAscending = ascending, .isNullsFirst = ascending}});
  }
  return keys;
}

std::vector<std::string> SortKey::toEntries(const std::vector<SortKey>& keys) {
  std::vector<std::string> entries;
  entries.reserve(keys.size());
  for (const auto& key : keys) {
    entries.push_back(
        key.order.isAscending ? key.column
                              : fmt::format("{} DESC", key.column));
  }
  return entries;
}

void HiveConnectorMetadata::validateOptions(
    const folly::F14FastMap<std::string, velox::Variant>& options) const {
  static const folly::F14FastSet<std::string_view> kAllowed = {
      HiveWriteOptions::kBucketedBy,
      HiveWriteOptions::kBucketCount,
      HiveWriteOptions::kPartitionedBy,
      HiveWriteOptions::kSortedBy,
      HiveWriteOptions::kFormat,
      HiveWriteOptions::kCompressionKind,
      HiveWriteOptions::kFieldDelim,
      HiveWriteOptions::kSerializationNullFormat,
  };

  for (auto& pair : options) {
    if (!kAllowed.contains(pair.first)) {
      VELOX_USER_FAIL("Option {} is not supported", pair.first);
    }
  }
}

} // namespace facebook::axiom::connector::hive
