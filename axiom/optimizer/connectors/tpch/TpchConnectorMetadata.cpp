// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "axiom/optimizer/connectors/tpch/TpchConnectorMetadata.h"

#include "axiom/optimizer/SchemaUtils.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/connectors/tpch/TpchConnectorSplit.h"
#include "velox/tpch/gen/TpchGen.h"

namespace facebook::velox::connector::tpch {

const std::unordered_map<std::string, double>& scaleFactorNamespaceMap() {
  const static std::unordered_map<std::string, double> sfmap = {
      {"tiny", 0.01},
      {"sf1", 1},
      {"sf100", 100},
      {"sf1000", 1000},
      {"sf10000", 10000},
      {"sf100000", 100000},
      {"sf300", 300},
      {"sf3000", 3000},
      {"sf30000", 30000}};
  return sfmap;
}

FOLLY_ALWAYS_INLINE constexpr std::string_view defaultTpchNamespace() {
  return "tiny";
}

std::vector<std::shared_ptr<const PartitionHandle>>
TpchSplitManager::listPartitions(const ConnectorTableHandlePtr& tableHandle) {
  return {std::make_shared<connector::PartitionHandle>()};
}

std::shared_ptr<SplitSource> TpchSplitManager::getSplitSource(
    const ConnectorTableHandlePtr& tableHandle,
    std::vector<std::shared_ptr<const PartitionHandle>> partitions,
    SplitOptions options) {
  auto* tpchTableHandle =
      dynamic_cast<const TpchTableHandle*>(tableHandle.get());
  VELOX_CHECK_NOT_NULL(
      tpchTableHandle, "Expected TpchTableHandle for TPCH connector");

  return std::make_shared<TpchSplitSource>(
      tpchTableHandle->getTable(),
      tpchTableHandle->getScaleFactor(),
      tpchTableHandle->connectorId(),
      options);
}

std::vector<SplitSource::SplitAndGroup> TpchSplitSource::getSplits(
    uint64_t targetBytes) {
  std::vector<SplitAndGroup> result;

  if (splits_.empty()) {
    // Generate splits if not already done
    auto rowType = velox::tpch::getTableSchema(table_);
    size_t rowSize = 0;
    for (auto i = 0; i < rowType->children().size(); i++) {
      // TODO: use actual size
      rowSize += 10;
    }
    const auto totalRows = velox::tpch::getRowCount(table_, scaleFactor_);
    const auto rowsPerSplit = options_.fileBytesPerSplit / rowSize;
    const auto numSplits = (totalRows + rowsPerSplit - 1) / rowsPerSplit;

    // TODO: adjust numSplits based on options_.targetSplitCount
    for (int64_t i = 0; i < numSplits; ++i) {
      splits_.push_back(
          std::make_shared<TpchConnectorSplit>(connectorId_, numSplits, i));
    }
  }

  if (currentSplit_ >= splits_.size()) {
    result.push_back(kNoMoreSplits);
    return result;
  }

  uint64_t bytes = 0;
  while (currentSplit_ < splits_.size()) {
    auto split = splits_[currentSplit_++];
    result.emplace_back(SplitAndGroup{split, 0});

    // TODO: use a more accurate size for the split
    bytes += options_.fileBytesPerSplit;

    if (bytes > targetBytes) {
      break;
    }
  }

  if (result.empty()) {
    result.push_back(SplitSource::SplitAndGroup{nullptr, 0});
  }

  return result;
}

TpchConnectorMetadata::TpchConnectorMetadata(TpchConnector* tpchConnector)
    : tpchConnector_(tpchConnector), splitManager_(this) {}

void TpchConnectorMetadata::initialize() {
  makeQueryCtx();
  initializeTables();
}

void TpchConnectorMetadata::ensureInitialized() const {
  std::lock_guard<std::mutex> l(mutex_);
  if (initialized_) {
    return;
  }
  const_cast<TpchConnectorMetadata*>(this)->initialize();
  initialized_ = true;
}

void TpchConnectorMetadata::makeQueryCtx() {
  queryCtx_ = makeQueryCtx("tpch_metadata");
}

std::shared_ptr<core::QueryCtx> TpchConnectorMetadata::makeQueryCtx(
    const std::string& queryId) {
  std::unordered_map<std::string, std::string> config;
  std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>
      connectorConfigs;

  return core::QueryCtx::create(
      tpchConnector_->executor(),
      core::QueryConfig(config),
      std::move(connectorConfigs),
      cache::AsyncDataCache::getInstance(),
      rootPool_->shared_from_this(),
      nullptr,
      queryId);
}

ColumnHandlePtr TpchConnectorMetadata::createColumnHandle(
    const TableLayout& layoutData,
    const std::string& columnName,
    std::vector<common::Subfield> subfields,
    std::optional<TypePtr> castToType,
    SubfieldMapping subfieldMapping) {
  return std::make_shared<connector::tpch::TpchColumnHandle>(columnName);
}

ConnectorTableHandlePtr TpchConnectorMetadata::createTableHandle(
    const TableLayout& layout,
    std::vector<ColumnHandlePtr> /*columnHandles*/,
    core::ExpressionEvaluator& /*evaluator*/,
    std::vector<core::TypedExprPtr> filters,
    std::vector<core::TypedExprPtr>& /*rejectedFilters*/,
    RowTypePtr /*dataColumns*/,
    std::optional<LookupKeys> /*lookupKeys*/) {
  auto* tpchLayout = dynamic_cast<const TpchTableLayout*>(&layout);
  velox::core::TypedExprPtr filterExpression;
  for (auto& filter : filters) {
    if (!filterExpression) {
      filterExpression = filter;
    } else {
      filterExpression = std::make_shared<velox::core::CallTypedExpr>(
          velox::BOOLEAN(),
          std::vector<velox::core::TypedExprPtr>{
              std::move(filterExpression), std::move(filter)},
          "and");
    }
  }
  return std::make_shared<TpchTableHandle>(
      tpchConnector_->connectorId(),
      tpchLayout->getTpchTable(),
      tpchLayout->getScaleFactor(),
      std::move(filterExpression));
}

void TpchConnectorMetadata::initializeTables() {
  for (auto tpchTable : velox::tpch::tables) {
    for (const auto& scaleFactorPair : scaleFactorNamespaceMap()) {
      loadTable(tpchTable, scaleFactorPair.first, scaleFactorPair.second);
    }
  }
}

void TpchConnectorMetadata::loadTable(
    velox::tpch::Table tpchTable,
    const std::string& ns,
    double scaleFactor) {
  const auto tableName =
      fmt::format("{}.{}", ns, velox::tpch::toTableName(tpchTable));
  const auto tableType = velox::tpch::getTableSchema(tpchTable);
  const auto numRows = velox::tpch::getRowCount(tpchTable, scaleFactor);

  auto table = std::make_unique<TpchTable>(tableName, tpchTable, scaleFactor);
  table->numRows_ = numRows;

  for (auto i = 0; i < tableType->size(); ++i) {
    const auto columnName = tableType->nameOf(i);
    const auto columnType = tableType->childAt(i);
    table->columns()[columnName] =
        std::make_unique<Column>(columnName, columnType);
  }
  table->setType(tableType);
  table->makeDefaultLayout(*this, scaleFactor);

  tables_[tableName] = std::move(table);
}

std::pair<int64_t, int64_t> TpchTableLayout::sample(
    const connector::ConnectorTableHandlePtr& handle,
    float pct,
    std::vector<core::TypedExprPtr> /* extraFilters */,
    RowTypePtr /* outputType */,
    const std::vector<common::Subfield>& /* fields */,
    HashStringAllocator* /* allocator */,
    std::vector<ColumnStatistics>* /* statistics */) const {
  const auto totalRows = velox::tpch::getRowCount(tpchTable_, scaleFactor_);
  const auto sampleRows = static_cast<int64_t>(totalRows * (pct / 100.0));
  return std::pair(sampleRows, sampleRows);
}

void TpchTable::makeDefaultLayout(
    TpchConnectorMetadata& metadata,
    double scaleFactor) {
  std::vector<const Column*> columns;
  for (auto i = 0; i < type_->size(); ++i) {
    auto name = type_->nameOf(i);
    columns.push_back(columns_[name].get());
  }
  auto* connector = metadata.tpchConnector();
  std::vector<const Column*> empty;
  auto layout = std::make_unique<TpchTableLayout>(
      name_,
      this,
      connector,
      std::move(columns),
      empty,
      empty,
      std::vector<SortOrder>{},
      empty,
      tpchTable_,
      scaleFactor);
  exportedLayouts_.push_back(layout.get());
  layouts_.push_back(std::move(layout));
}

const std::unordered_map<std::string, const Column*>& TpchTable::columnMap()
    const {
  std::lock_guard<std::mutex> l(mutex_);
  if (columns_.empty()) {
    return exportedColumns_;
  }
  for (auto& pair : columns_) {
    exportedColumns_[pair.first] = pair.second.get();
  }
  return exportedColumns_;
}

std::string getQualifiedName(const std::string& name) {
  velox::optimizer::TableNameParser parser(name);
  VELOX_CHECK(parser.valid(), "invalid table name '{}'", name);
  std::string qualifiedName;
  if (parser.schema().has_value()) {
    qualifiedName =
        fmt::format("{}.{}", parser.schema().value(), parser.table());
  } else {
    qualifiedName =
        fmt::format("{}.{}", defaultTpchNamespace(), parser.table());
  }
  return qualifiedName;
}

const Table* TpchConnectorMetadata::findTable(const std::string& name) {
  ensureInitialized();
  const auto qualifiedName = getQualifiedName(name);
  auto it = tables_.find(qualifiedName);
  if (it == tables_.end()) {
    return nullptr;
  }
  return it->second.get();
}

} // namespace facebook::velox::connector::tpch
