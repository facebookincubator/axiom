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
#include "axiom/connectors/system/InformationSchema.h"

#include <numeric>

#include <folly/CppAttributes.h>

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/system/SystemConnector.h"
#include "velox/core/Expressions.h"
#include "velox/expression/ExprToSubfieldFilter.h"
#include "velox/type/Filter.h"

namespace facebook::axiom::connector::system {

namespace {

velox::Variant nullVarchar() {
  return velox::Variant::null(velox::TypeKind::VARCHAR);
}

velox::Variant nullBigint() {
  return velox::Variant::null(velox::TypeKind::BIGINT);
}

// Number of digits a numeric type holds: the count its range allows for
// integers, the mantissa bits for floating point, and the declared precision
// for decimals. Null for every other type.
velox::Variant precisionOf(const velox::TypePtr& type) {
  if (type->isDecimal()) {
    return velox::Variant(
        static_cast<int64_t>(velox::getDecimalPrecisionScale(*type).first));
  }

  switch (type->kind()) {
    case velox::TypeKind::TINYINT:
      return velox::Variant(static_cast<int64_t>(3));
    case velox::TypeKind::SMALLINT:
      return velox::Variant(static_cast<int64_t>(5));
    case velox::TypeKind::INTEGER:
      return velox::Variant(static_cast<int64_t>(10));
    case velox::TypeKind::BIGINT:
      return velox::Variant(static_cast<int64_t>(19));
    case velox::TypeKind::REAL:
      return velox::Variant(static_cast<int64_t>(24));
    case velox::TypeKind::DOUBLE:
      return velox::Variant(static_cast<int64_t>(53));
    default:
      return nullBigint();
  }
}

velox::Variant scaleOf(const velox::TypePtr& type) {
  if (type->isDecimal()) {
    return velox::Variant(
        static_cast<int64_t>(velox::getDecimalPrecisionScale(*type).second));
  }
  return nullBigint();
}

// Marks how far a scan has read. Holds the schema and optional table being
// described, the table or view it resolved to or the virtual relation's type,
// and, for kColumns, the current column.
struct RowPosition {
  const InformationSchemaTableHandle* handle{nullptr};
  size_t schemaIndex{0};
  size_t tableIndex{0};
  size_t columnIndex{0};
  TablePtr table;
  ViewPtr view;
  velox::RowTypePtr informationSchemaTableType;

  // Spelling of the types kColumns reports. Never null while reading.
  const InformationSchema::TypeNameFormatter* typeName{nullptr};

  const std::string& catalog() const {
    return handle->catalog();
  }

  // Schemata uses names advertised by the catalog. Table-level scans use names
  // from the query because the scan consumes their filters.
  const std::string& schema() const {
    return handle->schemas()[schemaIndex];
  }

  const std::string& tableName() const {
    return handle->tables()[tableIndex];
  }

  // True once a named table has resolved.
  bool resolved() const {
    return table != nullptr || view != nullptr ||
        informationSchemaTableType != nullptr;
  }

  // True when the row describes a view. A view's columns come from its type,
  // with no connector column behind them.
  bool isView() const {
    return view != nullptr;
  }

  // The connector column kColumns is describing.
  const Column& column() const {
    VELOX_CHECK_NOT_NULL(
        table, "Only a catalog table has connector column metadata");
    const auto& name = rowType()->nameOf(columnIndex);
    const auto* found = table->findColumn(name);
    VELOX_CHECK_NOT_NULL(found, "Column not found: {}", name);
    return *found;
  }

  // Columns of the table being described.
  const velox::RowTypePtr& rowType() const {
    VELOX_CHECK(resolved(), "No table to describe");
    if (table != nullptr) {
      return table->type();
    }
    return view != nullptr ? view->type() : informationSchemaTableType;
  }
};

// A relation's column: its name and type, and how its value is read.
struct RelationColumn {
  std::string_view name;
  velox::TypePtr type;
  velox::Variant (*read)(const RowPosition&);
};

// Adapters matching RelationColumn::read for the columns every relation has.
velox::Variant catalogOf(const RowPosition& position) {
  return velox::Variant(position.catalog());
}

velox::Variant schemaOf(const RowPosition& position) {
  return velox::Variant(position.schema());
}

velox::Variant tableNameOf(const RowPosition& position) {
  return velox::Variant(position.tableName());
}

velox::Variant alwaysNull(const RowPosition& /*at*/) {
  return nullVarchar();
}

const std::vector<RelationColumn>& schemataColumns() {
  static const std::vector<RelationColumn> kRelation{
      {"catalog_name", velox::VARCHAR(), catalogOf},
      {"schema_name", velox::VARCHAR(), schemaOf},
  };
  return kRelation;
}

const std::vector<RelationColumn>& tablesColumns() {
  static const std::vector<RelationColumn> kRelation{
      {"table_catalog", velox::VARCHAR(), catalogOf},
      {"table_schema", velox::VARCHAR(), schemaOf},
      {"table_name", velox::VARCHAR(), tableNameOf},
      {"table_type",
       velox::VARCHAR(),
       [](const RowPosition& position) {
         return velox::Variant(
             std::string(
                 position.isView() ? InformationSchema::kViewType
                                   : InformationSchema::kBaseTableType));
       }},
  };
  return kRelation;
}

const std::vector<RelationColumn>& viewsColumns() {
  static const std::vector<RelationColumn> kRelation{
      {"table_catalog", velox::VARCHAR(), catalogOf},
      {"table_schema", velox::VARCHAR(), schemaOf},
      {"table_name", velox::VARCHAR(), tableNameOf},
      // Ownership is not tracked.
      {"view_owner", velox::VARCHAR(), alwaysNull},
      {"view_definition",
       velox::VARCHAR(),
       [](const RowPosition& position) {
         return velox::Variant(position.view->text());
       }},
  };
  return kRelation;
}

const std::vector<RelationColumn>& columnsColumns() {
  static const std::vector<RelationColumn> kRelation{
      {"table_catalog", velox::VARCHAR(), catalogOf},
      {"table_schema", velox::VARCHAR(), schemaOf},
      {"table_name", velox::VARCHAR(), tableNameOf},
      {"column_name",
       velox::VARCHAR(),
       [](const RowPosition& position) {
         return velox::Variant(
             position.rowType()->nameOf(position.columnIndex));
       }},
      {"ordinal_position",
       velox::BIGINT(),
       [](const RowPosition& position) {
         return velox::Variant(static_cast<int64_t>(position.columnIndex + 1));
       }},
      // Defaults apply to writes and are not reported.
      {"column_default", velox::VARCHAR(), alwaysNull},
      {"is_nullable",
       velox::VARCHAR(),
       [](const RowPosition& /*at*/) {
         // Nullability is not tracked, so every column reports as nullable.
         return velox::Variant(std::string("YES"));
       }},
      {"data_type",
       velox::VARCHAR(),
       [](const RowPosition& position) {
         return velox::Variant((*position.typeName)(
             *position.rowType()->childAt(position.columnIndex)));
       }},
      {"comment",
       velox::VARCHAR(),
       [](const RowPosition& position) {
         if (position.table == nullptr) {
           return nullVarchar();
         }
         const auto& comment = position.column().comment();
         return comment.has_value() ? velox::Variant(comment.value())
                                    : nullVarchar();
       }},
      {"extra_info",
       velox::VARCHAR(),
       [](const RowPosition& position) {
         if (position.table == nullptr) {
           return nullVarchar();
         }
         // The connector says what a column's role is, if anything.
         const auto& extraInfo = position.column().extraInfo();
         return extraInfo.has_value() ? velox::Variant(extraInfo.value())
                                      : nullVarchar();
       }},
      {"precision",
       velox::BIGINT(),
       [](const RowPosition& position) {
         return precisionOf(position.rowType()->childAt(position.columnIndex));
       }},
      {"scale",
       velox::BIGINT(),
       [](const RowPosition& position) {
         return scaleOf(position.rowType()->childAt(position.columnIndex));
       }},
      // A length is the declared width of a character type, which the type
      // system does not carry.
      {"length",
       velox::BIGINT(),
       [](const RowPosition& /*at*/) { return nullBigint(); }},
  };
  return kRelation;
}

// Returns the columns of the relation 'name', or nullptr if no relation goes
// by that name.
const std::vector<RelationColumn>* FOLLY_NULLABLE
findRelation(std::string_view name) {
  if (name == InformationSchema::kSchemata) {
    return &schemataColumns();
  }
  if (name == InformationSchema::kTables) {
    return &tablesColumns();
  }
  if (name == InformationSchema::kViews) {
    return &viewsColumns();
  }
  if (name == InformationSchema::kColumns) {
    return &columnsColumns();
  }
  return nullptr;
}

const std::vector<RelationColumn>& relationColumns(std::string_view name) {
  const auto* columns = findRelation(name);
  VELOX_CHECK_NOT_NULL(
      columns, "No such information_schema relation: {}", name);
  return *columns;
}

// Returns the values 'filter' restricts a column to, or std::nullopt if it
// does not restrict it to a finite set. Equality arrives as a single-value
// range and IN as a value set; a wider range, e.g. table_name > 't', names no
// tables to read and so leaves the query unbounded.
//
// Whether the filter also admits null is ignored: it is read only for
// table_schema and table_name, whose values are the names the query gave, so
// no row carries a null there and admitting one matches nothing more.
std::optional<std::vector<std::string>> filterValues(
    const velox::common::Filter& filter) {
  if (filter.kind() == velox::common::FilterKind::kAlwaysFalse) {
    // No value passes, so the query names no tables and reads nothing.
    return std::vector<std::string>{};
  }

  if (filter.kind() == velox::common::FilterKind::kBytesValues) {
    const auto& values =
        static_cast<const velox::common::BytesValues&>(filter).values();
    // The filter holds a set, whose order varies between processes. Sorting
    // keeps the handle and the plan text it prints stable.
    std::vector<std::string> sorted{values.begin(), values.end()};
    std::sort(sorted.begin(), sorted.end());
    return sorted;
  }

  if (filter.kind() == velox::common::FilterKind::kBytesRange) {
    const auto& range = static_cast<const velox::common::BytesRange&>(filter);
    if (range.isSingleValue()) {
      return std::vector<std::string>{range.lower()};
    }
  }

  return std::nullopt;
}

// Layout of an information_schema relation. Accepts the filters that name the
// tables to describe and rejects the rest, and fails a query those filters
// leave unbounded.
class InformationSchemaTableLayout : public TableLayout {
 public:
  InformationSchemaTableLayout(
      Table* table,
      velox::connector::Connector* connector,
      std::vector<const Column*> columns)
      : TableLayout(
            "default",
            table,
            connector,
            std::move(columns),
            /*partitionColumns=*/{},
            /*orderColumns=*/{},
            /*sortOrder=*/{},
            /*lookupKeys=*/{},
            /*supportsScan=*/true) {}

  bool supportsSampling() const override {
    return false;
  }

  // The rows come from catalog metadata, which only the coordinator can read.
  bool runsOnCoordinator() const override {
    return true;
  }

  velox::connector::ColumnHandlePtr createColumnHandle(
      const ConnectorSessionPtr& /*session*/,
      const std::string& columnName,
      std::vector<velox::common::Subfield> /*subfields*/,
      std::optional<velox::TypePtr> /*castToType*/,
      SubfieldMapping /*subfieldMapping*/) const override {
    return std::make_shared<SystemColumnHandle>(columnName);
  }

  velox::connector::ConnectorTableHandlePtr createTableHandle(
      const ConnectorSessionPtr& session,
      std::vector<velox::connector::ColumnHandlePtr> columnHandles,
      velox::core::ExpressionEvaluator& evaluator,
      std::vector<velox::core::TypedExprPtr> filters,
      std::vector<int32_t>& rejectedFilterIndices,
      velox::RowTypePtr dataColumns,
      std::optional<LookupKeys> lookupKeys) const override;
};

// An information_schema relation of one catalog.
class InformationSchemaTable : public Table {
 public:
  // 'serving' is the connector that serves the relations, not the catalog
  // being described.
  InformationSchemaTable(
      const SchemaTableName& tableName,
      const velox::RowTypePtr& schema,
      velox::connector::Connector* serving)
      : Table(tableName, Table::makeColumns(schema)) {
    layout_ = std::make_unique<InformationSchemaTableLayout>(
        this, serving, allColumns());
    layouts_.push_back(layout_.get());
  }

  const std::vector<const TableLayout*>& layouts() const override {
    return layouts_;
  }

  std::optional<uint64_t> numRows() const override {
    return std::nullopt;
  }

 private:
  std::vector<const TableLayout*> layouts_;
  std::unique_ptr<InformationSchemaTableLayout> layout_;
};

velox::connector::ConnectorTableHandlePtr
InformationSchemaTableLayout::createTableHandle(
    const ConnectorSessionPtr& session,
    std::vector<velox::connector::ColumnHandlePtr> /*columnHandles*/,
    velox::core::ExpressionEvaluator& evaluator,
    std::vector<velox::core::TypedExprPtr> filters,
    std::vector<int32_t>& rejectedFilterIndices,
    velox::RowTypePtr /*dataColumns*/,
    std::optional<LookupKeys> lookupKeys) const {
  VELOX_USER_CHECK(
      !lookupKeys.has_value(),
      "information_schema does not support index lookups");

  // Take filters on these columns: they decide how much metadata the scan
  // reads. Reject every other filter; it only removes rows the scan would
  // produce anyway, so the caller applies it above the scan.
  constexpr std::string_view kTableSchemaColumn = "table_schema";
  constexpr std::string_view kTableNameColumn = "table_name";

  const auto& relation = table().name().table;
  const auto catalog = InformationSchema::catalog(table().name().schema);
  const auto metadata =
      ConnectorMetadataRegistry::get(std::string{catalog.value()});
  if (relation == InformationSchema::kSchemata) {
    // The rows come from listSchemaNames(), which takes no schema-name
    // argument, so a pushed filter would not narrow what the scan reads. Every
    // filter is rejected and applied above the scan.
    rejectedFilterIndices.resize(filters.size());
    std::iota(rejectedFilterIndices.begin(), rejectedFilterIndices.end(), 0);

    auto schemas = InformationSchema::listedSchemaNames(*metadata, session);
    return std::make_shared<InformationSchemaTableHandle>(
        connectorId(),
        table().name(),
        std::move(schemas),
        std::vector<std::string>{});
  }

  std::optional<std::vector<std::string>> schemas;
  std::optional<std::vector<std::string>> tables;
  const auto& parser = velox::exec::ExprToSubfieldFilterParser::getInstance();
  for (size_t i = 0; i < filters.size(); ++i) {
    auto subfieldFilter = filters[i]->isCallKind()
        ? parser->leafCallToSubfieldFilter(
              *filters[i]->asUnchecked<velox::core::CallTypedExpr>(),
              &evaluator)
        : std::nullopt;

    if (!subfieldFilter.has_value()) {
      rejectedFilterIndices.push_back(static_cast<int32_t>(i));
      continue;
    }

    const std::string columnName = subfieldFilter->first.toString();
    const bool filtersSchema = columnName == kTableSchemaColumn;
    const bool filtersTable = columnName == kTableNameColumn;
    if (!filtersSchema && !filtersTable) {
      rejectedFilterIndices.push_back(static_cast<int32_t>(i));
      continue;
    }

    // A column named by two conjuncts is taken from the first; the rest
    // narrow the rows further, which the caller does above the scan.
    auto& pinned = filtersSchema ? schemas : tables;
    if (pinned.has_value()) {
      rejectedFilterIndices.push_back(static_cast<int32_t>(i));
      continue;
    }

    auto values = filterValues(*subfieldFilter->second);
    if (!values.has_value()) {
      rejectedFilterIndices.push_back(static_cast<int32_t>(i));
      continue;
    }

    pinned = std::move(values);
  }

  VELOX_USER_CHECK(
      schemas.has_value(),
      "Querying information_schema.{} requires a filter naming table_schema, e.g. table_schema = 's'",
      relation);
  VELOX_USER_CHECK(
      tables.has_value(),
      "Querying information_schema.{} requires a filter naming table_name, e.g. table_name = 't'",
      relation);

  // A schema named by a filter but absent from the catalog contributes no rows.
  // Dropping it here keeps the scan from asking, which a catalog may answer
  // with an error.
  std::vector<std::string> existing;
  for (auto& schema : schemas.value()) {
    if (schema == InformationSchema::kInformationSchema ||
        metadata->schemaExists(session, schema)) {
      existing.push_back(std::move(schema));
    }
  }

  return std::make_shared<InformationSchemaTableHandle>(
      connectorId(),
      table().name(),
      std::move(existing),
      std::move(tables).value());
}

// Reads the rows of an information_schema relation from the metadata of the
// catalog its handle names.
class InformationSchemaDataSource : public velox::connector::DataSource {
 public:
  InformationSchemaDataSource(
      const velox::RowTypePtr& outputType,
      std::shared_ptr<const InformationSchemaTableHandle> tableHandle,
      const velox::connector::ColumnHandleMap& columnHandles,
      velox::memory::MemoryPool* pool,
      const InformationSchema::TypeNameFormatter& typeName)
      : outputType_{outputType},
        tableHandle_{std::move(tableHandle)},
        relation_{relationColumns(tableHandle_->relation())},
        metadata_{ConnectorMetadataRegistry::get(tableHandle_->catalog())},
        outputColumns_{findOutputColumns(outputType, columnHandles)},
        position_{.handle = tableHandle_.get(), .typeName = &typeName},
        pool_{pool} {}

  void addSplit(
      std::shared_ptr<velox::connector::ConnectorSplit> split) override {
    VELOX_CHECK_NOT_NULL(split);
    hasSplit_ = true;
  }

  std::optional<velox::RowVectorPtr> next(
      uint64_t size,
      velox::ContinueFuture& future) override;

  void addDynamicFilter(
      velox::column_index_t,
      const std::shared_ptr<velox::common::Filter>&) override {
    VELOX_UNSUPPORTED("information_schema does not support dynamic filters");
  }

  uint64_t getCompletedBytes() override {
    return 0;
  }

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  std::unordered_map<std::string, velox::RuntimeMetric> getRuntimeStats()
      override {
    return {};
  }

 private:
  // The relation's columns a query selects, in output order. A query that
  // selects none, e.g. count(*), still describes each named table but reads
  // nothing from it.
  std::vector<const RelationColumn*> findOutputColumns(
      const velox::RowTypePtr& outputType,
      const velox::connector::ColumnHandleMap& columnHandles) const;

  // Moves to the next row and returns false when the relation is exhausted. A
  // named table that does not exist contributes no rows.
  bool advance();

  // Advances to the next table to describe, resolving it in the catalog.
  // Returns false when there are none left.
  bool nextTable();

  const velox::RowTypePtr outputType_;
  const std::shared_ptr<const InformationSchemaTableHandle> tableHandle_;
  const std::vector<RelationColumn>& relation_;
  const std::shared_ptr<ConnectorMetadata> metadata_;
  const std::vector<const RelationColumn*> outputColumns_;

  RowPosition position_;
  bool visitedSchema_{false};
  // Whether a named table has been visited, so the next one is the one after
  // it rather than the first.
  bool visitedTable_{false};
  bool exhausted_{false};

  velox::memory::MemoryPool* const pool_;
  bool hasSplit_{false};
  uint64_t completedRows_{0};
};

std::vector<const RelationColumn*>
InformationSchemaDataSource::findOutputColumns(
    const velox::RowTypePtr& outputType,
    const velox::connector::ColumnHandleMap& columnHandles) const {
  std::vector<const RelationColumn*> columns;
  columns.reserve(outputType->size());
  for (const auto& outputName : outputType->names()) {
    // An output column names the relation's column through its handle.
    const auto handle = columnHandles.find(outputName);
    VELOX_CHECK(
        handle != columnHandles.end(),
        "No column handle for output column: {}",
        outputName);
    const auto& name = handle->second->name();
    const auto it = std::find_if(
        relation_.begin(), relation_.end(), [&](const RelationColumn& column) {
          return column.name == name;
        });
    VELOX_CHECK(
        it != relation_.end(),
        "No such column in information_schema.{}: {}",
        tableHandle_->relation(),
        name);
    columns.push_back(&*it);
  }
  return columns;
}

bool InformationSchemaDataSource::nextTable() {
  const auto& schemas = tableHandle_->schemas();
  const auto& tables = tableHandle_->tables();

  if (visitedTable_) {
    ++position_.tableIndex;
    if (position_.tableIndex == tables.size()) {
      position_.tableIndex = 0;
      ++position_.schemaIndex;
    }
  }

  for (; position_.schemaIndex < schemas.size(); ++position_.schemaIndex) {
    for (; position_.tableIndex < tables.size(); ++position_.tableIndex) {
      const SchemaTableName name{
          schemas[position_.schemaIndex], tables[position_.tableIndex]};
      position_.table.reset();
      position_.view.reset();
      position_.informationSchemaTableType.reset();
      if (name.schema == InformationSchema::kInformationSchema) {
        position_.informationSchemaTableType =
            InformationSchema::tableSchema(name.table);
      } else {
        position_.table = metadata_->findTable(name);
        position_.view =
            position_.table == nullptr ? metadata_->findView(name) : nullptr;
      }
      position_.columnIndex = 0;
      visitedTable_ = true;

      // A table with no columns contributes no rows to 'columns'.
      if (position_.resolved() &&
          (tableHandle_->relation() != InformationSchema::kColumns ||
           position_.rowType()->size() > 0)) {
        return true;
      }
    }
    position_.tableIndex = 0;
  }

  return false;
}

bool InformationSchemaDataSource::advance() {
  if (exhausted_) {
    return false;
  }

  const auto& relation = tableHandle_->relation();

  if (relation == InformationSchema::kSchemata) {
    if (visitedSchema_) {
      ++position_.schemaIndex;
    }
    visitedSchema_ = true;
    if (position_.schemaIndex < tableHandle_->schemas().size()) {
      return true;
    }
    exhausted_ = true;
    return false;
  }

  if (relation == InformationSchema::kColumns && position_.resolved() &&
      ++position_.columnIndex < position_.rowType()->size()) {
    // A table contributes a row per column, so the source stays on it.
    return true;
  }

  // 'views' describes views only, so a table in the list is stepped over.
  do {
    if (!nextTable()) {
      exhausted_ = true;
      return false;
    }
  } while (relation == InformationSchema::kViews && position_.view == nullptr);

  return true;
}

std::optional<velox::RowVectorPtr> InformationSchemaDataSource::next(
    uint64_t size,
    velox::ContinueFuture& /*future*/) {
  VELOX_CHECK(hasSplit_, "No split added");

  std::vector<std::vector<velox::Variant>> columns(outputColumns_.size());
  for (auto& column : columns) {
    column.reserve(size);
  }

  velox::vector_size_t numRows = 0;
  for (uint64_t i = 0; i < size && advance(); ++i) {
    for (size_t column = 0; column < outputColumns_.size(); ++column) {
      columns[column].push_back(outputColumns_[column]->read(position_));
    }
    ++numRows;
  }

  if (numRows == 0) {
    return nullptr;
  }

  std::vector<velox::VectorPtr> children;
  children.reserve(columns.size());
  for (size_t column = 0; column < columns.size(); ++column) {
    children.push_back(
        velox::BaseVector::createFromVariants(
            outputType_->childAt(column), columns[column], pool_));
  }

  completedRows_ += numRows;
  return std::make_shared<velox::RowVector>(
      pool_, outputType_, nullptr, numRows, std::move(children));
}

} // namespace

std::string InformationSchema::schemaName(std::string_view catalog) {
  return fmt::format("{}{}", kPrefix, catalog);
}

std::optional<std::string_view> InformationSchema::catalog(
    std::string_view schemaName) {
  if (!schemaName.starts_with(kPrefix)) {
    return std::nullopt;
  }

  const auto catalog = schemaName.substr(kPrefix.size());
  if (catalog.empty()) {
    return std::nullopt;
  }
  return catalog;
}

std::vector<std::string> InformationSchema::listedSchemaNames(
    ConnectorMetadata& metadata,
    const ConnectorSessionPtr& session) {
  auto schemas = metadata.listSchemaNames(session);
  // A catalog may already include the virtual schema in its own list.
  if (std::find(schemas.begin(), schemas.end(), kInformationSchema) ==
      schemas.end()) {
    schemas.emplace_back(kInformationSchema);
  }
  return schemas;
}

const std::vector<std::string>& InformationSchema::tableNames() {
  static const std::vector<std::string> kNames{
      std::string{InformationSchema::kColumns},
      std::string{InformationSchema::kSchemata},
      std::string{InformationSchema::kTables},
      std::string{InformationSchema::kViews}};
  return kNames;
}

const velox::RowTypePtr& InformationSchema::tableSchema(
    std::string_view tableName) {
  static const velox::RowTypePtr kNone;
  static const folly::F14FastMap<std::string_view, velox::RowTypePtr> kSchemas =
      [] {
        folly::F14FastMap<std::string_view, velox::RowTypePtr> schemas;
        for (const auto& name : InformationSchema::tableNames()) {
          std::vector<std::string> names;
          std::vector<velox::TypePtr> types;
          for (const auto& column : relationColumns(name)) {
            names.emplace_back(column.name);
            types.push_back(column.type);
          }
          schemas.emplace(name, velox::ROW(std::move(names), std::move(types)));
        }
        return schemas;
      }();

  const auto it = kSchemas.find(tableName);
  return it == kSchemas.end() ? kNone : it->second;
}

namespace {
std::string catalogOrFail(std::string_view schemaName) {
  auto catalog = InformationSchema::catalog(schemaName);
  VELOX_CHECK(
      catalog.has_value(), "Not an information_schema name: {}", schemaName);
  return std::string{catalog.value()};
}
} // namespace

InformationSchemaTableHandle::InformationSchemaTableHandle(
    const std::string& connectorId,
    SchemaTableName schemaTableName,
    std::vector<std::string> schemas,
    std::vector<std::string> tables)
    : ConnectorTableHandle(connectorId),
      schemaTableName_{std::move(schemaTableName)},
      catalog_{catalogOrFail(schemaTableName_.schema)},
      qualifiedName_{fmt::format(
          "{}.{}",
          schemaTableName_.schema,
          schemaTableName_.table)},
      schemas_{std::move(schemas)},
      tables_{std::move(tables)} {}

folly::dynamic InformationSchemaTableHandle::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = InformationSchemaTableHandle::getClassName();
  obj["connectorId"] = connectorId();
  obj["schemaName"] = schemaTableName_.schema;
  obj["tableName"] = schemaTableName_.table;
  obj["schemas"] = folly::dynamic::array(schemas_.begin(), schemas_.end());
  obj["tables"] = folly::dynamic::array(tables_.begin(), tables_.end());

  return obj;
}

velox::connector::ConnectorTableHandlePtr InformationSchemaTableHandle::create(
    const folly::dynamic& obj,
    void* /*context*/) {
  const auto toStrings = [](const folly::dynamic& array) {
    std::vector<std::string> values;
    values.reserve(array.size());
    for (const auto& value : array) {
      values.push_back(value.asString());
    }
    return values;
  };

  return std::make_shared<InformationSchemaTableHandle>(
      obj["connectorId"].asString(),
      SchemaTableName{
          obj["schemaName"].asString(), obj["tableName"].asString()},
      toStrings(obj["schemas"]),
      toStrings(obj["tables"]));
}

TablePtr InformationSchema::findTable(
    const SchemaTableName& tableName,
    velox::connector::Connector* serving) {
  if (!catalog(tableName.schema).has_value()) {
    return nullptr;
  }

  const auto& schema = tableSchema(tableName.table);
  if (schema == nullptr) {
    return nullptr;
  }

  // A catalog nobody registered describes nothing, which reads as no such
  // table rather than a failure when the scan starts.
  if (ConnectorMetadataRegistry::tryGet(
          std::string{catalog(tableName.schema).value()}) == nullptr) {
    return nullptr;
  }

  return std::make_shared<InformationSchemaTable>(tableName, schema, serving);
}

std::string InformationSchema::defaultTypeName(const velox::Type& type) {
  return type.toString();
}

std::unique_ptr<velox::connector::DataSource> InformationSchema::makeDataSource(
    const std::shared_ptr<const InformationSchemaTableHandle>& tableHandle,
    const velox::RowTypePtr& outputType,
    const velox::connector::ColumnHandleMap& columnHandles,
    velox::memory::MemoryPool* pool,
    const TypeNameFormatter& typeName) {
  return std::make_unique<InformationSchemaDataSource>(
      outputType, tableHandle, columnHandles, pool, typeName);
}

} // namespace facebook::axiom::connector::system
