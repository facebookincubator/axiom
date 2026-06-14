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

#include "axiom/connectors/file/core/FileTable.h"

#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::connector::file {

using namespace facebook::velox;

std::pair<std::string, std::string> FileTable::parseName(
    const std::string& tableName) {
  auto pos = tableName.rfind('$');
  if (pos != std::string::npos && pos > 0) {
    auto suffix = tableName.substr(pos + 1);
    if (suffix.find('/') == std::string::npos) {
      return {tableName.substr(0, pos), std::move(suffix)};
    }
  }
  return {tableName, ""};
}

FileTableLayout::FileTableLayout(
    Table* table,
    velox::connector::Connector* connector,
    std::vector<const Column*> columns,
    std::string filePath,
    std::string suffix)
    : TableLayout(
          table->name().schema,
          table,
          connector,
          std::move(columns),
          /*partitionColumns=*/{},
          /*orderColumns=*/{},
          /*sortOrder=*/{},
          /*lookupKeys=*/{},
          /*supportsScan=*/true),
      filePath_(std::move(filePath)),
      suffix_(std::move(suffix)) {}

velox::connector::ColumnHandlePtr FileTableLayout::createColumnHandle(
    const ConnectorSessionPtr& /*session*/,
    const std::string& columnName,
    std::vector<velox::common::Subfield> /*subfields*/,
    std::optional<TypePtr> /*castToType*/,
    SubfieldMapping /*subfieldMapping*/) const {
  auto column = findColumn(columnName);
  VELOX_CHECK_NOT_NULL(column, "Column not found: {}", columnName);
  return std::make_shared<FileColumnHandle>(columnName);
}

velox::connector::ConnectorTableHandlePtr FileTableLayout::createTableHandle(
    const ConnectorSessionPtr& /*session*/,
    std::vector<velox::connector::ColumnHandlePtr> columnHandles,
    core::ExpressionEvaluator& /*evaluator*/,
    std::vector<core::TypedExprPtr> filters,
    std::vector<core::TypedExprPtr>& rejectedFilters,
    RowTypePtr /*dataColumns*/,
    std::optional<LookupKeys> /*lookupKeys*/) const {
  rejectedFilters = std::move(filters);
  return std::make_shared<FileTableHandle>(
      connectorId(),
      table().name(),
      filePath_,
      suffix_,
      table().type(),
      std::move(columnHandles));
}

FileTable::FileTable(
    const SchemaTableName& tableName,
    const RowTypePtr& schema,
    velox::connector::Connector* connector,
    std::string filePath,
    std::string suffix)
    : Table(tableName, makeColumns(schema)) {
  layout_ = std::make_unique<FileTableLayout>(
      this, connector, allColumns(), std::move(filePath), std::move(suffix));
  layouts_.push_back(layout_.get());
}

} // namespace facebook::axiom::connector::file
