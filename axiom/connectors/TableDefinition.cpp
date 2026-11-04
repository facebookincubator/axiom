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
#include "axiom/connectors/TableDefinition.h"

#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::connector {

// static
TableDefinition TableDefinition::create(
    const velox::RowTypePtr& rowType,
    folly::F14FastMap<std::string, velox::Variant> properties) {
  TableDefinition definition{.properties = std::move(properties)};
  definition.columns.reserve(rowType->size());
  for (uint32_t i = 0; i < rowType->size(); ++i) {
    definition.columns.push_back(
        {.name = rowType->nameOf(i), .type = rowType->childAt(i)});
  }
  return definition;
}

velox::RowTypePtr TableDefinition::rowType() const {
  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;
  names.reserve(columns.size());
  types.reserve(columns.size());
  for (const auto& column : columns) {
    names.push_back(column.name);
    types.push_back(column.type);
  }
  return velox::ROW(std::move(names), std::move(types));
}

folly::F14FastMap<std::string, std::string> TableDefinition::columnComments()
    const {
  folly::F14FastMap<std::string, std::string> comments;
  for (const auto& column : columns) {
    if (column.comment.has_value()) {
      comments.emplace(column.name, column.comment.value());
    }
  }
  return comments;
}

void TableDefinitionSupport::check(const ColumnDefinition& column) const {
  if (!column.nullable) {
    VELOX_USER_CHECK(
        nullability,
        "NOT NULL is not supported by this connector: {}",
        column.name);
  }
  if (column.comment.has_value()) {
    VELOX_USER_CHECK(
        columnComments,
        "Column comments are not supported by this connector: {}",
        column.name);
  }
  if (!column.properties.empty()) {
    VELOX_USER_CHECK(
        columnProperties,
        "Column properties are not supported by this connector: {}",
        column.name);
  }
}

void TableDefinitionSupport::check(const VectorIndexDefinition& index) const {
  VELOX_USER_CHECK(
      vectorIndexes,
      "Vector indexes are not supported by this connector: {}",
      index.name);
}

void TableDefinitionSupport::check(const TableDefinition& definition) const {
  for (const auto& column : definition.columns) {
    check(column);
  }
  if (definition.comment.has_value()) {
    VELOX_USER_CHECK(
        tableComment, "Table comments are not supported by this connector");
  }
  if (!definition.primaryKey.empty()) {
    VELOX_USER_CHECK(
        primaryKey, "PRIMARY KEY is not supported by this connector");
  }
  for (const auto& index : definition.vectorIndexes) {
    check(index);
  }
}

} // namespace facebook::axiom::connector
