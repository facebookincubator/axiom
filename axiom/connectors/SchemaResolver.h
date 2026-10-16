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

#pragma once

#include <string>

#include "axiom/connectors/ConnectorContext.h"
#include "axiom/connectors/ConnectorMetadata.h"

namespace facebook::axiom::connector {

class SchemaResolver {
 public:
  /// Constructs a SchemaResolver scoped to the given query context, with an
  /// optional default schema.
  explicit SchemaResolver(
      ConnectorContextPtr context,
      std::string defaultSchema = "");

  virtual ~SchemaResolver() = default;

  /// Adds a table that hasn't been committed yet to the schema. Used to
  /// register target table for CREATE TABLE AS SELECT queries. Can be called
  /// only once.
  void setTargetTable(
      const std::string& connectorId,
      const SchemaTableName& tableName,
      TablePtr table);

  /// Resolves a table name to a Table, or nullptr if the table doesn't exist.
  /// Throws a user error if the catalog has no registered metadata.
  virtual TablePtr findTable(
      const std::string& connectorId,
      const SchemaTableName& tableName) const;

  /// Resolves connector metadata in this resolver's catalog scope.
  std::shared_ptr<ConnectorMetadata> findMetadata(
      std::string_view connectorId) const;

  /// Returns the query context that scopes this resolver.
  const ConnectorContextPtr& context() const {
    return context_;
  }

 private:
  const ConnectorContextPtr context_;
  const std::string defaultSchema_;

  std::string targetConnectorId_;
  SchemaTableName targetTableName_;
  TablePtr targetTable_;
};

} // namespace facebook::axiom::connector
