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

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/connectors/ConnectorMetadataRegistry.h"

namespace facebook::axiom::connector {

class SchemaResolver {
 public:
  using Registry = ConnectorMetadataRegistry::Registry;

  /// Constructs a SchemaResolver scoped to the given registry, with an
  /// optional default schema.
  explicit SchemaResolver(
      const Registry& registry,
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
  /// If the table name has no schema, defaultSchema is used. If a connector
  /// for the specified catalog doesn't exist, an error will be returned.
  virtual TablePtr findTable(
      const std::string& connectorId,
      const SchemaTableName& tableName) const;

 private:
  // Reference member: SchemaResolver is intentionally non-assignable and
  // non-movable. The referenced Registry must outlive this resolver. Callers
  // typically construct one resolver per query.
  const Registry& registry_;
  const std::string defaultSchema_;

  std::string targetConnectorId_;
  SchemaTableName targetTableName_;
  TablePtr targetTable_;
};

} // namespace facebook::axiom::connector
