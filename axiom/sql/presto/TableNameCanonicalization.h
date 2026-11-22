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
#include <string_view>
#include <utility>

#include "axiom/common/SchemaTableName.h"
#include "axiom/sql/presto/ParserSession.h"
#include "axiom/sql/presto/ast/AstExpressions.h"

namespace axiom::sql::presto {

/// Builds connector table names from names as written, without
/// looking any table up.
class TableNameCanonicalization {
 public:
  TableNameCanonicalization() = delete;

  /// Returns true if 'schema' is information_schema, matched
  /// case-insensitively.
  static bool isInformationSchema(std::string_view schema);

  /// Returns 'identifier' as the catalog of 'connectorId' stores it
  /// (see connector::ConnectorMetadata::normalizeIdentifier()), or
  /// as written if no such connector is registered.
  static std::string normalizeIdentifier(
      const ParserSessionPtr& session,
      const std::string& connectorId,
      std::string_view identifier);

  /// Builds a connector table name from a name as written, filling
  /// a missing connector or schema from the parser defaults and
  /// canonicalizing the schema and table names as the connector
  /// stores them. No table is looked up.
  static std::pair<std::string, facebook::axiom::SchemaTableName>
  toConnectorTableInCatalog(
      const ParserSessionPtr& session,
      const QualifiedName& name,
      const std::string& defaultConnectorId,
      const std::string& defaultSchema);

  /// Builds a connector table name from a name as written, filling
  /// a missing connector or schema from the parser defaults and
  /// canonicalizing the schema and table names as the connector
  /// stores them. No table is looked up: an unregistered connector
  /// keeps the name as written.
  static std::pair<std::string, facebook::axiom::SchemaTableName>
  toConnectorTable(
      const ParserSessionPtr& session,
      const QualifiedName& name,
      const std::string& defaultConnectorId,
      const std::string& defaultSchema,
      std::string_view informationSchemaConnectorId);

  /// Builds a connector table name as toConnectorTable does,
  /// resolving information_schema through the connector it names
  /// by default.
  static std::pair<std::string, facebook::axiom::SchemaTableName>
  toConnectorTable(
      const ParserSessionPtr& session,
      const QualifiedName& name,
      const std::string& defaultConnectorId,
      const std::string& defaultSchema);
};

} // namespace axiom::sql::presto
