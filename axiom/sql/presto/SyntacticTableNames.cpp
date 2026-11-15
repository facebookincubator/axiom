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

#include "axiom/sql/presto/SyntacticTableNames.h"

#include <any>
#include <iostream>
#include <sstream>

#include <boost/algorithm/string/predicate.hpp>

#include "axiom/connectors/system/InformationSchema.h"
#include "axiom/sql/presto/ExpressionPlanner.h"
#include "axiom/sql/presto/PrestoParser.h"
#include "axiom/sql/presto/PrestoSqlError.h"
#include "axiom/sql/presto/RecursiveCteValidator.h"
#include "axiom/sql/presto/TableNameCanonicalization.h"
#include "axiom/sql/presto/ast/AstBuilder.h"
#include "axiom/sql/presto/ast/AstPrinter.h"
#include "axiom/sql/presto/ast/DefaultTraversalVisitor.h"
#include "axiom/sql/presto/grammar/SyntaxParser.h"

namespace axiom::sql::presto {
namespace {

// Names tables from an AST without resolving them.
class SyntacticTableVisitor : public DefaultTraversalVisitor {
 public:
  SyntacticTableVisitor(
      ParserSessionPtr session,
      std::string_view defaultConnectorId,
      std::string_view defaultSchema)
      : session_{std::move(session)},
        defaultConnectorId_{defaultConnectorId},
        defaultSchema_{defaultSchema} {}

  SyntacticTableNames take(std::optional<SqlStatementKind> kind) && {
    return SyntacticTableNames{
        std::move(rowInputTables_),
        std::move(rowOutputTable_),
        std::move(definitionInputTables_),
        std::move(definitionOutputTable_),
        std::move(kind),
    };
  }

 protected:
  void visitQuery(Query* node) override {
    scopes_.emplace_back();
    DefaultTraversalVisitor::visitQuery(node);
    scopes_.pop_back();
  }

  void visitWith(With* node) override {
    recursiveScopes_.push_back(node->isRecursive());
    DefaultTraversalVisitor::visitWith(node);
    recursiveScopes_.pop_back();
  }

  void visitWithQuery(WithQuery* node) override {
    // A CTE is recursive only when its enclosing WITH is recursive
    // and its body references its own name. A recursive alias is
    // visible inside its own query. A non-recursive alias becomes
    // visible only after its query, so a table the CTE reads is
    // named even when it shares the alias name.
    const auto canonicalName = canonicalizeName(node->name()->value());
    const bool recursive = !recursiveScopes_.empty() &&
        recursiveScopes_.back() &&
        RecursiveCteValidator::referencesCte(*node->query(), canonicalName);
    VELOX_CHECK(
        !scopes_.empty(), "WITH query without an enclosing query scope");
    if (recursive) {
      scopes_.back().insert(canonicalName);
    }
    DefaultTraversalVisitor::visitWithQuery(node);
    if (!recursive) {
      scopes_.back().insert(canonicalName);
    }
  }

  void visitTable(Table* node) override {
    if (isCteReference(*node->name())) {
      return;
    }
    rowInputTables_.insert(constructTableName(*node->name()));
    DefaultTraversalVisitor::visitTable(node);
  }

  void visitGraphElementTable(GraphElementTable* node) override {
    if (!isCteReference(*node->table())) {
      rowInputTables_.insert(constructTableName(*node->table()));
    }
    DefaultTraversalVisitor::visitGraphElementTable(node);
  }

  void visitInsert(Insert* node) override {
    setRowOutputTable(*node->target());
    DefaultTraversalVisitor::visitInsert(node);
  }

  void visitCreateTableAsSelect(CreateTableAsSelect* node) override {
    setRowOutputTable(*node->name());
    DefaultTraversalVisitor::visitCreateTableAsSelect(node);
  }

  void visitDelete(Delete* node) override {
    rowInputTables_.insert(constructTableName(*node->table()));
    setRowOutputTable(*node->table());
    DefaultTraversalVisitor::visitDelete(node);
  }

  void visitCreateTable(CreateTable* node) override {
    setDefinitionOutputTable(*node->name());
    DefaultTraversalVisitor::visitCreateTable(node);
  }

  void visitLikeClause(LikeClause* node) override {
    definitionInputTables_.insert(constructTableName(*node->tableName()));
  }

  void visitDropTable(DropTable* node) override {
    setDefinitionOutputTable(*node->tableName());
    DefaultTraversalVisitor::visitDropTable(node);
  }

  void visitAddVectorIndex(AddVectorIndex* node) override {
    setDefinitionOutputTable(*node->tableName());
    DefaultTraversalVisitor::visitAddVectorIndex(node);
  }

  void visitDropVectorIndex(DropVectorIndex* node) override {
    setDefinitionOutputTable(*node->tableName());
    DefaultTraversalVisitor::visitDropVectorIndex(node);
  }

  void visitRefreshVectorIndex(RefreshVectorIndex* node) override {
    rowInputTables_.insert(constructTableName(*node->tableName()));
    setRowOutputTable(*node->tableName());
    DefaultTraversalVisitor::visitRefreshVectorIndex(node);
  }

  void visitAddColumn(AddColumn* node) override {
    setDefinitionOutputTable(*node->tableName());
    DefaultTraversalVisitor::visitAddColumn(node);
  }

  void visitShowColumns(ShowColumns* node) override {
    definitionInputTables_.insert(constructTableName(*node->table()));
    DefaultTraversalVisitor::visitShowColumns(node);
  }

  void visitShowCreateTable(ShowCreateTable* node) override {
    definitionInputTables_.insert(constructTableName(*node->name()));
    DefaultTraversalVisitor::visitShowCreateTable(node);
  }

  void visitShowCreateView(ShowCreateView* node) override {
    definitionInputTables_.insert(constructTableName(*node->name()));
    DefaultTraversalVisitor::visitShowCreateView(node);
  }

  void visitShowStats(ShowStats* node) override {
    definitionInputTables_.insert(constructTableName(*node->table()));
    DefaultTraversalVisitor::visitShowStats(node);
  }

 private:
  // Returns true if a name is a single-part reference to a CTE alias
  // visible here.
  bool isCteReference(const QualifiedName& name) const {
    const auto& parts = name.parts();
    if (parts.size() != 1) {
      return false;
    }
    const auto canonicalName = canonicalizeName(parts[0]);
    for (const auto& scope : scopes_) {
      if (scope.count(canonicalName) > 0) {
        return true;
      }
    }
    return false;
  }

  facebook::axiom::CatalogSchemaTableName constructTableName(
      const QualifiedName& name) const {
    const auto& parts = name.parts();
    AXIOM_PRESTO_SYNTAX_CHECK(
        !parts.empty() && parts.size() <= 3,
        name.location(),
        name.suffix(),
        "Table name must have 1-3 components: {}",
        name.fullyQualifiedName());
    // Resolution here is name canonicalization only. No table is
    // looked up: an unregistered connector keeps the name as written.
    const auto [connectorId, connectorTable] =
        TableNameCanonicalization::toConnectorTable(
            session_,
            name,
            std::string(defaultConnectorId_),
            std::string(defaultSchema_),
            session_->options().informationSchemaConnectorId);
    return {connectorId, connectorTable};
  }

  void setRowOutputTable(const QualifiedName& name) {
    VELOX_CHECK(
        !rowOutputTable_.has_value(),
        "A statement names more than one row output table: {}",
        name.fullyQualifiedName());
    rowOutputTable_ = constructTableName(name);
  }

  void setDefinitionOutputTable(const QualifiedName& name) {
    VELOX_CHECK(
        !definitionOutputTable_.has_value(),
        "A statement names more than one definition output table: {}",
        name.fullyQualifiedName());
    definitionOutputTable_ = constructTableName(name);
  }

  // Session supplying parser options and the connector registry
  // for name canonicalization. No table is looked up through it.
  ParserSessionPtr session_;

  // Connector used for a table name with no catalog. Borrowed from
  // the parser, which outlives this visitor.
  std::string_view defaultConnectorId_;

  // Schema used for a table name with no schema. Borrowed from the
  // parser, which outlives this visitor.
  std::string_view defaultSchema_;

  // Aliases of the CTEs visible in each Query currently traversed,
  // innermost last. visitQuery pushes a scope for its WITH clause.
  std::vector<std::unordered_set<std::string>> scopes_;

  // Whether each WITH clause currently traversed is recursive.
  std::vector<bool> recursiveScopes_;

  // Tables whose rows the statement reads.
  std::unordered_set<facebook::axiom::CatalogSchemaTableName> rowInputTables_;

  // Table whose rows the statement writes, if any.
  std::optional<facebook::axiom::CatalogSchemaTableName> rowOutputTable_;

  // Tables whose definition alone the statement reads.
  std::unordered_set<facebook::axiom::CatalogSchemaTableName>
      definitionInputTables_;

  // Table whose definition alone the statement changes, if any.
  std::optional<facebook::axiom::CatalogSchemaTableName> definitionOutputTable_;
};

// Returns the planner kind for a statement form that has one, or
// nothing when it has no planner kind.
std::optional<SqlStatementKind> statementKindFor(const Statement& statement) {
  const auto type = statement.type();
  if (type == NodeType::kQuery || type == NodeType::kShowSchemas ||
      type == NodeType::kShowTables || type == NodeType::kShowCatalogs ||
      type == NodeType::kShowCreate || type == NodeType::kShowCreateView ||
      type == NodeType::kShowColumns || type == NodeType::kShowStats ||
      type == NodeType::kShowFunctions) {
    return SqlStatementKind::kSelect;
  }
  if (type == NodeType::kCreateTable) {
    return SqlStatementKind::kCreateTable;
  }
  if (type == NodeType::kCreateTableAsSelect) {
    return SqlStatementKind::kCreateTableAsSelect;
  }
  if (type == NodeType::kInsert) {
    return SqlStatementKind::kInsert;
  }
  if (type == NodeType::kRefreshVectorIndex) {
    return SqlStatementKind::kRefreshVectorIndex;
  }
  if (type == NodeType::kDelete) {
    return SqlStatementKind::kDelete;
  }
  if (type == NodeType::kDropTable) {
    return SqlStatementKind::kDropTable;
  }
  if (type == NodeType::kAddVectorIndex) {
    return SqlStatementKind::kAddVectorIndex;
  }
  if (type == NodeType::kDropVectorIndex) {
    return SqlStatementKind::kDropVectorIndex;
  }
  if (type == NodeType::kCreateSchema) {
    return SqlStatementKind::kCreateSchema;
  }
  if (type == NodeType::kDropSchema) {
    return SqlStatementKind::kDropSchema;
  }
  if (type == NodeType::kExplain) {
    return SqlStatementKind::kExplain;
  }
  if (type == NodeType::kShowStatsForQuery) {
    return SqlStatementKind::kShowStatsForQuery;
  }
  if (type == NodeType::kShowSession) {
    return SqlStatementKind::kShowSession;
  }
  if (type == NodeType::kSetSession) {
    return SqlStatementKind::kSetSession;
  }
  if (type == NodeType::kResetSession) {
    return SqlStatementKind::kResetSession;
  }
  if (type == NodeType::kUse) {
    return SqlStatementKind::kUse;
  }
  if (type == NodeType::kAddColumn) {
    return SqlStatementKind::kAddColumn;
  }
  if (type == NodeType::kCall) {
    return SqlStatementKind::kCall;
  }
  return std::nullopt;
}

} // namespace

// A catalog's information_schema relations are served by another connector,
// so a name whose schema is information_schema resolves there.
bool TableNameCanonicalization::isInformationSchema(std::string_view schema) {
  return boost::iequals(
      schema,
      facebook::axiom::connector::system::InformationSchema::
          kInformationSchema);
}

std::string TableNameCanonicalization::normalizeIdentifier(
    const ParserSessionPtr& session,
    const std::string& connectorId,
    std::string_view identifier) {
  const auto metadata = session->tryConnectorMetadata(connectorId);
  return metadata != nullptr ? metadata->normalizeIdentifier(identifier)
                             : std::string(identifier);
}

std::pair<std::string, facebook::axiom::SchemaTableName>
TableNameCanonicalization::toConnectorTableInCatalog(
    const ParserSessionPtr& session,
    const QualifiedName& name,
    const std::string& defaultConnectorId,
    const std::string& defaultSchema) {
  const auto& parts = name.parts();
  VELOX_CHECK(!parts.empty(), "Table name cannot be empty");
  VELOX_CHECK_LE(parts.size(), 3);

  // [[connector.]schema.]name
  const auto& connectorId = parts.size() == 3 ? parts[0] : defaultConnectorId;
  const auto& schema =
      parts.size() >= 2 ? parts[parts.size() - 2] : defaultSchema;
  return {
      connectorId,
      {normalizeIdentifier(session, connectorId, schema),
       normalizeIdentifier(session, connectorId, parts.back())}};
}

// The information_schema relations describe a catalog but are served by one
// connector, so '<catalog>.information_schema.<relation>' resolves to
// '<that connector>."$info_schema@<catalog>".<relation>'. The catalog travels
// in the schema, which is where the relation reads its metadata from.
std::pair<std::string, facebook::axiom::SchemaTableName>
TableNameCanonicalization::toConnectorTable(
    const ParserSessionPtr& session,
    const QualifiedName& name,
    const std::string& defaultConnectorId,
    const std::string& defaultSchema,
    std::string_view informationSchemaConnectorId) {
  auto [connectorId, table] = toConnectorTableInCatalog(
      session, name, defaultConnectorId, defaultSchema);

  if (!isInformationSchema(table.schema)) {
    return {std::move(connectorId), std::move(table)};
  }

  return {
      std::string(informationSchemaConnectorId),
      {facebook::axiom::connector::system::InformationSchema::schemaName(
           connectorId),
       std::move(table.table)}};
}

// Statement paths that do not carry the session's options resolve
// information_schema through the connector it names by default. The id is a
// deployment-wide setting, so a session that overrides it changes where a
// SELECT resolves, not where DESCRIBE does.
std::pair<std::string, facebook::axiom::SchemaTableName>
TableNameCanonicalization::toConnectorTable(
    const ParserSessionPtr& session,
    const QualifiedName& name,
    const std::string& defaultConnectorId,
    const std::string& defaultSchema) {
  return toConnectorTable(
      session,
      name,
      defaultConnectorId,
      defaultSchema,
      ParserOptions::kInformationSchemaConnectorIdDefault);
}

std::shared_ptr<Statement> PrestoParser::buildStatement(
    std::string_view sql,
    const ParserOptions& options,
    bool enableTracing) {
  SyntaxParser syntaxParser(sql, options.maxExpressionDepth);
  auto* context = syntaxParser.parse();

  AstBuilder astBuilder(options, enableTracing);
  auto query =
      std::any_cast<std::shared_ptr<Statement>>(astBuilder.visit(context));

  if (enableTracing) {
    std::stringstream astString;
    AstPrinter printer(astString);
    query->accept(&printer);

    std::cout << "AST: " << astString.str() << std::endl;
  }

  return query;
}

SyntacticTableNames PrestoParser::extractSyntacticTableNames(
    std::string_view sql) const {
  const auto statement = buildStatement(sql, session_->options(), false);
  SyntacticTableVisitor visitor(session_, defaultConnectorId_, defaultSchema_);
  visitor.process(statement.get());
  return std::move(visitor).take(statementKindFor(*statement));
}

} // namespace axiom::sql::presto
