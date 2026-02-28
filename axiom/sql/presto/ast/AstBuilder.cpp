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

#include "axiom/sql/presto/ast/AstBuilder.h"

#include <folly/Unicode.h>
#include "velox/common/base/Exceptions.h"

namespace axiom::sql::presto {

namespace {
template <typename T>
bool isDistinct(T* context) {
  return context->setQuantifier() != nullptr &&
      context->setQuantifier()->DISTINCT() != nullptr;
}

std::optional<std::string> getText(antlr4::Token* token) {
  if (token == nullptr) {
    return std::nullopt;
  }
  return token->getText();
}

NodeLocation getLocation(antlr4::Token* token) {
  return NodeLocation(token->getLine(), token->getCharPositionInLine());
}

NodeLocation getLocation(antlr4::ParserRuleContext* ctx) {
  return getLocation(ctx->getStart());
}

NodeLocation getLocation(antlr4::tree::TerminalNode* terminalNode) {
  return getLocation(terminalNode->getSymbol());
}

// Remove leading and trailing quotes.
std::string unquote(std::string_view value) {
  return std::string{value.substr(1, value.length() - 2)};
}

bool isHexDigit(char c) {
  return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') ||
      (c >= 'A' && c <= 'F');
}

// Check that the escape character is a printable ASCII character that is not a
// hex digit, quote, or '+'.
bool isValidUnicodeEscape(char c) {
  return c > 0x20 && c < 0x7F && !isHexDigit(c) && c != '+' && c != '"' &&
      c != '\'';
}

// Decode a Unicode string literal (U&'...' with optional UESCAPE).
std::string decodeUnicodeLiteral(
    PrestoSqlParser::UnicodeStringLiteralContext& ctx) {
  char escape = '\\';
  if (ctx.UESCAPE() != nullptr) {
    auto escapeString = unquote(ctx.STRING()->getText());
    VELOX_USER_CHECK_EQ(
        escapeString.size(),
        1,
        "Invalid Unicode escape character: {}",
        escapeString);
    escape = escapeString[0];
    VELOX_USER_CHECK(
        isValidUnicodeEscape(escape),
        "Invalid Unicode escape character: {}",
        escapeString);
  }

  // Strip 'U&' prefix and quotes: U&'content' -> content.
  auto rawText = ctx.UNICODE_STRING()->getText();
  auto rawContent = unquote(std::string_view{rawText}.substr(2));

  std::string result;
  std::string escapedChars;
  int32_t charsNeeded{0};

  enum State { kEmpty, kEscaped, kUnicodeSequence };
  auto state = kEmpty;

  for (auto ch : rawContent) {
    switch (state) {
      case kEmpty:
        if (ch == escape) {
          state = kEscaped;
        } else {
          result += ch;
        }
        break;
      case kEscaped:
        if (ch == escape) {
          result += escape;
          state = kEmpty;
        } else if (ch == '+') {
          state = kUnicodeSequence;
          charsNeeded = 6;
        } else {
          VELOX_USER_CHECK(isHexDigit(ch), "Invalid hexadecimal digit: {}", ch);
          state = kUnicodeSequence;
          charsNeeded = 4;
          escapedChars += ch;
        }
        break;
      case kUnicodeSequence:
        VELOX_USER_CHECK(
            isHexDigit(ch), "Incomplete escape sequence: {}", escapedChars);
        escapedChars += ch;
        if (escapedChars.size() == charsNeeded) {
          auto codePoint =
              static_cast<int32_t>(std::stoul(escapedChars, nullptr, 16));
          VELOX_USER_CHECK(
              codePoint >= 0 && codePoint <= 0x10FFFF,
              "Invalid escaped character: {}",
              escapedChars);
          VELOX_USER_CHECK(
              codePoint < 0xD800 || codePoint > 0xDFFF,
              "Invalid escaped character: {}. Escaped character is a surrogate. Use '\\+123456' instead.",
              escapedChars);
          folly::appendCodePointToUtf8(codePoint, result);
          escapedChars.clear();
          state = kEmpty;
        }
        break;
    }
  }

  VELOX_USER_CHECK(
      state == kEmpty, "Incomplete escape sequence: {}", escapedChars);
  return result;
}

} // namespace

void AstBuilder::trace(std::string_view name) const {
  if (enableTracing_) {
    std::cout << std::string(tracingIndent_ * 2, ' ') << name << std::endl;
  }
}

std::any AstBuilder::visitSingleStatement(
    PrestoSqlParser::SingleStatementContext* ctx) {
  return visit(ctx->statement());
}

std::any AstBuilder::visitQuery(PrestoSqlParser::QueryContext* ctx) {
  trace("visitQuery");

  auto queryNoWith = visitTyped<Query>(ctx->queryNoWith());

  return std::static_pointer_cast<Statement>(std::make_shared<Query>(
      getLocation(ctx),
      visitTyped<With>(ctx->with()),
      queryNoWith->queryBody(),
      queryNoWith->orderBy(),
      queryNoWith->offset(),
      queryNoWith->limit()));
}

std::any AstBuilder::visitQueryNoWith(
    PrestoSqlParser::QueryNoWithContext* ctx) {
  trace("visitQueryNoWith");

  OrderByPtr orderBy;
  if (ctx->ORDER() != nullptr) {
    orderBy = std::make_shared<OrderBy>(
        getLocation(ctx->ORDER()), visitTyped<SortItem>(ctx->sortItem()));
  }

  OffsetPtr offset;
  if (ctx->offset) {
    offset = std::make_shared<Offset>(getLocation(ctx), ctx->offset->getText());
  }

  auto limit = getText(ctx->limit);
  if (!limit.has_value()) {
    limit = getText(ctx->fetchFirstNRows);
  }

  auto term = visitTyped<QueryBody>(ctx->queryTerm());
  if (auto querySpec = std::dynamic_pointer_cast<QuerySpecification>(term)) {
    return std::make_shared<Query>(
        getLocation(ctx),
        /*with=*/nullptr,
        std::make_shared<QuerySpecification>(
            getLocation(ctx),
            querySpec->select(),
            querySpec->from(),
            querySpec->where(),
            querySpec->groupBy(),
            querySpec->having()),
        orderBy,
        offset,
        limit);
  }

  return std::make_shared<Query>(
      getLocation(ctx),
      /*with=*/nullptr,
      term,
      orderBy,
      offset,
      limit);
}

std::any AstBuilder::visitSelectSingle(
    PrestoSqlParser::SelectSingleContext* ctx) {
  trace("visitSelectSingle");
  auto expr = visitTyped<Expression>(ctx->expression());

  auto alias = visitTyped<Identifier>(ctx->identifier());

  return std::static_pointer_cast<SelectItem>(
      std::make_shared<SingleColumn>(getLocation(ctx), expr, alias));
}

std::any AstBuilder::visitQuerySpecification(
    PrestoSqlParser::QuerySpecificationContext* ctx) {
  trace("visitQuerySpecification");

  auto selectItems = visitTyped<SelectItem>(ctx->selectItem());

  RelationPtr from;
  auto relations = visitTyped<Relation>(ctx->relation());
  if (!relations.empty()) {
    // Synthesize implicit join nodes
    auto iterator = relations.begin();
    RelationPtr relation = *iterator;
    ++iterator;

    while (iterator != relations.end()) {
      relation = std::make_shared<Join>(
          getLocation(ctx),
          Join::Type::kImplicit,
          relation,
          *iterator,
          nullptr);
      ++iterator;
    }

    from = relation;
  }

  return std::static_pointer_cast<QueryBody>(
      std::make_shared<QuerySpecification>(
          getLocation(ctx),
          std::make_shared<Select>(
              getLocation(ctx), isDistinct(ctx), std::move(selectItems)),
          from,
          visitTyped<Expression>(ctx->where),
          visitTyped<GroupBy>(ctx->groupBy()),
          visitTyped<Expression>(ctx->having),
          nullptr // window
          ));
}

std::any AstBuilder::visitSampledRelation(
    PrestoSqlParser::SampledRelationContext* ctx) {
  trace("visitSampledRelation");
  auto child = visitTyped<Relation>(ctx->aliasedRelation());
  if (!ctx->TABLESAMPLE()) {
    return child;
  }

  SampledRelation::Type sampleType;
  if (ctx->sampleType()->BERNOULLI() != nullptr) {
    sampleType = SampledRelation::Type::kBernoulli;
  } else if (ctx->sampleType()->SYSTEM() != nullptr) {
    sampleType = SampledRelation::Type::kSystem;
  } else {
    throw std::runtime_error(
        "Unsupported table sample type: " + ctx->sampleType()->getText());
  }

  return std::static_pointer_cast<Relation>(std::make_shared<SampledRelation>(
      getLocation(ctx),
      child,
      sampleType,
      visitTyped<Expression>(ctx->percentage)));
}

std::any AstBuilder::visitAliasedRelation(
    PrestoSqlParser::AliasedRelationContext* ctx) {
  trace("visitAliasedRelation");
  auto child = visitTyped<Relation>(ctx->relationPrimary());
  if (!ctx->identifier()) {
    return child;
  }

  std::vector<IdentifierPtr> aliases;
  if (ctx->columnAliases() != nullptr) {
    aliases = visitTyped<Identifier>(ctx->columnAliases()->identifier());
  }

  return std::static_pointer_cast<Relation>(std::make_shared<AliasedRelation>(
      getLocation(ctx), child, visitIdentifier(ctx->identifier()), aliases));
}

std::any AstBuilder::visitTableName(PrestoSqlParser::TableNameContext* ctx) {
  trace("visitTableName");

  auto name = getQualifiedName(ctx->qualifiedName());
  return std::static_pointer_cast<Relation>(
      std::make_shared<Table>(getLocation(ctx), name));
}

std::any AstBuilder::visitSelectAll(PrestoSqlParser::SelectAllContext* ctx) {
  trace("visitSelectAll");

  std::shared_ptr<QualifiedName> name;
  if (ctx->qualifiedName() != nullptr) {
    name = getQualifiedName(ctx->qualifiedName());
  }

  return std::static_pointer_cast<SelectItem>(
      std::make_shared<AllColumns>(getLocation(ctx), name));
}

std::any AstBuilder::visitUnquotedIdentifier(
    PrestoSqlParser::UnquotedIdentifierContext* ctx) {
  return std::make_shared<Identifier>(getLocation(ctx), ctx->getText(), false);
}

// private
QualifiedNamePtr AstBuilder::getQualifiedName(
    PrestoSqlParser::QualifiedNameContext* ctx) {
  auto identifiers = visitTyped<Identifier>(ctx->identifier());

  std::vector<std::string> names;
  names.reserve(identifiers.size());
  for (auto& identifier : identifiers) {
    names.push_back(identifier->value());
  }
  return std::make_shared<QualifiedName>(getLocation(ctx), std::move(names));
}

std::any AstBuilder::visitStandaloneExpression(
    PrestoSqlParser::StandaloneExpressionContext* ctx) {
  trace("visitStandaloneExpression");
  return visitChildren("visitStandaloneExpression", ctx);
}

std::any AstBuilder::visitStandaloneRoutineBody(
    PrestoSqlParser::StandaloneRoutineBodyContext* ctx) {
  trace("visitStandaloneRoutineBody");
  return visitChildren("visitStandaloneRoutineBody", ctx);
}

std::any AstBuilder::visitStatementDefault(
    PrestoSqlParser::StatementDefaultContext* ctx) {
  trace("visitStatementDefault");
  return visitChildren("visitStatementDefault", ctx);
}

std::any AstBuilder::visitUse(PrestoSqlParser::UseContext* ctx) {
  trace("visitUse");

  std::shared_ptr<Identifier> catalog;
  if (ctx->catalog != nullptr) {
    catalog = visitIdentifier(ctx->catalog);
  }

  return std::static_pointer_cast<Statement>(std::make_shared<Use>(
      getLocation(ctx), std::move(catalog), visitIdentifier(ctx->schema)));
}

std::any AstBuilder::visitCreateSchema(
    PrestoSqlParser::CreateSchemaContext* ctx) {
  trace("visitCreateSchema");
  return visitChildren("visitCreateSchema", ctx);
}

std::any AstBuilder::visitDropSchema(PrestoSqlParser::DropSchemaContext* ctx) {
  trace("visitDropSchema");
  return visitChildren("visitDropSchema", ctx);
}

std::any AstBuilder::visitRenameSchema(
    PrestoSqlParser::RenameSchemaContext* ctx) {
  trace("visitRenameSchema");
  return visitChildren("visitRenameSchema", ctx);
}

std::any AstBuilder::visitCreateTableAsSelect(
    PrestoSqlParser::CreateTableAsSelectContext* ctx) {
  trace("visitCreateTableAsSelect");

  std::optional<std::string> comment;
  if (ctx->COMMENT() != nullptr) {
    comment = visitExpression(ctx->string())->as<StringLiteral>()->value();
  }

  std::vector<std::shared_ptr<Identifier>> columns;
  if (ctx->columnAliases()) {
    columns = visitTyped<Identifier>(ctx->columnAliases()->identifier());
  }

  std::vector<std::shared_ptr<Property>> properties;
  if (ctx->properties() != nullptr) {
    properties = visitTyped<Property>(ctx->properties()->property());
  }

  return std::static_pointer_cast<Statement>(
      std::make_shared<CreateTableAsSelect>(
          getLocation(ctx),
          getQualifiedName(ctx->qualifiedName()),
          visitTyped<Statement>(ctx->query()),
          /*notExists=*/ctx->EXISTS() != nullptr,
          std::move(properties),
          /*withData=*/ctx->NO() == nullptr,
          std::move(columns),
          std::move(comment)));
}

std::any AstBuilder::visitCreateTable(
    PrestoSqlParser::CreateTableContext* ctx) {
  trace("visitCreateTable");

  std::optional<std::string> comment;
  if (ctx->COMMENT() != nullptr) {
    comment = visitExpression(ctx->string())->as<StringLiteral>()->value();
  }

  std::vector<std::shared_ptr<Property>> properties;
  if (ctx->properties() != nullptr) {
    properties = visitTyped<Property>(ctx->properties()->property());
  }

  return std::static_pointer_cast<Statement>(std::make_shared<CreateTable>(
      getLocation(ctx),
      getQualifiedName(ctx->qualifiedName()),
      visitTyped<TableElement>(ctx->tableElement()),
      /*notExists=*/ctx->EXISTS() != nullptr,
      std::move(properties),
      std::move(comment)));
}

std::any AstBuilder::visitDropTable(PrestoSqlParser::DropTableContext* ctx) {
  trace("visitDropTable");

  return std::static_pointer_cast<Statement>(std::make_shared<DropTable>(
      getLocation(ctx),
      getQualifiedName(ctx->qualifiedName()),
      ctx->EXISTS() != nullptr));
}

std::any AstBuilder::visitInsertInto(PrestoSqlParser::InsertIntoContext* ctx) {
  trace("visitInsertInto");

  std::vector<std::shared_ptr<Identifier>> columns;
  if (ctx->columnAliases()) {
    columns = visitTyped<Identifier>(ctx->columnAliases()->identifier());
  };

  return std::static_pointer_cast<Statement>(std::make_shared<Insert>(
      getLocation(ctx),
      getQualifiedName(ctx->qualifiedName()),
      std::move(columns),
      visitTyped<Statement>(ctx->query())));
}

std::any AstBuilder::visitDelete(PrestoSqlParser::DeleteContext* ctx) {
  trace("visitDelete");
  return visitChildren("visitDelete", ctx);
}

std::any AstBuilder::visitTruncateTable(
    PrestoSqlParser::TruncateTableContext* ctx) {
  trace("visitTruncateTable");
  return visitChildren("visitTruncateTable", ctx);
}

std::any AstBuilder::visitRenameTable(
    PrestoSqlParser::RenameTableContext* ctx) {
  trace("visitRenameTable");
  return visitChildren("visitRenameTable", ctx);
}

std::any AstBuilder::visitRenameColumn(
    PrestoSqlParser::RenameColumnContext* ctx) {
  trace("visitRenameColumn");
  return visitChildren("visitRenameColumn", ctx);
}

std::any AstBuilder::visitDropColumn(PrestoSqlParser::DropColumnContext* ctx) {
  trace("visitDropColumn");
  return visitChildren("visitDropColumn", ctx);
}

std::any AstBuilder::visitAddColumn(PrestoSqlParser::AddColumnContext* ctx) {
  trace("visitAddColumn");
  return visitChildren("visitAddColumn", ctx);
}

std::any AstBuilder::visitAddConstraint(
    PrestoSqlParser::AddConstraintContext* ctx) {
  trace("visitAddConstraint");
  return visitChildren("visitAddConstraint", ctx);
}

std::any AstBuilder::visitDropConstraint(
    PrestoSqlParser::DropConstraintContext* ctx) {
  trace("visitDropConstraint");
  return visitChildren("visitDropConstraint", ctx);
}

std::any AstBuilder::visitAlterColumnSetNotNull(
    PrestoSqlParser::AlterColumnSetNotNullContext* ctx) {
  trace("visitAlterColumnSetNotNull");
  return visitChildren("visitAlterColumnSetNotNull", ctx);
}

std::any AstBuilder::visitAlterColumnDropNotNull(
    PrestoSqlParser::AlterColumnDropNotNullContext* ctx) {
  trace("visitAlterColumnDropNotNull");
  return visitChildren("visitAlterColumnDropNotNull", ctx);
}

std::any AstBuilder::visitSetTableProperties(
    PrestoSqlParser::SetTablePropertiesContext* ctx) {
  trace("visitSetTableProperties");
  return visitChildren("visitSetTableProperties", ctx);
}

std::any AstBuilder::visitAnalyze(PrestoSqlParser::AnalyzeContext* ctx) {
  trace("visitAnalyze");
  return visitChildren("visitAnalyze", ctx);
}

std::any AstBuilder::visitCreateType(PrestoSqlParser::CreateTypeContext* ctx) {
  trace("visitCreateType");
  return visitChildren("visitCreateType", ctx);
}

std::any AstBuilder::visitCreateView(PrestoSqlParser::CreateViewContext* ctx) {
  trace("visitCreateView");
  return visitChildren("visitCreateView", ctx);
}

std::any AstBuilder::visitRenameView(PrestoSqlParser::RenameViewContext* ctx) {
  trace("visitRenameView");
  return visitChildren("visitRenameView", ctx);
}

std::any AstBuilder::visitDropView(PrestoSqlParser::DropViewContext* ctx) {
  trace("visitDropView");
  return visitChildren("visitDropView", ctx);
}

std::any AstBuilder::visitCreateMaterializedView(
    PrestoSqlParser::CreateMaterializedViewContext* ctx) {
  trace("visitCreateMaterializedView");
  return visitChildren("visitCreateMaterializedView", ctx);
}

std::any AstBuilder::visitDropMaterializedView(
    PrestoSqlParser::DropMaterializedViewContext* ctx) {
  trace("visitDropMaterializedView");
  return visitChildren("visitDropMaterializedView", ctx);
}

std::any AstBuilder::visitRefreshMaterializedView(
    PrestoSqlParser::RefreshMaterializedViewContext* ctx) {
  trace("visitRefreshMaterializedView");
  return visitChildren("visitRefreshMaterializedView", ctx);
}

std::any AstBuilder::visitCreateFunction(
    PrestoSqlParser::CreateFunctionContext* ctx) {
  trace("visitCreateFunction");
  return visitChildren("visitCreateFunction", ctx);
}

std::any AstBuilder::visitAlterFunction(
    PrestoSqlParser::AlterFunctionContext* ctx) {
  trace("visitAlterFunction");
  return visitChildren("visitAlterFunction", ctx);
}

std::any AstBuilder::visitDropFunction(
    PrestoSqlParser::DropFunctionContext* ctx) {
  trace("visitDropFunction");
  return visitChildren("visitDropFunction", ctx);
}

std::any AstBuilder::visitCall(PrestoSqlParser::CallContext* ctx) {
  trace("visitCall");
  return visitChildren("visitCall", ctx);
}

std::any AstBuilder::visitCreateRole(PrestoSqlParser::CreateRoleContext* ctx) {
  trace("visitCreateRole");
  return visitChildren("visitCreateRole", ctx);
}

std::any AstBuilder::visitDropRole(PrestoSqlParser::DropRoleContext* ctx) {
  trace("visitDropRole");
  return visitChildren("visitDropRole", ctx);
}

std::any AstBuilder::visitGrantRoles(PrestoSqlParser::GrantRolesContext* ctx) {
  trace("visitGrantRoles");
  return visitChildren("visitGrantRoles", ctx);
}

std::any AstBuilder::visitRevokeRoles(
    PrestoSqlParser::RevokeRolesContext* ctx) {
  trace("visitRevokeRoles");
  return visitChildren("visitRevokeRoles", ctx);
}

std::any AstBuilder::visitSetRole(PrestoSqlParser::SetRoleContext* ctx) {
  trace("visitSetRole");
  return visitChildren("visitSetRole", ctx);
}

std::any AstBuilder::visitGrant(PrestoSqlParser::GrantContext* ctx) {
  trace("visitGrant");
  return visitChildren("visitGrant", ctx);
}

std::any AstBuilder::visitRevoke(PrestoSqlParser::RevokeContext* ctx) {
  trace("visitRevoke");
  return visitChildren("visitRevoke", ctx);
}

std::any AstBuilder::visitShowGrants(PrestoSqlParser::ShowGrantsContext* ctx) {
  trace("visitShowGrants");
  return visitChildren("visitShowGrants", ctx);
}

std::any AstBuilder::visitExplain(PrestoSqlParser::ExplainContext* ctx) {
  trace("visitExplain");

  return std::static_pointer_cast<Statement>(std::make_shared<Explain>(
      getLocation(ctx),
      visitTyped<Statement>(ctx->statement()),
      ctx->ANALYZE() != nullptr,
      ctx->VERBOSE() != nullptr,
      visitTyped<ExplainOption>(ctx->explainOption())));
}

std::any AstBuilder::visitShowCreateTable(
    PrestoSqlParser::ShowCreateTableContext* ctx) {
  trace("visitShowCreateTable");
  return visitChildren("visitShowCreateTable", ctx);
}

std::any AstBuilder::visitShowCreateView(
    PrestoSqlParser::ShowCreateViewContext* ctx) {
  trace("visitShowCreateView");
  return visitChildren("visitShowCreateView", ctx);
}

std::any AstBuilder::visitShowCreateMaterializedView(
    PrestoSqlParser::ShowCreateMaterializedViewContext* ctx) {
  trace("visitShowCreateMaterializedView");
  return visitChildren("visitShowCreateMaterializedView", ctx);
}

std::any AstBuilder::visitShowCreateFunction(
    PrestoSqlParser::ShowCreateFunctionContext* ctx) {
  trace("visitShowCreateFunction");
  return visitChildren("visitShowCreateFunction", ctx);
}

std::any AstBuilder::visitShowTables(PrestoSqlParser::ShowTablesContext* ctx) {
  trace("visitShowTables");
  return visitChildren("visitShowTables", ctx);
}

std::any AstBuilder::visitShowSchemas(
    PrestoSqlParser::ShowSchemasContext* ctx) {
  trace("visitShowSchemas");
  return visitChildren("visitShowSchemas", ctx);
}

std::any AstBuilder::visitShowCatalogs(
    PrestoSqlParser::ShowCatalogsContext* ctx) {
  trace("visitShowCatalogs");

  std::optional<std::string> likePattern;
  std::optional<std::string> escape;
  if (ctx->LIKE() != nullptr) {
    likePattern = visitExpression(ctx->pattern)->as<StringLiteral>()->value();
  }

  if (ctx->ESCAPE() != nullptr) {
    escape = visitExpression(ctx->escape)->as<StringLiteral>()->value();
  }

  return std::static_pointer_cast<Statement>((std::make_shared<ShowCatalogs>(
      getLocation(ctx), std::move(likePattern), std::move(escape))));
}

std::any AstBuilder::visitShowColumns(
    PrestoSqlParser::ShowColumnsContext* ctx) {
  trace("visitShowColumns");
  return std::static_pointer_cast<Statement>(std::make_shared<ShowColumns>(
      getLocation(ctx), getQualifiedName(ctx->qualifiedName())));
}

std::any AstBuilder::visitShowStats(PrestoSqlParser::ShowStatsContext* ctx) {
  trace("visitShowStats");
  return visitChildren("visitShowStats", ctx);
}

std::any AstBuilder::visitShowStatsForQuery(
    PrestoSqlParser::ShowStatsForQueryContext* ctx) {
  trace("visitShowStatsForQuery");
  return visitChildren("visitShowStatsForQuery", ctx);
}

std::any AstBuilder::visitShowRoles(PrestoSqlParser::ShowRolesContext* ctx) {
  trace("visitShowRoles");
  return visitChildren("visitShowRoles", ctx);
}

std::any AstBuilder::visitShowRoleGrants(
    PrestoSqlParser::ShowRoleGrantsContext* ctx) {
  trace("visitShowRoleGrants");
  return visitChildren("visitShowRoleGrants", ctx);
}

std::any AstBuilder::visitShowFunctions(
    PrestoSqlParser::ShowFunctionsContext* ctx) {
  trace("visitShowFunctions");

  std::optional<std::string> likePattern;
  std::optional<std::string> escape;
  if (ctx->LIKE() != nullptr) {
    likePattern = visitExpression(ctx->pattern)->as<StringLiteral>()->value();
  }

  if (ctx->ESCAPE() != nullptr) {
    escape = visitExpression(ctx->escape)->as<StringLiteral>()->value();
  }
  return std::static_pointer_cast<Statement>((std::make_shared<ShowFunctions>(
      getLocation(ctx), std::move(likePattern), std::move(escape))));
}

std::any AstBuilder::visitShowSession(
    PrestoSqlParser::ShowSessionContext* ctx) {
  trace("visitShowSession");
  return visitChildren("visitShowSession", ctx);
}

std::any AstBuilder::visitSetSession(PrestoSqlParser::SetSessionContext* ctx) {
  trace("visitSetSession");
  return visitChildren("visitSetSession", ctx);
}

std::any AstBuilder::visitResetSession(
    PrestoSqlParser::ResetSessionContext* ctx) {
  trace("visitResetSession");
  return visitChildren("visitResetSession", ctx);
}

std::any AstBuilder::visitStartTransaction(
    PrestoSqlParser::StartTransactionContext* ctx) {
  trace("visitStartTransaction");
  return visitChildren("visitStartTransaction", ctx);
}

std::any AstBuilder::visitCommit(PrestoSqlParser::CommitContext* ctx) {
  trace("visitCommit");
  return visitChildren("visitCommit", ctx);
}

std::any AstBuilder::visitRollback(PrestoSqlParser::RollbackContext* ctx) {
  trace("visitRollback");
  return visitChildren("visitRollback", ctx);
}

std::any AstBuilder::visitPrepare(PrestoSqlParser::PrepareContext* ctx) {
  trace("visitPrepare");
  return visitChildren("visitPrepare", ctx);
}

std::any AstBuilder::visitDeallocate(PrestoSqlParser::DeallocateContext* ctx) {
  trace("visitDeallocate");
  return visitChildren("visitDeallocate", ctx);
}

std::any AstBuilder::visitExecute(PrestoSqlParser::ExecuteContext* ctx) {
  trace("visitExecute");
  return visitChildren("visitExecute", ctx);
}

std::any AstBuilder::visitDescribeInput(
    PrestoSqlParser::DescribeInputContext* ctx) {
  trace("visitDescribeInput");
  return visitChildren("visitDescribeInput", ctx);
}

std::any AstBuilder::visitDescribeOutput(
    PrestoSqlParser::DescribeOutputContext* ctx) {
  trace("visitDescribeOutput");
  return visitChildren("visitDescribeOutput", ctx);
}

std::any AstBuilder::visitUpdate(PrestoSqlParser::UpdateContext* ctx) {
  trace("visitUpdate");
  return visitChildren("visitUpdate", ctx);
}

std::any AstBuilder::visitWith(PrestoSqlParser::WithContext* ctx) {
  trace("visitWith");
  return std::make_shared<With>(
      getLocation(ctx),
      ctx->RECURSIVE() != nullptr,
      visitTyped<WithQuery>(ctx->namedQuery()));
}

std::any AstBuilder::visitTableElement(
    PrestoSqlParser::TableElementContext* ctx) {
  trace("visitTableElement");
  if (ctx->columnDefinition() != nullptr) {
    return visitColumnDefinition(ctx->columnDefinition());
  }
  if (ctx->likeClause() != nullptr) {
    return visitLikeClause(ctx->likeClause());
  }
  if (ctx->constraintSpecification() != nullptr) {
    return visitConstraintSpecification(ctx->constraintSpecification());
  }
  return visitChildren("visitTableElement", ctx);
}

namespace {
std::string getIntervalFieldType(
    PrestoSqlParser::IntervalFieldContext* intervalField) {
  if (intervalField->YEAR() != nullptr) {
    return "YEAR";
  } else if (intervalField->MONTH() != nullptr) {
    return "MONTH";
  } else if (intervalField->DAY() != nullptr) {
    return "DAY";
  } else if (intervalField->HOUR() != nullptr) {
    return "HOUR";
  } else if (intervalField->MINUTE() != nullptr) {
    return "MINUTE";
  } else if (intervalField->SECOND() != nullptr) {
    return "SECOND";
  } else {
    throw std::runtime_error(
        "Unsupported interval field: " + intervalField->getText());
  }
}

TypeSignaturePtr toTypeSignature(
    PrestoSqlParser::TypeParameterContext* typeParam,
    const std::optional<std::string>& rowFieldName = std::nullopt);

TypeSignaturePtr toTypeSignature(
    PrestoSqlParser::TypeContext* ctx,
    const std::optional<std::string>& rowFieldName = std::nullopt) {
  if (ctx->baseType() != nullptr) {
    if (ctx->baseType()->DOUBLE_PRECISION() != nullptr) {
      return std::make_shared<TypeSignature>(
          getLocation(ctx), "DOUBLE", rowFieldName);
    }

    auto baseName = ctx->baseType()->getText();

    std::vector<TypeSignaturePtr> parameters;
    for (const auto& param : ctx->typeParameter()) {
      parameters.push_back(toTypeSignature(param));
    }

    return std::make_shared<TypeSignature>(
        getLocation(ctx),
        std::move(baseName),
        std::move(parameters),
        rowFieldName);
  }

  if (ctx->ARRAY() != nullptr) {
    return std::make_shared<TypeSignature>(
        getLocation(ctx),
        "ARRAY",
        std::vector<TypeSignaturePtr>{toTypeSignature(ctx->type(0))},
        rowFieldName);
  }

  if (ctx->MAP() != nullptr) {
    return std::make_shared<TypeSignature>(
        getLocation(ctx),
        "MAP",
        std::vector<TypeSignaturePtr>{
            toTypeSignature(ctx->type(0)), toTypeSignature(ctx->type(1))},
        rowFieldName);
  }

  if (ctx->ROW() != nullptr) {
    const auto& identifiers = ctx->identifier();
    const auto& typeParams = ctx->type();

    std::vector<TypeSignaturePtr> parameters;
    parameters.reserve(typeParams.size());
    for (auto i = 0; i < typeParams.size(); ++i) {
      parameters.push_back(
          toTypeSignature(typeParams[i], identifiers[i]->getText()));
    }

    return std::make_shared<TypeSignature>(
        getLocation(ctx), "ROW", std::move(parameters), rowFieldName);
  }

  if (ctx->INTERVAL() != nullptr) {
    const auto& intervalFields = ctx->intervalField();
    if (intervalFields.size() >= 2) {
      return std::make_shared<TypeSignature>(
          getLocation(ctx),
          "INTERVAL " + getIntervalFieldType(intervalFields[0]) + " TO " +
              getIntervalFieldType(intervalFields[1]),
          rowFieldName);
    }
  }

  throw std::runtime_error("Unsupported type specification: " + ctx->getText());
}

TypeSignaturePtr toTypeSignature(
    PrestoSqlParser::TypeParameterContext* ctx,
    const std::optional<std::string>& rowFieldName) {
  if (ctx->INTEGER_VALUE() != nullptr) {
    return std::make_shared<TypeSignature>(
        getLocation(ctx), ctx->INTEGER_VALUE()->getText(), rowFieldName);
  }

  if (ctx->type() != nullptr) {
    return toTypeSignature(ctx->type(), rowFieldName);
  }

  throw std::runtime_error("Unsupported typeParameter: " + ctx->getText());
}

} // namespace

std::any AstBuilder::visitColumnDefinition(
    PrestoSqlParser::ColumnDefinitionContext* ctx) {
  trace("visitColumnDefinition");

  std::optional<std::string> comment;
  if (ctx->COMMENT() != nullptr) {
    comment = visitExpression(ctx->string())->as<StringLiteral>()->value();
  }

  std::vector<std::shared_ptr<Property>> properties;
  if (ctx->properties() != nullptr) {
    properties = visitTyped<Property>(ctx->properties()->property());
  }

  return std::static_pointer_cast<TableElement>(
      std::make_shared<ColumnDefinition>(
          getLocation(ctx),
          visitIdentifier(ctx->identifier()),
          toTypeSignature(ctx->type()),
          /*nullable=*/ctx->NOT() == nullptr,
          std::move(properties),
          std::move(comment)));
}

std::any AstBuilder::visitLikeClause(PrestoSqlParser::LikeClauseContext* ctx) {
  trace("visitLikeClause");

  std::optional<LikeClause::PropertiesOption> propertiesOption;
  if (ctx->optionType != nullptr) {
    if (ctx->INCLUDING() != nullptr) {
      propertiesOption = LikeClause::PropertiesOption::kIncluding;
    } else {
      propertiesOption = LikeClause::PropertiesOption::kExcluding;
    }
  }

  return std::static_pointer_cast<TableElement>(std::make_shared<LikeClause>(
      getLocation(ctx),
      getQualifiedName(ctx->qualifiedName()),
      std::move(propertiesOption)));
}

std::any AstBuilder::visitProperties(PrestoSqlParser::PropertiesContext* ctx) {
  trace("visitProperties");
  return visitChildren("visitProperties", ctx);
}

std::any AstBuilder::visitProperty(PrestoSqlParser::PropertyContext* ctx) {
  trace("visitProperty");

  return std::make_shared<Property>(
      getLocation(ctx),
      visitIdentifier(ctx->identifier()),
      visitTyped<Expression>(ctx->expression()));
}

std::any AstBuilder::visitSqlParameterDeclaration(
    PrestoSqlParser::SqlParameterDeclarationContext* ctx) {
  trace("visitSqlParameterDeclaration");
  return visitChildren("visitSqlParameterDeclaration", ctx);
}

std::any AstBuilder::visitRoutineCharacteristics(
    PrestoSqlParser::RoutineCharacteristicsContext* ctx) {
  trace("visitRoutineCharacteristics");
  return visitChildren("visitRoutineCharacteristics", ctx);
}

std::any AstBuilder::visitRoutineCharacteristic(
    PrestoSqlParser::RoutineCharacteristicContext* ctx) {
  trace("visitRoutineCharacteristic");
  return visitChildren("visitRoutineCharacteristic", ctx);
}

std::any AstBuilder::visitAlterRoutineCharacteristics(
    PrestoSqlParser::AlterRoutineCharacteristicsContext* ctx) {
  trace("visitAlterRoutineCharacteristics");
  return visitChildren("visitAlterRoutineCharacteristics", ctx);
}

std::any AstBuilder::visitAlterRoutineCharacteristic(
    PrestoSqlParser::AlterRoutineCharacteristicContext* ctx) {
  trace("visitAlterRoutineCharacteristic");
  return visitChildren("visitAlterRoutineCharacteristic", ctx);
}

std::any AstBuilder::visitRoutineBody(
    PrestoSqlParser::RoutineBodyContext* ctx) {
  trace("visitRoutineBody");
  return visitChildren("visitRoutineBody", ctx);
}

std::any AstBuilder::visitReturnStatement(
    PrestoSqlParser::ReturnStatementContext* ctx) {
  trace("visitReturnStatement");
  return visitChildren("visitReturnStatement", ctx);
}

std::any AstBuilder::visitExternalBodyReference(
    PrestoSqlParser::ExternalBodyReferenceContext* ctx) {
  trace("visitExternalBodyReference");
  return visitChildren("visitExternalBodyReference", ctx);
}

std::any AstBuilder::visitLanguage(PrestoSqlParser::LanguageContext* ctx) {
  trace("visitLanguage");
  return visitChildren("visitLanguage", ctx);
}

std::any AstBuilder::visitDeterminism(
    PrestoSqlParser::DeterminismContext* ctx) {
  trace("visitDeterminism");
  return visitChildren("visitDeterminism", ctx);
}

std::any AstBuilder::visitNullCallClause(
    PrestoSqlParser::NullCallClauseContext* ctx) {
  trace("visitNullCallClause");
  return visitChildren("visitNullCallClause", ctx);
}

std::any AstBuilder::visitExternalRoutineName(
    PrestoSqlParser::ExternalRoutineNameContext* ctx) {
  trace("visitExternalRoutineName");
  return visitChildren("visitExternalRoutineName", ctx);
}

std::any AstBuilder::visitQueryTermDefault(
    PrestoSqlParser::QueryTermDefaultContext* ctx) {
  trace("visitQueryTermDefault");
  return visitChildren("visitQueryTermDefault", ctx);
}

std::any AstBuilder::visitSetOperation(
    PrestoSqlParser::SetOperationContext* ctx) {
  trace("visitSetOperation");

  auto left = visitTyped<QueryBody>(ctx->left);
  auto right = visitTyped<QueryBody>(ctx->right);

  bool distinct = true;
  if (ctx->setQuantifier() != nullptr) {
    if (ctx->setQuantifier()->DISTINCT() != nullptr) {
      distinct = true;
    } else if (ctx->setQuantifier()->ALL() != nullptr) {
      distinct = false;
    }
  }

  const auto tokenType = ctx->op->getType();
  if (tokenType == PrestoSqlParser::UNION) {
    return std::static_pointer_cast<QueryBody>(
        std::make_shared<Union>(getLocation(ctx), left, right, distinct));
  }

  if (tokenType == PrestoSqlParser::EXCEPT) {
    return std::static_pointer_cast<QueryBody>(
        std::make_shared<Except>(getLocation(ctx), left, right, distinct));
  }

  if (tokenType == PrestoSqlParser::INTERSECT) {
    return std::static_pointer_cast<QueryBody>(
        std::make_shared<Intersect>(getLocation(ctx), left, right, distinct));
  }

  throw std::runtime_error("Unsupported set operation: " + ctx->op->getText());
}

std::any AstBuilder::visitQueryPrimaryDefault(
    PrestoSqlParser::QueryPrimaryDefaultContext* ctx) {
  trace("visitQueryPrimaryDefault");
  return visitChildren("visitQueryPrimaryDefault", ctx);
}

std::any AstBuilder::visitTable(PrestoSqlParser::TableContext* ctx) {
  trace("visitTable");
  return visitChildren("visitTable", ctx);
}

std::any AstBuilder::visitInlineTable(
    PrestoSqlParser::InlineTableContext* ctx) {
  trace("visitInlineTable");
  return std::static_pointer_cast<QueryBody>(std::make_shared<Values>(
      getLocation(ctx), visitTyped<Expression>(ctx->expression())));
}

std::any AstBuilder::visitSubquery(PrestoSqlParser::SubqueryContext* ctx) {
  trace("visitSubquery");

  auto queryNoWith = visitTyped<Query>(ctx->queryNoWith());

  return std::static_pointer_cast<QueryBody>(
      std::make_shared<TableSubquery>(getLocation(ctx), queryNoWith));
}

std::any AstBuilder::visitSortItem(PrestoSqlParser::SortItemContext* ctx) {
  trace("visitSortItem");

  auto expression = visitTyped<Expression>(ctx->expression());

  SortItem::Ordering ordering = SortItem::Ordering::kAscending;
  if (ctx->ordering) {
    auto tokenType = ctx->ordering->getType();
    if (tokenType == PrestoSqlParser::ASC) {
      ordering = SortItem::Ordering::kAscending;
    } else if (tokenType == PrestoSqlParser::DESC) {
      ordering = SortItem::Ordering::kDescending;
    }
  }

  SortItem::NullOrdering nullOrdering = SortItem::NullOrdering::kUndefined;
  if (ctx->nullOrdering) {
    auto tokenType = ctx->nullOrdering->getType();
    if (tokenType == PrestoSqlParser::FIRST) {
      nullOrdering = SortItem::NullOrdering::kFirst;
    } else if (tokenType == PrestoSqlParser::LAST) {
      nullOrdering = SortItem::NullOrdering::kLast;
    }
  }

  return std::make_shared<SortItem>(
      getLocation(ctx), expression, ordering, nullOrdering);
}

std::any AstBuilder::visitGroupBy(PrestoSqlParser::GroupByContext* ctx) {
  trace("visitGroupBy");

  auto groupingElements = visitTyped<GroupingElement>(ctx->groupingElement());

  return std::make_shared<GroupBy>(
      getLocation(ctx), isDistinct(ctx), groupingElements);
}

std::any AstBuilder::visitSingleGroupingSet(
    PrestoSqlParser::SingleGroupingSetContext* ctx) {
  trace("visitSingleGroupingSet");

  auto expressions = visitTyped<Expression>(ctx->groupingSet()->expression());
  return std::static_pointer_cast<GroupingElement>(
      std::make_shared<SimpleGroupBy>(getLocation(ctx), expressions));
}

std::any AstBuilder::visitRollup(PrestoSqlParser::RollupContext* ctx) {
  trace("visitRollup");

  auto expressions = visitTyped<Expression>(ctx->expression());
  return std::static_pointer_cast<GroupingElement>(
      std::make_shared<Rollup>(getLocation(ctx), expressions));
}

std::any AstBuilder::visitCube(PrestoSqlParser::CubeContext* ctx) {
  trace("visitCube");

  auto expressions = visitTyped<Expression>(ctx->expression());
  return std::static_pointer_cast<GroupingElement>(
      std::make_shared<Cube>(getLocation(ctx), expressions));
}

std::any AstBuilder::visitMultipleGroupingSets(
    PrestoSqlParser::MultipleGroupingSetsContext* ctx) {
  trace("visitMultipleGroupingSets");

  std::vector<std::vector<ExpressionPtr>> sets;
  sets.reserve(ctx->groupingSet().size());
  for (auto* groupingSetCtx : ctx->groupingSet()) {
    sets.emplace_back(visitTyped<Expression>(groupingSetCtx->expression()));
  }
  return std::static_pointer_cast<GroupingElement>(
      std::make_shared<GroupingSets>(getLocation(ctx), sets));
}

std::any AstBuilder::visitGroupingSet(
    PrestoSqlParser::GroupingSetContext* ctx) {
  trace("visitGroupingSet");

  VELOX_UNREACHABLE("visitGroupingSet should not be called directly");
}

std::any AstBuilder::visitNamedQuery(PrestoSqlParser::NamedQueryContext* ctx) {
  trace("visitNamedQuery");

  std::optional<std::vector<std::shared_ptr<Identifier>>> columns;
  if (ctx->columnAliases()) {
    columns = visitTyped<Identifier>(ctx->columnAliases()->identifier());
  }

  return std::make_shared<WithQuery>(
      getLocation(ctx),
      visitIdentifier(ctx->name),
      visitTyped<Statement>(ctx->query()),
      columns);
}

std::any AstBuilder::visitSetQuantifier(
    PrestoSqlParser::SetQuantifierContext* ctx) {
  trace("visitSetQuantifier");
  return visitChildren("visitSetQuantifier", ctx);
}

std::any AstBuilder::visitRelationDefault(
    PrestoSqlParser::RelationDefaultContext* ctx) {
  trace("visitRelationDefault");
  return visitChildren("visitRelationDefault", ctx);
}

namespace {
Join::Type toJoinType(PrestoSqlParser::JoinTypeContext* joinTypeCtx) {
  if (!joinTypeCtx) {
    return Join::Type::kInner;
  }

  if (joinTypeCtx->LEFT() != nullptr) {
    return Join::Type::kLeft;
  } else if (joinTypeCtx->RIGHT() != nullptr) {
    return Join::Type::kRight;
  } else if (joinTypeCtx->FULL() != nullptr) {
    return Join::Type::kFull;
  }

  return Join::Type::kInner;
}

} // anonymous namespace

std::any AstBuilder::visitJoinRelation(
    PrestoSqlParser::JoinRelationContext* ctx) {
  trace("visitJoinRelation");

  auto left = visitTyped<Relation>(ctx->left);

  if (ctx->CROSS() != nullptr) {
    auto right = visitTyped<Relation>(ctx->right);
    return std::static_pointer_cast<Relation>(std::make_shared<Join>(
        getLocation(ctx), Join::Type::kCross, left, right, nullptr));
  }

  if (ctx->NATURAL() != nullptr) {
    auto right = visitTyped<Relation>(ctx->right);
    auto joinType = toJoinType(ctx->joinType());
    return std::static_pointer_cast<Relation>(
        std::make_shared<NaturalJoin>(getLocation(ctx), joinType, left, right));
  }

  // Handle regular join with criteria.
  auto right = visitTyped<Relation>(ctx->rightRelation);

  JoinCriteriaPtr joinCriteria;
  if (auto criteria = ctx->joinCriteria()) {
    if (criteria->ON() != nullptr) {
      auto expression = visitExpression(criteria->booleanExpression());
      joinCriteria = std::make_shared<JoinOn>(
          getLocation(ctx->joinCriteria()), expression);
    } else if (criteria->USING() != nullptr) {
      std::vector<IdentifierPtr> columns;
      for (auto identifierCtx : criteria->identifier()) {
        auto identifier = visitIdentifier(identifierCtx);
        columns.push_back(identifier);
      }
      joinCriteria = std::make_shared<JoinUsing>(
          getLocation(ctx->joinCriteria()), columns);
    } else {
      throw std::runtime_error("Unsupported join criteria");
    }
  }

  auto joinType = toJoinType(ctx->joinType());

  return std::static_pointer_cast<Relation>(std::make_shared<Join>(
      getLocation(ctx), joinType, left, right, joinCriteria));
}

std::any AstBuilder::visitJoinType(PrestoSqlParser::JoinTypeContext* ctx) {
  trace("visitJoinType");
  return visitChildren("visitJoinType", ctx);
}

std::any AstBuilder::visitJoinCriteria(
    PrestoSqlParser::JoinCriteriaContext* ctx) {
  trace("visitJoinCriteria");
  return visitChildren("visitJoinCriteria", ctx);
}

std::any AstBuilder::visitSampleType(PrestoSqlParser::SampleTypeContext* ctx) {
  trace("visitSampleType");
  return visitChildren("visitSampleType", ctx);
}

std::any AstBuilder::visitColumnAliases(
    PrestoSqlParser::ColumnAliasesContext* ctx) {
  trace("visitColumnAliases");
  return visitChildren("visitColumnAliases", ctx);
}

std::any AstBuilder::visitSubqueryRelation(
    PrestoSqlParser::SubqueryRelationContext* ctx) {
  trace("visitSubqueryRelation");

  return std::static_pointer_cast<Relation>(std::make_shared<TableSubquery>(
      getLocation(ctx), visitTyped<Statement>(ctx->query())));
}

std::any AstBuilder::visitUnnest(PrestoSqlParser::UnnestContext* ctx) {
  trace("visitUnnest");
  return std::static_pointer_cast<Relation>(std::make_shared<Unnest>(
      getLocation(ctx),
      visitTyped<Expression>(ctx->expression()),
      ctx->ORDINALITY() != nullptr));
}

std::any AstBuilder::visitLateral(PrestoSqlParser::LateralContext* ctx) {
  trace("visitLateral");
  return visitChildren("visitLateral", ctx);
}

std::any AstBuilder::visitParenthesizedRelation(
    PrestoSqlParser::ParenthesizedRelationContext* ctx) {
  trace("visitParenthesizedRelation");
  return visitTyped<Relation>(ctx->relation());
}

std::any AstBuilder::visitExpression(PrestoSqlParser::ExpressionContext* ctx) {
  trace("visitExpression");
  return visitChildren("visitExpression", ctx);
}

std::any AstBuilder::visitLogicalNot(PrestoSqlParser::LogicalNotContext* ctx) {
  trace("visitLogicalNot");
  return std::static_pointer_cast<Expression>(std::make_shared<NotExpression>(
      getLocation(ctx), visitExpression(ctx->booleanExpression())));
}

std::any AstBuilder::visitPredicated(PrestoSqlParser::PredicatedContext* ctx) {
  trace("visitPredicated");

  if (ctx->predicate() != nullptr) {
    return visitExpression(ctx->predicate());
  }

  return visitExpression(ctx->valueExpression());
}

std::any AstBuilder::visitLogicalBinary(
    PrestoSqlParser::LogicalBinaryContext* ctx) {
  trace("visitLogicalBinary");

  auto leftExpr = visitExpression(ctx->left);
  auto rightExpr = visitExpression(ctx->right);

  LogicalBinaryExpression::Operator op = ctx->AND() != nullptr
      ? LogicalBinaryExpression::Operator::kAnd
      : LogicalBinaryExpression::Operator::kOr;

  return std::static_pointer_cast<Expression>(
      std::make_shared<LogicalBinaryExpression>(
          getLocation(ctx), op, leftExpr, rightExpr));
}

namespace {

ComparisonExpression::Operator toComparisonOperator(size_t tokenType) {
  switch (tokenType) {
    case PrestoSqlParser::EQ:
      return ComparisonExpression::Operator::kEqual;
    case PrestoSqlParser::NEQ:
      return ComparisonExpression::Operator::kNotEqual;
    case PrestoSqlParser::LT:
      return ComparisonExpression::Operator::kLessThan;
    case PrestoSqlParser::LTE:
      return ComparisonExpression::Operator::kLessThanOrEqual;
    case PrestoSqlParser::GT:
      return ComparisonExpression::Operator::kGreaterThan;
    case PrestoSqlParser::GTE:
      return ComparisonExpression::Operator::kGreaterThanOrEqual;
    default:
      throw std::runtime_error(
          "Unsupported comparison operator: " + std::to_string(tokenType));
  }
}

} // anonymous namespace

std::any AstBuilder::visitComparison(PrestoSqlParser::ComparisonContext* ctx) {
  trace("visitComparison");

  auto leftExpr = visitExpression(ctx->value);
  auto rightExpr = visitExpression(ctx->right);

  auto operatorToken = ctx->comparisonOperator()->children[0];
  auto terminalNode = dynamic_cast<antlr4::tree::TerminalNode*>(operatorToken);
  auto op = toComparisonOperator(terminalNode->getSymbol()->getType());

  return std::static_pointer_cast<Expression>(
      std::make_shared<ComparisonExpression>(
          getLocation(ctx), op, leftExpr, rightExpr));
}

std::any AstBuilder::visitQuantifiedComparison(
    PrestoSqlParser::QuantifiedComparisonContext* ctx) {
  trace("visitQuantifiedComparison");
  return visitChildren("visitQuantifiedComparison", ctx);
}

namespace {
ExpressionPtr wrapInNot(
    const ExpressionPtr& expr,
    antlr4::tree::TerminalNode* notNode) {
  if (notNode != nullptr) {
    return std::static_pointer_cast<Expression>(
        std::make_shared<NotExpression>(expr->location(), expr));
  }

  return expr;
}
} // namespace

std::any AstBuilder::visitBetween(PrestoSqlParser::BetweenContext* ctx) {
  trace("visitBetween");

  auto between = std::make_shared<BetweenPredicate>(
      getLocation(ctx),
      visitExpression(ctx->value),
      visitExpression(ctx->lower),
      visitExpression(ctx->upper));

  return wrapInNot(between, ctx->NOT());
}

std::any AstBuilder::visitInList(PrestoSqlParser::InListContext* ctx) {
  trace("visitInList");

  auto inPredicate = std::make_shared<InPredicate>(
      getLocation(ctx),
      visitTyped<Expression>(ctx->value),
      std::make_shared<InListExpression>(
          getLocation(ctx), visitTyped<Expression>(ctx->expression())));

  return wrapInNot(inPredicate, ctx->NOT());
}

std::any AstBuilder::visitInSubquery(PrestoSqlParser::InSubqueryContext* ctx) {
  trace("visitInSubquery");

  auto inPredicate =
      std::static_pointer_cast<Expression>(std::make_shared<InPredicate>(
          getLocation(ctx),
          visitTyped<Expression>(ctx->value),
          std::make_shared<SubqueryExpression>(
              getLocation(ctx), visitTyped<Statement>(ctx->query()))));

  return wrapInNot(inPredicate, ctx->NOT());
}

std::any AstBuilder::visitLike(PrestoSqlParser::LikeContext* ctx) {
  trace("visitLike");

  auto like = std::make_shared<LikePredicate>(
      getLocation(ctx),
      visitTyped<Expression>(ctx->value),
      visitTyped<Expression>(ctx->pattern),
      visitTyped<Expression>(ctx->escape));

  return wrapInNot(like, ctx->NOT());
}

std::any AstBuilder::visitNullPredicate(
    PrestoSqlParser::NullPredicateContext* ctx) {
  trace("visitNullPredicate");

  if (ctx->NOT()) {
    return std::static_pointer_cast<Expression>(
        std::make_shared<IsNotNullPredicate>(
            getLocation(ctx), visitTyped<Expression>(ctx->value)));
  }

  return std::static_pointer_cast<Expression>(std::make_shared<IsNullPredicate>(
      getLocation(ctx), visitTyped<Expression>(ctx->value)));
}

std::any AstBuilder::visitDistinctFrom(
    PrestoSqlParser::DistinctFromContext* ctx) {
  trace("visitDistinctFrom");

  auto leftExpr = visitExpression(ctx->value);
  auto rightExpr = visitExpression(ctx->right);

  auto distinctFrom = std::static_pointer_cast<Expression>(
      std::make_shared<ComparisonExpression>(
          getLocation(ctx),
          ComparisonExpression::Operator::kIsDistinctFrom,
          leftExpr,
          rightExpr));

  return wrapInNot(distinctFrom, ctx->NOT());
}

std::any AstBuilder::visitValueExpressionDefault(
    PrestoSqlParser::ValueExpressionDefaultContext* ctx) {
  trace("visitValueExpressionDefault");
  return visitChildren("visitValueExpressionDefault", ctx);
}

std::any AstBuilder::visitConcatenation(
    PrestoSqlParser::ConcatenationContext* ctx) {
  trace("visitConcatenation");

  return std::static_pointer_cast<Expression>(std::make_shared<FunctionCall>(
      getLocation(ctx),
      std::make_shared<QualifiedName>(
          getLocation(ctx), std::vector<std::string>{"concat"}),
      visitTyped<Expression>(ctx->valueExpression())));
}

namespace {
ArithmeticBinaryExpression::Operator toArithmeticBinaryOperator(
    size_t tokenType) {
  switch (tokenType) {
    case PrestoSqlParser::PLUS:
      return ArithmeticBinaryExpression::Operator::kAdd;
    case PrestoSqlParser::MINUS:
      return ArithmeticBinaryExpression::Operator::kSubtract;
    case PrestoSqlParser::ASTERISK:
      return ArithmeticBinaryExpression::Operator::kMultiply;
    case PrestoSqlParser::SLASH:
      return ArithmeticBinaryExpression::Operator::kDivide;
    case PrestoSqlParser::PERCENT:
      return ArithmeticBinaryExpression::Operator::kModulus;
    default:
      throw std::runtime_error(
          "Unsupported arithmetic operator: " + std::to_string(tokenType));
  }
}

} // anonymous namespace

std::any AstBuilder::visitArithmeticBinary(
    PrestoSqlParser::ArithmeticBinaryContext* ctx) {
  trace("visitArithmeticBinary");

  auto leftExpr = visitExpression(ctx->left);
  auto rightExpr = visitExpression(ctx->right);

  auto op = toArithmeticBinaryOperator(ctx->op->getType());

  return std::static_pointer_cast<Expression>(
      std::make_shared<ArithmeticBinaryExpression>(
          getLocation(ctx), op, leftExpr, rightExpr));
}

namespace {
ArithmeticUnaryExpression::Sign toArithmeticSign(size_t tokenType) {
  switch (tokenType) {
    case PrestoSqlParser::PLUS:
      return ArithmeticUnaryExpression::Sign::kPlus;
    case PrestoSqlParser::MINUS:
      return ArithmeticUnaryExpression::Sign::kMinus;
    default:
      throw std::runtime_error(
          "Unsupported sign: " + std::to_string(tokenType));
  }
}

} // namespace

std::any AstBuilder::visitArithmeticUnary(
    PrestoSqlParser::ArithmeticUnaryContext* ctx) {
  trace("visitArithmeticUnary");

  auto expr = visitExpression(ctx->valueExpression());

  return std::static_pointer_cast<Expression>(
      std::make_shared<ArithmeticUnaryExpression>(
          getLocation(ctx), toArithmeticSign(ctx->op->getType()), expr));
}

std::any AstBuilder::visitAtTimeZone(PrestoSqlParser::AtTimeZoneContext* ctx) {
  trace("visitAtTimeZone");

  auto value = visitExpression(ctx->valueExpression());
  auto timeZone = visitExpression(ctx->timeZoneSpecifier());

  return std::static_pointer_cast<Expression>(
      std::make_shared<AtTimeZone>(getLocation(ctx), value, timeZone));
}

std::any AstBuilder::visitDereference(
    PrestoSqlParser::DereferenceContext* ctx) {
  trace("visitDereference");

  return std::static_pointer_cast<Expression>(
      std::make_shared<DereferenceExpression>(
          getLocation(ctx),
          visitExpression(ctx->base),
          visitIdentifier(ctx->fieldName)));
}

namespace {
bool equalsIgnoreCase(std::string_view left, std::string_view right) {
  if (left.size() != right.size()) {
    return false;
  }

  const auto n = left.size();
  for (auto i = 0; i < n; ++i) {
    if (std::toupper(left[i]) != std::toupper(right[i])) {
      return false;
    }
  }

  return true;
}

} // namespace

std::any AstBuilder::visitTypeConstructor(
    PrestoSqlParser::TypeConstructorContext* ctx) {
  trace("visitTypeConstructor");

  auto value = visitExpression(ctx->string())->as<StringLiteral>()->value();

  if (ctx->DOUBLE_PRECISION() != nullptr) {
    return std::static_pointer_cast<Expression>(
        std::make_shared<GenericLiteral>(
            getLocation(ctx),
            std::make_shared<TypeSignature>(getLocation(ctx), "DOUBLE"),
            value));
  }

  const auto type = toTypeSignature(ctx->type());
  const auto& baseName = type->baseName();

  if (equalsIgnoreCase(baseName, "time")) {
    return std::static_pointer_cast<Expression>(
        std::make_shared<TimeLiteral>(getLocation(ctx), value));
  }

  if (equalsIgnoreCase(baseName, "timestamp")) {
    return std::static_pointer_cast<Expression>(
        std::make_shared<TimestampLiteral>(getLocation(ctx), value));
  }

  if (equalsIgnoreCase(baseName, "decimal")) {
    return std::static_pointer_cast<Expression>(
        std::make_shared<DecimalLiteral>(getLocation(ctx), value));
  }

  if (equalsIgnoreCase(baseName, "char")) {
    return std::static_pointer_cast<Expression>(
        std::make_shared<CharLiteral>(getLocation(ctx), value));
  }

  return std::static_pointer_cast<Expression>(
      std::make_shared<GenericLiteral>(getLocation(ctx), type, value));
}

namespace {
CurrentTime::Function toCurrentTimeFunction(size_t tokenType) {
  switch (tokenType) {
    case PrestoSqlParser::CURRENT_DATE:
      return CurrentTime::Function::kDate;
    case PrestoSqlParser::CURRENT_TIME:
      return CurrentTime::Function::kTime;
    case PrestoSqlParser::CURRENT_TIMESTAMP:
      return CurrentTime::Function::kTimestamp;
    case PrestoSqlParser::LOCALTIME:
      return CurrentTime::Function::kLocaltime;
    case PrestoSqlParser::LOCALTIMESTAMP:
      return CurrentTime::Function::kLocaltimestamp;
    default:
      VELOX_UNREACHABLE();
  }
}
} // namespace

std::any AstBuilder::visitSpecialDateTimeFunction(
    PrestoSqlParser::SpecialDateTimeFunctionContext* ctx) {
  trace("visitSpecialDateTimeFunction");

  auto function = toCurrentTimeFunction(ctx->name->getType());

  std::optional<int> precision;
  if (ctx->precision != nullptr) {
    precision = std::stoi(ctx->precision->getText());
  }

  return std::static_pointer_cast<Expression>(
      std::make_shared<CurrentTime>(getLocation(ctx), function, precision));
}

std::any AstBuilder::visitSubstring(PrestoSqlParser::SubstringContext* ctx) {
  trace("visitSubstring");

  return std::static_pointer_cast<Expression>(std::make_shared<FunctionCall>(
      getLocation(ctx),
      std::make_shared<QualifiedName>(
          getLocation(ctx), std::vector<std::string>{"substr"}),
      visitTyped<Expression>(ctx->valueExpression())));
}

std::any AstBuilder::visitCast(PrestoSqlParser::CastContext* ctx) {
  trace("visitCast");

  const bool isTryCast = ctx->TRY_CAST() != nullptr;

  return std::static_pointer_cast<Expression>(std::make_shared<Cast>(
      getLocation(ctx),
      visitTyped<Expression>(ctx->expression()),
      toTypeSignature(ctx->type()),
      isTryCast));
}

std::any AstBuilder::visitLambda(PrestoSqlParser::LambdaContext* ctx) {
  trace("visitLambda");

  std::vector<std::shared_ptr<LambdaArgumentDeclaration>> arguments;
  arguments.reserve(ctx->identifier().size());
  for (auto* identifierCtx : ctx->identifier()) {
    arguments.emplace_back(
        std::make_shared<LambdaArgumentDeclaration>(
            getLocation(identifierCtx), visitIdentifier(identifierCtx)));
  }

  auto body = visitTyped<Expression>(ctx->expression());

  return std::static_pointer_cast<Expression>(
      std::make_shared<LambdaExpression>(getLocation(ctx), arguments, body));
}

std::any AstBuilder::visitParenthesizedExpression(
    PrestoSqlParser::ParenthesizedExpressionContext* ctx) {
  trace("visitParenthesizedExpression");
  return visit(ctx->expression());
}

std::any AstBuilder::visitParameter(PrestoSqlParser::ParameterContext* ctx) {
  trace("visitParameter");
  return visitChildren("visitParameter", ctx);
}

std::any AstBuilder::visitNormalize(PrestoSqlParser::NormalizeContext* ctx) {
  trace("visitNormalize");
  return visitChildren("visitNormalize", ctx);
}

std::any AstBuilder::visitIntervalLiteral(
    PrestoSqlParser::IntervalLiteralContext* ctx) {
  trace("visitIntervalLiteral");
  return visitChildren("visitIntervalLiteral", ctx);
}

std::any AstBuilder::visitNumericLiteral(
    PrestoSqlParser::NumericLiteralContext* ctx) {
  trace("visitNumericLiteral");
  return visitChildren("visitNumericLiteral", ctx);
}

std::any AstBuilder::visitBooleanLiteral(
    PrestoSqlParser::BooleanLiteralContext* ctx) {
  trace("visitBooleanLiteral");
  return visitChildren("visitBooleanLiteral", ctx);
}

std::any AstBuilder::visitSimpleCase(PrestoSqlParser::SimpleCaseContext* ctx) {
  trace("visitSimpleCase");

  return std::static_pointer_cast<Expression>(
      std::make_shared<SimpleCaseExpression>(
          getLocation(ctx),
          visitTyped<Expression>(ctx->valueExpression()),
          visitTyped<WhenClause>(ctx->whenClause()),
          visitTyped<Expression>(ctx->elseExpression)));
}

std::any AstBuilder::visitColumnReference(
    PrestoSqlParser::ColumnReferenceContext* ctx) {
  trace("visitColumnReference");
  return std::static_pointer_cast<Expression>(
      visitIdentifier(ctx->identifier()));
}

std::any AstBuilder::visitNullLiteral(
    PrestoSqlParser::NullLiteralContext* ctx) {
  trace("visitNullLiteral");

  return std::static_pointer_cast<Expression>(
      std::make_shared<NullLiteral>(getLocation(ctx)));
}

std::any AstBuilder::visitRowConstructor(
    PrestoSqlParser::RowConstructorContext* ctx) {
  trace("visitRowConstructor");
  return std::static_pointer_cast<Expression>(std::make_shared<Row>(
      getLocation(ctx), visitTyped<Expression>(ctx->expression())));
}

std::any AstBuilder::visitSubscript(PrestoSqlParser::SubscriptContext* ctx) {
  trace("visitSubscript");
  return std::static_pointer_cast<Expression>(
      std::make_shared<SubscriptExpression>(
          getLocation(ctx),
          visitTyped<Expression>(ctx->primaryExpression()),
          visitTyped<Expression>(ctx->valueExpression())));
}

std::any AstBuilder::visitSubqueryExpression(
    PrestoSqlParser::SubqueryExpressionContext* ctx) {
  trace("visitSubqueryExpression");

  return std::static_pointer_cast<Expression>(
      std::make_shared<SubqueryExpression>(
          getLocation(ctx), visitTyped<Statement>(ctx->query())));
}

std::any AstBuilder::visitBinaryLiteral(
    PrestoSqlParser::BinaryLiteralContext* ctx) {
  trace("visitBinaryLiteral");
  return visitChildren("visitBinaryLiteral", ctx);
}

std::any AstBuilder::visitCurrentUser(
    PrestoSqlParser::CurrentUserContext* ctx) {
  trace("visitCurrentUser");
  return visitChildren("visitCurrentUser", ctx);
}

namespace {
Extract::Field toField(std::string_view name) {
  if (equalsIgnoreCase(name, "YEAR")) {
    return Extract::Field::kYear;
  }

  if (equalsIgnoreCase(name, "QUARTER")) {
    return Extract::Field::kQuarter;
  }

  if (equalsIgnoreCase(name, "MONTH")) {
    return Extract::Field::kMonth;
  }

  if (equalsIgnoreCase(name, "WEEK")) {
    return Extract::Field::kWeek;
  }

  if (equalsIgnoreCase(name, "DAY")) {
    return Extract::Field::kDay;
  }

  if (equalsIgnoreCase(name, "DAY_OF_MONTH")) {
    return Extract::Field::kDayOfMonth;
  }

  if (equalsIgnoreCase(name, "DAY_OF_WEEK")) {
    return Extract::Field::kDayOfWeek;
  }

  if (equalsIgnoreCase(name, "DOW")) {
    return Extract::Field::kDow;
  }

  if (equalsIgnoreCase(name, "DAY_OF_YEAR")) {
    return Extract::Field::kDayOfYear;
  }

  if (equalsIgnoreCase(name, "DOY")) {
    return Extract::Field::kDoy;
  }

  if (equalsIgnoreCase(name, "YEAR_OF_WEEK")) {
    return Extract::Field::kYearOfWeek;
  }

  if (equalsIgnoreCase(name, "YOW")) {
    return Extract::Field::kYow;
  }

  if (equalsIgnoreCase(name, "HOUR")) {
    return Extract::Field::kHour;
  }

  if (equalsIgnoreCase(name, "MINUTE")) {
    return Extract::Field::kMinute;
  }

  if (equalsIgnoreCase(name, "SECOND")) {
    return Extract::Field::kSecond;
  }

  if (equalsIgnoreCase(name, "TIMEZONE_HOUR")) {
    return Extract::Field::kTimezoneHour;
  }

  if (equalsIgnoreCase(name, "TIMEZONE_MINUTE")) {
    return Extract::Field::kTimezoneMinute;
  }

  throw std::runtime_error(fmt::format("Invalid EXTRACT field: {}", name));
}
} // namespace

std::any AstBuilder::visitExtract(PrestoSqlParser::ExtractContext* ctx) {
  trace("visitExtract");

  const auto field = visitIdentifier(ctx->identifier())->value();

  return std::static_pointer_cast<Expression>(std::make_shared<Extract>(
      getLocation(ctx),
      visitExpression(ctx->valueExpression()),
      toField(field)));
}

std::any AstBuilder::visitStringLiteral(
    PrestoSqlParser::StringLiteralContext* ctx) {
  trace("visitStringLiteral");
  return visitChildren("visitStringLiteral", ctx);
}

std::any AstBuilder::visitArrayConstructor(
    PrestoSqlParser::ArrayConstructorContext* ctx) {
  trace("visitArrayConstructor");
  return std::static_pointer_cast<Expression>(
      std::make_shared<ArrayConstructor>(
          getLocation(ctx), visitTyped<Expression>(ctx->expression())));
}

std::any AstBuilder::visitFunctionCall(
    PrestoSqlParser::FunctionCallContext* ctx) {
  trace("visitFunctionCall");

  auto name = getQualifiedName(ctx->qualifiedName());

  auto filter = visitTyped<Expression>(ctx->filter());

  OrderByPtr orderBy;
  if (ctx->ORDER() != nullptr) {
    orderBy = std::make_shared<OrderBy>(
        getLocation(ctx->ORDER()), visitTyped<SortItem>(ctx->sortItem()));
  }

  const bool ignoreNulls = ctx->nullTreatment() != nullptr &&
      ctx->nullTreatment()->IGNORE() != nullptr;

  auto args = visitTyped<Expression>(ctx->expression());

  std::shared_ptr<Window> window;
  if (ctx->over() != nullptr) {
    window = visitTyped<Window>(ctx->over());
  }

  return std::static_pointer_cast<Expression>(std::make_shared<FunctionCall>(
      getLocation(ctx),
      name,
      window,
      filter,
      orderBy,
      isDistinct(ctx),
      ignoreNulls,
      args));
}

std::any AstBuilder::visitExists(PrestoSqlParser::ExistsContext* ctx) {
  trace("visitExists");

  return std::static_pointer_cast<Expression>(std::make_shared<ExistsPredicate>(
      getLocation(ctx),
      std::make_shared<SubqueryExpression>(
          getLocation(ctx), visitTyped<Statement>(ctx->query()))));
}

std::any AstBuilder::visitPosition(PrestoSqlParser::PositionContext* ctx) {
  trace("visitPosition");

  // POSITION(x IN y) is equivalent to strpos(y, x)
  return std::static_pointer_cast<Expression>(std::make_shared<FunctionCall>(
      getLocation(ctx),
      std::make_shared<QualifiedName>(
          getLocation(ctx), std::vector<std::string>{"strpos"}),
      std::vector<std::shared_ptr<Expression>>{
          visitTyped<Expression>(ctx->valueExpression(1)),
          visitTyped<Expression>(ctx->valueExpression(0))}));
}

std::any AstBuilder::visitSearchedCase(
    PrestoSqlParser::SearchedCaseContext* ctx) {
  trace("visitSearchedCase");

  return std::static_pointer_cast<Expression>(
      std::make_shared<SearchedCaseExpression>(
          getLocation(ctx),
          visitTyped<WhenClause>(ctx->whenClause()),
          visitTyped<Expression>(ctx->elseExpression)));
}

std::any AstBuilder::visitGroupingOperation(
    PrestoSqlParser::GroupingOperationContext* ctx) {
  trace("visitGroupingOperation");

  std::vector<std::shared_ptr<QualifiedName>> groupingColumns;
  groupingColumns.reserve(ctx->qualifiedName().size());
  for (auto* qnCtx : ctx->qualifiedName()) {
    groupingColumns.push_back(getQualifiedName(qnCtx));
  }
  return std::static_pointer_cast<Expression>(
      std::make_shared<GroupingOperation>(getLocation(ctx), groupingColumns));
}

std::any AstBuilder::visitBasicStringLiteral(
    PrestoSqlParser::BasicStringLiteralContext* ctx) {
  trace("visitBasicStringLiteral");
  return std::static_pointer_cast<Expression>(std::make_shared<StringLiteral>(
      getLocation(ctx), unquote(ctx->STRING()->getText())));
}

std::any AstBuilder::visitUnicodeStringLiteral(
    PrestoSqlParser::UnicodeStringLiteralContext* ctx) {
  trace("visitUnicodeStringLiteral");
  return std::static_pointer_cast<Expression>(std::make_shared<StringLiteral>(
      getLocation(ctx), decodeUnicodeLiteral(*ctx)));
}

std::any AstBuilder::visitNullTreatment(
    PrestoSqlParser::NullTreatmentContext* ctx) {
  trace("visitNullTreatment");
  return visitChildren("visitNullTreatment", ctx);
}

std::any AstBuilder::visitTimeZoneInterval(
    PrestoSqlParser::TimeZoneIntervalContext* ctx) {
  trace("visitTimeZoneInterval");
  return visitChildren("visitTimeZoneInterval", ctx);
}

std::any AstBuilder::visitTimeZoneString(
    PrestoSqlParser::TimeZoneStringContext* ctx) {
  trace("visitTimeZoneString");
  return visitExpression(ctx->string());
}

std::any AstBuilder::visitComparisonOperator(
    PrestoSqlParser::ComparisonOperatorContext* ctx) {
  trace("visitComparisonOperator");
  return visitChildren("visitComparisonOperator", ctx);
}

std::any AstBuilder::visitComparisonQuantifier(
    PrestoSqlParser::ComparisonQuantifierContext* ctx) {
  trace("visitComparisonQuantifier");
  return visitChildren("visitComparisonQuantifier", ctx);
}

std::any AstBuilder::visitBooleanValue(
    PrestoSqlParser::BooleanValueContext* ctx) {
  trace("visitBooleanValue");

  return std::static_pointer_cast<Expression>(std::make_shared<BooleanLiteral>(
      getLocation(ctx), ctx->TRUE() != nullptr));
}

namespace {
IntervalLiteral::Sign toIntervalSign(antlr4::Token* token) {
  if (token == nullptr) {
    return IntervalLiteral::Sign::kPositive;
  }

  switch (token->getType()) {
    case PrestoSqlParser::PLUS:
      return IntervalLiteral::Sign::kPositive;
    case PrestoSqlParser::MINUS:
      return IntervalLiteral::Sign::kNegative;
    default:
      VELOX_USER_FAIL("Unsupported sign: {}", token->getText());
  }
}

IntervalLiteral::IntervalField toIntervalField(antlr4::Token* token) {
  switch (token->getType()) {
    case PrestoSqlParser::YEAR:
      return IntervalLiteral::IntervalField::kYear;
    case PrestoSqlParser::MONTH:
      return IntervalLiteral::IntervalField::kMonth;
    case PrestoSqlParser::DAY:
      return IntervalLiteral::IntervalField::kDay;
    case PrestoSqlParser::HOUR:
      return IntervalLiteral::IntervalField::kHour;
    case PrestoSqlParser::MINUTE:
      return IntervalLiteral::IntervalField::kMinute;
    case PrestoSqlParser::SECOND:
      return IntervalLiteral::IntervalField::kSecond;
    default:
      VELOX_USER_FAIL("Unsupported interval field: {}", token->getText());
  }
}
} // namespace

std::any AstBuilder::visitInterval(PrestoSqlParser::IntervalContext* ctx) {
  trace("visitInterval");

  std::optional<IntervalLiteral::IntervalField> to;
  if (ctx->to != nullptr) {
    to = toIntervalField(ctx->to->start);
  }

  return std::static_pointer_cast<Expression>(std::make_shared<IntervalLiteral>(
      getLocation(ctx),
      visitExpression(ctx->string())->as<StringLiteral>()->value(),
      toIntervalSign(ctx->sign),
      toIntervalField(ctx->from->start),
      to));
}

std::any AstBuilder::visitIntervalField(
    PrestoSqlParser::IntervalFieldContext* ctx) {
  trace("visitIntervalField");
  return visitChildren("visitIntervalField", ctx);
}

std::any AstBuilder::visitNormalForm(PrestoSqlParser::NormalFormContext* ctx) {
  trace("visitNormalForm");
  return visitChildren("visitNormalForm", ctx);
}

std::any AstBuilder::visitTypes(PrestoSqlParser::TypesContext* ctx) {
  trace("visitTypes");
  return visitChildren("visitTypes", ctx);
}

std::any AstBuilder::visitType(PrestoSqlParser::TypeContext* ctx) {
  trace("visitType");
  return visitChildren("visitType", ctx);
}

std::any AstBuilder::visitTypeParameter(
    PrestoSqlParser::TypeParameterContext* ctx) {
  trace("visitTypeParameter");
  return visitChildren("visitTypeParameter", ctx);
}

std::any AstBuilder::visitBaseType(PrestoSqlParser::BaseTypeContext* ctx) {
  trace("visitBaseType");
  return visitChildren("visitBaseType", ctx);
}

std::any AstBuilder::visitWhenClause(PrestoSqlParser::WhenClauseContext* ctx) {
  trace("visitWhenClause");

  return std::make_shared<WhenClause>(
      getLocation(ctx),
      visitTyped<Expression>(ctx->condition),
      visitTyped<Expression>(ctx->result));
}

std::any AstBuilder::visitFilter(PrestoSqlParser::FilterContext* ctx) {
  trace("visitFilter");
  return visit(ctx->booleanExpression());
}

std::any AstBuilder::visitOver(PrestoSqlParser::OverContext* ctx) {
  trace("visitOver");

  auto partitionBy = visitTyped<Expression>(ctx->partition);

  std::shared_ptr<OrderBy> orderBy;
  if (!ctx->sortItem().empty()) {
    orderBy = std::make_shared<OrderBy>(
        getLocation(ctx), visitTyped<SortItem>(ctx->sortItem()));
  }

  std::shared_ptr<WindowFrame> frame;
  if (ctx->windowFrame() != nullptr) {
    frame = visitTyped<WindowFrame>(ctx->windowFrame());
  }

  return std::make_shared<Window>(
      getLocation(ctx), partitionBy, orderBy, frame);
}

std::any AstBuilder::visitWindowFrame(
    PrestoSqlParser::WindowFrameContext* ctx) {
  trace("visitWindowFrame");

  WindowFrame::Type frameType;
  if (ctx->frameType->getType() == PrestoSqlParser::RANGE) {
    frameType = WindowFrame::Type::kRange;
  } else if (ctx->frameType->getType() == PrestoSqlParser::ROWS) {
    frameType = WindowFrame::Type::kRows;
  } else {
    VELOX_CHECK(ctx->frameType->getType() == PrestoSqlParser::GROUPS);
    frameType = WindowFrame::Type::kGroups;
  }

  auto start = visitTyped<FrameBound>(ctx->start);

  std::shared_ptr<FrameBound> end;
  if (ctx->end != nullptr) {
    end = visitTyped<FrameBound>(ctx->end);
  }

  return std::make_shared<WindowFrame>(getLocation(ctx), frameType, start, end);
}

std::any AstBuilder::visitUnboundedFrame(
    PrestoSqlParser::UnboundedFrameContext* ctx) {
  trace("visitUnboundedFrame");

  auto boundType = ctx->boundType->getType() == PrestoSqlParser::PRECEDING
      ? FrameBound::Type::kUnboundedPreceding
      : FrameBound::Type::kUnboundedFollowing;

  return std::make_shared<FrameBound>(getLocation(ctx), boundType);
}

std::any AstBuilder::visitCurrentRowBound(
    PrestoSqlParser::CurrentRowBoundContext* ctx) {
  trace("visitCurrentRowBound");

  return std::make_shared<FrameBound>(
      getLocation(ctx), FrameBound::Type::kCurrentRow);
}

std::any AstBuilder::visitBoundedFrame(
    PrestoSqlParser::BoundedFrameContext* ctx) {
  trace("visitBoundedFrame");

  auto boundType = ctx->boundType->getType() == PrestoSqlParser::PRECEDING
      ? FrameBound::Type::kPreceding
      : FrameBound::Type::kFollowing;

  auto value = visitTyped<Expression>(ctx->expression());

  return std::make_shared<FrameBound>(getLocation(ctx), boundType, value);
}

std::any AstBuilder::visitUpdateAssignment(
    PrestoSqlParser::UpdateAssignmentContext* ctx) {
  trace("visitUpdateAssignment");
  return visitChildren("visitUpdateAssignment", ctx);
}

std::any AstBuilder::visitExplainFormat(
    PrestoSqlParser::ExplainFormatContext* ctx) {
  trace("visitExplainFormat");
  return visitChildren("visitExplainFormat", ctx);
}

ExplainType::Type toExplainType(PrestoSqlParser::ExplainTypeContext* ctx) {
  switch (ctx->value->getType()) {
    case PrestoSqlParser::IO:
      return ExplainType::Type::kIo;
    case PrestoSqlParser::LOGICAL:
      return ExplainType::Type::kLogical;
    case PrestoSqlParser::GRAPH:
      return ExplainType::Type::kGraph;
    case PrestoSqlParser::OPTIMIZED:
      return ExplainType::Type::kOptimized;
    case PrestoSqlParser::EXECUTABLE:
      return ExplainType::Type::kExecutable;
    case PrestoSqlParser::DISTRIBUTED:
      return ExplainType::Type::kDistributed;
    case PrestoSqlParser::VALIDATE:
      return ExplainType::Type::kValidate;
    default:
      VELOX_USER_FAIL("Unsupported EXPLAIN type: {}", ctx->value->getText());
  }
}

std::any AstBuilder::visitExplainType(
    PrestoSqlParser::ExplainTypeContext* ctx) {
  trace("visitExplainType");
  return std::static_pointer_cast<ExplainOption>(
      std::make_shared<ExplainType>(getLocation(ctx), toExplainType(ctx)));
}

std::any AstBuilder::visitIsolationLevel(
    PrestoSqlParser::IsolationLevelContext* ctx) {
  trace("visitIsolationLevel");
  return visitChildren("visitIsolationLevel", ctx);
}

std::any AstBuilder::visitTransactionAccessMode(
    PrestoSqlParser::TransactionAccessModeContext* ctx) {
  trace("visitTransactionAccessMode");
  return visitChildren("visitTransactionAccessMode", ctx);
}

std::any AstBuilder::visitReadUncommitted(
    PrestoSqlParser::ReadUncommittedContext* ctx) {
  trace("visitReadUncommitted");
  return visitChildren("visitReadUncommitted", ctx);
}

std::any AstBuilder::visitReadCommitted(
    PrestoSqlParser::ReadCommittedContext* ctx) {
  trace("visitReadCommitted");
  return visitChildren("visitReadCommitted", ctx);
}

std::any AstBuilder::visitRepeatableRead(
    PrestoSqlParser::RepeatableReadContext* ctx) {
  trace("visitRepeatableRead");
  return visitChildren("visitRepeatableRead", ctx);
}

std::any AstBuilder::visitSerializable(
    PrestoSqlParser::SerializableContext* ctx) {
  trace("visitSerializable");
  return visitChildren("visitSerializable", ctx);
}

std::any AstBuilder::visitPositionalArgument(
    PrestoSqlParser::PositionalArgumentContext* ctx) {
  trace("visitPositionalArgument");
  return visitChildren("visitPositionalArgument", ctx);
}

std::any AstBuilder::visitNamedArgument(
    PrestoSqlParser::NamedArgumentContext* ctx) {
  trace("visitNamedArgument");
  return visitChildren("visitNamedArgument", ctx);
}

std::any AstBuilder::visitPrivilege(PrestoSqlParser::PrivilegeContext* ctx) {
  trace("visitPrivilege");
  return visitChildren("visitPrivilege", ctx);
}

std::any AstBuilder::visitQualifiedName(
    PrestoSqlParser::QualifiedNameContext* ctx) {
  trace("visitQualifiedName");
  return visitChildren("visitQualifiedName", ctx);
}

std::any AstBuilder::visitTableVersion(
    PrestoSqlParser::TableVersionContext* ctx) {
  trace("visitTableVersion");
  return visitChildren("visitTableVersion", ctx);
}

std::any AstBuilder::visitTableversionasof(
    PrestoSqlParser::TableversionasofContext* ctx) {
  trace("visitTableversionasof");
  return visitChildren("visitTableversionasof", ctx);
}

std::any AstBuilder::visitTableversionbefore(
    PrestoSqlParser::TableversionbeforeContext* ctx) {
  trace("visitTableversionbefore");
  return visitChildren("visitTableversionbefore", ctx);
}

std::any AstBuilder::visitCurrentUserGrantor(
    PrestoSqlParser::CurrentUserGrantorContext* ctx) {
  trace("visitCurrentUserGrantor");
  return visitChildren("visitCurrentUserGrantor", ctx);
}

std::any AstBuilder::visitCurrentRoleGrantor(
    PrestoSqlParser::CurrentRoleGrantorContext* ctx) {
  trace("visitCurrentRoleGrantor");
  return visitChildren("visitCurrentRoleGrantor", ctx);
}

std::any AstBuilder::visitSpecifiedPrincipal(
    PrestoSqlParser::SpecifiedPrincipalContext* ctx) {
  trace("visitSpecifiedPrincipal");
  return visitChildren("visitSpecifiedPrincipal", ctx);
}

std::any AstBuilder::visitUserPrincipal(
    PrestoSqlParser::UserPrincipalContext* ctx) {
  trace("visitUserPrincipal");
  return visitChildren("visitUserPrincipal", ctx);
}

std::any AstBuilder::visitRolePrincipal(
    PrestoSqlParser::RolePrincipalContext* ctx) {
  trace("visitRolePrincipal");
  return visitChildren("visitRolePrincipal", ctx);
}

std::any AstBuilder::visitUnspecifiedPrincipal(
    PrestoSqlParser::UnspecifiedPrincipalContext* ctx) {
  trace("visitUnspecifiedPrincipal");
  return visitChildren("visitUnspecifiedPrincipal", ctx);
}

std::any AstBuilder::visitRoles(PrestoSqlParser::RolesContext* ctx) {
  trace("visitRoles");
  return visitChildren("visitRoles", ctx);
}

static void
replaceAll(std::string& str, const std::string& from, const std::string& to) {
  auto pos = str.find(from);
  while (pos != std::string::npos) {
    str.replace(pos, from.length(), to);
    pos = str.find(from, pos + to.length());
  }
}

std::any AstBuilder::visitQuotedIdentifier(
    PrestoSqlParser::QuotedIdentifierContext* ctx) {
  trace("visitQuotedIdentifier");

  auto token = ctx->getText();
  token = token.substr(1, token.size() - 2);
  replaceAll(token, "\"\"", "\"");

  return std::make_shared<Identifier>(getLocation(ctx), token, true);
}

std::any AstBuilder::visitBackQuotedIdentifier(
    PrestoSqlParser::BackQuotedIdentifierContext* ctx) {
  trace("visitBackQuotedIdentifier");
  return visitChildren("visitBackQuotedIdentifier", ctx);
}

std::any AstBuilder::visitDigitIdentifier(
    PrestoSqlParser::DigitIdentifierContext* ctx) {
  trace("visitDigitIdentifier");
  return visitChildren("visitDigitIdentifier", ctx);
}

std::any AstBuilder::visitDecimalLiteral(
    PrestoSqlParser::DecimalLiteralContext* ctx) {
  trace("visitDecimalLiteral");

  // TODO Introduce ParsingOptions to allow parsing decimal as either double
  // or decimal.

  return std::static_pointer_cast<Expression>(std::make_shared<DoubleLiteral>(
      getLocation(ctx), std::stod(ctx->getText())));
}

std::any AstBuilder::visitDoubleLiteral(
    PrestoSqlParser::DoubleLiteralContext* ctx) {
  trace("visitDoubleLiteral");
  return std::static_pointer_cast<Expression>(std::make_shared<DoubleLiteral>(
      getLocation(ctx), std::stod(ctx->getText())));
}

std::any AstBuilder::visitIntegerLiteral(
    PrestoSqlParser::IntegerLiteralContext* ctx) {
  trace("visitIntegerLiteral");

  int64_t value = std::stoll(ctx->getText());

  return std::static_pointer_cast<Expression>(
      std::make_shared<LongLiteral>(getLocation(ctx), value));
}

std::any AstBuilder::visitConstraintSpecification(
    PrestoSqlParser::ConstraintSpecificationContext* ctx) {
  trace("visitConstraintSpecification");
  if (ctx->namedConstraintSpecification() != nullptr) {
    return visitNamedConstraintSpecification(
        ctx->namedConstraintSpecification());
  }
  if (ctx->unnamedConstraintSpecification() != nullptr) {
    return visitUnnamedConstraintSpecification(
        ctx->unnamedConstraintSpecification());
  }
  return visitChildren("visitConstraintSpecification", ctx);
}

std::any AstBuilder::visitNamedConstraintSpecification(
    PrestoSqlParser::NamedConstraintSpecificationContext* ctx) {
  trace("visitNamedConstraintSpecification");

  auto unnamed = ctx->unnamedConstraintSpecification();
  auto constraintType = unnamed->constraintType()->PRIMARY() != nullptr
      ? ConstraintSpecification::ConstraintType::kPrimaryKey
      : ConstraintSpecification::ConstraintType::kUnique;

  std::vector<std::shared_ptr<Identifier>> columns;
  for (auto* identCtx : unnamed->columnAliases()->identifier()) {
    columns.push_back(visitIdentifier(identCtx));
  }

  return std::static_pointer_cast<TableElement>(
      std::make_shared<ConstraintSpecification>(
          getLocation(ctx),
          visitIdentifier(ctx->name),
          std::move(columns),
          constraintType));
}

std::any AstBuilder::visitUnnamedConstraintSpecification(
    PrestoSqlParser::UnnamedConstraintSpecificationContext* ctx) {
  trace("visitUnnamedConstraintSpecification");

  auto constraintType = ctx->constraintType()->PRIMARY() != nullptr
      ? ConstraintSpecification::ConstraintType::kPrimaryKey
      : ConstraintSpecification::ConstraintType::kUnique;

  std::vector<std::shared_ptr<Identifier>> columns;
  for (auto* identCtx : ctx->columnAliases()->identifier()) {
    columns.push_back(visitIdentifier(identCtx));
  }

  return std::static_pointer_cast<TableElement>(
      std::make_shared<ConstraintSpecification>(
          getLocation(ctx), nullptr, std::move(columns), constraintType));
}

std::any AstBuilder::visitConstraintType(
    PrestoSqlParser::ConstraintTypeContext* ctx) {
  trace("visitConstraintType");
  return visitChildren("visitConstraintType", ctx);
}

std::any AstBuilder::visitConstraintQualifiers(
    PrestoSqlParser::ConstraintQualifiersContext* ctx) {
  trace("visitConstraintQualifiers");
  return visitChildren("visitConstraintQualifiers", ctx);
}

std::any AstBuilder::visitConstraintQualifier(
    PrestoSqlParser::ConstraintQualifierContext* ctx) {
  trace("visitConstraintQualifier");
  return visitChildren("visitConstraintQualifier", ctx);
}

std::any AstBuilder::visitConstraintRely(
    PrestoSqlParser::ConstraintRelyContext* ctx) {
  trace("visitConstraintRely");
  return visitChildren("visitConstraintRely", ctx);
}

std::any AstBuilder::visitConstraintEnabled(
    PrestoSqlParser::ConstraintEnabledContext* ctx) {
  trace("visitConstraintEnabled");
  return visitChildren("visitConstraintEnabled", ctx);
}

std::any AstBuilder::visitConstraintEnforced(
    PrestoSqlParser::ConstraintEnforcedContext* ctx) {
  trace("visitConstraintEnforced");
  return visitChildren("visitConstraintEnforced", ctx);
}

std::any AstBuilder::visitNonReserved(
    PrestoSqlParser::NonReservedContext* ctx) {
  trace("visitNonReserved");
  return visitChildren("visitNonReserved", ctx);
}

} // namespace axiom::sql::presto
