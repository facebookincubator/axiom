/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

// Generated from PrestoSql.g4 by ANTLR 4.9.3

#pragma once

#include "antlr4-runtime.h"
#include "axiom/sql/presto/PrestoSqlVisitor.h"

namespace facebook::velox::sql {

/**
 * This class provides an empty implementation of PrestoSqlVisitor, which can be
 * extended to create a visitor which only needs to handle a subset of the
 * available methods.
 */
class PrestoSqlBaseVisitor : public PrestoSqlVisitor {
 public:
  virtual antlrcpp::Any visitSingleStatement(
      PrestoSqlParser::SingleStatementContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitStandaloneExpression(
      PrestoSqlParser::StandaloneExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitStandaloneRoutineBody(
      PrestoSqlParser::StandaloneRoutineBodyContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitStatementDefault(
      PrestoSqlParser::StatementDefaultContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitUse(PrestoSqlParser::UseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCreateSchema(
      PrestoSqlParser::CreateSchemaContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDropSchema(
      PrestoSqlParser::DropSchemaContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRenameSchema(
      PrestoSqlParser::RenameSchemaContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCreateTableAsSelect(
      PrestoSqlParser::CreateTableAsSelectContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCreateTable(
      PrestoSqlParser::CreateTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDropTable(
      PrestoSqlParser::DropTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitInsertInto(
      PrestoSqlParser::InsertIntoContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDelete(
      PrestoSqlParser::DeleteContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTruncateTable(
      PrestoSqlParser::TruncateTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRenameTable(
      PrestoSqlParser::RenameTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRenameColumn(
      PrestoSqlParser::RenameColumnContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDropColumn(
      PrestoSqlParser::DropColumnContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAddColumn(
      PrestoSqlParser::AddColumnContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAddConstraint(
      PrestoSqlParser::AddConstraintContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDropConstraint(
      PrestoSqlParser::DropConstraintContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAlterColumnSetNotNull(
      PrestoSqlParser::AlterColumnSetNotNullContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAlterColumnDropNotNull(
      PrestoSqlParser::AlterColumnDropNotNullContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSetTableProperties(
      PrestoSqlParser::SetTablePropertiesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAnalyze(
      PrestoSqlParser::AnalyzeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCreateType(
      PrestoSqlParser::CreateTypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCreateView(
      PrestoSqlParser::CreateViewContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRenameView(
      PrestoSqlParser::RenameViewContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDropView(
      PrestoSqlParser::DropViewContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCreateMaterializedView(
      PrestoSqlParser::CreateMaterializedViewContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDropMaterializedView(
      PrestoSqlParser::DropMaterializedViewContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRefreshMaterializedView(
      PrestoSqlParser::RefreshMaterializedViewContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCreateFunction(
      PrestoSqlParser::CreateFunctionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAlterFunction(
      PrestoSqlParser::AlterFunctionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDropFunction(
      PrestoSqlParser::DropFunctionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCall(PrestoSqlParser::CallContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCreateRole(
      PrestoSqlParser::CreateRoleContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDropRole(
      PrestoSqlParser::DropRoleContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitGrantRoles(
      PrestoSqlParser::GrantRolesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRevokeRoles(
      PrestoSqlParser::RevokeRolesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSetRole(
      PrestoSqlParser::SetRoleContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitGrant(
      PrestoSqlParser::GrantContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRevoke(
      PrestoSqlParser::RevokeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitShowGrants(
      PrestoSqlParser::ShowGrantsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitExplain(
      PrestoSqlParser::ExplainContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitShowCreateTable(
      PrestoSqlParser::ShowCreateTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitShowCreateView(
      PrestoSqlParser::ShowCreateViewContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitShowCreateMaterializedView(
      PrestoSqlParser::ShowCreateMaterializedViewContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitShowCreateFunction(
      PrestoSqlParser::ShowCreateFunctionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitShowTables(
      PrestoSqlParser::ShowTablesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitShowSchemas(
      PrestoSqlParser::ShowSchemasContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitShowCatalogs(
      PrestoSqlParser::ShowCatalogsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitShowColumns(
      PrestoSqlParser::ShowColumnsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitShowStats(
      PrestoSqlParser::ShowStatsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitShowStatsForQuery(
      PrestoSqlParser::ShowStatsForQueryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitShowRoles(
      PrestoSqlParser::ShowRolesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitShowRoleGrants(
      PrestoSqlParser::ShowRoleGrantsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitShowFunctions(
      PrestoSqlParser::ShowFunctionsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitShowSession(
      PrestoSqlParser::ShowSessionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSetSession(
      PrestoSqlParser::SetSessionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitResetSession(
      PrestoSqlParser::ResetSessionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitStartTransaction(
      PrestoSqlParser::StartTransactionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCommit(
      PrestoSqlParser::CommitContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRollback(
      PrestoSqlParser::RollbackContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPrepare(
      PrestoSqlParser::PrepareContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDeallocate(
      PrestoSqlParser::DeallocateContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitExecute(
      PrestoSqlParser::ExecuteContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDescribeInput(
      PrestoSqlParser::DescribeInputContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDescribeOutput(
      PrestoSqlParser::DescribeOutputContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitUpdate(
      PrestoSqlParser::UpdateContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitQuery(
      PrestoSqlParser::QueryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitWith(PrestoSqlParser::WithContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTableElement(
      PrestoSqlParser::TableElementContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnDefinition(
      PrestoSqlParser::ColumnDefinitionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitLikeClause(
      PrestoSqlParser::LikeClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitProperties(
      PrestoSqlParser::PropertiesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitProperty(
      PrestoSqlParser::PropertyContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSqlParameterDeclaration(
      PrestoSqlParser::SqlParameterDeclarationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRoutineCharacteristics(
      PrestoSqlParser::RoutineCharacteristicsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRoutineCharacteristic(
      PrestoSqlParser::RoutineCharacteristicContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAlterRoutineCharacteristics(
      PrestoSqlParser::AlterRoutineCharacteristicsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAlterRoutineCharacteristic(
      PrestoSqlParser::AlterRoutineCharacteristicContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRoutineBody(
      PrestoSqlParser::RoutineBodyContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitReturnStatement(
      PrestoSqlParser::ReturnStatementContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitExternalBodyReference(
      PrestoSqlParser::ExternalBodyReferenceContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitLanguage(
      PrestoSqlParser::LanguageContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDeterminism(
      PrestoSqlParser::DeterminismContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitNullCallClause(
      PrestoSqlParser::NullCallClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitExternalRoutineName(
      PrestoSqlParser::ExternalRoutineNameContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitQueryNoWith(
      PrestoSqlParser::QueryNoWithContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitQueryTermDefault(
      PrestoSqlParser::QueryTermDefaultContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSetOperation(
      PrestoSqlParser::SetOperationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitQueryPrimaryDefault(
      PrestoSqlParser::QueryPrimaryDefaultContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTable(
      PrestoSqlParser::TableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitInlineTable(
      PrestoSqlParser::InlineTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSubquery(
      PrestoSqlParser::SubqueryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSortItem(
      PrestoSqlParser::SortItemContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitQuerySpecification(
      PrestoSqlParser::QuerySpecificationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitGroupBy(
      PrestoSqlParser::GroupByContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSingleGroupingSet(
      PrestoSqlParser::SingleGroupingSetContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRollup(
      PrestoSqlParser::RollupContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCube(PrestoSqlParser::CubeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitMultipleGroupingSets(
      PrestoSqlParser::MultipleGroupingSetsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitGroupingSet(
      PrestoSqlParser::GroupingSetContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitNamedQuery(
      PrestoSqlParser::NamedQueryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSetQuantifier(
      PrestoSqlParser::SetQuantifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSelectSingle(
      PrestoSqlParser::SelectSingleContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSelectAll(
      PrestoSqlParser::SelectAllContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRelationDefault(
      PrestoSqlParser::RelationDefaultContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitJoinRelation(
      PrestoSqlParser::JoinRelationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitJoinType(
      PrestoSqlParser::JoinTypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitJoinCriteria(
      PrestoSqlParser::JoinCriteriaContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSampledRelation(
      PrestoSqlParser::SampledRelationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSampleType(
      PrestoSqlParser::SampleTypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAliasedRelation(
      PrestoSqlParser::AliasedRelationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnAliases(
      PrestoSqlParser::ColumnAliasesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTableName(
      PrestoSqlParser::TableNameContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSubqueryRelation(
      PrestoSqlParser::SubqueryRelationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitUnnest(
      PrestoSqlParser::UnnestContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitLateral(
      PrestoSqlParser::LateralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitParenthesizedRelation(
      PrestoSqlParser::ParenthesizedRelationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitExpression(
      PrestoSqlParser::ExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitLogicalNot(
      PrestoSqlParser::LogicalNotContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPredicated(
      PrestoSqlParser::PredicatedContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitLogicalBinary(
      PrestoSqlParser::LogicalBinaryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitComparison(
      PrestoSqlParser::ComparisonContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitQuantifiedComparison(
      PrestoSqlParser::QuantifiedComparisonContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitBetween(
      PrestoSqlParser::BetweenContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitInList(
      PrestoSqlParser::InListContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitInSubquery(
      PrestoSqlParser::InSubqueryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitLike(PrestoSqlParser::LikeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitNullPredicate(
      PrestoSqlParser::NullPredicateContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDistinctFrom(
      PrestoSqlParser::DistinctFromContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitValueExpressionDefault(
      PrestoSqlParser::ValueExpressionDefaultContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitConcatenation(
      PrestoSqlParser::ConcatenationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitArithmeticBinary(
      PrestoSqlParser::ArithmeticBinaryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitArithmeticUnary(
      PrestoSqlParser::ArithmeticUnaryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAtTimeZone(
      PrestoSqlParser::AtTimeZoneContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDereference(
      PrestoSqlParser::DereferenceContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTypeConstructor(
      PrestoSqlParser::TypeConstructorContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSpecialDateTimeFunction(
      PrestoSqlParser::SpecialDateTimeFunctionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSubstring(
      PrestoSqlParser::SubstringContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCast(PrestoSqlParser::CastContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitLambda(
      PrestoSqlParser::LambdaContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitParenthesizedExpression(
      PrestoSqlParser::ParenthesizedExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitParameter(
      PrestoSqlParser::ParameterContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitNormalize(
      PrestoSqlParser::NormalizeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitIntervalLiteral(
      PrestoSqlParser::IntervalLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitNumericLiteral(
      PrestoSqlParser::NumericLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitBooleanLiteral(
      PrestoSqlParser::BooleanLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSimpleCase(
      PrestoSqlParser::SimpleCaseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnReference(
      PrestoSqlParser::ColumnReferenceContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitNullLiteral(
      PrestoSqlParser::NullLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRowConstructor(
      PrestoSqlParser::RowConstructorContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSubscript(
      PrestoSqlParser::SubscriptContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSubqueryExpression(
      PrestoSqlParser::SubqueryExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitBinaryLiteral(
      PrestoSqlParser::BinaryLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCurrentUser(
      PrestoSqlParser::CurrentUserContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitExtract(
      PrestoSqlParser::ExtractContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitStringLiteral(
      PrestoSqlParser::StringLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitArrayConstructor(
      PrestoSqlParser::ArrayConstructorContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitFunctionCall(
      PrestoSqlParser::FunctionCallContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitExists(
      PrestoSqlParser::ExistsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPosition(
      PrestoSqlParser::PositionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSearchedCase(
      PrestoSqlParser::SearchedCaseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitGroupingOperation(
      PrestoSqlParser::GroupingOperationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitBasicStringLiteral(
      PrestoSqlParser::BasicStringLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitUnicodeStringLiteral(
      PrestoSqlParser::UnicodeStringLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitNullTreatment(
      PrestoSqlParser::NullTreatmentContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTimeZoneInterval(
      PrestoSqlParser::TimeZoneIntervalContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTimeZoneString(
      PrestoSqlParser::TimeZoneStringContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitComparisonOperator(
      PrestoSqlParser::ComparisonOperatorContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitComparisonQuantifier(
      PrestoSqlParser::ComparisonQuantifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitBooleanValue(
      PrestoSqlParser::BooleanValueContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitInterval(
      PrestoSqlParser::IntervalContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitIntervalField(
      PrestoSqlParser::IntervalFieldContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitNormalForm(
      PrestoSqlParser::NormalFormContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTypes(
      PrestoSqlParser::TypesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitType(PrestoSqlParser::TypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTypeParameter(
      PrestoSqlParser::TypeParameterContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitBaseType(
      PrestoSqlParser::BaseTypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitWhenClause(
      PrestoSqlParser::WhenClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitFilter(
      PrestoSqlParser::FilterContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitOver(PrestoSqlParser::OverContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitWindowFrame(
      PrestoSqlParser::WindowFrameContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitUnboundedFrame(
      PrestoSqlParser::UnboundedFrameContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCurrentRowBound(
      PrestoSqlParser::CurrentRowBoundContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitBoundedFrame(
      PrestoSqlParser::BoundedFrameContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitUpdateAssignment(
      PrestoSqlParser::UpdateAssignmentContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitExplainFormat(
      PrestoSqlParser::ExplainFormatContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitExplainType(
      PrestoSqlParser::ExplainTypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitIsolationLevel(
      PrestoSqlParser::IsolationLevelContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTransactionAccessMode(
      PrestoSqlParser::TransactionAccessModeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitReadUncommitted(
      PrestoSqlParser::ReadUncommittedContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitReadCommitted(
      PrestoSqlParser::ReadCommittedContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRepeatableRead(
      PrestoSqlParser::RepeatableReadContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSerializable(
      PrestoSqlParser::SerializableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPositionalArgument(
      PrestoSqlParser::PositionalArgumentContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitNamedArgument(
      PrestoSqlParser::NamedArgumentContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPrivilege(
      PrestoSqlParser::PrivilegeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitQualifiedName(
      PrestoSqlParser::QualifiedNameContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTableVersion(
      PrestoSqlParser::TableVersionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTableversionasof(
      PrestoSqlParser::TableversionasofContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTableversionbefore(
      PrestoSqlParser::TableversionbeforeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCurrentUserGrantor(
      PrestoSqlParser::CurrentUserGrantorContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCurrentRoleGrantor(
      PrestoSqlParser::CurrentRoleGrantorContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSpecifiedPrincipal(
      PrestoSqlParser::SpecifiedPrincipalContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitUserPrincipal(
      PrestoSqlParser::UserPrincipalContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRolePrincipal(
      PrestoSqlParser::RolePrincipalContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitUnspecifiedPrincipal(
      PrestoSqlParser::UnspecifiedPrincipalContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRoles(
      PrestoSqlParser::RolesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitUnquotedIdentifier(
      PrestoSqlParser::UnquotedIdentifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitQuotedIdentifier(
      PrestoSqlParser::QuotedIdentifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitBackQuotedIdentifier(
      PrestoSqlParser::BackQuotedIdentifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDigitIdentifier(
      PrestoSqlParser::DigitIdentifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDecimalLiteral(
      PrestoSqlParser::DecimalLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDoubleLiteral(
      PrestoSqlParser::DoubleLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitIntegerLiteral(
      PrestoSqlParser::IntegerLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitConstraintSpecification(
      PrestoSqlParser::ConstraintSpecificationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitNamedConstraintSpecification(
      PrestoSqlParser::NamedConstraintSpecificationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitUnnamedConstraintSpecification(
      PrestoSqlParser::UnnamedConstraintSpecificationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitConstraintType(
      PrestoSqlParser::ConstraintTypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitConstraintQualifiers(
      PrestoSqlParser::ConstraintQualifiersContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitConstraintQualifier(
      PrestoSqlParser::ConstraintQualifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitConstraintRely(
      PrestoSqlParser::ConstraintRelyContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitConstraintEnabled(
      PrestoSqlParser::ConstraintEnabledContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitConstraintEnforced(
      PrestoSqlParser::ConstraintEnforcedContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitNonReserved(
      PrestoSqlParser::NonReservedContext* ctx) override {
    return visitChildren(ctx);
  }
};

} // namespace facebook::velox::sql
