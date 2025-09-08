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

// Generated from PrestoSql.g4 by ANTLR 4.13.2

#pragma once

#include "PrestoSqlVisitor.h"
#include "antlr4-runtime.h"

/**
 * This class provides an empty implementation of PrestoSqlVisitor, which can be
 * extended to create a visitor which only needs to handle a subset of the
 * available methods.
 */
class PrestoSqlBaseVisitor : public PrestoSqlVisitor {
 public:
  virtual std::any visitSingleStatement(
      PrestoSqlParser::SingleStatementContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitStandaloneExpression(
      PrestoSqlParser::StandaloneExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitStandaloneRoutineBody(
      PrestoSqlParser::StandaloneRoutineBodyContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitStatementDefault(
      PrestoSqlParser::StatementDefaultContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUse(PrestoSqlParser::UseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreateSchema(
      PrestoSqlParser::CreateSchemaContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDropSchema(
      PrestoSqlParser::DropSchemaContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRenameSchema(
      PrestoSqlParser::RenameSchemaContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreateTableAsSelect(
      PrestoSqlParser::CreateTableAsSelectContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreateTable(
      PrestoSqlParser::CreateTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDropTable(
      PrestoSqlParser::DropTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitInsertInto(
      PrestoSqlParser::InsertIntoContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDelete(PrestoSqlParser::DeleteContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTruncateTable(
      PrestoSqlParser::TruncateTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRenameTable(
      PrestoSqlParser::RenameTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRenameColumn(
      PrestoSqlParser::RenameColumnContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDropColumn(
      PrestoSqlParser::DropColumnContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAddColumn(
      PrestoSqlParser::AddColumnContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAddConstraint(
      PrestoSqlParser::AddConstraintContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDropConstraint(
      PrestoSqlParser::DropConstraintContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAlterColumnSetNotNull(
      PrestoSqlParser::AlterColumnSetNotNullContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAlterColumnDropNotNull(
      PrestoSqlParser::AlterColumnDropNotNullContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSetTableProperties(
      PrestoSqlParser::SetTablePropertiesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAnalyze(PrestoSqlParser::AnalyzeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreateType(
      PrestoSqlParser::CreateTypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreateView(
      PrestoSqlParser::CreateViewContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRenameView(
      PrestoSqlParser::RenameViewContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDropView(
      PrestoSqlParser::DropViewContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreateMaterializedView(
      PrestoSqlParser::CreateMaterializedViewContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDropMaterializedView(
      PrestoSqlParser::DropMaterializedViewContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRefreshMaterializedView(
      PrestoSqlParser::RefreshMaterializedViewContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreateFunction(
      PrestoSqlParser::CreateFunctionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAlterFunction(
      PrestoSqlParser::AlterFunctionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDropFunction(
      PrestoSqlParser::DropFunctionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCall(PrestoSqlParser::CallContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreateRole(
      PrestoSqlParser::CreateRoleContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDropRole(
      PrestoSqlParser::DropRoleContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGrantRoles(
      PrestoSqlParser::GrantRolesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRevokeRoles(
      PrestoSqlParser::RevokeRolesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSetRole(PrestoSqlParser::SetRoleContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGrant(PrestoSqlParser::GrantContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRevoke(PrestoSqlParser::RevokeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowGrants(
      PrestoSqlParser::ShowGrantsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExplain(PrestoSqlParser::ExplainContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowCreateTable(
      PrestoSqlParser::ShowCreateTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowCreateView(
      PrestoSqlParser::ShowCreateViewContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowCreateMaterializedView(
      PrestoSqlParser::ShowCreateMaterializedViewContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowCreateFunction(
      PrestoSqlParser::ShowCreateFunctionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowTables(
      PrestoSqlParser::ShowTablesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowSchemas(
      PrestoSqlParser::ShowSchemasContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowCatalogs(
      PrestoSqlParser::ShowCatalogsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowColumns(
      PrestoSqlParser::ShowColumnsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowStats(
      PrestoSqlParser::ShowStatsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowStatsForQuery(
      PrestoSqlParser::ShowStatsForQueryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowRoles(
      PrestoSqlParser::ShowRolesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowRoleGrants(
      PrestoSqlParser::ShowRoleGrantsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowFunctions(
      PrestoSqlParser::ShowFunctionsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowSession(
      PrestoSqlParser::ShowSessionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSetSession(
      PrestoSqlParser::SetSessionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitResetSession(
      PrestoSqlParser::ResetSessionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitStartTransaction(
      PrestoSqlParser::StartTransactionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCommit(PrestoSqlParser::CommitContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRollback(
      PrestoSqlParser::RollbackContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPrepare(PrestoSqlParser::PrepareContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDeallocate(
      PrestoSqlParser::DeallocateContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExecute(PrestoSqlParser::ExecuteContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDescribeInput(
      PrestoSqlParser::DescribeInputContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDescribeOutput(
      PrestoSqlParser::DescribeOutputContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUpdate(PrestoSqlParser::UpdateContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQuery(PrestoSqlParser::QueryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitWith(PrestoSqlParser::WithContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTableElement(
      PrestoSqlParser::TableElementContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitColumnDefinition(
      PrestoSqlParser::ColumnDefinitionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLikeClause(
      PrestoSqlParser::LikeClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitProperties(
      PrestoSqlParser::PropertiesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitProperty(
      PrestoSqlParser::PropertyContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSqlParameterDeclaration(
      PrestoSqlParser::SqlParameterDeclarationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRoutineCharacteristics(
      PrestoSqlParser::RoutineCharacteristicsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRoutineCharacteristic(
      PrestoSqlParser::RoutineCharacteristicContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAlterRoutineCharacteristics(
      PrestoSqlParser::AlterRoutineCharacteristicsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAlterRoutineCharacteristic(
      PrestoSqlParser::AlterRoutineCharacteristicContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRoutineBody(
      PrestoSqlParser::RoutineBodyContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitReturnStatement(
      PrestoSqlParser::ReturnStatementContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExternalBodyReference(
      PrestoSqlParser::ExternalBodyReferenceContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLanguage(
      PrestoSqlParser::LanguageContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDeterminism(
      PrestoSqlParser::DeterminismContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNullCallClause(
      PrestoSqlParser::NullCallClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExternalRoutineName(
      PrestoSqlParser::ExternalRoutineNameContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQueryNoWith(
      PrestoSqlParser::QueryNoWithContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQueryTermDefault(
      PrestoSqlParser::QueryTermDefaultContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSetOperation(
      PrestoSqlParser::SetOperationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQueryPrimaryDefault(
      PrestoSqlParser::QueryPrimaryDefaultContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTable(PrestoSqlParser::TableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitInlineTable(
      PrestoSqlParser::InlineTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSubquery(
      PrestoSqlParser::SubqueryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSortItem(
      PrestoSqlParser::SortItemContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQuerySpecification(
      PrestoSqlParser::QuerySpecificationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGroupBy(PrestoSqlParser::GroupByContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSingleGroupingSet(
      PrestoSqlParser::SingleGroupingSetContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRollup(PrestoSqlParser::RollupContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCube(PrestoSqlParser::CubeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMultipleGroupingSets(
      PrestoSqlParser::MultipleGroupingSetsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGroupingSet(
      PrestoSqlParser::GroupingSetContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNamedQuery(
      PrestoSqlParser::NamedQueryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSetQuantifier(
      PrestoSqlParser::SetQuantifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSelectSingle(
      PrestoSqlParser::SelectSingleContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSelectAll(
      PrestoSqlParser::SelectAllContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRelationDefault(
      PrestoSqlParser::RelationDefaultContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitJoinRelation(
      PrestoSqlParser::JoinRelationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitJoinType(
      PrestoSqlParser::JoinTypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitJoinCriteria(
      PrestoSqlParser::JoinCriteriaContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSampledRelation(
      PrestoSqlParser::SampledRelationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSampleType(
      PrestoSqlParser::SampleTypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAliasedRelation(
      PrestoSqlParser::AliasedRelationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitColumnAliases(
      PrestoSqlParser::ColumnAliasesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTableName(
      PrestoSqlParser::TableNameContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSubqueryRelation(
      PrestoSqlParser::SubqueryRelationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnnest(PrestoSqlParser::UnnestContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLateral(PrestoSqlParser::LateralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitParenthesizedRelation(
      PrestoSqlParser::ParenthesizedRelationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExpression(
      PrestoSqlParser::ExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLogicalNot(
      PrestoSqlParser::LogicalNotContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPredicated(
      PrestoSqlParser::PredicatedContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLogicalBinary(
      PrestoSqlParser::LogicalBinaryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitComparison(
      PrestoSqlParser::ComparisonContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQuantifiedComparison(
      PrestoSqlParser::QuantifiedComparisonContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBetween(PrestoSqlParser::BetweenContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitInList(PrestoSqlParser::InListContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitInSubquery(
      PrestoSqlParser::InSubqueryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLike(PrestoSqlParser::LikeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNullPredicate(
      PrestoSqlParser::NullPredicateContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDistinctFrom(
      PrestoSqlParser::DistinctFromContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitValueExpressionDefault(
      PrestoSqlParser::ValueExpressionDefaultContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConcatenation(
      PrestoSqlParser::ConcatenationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitArithmeticBinary(
      PrestoSqlParser::ArithmeticBinaryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitArithmeticUnary(
      PrestoSqlParser::ArithmeticUnaryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAtTimeZone(
      PrestoSqlParser::AtTimeZoneContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDereference(
      PrestoSqlParser::DereferenceContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTypeConstructor(
      PrestoSqlParser::TypeConstructorContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSpecialDateTimeFunction(
      PrestoSqlParser::SpecialDateTimeFunctionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSubstring(
      PrestoSqlParser::SubstringContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCast(PrestoSqlParser::CastContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLambda(PrestoSqlParser::LambdaContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitParenthesizedExpression(
      PrestoSqlParser::ParenthesizedExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitParameter(
      PrestoSqlParser::ParameterContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNormalize(
      PrestoSqlParser::NormalizeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitIntervalLiteral(
      PrestoSqlParser::IntervalLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNumericLiteral(
      PrestoSqlParser::NumericLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBooleanLiteral(
      PrestoSqlParser::BooleanLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSimpleCase(
      PrestoSqlParser::SimpleCaseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitColumnReference(
      PrestoSqlParser::ColumnReferenceContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNullLiteral(
      PrestoSqlParser::NullLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRowConstructor(
      PrestoSqlParser::RowConstructorContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSubscript(
      PrestoSqlParser::SubscriptContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSubqueryExpression(
      PrestoSqlParser::SubqueryExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBinaryLiteral(
      PrestoSqlParser::BinaryLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCurrentUser(
      PrestoSqlParser::CurrentUserContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExtract(PrestoSqlParser::ExtractContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitStringLiteral(
      PrestoSqlParser::StringLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitArrayConstructor(
      PrestoSqlParser::ArrayConstructorContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitFunctionCall(
      PrestoSqlParser::FunctionCallContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExists(PrestoSqlParser::ExistsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPosition(
      PrestoSqlParser::PositionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSearchedCase(
      PrestoSqlParser::SearchedCaseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGroupingOperation(
      PrestoSqlParser::GroupingOperationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBasicStringLiteral(
      PrestoSqlParser::BasicStringLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnicodeStringLiteral(
      PrestoSqlParser::UnicodeStringLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNullTreatment(
      PrestoSqlParser::NullTreatmentContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTimeZoneInterval(
      PrestoSqlParser::TimeZoneIntervalContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTimeZoneString(
      PrestoSqlParser::TimeZoneStringContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitComparisonOperator(
      PrestoSqlParser::ComparisonOperatorContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitComparisonQuantifier(
      PrestoSqlParser::ComparisonQuantifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBooleanValue(
      PrestoSqlParser::BooleanValueContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitInterval(
      PrestoSqlParser::IntervalContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitIntervalField(
      PrestoSqlParser::IntervalFieldContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNormalForm(
      PrestoSqlParser::NormalFormContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTypes(PrestoSqlParser::TypesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitType(PrestoSqlParser::TypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTypeParameter(
      PrestoSqlParser::TypeParameterContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBaseType(
      PrestoSqlParser::BaseTypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitWhenClause(
      PrestoSqlParser::WhenClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitFilter(PrestoSqlParser::FilterContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitOver(PrestoSqlParser::OverContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitWindowFrame(
      PrestoSqlParser::WindowFrameContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnboundedFrame(
      PrestoSqlParser::UnboundedFrameContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCurrentRowBound(
      PrestoSqlParser::CurrentRowBoundContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBoundedFrame(
      PrestoSqlParser::BoundedFrameContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUpdateAssignment(
      PrestoSqlParser::UpdateAssignmentContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExplainFormat(
      PrestoSqlParser::ExplainFormatContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExplainType(
      PrestoSqlParser::ExplainTypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitIsolationLevel(
      PrestoSqlParser::IsolationLevelContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTransactionAccessMode(
      PrestoSqlParser::TransactionAccessModeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitReadUncommitted(
      PrestoSqlParser::ReadUncommittedContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitReadCommitted(
      PrestoSqlParser::ReadCommittedContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRepeatableRead(
      PrestoSqlParser::RepeatableReadContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSerializable(
      PrestoSqlParser::SerializableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPositionalArgument(
      PrestoSqlParser::PositionalArgumentContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNamedArgument(
      PrestoSqlParser::NamedArgumentContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPrivilege(
      PrestoSqlParser::PrivilegeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQualifiedName(
      PrestoSqlParser::QualifiedNameContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTableVersion(
      PrestoSqlParser::TableVersionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTableversionasof(
      PrestoSqlParser::TableversionasofContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTableversionbefore(
      PrestoSqlParser::TableversionbeforeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCurrentUserGrantor(
      PrestoSqlParser::CurrentUserGrantorContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCurrentRoleGrantor(
      PrestoSqlParser::CurrentRoleGrantorContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSpecifiedPrincipal(
      PrestoSqlParser::SpecifiedPrincipalContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUserPrincipal(
      PrestoSqlParser::UserPrincipalContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRolePrincipal(
      PrestoSqlParser::RolePrincipalContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnspecifiedPrincipal(
      PrestoSqlParser::UnspecifiedPrincipalContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRoles(PrestoSqlParser::RolesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnquotedIdentifier(
      PrestoSqlParser::UnquotedIdentifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQuotedIdentifier(
      PrestoSqlParser::QuotedIdentifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBackQuotedIdentifier(
      PrestoSqlParser::BackQuotedIdentifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDigitIdentifier(
      PrestoSqlParser::DigitIdentifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDecimalLiteral(
      PrestoSqlParser::DecimalLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDoubleLiteral(
      PrestoSqlParser::DoubleLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitIntegerLiteral(
      PrestoSqlParser::IntegerLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConstraintSpecification(
      PrestoSqlParser::ConstraintSpecificationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNamedConstraintSpecification(
      PrestoSqlParser::NamedConstraintSpecificationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnnamedConstraintSpecification(
      PrestoSqlParser::UnnamedConstraintSpecificationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConstraintType(
      PrestoSqlParser::ConstraintTypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConstraintQualifiers(
      PrestoSqlParser::ConstraintQualifiersContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConstraintQualifier(
      PrestoSqlParser::ConstraintQualifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConstraintRely(
      PrestoSqlParser::ConstraintRelyContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConstraintEnabled(
      PrestoSqlParser::ConstraintEnabledContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConstraintEnforced(
      PrestoSqlParser::ConstraintEnforcedContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNonReserved(
      PrestoSqlParser::NonReservedContext* ctx) override {
    return visitChildren(ctx);
  }
};
