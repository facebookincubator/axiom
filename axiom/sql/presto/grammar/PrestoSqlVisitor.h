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

#include "PrestoSqlParser.h"
#include "antlr4-runtime.h"

/**
 * This class defines an abstract visitor for a parse tree
 * produced by PrestoSqlParser.
 */
class PrestoSqlVisitor : public antlr4::tree::AbstractParseTreeVisitor {
 public:
  /**
   * Visit parse trees produced by PrestoSqlParser.
   */
  virtual std::any visitSingleStatement(
      PrestoSqlParser::SingleStatementContext* context) = 0;

  virtual std::any visitStandaloneExpression(
      PrestoSqlParser::StandaloneExpressionContext* context) = 0;

  virtual std::any visitStandaloneRoutineBody(
      PrestoSqlParser::StandaloneRoutineBodyContext* context) = 0;

  virtual std::any visitStatementDefault(
      PrestoSqlParser::StatementDefaultContext* context) = 0;

  virtual std::any visitUse(PrestoSqlParser::UseContext* context) = 0;

  virtual std::any visitCreateSchema(
      PrestoSqlParser::CreateSchemaContext* context) = 0;

  virtual std::any visitDropSchema(
      PrestoSqlParser::DropSchemaContext* context) = 0;

  virtual std::any visitRenameSchema(
      PrestoSqlParser::RenameSchemaContext* context) = 0;

  virtual std::any visitCreateTableAsSelect(
      PrestoSqlParser::CreateTableAsSelectContext* context) = 0;

  virtual std::any visitCreateTable(
      PrestoSqlParser::CreateTableContext* context) = 0;

  virtual std::any visitDropTable(
      PrestoSqlParser::DropTableContext* context) = 0;

  virtual std::any visitInsertInto(
      PrestoSqlParser::InsertIntoContext* context) = 0;

  virtual std::any visitDelete(PrestoSqlParser::DeleteContext* context) = 0;

  virtual std::any visitTruncateTable(
      PrestoSqlParser::TruncateTableContext* context) = 0;

  virtual std::any visitRenameTable(
      PrestoSqlParser::RenameTableContext* context) = 0;

  virtual std::any visitRenameColumn(
      PrestoSqlParser::RenameColumnContext* context) = 0;

  virtual std::any visitDropColumn(
      PrestoSqlParser::DropColumnContext* context) = 0;

  virtual std::any visitAddColumn(
      PrestoSqlParser::AddColumnContext* context) = 0;

  virtual std::any visitAddConstraint(
      PrestoSqlParser::AddConstraintContext* context) = 0;

  virtual std::any visitDropConstraint(
      PrestoSqlParser::DropConstraintContext* context) = 0;

  virtual std::any visitAlterColumnSetNotNull(
      PrestoSqlParser::AlterColumnSetNotNullContext* context) = 0;

  virtual std::any visitAlterColumnDropNotNull(
      PrestoSqlParser::AlterColumnDropNotNullContext* context) = 0;

  virtual std::any visitSetTableProperties(
      PrestoSqlParser::SetTablePropertiesContext* context) = 0;

  virtual std::any visitAnalyze(PrestoSqlParser::AnalyzeContext* context) = 0;

  virtual std::any visitCreateType(
      PrestoSqlParser::CreateTypeContext* context) = 0;

  virtual std::any visitCreateView(
      PrestoSqlParser::CreateViewContext* context) = 0;

  virtual std::any visitRenameView(
      PrestoSqlParser::RenameViewContext* context) = 0;

  virtual std::any visitDropView(PrestoSqlParser::DropViewContext* context) = 0;

  virtual std::any visitCreateMaterializedView(
      PrestoSqlParser::CreateMaterializedViewContext* context) = 0;

  virtual std::any visitDropMaterializedView(
      PrestoSqlParser::DropMaterializedViewContext* context) = 0;

  virtual std::any visitRefreshMaterializedView(
      PrestoSqlParser::RefreshMaterializedViewContext* context) = 0;

  virtual std::any visitCreateFunction(
      PrestoSqlParser::CreateFunctionContext* context) = 0;

  virtual std::any visitAlterFunction(
      PrestoSqlParser::AlterFunctionContext* context) = 0;

  virtual std::any visitDropFunction(
      PrestoSqlParser::DropFunctionContext* context) = 0;

  virtual std::any visitCall(PrestoSqlParser::CallContext* context) = 0;

  virtual std::any visitCreateRole(
      PrestoSqlParser::CreateRoleContext* context) = 0;

  virtual std::any visitDropRole(PrestoSqlParser::DropRoleContext* context) = 0;

  virtual std::any visitGrantRoles(
      PrestoSqlParser::GrantRolesContext* context) = 0;

  virtual std::any visitRevokeRoles(
      PrestoSqlParser::RevokeRolesContext* context) = 0;

  virtual std::any visitSetRole(PrestoSqlParser::SetRoleContext* context) = 0;

  virtual std::any visitGrant(PrestoSqlParser::GrantContext* context) = 0;

  virtual std::any visitRevoke(PrestoSqlParser::RevokeContext* context) = 0;

  virtual std::any visitShowGrants(
      PrestoSqlParser::ShowGrantsContext* context) = 0;

  virtual std::any visitExplain(PrestoSqlParser::ExplainContext* context) = 0;

  virtual std::any visitShowCreateTable(
      PrestoSqlParser::ShowCreateTableContext* context) = 0;

  virtual std::any visitShowCreateView(
      PrestoSqlParser::ShowCreateViewContext* context) = 0;

  virtual std::any visitShowCreateMaterializedView(
      PrestoSqlParser::ShowCreateMaterializedViewContext* context) = 0;

  virtual std::any visitShowCreateFunction(
      PrestoSqlParser::ShowCreateFunctionContext* context) = 0;

  virtual std::any visitShowTables(
      PrestoSqlParser::ShowTablesContext* context) = 0;

  virtual std::any visitShowSchemas(
      PrestoSqlParser::ShowSchemasContext* context) = 0;

  virtual std::any visitShowCatalogs(
      PrestoSqlParser::ShowCatalogsContext* context) = 0;

  virtual std::any visitShowColumns(
      PrestoSqlParser::ShowColumnsContext* context) = 0;

  virtual std::any visitShowStats(
      PrestoSqlParser::ShowStatsContext* context) = 0;

  virtual std::any visitShowStatsForQuery(
      PrestoSqlParser::ShowStatsForQueryContext* context) = 0;

  virtual std::any visitShowRoles(
      PrestoSqlParser::ShowRolesContext* context) = 0;

  virtual std::any visitShowRoleGrants(
      PrestoSqlParser::ShowRoleGrantsContext* context) = 0;

  virtual std::any visitShowFunctions(
      PrestoSqlParser::ShowFunctionsContext* context) = 0;

  virtual std::any visitShowSession(
      PrestoSqlParser::ShowSessionContext* context) = 0;

  virtual std::any visitSetSession(
      PrestoSqlParser::SetSessionContext* context) = 0;

  virtual std::any visitResetSession(
      PrestoSqlParser::ResetSessionContext* context) = 0;

  virtual std::any visitStartTransaction(
      PrestoSqlParser::StartTransactionContext* context) = 0;

  virtual std::any visitCommit(PrestoSqlParser::CommitContext* context) = 0;

  virtual std::any visitRollback(PrestoSqlParser::RollbackContext* context) = 0;

  virtual std::any visitPrepare(PrestoSqlParser::PrepareContext* context) = 0;

  virtual std::any visitDeallocate(
      PrestoSqlParser::DeallocateContext* context) = 0;

  virtual std::any visitExecute(PrestoSqlParser::ExecuteContext* context) = 0;

  virtual std::any visitDescribeInput(
      PrestoSqlParser::DescribeInputContext* context) = 0;

  virtual std::any visitDescribeOutput(
      PrestoSqlParser::DescribeOutputContext* context) = 0;

  virtual std::any visitUpdate(PrestoSqlParser::UpdateContext* context) = 0;

  virtual std::any visitQuery(PrestoSqlParser::QueryContext* context) = 0;

  virtual std::any visitWith(PrestoSqlParser::WithContext* context) = 0;

  virtual std::any visitTableElement(
      PrestoSqlParser::TableElementContext* context) = 0;

  virtual std::any visitColumnDefinition(
      PrestoSqlParser::ColumnDefinitionContext* context) = 0;

  virtual std::any visitLikeClause(
      PrestoSqlParser::LikeClauseContext* context) = 0;

  virtual std::any visitProperties(
      PrestoSqlParser::PropertiesContext* context) = 0;

  virtual std::any visitProperty(PrestoSqlParser::PropertyContext* context) = 0;

  virtual std::any visitSqlParameterDeclaration(
      PrestoSqlParser::SqlParameterDeclarationContext* context) = 0;

  virtual std::any visitRoutineCharacteristics(
      PrestoSqlParser::RoutineCharacteristicsContext* context) = 0;

  virtual std::any visitRoutineCharacteristic(
      PrestoSqlParser::RoutineCharacteristicContext* context) = 0;

  virtual std::any visitAlterRoutineCharacteristics(
      PrestoSqlParser::AlterRoutineCharacteristicsContext* context) = 0;

  virtual std::any visitAlterRoutineCharacteristic(
      PrestoSqlParser::AlterRoutineCharacteristicContext* context) = 0;

  virtual std::any visitRoutineBody(
      PrestoSqlParser::RoutineBodyContext* context) = 0;

  virtual std::any visitReturnStatement(
      PrestoSqlParser::ReturnStatementContext* context) = 0;

  virtual std::any visitExternalBodyReference(
      PrestoSqlParser::ExternalBodyReferenceContext* context) = 0;

  virtual std::any visitLanguage(PrestoSqlParser::LanguageContext* context) = 0;

  virtual std::any visitDeterminism(
      PrestoSqlParser::DeterminismContext* context) = 0;

  virtual std::any visitNullCallClause(
      PrestoSqlParser::NullCallClauseContext* context) = 0;

  virtual std::any visitExternalRoutineName(
      PrestoSqlParser::ExternalRoutineNameContext* context) = 0;

  virtual std::any visitQueryNoWith(
      PrestoSqlParser::QueryNoWithContext* context) = 0;

  virtual std::any visitQueryTermDefault(
      PrestoSqlParser::QueryTermDefaultContext* context) = 0;

  virtual std::any visitSetOperation(
      PrestoSqlParser::SetOperationContext* context) = 0;

  virtual std::any visitQueryPrimaryDefault(
      PrestoSqlParser::QueryPrimaryDefaultContext* context) = 0;

  virtual std::any visitTable(PrestoSqlParser::TableContext* context) = 0;

  virtual std::any visitInlineTable(
      PrestoSqlParser::InlineTableContext* context) = 0;

  virtual std::any visitSubquery(PrestoSqlParser::SubqueryContext* context) = 0;

  virtual std::any visitSortItem(PrestoSqlParser::SortItemContext* context) = 0;

  virtual std::any visitQuerySpecification(
      PrestoSqlParser::QuerySpecificationContext* context) = 0;

  virtual std::any visitGroupBy(PrestoSqlParser::GroupByContext* context) = 0;

  virtual std::any visitSingleGroupingSet(
      PrestoSqlParser::SingleGroupingSetContext* context) = 0;

  virtual std::any visitRollup(PrestoSqlParser::RollupContext* context) = 0;

  virtual std::any visitCube(PrestoSqlParser::CubeContext* context) = 0;

  virtual std::any visitMultipleGroupingSets(
      PrestoSqlParser::MultipleGroupingSetsContext* context) = 0;

  virtual std::any visitGroupingSet(
      PrestoSqlParser::GroupingSetContext* context) = 0;

  virtual std::any visitNamedQuery(
      PrestoSqlParser::NamedQueryContext* context) = 0;

  virtual std::any visitSetQuantifier(
      PrestoSqlParser::SetQuantifierContext* context) = 0;

  virtual std::any visitSelectSingle(
      PrestoSqlParser::SelectSingleContext* context) = 0;

  virtual std::any visitSelectAll(
      PrestoSqlParser::SelectAllContext* context) = 0;

  virtual std::any visitRelationDefault(
      PrestoSqlParser::RelationDefaultContext* context) = 0;

  virtual std::any visitJoinRelation(
      PrestoSqlParser::JoinRelationContext* context) = 0;

  virtual std::any visitJoinType(PrestoSqlParser::JoinTypeContext* context) = 0;

  virtual std::any visitJoinCriteria(
      PrestoSqlParser::JoinCriteriaContext* context) = 0;

  virtual std::any visitSampledRelation(
      PrestoSqlParser::SampledRelationContext* context) = 0;

  virtual std::any visitSampleType(
      PrestoSqlParser::SampleTypeContext* context) = 0;

  virtual std::any visitAliasedRelation(
      PrestoSqlParser::AliasedRelationContext* context) = 0;

  virtual std::any visitColumnAliases(
      PrestoSqlParser::ColumnAliasesContext* context) = 0;

  virtual std::any visitTableName(
      PrestoSqlParser::TableNameContext* context) = 0;

  virtual std::any visitSubqueryRelation(
      PrestoSqlParser::SubqueryRelationContext* context) = 0;

  virtual std::any visitUnnest(PrestoSqlParser::UnnestContext* context) = 0;

  virtual std::any visitLateral(PrestoSqlParser::LateralContext* context) = 0;

  virtual std::any visitParenthesizedRelation(
      PrestoSqlParser::ParenthesizedRelationContext* context) = 0;

  virtual std::any visitExpression(
      PrestoSqlParser::ExpressionContext* context) = 0;

  virtual std::any visitLogicalNot(
      PrestoSqlParser::LogicalNotContext* context) = 0;

  virtual std::any visitPredicated(
      PrestoSqlParser::PredicatedContext* context) = 0;

  virtual std::any visitLogicalBinary(
      PrestoSqlParser::LogicalBinaryContext* context) = 0;

  virtual std::any visitComparison(
      PrestoSqlParser::ComparisonContext* context) = 0;

  virtual std::any visitQuantifiedComparison(
      PrestoSqlParser::QuantifiedComparisonContext* context) = 0;

  virtual std::any visitBetween(PrestoSqlParser::BetweenContext* context) = 0;

  virtual std::any visitInList(PrestoSqlParser::InListContext* context) = 0;

  virtual std::any visitInSubquery(
      PrestoSqlParser::InSubqueryContext* context) = 0;

  virtual std::any visitLike(PrestoSqlParser::LikeContext* context) = 0;

  virtual std::any visitNullPredicate(
      PrestoSqlParser::NullPredicateContext* context) = 0;

  virtual std::any visitDistinctFrom(
      PrestoSqlParser::DistinctFromContext* context) = 0;

  virtual std::any visitValueExpressionDefault(
      PrestoSqlParser::ValueExpressionDefaultContext* context) = 0;

  virtual std::any visitConcatenation(
      PrestoSqlParser::ConcatenationContext* context) = 0;

  virtual std::any visitArithmeticBinary(
      PrestoSqlParser::ArithmeticBinaryContext* context) = 0;

  virtual std::any visitArithmeticUnary(
      PrestoSqlParser::ArithmeticUnaryContext* context) = 0;

  virtual std::any visitAtTimeZone(
      PrestoSqlParser::AtTimeZoneContext* context) = 0;

  virtual std::any visitDereference(
      PrestoSqlParser::DereferenceContext* context) = 0;

  virtual std::any visitTypeConstructor(
      PrestoSqlParser::TypeConstructorContext* context) = 0;

  virtual std::any visitSpecialDateTimeFunction(
      PrestoSqlParser::SpecialDateTimeFunctionContext* context) = 0;

  virtual std::any visitSubstring(
      PrestoSqlParser::SubstringContext* context) = 0;

  virtual std::any visitCast(PrestoSqlParser::CastContext* context) = 0;

  virtual std::any visitLambda(PrestoSqlParser::LambdaContext* context) = 0;

  virtual std::any visitParenthesizedExpression(
      PrestoSqlParser::ParenthesizedExpressionContext* context) = 0;

  virtual std::any visitParameter(
      PrestoSqlParser::ParameterContext* context) = 0;

  virtual std::any visitNormalize(
      PrestoSqlParser::NormalizeContext* context) = 0;

  virtual std::any visitIntervalLiteral(
      PrestoSqlParser::IntervalLiteralContext* context) = 0;

  virtual std::any visitNumericLiteral(
      PrestoSqlParser::NumericLiteralContext* context) = 0;

  virtual std::any visitBooleanLiteral(
      PrestoSqlParser::BooleanLiteralContext* context) = 0;

  virtual std::any visitSimpleCase(
      PrestoSqlParser::SimpleCaseContext* context) = 0;

  virtual std::any visitColumnReference(
      PrestoSqlParser::ColumnReferenceContext* context) = 0;

  virtual std::any visitNullLiteral(
      PrestoSqlParser::NullLiteralContext* context) = 0;

  virtual std::any visitRowConstructor(
      PrestoSqlParser::RowConstructorContext* context) = 0;

  virtual std::any visitSubscript(
      PrestoSqlParser::SubscriptContext* context) = 0;

  virtual std::any visitSubqueryExpression(
      PrestoSqlParser::SubqueryExpressionContext* context) = 0;

  virtual std::any visitBinaryLiteral(
      PrestoSqlParser::BinaryLiteralContext* context) = 0;

  virtual std::any visitCurrentUser(
      PrestoSqlParser::CurrentUserContext* context) = 0;

  virtual std::any visitExtract(PrestoSqlParser::ExtractContext* context) = 0;

  virtual std::any visitStringLiteral(
      PrestoSqlParser::StringLiteralContext* context) = 0;

  virtual std::any visitArrayConstructor(
      PrestoSqlParser::ArrayConstructorContext* context) = 0;

  virtual std::any visitFunctionCall(
      PrestoSqlParser::FunctionCallContext* context) = 0;

  virtual std::any visitExists(PrestoSqlParser::ExistsContext* context) = 0;

  virtual std::any visitPosition(PrestoSqlParser::PositionContext* context) = 0;

  virtual std::any visitSearchedCase(
      PrestoSqlParser::SearchedCaseContext* context) = 0;

  virtual std::any visitGroupingOperation(
      PrestoSqlParser::GroupingOperationContext* context) = 0;

  virtual std::any visitBasicStringLiteral(
      PrestoSqlParser::BasicStringLiteralContext* context) = 0;

  virtual std::any visitUnicodeStringLiteral(
      PrestoSqlParser::UnicodeStringLiteralContext* context) = 0;

  virtual std::any visitNullTreatment(
      PrestoSqlParser::NullTreatmentContext* context) = 0;

  virtual std::any visitTimeZoneInterval(
      PrestoSqlParser::TimeZoneIntervalContext* context) = 0;

  virtual std::any visitTimeZoneString(
      PrestoSqlParser::TimeZoneStringContext* context) = 0;

  virtual std::any visitComparisonOperator(
      PrestoSqlParser::ComparisonOperatorContext* context) = 0;

  virtual std::any visitComparisonQuantifier(
      PrestoSqlParser::ComparisonQuantifierContext* context) = 0;

  virtual std::any visitBooleanValue(
      PrestoSqlParser::BooleanValueContext* context) = 0;

  virtual std::any visitInterval(PrestoSqlParser::IntervalContext* context) = 0;

  virtual std::any visitIntervalField(
      PrestoSqlParser::IntervalFieldContext* context) = 0;

  virtual std::any visitNormalForm(
      PrestoSqlParser::NormalFormContext* context) = 0;

  virtual std::any visitTypes(PrestoSqlParser::TypesContext* context) = 0;

  virtual std::any visitType(PrestoSqlParser::TypeContext* context) = 0;

  virtual std::any visitTypeParameter(
      PrestoSqlParser::TypeParameterContext* context) = 0;

  virtual std::any visitBaseType(PrestoSqlParser::BaseTypeContext* context) = 0;

  virtual std::any visitWhenClause(
      PrestoSqlParser::WhenClauseContext* context) = 0;

  virtual std::any visitFilter(PrestoSqlParser::FilterContext* context) = 0;

  virtual std::any visitOver(PrestoSqlParser::OverContext* context) = 0;

  virtual std::any visitWindowFrame(
      PrestoSqlParser::WindowFrameContext* context) = 0;

  virtual std::any visitUnboundedFrame(
      PrestoSqlParser::UnboundedFrameContext* context) = 0;

  virtual std::any visitCurrentRowBound(
      PrestoSqlParser::CurrentRowBoundContext* context) = 0;

  virtual std::any visitBoundedFrame(
      PrestoSqlParser::BoundedFrameContext* context) = 0;

  virtual std::any visitUpdateAssignment(
      PrestoSqlParser::UpdateAssignmentContext* context) = 0;

  virtual std::any visitExplainFormat(
      PrestoSqlParser::ExplainFormatContext* context) = 0;

  virtual std::any visitExplainType(
      PrestoSqlParser::ExplainTypeContext* context) = 0;

  virtual std::any visitIsolationLevel(
      PrestoSqlParser::IsolationLevelContext* context) = 0;

  virtual std::any visitTransactionAccessMode(
      PrestoSqlParser::TransactionAccessModeContext* context) = 0;

  virtual std::any visitReadUncommitted(
      PrestoSqlParser::ReadUncommittedContext* context) = 0;

  virtual std::any visitReadCommitted(
      PrestoSqlParser::ReadCommittedContext* context) = 0;

  virtual std::any visitRepeatableRead(
      PrestoSqlParser::RepeatableReadContext* context) = 0;

  virtual std::any visitSerializable(
      PrestoSqlParser::SerializableContext* context) = 0;

  virtual std::any visitPositionalArgument(
      PrestoSqlParser::PositionalArgumentContext* context) = 0;

  virtual std::any visitNamedArgument(
      PrestoSqlParser::NamedArgumentContext* context) = 0;

  virtual std::any visitPrivilege(
      PrestoSqlParser::PrivilegeContext* context) = 0;

  virtual std::any visitQualifiedName(
      PrestoSqlParser::QualifiedNameContext* context) = 0;

  virtual std::any visitTableVersion(
      PrestoSqlParser::TableVersionContext* context) = 0;

  virtual std::any visitTableversionasof(
      PrestoSqlParser::TableversionasofContext* context) = 0;

  virtual std::any visitTableversionbefore(
      PrestoSqlParser::TableversionbeforeContext* context) = 0;

  virtual std::any visitCurrentUserGrantor(
      PrestoSqlParser::CurrentUserGrantorContext* context) = 0;

  virtual std::any visitCurrentRoleGrantor(
      PrestoSqlParser::CurrentRoleGrantorContext* context) = 0;

  virtual std::any visitSpecifiedPrincipal(
      PrestoSqlParser::SpecifiedPrincipalContext* context) = 0;

  virtual std::any visitUserPrincipal(
      PrestoSqlParser::UserPrincipalContext* context) = 0;

  virtual std::any visitRolePrincipal(
      PrestoSqlParser::RolePrincipalContext* context) = 0;

  virtual std::any visitUnspecifiedPrincipal(
      PrestoSqlParser::UnspecifiedPrincipalContext* context) = 0;

  virtual std::any visitRoles(PrestoSqlParser::RolesContext* context) = 0;

  virtual std::any visitUnquotedIdentifier(
      PrestoSqlParser::UnquotedIdentifierContext* context) = 0;

  virtual std::any visitQuotedIdentifier(
      PrestoSqlParser::QuotedIdentifierContext* context) = 0;

  virtual std::any visitBackQuotedIdentifier(
      PrestoSqlParser::BackQuotedIdentifierContext* context) = 0;

  virtual std::any visitDigitIdentifier(
      PrestoSqlParser::DigitIdentifierContext* context) = 0;

  virtual std::any visitDecimalLiteral(
      PrestoSqlParser::DecimalLiteralContext* context) = 0;

  virtual std::any visitDoubleLiteral(
      PrestoSqlParser::DoubleLiteralContext* context) = 0;

  virtual std::any visitIntegerLiteral(
      PrestoSqlParser::IntegerLiteralContext* context) = 0;

  virtual std::any visitConstraintSpecification(
      PrestoSqlParser::ConstraintSpecificationContext* context) = 0;

  virtual std::any visitNamedConstraintSpecification(
      PrestoSqlParser::NamedConstraintSpecificationContext* context) = 0;

  virtual std::any visitUnnamedConstraintSpecification(
      PrestoSqlParser::UnnamedConstraintSpecificationContext* context) = 0;

  virtual std::any visitConstraintType(
      PrestoSqlParser::ConstraintTypeContext* context) = 0;

  virtual std::any visitConstraintQualifiers(
      PrestoSqlParser::ConstraintQualifiersContext* context) = 0;

  virtual std::any visitConstraintQualifier(
      PrestoSqlParser::ConstraintQualifierContext* context) = 0;

  virtual std::any visitConstraintRely(
      PrestoSqlParser::ConstraintRelyContext* context) = 0;

  virtual std::any visitConstraintEnabled(
      PrestoSqlParser::ConstraintEnabledContext* context) = 0;

  virtual std::any visitConstraintEnforced(
      PrestoSqlParser::ConstraintEnforcedContext* context) = 0;

  virtual std::any visitNonReserved(
      PrestoSqlParser::NonReservedContext* context) = 0;
};
