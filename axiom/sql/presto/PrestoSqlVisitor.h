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
#include "axiom/sql/presto/PrestoSqlParser.h"

namespace facebook::velox::sql {

/**
 * This class defines an abstract visitor for a parse tree
 * produced by PrestoSqlParser.
 */
class PrestoSqlVisitor : public antlr4::tree::AbstractParseTreeVisitor {
 public:
  /**
   * Visit parse trees produced by PrestoSqlParser.
   */
  virtual antlrcpp::Any visitSingleStatement(
      PrestoSqlParser::SingleStatementContext* context) = 0;

  virtual antlrcpp::Any visitStandaloneExpression(
      PrestoSqlParser::StandaloneExpressionContext* context) = 0;

  virtual antlrcpp::Any visitStandaloneRoutineBody(
      PrestoSqlParser::StandaloneRoutineBodyContext* context) = 0;

  virtual antlrcpp::Any visitStatementDefault(
      PrestoSqlParser::StatementDefaultContext* context) = 0;

  virtual antlrcpp::Any visitUse(PrestoSqlParser::UseContext* context) = 0;

  virtual antlrcpp::Any visitCreateSchema(
      PrestoSqlParser::CreateSchemaContext* context) = 0;

  virtual antlrcpp::Any visitDropSchema(
      PrestoSqlParser::DropSchemaContext* context) = 0;

  virtual antlrcpp::Any visitRenameSchema(
      PrestoSqlParser::RenameSchemaContext* context) = 0;

  virtual antlrcpp::Any visitCreateTableAsSelect(
      PrestoSqlParser::CreateTableAsSelectContext* context) = 0;

  virtual antlrcpp::Any visitCreateTable(
      PrestoSqlParser::CreateTableContext* context) = 0;

  virtual antlrcpp::Any visitDropTable(
      PrestoSqlParser::DropTableContext* context) = 0;

  virtual antlrcpp::Any visitInsertInto(
      PrestoSqlParser::InsertIntoContext* context) = 0;

  virtual antlrcpp::Any visitDelete(
      PrestoSqlParser::DeleteContext* context) = 0;

  virtual antlrcpp::Any visitTruncateTable(
      PrestoSqlParser::TruncateTableContext* context) = 0;

  virtual antlrcpp::Any visitRenameTable(
      PrestoSqlParser::RenameTableContext* context) = 0;

  virtual antlrcpp::Any visitRenameColumn(
      PrestoSqlParser::RenameColumnContext* context) = 0;

  virtual antlrcpp::Any visitDropColumn(
      PrestoSqlParser::DropColumnContext* context) = 0;

  virtual antlrcpp::Any visitAddColumn(
      PrestoSqlParser::AddColumnContext* context) = 0;

  virtual antlrcpp::Any visitAddConstraint(
      PrestoSqlParser::AddConstraintContext* context) = 0;

  virtual antlrcpp::Any visitDropConstraint(
      PrestoSqlParser::DropConstraintContext* context) = 0;

  virtual antlrcpp::Any visitAlterColumnSetNotNull(
      PrestoSqlParser::AlterColumnSetNotNullContext* context) = 0;

  virtual antlrcpp::Any visitAlterColumnDropNotNull(
      PrestoSqlParser::AlterColumnDropNotNullContext* context) = 0;

  virtual antlrcpp::Any visitSetTableProperties(
      PrestoSqlParser::SetTablePropertiesContext* context) = 0;

  virtual antlrcpp::Any visitAnalyze(
      PrestoSqlParser::AnalyzeContext* context) = 0;

  virtual antlrcpp::Any visitCreateType(
      PrestoSqlParser::CreateTypeContext* context) = 0;

  virtual antlrcpp::Any visitCreateView(
      PrestoSqlParser::CreateViewContext* context) = 0;

  virtual antlrcpp::Any visitRenameView(
      PrestoSqlParser::RenameViewContext* context) = 0;

  virtual antlrcpp::Any visitDropView(
      PrestoSqlParser::DropViewContext* context) = 0;

  virtual antlrcpp::Any visitCreateMaterializedView(
      PrestoSqlParser::CreateMaterializedViewContext* context) = 0;

  virtual antlrcpp::Any visitDropMaterializedView(
      PrestoSqlParser::DropMaterializedViewContext* context) = 0;

  virtual antlrcpp::Any visitRefreshMaterializedView(
      PrestoSqlParser::RefreshMaterializedViewContext* context) = 0;

  virtual antlrcpp::Any visitCreateFunction(
      PrestoSqlParser::CreateFunctionContext* context) = 0;

  virtual antlrcpp::Any visitAlterFunction(
      PrestoSqlParser::AlterFunctionContext* context) = 0;

  virtual antlrcpp::Any visitDropFunction(
      PrestoSqlParser::DropFunctionContext* context) = 0;

  virtual antlrcpp::Any visitCall(PrestoSqlParser::CallContext* context) = 0;

  virtual antlrcpp::Any visitCreateRole(
      PrestoSqlParser::CreateRoleContext* context) = 0;

  virtual antlrcpp::Any visitDropRole(
      PrestoSqlParser::DropRoleContext* context) = 0;

  virtual antlrcpp::Any visitGrantRoles(
      PrestoSqlParser::GrantRolesContext* context) = 0;

  virtual antlrcpp::Any visitRevokeRoles(
      PrestoSqlParser::RevokeRolesContext* context) = 0;

  virtual antlrcpp::Any visitSetRole(
      PrestoSqlParser::SetRoleContext* context) = 0;

  virtual antlrcpp::Any visitGrant(PrestoSqlParser::GrantContext* context) = 0;

  virtual antlrcpp::Any visitRevoke(
      PrestoSqlParser::RevokeContext* context) = 0;

  virtual antlrcpp::Any visitShowGrants(
      PrestoSqlParser::ShowGrantsContext* context) = 0;

  virtual antlrcpp::Any visitExplain(
      PrestoSqlParser::ExplainContext* context) = 0;

  virtual antlrcpp::Any visitShowCreateTable(
      PrestoSqlParser::ShowCreateTableContext* context) = 0;

  virtual antlrcpp::Any visitShowCreateView(
      PrestoSqlParser::ShowCreateViewContext* context) = 0;

  virtual antlrcpp::Any visitShowCreateMaterializedView(
      PrestoSqlParser::ShowCreateMaterializedViewContext* context) = 0;

  virtual antlrcpp::Any visitShowCreateFunction(
      PrestoSqlParser::ShowCreateFunctionContext* context) = 0;

  virtual antlrcpp::Any visitShowTables(
      PrestoSqlParser::ShowTablesContext* context) = 0;

  virtual antlrcpp::Any visitShowSchemas(
      PrestoSqlParser::ShowSchemasContext* context) = 0;

  virtual antlrcpp::Any visitShowCatalogs(
      PrestoSqlParser::ShowCatalogsContext* context) = 0;

  virtual antlrcpp::Any visitShowColumns(
      PrestoSqlParser::ShowColumnsContext* context) = 0;

  virtual antlrcpp::Any visitShowStats(
      PrestoSqlParser::ShowStatsContext* context) = 0;

  virtual antlrcpp::Any visitShowStatsForQuery(
      PrestoSqlParser::ShowStatsForQueryContext* context) = 0;

  virtual antlrcpp::Any visitShowRoles(
      PrestoSqlParser::ShowRolesContext* context) = 0;

  virtual antlrcpp::Any visitShowRoleGrants(
      PrestoSqlParser::ShowRoleGrantsContext* context) = 0;

  virtual antlrcpp::Any visitShowFunctions(
      PrestoSqlParser::ShowFunctionsContext* context) = 0;

  virtual antlrcpp::Any visitShowSession(
      PrestoSqlParser::ShowSessionContext* context) = 0;

  virtual antlrcpp::Any visitSetSession(
      PrestoSqlParser::SetSessionContext* context) = 0;

  virtual antlrcpp::Any visitResetSession(
      PrestoSqlParser::ResetSessionContext* context) = 0;

  virtual antlrcpp::Any visitStartTransaction(
      PrestoSqlParser::StartTransactionContext* context) = 0;

  virtual antlrcpp::Any visitCommit(
      PrestoSqlParser::CommitContext* context) = 0;

  virtual antlrcpp::Any visitRollback(
      PrestoSqlParser::RollbackContext* context) = 0;

  virtual antlrcpp::Any visitPrepare(
      PrestoSqlParser::PrepareContext* context) = 0;

  virtual antlrcpp::Any visitDeallocate(
      PrestoSqlParser::DeallocateContext* context) = 0;

  virtual antlrcpp::Any visitExecute(
      PrestoSqlParser::ExecuteContext* context) = 0;

  virtual antlrcpp::Any visitDescribeInput(
      PrestoSqlParser::DescribeInputContext* context) = 0;

  virtual antlrcpp::Any visitDescribeOutput(
      PrestoSqlParser::DescribeOutputContext* context) = 0;

  virtual antlrcpp::Any visitUpdate(
      PrestoSqlParser::UpdateContext* context) = 0;

  virtual antlrcpp::Any visitQuery(PrestoSqlParser::QueryContext* context) = 0;

  virtual antlrcpp::Any visitWith(PrestoSqlParser::WithContext* context) = 0;

  virtual antlrcpp::Any visitTableElement(
      PrestoSqlParser::TableElementContext* context) = 0;

  virtual antlrcpp::Any visitColumnDefinition(
      PrestoSqlParser::ColumnDefinitionContext* context) = 0;

  virtual antlrcpp::Any visitLikeClause(
      PrestoSqlParser::LikeClauseContext* context) = 0;

  virtual antlrcpp::Any visitProperties(
      PrestoSqlParser::PropertiesContext* context) = 0;

  virtual antlrcpp::Any visitProperty(
      PrestoSqlParser::PropertyContext* context) = 0;

  virtual antlrcpp::Any visitSqlParameterDeclaration(
      PrestoSqlParser::SqlParameterDeclarationContext* context) = 0;

  virtual antlrcpp::Any visitRoutineCharacteristics(
      PrestoSqlParser::RoutineCharacteristicsContext* context) = 0;

  virtual antlrcpp::Any visitRoutineCharacteristic(
      PrestoSqlParser::RoutineCharacteristicContext* context) = 0;

  virtual antlrcpp::Any visitAlterRoutineCharacteristics(
      PrestoSqlParser::AlterRoutineCharacteristicsContext* context) = 0;

  virtual antlrcpp::Any visitAlterRoutineCharacteristic(
      PrestoSqlParser::AlterRoutineCharacteristicContext* context) = 0;

  virtual antlrcpp::Any visitRoutineBody(
      PrestoSqlParser::RoutineBodyContext* context) = 0;

  virtual antlrcpp::Any visitReturnStatement(
      PrestoSqlParser::ReturnStatementContext* context) = 0;

  virtual antlrcpp::Any visitExternalBodyReference(
      PrestoSqlParser::ExternalBodyReferenceContext* context) = 0;

  virtual antlrcpp::Any visitLanguage(
      PrestoSqlParser::LanguageContext* context) = 0;

  virtual antlrcpp::Any visitDeterminism(
      PrestoSqlParser::DeterminismContext* context) = 0;

  virtual antlrcpp::Any visitNullCallClause(
      PrestoSqlParser::NullCallClauseContext* context) = 0;

  virtual antlrcpp::Any visitExternalRoutineName(
      PrestoSqlParser::ExternalRoutineNameContext* context) = 0;

  virtual antlrcpp::Any visitQueryNoWith(
      PrestoSqlParser::QueryNoWithContext* context) = 0;

  virtual antlrcpp::Any visitQueryTermDefault(
      PrestoSqlParser::QueryTermDefaultContext* context) = 0;

  virtual antlrcpp::Any visitSetOperation(
      PrestoSqlParser::SetOperationContext* context) = 0;

  virtual antlrcpp::Any visitQueryPrimaryDefault(
      PrestoSqlParser::QueryPrimaryDefaultContext* context) = 0;

  virtual antlrcpp::Any visitTable(PrestoSqlParser::TableContext* context) = 0;

  virtual antlrcpp::Any visitInlineTable(
      PrestoSqlParser::InlineTableContext* context) = 0;

  virtual antlrcpp::Any visitSubquery(
      PrestoSqlParser::SubqueryContext* context) = 0;

  virtual antlrcpp::Any visitSortItem(
      PrestoSqlParser::SortItemContext* context) = 0;

  virtual antlrcpp::Any visitQuerySpecification(
      PrestoSqlParser::QuerySpecificationContext* context) = 0;

  virtual antlrcpp::Any visitGroupBy(
      PrestoSqlParser::GroupByContext* context) = 0;

  virtual antlrcpp::Any visitSingleGroupingSet(
      PrestoSqlParser::SingleGroupingSetContext* context) = 0;

  virtual antlrcpp::Any visitRollup(
      PrestoSqlParser::RollupContext* context) = 0;

  virtual antlrcpp::Any visitCube(PrestoSqlParser::CubeContext* context) = 0;

  virtual antlrcpp::Any visitMultipleGroupingSets(
      PrestoSqlParser::MultipleGroupingSetsContext* context) = 0;

  virtual antlrcpp::Any visitGroupingSet(
      PrestoSqlParser::GroupingSetContext* context) = 0;

  virtual antlrcpp::Any visitNamedQuery(
      PrestoSqlParser::NamedQueryContext* context) = 0;

  virtual antlrcpp::Any visitSetQuantifier(
      PrestoSqlParser::SetQuantifierContext* context) = 0;

  virtual antlrcpp::Any visitSelectSingle(
      PrestoSqlParser::SelectSingleContext* context) = 0;

  virtual antlrcpp::Any visitSelectAll(
      PrestoSqlParser::SelectAllContext* context) = 0;

  virtual antlrcpp::Any visitRelationDefault(
      PrestoSqlParser::RelationDefaultContext* context) = 0;

  virtual antlrcpp::Any visitJoinRelation(
      PrestoSqlParser::JoinRelationContext* context) = 0;

  virtual antlrcpp::Any visitJoinType(
      PrestoSqlParser::JoinTypeContext* context) = 0;

  virtual antlrcpp::Any visitJoinCriteria(
      PrestoSqlParser::JoinCriteriaContext* context) = 0;

  virtual antlrcpp::Any visitSampledRelation(
      PrestoSqlParser::SampledRelationContext* context) = 0;

  virtual antlrcpp::Any visitSampleType(
      PrestoSqlParser::SampleTypeContext* context) = 0;

  virtual antlrcpp::Any visitAliasedRelation(
      PrestoSqlParser::AliasedRelationContext* context) = 0;

  virtual antlrcpp::Any visitColumnAliases(
      PrestoSqlParser::ColumnAliasesContext* context) = 0;

  virtual antlrcpp::Any visitTableName(
      PrestoSqlParser::TableNameContext* context) = 0;

  virtual antlrcpp::Any visitSubqueryRelation(
      PrestoSqlParser::SubqueryRelationContext* context) = 0;

  virtual antlrcpp::Any visitUnnest(
      PrestoSqlParser::UnnestContext* context) = 0;

  virtual antlrcpp::Any visitLateral(
      PrestoSqlParser::LateralContext* context) = 0;

  virtual antlrcpp::Any visitParenthesizedRelation(
      PrestoSqlParser::ParenthesizedRelationContext* context) = 0;

  virtual antlrcpp::Any visitExpression(
      PrestoSqlParser::ExpressionContext* context) = 0;

  virtual antlrcpp::Any visitLogicalNot(
      PrestoSqlParser::LogicalNotContext* context) = 0;

  virtual antlrcpp::Any visitPredicated(
      PrestoSqlParser::PredicatedContext* context) = 0;

  virtual antlrcpp::Any visitLogicalBinary(
      PrestoSqlParser::LogicalBinaryContext* context) = 0;

  virtual antlrcpp::Any visitComparison(
      PrestoSqlParser::ComparisonContext* context) = 0;

  virtual antlrcpp::Any visitQuantifiedComparison(
      PrestoSqlParser::QuantifiedComparisonContext* context) = 0;

  virtual antlrcpp::Any visitBetween(
      PrestoSqlParser::BetweenContext* context) = 0;

  virtual antlrcpp::Any visitInList(
      PrestoSqlParser::InListContext* context) = 0;

  virtual antlrcpp::Any visitInSubquery(
      PrestoSqlParser::InSubqueryContext* context) = 0;

  virtual antlrcpp::Any visitLike(PrestoSqlParser::LikeContext* context) = 0;

  virtual antlrcpp::Any visitNullPredicate(
      PrestoSqlParser::NullPredicateContext* context) = 0;

  virtual antlrcpp::Any visitDistinctFrom(
      PrestoSqlParser::DistinctFromContext* context) = 0;

  virtual antlrcpp::Any visitValueExpressionDefault(
      PrestoSqlParser::ValueExpressionDefaultContext* context) = 0;

  virtual antlrcpp::Any visitConcatenation(
      PrestoSqlParser::ConcatenationContext* context) = 0;

  virtual antlrcpp::Any visitArithmeticBinary(
      PrestoSqlParser::ArithmeticBinaryContext* context) = 0;

  virtual antlrcpp::Any visitArithmeticUnary(
      PrestoSqlParser::ArithmeticUnaryContext* context) = 0;

  virtual antlrcpp::Any visitAtTimeZone(
      PrestoSqlParser::AtTimeZoneContext* context) = 0;

  virtual antlrcpp::Any visitDereference(
      PrestoSqlParser::DereferenceContext* context) = 0;

  virtual antlrcpp::Any visitTypeConstructor(
      PrestoSqlParser::TypeConstructorContext* context) = 0;

  virtual antlrcpp::Any visitSpecialDateTimeFunction(
      PrestoSqlParser::SpecialDateTimeFunctionContext* context) = 0;

  virtual antlrcpp::Any visitSubstring(
      PrestoSqlParser::SubstringContext* context) = 0;

  virtual antlrcpp::Any visitCast(PrestoSqlParser::CastContext* context) = 0;

  virtual antlrcpp::Any visitLambda(
      PrestoSqlParser::LambdaContext* context) = 0;

  virtual antlrcpp::Any visitParenthesizedExpression(
      PrestoSqlParser::ParenthesizedExpressionContext* context) = 0;

  virtual antlrcpp::Any visitParameter(
      PrestoSqlParser::ParameterContext* context) = 0;

  virtual antlrcpp::Any visitNormalize(
      PrestoSqlParser::NormalizeContext* context) = 0;

  virtual antlrcpp::Any visitIntervalLiteral(
      PrestoSqlParser::IntervalLiteralContext* context) = 0;

  virtual antlrcpp::Any visitNumericLiteral(
      PrestoSqlParser::NumericLiteralContext* context) = 0;

  virtual antlrcpp::Any visitBooleanLiteral(
      PrestoSqlParser::BooleanLiteralContext* context) = 0;

  virtual antlrcpp::Any visitSimpleCase(
      PrestoSqlParser::SimpleCaseContext* context) = 0;

  virtual antlrcpp::Any visitColumnReference(
      PrestoSqlParser::ColumnReferenceContext* context) = 0;

  virtual antlrcpp::Any visitNullLiteral(
      PrestoSqlParser::NullLiteralContext* context) = 0;

  virtual antlrcpp::Any visitRowConstructor(
      PrestoSqlParser::RowConstructorContext* context) = 0;

  virtual antlrcpp::Any visitSubscript(
      PrestoSqlParser::SubscriptContext* context) = 0;

  virtual antlrcpp::Any visitSubqueryExpression(
      PrestoSqlParser::SubqueryExpressionContext* context) = 0;

  virtual antlrcpp::Any visitBinaryLiteral(
      PrestoSqlParser::BinaryLiteralContext* context) = 0;

  virtual antlrcpp::Any visitCurrentUser(
      PrestoSqlParser::CurrentUserContext* context) = 0;

  virtual antlrcpp::Any visitExtract(
      PrestoSqlParser::ExtractContext* context) = 0;

  virtual antlrcpp::Any visitStringLiteral(
      PrestoSqlParser::StringLiteralContext* context) = 0;

  virtual antlrcpp::Any visitArrayConstructor(
      PrestoSqlParser::ArrayConstructorContext* context) = 0;

  virtual antlrcpp::Any visitFunctionCall(
      PrestoSqlParser::FunctionCallContext* context) = 0;

  virtual antlrcpp::Any visitExists(
      PrestoSqlParser::ExistsContext* context) = 0;

  virtual antlrcpp::Any visitPosition(
      PrestoSqlParser::PositionContext* context) = 0;

  virtual antlrcpp::Any visitSearchedCase(
      PrestoSqlParser::SearchedCaseContext* context) = 0;

  virtual antlrcpp::Any visitGroupingOperation(
      PrestoSqlParser::GroupingOperationContext* context) = 0;

  virtual antlrcpp::Any visitBasicStringLiteral(
      PrestoSqlParser::BasicStringLiteralContext* context) = 0;

  virtual antlrcpp::Any visitUnicodeStringLiteral(
      PrestoSqlParser::UnicodeStringLiteralContext* context) = 0;

  virtual antlrcpp::Any visitNullTreatment(
      PrestoSqlParser::NullTreatmentContext* context) = 0;

  virtual antlrcpp::Any visitTimeZoneInterval(
      PrestoSqlParser::TimeZoneIntervalContext* context) = 0;

  virtual antlrcpp::Any visitTimeZoneString(
      PrestoSqlParser::TimeZoneStringContext* context) = 0;

  virtual antlrcpp::Any visitComparisonOperator(
      PrestoSqlParser::ComparisonOperatorContext* context) = 0;

  virtual antlrcpp::Any visitComparisonQuantifier(
      PrestoSqlParser::ComparisonQuantifierContext* context) = 0;

  virtual antlrcpp::Any visitBooleanValue(
      PrestoSqlParser::BooleanValueContext* context) = 0;

  virtual antlrcpp::Any visitInterval(
      PrestoSqlParser::IntervalContext* context) = 0;

  virtual antlrcpp::Any visitIntervalField(
      PrestoSqlParser::IntervalFieldContext* context) = 0;

  virtual antlrcpp::Any visitNormalForm(
      PrestoSqlParser::NormalFormContext* context) = 0;

  virtual antlrcpp::Any visitTypes(PrestoSqlParser::TypesContext* context) = 0;

  virtual antlrcpp::Any visitType(PrestoSqlParser::TypeContext* context) = 0;

  virtual antlrcpp::Any visitTypeParameter(
      PrestoSqlParser::TypeParameterContext* context) = 0;

  virtual antlrcpp::Any visitBaseType(
      PrestoSqlParser::BaseTypeContext* context) = 0;

  virtual antlrcpp::Any visitWhenClause(
      PrestoSqlParser::WhenClauseContext* context) = 0;

  virtual antlrcpp::Any visitFilter(
      PrestoSqlParser::FilterContext* context) = 0;

  virtual antlrcpp::Any visitOver(PrestoSqlParser::OverContext* context) = 0;

  virtual antlrcpp::Any visitWindowFrame(
      PrestoSqlParser::WindowFrameContext* context) = 0;

  virtual antlrcpp::Any visitUnboundedFrame(
      PrestoSqlParser::UnboundedFrameContext* context) = 0;

  virtual antlrcpp::Any visitCurrentRowBound(
      PrestoSqlParser::CurrentRowBoundContext* context) = 0;

  virtual antlrcpp::Any visitBoundedFrame(
      PrestoSqlParser::BoundedFrameContext* context) = 0;

  virtual antlrcpp::Any visitUpdateAssignment(
      PrestoSqlParser::UpdateAssignmentContext* context) = 0;

  virtual antlrcpp::Any visitExplainFormat(
      PrestoSqlParser::ExplainFormatContext* context) = 0;

  virtual antlrcpp::Any visitExplainType(
      PrestoSqlParser::ExplainTypeContext* context) = 0;

  virtual antlrcpp::Any visitIsolationLevel(
      PrestoSqlParser::IsolationLevelContext* context) = 0;

  virtual antlrcpp::Any visitTransactionAccessMode(
      PrestoSqlParser::TransactionAccessModeContext* context) = 0;

  virtual antlrcpp::Any visitReadUncommitted(
      PrestoSqlParser::ReadUncommittedContext* context) = 0;

  virtual antlrcpp::Any visitReadCommitted(
      PrestoSqlParser::ReadCommittedContext* context) = 0;

  virtual antlrcpp::Any visitRepeatableRead(
      PrestoSqlParser::RepeatableReadContext* context) = 0;

  virtual antlrcpp::Any visitSerializable(
      PrestoSqlParser::SerializableContext* context) = 0;

  virtual antlrcpp::Any visitPositionalArgument(
      PrestoSqlParser::PositionalArgumentContext* context) = 0;

  virtual antlrcpp::Any visitNamedArgument(
      PrestoSqlParser::NamedArgumentContext* context) = 0;

  virtual antlrcpp::Any visitPrivilege(
      PrestoSqlParser::PrivilegeContext* context) = 0;

  virtual antlrcpp::Any visitQualifiedName(
      PrestoSqlParser::QualifiedNameContext* context) = 0;

  virtual antlrcpp::Any visitTableVersion(
      PrestoSqlParser::TableVersionContext* context) = 0;

  virtual antlrcpp::Any visitTableversionasof(
      PrestoSqlParser::TableversionasofContext* context) = 0;

  virtual antlrcpp::Any visitTableversionbefore(
      PrestoSqlParser::TableversionbeforeContext* context) = 0;

  virtual antlrcpp::Any visitCurrentUserGrantor(
      PrestoSqlParser::CurrentUserGrantorContext* context) = 0;

  virtual antlrcpp::Any visitCurrentRoleGrantor(
      PrestoSqlParser::CurrentRoleGrantorContext* context) = 0;

  virtual antlrcpp::Any visitSpecifiedPrincipal(
      PrestoSqlParser::SpecifiedPrincipalContext* context) = 0;

  virtual antlrcpp::Any visitUserPrincipal(
      PrestoSqlParser::UserPrincipalContext* context) = 0;

  virtual antlrcpp::Any visitRolePrincipal(
      PrestoSqlParser::RolePrincipalContext* context) = 0;

  virtual antlrcpp::Any visitUnspecifiedPrincipal(
      PrestoSqlParser::UnspecifiedPrincipalContext* context) = 0;

  virtual antlrcpp::Any visitRoles(PrestoSqlParser::RolesContext* context) = 0;

  virtual antlrcpp::Any visitUnquotedIdentifier(
      PrestoSqlParser::UnquotedIdentifierContext* context) = 0;

  virtual antlrcpp::Any visitQuotedIdentifier(
      PrestoSqlParser::QuotedIdentifierContext* context) = 0;

  virtual antlrcpp::Any visitBackQuotedIdentifier(
      PrestoSqlParser::BackQuotedIdentifierContext* context) = 0;

  virtual antlrcpp::Any visitDigitIdentifier(
      PrestoSqlParser::DigitIdentifierContext* context) = 0;

  virtual antlrcpp::Any visitDecimalLiteral(
      PrestoSqlParser::DecimalLiteralContext* context) = 0;

  virtual antlrcpp::Any visitDoubleLiteral(
      PrestoSqlParser::DoubleLiteralContext* context) = 0;

  virtual antlrcpp::Any visitIntegerLiteral(
      PrestoSqlParser::IntegerLiteralContext* context) = 0;

  virtual antlrcpp::Any visitConstraintSpecification(
      PrestoSqlParser::ConstraintSpecificationContext* context) = 0;

  virtual antlrcpp::Any visitNamedConstraintSpecification(
      PrestoSqlParser::NamedConstraintSpecificationContext* context) = 0;

  virtual antlrcpp::Any visitUnnamedConstraintSpecification(
      PrestoSqlParser::UnnamedConstraintSpecificationContext* context) = 0;

  virtual antlrcpp::Any visitConstraintType(
      PrestoSqlParser::ConstraintTypeContext* context) = 0;

  virtual antlrcpp::Any visitConstraintQualifiers(
      PrestoSqlParser::ConstraintQualifiersContext* context) = 0;

  virtual antlrcpp::Any visitConstraintQualifier(
      PrestoSqlParser::ConstraintQualifierContext* context) = 0;

  virtual antlrcpp::Any visitConstraintRely(
      PrestoSqlParser::ConstraintRelyContext* context) = 0;

  virtual antlrcpp::Any visitConstraintEnabled(
      PrestoSqlParser::ConstraintEnabledContext* context) = 0;

  virtual antlrcpp::Any visitConstraintEnforced(
      PrestoSqlParser::ConstraintEnforcedContext* context) = 0;

  virtual antlrcpp::Any visitNonReserved(
      PrestoSqlParser::NonReservedContext* context) = 0;
};

} // namespace facebook::velox::sql
