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

#include <antlr4-runtime.h>
#include <iostream>
#include "axiom/sql/presto/ast/AstNodesAll.h"
#include "axiom/sql/presto/grammar/PrestoSqlParser.h"
#include "axiom/sql/presto/grammar/PrestoSqlVisitor.h"

namespace axiom::sql::presto {

class AstBuilder : public PrestoSqlVisitor {
 public:
  AstBuilder(bool enableTracing = false) : enableTracing_{enableTracing} {}

  std::any visitSingleStatement(
      PrestoSqlParser::SingleStatementContext* ctx) override;

  std::any visitStandaloneExpression(
      PrestoSqlParser::StandaloneExpressionContext* ctx) override;

  std::any visitStandaloneRoutineBody(
      PrestoSqlParser::StandaloneRoutineBodyContext* ctx) override;

  std::any visitStatementDefault(
      PrestoSqlParser::StatementDefaultContext* ctx) override;

  std::any visitUse(PrestoSqlParser::UseContext* ctx) override;

  std::any visitCreateSchema(
      PrestoSqlParser::CreateSchemaContext* ctx) override;

  std::any visitDropSchema(PrestoSqlParser::DropSchemaContext* ctx) override;

  std::any visitRenameSchema(
      PrestoSqlParser::RenameSchemaContext* ctx) override;

  std::any visitCreateTableAsSelect(
      PrestoSqlParser::CreateTableAsSelectContext* ctx) override;

  std::any visitCreateTable(PrestoSqlParser::CreateTableContext* ctx) override;

  std::any visitDropTable(PrestoSqlParser::DropTableContext* ctx) override;

  std::any visitInsertInto(PrestoSqlParser::InsertIntoContext* ctx) override;

  std::any visitDelete(PrestoSqlParser::DeleteContext* ctx) override;

  std::any visitTruncateTable(
      PrestoSqlParser::TruncateTableContext* ctx) override;

  std::any visitRenameTable(PrestoSqlParser::RenameTableContext* ctx) override;

  std::any visitRenameColumn(
      PrestoSqlParser::RenameColumnContext* ctx) override;

  std::any visitDropColumn(PrestoSqlParser::DropColumnContext* ctx) override;

  std::any visitAddColumn(PrestoSqlParser::AddColumnContext* ctx) override;

  std::any visitAddConstraint(
      PrestoSqlParser::AddConstraintContext* ctx) override;

  std::any visitDropConstraint(
      PrestoSqlParser::DropConstraintContext* ctx) override;

  std::any visitAlterColumnSetNotNull(
      PrestoSqlParser::AlterColumnSetNotNullContext* ctx) override;

  std::any visitAlterColumnDropNotNull(
      PrestoSqlParser::AlterColumnDropNotNullContext* ctx) override;

  std::any visitSetTableProperties(
      PrestoSqlParser::SetTablePropertiesContext* ctx) override;

  std::any visitAnalyze(PrestoSqlParser::AnalyzeContext* ctx) override;

  std::any visitCreateType(PrestoSqlParser::CreateTypeContext* ctx) override;

  std::any visitCreateView(PrestoSqlParser::CreateViewContext* ctx) override;

  std::any visitRenameView(PrestoSqlParser::RenameViewContext* ctx) override;

  std::any visitDropView(PrestoSqlParser::DropViewContext* ctx) override;

  std::any visitCreateMaterializedView(
      PrestoSqlParser::CreateMaterializedViewContext* ctx) override;

  std::any visitDropMaterializedView(
      PrestoSqlParser::DropMaterializedViewContext* ctx) override;

  std::any visitRefreshMaterializedView(
      PrestoSqlParser::RefreshMaterializedViewContext* ctx) override;

  std::any visitCreateFunction(
      PrestoSqlParser::CreateFunctionContext* ctx) override;

  std::any visitAlterFunction(
      PrestoSqlParser::AlterFunctionContext* ctx) override;

  std::any visitDropFunction(
      PrestoSqlParser::DropFunctionContext* ctx) override;

  std::any visitCall(PrestoSqlParser::CallContext* ctx) override;

  std::any visitCreateRole(PrestoSqlParser::CreateRoleContext* ctx) override;

  std::any visitDropRole(PrestoSqlParser::DropRoleContext* ctx) override;

  std::any visitGrantRoles(PrestoSqlParser::GrantRolesContext* ctx) override;

  std::any visitRevokeRoles(PrestoSqlParser::RevokeRolesContext* ctx) override;

  std::any visitSetRole(PrestoSqlParser::SetRoleContext* ctx) override;

  std::any visitGrant(PrestoSqlParser::GrantContext* ctx) override;

  std::any visitRevoke(PrestoSqlParser::RevokeContext* ctx) override;

  std::any visitShowGrants(PrestoSqlParser::ShowGrantsContext* ctx) override;

  std::any visitExplain(PrestoSqlParser::ExplainContext* ctx) override;

  std::any visitShowCreateTable(
      PrestoSqlParser::ShowCreateTableContext* ctx) override;

  std::any visitShowCreateView(
      PrestoSqlParser::ShowCreateViewContext* ctx) override;

  std::any visitShowCreateMaterializedView(
      PrestoSqlParser::ShowCreateMaterializedViewContext* ctx) override;

  std::any visitShowCreateFunction(
      PrestoSqlParser::ShowCreateFunctionContext* ctx) override;

  std::any visitShowTables(PrestoSqlParser::ShowTablesContext* ctx) override;

  std::any visitShowSchemas(PrestoSqlParser::ShowSchemasContext* ctx) override;

  std::any visitShowCatalogs(
      PrestoSqlParser::ShowCatalogsContext* ctx) override;

  std::any visitShowColumns(PrestoSqlParser::ShowColumnsContext* ctx) override;

  std::any visitShowStats(PrestoSqlParser::ShowStatsContext* ctx) override;

  std::any visitShowStatsForQuery(
      PrestoSqlParser::ShowStatsForQueryContext* ctx) override;

  std::any visitShowRoles(PrestoSqlParser::ShowRolesContext* ctx) override;

  std::any visitShowRoleGrants(
      PrestoSqlParser::ShowRoleGrantsContext* ctx) override;

  std::any visitShowFunctions(
      PrestoSqlParser::ShowFunctionsContext* ctx) override;

  std::any visitShowSession(PrestoSqlParser::ShowSessionContext* ctx) override;

  std::any visitSetSession(PrestoSqlParser::SetSessionContext* ctx) override;

  std::any visitResetSession(
      PrestoSqlParser::ResetSessionContext* ctx) override;

  std::any visitStartTransaction(
      PrestoSqlParser::StartTransactionContext* ctx) override;

  std::any visitCommit(PrestoSqlParser::CommitContext* ctx) override;

  std::any visitRollback(PrestoSqlParser::RollbackContext* ctx) override;

  std::any visitPrepare(PrestoSqlParser::PrepareContext* ctx) override;

  std::any visitDeallocate(PrestoSqlParser::DeallocateContext* ctx) override;

  std::any visitExecute(PrestoSqlParser::ExecuteContext* ctx) override;

  std::any visitDescribeInput(
      PrestoSqlParser::DescribeInputContext* ctx) override;

  std::any visitDescribeOutput(
      PrestoSqlParser::DescribeOutputContext* ctx) override;

  std::any visitUpdate(PrestoSqlParser::UpdateContext* ctx) override;

  std::any visitQuery(PrestoSqlParser::QueryContext* ctx) override;

  std::any visitWith(PrestoSqlParser::WithContext* ctx) override;

  std::any visitTableElement(
      PrestoSqlParser::TableElementContext* ctx) override;

  std::any visitColumnDefinition(
      PrestoSqlParser::ColumnDefinitionContext* ctx) override;

  std::any visitLikeClause(PrestoSqlParser::LikeClauseContext* ctx) override;

  std::any visitProperties(PrestoSqlParser::PropertiesContext* ctx) override;

  std::any visitProperty(PrestoSqlParser::PropertyContext* ctx) override;

  std::any visitSqlParameterDeclaration(
      PrestoSqlParser::SqlParameterDeclarationContext* ctx) override;

  std::any visitRoutineCharacteristics(
      PrestoSqlParser::RoutineCharacteristicsContext* ctx) override;

  std::any visitRoutineCharacteristic(
      PrestoSqlParser::RoutineCharacteristicContext* ctx) override;

  std::any visitAlterRoutineCharacteristics(
      PrestoSqlParser::AlterRoutineCharacteristicsContext* ctx) override;

  std::any visitAlterRoutineCharacteristic(
      PrestoSqlParser::AlterRoutineCharacteristicContext* ctx) override;

  std::any visitRoutineBody(PrestoSqlParser::RoutineBodyContext* ctx) override;

  std::any visitReturnStatement(
      PrestoSqlParser::ReturnStatementContext* ctx) override;

  std::any visitExternalBodyReference(
      PrestoSqlParser::ExternalBodyReferenceContext* ctx) override;

  std::any visitLanguage(PrestoSqlParser::LanguageContext* ctx) override;

  std::any visitDeterminism(PrestoSqlParser::DeterminismContext* ctx) override;

  std::any visitNullCallClause(
      PrestoSqlParser::NullCallClauseContext* ctx) override;

  std::any visitExternalRoutineName(
      PrestoSqlParser::ExternalRoutineNameContext* ctx) override;

  std::any visitQueryNoWith(PrestoSqlParser::QueryNoWithContext* ctx) override;

  std::any visitQueryTermDefault(
      PrestoSqlParser::QueryTermDefaultContext* ctx) override;

  std::any visitSetOperation(
      PrestoSqlParser::SetOperationContext* ctx) override;

  std::any visitQueryPrimaryDefault(
      PrestoSqlParser::QueryPrimaryDefaultContext* ctx) override;

  std::any visitTable(PrestoSqlParser::TableContext* ctx) override;

  std::any visitInlineTable(PrestoSqlParser::InlineTableContext* ctx) override;

  std::any visitSubquery(PrestoSqlParser::SubqueryContext* ctx) override;

  std::any visitSortItem(PrestoSqlParser::SortItemContext* ctx) override;

  std::any visitQuerySpecification(
      PrestoSqlParser::QuerySpecificationContext* ctx) override;

  std::any visitGroupBy(PrestoSqlParser::GroupByContext* ctx) override;

  std::any visitSingleGroupingSet(
      PrestoSqlParser::SingleGroupingSetContext* ctx) override;

  std::any visitRollup(PrestoSqlParser::RollupContext* ctx) override;

  std::any visitCube(PrestoSqlParser::CubeContext* ctx) override;

  std::any visitMultipleGroupingSets(
      PrestoSqlParser::MultipleGroupingSetsContext* ctx) override;

  std::any visitGroupingSet(PrestoSqlParser::GroupingSetContext* ctx) override;

  std::any visitNamedQuery(PrestoSqlParser::NamedQueryContext* ctx) override;

  std::any visitSetQuantifier(
      PrestoSqlParser::SetQuantifierContext* ctx) override;

  std::any visitSelectSingle(
      PrestoSqlParser::SelectSingleContext* ctx) override;

  std::any visitSelectAll(PrestoSqlParser::SelectAllContext* ctx) override;

  std::any visitRelationDefault(
      PrestoSqlParser::RelationDefaultContext* ctx) override;

  std::any visitJoinRelation(
      PrestoSqlParser::JoinRelationContext* ctx) override;

  std::any visitJoinType(PrestoSqlParser::JoinTypeContext* ctx) override;

  std::any visitJoinCriteria(
      PrestoSqlParser::JoinCriteriaContext* ctx) override;

  std::any visitSampledRelation(
      PrestoSqlParser::SampledRelationContext* ctx) override;

  std::any visitSampleType(PrestoSqlParser::SampleTypeContext* ctx) override;

  std::any visitAliasedRelation(
      PrestoSqlParser::AliasedRelationContext* ctx) override;

  std::any visitColumnAliases(
      PrestoSqlParser::ColumnAliasesContext* ctx) override;

  std::any visitTableName(PrestoSqlParser::TableNameContext* ctx) override;

  std::any visitSubqueryRelation(
      PrestoSqlParser::SubqueryRelationContext* ctx) override;

  std::any visitUnnest(PrestoSqlParser::UnnestContext* ctx) override;

  std::any visitLateral(PrestoSqlParser::LateralContext* ctx) override;

  std::any visitParenthesizedRelation(
      PrestoSqlParser::ParenthesizedRelationContext* ctx) override;

  std::any visitExpression(PrestoSqlParser::ExpressionContext* ctx) override;

  std::any visitLogicalNot(PrestoSqlParser::LogicalNotContext* ctx) override;

  std::any visitPredicated(PrestoSqlParser::PredicatedContext* ctx) override;

  std::any visitLogicalBinary(
      PrestoSqlParser::LogicalBinaryContext* ctx) override;

  std::any visitComparison(PrestoSqlParser::ComparisonContext* ctx) override;

  std::any visitQuantifiedComparison(
      PrestoSqlParser::QuantifiedComparisonContext* ctx) override;

  std::any visitBetween(PrestoSqlParser::BetweenContext* ctx) override;

  std::any visitInList(PrestoSqlParser::InListContext* ctx) override;

  std::any visitInSubquery(PrestoSqlParser::InSubqueryContext* ctx) override;

  std::any visitLike(PrestoSqlParser::LikeContext* ctx) override;

  std::any visitNullPredicate(
      PrestoSqlParser::NullPredicateContext* ctx) override;

  std::any visitDistinctFrom(
      PrestoSqlParser::DistinctFromContext* ctx) override;

  std::any visitValueExpressionDefault(
      PrestoSqlParser::ValueExpressionDefaultContext* ctx) override;

  std::any visitConcatenation(
      PrestoSqlParser::ConcatenationContext* ctx) override;

  std::any visitArithmeticBinary(
      PrestoSqlParser::ArithmeticBinaryContext* ctx) override;

  std::any visitArithmeticUnary(
      PrestoSqlParser::ArithmeticUnaryContext* ctx) override;

  std::any visitAtTimeZone(PrestoSqlParser::AtTimeZoneContext* ctx) override;

  std::any visitDereference(PrestoSqlParser::DereferenceContext* ctx) override;

  std::any visitTypeConstructor(
      PrestoSqlParser::TypeConstructorContext* ctx) override;

  std::any visitSpecialDateTimeFunction(
      PrestoSqlParser::SpecialDateTimeFunctionContext* ctx) override;

  std::any visitSubstring(PrestoSqlParser::SubstringContext* ctx) override;

  std::any visitCast(PrestoSqlParser::CastContext* ctx) override;

  std::any visitLambda(PrestoSqlParser::LambdaContext* ctx) override;

  std::any visitParenthesizedExpression(
      PrestoSqlParser::ParenthesizedExpressionContext* ctx) override;

  std::any visitParameter(PrestoSqlParser::ParameterContext* ctx) override;

  std::any visitNormalize(PrestoSqlParser::NormalizeContext* ctx) override;

  std::any visitIntervalLiteral(
      PrestoSqlParser::IntervalLiteralContext* ctx) override;

  std::any visitNumericLiteral(
      PrestoSqlParser::NumericLiteralContext* ctx) override;

  std::any visitBooleanLiteral(
      PrestoSqlParser::BooleanLiteralContext* ctx) override;

  std::any visitSimpleCase(PrestoSqlParser::SimpleCaseContext* ctx) override;

  std::any visitColumnReference(
      PrestoSqlParser::ColumnReferenceContext* ctx) override;

  std::any visitNullLiteral(PrestoSqlParser::NullLiteralContext* ctx) override;

  std::any visitRowConstructor(
      PrestoSqlParser::RowConstructorContext* ctx) override;

  std::any visitSubscript(PrestoSqlParser::SubscriptContext* ctx) override;

  std::any visitSubqueryExpression(
      PrestoSqlParser::SubqueryExpressionContext* ctx) override;

  std::any visitBinaryLiteral(
      PrestoSqlParser::BinaryLiteralContext* ctx) override;

  std::any visitCurrentUser(PrestoSqlParser::CurrentUserContext* ctx) override;

  std::any visitExtract(PrestoSqlParser::ExtractContext* ctx) override;

  std::any visitStringLiteral(
      PrestoSqlParser::StringLiteralContext* ctx) override;

  std::any visitArrayConstructor(
      PrestoSqlParser::ArrayConstructorContext* ctx) override;

  std::any visitFunctionCall(
      PrestoSqlParser::FunctionCallContext* ctx) override;

  std::any visitExists(PrestoSqlParser::ExistsContext* ctx) override;

  std::any visitPosition(PrestoSqlParser::PositionContext* ctx) override;

  std::any visitSearchedCase(
      PrestoSqlParser::SearchedCaseContext* ctx) override;

  std::any visitGroupingOperation(
      PrestoSqlParser::GroupingOperationContext* ctx) override;

  std::any visitBasicStringLiteral(
      PrestoSqlParser::BasicStringLiteralContext* ctx) override;

  std::any visitUnicodeStringLiteral(
      PrestoSqlParser::UnicodeStringLiteralContext* ctx) override;

  std::any visitNullTreatment(
      PrestoSqlParser::NullTreatmentContext* ctx) override;

  std::any visitTimeZoneInterval(
      PrestoSqlParser::TimeZoneIntervalContext* ctx) override;

  std::any visitTimeZoneString(
      PrestoSqlParser::TimeZoneStringContext* ctx) override;

  std::any visitComparisonOperator(
      PrestoSqlParser::ComparisonOperatorContext* ctx) override;

  std::any visitComparisonQuantifier(
      PrestoSqlParser::ComparisonQuantifierContext* ctx) override;

  std::any visitBooleanValue(
      PrestoSqlParser::BooleanValueContext* ctx) override;

  std::any visitInterval(PrestoSqlParser::IntervalContext* ctx) override;

  std::any visitIntervalField(
      PrestoSqlParser::IntervalFieldContext* ctx) override;

  std::any visitNormalForm(PrestoSqlParser::NormalFormContext* ctx) override;

  std::any visitTypes(PrestoSqlParser::TypesContext* ctx) override;

  std::any visitType(PrestoSqlParser::TypeContext* ctx) override;

  std::any visitTypeParameter(
      PrestoSqlParser::TypeParameterContext* ctx) override;

  std::any visitBaseType(PrestoSqlParser::BaseTypeContext* ctx) override;

  std::any visitWhenClause(PrestoSqlParser::WhenClauseContext* ctx) override;

  std::any visitFilter(PrestoSqlParser::FilterContext* ctx) override;

  std::any visitOver(PrestoSqlParser::OverContext* ctx) override;

  std::any visitWindowFrame(PrestoSqlParser::WindowFrameContext* ctx) override;

  std::any visitUnboundedFrame(
      PrestoSqlParser::UnboundedFrameContext* ctx) override;

  std::any visitCurrentRowBound(
      PrestoSqlParser::CurrentRowBoundContext* ctx) override;

  std::any visitBoundedFrame(
      PrestoSqlParser::BoundedFrameContext* ctx) override;

  std::any visitUpdateAssignment(
      PrestoSqlParser::UpdateAssignmentContext* ctx) override;

  std::any visitExplainFormat(
      PrestoSqlParser::ExplainFormatContext* ctx) override;

  std::any visitExplainType(PrestoSqlParser::ExplainTypeContext* ctx) override;

  std::any visitIsolationLevel(
      PrestoSqlParser::IsolationLevelContext* ctx) override;

  std::any visitTransactionAccessMode(
      PrestoSqlParser::TransactionAccessModeContext* ctx) override;

  std::any visitReadUncommitted(
      PrestoSqlParser::ReadUncommittedContext* ctx) override;

  std::any visitReadCommitted(
      PrestoSqlParser::ReadCommittedContext* ctx) override;

  std::any visitRepeatableRead(
      PrestoSqlParser::RepeatableReadContext* ctx) override;

  std::any visitSerializable(
      PrestoSqlParser::SerializableContext* ctx) override;

  std::any visitPositionalArgument(
      PrestoSqlParser::PositionalArgumentContext* ctx) override;

  std::any visitNamedArgument(
      PrestoSqlParser::NamedArgumentContext* ctx) override;

  std::any visitPrivilege(PrestoSqlParser::PrivilegeContext* ctx) override;

  std::any visitQualifiedName(
      PrestoSqlParser::QualifiedNameContext* ctx) override;

  std::any visitTableVersion(
      PrestoSqlParser::TableVersionContext* ctx) override;

  std::any visitTableversionasof(
      PrestoSqlParser::TableversionasofContext* ctx) override;

  std::any visitTableversionbefore(
      PrestoSqlParser::TableversionbeforeContext* ctx) override;

  std::any visitCurrentUserGrantor(
      PrestoSqlParser::CurrentUserGrantorContext* ctx) override;

  std::any visitCurrentRoleGrantor(
      PrestoSqlParser::CurrentRoleGrantorContext* ctx) override;

  std::any visitSpecifiedPrincipal(
      PrestoSqlParser::SpecifiedPrincipalContext* ctx) override;

  std::any visitUserPrincipal(
      PrestoSqlParser::UserPrincipalContext* ctx) override;

  std::any visitRolePrincipal(
      PrestoSqlParser::RolePrincipalContext* ctx) override;

  std::any visitUnspecifiedPrincipal(
      PrestoSqlParser::UnspecifiedPrincipalContext* ctx) override;

  std::any visitRoles(PrestoSqlParser::RolesContext* ctx) override;

  std::any visitUnquotedIdentifier(
      PrestoSqlParser::UnquotedIdentifierContext* ctx) override;

  std::any visitQuotedIdentifier(
      PrestoSqlParser::QuotedIdentifierContext* ctx) override;

  std::any visitBackQuotedIdentifier(
      PrestoSqlParser::BackQuotedIdentifierContext* ctx) override;

  std::any visitDigitIdentifier(
      PrestoSqlParser::DigitIdentifierContext* ctx) override;

  std::any visitDecimalLiteral(
      PrestoSqlParser::DecimalLiteralContext* ctx) override;

  std::any visitDoubleLiteral(
      PrestoSqlParser::DoubleLiteralContext* ctx) override;

  std::any visitIntegerLiteral(
      PrestoSqlParser::IntegerLiteralContext* ctx) override;

  std::any visitConstraintSpecification(
      PrestoSqlParser::ConstraintSpecificationContext* ctx) override;

  std::any visitNamedConstraintSpecification(
      PrestoSqlParser::NamedConstraintSpecificationContext* ctx) override;

  std::any visitUnnamedConstraintSpecification(
      PrestoSqlParser::UnnamedConstraintSpecificationContext* ctx) override;

  std::any visitConstraintType(
      PrestoSqlParser::ConstraintTypeContext* ctx) override;

  std::any visitConstraintQualifiers(
      PrestoSqlParser::ConstraintQualifiersContext* ctx) override;

  std::any visitConstraintQualifier(
      PrestoSqlParser::ConstraintQualifierContext* ctx) override;

  std::any visitConstraintRely(
      PrestoSqlParser::ConstraintRelyContext* ctx) override;

  std::any visitConstraintEnabled(
      PrestoSqlParser::ConstraintEnabledContext* ctx) override;

  std::any visitConstraintEnforced(
      PrestoSqlParser::ConstraintEnforcedContext* ctx) override;

  std::any visitNonReserved(PrestoSqlParser::NonReservedContext* ctx) override;

 private:
  template <typename T, typename TContext>
  std::vector<std::shared_ptr<T>> visitTyped(
      const std::vector<TContext*>& contexts) {
    std::vector<std::shared_ptr<T>> result;
    result.reserve(contexts.size());
    for (auto ctx : contexts) {
      result.emplace_back(visitTyped<T>(ctx));
    }
    return result;
  }

  template <typename T, typename TContext>
  std::shared_ptr<T> visitTyped(TContext* ctx) {
    if (ctx == nullptr) {
      return nullptr;
    }

    return std::any_cast<std::shared_ptr<T>>(visit(ctx));
  }

  template <typename TContext>
  ExpressionPtr visitExpression(TContext* ctx) {
    return visitTyped<Expression>(ctx);
  }

  IdentifierPtr visitIdentifier(PrestoSqlParser::IdentifierContext* ctx) {
    return visitTyped<Identifier>(ctx);
  }

  std::any aggregateResult(std::any aggregate, std::any nextResult) override {
    if (aggregate.has_value()) {
      throw std::runtime_error(
          "aggregateResult called with non-null aggregate");
    }

    return nextResult;
  }

  std::any visitChildren(antlr4::tree::ParseTree* node) override {
    if (enableTracing_) {
      std::cout << "Visiting children: " << node->children.size() << std::endl;
    }

    return PrestoSqlVisitor::visitChildren(node);
  }

  QualifiedNamePtr getQualifiedName(PrestoSqlParser::QualifiedNameContext* ctx);

  void trace(std::string_view name) const;

  const bool enableTracing_;
};

} // namespace axiom::sql::presto
