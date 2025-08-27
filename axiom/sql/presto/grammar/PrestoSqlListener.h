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
 * This interface defines an abstract listener for a parse tree produced by
 * PrestoSqlParser.
 */
class PrestoSqlListener : public antlr4::tree::ParseTreeListener {
 public:
  virtual void enterSingleStatement(
      PrestoSqlParser::SingleStatementContext* ctx) = 0;
  virtual void exitSingleStatement(
      PrestoSqlParser::SingleStatementContext* ctx) = 0;

  virtual void enterStandaloneExpression(
      PrestoSqlParser::StandaloneExpressionContext* ctx) = 0;
  virtual void exitStandaloneExpression(
      PrestoSqlParser::StandaloneExpressionContext* ctx) = 0;

  virtual void enterStandaloneRoutineBody(
      PrestoSqlParser::StandaloneRoutineBodyContext* ctx) = 0;
  virtual void exitStandaloneRoutineBody(
      PrestoSqlParser::StandaloneRoutineBodyContext* ctx) = 0;

  virtual void enterStatementDefault(
      PrestoSqlParser::StatementDefaultContext* ctx) = 0;
  virtual void exitStatementDefault(
      PrestoSqlParser::StatementDefaultContext* ctx) = 0;

  virtual void enterUse(PrestoSqlParser::UseContext* ctx) = 0;
  virtual void exitUse(PrestoSqlParser::UseContext* ctx) = 0;

  virtual void enterCreateSchema(PrestoSqlParser::CreateSchemaContext* ctx) = 0;
  virtual void exitCreateSchema(PrestoSqlParser::CreateSchemaContext* ctx) = 0;

  virtual void enterDropSchema(PrestoSqlParser::DropSchemaContext* ctx) = 0;
  virtual void exitDropSchema(PrestoSqlParser::DropSchemaContext* ctx) = 0;

  virtual void enterRenameSchema(PrestoSqlParser::RenameSchemaContext* ctx) = 0;
  virtual void exitRenameSchema(PrestoSqlParser::RenameSchemaContext* ctx) = 0;

  virtual void enterCreateTableAsSelect(
      PrestoSqlParser::CreateTableAsSelectContext* ctx) = 0;
  virtual void exitCreateTableAsSelect(
      PrestoSqlParser::CreateTableAsSelectContext* ctx) = 0;

  virtual void enterCreateTable(PrestoSqlParser::CreateTableContext* ctx) = 0;
  virtual void exitCreateTable(PrestoSqlParser::CreateTableContext* ctx) = 0;

  virtual void enterDropTable(PrestoSqlParser::DropTableContext* ctx) = 0;
  virtual void exitDropTable(PrestoSqlParser::DropTableContext* ctx) = 0;

  virtual void enterInsertInto(PrestoSqlParser::InsertIntoContext* ctx) = 0;
  virtual void exitInsertInto(PrestoSqlParser::InsertIntoContext* ctx) = 0;

  virtual void enterDelete(PrestoSqlParser::DeleteContext* ctx) = 0;
  virtual void exitDelete(PrestoSqlParser::DeleteContext* ctx) = 0;

  virtual void enterTruncateTable(
      PrestoSqlParser::TruncateTableContext* ctx) = 0;
  virtual void exitTruncateTable(
      PrestoSqlParser::TruncateTableContext* ctx) = 0;

  virtual void enterRenameTable(PrestoSqlParser::RenameTableContext* ctx) = 0;
  virtual void exitRenameTable(PrestoSqlParser::RenameTableContext* ctx) = 0;

  virtual void enterRenameColumn(PrestoSqlParser::RenameColumnContext* ctx) = 0;
  virtual void exitRenameColumn(PrestoSqlParser::RenameColumnContext* ctx) = 0;

  virtual void enterDropColumn(PrestoSqlParser::DropColumnContext* ctx) = 0;
  virtual void exitDropColumn(PrestoSqlParser::DropColumnContext* ctx) = 0;

  virtual void enterAddColumn(PrestoSqlParser::AddColumnContext* ctx) = 0;
  virtual void exitAddColumn(PrestoSqlParser::AddColumnContext* ctx) = 0;

  virtual void enterAddConstraint(
      PrestoSqlParser::AddConstraintContext* ctx) = 0;
  virtual void exitAddConstraint(
      PrestoSqlParser::AddConstraintContext* ctx) = 0;

  virtual void enterDropConstraint(
      PrestoSqlParser::DropConstraintContext* ctx) = 0;
  virtual void exitDropConstraint(
      PrestoSqlParser::DropConstraintContext* ctx) = 0;

  virtual void enterAlterColumnSetNotNull(
      PrestoSqlParser::AlterColumnSetNotNullContext* ctx) = 0;
  virtual void exitAlterColumnSetNotNull(
      PrestoSqlParser::AlterColumnSetNotNullContext* ctx) = 0;

  virtual void enterAlterColumnDropNotNull(
      PrestoSqlParser::AlterColumnDropNotNullContext* ctx) = 0;
  virtual void exitAlterColumnDropNotNull(
      PrestoSqlParser::AlterColumnDropNotNullContext* ctx) = 0;

  virtual void enterSetTableProperties(
      PrestoSqlParser::SetTablePropertiesContext* ctx) = 0;
  virtual void exitSetTableProperties(
      PrestoSqlParser::SetTablePropertiesContext* ctx) = 0;

  virtual void enterAnalyze(PrestoSqlParser::AnalyzeContext* ctx) = 0;
  virtual void exitAnalyze(PrestoSqlParser::AnalyzeContext* ctx) = 0;

  virtual void enterCreateType(PrestoSqlParser::CreateTypeContext* ctx) = 0;
  virtual void exitCreateType(PrestoSqlParser::CreateTypeContext* ctx) = 0;

  virtual void enterCreateView(PrestoSqlParser::CreateViewContext* ctx) = 0;
  virtual void exitCreateView(PrestoSqlParser::CreateViewContext* ctx) = 0;

  virtual void enterRenameView(PrestoSqlParser::RenameViewContext* ctx) = 0;
  virtual void exitRenameView(PrestoSqlParser::RenameViewContext* ctx) = 0;

  virtual void enterDropView(PrestoSqlParser::DropViewContext* ctx) = 0;
  virtual void exitDropView(PrestoSqlParser::DropViewContext* ctx) = 0;

  virtual void enterCreateMaterializedView(
      PrestoSqlParser::CreateMaterializedViewContext* ctx) = 0;
  virtual void exitCreateMaterializedView(
      PrestoSqlParser::CreateMaterializedViewContext* ctx) = 0;

  virtual void enterDropMaterializedView(
      PrestoSqlParser::DropMaterializedViewContext* ctx) = 0;
  virtual void exitDropMaterializedView(
      PrestoSqlParser::DropMaterializedViewContext* ctx) = 0;

  virtual void enterRefreshMaterializedView(
      PrestoSqlParser::RefreshMaterializedViewContext* ctx) = 0;
  virtual void exitRefreshMaterializedView(
      PrestoSqlParser::RefreshMaterializedViewContext* ctx) = 0;

  virtual void enterCreateFunction(
      PrestoSqlParser::CreateFunctionContext* ctx) = 0;
  virtual void exitCreateFunction(
      PrestoSqlParser::CreateFunctionContext* ctx) = 0;

  virtual void enterAlterFunction(
      PrestoSqlParser::AlterFunctionContext* ctx) = 0;
  virtual void exitAlterFunction(
      PrestoSqlParser::AlterFunctionContext* ctx) = 0;

  virtual void enterDropFunction(PrestoSqlParser::DropFunctionContext* ctx) = 0;
  virtual void exitDropFunction(PrestoSqlParser::DropFunctionContext* ctx) = 0;

  virtual void enterCall(PrestoSqlParser::CallContext* ctx) = 0;
  virtual void exitCall(PrestoSqlParser::CallContext* ctx) = 0;

  virtual void enterCreateRole(PrestoSqlParser::CreateRoleContext* ctx) = 0;
  virtual void exitCreateRole(PrestoSqlParser::CreateRoleContext* ctx) = 0;

  virtual void enterDropRole(PrestoSqlParser::DropRoleContext* ctx) = 0;
  virtual void exitDropRole(PrestoSqlParser::DropRoleContext* ctx) = 0;

  virtual void enterGrantRoles(PrestoSqlParser::GrantRolesContext* ctx) = 0;
  virtual void exitGrantRoles(PrestoSqlParser::GrantRolesContext* ctx) = 0;

  virtual void enterRevokeRoles(PrestoSqlParser::RevokeRolesContext* ctx) = 0;
  virtual void exitRevokeRoles(PrestoSqlParser::RevokeRolesContext* ctx) = 0;

  virtual void enterSetRole(PrestoSqlParser::SetRoleContext* ctx) = 0;
  virtual void exitSetRole(PrestoSqlParser::SetRoleContext* ctx) = 0;

  virtual void enterGrant(PrestoSqlParser::GrantContext* ctx) = 0;
  virtual void exitGrant(PrestoSqlParser::GrantContext* ctx) = 0;

  virtual void enterRevoke(PrestoSqlParser::RevokeContext* ctx) = 0;
  virtual void exitRevoke(PrestoSqlParser::RevokeContext* ctx) = 0;

  virtual void enterShowGrants(PrestoSqlParser::ShowGrantsContext* ctx) = 0;
  virtual void exitShowGrants(PrestoSqlParser::ShowGrantsContext* ctx) = 0;

  virtual void enterExplain(PrestoSqlParser::ExplainContext* ctx) = 0;
  virtual void exitExplain(PrestoSqlParser::ExplainContext* ctx) = 0;

  virtual void enterShowCreateTable(
      PrestoSqlParser::ShowCreateTableContext* ctx) = 0;
  virtual void exitShowCreateTable(
      PrestoSqlParser::ShowCreateTableContext* ctx) = 0;

  virtual void enterShowCreateView(
      PrestoSqlParser::ShowCreateViewContext* ctx) = 0;
  virtual void exitShowCreateView(
      PrestoSqlParser::ShowCreateViewContext* ctx) = 0;

  virtual void enterShowCreateMaterializedView(
      PrestoSqlParser::ShowCreateMaterializedViewContext* ctx) = 0;
  virtual void exitShowCreateMaterializedView(
      PrestoSqlParser::ShowCreateMaterializedViewContext* ctx) = 0;

  virtual void enterShowCreateFunction(
      PrestoSqlParser::ShowCreateFunctionContext* ctx) = 0;
  virtual void exitShowCreateFunction(
      PrestoSqlParser::ShowCreateFunctionContext* ctx) = 0;

  virtual void enterShowTables(PrestoSqlParser::ShowTablesContext* ctx) = 0;
  virtual void exitShowTables(PrestoSqlParser::ShowTablesContext* ctx) = 0;

  virtual void enterShowSchemas(PrestoSqlParser::ShowSchemasContext* ctx) = 0;
  virtual void exitShowSchemas(PrestoSqlParser::ShowSchemasContext* ctx) = 0;

  virtual void enterShowCatalogs(PrestoSqlParser::ShowCatalogsContext* ctx) = 0;
  virtual void exitShowCatalogs(PrestoSqlParser::ShowCatalogsContext* ctx) = 0;

  virtual void enterShowColumns(PrestoSqlParser::ShowColumnsContext* ctx) = 0;
  virtual void exitShowColumns(PrestoSqlParser::ShowColumnsContext* ctx) = 0;

  virtual void enterShowStats(PrestoSqlParser::ShowStatsContext* ctx) = 0;
  virtual void exitShowStats(PrestoSqlParser::ShowStatsContext* ctx) = 0;

  virtual void enterShowStatsForQuery(
      PrestoSqlParser::ShowStatsForQueryContext* ctx) = 0;
  virtual void exitShowStatsForQuery(
      PrestoSqlParser::ShowStatsForQueryContext* ctx) = 0;

  virtual void enterShowRoles(PrestoSqlParser::ShowRolesContext* ctx) = 0;
  virtual void exitShowRoles(PrestoSqlParser::ShowRolesContext* ctx) = 0;

  virtual void enterShowRoleGrants(
      PrestoSqlParser::ShowRoleGrantsContext* ctx) = 0;
  virtual void exitShowRoleGrants(
      PrestoSqlParser::ShowRoleGrantsContext* ctx) = 0;

  virtual void enterShowFunctions(
      PrestoSqlParser::ShowFunctionsContext* ctx) = 0;
  virtual void exitShowFunctions(
      PrestoSqlParser::ShowFunctionsContext* ctx) = 0;

  virtual void enterShowSession(PrestoSqlParser::ShowSessionContext* ctx) = 0;
  virtual void exitShowSession(PrestoSqlParser::ShowSessionContext* ctx) = 0;

  virtual void enterSetSession(PrestoSqlParser::SetSessionContext* ctx) = 0;
  virtual void exitSetSession(PrestoSqlParser::SetSessionContext* ctx) = 0;

  virtual void enterResetSession(PrestoSqlParser::ResetSessionContext* ctx) = 0;
  virtual void exitResetSession(PrestoSqlParser::ResetSessionContext* ctx) = 0;

  virtual void enterStartTransaction(
      PrestoSqlParser::StartTransactionContext* ctx) = 0;
  virtual void exitStartTransaction(
      PrestoSqlParser::StartTransactionContext* ctx) = 0;

  virtual void enterCommit(PrestoSqlParser::CommitContext* ctx) = 0;
  virtual void exitCommit(PrestoSqlParser::CommitContext* ctx) = 0;

  virtual void enterRollback(PrestoSqlParser::RollbackContext* ctx) = 0;
  virtual void exitRollback(PrestoSqlParser::RollbackContext* ctx) = 0;

  virtual void enterPrepare(PrestoSqlParser::PrepareContext* ctx) = 0;
  virtual void exitPrepare(PrestoSqlParser::PrepareContext* ctx) = 0;

  virtual void enterDeallocate(PrestoSqlParser::DeallocateContext* ctx) = 0;
  virtual void exitDeallocate(PrestoSqlParser::DeallocateContext* ctx) = 0;

  virtual void enterExecute(PrestoSqlParser::ExecuteContext* ctx) = 0;
  virtual void exitExecute(PrestoSqlParser::ExecuteContext* ctx) = 0;

  virtual void enterDescribeInput(
      PrestoSqlParser::DescribeInputContext* ctx) = 0;
  virtual void exitDescribeInput(
      PrestoSqlParser::DescribeInputContext* ctx) = 0;

  virtual void enterDescribeOutput(
      PrestoSqlParser::DescribeOutputContext* ctx) = 0;
  virtual void exitDescribeOutput(
      PrestoSqlParser::DescribeOutputContext* ctx) = 0;

  virtual void enterUpdate(PrestoSqlParser::UpdateContext* ctx) = 0;
  virtual void exitUpdate(PrestoSqlParser::UpdateContext* ctx) = 0;

  virtual void enterQuery(PrestoSqlParser::QueryContext* ctx) = 0;
  virtual void exitQuery(PrestoSqlParser::QueryContext* ctx) = 0;

  virtual void enterWith(PrestoSqlParser::WithContext* ctx) = 0;
  virtual void exitWith(PrestoSqlParser::WithContext* ctx) = 0;

  virtual void enterTableElement(PrestoSqlParser::TableElementContext* ctx) = 0;
  virtual void exitTableElement(PrestoSqlParser::TableElementContext* ctx) = 0;

  virtual void enterColumnDefinition(
      PrestoSqlParser::ColumnDefinitionContext* ctx) = 0;
  virtual void exitColumnDefinition(
      PrestoSqlParser::ColumnDefinitionContext* ctx) = 0;

  virtual void enterLikeClause(PrestoSqlParser::LikeClauseContext* ctx) = 0;
  virtual void exitLikeClause(PrestoSqlParser::LikeClauseContext* ctx) = 0;

  virtual void enterProperties(PrestoSqlParser::PropertiesContext* ctx) = 0;
  virtual void exitProperties(PrestoSqlParser::PropertiesContext* ctx) = 0;

  virtual void enterProperty(PrestoSqlParser::PropertyContext* ctx) = 0;
  virtual void exitProperty(PrestoSqlParser::PropertyContext* ctx) = 0;

  virtual void enterSqlParameterDeclaration(
      PrestoSqlParser::SqlParameterDeclarationContext* ctx) = 0;
  virtual void exitSqlParameterDeclaration(
      PrestoSqlParser::SqlParameterDeclarationContext* ctx) = 0;

  virtual void enterRoutineCharacteristics(
      PrestoSqlParser::RoutineCharacteristicsContext* ctx) = 0;
  virtual void exitRoutineCharacteristics(
      PrestoSqlParser::RoutineCharacteristicsContext* ctx) = 0;

  virtual void enterRoutineCharacteristic(
      PrestoSqlParser::RoutineCharacteristicContext* ctx) = 0;
  virtual void exitRoutineCharacteristic(
      PrestoSqlParser::RoutineCharacteristicContext* ctx) = 0;

  virtual void enterAlterRoutineCharacteristics(
      PrestoSqlParser::AlterRoutineCharacteristicsContext* ctx) = 0;
  virtual void exitAlterRoutineCharacteristics(
      PrestoSqlParser::AlterRoutineCharacteristicsContext* ctx) = 0;

  virtual void enterAlterRoutineCharacteristic(
      PrestoSqlParser::AlterRoutineCharacteristicContext* ctx) = 0;
  virtual void exitAlterRoutineCharacteristic(
      PrestoSqlParser::AlterRoutineCharacteristicContext* ctx) = 0;

  virtual void enterRoutineBody(PrestoSqlParser::RoutineBodyContext* ctx) = 0;
  virtual void exitRoutineBody(PrestoSqlParser::RoutineBodyContext* ctx) = 0;

  virtual void enterReturnStatement(
      PrestoSqlParser::ReturnStatementContext* ctx) = 0;
  virtual void exitReturnStatement(
      PrestoSqlParser::ReturnStatementContext* ctx) = 0;

  virtual void enterExternalBodyReference(
      PrestoSqlParser::ExternalBodyReferenceContext* ctx) = 0;
  virtual void exitExternalBodyReference(
      PrestoSqlParser::ExternalBodyReferenceContext* ctx) = 0;

  virtual void enterLanguage(PrestoSqlParser::LanguageContext* ctx) = 0;
  virtual void exitLanguage(PrestoSqlParser::LanguageContext* ctx) = 0;

  virtual void enterDeterminism(PrestoSqlParser::DeterminismContext* ctx) = 0;
  virtual void exitDeterminism(PrestoSqlParser::DeterminismContext* ctx) = 0;

  virtual void enterNullCallClause(
      PrestoSqlParser::NullCallClauseContext* ctx) = 0;
  virtual void exitNullCallClause(
      PrestoSqlParser::NullCallClauseContext* ctx) = 0;

  virtual void enterExternalRoutineName(
      PrestoSqlParser::ExternalRoutineNameContext* ctx) = 0;
  virtual void exitExternalRoutineName(
      PrestoSqlParser::ExternalRoutineNameContext* ctx) = 0;

  virtual void enterQueryNoWith(PrestoSqlParser::QueryNoWithContext* ctx) = 0;
  virtual void exitQueryNoWith(PrestoSqlParser::QueryNoWithContext* ctx) = 0;

  virtual void enterQueryTermDefault(
      PrestoSqlParser::QueryTermDefaultContext* ctx) = 0;
  virtual void exitQueryTermDefault(
      PrestoSqlParser::QueryTermDefaultContext* ctx) = 0;

  virtual void enterSetOperation(PrestoSqlParser::SetOperationContext* ctx) = 0;
  virtual void exitSetOperation(PrestoSqlParser::SetOperationContext* ctx) = 0;

  virtual void enterQueryPrimaryDefault(
      PrestoSqlParser::QueryPrimaryDefaultContext* ctx) = 0;
  virtual void exitQueryPrimaryDefault(
      PrestoSqlParser::QueryPrimaryDefaultContext* ctx) = 0;

  virtual void enterTable(PrestoSqlParser::TableContext* ctx) = 0;
  virtual void exitTable(PrestoSqlParser::TableContext* ctx) = 0;

  virtual void enterInlineTable(PrestoSqlParser::InlineTableContext* ctx) = 0;
  virtual void exitInlineTable(PrestoSqlParser::InlineTableContext* ctx) = 0;

  virtual void enterSubquery(PrestoSqlParser::SubqueryContext* ctx) = 0;
  virtual void exitSubquery(PrestoSqlParser::SubqueryContext* ctx) = 0;

  virtual void enterSortItem(PrestoSqlParser::SortItemContext* ctx) = 0;
  virtual void exitSortItem(PrestoSqlParser::SortItemContext* ctx) = 0;

  virtual void enterQuerySpecification(
      PrestoSqlParser::QuerySpecificationContext* ctx) = 0;
  virtual void exitQuerySpecification(
      PrestoSqlParser::QuerySpecificationContext* ctx) = 0;

  virtual void enterGroupBy(PrestoSqlParser::GroupByContext* ctx) = 0;
  virtual void exitGroupBy(PrestoSqlParser::GroupByContext* ctx) = 0;

  virtual void enterSingleGroupingSet(
      PrestoSqlParser::SingleGroupingSetContext* ctx) = 0;
  virtual void exitSingleGroupingSet(
      PrestoSqlParser::SingleGroupingSetContext* ctx) = 0;

  virtual void enterRollup(PrestoSqlParser::RollupContext* ctx) = 0;
  virtual void exitRollup(PrestoSqlParser::RollupContext* ctx) = 0;

  virtual void enterCube(PrestoSqlParser::CubeContext* ctx) = 0;
  virtual void exitCube(PrestoSqlParser::CubeContext* ctx) = 0;

  virtual void enterMultipleGroupingSets(
      PrestoSqlParser::MultipleGroupingSetsContext* ctx) = 0;
  virtual void exitMultipleGroupingSets(
      PrestoSqlParser::MultipleGroupingSetsContext* ctx) = 0;

  virtual void enterGroupingSet(PrestoSqlParser::GroupingSetContext* ctx) = 0;
  virtual void exitGroupingSet(PrestoSqlParser::GroupingSetContext* ctx) = 0;

  virtual void enterNamedQuery(PrestoSqlParser::NamedQueryContext* ctx) = 0;
  virtual void exitNamedQuery(PrestoSqlParser::NamedQueryContext* ctx) = 0;

  virtual void enterSetQuantifier(
      PrestoSqlParser::SetQuantifierContext* ctx) = 0;
  virtual void exitSetQuantifier(
      PrestoSqlParser::SetQuantifierContext* ctx) = 0;

  virtual void enterSelectSingle(PrestoSqlParser::SelectSingleContext* ctx) = 0;
  virtual void exitSelectSingle(PrestoSqlParser::SelectSingleContext* ctx) = 0;

  virtual void enterSelectAll(PrestoSqlParser::SelectAllContext* ctx) = 0;
  virtual void exitSelectAll(PrestoSqlParser::SelectAllContext* ctx) = 0;

  virtual void enterRelationDefault(
      PrestoSqlParser::RelationDefaultContext* ctx) = 0;
  virtual void exitRelationDefault(
      PrestoSqlParser::RelationDefaultContext* ctx) = 0;

  virtual void enterJoinRelation(PrestoSqlParser::JoinRelationContext* ctx) = 0;
  virtual void exitJoinRelation(PrestoSqlParser::JoinRelationContext* ctx) = 0;

  virtual void enterJoinType(PrestoSqlParser::JoinTypeContext* ctx) = 0;
  virtual void exitJoinType(PrestoSqlParser::JoinTypeContext* ctx) = 0;

  virtual void enterJoinCriteria(PrestoSqlParser::JoinCriteriaContext* ctx) = 0;
  virtual void exitJoinCriteria(PrestoSqlParser::JoinCriteriaContext* ctx) = 0;

  virtual void enterSampledRelation(
      PrestoSqlParser::SampledRelationContext* ctx) = 0;
  virtual void exitSampledRelation(
      PrestoSqlParser::SampledRelationContext* ctx) = 0;

  virtual void enterSampleType(PrestoSqlParser::SampleTypeContext* ctx) = 0;
  virtual void exitSampleType(PrestoSqlParser::SampleTypeContext* ctx) = 0;

  virtual void enterAliasedRelation(
      PrestoSqlParser::AliasedRelationContext* ctx) = 0;
  virtual void exitAliasedRelation(
      PrestoSqlParser::AliasedRelationContext* ctx) = 0;

  virtual void enterColumnAliases(
      PrestoSqlParser::ColumnAliasesContext* ctx) = 0;
  virtual void exitColumnAliases(
      PrestoSqlParser::ColumnAliasesContext* ctx) = 0;

  virtual void enterTableName(PrestoSqlParser::TableNameContext* ctx) = 0;
  virtual void exitTableName(PrestoSqlParser::TableNameContext* ctx) = 0;

  virtual void enterSubqueryRelation(
      PrestoSqlParser::SubqueryRelationContext* ctx) = 0;
  virtual void exitSubqueryRelation(
      PrestoSqlParser::SubqueryRelationContext* ctx) = 0;

  virtual void enterUnnest(PrestoSqlParser::UnnestContext* ctx) = 0;
  virtual void exitUnnest(PrestoSqlParser::UnnestContext* ctx) = 0;

  virtual void enterLateral(PrestoSqlParser::LateralContext* ctx) = 0;
  virtual void exitLateral(PrestoSqlParser::LateralContext* ctx) = 0;

  virtual void enterParenthesizedRelation(
      PrestoSqlParser::ParenthesizedRelationContext* ctx) = 0;
  virtual void exitParenthesizedRelation(
      PrestoSqlParser::ParenthesizedRelationContext* ctx) = 0;

  virtual void enterExpression(PrestoSqlParser::ExpressionContext* ctx) = 0;
  virtual void exitExpression(PrestoSqlParser::ExpressionContext* ctx) = 0;

  virtual void enterLogicalNot(PrestoSqlParser::LogicalNotContext* ctx) = 0;
  virtual void exitLogicalNot(PrestoSqlParser::LogicalNotContext* ctx) = 0;

  virtual void enterPredicated(PrestoSqlParser::PredicatedContext* ctx) = 0;
  virtual void exitPredicated(PrestoSqlParser::PredicatedContext* ctx) = 0;

  virtual void enterLogicalBinary(
      PrestoSqlParser::LogicalBinaryContext* ctx) = 0;
  virtual void exitLogicalBinary(
      PrestoSqlParser::LogicalBinaryContext* ctx) = 0;

  virtual void enterComparison(PrestoSqlParser::ComparisonContext* ctx) = 0;
  virtual void exitComparison(PrestoSqlParser::ComparisonContext* ctx) = 0;

  virtual void enterQuantifiedComparison(
      PrestoSqlParser::QuantifiedComparisonContext* ctx) = 0;
  virtual void exitQuantifiedComparison(
      PrestoSqlParser::QuantifiedComparisonContext* ctx) = 0;

  virtual void enterBetween(PrestoSqlParser::BetweenContext* ctx) = 0;
  virtual void exitBetween(PrestoSqlParser::BetweenContext* ctx) = 0;

  virtual void enterInList(PrestoSqlParser::InListContext* ctx) = 0;
  virtual void exitInList(PrestoSqlParser::InListContext* ctx) = 0;

  virtual void enterInSubquery(PrestoSqlParser::InSubqueryContext* ctx) = 0;
  virtual void exitInSubquery(PrestoSqlParser::InSubqueryContext* ctx) = 0;

  virtual void enterLike(PrestoSqlParser::LikeContext* ctx) = 0;
  virtual void exitLike(PrestoSqlParser::LikeContext* ctx) = 0;

  virtual void enterNullPredicate(
      PrestoSqlParser::NullPredicateContext* ctx) = 0;
  virtual void exitNullPredicate(
      PrestoSqlParser::NullPredicateContext* ctx) = 0;

  virtual void enterDistinctFrom(PrestoSqlParser::DistinctFromContext* ctx) = 0;
  virtual void exitDistinctFrom(PrestoSqlParser::DistinctFromContext* ctx) = 0;

  virtual void enterValueExpressionDefault(
      PrestoSqlParser::ValueExpressionDefaultContext* ctx) = 0;
  virtual void exitValueExpressionDefault(
      PrestoSqlParser::ValueExpressionDefaultContext* ctx) = 0;

  virtual void enterConcatenation(
      PrestoSqlParser::ConcatenationContext* ctx) = 0;
  virtual void exitConcatenation(
      PrestoSqlParser::ConcatenationContext* ctx) = 0;

  virtual void enterArithmeticBinary(
      PrestoSqlParser::ArithmeticBinaryContext* ctx) = 0;
  virtual void exitArithmeticBinary(
      PrestoSqlParser::ArithmeticBinaryContext* ctx) = 0;

  virtual void enterArithmeticUnary(
      PrestoSqlParser::ArithmeticUnaryContext* ctx) = 0;
  virtual void exitArithmeticUnary(
      PrestoSqlParser::ArithmeticUnaryContext* ctx) = 0;

  virtual void enterAtTimeZone(PrestoSqlParser::AtTimeZoneContext* ctx) = 0;
  virtual void exitAtTimeZone(PrestoSqlParser::AtTimeZoneContext* ctx) = 0;

  virtual void enterDereference(PrestoSqlParser::DereferenceContext* ctx) = 0;
  virtual void exitDereference(PrestoSqlParser::DereferenceContext* ctx) = 0;

  virtual void enterTypeConstructor(
      PrestoSqlParser::TypeConstructorContext* ctx) = 0;
  virtual void exitTypeConstructor(
      PrestoSqlParser::TypeConstructorContext* ctx) = 0;

  virtual void enterSpecialDateTimeFunction(
      PrestoSqlParser::SpecialDateTimeFunctionContext* ctx) = 0;
  virtual void exitSpecialDateTimeFunction(
      PrestoSqlParser::SpecialDateTimeFunctionContext* ctx) = 0;

  virtual void enterSubstring(PrestoSqlParser::SubstringContext* ctx) = 0;
  virtual void exitSubstring(PrestoSqlParser::SubstringContext* ctx) = 0;

  virtual void enterCast(PrestoSqlParser::CastContext* ctx) = 0;
  virtual void exitCast(PrestoSqlParser::CastContext* ctx) = 0;

  virtual void enterLambda(PrestoSqlParser::LambdaContext* ctx) = 0;
  virtual void exitLambda(PrestoSqlParser::LambdaContext* ctx) = 0;

  virtual void enterParenthesizedExpression(
      PrestoSqlParser::ParenthesizedExpressionContext* ctx) = 0;
  virtual void exitParenthesizedExpression(
      PrestoSqlParser::ParenthesizedExpressionContext* ctx) = 0;

  virtual void enterParameter(PrestoSqlParser::ParameterContext* ctx) = 0;
  virtual void exitParameter(PrestoSqlParser::ParameterContext* ctx) = 0;

  virtual void enterNormalize(PrestoSqlParser::NormalizeContext* ctx) = 0;
  virtual void exitNormalize(PrestoSqlParser::NormalizeContext* ctx) = 0;

  virtual void enterIntervalLiteral(
      PrestoSqlParser::IntervalLiteralContext* ctx) = 0;
  virtual void exitIntervalLiteral(
      PrestoSqlParser::IntervalLiteralContext* ctx) = 0;

  virtual void enterNumericLiteral(
      PrestoSqlParser::NumericLiteralContext* ctx) = 0;
  virtual void exitNumericLiteral(
      PrestoSqlParser::NumericLiteralContext* ctx) = 0;

  virtual void enterBooleanLiteral(
      PrestoSqlParser::BooleanLiteralContext* ctx) = 0;
  virtual void exitBooleanLiteral(
      PrestoSqlParser::BooleanLiteralContext* ctx) = 0;

  virtual void enterSimpleCase(PrestoSqlParser::SimpleCaseContext* ctx) = 0;
  virtual void exitSimpleCase(PrestoSqlParser::SimpleCaseContext* ctx) = 0;

  virtual void enterColumnReference(
      PrestoSqlParser::ColumnReferenceContext* ctx) = 0;
  virtual void exitColumnReference(
      PrestoSqlParser::ColumnReferenceContext* ctx) = 0;

  virtual void enterNullLiteral(PrestoSqlParser::NullLiteralContext* ctx) = 0;
  virtual void exitNullLiteral(PrestoSqlParser::NullLiteralContext* ctx) = 0;

  virtual void enterRowConstructor(
      PrestoSqlParser::RowConstructorContext* ctx) = 0;
  virtual void exitRowConstructor(
      PrestoSqlParser::RowConstructorContext* ctx) = 0;

  virtual void enterSubscript(PrestoSqlParser::SubscriptContext* ctx) = 0;
  virtual void exitSubscript(PrestoSqlParser::SubscriptContext* ctx) = 0;

  virtual void enterSubqueryExpression(
      PrestoSqlParser::SubqueryExpressionContext* ctx) = 0;
  virtual void exitSubqueryExpression(
      PrestoSqlParser::SubqueryExpressionContext* ctx) = 0;

  virtual void enterBinaryLiteral(
      PrestoSqlParser::BinaryLiteralContext* ctx) = 0;
  virtual void exitBinaryLiteral(
      PrestoSqlParser::BinaryLiteralContext* ctx) = 0;

  virtual void enterCurrentUser(PrestoSqlParser::CurrentUserContext* ctx) = 0;
  virtual void exitCurrentUser(PrestoSqlParser::CurrentUserContext* ctx) = 0;

  virtual void enterExtract(PrestoSqlParser::ExtractContext* ctx) = 0;
  virtual void exitExtract(PrestoSqlParser::ExtractContext* ctx) = 0;

  virtual void enterStringLiteral(
      PrestoSqlParser::StringLiteralContext* ctx) = 0;
  virtual void exitStringLiteral(
      PrestoSqlParser::StringLiteralContext* ctx) = 0;

  virtual void enterArrayConstructor(
      PrestoSqlParser::ArrayConstructorContext* ctx) = 0;
  virtual void exitArrayConstructor(
      PrestoSqlParser::ArrayConstructorContext* ctx) = 0;

  virtual void enterFunctionCall(PrestoSqlParser::FunctionCallContext* ctx) = 0;
  virtual void exitFunctionCall(PrestoSqlParser::FunctionCallContext* ctx) = 0;

  virtual void enterExists(PrestoSqlParser::ExistsContext* ctx) = 0;
  virtual void exitExists(PrestoSqlParser::ExistsContext* ctx) = 0;

  virtual void enterPosition(PrestoSqlParser::PositionContext* ctx) = 0;
  virtual void exitPosition(PrestoSqlParser::PositionContext* ctx) = 0;

  virtual void enterSearchedCase(PrestoSqlParser::SearchedCaseContext* ctx) = 0;
  virtual void exitSearchedCase(PrestoSqlParser::SearchedCaseContext* ctx) = 0;

  virtual void enterGroupingOperation(
      PrestoSqlParser::GroupingOperationContext* ctx) = 0;
  virtual void exitGroupingOperation(
      PrestoSqlParser::GroupingOperationContext* ctx) = 0;

  virtual void enterBasicStringLiteral(
      PrestoSqlParser::BasicStringLiteralContext* ctx) = 0;
  virtual void exitBasicStringLiteral(
      PrestoSqlParser::BasicStringLiteralContext* ctx) = 0;

  virtual void enterUnicodeStringLiteral(
      PrestoSqlParser::UnicodeStringLiteralContext* ctx) = 0;
  virtual void exitUnicodeStringLiteral(
      PrestoSqlParser::UnicodeStringLiteralContext* ctx) = 0;

  virtual void enterNullTreatment(
      PrestoSqlParser::NullTreatmentContext* ctx) = 0;
  virtual void exitNullTreatment(
      PrestoSqlParser::NullTreatmentContext* ctx) = 0;

  virtual void enterTimeZoneInterval(
      PrestoSqlParser::TimeZoneIntervalContext* ctx) = 0;
  virtual void exitTimeZoneInterval(
      PrestoSqlParser::TimeZoneIntervalContext* ctx) = 0;

  virtual void enterTimeZoneString(
      PrestoSqlParser::TimeZoneStringContext* ctx) = 0;
  virtual void exitTimeZoneString(
      PrestoSqlParser::TimeZoneStringContext* ctx) = 0;

  virtual void enterComparisonOperator(
      PrestoSqlParser::ComparisonOperatorContext* ctx) = 0;
  virtual void exitComparisonOperator(
      PrestoSqlParser::ComparisonOperatorContext* ctx) = 0;

  virtual void enterComparisonQuantifier(
      PrestoSqlParser::ComparisonQuantifierContext* ctx) = 0;
  virtual void exitComparisonQuantifier(
      PrestoSqlParser::ComparisonQuantifierContext* ctx) = 0;

  virtual void enterBooleanValue(PrestoSqlParser::BooleanValueContext* ctx) = 0;
  virtual void exitBooleanValue(PrestoSqlParser::BooleanValueContext* ctx) = 0;

  virtual void enterInterval(PrestoSqlParser::IntervalContext* ctx) = 0;
  virtual void exitInterval(PrestoSqlParser::IntervalContext* ctx) = 0;

  virtual void enterIntervalField(
      PrestoSqlParser::IntervalFieldContext* ctx) = 0;
  virtual void exitIntervalField(
      PrestoSqlParser::IntervalFieldContext* ctx) = 0;

  virtual void enterNormalForm(PrestoSqlParser::NormalFormContext* ctx) = 0;
  virtual void exitNormalForm(PrestoSqlParser::NormalFormContext* ctx) = 0;

  virtual void enterTypes(PrestoSqlParser::TypesContext* ctx) = 0;
  virtual void exitTypes(PrestoSqlParser::TypesContext* ctx) = 0;

  virtual void enterType(PrestoSqlParser::TypeContext* ctx) = 0;
  virtual void exitType(PrestoSqlParser::TypeContext* ctx) = 0;

  virtual void enterTypeParameter(
      PrestoSqlParser::TypeParameterContext* ctx) = 0;
  virtual void exitTypeParameter(
      PrestoSqlParser::TypeParameterContext* ctx) = 0;

  virtual void enterBaseType(PrestoSqlParser::BaseTypeContext* ctx) = 0;
  virtual void exitBaseType(PrestoSqlParser::BaseTypeContext* ctx) = 0;

  virtual void enterWhenClause(PrestoSqlParser::WhenClauseContext* ctx) = 0;
  virtual void exitWhenClause(PrestoSqlParser::WhenClauseContext* ctx) = 0;

  virtual void enterFilter(PrestoSqlParser::FilterContext* ctx) = 0;
  virtual void exitFilter(PrestoSqlParser::FilterContext* ctx) = 0;

  virtual void enterOver(PrestoSqlParser::OverContext* ctx) = 0;
  virtual void exitOver(PrestoSqlParser::OverContext* ctx) = 0;

  virtual void enterWindowFrame(PrestoSqlParser::WindowFrameContext* ctx) = 0;
  virtual void exitWindowFrame(PrestoSqlParser::WindowFrameContext* ctx) = 0;

  virtual void enterUnboundedFrame(
      PrestoSqlParser::UnboundedFrameContext* ctx) = 0;
  virtual void exitUnboundedFrame(
      PrestoSqlParser::UnboundedFrameContext* ctx) = 0;

  virtual void enterCurrentRowBound(
      PrestoSqlParser::CurrentRowBoundContext* ctx) = 0;
  virtual void exitCurrentRowBound(
      PrestoSqlParser::CurrentRowBoundContext* ctx) = 0;

  virtual void enterBoundedFrame(PrestoSqlParser::BoundedFrameContext* ctx) = 0;
  virtual void exitBoundedFrame(PrestoSqlParser::BoundedFrameContext* ctx) = 0;

  virtual void enterUpdateAssignment(
      PrestoSqlParser::UpdateAssignmentContext* ctx) = 0;
  virtual void exitUpdateAssignment(
      PrestoSqlParser::UpdateAssignmentContext* ctx) = 0;

  virtual void enterExplainFormat(
      PrestoSqlParser::ExplainFormatContext* ctx) = 0;
  virtual void exitExplainFormat(
      PrestoSqlParser::ExplainFormatContext* ctx) = 0;

  virtual void enterExplainType(PrestoSqlParser::ExplainTypeContext* ctx) = 0;
  virtual void exitExplainType(PrestoSqlParser::ExplainTypeContext* ctx) = 0;

  virtual void enterIsolationLevel(
      PrestoSqlParser::IsolationLevelContext* ctx) = 0;
  virtual void exitIsolationLevel(
      PrestoSqlParser::IsolationLevelContext* ctx) = 0;

  virtual void enterTransactionAccessMode(
      PrestoSqlParser::TransactionAccessModeContext* ctx) = 0;
  virtual void exitTransactionAccessMode(
      PrestoSqlParser::TransactionAccessModeContext* ctx) = 0;

  virtual void enterReadUncommitted(
      PrestoSqlParser::ReadUncommittedContext* ctx) = 0;
  virtual void exitReadUncommitted(
      PrestoSqlParser::ReadUncommittedContext* ctx) = 0;

  virtual void enterReadCommitted(
      PrestoSqlParser::ReadCommittedContext* ctx) = 0;
  virtual void exitReadCommitted(
      PrestoSqlParser::ReadCommittedContext* ctx) = 0;

  virtual void enterRepeatableRead(
      PrestoSqlParser::RepeatableReadContext* ctx) = 0;
  virtual void exitRepeatableRead(
      PrestoSqlParser::RepeatableReadContext* ctx) = 0;

  virtual void enterSerializable(PrestoSqlParser::SerializableContext* ctx) = 0;
  virtual void exitSerializable(PrestoSqlParser::SerializableContext* ctx) = 0;

  virtual void enterPositionalArgument(
      PrestoSqlParser::PositionalArgumentContext* ctx) = 0;
  virtual void exitPositionalArgument(
      PrestoSqlParser::PositionalArgumentContext* ctx) = 0;

  virtual void enterNamedArgument(
      PrestoSqlParser::NamedArgumentContext* ctx) = 0;
  virtual void exitNamedArgument(
      PrestoSqlParser::NamedArgumentContext* ctx) = 0;

  virtual void enterPrivilege(PrestoSqlParser::PrivilegeContext* ctx) = 0;
  virtual void exitPrivilege(PrestoSqlParser::PrivilegeContext* ctx) = 0;

  virtual void enterQualifiedName(
      PrestoSqlParser::QualifiedNameContext* ctx) = 0;
  virtual void exitQualifiedName(
      PrestoSqlParser::QualifiedNameContext* ctx) = 0;

  virtual void enterTableVersion(PrestoSqlParser::TableVersionContext* ctx) = 0;
  virtual void exitTableVersion(PrestoSqlParser::TableVersionContext* ctx) = 0;

  virtual void enterTableversionasof(
      PrestoSqlParser::TableversionasofContext* ctx) = 0;
  virtual void exitTableversionasof(
      PrestoSqlParser::TableversionasofContext* ctx) = 0;

  virtual void enterTableversionbefore(
      PrestoSqlParser::TableversionbeforeContext* ctx) = 0;
  virtual void exitTableversionbefore(
      PrestoSqlParser::TableversionbeforeContext* ctx) = 0;

  virtual void enterCurrentUserGrantor(
      PrestoSqlParser::CurrentUserGrantorContext* ctx) = 0;
  virtual void exitCurrentUserGrantor(
      PrestoSqlParser::CurrentUserGrantorContext* ctx) = 0;

  virtual void enterCurrentRoleGrantor(
      PrestoSqlParser::CurrentRoleGrantorContext* ctx) = 0;
  virtual void exitCurrentRoleGrantor(
      PrestoSqlParser::CurrentRoleGrantorContext* ctx) = 0;

  virtual void enterSpecifiedPrincipal(
      PrestoSqlParser::SpecifiedPrincipalContext* ctx) = 0;
  virtual void exitSpecifiedPrincipal(
      PrestoSqlParser::SpecifiedPrincipalContext* ctx) = 0;

  virtual void enterUserPrincipal(
      PrestoSqlParser::UserPrincipalContext* ctx) = 0;
  virtual void exitUserPrincipal(
      PrestoSqlParser::UserPrincipalContext* ctx) = 0;

  virtual void enterRolePrincipal(
      PrestoSqlParser::RolePrincipalContext* ctx) = 0;
  virtual void exitRolePrincipal(
      PrestoSqlParser::RolePrincipalContext* ctx) = 0;

  virtual void enterUnspecifiedPrincipal(
      PrestoSqlParser::UnspecifiedPrincipalContext* ctx) = 0;
  virtual void exitUnspecifiedPrincipal(
      PrestoSqlParser::UnspecifiedPrincipalContext* ctx) = 0;

  virtual void enterRoles(PrestoSqlParser::RolesContext* ctx) = 0;
  virtual void exitRoles(PrestoSqlParser::RolesContext* ctx) = 0;

  virtual void enterUnquotedIdentifier(
      PrestoSqlParser::UnquotedIdentifierContext* ctx) = 0;
  virtual void exitUnquotedIdentifier(
      PrestoSqlParser::UnquotedIdentifierContext* ctx) = 0;

  virtual void enterQuotedIdentifier(
      PrestoSqlParser::QuotedIdentifierContext* ctx) = 0;
  virtual void exitQuotedIdentifier(
      PrestoSqlParser::QuotedIdentifierContext* ctx) = 0;

  virtual void enterBackQuotedIdentifier(
      PrestoSqlParser::BackQuotedIdentifierContext* ctx) = 0;
  virtual void exitBackQuotedIdentifier(
      PrestoSqlParser::BackQuotedIdentifierContext* ctx) = 0;

  virtual void enterDigitIdentifier(
      PrestoSqlParser::DigitIdentifierContext* ctx) = 0;
  virtual void exitDigitIdentifier(
      PrestoSqlParser::DigitIdentifierContext* ctx) = 0;

  virtual void enterDecimalLiteral(
      PrestoSqlParser::DecimalLiteralContext* ctx) = 0;
  virtual void exitDecimalLiteral(
      PrestoSqlParser::DecimalLiteralContext* ctx) = 0;

  virtual void enterDoubleLiteral(
      PrestoSqlParser::DoubleLiteralContext* ctx) = 0;
  virtual void exitDoubleLiteral(
      PrestoSqlParser::DoubleLiteralContext* ctx) = 0;

  virtual void enterIntegerLiteral(
      PrestoSqlParser::IntegerLiteralContext* ctx) = 0;
  virtual void exitIntegerLiteral(
      PrestoSqlParser::IntegerLiteralContext* ctx) = 0;

  virtual void enterConstraintSpecification(
      PrestoSqlParser::ConstraintSpecificationContext* ctx) = 0;
  virtual void exitConstraintSpecification(
      PrestoSqlParser::ConstraintSpecificationContext* ctx) = 0;

  virtual void enterNamedConstraintSpecification(
      PrestoSqlParser::NamedConstraintSpecificationContext* ctx) = 0;
  virtual void exitNamedConstraintSpecification(
      PrestoSqlParser::NamedConstraintSpecificationContext* ctx) = 0;

  virtual void enterUnnamedConstraintSpecification(
      PrestoSqlParser::UnnamedConstraintSpecificationContext* ctx) = 0;
  virtual void exitUnnamedConstraintSpecification(
      PrestoSqlParser::UnnamedConstraintSpecificationContext* ctx) = 0;

  virtual void enterConstraintType(
      PrestoSqlParser::ConstraintTypeContext* ctx) = 0;
  virtual void exitConstraintType(
      PrestoSqlParser::ConstraintTypeContext* ctx) = 0;

  virtual void enterConstraintQualifiers(
      PrestoSqlParser::ConstraintQualifiersContext* ctx) = 0;
  virtual void exitConstraintQualifiers(
      PrestoSqlParser::ConstraintQualifiersContext* ctx) = 0;

  virtual void enterConstraintQualifier(
      PrestoSqlParser::ConstraintQualifierContext* ctx) = 0;
  virtual void exitConstraintQualifier(
      PrestoSqlParser::ConstraintQualifierContext* ctx) = 0;

  virtual void enterConstraintRely(
      PrestoSqlParser::ConstraintRelyContext* ctx) = 0;
  virtual void exitConstraintRely(
      PrestoSqlParser::ConstraintRelyContext* ctx) = 0;

  virtual void enterConstraintEnabled(
      PrestoSqlParser::ConstraintEnabledContext* ctx) = 0;
  virtual void exitConstraintEnabled(
      PrestoSqlParser::ConstraintEnabledContext* ctx) = 0;

  virtual void enterConstraintEnforced(
      PrestoSqlParser::ConstraintEnforcedContext* ctx) = 0;
  virtual void exitConstraintEnforced(
      PrestoSqlParser::ConstraintEnforcedContext* ctx) = 0;

  virtual void enterNonReserved(PrestoSqlParser::NonReservedContext* ctx) = 0;
  virtual void exitNonReserved(PrestoSqlParser::NonReservedContext* ctx) = 0;
};
