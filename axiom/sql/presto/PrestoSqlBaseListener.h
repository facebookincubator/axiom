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
#include "axiom/sql/presto/PrestoSqlListener.h"

namespace facebook::velox::sql {

/**
 * This class provides an empty implementation of PrestoSqlListener,
 * which can be extended to create a listener which only needs to handle a
 * subset of the available methods.
 */
class PrestoSqlBaseListener : public PrestoSqlListener {
 public:
  virtual void enterSingleStatement(
      PrestoSqlParser::SingleStatementContext* /*ctx*/) override {}
  virtual void exitSingleStatement(
      PrestoSqlParser::SingleStatementContext* /*ctx*/) override {}

  virtual void enterStandaloneExpression(
      PrestoSqlParser::StandaloneExpressionContext* /*ctx*/) override {}
  virtual void exitStandaloneExpression(
      PrestoSqlParser::StandaloneExpressionContext* /*ctx*/) override {}

  virtual void enterStandaloneRoutineBody(
      PrestoSqlParser::StandaloneRoutineBodyContext* /*ctx*/) override {}
  virtual void exitStandaloneRoutineBody(
      PrestoSqlParser::StandaloneRoutineBodyContext* /*ctx*/) override {}

  virtual void enterStatementDefault(
      PrestoSqlParser::StatementDefaultContext* /*ctx*/) override {}
  virtual void exitStatementDefault(
      PrestoSqlParser::StatementDefaultContext* /*ctx*/) override {}

  virtual void enterUse(PrestoSqlParser::UseContext* /*ctx*/) override {}
  virtual void exitUse(PrestoSqlParser::UseContext* /*ctx*/) override {}

  virtual void enterCreateSchema(
      PrestoSqlParser::CreateSchemaContext* /*ctx*/) override {}
  virtual void exitCreateSchema(
      PrestoSqlParser::CreateSchemaContext* /*ctx*/) override {}

  virtual void enterDropSchema(
      PrestoSqlParser::DropSchemaContext* /*ctx*/) override {}
  virtual void exitDropSchema(
      PrestoSqlParser::DropSchemaContext* /*ctx*/) override {}

  virtual void enterRenameSchema(
      PrestoSqlParser::RenameSchemaContext* /*ctx*/) override {}
  virtual void exitRenameSchema(
      PrestoSqlParser::RenameSchemaContext* /*ctx*/) override {}

  virtual void enterCreateTableAsSelect(
      PrestoSqlParser::CreateTableAsSelectContext* /*ctx*/) override {}
  virtual void exitCreateTableAsSelect(
      PrestoSqlParser::CreateTableAsSelectContext* /*ctx*/) override {}

  virtual void enterCreateTable(
      PrestoSqlParser::CreateTableContext* /*ctx*/) override {}
  virtual void exitCreateTable(
      PrestoSqlParser::CreateTableContext* /*ctx*/) override {}

  virtual void enterDropTable(
      PrestoSqlParser::DropTableContext* /*ctx*/) override {}
  virtual void exitDropTable(
      PrestoSqlParser::DropTableContext* /*ctx*/) override {}

  virtual void enterInsertInto(
      PrestoSqlParser::InsertIntoContext* /*ctx*/) override {}
  virtual void exitInsertInto(
      PrestoSqlParser::InsertIntoContext* /*ctx*/) override {}

  virtual void enterDelete(PrestoSqlParser::DeleteContext* /*ctx*/) override {}
  virtual void exitDelete(PrestoSqlParser::DeleteContext* /*ctx*/) override {}

  virtual void enterTruncateTable(
      PrestoSqlParser::TruncateTableContext* /*ctx*/) override {}
  virtual void exitTruncateTable(
      PrestoSqlParser::TruncateTableContext* /*ctx*/) override {}

  virtual void enterRenameTable(
      PrestoSqlParser::RenameTableContext* /*ctx*/) override {}
  virtual void exitRenameTable(
      PrestoSqlParser::RenameTableContext* /*ctx*/) override {}

  virtual void enterRenameColumn(
      PrestoSqlParser::RenameColumnContext* /*ctx*/) override {}
  virtual void exitRenameColumn(
      PrestoSqlParser::RenameColumnContext* /*ctx*/) override {}

  virtual void enterDropColumn(
      PrestoSqlParser::DropColumnContext* /*ctx*/) override {}
  virtual void exitDropColumn(
      PrestoSqlParser::DropColumnContext* /*ctx*/) override {}

  virtual void enterAddColumn(
      PrestoSqlParser::AddColumnContext* /*ctx*/) override {}
  virtual void exitAddColumn(
      PrestoSqlParser::AddColumnContext* /*ctx*/) override {}

  virtual void enterAddConstraint(
      PrestoSqlParser::AddConstraintContext* /*ctx*/) override {}
  virtual void exitAddConstraint(
      PrestoSqlParser::AddConstraintContext* /*ctx*/) override {}

  virtual void enterDropConstraint(
      PrestoSqlParser::DropConstraintContext* /*ctx*/) override {}
  virtual void exitDropConstraint(
      PrestoSqlParser::DropConstraintContext* /*ctx*/) override {}

  virtual void enterAlterColumnSetNotNull(
      PrestoSqlParser::AlterColumnSetNotNullContext* /*ctx*/) override {}
  virtual void exitAlterColumnSetNotNull(
      PrestoSqlParser::AlterColumnSetNotNullContext* /*ctx*/) override {}

  virtual void enterAlterColumnDropNotNull(
      PrestoSqlParser::AlterColumnDropNotNullContext* /*ctx*/) override {}
  virtual void exitAlterColumnDropNotNull(
      PrestoSqlParser::AlterColumnDropNotNullContext* /*ctx*/) override {}

  virtual void enterSetTableProperties(
      PrestoSqlParser::SetTablePropertiesContext* /*ctx*/) override {}
  virtual void exitSetTableProperties(
      PrestoSqlParser::SetTablePropertiesContext* /*ctx*/) override {}

  virtual void enterAnalyze(PrestoSqlParser::AnalyzeContext* /*ctx*/) override {
  }
  virtual void exitAnalyze(PrestoSqlParser::AnalyzeContext* /*ctx*/) override {}

  virtual void enterCreateType(
      PrestoSqlParser::CreateTypeContext* /*ctx*/) override {}
  virtual void exitCreateType(
      PrestoSqlParser::CreateTypeContext* /*ctx*/) override {}

  virtual void enterCreateView(
      PrestoSqlParser::CreateViewContext* /*ctx*/) override {}
  virtual void exitCreateView(
      PrestoSqlParser::CreateViewContext* /*ctx*/) override {}

  virtual void enterRenameView(
      PrestoSqlParser::RenameViewContext* /*ctx*/) override {}
  virtual void exitRenameView(
      PrestoSqlParser::RenameViewContext* /*ctx*/) override {}

  virtual void enterDropView(
      PrestoSqlParser::DropViewContext* /*ctx*/) override {}
  virtual void exitDropView(
      PrestoSqlParser::DropViewContext* /*ctx*/) override {}

  virtual void enterCreateMaterializedView(
      PrestoSqlParser::CreateMaterializedViewContext* /*ctx*/) override {}
  virtual void exitCreateMaterializedView(
      PrestoSqlParser::CreateMaterializedViewContext* /*ctx*/) override {}

  virtual void enterDropMaterializedView(
      PrestoSqlParser::DropMaterializedViewContext* /*ctx*/) override {}
  virtual void exitDropMaterializedView(
      PrestoSqlParser::DropMaterializedViewContext* /*ctx*/) override {}

  virtual void enterRefreshMaterializedView(
      PrestoSqlParser::RefreshMaterializedViewContext* /*ctx*/) override {}
  virtual void exitRefreshMaterializedView(
      PrestoSqlParser::RefreshMaterializedViewContext* /*ctx*/) override {}

  virtual void enterCreateFunction(
      PrestoSqlParser::CreateFunctionContext* /*ctx*/) override {}
  virtual void exitCreateFunction(
      PrestoSqlParser::CreateFunctionContext* /*ctx*/) override {}

  virtual void enterAlterFunction(
      PrestoSqlParser::AlterFunctionContext* /*ctx*/) override {}
  virtual void exitAlterFunction(
      PrestoSqlParser::AlterFunctionContext* /*ctx*/) override {}

  virtual void enterDropFunction(
      PrestoSqlParser::DropFunctionContext* /*ctx*/) override {}
  virtual void exitDropFunction(
      PrestoSqlParser::DropFunctionContext* /*ctx*/) override {}

  virtual void enterCall(PrestoSqlParser::CallContext* /*ctx*/) override {}
  virtual void exitCall(PrestoSqlParser::CallContext* /*ctx*/) override {}

  virtual void enterCreateRole(
      PrestoSqlParser::CreateRoleContext* /*ctx*/) override {}
  virtual void exitCreateRole(
      PrestoSqlParser::CreateRoleContext* /*ctx*/) override {}

  virtual void enterDropRole(
      PrestoSqlParser::DropRoleContext* /*ctx*/) override {}
  virtual void exitDropRole(
      PrestoSqlParser::DropRoleContext* /*ctx*/) override {}

  virtual void enterGrantRoles(
      PrestoSqlParser::GrantRolesContext* /*ctx*/) override {}
  virtual void exitGrantRoles(
      PrestoSqlParser::GrantRolesContext* /*ctx*/) override {}

  virtual void enterRevokeRoles(
      PrestoSqlParser::RevokeRolesContext* /*ctx*/) override {}
  virtual void exitRevokeRoles(
      PrestoSqlParser::RevokeRolesContext* /*ctx*/) override {}

  virtual void enterSetRole(PrestoSqlParser::SetRoleContext* /*ctx*/) override {
  }
  virtual void exitSetRole(PrestoSqlParser::SetRoleContext* /*ctx*/) override {}

  virtual void enterGrant(PrestoSqlParser::GrantContext* /*ctx*/) override {}
  virtual void exitGrant(PrestoSqlParser::GrantContext* /*ctx*/) override {}

  virtual void enterRevoke(PrestoSqlParser::RevokeContext* /*ctx*/) override {}
  virtual void exitRevoke(PrestoSqlParser::RevokeContext* /*ctx*/) override {}

  virtual void enterShowGrants(
      PrestoSqlParser::ShowGrantsContext* /*ctx*/) override {}
  virtual void exitShowGrants(
      PrestoSqlParser::ShowGrantsContext* /*ctx*/) override {}

  virtual void enterExplain(PrestoSqlParser::ExplainContext* /*ctx*/) override {
  }
  virtual void exitExplain(PrestoSqlParser::ExplainContext* /*ctx*/) override {}

  virtual void enterShowCreateTable(
      PrestoSqlParser::ShowCreateTableContext* /*ctx*/) override {}
  virtual void exitShowCreateTable(
      PrestoSqlParser::ShowCreateTableContext* /*ctx*/) override {}

  virtual void enterShowCreateView(
      PrestoSqlParser::ShowCreateViewContext* /*ctx*/) override {}
  virtual void exitShowCreateView(
      PrestoSqlParser::ShowCreateViewContext* /*ctx*/) override {}

  virtual void enterShowCreateMaterializedView(
      PrestoSqlParser::ShowCreateMaterializedViewContext* /*ctx*/) override {}
  virtual void exitShowCreateMaterializedView(
      PrestoSqlParser::ShowCreateMaterializedViewContext* /*ctx*/) override {}

  virtual void enterShowCreateFunction(
      PrestoSqlParser::ShowCreateFunctionContext* /*ctx*/) override {}
  virtual void exitShowCreateFunction(
      PrestoSqlParser::ShowCreateFunctionContext* /*ctx*/) override {}

  virtual void enterShowTables(
      PrestoSqlParser::ShowTablesContext* /*ctx*/) override {}
  virtual void exitShowTables(
      PrestoSqlParser::ShowTablesContext* /*ctx*/) override {}

  virtual void enterShowSchemas(
      PrestoSqlParser::ShowSchemasContext* /*ctx*/) override {}
  virtual void exitShowSchemas(
      PrestoSqlParser::ShowSchemasContext* /*ctx*/) override {}

  virtual void enterShowCatalogs(
      PrestoSqlParser::ShowCatalogsContext* /*ctx*/) override {}
  virtual void exitShowCatalogs(
      PrestoSqlParser::ShowCatalogsContext* /*ctx*/) override {}

  virtual void enterShowColumns(
      PrestoSqlParser::ShowColumnsContext* /*ctx*/) override {}
  virtual void exitShowColumns(
      PrestoSqlParser::ShowColumnsContext* /*ctx*/) override {}

  virtual void enterShowStats(
      PrestoSqlParser::ShowStatsContext* /*ctx*/) override {}
  virtual void exitShowStats(
      PrestoSqlParser::ShowStatsContext* /*ctx*/) override {}

  virtual void enterShowStatsForQuery(
      PrestoSqlParser::ShowStatsForQueryContext* /*ctx*/) override {}
  virtual void exitShowStatsForQuery(
      PrestoSqlParser::ShowStatsForQueryContext* /*ctx*/) override {}

  virtual void enterShowRoles(
      PrestoSqlParser::ShowRolesContext* /*ctx*/) override {}
  virtual void exitShowRoles(
      PrestoSqlParser::ShowRolesContext* /*ctx*/) override {}

  virtual void enterShowRoleGrants(
      PrestoSqlParser::ShowRoleGrantsContext* /*ctx*/) override {}
  virtual void exitShowRoleGrants(
      PrestoSqlParser::ShowRoleGrantsContext* /*ctx*/) override {}

  virtual void enterShowFunctions(
      PrestoSqlParser::ShowFunctionsContext* /*ctx*/) override {}
  virtual void exitShowFunctions(
      PrestoSqlParser::ShowFunctionsContext* /*ctx*/) override {}

  virtual void enterShowSession(
      PrestoSqlParser::ShowSessionContext* /*ctx*/) override {}
  virtual void exitShowSession(
      PrestoSqlParser::ShowSessionContext* /*ctx*/) override {}

  virtual void enterSetSession(
      PrestoSqlParser::SetSessionContext* /*ctx*/) override {}
  virtual void exitSetSession(
      PrestoSqlParser::SetSessionContext* /*ctx*/) override {}

  virtual void enterResetSession(
      PrestoSqlParser::ResetSessionContext* /*ctx*/) override {}
  virtual void exitResetSession(
      PrestoSqlParser::ResetSessionContext* /*ctx*/) override {}

  virtual void enterStartTransaction(
      PrestoSqlParser::StartTransactionContext* /*ctx*/) override {}
  virtual void exitStartTransaction(
      PrestoSqlParser::StartTransactionContext* /*ctx*/) override {}

  virtual void enterCommit(PrestoSqlParser::CommitContext* /*ctx*/) override {}
  virtual void exitCommit(PrestoSqlParser::CommitContext* /*ctx*/) override {}

  virtual void enterRollback(
      PrestoSqlParser::RollbackContext* /*ctx*/) override {}
  virtual void exitRollback(
      PrestoSqlParser::RollbackContext* /*ctx*/) override {}

  virtual void enterPrepare(PrestoSqlParser::PrepareContext* /*ctx*/) override {
  }
  virtual void exitPrepare(PrestoSqlParser::PrepareContext* /*ctx*/) override {}

  virtual void enterDeallocate(
      PrestoSqlParser::DeallocateContext* /*ctx*/) override {}
  virtual void exitDeallocate(
      PrestoSqlParser::DeallocateContext* /*ctx*/) override {}

  virtual void enterExecute(PrestoSqlParser::ExecuteContext* /*ctx*/) override {
  }
  virtual void exitExecute(PrestoSqlParser::ExecuteContext* /*ctx*/) override {}

  virtual void enterDescribeInput(
      PrestoSqlParser::DescribeInputContext* /*ctx*/) override {}
  virtual void exitDescribeInput(
      PrestoSqlParser::DescribeInputContext* /*ctx*/) override {}

  virtual void enterDescribeOutput(
      PrestoSqlParser::DescribeOutputContext* /*ctx*/) override {}
  virtual void exitDescribeOutput(
      PrestoSqlParser::DescribeOutputContext* /*ctx*/) override {}

  virtual void enterUpdate(PrestoSqlParser::UpdateContext* /*ctx*/) override {}
  virtual void exitUpdate(PrestoSqlParser::UpdateContext* /*ctx*/) override {}

  virtual void enterQuery(PrestoSqlParser::QueryContext* /*ctx*/) override {}
  virtual void exitQuery(PrestoSqlParser::QueryContext* /*ctx*/) override {}

  virtual void enterWith(PrestoSqlParser::WithContext* /*ctx*/) override {}
  virtual void exitWith(PrestoSqlParser::WithContext* /*ctx*/) override {}

  virtual void enterTableElement(
      PrestoSqlParser::TableElementContext* /*ctx*/) override {}
  virtual void exitTableElement(
      PrestoSqlParser::TableElementContext* /*ctx*/) override {}

  virtual void enterColumnDefinition(
      PrestoSqlParser::ColumnDefinitionContext* /*ctx*/) override {}
  virtual void exitColumnDefinition(
      PrestoSqlParser::ColumnDefinitionContext* /*ctx*/) override {}

  virtual void enterLikeClause(
      PrestoSqlParser::LikeClauseContext* /*ctx*/) override {}
  virtual void exitLikeClause(
      PrestoSqlParser::LikeClauseContext* /*ctx*/) override {}

  virtual void enterProperties(
      PrestoSqlParser::PropertiesContext* /*ctx*/) override {}
  virtual void exitProperties(
      PrestoSqlParser::PropertiesContext* /*ctx*/) override {}

  virtual void enterProperty(
      PrestoSqlParser::PropertyContext* /*ctx*/) override {}
  virtual void exitProperty(
      PrestoSqlParser::PropertyContext* /*ctx*/) override {}

  virtual void enterSqlParameterDeclaration(
      PrestoSqlParser::SqlParameterDeclarationContext* /*ctx*/) override {}
  virtual void exitSqlParameterDeclaration(
      PrestoSqlParser::SqlParameterDeclarationContext* /*ctx*/) override {}

  virtual void enterRoutineCharacteristics(
      PrestoSqlParser::RoutineCharacteristicsContext* /*ctx*/) override {}
  virtual void exitRoutineCharacteristics(
      PrestoSqlParser::RoutineCharacteristicsContext* /*ctx*/) override {}

  virtual void enterRoutineCharacteristic(
      PrestoSqlParser::RoutineCharacteristicContext* /*ctx*/) override {}
  virtual void exitRoutineCharacteristic(
      PrestoSqlParser::RoutineCharacteristicContext* /*ctx*/) override {}

  virtual void enterAlterRoutineCharacteristics(
      PrestoSqlParser::AlterRoutineCharacteristicsContext* /*ctx*/) override {}
  virtual void exitAlterRoutineCharacteristics(
      PrestoSqlParser::AlterRoutineCharacteristicsContext* /*ctx*/) override {}

  virtual void enterAlterRoutineCharacteristic(
      PrestoSqlParser::AlterRoutineCharacteristicContext* /*ctx*/) override {}
  virtual void exitAlterRoutineCharacteristic(
      PrestoSqlParser::AlterRoutineCharacteristicContext* /*ctx*/) override {}

  virtual void enterRoutineBody(
      PrestoSqlParser::RoutineBodyContext* /*ctx*/) override {}
  virtual void exitRoutineBody(
      PrestoSqlParser::RoutineBodyContext* /*ctx*/) override {}

  virtual void enterReturnStatement(
      PrestoSqlParser::ReturnStatementContext* /*ctx*/) override {}
  virtual void exitReturnStatement(
      PrestoSqlParser::ReturnStatementContext* /*ctx*/) override {}

  virtual void enterExternalBodyReference(
      PrestoSqlParser::ExternalBodyReferenceContext* /*ctx*/) override {}
  virtual void exitExternalBodyReference(
      PrestoSqlParser::ExternalBodyReferenceContext* /*ctx*/) override {}

  virtual void enterLanguage(
      PrestoSqlParser::LanguageContext* /*ctx*/) override {}
  virtual void exitLanguage(
      PrestoSqlParser::LanguageContext* /*ctx*/) override {}

  virtual void enterDeterminism(
      PrestoSqlParser::DeterminismContext* /*ctx*/) override {}
  virtual void exitDeterminism(
      PrestoSqlParser::DeterminismContext* /*ctx*/) override {}

  virtual void enterNullCallClause(
      PrestoSqlParser::NullCallClauseContext* /*ctx*/) override {}
  virtual void exitNullCallClause(
      PrestoSqlParser::NullCallClauseContext* /*ctx*/) override {}

  virtual void enterExternalRoutineName(
      PrestoSqlParser::ExternalRoutineNameContext* /*ctx*/) override {}
  virtual void exitExternalRoutineName(
      PrestoSqlParser::ExternalRoutineNameContext* /*ctx*/) override {}

  virtual void enterQueryNoWith(
      PrestoSqlParser::QueryNoWithContext* /*ctx*/) override {}
  virtual void exitQueryNoWith(
      PrestoSqlParser::QueryNoWithContext* /*ctx*/) override {}

  virtual void enterQueryTermDefault(
      PrestoSqlParser::QueryTermDefaultContext* /*ctx*/) override {}
  virtual void exitQueryTermDefault(
      PrestoSqlParser::QueryTermDefaultContext* /*ctx*/) override {}

  virtual void enterSetOperation(
      PrestoSqlParser::SetOperationContext* /*ctx*/) override {}
  virtual void exitSetOperation(
      PrestoSqlParser::SetOperationContext* /*ctx*/) override {}

  virtual void enterQueryPrimaryDefault(
      PrestoSqlParser::QueryPrimaryDefaultContext* /*ctx*/) override {}
  virtual void exitQueryPrimaryDefault(
      PrestoSqlParser::QueryPrimaryDefaultContext* /*ctx*/) override {}

  virtual void enterTable(PrestoSqlParser::TableContext* /*ctx*/) override {}
  virtual void exitTable(PrestoSqlParser::TableContext* /*ctx*/) override {}

  virtual void enterInlineTable(
      PrestoSqlParser::InlineTableContext* /*ctx*/) override {}
  virtual void exitInlineTable(
      PrestoSqlParser::InlineTableContext* /*ctx*/) override {}

  virtual void enterSubquery(
      PrestoSqlParser::SubqueryContext* /*ctx*/) override {}
  virtual void exitSubquery(
      PrestoSqlParser::SubqueryContext* /*ctx*/) override {}

  virtual void enterSortItem(
      PrestoSqlParser::SortItemContext* /*ctx*/) override {}
  virtual void exitSortItem(
      PrestoSqlParser::SortItemContext* /*ctx*/) override {}

  virtual void enterQuerySpecification(
      PrestoSqlParser::QuerySpecificationContext* /*ctx*/) override {}
  virtual void exitQuerySpecification(
      PrestoSqlParser::QuerySpecificationContext* /*ctx*/) override {}

  virtual void enterGroupBy(PrestoSqlParser::GroupByContext* /*ctx*/) override {
  }
  virtual void exitGroupBy(PrestoSqlParser::GroupByContext* /*ctx*/) override {}

  virtual void enterSingleGroupingSet(
      PrestoSqlParser::SingleGroupingSetContext* /*ctx*/) override {}
  virtual void exitSingleGroupingSet(
      PrestoSqlParser::SingleGroupingSetContext* /*ctx*/) override {}

  virtual void enterRollup(PrestoSqlParser::RollupContext* /*ctx*/) override {}
  virtual void exitRollup(PrestoSqlParser::RollupContext* /*ctx*/) override {}

  virtual void enterCube(PrestoSqlParser::CubeContext* /*ctx*/) override {}
  virtual void exitCube(PrestoSqlParser::CubeContext* /*ctx*/) override {}

  virtual void enterMultipleGroupingSets(
      PrestoSqlParser::MultipleGroupingSetsContext* /*ctx*/) override {}
  virtual void exitMultipleGroupingSets(
      PrestoSqlParser::MultipleGroupingSetsContext* /*ctx*/) override {}

  virtual void enterGroupingSet(
      PrestoSqlParser::GroupingSetContext* /*ctx*/) override {}
  virtual void exitGroupingSet(
      PrestoSqlParser::GroupingSetContext* /*ctx*/) override {}

  virtual void enterNamedQuery(
      PrestoSqlParser::NamedQueryContext* /*ctx*/) override {}
  virtual void exitNamedQuery(
      PrestoSqlParser::NamedQueryContext* /*ctx*/) override {}

  virtual void enterSetQuantifier(
      PrestoSqlParser::SetQuantifierContext* /*ctx*/) override {}
  virtual void exitSetQuantifier(
      PrestoSqlParser::SetQuantifierContext* /*ctx*/) override {}

  virtual void enterSelectSingle(
      PrestoSqlParser::SelectSingleContext* /*ctx*/) override {}
  virtual void exitSelectSingle(
      PrestoSqlParser::SelectSingleContext* /*ctx*/) override {}

  virtual void enterSelectAll(
      PrestoSqlParser::SelectAllContext* /*ctx*/) override {}
  virtual void exitSelectAll(
      PrestoSqlParser::SelectAllContext* /*ctx*/) override {}

  virtual void enterRelationDefault(
      PrestoSqlParser::RelationDefaultContext* /*ctx*/) override {}
  virtual void exitRelationDefault(
      PrestoSqlParser::RelationDefaultContext* /*ctx*/) override {}

  virtual void enterJoinRelation(
      PrestoSqlParser::JoinRelationContext* /*ctx*/) override {}
  virtual void exitJoinRelation(
      PrestoSqlParser::JoinRelationContext* /*ctx*/) override {}

  virtual void enterJoinType(
      PrestoSqlParser::JoinTypeContext* /*ctx*/) override {}
  virtual void exitJoinType(
      PrestoSqlParser::JoinTypeContext* /*ctx*/) override {}

  virtual void enterJoinCriteria(
      PrestoSqlParser::JoinCriteriaContext* /*ctx*/) override {}
  virtual void exitJoinCriteria(
      PrestoSqlParser::JoinCriteriaContext* /*ctx*/) override {}

  virtual void enterSampledRelation(
      PrestoSqlParser::SampledRelationContext* /*ctx*/) override {}
  virtual void exitSampledRelation(
      PrestoSqlParser::SampledRelationContext* /*ctx*/) override {}

  virtual void enterSampleType(
      PrestoSqlParser::SampleTypeContext* /*ctx*/) override {}
  virtual void exitSampleType(
      PrestoSqlParser::SampleTypeContext* /*ctx*/) override {}

  virtual void enterAliasedRelation(
      PrestoSqlParser::AliasedRelationContext* /*ctx*/) override {}
  virtual void exitAliasedRelation(
      PrestoSqlParser::AliasedRelationContext* /*ctx*/) override {}

  virtual void enterColumnAliases(
      PrestoSqlParser::ColumnAliasesContext* /*ctx*/) override {}
  virtual void exitColumnAliases(
      PrestoSqlParser::ColumnAliasesContext* /*ctx*/) override {}

  virtual void enterTableName(
      PrestoSqlParser::TableNameContext* /*ctx*/) override {}
  virtual void exitTableName(
      PrestoSqlParser::TableNameContext* /*ctx*/) override {}

  virtual void enterSubqueryRelation(
      PrestoSqlParser::SubqueryRelationContext* /*ctx*/) override {}
  virtual void exitSubqueryRelation(
      PrestoSqlParser::SubqueryRelationContext* /*ctx*/) override {}

  virtual void enterUnnest(PrestoSqlParser::UnnestContext* /*ctx*/) override {}
  virtual void exitUnnest(PrestoSqlParser::UnnestContext* /*ctx*/) override {}

  virtual void enterLateral(PrestoSqlParser::LateralContext* /*ctx*/) override {
  }
  virtual void exitLateral(PrestoSqlParser::LateralContext* /*ctx*/) override {}

  virtual void enterParenthesizedRelation(
      PrestoSqlParser::ParenthesizedRelationContext* /*ctx*/) override {}
  virtual void exitParenthesizedRelation(
      PrestoSqlParser::ParenthesizedRelationContext* /*ctx*/) override {}

  virtual void enterExpression(
      PrestoSqlParser::ExpressionContext* /*ctx*/) override {}
  virtual void exitExpression(
      PrestoSqlParser::ExpressionContext* /*ctx*/) override {}

  virtual void enterLogicalNot(
      PrestoSqlParser::LogicalNotContext* /*ctx*/) override {}
  virtual void exitLogicalNot(
      PrestoSqlParser::LogicalNotContext* /*ctx*/) override {}

  virtual void enterPredicated(
      PrestoSqlParser::PredicatedContext* /*ctx*/) override {}
  virtual void exitPredicated(
      PrestoSqlParser::PredicatedContext* /*ctx*/) override {}

  virtual void enterLogicalBinary(
      PrestoSqlParser::LogicalBinaryContext* /*ctx*/) override {}
  virtual void exitLogicalBinary(
      PrestoSqlParser::LogicalBinaryContext* /*ctx*/) override {}

  virtual void enterComparison(
      PrestoSqlParser::ComparisonContext* /*ctx*/) override {}
  virtual void exitComparison(
      PrestoSqlParser::ComparisonContext* /*ctx*/) override {}

  virtual void enterQuantifiedComparison(
      PrestoSqlParser::QuantifiedComparisonContext* /*ctx*/) override {}
  virtual void exitQuantifiedComparison(
      PrestoSqlParser::QuantifiedComparisonContext* /*ctx*/) override {}

  virtual void enterBetween(PrestoSqlParser::BetweenContext* /*ctx*/) override {
  }
  virtual void exitBetween(PrestoSqlParser::BetweenContext* /*ctx*/) override {}

  virtual void enterInList(PrestoSqlParser::InListContext* /*ctx*/) override {}
  virtual void exitInList(PrestoSqlParser::InListContext* /*ctx*/) override {}

  virtual void enterInSubquery(
      PrestoSqlParser::InSubqueryContext* /*ctx*/) override {}
  virtual void exitInSubquery(
      PrestoSqlParser::InSubqueryContext* /*ctx*/) override {}

  virtual void enterLike(PrestoSqlParser::LikeContext* /*ctx*/) override {}
  virtual void exitLike(PrestoSqlParser::LikeContext* /*ctx*/) override {}

  virtual void enterNullPredicate(
      PrestoSqlParser::NullPredicateContext* /*ctx*/) override {}
  virtual void exitNullPredicate(
      PrestoSqlParser::NullPredicateContext* /*ctx*/) override {}

  virtual void enterDistinctFrom(
      PrestoSqlParser::DistinctFromContext* /*ctx*/) override {}
  virtual void exitDistinctFrom(
      PrestoSqlParser::DistinctFromContext* /*ctx*/) override {}

  virtual void enterValueExpressionDefault(
      PrestoSqlParser::ValueExpressionDefaultContext* /*ctx*/) override {}
  virtual void exitValueExpressionDefault(
      PrestoSqlParser::ValueExpressionDefaultContext* /*ctx*/) override {}

  virtual void enterConcatenation(
      PrestoSqlParser::ConcatenationContext* /*ctx*/) override {}
  virtual void exitConcatenation(
      PrestoSqlParser::ConcatenationContext* /*ctx*/) override {}

  virtual void enterArithmeticBinary(
      PrestoSqlParser::ArithmeticBinaryContext* /*ctx*/) override {}
  virtual void exitArithmeticBinary(
      PrestoSqlParser::ArithmeticBinaryContext* /*ctx*/) override {}

  virtual void enterArithmeticUnary(
      PrestoSqlParser::ArithmeticUnaryContext* /*ctx*/) override {}
  virtual void exitArithmeticUnary(
      PrestoSqlParser::ArithmeticUnaryContext* /*ctx*/) override {}

  virtual void enterAtTimeZone(
      PrestoSqlParser::AtTimeZoneContext* /*ctx*/) override {}
  virtual void exitAtTimeZone(
      PrestoSqlParser::AtTimeZoneContext* /*ctx*/) override {}

  virtual void enterDereference(
      PrestoSqlParser::DereferenceContext* /*ctx*/) override {}
  virtual void exitDereference(
      PrestoSqlParser::DereferenceContext* /*ctx*/) override {}

  virtual void enterTypeConstructor(
      PrestoSqlParser::TypeConstructorContext* /*ctx*/) override {}
  virtual void exitTypeConstructor(
      PrestoSqlParser::TypeConstructorContext* /*ctx*/) override {}

  virtual void enterSpecialDateTimeFunction(
      PrestoSqlParser::SpecialDateTimeFunctionContext* /*ctx*/) override {}
  virtual void exitSpecialDateTimeFunction(
      PrestoSqlParser::SpecialDateTimeFunctionContext* /*ctx*/) override {}

  virtual void enterSubstring(
      PrestoSqlParser::SubstringContext* /*ctx*/) override {}
  virtual void exitSubstring(
      PrestoSqlParser::SubstringContext* /*ctx*/) override {}

  virtual void enterCast(PrestoSqlParser::CastContext* /*ctx*/) override {}
  virtual void exitCast(PrestoSqlParser::CastContext* /*ctx*/) override {}

  virtual void enterLambda(PrestoSqlParser::LambdaContext* /*ctx*/) override {}
  virtual void exitLambda(PrestoSqlParser::LambdaContext* /*ctx*/) override {}

  virtual void enterParenthesizedExpression(
      PrestoSqlParser::ParenthesizedExpressionContext* /*ctx*/) override {}
  virtual void exitParenthesizedExpression(
      PrestoSqlParser::ParenthesizedExpressionContext* /*ctx*/) override {}

  virtual void enterParameter(
      PrestoSqlParser::ParameterContext* /*ctx*/) override {}
  virtual void exitParameter(
      PrestoSqlParser::ParameterContext* /*ctx*/) override {}

  virtual void enterNormalize(
      PrestoSqlParser::NormalizeContext* /*ctx*/) override {}
  virtual void exitNormalize(
      PrestoSqlParser::NormalizeContext* /*ctx*/) override {}

  virtual void enterIntervalLiteral(
      PrestoSqlParser::IntervalLiteralContext* /*ctx*/) override {}
  virtual void exitIntervalLiteral(
      PrestoSqlParser::IntervalLiteralContext* /*ctx*/) override {}

  virtual void enterNumericLiteral(
      PrestoSqlParser::NumericLiteralContext* /*ctx*/) override {}
  virtual void exitNumericLiteral(
      PrestoSqlParser::NumericLiteralContext* /*ctx*/) override {}

  virtual void enterBooleanLiteral(
      PrestoSqlParser::BooleanLiteralContext* /*ctx*/) override {}
  virtual void exitBooleanLiteral(
      PrestoSqlParser::BooleanLiteralContext* /*ctx*/) override {}

  virtual void enterSimpleCase(
      PrestoSqlParser::SimpleCaseContext* /*ctx*/) override {}
  virtual void exitSimpleCase(
      PrestoSqlParser::SimpleCaseContext* /*ctx*/) override {}

  virtual void enterColumnReference(
      PrestoSqlParser::ColumnReferenceContext* /*ctx*/) override {}
  virtual void exitColumnReference(
      PrestoSqlParser::ColumnReferenceContext* /*ctx*/) override {}

  virtual void enterNullLiteral(
      PrestoSqlParser::NullLiteralContext* /*ctx*/) override {}
  virtual void exitNullLiteral(
      PrestoSqlParser::NullLiteralContext* /*ctx*/) override {}

  virtual void enterRowConstructor(
      PrestoSqlParser::RowConstructorContext* /*ctx*/) override {}
  virtual void exitRowConstructor(
      PrestoSqlParser::RowConstructorContext* /*ctx*/) override {}

  virtual void enterSubscript(
      PrestoSqlParser::SubscriptContext* /*ctx*/) override {}
  virtual void exitSubscript(
      PrestoSqlParser::SubscriptContext* /*ctx*/) override {}

  virtual void enterSubqueryExpression(
      PrestoSqlParser::SubqueryExpressionContext* /*ctx*/) override {}
  virtual void exitSubqueryExpression(
      PrestoSqlParser::SubqueryExpressionContext* /*ctx*/) override {}

  virtual void enterBinaryLiteral(
      PrestoSqlParser::BinaryLiteralContext* /*ctx*/) override {}
  virtual void exitBinaryLiteral(
      PrestoSqlParser::BinaryLiteralContext* /*ctx*/) override {}

  virtual void enterCurrentUser(
      PrestoSqlParser::CurrentUserContext* /*ctx*/) override {}
  virtual void exitCurrentUser(
      PrestoSqlParser::CurrentUserContext* /*ctx*/) override {}

  virtual void enterExtract(PrestoSqlParser::ExtractContext* /*ctx*/) override {
  }
  virtual void exitExtract(PrestoSqlParser::ExtractContext* /*ctx*/) override {}

  virtual void enterStringLiteral(
      PrestoSqlParser::StringLiteralContext* /*ctx*/) override {}
  virtual void exitStringLiteral(
      PrestoSqlParser::StringLiteralContext* /*ctx*/) override {}

  virtual void enterArrayConstructor(
      PrestoSqlParser::ArrayConstructorContext* /*ctx*/) override {}
  virtual void exitArrayConstructor(
      PrestoSqlParser::ArrayConstructorContext* /*ctx*/) override {}

  virtual void enterFunctionCall(
      PrestoSqlParser::FunctionCallContext* /*ctx*/) override {}
  virtual void exitFunctionCall(
      PrestoSqlParser::FunctionCallContext* /*ctx*/) override {}

  virtual void enterExists(PrestoSqlParser::ExistsContext* /*ctx*/) override {}
  virtual void exitExists(PrestoSqlParser::ExistsContext* /*ctx*/) override {}

  virtual void enterPosition(
      PrestoSqlParser::PositionContext* /*ctx*/) override {}
  virtual void exitPosition(
      PrestoSqlParser::PositionContext* /*ctx*/) override {}

  virtual void enterSearchedCase(
      PrestoSqlParser::SearchedCaseContext* /*ctx*/) override {}
  virtual void exitSearchedCase(
      PrestoSqlParser::SearchedCaseContext* /*ctx*/) override {}

  virtual void enterGroupingOperation(
      PrestoSqlParser::GroupingOperationContext* /*ctx*/) override {}
  virtual void exitGroupingOperation(
      PrestoSqlParser::GroupingOperationContext* /*ctx*/) override {}

  virtual void enterBasicStringLiteral(
      PrestoSqlParser::BasicStringLiteralContext* /*ctx*/) override {}
  virtual void exitBasicStringLiteral(
      PrestoSqlParser::BasicStringLiteralContext* /*ctx*/) override {}

  virtual void enterUnicodeStringLiteral(
      PrestoSqlParser::UnicodeStringLiteralContext* /*ctx*/) override {}
  virtual void exitUnicodeStringLiteral(
      PrestoSqlParser::UnicodeStringLiteralContext* /*ctx*/) override {}

  virtual void enterNullTreatment(
      PrestoSqlParser::NullTreatmentContext* /*ctx*/) override {}
  virtual void exitNullTreatment(
      PrestoSqlParser::NullTreatmentContext* /*ctx*/) override {}

  virtual void enterTimeZoneInterval(
      PrestoSqlParser::TimeZoneIntervalContext* /*ctx*/) override {}
  virtual void exitTimeZoneInterval(
      PrestoSqlParser::TimeZoneIntervalContext* /*ctx*/) override {}

  virtual void enterTimeZoneString(
      PrestoSqlParser::TimeZoneStringContext* /*ctx*/) override {}
  virtual void exitTimeZoneString(
      PrestoSqlParser::TimeZoneStringContext* /*ctx*/) override {}

  virtual void enterComparisonOperator(
      PrestoSqlParser::ComparisonOperatorContext* /*ctx*/) override {}
  virtual void exitComparisonOperator(
      PrestoSqlParser::ComparisonOperatorContext* /*ctx*/) override {}

  virtual void enterComparisonQuantifier(
      PrestoSqlParser::ComparisonQuantifierContext* /*ctx*/) override {}
  virtual void exitComparisonQuantifier(
      PrestoSqlParser::ComparisonQuantifierContext* /*ctx*/) override {}

  virtual void enterBooleanValue(
      PrestoSqlParser::BooleanValueContext* /*ctx*/) override {}
  virtual void exitBooleanValue(
      PrestoSqlParser::BooleanValueContext* /*ctx*/) override {}

  virtual void enterInterval(
      PrestoSqlParser::IntervalContext* /*ctx*/) override {}
  virtual void exitInterval(
      PrestoSqlParser::IntervalContext* /*ctx*/) override {}

  virtual void enterIntervalField(
      PrestoSqlParser::IntervalFieldContext* /*ctx*/) override {}
  virtual void exitIntervalField(
      PrestoSqlParser::IntervalFieldContext* /*ctx*/) override {}

  virtual void enterNormalForm(
      PrestoSqlParser::NormalFormContext* /*ctx*/) override {}
  virtual void exitNormalForm(
      PrestoSqlParser::NormalFormContext* /*ctx*/) override {}

  virtual void enterTypes(PrestoSqlParser::TypesContext* /*ctx*/) override {}
  virtual void exitTypes(PrestoSqlParser::TypesContext* /*ctx*/) override {}

  virtual void enterType(PrestoSqlParser::TypeContext* /*ctx*/) override {}
  virtual void exitType(PrestoSqlParser::TypeContext* /*ctx*/) override {}

  virtual void enterTypeParameter(
      PrestoSqlParser::TypeParameterContext* /*ctx*/) override {}
  virtual void exitTypeParameter(
      PrestoSqlParser::TypeParameterContext* /*ctx*/) override {}

  virtual void enterBaseType(
      PrestoSqlParser::BaseTypeContext* /*ctx*/) override {}
  virtual void exitBaseType(
      PrestoSqlParser::BaseTypeContext* /*ctx*/) override {}

  virtual void enterWhenClause(
      PrestoSqlParser::WhenClauseContext* /*ctx*/) override {}
  virtual void exitWhenClause(
      PrestoSqlParser::WhenClauseContext* /*ctx*/) override {}

  virtual void enterFilter(PrestoSqlParser::FilterContext* /*ctx*/) override {}
  virtual void exitFilter(PrestoSqlParser::FilterContext* /*ctx*/) override {}

  virtual void enterOver(PrestoSqlParser::OverContext* /*ctx*/) override {}
  virtual void exitOver(PrestoSqlParser::OverContext* /*ctx*/) override {}

  virtual void enterWindowFrame(
      PrestoSqlParser::WindowFrameContext* /*ctx*/) override {}
  virtual void exitWindowFrame(
      PrestoSqlParser::WindowFrameContext* /*ctx*/) override {}

  virtual void enterUnboundedFrame(
      PrestoSqlParser::UnboundedFrameContext* /*ctx*/) override {}
  virtual void exitUnboundedFrame(
      PrestoSqlParser::UnboundedFrameContext* /*ctx*/) override {}

  virtual void enterCurrentRowBound(
      PrestoSqlParser::CurrentRowBoundContext* /*ctx*/) override {}
  virtual void exitCurrentRowBound(
      PrestoSqlParser::CurrentRowBoundContext* /*ctx*/) override {}

  virtual void enterBoundedFrame(
      PrestoSqlParser::BoundedFrameContext* /*ctx*/) override {}
  virtual void exitBoundedFrame(
      PrestoSqlParser::BoundedFrameContext* /*ctx*/) override {}

  virtual void enterUpdateAssignment(
      PrestoSqlParser::UpdateAssignmentContext* /*ctx*/) override {}
  virtual void exitUpdateAssignment(
      PrestoSqlParser::UpdateAssignmentContext* /*ctx*/) override {}

  virtual void enterExplainFormat(
      PrestoSqlParser::ExplainFormatContext* /*ctx*/) override {}
  virtual void exitExplainFormat(
      PrestoSqlParser::ExplainFormatContext* /*ctx*/) override {}

  virtual void enterExplainType(
      PrestoSqlParser::ExplainTypeContext* /*ctx*/) override {}
  virtual void exitExplainType(
      PrestoSqlParser::ExplainTypeContext* /*ctx*/) override {}

  virtual void enterIsolationLevel(
      PrestoSqlParser::IsolationLevelContext* /*ctx*/) override {}
  virtual void exitIsolationLevel(
      PrestoSqlParser::IsolationLevelContext* /*ctx*/) override {}

  virtual void enterTransactionAccessMode(
      PrestoSqlParser::TransactionAccessModeContext* /*ctx*/) override {}
  virtual void exitTransactionAccessMode(
      PrestoSqlParser::TransactionAccessModeContext* /*ctx*/) override {}

  virtual void enterReadUncommitted(
      PrestoSqlParser::ReadUncommittedContext* /*ctx*/) override {}
  virtual void exitReadUncommitted(
      PrestoSqlParser::ReadUncommittedContext* /*ctx*/) override {}

  virtual void enterReadCommitted(
      PrestoSqlParser::ReadCommittedContext* /*ctx*/) override {}
  virtual void exitReadCommitted(
      PrestoSqlParser::ReadCommittedContext* /*ctx*/) override {}

  virtual void enterRepeatableRead(
      PrestoSqlParser::RepeatableReadContext* /*ctx*/) override {}
  virtual void exitRepeatableRead(
      PrestoSqlParser::RepeatableReadContext* /*ctx*/) override {}

  virtual void enterSerializable(
      PrestoSqlParser::SerializableContext* /*ctx*/) override {}
  virtual void exitSerializable(
      PrestoSqlParser::SerializableContext* /*ctx*/) override {}

  virtual void enterPositionalArgument(
      PrestoSqlParser::PositionalArgumentContext* /*ctx*/) override {}
  virtual void exitPositionalArgument(
      PrestoSqlParser::PositionalArgumentContext* /*ctx*/) override {}

  virtual void enterNamedArgument(
      PrestoSqlParser::NamedArgumentContext* /*ctx*/) override {}
  virtual void exitNamedArgument(
      PrestoSqlParser::NamedArgumentContext* /*ctx*/) override {}

  virtual void enterPrivilege(
      PrestoSqlParser::PrivilegeContext* /*ctx*/) override {}
  virtual void exitPrivilege(
      PrestoSqlParser::PrivilegeContext* /*ctx*/) override {}

  virtual void enterQualifiedName(
      PrestoSqlParser::QualifiedNameContext* /*ctx*/) override {}
  virtual void exitQualifiedName(
      PrestoSqlParser::QualifiedNameContext* /*ctx*/) override {}

  virtual void enterTableVersion(
      PrestoSqlParser::TableVersionContext* /*ctx*/) override {}
  virtual void exitTableVersion(
      PrestoSqlParser::TableVersionContext* /*ctx*/) override {}

  virtual void enterTableversionasof(
      PrestoSqlParser::TableversionasofContext* /*ctx*/) override {}
  virtual void exitTableversionasof(
      PrestoSqlParser::TableversionasofContext* /*ctx*/) override {}

  virtual void enterTableversionbefore(
      PrestoSqlParser::TableversionbeforeContext* /*ctx*/) override {}
  virtual void exitTableversionbefore(
      PrestoSqlParser::TableversionbeforeContext* /*ctx*/) override {}

  virtual void enterCurrentUserGrantor(
      PrestoSqlParser::CurrentUserGrantorContext* /*ctx*/) override {}
  virtual void exitCurrentUserGrantor(
      PrestoSqlParser::CurrentUserGrantorContext* /*ctx*/) override {}

  virtual void enterCurrentRoleGrantor(
      PrestoSqlParser::CurrentRoleGrantorContext* /*ctx*/) override {}
  virtual void exitCurrentRoleGrantor(
      PrestoSqlParser::CurrentRoleGrantorContext* /*ctx*/) override {}

  virtual void enterSpecifiedPrincipal(
      PrestoSqlParser::SpecifiedPrincipalContext* /*ctx*/) override {}
  virtual void exitSpecifiedPrincipal(
      PrestoSqlParser::SpecifiedPrincipalContext* /*ctx*/) override {}

  virtual void enterUserPrincipal(
      PrestoSqlParser::UserPrincipalContext* /*ctx*/) override {}
  virtual void exitUserPrincipal(
      PrestoSqlParser::UserPrincipalContext* /*ctx*/) override {}

  virtual void enterRolePrincipal(
      PrestoSqlParser::RolePrincipalContext* /*ctx*/) override {}
  virtual void exitRolePrincipal(
      PrestoSqlParser::RolePrincipalContext* /*ctx*/) override {}

  virtual void enterUnspecifiedPrincipal(
      PrestoSqlParser::UnspecifiedPrincipalContext* /*ctx*/) override {}
  virtual void exitUnspecifiedPrincipal(
      PrestoSqlParser::UnspecifiedPrincipalContext* /*ctx*/) override {}

  virtual void enterRoles(PrestoSqlParser::RolesContext* /*ctx*/) override {}
  virtual void exitRoles(PrestoSqlParser::RolesContext* /*ctx*/) override {}

  virtual void enterUnquotedIdentifier(
      PrestoSqlParser::UnquotedIdentifierContext* /*ctx*/) override {}
  virtual void exitUnquotedIdentifier(
      PrestoSqlParser::UnquotedIdentifierContext* /*ctx*/) override {}

  virtual void enterQuotedIdentifier(
      PrestoSqlParser::QuotedIdentifierContext* /*ctx*/) override {}
  virtual void exitQuotedIdentifier(
      PrestoSqlParser::QuotedIdentifierContext* /*ctx*/) override {}

  virtual void enterBackQuotedIdentifier(
      PrestoSqlParser::BackQuotedIdentifierContext* /*ctx*/) override {}
  virtual void exitBackQuotedIdentifier(
      PrestoSqlParser::BackQuotedIdentifierContext* /*ctx*/) override {}

  virtual void enterDigitIdentifier(
      PrestoSqlParser::DigitIdentifierContext* /*ctx*/) override {}
  virtual void exitDigitIdentifier(
      PrestoSqlParser::DigitIdentifierContext* /*ctx*/) override {}

  virtual void enterDecimalLiteral(
      PrestoSqlParser::DecimalLiteralContext* /*ctx*/) override {}
  virtual void exitDecimalLiteral(
      PrestoSqlParser::DecimalLiteralContext* /*ctx*/) override {}

  virtual void enterDoubleLiteral(
      PrestoSqlParser::DoubleLiteralContext* /*ctx*/) override {}
  virtual void exitDoubleLiteral(
      PrestoSqlParser::DoubleLiteralContext* /*ctx*/) override {}

  virtual void enterIntegerLiteral(
      PrestoSqlParser::IntegerLiteralContext* /*ctx*/) override {}
  virtual void exitIntegerLiteral(
      PrestoSqlParser::IntegerLiteralContext* /*ctx*/) override {}

  virtual void enterConstraintSpecification(
      PrestoSqlParser::ConstraintSpecificationContext* /*ctx*/) override {}
  virtual void exitConstraintSpecification(
      PrestoSqlParser::ConstraintSpecificationContext* /*ctx*/) override {}

  virtual void enterNamedConstraintSpecification(
      PrestoSqlParser::NamedConstraintSpecificationContext* /*ctx*/) override {}
  virtual void exitNamedConstraintSpecification(
      PrestoSqlParser::NamedConstraintSpecificationContext* /*ctx*/) override {}

  virtual void enterUnnamedConstraintSpecification(
      PrestoSqlParser::UnnamedConstraintSpecificationContext* /*ctx*/)
      override {}
  virtual void exitUnnamedConstraintSpecification(
      PrestoSqlParser::UnnamedConstraintSpecificationContext* /*ctx*/)
      override {}

  virtual void enterConstraintType(
      PrestoSqlParser::ConstraintTypeContext* /*ctx*/) override {}
  virtual void exitConstraintType(
      PrestoSqlParser::ConstraintTypeContext* /*ctx*/) override {}

  virtual void enterConstraintQualifiers(
      PrestoSqlParser::ConstraintQualifiersContext* /*ctx*/) override {}
  virtual void exitConstraintQualifiers(
      PrestoSqlParser::ConstraintQualifiersContext* /*ctx*/) override {}

  virtual void enterConstraintQualifier(
      PrestoSqlParser::ConstraintQualifierContext* /*ctx*/) override {}
  virtual void exitConstraintQualifier(
      PrestoSqlParser::ConstraintQualifierContext* /*ctx*/) override {}

  virtual void enterConstraintRely(
      PrestoSqlParser::ConstraintRelyContext* /*ctx*/) override {}
  virtual void exitConstraintRely(
      PrestoSqlParser::ConstraintRelyContext* /*ctx*/) override {}

  virtual void enterConstraintEnabled(
      PrestoSqlParser::ConstraintEnabledContext* /*ctx*/) override {}
  virtual void exitConstraintEnabled(
      PrestoSqlParser::ConstraintEnabledContext* /*ctx*/) override {}

  virtual void enterConstraintEnforced(
      PrestoSqlParser::ConstraintEnforcedContext* /*ctx*/) override {}
  virtual void exitConstraintEnforced(
      PrestoSqlParser::ConstraintEnforcedContext* /*ctx*/) override {}

  virtual void enterNonReserved(
      PrestoSqlParser::NonReservedContext* /*ctx*/) override {}
  virtual void exitNonReserved(
      PrestoSqlParser::NonReservedContext* /*ctx*/) override {}

  virtual void enterEveryRule(antlr4::ParserRuleContext* /*ctx*/) override {}
  virtual void exitEveryRule(antlr4::ParserRuleContext* /*ctx*/) override {}
  virtual void visitTerminal(antlr4::tree::TerminalNode* /*node*/) override {}
  virtual void visitErrorNode(antlr4::tree::ErrorNode* /*node*/) override {}
};

} // namespace facebook::velox::sql
