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

// Generated from SqlBaseParser.g4 by ANTLR 4.13.2

#pragma once

#include "antlr4-runtime.h"
#include "axiom/sql/spark/grammar/SqlBaseParserVisitor.h"

namespace axiom::sql::spark {

/**
 * This class provides an empty implementation of SqlBaseParserVisitor, which
 * can be extended to create a visitor which only needs to handle a subset of
 * the available methods.
 */
class SqlBaseParserBaseVisitor : public SqlBaseParserVisitor {
 public:
  virtual std::any visitSingleStatement(
      SqlBaseParser::SingleStatementContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSingleExpression(
      SqlBaseParser::SingleExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSingleTableIdentifier(
      SqlBaseParser::SingleTableIdentifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSingleMultipartIdentifier(
      SqlBaseParser::SingleMultipartIdentifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSingleFunctionIdentifier(
      SqlBaseParser::SingleFunctionIdentifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSingleDataType(
      SqlBaseParser::SingleDataTypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSingleTableSchema(
      SqlBaseParser::SingleTableSchemaContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitStatementDefault(
      SqlBaseParser::StatementDefaultContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDmlStatement(
      SqlBaseParser::DmlStatementContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUse(SqlBaseParser::UseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUseNamespace(
      SqlBaseParser::UseNamespaceContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSetCatalog(
      SqlBaseParser::SetCatalogContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreateNamespace(
      SqlBaseParser::CreateNamespaceContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSetNamespaceProperties(
      SqlBaseParser::SetNamespacePropertiesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSetNamespaceLocation(
      SqlBaseParser::SetNamespaceLocationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDropNamespace(
      SqlBaseParser::DropNamespaceContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowNamespaces(
      SqlBaseParser::ShowNamespacesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreateTable(
      SqlBaseParser::CreateTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreateTableLike(
      SqlBaseParser::CreateTableLikeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitReplaceTable(
      SqlBaseParser::ReplaceTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAnalyze(SqlBaseParser::AnalyzeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAnalyzeTables(
      SqlBaseParser::AnalyzeTablesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAddTableColumns(
      SqlBaseParser::AddTableColumnsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRenameTableColumn(
      SqlBaseParser::RenameTableColumnContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDropTableColumns(
      SqlBaseParser::DropTableColumnsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRenameTable(
      SqlBaseParser::RenameTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSetTableProperties(
      SqlBaseParser::SetTablePropertiesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnsetTableProperties(
      SqlBaseParser::UnsetTablePropertiesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAlterTableAlterColumn(
      SqlBaseParser::AlterTableAlterColumnContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitHiveChangeColumn(
      SqlBaseParser::HiveChangeColumnContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitHiveReplaceColumns(
      SqlBaseParser::HiveReplaceColumnsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSetTableSerDe(
      SqlBaseParser::SetTableSerDeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAddTablePartition(
      SqlBaseParser::AddTablePartitionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRenameTablePartition(
      SqlBaseParser::RenameTablePartitionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDropTablePartitions(
      SqlBaseParser::DropTablePartitionsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSetTableLocation(
      SqlBaseParser::SetTableLocationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRecoverPartitions(
      SqlBaseParser::RecoverPartitionsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDropTable(
      SqlBaseParser::DropTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDropView(SqlBaseParser::DropViewContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreateView(
      SqlBaseParser::CreateViewContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreateTempViewUsing(
      SqlBaseParser::CreateTempViewUsingContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAlterViewQuery(
      SqlBaseParser::AlterViewQueryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreateFunction(
      SqlBaseParser::CreateFunctionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDropFunction(
      SqlBaseParser::DropFunctionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExplain(SqlBaseParser::ExplainContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowTables(
      SqlBaseParser::ShowTablesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowTableExtended(
      SqlBaseParser::ShowTableExtendedContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowTblProperties(
      SqlBaseParser::ShowTblPropertiesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowColumns(
      SqlBaseParser::ShowColumnsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowViews(
      SqlBaseParser::ShowViewsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowPartitions(
      SqlBaseParser::ShowPartitionsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowFunctions(
      SqlBaseParser::ShowFunctionsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowCreateTable(
      SqlBaseParser::ShowCreateTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowCurrentNamespace(
      SqlBaseParser::ShowCurrentNamespaceContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowCatalogs(
      SqlBaseParser::ShowCatalogsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDescribeFunction(
      SqlBaseParser::DescribeFunctionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDescribeNamespace(
      SqlBaseParser::DescribeNamespaceContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDescribeRelation(
      SqlBaseParser::DescribeRelationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDescribeQuery(
      SqlBaseParser::DescribeQueryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCommentNamespace(
      SqlBaseParser::CommentNamespaceContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCommentTable(
      SqlBaseParser::CommentTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRefreshTable(
      SqlBaseParser::RefreshTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRefreshFunction(
      SqlBaseParser::RefreshFunctionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRefreshResource(
      SqlBaseParser::RefreshResourceContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCacheTable(
      SqlBaseParser::CacheTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUncacheTable(
      SqlBaseParser::UncacheTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitClearCache(
      SqlBaseParser::ClearCacheContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLoadData(SqlBaseParser::LoadDataContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTruncateTable(
      SqlBaseParser::TruncateTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRepairTable(
      SqlBaseParser::RepairTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitManageResource(
      SqlBaseParser::ManageResourceContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitFailNativeCommand(
      SqlBaseParser::FailNativeCommandContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSetTimeZone(
      SqlBaseParser::SetTimeZoneContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSetQuotedConfiguration(
      SqlBaseParser::SetQuotedConfigurationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSetConfiguration(
      SqlBaseParser::SetConfigurationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitResetQuotedConfiguration(
      SqlBaseParser::ResetQuotedConfigurationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitResetConfiguration(
      SqlBaseParser::ResetConfigurationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreateIndex(
      SqlBaseParser::CreateIndexContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDropIndex(
      SqlBaseParser::DropIndexContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTimezone(SqlBaseParser::TimezoneContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConfigKey(
      SqlBaseParser::ConfigKeyContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConfigValue(
      SqlBaseParser::ConfigValueContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnsupportedHiveNativeCommands(
      SqlBaseParser::UnsupportedHiveNativeCommandsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreateTableHeader(
      SqlBaseParser::CreateTableHeaderContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitReplaceTableHeader(
      SqlBaseParser::ReplaceTableHeaderContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBucketSpec(
      SqlBaseParser::BucketSpecContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSkewSpec(SqlBaseParser::SkewSpecContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLocationSpec(
      SqlBaseParser::LocationSpecContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCommentSpec(
      SqlBaseParser::CommentSpecContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQuery(SqlBaseParser::QueryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitInsertOverwriteTable(
      SqlBaseParser::InsertOverwriteTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitInsertIntoTable(
      SqlBaseParser::InsertIntoTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitInsertIntoReplaceWhere(
      SqlBaseParser::InsertIntoReplaceWhereContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitInsertOverwriteHiveDir(
      SqlBaseParser::InsertOverwriteHiveDirContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitInsertOverwriteDir(
      SqlBaseParser::InsertOverwriteDirContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPartitionSpecLocation(
      SqlBaseParser::PartitionSpecLocationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPartitionSpec(
      SqlBaseParser::PartitionSpecContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPartitionVal(
      SqlBaseParser::PartitionValContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNamespaceKeyword(
      SqlBaseParser::NamespaceKeywordContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNamespaces(
      SqlBaseParser::NamespacesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDescribeFuncName(
      SqlBaseParser::DescribeFuncNameContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDescribeColName(
      SqlBaseParser::DescribeColNameContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCtes(SqlBaseParser::CtesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNamedQuery(
      SqlBaseParser::NamedQueryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTableProvider(
      SqlBaseParser::TableProviderContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreateTableClauses(
      SqlBaseParser::CreateTableClausesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPropertyList(
      SqlBaseParser::PropertyListContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitProperty(SqlBaseParser::PropertyContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPropertyKey(
      SqlBaseParser::PropertyKeyContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPropertyValue(
      SqlBaseParser::PropertyValueContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConstantList(
      SqlBaseParser::ConstantListContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNestedConstantList(
      SqlBaseParser::NestedConstantListContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreateFileFormat(
      SqlBaseParser::CreateFileFormatContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTableFileFormat(
      SqlBaseParser::TableFileFormatContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGenericFileFormat(
      SqlBaseParser::GenericFileFormatContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitStorageHandler(
      SqlBaseParser::StorageHandlerContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitResource(SqlBaseParser::ResourceContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSingleInsertQuery(
      SqlBaseParser::SingleInsertQueryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMultiInsertQuery(
      SqlBaseParser::MultiInsertQueryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDeleteFromTable(
      SqlBaseParser::DeleteFromTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUpdateTable(
      SqlBaseParser::UpdateTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMergeIntoTable(
      SqlBaseParser::MergeIntoTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQueryOrganization(
      SqlBaseParser::QueryOrganizationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMultiInsertQueryBody(
      SqlBaseParser::MultiInsertQueryBodyContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQueryTermDefault(
      SqlBaseParser::QueryTermDefaultContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSetOperation(
      SqlBaseParser::SetOperationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQueryPrimaryDefault(
      SqlBaseParser::QueryPrimaryDefaultContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitFromStmt(SqlBaseParser::FromStmtContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTable(SqlBaseParser::TableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitInlineTableDefault1(
      SqlBaseParser::InlineTableDefault1Context* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSubquery(SqlBaseParser::SubqueryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSortItem(SqlBaseParser::SortItemContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitFromStatement(
      SqlBaseParser::FromStatementContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitFromStatementBody(
      SqlBaseParser::FromStatementBodyContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTransformQuerySpecification(
      SqlBaseParser::TransformQuerySpecificationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRegularQuerySpecification(
      SqlBaseParser::RegularQuerySpecificationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTransformClause(
      SqlBaseParser::TransformClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSelectClause(
      SqlBaseParser::SelectClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSetClause(
      SqlBaseParser::SetClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMatchedClause(
      SqlBaseParser::MatchedClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNotMatchedClause(
      SqlBaseParser::NotMatchedClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNotMatchedBySourceClause(
      SqlBaseParser::NotMatchedBySourceClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMatchedAction(
      SqlBaseParser::MatchedActionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNotMatchedAction(
      SqlBaseParser::NotMatchedActionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNotMatchedBySourceAction(
      SqlBaseParser::NotMatchedBySourceActionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAssignmentList(
      SqlBaseParser::AssignmentListContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAssignment(
      SqlBaseParser::AssignmentContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitWhereClause(
      SqlBaseParser::WhereClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitHavingClause(
      SqlBaseParser::HavingClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitHint(SqlBaseParser::HintContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitHintStatement(
      SqlBaseParser::HintStatementContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitFromClause(
      SqlBaseParser::FromClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTemporalClause(
      SqlBaseParser::TemporalClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAggregationClause(
      SqlBaseParser::AggregationClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGroupByClause(
      SqlBaseParser::GroupByClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGroupingAnalytics(
      SqlBaseParser::GroupingAnalyticsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGroupingElement(
      SqlBaseParser::GroupingElementContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGroupingSet(
      SqlBaseParser::GroupingSetContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPivotClause(
      SqlBaseParser::PivotClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPivotColumn(
      SqlBaseParser::PivotColumnContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPivotValue(
      SqlBaseParser::PivotValueContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnpivotClause(
      SqlBaseParser::UnpivotClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnpivotNullClause(
      SqlBaseParser::UnpivotNullClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnpivotOperator(
      SqlBaseParser::UnpivotOperatorContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnpivotSingleValueColumnClause(
      SqlBaseParser::UnpivotSingleValueColumnClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnpivotMultiValueColumnClause(
      SqlBaseParser::UnpivotMultiValueColumnClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnpivotColumnSet(
      SqlBaseParser::UnpivotColumnSetContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnpivotValueColumn(
      SqlBaseParser::UnpivotValueColumnContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnpivotNameColumn(
      SqlBaseParser::UnpivotNameColumnContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnpivotColumnAndAlias(
      SqlBaseParser::UnpivotColumnAndAliasContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnpivotColumn(
      SqlBaseParser::UnpivotColumnContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnpivotAlias(
      SqlBaseParser::UnpivotAliasContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLateralView(
      SqlBaseParser::LateralViewContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSetQuantifier(
      SqlBaseParser::SetQuantifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRelation(SqlBaseParser::RelationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRelationExtension(
      SqlBaseParser::RelationExtensionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitJoinRelation(
      SqlBaseParser::JoinRelationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitJoinType(SqlBaseParser::JoinTypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitJoinCriteria(
      SqlBaseParser::JoinCriteriaContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSample(SqlBaseParser::SampleContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSampleByPercentile(
      SqlBaseParser::SampleByPercentileContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSampleByRows(
      SqlBaseParser::SampleByRowsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSampleByBucket(
      SqlBaseParser::SampleByBucketContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSampleByBytes(
      SqlBaseParser::SampleByBytesContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitIdentifierList(
      SqlBaseParser::IdentifierListContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitIdentifierSeq(
      SqlBaseParser::IdentifierSeqContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitOrderedIdentifierList(
      SqlBaseParser::OrderedIdentifierListContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitOrderedIdentifier(
      SqlBaseParser::OrderedIdentifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitIdentifierCommentList(
      SqlBaseParser::IdentifierCommentListContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitIdentifierComment(
      SqlBaseParser::IdentifierCommentContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTableName(
      SqlBaseParser::TableNameContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAliasedQuery(
      SqlBaseParser::AliasedQueryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAliasedRelation(
      SqlBaseParser::AliasedRelationContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitInlineTableDefault2(
      SqlBaseParser::InlineTableDefault2Context* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTableValuedFunction(
      SqlBaseParser::TableValuedFunctionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitInlineTable(
      SqlBaseParser::InlineTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitFunctionTable(
      SqlBaseParser::FunctionTableContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTableAlias(
      SqlBaseParser::TableAliasContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRowFormatSerde(
      SqlBaseParser::RowFormatSerdeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRowFormatDelimited(
      SqlBaseParser::RowFormatDelimitedContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMultipartIdentifierList(
      SqlBaseParser::MultipartIdentifierListContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMultipartIdentifier(
      SqlBaseParser::MultipartIdentifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMultipartIdentifierPropertyList(
      SqlBaseParser::MultipartIdentifierPropertyListContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMultipartIdentifierProperty(
      SqlBaseParser::MultipartIdentifierPropertyContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTableIdentifier(
      SqlBaseParser::TableIdentifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitFunctionIdentifier(
      SqlBaseParser::FunctionIdentifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNamedExpression(
      SqlBaseParser::NamedExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNamedExpressionSeq(
      SqlBaseParser::NamedExpressionSeqContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPartitionFieldList(
      SqlBaseParser::PartitionFieldListContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPartitionTransform(
      SqlBaseParser::PartitionTransformContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPartitionColumn(
      SqlBaseParser::PartitionColumnContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitIdentityTransform(
      SqlBaseParser::IdentityTransformContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitApplyTransform(
      SqlBaseParser::ApplyTransformContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTransformArgument(
      SqlBaseParser::TransformArgumentContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExpression(
      SqlBaseParser::ExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExpressionSeq(
      SqlBaseParser::ExpressionSeqContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLogicalNot(
      SqlBaseParser::LogicalNotContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPredicated(
      SqlBaseParser::PredicatedContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExists(SqlBaseParser::ExistsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLogicalBinary(
      SqlBaseParser::LogicalBinaryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPredicate(
      SqlBaseParser::PredicateContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitValueExpressionDefault(
      SqlBaseParser::ValueExpressionDefaultContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitComparison(
      SqlBaseParser::ComparisonContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitArithmeticBinary(
      SqlBaseParser::ArithmeticBinaryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitArithmeticUnary(
      SqlBaseParser::ArithmeticUnaryContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDatetimeUnit(
      SqlBaseParser::DatetimeUnitContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitStruct(SqlBaseParser::StructContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDereference(
      SqlBaseParser::DereferenceContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTimestampadd(
      SqlBaseParser::TimestampaddContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSubstring(
      SqlBaseParser::SubstringContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCast(SqlBaseParser::CastContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLambda(SqlBaseParser::LambdaContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitParenthesizedExpression(
      SqlBaseParser::ParenthesizedExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAny_value(
      SqlBaseParser::Any_valueContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTrim(SqlBaseParser::TrimContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSimpleCase(
      SqlBaseParser::SimpleCaseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCurrentLike(
      SqlBaseParser::CurrentLikeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitColumnReference(
      SqlBaseParser::ColumnReferenceContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRowConstructor(
      SqlBaseParser::RowConstructorContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLast(SqlBaseParser::LastContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitStar(SqlBaseParser::StarContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitOverlay(SqlBaseParser::OverlayContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSubscript(
      SqlBaseParser::SubscriptContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTimestampdiff(
      SqlBaseParser::TimestampdiffContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSubqueryExpression(
      SqlBaseParser::SubqueryExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConstantDefault(
      SqlBaseParser::ConstantDefaultContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExtract(SqlBaseParser::ExtractContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPercentile(
      SqlBaseParser::PercentileContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitFunctionCall(
      SqlBaseParser::FunctionCallContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSearchedCase(
      SqlBaseParser::SearchedCaseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPosition(SqlBaseParser::PositionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitFirst(SqlBaseParser::FirstContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNullLiteral(
      SqlBaseParser::NullLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitParameterLiteral(
      SqlBaseParser::ParameterLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitIntervalLiteral(
      SqlBaseParser::IntervalLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTypeConstructor(
      SqlBaseParser::TypeConstructorContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNumericLiteral(
      SqlBaseParser::NumericLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBooleanLiteral(
      SqlBaseParser::BooleanLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitStringLiteral(
      SqlBaseParser::StringLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitComparisonOperator(
      SqlBaseParser::ComparisonOperatorContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitArithmeticOperator(
      SqlBaseParser::ArithmeticOperatorContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPredicateOperator(
      SqlBaseParser::PredicateOperatorContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBooleanValue(
      SqlBaseParser::BooleanValueContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitInterval(SqlBaseParser::IntervalContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitErrorCapturingMultiUnitsInterval(
      SqlBaseParser::ErrorCapturingMultiUnitsIntervalContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMultiUnitsInterval(
      SqlBaseParser::MultiUnitsIntervalContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitErrorCapturingUnitToUnitInterval(
      SqlBaseParser::ErrorCapturingUnitToUnitIntervalContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnitToUnitInterval(
      SqlBaseParser::UnitToUnitIntervalContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitIntervalValue(
      SqlBaseParser::IntervalValueContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnitInMultiUnits(
      SqlBaseParser::UnitInMultiUnitsContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnitInUnitToUnit(
      SqlBaseParser::UnitInUnitToUnitContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitColPosition(
      SqlBaseParser::ColPositionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitComplexDataType(
      SqlBaseParser::ComplexDataTypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitYearMonthIntervalDataType(
      SqlBaseParser::YearMonthIntervalDataTypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDayTimeIntervalDataType(
      SqlBaseParser::DayTimeIntervalDataTypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPrimitiveDataType(
      SqlBaseParser::PrimitiveDataTypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQualifiedColTypeWithPositionList(
      SqlBaseParser::QualifiedColTypeWithPositionListContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQualifiedColTypeWithPosition(
      SqlBaseParser::QualifiedColTypeWithPositionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDefaultExpression(
      SqlBaseParser::DefaultExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitColTypeList(
      SqlBaseParser::ColTypeListContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitColType(SqlBaseParser::ColTypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreateOrReplaceTableColTypeList(
      SqlBaseParser::CreateOrReplaceTableColTypeListContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreateOrReplaceTableColType(
      SqlBaseParser::CreateOrReplaceTableColTypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitColDefinitionOption(
      SqlBaseParser::ColDefinitionOptionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGenerationExpression(
      SqlBaseParser::GenerationExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitComplexColTypeList(
      SqlBaseParser::ComplexColTypeListContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitComplexColType(
      SqlBaseParser::ComplexColTypeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitWhenClause(
      SqlBaseParser::WhenClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitWindowClause(
      SqlBaseParser::WindowClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNamedWindow(
      SqlBaseParser::NamedWindowContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitWindowRef(
      SqlBaseParser::WindowRefContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitWindowDef(
      SqlBaseParser::WindowDefContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitWindowFrame(
      SqlBaseParser::WindowFrameContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitFrameBound(
      SqlBaseParser::FrameBoundContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQualifiedNameList(
      SqlBaseParser::QualifiedNameListContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitFunctionName(
      SqlBaseParser::FunctionNameContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQualifiedName(
      SqlBaseParser::QualifiedNameContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitErrorCapturingIdentifier(
      SqlBaseParser::ErrorCapturingIdentifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitErrorIdent(
      SqlBaseParser::ErrorIdentContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRealIdent(
      SqlBaseParser::RealIdentContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitIdentifier(
      SqlBaseParser::IdentifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnquotedIdentifier(
      SqlBaseParser::UnquotedIdentifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQuotedIdentifierAlternative(
      SqlBaseParser::QuotedIdentifierAlternativeContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQuotedIdentifier(
      SqlBaseParser::QuotedIdentifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBackQuotedIdentifier(
      SqlBaseParser::BackQuotedIdentifierContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExponentLiteral(
      SqlBaseParser::ExponentLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDecimalLiteral(
      SqlBaseParser::DecimalLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLegacyDecimalLiteral(
      SqlBaseParser::LegacyDecimalLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitIntegerLiteral(
      SqlBaseParser::IntegerLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBigIntLiteral(
      SqlBaseParser::BigIntLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSmallIntLiteral(
      SqlBaseParser::SmallIntLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTinyIntLiteral(
      SqlBaseParser::TinyIntLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDoubleLiteral(
      SqlBaseParser::DoubleLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitFloatLiteral(
      SqlBaseParser::FloatLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBigDecimalLiteral(
      SqlBaseParser::BigDecimalLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAlterColumnAction(
      SqlBaseParser::AlterColumnActionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitStringLit(
      SqlBaseParser::StringLitContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitComment(SqlBaseParser::CommentContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitVersion(SqlBaseParser::VersionContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAnsiNonReserved(
      SqlBaseParser::AnsiNonReservedContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitStrictNonReserved(
      SqlBaseParser::StrictNonReservedContext* ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNonReserved(
      SqlBaseParser::NonReservedContext* ctx) override {
    return visitChildren(ctx);
  }
};

} // namespace axiom::sql::spark
