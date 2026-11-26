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
#include "axiom/sql/spark/grammar/SqlBaseParser.h"

namespace axiom::sql::spark {

/**
 * This class defines an abstract visitor for a parse tree
 * produced by SqlBaseParser.
 */
class SqlBaseParserVisitor : public antlr4::tree::AbstractParseTreeVisitor {
 public:
  /**
   * Visit parse trees produced by SqlBaseParser.
   */
  virtual std::any visitSingleStatement(
      SqlBaseParser::SingleStatementContext* context) = 0;

  virtual std::any visitSingleExpression(
      SqlBaseParser::SingleExpressionContext* context) = 0;

  virtual std::any visitSingleTableIdentifier(
      SqlBaseParser::SingleTableIdentifierContext* context) = 0;

  virtual std::any visitSingleMultipartIdentifier(
      SqlBaseParser::SingleMultipartIdentifierContext* context) = 0;

  virtual std::any visitSingleFunctionIdentifier(
      SqlBaseParser::SingleFunctionIdentifierContext* context) = 0;

  virtual std::any visitSingleDataType(
      SqlBaseParser::SingleDataTypeContext* context) = 0;

  virtual std::any visitSingleTableSchema(
      SqlBaseParser::SingleTableSchemaContext* context) = 0;

  virtual std::any visitStatementDefault(
      SqlBaseParser::StatementDefaultContext* context) = 0;

  virtual std::any visitDmlStatement(
      SqlBaseParser::DmlStatementContext* context) = 0;

  virtual std::any visitUse(SqlBaseParser::UseContext* context) = 0;

  virtual std::any visitUseNamespace(
      SqlBaseParser::UseNamespaceContext* context) = 0;

  virtual std::any visitSetCatalog(
      SqlBaseParser::SetCatalogContext* context) = 0;

  virtual std::any visitCreateNamespace(
      SqlBaseParser::CreateNamespaceContext* context) = 0;

  virtual std::any visitSetNamespaceProperties(
      SqlBaseParser::SetNamespacePropertiesContext* context) = 0;

  virtual std::any visitSetNamespaceLocation(
      SqlBaseParser::SetNamespaceLocationContext* context) = 0;

  virtual std::any visitDropNamespace(
      SqlBaseParser::DropNamespaceContext* context) = 0;

  virtual std::any visitShowNamespaces(
      SqlBaseParser::ShowNamespacesContext* context) = 0;

  virtual std::any visitCreateTable(
      SqlBaseParser::CreateTableContext* context) = 0;

  virtual std::any visitCreateTableLike(
      SqlBaseParser::CreateTableLikeContext* context) = 0;

  virtual std::any visitReplaceTable(
      SqlBaseParser::ReplaceTableContext* context) = 0;

  virtual std::any visitAnalyze(SqlBaseParser::AnalyzeContext* context) = 0;

  virtual std::any visitAnalyzeTables(
      SqlBaseParser::AnalyzeTablesContext* context) = 0;

  virtual std::any visitAddTableColumns(
      SqlBaseParser::AddTableColumnsContext* context) = 0;

  virtual std::any visitRenameTableColumn(
      SqlBaseParser::RenameTableColumnContext* context) = 0;

  virtual std::any visitDropTableColumns(
      SqlBaseParser::DropTableColumnsContext* context) = 0;

  virtual std::any visitRenameTable(
      SqlBaseParser::RenameTableContext* context) = 0;

  virtual std::any visitSetTableProperties(
      SqlBaseParser::SetTablePropertiesContext* context) = 0;

  virtual std::any visitUnsetTableProperties(
      SqlBaseParser::UnsetTablePropertiesContext* context) = 0;

  virtual std::any visitAlterTableAlterColumn(
      SqlBaseParser::AlterTableAlterColumnContext* context) = 0;

  virtual std::any visitHiveChangeColumn(
      SqlBaseParser::HiveChangeColumnContext* context) = 0;

  virtual std::any visitHiveReplaceColumns(
      SqlBaseParser::HiveReplaceColumnsContext* context) = 0;

  virtual std::any visitSetTableSerDe(
      SqlBaseParser::SetTableSerDeContext* context) = 0;

  virtual std::any visitAddTablePartition(
      SqlBaseParser::AddTablePartitionContext* context) = 0;

  virtual std::any visitRenameTablePartition(
      SqlBaseParser::RenameTablePartitionContext* context) = 0;

  virtual std::any visitDropTablePartitions(
      SqlBaseParser::DropTablePartitionsContext* context) = 0;

  virtual std::any visitSetTableLocation(
      SqlBaseParser::SetTableLocationContext* context) = 0;

  virtual std::any visitRecoverPartitions(
      SqlBaseParser::RecoverPartitionsContext* context) = 0;

  virtual std::any visitDropTable(SqlBaseParser::DropTableContext* context) = 0;

  virtual std::any visitDropView(SqlBaseParser::DropViewContext* context) = 0;

  virtual std::any visitCreateView(
      SqlBaseParser::CreateViewContext* context) = 0;

  virtual std::any visitCreateTempViewUsing(
      SqlBaseParser::CreateTempViewUsingContext* context) = 0;

  virtual std::any visitAlterViewQuery(
      SqlBaseParser::AlterViewQueryContext* context) = 0;

  virtual std::any visitCreateFunction(
      SqlBaseParser::CreateFunctionContext* context) = 0;

  virtual std::any visitDropFunction(
      SqlBaseParser::DropFunctionContext* context) = 0;

  virtual std::any visitExplain(SqlBaseParser::ExplainContext* context) = 0;

  virtual std::any visitShowTables(
      SqlBaseParser::ShowTablesContext* context) = 0;

  virtual std::any visitShowTableExtended(
      SqlBaseParser::ShowTableExtendedContext* context) = 0;

  virtual std::any visitShowTblProperties(
      SqlBaseParser::ShowTblPropertiesContext* context) = 0;

  virtual std::any visitShowColumns(
      SqlBaseParser::ShowColumnsContext* context) = 0;

  virtual std::any visitShowViews(SqlBaseParser::ShowViewsContext* context) = 0;

  virtual std::any visitShowPartitions(
      SqlBaseParser::ShowPartitionsContext* context) = 0;

  virtual std::any visitShowFunctions(
      SqlBaseParser::ShowFunctionsContext* context) = 0;

  virtual std::any visitShowCreateTable(
      SqlBaseParser::ShowCreateTableContext* context) = 0;

  virtual std::any visitShowCurrentNamespace(
      SqlBaseParser::ShowCurrentNamespaceContext* context) = 0;

  virtual std::any visitShowCatalogs(
      SqlBaseParser::ShowCatalogsContext* context) = 0;

  virtual std::any visitDescribeFunction(
      SqlBaseParser::DescribeFunctionContext* context) = 0;

  virtual std::any visitDescribeNamespace(
      SqlBaseParser::DescribeNamespaceContext* context) = 0;

  virtual std::any visitDescribeRelation(
      SqlBaseParser::DescribeRelationContext* context) = 0;

  virtual std::any visitDescribeQuery(
      SqlBaseParser::DescribeQueryContext* context) = 0;

  virtual std::any visitCommentNamespace(
      SqlBaseParser::CommentNamespaceContext* context) = 0;

  virtual std::any visitCommentTable(
      SqlBaseParser::CommentTableContext* context) = 0;

  virtual std::any visitRefreshTable(
      SqlBaseParser::RefreshTableContext* context) = 0;

  virtual std::any visitRefreshFunction(
      SqlBaseParser::RefreshFunctionContext* context) = 0;

  virtual std::any visitRefreshResource(
      SqlBaseParser::RefreshResourceContext* context) = 0;

  virtual std::any visitCacheTable(
      SqlBaseParser::CacheTableContext* context) = 0;

  virtual std::any visitUncacheTable(
      SqlBaseParser::UncacheTableContext* context) = 0;

  virtual std::any visitClearCache(
      SqlBaseParser::ClearCacheContext* context) = 0;

  virtual std::any visitLoadData(SqlBaseParser::LoadDataContext* context) = 0;

  virtual std::any visitTruncateTable(
      SqlBaseParser::TruncateTableContext* context) = 0;

  virtual std::any visitRepairTable(
      SqlBaseParser::RepairTableContext* context) = 0;

  virtual std::any visitManageResource(
      SqlBaseParser::ManageResourceContext* context) = 0;

  virtual std::any visitFailNativeCommand(
      SqlBaseParser::FailNativeCommandContext* context) = 0;

  virtual std::any visitSetTimeZone(
      SqlBaseParser::SetTimeZoneContext* context) = 0;

  virtual std::any visitSetQuotedConfiguration(
      SqlBaseParser::SetQuotedConfigurationContext* context) = 0;

  virtual std::any visitSetConfiguration(
      SqlBaseParser::SetConfigurationContext* context) = 0;

  virtual std::any visitResetQuotedConfiguration(
      SqlBaseParser::ResetQuotedConfigurationContext* context) = 0;

  virtual std::any visitResetConfiguration(
      SqlBaseParser::ResetConfigurationContext* context) = 0;

  virtual std::any visitCreateIndex(
      SqlBaseParser::CreateIndexContext* context) = 0;

  virtual std::any visitDropIndex(SqlBaseParser::DropIndexContext* context) = 0;

  virtual std::any visitTimezone(SqlBaseParser::TimezoneContext* context) = 0;

  virtual std::any visitConfigKey(SqlBaseParser::ConfigKeyContext* context) = 0;

  virtual std::any visitConfigValue(
      SqlBaseParser::ConfigValueContext* context) = 0;

  virtual std::any visitUnsupportedHiveNativeCommands(
      SqlBaseParser::UnsupportedHiveNativeCommandsContext* context) = 0;

  virtual std::any visitCreateTableHeader(
      SqlBaseParser::CreateTableHeaderContext* context) = 0;

  virtual std::any visitReplaceTableHeader(
      SqlBaseParser::ReplaceTableHeaderContext* context) = 0;

  virtual std::any visitBucketSpec(
      SqlBaseParser::BucketSpecContext* context) = 0;

  virtual std::any visitSkewSpec(SqlBaseParser::SkewSpecContext* context) = 0;

  virtual std::any visitLocationSpec(
      SqlBaseParser::LocationSpecContext* context) = 0;

  virtual std::any visitCommentSpec(
      SqlBaseParser::CommentSpecContext* context) = 0;

  virtual std::any visitQuery(SqlBaseParser::QueryContext* context) = 0;

  virtual std::any visitInsertOverwriteTable(
      SqlBaseParser::InsertOverwriteTableContext* context) = 0;

  virtual std::any visitInsertIntoTable(
      SqlBaseParser::InsertIntoTableContext* context) = 0;

  virtual std::any visitInsertIntoReplaceWhere(
      SqlBaseParser::InsertIntoReplaceWhereContext* context) = 0;

  virtual std::any visitInsertOverwriteHiveDir(
      SqlBaseParser::InsertOverwriteHiveDirContext* context) = 0;

  virtual std::any visitInsertOverwriteDir(
      SqlBaseParser::InsertOverwriteDirContext* context) = 0;

  virtual std::any visitPartitionSpecLocation(
      SqlBaseParser::PartitionSpecLocationContext* context) = 0;

  virtual std::any visitPartitionSpec(
      SqlBaseParser::PartitionSpecContext* context) = 0;

  virtual std::any visitPartitionVal(
      SqlBaseParser::PartitionValContext* context) = 0;

  virtual std::any visitNamespaceKeyword(
      SqlBaseParser::NamespaceKeywordContext* context) = 0;

  virtual std::any visitNamespaces(
      SqlBaseParser::NamespacesContext* context) = 0;

  virtual std::any visitDescribeFuncName(
      SqlBaseParser::DescribeFuncNameContext* context) = 0;

  virtual std::any visitDescribeColName(
      SqlBaseParser::DescribeColNameContext* context) = 0;

  virtual std::any visitCtes(SqlBaseParser::CtesContext* context) = 0;

  virtual std::any visitNamedQuery(
      SqlBaseParser::NamedQueryContext* context) = 0;

  virtual std::any visitTableProvider(
      SqlBaseParser::TableProviderContext* context) = 0;

  virtual std::any visitCreateTableClauses(
      SqlBaseParser::CreateTableClausesContext* context) = 0;

  virtual std::any visitPropertyList(
      SqlBaseParser::PropertyListContext* context) = 0;

  virtual std::any visitProperty(SqlBaseParser::PropertyContext* context) = 0;

  virtual std::any visitPropertyKey(
      SqlBaseParser::PropertyKeyContext* context) = 0;

  virtual std::any visitPropertyValue(
      SqlBaseParser::PropertyValueContext* context) = 0;

  virtual std::any visitConstantList(
      SqlBaseParser::ConstantListContext* context) = 0;

  virtual std::any visitNestedConstantList(
      SqlBaseParser::NestedConstantListContext* context) = 0;

  virtual std::any visitCreateFileFormat(
      SqlBaseParser::CreateFileFormatContext* context) = 0;

  virtual std::any visitTableFileFormat(
      SqlBaseParser::TableFileFormatContext* context) = 0;

  virtual std::any visitGenericFileFormat(
      SqlBaseParser::GenericFileFormatContext* context) = 0;

  virtual std::any visitStorageHandler(
      SqlBaseParser::StorageHandlerContext* context) = 0;

  virtual std::any visitResource(SqlBaseParser::ResourceContext* context) = 0;

  virtual std::any visitSingleInsertQuery(
      SqlBaseParser::SingleInsertQueryContext* context) = 0;

  virtual std::any visitMultiInsertQuery(
      SqlBaseParser::MultiInsertQueryContext* context) = 0;

  virtual std::any visitDeleteFromTable(
      SqlBaseParser::DeleteFromTableContext* context) = 0;

  virtual std::any visitUpdateTable(
      SqlBaseParser::UpdateTableContext* context) = 0;

  virtual std::any visitMergeIntoTable(
      SqlBaseParser::MergeIntoTableContext* context) = 0;

  virtual std::any visitQueryOrganization(
      SqlBaseParser::QueryOrganizationContext* context) = 0;

  virtual std::any visitMultiInsertQueryBody(
      SqlBaseParser::MultiInsertQueryBodyContext* context) = 0;

  virtual std::any visitQueryTermDefault(
      SqlBaseParser::QueryTermDefaultContext* context) = 0;

  virtual std::any visitSetOperation(
      SqlBaseParser::SetOperationContext* context) = 0;

  virtual std::any visitQueryPrimaryDefault(
      SqlBaseParser::QueryPrimaryDefaultContext* context) = 0;

  virtual std::any visitFromStmt(SqlBaseParser::FromStmtContext* context) = 0;

  virtual std::any visitTable(SqlBaseParser::TableContext* context) = 0;

  virtual std::any visitInlineTableDefault1(
      SqlBaseParser::InlineTableDefault1Context* context) = 0;

  virtual std::any visitSubquery(SqlBaseParser::SubqueryContext* context) = 0;

  virtual std::any visitSortItem(SqlBaseParser::SortItemContext* context) = 0;

  virtual std::any visitFromStatement(
      SqlBaseParser::FromStatementContext* context) = 0;

  virtual std::any visitFromStatementBody(
      SqlBaseParser::FromStatementBodyContext* context) = 0;

  virtual std::any visitTransformQuerySpecification(
      SqlBaseParser::TransformQuerySpecificationContext* context) = 0;

  virtual std::any visitRegularQuerySpecification(
      SqlBaseParser::RegularQuerySpecificationContext* context) = 0;

  virtual std::any visitTransformClause(
      SqlBaseParser::TransformClauseContext* context) = 0;

  virtual std::any visitSelectClause(
      SqlBaseParser::SelectClauseContext* context) = 0;

  virtual std::any visitSetClause(SqlBaseParser::SetClauseContext* context) = 0;

  virtual std::any visitMatchedClause(
      SqlBaseParser::MatchedClauseContext* context) = 0;

  virtual std::any visitNotMatchedClause(
      SqlBaseParser::NotMatchedClauseContext* context) = 0;

  virtual std::any visitNotMatchedBySourceClause(
      SqlBaseParser::NotMatchedBySourceClauseContext* context) = 0;

  virtual std::any visitMatchedAction(
      SqlBaseParser::MatchedActionContext* context) = 0;

  virtual std::any visitNotMatchedAction(
      SqlBaseParser::NotMatchedActionContext* context) = 0;

  virtual std::any visitNotMatchedBySourceAction(
      SqlBaseParser::NotMatchedBySourceActionContext* context) = 0;

  virtual std::any visitAssignmentList(
      SqlBaseParser::AssignmentListContext* context) = 0;

  virtual std::any visitAssignment(
      SqlBaseParser::AssignmentContext* context) = 0;

  virtual std::any visitWhereClause(
      SqlBaseParser::WhereClauseContext* context) = 0;

  virtual std::any visitHavingClause(
      SqlBaseParser::HavingClauseContext* context) = 0;

  virtual std::any visitHint(SqlBaseParser::HintContext* context) = 0;

  virtual std::any visitHintStatement(
      SqlBaseParser::HintStatementContext* context) = 0;

  virtual std::any visitFromClause(
      SqlBaseParser::FromClauseContext* context) = 0;

  virtual std::any visitTemporalClause(
      SqlBaseParser::TemporalClauseContext* context) = 0;

  virtual std::any visitAggregationClause(
      SqlBaseParser::AggregationClauseContext* context) = 0;

  virtual std::any visitGroupByClause(
      SqlBaseParser::GroupByClauseContext* context) = 0;

  virtual std::any visitGroupingAnalytics(
      SqlBaseParser::GroupingAnalyticsContext* context) = 0;

  virtual std::any visitGroupingElement(
      SqlBaseParser::GroupingElementContext* context) = 0;

  virtual std::any visitGroupingSet(
      SqlBaseParser::GroupingSetContext* context) = 0;

  virtual std::any visitPivotClause(
      SqlBaseParser::PivotClauseContext* context) = 0;

  virtual std::any visitPivotColumn(
      SqlBaseParser::PivotColumnContext* context) = 0;

  virtual std::any visitPivotValue(
      SqlBaseParser::PivotValueContext* context) = 0;

  virtual std::any visitUnpivotClause(
      SqlBaseParser::UnpivotClauseContext* context) = 0;

  virtual std::any visitUnpivotNullClause(
      SqlBaseParser::UnpivotNullClauseContext* context) = 0;

  virtual std::any visitUnpivotOperator(
      SqlBaseParser::UnpivotOperatorContext* context) = 0;

  virtual std::any visitUnpivotSingleValueColumnClause(
      SqlBaseParser::UnpivotSingleValueColumnClauseContext* context) = 0;

  virtual std::any visitUnpivotMultiValueColumnClause(
      SqlBaseParser::UnpivotMultiValueColumnClauseContext* context) = 0;

  virtual std::any visitUnpivotColumnSet(
      SqlBaseParser::UnpivotColumnSetContext* context) = 0;

  virtual std::any visitUnpivotValueColumn(
      SqlBaseParser::UnpivotValueColumnContext* context) = 0;

  virtual std::any visitUnpivotNameColumn(
      SqlBaseParser::UnpivotNameColumnContext* context) = 0;

  virtual std::any visitUnpivotColumnAndAlias(
      SqlBaseParser::UnpivotColumnAndAliasContext* context) = 0;

  virtual std::any visitUnpivotColumn(
      SqlBaseParser::UnpivotColumnContext* context) = 0;

  virtual std::any visitUnpivotAlias(
      SqlBaseParser::UnpivotAliasContext* context) = 0;

  virtual std::any visitLateralView(
      SqlBaseParser::LateralViewContext* context) = 0;

  virtual std::any visitSetQuantifier(
      SqlBaseParser::SetQuantifierContext* context) = 0;

  virtual std::any visitRelation(SqlBaseParser::RelationContext* context) = 0;

  virtual std::any visitRelationExtension(
      SqlBaseParser::RelationExtensionContext* context) = 0;

  virtual std::any visitJoinRelation(
      SqlBaseParser::JoinRelationContext* context) = 0;

  virtual std::any visitJoinType(SqlBaseParser::JoinTypeContext* context) = 0;

  virtual std::any visitJoinCriteria(
      SqlBaseParser::JoinCriteriaContext* context) = 0;

  virtual std::any visitSample(SqlBaseParser::SampleContext* context) = 0;

  virtual std::any visitSampleByPercentile(
      SqlBaseParser::SampleByPercentileContext* context) = 0;

  virtual std::any visitSampleByRows(
      SqlBaseParser::SampleByRowsContext* context) = 0;

  virtual std::any visitSampleByBucket(
      SqlBaseParser::SampleByBucketContext* context) = 0;

  virtual std::any visitSampleByBytes(
      SqlBaseParser::SampleByBytesContext* context) = 0;

  virtual std::any visitIdentifierList(
      SqlBaseParser::IdentifierListContext* context) = 0;

  virtual std::any visitIdentifierSeq(
      SqlBaseParser::IdentifierSeqContext* context) = 0;

  virtual std::any visitOrderedIdentifierList(
      SqlBaseParser::OrderedIdentifierListContext* context) = 0;

  virtual std::any visitOrderedIdentifier(
      SqlBaseParser::OrderedIdentifierContext* context) = 0;

  virtual std::any visitIdentifierCommentList(
      SqlBaseParser::IdentifierCommentListContext* context) = 0;

  virtual std::any visitIdentifierComment(
      SqlBaseParser::IdentifierCommentContext* context) = 0;

  virtual std::any visitTableName(SqlBaseParser::TableNameContext* context) = 0;

  virtual std::any visitAliasedQuery(
      SqlBaseParser::AliasedQueryContext* context) = 0;

  virtual std::any visitAliasedRelation(
      SqlBaseParser::AliasedRelationContext* context) = 0;

  virtual std::any visitInlineTableDefault2(
      SqlBaseParser::InlineTableDefault2Context* context) = 0;

  virtual std::any visitTableValuedFunction(
      SqlBaseParser::TableValuedFunctionContext* context) = 0;

  virtual std::any visitInlineTable(
      SqlBaseParser::InlineTableContext* context) = 0;

  virtual std::any visitFunctionTable(
      SqlBaseParser::FunctionTableContext* context) = 0;

  virtual std::any visitTableAlias(
      SqlBaseParser::TableAliasContext* context) = 0;

  virtual std::any visitRowFormatSerde(
      SqlBaseParser::RowFormatSerdeContext* context) = 0;

  virtual std::any visitRowFormatDelimited(
      SqlBaseParser::RowFormatDelimitedContext* context) = 0;

  virtual std::any visitMultipartIdentifierList(
      SqlBaseParser::MultipartIdentifierListContext* context) = 0;

  virtual std::any visitMultipartIdentifier(
      SqlBaseParser::MultipartIdentifierContext* context) = 0;

  virtual std::any visitMultipartIdentifierPropertyList(
      SqlBaseParser::MultipartIdentifierPropertyListContext* context) = 0;

  virtual std::any visitMultipartIdentifierProperty(
      SqlBaseParser::MultipartIdentifierPropertyContext* context) = 0;

  virtual std::any visitTableIdentifier(
      SqlBaseParser::TableIdentifierContext* context) = 0;

  virtual std::any visitFunctionIdentifier(
      SqlBaseParser::FunctionIdentifierContext* context) = 0;

  virtual std::any visitNamedExpression(
      SqlBaseParser::NamedExpressionContext* context) = 0;

  virtual std::any visitNamedExpressionSeq(
      SqlBaseParser::NamedExpressionSeqContext* context) = 0;

  virtual std::any visitPartitionFieldList(
      SqlBaseParser::PartitionFieldListContext* context) = 0;

  virtual std::any visitPartitionTransform(
      SqlBaseParser::PartitionTransformContext* context) = 0;

  virtual std::any visitPartitionColumn(
      SqlBaseParser::PartitionColumnContext* context) = 0;

  virtual std::any visitIdentityTransform(
      SqlBaseParser::IdentityTransformContext* context) = 0;

  virtual std::any visitApplyTransform(
      SqlBaseParser::ApplyTransformContext* context) = 0;

  virtual std::any visitTransformArgument(
      SqlBaseParser::TransformArgumentContext* context) = 0;

  virtual std::any visitExpression(
      SqlBaseParser::ExpressionContext* context) = 0;

  virtual std::any visitExpressionSeq(
      SqlBaseParser::ExpressionSeqContext* context) = 0;

  virtual std::any visitLogicalNot(
      SqlBaseParser::LogicalNotContext* context) = 0;

  virtual std::any visitPredicated(
      SqlBaseParser::PredicatedContext* context) = 0;

  virtual std::any visitExists(SqlBaseParser::ExistsContext* context) = 0;

  virtual std::any visitLogicalBinary(
      SqlBaseParser::LogicalBinaryContext* context) = 0;

  virtual std::any visitPredicate(SqlBaseParser::PredicateContext* context) = 0;

  virtual std::any visitValueExpressionDefault(
      SqlBaseParser::ValueExpressionDefaultContext* context) = 0;

  virtual std::any visitComparison(
      SqlBaseParser::ComparisonContext* context) = 0;

  virtual std::any visitArithmeticBinary(
      SqlBaseParser::ArithmeticBinaryContext* context) = 0;

  virtual std::any visitArithmeticUnary(
      SqlBaseParser::ArithmeticUnaryContext* context) = 0;

  virtual std::any visitDatetimeUnit(
      SqlBaseParser::DatetimeUnitContext* context) = 0;

  virtual std::any visitStruct(SqlBaseParser::StructContext* context) = 0;

  virtual std::any visitDereference(
      SqlBaseParser::DereferenceContext* context) = 0;

  virtual std::any visitTimestampadd(
      SqlBaseParser::TimestampaddContext* context) = 0;

  virtual std::any visitSubstring(SqlBaseParser::SubstringContext* context) = 0;

  virtual std::any visitCast(SqlBaseParser::CastContext* context) = 0;

  virtual std::any visitLambda(SqlBaseParser::LambdaContext* context) = 0;

  virtual std::any visitParenthesizedExpression(
      SqlBaseParser::ParenthesizedExpressionContext* context) = 0;

  virtual std::any visitAny_value(SqlBaseParser::Any_valueContext* context) = 0;

  virtual std::any visitTrim(SqlBaseParser::TrimContext* context) = 0;

  virtual std::any visitSimpleCase(
      SqlBaseParser::SimpleCaseContext* context) = 0;

  virtual std::any visitCurrentLike(
      SqlBaseParser::CurrentLikeContext* context) = 0;

  virtual std::any visitColumnReference(
      SqlBaseParser::ColumnReferenceContext* context) = 0;

  virtual std::any visitRowConstructor(
      SqlBaseParser::RowConstructorContext* context) = 0;

  virtual std::any visitLast(SqlBaseParser::LastContext* context) = 0;

  virtual std::any visitStar(SqlBaseParser::StarContext* context) = 0;

  virtual std::any visitOverlay(SqlBaseParser::OverlayContext* context) = 0;

  virtual std::any visitSubscript(SqlBaseParser::SubscriptContext* context) = 0;

  virtual std::any visitTimestampdiff(
      SqlBaseParser::TimestampdiffContext* context) = 0;

  virtual std::any visitSubqueryExpression(
      SqlBaseParser::SubqueryExpressionContext* context) = 0;

  virtual std::any visitConstantDefault(
      SqlBaseParser::ConstantDefaultContext* context) = 0;

  virtual std::any visitExtract(SqlBaseParser::ExtractContext* context) = 0;

  virtual std::any visitPercentile(
      SqlBaseParser::PercentileContext* context) = 0;

  virtual std::any visitFunctionCall(
      SqlBaseParser::FunctionCallContext* context) = 0;

  virtual std::any visitSearchedCase(
      SqlBaseParser::SearchedCaseContext* context) = 0;

  virtual std::any visitPosition(SqlBaseParser::PositionContext* context) = 0;

  virtual std::any visitFirst(SqlBaseParser::FirstContext* context) = 0;

  virtual std::any visitNullLiteral(
      SqlBaseParser::NullLiteralContext* context) = 0;

  virtual std::any visitParameterLiteral(
      SqlBaseParser::ParameterLiteralContext* context) = 0;

  virtual std::any visitIntervalLiteral(
      SqlBaseParser::IntervalLiteralContext* context) = 0;

  virtual std::any visitTypeConstructor(
      SqlBaseParser::TypeConstructorContext* context) = 0;

  virtual std::any visitNumericLiteral(
      SqlBaseParser::NumericLiteralContext* context) = 0;

  virtual std::any visitBooleanLiteral(
      SqlBaseParser::BooleanLiteralContext* context) = 0;

  virtual std::any visitStringLiteral(
      SqlBaseParser::StringLiteralContext* context) = 0;

  virtual std::any visitComparisonOperator(
      SqlBaseParser::ComparisonOperatorContext* context) = 0;

  virtual std::any visitArithmeticOperator(
      SqlBaseParser::ArithmeticOperatorContext* context) = 0;

  virtual std::any visitPredicateOperator(
      SqlBaseParser::PredicateOperatorContext* context) = 0;

  virtual std::any visitBooleanValue(
      SqlBaseParser::BooleanValueContext* context) = 0;

  virtual std::any visitInterval(SqlBaseParser::IntervalContext* context) = 0;

  virtual std::any visitErrorCapturingMultiUnitsInterval(
      SqlBaseParser::ErrorCapturingMultiUnitsIntervalContext* context) = 0;

  virtual std::any visitMultiUnitsInterval(
      SqlBaseParser::MultiUnitsIntervalContext* context) = 0;

  virtual std::any visitErrorCapturingUnitToUnitInterval(
      SqlBaseParser::ErrorCapturingUnitToUnitIntervalContext* context) = 0;

  virtual std::any visitUnitToUnitInterval(
      SqlBaseParser::UnitToUnitIntervalContext* context) = 0;

  virtual std::any visitIntervalValue(
      SqlBaseParser::IntervalValueContext* context) = 0;

  virtual std::any visitUnitInMultiUnits(
      SqlBaseParser::UnitInMultiUnitsContext* context) = 0;

  virtual std::any visitUnitInUnitToUnit(
      SqlBaseParser::UnitInUnitToUnitContext* context) = 0;

  virtual std::any visitColPosition(
      SqlBaseParser::ColPositionContext* context) = 0;

  virtual std::any visitComplexDataType(
      SqlBaseParser::ComplexDataTypeContext* context) = 0;

  virtual std::any visitYearMonthIntervalDataType(
      SqlBaseParser::YearMonthIntervalDataTypeContext* context) = 0;

  virtual std::any visitDayTimeIntervalDataType(
      SqlBaseParser::DayTimeIntervalDataTypeContext* context) = 0;

  virtual std::any visitPrimitiveDataType(
      SqlBaseParser::PrimitiveDataTypeContext* context) = 0;

  virtual std::any visitQualifiedColTypeWithPositionList(
      SqlBaseParser::QualifiedColTypeWithPositionListContext* context) = 0;

  virtual std::any visitQualifiedColTypeWithPosition(
      SqlBaseParser::QualifiedColTypeWithPositionContext* context) = 0;

  virtual std::any visitDefaultExpression(
      SqlBaseParser::DefaultExpressionContext* context) = 0;

  virtual std::any visitColTypeList(
      SqlBaseParser::ColTypeListContext* context) = 0;

  virtual std::any visitColType(SqlBaseParser::ColTypeContext* context) = 0;

  virtual std::any visitCreateOrReplaceTableColTypeList(
      SqlBaseParser::CreateOrReplaceTableColTypeListContext* context) = 0;

  virtual std::any visitCreateOrReplaceTableColType(
      SqlBaseParser::CreateOrReplaceTableColTypeContext* context) = 0;

  virtual std::any visitColDefinitionOption(
      SqlBaseParser::ColDefinitionOptionContext* context) = 0;

  virtual std::any visitGenerationExpression(
      SqlBaseParser::GenerationExpressionContext* context) = 0;

  virtual std::any visitComplexColTypeList(
      SqlBaseParser::ComplexColTypeListContext* context) = 0;

  virtual std::any visitComplexColType(
      SqlBaseParser::ComplexColTypeContext* context) = 0;

  virtual std::any visitWhenClause(
      SqlBaseParser::WhenClauseContext* context) = 0;

  virtual std::any visitWindowClause(
      SqlBaseParser::WindowClauseContext* context) = 0;

  virtual std::any visitNamedWindow(
      SqlBaseParser::NamedWindowContext* context) = 0;

  virtual std::any visitWindowRef(SqlBaseParser::WindowRefContext* context) = 0;

  virtual std::any visitWindowDef(SqlBaseParser::WindowDefContext* context) = 0;

  virtual std::any visitWindowFrame(
      SqlBaseParser::WindowFrameContext* context) = 0;

  virtual std::any visitFrameBound(
      SqlBaseParser::FrameBoundContext* context) = 0;

  virtual std::any visitQualifiedNameList(
      SqlBaseParser::QualifiedNameListContext* context) = 0;

  virtual std::any visitFunctionName(
      SqlBaseParser::FunctionNameContext* context) = 0;

  virtual std::any visitQualifiedName(
      SqlBaseParser::QualifiedNameContext* context) = 0;

  virtual std::any visitErrorCapturingIdentifier(
      SqlBaseParser::ErrorCapturingIdentifierContext* context) = 0;

  virtual std::any visitErrorIdent(
      SqlBaseParser::ErrorIdentContext* context) = 0;

  virtual std::any visitRealIdent(SqlBaseParser::RealIdentContext* context) = 0;

  virtual std::any visitIdentifier(
      SqlBaseParser::IdentifierContext* context) = 0;

  virtual std::any visitUnquotedIdentifier(
      SqlBaseParser::UnquotedIdentifierContext* context) = 0;

  virtual std::any visitQuotedIdentifierAlternative(
      SqlBaseParser::QuotedIdentifierAlternativeContext* context) = 0;

  virtual std::any visitQuotedIdentifier(
      SqlBaseParser::QuotedIdentifierContext* context) = 0;

  virtual std::any visitBackQuotedIdentifier(
      SqlBaseParser::BackQuotedIdentifierContext* context) = 0;

  virtual std::any visitExponentLiteral(
      SqlBaseParser::ExponentLiteralContext* context) = 0;

  virtual std::any visitDecimalLiteral(
      SqlBaseParser::DecimalLiteralContext* context) = 0;

  virtual std::any visitLegacyDecimalLiteral(
      SqlBaseParser::LegacyDecimalLiteralContext* context) = 0;

  virtual std::any visitIntegerLiteral(
      SqlBaseParser::IntegerLiteralContext* context) = 0;

  virtual std::any visitBigIntLiteral(
      SqlBaseParser::BigIntLiteralContext* context) = 0;

  virtual std::any visitSmallIntLiteral(
      SqlBaseParser::SmallIntLiteralContext* context) = 0;

  virtual std::any visitTinyIntLiteral(
      SqlBaseParser::TinyIntLiteralContext* context) = 0;

  virtual std::any visitDoubleLiteral(
      SqlBaseParser::DoubleLiteralContext* context) = 0;

  virtual std::any visitFloatLiteral(
      SqlBaseParser::FloatLiteralContext* context) = 0;

  virtual std::any visitBigDecimalLiteral(
      SqlBaseParser::BigDecimalLiteralContext* context) = 0;

  virtual std::any visitAlterColumnAction(
      SqlBaseParser::AlterColumnActionContext* context) = 0;

  virtual std::any visitStringLit(SqlBaseParser::StringLitContext* context) = 0;

  virtual std::any visitComment(SqlBaseParser::CommentContext* context) = 0;

  virtual std::any visitVersion(SqlBaseParser::VersionContext* context) = 0;

  virtual std::any visitAnsiNonReserved(
      SqlBaseParser::AnsiNonReservedContext* context) = 0;

  virtual std::any visitStrictNonReserved(
      SqlBaseParser::StrictNonReservedContext* context) = 0;

  virtual std::any visitNonReserved(
      SqlBaseParser::NonReservedContext* context) = 0;
};

} // namespace axiom::sql::spark
