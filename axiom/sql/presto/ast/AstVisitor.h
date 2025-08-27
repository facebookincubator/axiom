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

#include <iostream>

#include "axiom/sql/presto/ast/AstExpressions.h"
#include "axiom/sql/presto/ast/AstFunctions.h"
#include "axiom/sql/presto/ast/AstLiterals.h"
#include "axiom/sql/presto/ast/AstNode.h"
#include "axiom/sql/presto/ast/AstRelations.h"
#include "axiom/sql/presto/ast/AstStatements.h"
#include "axiom/sql/presto/ast/AstSupport.h"

namespace axiom::sql::presto {

class AstVisitor {
 public:
  virtual ~AstVisitor() = default;

  // Default implementation of specific visitXxx method.
  virtual void defaultVisit(Node* node) {
    std::cout << "visit" << NodeTypeName::toName(node->type()) << std::endl;
  }

  // Literals
  virtual void visitBooleanLiteral(BooleanLiteral* node) {
    defaultVisit(node);
  }

  virtual void visitStringLiteral(StringLiteral* node) {
    defaultVisit(node);
  }

  virtual void visitBinaryLiteral(BinaryLiteral* node) {
    defaultVisit(node);
  }

  virtual void visitCharLiteral(CharLiteral* node) {
    defaultVisit(node);
  }

  virtual void visitLongLiteral(LongLiteral* node) {
    defaultVisit(node);
  }

  virtual void visitDoubleLiteral(DoubleLiteral* node) {
    defaultVisit(node);
  }

  virtual void visitDecimalLiteral(DecimalLiteral* node) {
    defaultVisit(node);
  }

  virtual void visitGenericLiteral(GenericLiteral* node) {
    defaultVisit(node);
  }

  virtual void visitNullLiteral(NullLiteral* node) {
    defaultVisit(node);
  }

  virtual void visitTimeLiteral(TimeLiteral* node) {
    defaultVisit(node);
  }

  virtual void visitTimestampLiteral(TimestampLiteral* node) {
    defaultVisit(node);
  }

  virtual void visitIntervalLiteral(IntervalLiteral* node) {
    defaultVisit(node);
  }

  virtual void visitEnumLiteral(EnumLiteral* node) {
    defaultVisit(node);
  }

  // Identifiers and References
  virtual void visitIdentifier(Identifier* node) {
    defaultVisit(node);
  }

  virtual void visitQualifiedName(QualifiedName* node) {
    defaultVisit(node);
  }

  virtual void visitDereferenceExpression(DereferenceExpression* node) {
    defaultVisit(node);
  }

  virtual void visitFieldReference(FieldReference* node) {
    defaultVisit(node);
  }

  virtual void visitSymbolReference(SymbolReference* node) {
    defaultVisit(node);
  }

  virtual void visitParameter(Parameter* node) {
    defaultVisit(node);
  }

  // Arithmetic and Comparison Expressions
  virtual void visitArithmeticBinaryExpression(
      ArithmeticBinaryExpression* node) {
    defaultVisit(node);
  }

  virtual void visitArithmeticUnaryExpression(ArithmeticUnaryExpression* node) {
    defaultVisit(node);
  }

  virtual void visitComparisonExpression(ComparisonExpression* node) {
    defaultVisit(node);
  }

  virtual void visitBetweenPredicate(BetweenPredicate* node) {
    defaultVisit(node);
  }

  virtual void visitInPredicate(InPredicate* node) {
    defaultVisit(node);
  }

  virtual void visitInListExpression(InListExpression* node) {
    defaultVisit(node);
  }

  virtual void visitIsNullPredicate(IsNullPredicate* node) {
    defaultVisit(node);
  }

  virtual void visitIsNotNullPredicate(IsNotNullPredicate* node) {
    defaultVisit(node);
  }

  virtual void visitLikePredicate(LikePredicate* node) {
    defaultVisit(node);
  }

  virtual void visitExistsPredicate(ExistsPredicate* node) {
    defaultVisit(node);
  }

  virtual void visitQuantifiedComparisonExpression(
      QuantifiedComparisonExpression* node) {
    defaultVisit(node);
  }

  // Logical Expressions
  virtual void visitLogicalBinaryExpression(LogicalBinaryExpression* node) {
    defaultVisit(node);
  }

  virtual void visitNotExpression(NotExpression* node) {
    defaultVisit(node);
  }

  // Conditional Expressions
  virtual void visitIfExpression(IfExpression* node) {
    defaultVisit(node);
  }

  virtual void visitCoalesceExpression(CoalesceExpression* node) {
    defaultVisit(node);
  }

  virtual void visitNullIfExpression(NullIfExpression* node) {
    defaultVisit(node);
  }

  virtual void visitWhenClause(WhenClause* node) {
    defaultVisit(node);
  }

  virtual void visitSearchedCaseExpression(SearchedCaseExpression* node) {
    defaultVisit(node);
  }

  virtual void visitSimpleCaseExpression(SimpleCaseExpression* node) {
    defaultVisit(node);
  }

  virtual void visitTryExpression(TryExpression* node) {
    defaultVisit(node);
  }

  // Function and Call Expressions
  virtual void visitFunctionCall(FunctionCall* node) {
    defaultVisit(node);
  }

  virtual void visitCast(Cast* node) {
    defaultVisit(node);
  }

  virtual void visitExtract(Extract* node) {
    defaultVisit(node);
  }

  virtual void visitCurrentTime(CurrentTime* node) {
    defaultVisit(node);
  }

  virtual void visitCurrentUser(CurrentUser* node) {
    defaultVisit(node);
  }

  virtual void visitAtTimeZone(AtTimeZone* node) {
    defaultVisit(node);
  }

  // Complex Expressions
  virtual void visitSubqueryExpression(SubqueryExpression* node) {
    defaultVisit(node);
  }

  virtual void visitArrayConstructor(ArrayConstructor* node) {
    defaultVisit(node);
  }

  virtual void visitRow(Row* node) {
    defaultVisit(node);
  }

  virtual void visitSubscriptExpression(SubscriptExpression* node) {
    defaultVisit(node);
  }

  virtual void visitLambdaExpression(LambdaExpression* node) {
    defaultVisit(node);
  }

  virtual void visitLambdaArgumentDeclaration(LambdaArgumentDeclaration* node) {
    defaultVisit(node);
  }

  virtual void visitBindExpression(BindExpression* node) {
    defaultVisit(node);
  }

  virtual void visitGroupingOperation(GroupingOperation* node) {
    defaultVisit(node);
  }

  virtual void visitTableVersionExpression(TableVersionExpression* node) {
    defaultVisit(node);
  }

  // Query structures
  virtual void visitQuery(Query* node) {
    defaultVisit(node);
  }

  virtual void visitQuerySpecification(QuerySpecification* node) {
    defaultVisit(node);
  }

  virtual void visitSelect(Select* node) {
    defaultVisit(node);
  }

  virtual void visitSingleColumn(SingleColumn* node) {
    defaultVisit(node);
  }

  virtual void visitAllColumns(AllColumns* node) {
    defaultVisit(node);
  }

  virtual void visitWith(With* node) {
    defaultVisit(node);
  }

  virtual void visitWithQuery(WithQuery* node) {
    defaultVisit(node);
  }

  virtual void visitOrderBy(OrderBy* node) {
    defaultVisit(node);
  }

  virtual void visitSortItem(SortItem* node) {
    defaultVisit(node);
  }

  virtual void visitGroupBy(GroupBy* node) {
    defaultVisit(node);
  }

  virtual void visitSimpleGroupBy(SimpleGroupBy* node) {
    defaultVisit(node);
  }

  virtual void visitGroupingSets(GroupingSets* node) {
    defaultVisit(node);
  }

  virtual void visitCube(Cube* node) {
    defaultVisit(node);
  }

  virtual void visitRollup(Rollup* node) {
    defaultVisit(node);
  }

  virtual void visitOffset(Offset* node) {
    defaultVisit(node);
  }

  // Relations
  virtual void visitTable(Table* node) {
    defaultVisit(node);
  }

  virtual void visitAliasedRelation(AliasedRelation* node) {
    defaultVisit(node);
  }

  virtual void visitSampledRelation(SampledRelation* node) {
    defaultVisit(node);
  }

  virtual void visitTableSubquery(TableSubquery* node) {
    defaultVisit(node);
  }

  virtual void visitLateral(Lateral* node) {
    defaultVisit(node);
  }

  virtual void visitUnnest(Unnest* node) {
    defaultVisit(node);
  }

  virtual void visitValues(Values* node) {
    defaultVisit(node);
  }

  // Joins
  virtual void visitJoin(Join* node) {
    defaultVisit(node);
  }

  virtual void visitJoinOn(JoinOn* node) {
    defaultVisit(node);
  }

  virtual void visitJoinUsing(JoinUsing* node) {
    defaultVisit(node);
  }

  virtual void visitNaturalJoin(NaturalJoin* node) {
    defaultVisit(node);
  }

  // Set Operations
  virtual void visitUnion(Union* node) {
    defaultVisit(node);
  }

  virtual void visitIntersect(Intersect* node) {
    defaultVisit(node);
  }

  virtual void visitExcept(Except* node) {
    defaultVisit(node);
  }

  // DDL Statements
  virtual void visitCreateTable(CreateTable* node) {
    defaultVisit(node);
  }

  virtual void visitCreateTableAsSelect(CreateTableAsSelect* node) {
    defaultVisit(node);
  }

  virtual void visitCreateView(CreateView* node) {
    defaultVisit(node);
  }

  virtual void visitCreateMaterializedView(CreateMaterializedView* node) {
    defaultVisit(node);
  }

  virtual void visitCreateSchema(CreateSchema* node) {
    defaultVisit(node);
  }

  virtual void visitCreateFunction(CreateFunction* node) {
    defaultVisit(node);
  }

  virtual void visitCreateRole(CreateRole* node) {
    defaultVisit(node);
  }

  virtual void visitCreateType(CreateType* node) {
    defaultVisit(node);
  }

  virtual void visitDropTable(DropTable* node) {
    defaultVisit(node);
  }

  virtual void visitDropView(DropView* node) {
    defaultVisit(node);
  }

  virtual void visitDropMaterializedView(DropMaterializedView* node) {
    defaultVisit(node);
  }

  virtual void visitDropSchema(DropSchema* node) {
    defaultVisit(node);
  }

  // DML Statements
  virtual void visitInsert(Insert* node) {
    defaultVisit(node);
  }

  virtual void visitUpdateAssignment(UpdateAssignment* node) {
    defaultVisit(node);
  }

  virtual void visitUpdate(Update* node) {
    defaultVisit(node);
  }

  virtual void visitDelete(Delete* node) {
    defaultVisit(node);
  }

  // Utility Statements
  virtual void visitExplain(Explain* node) {
    defaultVisit(node);
  }

  virtual void visitAnalyze(Analyze* node) {
    defaultVisit(node);
  }

  virtual void visitCall(Call* node) {
    defaultVisit(node);
  }

  // Transaction Statements
  virtual void visitStartTransaction(StartTransaction* node) {
    defaultVisit(node);
  }

  virtual void visitCommit(Commit* node) {
    defaultVisit(node);
  }

  virtual void visitRollback(Rollback* node) {
    defaultVisit(node);
  }

  // Table Elements
  virtual void visitColumnDefinition(ColumnDefinition* node) {
    defaultVisit(node);
  }

  virtual void visitLikeClause(LikeClause* node) {
    defaultVisit(node);
  }

  virtual void visitConstraintSpecification(ConstraintSpecification* node) {
    defaultVisit(node);
  }

  // Support Classes
  virtual void visitTypeSignature(TypeSignature* node) {
    defaultVisit(node);
  }

  virtual void visitProperty(Property* node) {
    defaultVisit(node);
  }

  virtual void visitCallArgument(CallArgument* node) {
    defaultVisit(node);
  }

  virtual void visitWindow(Window* node) {
    defaultVisit(node);
  }

  virtual void visitWindowFrame(WindowFrame* node) {
    defaultVisit(node);
  }

  virtual void visitFrameBound(FrameBound* node) {
    defaultVisit(node);
  }

  virtual void visitPrincipalSpecification(PrincipalSpecification* node) {
    defaultVisit(node);
  }

  virtual void visitGrantorSpecification(GrantorSpecification* node) {
    defaultVisit(node);
  }

  virtual void visitIsolation(Isolation* node) {
    defaultVisit(node);
  }

  virtual void visitTransactionAccessMode(TransactionAccessMode* node) {
    defaultVisit(node);
  }

  virtual void visitSqlParameterDeclaration(SqlParameterDeclaration* node) {
    defaultVisit(node);
  }

  virtual void visitRoutineCharacteristics(RoutineCharacteristics* node) {
    defaultVisit(node);
  }

  virtual void visitExternalBodyReference(ExternalBodyReference* node) {
    defaultVisit(node);
  }

  virtual void visitReturn(Return* node) {
    defaultVisit(node);
  }

  virtual void visitExplainFormat(ExplainFormat* node) {
    defaultVisit(node);
  }

  virtual void visitExplainType(ExplainType* node) {
    defaultVisit(node);
  }

  // Generic visit method
  virtual void visit(Node* node) {
    defaultVisit(node);
  }
};

} // namespace axiom::sql::presto
