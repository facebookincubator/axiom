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

#include <functional>

#include "axiom/sql/presto/ast/AstVisitor.h"

namespace axiom::sql::presto {

class AstPrinter : public AstVisitor {
 public:
  explicit AstPrinter(std::ostream& out) : out_(out) {}

  // Literals
  void visitBooleanLiteral(BooleanLiteral* node) override;

  void visitStringLiteral(StringLiteral* node) override;

  void visitBinaryLiteral(BinaryLiteral* node) override;

  void visitCharLiteral(CharLiteral* node) override;

  void visitLongLiteral(LongLiteral* node) override;

  void visitDoubleLiteral(DoubleLiteral* node) override;

  void visitDecimalLiteral(DecimalLiteral* node) override;

  void visitGenericLiteral(GenericLiteral* node) override;

  void visitNullLiteral(NullLiteral* node) override;

  void visitTimeLiteral(TimeLiteral* node) override;

  void visitTimestampLiteral(TimestampLiteral* node) override;

  void visitIntervalLiteral(IntervalLiteral* node) override;

  void visitEnumLiteral(EnumLiteral* node) override;

  // Identifiers and References
  void visitIdentifier(Identifier* node) override;

  void visitQualifiedName(QualifiedName* node) override;

  void visitDereferenceExpression(DereferenceExpression* node) override;

  void visitFieldReference(FieldReference* node) override;

  void visitSymbolReference(SymbolReference* node) override;

  void visitParameter(Parameter* node) override;

  // Arithmetic and Comparison Expressions
  void visitArithmeticBinaryExpression(
      ArithmeticBinaryExpression* node) override;

  void visitArithmeticUnaryExpression(ArithmeticUnaryExpression* node) override;

  void visitComparisonExpression(ComparisonExpression* node) override;

  void visitBetweenPredicate(BetweenPredicate* node) override;

  void visitInPredicate(InPredicate* node) override;

  void visitInListExpression(InListExpression* node) override;

  void visitIsNullPredicate(IsNullPredicate* node) override;

  void visitIsNotNullPredicate(IsNotNullPredicate* node) override;

  void visitLikePredicate(LikePredicate* node) override;

  void visitExistsPredicate(ExistsPredicate* node) override;

  void visitQuantifiedComparisonExpression(
      QuantifiedComparisonExpression* node) override;

  // Logical Expressions
  void visitLogicalBinaryExpression(LogicalBinaryExpression* node) override;

  void visitNotExpression(NotExpression* node) override;

  // Conditional Expressions
  void visitIfExpression(IfExpression* node) override;

  void visitCoalesceExpression(CoalesceExpression* node) override;

  void visitNullIfExpression(NullIfExpression* node) override;

  void visitWhenClause(WhenClause* node) override;

  void visitSearchedCaseExpression(SearchedCaseExpression* node) override;

  void visitSimpleCaseExpression(SimpleCaseExpression* node) override;

  void visitTryExpression(TryExpression* node) override;

  // Function and Call Expressions
  void visitFunctionCall(FunctionCall* node) override;

  void visitCast(Cast* node) override;

  void visitExtract(Extract* node) override;

  void visitCurrentTime(CurrentTime* node) override;

  void visitCurrentUser(CurrentUser* node) override;

  void visitAtTimeZone(AtTimeZone* node) override;

  // Complex Expressions
  void visitSubqueryExpression(SubqueryExpression* node) override;

  void visitArrayConstructor(ArrayConstructor* node) override;

  void visitRow(Row* node) override;

  void visitSubscriptExpression(SubscriptExpression* node) override;

  void visitLambdaExpression(LambdaExpression* node) override;

  void visitLambdaArgumentDeclaration(LambdaArgumentDeclaration* node) override;

  void visitBindExpression(BindExpression* node) override;

  void visitGroupingOperation(GroupingOperation* node) override;

  void visitTableVersionExpression(TableVersionExpression* node) override;

  // Query structures
  void visitQuery(Query* node) override;

  void visitQuerySpecification(QuerySpecification* node) override;

  void visitSelect(Select* node) override;

  void visitSingleColumn(SingleColumn* node) override;

  void visitAllColumns(AllColumns* node) override;

  void visitWith(With* node) override;

  void visitWithQuery(WithQuery* node) override;

  void visitOrderBy(OrderBy* node) override;

  void visitSortItem(SortItem* node) override;

  void visitGroupBy(GroupBy* node) override;

  void visitSimpleGroupBy(SimpleGroupBy* node) override;

  void visitGroupingSets(GroupingSets* node) override;

  void visitCube(Cube* node) override;

  void visitRollup(Rollup* node) override;

  void visitOffset(Offset* node) override;

  // Relations
  void visitTable(Table* node) override;

  void visitAliasedRelation(AliasedRelation* node) override;

  void visitSampledRelation(SampledRelation* node) override;

  void visitTableSubquery(TableSubquery* node) override;

  void visitLateral(Lateral* node) override;

  void visitUnnest(Unnest* node) override;

  void visitValues(Values* node) override;

  // Joins
  void visitJoin(Join* node) override;

  void visitJoinOn(JoinOn* node) override;

  void visitJoinUsing(JoinUsing* node) override;

  void visitNaturalJoin(NaturalJoin* node) override;

  // Set Operations
  void visitUnion(Union* node) override;

  void visitIntersect(Intersect* node) override;

  void visitExcept(Except* node) override;

  // DDL Statements
  void visitCreateTable(CreateTable* node) override;

  void visitCreateTableAsSelect(CreateTableAsSelect* node) override;

  void visitCreateView(CreateView* node) override;

  void visitCreateMaterializedView(CreateMaterializedView* node) override;

  void visitCreateSchema(CreateSchema* node) override;

  void visitCreateFunction(CreateFunction* node) override;

  void visitCreateRole(CreateRole* node) override;

  void visitCreateType(CreateType* node) override;

  void visitDropTable(DropTable* node) override;

  void visitDropView(DropView* node) override;

  void visitDropMaterializedView(DropMaterializedView* node) override;

  void visitDropSchema(DropSchema* node) override;

  // DML Statements
  void visitInsert(Insert* node) override;

  void visitUpdateAssignment(UpdateAssignment* node) override;

  void visitUpdate(Update* node) override;

  void visitDelete(Delete* node) override;

  // Utility Statements
  void visitExplain(Explain* node) override;

  void visitAnalyze(Analyze* node) override;

  void visitCall(Call* node) override;

  // Transaction Statements
  void visitStartTransaction(StartTransaction* node) override;

  void visitCommit(Commit* node) override;

  void visitRollback(Rollback* node) override;

  // Table Elements
  void visitColumnDefinition(ColumnDefinition* node) override;

  void visitLikeClause(LikeClause* node) override;

  void visitConstraintSpecification(ConstraintSpecification* node) override;

  // Support Classes
  void visitTypeSignature(TypeSignature* node) override;

  void visitProperty(Property* node) override;

  void visitCallArgument(CallArgument* node) override;

  void visitWindow(Window* node) override;

  void visitWindowFrame(WindowFrame* node) override;

  void visitFrameBound(FrameBound* node) override;

  void visitPrincipalSpecification(PrincipalSpecification* node) override;

  void visitGrantorSpecification(GrantorSpecification* node) override;

  void visitIsolation(Isolation* node) override;

  void visitTransactionAccessMode(TransactionAccessMode* node) override;

  void visitSqlParameterDeclaration(SqlParameterDeclaration* node) override;

  void visitRoutineCharacteristics(RoutineCharacteristics* node) override;

  void visitExternalBodyReference(ExternalBodyReference* node) override;

  void visitReturn(Return* node) override;

  void visitExplainFormat(ExplainFormat* node) override;

  void visitExplainType(ExplainType* node) override;

  void visitShowColumns(ShowColumns* node) override;

 private:
  void defaultVisit(Node* node) override {
    printIndent();
    out_ << "visit" << NodeTypeName::toName(node->type()) << std::endl;
  }

  void printHeader(
      const std::string& name,
      Node* node,
      const std::function<void(std::ostream& out)>& printDetails =
          nullptr) const;

  void printHeader(
      const std::string& name,
      const std::shared_ptr<Node>& node,
      const std::function<void(std::ostream& out)>& printDetails =
          nullptr) const;

  void printChild(const std::string& name, const std::shared_ptr<Node>& node);

  void printIndent();

  std::ostream& out_;
  int indent_ = 0;
};
} // namespace axiom::sql::presto
