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

#include "axiom/sql/presto/ast/AstVisitor.h"
#include "velox/common/base/Exceptions.h"

namespace axiom::sql::presto {

// A traversal base class which recursively visits each node.
// Inherit from this class and override handlers for any node
// type which needs custom logic.
class DefaultTraversalVisitor : public AstVisitor {
 public:
  virtual ~DefaultTraversalVisitor() = default;

  void process(Node* node) {
    node->accept(this);
  }

 protected:
  void defaultVisit(Node* node) override {
    // If a node is added to AstVisitor but not overridden here
    // with traversal logic, some part of the AST will be skipped.
    // To prevent this, DefaultTraversalVisitor will throw an
    // error for any node which triggers default handling.
    VELOX_CHECK(
        false,
        "Unhandled node type in DefaultTraversalVisitor: {}",
        NodeTypeName::toName(node->type()));
  }

  void visitBooleanLiteral(BooleanLiteral* /*node*/) override {}

  void visitStringLiteral(StringLiteral* /*node*/) override {}

  void visitBinaryLiteral(BinaryLiteral* /*node*/) override {}

  void visitCharLiteral(CharLiteral* /*node*/) override {}

  void visitLongLiteral(LongLiteral* /*node*/) override {}

  void visitDoubleLiteral(DoubleLiteral* /*node*/) override {}

  void visitDecimalLiteral(DecimalLiteral* /*node*/) override {}

  void visitGenericLiteral(GenericLiteral* /*node*/) override {}

  void visitNullLiteral(NullLiteral* /*node*/) override {}

  void visitTimeLiteral(TimeLiteral* /*node*/) override {}

  void visitTimestampLiteral(TimestampLiteral* /*node*/) override {}

  void visitIntervalLiteral(IntervalLiteral* /*node*/) override {}

  void visitEnumLiteral(EnumLiteral* /*node*/) override {}

  void visitIdentifier(Identifier* /*node*/) override {}

  void visitQualifiedName(QualifiedName* /*node*/) override {}

  void visitFieldReference(FieldReference* /*node*/) override {}

  void visitSymbolReference(SymbolReference* /*node*/) override {}

  void visitParameter(Parameter* /*node*/) override {}

  void visitQuery(Query* node) override {
    if (node->with()) {
      node->with()->accept(this);
    }
    if (node->queryBody()) {
      node->queryBody()->accept(this);
    }
    if (node->orderBy()) {
      node->orderBy()->accept(this);
    }
    if (node->offset()) {
      node->offset()->accept(this);
    }
  }

  void visitQuerySpecification(QuerySpecification* node) override {
    if (node->select()) {
      node->select()->accept(this);
    }
    if (node->from()) {
      node->from()->accept(this);
    }
    if (node->where()) {
      node->where()->accept(this);
    }
    if (node->groupBy()) {
      node->groupBy()->accept(this);
    }
    if (node->having()) {
      node->having()->accept(this);
    }
    if (node->orderBy()) {
      node->orderBy()->accept(this);
    }
    if (node->offset()) {
      node->offset()->accept(this);
    }
  }

  void visitSelect(Select* node) override {
    for (const auto& item : node->selectItems()) {
      item->accept(this);
    }
  }

  void visitSingleColumn(SingleColumn* node) override {
    if (node->expression()) {
      node->expression()->accept(this);
    }
    if (node->alias()) {
      node->alias()->accept(this);
    }
  }

  void visitAllColumns(AllColumns* node) override {
    if (node->prefix()) {
      node->prefix()->accept(this);
    }
  }

  void visitWith(With* node) override {
    for (const auto& query : node->queries()) {
      query->accept(this);
    }
  }

  void visitWithQuery(WithQuery* node) override {
    if (node->name()) {
      node->name()->accept(this);
    }
    if (node->query()) {
      node->query()->accept(this);
    }
  }

  void visitOrderBy(OrderBy* node) override {
    for (const auto& item : node->sortItems()) {
      item->accept(this);
    }
  }

  void visitSortItem(SortItem* node) override {
    if (node->sortKey()) {
      node->sortKey()->accept(this);
    }
  }

  void visitGroupBy(GroupBy* node) override {
    for (const auto& element : node->groupingElements()) {
      element->accept(this);
    }
  }

  void visitSimpleGroupBy(SimpleGroupBy* node) override {
    for (const auto& expr : node->expressions()) {
      expr->accept(this);
    }
  }

  void visitGroupingSets(GroupingSets* node) override {
    for (const auto& set : node->sets()) {
      for (const auto& expr : set) {
        expr->accept(this);
      }
    }
  }

  void visitCube(Cube* node) override {
    for (const auto& expr : node->expressions()) {
      expr->accept(this);
    }
  }

  void visitRollup(Rollup* node) override {
    for (const auto& expr : node->expressions()) {
      expr->accept(this);
    }
  }

  void visitOffset(Offset* /*node*/) override {}

  void visitTable(Table* node) override {
    if (node->name()) {
      node->name()->accept(this);
    }
    if (node->version()) {
      node->version()->accept(this);
    }
  }

  void visitAliasedRelation(AliasedRelation* node) override {
    if (node->relation()) {
      node->relation()->accept(this);
    }
    if (node->alias()) {
      node->alias()->accept(this);
    }
  }

  void visitSampledRelation(SampledRelation* node) override {
    if (node->relation()) {
      node->relation()->accept(this);
    }
    if (node->samplePercentage()) {
      node->samplePercentage()->accept(this);
    }
  }

  void visitTableSubquery(TableSubquery* node) override {
    if (node->query()) {
      node->query()->accept(this);
    }
  }

  void visitLateral(Lateral* node) override {
    if (node->query()) {
      node->query()->accept(this);
    }
  }

  void visitUnnest(Unnest* node) override {
    for (const auto& expr : node->expressions()) {
      expr->accept(this);
    }
  }

  void visitValues(Values* node) override {
    for (const auto& row : node->rows()) {
      row->accept(this);
    }
  }

  void visitJoin(Join* node) override {
    if (node->left()) {
      node->left()->accept(this);
    }
    if (node->right()) {
      node->right()->accept(this);
    }
    if (node->criteria()) {
      node->criteria()->accept(this);
    }
  }

  void visitJoinOn(JoinOn* node) override {
    if (node->expression()) {
      node->expression()->accept(this);
    }
  }

  void visitJoinUsing(JoinUsing* node) override {
    for (const auto& column : node->columns()) {
      column->accept(this);
    }
  }

  void visitNaturalJoin(NaturalJoin* node) override {
    if (node->left()) {
      node->left()->accept(this);
    }
    if (node->right()) {
      node->right()->accept(this);
    }
  }

  void visitUnion(Union* node) override {
    if (node->left()) {
      node->left()->accept(this);
    }
    if (node->right()) {
      node->right()->accept(this);
    }
  }

  void visitIntersect(Intersect* node) override {
    if (node->left()) {
      node->left()->accept(this);
    }
    if (node->right()) {
      node->right()->accept(this);
    }
  }

  void visitExcept(Except* node) override {
    if (node->left()) {
      node->left()->accept(this);
    }
    if (node->right()) {
      node->right()->accept(this);
    }
  }

  void visitInsert(Insert* node) override {
    if (node->target()) {
      node->target()->accept(this);
    }
    if (node->query()) {
      node->query()->accept(this);
    }
  }

  void visitUpdate(Update* node) override {
    if (node->table()) {
      node->table()->accept(this);
    }
    for (const auto& assignment : node->assignments()) {
      assignment->accept(this);
    }
    if (node->where()) {
      node->where()->accept(this);
    }
  }

  void visitUpdateAssignment(UpdateAssignment* node) override {
    if (node->name()) {
      node->name()->accept(this);
    }
    if (node->value()) {
      node->value()->accept(this);
    }
  }

  void visitDelete(Delete* node) override {
    if (node->table()) {
      node->table()->accept(this);
    }
    if (node->where()) {
      node->where()->accept(this);
    }
  }

  void visitCreateTable(CreateTable* node) override {
    if (node->name()) {
      node->name()->accept(this);
    }
    for (const auto& element : node->elements()) {
      element->accept(this);
    }
    for (const auto& property : node->properties()) {
      property->accept(this);
    }
  }

  void visitCreateTableAsSelect(CreateTableAsSelect* node) override {
    if (node->name()) {
      node->name()->accept(this);
    }
    if (node->query()) {
      node->query()->accept(this);
    }
    for (const auto& property : node->properties()) {
      property->accept(this);
    }
  }

  void visitCreateView(CreateView* node) override {
    if (node->name()) {
      node->name()->accept(this);
    }
    if (node->query()) {
      node->query()->accept(this);
    }
  }

  void visitCreateMaterializedView(CreateMaterializedView* node) override {
    if (node->name()) {
      node->name()->accept(this);
    }
    if (node->query()) {
      node->query()->accept(this);
    }
    for (const auto& property : node->properties()) {
      property->accept(this);
    }
  }

  void visitCreateSchema(CreateSchema* node) override {
    if (node->schemaName()) {
      node->schemaName()->accept(this);
    }
    for (const auto& property : node->properties()) {
      property->accept(this);
    }
  }

  void visitCreateFunction(CreateFunction* node) override {
    if (node->functionName()) {
      node->functionName()->accept(this);
    }
    for (const auto& param : node->parameters()) {
      param->accept(this);
    }
    if (node->characteristics()) {
      node->characteristics()->accept(this);
    }
    if (node->body()) {
      node->body()->accept(this);
    }
  }

  void visitCreateRole(CreateRole* node) override {
    if (node->grantor()) {
      node->grantor()->accept(this);
    }
  }

  void visitCreateType(CreateType* node) override {
    if (node->name()) {
      node->name()->accept(this);
    }
  }

  void visitDropTable(DropTable* node) override {
    if (node->tableName()) {
      node->tableName()->accept(this);
    }
  }

  void visitDropView(DropView* node) override {
    if (node->viewName()) {
      node->viewName()->accept(this);
    }
  }

  void visitDropMaterializedView(DropMaterializedView* node) override {
    if (node->viewName()) {
      node->viewName()->accept(this);
    }
  }

  void visitDropSchema(DropSchema* node) override {
    if (node->schemaName()) {
      node->schemaName()->accept(this);
    }
  }

  void visitExplain(Explain* node) override {
    if (node->statement()) {
      node->statement()->accept(this);
    }
    for (const auto& option : node->options()) {
      option->accept(this);
    }
  }

  void visitAnalyze(Analyze* node) override {
    if (node->tableName()) {
      node->tableName()->accept(this);
    }
    for (const auto& property : node->properties()) {
      property->accept(this);
    }
  }

  void visitCall(Call* node) override {
    if (node->name()) {
      node->name()->accept(this);
    }
    for (const auto& arg : node->arguments()) {
      arg->accept(this);
    }
  }

  void visitStartTransaction(StartTransaction* /*node*/) override {}

  void visitCommit(Commit* /*node*/) override {}

  void visitRollback(Rollback* /*node*/) override {}

  void visitColumnDefinition(ColumnDefinition* node) override {
    if (node->name()) {
      node->name()->accept(this);
    }
    for (const auto& property : node->properties()) {
      property->accept(this);
    }
  }

  void visitLikeClause(LikeClause* node) override {
    if (node->tableName()) {
      node->tableName()->accept(this);
    }
  }

  void visitConstraintSpecification(
      ConstraintSpecification* /*node*/) override {}

  void visitArithmeticBinaryExpression(
      ArithmeticBinaryExpression* node) override {
    if (node->left()) {
      node->left()->accept(this);
    }
    if (node->right()) {
      node->right()->accept(this);
    }
  }

  void visitArithmeticUnaryExpression(
      ArithmeticUnaryExpression* node) override {
    if (node->value()) {
      node->value()->accept(this);
    }
  }

  void visitComparisonExpression(ComparisonExpression* node) override {
    if (node->left()) {
      node->left()->accept(this);
    }
    if (node->right()) {
      node->right()->accept(this);
    }
  }

  void visitBetweenPredicate(BetweenPredicate* node) override {
    if (node->value()) {
      node->value()->accept(this);
    }
    if (node->min()) {
      node->min()->accept(this);
    }
    if (node->max()) {
      node->max()->accept(this);
    }
  }

  void visitInPredicate(InPredicate* node) override {
    if (node->value()) {
      node->value()->accept(this);
    }
    if (node->valueList()) {
      node->valueList()->accept(this);
    }
  }

  void visitInListExpression(InListExpression* node) override {
    for (const auto& value : node->values()) {
      value->accept(this);
    }
  }

  void visitIsNullPredicate(IsNullPredicate* node) override {
    if (node->value()) {
      node->value()->accept(this);
    }
  }

  void visitIsNotNullPredicate(IsNotNullPredicate* node) override {
    if (node->value()) {
      node->value()->accept(this);
    }
  }

  void visitLikePredicate(LikePredicate* node) override {
    if (node->value()) {
      node->value()->accept(this);
    }
    if (node->pattern()) {
      node->pattern()->accept(this);
    }
    if (node->escape()) {
      node->escape()->accept(this);
    }
  }

  void visitExistsPredicate(ExistsPredicate* node) override {
    if (node->subquery()) {
      node->subquery()->accept(this);
    }
  }

  void visitQuantifiedComparisonExpression(
      QuantifiedComparisonExpression* node) override {
    if (node->value()) {
      node->value()->accept(this);
    }
    if (node->subquery()) {
      node->subquery()->accept(this);
    }
  }

  void visitLogicalBinaryExpression(LogicalBinaryExpression* node) override {
    if (node->left()) {
      node->left()->accept(this);
    }
    if (node->right()) {
      node->right()->accept(this);
    }
  }

  void visitNotExpression(NotExpression* node) override {
    if (node->value()) {
      node->value()->accept(this);
    }
  }

  void visitSearchedCaseExpression(SearchedCaseExpression* node) override {
    for (const auto& clause : node->whenClauses()) {
      clause->accept(this);
    }
    if (node->defaultValue()) {
      node->defaultValue()->accept(this);
    }
  }

  void visitSimpleCaseExpression(SimpleCaseExpression* node) override {
    if (node->operand()) {
      node->operand()->accept(this);
    }
    for (const auto& clause : node->whenClauses()) {
      clause->accept(this);
    }
    if (node->defaultValue()) {
      node->defaultValue()->accept(this);
    }
  }

  void visitWhenClause(WhenClause* node) override {
    if (node->operand()) {
      node->operand()->accept(this);
    }
    if (node->result()) {
      node->result()->accept(this);
    }
  }

  void visitCoalesceExpression(CoalesceExpression* node) override {
    for (const auto& operand : node->operands()) {
      operand->accept(this);
    }
  }

  void visitIfExpression(IfExpression* node) override {
    if (node->condition()) {
      node->condition()->accept(this);
    }
    if (node->trueValue()) {
      node->trueValue()->accept(this);
    }
    if (node->falseValue()) {
      node->falseValue()->accept(this);
    }
  }

  void visitNullIfExpression(NullIfExpression* node) override {
    if (node->first()) {
      node->first()->accept(this);
    }
    if (node->second()) {
      node->second()->accept(this);
    }
  }

  void visitTryExpression(TryExpression* node) override {
    if (node->innerExpression()) {
      node->innerExpression()->accept(this);
    }
  }

  void visitFunctionCall(FunctionCall* node) override {
    if (node->name()) {
      node->name()->accept(this);
    }
    for (const auto& arg : node->arguments()) {
      arg->accept(this);
    }
    if (node->filter()) {
      node->filter()->accept(this);
    }
    if (node->orderBy()) {
      node->orderBy()->accept(this);
    }
    if (node->window()) {
      node->window()->accept(this);
    }
  }

  void visitCast(Cast* node) override {
    if (node->expression()) {
      node->expression()->accept(this);
    }
  }

  void visitExtract(Extract* node) override {
    if (node->expression()) {
      node->expression()->accept(this);
    }
  }

  void visitCurrentTime(CurrentTime* /*node*/) override {}

  void visitCurrentUser(CurrentUser* /*node*/) override {}

  void visitAtTimeZone(AtTimeZone* node) override {
    if (node->value()) {
      node->value()->accept(this);
    }
    if (node->timeZone()) {
      node->timeZone()->accept(this);
    }
  }

  void visitSubqueryExpression(SubqueryExpression* node) override {
    if (node->query()) {
      node->query()->accept(this);
    }
  }

  void visitArrayConstructor(ArrayConstructor* node) override {
    for (const auto& value : node->values()) {
      value->accept(this);
    }
  }

  void visitRow(Row* node) override {
    for (const auto& item : node->items()) {
      item->accept(this);
    }
  }

  void visitSubscriptExpression(SubscriptExpression* node) override {
    if (node->base()) {
      node->base()->accept(this);
    }
    if (node->index()) {
      node->index()->accept(this);
    }
  }

  void visitLambdaExpression(LambdaExpression* node) override {
    for (const auto& arg : node->arguments()) {
      arg->accept(this);
    }
    if (node->body()) {
      node->body()->accept(this);
    }
  }

  void visitLambdaArgumentDeclaration(
      LambdaArgumentDeclaration* /*node*/) override {}

  void visitBindExpression(BindExpression* node) override {
    for (const auto& value : node->values()) {
      value->accept(this);
    }
    if (node->function()) {
      node->function()->accept(this);
    }
  }

  void visitGroupingOperation(GroupingOperation* node) override {
    for (const auto& arg : node->groupingColumns()) {
      arg->accept(this);
    }
  }

  void visitTableVersionExpression(TableVersionExpression* node) override {
    if (node->expression()) {
      node->expression()->accept(this);
    }
  }

  void visitDereferenceExpression(DereferenceExpression* node) override {
    if (node->base()) {
      node->base()->accept(this);
    }
    if (node->field()) {
      node->field()->accept(this);
    }
  }

  void visitTypeSignature(TypeSignature* /*node*/) override {}

  void visitProperty(Property* node) override {
    if (node->name()) {
      node->name()->accept(this);
    }
    if (node->value()) {
      node->value()->accept(this);
    }
  }

  void visitCallArgument(CallArgument* node) override {
    if (node->name()) {
      node->name()->accept(this);
    }
    if (node->value()) {
      node->value()->accept(this);
    }
  }

  void visitWindow(Window* node) override {
    for (const auto& expr : node->partitionBy()) {
      expr->accept(this);
    }
    if (node->orderBy()) {
      node->orderBy()->accept(this);
    }
    if (node->frame()) {
      node->frame()->accept(this);
    }
  }

  void visitWindowFrame(WindowFrame* node) override {
    if (node->start()) {
      node->start()->accept(this);
    }
    if (node->end()) {
      node->end()->accept(this);
    }
  }

  void visitFrameBound(FrameBound* node) override {
    if (node->value()) {
      node->value().value()->accept(this);
    }
  }

  void visitPrincipalSpecification(PrincipalSpecification* /*node*/) override {}

  void visitGrantorSpecification(GrantorSpecification* node) override {
    if (node->principal()) {
      node->principal()->accept(this);
    }
  }

  void visitIsolation(Isolation* /*node*/) override {}

  void visitTransactionAccessMode(TransactionAccessMode* /*node*/) override {}

  void visitSqlParameterDeclaration(SqlParameterDeclaration* node) override {
    if (node->name()) {
      node->name()->accept(this);
    }
  }

  void visitRoutineCharacteristics(RoutineCharacteristics* /*node*/) override {}

  void visitExternalBodyReference(ExternalBodyReference* /*node*/) override {}

  void visitReturn(Return* node) override {
    if (node->expression()) {
      node->expression()->accept(this);
    }
  }

  void visitExplainFormat(ExplainFormat* /*node*/) override {}

  void visitExplainType(ExplainType* /*node*/) override {}

  void visitShowCatalogs(ShowCatalogs* /*node*/) override {}

  void visitShowColumns(ShowColumns* node) override {
    if (node->table()) {
      node->table()->accept(this);
    }
  }

  void visitShowFunctions(ShowFunctions* /*node*/) override {}
};

} // namespace axiom::sql::presto
