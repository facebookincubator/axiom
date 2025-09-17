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

#include "axiom/sql/presto/ast/AstPrinter.h"
#include <fmt/format.h>

namespace axiom::sql::presto {

void AstPrinter::visitQuery(Query* node) {
  printHeader("Query", node);

  indent_++;
  node->queryBody()->accept(this);
  indent_--;

  if (node->orderBy() != nullptr) {
    indent_++;
    node->orderBy()->accept(this);
    indent_--;
  }

  if (const auto& offset = node->offset()) {
    indent_++;
    printHeader(
        "OFFSET", offset, [&](std::ostream& out) { out << offset->offset(); });
    indent_--;
  }

  if (node->limit().has_value()) {
    indent_++;
    printHeader("LIMIT", node, [&](std::ostream& out) {
      out << node->limit().value();
    });
    indent_--;
  }
}

void AstPrinter::visitQuerySpecification(QuerySpecification* node) {
  printHeader("QuerySpecification", node);
  indent_++;

  node->select()->accept(this);

  if (node->from() != nullptr) {
    node->from()->accept(this);
  }

  if (node->where() != nullptr) {
    printChild("WHERE", node->where());
  }

  if (node->groupBy() != nullptr) {
    node->groupBy()->accept(this);
  }

  if (node->having() != nullptr) {
    printChild("HAVING", node->having());
  }

  indent_--;
}

namespace {
std::string toString(ArithmeticBinaryExpression::Operator op) {
  switch (op) {
    case ArithmeticBinaryExpression::Operator::kAdd:
      return "+";
    case ArithmeticBinaryExpression::Operator::kSubtract:
      return "-";
    case ArithmeticBinaryExpression::Operator::kMultiply:
      return "*";
    case ArithmeticBinaryExpression::Operator::kDivide:
      return "/";
    case ArithmeticBinaryExpression::Operator::kModulus:
      return "%";
  }
  throw std::runtime_error("Unsupported arithmetic operator");
}

std::string toString(LogicalBinaryExpression::Operator op) {
  switch (op) {
    case LogicalBinaryExpression::Operator::kAnd:
      return "and";
    case LogicalBinaryExpression::Operator::kOr:
      return "or";
  }

  throw std::runtime_error("Unsupported logical operator");
}

std::string toString(ComparisonExpression::Operator op) {
  switch (op) {
    case ComparisonExpression::Operator::kEqual:
      return "=";
    case ComparisonExpression::Operator::kNotEqual:
      return "!=";
    case ComparisonExpression::Operator::kLessThan:
      return "<";
    case ComparisonExpression::Operator::kLessThanOrEqual:
      return "<=";
    case ComparisonExpression::Operator::kGreaterThan:
      return ">";
    case ComparisonExpression::Operator::kGreaterThanOrEqual:
      return ">=";
    default:
      throw std::runtime_error("Unsupported comparison operator");
  }
}

std::string toString(SortItem::Ordering ordering) {
  switch (ordering) {
    case SortItem::Ordering::kAscending:
      return "ASC";
    case SortItem::Ordering::kDescending:
      return "DESC";
    default:
      throw std::runtime_error("Unsupported sort ordering");
  }
}

std::string toString(SortItem::NullOrdering nullOrdering) {
  switch (nullOrdering) {
    case SortItem::NullOrdering::kFirst:
      return "NULLS FIRST";
    case SortItem::NullOrdering::kLast:
      return "NULLS LAST";
    case SortItem::NullOrdering::kUndefined:
      return "NULLS UNDEFINED";
    default:
      throw std::runtime_error("Unsupported null ordering");
  }
}
} // namespace

void AstPrinter::visitArithmeticBinaryExpression(
    ArithmeticBinaryExpression* node) {
  printHeader("Arithmetic", node, [&](std::ostream& out) {
    out << toString(node->op());
  });

  indent_++;
  printChild("Left", node->left());
  printChild("Right", node->right());
  indent_--;
}

void AstPrinter::visitLogicalBinaryExpression(LogicalBinaryExpression* node) {
  printHeader(
      "Logical", node, [&](std::ostream& out) { out << toString(node->op()); });

  indent_++;
  printChild("Left", node->left());
  printChild("Right", node->right());
  indent_--;
}

void AstPrinter::visitNotExpression(NotExpression* node) {
  printHeader("Not", node);

  indent_++;
  node->value()->accept(this);
  indent_--;
}

void AstPrinter::visitLikePredicate(LikePredicate* node) {
  printHeader("Like", node);

  indent_++;
  printChild("Value", node->value());
  printChild("Pattern", node->pattern());
  if (node->escape()) {
    printChild("Escape", node->escape());
  }
  indent_--;
}

void AstPrinter::visitSelect(Select* node) {
  printHeader("SELECT", node, [&](std::ostream& out) {
    if (node->isDistinct()) {
      out << "DISTINCT";
    };
  });

  indent_++;
  for (const auto& item : node->selectItems()) {
    item->accept(this);
  }
  indent_--;
}

void AstPrinter::visitSingleColumn(SingleColumn* node) {
  printHeader("Column", node, [&](std::ostream& out) {
    if (node->alias() != nullptr) {
      out_ << node->alias()->value();
    }
  });

  indent_++;
  node->expression()->accept(this);
  indent_--;
}

void AstPrinter::visitAliasedRelation(AliasedRelation* node) {
  printHeader("AliasedRelation", node, [&](std::ostream& out) {
    out << node->alias()->value();

    if (!node->columnNames().empty()) {
      out << " => ";
      for (auto i = 0; i < node->columnNames().size(); ++i) {
        if (i > 0) {
          out << ", ";
        }
        out << node->columnNames().at(i)->value();
      }
    }
  });

  indent_++;
  node->relation()->accept(this);
  indent_--;
}

void AstPrinter::visitTable(Table* node) {
  printHeader("FROM", node, [&](std::ostream& out) {
    out << "Table(" << node->name()->suffix() << ")";
  });
}

void AstPrinter::visitTableSubquery(TableSubquery* node) {
  printHeader("TableSubquery", node);

  indent_++;
  node->query()->accept(this);
  indent_--;
}

void AstPrinter::visitIdentifier(Identifier* node) {
  printHeader(
      "Identifier", node, [&](std::ostream& out) { out << node->value(); });
}

void AstPrinter::visitLongLiteral(LongLiteral* node) {
  printHeader("Long", node, [&](std::ostream& out) { out << node->value(); });
}

void AstPrinter::visitStringLiteral(StringLiteral* node) {
  printHeader("String", node, [&](std::ostream& out) { out << node->value(); });
}

void AstPrinter::visitBooleanLiteral(BooleanLiteral* node) {
  printHeader("Boolean", node, [&](std::ostream& out) {
    out << (node->value() ? "true" : "false");
  });
}

void AstPrinter::visitGroupBy(GroupBy* node) {
  printHeader("GROUP BY", node);

  indent_++;
  for (const auto& item : node->groupingElements()) {
    item->accept(this);
  }
  indent_--;
}

void AstPrinter::visitSimpleGroupBy(SimpleGroupBy* node) {
  for (const auto& item : node->expressions()) {
    item->accept(this);
  }
}

void AstPrinter::visitFunctionCall(FunctionCall* node) {
  printHeader("FunctionCall", node, [&](std::ostream& out) {
    out << node->name()->suffix();

    if (node->isDistinct()) {
      out << " (DISTINCT)";
    }
  });

  if (!node->arguments().empty()) {
    indent_++;
    printIndent();
    out_ << "Arguments:\n";
    indent_++;
    for (const auto& arg : node->arguments()) {
      arg->accept(this);
    }
    indent_--;
    indent_--;
  }
}

void AstPrinter::visitOrderBy(OrderBy* node) {
  printHeader("ORDER BY", node);

  indent_++;
  for (const auto& item : node->sortItems()) {
    item->accept(this);
  }
  indent_--;
}

void AstPrinter::visitSortItem(SortItem* node) {
  printHeader("SortItem", node, [&](std::ostream& out) {
    out << toString(node->ordering());
    if (node->nullOrdering() != SortItem::NullOrdering::kUndefined) {
      out << " " << toString(node->nullOrdering());
    }
  });

  indent_++;
  node->sortKey()->accept(this);
  indent_--;
}

void AstPrinter::visitDereferenceExpression(DereferenceExpression* node) {
  printHeader("Dereference", node);

  indent_++;
  node->base()->accept(this);
  printChild("Field", node->field());
  indent_--;
}

void AstPrinter::visitAllColumns(AllColumns* node) {
  printHeader("AllColumns", node, [&](std::ostream& out) { out << "*"; });
}

void AstPrinter::visitJoin(Join* node) {
  printHeader("JOIN", node);

  indent_++;
  printChild("Left", node->left());
  printChild("Right", node->right());
  if (node->criteria() != nullptr) {
    printChild("Criteria", node->criteria());
  }
  indent_--;
}

void AstPrinter::visitJoinOn(JoinOn* node) {
  printHeader("JoinOn", node);

  indent_++;
  node->expression()->accept(this);
  indent_--;
}

void AstPrinter::visitComparisonExpression(ComparisonExpression* node) {
  printHeader("Comparison", node, [&](std::ostream& out) {
    out << toString(node->op());
  });

  indent_++;
  printChild("Left", node->left());
  printChild("Right", node->right());
  indent_--;
}

void AstPrinter::printHeader(
    std::string_view name,
    Node* node,
    const std::function<void(std::ostream& out)>& printDetails) const {
  out_ << std::string(indent_ * 2, ' ') << name << " ";

  out_ << fmt::format(
      "({}:{})", node->location().line, node->location().charPosition);

  out_ << ": ";

  if (printDetails != nullptr) {
    printDetails(out_);
  }

  out_ << std::endl;
}

void AstPrinter::printHeader(
    std::string_view name,
    const std::shared_ptr<Node>& node,
    const std::function<void(std::ostream& out)>& printDetails) const {
  if (node != nullptr) {
    printHeader(name, node.get(), printDetails);
  }
}

void AstPrinter::printChild(
    std::string_view name,
    const std::shared_ptr<Node>& node) {
  printHeader(name, node);
  indent_++;
  node->accept(this);
  indent_--;
}

void AstPrinter::printIndent() {
  out_ << std::string(indent_ * 2, ' ');
}

// Additional Literals
void AstPrinter::visitBinaryLiteral(BinaryLiteral* node) {
  printHeader("Binary", node, [&](std::ostream& out) { out << node->value(); });
}

void AstPrinter::visitCharLiteral(CharLiteral* node) {
  printHeader("Char", node, [&](std::ostream& out) { out << node->value(); });
}

void AstPrinter::visitDoubleLiteral(DoubleLiteral* node) {
  printHeader("Double", node, [&](std::ostream& out) { out << node->value(); });
}

void AstPrinter::visitDecimalLiteral(DecimalLiteral* node) {
  printHeader(
      "Decimal", node, [&](std::ostream& out) { out << node->value(); });
}

void AstPrinter::visitGenericLiteral(GenericLiteral* node) {
  printHeader("Generic", node, [&](std::ostream& out) {
    out << node->type() << "(" << node->value() << ")";
  });
}

void AstPrinter::visitNullLiteral(NullLiteral* node) {
  printHeader("Null", node);
}

void AstPrinter::visitTimeLiteral(TimeLiteral* node) {
  printHeader("Time", node, [&](std::ostream& out) { out << node->value(); });
}

void AstPrinter::visitTimestampLiteral(TimestampLiteral* node) {
  printHeader(
      "Timestamp", node, [&](std::ostream& out) { out << node->value(); });
}

void AstPrinter::visitIntervalLiteral(IntervalLiteral* node) {
  defaultVisit(node);
}

void AstPrinter::visitEnumLiteral(EnumLiteral* node) {
  printHeader("Enum", node, [&](std::ostream& out) { out << node->value(); });
}

// Identifiers and References
void AstPrinter::visitQualifiedName(QualifiedName* node) {
  printHeader(
      "QualifiedName", node, [&](std::ostream& out) { out << node->suffix(); });
}

void AstPrinter::visitFieldReference(FieldReference* node) {
  printHeader("FieldReference", node, [&](std::ostream& out) {
    out << node->fieldIndex();
  });
}

void AstPrinter::visitSymbolReference(SymbolReference* node) {
  printHeader(
      "SymbolReference", node, [&](std::ostream& out) { out << node->name(); });
}

void AstPrinter::visitParameter(Parameter* node) {
  printHeader(
      "Parameter", node, [&](std::ostream& out) { out << node->position(); });
}

// Arithmetic and Comparison Expressions
void AstPrinter::visitArithmeticUnaryExpression(
    ArithmeticUnaryExpression* node) {
  printHeader("ArithmeticUnary", node);

  indent_++;
  node->value()->accept(this);
  indent_--;
}

void AstPrinter::visitBetweenPredicate(BetweenPredicate* node) {
  printHeader("Between", node);

  indent_++;
  printChild("Value", node->value());
  printChild("Min", node->min());
  printChild("Max", node->max());
  indent_--;
}

void AstPrinter::visitInPredicate(InPredicate* node) {
  printHeader("In", node);

  indent_++;
  printChild("Value", node->value());
  printChild("ValueList", node->valueList());
  indent_--;
}

void AstPrinter::visitInListExpression(InListExpression* node) {
  printHeader("InList", node);

  indent_++;
  for (const auto& value : node->values()) {
    value->accept(this);
  }
  indent_--;
}

void AstPrinter::visitIsNullPredicate(IsNullPredicate* node) {
  printHeader("IsNull", node);

  indent_++;
  node->value()->accept(this);
  indent_--;
}

void AstPrinter::visitIsNotNullPredicate(IsNotNullPredicate* node) {
  printHeader("IsNotNull", node);

  indent_++;
  node->value()->accept(this);
  indent_--;
}

void AstPrinter::visitExistsPredicate(ExistsPredicate* node) {
  printHeader("Exists", node);

  indent_++;
  node->subquery()->accept(this);
  indent_--;
}

void AstPrinter::visitQuantifiedComparisonExpression(
    QuantifiedComparisonExpression* node) {
  printHeader("QuantifiedComparison", node);

  indent_++;
  printChild("Value", node->value());
  printChild("Subquery", node->subquery());
  indent_--;
}

// Conditional Expressions
void AstPrinter::visitIfExpression(IfExpression* node) {
  printHeader("If", node);

  indent_++;
  printChild("Condition", node->condition());
  printChild("TrueValue", node->trueValue());
  if (node->falseValue()) {
    printChild("FalseValue", node->falseValue());
  }
  indent_--;
}

void AstPrinter::visitCoalesceExpression(CoalesceExpression* node) {
  printHeader("Coalesce", node);

  indent_++;
  for (const auto& operand : node->operands()) {
    operand->accept(this);
  }
  indent_--;
}

void AstPrinter::visitNullIfExpression(NullIfExpression* node) {
  printHeader("NullIf", node);

  indent_++;
  printChild("First", node->first());
  printChild("Second", node->second());
  indent_--;
}

void AstPrinter::visitWhenClause(WhenClause* node) {
  printHeader("When", node);

  indent_++;
  printChild("Operand", node->operand());
  printChild("Result", node->result());
  indent_--;
}

void AstPrinter::visitSearchedCaseExpression(SearchedCaseExpression* node) {
  printHeader("SearchedCase", node);

  indent_++;
  for (const auto& whenClause : node->whenClauses()) {
    whenClause->accept(this);
  }
  if (node->defaultValue()) {
    printChild("Default", node->defaultValue());
  }
  indent_--;
}

void AstPrinter::visitSimpleCaseExpression(SimpleCaseExpression* node) {
  printHeader("SimpleCase", node);

  indent_++;
  printChild("Operand", node->operand());
  for (const auto& whenClause : node->whenClauses()) {
    whenClause->accept(this);
  }
  if (node->defaultValue()) {
    printChild("Default", node->defaultValue());
  }
  indent_--;
}

void AstPrinter::visitTryExpression(TryExpression* node) {
  printHeader("Try", node);

  indent_++;
  node->innerExpression()->accept(this);
  indent_--;
}

// Function and Call Expressions
void AstPrinter::visitCast(Cast* node) {
  defaultVisit(node);
}

void AstPrinter::visitExtract(Extract* node) {
  printHeader("Extract", node);

  indent_++;
  printChild("Expression", node->expression());
  indent_--;
}

void AstPrinter::visitCurrentTime(CurrentTime* node) {
  printHeader("CurrentTime", node);
}

void AstPrinter::visitCurrentUser(CurrentUser* node) {
  printHeader("CurrentUser", node);
}

void AstPrinter::visitAtTimeZone(AtTimeZone* node) {
  printHeader("AtTimeZone", node);

  indent_++;
  printChild("Value", node->value());
  printChild("TimeZone", node->timeZone());
  indent_--;
}

// Complex Expressions
void AstPrinter::visitSubqueryExpression(SubqueryExpression* node) {
  printHeader("Subquery", node);

  indent_++;
  node->query()->accept(this);
  indent_--;
}

void AstPrinter::visitArrayConstructor(ArrayConstructor* node) {
  printHeader("Array", node);

  indent_++;
  for (const auto& value : node->values()) {
    value->accept(this);
  }
  indent_--;
}

void AstPrinter::visitRow(Row* node) {
  printHeader("Row", node);

  indent_++;
  for (const auto& item : node->items()) {
    item->accept(this);
  }
  indent_--;
}

void AstPrinter::visitSubscriptExpression(SubscriptExpression* node) {
  printHeader("Subscript", node);

  indent_++;
  printChild("Base", node->base());
  printChild("Index", node->index());
  indent_--;
}

void AstPrinter::visitLambdaExpression(LambdaExpression* node) {
  printHeader("Lambda", node);

  indent_++;
  for (const auto& arg : node->arguments()) {
    arg->accept(this);
  }
  printChild("Body", node->body());
  indent_--;
}

void AstPrinter::visitLambdaArgumentDeclaration(
    LambdaArgumentDeclaration* node) {
  printHeader("LambdaArgument", node, [&](std::ostream& out) {
    out << node->name()->value();
  });
}

void AstPrinter::visitBindExpression(BindExpression* node) {
  printHeader("Bind", node);

  indent_++;
  for (const auto& value : node->values()) {
    value->accept(this);
  }
  printChild("Function", node->function());
  indent_--;
}

void AstPrinter::visitGroupingOperation(GroupingOperation* node) {
  printHeader("Grouping", node);

  indent_++;
  for (const auto& groupingColumn : node->groupingColumns()) {
    groupingColumn->accept(this);
  }
  indent_--;
}

void AstPrinter::visitTableVersionExpression(TableVersionExpression* node) {
  defaultVisit(node);
}

// Query structures
void AstPrinter::visitWith(With* node) {
  printHeader("With", node);

  indent_++;
  for (const auto& query : node->queries()) {
    query->accept(this);
  }
  indent_--;
}

void AstPrinter::visitWithQuery(WithQuery* node) {
  printHeader("WithQuery", node, [&](std::ostream& out) {
    out << node->name()->value();
  });

  indent_++;
  printChild("Query", node->query());
  indent_--;
}

void AstPrinter::visitGroupingSets(GroupingSets* node) {
  defaultVisit(node);
}

void AstPrinter::visitCube(Cube* node) {
  defaultVisit(node);
}

void AstPrinter::visitRollup(Rollup* node) {
  printHeader("Rollup", node);

  indent_++;
  for (const auto& expression : node->expressions()) {
    expression->accept(this);
  }
  indent_--;
}

void AstPrinter::visitOffset(Offset* node) {
  printHeader(
      "Offset", node, [&](std::ostream& out) { out << node->offset(); });
}

// Relations
void AstPrinter::visitSampledRelation(SampledRelation* node) {
  printHeader("SampledRelation", node);

  indent_++;
  printChild("Relation", node->relation());
  indent_--;
}

void AstPrinter::visitLateral(Lateral* node) {
  printHeader("Lateral", node);

  indent_++;
  printChild("Query", node->query());
  indent_--;
}

void AstPrinter::visitUnnest(Unnest* node) {
  printHeader("Unnest", node, [&](std::ostream& out) {
    if (node->isWithOrdinality()) {
      out << "with ordinality";
    }
  });

  indent_++;
  for (const auto& expression : node->expressions()) {
    expression->accept(this);
  }
  indent_--;
}

void AstPrinter::visitValues(Values* node) {
  printHeader("Values", node);

  indent_++;
  for (const auto& row : node->rows()) {
    row->accept(this);
  }
  indent_--;
}

// Joins
void AstPrinter::visitJoinUsing(JoinUsing* node) {
  printHeader("JoinUsing", node);

  indent_++;
  for (const auto& column : node->columns()) {
    column->accept(this);
  }
  indent_--;
}

void AstPrinter::visitNaturalJoin(NaturalJoin* node) {
  printHeader("NaturalJoin", node);
}

// Set Operations
void AstPrinter::visitUnion(Union* node) {
  defaultVisit(node);
}

void AstPrinter::visitIntersect(Intersect* node) {
  defaultVisit(node);
}

void AstPrinter::visitExcept(Except* node) {
  printHeader("Except", node);

  indent_++;
  printChild("Left", node->left());
  printChild("Right", node->right());
  indent_--;
}

// DDL Statements
void AstPrinter::visitCreateTable(CreateTable* node) {
  printHeader("CreateTable", node, [&](std::ostream& out) {
    out << node->name()->suffix();
  });

  indent_++;
  for (const auto& element : node->elements()) {
    element->accept(this);
  }
  indent_--;
}

void AstPrinter::visitCreateTableAsSelect(CreateTableAsSelect* node) {
  printHeader("CreateTableAsSelect", node, [&](std::ostream& out) {
    out << node->name()->suffix();
  });

  indent_++;
  printChild("Query", node->query());
  indent_--;
}

void AstPrinter::visitCreateView(CreateView* node) {
  printHeader("CreateView", node, [&](std::ostream& out) {
    out << node->name()->suffix();
  });

  indent_++;
  printChild("Query", node->query());
  indent_--;
}

void AstPrinter::visitCreateMaterializedView(CreateMaterializedView* node) {
  printHeader("CreateMaterializedView", node, [&](std::ostream& out) {
    out << node->name()->suffix();
  });

  indent_++;
  printChild("Query", node->query());
  indent_--;
}

void AstPrinter::visitCreateSchema(CreateSchema* node) {
  printHeader("CreateSchema", node, [&](std::ostream& out) {
    out << node->schemaName()->suffix();
  });
}

void AstPrinter::visitCreateFunction(CreateFunction* node) {
  printHeader("CreateFunction", node, [&](std::ostream& out) {
    out << node->functionName()->suffix();
  });

  indent_++;
  for (const auto& param : node->parameters()) {
    param->accept(this);
  }
  printChild("Body", node->body());
  indent_--;
}

void AstPrinter::visitCreateRole(CreateRole* node) {
  defaultVisit(node);
}

void AstPrinter::visitCreateType(CreateType* node) {
  printHeader("CreateType", node, [&](std::ostream& out) {
    out << node->name()->suffix();
  });
}

void AstPrinter::visitDropTable(DropTable* node) {
  printHeader("DropTable", node, [&](std::ostream& out) {
    out << node->tableName()->suffix();
  });
}

void AstPrinter::visitDropView(DropView* node) {
  defaultVisit(node);
}

void AstPrinter::visitDropMaterializedView(DropMaterializedView* node) {
  defaultVisit(node);
}

void AstPrinter::visitDropSchema(DropSchema* node) {
  printHeader("DropSchema", node, [&](std::ostream& out) {
    out << node->schemaName()->suffix();
  });
}

// DML Statements
void AstPrinter::visitInsert(Insert* node) {
  printHeader("Insert", node, [&](std::ostream& out) {
    out << node->target()->suffix();
  });

  indent_++;
  printChild("Query", node->query());
  indent_--;
}

void AstPrinter::visitUpdateAssignment(UpdateAssignment* node) {
  printHeader("UpdateAssignment", node, [&](std::ostream& out) {
    out << node->name()->value();
  });

  indent_++;
  printChild("Value", node->value());
  indent_--;
}

void AstPrinter::visitUpdate(Update* node) {
  printHeader("Update", node, [&](std::ostream& out) {
    out << node->table()->suffix();
  });

  indent_++;
  for (const auto& assignment : node->assignments()) {
    assignment->accept(this);
  }
  if (node->where()) {
    printChild("Where", node->where());
  }
  indent_--;
}

void AstPrinter::visitDelete(Delete* node) {
  printHeader("Delete", node);

  indent_++;
  printChild("Table", node->table());
  if (node->where()) {
    printChild("Where", node->where());
  }
  indent_--;
}

// Utility Statements
void AstPrinter::visitExplain(Explain* node) {
  printHeader("Explain", node, [&](std::ostream& out) {
    if (node->isAnalyze()) {
      out << "analyze ";
    }

    if (node->isVerbose()) {
      out << "verbose";
    }
  });

  indent_++;
  printChild("Statement", node->statement());
  indent_--;
}

void AstPrinter::visitAnalyze(Analyze* node) {
  printHeader("Analyze", node, [&](std::ostream& out) {
    out << node->tableName()->suffix();
  });
}

void AstPrinter::visitCall(Call* node) {
  printHeader(
      "Call", node, [&](std::ostream& out) { out << node->name()->suffix(); });

  indent_++;
  for (const auto& argument : node->arguments()) {
    argument->accept(this);
  }
  indent_--;
}

// Transaction Statements
void AstPrinter::visitStartTransaction(StartTransaction* node) {
  printHeader("StartTransaction", node);

  indent_++;
  for (const auto& mode : node->transactionModes()) {
    mode->accept(this);
  }
  indent_--;
}

void AstPrinter::visitCommit(Commit* node) {
  printHeader("Commit", node);
}

void AstPrinter::visitRollback(Rollback* node) {
  printHeader("Rollback", node);
}

// Table Elements
void AstPrinter::visitColumnDefinition(ColumnDefinition* node) {
  defaultVisit(node);
}

void AstPrinter::visitLikeClause(LikeClause* node) {
  printHeader("LikeClause", node, [&](std::ostream& out) {
    out << node->tableName()->suffix();
  });
}

void AstPrinter::visitConstraintSpecification(ConstraintSpecification* node) {
  printHeader("ConstraintSpecification", node);
}

// Support Classes
void AstPrinter::visitTypeSignature(TypeSignature* node) {
  defaultVisit(node);
}

void AstPrinter::visitProperty(Property* node) {
  printHeader("Property", node, [&](std::ostream& out) {
    out << node->name()->value();
  });

  indent_++;
  printChild("Value", node->value());
  indent_--;
}

void AstPrinter::visitCallArgument(CallArgument* node) {
  printHeader("CallArgument", node);

  indent_++;
  printChild("Value", node->value());
  indent_--;
}

void AstPrinter::visitWindow(Window* node) {
  defaultVisit(node);
}

void AstPrinter::visitWindowFrame(WindowFrame* node) {
  printHeader("WindowFrame", node);

  indent_++;
  if (node->start()) {
    printChild("Start", node->start());
  }
  if (node->end()) {
    printChild("End", node->end());
  }
  indent_--;
}

void AstPrinter::visitFrameBound(FrameBound* node) {
  printHeader("FrameBound", node);
}

void AstPrinter::visitPrincipalSpecification(PrincipalSpecification* node) {
  printHeader("PrincipalSpecification", node);
}

void AstPrinter::visitGrantorSpecification(GrantorSpecification* node) {
  printHeader("GrantorSpecification", node);
}

void AstPrinter::visitIsolation(Isolation* node) {
  printHeader("Isolation", node);
}

void AstPrinter::visitTransactionAccessMode(TransactionAccessMode* node) {
  printHeader("TransactionAccessMode", node);
}

void AstPrinter::visitSqlParameterDeclaration(SqlParameterDeclaration* node) {
  defaultVisit(node);
}

void AstPrinter::visitRoutineCharacteristics(RoutineCharacteristics* node) {
  printHeader("RoutineCharacteristics", node);
}

void AstPrinter::visitExternalBodyReference(ExternalBodyReference* node) {
  defaultVisit(node);
}

void AstPrinter::visitReturn(Return* node) {
  defaultVisit(node);
}

void AstPrinter::visitExplainFormat(ExplainFormat* node) {
  printHeader("ExplainFormat", node);
}

void AstPrinter::visitExplainType(ExplainType* node) {
  printHeader("ExplainType", node);
}

void AstPrinter::visitShowColumns(ShowColumns* node) {
  printHeader("ShowColumns", node);
}

} // namespace axiom::sql::presto
