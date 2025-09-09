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

#include <optional>
#include <vector>
#include "axiom/sql/presto/ast/AstNode.h"

namespace axiom::sql::presto {

// Forward declarations
class Identifier;
class QualifiedName;
class Window;
class OrderBy;

class TypeSignature;
using TypeSignaturePtr = std::shared_ptr<TypeSignature>;

// Conditional Expressions
class IfExpression : public Expression {
 public:
  IfExpression(
      NodeLocation location,
      const ExpressionPtr& condition,
      const ExpressionPtr& trueValue,
      const ExpressionPtr& falseValue = nullptr)
      : Expression(NodeType::kIfExpression, location),
        condition_(condition),
        trueValue_(trueValue),
        falseValue_(falseValue) {}

  const ExpressionPtr& condition() const {
    return condition_;
  }

  const ExpressionPtr& trueValue() const {
    return trueValue_;
  }

  const ExpressionPtr& falseValue() const {
    return falseValue_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  ExpressionPtr condition_;
  ExpressionPtr trueValue_;
  ExpressionPtr falseValue_;
};

class CoalesceExpression : public Expression {
 public:
  explicit CoalesceExpression(
      NodeLocation location,
      const std::vector<ExpressionPtr>& operands)
      : Expression(NodeType::kCoalesceExpression, location),
        operands_(operands) {}

  const std::vector<ExpressionPtr>& operands() const {
    return operands_;
  }
  void accept(AstVisitor* visitor) override;

 private:
  std::vector<ExpressionPtr> operands_;
};

class NullIfExpression : public Expression {
 public:
  NullIfExpression(
      NodeLocation location,
      const ExpressionPtr& first,
      const ExpressionPtr& second)
      : Expression(NodeType::kNullIfExpression, location),
        first_(first),
        second_(second) {}

  const ExpressionPtr& first() const {
    return first_;
  }

  const ExpressionPtr& second() const {
    return second_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  ExpressionPtr first_;
  ExpressionPtr second_;
};

class WhenClause : public Node {
 public:
  WhenClause(
      NodeLocation location,
      const ExpressionPtr& operand,
      const ExpressionPtr& result)
      : Node(NodeType::kWhenClause, location),
        operand_(operand),
        result_(result) {}

  const ExpressionPtr& operand() const {
    return operand_;
  }

  const ExpressionPtr& result() const {
    return result_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  ExpressionPtr operand_;
  ExpressionPtr result_;
};

class SearchedCaseExpression : public Expression {
 public:
  SearchedCaseExpression(
      NodeLocation location,
      const std::vector<std::shared_ptr<WhenClause>>& whenClauses,
      const ExpressionPtr& defaultValue = nullptr)
      : Expression(NodeType::kSearchedCaseExpression, location),
        whenClauses_(whenClauses),
        defaultValue_(defaultValue) {}

  const std::vector<std::shared_ptr<WhenClause>>& whenClauses() const {
    return whenClauses_;
  }

  const ExpressionPtr& defaultValue() const {
    return defaultValue_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::vector<std::shared_ptr<WhenClause>> whenClauses_;
  ExpressionPtr defaultValue_;
};

class SimpleCaseExpression : public Expression {
 public:
  SimpleCaseExpression(
      NodeLocation location,
      const ExpressionPtr& operand,
      const std::vector<std::shared_ptr<WhenClause>>& whenClauses,
      const ExpressionPtr& defaultValue = nullptr)
      : Expression(NodeType::kSimpleCaseExpression, location),
        operand_(operand),
        whenClauses_(whenClauses),
        defaultValue_(defaultValue) {}

  const ExpressionPtr& operand() const {
    return operand_;
  }

  const std::vector<std::shared_ptr<WhenClause>>& whenClauses() const {
    return whenClauses_;
  }

  const ExpressionPtr& defaultValue() const {
    return defaultValue_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  ExpressionPtr operand_;
  std::vector<std::shared_ptr<WhenClause>> whenClauses_;
  ExpressionPtr defaultValue_;
};

class TryExpression : public Expression {
 public:
  explicit TryExpression(
      NodeLocation location,
      const ExpressionPtr& innerExpression)
      : Expression(NodeType::kTryExpression, location),
        innerExpression_(innerExpression) {}

  const ExpressionPtr& innerExpression() const {
    return innerExpression_;
  }
  void accept(AstVisitor* visitor) override;

 private:
  ExpressionPtr innerExpression_;
};

// Function and Call Expressions
class FunctionCall : public Expression {
 public:
  FunctionCall(
      NodeLocation location,
      const std::shared_ptr<QualifiedName>& name,
      const std::shared_ptr<Window>& window,
      bool distinct,
      const std::vector<ExpressionPtr>& arguments)
      : Expression(NodeType::kFunctionCall, location),
        name_(name),
        window_(window),
        distinct_(distinct),
        arguments_(arguments) {}

  const std::shared_ptr<QualifiedName>& name() const {
    return name_;
  }

  const std::shared_ptr<Window>& window() const {
    return window_;
  }

  bool isDistinct() const {
    return distinct_;
  }

  const std::vector<ExpressionPtr>& arguments() const {
    return arguments_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<QualifiedName> name_;
  std::shared_ptr<Window> window_;
  bool distinct_;
  std::vector<ExpressionPtr> arguments_;
};

class Cast : public Expression {
 public:
  Cast(
      NodeLocation location,
      const ExpressionPtr& expression,
      const TypeSignaturePtr& type,
      bool safe = false)
      : Expression(NodeType::kCast, location),
        expression_(expression),
        toType_(type),
        safe_(safe) {}

  const ExpressionPtr& expression() const {
    return expression_;
  }

  const TypeSignaturePtr& toType() const {
    return toType_;
  }

  bool isSafe() const {
    return safe_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  ExpressionPtr expression_;
  TypeSignaturePtr toType_;
  bool safe_;
};

class Extract : public Expression {
 public:
  enum class Field {
    kYear,
    kQuarter,
    kMonth,
    kWeek,
    kDay,
    kDayOfMonth,
    kDayOfWeek,
    kDow,
    kDayOfYear,
    kDoy,
    kYearOfWeek,
    kYow,
    kHour,
    kMinute,
    kSecond,
    kTimezoneHour,
    kTimezoneMinute
  };

  Extract(NodeLocation location, const ExpressionPtr& expression, Field field)
      : Expression(NodeType::kExtract, location),
        expression_(expression),
        field_(field) {}

  const ExpressionPtr& expression() const {
    return expression_;
  }

  Field field() const {
    return field_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  ExpressionPtr expression_;
  Field field_;
};

class CurrentTime : public Expression {
 public:
  enum class Function { kTime, kDate, kTimestamp, kLocaltime, kLocaltimestamp };

  CurrentTime(
      NodeLocation location,
      Function function,
      std::optional<int> precision = std::nullopt)
      : Expression(NodeType::kCurrentTime, location),
        function_(function),
        precision_(precision) {}

  Function function() const {
    return function_;
  }

  const std::optional<int>& precision() const {
    return precision_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  Function function_;
  std::optional<int> precision_;
};

class CurrentUser : public Expression {
 public:
  explicit CurrentUser(NodeLocation location)
      : Expression(NodeType::kCurrentUser, location) {}
  void accept(AstVisitor* visitor) override;
};

class AtTimeZone : public Expression {
 public:
  AtTimeZone(
      NodeLocation location,
      const ExpressionPtr& value,
      const ExpressionPtr& timeZone)
      : Expression(NodeType::kAtTimeZone, location),
        value_(value),
        timeZone_(timeZone) {}

  const ExpressionPtr& value() const {
    return value_;
  }

  const ExpressionPtr& timeZone() const {
    return timeZone_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  ExpressionPtr value_;
  ExpressionPtr timeZone_;
};

// Complex Expressions
class SubqueryExpression : public Expression {
 public:
  explicit SubqueryExpression(NodeLocation location, const StatementPtr& query)
      : Expression(NodeType::kSubqueryExpression, location), query_(query) {}

  const StatementPtr& query() const {
    return query_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  StatementPtr query_;
};

class ArrayConstructor : public Expression {
 public:
  explicit ArrayConstructor(
      NodeLocation location,
      const std::vector<ExpressionPtr>& values)
      : Expression(NodeType::kArrayConstructor, location), values_(values) {}

  const std::vector<ExpressionPtr>& values() const {
    return values_;
  }
  void accept(AstVisitor* visitor) override;

 private:
  std::vector<ExpressionPtr> values_;
};

class Row : public Expression {
 public:
  explicit Row(NodeLocation location, const std::vector<ExpressionPtr>& items)
      : Expression(NodeType::kRow, location), items_(items) {}

  const std::vector<ExpressionPtr>& items() const {
    return items_;
  }
  void accept(AstVisitor* visitor) override;

 private:
  std::vector<ExpressionPtr> items_;
};

class SubscriptExpression : public Expression {
 public:
  SubscriptExpression(
      NodeLocation location,
      const ExpressionPtr& base,
      const ExpressionPtr& index)
      : Expression(NodeType::kSubscriptExpression, location),
        base_(base),
        index_(index) {}

  const ExpressionPtr& base() const {
    return base_;
  }

  const ExpressionPtr& index() const {
    return index_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  ExpressionPtr base_;
  ExpressionPtr index_;
};

class LambdaArgumentDeclaration : public Node {
 public:
  explicit LambdaArgumentDeclaration(
      NodeLocation location,
      const std::shared_ptr<Identifier>& name)
      : Node(NodeType::kLambdaArgumentDeclaration, location), name_(name) {}

  const std::shared_ptr<Identifier>& name() const {
    return name_;
  }
  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<Identifier> name_;
};

class LambdaExpression : public Expression {
 public:
  LambdaExpression(
      NodeLocation location,
      const std::vector<std::shared_ptr<LambdaArgumentDeclaration>>& arguments,
      const ExpressionPtr& body)
      : Expression(NodeType::kLambdaExpression, location),
        arguments_(arguments),
        body_(body) {}

  const std::vector<std::shared_ptr<LambdaArgumentDeclaration>>& arguments()
      const {
    return arguments_;
  }

  const ExpressionPtr& body() const {
    return body_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::vector<std::shared_ptr<LambdaArgumentDeclaration>> arguments_;
  ExpressionPtr body_;
};

class BindExpression : public Expression {
 public:
  BindExpression(
      NodeLocation location,
      const std::vector<ExpressionPtr>& values,
      const ExpressionPtr& function)
      : Expression(NodeType::kBindExpression, location),
        values_(values),
        function_(function) {}

  const std::vector<ExpressionPtr>& values() const {
    return values_;
  }

  const ExpressionPtr& function() const {
    return function_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::vector<ExpressionPtr> values_;
  ExpressionPtr function_;
};

class GroupingOperation : public Expression {
 public:
  explicit GroupingOperation(
      NodeLocation location,
      const std::vector<std::shared_ptr<QualifiedName>>& groupingColumns)
      : Expression(NodeType::kGroupingOperation, location),
        groupingColumns_(groupingColumns) {}

  const std::vector<std::shared_ptr<QualifiedName>>& groupingColumns() const {
    return groupingColumns_;
  }
  void accept(AstVisitor* visitor) override;

 private:
  std::vector<std::shared_ptr<QualifiedName>> groupingColumns_;
};

class TableVersionExpression : public Expression {
 public:
  enum class TableVersionType { kTimestamp, kVersion };

  TableVersionExpression(
      NodeLocation location,
      TableVersionType type,
      const ExpressionPtr& expression)
      : Expression(NodeType::kTableVersionExpression, location),
        type_(type),
        expression_(expression) {}

  TableVersionType tableVersionType() const {
    return type_;
  }

  const ExpressionPtr& expression() const {
    return expression_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  TableVersionType type_;
  ExpressionPtr expression_;
};

} // namespace axiom::sql::presto
