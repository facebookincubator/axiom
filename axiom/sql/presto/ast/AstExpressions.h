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

#include <vector>
#include "axiom/sql/presto/ast/AstNode.h"

namespace axiom::sql::presto {

// Forward declarations
class QualifiedName;

// Identifiers and References
class Identifier : public Expression {
 public:
  explicit Identifier(
      NodeLocation location,
      const std::string& value,
      bool delimited)
      : Expression(NodeType::kIdentifier, location),
        value_(value),
        delimited_(delimited) {}

  const std::string& value() const {
    return value_;
  }

  bool isDelimited() const {
    return delimited_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::string value_;
  bool delimited_;
};

using IdentifierPtr = std::shared_ptr<Identifier>;

class QualifiedName : public Expression {
 public:
  explicit QualifiedName(
      NodeLocation location,
      const std::vector<std::string>& parts)
      : Expression(NodeType::kQualifiedName, location), parts_(parts) {}

  const std::vector<std::string>& parts() const {
    return parts_;
  }

  std::string suffix() const {
    return parts_.empty() ? "" : parts_.back();
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::vector<std::string> parts_;
};

using QualifiedNamePtr = std::shared_ptr<QualifiedName>;

class DereferenceExpression : public Expression {
 public:
  DereferenceExpression(
      NodeLocation location,
      const ExpressionPtr& base,
      const std::shared_ptr<Identifier>& field)
      : Expression(NodeType::kDereferenceExpression, location),
        base_(base),
        field_(field) {}

  const ExpressionPtr& base() const {
    return base_;
  }

  const std::shared_ptr<Identifier>& field() const {
    return field_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  ExpressionPtr base_;
  std::shared_ptr<Identifier> field_;
};

class FieldReference : public Expression {
 public:
  explicit FieldReference(NodeLocation location, int fieldIndex)
      : Expression(NodeType::kFieldReference, location),
        fieldIndex_(fieldIndex) {}

  int fieldIndex() const {
    return fieldIndex_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  int fieldIndex_;
};

class SymbolReference : public Expression {
 public:
  explicit SymbolReference(NodeLocation location, const std::string& name)
      : Expression(NodeType::kSymbolReference, location), name_(name) {}

  const std::string& name() const {
    return name_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::string name_;
};

class Parameter : public Expression {
 public:
  explicit Parameter(NodeLocation location, int position)
      : Expression(NodeType::kParameter, location), position_(position) {}

  int position() const {
    return position_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  int position_;
};

// Arithmetic Expressions
class ArithmeticBinaryExpression : public Expression {
 public:
  enum class Operator { kAdd, kSubtract, kMultiply, kDivide, kModulus };

  ArithmeticBinaryExpression(
      NodeLocation location,
      Operator op,
      const ExpressionPtr& left,
      const ExpressionPtr& right)
      : Expression(NodeType::kArithmeticBinaryExpression, location),
        operator_(op),
        left_(left),
        right_(right) {}

  Operator op() const {
    return operator_;
  }

  const ExpressionPtr& left() const {
    return left_;
  }

  const ExpressionPtr& right() const {
    return right_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  Operator operator_;
  ExpressionPtr left_;
  ExpressionPtr right_;
};

class ArithmeticUnaryExpression : public Expression {
 public:
  enum class Sign { kPlus, kMinus };

  ArithmeticUnaryExpression(
      NodeLocation location,
      Sign sign,
      const ExpressionPtr& value)
      : Expression(NodeType::kArithmeticUnaryExpression, location),
        sign_(sign),
        value_(value) {}

  Sign sign() const {
    return sign_;
  }

  const ExpressionPtr& value() const {
    return value_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  Sign sign_;
  ExpressionPtr value_;
};

// Comparison Expressions
class ComparisonExpression : public Expression {
 public:
  enum class Operator {
    kEqual,
    kNotEqual,
    kLessThan,
    kLessThanOrEqual,
    kGreaterThan,
    kGreaterThanOrEqual,
    kIsDistinctFrom
  };

  ComparisonExpression(
      NodeLocation location,
      Operator op,
      const ExpressionPtr& left,
      const ExpressionPtr& right)
      : Expression(NodeType::kComparisonExpression, location),
        operator_(op),
        left_(left),
        right_(right) {}

  Operator op() const {
    return operator_;
  }

  const ExpressionPtr& left() const {
    return left_;
  }

  const ExpressionPtr& right() const {
    return right_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  Operator operator_;
  ExpressionPtr left_;
  ExpressionPtr right_;
};

class BetweenPredicate : public Expression {
 public:
  BetweenPredicate(
      NodeLocation location,
      const ExpressionPtr& value,
      const ExpressionPtr& min,
      const ExpressionPtr& max)
      : Expression(NodeType::kBetweenPredicate, location),
        value_(value),
        min_(min),
        max_(max) {}

  const ExpressionPtr& value() const {
    return value_;
  }

  const ExpressionPtr& min() const {
    return min_;
  }

  const ExpressionPtr& max() const {
    return max_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  ExpressionPtr value_;
  ExpressionPtr min_;
  ExpressionPtr max_;
};

class InPredicate : public Expression {
 public:
  InPredicate(
      NodeLocation location,
      const ExpressionPtr& value,
      const ExpressionPtr& valueList)
      : Expression(NodeType::kInPredicate, location),
        value_(value),
        valueList_(valueList) {}

  const ExpressionPtr& value() const {
    return value_;
  }

  const ExpressionPtr& valueList() const {
    return valueList_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  ExpressionPtr value_;
  ExpressionPtr valueList_;
};

class InListExpression : public Expression {
 public:
  explicit InListExpression(
      NodeLocation location,
      const std::vector<ExpressionPtr>& values)
      : Expression(NodeType::kInListExpression, location), values_(values) {}

  const std::vector<ExpressionPtr>& values() const {
    return values_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::vector<ExpressionPtr> values_;
};

class IsNullPredicate : public Expression {
 public:
  explicit IsNullPredicate(NodeLocation location, const ExpressionPtr& value)
      : Expression(NodeType::kIsNullPredicate, location), value_(value) {}

  const ExpressionPtr& value() const {
    return value_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  ExpressionPtr value_;
};

class IsNotNullPredicate : public Expression {
 public:
  explicit IsNotNullPredicate(NodeLocation location, const ExpressionPtr& value)
      : Expression(NodeType::kIsNotNullPredicate, location), value_(value) {}

  const ExpressionPtr& value() const {
    return value_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  ExpressionPtr value_;
};

class LikePredicate : public Expression {
 public:
  LikePredicate(
      NodeLocation location,
      const ExpressionPtr& value,
      const ExpressionPtr& pattern,
      const ExpressionPtr& escape = nullptr)
      : Expression(NodeType::kLikePredicate, location),
        value_(value),
        pattern_(pattern),
        escape_(escape) {}

  const ExpressionPtr& value() const {
    return value_;
  }

  const ExpressionPtr& pattern() const {
    return pattern_;
  }

  const ExpressionPtr& escape() const {
    return escape_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  ExpressionPtr value_;
  ExpressionPtr pattern_;
  ExpressionPtr escape_;
};

class ExistsPredicate : public Expression {
 public:
  explicit ExistsPredicate(NodeLocation location, const ExpressionPtr& subquery)
      : Expression(NodeType::kExistsPredicate, location), subquery_(subquery) {}

  const ExpressionPtr& subquery() const {
    return subquery_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  ExpressionPtr subquery_;
};

class QuantifiedComparisonExpression : public Expression {
 public:
  enum class Quantifier { kAll, kAny, kSome };

  QuantifiedComparisonExpression(
      NodeLocation location,
      ComparisonExpression::Operator operator_,
      Quantifier quantifier,
      const ExpressionPtr& value,
      const ExpressionPtr& subquery)
      : Expression(NodeType::kQuantifiedComparisonExpression, location),
        operator_(operator_),
        quantifier_(quantifier),
        value_(value),
        subquery_(subquery) {}

  ComparisonExpression::Operator op() const {
    return operator_;
  }

  Quantifier quantifier() const {
    return quantifier_;
  }

  const ExpressionPtr& value() const {
    return value_;
  }

  const ExpressionPtr& subquery() const {
    return subquery_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  ComparisonExpression::Operator operator_;
  Quantifier quantifier_;
  ExpressionPtr value_;
  ExpressionPtr subquery_;
};

// Logical Expressions
class LogicalBinaryExpression : public Expression {
 public:
  enum class Operator { kAnd, kOr };

  LogicalBinaryExpression(
      NodeLocation location,
      Operator op,
      const ExpressionPtr& left,
      const ExpressionPtr& right)
      : Expression(NodeType::kLogicalBinaryExpression, location),
        operator_(op),
        left_(left),
        right_(right) {}

  Operator op() const {
    return operator_;
  }

  const ExpressionPtr& left() const {
    return left_;
  }

  const ExpressionPtr& right() const {
    return right_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  Operator operator_;
  ExpressionPtr left_;
  ExpressionPtr right_;
};

class NotExpression : public Expression {
 public:
  explicit NotExpression(NodeLocation location, const ExpressionPtr& value)
      : Expression(NodeType::kNotExpression, location), value_(value) {}

  const ExpressionPtr& value() const {
    return value_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  ExpressionPtr value_;
};

} // namespace axiom::sql::presto
