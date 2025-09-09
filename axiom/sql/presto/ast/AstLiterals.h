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
#include <string>
#include "axiom/sql/presto/ast/AstNode.h"

namespace axiom::sql::presto {

class TypeSignature;
using TypeSignaturePtr = std::shared_ptr<TypeSignature>;

class Literal : public Expression {
 public:
  explicit Literal(NodeType type, NodeLocation location)
      : Expression(type, location) {}
};

using LiteralPtr = std::shared_ptr<Literal>;

class BooleanLiteral : public Literal {
 public:
  explicit BooleanLiteral(NodeLocation location, bool value)
      : Literal(NodeType::kBooleanLiteral, location), value_(value) {}

  bool value() const {
    return value_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  bool value_;
};

class StringLiteral : public Literal {
 public:
  explicit StringLiteral(NodeLocation location, const std::string& value)
      : Literal(NodeType::kStringLiteral, location), value_(value) {}

  const std::string& value() const {
    return value_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::string value_;
};

class BinaryLiteral : public Literal {
 public:
  explicit BinaryLiteral(NodeLocation location, const std::string& value)
      : Literal(NodeType::kBinaryLiteral, location), value_(value) {}

  const std::string& value() const {
    return value_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::string value_;
};

class CharLiteral : public Literal {
 public:
  explicit CharLiteral(NodeLocation location, const std::string& value)
      : Literal(NodeType::kCharLiteral, location), value_(value) {}

  const std::string& value() const {
    return value_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::string value_;
};

class LongLiteral : public Literal {
 public:
  explicit LongLiteral(NodeLocation location, int64_t value)
      : Literal(NodeType::kLongLiteral, location), value_(value) {}

  int64_t value() const {
    return value_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  int64_t value_;
};

class DoubleLiteral : public Literal {
 public:
  explicit DoubleLiteral(NodeLocation location, double value)
      : Literal(NodeType::kDoubleLiteral, location), value_(value) {}

  double value() const {
    return value_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  double value_;
};

class DecimalLiteral : public Literal {
 public:
  explicit DecimalLiteral(NodeLocation location, const std::string& value)
      : Literal(NodeType::kDecimalLiteral, location), value_(value) {}

  const std::string& value() const {
    return value_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::string value_;
};

class GenericLiteral : public Literal {
 public:
  GenericLiteral(
      NodeLocation location,
      const TypeSignaturePtr& valueType,
      const std::string& value)
      : Literal(NodeType::kGenericLiteral, location),
        valueType_(valueType),
        value_(value) {}

  const TypeSignaturePtr& valueType() const {
    return valueType_;
  }

  const std::string& value() const {
    return value_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  TypeSignaturePtr valueType_;
  std::string value_;
};

class NullLiteral : public Literal {
 public:
  explicit NullLiteral(NodeLocation location)
      : Literal(NodeType::kNullLiteral, location) {}
  void accept(AstVisitor* visitor) override;
};

class TimeLiteral : public Literal {
 public:
  explicit TimeLiteral(NodeLocation location, const std::string& value)
      : Literal(NodeType::kTimeLiteral, location), value_(value) {}

  const std::string& value() const {
    return value_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::string value_;
};

class TimestampLiteral : public Literal {
 public:
  explicit TimestampLiteral(NodeLocation location, const std::string& value)
      : Literal(NodeType::kTimestampLiteral, location), value_(value) {}

  const std::string& value() const {
    return value_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::string value_;
};

class IntervalLiteral : public Literal {
 public:
  enum class Sign { kPositive, kNegative };
  enum class IntervalField { kYear, kMonth, kDay, kHour, kMinute, kSecond };

  IntervalLiteral(
      NodeLocation location,
      const std::string& value,
      Sign sign,
      IntervalField startField,
      std::optional<IntervalField> endField = std::nullopt)
      : Literal(NodeType::kIntervalLiteral, location),
        value_(value),
        sign_(sign),
        startField_(startField),
        endField_(endField) {}

  const std::string& value() const {
    return value_;
  }

  Sign sign() const {
    return sign_;
  }

  IntervalField startField() const {
    return startField_;
  }

  const std::optional<IntervalField>& endField() const {
    return endField_;
  }

  bool isYearToMonth() const {
    return startField_ == IntervalField::kYear ||
        startField_ == IntervalField::kMonth;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::string value_;
  Sign sign_;
  IntervalField startField_;
  std::optional<IntervalField> endField_;
};

class EnumLiteral : public Literal {
 public:
  explicit EnumLiteral(NodeLocation location, const std::string& value)
      : Literal(NodeType::kEnumLiteral, location), value_(value) {}

  const std::string& value() const {
    return value_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::string value_;
};

} // namespace axiom::sql::presto
