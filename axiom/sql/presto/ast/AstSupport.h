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
class OrderBy;

// Support Classes

class TypeSignature;
using TypeSignaturePtr = std::shared_ptr<TypeSignature>;

// Base type (e.g. map) and optional parameters (e.g. K, V).
class TypeSignature : public Node {
 public:
  /// @param baseName The base name of the type. Could describe a concrete type
  /// name (e.g. map, bigint, double), or a variable (e.g. K, V).
  /// @param parameters The optional parameters for the type. For example, the
  /// signature "map(K, V)" would have two parameters, "K", and "V". All
  /// parameters must be of the same ParameterType.
  /// @param rowFieldName if this type signature is a field of another parent
  /// row type, it can optionally have a name. E.g. `row(id bigint)` would have
  /// "id" set as rowFieldName in the "bigint" parameter.
  TypeSignature(
      NodeLocation location,
      std::string baseName,
      std::vector<TypeSignaturePtr> parameters,
      std::optional<std::string> rowFieldName = std::nullopt)
      : Node(NodeType::kTypeSignature, location),
        baseName_{std::move(baseName)},
        parameters_{std::move(parameters)},
        rowFieldName_(std::move(rowFieldName)) {}

  TypeSignature(
      NodeLocation location,
      std::string baseName,
      std::optional<std::string> rowFieldName = std::nullopt)
      : Node(NodeType::kTypeSignature, location),
        baseName_{std::move(baseName)},
        parameters_{},
        rowFieldName_(std::move(rowFieldName)) {}

  const std::string& baseName() const {
    return baseName_;
  }

  const std::vector<TypeSignaturePtr>& parameters() const {
    return parameters_;
  }

  const std::optional<std::string>& rowFieldName() const {
    return rowFieldName_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  const std::string baseName_;
  const std::vector<TypeSignaturePtr> parameters_;
  const std::optional<std::string> rowFieldName_;
};

using TypeSignaturePtr = std::shared_ptr<TypeSignature>;

class Property : public Node {
 public:
  Property(
      NodeLocation location,
      const std::shared_ptr<Identifier>& name,
      const ExpressionPtr& value)
      : Node(NodeType::kProperty, location), name_(name), value_(value) {}

  const std::shared_ptr<Identifier>& name() const {
    return name_;
  }

  const ExpressionPtr& value() const {
    return value_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<Identifier> name_;
  ExpressionPtr value_;
};

class CallArgument : public Node {
 public:
  CallArgument(
      NodeLocation location,
      const std::shared_ptr<Identifier>& name,
      const ExpressionPtr& value)
      : Node(NodeType::kCallArgument, location), name_(name), value_(value) {}

  const std::shared_ptr<Identifier>& name() const {
    return name_;
  }

  const ExpressionPtr& value() const {
    return value_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<Identifier> name_;
  ExpressionPtr value_;
};

// Window Classes
class FrameBound : public Node {
 public:
  enum class Type {
    kUnboundedPreceding,
    kPreceding,
    kCurrentRow,
    kFollowing,
    kUnboundedFollowing
  };

  explicit FrameBound(
      NodeLocation location,
      Type type,
      std::optional<ExpressionPtr> value = std::nullopt)
      : Node(NodeType::kFrameBound, location),
        boundType_(type),
        value_(value) {}

  Type boundType() const {
    return boundType_;
  }

  const std::optional<ExpressionPtr>& value() const {
    return value_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  Type boundType_;
  std::optional<ExpressionPtr> value_;
};

class WindowFrame : public Node {
 public:
  enum class Type { kRange, kRows, kGroups };

  WindowFrame(
      NodeLocation location,
      Type type,
      const std::shared_ptr<FrameBound>& start,
      const std::shared_ptr<FrameBound>& end = nullptr)
      : Node(NodeType::kWindowFrame, location),
        frameType_(type),
        start_(start),
        end_(end) {}

  Type frameType() const {
    return frameType_;
  }

  const std::shared_ptr<FrameBound>& start() const {
    return start_;
  }

  const std::shared_ptr<FrameBound>& end() const {
    return end_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  Type frameType_;
  std::shared_ptr<FrameBound> start_;
  std::shared_ptr<FrameBound> end_;
};

class Window : public Node {
 public:
  explicit Window(
      NodeLocation location,
      const std::vector<ExpressionPtr>& partitionBy,
      const std::shared_ptr<OrderBy>& orderBy = nullptr,
      const std::shared_ptr<WindowFrame>& frame = nullptr)
      : Node(NodeType::kWindow, location),
        partitionBy_(partitionBy),
        orderBy_(orderBy),
        frame_(frame) {}

  const std::vector<ExpressionPtr>& partitionBy() const {
    return partitionBy_;
  }

  const std::shared_ptr<OrderBy>& orderBy() const {
    return orderBy_;
  }

  const std::shared_ptr<WindowFrame>& frame() const {
    return frame_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::vector<ExpressionPtr> partitionBy_;
  std::shared_ptr<OrderBy> orderBy_;
  std::shared_ptr<WindowFrame> frame_;
};

// Sorting and Grouping
class SortItem : public Node {
 public:
  enum class Ordering { kAscending, kDescending };
  enum class NullOrdering { kFirst, kLast, kUndefined };

  SortItem(
      NodeLocation location,
      ExpressionPtr sortKey,
      Ordering ordering,
      NullOrdering nullOrdering)
      : Node(NodeType::kSortItem, location),
        sortKey_(sortKey),
        ordering_(ordering),
        nullOrdering_(nullOrdering) {}

  const ExpressionPtr& sortKey() const {
    return sortKey_;
  }

  Ordering ordering() const {
    return ordering_;
  }

  bool isAscending() const {
    return ordering_ == Ordering::kAscending;
  }

  NullOrdering nullOrdering() const {
    return nullOrdering_;
  }

  bool isNullsFirst() const {
    return nullOrdering_ == NullOrdering::kFirst;
  }

  void accept(AstVisitor* visitor) override;

 private:
  ExpressionPtr sortKey_;
  Ordering ordering_;
  NullOrdering nullOrdering_;
};

class OrderBy : public Node {
 public:
  explicit OrderBy(
      NodeLocation location,
      const std::vector<std::shared_ptr<SortItem>>& sortItems)
      : Node(NodeType::kOrderBy, location), sortItems_(sortItems) {}

  const std::vector<std::shared_ptr<SortItem>>& sortItems() const {
    return sortItems_;
  }
  void accept(AstVisitor* visitor) override;

 private:
  std::vector<std::shared_ptr<SortItem>> sortItems_;
};

using OrderByPtr = std::shared_ptr<OrderBy>;

class Offset : public Node {
 public:
  explicit Offset(NodeLocation location, const std::string& offset)
      : Node(NodeType::kOffset, location), offset_(offset) {}

  const std::string& offset() const {
    return offset_;
  }
  void accept(AstVisitor* visitor) override;

 private:
  std::string offset_;
};

using OffsetPtr = std::shared_ptr<Offset>;

// Grouping Elements
class GroupingElement : public Node {
 public:
  explicit GroupingElement(NodeLocation location, NodeType type)
      : Node(type, location) {}
};

using GroupingElementPtr = std::shared_ptr<GroupingElement>;

class SimpleGroupBy : public GroupingElement {
 public:
  explicit SimpleGroupBy(
      NodeLocation location,
      const std::vector<ExpressionPtr>& expressions)
      : GroupingElement(location, NodeType::kSimpleGroupBy),
        expressions_(expressions) {}

  const std::vector<ExpressionPtr>& expressions() const {
    return expressions_;
  }
  void accept(AstVisitor* visitor) override;

 private:
  std::vector<ExpressionPtr> expressions_;
};

class GroupingSets : public GroupingElement {
 public:
  explicit GroupingSets(
      NodeLocation location,
      const std::vector<std::vector<ExpressionPtr>>& sets)
      : GroupingElement(location, NodeType::kGroupingSets), sets_(sets) {}

  const std::vector<std::vector<ExpressionPtr>>& sets() const {
    return sets_;
  }
  void accept(AstVisitor* visitor) override;

 private:
  std::vector<std::vector<ExpressionPtr>> sets_;
};

class Cube : public GroupingElement {
 public:
  explicit Cube(
      NodeLocation location,
      const std::vector<ExpressionPtr>& expressions)
      : GroupingElement(location, NodeType::kCube), expressions_(expressions) {}

  const std::vector<ExpressionPtr>& expressions() const {
    return expressions_;
  }
  void accept(AstVisitor* visitor) override;

 private:
  std::vector<ExpressionPtr> expressions_;
};

class Rollup : public GroupingElement {
 public:
  explicit Rollup(
      NodeLocation location,
      const std::vector<ExpressionPtr>& expressions)
      : GroupingElement(location, NodeType::kRollup),
        expressions_(expressions) {}

  const std::vector<ExpressionPtr>& expressions() const {
    return expressions_;
  }
  void accept(AstVisitor* visitor) override;

 private:
  std::vector<ExpressionPtr> expressions_;
};

class GroupBy : public Node {
 public:
  GroupBy(
      NodeLocation location,
      bool distinct,
      const std::vector<GroupingElementPtr>& groupingElements)
      : Node(NodeType::kGroupBy, location),
        distinct_(distinct),
        groupingElements_(groupingElements) {}

  bool isDistinct() const {
    return distinct_;
  }
  const std::vector<GroupingElementPtr>& groupingElements() const {
    return groupingElements_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  bool distinct_;
  std::vector<GroupingElementPtr> groupingElements_;
};

using GroupByPtr = std::shared_ptr<GroupBy>;

// WITH clause
class WithQuery : public Node {
 public:
  WithQuery(
      NodeLocation location,
      const std::shared_ptr<Identifier>& name,
      const StatementPtr& query,
      std::optional<std::vector<std::shared_ptr<Identifier>>> columnNames =
          std::nullopt)
      : Node(NodeType::kWithQuery, location),
        name_(name),
        query_(query),
        columnNames_(std::move(columnNames)) {}

  const std::shared_ptr<Identifier>& name() const {
    return name_;
  }

  const StatementPtr& query() const {
    return query_;
  }

  const std::optional<std::vector<std::shared_ptr<Identifier>>>& columnNames()
      const {
    return columnNames_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<Identifier> name_;
  StatementPtr query_;
  std::optional<std::vector<std::shared_ptr<Identifier>>> columnNames_;
};

class With : public Node {
 public:
  With(
      NodeLocation location,
      bool recursive,
      const std::vector<std::shared_ptr<WithQuery>>& queries)
      : Node(NodeType::kWith, location),
        recursive_(recursive),
        queries_(queries) {}

  bool isRecursive() const {
    return recursive_;
  }
  const std::vector<std::shared_ptr<WithQuery>>& queries() const {
    return queries_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  bool recursive_;
  std::vector<std::shared_ptr<WithQuery>> queries_;
};

// Table Elements
class TableElement : public Node {
 public:
  explicit TableElement(NodeType type, NodeLocation location)
      : Node(type, location) {}
};

using TableElementPtr = std::shared_ptr<TableElement>;

class ColumnDefinition : public TableElement {
 public:
  ColumnDefinition(
      NodeLocation location,
      const std::shared_ptr<Identifier>& name,
      const std::string& columnType,
      bool nullable,
      const std::vector<std::shared_ptr<Property>>& properties,
      std::optional<std::string> comment = std::nullopt)
      : TableElement(NodeType::kColumnDefinition, location),
        name_(name),
        columnType_(columnType),
        nullable_(nullable),
        properties_(properties),
        comment_(std::move(comment)) {}

  const std::shared_ptr<Identifier>& name() const {
    return name_;
  }

  const std::string& columnType() const {
    return columnType_;
  }

  bool isNullable() const {
    return nullable_;
  }

  const std::vector<std::shared_ptr<Property>>& properties() const {
    return properties_;
  }

  const std::optional<std::string>& comment() const {
    return comment_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<Identifier> name_;
  std::string columnType_;
  bool nullable_;
  std::vector<std::shared_ptr<Property>> properties_;
  std::optional<std::string> comment_;
};

class LikeClause : public TableElement {
 public:
  enum class PropertiesOption { kIncluding, kExcluding };

  explicit LikeClause(
      NodeLocation location,
      const std::shared_ptr<QualifiedName>& tableName,
      std::optional<PropertiesOption> propertiesOption = std::nullopt)
      : TableElement(NodeType::kLikeClause, location),
        tableName_(tableName),
        propertiesOption_(propertiesOption) {}

  const std::shared_ptr<QualifiedName>& tableName() const {
    return tableName_;
  }

  const std::optional<PropertiesOption>& propertiesOption() const {
    return propertiesOption_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<QualifiedName> tableName_;
  std::optional<PropertiesOption> propertiesOption_;
};

class ConstraintSpecification : public TableElement {
 public:
  enum class ConstraintType { kUnique, kPrimaryKey };

  ConstraintSpecification(
      NodeLocation location,
      const std::shared_ptr<Identifier>& name,
      const std::vector<std::shared_ptr<Identifier>>& columns,
      ConstraintType type)
      : TableElement(NodeType::kConstraintSpecification, location),
        name_(name),
        columns_(columns),
        type_(type) {}

  const std::shared_ptr<Identifier>& name() const {
    return name_;
  }

  const std::vector<std::shared_ptr<Identifier>>& columns() const {
    return columns_;
  }

  ConstraintType constraintType() const {
    return type_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<Identifier> name_;
  std::vector<std::shared_ptr<Identifier>> columns_;
  ConstraintType type_;
};

// Security and Transaction Support
class PrincipalSpecification : public Node {
 public:
  enum class Type { kUnspecified, kUser, kRole };

  PrincipalSpecification(
      NodeLocation location,
      Type principalType,
      const std::shared_ptr<Identifier>& name)
      : Node(NodeType::kPrincipalSpecification, location),
        principalType_(principalType),
        name_(name) {}

  Type principalType() const {
    return principalType_;
  }

  const std::shared_ptr<Identifier>& name() const {
    return name_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  Type principalType_;
  std::shared_ptr<Identifier> name_;
};

class GrantorSpecification : public Node {
 public:
  enum class Type { kPrincipal, kCurrentUser, kCurrentRole };

  explicit GrantorSpecification(
      NodeLocation location,
      Type principalType,
      const std::shared_ptr<PrincipalSpecification>& principal = nullptr)
      : Node(NodeType::kGrantorSpecification, location),
        principalType_(principalType),
        principal_(principal) {}

  Type principalType() const {
    return principalType_;
  }

  const std::shared_ptr<PrincipalSpecification>& principal() const {
    return principal_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  Type principalType_;
  std::shared_ptr<PrincipalSpecification> principal_;
};

class Isolation : public Node {
 public:
  enum class Level {
    kSerializable,
    kRepeatableRead,
    kReadCommitted,
    kReadUncommitted
  };

  explicit Isolation(NodeLocation location, Level level)
      : Node(NodeType::kIsolation, location), level_(level) {}

  Level level() const {
    return level_;
  }
  void accept(AstVisitor* visitor) override;

 private:
  Level level_;
};

class TransactionAccessMode : public Node {
 public:
  enum class Type { kReadOnly, kReadWrite };

  explicit TransactionAccessMode(NodeLocation location, Type accessMode)
      : Node(NodeType::kTransactionAccessMode, location),
        accessMode_(accessMode) {}

  Type accessMode() const {
    return accessMode_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  Type accessMode_;
};

class TransactionMode : public Node {
 public:
  explicit TransactionMode(NodeType type, NodeLocation location)
      : Node(type, location) {}
};

using TransactionModePtr = std::shared_ptr<TransactionMode>;

// Function and Routine Support
class SqlParameterDeclaration : public Node {
 public:
  SqlParameterDeclaration(
      NodeLocation location,
      const std::shared_ptr<Identifier>& name,
      const std::string& parameterType)
      : Node(NodeType::kSqlParameterDeclaration, location),
        name_(name),
        parameterType_(parameterType) {}

  const std::shared_ptr<Identifier>& name() const {
    return name_;
  }

  const std::string& parameterType() const {
    return parameterType_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<Identifier> name_;
  std::string parameterType_;
};

class RoutineCharacteristics : public Node {
 public:
  enum class Determinism { kDeterministic, kNotDeterministic };
  enum class NullCallClause { kReturnsNullOnNullInput, kCalledOnNullInput };

  RoutineCharacteristics(
      NodeLocation location,
      std::optional<Determinism> determinism = std::nullopt,
      std::optional<NullCallClause> nullCallClause = std::nullopt)
      : Node(NodeType::kRoutineCharacteristics, location),
        determinism_(determinism),
        nullCallClause_(nullCallClause) {}

  const std::optional<Determinism>& determinism() const {
    return determinism_;
  }

  const std::optional<NullCallClause>& nullCallClause() const {
    return nullCallClause_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::optional<Determinism> determinism_;
  std::optional<NullCallClause> nullCallClause_;
};

class RoutineBody : public Node {
 public:
  explicit RoutineBody(NodeType type, NodeLocation location)
      : Node(type, location) {}
};

using RoutineBodyPtr = std::shared_ptr<RoutineBody>;

class ExternalBodyReference : public RoutineBody {
 public:
  explicit ExternalBodyReference(
      NodeLocation location,
      const std::string& externalName)
      : RoutineBody(NodeType::kExternalBodyReference, location),
        externalName_(externalName) {}

  const std::string& externalName() const {
    return externalName_;
  }
  void accept(AstVisitor* visitor) override;

 private:
  std::string externalName_;
};

class Return : public RoutineBody {
 public:
  explicit Return(NodeLocation location, ExpressionPtr expression)
      : RoutineBody(NodeType::kReturn, location), expression_(expression) {}

  const ExpressionPtr& expression() const {
    return expression_;
  }
  void accept(AstVisitor* visitor) override;

 private:
  ExpressionPtr expression_;
};

// Explain Support
class ExplainOption : public Node {
 public:
  explicit ExplainOption(NodeType type, NodeLocation location)
      : Node(type, location) {}
};

using ExplainOptionPtr = std::shared_ptr<ExplainOption>;

class ExplainFormat : public ExplainOption {
 public:
  enum class Type { kText, kGraphviz, kJson };

  explicit ExplainFormat(NodeLocation location, Type formatType)
      : ExplainOption(NodeType::kExplainFormat, location),
        formatType_(formatType) {}

  Type formatType() const {
    return formatType_;
  }
  void accept(AstVisitor* visitor) override;

 private:
  Type formatType_;
};

class ExplainType : public ExplainOption {
 public:
  enum class Type { kLogical, kGraph, kDistributed, kValidate, kIo };

  explicit ExplainType(NodeLocation location, Type explainType)
      : ExplainOption(NodeType::kExplainType, location),
        explainType_(explainType) {}

  Type explainType() const {
    return explainType_;
  }
  void accept(AstVisitor* visitor) override;

 private:
  Type explainType_;
};

} // namespace axiom::sql::presto
