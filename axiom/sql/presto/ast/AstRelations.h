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
#include <utility>
#include <vector>
#include "axiom/sql/presto/ast/AstNode.h"

namespace axiom::sql::presto {

// Forward declarations
class QualifiedName;
class Identifier;
class Expression;
class Select;
class OrderBy;
class GroupBy;
class With;
class Window;
class WindowFrame;
class FrameBound;
class SortItem;
class GroupingElement;
class Offset;
class TableVersionExpression;

// Query Body base class
class QueryBody : public Relation {
 public:
  explicit QueryBody(NodeType type, NodeLocation location)
      : Relation(type, location) {}
};

using QueryBodyPtr = std::shared_ptr<QueryBody>;

// Query Structure Classes
class Query : public Statement {
 public:
  Query(
      NodeLocation location,
      const std::shared_ptr<With>& with,
      const QueryBodyPtr& queryBody,
      const std::shared_ptr<OrderBy>& orderBy = nullptr,
      const std::shared_ptr<Offset>& offset = nullptr,
      std::optional<std::string> limit = std::nullopt)
      : Statement(NodeType::kQuery, location),
        with_(with),
        queryBody_(queryBody),
        orderBy_(orderBy),
        offset_(offset),
        limit_(std::move(limit)) {}

  const std::shared_ptr<With>& with() const {
    return with_;
  }

  const QueryBodyPtr& queryBody() const {
    return queryBody_;
  }

  const std::shared_ptr<OrderBy>& orderBy() const {
    return orderBy_;
  }

  const std::shared_ptr<Offset>& offset() const {
    return offset_;
  }

  const std::optional<std::string>& limit() const {
    return limit_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<With> with_;
  QueryBodyPtr queryBody_;
  std::shared_ptr<OrderBy> orderBy_;
  std::shared_ptr<Offset> offset_;
  std::optional<std::string> limit_;
};

class QuerySpecification : public QueryBody {
 public:
  QuerySpecification(
      NodeLocation location,
      const std::shared_ptr<Select>& select,
      const RelationPtr& from = nullptr,
      const ExpressionPtr& where = nullptr,
      const std::shared_ptr<GroupBy>& groupBy = nullptr,
      const ExpressionPtr& having = nullptr,
      const std::shared_ptr<OrderBy>& orderBy = nullptr,
      const std::shared_ptr<Offset>& offset = nullptr,
      std::optional<std::string> limit = std::nullopt)
      : QueryBody(NodeType::kQuerySpecification, location),
        select_(select),
        from_(from),
        where_(where),
        groupBy_(groupBy),
        having_(having),
        orderBy_(orderBy),
        offset_(offset),
        limit_(std::move(limit)) {}

  const std::shared_ptr<Select>& select() const {
    return select_;
  }

  const RelationPtr& from() const {
    return from_;
  }

  const ExpressionPtr& where() const {
    return where_;
  }

  const std::shared_ptr<GroupBy>& groupBy() const {
    return groupBy_;
  }

  const ExpressionPtr& having() const {
    return having_;
  }

  const std::shared_ptr<OrderBy>& orderBy() const {
    return orderBy_;
  }

  const std::shared_ptr<Offset>& offset() const {
    return offset_;
  }

  const std::optional<std::string>& limit() const {
    return limit_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<Select> select_;
  RelationPtr from_;
  ExpressionPtr where_;
  std::shared_ptr<GroupBy> groupBy_;
  ExpressionPtr having_;
  std::shared_ptr<OrderBy> orderBy_;
  std::shared_ptr<Offset> offset_;
  std::optional<std::string> limit_;
};

// SELECT items
class SelectItem : public Node {
 public:
  explicit SelectItem(NodeType type, NodeLocation location)
      : Node(type, location) {}
};

using SelectItemPtr = std::shared_ptr<SelectItem>;

class SingleColumn : public SelectItem {
 public:
  SingleColumn(
      NodeLocation location,
      const ExpressionPtr& expression,
      const std::shared_ptr<Identifier>& alias = nullptr)
      : SelectItem(NodeType::kSingleColumn, location),
        expression_(expression),
        alias_(alias) {}

  const ExpressionPtr& expression() const {
    return expression_;
  }

  const std::shared_ptr<Identifier>& alias() const {
    return alias_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  ExpressionPtr expression_;
  std::shared_ptr<Identifier> alias_;
};

class AllColumns : public SelectItem {
 public:
  explicit AllColumns(
      NodeLocation location,
      const std::shared_ptr<QualifiedName>& prefix = nullptr)
      : SelectItem(NodeType::kAllColumns, location), prefix_(prefix) {}

  const std::shared_ptr<QualifiedName>& prefix() const {
    return prefix_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<QualifiedName> prefix_;
};

class Select : public Node {
 public:
  Select(
      NodeLocation location,
      bool distinct,
      const std::vector<SelectItemPtr>& selectItems)
      : Node(NodeType::kSelect, location),
        distinct_(distinct),
        selectItems_(selectItems) {}

  bool isDistinct() const {
    return distinct_;
  }

  const std::vector<SelectItemPtr>& selectItems() const {
    return selectItems_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  bool distinct_;
  std::vector<SelectItemPtr> selectItems_;
};

// Table Relations
class Table : public QueryBody {
 public:
  Table(
      NodeLocation location,
      const std::shared_ptr<QualifiedName>& name,
      const std::shared_ptr<TableVersionExpression>& version = nullptr)
      : QueryBody(NodeType::kTable, location), name_(name), version_(version) {}

  const std::shared_ptr<QualifiedName>& name() const {
    return name_;
  }

  const std::shared_ptr<TableVersionExpression>& version() const {
    return version_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<QualifiedName> name_;
  std::shared_ptr<TableVersionExpression> version_;
};

class AliasedRelation : public Relation {
 public:
  AliasedRelation(
      NodeLocation location,
      const RelationPtr& relation,
      const std::shared_ptr<Identifier>& alias,
      const std::vector<std::shared_ptr<Identifier>>& columnNames = {})
      : Relation(NodeType::kAliasedRelation, location),
        relation_(relation),
        alias_(alias),
        columnNames_(columnNames) {}

  const RelationPtr& relation() const {
    return relation_;
  }

  const std::shared_ptr<Identifier>& alias() const {
    return alias_;
  }

  const std::vector<std::shared_ptr<Identifier>>& columnNames() const {
    return columnNames_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  RelationPtr relation_;
  std::shared_ptr<Identifier> alias_;
  std::vector<std::shared_ptr<Identifier>> columnNames_;
};

class SampledRelation : public Relation {
 public:
  enum class Type { kBernoulli, kSystem };

  SampledRelation(
      NodeLocation location,
      const RelationPtr& relation,
      Type sampleType,
      const ExpressionPtr& samplePercentage)
      : Relation(NodeType::kSampledRelation, location),
        relation_(relation),
        sampleType_(sampleType),
        samplePercentage_(samplePercentage) {}

  const RelationPtr& relation() const {
    return relation_;
  }

  Type sampleType() const {
    return sampleType_;
  }

  const ExpressionPtr& samplePercentage() const {
    return samplePercentage_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  RelationPtr relation_;
  Type sampleType_;
  ExpressionPtr samplePercentage_;
};

class TableSubquery : public QueryBody {
 public:
  explicit TableSubquery(NodeLocation location, const StatementPtr& query)
      : QueryBody(NodeType::kTableSubquery, location), query_(query) {}

  const StatementPtr& query() const {
    return query_;
  }
  void accept(AstVisitor* visitor) override;

 private:
  StatementPtr query_;
};

class Lateral : public Relation {
 public:
  explicit Lateral(NodeLocation location, const RelationPtr& query)
      : Relation(NodeType::kLateral, location), query_(query) {}

  const RelationPtr& query() const {
    return query_;
  }
  void accept(AstVisitor* visitor) override;

 private:
  RelationPtr query_;
};

class Unnest : public Relation {
 public:
  Unnest(
      NodeLocation location,
      const std::vector<ExpressionPtr>& expressions,
      bool withOrdinality)
      : Relation(NodeType::kUnnest, location),
        expressions_(expressions),
        withOrdinality_(withOrdinality) {}

  const std::vector<ExpressionPtr>& expressions() const {
    return expressions_;
  }

  bool isWithOrdinality() const {
    return withOrdinality_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::vector<ExpressionPtr> expressions_;
  bool withOrdinality_;
};

class Values : public QueryBody {
 public:
  explicit Values(NodeLocation location, const std::vector<ExpressionPtr>& rows)
      : QueryBody(NodeType::kValues, location), rows_(rows) {}

  const std::vector<ExpressionPtr>& rows() const {
    return rows_;
  }
  void accept(AstVisitor* visitor) override;

 private:
  std::vector<ExpressionPtr> rows_;
};

// Join Classes
class JoinCriteria : public Node {
 public:
  explicit JoinCriteria(NodeType type, NodeLocation location)
      : Node(type, location) {}
};

using JoinCriteriaPtr = std::shared_ptr<JoinCriteria>;

class JoinOn : public JoinCriteria {
 public:
  explicit JoinOn(NodeLocation location, const ExpressionPtr& expression)
      : JoinCriteria(NodeType::kJoinOn, location), expression_(expression) {}

  const ExpressionPtr& expression() const {
    return expression_;
  }
  void accept(AstVisitor* visitor) override;

 private:
  ExpressionPtr expression_;
};

class JoinUsing : public JoinCriteria {
 public:
  explicit JoinUsing(
      NodeLocation location,
      const std::vector<std::shared_ptr<Identifier>>& columns)
      : JoinCriteria(NodeType::kJoinUsing, location), columns_(columns) {}

  const std::vector<std::shared_ptr<Identifier>>& columns() const {
    return columns_;
  }
  void accept(AstVisitor* visitor) override;

 private:
  std::vector<std::shared_ptr<Identifier>> columns_;
};

class Join : public Relation {
 public:
  enum class Type { kCross, kInner, kLeft, kRight, kFull, kImplicit };

  Join(
      NodeLocation location,
      Type joinType,
      const RelationPtr& left,
      const RelationPtr& right,
      const JoinCriteriaPtr& criteria = nullptr)
      : Relation(NodeType::kJoin, location),
        joinType_(joinType),
        left_(left),
        right_(right),
        criteria_(criteria) {}

  Type joinType() const {
    return joinType_;
  }

  const RelationPtr& left() const {
    return left_;
  }

  const RelationPtr& right() const {
    return right_;
  }

  const JoinCriteriaPtr& criteria() const {
    return criteria_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  Type joinType_;
  RelationPtr left_;
  RelationPtr right_;
  JoinCriteriaPtr criteria_;
};

class NaturalJoin : public Relation {
 public:
  NaturalJoin(
      NodeLocation location,
      Join::Type joinType,
      const RelationPtr& left,
      const RelationPtr& right)
      : Relation(NodeType::kNaturalJoin, location),
        joinType_(joinType),
        left_(left),
        right_(right) {}

  Join::Type joinType() const {
    return joinType_;
  }

  const RelationPtr& left() const {
    return left_;
  }

  const RelationPtr& right() const {
    return right_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  Join::Type joinType_;
  RelationPtr left_;
  RelationPtr right_;
};

// Set Operations
class SetOperation : public QueryBody {
 public:
  SetOperation(
      NodeType type,
      NodeLocation location,
      const QueryBodyPtr& left,
      const QueryBodyPtr& right,
      bool distinct)
      : QueryBody(type, location),
        left_(left),
        right_(right),
        distinct_(distinct) {}

  const QueryBodyPtr& left() const {
    return left_;
  }

  const QueryBodyPtr& right() const {
    return right_;
  }

  bool isDistinct() const {
    return distinct_;
  }

 protected:
  QueryBodyPtr left_;
  QueryBodyPtr right_;
  bool distinct_;
};

class Union : public SetOperation {
 public:
  Union(
      NodeLocation location,
      const QueryBodyPtr& left,
      const QueryBodyPtr& right,
      bool distinct)
      : SetOperation(NodeType::kUnion, location, left, right, distinct) {}

  void accept(AstVisitor* visitor) override;
};

class Intersect : public SetOperation {
 public:
  Intersect(
      NodeLocation location,
      const QueryBodyPtr& left,
      const QueryBodyPtr& right,
      bool distinct)
      : SetOperation(NodeType::kIntersect, location, left, right, distinct) {}

  void accept(AstVisitor* visitor) override;
};

class Except : public SetOperation {
 public:
  Except(
      NodeLocation location,
      const QueryBodyPtr& left,
      const QueryBodyPtr& right,
      bool distinct)
      : SetOperation(NodeType::kExcept, location, left, right, distinct) {}

  void accept(AstVisitor* visitor) override;
};

} // namespace axiom::sql::presto
