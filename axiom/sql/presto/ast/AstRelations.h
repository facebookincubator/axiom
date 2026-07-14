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

#include <folly/hash/Hash.h>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include "axiom/sql/presto/ast/AstExpressions.h"
#include "axiom/sql/presto/ast/AstNode.h"
#include "axiom/sql/presto/ast/AstSupport.h"

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

  size_t hash() const override {
    return folly::hash::hash_combine(
        Node::deepHash(with_),
        Node::deepHash(queryBody_),
        Node::deepHash(orderBy_),
        Node::deepHash(offset_),
        limit_.has_value() ? std::hash<std::string>{}(*limit_) : 0);
  }

 protected:
  bool equals(const Node& other) const override {
    const auto& o = *other.as<Query>();
    return Node::deepEqual(with_, o.with_) &&
        Node::deepEqual(queryBody_, o.queryBody_) &&
        Node::deepEqual(orderBy_, o.orderBy_) &&
        Node::deepEqual(offset_, o.offset_) && limit_ == o.limit_;
  }

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

  size_t hash() const override {
    return folly::hash::hash_combine(
        Node::deepHash(select_),
        Node::deepHash(from_),
        Node::deepHash(where_),
        Node::deepHash(groupBy_),
        Node::deepHash(having_),
        Node::deepHash(orderBy_),
        Node::deepHash(offset_),
        limit_.has_value() ? std::hash<std::string>{}(*limit_) : 0);
  }

 protected:
  bool equals(const Node& other) const override {
    const auto& o = *other.as<QuerySpecification>();
    return Node::deepEqual(select_, o.select_) &&
        Node::deepEqual(from_, o.from_) && Node::deepEqual(where_, o.where_) &&
        Node::deepEqual(groupBy_, o.groupBy_) &&
        Node::deepEqual(having_, o.having_) &&
        Node::deepEqual(orderBy_, o.orderBy_) &&
        Node::deepEqual(offset_, o.offset_) && limit_ == o.limit_;
  }

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

  size_t hash() const override {
    return folly::hash::hash_combine(
        Node::deepHash(expression_), Node::deepHash(alias_));
  }

 protected:
  bool equals(const Node& other) const override {
    const auto& o = *other.as<SingleColumn>();
    return Node::deepEqual(expression_, o.expression_) &&
        Node::deepEqual(alias_, o.alias_);
  }

 private:
  ExpressionPtr expression_;
  std::shared_ptr<Identifier> alias_;
};

/// Represents a single REPLACE item: expression AS column_name.
struct ReplaceItem {
  /// Expression that replaces the column value.
  ExpressionPtr expression;
  /// Column being replaced.
  std::shared_ptr<Identifier> column;

  bool operator==(const ReplaceItem& other) const {
    return Node::deepEqual(expression, other.expression) &&
        Node::deepEqual(column, other.column);
  }

  size_t hash() const {
    return folly::hash::hash_combine(
        Node::deepHash(expression), Node::deepHash(column));
  }
};

/// Represents SELECT * or t.* with optional EXCLUDE and REPLACE modifiers.
class AllColumns : public SelectItem {
 public:
  /// Constructs a star expression with optional prefix and modifiers.
  AllColumns(
      NodeLocation location,
      std::shared_ptr<QualifiedName> prefix,
      std::vector<std::shared_ptr<Identifier>> excludeColumns,
      std::vector<ReplaceItem> replaceItems)
      : SelectItem(NodeType::kAllColumns, location),
        prefix_(std::move(prefix)),
        excludeColumns_(std::move(excludeColumns)),
        replaceItems_(std::move(replaceItems)) {}

  const std::shared_ptr<QualifiedName>& prefix() const {
    return prefix_;
  }

  /// Column names to exclude from star expansion.
  const std::vector<std::shared_ptr<Identifier>>& excludeColumns() const {
    return excludeColumns_;
  }

  /// Replacement expressions for named columns in star expansion.
  const std::vector<ReplaceItem>& replaceItems() const {
    return replaceItems_;
  }

  void accept(AstVisitor* visitor) override;

  size_t hash() const override {
    size_t replaceHash = replaceItems_.size();
    for (const auto& item : replaceItems_) {
      replaceHash = folly::hash::hash_combine(replaceHash, item.hash());
    }
    return folly::hash::hash_combine(
        Node::deepHash(prefix_),
        Node::deepHashAll(excludeColumns_),
        replaceHash);
  }

 protected:
  bool equals(const Node& other) const override {
    const auto& o = *other.as<AllColumns>();
    return Node::deepEqual(prefix_, o.prefix_) &&
        Node::deepEqualAll(excludeColumns_, o.excludeColumns_) &&
        replaceItems_ == o.replaceItems_;
  }

 private:
  std::shared_ptr<QualifiedName> prefix_;
  std::vector<std::shared_ptr<Identifier>> excludeColumns_;
  std::vector<ReplaceItem> replaceItems_;
};

/// Represents COLUMNS('regex') or t.COLUMNS('regex') in SELECT.
class SelectColumns : public SelectItem {
 public:
  SelectColumns(
      NodeLocation location,
      std::string pattern,
      std::shared_ptr<QualifiedName> prefix,
      std::vector<std::shared_ptr<Identifier>> excludeColumns,
      std::vector<ReplaceItem> replaceItems)
      : SelectItem(NodeType::kSelectColumns, location),
        pattern_(std::move(pattern)),
        prefix_(std::move(prefix)),
        excludeColumns_(std::move(excludeColumns)),
        replaceItems_(std::move(replaceItems)) {}

  /// Regex pattern to match column names.
  const std::string& pattern() const {
    return pattern_;
  }

  /// Optional table prefix (for t.COLUMNS('regex')).
  const std::shared_ptr<QualifiedName>& prefix() const {
    return prefix_;
  }

  /// Column names to exclude from matched columns.
  const std::vector<std::shared_ptr<Identifier>>& excludeColumns() const {
    return excludeColumns_;
  }

  /// Replacement expressions for named columns.
  const std::vector<ReplaceItem>& replaceItems() const {
    return replaceItems_;
  }

  void accept(AstVisitor* visitor) override;

  size_t hash() const override {
    size_t replaceHash = replaceItems_.size();
    for (const auto& item : replaceItems_) {
      replaceHash = folly::hash::hash_combine(replaceHash, item.hash());
    }
    return folly::hash::hash_combine(
        std::hash<std::string>{}(pattern_),
        Node::deepHash(prefix_),
        Node::deepHashAll(excludeColumns_),
        replaceHash);
  }

 protected:
  bool equals(const Node& other) const override {
    const auto& o = *other.as<SelectColumns>();
    return pattern_ == o.pattern_ && Node::deepEqual(prefix_, o.prefix_) &&
        Node::deepEqualAll(excludeColumns_, o.excludeColumns_) &&
        replaceItems_ == o.replaceItems_;
  }

 private:
  std::string pattern_;
  std::shared_ptr<QualifiedName> prefix_;
  std::vector<std::shared_ptr<Identifier>> excludeColumns_;
  std::vector<ReplaceItem> replaceItems_;
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

  size_t hash() const override {
    return folly::hash::hash_combine(
        std::hash<bool>{}(distinct_), Node::deepHashAll(selectItems_));
  }

 protected:
  bool equals(const Node& other) const override {
    const auto& o = *other.as<Select>();
    return distinct_ == o.distinct_ &&
        Node::deepEqualAll(selectItems_, o.selectItems_);
  }

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

  size_t hash() const override {
    return folly::hash::hash_combine(
        Node::deepHash(name_), Node::deepHash(version_));
  }

 protected:
  bool equals(const Node& other) const override {
    const auto& o = *other.as<Table>();
    return Node::deepEqual(name_, o.name_) &&
        Node::deepEqual(version_, o.version_);
  }

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

  size_t hash() const override {
    return folly::hash::hash_combine(
        Node::deepHash(relation_),
        Node::deepHash(alias_),
        Node::deepHashAll(columnNames_));
  }

 protected:
  bool equals(const Node& other) const override {
    const auto& o = *other.as<AliasedRelation>();
    return Node::deepEqual(relation_, o.relation_) &&
        Node::deepEqual(alias_, o.alias_) &&
        Node::deepEqualAll(columnNames_, o.columnNames_);
  }

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

  size_t hash() const override {
    return folly::hash::hash_combine(
        Node::deepHash(relation_),
        std::hash<Type>{}(sampleType_),
        Node::deepHash(samplePercentage_));
  }

 protected:
  bool equals(const Node& other) const override {
    const auto& o = *other.as<SampledRelation>();
    return Node::deepEqual(relation_, o.relation_) &&
        sampleType_ == o.sampleType_ &&
        Node::deepEqual(samplePercentage_, o.samplePercentage_);
  }

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

  size_t hash() const override {
    return Node::deepHash(query_);
  }

 protected:
  bool equals(const Node& other) const override {
    return Node::deepEqual(query_, other.as<TableSubquery>()->query_);
  }

 private:
  StatementPtr query_;
};

class Lateral : public Relation {
 public:
  explicit Lateral(NodeLocation location, const StatementPtr& query)
      : Relation(NodeType::kLateral, location), query_(query) {}

  const StatementPtr& query() const {
    return query_;
  }
  void accept(AstVisitor* visitor) override;

  size_t hash() const override {
    return Node::deepHash(query_);
  }

 protected:
  bool equals(const Node& other) const override {
    return Node::deepEqual(query_, other.as<Lateral>()->query_);
  }

 private:
  StatementPtr query_;
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

  size_t hash() const override {
    return folly::hash::hash_combine(
        Node::deepHashAll(expressions_), std::hash<bool>{}(withOrdinality_));
  }

 protected:
  bool equals(const Node& other) const override {
    const auto& o = *other.as<Unnest>();
    return Node::deepEqualAll(expressions_, o.expressions_) &&
        withOrdinality_ == o.withOrdinality_;
  }

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

  size_t hash() const override {
    return Node::deepHashAll(rows_);
  }

 protected:
  bool equals(const Node& other) const override {
    return Node::deepEqualAll(rows_, other.as<Values>()->rows_);
  }

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

  size_t hash() const override {
    return Node::deepHash(expression_);
  }

 protected:
  bool equals(const Node& other) const override {
    return Node::deepEqual(expression_, other.as<JoinOn>()->expression_);
  }

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

  size_t hash() const override {
    return Node::deepHashAll(columns_);
  }

 protected:
  bool equals(const Node& other) const override {
    return Node::deepEqualAll(columns_, other.as<JoinUsing>()->columns_);
  }

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

  size_t hash() const override {
    return folly::hash::hash_combine(
        std::hash<Type>{}(joinType_),
        Node::deepHash(left_),
        Node::deepHash(right_),
        Node::deepHash(criteria_));
  }

 protected:
  bool equals(const Node& other) const override {
    const auto& o = *other.as<Join>();
    return joinType_ == o.joinType_ && Node::deepEqual(left_, o.left_) &&
        Node::deepEqual(right_, o.right_) &&
        Node::deepEqual(criteria_, o.criteria_);
  }

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

  size_t hash() const override {
    return folly::hash::hash_combine(
        std::hash<Join::Type>{}(joinType_),
        Node::deepHash(left_),
        Node::deepHash(right_));
  }

 protected:
  bool equals(const Node& other) const override {
    const auto& o = *other.as<NaturalJoin>();
    return joinType_ == o.joinType_ && Node::deepEqual(left_, o.left_) &&
        Node::deepEqual(right_, o.right_);
  }

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

  size_t hash() const override {
    return folly::hash::hash_combine(
        Node::deepHash(left_),
        Node::deepHash(right_),
        std::hash<bool>{}(distinct_));
  }

 protected:
  bool equals(const Node& other) const override {
    const auto& o = *other.as<Union>();
    return Node::deepEqual(left_, o.left_) &&
        Node::deepEqual(right_, o.right_) && distinct_ == o.distinct_;
  }
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

  size_t hash() const override {
    return folly::hash::hash_combine(
        Node::deepHash(left_),
        Node::deepHash(right_),
        std::hash<bool>{}(distinct_));
  }

 protected:
  bool equals(const Node& other) const override {
    const auto& o = *other.as<Intersect>();
    return Node::deepEqual(left_, o.left_) &&
        Node::deepEqual(right_, o.right_) && distinct_ == o.distinct_;
  }
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

  size_t hash() const override {
    return folly::hash::hash_combine(
        Node::deepHash(left_),
        Node::deepHash(right_),
        std::hash<bool>{}(distinct_));
  }

 protected:
  bool equals(const Node& other) const override {
    const auto& o = *other.as<Except>();
    return Node::deepEqual(left_, o.left_) &&
        Node::deepEqual(right_, o.right_) && distinct_ == o.distinct_;
  }
};

} // namespace axiom::sql::presto
