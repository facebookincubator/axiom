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
#include <memory>
#include <vector>
#include "axiom/common/Enums.h"
#include "velox/common/base/Exceptions.h"

namespace axiom::sql::presto {

class AstVisitor;

enum class NodeType {
  // Base types
  kNode,
  kStatement,
  kExpression,
  kRelation,
  kQueryBody,

  // Literals
  kBooleanLiteral,
  kStringLiteral,
  kLongLiteral,
  kDoubleLiteral,
  kDecimalLiteral,
  kGenericLiteral,
  kNullLiteral,
  kBinaryLiteral,
  kCharLiteral,
  kTimeLiteral,
  kTimestampLiteral,
  kIntervalLiteral,
  kEnumLiteral,

  // Identifiers and References
  kIdentifier,
  kQualifiedName,
  kDereferenceExpression,
  kFieldReference,
  kSymbolReference,
  kParameter,

  // Arithmetic Expressions
  kArithmeticBinaryExpression,
  kArithmeticUnaryExpression,

  // Comparison Expressions
  kComparisonExpression,
  kBetweenPredicate,
  kInPredicate,
  kInListExpression,
  kIsNullPredicate,
  kIsNotNullPredicate,
  kLikePredicate,
  kExistsPredicate,
  kQuantifiedComparisonExpression,

  // Logical Expressions
  kLogicalBinaryExpression,
  kNotExpression,

  // Conditional Expressions
  kIfExpression,
  kCoalesceExpression,
  kNullIfExpression,
  kSearchedCaseExpression,
  kSimpleCaseExpression,
  kWhenClause,
  kTryExpression,

  // Function and Call Expressions
  kFunctionCall,
  kCast,
  kExtract,
  kCurrentTime,
  kCurrentUser,
  kAtTimeZone,

  // Complex Expressions
  kSubqueryExpression,
  kArrayConstructor,
  kRow,
  kNamedRow,
  kSubscriptExpression,
  kLambdaExpression,
  kLambdaArgumentDeclaration,
  kBindExpression,
  kGroupingOperation,
  kTableVersionExpression,

  // Query Structures
  kQuery,
  kQuerySpecification,
  kSelect,
  kSelectItem,
  kSingleColumn,
  kAllColumns,
  kSelectColumns,
  kWith,
  kWithQuery,
  kOrderBy,
  kSortItem,
  kGroupBy,
  kGroupingSets,
  kCube,
  kRollup,
  kSimpleGroupBy,
  kOffset,
  kHaving,
  kLimit,
  kWhere,

  // Relations
  kTable,
  kAliasedRelation,
  kSampledRelation,
  kTableSubquery,
  kLateral,
  kUnnest,
  kValues,

  // Joins
  kJoin,
  kJoinOn,
  kJoinUsing,
  kNaturalJoin,

  // Set Operations
  kUnion,
  kIntersect,
  kExcept,

  // DDL Statements
  kCreateTable,
  kCreateTableAsSelect,
  kCreateView,
  kCreateMaterializedView,
  kCreateSchema,
  kCreateFunction,
  kCreateRole,
  kCreateType,
  kDropTable,
  kDropView,
  kDropMaterializedView,
  kDropSchema,
  kDropFunction,
  kDropRole,
  kAlterFunction,
  kAddColumn,
  kDropColumn,
  kRenameColumn,
  kRenameTable,
  kRenameSchema,
  kAddConstraint,
  kDropConstraint,
  kTruncateTable,

  // DML Statements
  kInsert,
  kUpdate,
  kDelete,
  kUpdateAssignment,

  // Show Statements
  kShowTables,
  kShowColumns,
  kShowSchemas,
  kShowCatalogs,
  kShowFunctions,
  kShowCreate,
  kShowCreateView,
  kShowCreateFunction,
  kShowSession,
  kShowStats,
  kShowStatsForQuery,
  kShowGrants,
  kShowRoles,
  kShowRoleGrants,

  // Transaction Statements
  kStartTransaction,
  kCommit,
  kRollback,
  kSetSession,
  kResetSession,
  kSetRole,

  // Security Statements
  kGrant,
  kRevoke,
  kGrantRoles,
  kRevokeRoles,

  // Utility Statements
  kExplain,
  kAnalyze,
  kCall,
  kPrepare,
  kExecute,
  kDeallocate,
  kUse,
  kDescribeInput,
  kDescribeOutput,
  kRefreshMaterializedView,
  kAlterRoutineCharacteristics,

  // Table Elements
  kColumnDefinition,
  kLikeClause,
  kConstraintSpecification,

  // Support Classes
  kTypeSignature,
  kProperty,
  kCallArgument,
  kWindow,
  kWindowFrame,
  kFrameBound,
  kIsolation,
  kTransactionAccessMode,
  kPrincipalSpecification,
  kGrantorSpecification,
  kRoutineCharacteristics,
  kExternalBodyReference,
  kReturn,
  kSqlParameterDeclaration,

  // Explain Options
  kExplainFormat,
  kExplainType,
  kExplainOption
};

AXIOM_DECLARE_ENUM_NAME(NodeType);

struct NodeLocation {
  int32_t line{-1};
  int32_t charPosition{-1};
};

class Node {
 public:
  Node(NodeType type, NodeLocation location)
      : type_(type), location_(location) {}

  virtual ~Node() = default;

  NodeType type() const {
    return type_;
  }

  NodeLocation location() const {
    return location_;
  }

  virtual void accept(AstVisitor* visitor) = 0;

  bool is(NodeType type) const {
    return type_ == type;
  }

  template <typename T>
  T* as() {
    return dynamic_cast<T*>(this);
  }

  template <typename T>
  const T* as() const {
    return dynamic_cast<const T*>(this);
  }

  /// Deep structural hash matching equals().
  virtual size_t hash() const = 0;

  bool operator==(const Node& other) const {
    return type_ == other.type_ && equals(other);
  }

  bool operator!=(const Node& other) const {
    return !(*this == other);
  }

  /// Null-safe deep equality on two Node trees. Returns true if both
  /// are null, false if exactly one is null, otherwise `*a == *b`.
  /// 'T' must be Node or a subclass.
  template <typename T>
  static bool deepEqual(
      const std::shared_ptr<T>& a,
      const std::shared_ptr<T>& b) {
    if (a == b) {
      return true;
    }
    if (!a || !b) {
      return false;
    }
    return *a == *b;
  }

  /// Null-safe deep hash. Returns 0 for null.
  /// 'T' must be Node or a subclass.
  template <typename T>
  static size_t deepHash(const std::shared_ptr<T>& a) {
    return a ? a->hash() : 0;
  }

  /// Deep element-wise equality for two vectors of Node pointers.
  /// 'T' must be Node or a subclass.
  template <typename T>
  static bool deepEqualAll(
      const std::vector<std::shared_ptr<T>>& a,
      const std::vector<std::shared_ptr<T>>& b) {
    if (a.size() != b.size()) {
      return false;
    }
    for (size_t i = 0; i < a.size(); ++i) {
      if (!deepEqual(a[i], b[i])) {
        return false;
      }
    }
    return true;
  }

  /// Combined hash for a vector of Node pointers, matching deepEqualAll.
  template <typename T>
  static size_t deepHashAll(const std::vector<std::shared_ptr<T>>& a) {
    size_t h = a.size();
    for (const auto& item : a) {
      h = folly::hash::hash_combine(h, deepHash(item));
    }
    return h;
  }

 protected:
  /// Deep structural equality on the AST. Ignores NodeLocation. Called by
  /// operator== after the node types have been confirmed to match, so
  /// overrides may assume 'other' has the same dynamic type as 'this' and
  /// use 'other.as<Subclass>()' without a null check.
  virtual bool equals(const Node& other) const = 0;

 private:
  const NodeType type_;
  const NodeLocation location_;
};

using NodePtr = std::shared_ptr<Node>;

class Expression : public Node {
 public:
  explicit Expression(NodeType type, NodeLocation location)
      : Node(type, location) {}
};

using ExpressionPtr = std::shared_ptr<Expression>;

class Statement : public Node {
 public:
  Statement(NodeType type, NodeLocation location) : Node(type, location) {}

  size_t hash() const override {
    VELOX_NYI("Statement::hash not implemented");
  }

 protected:
  bool equals(const Node&) const override {
    VELOX_NYI("Statement::equals not implemented");
  }
};

using StatementPtr = std::shared_ptr<Statement>;

class Relation : public Node {
 public:
  explicit Relation(NodeType type, NodeLocation location)
      : Node(type, location) {}
};

using RelationPtr = std::shared_ptr<Relation>;

} // namespace axiom::sql::presto

AXIOM_ENUM_FORMATTER(axiom::sql::presto::NodeType);
