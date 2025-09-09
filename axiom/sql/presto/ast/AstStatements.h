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
class Identifier;
class QualifiedName;
class TableElement;
class Property;
class ColumnDefinition;
class Expression;
class Relation;
class PrincipalSpecification;
class GrantorSpecification;
class RoutineCharacteristics;
class ExplainOption;
class CallArgument;
class UpdateAssignment;
class RoutineBody;
class SqlParameterDeclaration;
class TransactionMode;

// DDL Statements
class CreateTable : public Statement {
 public:
  CreateTable(
      NodeLocation location,
      const std::shared_ptr<QualifiedName>& name,
      const std::vector<std::shared_ptr<TableElement>>& elements,
      bool notExists,
      const std::vector<std::shared_ptr<Property>>& properties,
      std::optional<std::string> comment = std::nullopt)
      : Statement(NodeType::kCreateTable, location),
        name_(name),
        elements_(elements),
        notExists_(notExists),
        properties_(properties),
        comment_(std::move(comment)) {}

  const std::shared_ptr<QualifiedName>& name() const {
    return name_;
  }

  const std::vector<std::shared_ptr<TableElement>>& elements() const {
    return elements_;
  }

  bool isNotExists() const {
    return notExists_;
  }

  const std::vector<std::shared_ptr<Property>>& properties() const {
    return properties_;
  }

  const std::optional<std::string>& comment() const {
    return comment_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<QualifiedName> name_;
  std::vector<std::shared_ptr<TableElement>> elements_;
  bool notExists_;
  std::vector<std::shared_ptr<Property>> properties_;
  std::optional<std::string> comment_;
};

class CreateTableAsSelect : public Statement {
 public:
  CreateTableAsSelect(
      NodeLocation location,
      const std::shared_ptr<QualifiedName>& name,
      const StatementPtr& query,
      bool notExists,
      const std::vector<std::shared_ptr<Property>>& properties,
      bool withData,
      std::optional<std::string> comment = std::nullopt)
      : Statement(NodeType::kCreateTableAsSelect, location),
        name_(name),
        query_(query),
        notExists_(notExists),
        properties_(properties),
        withData_(withData),
        comment_(std::move(comment)) {}

  const std::shared_ptr<QualifiedName>& name() const {
    return name_;
  }

  const StatementPtr& query() const {
    return query_;
  }

  bool isNotExists() const {
    return notExists_;
  }

  const std::vector<std::shared_ptr<Property>>& properties() const {
    return properties_;
  }

  bool isWithData() const {
    return withData_;
  }

  const std::optional<std::string>& comment() const {
    return comment_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<QualifiedName> name_;
  StatementPtr query_;
  bool notExists_;
  std::vector<std::shared_ptr<Property>> properties_;
  bool withData_;
  std::optional<std::string> comment_;
};

class CreateView : public Statement {
 public:
  enum class Security { kInvoker, kDefiner };

  CreateView(
      NodeLocation location,
      const std::shared_ptr<QualifiedName>& name,
      const StatementPtr& query,
      bool replace,
      std::optional<Security> security = std::nullopt,
      std::optional<std::string> comment = std::nullopt)
      : Statement(NodeType::kCreateView, location),
        name_(name),
        query_(query),
        replace_(replace),
        security_(security),
        comment_(std::move(comment)) {}

  const std::shared_ptr<QualifiedName>& name() const {
    return name_;
  }

  const StatementPtr& query() const {
    return query_;
  }

  bool isReplace() const {
    return replace_;
  }

  const std::optional<Security>& security() const {
    return security_;
  }

  const std::optional<std::string>& comment() const {
    return comment_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<QualifiedName> name_;
  StatementPtr query_;
  bool replace_;
  std::optional<Security> security_;
  std::optional<std::string> comment_;
};

class CreateMaterializedView : public Statement {
 public:
  CreateMaterializedView(
      NodeLocation location,
      const std::shared_ptr<QualifiedName>& name,
      const StatementPtr& query,
      bool notExists,
      const std::vector<std::shared_ptr<Property>>& properties,
      std::optional<std::string> comment = std::nullopt)
      : Statement(NodeType::kCreateMaterializedView, location),
        name_(name),
        query_(query),
        notExists_(notExists),
        properties_(properties),
        comment_(std::move(comment)) {}

  const std::shared_ptr<QualifiedName>& name() const {
    return name_;
  }

  const StatementPtr& query() const {
    return query_;
  }

  bool isNotExists() const {
    return notExists_;
  }

  const std::vector<std::shared_ptr<Property>>& properties() const {
    return properties_;
  }

  const std::optional<std::string>& comment() const {
    return comment_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<QualifiedName> name_;
  StatementPtr query_;
  bool notExists_;
  std::vector<std::shared_ptr<Property>> properties_;
  std::optional<std::string> comment_;
};

class CreateSchema : public Statement {
 public:
  CreateSchema(
      NodeLocation location,
      const std::shared_ptr<QualifiedName>& schemaName,
      bool notExists,
      const std::vector<std::shared_ptr<Property>>& properties)
      : Statement(NodeType::kCreateSchema, location),
        schemaName_(schemaName),
        notExists_(notExists),
        properties_(properties) {}

  const std::shared_ptr<QualifiedName>& schemaName() const {
    return schemaName_;
  }

  bool isNotExists() const {
    return notExists_;
  }

  const std::vector<std::shared_ptr<Property>>& properties() const {
    return properties_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<QualifiedName> schemaName_;
  bool notExists_;
  std::vector<std::shared_ptr<Property>> properties_;
};

class CreateFunction : public Statement {
 public:
  CreateFunction(
      NodeLocation location,
      const std::shared_ptr<QualifiedName>& functionName,
      bool replace,
      const std::vector<std::shared_ptr<SqlParameterDeclaration>>& parameters,
      const std::string& returnType,
      const std::shared_ptr<RoutineCharacteristics>& characteristics,
      const std::shared_ptr<RoutineBody>& body)
      : Statement(NodeType::kCreateFunction, location),
        functionName_(functionName),
        replace_(replace),
        parameters_(parameters),
        returnType_(returnType),
        characteristics_(characteristics),
        body_(body) {}

  const std::shared_ptr<QualifiedName>& functionName() const {
    return functionName_;
  }

  bool isReplace() const {
    return replace_;
  }

  const std::vector<std::shared_ptr<SqlParameterDeclaration>>& parameters()
      const {
    return parameters_;
  }

  const std::string& returnType() const {
    return returnType_;
  }

  const std::shared_ptr<RoutineCharacteristics>& characteristics() const {
    return characteristics_;
  }

  const std::shared_ptr<RoutineBody>& body() const {
    return body_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<QualifiedName> functionName_;
  bool replace_;
  std::vector<std::shared_ptr<SqlParameterDeclaration>> parameters_;
  std::string returnType_;
  std::shared_ptr<RoutineCharacteristics> characteristics_;
  std::shared_ptr<RoutineBody> body_;
};

class CreateRole : public Statement {
 public:
  CreateRole(
      NodeLocation location,
      const std::string& name,
      const std::shared_ptr<PrincipalSpecification>& grantor = nullptr)
      : Statement(NodeType::kCreateRole, location),
        name_(name),
        grantor_(grantor) {}

  const std::string& name() const {
    return name_;
  }

  const std::shared_ptr<PrincipalSpecification>& grantor() const {
    return grantor_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::string name_;
  std::shared_ptr<PrincipalSpecification> grantor_;
};

class CreateType : public Statement {
 public:
  CreateType(
      NodeLocation location,
      const std::shared_ptr<QualifiedName>& name,
      const std::vector<std::string>& enumValues)
      : Statement(NodeType::kCreateType, location),
        name_(name),
        enumValues_(enumValues) {}

  const std::shared_ptr<QualifiedName>& name() const {
    return name_;
  }

  const std::vector<std::string>& enumValues() const {
    return enumValues_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<QualifiedName> name_;
  std::vector<std::string> enumValues_;
};

// Drop Statements
class DropTable : public Statement {
 public:
  DropTable(
      NodeLocation location,
      const std::shared_ptr<QualifiedName>& tableName,
      bool exists)
      : Statement(NodeType::kDropTable, location),
        tableName_(tableName),
        exists_(exists) {}

  const std::shared_ptr<QualifiedName>& tableName() const {
    return tableName_;
  }

  bool isExists() const {
    return exists_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<QualifiedName> tableName_;
  bool exists_;
};

class DropView : public Statement {
 public:
  DropView(
      NodeLocation location,
      const std::shared_ptr<QualifiedName>& viewName,
      bool exists)
      : Statement(NodeType::kDropView, location),
        viewName_(viewName),
        exists_(exists) {}

  const std::shared_ptr<QualifiedName>& viewName() const {
    return viewName_;
  }

  bool isExists() const {
    return exists_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<QualifiedName> viewName_;
  bool exists_;
};

class DropMaterializedView : public Statement {
 public:
  DropMaterializedView(
      NodeLocation location,
      const std::shared_ptr<QualifiedName>& viewName,
      bool exists)
      : Statement(NodeType::kDropMaterializedView, location),
        viewName_(viewName),
        exists_(exists) {}

  const std::shared_ptr<QualifiedName>& viewName() const {
    return viewName_;
  }

  bool isExists() const {
    return exists_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<QualifiedName> viewName_;
  bool exists_;
};

class DropSchema : public Statement {
 public:
  enum class DropBehavior { kRestrict, kCascade };

  DropSchema(
      NodeLocation location,
      const std::shared_ptr<QualifiedName>& schemaName,
      bool exists,
      DropBehavior behavior)
      : Statement(NodeType::kDropSchema, location),
        schemaName_(schemaName),
        exists_(exists),
        behavior_(behavior) {}

  const std::shared_ptr<QualifiedName>& schemaName() const {
    return schemaName_;
  }

  bool isExists() const {
    return exists_;
  }

  DropBehavior behavior() const {
    return behavior_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<QualifiedName> schemaName_;
  bool exists_;
  DropBehavior behavior_;
};

// DML Statements
class Insert : public Statement {
 public:
  Insert(
      NodeLocation location,
      const std::shared_ptr<QualifiedName>& target,
      std::vector<std::shared_ptr<Identifier>> columns,
      const StatementPtr& query)
      : Statement(NodeType::kInsert, location),
        target_(target),
        columns_(std::move(columns)),
        query_(query) {}

  const std::shared_ptr<QualifiedName>& target() const {
    return target_;
  }

  const std::vector<std::shared_ptr<Identifier>>& columns() const {
    return columns_;
  }

  const StatementPtr& query() const {
    return query_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<QualifiedName> target_;
  std::vector<std::shared_ptr<Identifier>> columns_;
  StatementPtr query_;
};

class UpdateAssignment : public Node {
 public:
  UpdateAssignment(
      NodeLocation location,
      const std::shared_ptr<Identifier>& name,
      const ExpressionPtr& value)
      : Node(NodeType::kUpdateAssignment, location),
        name_(name),
        value_(value) {}

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

class Update : public Statement {
 public:
  Update(
      NodeLocation location,
      const std::shared_ptr<QualifiedName>& table,
      const std::vector<std::shared_ptr<UpdateAssignment>>& assignments,
      const ExpressionPtr& where = nullptr)
      : Statement(NodeType::kUpdate, location),
        table_(table),
        assignments_(assignments),
        where_(where) {}

  const std::shared_ptr<QualifiedName>& table() const {
    return table_;
  }

  const std::vector<std::shared_ptr<UpdateAssignment>>& assignments() const {
    return assignments_;
  }

  const ExpressionPtr& where() const {
    return where_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<QualifiedName> table_;
  std::vector<std::shared_ptr<UpdateAssignment>> assignments_;
  ExpressionPtr where_;
};

class Delete : public Statement {
 public:
  Delete(
      NodeLocation location,
      const std::shared_ptr<QualifiedName>& table,
      const ExpressionPtr& where = nullptr)
      : Statement(NodeType::kDelete, location), table_(table), where_(where) {}

  const std::shared_ptr<QualifiedName>& table() const {
    return table_;
  }

  const ExpressionPtr& where() const {
    return where_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<QualifiedName> table_;
  ExpressionPtr where_;
};

// Utility Statements
class Explain : public Statement {
 public:
  Explain(
      NodeLocation location,
      const StatementPtr& statement,
      bool analyze,
      bool verbose,
      const std::vector<std::shared_ptr<ExplainOption>>& options)
      : Statement(NodeType::kExplain, location),
        statement_(statement),
        analyze_(analyze),
        verbose_(verbose),
        options_(options) {}

  const StatementPtr& statement() const {
    return statement_;
  }

  bool isAnalyze() const {
    return analyze_;
  }

  bool isVerbose() const {
    return verbose_;
  }

  const std::vector<std::shared_ptr<ExplainOption>>& options() const {
    return options_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  const StatementPtr statement_;
  const bool analyze_;
  const bool verbose_;
  const std::vector<std::shared_ptr<ExplainOption>> options_;
};

class Analyze : public Statement {
 public:
  Analyze(
      NodeLocation location,
      const std::shared_ptr<QualifiedName>& tableName,
      const std::vector<std::shared_ptr<Property>>& properties)
      : Statement(NodeType::kAnalyze, location),
        tableName_(tableName),
        properties_(properties) {}

  const std::shared_ptr<QualifiedName>& tableName() const {
    return tableName_;
  }

  const std::vector<std::shared_ptr<Property>>& properties() const {
    return properties_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<QualifiedName> tableName_;
  std::vector<std::shared_ptr<Property>> properties_;
};

class Call : public Statement {
 public:
  Call(
      NodeLocation location,
      const std::shared_ptr<QualifiedName>& name,
      const std::vector<std::shared_ptr<CallArgument>>& arguments)
      : Statement(NodeType::kCall, location),
        name_(name),
        arguments_(arguments) {}

  const std::shared_ptr<QualifiedName>& name() const {
    return name_;
  }

  const std::vector<std::shared_ptr<CallArgument>>& arguments() const {
    return arguments_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<QualifiedName> name_;
  std::vector<std::shared_ptr<CallArgument>> arguments_;
};

// Transaction Statements
class StartTransaction : public Statement {
 public:
  explicit StartTransaction(
      NodeLocation location,
      const std::vector<std::shared_ptr<TransactionMode>>& transactionModes)
      : Statement(NodeType::kStartTransaction, location),
        transactionModes_(transactionModes) {}

  const std::vector<std::shared_ptr<TransactionMode>>& transactionModes()
      const {
    return transactionModes_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::vector<std::shared_ptr<TransactionMode>> transactionModes_;
};

class Commit : public Statement {
 public:
  explicit Commit(NodeLocation location)
      : Statement(NodeType::kCommit, location) {}
  void accept(AstVisitor* visitor) override;
};

class Rollback : public Statement {
 public:
  explicit Rollback(NodeLocation location)
      : Statement(NodeType::kRollback, location) {}
  void accept(AstVisitor* visitor) override;
};

// Show Statements
class ShowColumns : public Statement {
 public:
  explicit ShowColumns(
      NodeLocation location,
      const std::shared_ptr<QualifiedName>& table)
      : Statement(NodeType::kShowColumns, location), table_(table) {}

  const std::shared_ptr<QualifiedName>& table() const {
    return table_;
  }

  void accept(AstVisitor* visitor) override;

 private:
  std::shared_ptr<QualifiedName> table_;
};

} // namespace axiom::sql::presto
