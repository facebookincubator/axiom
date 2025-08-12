/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include "axiom/logical_plan/LogicalPlanNode.h"
#include "velox/vector/ComplexVector.h"

#include <duckdb.hpp> // @manual

namespace facebook::velox::optimizer::test {

enum class SqlStatementKind {
  kSelect,
  kExplain,
};

class SqlStatement {
 public:
  SqlStatement(SqlStatementKind kind) : kind_{kind} {}

  virtual ~SqlStatement() = default;

  SqlStatementKind kind() const {
    return kind_;
  }

  bool isSelect() const {
    return kind_ == SqlStatementKind::kSelect;
  }

  bool isExplain() const {
    return kind_ == SqlStatementKind::kExplain;
  }

  template <typename T>
  const T* asUnchecked() const {
    return dynamic_cast<const T*>(this);
  }

 private:
  const SqlStatementKind kind_;
};

using SqlStatementPtr = std::shared_ptr<const SqlStatement>;

class SelectStatement : public SqlStatement {
 public:
  SelectStatement(logical_plan::LogicalPlanNodePtr plan)
      : SqlStatement(SqlStatementKind::kSelect), plan_{std::move(plan)} {}

  logical_plan::LogicalPlanNodePtr plan() const {
    return plan_;
  }

 private:
  const logical_plan::LogicalPlanNodePtr plan_;
};

class ExplainStatement : public SqlStatement {
 public:
  ExplainStatement(SqlStatementPtr statement)
      : SqlStatement(SqlStatementKind::kExplain),
        statement_{std::move(statement)} {}

  SqlStatementPtr statement() const {
    return statement_;
  }

 private:
  const SqlStatementPtr statement_;
};

class QuerySqlParser {
 public:
  /// TODO Add support for queries that use tables from multiple connectors.
  ///
  /// @param connectorId The ID of the connector that can read all tables in the
  /// query.
  QuerySqlParser(const std::string& connectorId, memory::MemoryPool* pool)
      : connectorId_{connectorId}, pool_{pool} {}

  void registerTable(const std::string& name, const RowTypePtr& type);

  void registerScalarFunction(
      const std::string& name,
      const std::vector<TypePtr>& argTypes,
      const TypePtr& returnType);

  // TODO Allow replacing built-in DuckDB functions. Currently, replacing "sum"
  // causes a crash (a bug in DuckDB). Replacing existing functions is useful
  // when signatures don't match.
  void registerAggregateFunction(
      const std::string& name,
      const std::vector<TypePtr>& argTypes,
      const TypePtr& returnType);

  SqlStatementPtr parse(const std::string& sql);

 private:
  const std::string connectorId_;
  ::duckdb::DuckDB db_;
  ::duckdb::Connection conn_{db_};
  memory::MemoryPool* pool_;
  std::unordered_map<std::string, std::vector<RowVectorPtr>> tables_;
};

} // namespace facebook::velox::optimizer::test
