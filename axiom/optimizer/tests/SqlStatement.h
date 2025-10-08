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

#include "axiom/logical_plan/LogicalPlanNode.h"

namespace facebook::axiom::optimizer::test {

enum class SqlStatementKind {
  kSelect,
  kExplain,
};

class SqlStatement {
 public:
  explicit SqlStatement(SqlStatementKind kind) : kind_{kind} {}

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
  explicit SelectStatement(logical_plan::LogicalPlanNodePtr plan)
      : SqlStatement(SqlStatementKind::kSelect), plan_{std::move(plan)} {}

  const logical_plan::LogicalPlanNodePtr& plan() const {
    return plan_;
  }

 private:
  const logical_plan::LogicalPlanNodePtr plan_;
};

class ExplainStatement : public SqlStatement {
 public:
  enum class Type { kLogical, kGraph, kDistributed };

  /// 'type' applies only when 'analyze' is false.
  explicit ExplainStatement(
      SqlStatementPtr statement,
      bool analyze = false,
      Type type = Type::kLogical)
      : SqlStatement(SqlStatementKind::kExplain),
        statement_{std::move(statement)},
        analyze_{analyze},
        type_{type} {}

  const SqlStatementPtr& statement() const {
    return statement_;
  }

  bool isAnalyze() const {
    return analyze_;
  }

  Type type() const {
    return type_;
  }

 private:
  const SqlStatementPtr statement_;
  const bool analyze_;
  const Type type_;
};

} // namespace facebook::axiom::optimizer::test
