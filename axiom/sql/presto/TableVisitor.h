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

#include <unordered_set>

#include "axiom/sql/presto/ast/DefaultTraversalVisitor.h"

namespace axiom::sql::presto {

// Analyzes an expression to extract the fully-qualified names of any
// input or output tables or views in the expression. Table accesses
// inside CTEs are included, even if the CTE is never read from.
class TableVisitor : public DefaultTraversalVisitor {
 public:
  TableVisitor(
      const std::string& defaultConnectorId,
      const std::optional<std::string>& defaultSchema);

  const std::unordered_set<std::string>& inputTables() const {
    return inputTables_;
  }

  const std::optional<std::string>& outputTable() const {
    return outputTable_;
  }

 protected:
  void visitWithQuery(WithQuery* node) override;
  void visitTable(Table* node) override;
  void visitInsert(Insert* node) override;
  void visitCreateTableAsSelect(CreateTableAsSelect* node) override;
  void visitUpdate(Update* node) override;
  void visitDelete(Delete* node) override;
  void visitCreateTable(CreateTable* node) override;
  void visitCreateView(CreateView* node) override;
  void visitCreateMaterializedView(CreateMaterializedView* node) override;
  void visitDropTable(DropTable* node) override;
  void visitDropView(DropView* node) override;
  void visitDropMaterializedView(DropMaterializedView* node) override;

 private:
  std::string constructTableName(const QualifiedName& name) const;
  void setOutputTable(const QualifiedName& name);

  const std::string& defaultConnectorId_;
  const std::optional<std::string>& defaultSchema_;
  std::unordered_set<std::string> ctes_;
  std::unordered_set<std::string> inputTables_;
  std::optional<std::string> outputTable_;
};

} // namespace axiom::sql::presto
