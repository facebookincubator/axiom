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

#include "axiom/optimizer/tests/SqlStatement.h"
#include "velox/vector/ComplexVector.h"

#include <duckdb.hpp> // @manual

namespace facebook::velox::optimizer::test {

class DuckParser {
 public:
  /// TODO Add support for queries that use tables from multiple connectors.
  ///
  /// @param connectorId The ID of the connector that can read all tables in the
  /// query.
  DuckParser(const std::string& connectorId, memory::MemoryPool* pool)
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
