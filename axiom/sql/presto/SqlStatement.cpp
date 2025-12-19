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

#include "axiom/sql/presto/SqlStatement.h"

#include <folly/container/F14Map.h>

namespace axiom::sql::presto {

namespace {

const auto& statementKindNames() {
  static const folly::F14FastMap<SqlStatementKind, std::string_view> kNames = {
      {SqlStatementKind::kSelect, "SELECT"},
      {SqlStatementKind::kCreateTableAsSelect, "CREATE TABLE AS SELECT"},
      {SqlStatementKind::kInsert, "INSERT"},
      {SqlStatementKind::kDropTable, "DROP TABLE"},
      {SqlStatementKind::kExplain, "EXPLAIN"},
  };

  return kNames;
}

} // namespace

AXIOM_DEFINE_ENUM_NAME(SqlStatementKind, statementKindNames);

std::string_view SqlStatement::kindName() const {
  return SqlStatementKindName::toName(kind_);
}

} // namespace axiom::sql::presto
