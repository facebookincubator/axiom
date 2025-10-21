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

#include "axiom/sql/presto/SqlStatement.h"

namespace axiom::sql::presto {

class PrestoParser {
 public:
  PrestoParser(
      const std::string& defaultConnectorId,
      facebook::velox::memory::MemoryPool* pool)
      : defaultConnectorId_{defaultConnectorId}, pool_{pool} {}

  SqlStatementPtr parse(std::string_view sql, bool enableTracing = false);

  facebook::axiom::logical_plan::ExprPtr parseExpression(
      std::string_view sql,
      bool enableTracing = false);

 private:
  SqlStatementPtr doParse(std::string_view sql, bool enableTracing);

  const std::string defaultConnectorId_;
  facebook::velox::memory::MemoryPool* pool_;
};

} // namespace axiom::sql::presto
