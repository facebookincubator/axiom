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

#include "axiom/sql/presto/PrestoSqlLexer.h"
#include "axiom/sql/presto/PrestoSqlParser.h"

namespace facebook::velox::sql {

class ParserHelper {
 public:
  explicit ParserHelper(const std::string& queryText);

  /// Simple API to test the ANTLR parser which calls the query() API. This
  /// function must be called before synaxErrorCount().
  void parse();

  /// Returns the number of syntax errors found during parsing. This function
  /// must be called after parse() is called.
  size_t synaxErrorCount() const;

 private:
  std::unique_ptr<antlr4::ANTLRInputStream> inputStream_;
  std::unique_ptr<PrestoSqlLexer> lexer_;
  std::unique_ptr<antlr4::CommonTokenStream> tokenStream_;
  std::unique_ptr<PrestoSqlParser> parser_;
};
} // namespace facebook::velox::sql
