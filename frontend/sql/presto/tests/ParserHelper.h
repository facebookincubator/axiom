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

#include "velox/frontend/sql/presto/PrestoSqlLexer.h"
#include "velox/frontend/sql/presto/PrestoSqlParser.h"

namespace facebook::velox::frontend::sql::presto {

using namespace antlr4;
using namespace antlrcpp;

class ParserHelper {
 public:
  PrestoSqlParser* createParser(const std::string& input);

 public:
  std::unique_ptr<ANTLRInputStream> inputStream_;
  std::unique_ptr<PrestoSqlLexer> lexer_;
  std::unique_ptr<CommonTokenStream> tokenStream_;
  std::unique_ptr<PrestoSqlParser> parser_;
};
} // namespace facebook::velox::frontend::sql::presto
