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

#include "velox/frontend/sql/presto/tests/ParserHelper.h"
#include "velox/frontend/sql/presto/tests/UpperCasedInput.h"

namespace facebook::velox::frontend::sql::presto {

using namespace antlr4;
using namespace antlrcpp;

PrestoSqlParser* ParserHelper::createParser(const std::string& input) {
  inputStream_ = std::make_unique<UpperCasedInput>(input);
  lexer_ = std::make_unique<PrestoSqlLexer>(inputStream_.get());
  tokenStream_ = std::make_unique<CommonTokenStream>(lexer_.get());
  parser_ = std::make_unique<PrestoSqlParser>(tokenStream_.get());
  return parser_.get();
}

} // namespace facebook::velox::frontend::sql::presto
