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

#include <fmt/format.h>
#include "axiom/sql/presto/ast/UpperCaseInputStream.h"
#include "axiom/sql/presto/grammar/PrestoSqlLexer.h"
#include "axiom/sql/presto/grammar/PrestoSqlParser.h"

namespace axiom::sql::presto {

namespace detail {
class ErrorListener : public antlr4::BaseErrorListener {
 public:
  void syntaxError(
      antlr4::Recognizer* recognizer,
      antlr4::Token* offendingSymbol,
      size_t line,
      size_t charPositionInLine,
      const std::string& msg,
      std::exception_ptr e) override {
    if (firstError.empty()) {
      firstError = fmt::format(
          "Syntax error at {}:{}: {}", line, charPositionInLine, msg);
    }
  }

  std::string firstError;
};
} // namespace detail

/// Creates Presto SQL parser.
class ParserHelper {
 public:
  explicit ParserHelper(std::string_view sql)
      : inputStream_(std::make_unique<UpperCaseInputStream>(sql)),
        lexer_(std::make_unique<PrestoSqlLexer>(inputStream_.get())),
        tokenStream_(std::make_unique<antlr4::CommonTokenStream>(lexer_.get())),
        parser_(std::make_unique<PrestoSqlParser>(tokenStream_.get())) {
    lexer_->removeErrorListeners();
    lexer_->addErrorListener(&errorListener_);

    parser_->removeErrorListeners();
    parser_->addErrorListener(&errorListener_);
  }

  PrestoSqlParser& parser() const {
    return *parser_;
  }

  PrestoSqlParser::StatementContext* parse() const {
    auto ctx = parser_->statement();

    if (parser_->getNumberOfSyntaxErrors() > 0) {
      throw std::runtime_error(errorListener_.firstError);
    }

    return ctx;
  }

 private:
  std::unique_ptr<antlr4::ANTLRInputStream> inputStream_;
  std::unique_ptr<PrestoSqlLexer> lexer_;
  std::unique_ptr<antlr4::CommonTokenStream> tokenStream_;
  std::unique_ptr<PrestoSqlParser> parser_;
  detail::ErrorListener errorListener_;
};
} // namespace axiom::sql::presto
