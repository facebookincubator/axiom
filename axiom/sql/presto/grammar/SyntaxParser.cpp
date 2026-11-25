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

#include "axiom/sql/presto/grammar/SyntaxParser.h"

#include "axiom/sql/DepthLimitListener.h"
#include "axiom/sql/SyntaxErrorListener.h"
#include "axiom/sql/UpperCaseInputStream.h"
#include "axiom/sql/presto/PrestoSqlError.h"
#include "axiom/sql/presto/grammar/PrestoSqlLexer.h"

namespace axiom::sql::presto {

namespace {

[[noreturn]] void failDepthLimit(const antlr4::Token& start) {
  static const std::string kMessage{"Expression exceeds maximum nesting depth"};
  throw PrestoSqlError(
      kMessage,
      start.getLine() - 1, // 0-based
      start.getCharPositionInLine(),
      /*token=*/std::nullopt,
      PrestoSqlErrorKind::kSemantic,
      kMessage);
}

} // namespace

struct SyntaxParser::Impl {
  Impl(std::string_view sql, uint32_t maxDepth)
      : input{sql},
        lexer{&input},
        tokens{&lexer},
        parser{&tokens},
        depthListener{maxDepth, failDepthLimit} {
    lexer.removeErrorListeners();
    lexer.addErrorListener(&errorListener);

    parser.removeErrorListeners();
    parser.addErrorListener(&errorListener);
    parser.addParseListener(&depthListener);

    // Use SLL prediction mode for faster parsing. SLL is much faster than LL
    // mode and works for most SQL queries. If SLL fails, we fall back to LL.
    parser.getInterpreter<antlr4::atn::ParserATNSimulator>()->setPredictionMode(
        antlr4::atn::PredictionMode::SLL);
  }

  // Each member reads from the one declared before it.
  UpperCaseInputStream input;
  PrestoSqlLexer lexer;
  antlr4::CommonTokenStream tokens;
  PrestoSqlParser parser;
  SyntaxErrorListener errorListener;
  DepthLimitListener depthListener;
};

SyntaxParser::SyntaxParser(std::string_view sql, uint32_t maxDepth)
    : impl_{std::make_unique<Impl>(sql, maxDepth)} {}

SyntaxParser::~SyntaxParser() = default;

PrestoSqlParser::StatementContext* SyntaxParser::parse() {
  // Try SLL mode first (fast path).
  try {
    auto ctx = impl_->parser.singleStatement();
    if (impl_->parser.getNumberOfSyntaxErrors() == 0) {
      return ctx->statement();
    }
  } catch (const PrestoSqlError&) {
    // Don't re-parse in LL, it would re-hit this semantic error.
    throw;
  } catch (const std::exception&) {
    // SLL mode failed, fall through to LL mode.
  }

  // Fall back to LL mode (slower but handles all valid SQL).
  impl_->tokens.seek(0);
  // Reset both the parser and errorListener so that errors from the SLL
  // attempt are discarded. Either the LL path succeeds and we return a
  // valid AST, or it fails and we capture fresh LL error info.
  impl_->parser.reset();
  impl_->errorListener.reset();
  impl_->depthListener.reset();
  impl_->parser.getInterpreter<antlr4::atn::ParserATNSimulator>()
      ->setPredictionMode(antlr4::atn::PredictionMode::LL);

  auto ctx = impl_->parser.singleStatement();
  if (impl_->parser.getNumberOfSyntaxErrors() > 0) {
    throw PrestoSqlError(
        impl_->errorListener.firstError(),
        impl_->errorListener.line(),
        impl_->errorListener.column(),
        impl_->errorListener.token(),
        PrestoSqlErrorKind::kSyntax,
        impl_->errorListener.firstError());
  }

  return ctx->statement();
}

} // namespace axiom::sql::presto
