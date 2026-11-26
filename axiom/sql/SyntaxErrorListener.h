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

#include <optional>
#include <string>

#include <antlr4-runtime.h>

namespace axiom::sql {

/// Records the first syntax error an ANTLR lexer or parser reports, so the
/// caller can fail with the error type of its dialect.
///
///   SyntaxErrorListener errors;
///   parser.removeErrorListeners();
///   parser.addErrorListener(&errors);
///   parser.singleStatement();
///   if (errors.hasError()) {
///     // Fail with errors.firstError(), errors.line(), errors.column().
///   }
class SyntaxErrorListener : public antlr4::BaseErrorListener {
 public:
  void syntaxError(
      antlr4::Recognizer* recognizer,
      antlr4::Token* offendingSymbol,
      size_t line,
      size_t charPositionInLine,
      const std::string& message,
      std::exception_ptr error) override;

  /// Forgets the recorded error, to parse again.
  void reset() {
    firstError_.reset();
  }

  bool hasError() const {
    return firstError_.has_value();
  }

  /// ANTLR's description of the first error. Requires hasError().
  const std::string& firstError() const {
    return firstError_.value();
  }

  /// 0-based line of the first error.
  size_t line() const {
    return line_;
  }

  /// 0-based column of the first error.
  size_t column() const {
    return column_;
  }

  /// Text of the token at the first error; empty for a lexer error.
  const std::string& token() const {
    return token_;
  }

 private:
  std::optional<std::string> firstError_;
  size_t line_{};
  size_t column_{};
  std::string token_;
};

} // namespace axiom::sql
