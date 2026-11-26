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

#include "axiom/sql/SyntaxErrorListener.h"

namespace axiom::sql {

void SyntaxErrorListener::syntaxError(
    antlr4::Recognizer* /*recognizer*/,
    antlr4::Token* offendingSymbol,
    size_t line,
    size_t charPositionInLine,
    const std::string& message,
    std::exception_ptr /*error*/) {
  if (firstError_.has_value()) {
    return;
  }
  line_ = line - 1;
  column_ = charPositionInLine;
  firstError_ = message;
  token_ =
      offendingSymbol != nullptr ? offendingSymbol->getText() : std::string();
}

} // namespace axiom::sql
