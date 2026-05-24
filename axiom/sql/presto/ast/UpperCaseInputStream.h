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

#include <sys/types.h>
#include <cstddef>

#include <antlr4-runtime.h>

namespace axiom::sql::presto {
/**
 * ANTLRInputStream wrapper that case-folds ASCII letters so the lexer
 * matches keywords case-insensitively. Presto's keyword and unquoted-
 * identifier grammar is ASCII-only; non-ASCII codepoints pass through
 * unchanged.
 */
class UpperCaseInputStream final : public antlr4::ANTLRInputStream {
 public:
  explicit UpperCaseInputStream(std::string_view input)
      : antlr4::ANTLRInputStream(input) {}

  size_t LA(ssize_t i) override {
    size_t c = antlr4::ANTLRInputStream::LA(i);
    if (c - 'a' < kNumAsciiLowerLetters) {
      return c - ('a' - 'A');
    }
    return c;
  }

 private:
  static constexpr size_t kNumAsciiLowerLetters = 'z' - 'a' + 1;
};

} // namespace axiom::sql::presto
