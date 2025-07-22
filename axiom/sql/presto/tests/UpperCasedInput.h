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

#include <sys/types.h>
#include <cstddef>

#include <antlr4-runtime/antlr4-runtime.h>

namespace facebook::velox::sql {

/// This class is a thin wrapper around ANTLRInputStream to allow streams to be
/// case-insensitive by always forcing the stream to be upper case. This is done
/// by wrapping the look-ahead and checking if the value returned != 0. For now,
/// we uppercase all; however, we may need to reconsider EOF or use sophisicated
/// unicode when supported.
class UpperCasedInput final : public antlr4::ANTLRInputStream {
 public:
  explicit UpperCasedInput(const std::string& input)
      : antlr4::ANTLRInputStream(input) {}

  /// Force the lookahead function casing to be upper case.
  size_t LA(ssize_t i) override {
    size_t c = antlr4::ANTLRInputStream::LA(i);
    if (c == 0) {
      return c;
    }
    // ANTLRInputStream consumes the input and on LA, we want to convert the
    // input symbol to be upper case so that grammars can be case insensitive.
    return toupper(c);
  }
};

} // namespace facebook::velox::sql
