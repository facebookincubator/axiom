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

#include <exception>
#include <string>

namespace axiom::sql::presto {

/// Exception thrown when parsing a Presto SQL query fails.
class PrestoParseError : public std::exception {
 public:
  PrestoParseError(
      const std::string& message,
      size_t line,
      size_t column,
      std::string token)
      : formatted_(
            "Syntax error at " + std::to_string(line) + ":" +
            std::to_string(column) + ": " + message),
        messageLength_(message.size()),
        line_(line),
        column_(column),
        token_(std::move(token)) {}

  /// 0-based line number.
  size_t line() const {
    return line_;
  }

  /// 0-based column number.
  size_t column() const {
    return column_;
  }

  /// Offending token text. May be empty.
  const std::string& token() const {
    return token_;
  }

  /// Raw error description from the parser, without the line:column prefix.
  std::string_view message() const noexcept {
    return std::string_view(formatted_)
        .substr(formatted_.size() - messageLength_);
  }

  const char* what() const noexcept override {
    return formatted_.data();
  }

  /// Returns a copy with adjusted line and column.
  PrestoParseError withOffset(size_t lineOffset, size_t columnOffset) const {
    return PrestoParseError(
        std::string(message()),
        line_ + lineOffset,
        column_ + columnOffset,
        token_);
  }

 private:
  std::string formatted_;
  size_t messageLength_;
  size_t line_;
  size_t column_;
  std::string token_;
};

} // namespace axiom::sql::presto
