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
/// The message contains the detailed error message from the parser.
class PrestoParseError : public std::exception {
 public:
  /* implicit */ PrestoParseError(std::string_view message)
      : message_(message) {}

  std::string_view message() const noexcept {
    return message_;
  }

  const char* what() const noexcept override {
    return message_.data();
  }

 private:
  std::string message_;
};

} // namespace axiom::sql::presto
