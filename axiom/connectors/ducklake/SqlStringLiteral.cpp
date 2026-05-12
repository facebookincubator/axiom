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

#include "axiom/connectors/ducklake/SqlStringLiteral.h"

namespace facebook::axiom::connector::ducklake {

std::string quoteSqlStringLiteral(std::string_view value) {
  static constexpr char kSingleQuote = '\'';

  std::string result;
  result.reserve(value.size() + 2);
  result.push_back(kSingleQuote);

  size_t previousPosition{0};
  size_t quotePosition = value.find(kSingleQuote);
  while (quotePosition != std::string_view::npos) {
    result.append(
        value.substr(previousPosition, quotePosition - previousPosition));
    result.push_back(kSingleQuote);
    result.push_back(kSingleQuote);
    previousPosition = quotePosition + 1;
    quotePosition = value.find(kSingleQuote, previousPosition);
  }

  result.append(value.substr(previousPosition));
  result.push_back(kSingleQuote);
  return result;
}

} // namespace facebook::axiom::connector::ducklake
