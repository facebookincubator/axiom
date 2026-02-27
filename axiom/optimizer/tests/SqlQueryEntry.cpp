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

#include "axiom/optimizer/tests/SqlQueryEntry.h"

#include <sstream>

namespace facebook::axiom::optimizer::test {

namespace {
const auto& typeNames() {
  static const folly::F14FastMap<QueryEntry::Type, std::string_view> kNames = {
      {QueryEntry::Type::kResults, "results"},
      {QueryEntry::Type::kOrdered, "ordered"},
      {QueryEntry::Type::kCount, "count"},
      {QueryEntry::Type::kError, "error"},
  };
  return kNames;
}
} // namespace

AXIOM_DEFINE_EMBEDDED_ENUM_NAME(QueryEntry, Type, typeNames)

std::vector<QueryEntry> QueryEntry::parse(const std::string& content) {
  std::vector<QueryEntry> entries;
  std::istringstream stream(content);
  std::string line;

  QueryEntry current;
  std::string sqlLines;
  // Track the line number in the file (1-based).
  int32_t lineNumber = 0;
  int32_t sqlStartLine = 0;
  bool disabled = false;

  auto finishEntry = [&]() {
    // Trim trailing whitespace from SQL.
    while (!sqlLines.empty() &&
           (sqlLines.back() == '\n' || sqlLines.back() == ' ')) {
      sqlLines.pop_back();
    }

    if (!sqlLines.empty() && !disabled) {
      current.sql = std::move(sqlLines);
      current.lineNumber = sqlStartLine;
      entries.push_back(std::move(current));
    }

    current = QueryEntry{};
    sqlLines.clear();
    sqlStartLine = 0;
    disabled = false;
  };

  while (std::getline(stream, line)) {
    ++lineNumber;

    if (line == "----") {
      finishEntry();
      continue;
    }

    // Check for annotation comments before the SQL starts.
    if (sqlLines.empty() && line.size() >= 3 && line.substr(0, 3) == "-- ") {
      auto annotation = line.substr(3);

      if (annotation == "ordered") {
        current.type = Type::kOrdered;
      } else if (annotation == "disabled") {
        disabled = true;
      } else if (annotation.substr(0, 6) == "count ") {
        current.type = Type::kCount;
        current.expectedCount = std::stoull(annotation.substr(6));
      } else if (annotation.substr(0, 7) == "error: ") {
        current.type = Type::kError;
        current.expectedError = annotation.substr(7);
      } else if (annotation.substr(0, 8) == "duckdb: ") {
        current.duckDbSql = annotation.substr(8);
      }
      // Ignore unrecognized annotations (they may be regular SQL comments).
      continue;
    }

    if (sqlLines.empty()) {
      sqlStartLine = lineNumber;
    }

    if (!sqlLines.empty()) {
      sqlLines += '\n';
    }
    sqlLines += line;
  }

  finishEntry();
  return entries;
}

} // namespace facebook::axiom::optimizer::test
