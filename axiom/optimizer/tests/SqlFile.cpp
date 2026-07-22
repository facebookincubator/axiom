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

#include "axiom/optimizer/tests/SqlFile.h"

#include <folly/FileUtil.h>
#include <filesystem>
#include <sstream>
#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::optimizer::test {

namespace {
const auto& typeNames() {
  static const folly::F14FastMap<QueryEntry::Type, std::string_view> kNames = {
      {QueryEntry::Type::kResults, "results"},
      {QueryEntry::Type::kOrdered, "ordered"},
      {QueryEntry::Type::kCount, "count"},
  };
  return kNames;
}
} // namespace

AXIOM_DEFINE_EMBEDDED_ENUM_NAME(QueryEntry, Type, typeNames)

namespace {

// Returns leading-whitespace-trimmed view of 'sv'.
std::string_view ltrim(std::string_view sv) {
  size_t i = 0;
  while (i < sv.size() && (sv[i] == ' ' || sv[i] == '\t')) {
    ++i;
  }
  return sv.substr(i);
}

// Trims trailing whitespace and newlines from 's' in place.
void rtrim(std::string& s) {
  while (!s.empty() &&
         (s.back() == '\n' || s.back() == ' ' || s.back() == '\t' ||
          s.back() == '\r')) {
    s.pop_back();
  }
}

// Splits 'content' on '----' separator lines, trims each statement, and
// appends non-empty results to 'statements'.
void splitOnSeparator(
    std::string_view content,
    std::vector<std::string>& statements) {
  std::istringstream stream{std::string(content)};
  std::string line;
  std::string current;

  auto flush = [&]() {
    rtrim(current);
    if (!current.empty()) {
      statements.push_back(std::move(current));
    }
    current.clear();
  };

  while (std::getline(stream, line)) {
    if (line == "----") {
      flush();
      continue;
    }
    if (!current.empty()) {
      current += '\n';
    }
    current += line;
  }
  flush();
}

// Reads the entire contents of 'path'. Throws if the file cannot be read.
std::string readFileOrThrow(const std::string& path) {
  std::string content;
  VELOX_USER_CHECK(
      folly::readFile(path.c_str(), content),
      "Failed to read setup file: {}",
      path);
  return content;
}

// Parses queries from 'content' (after any setup directives have been
// stripped). 'lineOffset' is the 1-based line number of the first line of
// 'content' in the original source file, used so QueryEntry::lineNumber
// reflects the source position.
std::vector<QueryEntry> parseQueries(
    std::string_view content,
    int32_t lineOffset) {
  std::vector<QueryEntry> entries;
  std::istringstream stream{std::string(content)};
  std::string line;

  QueryEntry current;
  std::string sqlLines;
  int32_t lineNumber = lineOffset - 1;
  int32_t sqlStartLine = 0;
  bool disabled = false;

  auto finishEntry = [&]() {
    while (!sqlLines.empty() &&
           (sqlLines.back() == '\n' || sqlLines.back() == ' ')) {
      sqlLines.pop_back();
    }

    if (!sqlLines.empty() && !disabled) {
      // `-- columns` checks output column names, so it needs a result set from
      // at least one optimizer: reject it only for a count query or one that
      // fails in both v1 and v2.
      VELOX_CHECK(
          !current.checkColumnNames ||
              ((current.type == QueryEntry::Type::kResults ||
                current.type == QueryEntry::Type::kOrdered) &&
               !current.expectError()),
          "'-- columns' can only be used with 'results' or 'ordered' queries that succeed in at least one optimizer at line {}",
          sqlStartLine);

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

    if (sqlLines.empty() && (line.empty() || line == "--")) {
      continue;
    }
    if (sqlLines.empty() && line.size() >= 3 && line.substr(0, 3) == "-- ") {
      auto annotation = line.substr(3);

      VELOX_USER_CHECK(
          !annotation.starts_with("setup") &&
              !annotation.starts_with("end_setup"),
          "Setup directives must appear before the first query (line {}): {}",
          lineNumber,
          line);

      if (annotation == "ordered") {
        current.type = QueryEntry::Type::kOrdered;
      } else if (annotation == "disabled") {
        disabled = true;
      } else if (annotation.substr(0, 6) == "count ") {
        current.type = QueryEntry::Type::kCount;
        current.expectedCount = std::stoull(annotation.substr(6));
      } else if (annotation.substr(0, 7) == "error: ") {
        VELOX_USER_CHECK(
            current.expectedErrorV1.empty() && current.expectedErrorV2.empty(),
            "-- error: cannot be combined with -- error_v1:/-- error_v2: (line {})",
            lineNumber);
        current.expectedError = annotation.substr(7);
        current.expectedErrorV1 = current.expectedError;
        current.expectedErrorV2 = current.expectedError;
      } else if (annotation.substr(0, 10) == "error_v1: ") {
        VELOX_USER_CHECK(
            current.expectedError.empty(),
            "-- error_v1: cannot be combined with -- error: (line {})",
            lineNumber);
        VELOX_USER_CHECK(
            current.expectedErrorV1.empty(),
            "duplicate -- error_v1: (line {})",
            lineNumber);
        current.expectedErrorV1 = annotation.substr(10);
      } else if (annotation.substr(0, 10) == "error_v2: ") {
        VELOX_USER_CHECK(
            current.expectedError.empty(),
            "-- error_v2: cannot be combined with -- error: (line {})",
            lineNumber);
        VELOX_USER_CHECK(
            current.expectedErrorV2.empty(),
            "duplicate -- error_v2: (line {})",
            lineNumber);
        current.expectedErrorV2 = annotation.substr(10);
      } else if (annotation.substr(0, 8) == "duckdb: ") {
        current.duckDbSql = annotation.substr(8);
      } else if (annotation == "columns") {
        current.checkColumnNames = true;
      }
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

} // namespace

SqlFile SqlFile::parse(
    std::string_view content,
    std::string_view baseDir,
    const Options& options) {
  SqlFile result;

  std::istringstream stream{std::string(content)};
  std::string line;
  size_t bytesConsumed = 0;
  bool inSetupBlock = false;
  std::string setupBlockBody;
  bool sawNonSetupLine = false;

  // Precompute the "-- <name>:" prefix for each custom directive so the scan
  // loop doesn't rebuild them per line.
  std::vector<std::pair<std::string, std::string>> customPrefixes;
  customPrefixes.reserve(options.customSetupDirectives.size());
  for (const auto& name : options.customSetupDirectives) {
    customPrefixes.emplace_back("-- " + name + ":", name);
  }

  // Scan the top of the content for setup directives. Stop at the first
  // non-directive line outside a setup block.
  while (!sawNonSetupLine && std::getline(stream, line)) {
    const auto lineEnd = static_cast<size_t>(stream.tellg());
    const auto trimmed = ltrim(line);

    if (inSetupBlock) {
      if (trimmed == "-- end_setup") {
        splitOnSeparator(setupBlockBody, result.setupStatements);
        setupBlockBody.clear();
        inSetupBlock = false;
        bytesConsumed = lineEnd;
        continue;
      }
      if (!setupBlockBody.empty()) {
        setupBlockBody += '\n';
      }
      setupBlockBody += line;
      bytesConsumed = lineEnd;
      continue;
    }

    if (trimmed.empty() || trimmed == "--") {
      bytesConsumed = lineEnd;
      continue;
    }

    if (trimmed.starts_with("-- connector:")) {
      auto name = std::string(ltrim(trimmed.substr(13)));
      rtrim(name);
      if (name == "test") {
        result.connector = TestConnectorKind::kTest;
      } else if (name == "hive") {
        result.connector = TestConnectorKind::kLocalHive;
      } else {
        VELOX_USER_FAIL(
            "Unknown connector directive '{}' (expected 'test' or 'hive')",
            name);
      }
      bytesConsumed = lineEnd;
      continue;
    }

    if (trimmed.starts_with("-- setup_file:")) {
      auto refPath = std::string(ltrim(trimmed.substr(15)));
      VELOX_USER_CHECK(!refPath.empty(), "setup_file directive missing path");
      VELOX_USER_CHECK(
          !baseDir.empty(),
          "setup_file directive requires a non-empty baseDir");
      auto resolved = (std::filesystem::path(baseDir) / refPath).string();
      auto refContent = readFileOrThrow(resolved);
      splitOnSeparator(refContent, result.setupStatements);
      bytesConsumed = lineEnd;
      continue;
    }

    if (trimmed == "-- setup") {
      inSetupBlock = true;
      bytesConsumed = lineEnd;
      continue;
    }

    bool capturedCustom = false;
    for (const auto& [prefix, name] : customPrefixes) {
      if (trimmed.starts_with(prefix)) {
        auto value = std::string(ltrim(trimmed.substr(prefix.size())));
        rtrim(value);
        result.directives[name] = std::move(value);
        bytesConsumed = lineEnd;
        capturedCustom = true;
        break;
      }
    }
    if (capturedCustom) {
      continue;
    }

    // First non-directive line: end of setup region. Don't advance
    // bytesConsumed — including plain comments, which often carry
    // per-query annotations like '-- duckdb:' or '-- ordered' that the
    // query parser must see.
    sawNonSetupLine = true;
  }

  VELOX_USER_CHECK(
      !inSetupBlock, "Unterminated setup block (missing '-- end_setup')");

  // Count consumed lines so query line numbers reflect the original
  // source position.
  int32_t consumedLines = 0;
  for (size_t i = 0; i < bytesConsumed; ++i) {
    if (content[i] == '\n') {
      ++consumedLines;
    }
  }

  result.entries = parseQueries(
      content.substr(bytesConsumed), /*lineOffset=*/consumedLines + 1);
  return result;
}

} // namespace facebook::axiom::optimizer::test
