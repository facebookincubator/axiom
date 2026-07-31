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

#include "axiom/cli/StdinReader.h"
#include <fmt/format.h>
#include <algorithm>
#include <cerrno>
#include <sstream>
#include "axiom/cli/linenoise/linenoise.h"
#include "folly/ScopeGuard.h"

namespace axiom::cli {

namespace {
std::string collapseWhitespace(std::string str) {
  std::replace_if(str.begin(), str.end(), ::isspace, ' ');
  str.erase(
      std::unique(
          str.begin(),
          str.end(),
          [](char a, char b) { return a == ' ' && b == ' '; }),
      str.end());
  return str;
}

// Update block comment state by scanning for /* and */ in the line.
// Returns true if the end of the line is inside a block comment.
bool updateBlockCommentState(const std::string& line, bool inBlockComment) {
  for (size_t i = 0; i < line.size(); ++i) {
    if (inBlockComment) {
      if (line[i] == '*' && i + 1 < line.size() && line[i + 1] == '/') {
        inBlockComment = false;
        ++i;
      }
    } else {
      if (line[i] == '/' && i + 1 < line.size() && line[i + 1] == '*') {
        inBlockComment = true;
        ++i;
      } else if (line[i] == '-' && i + 1 < line.size() && line[i + 1] == '-') {
        // Rest of line is a single-line comment.
        break;
      }
    }
  }
  return inBlockComment;
}

// Returns true if the trailing semicolon (at position semicolonPos) is inside
// a single-line comment (-- ...) or a block comment that was opened on this
// line and not yet closed.
bool isSemicolonInLineComment(
    const std::string& line,
    int64_t startPos,
    int64_t semicolonPos) {
  for (int64_t i = startPos; i < semicolonPos; ++i) {
    if (line[i] == '-' && i + 1 < semicolonPos && line[i + 1] == '-') {
      return true;
    }
  }
  return false;
}
} // namespace

std::string readCommand(const std::string& prompt, bool& atEnd) {
  std::stringstream command;
  atEnd = false;

  bool stripLeadingSpaces = true;
  bool inBlockComment = false;

  for (;;) {
    errno = 0;
    char* rawLine = linenoise(prompt.c_str());
    if (rawLine == nullptr) {
      // linenoise runs the terminal in raw mode with ISIG cleared, so Ctrl+C is
      // not a signal here -- it returns nullptr with errno == EAGAIN (Ctrl+D
      // and EOF set ENOENT). Map Ctrl+C to "discard the partial command and
      // re-prompt" (like readline/psql); Ctrl+D and EOF end input.
      if (errno == EAGAIN) {
        command.str("");
        command.clear();
        stripLeadingSpaces = true;
        inBlockComment = false;
        continue;
      }
      break;
    }
    SCOPE_EXIT {
      free(rawLine);
    };

    std::string line(rawLine);

    int64_t startPos = 0;
    if (stripLeadingSpaces) {
      for (; startPos < line.size(); ++startPos) {
        if (std::isspace(line[startPos])) {
          continue;
        }
        break;
      }
    }

    if (startPos == line.size()) {
      continue;
    }

    // Dot-commands don't require a ';' terminator. Return immediately
    // when the first non-whitespace character is '.'.
    if (stripLeadingSpaces && line[startPos] == '.') {
      auto result = line.substr(startPos);
      // Strip trailing ';' and whitespace if present, for consistency
      // with how SQL commands are returned.
      auto end = result.find_last_not_of(" \t;");
      if (end != std::string::npos) {
        result = result.substr(0, end + 1);
      }
      linenoiseHistoryAdd(line.c_str());
      return result;
    }

    bool lineEndInComment = updateBlockCommentState(line, inBlockComment);

    // Only check for terminal ';' if not inside a block comment.
    if (!lineEndInComment) {
      // Allow spaces after ';'.
      for (int64_t i = line.size() - 1; i >= startPos; --i) {
        if (std::isspace(line[i])) {
          continue;
        }

        if (line[i] == ';' && !isSemicolonInLineComment(line, startPos, i)) {
          command << line.substr(startPos, i - startPos);
          auto history = collapseWhitespace(command.str());
          linenoiseHistoryAdd(fmt::format("{};", history).c_str());
          return command.str();
        }

        break;
      }
    }

    inBlockComment = lineEndInComment;
    stripLeadingSpaces = false;
    command << line.substr(startPos) << std::endl;
  }
  atEnd = true;
  return command.str();
}

} // namespace axiom::cli
