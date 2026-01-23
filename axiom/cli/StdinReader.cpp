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
#include <sstream>
#include "axiom/cli/linenoise/linenoise.h"
#include "folly/ScopeGuard.h"

namespace axiom::cli {

std::string readCommand(const std::string& prompt, bool& atEnd) {
  std::stringstream command;
  atEnd = false;

  bool stripLeadingSpaces = true;

  while (char* rawLine = linenoise(prompt.c_str())) {
    SCOPE_EXIT {
      if (rawLine != nullptr) {
        free(rawLine);
      }
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

    // Allow spaces after ';'.
    for (int64_t i = line.size() - 1; i >= startPos; --i) {
      if (std::isspace(line[i])) {
        continue;
      }

      if (line[i] == ';') {
        command << line.substr(startPos, i - startPos);
        linenoiseHistoryAdd(fmt::format("{};", command.str()).c_str());
        return command.str();
      }

      break;
    }

    stripLeadingSpaces = false;
    command << line.substr(startPos) << std::endl;
  }
  atEnd = true;
  return "";
}

} // namespace axiom::cli
