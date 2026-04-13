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

#include "axiom/cli/Console.h"
#include <fmt/core.h>
#include <folly/FileUtil.h>
#include <unistd.h>
#include <algorithm>
#include <iostream>
#include <optional>
#include <set>
#include "axiom/cli/ResultPrinter.h"
#include "axiom/cli/StdinReader.h"
#include "axiom/cli/Timing.h"
#include "axiom/cli/linenoise/linenoise.h"
#include "velox/common/base/SuccinctPrinter.h"

DEFINE_string(
    data_path,
    "",
    "Root path of data. Data layout must follow Hive-style partitioning. ");
DEFINE_string(data_format, "parquet", "Data format: parquet or dwrf.");
DEFINE_uint64(
    split_target_bytes,
    16 << 20,
    "Approx bytes covered by one split");

DEFINE_uint32(optimizer_trace, 0, "Optimizer trace level");

DEFINE_int32(max_rows, 100, "Max number of printed result rows");

DEFINE_int32(num_workers, 4, "Number of in-process workers");
DEFINE_int32(num_drivers, 4, "Number of drivers per worker");

DEFINE_string(
    query,
    "",
    "Text of query. If empty, reads ';' separated queries from standard input");

DEFINE_string(
    init,
    "",
    "Path to a SQL file to execute on startup before entering interactive mode or running --query");

DEFINE_bool(debug, false, "Enable debug mode");

using namespace facebook::velox;

namespace {
// Extracts a file path argument from a dot-command string like ".run <file>".
// Returns the trimmed path, or empty string if none.
std::string parseDotCommandPath(const std::string& command, size_t prefixLen) {
  if (command.size() <= prefixLen) {
    return {};
  }
  auto filePath = command.substr(prefixLen);
  auto start = filePath.find_first_not_of(" \t\n\r");
  auto end = filePath.find_last_not_of(" \t\n\r");
  if (start == std::string::npos) {
    return {};
  }
  return filePath.substr(start, end - start + 1);
}

// Returns the path to the persistent history file, or std::nullopt if HOME is
// unset.
std::optional<std::string> getHistoryFilePath() {
  const char* home = getenv("HOME");
  if (home == nullptr) {
    return std::nullopt;
  }
  return std::string(home) + "/.axiom_cli.history";
}
} // namespace

namespace axiom::sql {

Console::Console(SqlQueryRunner& runner) : runner_{runner} {}

void Console::initialize() {
  gflags::SetUsageMessage(
      "Axiom local SQL command line. "
      "Run 'axiom_sql --help' for available options.\n");

  // Disable logging to stderr if not in debug mode.
  FLAGS_logtostderr = FLAGS_debug;
}

void Console::run() {
  if (!FLAGS_init.empty()) {
    std::string sql;
    auto success = folly::readFile(FLAGS_init.c_str(), sql);
    VELOX_USER_CHECK(success, "Cannot open init file: {}", FLAGS_init);
    runNoThrow(sql, false);
  }

  if (!FLAGS_query.empty()) {
    runNoThrow(FLAGS_query, false);
  } else {
    const bool interactive = isatty(STDIN_FILENO);
    if (interactive) {
      std::cout << "Axiom SQL. Type statement and end with ;.\n"
                   "Type .help for available commands."
                << std::endl;
    }
    readCommands("SQL> ", interactive);
  }
}

void Console::runNoThrow(std::string_view sql, bool isInteractive) {
  // Parse and execute statements one at a time so that DDL statements
  // (e.g. CREATE TABLE) take effect before subsequent statements (e.g.
  // INSERT) are parsed.
  for (const auto& sqlText : runner_.splitStatements(sql)) {
    if (sqlText.empty()) {
      continue;
    }

    // The completion callback captures telemetry for error reporting.
    QueryCompletionInfo completionInfo;

    SqlQueryRunner::RunOptions options{
        .numWorkers = FLAGS_num_workers,
        .numDrivers = FLAGS_num_drivers,
        .splitTargetBytes = FLAGS_split_target_bytes,
        .optimizerTraceFlags = FLAGS_optimizer_trace,
        .debugMode = FLAGS_debug,
        .onComplete =
            [&](const QueryCompletionInfo& info) { completionInfo = info; },
    };

    auto formatTiming = [](const QueryTiming& timing,
                           const cli::Timing& cpuTiming) {
      return fmt::format(
          "Parsing: {} | Optimizing: {} | Executing: {} | Total: {}",
          facebook::velox::succinctMicros(timing.parse),
          facebook::velox::succinctMicros(timing.optimize),
          facebook::velox::succinctMicros(timing.execute),
          cpuTiming.toString());
    };

    cli::Timing cpuTiming;
    try {
      auto result = cli::time<SqlQueryRunner::SqlResult>(
          [&]() { return runner_.run(sqlText, options); }, cpuTiming);

      if (result.message.has_value()) {
        std::cout << result.message.value() << std::endl;
      } else {
        if (FLAGS_debug && !result.results.empty()) {
          std::cout << result.results.front()->rowType()->toString()
                    << std::endl;
        }
        cli::printResults(result.results, FLAGS_max_rows);
      }

      if (isInteractive) {
        std::cout << "Query ID: " << completionInfo.startInfo.queryId << " | "
                  << formatTiming(completionInfo.timing, cpuTiming)
                  << std::endl;
      }
    } catch (const std::exception&) {
      // run() threw after firing the completion callback, so telemetry
      // was captured.
      std::cerr << "Query failed: " << completionInfo.errorInfo->message
                << std::endl;
      if (isInteractive) {
        std::cerr << "Query ID: " << completionInfo.startInfo.queryId << " | "
                  << formatTiming(completionInfo.timing, cpuTiming)
                  << std::endl;
      }
      return;
    }
  }
}

void Console::readCommands(const std::string& prompt, bool interactive) {
  linenoiseSetMultiLine(1);
  linenoiseHistorySetMaxLen(1024);

  auto historyFile = getHistoryFilePath();
  if (historyFile.has_value()) {
    linenoiseHistoryLoad(historyFile->c_str());
  }

  std::set<std::string> modifiedFlags;

  for (;;) {
    bool atEnd;
    std::string command = cli::readCommand(prompt, atEnd);
    if (atEnd) {
      if (!command.empty()) {
        runNoThrow(command, interactive);
      }
      break;
    }

    if (command.empty()) {
      continue;
    }

    // Save history after each command is added.
    if (historyFile.has_value()) {
      linenoiseHistorySave(historyFile->c_str());
    }

    if (command.starts_with(".exit") || command.starts_with(".quit")) {
      break;
    }

    if (command.starts_with(".help")) {
      static const char* helpText =
          "Axiom Interactive SQL\n\n"
          "Type SQL and end with ';'. Dot-commands do not require ';'.\n\n"
          "Commands:\n\n"
          "  .help              - Show this help text.\n"
          "  .run <file>        - Execute SQL statements from a file.\n"
          "  .set <name> <val>  - Set a gflag at runtime.\n"
          "  .clear <name>      - Reset a flag to its default value.\n"
          "  .flags             - Show all modified flags.\n"
          "  .exit / .quit      - Exit the CLI.\n\n"
          "Useful flags:\n\n"
          "  num_workers      - Number of workers for distributed plans (1 = single node).\n"
          "  num_drivers      - Number of drivers (threads) per pipeline per worker.\n"
          "  max_rows         - Maximum number of printed result rows.\n"
          "  optimizer_trace  - Optimizer trace level (0 = off).\n\n";

      std::cout << helpText << std::flush;
      continue;
    }

    if (command.starts_with(".run")) {
      auto filePath = parseDotCommandPath(command, 4);
      if (filePath.empty()) {
        std::cerr << "Usage: .run <file>" << std::endl;
        continue;
      }

      std::string sql;
      if (!folly::readFile(filePath.c_str(), sql)) {
        std::cerr << "Cannot open file: " << filePath << std::endl;
        continue;
      }
      runNoThrow(sql, interactive);
      continue;
    }

    if (command.starts_with(".set")) {
      auto args = parseDotCommandPath(command, 4);
      char* flag = nullptr;
      char* value = nullptr;
      SCOPE_EXIT {
        if (flag != nullptr) {
          free(flag);
        }
        if (value != nullptr) {
          free(value);
        }
      };
      if (sscanf(args.c_str(), "%ms %ms", &flag, &value) != 2) {
        std::cerr << "Usage: .set <flag_name> <value>" << std::endl;
        continue;
      }
      auto message = gflags::SetCommandLineOption(flag, value);
      if (!message.empty()) {
        std::cout << message << std::flush;
        modifiedFlags.insert(std::string(flag));
      } else {
        std::cerr << "Failed to set flag '" << flag << "' to '" << value << "'"
                  << std::endl;
      }
      continue;
    }

    if (command.starts_with(".clear")) {
      auto flagName = parseDotCommandPath(command, 6);
      if (flagName.empty()) {
        std::cerr << "Usage: .clear <flag_name>" << std::endl;
        continue;
      }
      gflags::CommandLineFlagInfo info;
      if (!gflags::GetCommandLineFlagInfo(flagName.c_str(), &info)) {
        std::cerr << "Failed to clear flag '" << flagName << "'" << std::endl;
        continue;
      }
      auto message = gflags::SetCommandLineOption(
          flagName.c_str(), info.default_value.c_str());
      if (!message.empty()) {
        std::cout << message << std::flush;
        modifiedFlags.erase(flagName);
      }
      continue;
    }

    if (command.starts_with(".flags")) {
      // Show CLI-relevant flags with current values and default markers.
      // Flags that take effect on each query execution.
      static const std::vector<std::string> kFlagNames = {
          "num_workers",
          "num_drivers",
          "max_rows",
          "optimizer_trace",
      };
      for (const auto& name : kFlagNames) {
        gflags::CommandLineFlagInfo info;
        if (gflags::GetCommandLineFlagInfo(name.c_str(), &info)) {
          std::cout << "  " << name << " = " << info.current_value;
          if (info.current_value == info.default_value) {
            std::cout << " (default)";
          }
          std::cout << std::endl;
        }
      }
      continue;
    }

    if (command.starts_with(".")) {
      std::cerr << "Unknown command: " << command
                << ". Type .help for available commands." << std::endl;
      continue;
    }

    runNoThrow(command, interactive);
  }
}
} // namespace axiom::sql
