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
#include <sys/ioctl.h>
#include <unistd.h>
#include <algorithm>
#include <iostream>
#include <iterator>
#include <optional>
#include <set>
#include "axiom/cli/LiveProgressDisplay.h"
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
DEFINE_string(
    etc_dir,
    "",
    "Path to a directory of connector .properties files. Mutually exclusive with `--data_path`.");
DEFINE_uint64(
    split_target_bytes,
    16 << 20,
    "Approx bytes covered by one split");

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

DEFINE_bool(
    print_timing,
    false,
    "Print per-statement timing (parse / optimize / execute). Always on in "
    "interactive mode.");

DEFINE_bool(
    show_live_progress,
    false,
    "Draw the live progress grid for --query and piped-stdin runs (when stderr "
    "is a terminal). The interactive REPL always shows it; this only opts the "
    "non-interactive paths in.");

DEFINE_int32(
    repeat,
    1,
    "Run the --query input this many times back-to-back. Useful for perf "
    "measurements, re-running flaky queries, or queries with "
    "non-deterministic results. The input must be a single statement. "
    "Does not apply to --init. Stops on the first error.");

using namespace facebook::velox;

namespace {
// Terminal width assumed when the real width cannot be queried (output is not a
// tty or the ioctl fails).
constexpr int kDefaultTerminalWidth = 80;

// Returns the terminal width for `fileDescriptor`, or kDefaultTerminalWidth if
// it cannot be determined. Re-queried per progress frame so the grid tracks a
// live terminal resize.
int terminalWidth(int fileDescriptor) {
  struct winsize windowSize{};
  if (ioctl(fileDescriptor, TIOCGWINSZ, &windowSize) == 0 &&
      windowSize.ws_col > 0) {
    return windowSize.ws_col;
  }
  return kDefaultTerminalWidth;
}

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
  gflags::CommandLineFlagInfo repeatInfo;
  gflags::GetCommandLineFlagInfo("repeat", &repeatInfo);

  VELOX_USER_CHECK_GE(FLAGS_repeat, 1, "--repeat must be at least 1");

  if (!FLAGS_init.empty()) {
    std::string sql;
    auto success = folly::readFile(FLAGS_init.c_str(), sql);
    VELOX_USER_CHECK(success, "Cannot open init file: {}", FLAGS_init);
    runMultiple(sql, FLAGS_print_timing, /*showProgress=*/false);
  }

  const bool interactive = isatty(STDIN_FILENO);

  // The live progress grid renders on stderr and is erased before results
  // print, so it needs stderr to be a terminal and the runner to report
  // progress. The interactive REPL shows it by default; the non-interactive
  // paths (--query, piped stdin) only opt in via --show_live_progress.
  const bool terminalProgress =
      isatty(STDERR_FILENO) != 0 && runner_.supportsProgress();

  // Pick the user-SQL source: --query first, then piped stdin.
  std::string userSql;
  if (!FLAGS_query.empty()) {
    userSql = FLAGS_query;
  } else if (!interactive) {
    userSql.assign(std::istreambuf_iterator<char>(std::cin), {});
  }

  if (!userSql.empty()) {
    if (repeatInfo.is_default) {
      runMultiple(
          userSql,
          FLAGS_print_timing,
          FLAGS_show_live_progress && terminalProgress);
    } else {
      // Explicit --repeat: treat the input as a single statement. Skip the
      // progress grid so its periodic redraws do not perturb the timing this
      // mode exists to measure.
      runRepeat(userSql, FLAGS_repeat, FLAGS_print_timing);
    }
    return;
  }

  // No user SQL: enter interactive REPL.
  VELOX_USER_CHECK(
      repeatInfo.is_default, "--repeat is not supported in interactive mode");
  if (interactive) {
    std::cout << "Axiom SQL. Type statement and end with ;.\n"
                 "Type .help for available commands."
              << std::endl;
  }
  readCommands("SQL> ", interactive || FLAGS_print_timing, terminalProgress);
}

namespace {
std::string formatTiming(
    const QueryTiming& timing,
    const cli::Timing& cpuTiming) {
  return fmt::format(
      "Parsing: {} | Optimizing: {} | Executing: {} | Total: {}",
      facebook::velox::succinctMicros(timing.parse),
      facebook::velox::succinctMicros(timing.optimize),
      facebook::velox::succinctMicros(timing.execute),
      cpuTiming.toString());
}
} // namespace

bool Console::runOnce(
    std::string_view sql,
    bool printTiming,
    bool showProgress) {
  QueryCompletionInfo completionInfo;

  SqlQueryRunner::RunOptions options{
      .numWorkers = FLAGS_num_workers,
      .numDrivers = FLAGS_num_drivers,
      .splitTargetBytes = FLAGS_split_target_bytes,
      .debugMode = FLAGS_debug,
      .onComplete =
          [&](const QueryCompletionInfo& info) { completionInfo = info; },
  };

  // When enabled by the caller, draw a live status grid on stderr while the
  // query runs.
  std::optional<cli::LiveProgressDisplay> progress;
  if (showProgress) {
    progress.emplace(
        std::cerr,
        cli::LiveProgressDisplay::Options{
            .showSplitAndCpuDetail = FLAGS_debug});
    options.onProgress =
        [&](const facebook::axiom::runner::QueryProgress& info) {
          progress->update(info, terminalWidth(STDERR_FILENO));
        };
  }

  cli::Timing cpuTiming;
  try {
    auto result = cli::time<SqlQueryRunner::SqlResult>(
        [&]() { return runner_.run(sql, options); }, cpuTiming);

    if (progress) {
      progress->clear();
    }

    if (result.message.has_value()) {
      std::cout << result.message.value() << std::endl;
    } else {
      if (FLAGS_debug && !result.results.empty()) {
        std::cout << result.results.front()->rowType()->toString() << std::endl;
      }
      cli::printResults(result.results, FLAGS_max_rows);
    }

    if (printTiming) {
      std::cout << "Query ID: " << completionInfo.startInfo.queryId << " | "
                << formatTiming(completionInfo.timing, cpuTiming) << std::endl;
    }
    return true;
  } catch (const std::exception&) {
    if (progress) {
      progress->clear();
    }
    // run() threw after firing the completion callback, so telemetry was
    // captured.
    std::cerr << "Query failed: " << completionInfo.errorInfo->message
              << std::endl;
    if (printTiming) {
      std::cerr << "Query ID: " << completionInfo.startInfo.queryId << " | "
                << formatTiming(completionInfo.timing, cpuTiming) << std::endl;
    }
    return false;
  }
}

void Console::runMultiple(
    std::string_view sql,
    bool printTiming,
    bool showProgress) {
  // Parse and execute statements one at a time so that DDL statements
  // (e.g. CREATE TABLE) take effect before subsequent statements (e.g.
  // INSERT) are parsed.
  for (const auto& sqlText : runner_.splitStatements(sql)) {
    if (sqlText.empty()) {
      continue;
    }
    if (!runOnce(sqlText, printTiming, showProgress)) {
      return;
    }
  }
}

void Console::runRepeat(std::string_view sql, int repeat, bool printTiming) {
  for (int i = 0; i < repeat; ++i) {
    if (!runOnce(sql, printTiming, /*showProgress=*/false)) {
      return;
    }
  }
}

void Console::readCommands(
    const std::string& prompt,
    bool printTiming,
    bool showProgress) {
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
        runMultiple(command, printTiming, showProgress);
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
          "  max_rows         - Maximum number of printed result rows.\n\n";

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
      runMultiple(sql, printTiming, showProgress);
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

    runMultiple(command, printTiming, showProgress);
  }
}
} // namespace axiom::sql
