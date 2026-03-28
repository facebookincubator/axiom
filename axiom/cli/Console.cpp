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
#include <folly/FileUtil.h>
#include <unistd.h>
#include <iostream>
#include <optional>
#include <set>
#include "axiom/cli/QueryIdGenerator.h"
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

Console::Console(
    SqlQueryRunner& runner,
    PermissionCheck permissionCheck,
    std::shared_ptr<cli::QueryIdGenerator> queryIdGenerator,
    QueryStartCallback startCallback,
    QueryCompletionCallback completionCallback)
    : runner_{runner},
      permissionCheck_{std::move(permissionCheck)},
      queryIdGenerator_{
          queryIdGenerator ? std::move(queryIdGenerator)
                           : std::make_shared<cli::QueryIdGenerator>()},
      startCallback_{std::move(startCallback)},
      completionCallback_{std::move(completionCallback)} {}

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
                   "flag name = value; sets a gflag.\n"
                   "help; prints help text."
                << std::endl;
    }
    readCommands("SQL> ", interactive);
  }
}

void Console::notifyStart(const QueryStartInfo& info) {
  if (startCallback_) {
    try {
      startCallback_(info);
    } catch (const std::exception& ex) {
      LOG(WARNING) << "Start callback failed: " << ex.what();
    }
  }
}

void Console::notifyCompletion(const QueryCompletionInfo& info) {
  if (completionCallback_) {
    try {
      completionCallback_(info);
    } catch (const std::exception& ex) {
      LOG(WARNING) << "Completion callback failed: " << ex.what();
    }
  }
}

void Console::runNoThrow(std::string_view sql, bool isInteractive) {
  const SqlQueryRunner::RunOptions defaultOptions{
      .numWorkers = FLAGS_num_workers,
      .numDrivers = FLAGS_num_drivers,
      .splitTargetBytes = FLAGS_split_target_bytes,
      .optimizerTraceFlags = FLAGS_optimizer_trace,
      .debugMode = FLAGS_debug,
  };

  // Parse and execute statements one at a time so that DDL statements
  // (e.g. CREATE TABLE) take effect before subsequent statements (e.g.
  // INSERT) are parsed.
  for (const auto& sqlText : runner_.splitStatements(sql)) {
    if (sqlText.empty()) {
      continue;
    }

    const auto queryId = queryIdGenerator_->createNextQueryId();
    auto options = defaultOptions;
    options.queryId = queryId;
    const auto createTime = std::chrono::system_clock::now();

    // Notify start callback before parse so that every query gets a start
    // event. Parse or permission failures are handled by completionCallback_
    // in the catch block.
    notifyStart({queryId, std::string(sqlText), createTime});

    try {
      cli::Timing parseTiming;
      cli::Timing statementTiming;
      cli::Timing totalTiming;
      auto result = cli::time<SqlQueryRunner::SqlResult>(
          [&]() {
            auto statement = cli::time<presto::SqlStatementPtr>(
                [&]() { return runner_.parseSingle(sqlText, options); },
                parseTiming);

            if (isInteractive) {
              std::cout << "Query ID: " << queryId
                        << " | Parsing: " << parseTiming.toString()
                        << std::endl;
            }

            // Permission check after parsing, before execution.
            if (permissionCheck_) {
              const auto& schema = options.defaultSchema
                  ? options.defaultSchema
                  : runner_.defaultSchema();
              options.tokenProvider = permissionCheck_(
                  queryId,
                  sqlText,
                  options.defaultConnectorId.value_or(
                      runner_.defaultConnectorId()),
                  schema ? std::optional<std::string_view>{*schema}
                         : std::nullopt,
                  statement->views());
            }

            return cli::time<SqlQueryRunner::SqlResult>(
                [&]() { return runner_.run(*statement, options); },
                statementTiming);
          },
          totalTiming);

      if (result.message.has_value()) {
        std::cout << result.message.value() << std::endl;
      } else {
        if (FLAGS_debug && !result.results.empty()) {
          std::cout << result.results.front()->rowType()->toString()
                    << std::endl;
        }
        cli::printResults(result.results, FLAGS_max_rows);
      }

      // Notify completion callback on success.
      int64_t numOutputRows{0};
      for (const auto& rowVector : result.results) {
        numOutputRows += rowVector->size();
      }
      uint64_t executionMicros{
          statementTiming.micros > result.optimizeMicros
              ? statementTiming.micros - result.optimizeMicros
              : 0};
      if (isInteractive) {
        std::cout << "Query ID: " << queryId << " | Optimizing: "
                  << facebook::velox::succinctNanos(
                         result.optimizeMicros * 1'000)
                  << " | Executing: "
                  << facebook::velox::succinctNanos(executionMicros * 1'000)
                  << " | Total: " << totalTiming.toString() << std::endl;
      }
      notifyCompletion(
          QueryCompletionInfo{
              .startInfo = {queryId, std::string(sqlText), createTime},
              .planString = std::move(result.planString),
              .parseMicros = parseTiming.micros,
              .optimizeMicros = result.optimizeMicros,
              .executionMicros = executionMicros,
              .totalMicros = totalTiming.micros,
              .numOutputRows = numOutputRows,
              .endTime = std::chrono::system_clock::now(),
          });
    } catch (std::exception& e) {
      // Notify completion callback on failure.
      notifyCompletion(
          QueryCompletionInfo{
              .startInfo = {queryId, std::string(sqlText), createTime},
              .errorInfo = ErrorInfo{e.what()},
              .endTime = std::chrono::system_clock::now(),
          });
      std::cerr << "Query ID: " << queryId << " | Query failed: " << e.what()
                << std::endl;
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

    if (command.starts_with("exit") || command.starts_with("quit")) {
      break;
    }

    if (command.starts_with("help")) {
      static const char* helpText =
          "Axiom Interactive SQL\n\n"
          "Type SQL and end with ';'.\n"
          "To set a flag, type 'flag <gflag_name> = <value>;' Leave a space on either side of '='.\n\n"
          "Useful flags:\n\n"
          "num_workers - Make a distributed plan for this many workers. Runs it in-process with remote exchanges with serialization and passing data in memory. If num_workers is 1, makes single node plans without remote exchanges.\n\n"
          "num_drivers - Specifies the parallelism for workers. This many threads per pipeline per worker.\n\n";

      std::cout << helpText;
      continue;
    }

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

    if (sscanf(command.c_str(), "flag %ms = %ms", &flag, &value) == 2) {
      auto message = gflags::SetCommandLineOption(flag, value);
      if (!message.empty()) {
        std::cout << message;
        modifiedFlags.insert(std::string(flag));
      } else {
        std::cout << "Failed to set flag '" << flag << "' to '" << value << "'"
                  << std::endl;
      }
      continue;
    }

    if (sscanf(command.c_str(), "clear %ms", &flag) == 1) {
      gflags::CommandLineFlagInfo info;
      if (!gflags::GetCommandLineFlagInfo(flag, &info)) {
        std::cout << "Failed to clear flag '" << flag << "'" << std::endl;
        continue;
      }
      auto message =
          gflags::SetCommandLineOption(flag, info.default_value.c_str());
      if (!message.empty()) {
        std::cout << message;
      }
      continue;
    }

    if (command.starts_with("flags")) {
      std::cout << "Modified flags (" << modifiedFlags.size() << "):\n";
      for (const auto& name : modifiedFlags) {
        std::string flagValue;
        if (gflags::GetCommandLineOption(name.c_str(), &flagValue)) {
          std::cout << name << " = " << flagValue << std::endl;
        }
      }
      continue;
    }

    runNoThrow(command, interactive);
  }
}
} // namespace axiom::sql
