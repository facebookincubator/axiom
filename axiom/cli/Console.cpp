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
#include <iostream>
#include "axiom/cli/ResultPrinter.h"
#include "axiom/cli/StdinReader.h"
#include "axiom/cli/Timing.h"
#include "axiom/cli/linenoise/linenoise.h"

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

DEFINE_bool(debug, false, "Enable debug mode");

using namespace facebook::velox;

namespace axiom::sql {

void Console::initialize() {
  gflags::SetUsageMessage(
      "Axiom local SQL command line. "
      "Run 'axiom_sql --help' for available options.\n");

  // Disable logging to stderr if not in debug mode.
  FLAGS_logtostderr = FLAGS_debug;
}

void Console::run() {
  if (!FLAGS_query.empty()) {
    runNoThrow(FLAGS_query, false);
  } else {
    std::cout << "Axiom SQL. Type statement and end with ;.\n"
                 "flag name = value; sets a gflag.\n"
                 "help; prints help text."
              << std::endl;
    readCommands("SQL> ");
  }
}

void Console::runNoThrow(std::string_view sql, bool isInteractive) {
  const SqlQueryRunner::RunOptions options{
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

    try {
      cli::Timing parseTiming;
      auto statement = cli::time<presto::SqlStatementPtr>(
          [&]() { return runner_.parseSingle(sqlText, options); }, parseTiming);

      if (isInteractive) {
        std::cout << "Parsing: " << parseTiming.toString() << std::endl;
      }

      cli::Timing statementTiming;
      auto result = cli::time<SqlQueryRunner::SqlResult>(
          [&]() { return runner_.run(*statement, options); }, statementTiming);

      if (result.message.has_value()) {
        std::cout << result.message.value() << std::endl;
      } else {
        cli::printResults(result.results, FLAGS_max_rows);
      }

      if (isInteractive) {
        std::cout << "Optimizing and Executing: " << statementTiming.toString()
                  << std::endl;
      }
    } catch (std::exception& e) {
      std::cerr << "Query failed: " << e.what() << std::endl;
      return;
    }
  }
}

void Console::readCommands(const std::string& prompt) {
  linenoiseSetMultiLine(1);
  linenoiseHistorySetMaxLen(1024);

  std::set<std::string> modifiedFlags;

  for (;;) {
    bool atEnd;
    std::string command = cli::readCommand(prompt, atEnd);
    if (atEnd) {
      break;
    }

    if (command.empty()) {
      continue;
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

    if (sscanf(command.c_str(), "session %ms = %ms", &flag, &value) == 2) {
      std::cout << "Session '" << flag << "' set to '" << value << "'"
                << std::endl;
      runner_.sessionConfig()[std::string(flag)] = std::string(value);
      continue;
    }

    runNoThrow(command);
  }
}
} // namespace axiom::sql
