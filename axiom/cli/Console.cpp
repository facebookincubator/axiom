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
#include <sys/resource.h>
#include <iostream>
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

DEFINE_bool(sample_joins, false, "Enable join sampling");
DEFINE_bool(sample_filters, false, "Enable filter sampling");

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

namespace {
struct Timing {
  uint64_t micros{0};
  uint64_t userNanos{0};
  uint64_t systemNanos{0};

  std::string toString() const {
    double pct = 0;
    if (micros > 0) {
      pct = 100 * (userNanos + systemNanos) / (micros * 1000);
    }

    std::stringstream out;
    out << succinctNanos(micros * 1000) << " / " << succinctNanos(userNanos)
        << " user / " << succinctNanos(systemNanos) << " system (" << pct
        << "%)";
    return out.str();
  }
};

template <typename T>
T time(const std::function<T()>& func, Timing& timing) {
  struct rusage start{};
  getrusage(RUSAGE_SELF, &start);
  SCOPE_EXIT {
    struct rusage end{};
    getrusage(RUSAGE_SELF, &end);
    auto tvNanos = [](struct timeval tv) {
      return tv.tv_sec * 1'000'000'000 + tv.tv_usec * 1'000;
    };
    timing.userNanos = tvNanos(end.ru_utime) - tvNanos(start.ru_utime);
    timing.systemNanos = tvNanos(end.ru_stime) - tvNanos(start.ru_stime);
  };

  MicrosecondTimer timer(&timing.micros);
  return func();
}

int64_t countResults(const std::vector<RowVectorPtr>& results) {
  int64_t numRows = 0;
  for (const auto& result : results) {
    numRows += result->size();
  }
  return numRows;
}

int32_t printResults(const std::vector<RowVectorPtr>& results) {
  const auto numRows = countResults(results);

  auto printFooter = [&]() {
    std::cout << "(" << numRows << " rows in " << results.size() << " batches)"
              << std::endl
              << std::endl;
  };

  if (numRows == 0) {
    printFooter();
    return 0;
  }

  const auto type = results.front()->rowType();
  std::cout << type->toString() << std::endl;

  const auto numColumns = type->size();

  std::vector<std::vector<std::string>> data;
  std::vector<size_t> widths(numColumns, 0);
  std::vector<bool> alignLeft(numColumns);

  for (auto i = 0; i < numColumns; ++i) {
    widths[i] = type->nameOf(i).size();
    alignLeft[i] = type->childAt(i)->isVarchar();
  }

  auto printSeparator = [&]() {
    std::cout << std::setfill('-');
    for (auto i = 0; i < numColumns; ++i) {
      if (i > 0) {
        std::cout << "-+-";
      }
      std::cout << std::setw(widths[i]) << "";
    }
    std::cout << std::endl;
    std::cout << std::setfill(' ');
  };

  auto printRow = [&](const auto& row) {
    for (auto i = 0; i < numColumns; ++i) {
      if (i > 0) {
        std::cout << " | ";
      }
      std::cout << std::setw(widths[i]);
      if (alignLeft[i]) {
        std::cout << std::left;
      } else {
        std::cout << std::right;
      }
      std::cout << row[i];
    }
    std::cout << std::endl;
  };

  int32_t numPrinted = 0;

  auto doPrint = [&]() {
    printSeparator();
    printRow(type->names());
    printSeparator();

    for (const auto& row : data) {
      printRow(row);
    }

    if (numPrinted < numRows) {
      std::cout << std::endl;
      std::cout << "..." << (numRows - numPrinted) << " more rows."
                << std::endl;
    }

    printFooter();
  };

  for (const auto& result : results) {
    for (auto row = 0; row < result->size(); ++row) {
      data.emplace_back();

      auto& rowData = data.back();
      rowData.resize(numColumns);
      for (auto column = 0; column < numColumns; ++column) {
        rowData[column] = result->childAt(column)->toString(row);
        widths[column] = std::max(widths[column], rowData[column].size());
      }

      ++numPrinted;
      if (numPrinted >= FLAGS_max_rows) {
        doPrint();
        return numRows;
      }
    }
  }

  doPrint();

  return numRows;
}
} // namespace

void Console::runNoThrow(std::string_view sql, bool isInteractive) {
  const SqlQueryRunner::RunOptions options{
      .numWorkers = FLAGS_num_workers,
      .numDrivers = FLAGS_num_drivers,
      .splitTargetBytes = FLAGS_split_target_bytes,
      .optimizerTraceFlags = FLAGS_optimizer_trace,
      .debugMode = FLAGS_debug,
      .sampleJoins = FLAGS_sample_joins,
      .sampleFilters = FLAGS_sample_filters,
  };

  decltype(runner_.parseMultiple(sql, options)) statements;
  try {
    // Parse all statements upfront.
    statements = runner_.parseMultiple(sql, options);
  } catch (std::exception& e) {
    std::cerr << "Parse failed: " << e.what() << std::endl;
    return;
  }

  // Execute each statement with timing.
  for (const auto& statement : statements) {
    try {
      Timing statementTiming;
      auto result = time<SqlQueryRunner::SqlResult>(
          [&]() { return runner_.run(*statement, options); }, statementTiming);

      if (result.message.has_value()) {
        std::cout << result.message.value() << std::endl;
      } else {
        printResults(result.results);
      }

      if (isInteractive) {
        // In interactive mode, show per-statement timing.
        std::cout << statementTiming.toString() << std::endl;
      }
    } catch (std::exception& e) {
      std::cerr << "Query failed: " << e.what() << std::endl;
      // Stop executing remaining statements in this block.
      return;
    }
  }
}

namespace {

// Reads multi-line command from 'in' until encounters ';' followed by
// zero or
// more whitespaces.
// @return Command text with leading and trailing whitespaces as well as
// trailing ';' removed.
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
} // namespace

void Console::readCommands(const std::string& prompt) {
  linenoiseSetMultiLine(1);
  linenoiseHistorySetMaxLen(1024);

  std::set<std::string> modifiedFlags;

  for (;;) {
    bool atEnd;
    std::string command = readCommand(prompt, atEnd);
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

    if (command.starts_with("savehistory")) {
      runner_.saveHistory(FLAGS_data_path + "/.history");
      continue;
    }

    if (command.starts_with("clearhistory")) {
      runner_.clearHistory();
      continue;
    }

    runNoThrow(command);
  }
}
} // namespace axiom::sql
