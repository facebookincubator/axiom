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
#pragma once

#include "axiom/cli/SqlQueryRunner.h"

DECLARE_string(data_path);
DECLARE_string(data_format);
DECLARE_string(etc_dir);
DECLARE_bool(debug);

namespace axiom::sql {

/// SQL console that executes queries from command-line flags, files, or
/// interactive stdin input.
class Console {
 public:
  explicit Console(SqlQueryRunner& runner);

  /// Initializes the CLI with usage message and logging settings.
  void initialize();

  /// Runs the CLI. Executes `--query` if set, otherwise reads piped
  /// stdin if non-interactive, otherwise enters the interactive REPL.
  /// Honors `--repeat` for `--query` and piped-stdin paths.
  ///
  /// Throws VeloxUserError on invalid CLI flags. Query failures during
  /// execution are caught internally and printed to stderr; they do
  /// not throw.
  void run();

 private:
  // Runs a single SQL statement and prints results/timing. Returns true on
  // success, false if the query threw.
  bool runOnce(std::string_view sql, bool printTiming);

  // Splits 'sql' into individual statements and runs each one in sequence.
  // Stops on the first failure.
  void runMultiple(std::string_view sql, bool printTiming);

  // Runs 'sql' (a single SQL statement) 'repeat' times back-to-back.
  // Stops on the first failure.
  void runRepeat(std::string_view sql, int repeat, bool printTiming);

  // Reads and executes commands from standard input in interactive mode.
  void readCommands(const std::string& prompt, bool printTiming);

  SqlQueryRunner& runner_;
};

} // namespace axiom::sql
