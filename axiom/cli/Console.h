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

/// Executes SQL through a SqlQueryRunner, driven from command-line flags, an
/// init/`.run` file, piped stdin, or an interactive REPL, and prints results
/// and timing to stdout/stderr.
///
/// A live progress grid is drawn on stderr while a query runs when stderr is a
/// terminal and `runner` was built with a progress scheduler
/// (SqlQueryRunner::supportsProgress()). The interactive REPL shows it by
/// default; the non-interactive paths (`--query`, piped stdin) opt in via
/// `--show_live_progress`. It is erased before results print, so redirected
/// stdout stays clean, and it is hidden when stderr is redirected (scripted or
/// captured runs). `--init` and `--repeat` never draw it.
///
/// @code
///   folly::FunctionScheduler scheduler;  // enables progress reporting
///   SqlQueryRunner runner{user, &scheduler};
///   Console console{runner};
///   console.initialize();
///   console.run();
/// @endcode
///
/// Invariants:
/// - `runner` must outlive the Console.
/// - run() owns process-wide stdin/stdout/stderr while it executes; do not
///   write to them concurrently.
/// - Query failures are caught and printed; only invalid CLI flags throw.
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
  // success, false if the query threw. Draws the live progress grid when
  // 'showProgress' is set.
  bool runOnce(std::string_view sql, bool printTiming, bool showProgress);

  // Splits 'sql' into individual statements and runs each one in sequence.
  // Stops on the first failure. Passes 'showProgress' through to each.
  void runMultiple(std::string_view sql, bool printTiming, bool showProgress);

  // Runs 'sql' (a single SQL statement) 'repeat' times back-to-back.
  // Stops on the first failure.
  void runRepeat(std::string_view sql, int repeat, bool printTiming);

  // Reads and executes commands from standard input in interactive mode.
  // Passes 'showProgress' through to the statements it runs.
  void
  readCommands(const std::string& prompt, bool printTiming, bool showProgress);

  SqlQueryRunner& runner_;
};

} // namespace axiom::sql
