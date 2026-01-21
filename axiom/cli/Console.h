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
DECLARE_bool(debug);

namespace axiom::sql {

class Console {
 public:
  explicit Console(SqlQueryRunner& runner) : runner_{runner} {}

  void initialize();

  /// Runs the CLI, either executing a single query if passed in
  /// or entering interactive mode to read commands from stdin.
  void run();

 private:
  // Reads and executes commands from standard input in interactive mode.
  void readCommands(const std::string& prompt);

  SqlQueryRunner& runner_;
};

} // namespace axiom::sql
