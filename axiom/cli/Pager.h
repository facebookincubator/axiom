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

#include <string_view>

namespace axiom::cli {

/// Shows text through a terminal pager such as 'less'.
///
/// A pager is useful when the output goes to a terminal, and not when it is
/// redirected to a file or to another program. The caller decides which of the
/// two it has, and prints the text itself when the pager did not show it:
///
///   if (!isatty(STDOUT_FILENO) || !Pager::print(table, FLAGS_pager)) {
///     std::cout << table;
///   }
class Pager {
 public:
  /// Run 'command' through the shell and send 'text' to it. Return true if
  /// the pager showed the text, and false if the caller still has to print it.
  ///
  /// A pager can end in four ways:
  ///  - It reads all of the text: shown.
  ///  - The user stops it early, by pressing 'q' in less for example: shown.
  ///    The rest of the text cannot be written and the pager ends with an error
  ///    status, but the user has seen as much as they wanted to see, and
  ///    printing the text again would undo that choice.
  ///  - It reads nothing at all: not shown.
  ///  - The shell cannot run it, because the pager command does not exist:
  ///    not shown. Text short enough to fit in the pipe is written in full even
  ///    in this case, so the amount written does not reveal it. The exit status
  ///    of the shell does.
  ///
  /// A pager that ends early also closes the pipe while the text is still being
  /// written, which would otherwise kill the process with the SIGPIPE signal.
  /// That signal is ignored while the text is written, and its earlier handling
  /// is restored afterwards.
  static bool print(std::string_view text, std::string_view command);
};

} // namespace axiom::cli
