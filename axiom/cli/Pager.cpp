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

#include "axiom/cli/Pager.h"
#include <sys/wait.h>
#include <unistd.h>
#include <algorithm>
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <string>

namespace axiom::cli {
namespace {

// Largest amount of text handed to a single write. A write that fails after
// transferring part of its buffer reports the error but not how much it wrote,
// so writing in small pieces limits how much a failure can hide.
constexpr size_t kWriteChunkSize = 4096;

// Writes all of 'text' to 'fileDescriptor'. One write can stop before the end
// when a signal interrupts it, or when the reader closes the pipe, so keep
// writing until everything is written or a write fails. Returns how many bytes
// were written; less than the size of 'text' means a write failed.
size_t writeAll(std::string_view text, int fileDescriptor) {
  size_t offset{0};
  while (offset < text.size()) {
    const auto chunkSize = std::min(kWriteChunkSize, text.size() - offset);
    const auto written =
        ::write(fileDescriptor, text.data() + offset, chunkSize);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      break;
    }
    offset += written;
  }
  return offset;
}

// Whether the status from pclose says the shell could not run the command at
// all. The shell reports 127 when it cannot find the command, and 126 when it
// finds one it cannot execute. Every other failing status comes from a pager
// that did run.
bool couldNotRun(int status) {
  if (status < 0 || !WIFEXITED(status)) {
    return false;
  }
  const int exitCode = WEXITSTATUS(status);
  return exitCode == 126 || exitCode == 127;
}

} // namespace

bool Pager::print(std::string_view text, std::string_view command) {
  FILE* pipe = popen(std::string(command).c_str(), "w");
  if (pipe == nullptr) {
    return false;
  }

  auto* previousSigpipe = std::signal(SIGPIPE, SIG_IGN);
  // Writing to the pipe directly, rather than through a buffered stream, keeps
  // the count below meaningful: a buffered write reports success for bytes the
  // pager never received.
  const auto written = writeAll(text, fileno(pipe));
  const int status = pclose(pipe);
  if (previousSigpipe != SIG_ERR) {
    std::signal(SIGPIPE, previousSigpipe);
  }

  return written > 0 && !couldNotRun(status);
}

} // namespace axiom::cli
