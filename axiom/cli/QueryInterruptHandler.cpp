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

#include "axiom/cli/QueryInterruptHandler.h"

#include <fcntl.h>
#include <unistd.h>
#include <cerrno>

#include <folly/ScopeGuard.h>

#include "velox/common/base/Exceptions.h"

namespace axiom::sql {
namespace {

// Write end of the SIGINT self-pipe; -1 when no handler is installed.
// File-scope atomic because a signal handler cannot capture state.
// NOLINTNEXTLINE(facebook-avoid-non-const-global-variables)
std::atomic<int> gInterruptPipeWriteFd{-1};

// Monotonic count of SIGINTs observed. arm() snapshots it when a query arms;
// the drain thread cancels only if the count advanced past that snapshot,
// so a SIGINT buffered before the query armed (init script, --repeat) can't
// cancel it. Lock-free atomic: the signal handler mutates it.
// NOLINTNEXTLINE(facebook-avoid-non-const-global-variables)
std::atomic<uint64_t> gInterruptSignalSeq{0};
static_assert(
    std::atomic<uint64_t>::is_always_lock_free,
    "SIGINT sequence counter must be lock-free to be async-signal-safe");

// SIGINT handler: async-signal-safe. Bumps the signal count and nudges the
// self-pipe. A full pipe (EAGAIN) is fine -- one pending byte wakes the reader.
void interruptSignalHandler(int /*sig*/) {
  const int fd = gInterruptPipeWriteFd.load(std::memory_order_relaxed);
  if (fd >= 0) {
    // Bump before the wakeup so the drain thread, which loads the count after
    // reading the byte, observes this signal.
    gInterruptSignalSeq.fetch_add(1, std::memory_order_seq_cst);
    // Save/restore errno: SIGINT may be delivered to any thread (including a
    // Velox executor thread mid-syscall), and write() can clobber errno.
    const int savedErrno = errno;
    const char byte{1};
    const ssize_t ignored = write(fd, &byte, 1);
    (void)ignored;
    errno = savedErrno;
  }
}
} // namespace

QueryInterruptHandler::QueryInterruptHandler() {
  // Enforce the single-live-handler invariant (see class doc).
  VELOX_CHECK_EQ(
      gInterruptPipeWriteFd.load(),
      -1,
      "A QueryInterruptHandler is already installed");
  VELOX_CHECK_EQ(pipe(pipeFds_), 0, "Failed to create interrupt pipe");
  // No destructor runs on a partially constructed object, so undo the pipe and
  // handler here if a later step throws.
  bool sigactionInstalled{false};
  SCOPE_FAIL {
    if (sigactionInstalled) {
      sigaction(SIGINT, &savedAction_, nullptr);
    }
    close(pipeFds_[0]);
    close(pipeFds_[1]);
  };
  // Non-blocking write end so the handler never blocks; OR into existing flags.
  const int flags = fcntl(pipeFds_[1], F_GETFL, 0);
  VELOX_CHECK_GE(flags, 0, "Failed to read interrupt pipe flags");
  VELOX_CHECK_EQ(
      fcntl(pipeFds_[1], F_SETFL, flags | O_NONBLOCK),
      0,
      "Failed to set interrupt pipe non-blocking");

  struct sigaction action{};
  action.sa_handler = interruptSignalHandler;
  sigemptyset(&action.sa_mask);
  // Restart syscalls interrupted on other threads (e.g. executor I/O) instead
  // of failing them with EINTR.
  action.sa_flags = SA_RESTART;
  VELOX_CHECK_EQ(
      sigaction(SIGINT, &action, &savedAction_),
      0,
      "Failed to install SIGINT handler");
  sigactionInstalled = true;

  // All fallible steps done. Start the drain thread, then publish the write fd
  // last (noexcept) so no failure path leaves it pointing at a closed pipe.
  thread_ = std::thread([this] { drainPipe(); });
  gInterruptPipeWriteFd.store(pipeFds_[1]);
}

QueryInterruptHandler::~QueryInterruptHandler() {
  // Ignore SIGINT during teardown so no new handler runs while we clear the fd
  // and close the pipe; a late Ctrl+C is dropped rather than nudging a closing
  // pipe or taking the default terminate action.
  //
  // Accepted race: SIG_IGN does not stop a handler already running on another
  // thread. It may write() to the write-fd after we close it below -- a
  // harmless EBADF, or a stray byte if that fd number was reused by then. The
  // handler can't be interrupted from here, so this narrow window is accepted.
  struct sigaction ignore{};
  ignore.sa_handler = SIG_IGN;
  sigemptyset(&ignore.sa_mask);
  sigaction(SIGINT, &ignore, nullptr);

  gInterruptPipeWriteFd.store(-1);
  stop_.store(true);
  // Close the write end so the drain thread's blocked read() returns EOF and
  // its loop exits, so join() cannot hang. (A wakeup byte could be dropped if
  // the non-blocking pipe were full; EOF is the guarantee.)
  close(pipeFds_[1]);
  thread_.join();
  close(pipeFds_[0]);

  // Restore the previous SIGINT disposition now that the pipe is gone.
  sigaction(SIGINT, &savedAction_, nullptr);
}

folly::CancellationToken QueryInterruptHandler::arm() {
  std::lock_guard<std::mutex> lock(mutex_);
  // Snapshot the count so only SIGINTs after this cancel the new source (see
  // gInterruptSignalSeq). A fresh source each time so a prior cancellation
  // cannot carry over.
  armSignalSeq_ = gInterruptSignalSeq.load(std::memory_order_seq_cst);
  source_.emplace();
  return source_->getToken();
}

void QueryInterruptHandler::disarm() {
  std::lock_guard<std::mutex> lock(mutex_);
  source_.reset();
}

void QueryInterruptHandler::drainPipe() {
  char buffer[64];
  for (;;) {
    const ssize_t bytes = read(pipeFds_[0], buffer, sizeof(buffer));
    if (bytes <= 0) {
      if (bytes < 0 && errno == EINTR) {
        continue;
      }
      break;
    }
    if (stop_.load()) {
      break;
    }
    // Snapshot the count once: it drives the cancel decision and is published
    // as the processed-sequence fence, so both must agree on the same value.
    const uint64_t observedSeq =
        gInterruptSignalSeq.load(std::memory_order_seq_cst);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (source_.has_value() && observedSeq > armSignalSeq_) {
        source_->requestCancellation();
      }
      lastProcessedSignalSeq_ = observedSeq;
    }
    processedCv_.notify_all();
  }
}

uint64_t QueryInterruptHandler::testingSignalSeq() const {
  return gInterruptSignalSeq.load(std::memory_order_seq_cst);
}

bool QueryInterruptHandler::testingWaitForProcessedSignalSeq(
    uint64_t targetSeq,
    std::chrono::milliseconds timeout) {
  std::unique_lock<std::mutex> lock(mutex_);
  return processedCv_.wait_for(
      lock, timeout, [&] { return lastProcessedSignalSeq_ >= targetSeq; });
}

} // namespace axiom::sql
