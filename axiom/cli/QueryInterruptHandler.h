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

#include <signal.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <thread>

#include <folly/CancellationToken.h>

namespace axiom::sql {

/// Cancels the in-flight query on SIGINT while keeping the CLI alive: while a
/// query runs, Ctrl+C trips the armed cancellation token instead of terminating
/// the process.
///
/// Install one for the session, then arm() around each query and pass the
/// returned token to the runner via RunOptions::cancellationToken:
/// @code
///   QueryInterruptHandler interrupt;
///   options.cancellationToken = interrupt.arm();
///   SCOPE_EXIT { interrupt.disarm(); };
///   runner.run(sql, options); // Ctrl+C trips the token, cancelling the query
/// @endcode
///
/// A signal handler (not sigwait) is used so the process-wide disposition
/// covers every thread: SIGINT delivered to a Velox executor thread runs the
/// handler rather than terminating, so no thread needs SIGINT blocked.
/// requestCancellation() is not async-signal-safe, so the handler only nudges a
/// self-pipe and a drain thread does the cancellation.
///
/// Invariants:
/// - Only one handler may be live at a time (the constructor VELOX_CHECKs this;
///   the signal handler reads a single file-scope pipe fd).
/// - Not copyable or movable.
/// - The constructor installs the SIGINT handler; the destructor restores the
///   previous disposition.
class QueryInterruptHandler {
 public:
  QueryInterruptHandler();
  ~QueryInterruptHandler();

  QueryInterruptHandler(const QueryInterruptHandler&) = delete;
  QueryInterruptHandler& operator=(const QueryInterruptHandler&) = delete;
  QueryInterruptHandler(QueryInterruptHandler&&) = delete;
  QueryInterruptHandler& operator=(QueryInterruptHandler&&) = delete;

  /// Arms cancellation for the next query: creates a fresh cancellation source
  /// and returns its token to pass to the runner via
  /// RunOptions::cancellationToken. A SIGINT delivered after this trips it.
  folly::CancellationToken arm();

  /// Disarms cancellation between queries, so a stray SIGINT is absorbed rather
  /// than cancelling the next query.
  void disarm();

  /// Count of SIGINTs observed so far. Read after raise() to learn the target
  /// sequence, then fence on it with testingWaitForProcessedSignalSeq().
  /// Testing only.
  uint64_t testingSignalSeq() const;

  /// Blocks until the drain thread has processed every SIGINT up to and
  /// including 'targetSeq' (decided whether to cancel), or 'timeout' elapses;
  /// returns true if the fence was reached. Lets a test prove a signal did not
  /// cancel a source without a fixed sleep. Testing only.
  bool testingWaitForProcessedSignalSeq(
      uint64_t targetSeq,
      std::chrono::milliseconds timeout);

 private:
  // Reads the self-pipe on a background thread; on a wakeup, trips 'source_'.
  void drainPipe();

  int pipeFds_[2]{-1, -1};
  struct sigaction savedAction_{};
  std::mutex mutex_;
  // Owned; arm() re-creates it (so a prior cancellation never carries over) and
  // disarm() clears it. Set only while a query is armed. Guarded by mutex_.
  std::optional<folly::CancellationSource> source_;
  // SIGINT count observed when the current source armed; a signal cancels the
  // source only if the count has advanced past this. Guarded by mutex_.
  uint64_t armSignalSeq_{0};
  // Highest SIGINT sequence number the drain thread has finished acting on.
  // Advances after each cancel/absorb decision so a test can fence on a
  // specific signal. Guarded by mutex_.
  uint64_t lastProcessedSignalSeq_{0};
  // Notified when lastProcessedSignalSeq_ advances.
  std::condition_variable processedCv_;
  std::atomic_bool stop_{false};
  std::thread thread_;
};

} // namespace axiom::sql
