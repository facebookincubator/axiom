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

#include <folly/Synchronized.h>
#include <ostream>

namespace facebook::axiom::runner {
struct QueryProgress;
} // namespace facebook::axiom::runner

namespace axiom::cli {

/// Renders live query progress as an in-place status block on a stream. Each
/// update() redraws the block -- a summary line, optional detail, and a
/// per-stage grid -- over the previous one; clear() erases it so it does not
/// linger before query results are printed.
///
/// This class always renders: the caller decides whether progress should be
/// shown at all (e.g. only for an interactive REPL with a terminal stderr) and
/// only then constructs one. It draws to a stream and takes the target width
/// per update(), so it has no file-descriptor or terminal dependency of its
/// own; the caller supplies the width (re-query it each call to track a live
/// terminal resize).
///
/// update() runs on the background progress-poller thread while clear() runs on
/// the main thread; a lock serializes them and the cursor/line state they
/// share.
///
/// @code
///   cli::LiveProgressDisplay display(std::cerr, /*options=*/{});
///   // Feed it a snapshot whenever progress is reported:
///   display.update(progress, /*width=*/80);
///   // ...repeat as progress advances...
///   display.clear();  // erase the block before printing results
/// @endcode
///
/// Invariant the caller must uphold: update()/clear() are the only writers to
/// `out` while a query runs.
class LiveProgressDisplay {
 public:
  /// Display knobs; defaults render the compact view.
  struct Options {
    /// When true, also draws the per-split and CPU-time detail lines.
    bool showSplitAndCpuDetail{false};
  };

  /// @param out Stream to draw on (typically std::cerr).
  /// @param options Display knobs.
  LiveProgressDisplay(std::ostream& out, Options options);

  /// Erases any drawn status block so an exception that bypasses the explicit
  /// clear() call still restores the terminal. Idempotent with clear().
  ~LiveProgressDisplay();

  LiveProgressDisplay(const LiveProgressDisplay&) = delete;
  LiveProgressDisplay& operator=(const LiveProgressDisplay&) = delete;
  LiveProgressDisplay(LiveProgressDisplay&&) = delete;
  LiveProgressDisplay& operator=(LiveProgressDisplay&&) = delete;

  /// Redraws the status block in place from a progress snapshot, laid out for a
  /// terminal `width` columns wide.
  void update(const facebook::axiom::runner::QueryProgress& info, int width);

  /// Erases the status block. Call once after the query completes, before
  /// printing results. No-op when nothing has been drawn yet.
  void clear();

 private:
  // Stream receiving the ANSI status output (typically std::cerr).
  std::ostream& out_;
  // Display knobs decided once at construction.
  const Options options_;

  // Rows drawn in the current block, reset to 0 once it is cleared. Held under
  // the lock across the whole frame so the poller and main threads never
  // interleave terminal writes.
  folly::Synchronized<int> lines_{0};
};

} // namespace axiom::cli
