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

#include <functional>

#include <folly/CancellationToken.h>
#include <folly/coro/AsyncGenerator.h>
#include <folly/coro/Task.h>

#include "axiom/common/Enums.h"
#include "velox/exec/TaskStats.h"

namespace facebook::axiom::optimizer {
struct ExecutableFragment;
} // namespace facebook::axiom::optimizer

/// Base classes for multifragment Velox query execution.
namespace facebook::axiom::runner {

/// Base class for executing multifragment Velox queries. One instance
/// of a Runner coordinates the execution of one multifragment
/// query. Different derived classes can support different shuffles
/// and different scheduling either in process or in a cluster. Unless
/// otherwise stated, the member functions are thread safe as long as
/// the caller holds an owning reference to the runner.
class Runner {
 public:
  enum class State { kInitialized, kRunning, kFinished, kError, kCancelled };

  AXIOM_DECLARE_EMBEDDED_ENUM_NAME(State);

  virtual ~Runner() = default;

  /// Returns a generator that yields successive result batches until the query
  /// is done (the generator ends). `co_await gen.next()` rethrows any
  /// execution-time error. Each batch is backed by a memory pool owned by the
  /// runner and stays valid only until the runner is destroyed; a caller that
  /// needs a batch to outlive the runner must copy it out (as optimizer
  /// constant folding does).
  ///
  /// The caller must `co_await co_close()` exactly once when done with the
  /// generator — whether it drained fully, stopped early, or was unwound by an
  /// exception — before destroying the runner. To stop early, stop pulling and
  /// then co_await co_close(); dropping the generator alone does NOT reap.
  /// External or deadline cancellation still composes through the awaiting
  /// scope's cancellation token (e.g. folly::coro::timeout or
  /// co_withCancellation); there is no separate public cancel.
  ///
  /// Awaiting the read/produce-drain path never blocks the awaiting thread, so
  /// it is safe on a Velox executor thread. Exception: on the write path the
  /// INSERT/CTAS commit is a point of no return that runs to completion, and
  /// its error-path cleanup waits for tasks to stop — both may block, so a
  /// write-plan execute() should not be awaited on an executor thread.
  virtual folly::coro::AsyncGenerator<velox::RowVectorPtr> execute() = 0;

  /// Terminal, owner-scoped wind-down of one execute() run: stops anything
  /// still running and reaps it (split generation joined, tasks completed,
  /// final stats captured, pools released), and does not complete until that is
  /// done. The owner co_awaits this exactly once before destroying the runner,
  /// whether the generator drained fully or stopped early — the destructor
  /// asserts it ran rather than doing blocking teardown itself. Because it is
  /// awaited (not a blocking destructor), it is safe to co_await on a Velox
  /// executor thread. Idempotent. This is not an external cancel that returns
  /// before work stops; deadline/scope cancellation still composes through
  /// execute()'s awaiting token, while co_close() is the owner's lifecycle
  /// finish.
  virtual folly::coro::Task<void> co_close() = 0;

  /// Returns Task stats for each fragment of the plan. The stats correspond 1:1
  /// to the stages in the MultiFragmentPlan. May be called at any time: while
  /// the query is running it returns an in-progress snapshot; once execution
  /// has been reaped it returns the final stats.
  virtual std::vector<velox::exec::TaskStats> stats() const = 0;

  /// Returns the executable fragments of the plan being run, ordered so that
  /// fragments()[i] corresponds to stats()[i].
  virtual const std::vector<optimizer::ExecutableFragment>& fragments()
      const = 0;

  /// Returns the state of execution.
  virtual State state() const = 0;

  /// Convenience that synchronously drives execute() to completion, invoking
  /// 'onBatch' for each result batch, then co_closes the runner before
  /// returning. For callers that are not themselves coroutines — synchronous
  /// entry points such as the CLI, FFI boundaries, and tests. A coroutine
  /// caller should await execute() (and co_close()) directly instead. Blocks
  /// the calling thread, so it must NOT be called from a Velox executor thread
  /// (blocking there starves the executor).
  ///
  /// When 'timeoutMicros' > 0, enforces a cooperative deadline over execution
  /// only. It is not a hard wall-clock cap: it bounds the execute() phase, not
  /// the parse/permission/optimize phases a caller runs before drain() nor a
  /// write commit; a non-yielding operator can overrun it; and reaping the
  /// cancelled run adds teardown latency. On the deadline the drain fails with
  /// VELOX_USER_FAIL.
  ///
  /// 'cancelToken' lets any client cancel the running query from another thread
  /// (the drain thread is blocked in blockingWait): the client holds the paired
  /// folly::CancellationSource and calls requestCancellation(). Cancellation
  /// composes with the deadline through execute()'s awaiting scope. A genuine
  /// execution error always surfaces as itself: the deadline/cancel outcome is
  /// reported only when the run wound down benignly (no execution error), so a
  /// deadline or Ctrl+C racing a real error never masks it. In that benign case
  /// a deadline throws the timeout VELOX_USER_FAIL, and an external cancel
  /// throws a VeloxUserError carrying kQueryCancelledErrorCode so clients can
  /// report a cancellation off a typed error code rather than message text. The
  /// CLI trips it from a SIGINT handler; other clients (e.g. PVC2) trip it from
  /// their own cancel entry point.
  void drain(
      const std::function<void(velox::RowVectorPtr)>& onBatch,
      int64_t timeoutMicros = 0,
      folly::CancellationToken cancelToken = {});

  /// Velox error code on the VeloxUserError drain() throws when a query is
  /// stopped by an external cancel (as opposed to the deadline or a genuine
  /// execution error). Clients match ErrorInfo::errorCode on it to report a
  /// cancellation distinctly, without depending on the message text.
  static constexpr const char* kQueryCancelledErrorCode =
      "AXIOM_QUERY_CANCELLED";
};

} // namespace facebook::axiom::runner

AXIOM_EMBEDDED_ENUM_FORMATTER(facebook::axiom::runner::Runner, State);
