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

#include <atomic>
#include <functional>
#include <memory>

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
///
/// A caller normally consumes one execution to completion:
/// @code
/// auto&& [results] = co_await folly::coro::co_scope_exit(
///     folly::coro::co_cleanup,
///     runner->execute(timeoutMicros));
/// while (auto batch = co_await results.next()) {
///   consume(std::move(*batch));
/// }
/// @endcode
/// Lifecycle:
/// - Pull at most one execution from a Runner.
/// - Await `cleanup()` on every generator returned by `execute()`, pulled or
///   not, before destroying the runner.
/// - Result batches stay valid until the runner is destroyed.
class Runner {
 public:
  enum class State { kInitialized, kRunning, kFinished, kError, kCancelled };

  AXIOM_DECLARE_EMBEDDED_ENUM_NAME(State);

  virtual ~Runner() = default;

  /// Executes the plan and yields successive result batches.
  ///
  /// Execution outcomes:
  /// - Drained: the generator ends.
  /// - Execution error: rethrown as-is.
  /// - Caller cancels a pending pull: the complete execution stops, and the
  ///   pull throws `folly::OperationCancelled`.
  /// - Deadline expires: it starts on the first pull, runs between pulls, and
  ///   throws `VeloxUserError`.
  ///
  /// Each pull's cancellation token applies only while that pull is pending.
  /// A zero `timeoutMicros` disables the deadline.
  ///
  /// A terminal pull finishes execution cleanup before returning end-of-stream
  /// or throwing, so `stats()` then returns final task stats. Uses Folly's
  /// `CleanableAsyncGenerator` async-cleanup pattern: the owner must await
  /// `cleanup()` on every exit, including for a generator that was never
  /// pulled. Cleanup stops an execution that was not drained, finishes its
  /// cleanup, and cancels its deadline.
  ///
  /// Result batches are backed by a memory pool owned by the runner and remain
  /// valid only until the runner is destroyed. Awaiting the read path never
  /// blocks the awaiting thread. A write commit is a point of no return and may
  /// block, so a write-plan execution must not run on an executor thread.
  folly::coro::CleanableAsyncGenerator<velox::RowVectorPtr> execute(
      int64_t timeoutMicros = 0);

  /// Returns Task stats for each fragment of the plan. The stats correspond 1:1
  /// to the stages in the MultiFragmentPlan. May be called at any time: while
  /// the query is running it returns an in-progress snapshot; once execution
  /// cleanup has finished it returns the final stats.
  virtual std::vector<velox::exec::TaskStats> stats() const = 0;

  /// Returns the executable fragments of the plan being run, ordered so that
  /// fragments()[i] corresponds to stats()[i].
  virtual const std::vector<optimizer::ExecutableFragment>& fragments()
      const = 0;

  /// Returns the state of execution.
  virtual State state() const = 0;

  /// Synchronous convenience that drives `execute()` to completion and invokes
  /// `onBatch` for each result batch. An `onBatch` exception stops the
  /// execution and finishes cleanup before propagation. Blocks the calling
  /// thread, so it must not be called from a Velox executor thread.
  void drain(
      const std::function<void(velox::RowVectorPtr)>& onBatch,
      int64_t timeoutMicros = 0);

 protected:
  // Produces the runner-specific result stream under the stable cancellation
  // token supplied by `execute()`.
  virtual folly::coro::AsyncGenerator<velox::RowVectorPtr> executeImpl() = 0;

  // Stops runner-specific work and releases its resources. Implementations
  // must be idempotent across terminal and scope-exit cleanup.
  virtual folly::coro::Task<void> co_cleanupImpl() = 0;

 private:
  // Prevents a second result stream from being pulled from this Runner.
  std::atomic<bool> executionStarted_{false};
};

} // namespace facebook::axiom::runner

AXIOM_EMBEDDED_ENUM_FORMATTER(facebook::axiom::runner::Runner, State);
