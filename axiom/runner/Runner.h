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

#include <exception>
#include <functional>
#include <memory>

#include <folly/Synchronized.h>
#include <folly/coro/AsyncGenerator.h>
#include <folly/coro/Baton.h>
#include <folly/coro/Task.h>

#include "axiom/common/Enums.h"
#include "velox/exec/TaskStats.h"

namespace folly {
class Timekeeper;
}

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
/// auto results = runner->execute(timeoutMicros);
/// while (auto batch = co_await results.next()) {
///   consume(std::move(*batch));
/// }
/// @endcode
/// If the caller stops early, it destroys `results` and then awaits
/// `co_close()`. Only one execution may be started on a Runner.
class Runner {
 public:
  enum class State { kInitialized, kRunning, kFinished, kError, kCancelled };

  AXIOM_DECLARE_EMBEDDED_ENUM_NAME(State);

  virtual ~Runner() = default;

  /// Executes the plan and yields successive result batches. Each pull's
  /// cancellation token applies only while that pull is pending; completed
  /// pulls cannot cancel the remaining stream. `timeoutMicros` is one
  /// cooperative deadline for the complete execution, or zero for no deadline.
  /// A deadline failure surfaces as a Velox user error; caller cancellation
  /// surfaces as `folly::OperationCancelled`.
  ///
  /// Driving the generator to a terminal result always reaps the execution via
  /// `co_close()`, including after an error, deadline, or cancellation. A
  /// caller that stops pulling early must destroy the generator and then
  /// invoke `co_close()` explicitly. Dropping a non-terminal generator does
  /// not reap the runner-specific execution.
  ///
  /// Result batches are backed by a memory pool owned by the runner and remain
  /// valid only until the runner is destroyed. Awaiting the read path never
  /// blocks the awaiting thread. A write commit is a point of no return and may
  /// block, so a write-plan execution must not run on an executor thread.
  folly::coro::AsyncGenerator<velox::RowVectorPtr> execute(
      int64_t timeoutMicros = 0);

  /// Overrides the deadline timekeeper for deterministic tests. Must be called
  /// before execution starts.
  void testingSetTimekeeper(std::shared_ptr<folly::Timekeeper> timekeeper);

  /// Stops and reaps one execution, including split generation, tasks, final
  /// stats, and pools. `execute()` invokes this before returning a terminal
  /// result. A caller invokes it directly only after stopping consumption
  /// early. The destructor asserts that reap completed rather than performing
  /// blocking teardown itself. Awaiting it is safe on a Velox executor thread,
  /// and repeated calls are idempotent.
  folly::coro::Task<void> co_close();

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

  /// Synchronous convenience that drives `execute()` to completion and invokes
  /// `onBatch` for each result batch. An `onBatch` exception stops and reaps
  /// the execution before propagation. Blocks the calling thread, so it must
  /// not be called from a Velox executor thread.
  void drain(
      const std::function<void(velox::RowVectorPtr)>& onBatch,
      int64_t timeoutMicros = 0);

 protected:
  // Produces the runner-specific result stream under the stable cancellation
  // token supplied by `execute()`.
  virtual folly::coro::AsyncGenerator<velox::RowVectorPtr> executeImpl() = 0;

  // Stops and reaps runner-specific resources after Runner has joined its
  // execution deadline. Implementations must be idempotent.
  virtual folly::coro::Task<void> co_closeImpl() = 0;

 private:
  class Deadline;

  // Coordinates the single execution and serializes concurrent close calls.
  struct Lifecycle {
    // Prevents a second execution from starting on this Runner.
    bool executionStarted{false};
    // Elects the one close caller that performs resource cleanup.
    bool closeStarted{false};
    // Publishes completion and `closeError` to all close callers.
    bool closeFinished{false};
    // Keeps the active deadline alive if its generator is destroyed early.
    std::shared_ptr<Deadline> deadline;
    // Replays the cleanup failure to concurrent or repeated close callers.
    std::exception_ptr closeError;
  };

  // Supplies timers for execution deadlines. Null uses Folly's process
  // timekeeper.
  std::shared_ptr<folly::Timekeeper> timeoutTimekeeper_;
  // Protects the one-execution and one-close lifecycle invariants.
  folly::Synchronized<Lifecycle> lifecycle_;
  // Releases close callers waiting for the elected cleanup owner.
  folly::coro::Baton closeFinished_;
};

} // namespace facebook::axiom::runner

AXIOM_EMBEDDED_ENUM_FORMATTER(facebook::axiom::runner::Runner, State);
