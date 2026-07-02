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

#include <folly/coro/AsyncGenerator.h>

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
  /// is done (the generator ends). Each batch is allocated in the pool of the
  /// QueryCtx given to the Runner implementation; `co_await gen.next()`
  /// rethrows any execution-time error.
  ///
  /// Honors cooperative cancellation: cancelling the awaiting scope (e.g. via
  /// folly::coro::timeout, or an explicit abort()) interrupts in-flight work
  /// through the underlying Task and surfaces the error from the await — this
  /// is how a query deadline is enforced without a side thread. Awaiting the
  /// read/produce-drain path never blocks the awaiting thread, so it is safe on
  /// a Velox executor thread. Exception: on the write path the INSERT/CTAS
  /// commit is a point of no return that runs to completion, and its error-path
  /// cleanup waits for tasks to stop — both may block, so a write-plan
  /// execute() should not be awaited on an executor thread.
  ///
  /// To stop before the generator is exhausted, call abort(); state() only
  /// reaches a terminal value once the generator ends or abort() runs. Simply
  /// dropping a partially-consumed generator leaves state() == kRunning until
  /// waitForCompletion().
  virtual folly::coro::AsyncGenerator<velox::RowVectorPtr> execute() = 0;

  /// Returns Task stats for each fragment of the plan. The stats correspond 1:1
  /// to the stages in the MultiFragmentPlan. May be called at any time: while
  /// the query is running it returns an in-progress snapshot; once
  /// waitForCompletion() has run it returns the final stats.
  virtual std::vector<velox::exec::TaskStats> stats() const = 0;

  /// Returns the executable fragments of the plan being run, ordered so that
  /// fragments()[i] corresponds to stats()[i].
  virtual const std::vector<optimizer::ExecutableFragment>& fragments()
      const = 0;

  /// Returns the state of execution.
  virtual State state() const = 0;

  /// Cancels the possibly pending execution. Returns immediately, before
  /// execution has actually stopped; use waitForCompletion() to wait for all
  /// execution resources to be freed.
  ///
  /// May be called from any thread, at any point in the runner's lifetime,
  /// without serialization, and is idempotent. It is a no-op once execution has
  /// finished (state() == kFinished). Otherwise state() becomes kCancelled and
  /// the in-flight or next execute() pull surfaces the cancellation error.
  ///
  /// Cancellation is cooperative: the produce/drain phase stops promptly at the
  /// drivers' yield points. It does not interrupt an in-flight commit on the
  /// write path, nor initial metastore partition-listing (not yet cancelable).
  virtual void abort() = 0;

  /// Waits up to 'maxWaitMicros' for all activity of the execution to cease.
  /// This is used in tests to ensure that all pools are empty and unreferenced
  /// before teardown. Returns true if all activity ceased within the timeout,
  /// false otherwise.
  virtual bool waitForCompletion(int32_t maxWaitMicros) = 0;

  /// Convenience helper that synchronously drains and returns all result
  /// batches by blocking the calling thread on execute(). Every caller could
  /// instead be a coroutine awaiting execute(); this exists only where that is
  /// inconvenient — non-production and test code. Must NOT be called from a
  /// Velox executor thread (blocking there starves the executor). Holds the
  /// whole result set in memory: callers that consume batches incrementally
  /// should stream execute() rather than use this.
  std::vector<velox::RowVectorPtr> drain();
};

} // namespace facebook::axiom::runner

AXIOM_EMBEDDED_ENUM_FORMATTER(facebook::axiom::runner::Runner, State);
