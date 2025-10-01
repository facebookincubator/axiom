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

#include "axiom/common/Enums.h"
#include "velox/exec/TaskStats.h"

/// Base classes for multifragment Velox query execution.
namespace facebook::axiom::runner {

/// Base class for asynchronous split generation. Splits generation may
/// be started in parallel for all scans present in the task. Concurrency
/// details are dependent on the specific implementation
/// of derived classes and are not mandated by the interface.
class SplitGenerator {
 public:
  virtual ~SplitGenerator() = default;

  /// Interrupt any running split generation processes. This method should
  /// set a flag that causes ongoing split generation to stop voluntarily.
  /// Split generation should avoid long-blocking calls in order to remain
  /// responsive to interruption. If split generation is already complete
  /// at the time of the interruption, this may have no effect.
  virtual void interrupt() = 0;

  /// Generate splits asynchronously for the given stage and table scan node.
  /// Returns a ContinueFuture that will be fulfilled when split generation
  /// is complete. Synchronous split generators may perform split generation
  /// immediately and return a fulfilled future, while asynchronous generators
  /// should schedule generation on a separate executor and return the
  /// corresponding future.
  ///
  /// @param stage The stage that will receive the splits
  /// @param node The table scan node for which to generate splits
  /// @return A ContinueFuture that completes when all splits are generated
  /// or the SplitGenerator has received an interrupt.
  virtual velox::ContinueFuture generateSplits(
      const std::vector<std::shared_ptr<velox::exec::Task>>& stage,
      velox::core::TableScanNodePtr node) = 0;
};

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

  /// Returns the next batch of results. Returns nullptr when no more results.
  /// Throws any execution time errors. The result is allocated in the pool of
  /// QueryCtx given to the Runner implementation. The caller is responsible for
  /// serializing calls from different threads.
  virtual velox::RowVectorPtr next() = 0;

  /// Returns Task stats for each fragment of the plan. The stats correspond 1:1
  /// to the stages in the MultiFragmentPlan. This may be called at any time.
  /// before waitForCompletion() or abort().
  virtual std::vector<velox::exec::TaskStats> stats() const = 0;

  /// Returns the state of execution.
  virtual State state() const = 0;

  /// Cancels the possibly pending execution. Returns immediately, thus before
  /// the execution is actually finished. Use waitForCompletion() to wait for
  /// all execution resources to be freed. May be called from any thread without
  /// serialization.
  virtual void abort() = 0;

  /// Waits up to 'maxWaitMicros' for all activity of the execution to cease.
  /// This is used in tests to ensure that all pools are empty and unreferenced
  /// before teardown.
  virtual void waitForCompletion(int32_t maxWaitMicros) = 0;
};

} // namespace facebook::axiom::runner

AXIOM_EMBEDDED_ENUM_FORMATTER(facebook::axiom::runner::Runner, State);
