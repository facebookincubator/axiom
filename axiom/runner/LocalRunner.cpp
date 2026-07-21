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

#include "axiom/runner/LocalRunner.h"
#include <folly/CancellationToken.h>
#include <folly/coro/AsyncGenerator.h>
#include <folly/coro/AsyncScope.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Coroutine.h>
#include <folly/coro/Task.h>
#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "velox/common/base/SpillConfig.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/time/Timer.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/PlanNodeStats.h"

namespace facebook::axiom::runner {
namespace {

/// Testing proxy for a split source managed by a system with full metadata
/// access.
class SimpleSplitSource : public connector::SplitSource {
 public:
  explicit SimpleSplitSource(
      std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> splits)
      : splits_(std::move(splits)) {}

  folly::coro::Task<connector::SplitBatch> co_getSplits(
      uint32_t maxSplitCount) override {
    connector::SplitBatch batch;
    auto end = std::min(
        nextIndex_ + static_cast<size_t>(maxSplitCount), splits_.size());
    for (auto i = nextIndex_; i < end; ++i) {
      batch.splits.push_back(
          connector::Split{.connectorSplit = std::move(splits_.at(i))});
    }
    nextIndex_ = end;
    batch.noMoreSplits = (nextIndex_ >= splits_.size());
    co_return batch;
  }

 private:
  std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> splits_;
  size_t nextIndex_{0};
};
} // namespace

std::shared_ptr<connector::SplitSource>
SimpleSplitSourceFactory::splitSourceForScan(
    const connector::ConnectorSessionPtr& /* session */,
    const velox::core::TableScanNode& scan,
    const std::shared_ptr<connector::PartitionType>& /*partitionType*/,
    std::optional<double> samplePercentage) {
  VELOX_USER_CHECK(
      !samplePercentage.has_value(),
      "SYSTEM sampling is not supported for pre-materialized splits");
  auto it = nodeSplitMap_.find(scan.id());
  if (it == nodeSplitMap_.end()) {
    VELOX_FAIL("Splits are not provided for scan {}", scan.id());
  }
  return std::make_shared<SimpleSplitSource>(it->second);
}

std::shared_ptr<connector::SplitSource>
ConnectorSplitSourceFactory::splitSourceForScan(
    const connector::ConnectorSessionPtr& session,
    const velox::core::TableScanNode& scan,
    const std::shared_ptr<connector::PartitionType>& partitionType,
    std::optional<double> samplePercentage) {
  const auto& handle = scan.tableHandle();
  auto metadata =
      connector::ConnectorMetadataRegistry::get(handle->connectorId());
  auto splitManager = metadata->splitManager();

  auto listCpuStart = velox::process::threadCpuNanos();
  auto listThreadId = std::this_thread::get_id();
  auto listStart = std::chrono::steady_clock::now();
  auto partitions = folly::coro::blockingWait(
      splitManager->co_listPartitions(session, handle));
  recordCpuIfSameThread(
      runtimeStats_,
      QueryRuntimeStats::kListPartitionsCpuNanos,
      listCpuStart,
      listThreadId);
  runtimeStats_.recordTiming(
      QueryRuntimeStats::kListPartitionsWallNanos,
      std::chrono::steady_clock::now() - listStart);
  runtimeStats_.recordCount(
      QueryRuntimeStats::kListPartitionsCount, partitions.size());

  return splitManager->getSplitSource(
      session, handle, partitions, partitionType, samplePercentage);
}

namespace {

std::shared_ptr<velox::exec::RemoteConnectorSplit> remoteSplit(
    const std::string& taskId) {
  return std::make_shared<velox::exec::RemoteConnectorSplit>(taskId);
}

// Builds per-task SpillDiskOptions under the base spill directory. Each task
// gets a unique subdirectory created lazily on first spill.
std::optional<velox::common::SpillDiskOptions> makeSpillDiskOptions(
    const std::string& baseSpillDirectory,
    const std::string& taskId) {
  if (baseSpillDirectory.empty()) {
    return std::nullopt;
  }
  velox::common::SpillDiskOptions options;
  options.spillDirPath = fmt::format("{}/{}", baseSpillDirectory, taskId);
  options.spillDirCreated = false;
  options.spillDirCreateCb = [path = options.spillDirPath]() {
    auto fileSystem = velox::filesystems::getFileSystem(path, nullptr);
    fileSystem->mkdir(path);
    return path;
  };
  return options;
}

folly::coro::Task<void> co_generateAndDistributeSplits(
    std::shared_ptr<connector::SplitSource> source,
    velox::core::PlanNodeId scanId,
    std::vector<std::shared_ptr<velox::exec::Task>> tasks,
    std::function<void(std::exception_ptr)> onError,
    QueryRuntimeStats& runtimeStats) {
  std::exception_ptr ex;
  // Injected by CancellableAsyncScope::add(); the runner's co_reap() requests
  // cancellation on teardown so this loop stops enumerating instead of draining
  // the source to noMoreSplits.
  const auto cancelToken = co_await folly::coro::co_current_cancellation_token;
  try {
    VELOX_CHECK(!tasks.empty(), "tasks must not be empty");

    auto getSplitsCpuStart = velox::process::threadCpuNanos();
    auto getSplitsThreadId = std::this_thread::get_id();
    auto getSplitsStart = std::chrono::steady_clock::now();
    int64_t splitCount = 0;
    size_t taskIdx = 0;
    for (;;) {
      // Teardown requested: the tasks are being reaped, so remaining splits are
      // unnecessary. Bounds the reap's split-scope join to one co_getSplits().
      if (cancelToken.isCancellationRequested()) {
        break;
      }
      auto batch = co_await source->co_getSplits(1);
      for (auto& split : batch.splits) {
        size_t targetTask;
        if (split.groupId.has_value()) {
          // The connector guarantees groupId is in [0, tasks.size()).
          VELOX_CHECK_LT(static_cast<size_t>(*split.groupId), tasks.size());
          targetTask = static_cast<size_t>(*split.groupId);
        } else {
          targetTask = taskIdx;
          taskIdx = (taskIdx + 1) % tasks.size();
        }
        tasks.at(targetTask)
            ->addSplit(
                scanId, velox::exec::Split(std::move(split.connectorSplit)));
        ++splitCount;
      }
      if (batch.noMoreSplits) {
        break;
      }
    }
    for (auto& task : tasks) {
      task->noMoreSplits(scanId);
    }

    recordCpuIfSameThread(
        runtimeStats,
        QueryRuntimeStats::kGetSplitsCpuNanos,
        getSplitsCpuStart,
        getSplitsThreadId);
    runtimeStats.recordTiming(
        QueryRuntimeStats::kGetSplitsWallNanos,
        std::chrono::steady_clock::now() - getSplitsStart);
    runtimeStats.recordCount(QueryRuntimeStats::kGetSplitsCount, splitCount);
  } catch (const folly::OperationCancelled&) {
    // co_getSplits() observed the teardown cancellation mid-call. This is a
    // clean stop, not a query error, so do not propagate it to onError().
  } catch (...) {
    ex = std::current_exception();
  }
  co_await source->co_close();
  if (ex) {
    onError(ex);
    std::rethrow_exception(ex);
  }
}

void getTopologicalOrder(
    const std::vector<optimizer::ExecutableFragment>& fragments,
    int32_t index,
    const folly::F14FastMap<std::string, int32_t>& taskPrefixToIndex,
    std::vector<bool>& visited,
    std::stack<int32_t>& indices) {
  visited[index] = true;
  for (const auto& input : fragments.at(index).inputStages) {
    if (!visited[taskPrefixToIndex.at(input.producerTaskPrefix)]) {
      getTopologicalOrder(
          fragments,
          taskPrefixToIndex.at(input.producerTaskPrefix),
          taskPrefixToIndex,
          visited,
          indices);
    }
  }
  indices.push(index);
}

std::vector<optimizer::ExecutableFragment> topologicalSort(
    const std::vector<optimizer::ExecutableFragment>& fragments) {
  folly::F14FastMap<std::string, int32_t> taskPrefixToIndex;
  for (auto i = 0; i < fragments.size(); ++i) {
    taskPrefixToIndex[fragments[i].taskPrefix] = i;
  }

  std::stack<int32_t> indices;
  std::vector<bool> visited(fragments.size(), false);
  for (auto i = 0; i < fragments.size(); ++i) {
    if (!visited[i]) {
      getTopologicalOrder(fragments, i, taskPrefixToIndex, visited, indices);
    }
  }

  VELOX_CHECK_EQ(indices.size(), fragments.size());
  std::vector<optimizer::ExecutableFragment> result;
  result.reserve(indices.size());
  while (!indices.empty()) {
    result.push_back(fragments[indices.top()]);
    indices.pop();
  }
  std::reverse(result.begin(), result.end());
  return result;
}
} // namespace

LocalRunner::LocalRunner(
    RunnerSessionPtr session,
    optimizer::MultiFragmentPlanPtr plan,
    optimizer::FinishWrite finishWrite,
    std::shared_ptr<velox::core::QueryCtx> queryCtx,
    std::shared_ptr<SplitSourceFactory> splitSourceFactory,
    std::shared_ptr<velox::memory::MemoryPool> outputPool,
    std::string baseSpillDirectory,
    QueryRuntimeStats& runtimeStats)
    : session_{std::move(session)},
      plan_{std::move(plan)},
      fragments_{topologicalSort(plan_->fragments())},
      finishWrite_{std::move(finishWrite)},
      splitSourceFactory_{std::move(splitSourceFactory)},
      baseSpillDirectory_{std::move(baseSpillDirectory)},
      runtimeStats_(runtimeStats) {
  params_.queryCtx = std::move(queryCtx);
  params_.outputPool = std::move(outputPool);
  if (params_.outputPool == nullptr) {
    // Own the cursor's output pool (held by params_ for the runner's lifetime)
    // rather than letting the cursor create an internal one. Result batches are
    // allocated here, and reap() releases the cursor while a caller may still
    // hold batches; a runner-owned pool lets those batches outlive the cursor.
    // It is freed when the runner is destroyed.
    params_.outputPool =
        params_.queryCtx->pool()->addLeafChild("localRunnerOutput");
  }
  if (!baseSpillDirectory_.empty()) {
    params_.spillDirectory = baseSpillDirectory_;
  }

  VELOX_CHECK_NOT_NULL(session_);
  VELOX_CHECK_NOT_NULL(splitSourceFactory_);
  VELOX_CHECK(!finishWrite_ || params_.outputPool != nullptr);
}

LocalRunner::~LocalRunner() {
  // A started runner must be wound down via co_await co_close() before being
  // destroyed; the destructor asserts that rather than reaping here, so no drop
  // ever runs blocking teardown on an executor thread. A never-started runner
  // (execute() created but never pulled) needs no close.
  VELOX_CHECK(
      closed_ || state_ == State::kInitialized,
      "co_close() must be awaited before destroying a started LocalRunner");
  // CancellableAsyncScope requires cancelAndJoinAsync()/joinAsync() to complete
  // before it is destroyed. co_close() already did so for a started runner; for
  // a never-started one the scope is empty and this returns immediately.
  if (!splitScopeJoined_) {
    folly::coro::blockingWait(splitScope_.cancelAndJoinAsync());
  }
}

folly::coro::Task<velox::RowVectorPtr> LocalRunner::co_pull() {
  if (!cursor_) {
    start();
  }

  velox::ContinueFuture future = velox::ContinueFuture::makeEmpty();
  if (cursor_->moveNext(&future)) {
    co_return cursor_->current();
  }
  if (!future.valid()) {
    // No future: producers are at end (or a drain boundary).
    co_return nullptr;
  }

  // The cursor wakes the consumer only when a batch is queued or all producers
  // are finished/drained, so a resolved wait-future implies the next dequeue
  // yields data-or-terminal -- a single await is enough.
  co_await std::move(future);
  if (cursor_->moveNext(&future)) {
    co_return cursor_->current();
  }
  VELOX_CHECK(
      !future.valid(), "Cursor re-blocked after a resolved wait-future");
  co_return nullptr;
}

folly::coro::AsyncGenerator<velox::RowVectorPtr> LocalRunner::execute() {
  // Cancelling the awaiting scope cancels the tasks; the next moveNext() then
  // surfaces the task error. The callback may run on another thread, but
  // cancelTasks() is safe from any thread. One registration covers the whole
  // drain, including the write path below.
  const auto token = co_await folly::coro::co_current_cancellation_token;
  folly::CancellationCallback cancelCallback{token, [this] { cancelTasks(); }};

  if (finishWrite_) {
    co_yield makeWriteResult(co_await co_runWrite());
    co_return;
  }

  while (auto rows = co_await co_pull()) {
    co_yield std::move(rows);
  }

  // Reached end of output cleanly. Move kRunning -> kFinished under 'mutex_' so
  // it serializes with cancelTasks()/onError: the transition cannot be
  // interleaved with cancelTasks()'s read-guard-then-write and thus cannot
  // clobber, or be clobbered by, a terminal state another thread set
  // concurrently.
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ == State::kRunning) {
      state_ = State::kFinished;
    }
  }
}

folly::coro::Task<int64_t> LocalRunner::co_runWrite() {
  std::vector<velox::RowVectorPtr> result;
  auto finishWrite = std::move(finishWrite_);
  std::exception_ptr drainError;
  try {
    while (auto rows = co_await co_pull()) {
      result.push_back(std::move(rows));
    }
  } catch (const std::exception&) {
    // Capture and handle after the handler: co_await is illegal inside a catch
    // handler, and the reap below must be awaited.
    drainError = std::current_exception();
  }
  if (drainError) {
    // The drain failed: cancelTasks() (cancellation) or the task onError
    // callback already recorded the terminal state. Release the write resources
    // and rethrow the drain error without touching state_. Both cleanup steps
    // are best-effort: log their own failures but never mask the drain error.
    // co_reap() here matches what the owner's co_close() would run; co_close()
    // is idempotent, so a later close is a no-op.
    try {
      co_await co_reap();
    } catch (const std::exception& e) {
      LOG(ERROR) << e.what()
                 << " while waiting for completion after error in write query";
    }
    try {
      std::move(finishWrite).abort().get();
    } catch (const std::exception& e) {
      LOG(ERROR) << e.what()
                 << " while aborting write after error in write query";
    }
    std::rethrow_exception(drainError);
  }

  int64_t rows{0};
  try {
    rows = std::move(finishWrite).commit(result).get();
  } catch (const std::exception&) {
    // The commit runs outside the drain, so no cancelTasks()/onError ran for
    // it; record the failure explicitly (under 'mutex_' to serialize with
    // cancelTasks()), setting error_ alongside state_ as the other error paths
    // do so a caller inspecting error_ after a failed commit sees it.
    {
      std::lock_guard<std::mutex> lock(mutex_);
      state_ = State::kError;
      error_ = std::current_exception();
    }
    throw;
  }
  // A successful commit is the point of no return: the write is durable, so
  // mark finished (under 'mutex_') even if a late cancel raced in.
  {
    std::lock_guard<std::mutex> lock(mutex_);
    state_ = State::kFinished;
  }
  co_return rows;
}

velox::RowVectorPtr LocalRunner::makeWriteResult(int64_t rows) {
  auto child = velox::BaseVector::create<velox::FlatVector<int64_t>>(
      velox::BIGINT(), /*size=*/1, params_.outputPool.get());
  child->set(0, rows);

  return std::make_shared<velox::RowVector>(
      params_.outputPool.get(),
      velox::ROW("rows", velox::BIGINT()),
      /*nulls=*/nullptr,
      /*length=*/1,
      std::vector<velox::VectorPtr>{std::move(child)});
}

void LocalRunner::start() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    // A cancel (or error) that landed before start() leaves the runner terminal
    // with no cursor; surface that error rather than starting and tripping the
    // state precondition below.
    if (error_) {
      std::rethrow_exception(error_);
    }
    VELOX_CHECK_EQ(state_, State::kInitialized);
  }

  params_.maxDrivers = plan_->options().numDrivers;
  params_.planNode = fragments_.back().fragment.planNode;
  params_.serialExecution = !params_.queryCtx->isExecutorSupplied();

  VELOX_CHECK_LE(
      fragments_.back().width.value_or(1),
      1,
      "Last fragment must be single-task");

  auto cursor = velox::exec::TaskCursor::create(params_);
  makeStages(cursor->task());

  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!error_) {
      cursor_ = std::move(cursor);
      state_ = State::kRunning;
    }
  }

  if (!cursor_) {
    // The cursor was not set because previous fragments had an error.
    cancelTasks();
    std::rethrow_exception(error_);
  }
}

std::shared_ptr<connector::SplitSource> LocalRunner::splitSourceForScan(
    const connector::ConnectorSessionPtr& session,
    const velox::core::TableScanNode& scan,
    const std::shared_ptr<connector::PartitionType>& partitionType,
    std::optional<double> samplePercentage) {
  return splitSourceFactory_->splitSourceForScan(
      session, scan, partitionType, samplePercentage);
}

void LocalRunner::cancelTasks() {
  // cancelTasks() may be invoked from any thread via the cancellation callback,
  // so serialize the error/state mutation under 'mutex_' against start() (which
  // reads error_ under the same lock) and a second concurrent cancelTasks().
  std::vector<std::shared_ptr<velox::exec::Task>> tasks;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    // A completed query stays finished: the cancel callback can fire after
    // execute() moved to kFinished but before it deregisters, and that late
    // cancel must neither clobber kFinished nor re-error completed tasks.
    if (state_ == State::kFinished) {
      return;
    }
    // If called without a previous error, set the error to cancellation.
    // onError records its own error before calling cancelTasks() to propagate
    // it, so it must not be overwritten here.
    if (!error_) {
      try {
        state_ = State::kCancelled;
        VELOX_FAIL("Query cancelled");
      } catch (const std::exception&) {
        error_ = std::current_exception();
      }
    }
    // Copy tasks under the lock to prevent use-after-free if reap() clears
    // stages_ concurrently. Propagate to the cursor here too, under the lock
    // that guards cursor_.
    for (auto& stage : stages_) {
      for (auto& task : stage) {
        tasks.push_back(task);
      }
    }
    if (cursor_) {
      cursor_->setError(error_);
    }
  }
  VELOX_CHECK(state_ != State::kInitialized);

  for (auto& task : tasks) {
    task->setError(error_);
  }
}

folly::coro::Task<void> LocalRunner::co_reap() {
  // Cancel and join the split scope first. This is the only teardown step
  // needed when execute() was created but never pulled (state_ == kInitialized:
  // no cursor, no tasks); the loops below are then no-ops. Idempotent via
  // splitScopeJoined_. Awaited (not blockingWait), so it releases the thread
  // while it waits instead of holding it -- this is what makes teardown safe on
  // a Velox executor thread. cancelAndJoinAsync() requests cancellation on the
  // split coroutines (which check the token each iteration) so the join is
  // bounded to at most one in-flight co_getSplits() rather than draining each
  // source to noMoreSplits.
  if (!splitScopeJoined_) {
    // Mark joined before awaiting: cancelAndJoinAsync() sets the scope's own
    // joined_ once its barrier completes, even if it then rethrows a split
    // error, so the destructor must not attempt a second join.
    splitScopeJoined_ = true;
    co_await splitScope_.cancelAndJoinAsync();
  }

  // Wait for every task to stop running, then snapshot final stats while the
  // tasks are still alive. taskCompletionFuture (unlike taskDeletionFuture)
  // does not require releasing the tasks, so stats remain readable here.
  std::vector<velox::ContinueFuture> completionFutures;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& stage : stages_) {
      for (auto& task : stage) {
        completionFutures.push_back(task->taskCompletionFuture());
      }
    }
  }
  bool completed = true;
  try {
    co_await folly::collectAll(std::move(completionFutures))
        .within(std::chrono::microseconds(kReapTimeoutMicros));
  } catch (const folly::FutureTimeout&) {
    completed = false;
  }

  // Tear down: capture final stats before the tasks go away, then drop the
  // cursor and tasks and wait for their deletion so their pools are released.
  // Resetting the cursor here (releasing its output queue) is safe because
  // result batches are allocated in outputPool -- owned by the runner or the
  // caller -- which outlives the cursor, so batches the caller still holds stay
  // valid until the runner is destroyed.
  std::vector<velox::ContinueFuture> deletionFutures;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (completed && !finalStats_.has_value()) {
      finalStats_ = aggregatedStats();
    }
    cursor_.reset();
    for (auto& stage : stages_) {
      for (auto& task : stage) {
        deletionFutures.push_back(task->taskDeletionFuture());
      }
      stage.clear();
    }
  }
  try {
    co_await folly::collectAll(std::move(deletionFutures))
        .within(std::chrono::microseconds(kReapTimeoutMicros));
  } catch (const folly::FutureTimeout&) {
    // Best effort: pools may linger briefly if a task never stops, but the reap
    // does not hang unboundedly.
  }
}

folly::coro::Task<void> LocalRunner::co_close() {
  // Idempotent: a second close, or one after the co_runWrite() error path
  // already reaped, is a no-op.
  if (closed_) {
    co_return;
  }
  // Stop anything still running so the split coroutines and tasks wind down,
  // then reap. A clean finish is already kFinished and a failure kError; only a
  // still-running run needs cancelTasks(), which lands a benign kCancelled.
  if (state_ == State::kRunning) {
    cancelTasks();
  }
  // Teardown must not throw: callers blockingWait(co_close()) on exception
  // paths (drain()'s error handling, ~AxiomReader, JoinSample's scope guard),
  // where a throw would mask the query's real error or std::terminate in a
  // destructor. The query error is already surfaced through the pull; a
  // split-scope join or task-teardown error here is secondary, so log and
  // swallow it. closed_ is set regardless so the destructor's assert passes and
  // it does not re-join.
  try {
    co_await co_reap();
  } catch (const std::exception& e) {
    LOG(ERROR) << "Error during LocalRunner teardown: " << e.what();
  }
  closed_ = true;
}

namespace {
// Returns true if the producer needs updateOutputBuffers to be called with the
// number of consumer tasks. Broadcast and arbitrary outputs need this because
// the producer doesn't know the consumer count from the plan alone.
bool needsOutputBufferUpdate(const velox::core::PlanFragment& fragment) {
  auto partitionedOutputNode =
      std::dynamic_pointer_cast<const velox::core::PartitionedOutputNode>(
          fragment.planNode);
  if (!partitionedOutputNode) {
    return false;
  }
  return partitionedOutputNode->isBroadcast() ||
      partitionedOutputNode->isArbitrary();
}

void gatherScans(
    const velox::core::PlanNodePtr& plan,
    std::vector<velox::core::TableScanNodePtr>& scans) {
  if (auto scan =
          std::dynamic_pointer_cast<const velox::core::TableScanNode>(plan)) {
    scans.push_back(scan);
    return;
  }
  for (const auto& source : plan->sources()) {
    gatherScans(source, scans);
  }
}
} // namespace

void LocalRunner::makeStages(
    const std::shared_ptr<velox::exec::Task>& lastStageTask) {
  auto sharedRunner = shared_from_this();
  // Use weak_ptr to avoid a reference cycle:
  // Task -> onError -> shared_ptr<LocalRunner> -> stages_ -> shared_ptr<Task>.
  auto onError = [weak = std::weak_ptr<LocalRunner>(sharedRunner)](
                     std::exception_ptr error) {
    auto self = weak.lock();
    if (!self) {
      return;
    }
    {
      std::lock_guard<std::mutex> lock(self->mutex_);
      if (self->error_) {
        return;
      }
      self->state_ = State::kError;
      self->error_ = std::move(error);
    }
    // Call cancelTasks() unconditionally: reading cursor_ here would race
    // start() assigning it (onError can fire from a stage task before start()
    // finishes). cancelTasks() re-locks and no-ops on a null cursor_, still
    // propagating the error to sibling tasks when the failure precedes cursor
    // creation.
    self->cancelTasks();
  };

  // Mapping from task prefix to the stage index and whether it is a broadcast.
  folly::F14FastMap<std::string, std::pair<int32_t, bool>> stageMap;
  for (auto fragmentIndex = 0; fragmentIndex < fragments_.size() - 1;
       ++fragmentIndex) {
    const auto& fragment = fragments_[fragmentIndex];
    {
      // Serialize stages_ mutation with cancelTasks()/reap(), which read it
      // under mutex_ and may run on a task thread (a task started below can
      // fire onError before makeStages() finishes building the rest of
      // stages_).
      std::lock_guard<std::mutex> lock(mutex_);
      stageMap[fragment.taskPrefix] = {
          stages_.size(), needsOutputBufferUpdate(fragment.fragment)};
      stages_.emplace_back();
    }

    auto numTasks = fragment.width.value_or(
        fragment.type == optimizer::FragmentType::kSource
            ? plan_->options().numWorkers
            : 1);
    for (auto i = 0; i < numTasks; ++i) {
      auto taskId = fmt::format(
          "local://{}/{}.{}",
          params_.queryCtx->queryId(),
          fragment.taskPrefix,
          i);
      // Each task in the stage gets a distinct taskUniqueId so AssignUniqueId
      // operators across tasks produce non-overlapping ids. Copy the shared
      // fragment so the per-task value does not leak to sibling tasks.
      auto taskFragment = fragment.fragment;
      taskFragment.taskUniqueId = i;
      auto task = velox::exec::Task::create(
          taskId,
          std::move(taskFragment),
          i,
          params_.queryCtx,
          velox::exec::Task::ExecutionMode::kParallel,
          velox::exec::ConsumerSupplier{},
          /*memoryArbitrationPriority=*/0,
          makeSpillDiskOptions(baseSpillDirectory_, taskId),
          onError);
      {
        std::lock_guard<std::mutex> lock(mutex_);
        stages_.back().push_back(task);
      }
      task->start(plan_->options().numDrivers);
    }
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    stages_.push_back({lastStageTask});
  }

  try {
    for (auto fragmentIndex = 0; fragmentIndex < fragments_.size();
         ++fragmentIndex) {
      const auto& fragment = fragments_[fragmentIndex];
      const auto& stage = stages_[fragmentIndex];

      std::vector<velox::core::TableScanNodePtr> scans;
      gatherScans(fragment.fragment.planNode, scans);

      for (const auto& scan : scans) {
        std::shared_ptr<connector::PartitionType> partitionType;
        if (auto it = fragment.groupedNodes.find(scan->id());
            it != fragment.groupedNodes.end()) {
          partitionType = it->second;
        }
        std::optional<double> samplePercentage;
        if (auto it = fragment.sampledScans.find(scan->id());
            it != fragment.sampledScans.end()) {
          samplePercentage = it->second;
        }
        auto source = splitSourceForScan(
            session_->toConnectorSession(scan->tableHandle()->connectorId()),
            *scan,
            partitionType,
            samplePercentage);
        splitScope_.add(
            folly::coro::co_withExecutor(
                params_.queryCtx->executor(),
                co_generateAndDistributeSplits(
                    source, scan->id(), stage, onError, runtimeStats_)));
      }

      for (const auto& input : fragment.inputStages) {
        const auto [sourceStage, needsUpdate] =
            stageMap[input.producerTaskPrefix];

        std::vector<std::shared_ptr<velox::exec::RemoteConnectorSplit>>
            sourceSplits;
        for (const auto& task : stages_[sourceStage]) {
          sourceSplits.push_back(remoteSplit(task->taskId()));

          if (needsUpdate) {
            task->updateOutputBuffers(static_cast<int>(stage.size()), true);
          }
        }

        for (auto& task : stage) {
          for (const auto& remote : sourceSplits) {
            task->addSplit(input.consumerNodeId, velox::exec::Split(remote));
          }
        }
      }

      for (const auto& input : fragment.inputStages) {
        for (auto& task : stage) {
          task->noMoreSplits(input.consumerNodeId);
        }
      }
    }
  } catch (const std::exception&) {
    onError(std::current_exception());
  }
}

std::vector<velox::exec::TaskStats> LocalRunner::stats() const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (finalStats_.has_value()) {
    return *finalStats_;
  }
  return aggregatedStats();
}

std::vector<velox::exec::TaskStats> LocalRunner::aggregatedStats() const {
  std::vector<velox::exec::TaskStats> result;
  for (const auto& tasks : stages_) {
    VELOX_CHECK(!tasks.empty());

    auto stats = tasks[0]->taskStats();
    for (auto i = 1; i < tasks.size(); ++i) {
      const auto moreStats = tasks[i]->taskStats();
      for (auto pipeline = 0; pipeline < stats.pipelineStats.size();
           ++pipeline) {
        auto& pipelineStats = stats.pipelineStats[pipeline];
        for (auto op = 0; op < pipelineStats.operatorStats.size(); ++op) {
          pipelineStats.operatorStats[op].add(
              moreStats.pipelineStats[pipeline].operatorStats[op]);
        }
      }
    }
    result.push_back(std::move(stats));
  }
  return result;
}

namespace {
void printCustomStats(
    const std::unordered_map<std::string, velox::RuntimeMetric>& stats,
    std::string_view indentation,
    std::ostream& stream) {
  int width = 0;
  for (const auto& entry : stats) {
    if (width < entry.first.size()) {
      width = entry.first.size();
    }
  }
  width += 3;

  // Copy to a map to get a deterministic output.
  std::map<std::string_view, velox::RuntimeMetric> orderedStats;
  for (const auto& [name, metric] : stats) {
    orderedStats[name] = metric;
  }

  for (const auto& [name, metric] : orderedStats) {
    stream << indentation << std::left << std::setw(width) << name;
    metric.printMetric(stream);
    stream << std::endl;
  }
}
} // namespace

std::string LocalRunner::printPlanWithStats(
    bool includeCustomStats,
    const std::function<void(
        const velox::core::PlanNodeId& nodeId,
        std::string_view indentation,
        std::ostream& out)>& addContext) const {
  folly::F14FastSet<velox::core::PlanNodeId> leafNodeIds;
  for (const auto& fragment : fragments_) {
    for (const auto& nodeId : fragment.fragment.planNode->leafPlanNodeIds()) {
      leafNodeIds.insert(nodeId);
    }
  }

  const auto taskStats = stats();

  folly::F14FastMap<velox::core::PlanNodeId, velox::exec::PlanNodeStats>
      planNodeStats;
  for (const auto& stats : taskStats) {
    auto planStats = velox::exec::toPlanStats(stats);
    for (auto& [id, nodeStats] : planStats) {
      bool ok = planNodeStats.emplace(id, std::move(nodeStats)).second;
      VELOX_CHECK(
          ok,
          "Plan node IDs must be unique across fragments. "
          "Found duplicate ID: {}",
          id);
    }
  }

  return plan_->toString(
      true, [&](const auto& planNodeId, const auto& indentation, auto& out) {
        if (addContext != nullptr) {
          addContext(planNodeId, indentation, out);
        }

        auto statsIt = planNodeStats.find(planNodeId);
        if (statsIt != planNodeStats.end()) {
          out << indentation
              << statsIt->second.toString(leafNodeIds.contains(planNodeId))
              << std::endl;
          if (includeCustomStats) {
            printCustomStats(statsIt->second.customStats, indentation, out);
          }
        }
      });
}

} // namespace facebook::axiom::runner
