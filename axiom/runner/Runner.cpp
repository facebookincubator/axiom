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

#include "axiom/runner/Runner.h"

#include <atomic>
#include <optional>

#include <folly/CancellationToken.h>
#include <folly/OperationCancelled.h>
#include <folly/container/F14Map.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Cleanup.h>
#include <folly/coro/Invoke.h>
#include <folly/coro/ScopeExit.h>
#include <folly/coro/Task.h>
#include <folly/coro/WithCancellation.h>
#include <folly/futures/Future.h>
#include <glog/logging.h>

#include "velox/common/base/Exceptions.h"

namespace facebook::axiom::runner {

namespace {
const auto& stateNames() {
  static const folly::F14FastMap<Runner::State, std::string_view> kNames = {
      {Runner::State::kInitialized, "initialized"},
      {Runner::State::kRunning, "running"},
      {Runner::State::kCancelled, "cancelled"},
      {Runner::State::kError, "error"},
      {Runner::State::kFinished, "finished"},
  };

  return kNames;
}

// Identifies the first source that requests execution cancellation.
enum class StopReason { kNone, kCaller, kDeadline };

// Coordinates stop ownership and the stable cancellation token for one
// execution. The runner's State remains the terminal execution outcome.
struct ExecutionControl {
  // Records which cancellation source won the execution-wide stop race.
  std::atomic<StopReason> stopReason{StopReason::kNone};
  // Supplies the stable token consumed by the runner-specific result stream.
  folly::CancellationSource cancellation;
};

// Records the first stop reason and trips the stable token; later requests keep
// that reason.
void requestStop(ExecutionControl& execution, StopReason reason) {
  auto expected = StopReason::kNone;
  if (execution.stopReason.compare_exchange_strong(expected, reason)) {
    execution.cancellation.requestCancellation();
  }
}

// Runs one cleanup action without allowing a secondary failure to escape the
// generator's asynchronous scope exit.
folly::coro::Task<void> co_logCleanupFailure(
    std::string_view operation,
    folly::coro::Task<void> cleanup) {
  try {
    co_await std::move(cleanup);
  } catch (const std::exception& exception) {
    LOG(ERROR) << operation << " failed: " << exception.what();
  } catch (...) {
    LOG(ERROR) << operation << " failed with a non-standard exception";
  }
}

// Applies the awaiting token only while one pull is pending and forwards stop
// requests through the execution's stable cancellation token.
folly::coro::Task<std::optional<velox::RowVectorPtr>> co_pullNext(
    folly::coro::AsyncGenerator<velox::RowVectorPtr>& generator,
    ExecutionControl& execution) {
  const auto pullToken = co_await folly::coro::co_current_cancellation_token;
  folly::CancellationCallback pullCancellation{
      pullToken, [&execution] { requestStop(execution, StopReason::kCaller); }};
  auto next = co_await folly::coro::co_withCancellation(
      execution.cancellation.getToken(), generator.next());
  if (!next) {
    co_return std::nullopt;
  }
  co_return std::move(*next);
}

} // namespace

AXIOM_DEFINE_EMBEDDED_ENUM_NAME(Runner, State, stateNames);

folly::coro::CleanableAsyncGenerator<velox::RowVectorPtr> Runner::execute(
    int64_t timeoutMicros) {
  VELOX_CHECK_EQ(
      executionStarted_.exchange(true),
      false,
      "Runner execution has already started");

  std::exception_ptr error;
  auto execution = std::make_shared<ExecutionControl>();
  auto&& [deadlineFuture] = co_await folly::coro::co_scope_exit(
      [this](std::optional<folly::Future<folly::Unit>> future)
          -> folly::coro::Task<void> {
        if (future.has_value()) {
          future->cancel();
        }
        co_await co_logCleanupFailure(
            "Runner resource cleanup", this->co_cleanupImpl());
      },
      std::optional<folly::Future<folly::Unit>>{});

  if (timeoutMicros > 0) {
    deadlineFuture =
        folly::futures::sleep(std::chrono::microseconds(timeoutMicros))
            .toUnsafeFuture()
            .thenValue([execution](folly::Unit) {
              requestStop(*execution, StopReason::kDeadline);
            });
  }

  auto generator = this->executeImpl();
  try {
    while (true) {
      auto next = co_await co_pullNext(generator, *execution);
      if (!next) {
        break;
      }
      co_yield std::move(*next);
    }
  } catch (...) {
    error = std::current_exception();
  }

  const auto finalState = state();
  if (deadlineFuture.has_value()) {
    deadlineFuture->cancel();
    deadlineFuture.reset();
  }
  co_await co_logCleanupFailure("Runner resource cleanup", co_cleanupImpl());

  switch (finalState) {
    case State::kFinished:
      if (error) {
        std::rethrow_exception(error);
      }
      co_return;
    case State::kError:
      if (error) {
        std::rethrow_exception(error);
      }
      VELOX_FAIL("Runner entered the error state without an exception");
    case State::kCancelled:
      if (execution->stopReason.load() == StopReason::kDeadline) {
        VELOX_USER_FAIL(
            "Query exceeded maximum time limit of {:.2f}s",
            timeoutMicros / 1'000'000.0);
      }
      throw folly::OperationCancelled{};
    case State::kInitialized:
    case State::kRunning:
      if (error) {
        std::rethrow_exception(error);
      }
      VELOX_FAIL("Runner result stream ended in state {}", finalState);
  }
  VELOX_UNREACHABLE();
}

void Runner::drain(
    const std::function<void(velox::RowVectorPtr)>& onBatch,
    int64_t timeoutMicros) {
  folly::coro::blockingWait(
      folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
        auto&& [generator] = co_await folly::coro::co_scope_exit(
            folly::coro::co_cleanup, execute(timeoutMicros));
        while (auto batch = co_await generator.next()) {
          onBatch(std::move(*batch));
        }
      }));
}

} // namespace facebook::axiom::runner
