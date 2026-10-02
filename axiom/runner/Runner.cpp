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

#include <folly/CancellationToken.h>
#include <folly/OperationCancelled.h>
#include <folly/container/F14Map.h>
#include <folly/coro/AsyncScope.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/CurrentExecutor.h>
#include <folly/coro/Invoke.h>
#include <folly/coro/Sleep.h>
#include <folly/coro/Task.h>
#include <folly/coro/WithCancellation.h>
#include <glog/logging.h>
#include <atomic>
#include <optional>
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

// Records the terminal outcome of one execution. Only the first transition
// from kRunning is accepted via compare-exchange.
enum class ExecutionOutcome {
  kRunning,
  kDrained,
  kCancelled,
  kTimedOut,
  kError,
};

// Coordinates the one-shot outcome race and stable cancellation token for one
// execution.
struct ExecutionControl {
  // Records the first terminal event that wins the execution race.
  std::atomic<ExecutionOutcome> outcome{ExecutionOutcome::kRunning};
  // Supplies the stable token consumed by the runner-specific result stream.
  folly::CancellationSource cancellation;
};

// Keeps enum checks formattable across fmt versions.
constexpr int format_as(ExecutionOutcome outcome) {
  return static_cast<int>(outcome);
}

// Claims the outcome so exactly one competing completion or stop request wins.
bool claimOutcome(ExecutionControl& execution, ExecutionOutcome desired) {
  auto expected = ExecutionOutcome::kRunning;
  return execution.outcome.compare_exchange_strong(expected, desired);
}

// Cancels the runner only when this stop request claims the outcome.
void requestStop(ExecutionControl& execution, ExecutionOutcome stopOutcome) {
  if (claimOutcome(execution, stopOutcome)) {
    execution.cancellation.requestCancellation();
  }
}

// Requests a timeout after the deadline. Cancellation of this timer task is
// consumed without changing the execution outcome.
folly::coro::Task<void> co_requestTimeout(
    std::chrono::microseconds timeout,
    std::shared_ptr<ExecutionControl> execution,
    std::shared_ptr<folly::Timekeeper> timekeeper) {
  try {
    co_await folly::coro::sleep(timeout, timekeeper.get());
  } catch (const folly::OperationCancelled&) {
    co_return;
  }
  requestStop(*execution, ExecutionOutcome::kTimedOut);
}
} // namespace

// Owns the deadline task independently of the execute() generator frame so an
// early generator destruction can be followed by co_close().
class Runner::Deadline {
 public:
  Deadline(
      folly::Executor::KeepAlive<> executor,
      std::chrono::microseconds timeout,
      std::shared_ptr<ExecutionControl> execution,
      std::shared_ptr<folly::Timekeeper> timekeeper) {
    scope_.add(
        folly::coro::co_withExecutor(
            std::move(executor),
            co_requestTimeout(
                timeout, std::move(execution), std::move(timekeeper))));
  }

  // Cancels the timer and waits until it no longer references execution state.
  folly::coro::Task<void> co_stop() {
    co_await scope_.cancelAndJoinAsync();
  }

 private:
  // Contains the one cooperative deadline task.
  folly::coro::CancellableAsyncScope scope_;
};

namespace {

// Applies the awaiting token only while one pull is pending and forwards stop
// requests through the execution's stable cancellation token.
folly::coro::Task<std::optional<velox::RowVectorPtr>> co_pullNext(
    folly::coro::AsyncGenerator<velox::RowVectorPtr>& generator,
    ExecutionControl& execution) {
  const auto pullToken = co_await folly::coro::co_current_cancellation_token;
  folly::CancellationCallback pullCancellation{
      pullToken,
      [&execution] { requestStop(execution, ExecutionOutcome::kCancelled); }};
  auto next = co_await folly::coro::co_withCancellation(
      execution.cancellation.getToken(), generator.next());
  if (!next) {
    co_return std::nullopt;
  }
  co_return std::move(*next);
}

// Reaps the runner under a cancellation shield while preserving an earlier
// terminal error over a cleanup failure.
folly::coro::Task<std::exception_ptr>
co_reap(Runner& runner, ExecutionControl& execution, std::exception_ptr error) {
  try {
    co_await folly::coro::co_withCancellation(
        folly::CancellationToken{}, runner.co_close());
  } catch (const std::exception& exception) {
    if (execution.outcome.load() == ExecutionOutcome::kDrained) {
      error = std::current_exception();
      execution.outcome.store(ExecutionOutcome::kError);
    } else {
      LOG(WARNING) << "co_close() failed during reap, surfacing the original "
                      "stop reason instead: "
                   << exception.what();
    }
  } catch (...) {
    if (execution.outcome.load() == ExecutionOutcome::kDrained) {
      error = std::current_exception();
      execution.outcome.store(ExecutionOutcome::kError);
    } else {
      LOG(WARNING) << "co_close() failed during reap with a non-standard "
                      "exception, surfacing the original stop reason instead";
    }
  }
  co_return error;
}

// Stops the deadline, reaps the runner, and reports the outcome that won the
// execution race.
folly::coro::Task<void> co_finalizeExecution(
    Runner& runner,
    ExecutionControl& execution,
    std::exception_ptr error,
    int64_t timeoutMicros) {
  error = co_await co_reap(runner, execution, std::move(error));
  if (execution.outcome.load() == ExecutionOutcome::kTimedOut) {
    VELOX_USER_FAIL(
        "Query exceeded maximum time limit of {:.2f}s",
        timeoutMicros / 1'000'000.0);
  }
  if (execution.outcome.load() == ExecutionOutcome::kCancelled) {
    throw folly::OperationCancelled{};
  }
  if (error) {
    std::rethrow_exception(error);
  }
  VELOX_CHECK_EQ(execution.outcome.load(), ExecutionOutcome::kDrained);
}

// Reaps after a consumer callback fails without replacing the callback error
// with a secondary cleanup failure.
folly::coro::Task<void> co_closeAfterConsumerError(Runner& runner) {
  try {
    co_await folly::coro::co_withCancellation(
        folly::CancellationToken{}, runner.co_close());
  } catch (const std::exception& exception) {
    LOG(WARNING) << "co_close() failed after a result consumer error: "
                 << exception.what();
  } catch (...) {
    LOG(WARNING) << "co_close() failed with a non-standard exception after a "
                    "result consumer error";
  }
}
} // namespace

AXIOM_DEFINE_EMBEDDED_ENUM_NAME(Runner, State, stateNames);

folly::coro::AsyncGenerator<velox::RowVectorPtr> Runner::execute(
    int64_t timeoutMicros) {
  std::exception_ptr error;
  auto execution = std::make_shared<ExecutionControl>();

  std::optional<folly::Executor::KeepAlive<>> deadlineExecutor;
  if (timeoutMicros > 0) {
    deadlineExecutor.emplace(co_await folly::coro::co_current_executor);
  }

  {
    auto lifecycle = lifecycle_.wlock();
    VELOX_CHECK(
        !lifecycle->executionStarted && !lifecycle->closeStarted,
        "Runner execution has already started or closed");
    lifecycle->executionStarted = true;
    if (deadlineExecutor) {
      lifecycle->deadline = std::make_shared<Deadline>(
          std::move(*deadlineExecutor),
          std::chrono::microseconds(timeoutMicros),
          execution,
          timeoutTimekeeper_);
    }
  }

  auto generator = executeImpl();
  try {
    while (execution->outcome.load() == ExecutionOutcome::kRunning) {
      auto next = co_await co_pullNext(generator, *execution);
      if (!next) {
        claimOutcome(*execution, ExecutionOutcome::kDrained);
        break;
      }
      co_yield std::move(*next);
    }
  } catch (const folly::OperationCancelled&) {
    claimOutcome(*execution, ExecutionOutcome::kCancelled);
  } catch (...) {
    error = std::current_exception();
    claimOutcome(*execution, ExecutionOutcome::kError);
  }

  co_await co_finalizeExecution(
      *this, *execution, std::move(error), timeoutMicros);
}

void Runner::testingSetTimekeeper(
    std::shared_ptr<folly::Timekeeper> timekeeper) {
  VELOX_CHECK_NOT_NULL(timekeeper);
  auto lifecycle = lifecycle_.wlock();
  VELOX_CHECK(
      !lifecycle->executionStarted,
      "The deadline timekeeper cannot change after execution starts");
  timeoutTimekeeper_ = std::move(timekeeper);
}

folly::coro::Task<void> Runner::co_close() {
  bool ownsClose{false};
  bool alreadyFinished{false};
  std::shared_ptr<Deadline> deadline;
  std::exception_ptr closeError;
  {
    auto lifecycle = lifecycle_.wlock();
    alreadyFinished = lifecycle->closeFinished;
    closeError = lifecycle->closeError;
    if (!lifecycle->closeStarted) {
      lifecycle->closeStarted = true;
      ownsClose = true;
      deadline = lifecycle->deadline;
    }
  }

  if (alreadyFinished) {
    if (closeError) {
      std::rethrow_exception(closeError);
    }
    co_return;
  }
  if (!ownsClose) {
    co_await closeFinished_;
    closeError = lifecycle_.rlock()->closeError;
    if (closeError) {
      std::rethrow_exception(closeError);
    }
    co_return;
  }

  try {
    if (deadline) {
      co_await folly::coro::co_withCancellation(
          folly::CancellationToken{}, deadline->co_stop());
    }
  } catch (...) {
    closeError = std::current_exception();
  }
  try {
    co_await folly::coro::co_withCancellation(
        folly::CancellationToken{}, co_closeImpl());
  } catch (const std::exception& exception) {
    if (!closeError) {
      closeError = std::current_exception();
    } else {
      LOG(WARNING) << "Runner-specific cleanup also failed: "
                   << exception.what();
    }
  } catch (...) {
    if (!closeError) {
      closeError = std::current_exception();
    } else {
      LOG(WARNING) << "Runner-specific cleanup also failed with a "
                      "non-standard exception";
    }
  }

  {
    auto lifecycle = lifecycle_.wlock();
    lifecycle->closeError = closeError;
    lifecycle->closeFinished = true;
  }
  closeFinished_.post();
  if (closeError) {
    std::rethrow_exception(closeError);
  }
}

void Runner::drain(
    const std::function<void(velox::RowVectorPtr)>& onBatch,
    int64_t timeoutMicros) {
  folly::coro::blockingWait(
      folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
        std::exception_ptr consumerError;
        {
          auto generator = execute(timeoutMicros);
          while (auto batch = co_await generator.next()) {
            try {
              onBatch(std::move(*batch));
            } catch (...) {
              consumerError = std::current_exception();
              break;
            }
          }
        }
        if (consumerError) {
          co_await co_closeAfterConsumerError(*this);
          std::rethrow_exception(consumerError);
        }
      }));
}

} // namespace facebook::axiom::runner
