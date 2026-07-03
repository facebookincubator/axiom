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
#include <folly/container/F14Map.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Task.h>
#include <folly/coro/Timeout.h>
#include <folly/coro/WithCancellation.h>
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
} // namespace

AXIOM_DEFINE_EMBEDDED_ENUM_NAME(Runner, State, stateNames);

void Runner::drain(
    const std::function<void(velox::RowVectorPtr)>& onBatch,
    int64_t timeoutMicros) {
  // Hold the generator as a local; drain() co_closes the runner below (on this
  // calling, non-executor thread) so the run is always reaped before return.
  auto generator = execute();
  std::atomic<bool> timedOut{false};
  auto loop = [&]() -> folly::coro::Task<void> {
    const auto token = co_await folly::coro::co_current_cancellation_token;
    folly::CancellationCallback onTimeout{token, [&] { timedOut.store(true); }};
    while (auto batch = co_await generator.next()) {
      onBatch(std::move(*batch));
    }
  };

  std::exception_ptr error;
  try {
    if (timeoutMicros > 0) {
      folly::coro::blockingWait(
          folly::coro::timeout(
              loop(), std::chrono::microseconds(timeoutMicros)));
    } else {
      folly::coro::blockingWait(loop());
    }
  } catch (const std::exception&) {
    error = std::current_exception();
  }

  // Reap whether the drain succeeded, timed out, or failed, before surfacing
  // any error. On a deadline this reaps the cancelled run; VELOX_USER_FAIL is
  // thrown only after the reap completes.
  folly::coro::blockingWait(co_close());

  if (error) {
    if (timedOut.load()) {
      VELOX_USER_FAIL(
          "Query exceeded maximum time limit of {:.2f}s",
          timeoutMicros / 1'000'000.0);
    }
    std::rethrow_exception(error);
  }
}

} // namespace facebook::axiom::runner
