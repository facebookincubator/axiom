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

#include <folly/container/F14Map.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Invoke.h>
#include <folly/coro/Task.h>
#include <folly/coro/Timeout.h>
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
} // namespace

AXIOM_DEFINE_EMBEDDED_ENUM_NAME(Runner, State, stateNames);

void Runner::drain(
    const std::function<void(velox::RowVectorPtr)>& onBatch,
    int64_t timeoutMicros) {
  folly::coro::blockingWait(
      folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
        auto generator = execute();
        auto loop = [&]() -> folly::coro::Task<void> {
          while (auto batch = co_await generator.next()) {
            onBatch(std::move(*batch));
          }
        };
        // co_invoke the loop: folly::coro::timeout() takes the awaitable by
        // value and does not immediately co_await it, so invoking the lambda
        // bare would leave the returned Task holding a dangling reference.
        std::exception_ptr error;
        bool timedOut = false;
        try {
          if (timeoutMicros > 0) {
            co_await folly::coro::timeout(
                folly::coro::co_invoke(loop),
                std::chrono::microseconds(timeoutMicros));
          } else {
            co_await folly::coro::co_invoke(loop);
          }
        } catch (const folly::FutureTimeout&) {
          timedOut = true;
        } catch (const std::exception&) {
          error = std::current_exception();
        }
        // Reap regardless, before surfacing any error or the deadline failure.
        // A reap failure must not mask the original stop reason: if the drain
        // already failed or timed out, keep that and let the reap error be
        // secondary.
        try {
          co_await co_close();
        } catch (const std::exception& e) {
          if (!timedOut && !error) {
            throw;
          }
          LOG(WARNING) << "co_close() failed during reap, surfacing the "
                          "original stop reason instead: "
                       << e.what();
        }
        if (timedOut) {
          VELOX_USER_FAIL(
              "Query exceeded maximum time limit of {:.2f}s",
              timeoutMicros / 1'000'000.0);
        }
        if (error) {
          std::rethrow_exception(error);
        }
      }));
}

} // namespace facebook::axiom::runner
