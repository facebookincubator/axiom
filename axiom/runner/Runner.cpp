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
    int64_t timeoutMicros,
    folly::CancellationToken cancelToken) {
  // Hold the generator as a local; drain() co_closes the runner below (on this
  // calling, non-executor thread) so the run is always reaped before return.
  auto generator = execute();

  // Why the run stopped, attributed the instant the awaiting scope is first
  // cancelled and never overwritten afterwards. The deadline and the external
  // cancel token both cancel the same scope, so we cannot tell them apart after
  // the fact (a late external cancel would otherwise relabel a genuine
  // timeout). Recording once, at fire time, makes whichever tripped first win.
  enum class Stop : uint8_t { kNone, kDeadline, kExternalCancel };
  std::atomic<Stop> stop{Stop::kNone};

  auto loop = [&]() -> folly::coro::Task<void> {
    const auto token = co_await folly::coro::co_current_cancellation_token;
    folly::CancellationCallback onStop{
        token, [&] {
          Stop expected = Stop::kNone;
          stop.compare_exchange_strong(
              expected,
              cancelToken.isCancellationRequested() ? Stop::kExternalCancel
                                                    : Stop::kDeadline);
        }};
    while (auto batch = co_await generator.next()) {
      onBatch(std::move(*batch));
    }
  };
  // The deadline and the external cancel token both trip execute()'s awaiting
  // scope: coro::timeout injects the deadline token, and co_withCancellation
  // injects 'cancelToken' (empty by default -- a no-op for callers that pass
  // none). coro::timeout also observes the outer token, so an external cancel
  // composes with the deadline.
  auto drive = [&]() -> folly::coro::Task<void> {
    if (timeoutMicros > 0) {
      co_await folly::coro::timeout(
          loop(), std::chrono::microseconds(timeoutMicros));
    } else {
      co_await loop();
    }
  };

  std::exception_ptr error;
  try {
    folly::coro::blockingWait(
        folly::coro::co_withCancellation(cancelToken, drive()));
  } catch (const std::exception&) {
    error = std::current_exception();
  }

  // Reap whether the drain succeeded, timed out, was cancelled, or failed,
  // before surfacing any error.
  folly::coro::blockingWait(co_close());

  if (error) {
    // Report a deadline/external-cancel outcome only when THIS drain cancelled
    // the awaiting scope ('stop', set by the cancellation callback fired by
    // coro::timeout or the external token) and the run recorded no genuine
    // execution error (state() != kError). A genuine error surfaces through the
    // generator with 'stop' == kNone -- co_close() above may then stamp the
    // runner kCancelled while reaping the still-running tasks, so state() alone
    // cannot distinguish a benign wind-down from a real failure that surfaced
    // through the cursor. A real error that raced a deadline/cancel lands
    // kError and likewise wins. Either way the genuine error must surface as
    // itself.
    if (state() != State::kError) {
      if (stop.load() == Stop::kDeadline) {
        VELOX_USER_FAIL(
            "Query exceeded the execution time limit of {:.2f}s",
            timeoutMicros / 1'000'000.0);
      }
      if (stop.load() == Stop::kExternalCancel) {
        // Throw a VeloxUserError tagged with a distinct error code so clients
        // report a cancellation off a typed code, not the message.
        _VELOX_THROW(
            ::facebook::velox::VeloxUserError,
            ::facebook::velox::error_source::kErrorSourceUser.c_str(),
            kQueryCancelledErrorCode,
            /*isRetriable=*/false,
            "Query was cancelled");
      }
    }
    std::rethrow_exception(error);
  }
}

} // namespace facebook::axiom::runner
