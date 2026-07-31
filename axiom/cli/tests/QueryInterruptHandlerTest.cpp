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

#include "axiom/cli/QueryInterruptHandler.h"

#include <signal.h>
#include <chrono>

#include <folly/CancellationToken.h>
#include <folly/synchronization/Baton.h>
#include <gtest/gtest.h>

namespace axiom::sql {
namespace {

// Blocks until 'token' is cancelled or 'timeout' elapses; returns whether it
// was cancelled. Event-driven: the CancellationCallback posts the baton the
// moment cancellation is requested, and fires inline if 'token' is already
// cancelled, so there is no poll and no fixed sleep.
bool waitForCancellation(
    const folly::CancellationToken& token,
    std::chrono::milliseconds timeout) {
  folly::Baton<> baton;
  folly::CancellationCallback callback(token, [&] { baton.post(); });
  return baton.try_wait_for(timeout);
}

// A SIGINT delivered while armed trips the armed token. raise() runs the
// handler on this thread; the drain thread then requests cancellation, so the
// cancellation lands shortly after (bounded wait, not a race).
TEST(QueryInterruptHandlerTest, sigintCancelsArmedToken) {
  QueryInterruptHandler handler;
  const auto token = handler.arm();

  raise(SIGINT);

  EXPECT_TRUE(waitForCancellation(token, std::chrono::seconds(5)));
}

// With cancellation disarmed, SIGINT is absorbed: nothing is cancelled and the
// process is not terminated.
TEST(QueryInterruptHandlerTest, disarmedSigintIsAbsorbed) {
  QueryInterruptHandler handler;
  const auto token = handler.arm();
  handler.disarm();

  raise(SIGINT);

  // Fence on the drain thread finishing with this exact signal, then confirm it
  // absorbed it rather than cancelling -- deterministic, no fixed sleep.
  const uint64_t seq = handler.testingSignalSeq();
  ASSERT_TRUE(
      handler.testingWaitForProcessedSignalSeq(seq, std::chrono::seconds(5)));
  EXPECT_FALSE(token.isCancellationRequested());
}

// A SIGINT delivered before arming must not cancel the next query: only a
// signal counted after arm() cancels its token. Guards against a stray Ctrl+C
// between statements (init script, --repeat) cancelling the next query.
TEST(QueryInterruptHandlerTest, staleSigintDoesNotCancelNextQuery) {
  QueryInterruptHandler handler;

  // SIGINT arrives before arming. raise() runs the handler synchronously on
  // this thread, so the signal is counted before arm().
  raise(SIGINT);

  const auto token = handler.arm();

  // The pre-arm signal must not cancel this token. Fence on the drain thread
  // finishing with it, then assert it was not applied -- deterministic.
  const uint64_t staleSeq = handler.testingSignalSeq();
  ASSERT_TRUE(handler.testingWaitForProcessedSignalSeq(
      staleSeq, std::chrono::seconds(5)));
  EXPECT_FALSE(token.isCancellationRequested());

  // A signal delivered after arming does cancel it.
  raise(SIGINT);
  EXPECT_TRUE(waitForCancellation(token, std::chrono::seconds(5)));
}

// The handler restores the previous SIGINT disposition when it is destroyed.
TEST(QueryInterruptHandlerTest, restoresPreviousDispositionOnDestruction) {
  struct sigaction ignore{};
  ignore.sa_handler = SIG_IGN;
  sigemptyset(&ignore.sa_mask);
  ASSERT_EQ(sigaction(SIGINT, &ignore, nullptr), 0);

  {
    QueryInterruptHandler handler;
    // While installed, the disposition is the handler's, not the prior SIG_IGN.
    struct sigaction current{};
    ASSERT_EQ(sigaction(SIGINT, nullptr, &current), 0);
    EXPECT_NE(current.sa_handler, SIG_IGN);
  }

  // After destruction the prior disposition (SIG_IGN) is back.
  struct sigaction restored{};
  ASSERT_EQ(sigaction(SIGINT, nullptr, &restored), 0);
  EXPECT_EQ(restored.sa_handler, SIG_IGN);

  // Leave SIGINT at the default so a stray signal cannot wedge later tests.
  struct sigaction dfl{};
  dfl.sa_handler = SIG_DFL;
  sigemptyset(&dfl.sa_mask);
  sigaction(SIGINT, &dfl, nullptr);
}

// A second live handler is rejected: the singleton self-pipe fd would otherwise
// be redirected.
TEST(QueryInterruptHandlerTest, rejectsSecondConcurrentHandler) {
  QueryInterruptHandler handler;
  EXPECT_ANY_THROW({ QueryInterruptHandler second; });
}

} // namespace
} // namespace axiom::sql
