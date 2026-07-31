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

#include "axiom/cli/Console.h"
#include <folly/CancellationToken.h>
#include <folly/ScopeGuard.h>
#include <folly/coro/AsyncGenerator.h>
#include <folly/coro/CurrentExecutor.h>
#include <folly/synchronization/Baton.h>
#include <gtest/gtest.h>
#include <signal.h>
#include <atomic>
#include <chrono>
#include <thread>
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/tests/TestConnector.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

DECLARE_string(query);

using namespace facebook::velox;

namespace axiom::sql {
namespace {

// A SqlQueryRunner whose co_run() blocks until cancellation, then reports it
// exactly as the real co_run() does. Lets the Console cancellation path be
// tested deterministically: the test waits for co_run() to be entered (the
// query's source is armed by runOnce beforehand), delivers one SIGINT, and the
// fake observes the resulting cancellation -- no query executes and no
// signal-retry loop is needed.
class FakeCancelRunner : public SqlQueryRunner {
 public:
  using SqlQueryRunner::SqlQueryRunner;

  // Posted once co_run() has been entered and the cancellation source is armed.
  folly::Baton<>& entered() {
    return entered_;
  }

  // Whether the RunOptions handed to co_run() carried a live token, i.e.
  // Console set RunOptions::cancellationToken and run() forwarded it down.
  bool sawCancellableToken() const {
    return sawCancellableToken_.load();
  }

  folly::coro::AsyncGenerator<SqlResultChunk> co_run(
      std::string /*sql*/,
      RunOptions options) override {
    sawCancellableToken_.store(options.cancellationToken.canBeCancelled());
    entered_.post();
    // Observe cancellation the way the real co_run() does: ambiently, through
    // the token run() composes onto the drain from options.cancellationToken.
    // Reacting to options.cancellationToken directly would still pass if run()
    // stopped composing the token, so read the ambient one it actually
    // installs.
    const auto token = co_await folly::coro::co_current_cancellation_token;
    folly::Baton<> cancelled;
    folly::CancellationCallback callback(token, [&] { cancelled.post(); });
    // Bounded so a cancellation regression fails the test instead of hanging.
    if (!cancelled.try_wait_for(std::chrono::seconds(30))) {
      co_return;
    }
    throw QueryCancelledError{};
  }

 private:
  folly::Baton<> entered_;
  std::atomic<bool> sawCancellableToken_{false};
};

class ConsoleTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    facebook::velox::memory::MemoryManager::testingSetInstance(
        facebook::velox::memory::MemoryManager::Options{});
  }

  void TearDown() override {
    // Restore FLAGS_query to avoid polluting other tests.
    FLAGS_query = "";
    for (const auto& id : connectorIds_) {
      facebook::axiom::connector::ConnectorMetadataRegistry::global().erase(id);
      facebook::velox::connector::ConnectorRegistry::global().erase(id);
    }
  }

  template <typename RunnerT = SqlQueryRunner>
  std::unique_ptr<RunnerT> makeRunner(PermissionCheck permissionCheck = {}) {
    auto runner = std::make_unique<RunnerT>("test_user");

    runner->initialize(
        [&]() {
          static int32_t kCounter = 0;

          auto testConnector =
              std::make_shared<facebook::axiom::connector::TestConnector>(
                  fmt::format("console_test{}", kCounter++));
          facebook::velox::connector::ConnectorRegistry::global().insert(
              testConnector->connectorId(), testConnector);
          facebook::axiom::connector::ConnectorMetadataRegistry::global()
              .insert(testConnector->connectorId(), testConnector->metadata());

          connectorIds_.emplace_back(testConnector->connectorId());

          return std::make_pair(
              testConnector->connectorId(),
              std::string(
                  facebook::axiom::connector::TestConnector::kDefaultSchema));
        },
        std::move(permissionCheck));

    return runner;
  }

 private:
  std::vector<std::string> connectorIds_;
};

TEST_F(ConsoleTest, permissionCheckCalledBeforeExecution) {
  bool called = false;
  std::string capturedSql;
  std::string capturedCatalog;

  auto runner = makeRunner([&](std::string_view /*queryId*/,
                               std::string_view sql,
                               std::string_view catalog,
                               std::optional<std::string_view> /*schema*/,
                               const auto& /*views*/,
                               const auto& /*referencedTables*/) {
    called = true;
    capturedSql = std::string(sql);
    capturedCatalog = std::string(catalog);
    return std::shared_ptr<facebook::velox::filesystems::TokenProvider>{};
  });

  Console console{*runner};
  console.initialize();

  FLAGS_query = "SELECT 1";
  console.run();

  ASSERT_TRUE(called);
  EXPECT_EQ(capturedSql, "SELECT 1");
  EXPECT_FALSE(capturedCatalog.empty());
}

// SIGINT during a running query cancels that query and leaves the CLI alive:
// run() installs the interrupt handler for the session, runOnce registers the
// query's cancellation source, and Console reports the cancellation rather than
// terminating. FakeCancelRunner holds run() open until the source is tripped,
// so the signal is delivered at a deterministic point (no retry loop).
TEST_F(ConsoleTest, sigintCancelsRunningQuery) {
  auto runner = makeRunner<FakeCancelRunner>();
  ASSERT_TRUE(runner);

  Console console{*runner};
  console.initialize();
  FLAGS_query = "SELECT 1";

  // Ignore SIGINT outside run()'s handler window (before it installs, after it
  // restores) so a signal landing there is harmless rather than terminating the
  // test process.
  struct sigaction ignore{};
  ignore.sa_handler = SIG_IGN;
  sigemptyset(&ignore.sa_mask);
  ASSERT_EQ(sigaction(SIGINT, &ignore, nullptr), 0);

  testing::internal::CaptureStderr();
  std::thread queryThread([&] { console.run(); });
  // Join on any early exit (e.g. an ASSERT failure below) so the thread is
  // never destroyed while joinable -- that would std::terminate the test
  // process.
  SCOPE_EXIT {
    if (queryThread.joinable()) {
      queryThread.join();
    }
  };

  // Wait until run() is executing with the query's cancellation source armed,
  // then deliver exactly one SIGINT. The handler trips the armed source and the
  // fake runner observes the cancellation, so run() returns on its own.
  ASSERT_TRUE(runner->entered().try_wait_for(std::chrono::seconds(30)));
  raise(SIGINT);
  queryThread.join();

  // run() carried Console's per-query token through RunOptions into co_run.
  EXPECT_TRUE(runner->sawCancellableToken());

  const std::string captured = testing::internal::GetCapturedStderr();
  EXPECT_NE(captured.find("Query cancelled."), std::string::npos);

  struct sigaction dfl{};
  dfl.sa_handler = SIG_DFL;
  sigemptyset(&dfl.sa_mask);
  sigaction(SIGINT, &dfl, nullptr);
}

} // namespace
} // namespace axiom::sql
