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

#include "axiom/connectors/ConnectorContext.h"

#include <atomic>
#include <barrier>
#include <thread>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/common/base/ConcurrentRuntimeStatWriter.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::connector {
namespace {

ConnectorContextPtr makeContext(
    ConnectorProperties properties,
    std::atomic<int32_t>* writerCalls,
    velox::BaseRuntimeStatWriter& writer) {
  return std::make_shared<ConnectorContext>(
      "q1",
      "user",
      std::move(properties),
      [writerCalls, &writer](
          std::string_view) -> std::shared_ptr<velox::BaseRuntimeStatWriter> {
        ++*writerCalls;
        return std::shared_ptr<velox::BaseRuntimeStatWriter>(
            &writer, [](auto*) {});
      });
}

// Every caller in a query reaches a connector through one session.
TEST(ConnectorContextTest, sessionForIsMemoizedPerConnector) {
  velox::ConcurrentRuntimeStatWriter writer;
  std::atomic<int32_t> writerCalls{0};
  auto context = makeContext({}, &writerCalls, writer);

  auto first = context->sessionFor("a");
  auto second = context->sessionFor("a");

  EXPECT_EQ(first.get(), second.get());
  EXPECT_EQ(writerCalls, 1);
  EXPECT_EQ(&first->statsWriter(), &writer);
}

// Two connectors of one query get separate sessions.
TEST(ConnectorContextTest, connectorsGetDistinctSessions) {
  velox::ConcurrentRuntimeStatWriter writer;
  std::atomic<int32_t> writerCalls{0};
  auto context = makeContext({}, &writerCalls, writer);

  auto firstSession = context->sessionFor("a");
  auto secondSession = context->sessionFor("b");

  EXPECT_NE(firstSession.get(), secondSession.get());
  EXPECT_EQ(writerCalls, 2);
}

// A connector sees only its own slice of the query's properties.
TEST(ConnectorContextTest, sessionCarriesThisConnectorsProperties) {
  velox::ConcurrentRuntimeStatWriter writer;
  std::atomic<int32_t> writerCalls{0};
  auto context = makeContext(
      ConnectorProperties{
          {"a", Properties{{"max_rows_per_scan", "10"}}},
          {"b", Properties{{"max_rows_per_scan", "20"}}}},
      &writerCalls,
      writer);

  auto session = context->sessionFor("a");

  EXPECT_EQ(session->queryId(), "q1");
  EXPECT_EQ(session->user(), "user");
  EXPECT_EQ(session->property("max_rows_per_scan"), "10");
}

// The contract is that concurrent callers get one session, not merely one per
// caller, so the writer is requested exactly once.
TEST(ConnectorContextTest, concurrentCallersGetOneSession) {
  velox::ConcurrentRuntimeStatWriter writer;
  std::atomic<int32_t> writerCalls{0};
  auto context = makeContext({}, &writerCalls, writer);

  constexpr int32_t kThreads = 8;
  std::vector<ConnectorSessionPtr> sessions(kThreads);
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  std::barrier barrier{kThreads};
  for (int32_t i = 0; i < kThreads; ++i) {
    threads.emplace_back([&, i] {
      barrier.arrive_and_wait();
      sessions[i] = context->sessionFor("a");
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_THAT(sessions, testing::Each(sessions[0]));
  EXPECT_EQ(writerCalls, 1);
}

// A throw while building leaves the session unbuilt, so the next caller builds
// it rather than seeing a half-made one or the earlier failure.
TEST(ConnectorContextTest, sessionIsBuiltAfterAFailedAttempt) {
  velox::ConcurrentRuntimeStatWriter writer;
  bool threw = false;
  auto context = std::make_shared<ConnectorContext>(
      "q1",
      "user",
      ConnectorProperties{},
      [&](std::string_view) -> std::shared_ptr<velox::BaseRuntimeStatWriter> {
        if (!threw) {
          threw = true;
          VELOX_FAIL("Writer unavailable");
        }
        return std::shared_ptr<velox::BaseRuntimeStatWriter>(
            &writer, [](auto*) {});
      });

  VELOX_ASSERT_THROW(context->sessionFor("a"), "Writer unavailable");

  EXPECT_EQ(&context->sessionFor("a")->statsWriter(), &writer);
}

// A context without a writer provider has no way to wire a session.
TEST(ConnectorContextTest, contextRequiresAWriterProvider) {
  VELOX_ASSERT_THROW(
      ConnectorContext("q1", "user", {}, nullptr),
      "requires a stat writer provider");
}

// A provider that yields no writer fails rather than leaving a session unwired.
TEST(ConnectorContextTest, nullWriterFromProviderFails) {
  auto context = std::make_shared<ConnectorContext>(
      "q1",
      "user",
      ConnectorProperties{},
      [](std::string_view) -> std::shared_ptr<velox::BaseRuntimeStatWriter> {
        return nullptr;
      });
  VELOX_ASSERT_THROW(
      context->sessionFor("a"), "Stat writer provider returned null");
}

} // namespace
} // namespace facebook::axiom::connector
