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
#include <thread>
#include <vector>

#include <folly/synchronization/test/Barrier.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "axiom/connectors/tests/TestConnector.h"
#include "velox/common/base/ConcurrentRuntimeStatWriter.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::connector {
namespace {

// Carries a serial, so a rebuilt state is distinguishable from a reused one.
class CountingQueryState : public ConnectorQueryState {
 public:
  explicit CountingQueryState(int32_t serial) : serial{serial} {}

  const int32_t serial;
};

// Hands out a distinct state on every call.
class CountingMetadata : public TestConnectorMetadata {
 public:
  using TestConnectorMetadata::TestConnectorMetadata;

  std::unique_ptr<ConnectorQueryState> makeQueryState(
      const ConnectorSession& session) const override {
    EXPECT_EQ(session.queryId(), "q1");
    EXPECT_EQ(session.user(), "user");
    seenMaxRows = session.property("max_rows_per_scan");
    return std::make_unique<CountingQueryState>(++calls);
  }

  mutable std::optional<std::string_view> seenMaxRows;

  mutable std::atomic<int32_t> calls{0};
};

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
  TestConnector connector{"a"};
  CountingMetadata metadata{&connector};
  velox::ConcurrentRuntimeStatWriter writer;
  std::atomic<int32_t> writerCalls{0};
  auto context = makeContext({}, &writerCalls, writer);

  auto first = context->sessionFor(metadata);
  auto second = context->sessionFor(metadata);

  EXPECT_EQ(first.get(), second.get());
  EXPECT_EQ(metadata.calls, 1);
  EXPECT_EQ(writerCalls, 1);
  EXPECT_EQ(&first->statsWriter(), &writer);
  EXPECT_EQ(first->queryState()->asChecked<CountingQueryState>()->serial, 1);
}

// Two connectors of one query get separate sessions.
TEST(ConnectorContextTest, connectorsGetDistinctSessions) {
  TestConnector first{"a"};
  TestConnector second{"b"};
  CountingMetadata firstMetadata{&first};
  CountingMetadata secondMetadata{&second};
  velox::ConcurrentRuntimeStatWriter writer;
  std::atomic<int32_t> writerCalls{0};
  auto context = makeContext({}, &writerCalls, writer);

  auto firstSession = context->sessionFor(firstMetadata);
  auto secondSession = context->sessionFor(secondMetadata);

  EXPECT_NE(firstSession.get(), secondSession.get());
  EXPECT_EQ(writerCalls, 2);
}

// A connector sees only its own slice of the query's properties.
TEST(ConnectorContextTest, sessionCarriesThisConnectorsProperties) {
  TestConnector connector{"a"};
  CountingMetadata metadata{&connector};
  velox::ConcurrentRuntimeStatWriter writer;
  std::atomic<int32_t> writerCalls{0};
  auto context = makeContext(
      ConnectorProperties{
          {"a", Properties{{"max_rows_per_scan", "10"}}},
          {"b", Properties{{"max_rows_per_scan", "20"}}}},
      &writerCalls,
      writer);

  auto session = context->sessionFor(metadata);

  EXPECT_EQ(session->queryId(), "q1");
  EXPECT_EQ(session->user(), "user");
  EXPECT_EQ(session->property("max_rows_per_scan"), "10");
  // The hook sees the same slice the session carries.
  EXPECT_EQ(metadata.seenMaxRows, "10");
}

// A connector that keeps nothing for a query gets a session with no state.
TEST(ConnectorContextTest, connectorWithoutStateGetsNone) {
  TestConnector plain{"plain"};
  velox::ConcurrentRuntimeStatWriter writer;
  std::atomic<int32_t> writerCalls{0};
  auto context = makeContext({}, &writerCalls, writer);

  EXPECT_EQ(context->sessionFor(*plain.metadata())->queryState(), nullptr);
}

// The contract is that concurrent callers get one session, not merely one per
// caller, so the connector's state is built exactly once.
TEST(ConnectorContextTest, concurrentCallersGetOneSession) {
  TestConnector connector{"a"};
  CountingMetadata metadata{&connector};
  velox::ConcurrentRuntimeStatWriter writer;
  std::atomic<int32_t> writerCalls{0};
  auto context = makeContext({}, &writerCalls, writer);

  constexpr int32_t kThreads = 8;
  std::vector<ConnectorSessionPtr> sessions(kThreads);
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  folly::test::Barrier barrier{kThreads};
  for (int32_t i = 0; i < kThreads; ++i) {
    threads.emplace_back([&, i] {
      barrier.wait();
      sessions[i] = context->sessionFor(metadata);
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_THAT(sessions, testing::Each(sessions[0]));
  EXPECT_EQ(metadata.calls, 1);
  EXPECT_EQ(writerCalls, 1);
}

// Two connectors reporting one id would otherwise share a session, and with it
// the first one's state and property slice.
TEST(ConnectorContextTest, metadataReportingADuplicateIdFails) {
  TestConnector first{"a"};
  TestConnector second{"a"};
  CountingMetadata firstMetadata{&first};
  CountingMetadata secondMetadata{&second};
  velox::ConcurrentRuntimeStatWriter writer;
  std::atomic<int32_t> writerCalls{0};
  auto context = makeContext({}, &writerCalls, writer);

  context->sessionFor(firstMetadata);

  VELOX_ASSERT_THROW(
      context->sessionFor(secondMetadata),
      "Two ConnectorMetadata instances report the same connector id");
}

// A throw while making state leaves the session unbuilt, so the next caller
// builds it rather than seeing a half-made one or the earlier failure.
TEST(ConnectorContextTest, sessionIsBuiltAfterStateMakingThrows) {
  class ThrowsOnceMetadata : public TestConnectorMetadata {
   public:
    using TestConnectorMetadata::TestConnectorMetadata;

    std::unique_ptr<ConnectorQueryState> makeQueryState(
        const ConnectorSession&) const override {
      if (!threw) {
        threw = true;
        VELOX_FAIL("Connector state unavailable");
      }
      return std::make_unique<CountingQueryState>(2);
    }

    mutable bool threw{false};
  };

  TestConnector connector{"a"};
  ThrowsOnceMetadata metadata{&connector};
  velox::ConcurrentRuntimeStatWriter writer;
  std::atomic<int32_t> writerCalls{0};
  auto context = makeContext({}, &writerCalls, writer);

  VELOX_ASSERT_THROW(
      context->sessionFor(metadata), "Connector state unavailable");

  auto session = context->sessionFor(metadata);
  EXPECT_EQ(session->queryStateAs<CountingQueryState>().serial, 2);
}

// A connector asking for its own session while building its state fails.
TEST(ConnectorContextTest, reentrantSessionRequestFails) {
  TestConnector connector{"a"};
  velox::ConcurrentRuntimeStatWriter writer;
  std::atomic<int32_t> writerCalls{0};
  auto context = makeContext({}, &writerCalls, writer);

  class ReentrantMetadata : public TestConnectorMetadata {
   public:
    ReentrantMetadata(TestConnector* connector, ConnectorContext* context)
        : TestConnectorMetadata{connector}, context_{context} {}

    std::unique_ptr<ConnectorQueryState> makeQueryState(
        const ConnectorSession&) const override {
      context_->sessionFor(*this);
      return nullptr;
    }

   private:
    ConnectorContext* const context_;
  };

  ReentrantMetadata metadata{&connector, context.get()};
  VELOX_ASSERT_THROW(
      context->sessionFor(metadata), "while building its query state");
}

// A context without a writer provider has no way to wire a session.
TEST(ConnectorContextTest, contextRequiresAWriterProvider) {
  VELOX_ASSERT_THROW(
      ConnectorContext("q1", "user", {}, nullptr),
      "requires a stat writer provider");
}

// A provider that yields no writer fails rather than leaving a session unwired.
TEST(ConnectorContextTest, nullWriterFromProviderFails) {
  TestConnector connector{"a"};
  auto context = std::make_shared<ConnectorContext>(
      "q1",
      "user",
      ConnectorProperties{},
      [](std::string_view) -> std::shared_ptr<velox::BaseRuntimeStatWriter> {
        return nullptr;
      });
  VELOX_ASSERT_THROW(
      context->sessionFor(*connector.metadata()),
      "Stat writer provider returned null");
}

} // namespace
} // namespace facebook::axiom::connector
