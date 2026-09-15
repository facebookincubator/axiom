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

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/tests/TestConnector.h"
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

// Records what the session carried when the connector was asked for state.
class RecordedState : public ConnectorQueryState {
 public:
  RecordedState(
      std::string label,
      std::string queryId,
      const velox::BaseRuntimeStatWriter* writer)
      : label_{std::move(label)},
        queryId_{std::move(queryId)},
        writer_{writer} {}

  // Identifies the connector that made this, so a test can tell them apart.
  const std::string& label() const {
    return label_;
  }

  const std::string& queryId() const {
    return queryId_;
  }

  const velox::BaseRuntimeStatWriter* writer() const {
    return writer_;
  }

 private:
  const std::string label_;
  const std::string queryId_;
  const velox::BaseRuntimeStatWriter* const writer_;
};

class StateMakingMetadata : public TestConnectorMetadata {
 public:
  StateMakingMetadata(TestConnector* connector, std::string label)
      : TestConnectorMetadata(connector), label_{std::move(label)} {}

  std::unique_ptr<ConnectorQueryState> makeQueryState(
      const ConnectorSession& session) const override {
    ++numCalls;
    return std::make_unique<RecordedState>(
        label_, session.queryId(), &session.statsWriter());
  }

  mutable int32_t numCalls{0};

 private:
  const std::string label_;
};

class ConnectorContextQueryStateTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ConnectorMetadataRegistry::unregisterAll();
  }

  void TearDown() override {
    ConnectorMetadataRegistry::unregisterAll();
  }

  // Members, so a registration never outlives the connector it points at.
  TestConnector connectorA_{"a"};
  TestConnector connectorB_{"b"};
};

// A connector that keeps nothing leaves its session's slot empty.
TEST_F(ConnectorContextQueryStateTest, noState) {
  ConnectorMetadataRegistry::global().insert(
      "a", std::make_shared<TestConnectorMetadata>(&connectorA_));
  velox::ConcurrentRuntimeStatWriter writer;
  std::atomic<int32_t> writerCalls{0};
  auto context = makeContext({}, &writerCalls, writer);

  EXPECT_EQ(context->sessionFor("a")->queryState(), nullptr);
}

// An id nothing is registered under yields a session without state.
TEST_F(ConnectorContextQueryStateTest, noStateWhenUnregistered) {
  velox::ConcurrentRuntimeStatWriter writer;
  std::atomic<int32_t> writerCalls{0};
  auto context = makeContext({}, &writerCalls, writer);

  EXPECT_EQ(context->sessionFor("a")->queryState(), nullptr);
}

// Each session carries its own connector's state, not any registered one.
TEST_F(ConnectorContextQueryStateTest, statePerConnector) {
  ConnectorMetadataRegistry::global().insert(
      "a", std::make_shared<StateMakingMetadata>(&connectorA_, "a"));
  ConnectorMetadataRegistry::global().insert(
      "b", std::make_shared<StateMakingMetadata>(&connectorB_, "b"));
  velox::ConcurrentRuntimeStatWriter writer;
  std::atomic<int32_t> writerCalls{0};
  auto context = makeContext({}, &writerCalls, writer);

  EXPECT_EQ(
      context->sessionFor("a")->queryStateAs<RecordedState>().label(), "a");
  EXPECT_EQ(
      context->sessionFor("b")->queryStateAs<RecordedState>().label(), "b");
}

// The state is the query's, so it is built with the session and not again.
TEST_F(ConnectorContextQueryStateTest, builtOnce) {
  auto metadata = std::make_shared<StateMakingMetadata>(&connectorA_, "a");
  ConnectorMetadataRegistry::global().insert("a", metadata);
  velox::ConcurrentRuntimeStatWriter writer;
  std::atomic<int32_t> writerCalls{0};
  auto context = makeContext({}, &writerCalls, writer);

  context->sessionFor("a");
  context->sessionFor("a");

  EXPECT_EQ(metadata->numCalls, 1);
}

// The session is fully wired before the connector is asked for state.
TEST_F(ConnectorContextQueryStateTest, seesTheWiredSession) {
  ConnectorMetadataRegistry::global().insert(
      "a", std::make_shared<StateMakingMetadata>(&connectorA_, "a"));
  velox::ConcurrentRuntimeStatWriter writer;
  std::atomic<int32_t> writerCalls{0};
  auto context = makeContext({}, &writerCalls, writer);

  const auto& state = context->sessionFor("a")->queryStateAs<RecordedState>();

  EXPECT_EQ(state.queryId(), "q1");
  EXPECT_EQ(state.writer(), &writer);
}

// A caller racing the build never reaches a session without state.
TEST_F(ConnectorContextQueryStateTest, concurrentCallersSeeState) {
  ConnectorMetadataRegistry::global().insert(
      "a", std::make_shared<StateMakingMetadata>(&connectorA_, "a"));
  velox::ConcurrentRuntimeStatWriter writer;
  std::atomic<int32_t> writerCalls{0};
  auto context = makeContext({}, &writerCalls, writer);

  constexpr int32_t kThreads = 8;
  std::vector<const ConnectorQueryState*> states(kThreads);
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  std::barrier barrier{kThreads};
  for (int32_t i = 0; i < kThreads; ++i) {
    threads.emplace_back([&, i] {
      barrier.arrive_and_wait();
      states[i] = context->sessionFor("a")->queryState();
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_THAT(states, testing::Each(testing::NotNull()));
}

} // namespace
} // namespace facebook::axiom::connector
