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

#include "axiom/connectors/tests/TestConnector.h"
#include "velox/common/base/ConcurrentRuntimeStatWriter.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::connector {
namespace {

// Query state that keeps the id of the query it was created for.
class QueryIdState : public ConnectorQueryState {
 public:
  explicit QueryIdState(std::string queryId) : queryId_{std::move(queryId)} {}

  const std::string& queryId() const {
    return queryId_;
  }

 private:
  const std::string queryId_;
};

// Creates query state from the session's query identity.
class QueryStateMetadata : public TestConnectorMetadata {
 public:
  explicit QueryStateMetadata(TestConnector* connector)
      : TestConnectorMetadata(connector) {}

  std::unique_ptr<ConnectorQueryState> makeQueryState(
      const ConnectorSession& session) const override {
    return std::make_unique<QueryIdState>(session.queryId());
  }
};

// Owns the metadata registry used by ConnectorContext tests. Connector 'a'
// keeps query state; connector 'b' keeps none.
class ConnectorContextTest : public testing::Test {
 protected:
  void SetUp() override {
    metadataRegistry_->insert(
        "a", std::make_shared<QueryStateMetadata>(&firstConnector_));
    metadataRegistry_->insert("b", secondConnector_.metadata());
  }

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
        },
        *metadataRegistry_);
  }

  TestConnector firstConnector_{"a"};
  TestConnector secondConnector_{"b"};
  std::shared_ptr<ConnectorMetadataRegistry::Registry> metadataRegistry_{
      ConnectorMetadataRegistry::create()};
};

// Every caller in a query reaches a connector through one session.
TEST_F(ConnectorContextTest, sessionForIsMemoizedPerConnector) {
  velox::ConcurrentRuntimeStatWriter writer;
  std::atomic<int32_t> writerCalls{0};
  auto context = makeContext({}, &writerCalls, writer);

  auto first = context->sessionFor("a");
  auto second = context->sessionFor("a");

  EXPECT_EQ(first.get(), second.get());
  EXPECT_EQ(writerCalls, 1);
  EXPECT_EQ(&first->statsWriter(), &writer);
  auto* queryState = first->queryStateAs<QueryIdState>();
  ASSERT_NE(queryState, nullptr);
  EXPECT_EQ(queryState->queryId(), "q1");
}

// Two connectors of one query get separate sessions, and only a connector whose
// metadata makes query state has any.
TEST_F(ConnectorContextTest, connectorsGetDistinctSessions) {
  velox::ConcurrentRuntimeStatWriter writer;
  std::atomic<int32_t> writerCalls{0};
  auto context = makeContext({}, &writerCalls, writer);

  auto firstSession = context->sessionFor("a");
  auto secondSession = context->sessionFor("b");

  EXPECT_NE(firstSession.get(), secondSession.get());
  EXPECT_EQ(writerCalls, 2);
  EXPECT_NE(firstSession->queryStateAs<QueryIdState>(), nullptr);
  EXPECT_EQ(secondSession->queryStateAs<QueryIdState>(), nullptr);
}

// A connector sees only its own slice of the query's properties.
TEST_F(ConnectorContextTest, sessionCarriesThisConnectorsProperties) {
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
// caller, so the writer is requested exactly once. Each caller also sees the
// session's query state already initialized.
TEST_F(ConnectorContextTest, concurrentCallersGetOneSession) {
  velox::ConcurrentRuntimeStatWriter writer;
  std::atomic<int32_t> writerCalls{0};
  auto context = makeContext({}, &writerCalls, writer);

  constexpr int32_t kThreads = 8;
  std::vector<ConnectorSessionPtr> sessions(kThreads);
  std::vector<QueryIdState*> queryStates(kThreads);
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  std::barrier barrier{kThreads};
  for (int32_t i = 0; i < kThreads; ++i) {
    threads.emplace_back([&, i] {
      barrier.arrive_and_wait();
      sessions[i] = context->sessionFor("a");
      queryStates[i] = sessions[i]->queryStateAs<QueryIdState>();
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_THAT(sessions, testing::Each(sessions[0]));
  EXPECT_EQ(writerCalls, 1);
  ASSERT_NE(queryStates[0], nullptr);
  EXPECT_THAT(queryStates, testing::Each(queryStates[0]));
}

// A throw while building leaves the session unbuilt, so the next caller builds
// it rather than seeing a half-made one or the earlier failure.
TEST_F(ConnectorContextTest, sessionIsBuiltAfterAFailedAttempt) {
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
      },
      *metadataRegistry_);

  VELOX_ASSERT_THROW(context->sessionFor("a"), "Writer unavailable");

  EXPECT_EQ(&context->sessionFor("a")->statsWriter(), &writer);
}

// A context without a writer provider has no way to wire a session.
TEST_F(ConnectorContextTest, contextRequiresAWriterProvider) {
  VELOX_ASSERT_THROW(
      ConnectorContext("q1", "user", {}, nullptr, *metadataRegistry_),
      "requires a stat writer provider");
}

// A provider that yields no writer fails rather than leaving a session unwired.
TEST_F(ConnectorContextTest, nullWriterFromProviderFails) {
  auto context = std::make_shared<ConnectorContext>(
      "q1",
      "user",
      ConnectorProperties{},
      [](std::string_view) -> std::shared_ptr<velox::BaseRuntimeStatWriter> {
        return nullptr;
      },
      *metadataRegistry_);
  VELOX_ASSERT_THROW(
      context->sessionFor("a"), "Stat writer provider returned null");
}

// A context returns the registry the application passed.
TEST_F(ConnectorContextTest, metadataRegistry) {
  ConnectorContext context(
      "q1",
      "user",
      {},
      ConnectorContext::noopStatWriterProvider(),
      *metadataRegistry_);

  EXPECT_EQ(&context.metadataRegistry(), metadataRegistry_.get());
}

// A connector session requires registered metadata.
TEST_F(ConnectorContextTest, missingMetadata) {
  metadataRegistry_->clear();
  ConnectorContext context(
      "q1",
      "user",
      {},
      ConnectorContext::noopStatWriterProvider(),
      *metadataRegistry_);

  VELOX_ASSERT_THROW(
      context.sessionFor("a"), "ConnectorMetadata not registered: a");
}

} // namespace
} // namespace facebook::axiom::connector
