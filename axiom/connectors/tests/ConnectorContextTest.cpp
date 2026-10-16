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

#include <folly/ScopeGuard.h>

#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/connectors/tests/TestConnectorContext.h"
#include "velox/common/base/ConcurrentRuntimeStatWriter.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/core/QueryCtx.h"

namespace facebook::axiom::connector {
namespace {

IsolatedTestContext makeContext(
    ConnectorProperties properties,
    std::atomic<int32_t>* writerCalls,
    velox::BaseRuntimeStatWriter& writer) {
  std::vector<TestCatalogRegistration> catalogs;
  for (const auto connectorId : {"a", "b", "catalog"}) {
    auto connector = std::make_shared<TestConnector>(connectorId);
    catalogs.emplace_back(connector, connector->metadata());
  }
  return makeTestContext(
      "q1",
      std::move(catalogs),
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
  auto isolated = makeContext({}, &writerCalls, writer);
  const auto& context = isolated.connectorContext;

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
  auto isolated = makeContext({}, &writerCalls, writer);
  const auto& context = isolated.connectorContext;

  auto firstSession = context->sessionFor("a");
  auto secondSession = context->sessionFor("b");

  EXPECT_NE(firstSession.get(), secondSession.get());
  EXPECT_EQ(writerCalls, 2);
}

// A connector sees only its own slice of the query's properties.
TEST(ConnectorContextTest, sessionCarriesThisConnectorsProperties) {
  velox::ConcurrentRuntimeStatWriter writer;
  std::atomic<int32_t> writerCalls{0};
  auto isolated = makeContext(
      ConnectorProperties{
          {"a", Properties{{"max_rows_per_scan", "10"}}},
          {"b", Properties{{"max_rows_per_scan", "20"}}}},
      &writerCalls,
      writer);
  const auto& context = isolated.connectorContext;

  auto session = context->sessionFor("a");

  EXPECT_EQ(session->queryId(), "q1");
  EXPECT_EQ(session->user(), "user");
  EXPECT_EQ(session->property("max_rows_per_scan"), "10");
}

// A connector without registered metadata still receives a query session.
TEST(ConnectorContextTest, sessionWithoutMetadata) {
  velox::ConcurrentRuntimeStatWriter writer;
  std::atomic<int32_t> writerCalls{0};
  auto isolated = makeTestContext(
      "q1",
      {},
      "user",
      ConnectorProperties{
          {"unregistered", Properties{{"max_rows_per_scan", "10"}}}},
      [&writerCalls, &writer](
          std::string_view) -> std::shared_ptr<velox::BaseRuntimeStatWriter> {
        ++writerCalls;
        return std::shared_ptr<velox::BaseRuntimeStatWriter>(
            &writer, [](auto*) {});
      });

  const auto session = isolated.connectorContext->sessionFor("unregistered");

  EXPECT_EQ(session->queryId(), "q1");
  EXPECT_EQ(session->user(), "user");
  EXPECT_EQ(session->property("max_rows_per_scan"), "10");
  EXPECT_EQ(writerCalls, 1);
}

// The contract is that concurrent callers get one session, not merely one per
// caller, so the writer is requested exactly once.
TEST(ConnectorContextTest, concurrentCallersGetOneSession) {
  velox::ConcurrentRuntimeStatWriter writer;
  std::atomic<int32_t> writerCalls{0};
  auto isolated = makeContext({}, &writerCalls, writer);
  const auto& context = isolated.connectorContext;

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
  auto connector = std::make_shared<TestConnector>("a");
  auto isolated = makeTestContext(
      "q1",
      std::vector<TestCatalogRegistration>{{connector, connector->metadata()}},
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
  const auto& context = isolated.connectorContext;

  VELOX_ASSERT_THROW(context->sessionFor("a"), "Writer unavailable");

  EXPECT_EQ(&context->sessionFor("a")->statsWriter(), &writer);
}

// A context without a writer provider has no way to wire a session.
TEST(ConnectorContextTest, contextRequiresAWriterProvider) {
  VELOX_ASSERT_THROW(
      ConnectorContext(
          "q1", "user", {}, nullptr, ConnectorMetadataRegistry::create()),
      "requires a stat writer provider");
}

TEST(ConnectorContextTest, metadataRequired) {
  VELOX_ASSERT_THROW(
      ConnectorContext(
          "q1",
          "user",
          {},
          ConnectorContext::noopStatWriterProvider(),
          nullptr),
      "requires a connector metadata registry");
}

// A provider that yields no writer fails rather than leaving a session unwired.
TEST(ConnectorContextTest, nullWriterFromProviderFails) {
  auto context = std::make_shared<ConnectorContext>(
      "q1",
      "user",
      ConnectorProperties{},
      [](std::string_view) -> std::shared_ptr<velox::BaseRuntimeStatWriter> {
        return nullptr;
      },
      ConnectorMetadataRegistry::create());
  VELOX_ASSERT_THROW(
      context->sessionFor("a"), "Stat writer provider returned null");
}

TEST(ConnectorContextTest, metadataIsolation) {
  auto firstConnector = std::make_shared<TestConnector>("catalog");
  auto secondConnector = std::make_shared<TestConnector>("catalog");
  auto first = makeTestContext(
      "q1",
      std::vector<TestCatalogRegistration>{
          {firstConnector, firstConnector->metadata()}},
      "user");
  auto second = makeTestContext(
      "q2",
      std::vector<TestCatalogRegistration>{
          {secondConnector, secondConnector->metadata()}},
      "user");

  EXPECT_EQ(
      first.connectorContext->metadata("catalog"), firstConnector->metadata());
  EXPECT_EQ(
      second.connectorContext->metadata("catalog"),
      secondConnector->metadata());
}

TEST(ConnectorContextTest, isolatedAttachment) {
  if (!velox::memory::MemoryManager::testInstance()) {
    velox::memory::MemoryManager::testingSetInstance({});
  }
  auto connector = std::make_shared<TestConnector>("catalog");
  auto isolated = makeTestContext(
      "q1",
      std::vector<TestCatalogRegistration>{{connector, connector->metadata()}},
      "user");
  auto queryCtx = velox::core::QueryCtx::create();

  queryCtx->setRegistry(
      velox::connector::ConnectorRegistry::kRegistryKey,
      isolated.connectorRegistry);

  EXPECT_EQ(
      velox::connector::ConnectorRegistry::tryGet(*queryCtx, "catalog"),
      connector);
}

TEST(ConnectorContextTest, connectorLifetime) {
  auto connector = std::make_shared<TestConnector>("catalog");
  std::weak_ptr<TestConnector> weakConnector = connector;
  auto isolated = makeTestContext(
      "q1",
      std::vector<TestCatalogRegistration>{{connector, connector->metadata()}},
      "user");
  connector.reset();

  EXPECT_FALSE(weakConnector.expired());
  EXPECT_NE(isolated.connectorContext->metadata("catalog"), nullptr);
  isolated.connectorRegistry.reset();
  isolated.metadataRegistry.reset();
  EXPECT_FALSE(weakConnector.expired());
  isolated.connectorContext.reset();
  EXPECT_TRUE(weakConnector.expired());
}

// The testing context resolves process-wide metadata.
TEST(ConnectorContextTest, processWide) {
  constexpr std::string_view kConnectorId = "process_wide_catalog";
  auto connector = std::make_shared<TestConnector>(std::string{kConnectorId});
  ConnectorMetadataRegistry::global().insert(
      std::string{kConnectorId}, connector->metadata());
  [[maybe_unused]] auto cleanup = folly::makeGuard([&] {
    ConnectorMetadataRegistry::global().erase(std::string{kConnectorId});
  });

  auto context = ConnectorContext::testingCreate(
      "q1",
      "user",
      ConnectorProperties{},
      ConnectorContext::noopStatWriterProvider());
  EXPECT_THAT(context->metadataIds(), testing::Contains(kConnectorId));

  EXPECT_EQ(
      ConnectorMetadataRegistry::tryGet(std::string{kConnectorId}),
      connector->metadata());
  EXPECT_EQ(context->metadata(kConnectorId), connector->metadata());
}

} // namespace
} // namespace facebook::axiom::connector
