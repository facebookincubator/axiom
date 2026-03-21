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
#include <gtest/gtest.h>
#include <thread>
#include "axiom/connectors/tests/TestConnector.h"

DECLARE_string(query);

using namespace facebook::velox;

namespace axiom::sql {
namespace {

class ConsoleTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    facebook::velox::memory::MemoryManager::testingSetInstance(
        facebook::velox::memory::MemoryManager::Options{});
  }

  void TearDown() override {
    // Restore FLAGS_query to avoid polluting other tests.
    FLAGS_query = "";
    for (const auto& id : connectorIds_) {
      facebook::velox::connector::unregisterConnector(id);
    }
  }

  std::unique_ptr<SqlQueryRunner> makeRunner() {
    auto runner = std::make_unique<SqlQueryRunner>();

    runner->initialize([&]() {
      static int32_t kCounter = 0;

      auto testConnector =
          std::make_shared<facebook::axiom::connector::TestConnector>(
              fmt::format("console_test{}", kCounter++));
      facebook::velox::connector::registerConnector(testConnector);

      connectorIds_.emplace_back(testConnector->connectorId());

      return std::make_pair(
          testConnector->connectorId(),
          std::string(
              facebook::axiom::connector::TestConnector::kDefaultSchema));
    });

    return runner;
  }

 private:
  std::vector<std::string> connectorIds_;
};

TEST_F(ConsoleTest, permissionCheckCalledBeforeExecution) {
  auto runner = makeRunner();

  bool called = false;
  std::string capturedSql;
  std::string capturedCatalog;

  PermissionCheck check = [&](std::string_view /*queryId*/,
                              std::string_view sql,
                              std::string_view catalog,
                              std::optional<std::string_view> /*schema*/,
                              const auto& /*views*/) {
    called = true;
    capturedSql = std::string(sql);
    capturedCatalog = std::string(catalog);
    return std::shared_ptr<facebook::velox::filesystems::TokenProvider>{};
  };

  Console console{*runner, std::move(check)};
  console.initialize();

  FLAGS_query = "SELECT 1";
  console.run();

  ASSERT_TRUE(called);
  EXPECT_EQ(capturedSql, "SELECT 1");
  EXPECT_FALSE(capturedCatalog.empty());
}

TEST_F(ConsoleTest, completionCallbackReceivesTiming) {
  auto runner = makeRunner();

  QueryCompletionInfo captured;
  QueryCompletionCallback callback = [&](const QueryCompletionInfo& info) {
    captured = info;
  };

  Console console{*runner, nullptr, nullptr, nullptr, std::move(callback)};
  console.initialize();

  FLAGS_query = "SELECT 1";
  console.run();

  EXPECT_GT(captured.parseMicros, 0);
  EXPECT_GT(captured.totalMicros, 0);
  EXPECT_GE(captured.totalMicros, captured.parseMicros);
  EXPECT_GE(
      captured.totalMicros,
      captured.parseMicros + captured.optimizeMicros +
          captured.executionMicros);
}

TEST_F(ConsoleTest, startCallbackFiredBeforeCompletion) {
  auto runner = makeRunner();

  std::string startQueryId;
  std::string completionQueryId;

  QueryStartCallback startCallback = [&](const QueryStartInfo& info) {
    startQueryId = info.queryId;
    EXPECT_EQ(info.query, "SELECT 1");
  };

  QueryCompletionCallback completionCallback =
      [&](const QueryCompletionInfo& info) {
        completionQueryId = info.startInfo.queryId;
      };

  Console console{
      *runner,
      nullptr,
      nullptr,
      std::move(startCallback),
      std::move(completionCallback)};
  console.initialize();

  FLAGS_query = "SELECT 1";
  console.run();

  EXPECT_FALSE(startQueryId.empty());
  EXPECT_EQ(startQueryId, completionQueryId);
}

TEST_F(ConsoleTest, completionCallbackOnError) {
  auto runner = makeRunner();

  QueryCompletionInfo captured;
  QueryCompletionCallback callback = [&](const QueryCompletionInfo& info) {
    captured = info;
  };

  Console console{*runner, nullptr, nullptr, nullptr, std::move(callback)};
  console.initialize();

  FLAGS_query = "SELECT * FROM nonexistent_table";
  console.run();

  EXPECT_TRUE(captured.errorInfo.has_value());
  EXPECT_FALSE(captured.errorInfo->message.empty());
}

TEST_F(ConsoleTest, multiStatementTimingPerStatement) {
  auto runner = makeRunner();

  std::vector<QueryCompletionInfo> completions;
  QueryCompletionCallback callback = [&](const QueryCompletionInfo& info) {
    completions.push_back(info);
  };

  Console console{*runner, nullptr, nullptr, nullptr, std::move(callback)};
  console.initialize();

  FLAGS_query = "SELECT 1; SELECT 2";
  console.run();

  ASSERT_EQ(completions.size(), 2);
  for (const auto& info : completions) {
    EXPECT_GT(info.parseMicros, 0);
    EXPECT_GT(info.totalMicros, 0);
    EXPECT_GE(info.totalMicros, info.parseMicros);
  }
}

TEST_F(ConsoleTest, totalTimingIncludesAllPhases) {
  auto runner = makeRunner();

  QueryCompletionInfo captured;
  QueryCompletionCallback callback = [&](const QueryCompletionInfo& info) {
    captured = info;
  };

  // Inject a permission check that sleeps 10ms to create a measurable gap
  // between parse and execute timers.
  PermissionCheck slowCheck = [&](std::string_view /*queryId*/,
                                  std::string_view /*sql*/,
                                  std::string_view /*catalog*/,
                                  std::optional<std::string_view> /*schema*/,
                                  const auto& /*views*/) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    return std::shared_ptr<facebook::velox::filesystems::TokenProvider>{};
  };

  Console console{
      *runner, std::move(slowCheck), nullptr, nullptr, std::move(callback)};
  console.initialize();

  FLAGS_query = "SELECT 1";
  console.run();

  auto phaseSum =
      captured.parseMicros + captured.optimizeMicros + captured.executionMicros;
  EXPECT_GT(captured.totalMicros, 0);
  EXPECT_GE(captured.totalMicros, phaseSum);
  // The 10ms sleep in the permission check creates a gap that only the outer
  // timer captures.
  EXPECT_GT(captured.totalMicros - phaseSum, 5'000)
      << "Expected at least 5ms gap from the slow permission check, got "
      << (captured.totalMicros - phaseSum) << "us";
}

} // namespace
} // namespace axiom::sql
