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
#include "axiom/connectors/tests/TestConnector.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"

DECLARE_string(query);

using namespace facebook::velox;

namespace axiom::sql {
namespace {

class ConsoleTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    facebook::velox::memory::MemoryManager::testingSetInstance(
        facebook::velox::memory::MemoryManager::Options{});
    facebook::velox::exec::ExchangeSource::registerFactory(
        facebook::velox::exec::test::createLocalExchangeSource);
  }

  void TearDown() override {
    // Restore FLAGS_query to avoid polluting other tests.
    FLAGS_query = "";
    for (const auto& id : connectorIds_) {
      facebook::velox::connector::ConnectorRegistry::global().erase(id);
    }
  }

  std::unique_ptr<SqlQueryRunner> makeRunner(
      PermissionCheck permissionCheck = {}) {
    auto runner = std::make_unique<SqlQueryRunner>();

    runner->initialize(
        [&]() {
          static int32_t kCounter = 0;

          auto testConnector =
              std::make_shared<facebook::axiom::connector::TestConnector>(
                  fmt::format("console_test{}", kCounter++));
          facebook::velox::connector::ConnectorRegistry::global().insert(
              testConnector->connectorId(), testConnector);

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

} // namespace
} // namespace axiom::sql
