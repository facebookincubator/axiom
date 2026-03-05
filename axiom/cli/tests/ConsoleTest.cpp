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

      return std::make_pair(testConnector->connectorId(), std::nullopt);
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

  PermissionCheck check = [&](std::string_view sql,
                              std::string_view catalog,
                              std::optional<std::string_view> /*schema*/,
                              const auto& /*views*/) {
    called = true;
    capturedSql = std::string(sql);
    capturedCatalog = std::string(catalog);
  };

  Console console{*runner, std::move(check)};
  console.initialize();

  FLAGS_query = "SELECT 1";
  console.run();

  ASSERT_TRUE(called);
  EXPECT_EQ(capturedSql, "SELECT 1");
  EXPECT_FALSE(capturedCatalog.empty());
}

} // namespace
} // namespace axiom::sql
