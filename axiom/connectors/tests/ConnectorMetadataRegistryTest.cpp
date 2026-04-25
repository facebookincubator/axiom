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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/QueryCtx.h"

using namespace facebook::velox;
using namespace facebook::axiom::connector;

namespace facebook::axiom::connector {
namespace {

// Implements all pure virtuals with stubs since only registry mechanics are
// under test.
class StubConnectorMetadata : public ConnectorMetadata {
 public:
  explicit StubConnectorMetadata(std::string label)
      : label_{std::move(label)} {}

  const std::string& label() const {
    return label_;
  }

  TablePtr findTable(const SchemaTableName& /*tableName*/) override {
    return nullptr;
  }

  ConnectorSplitManager* splitManager() override {
    return nullptr;
  }

  std::vector<std::string> listSchemaNames(
      const ConnectorSessionPtr& /*session*/) override {
    return {};
  }

  bool schemaExists(
      const ConnectorSessionPtr& /*session*/,
      const std::string& /*schemaName*/) override {
    return false;
  }

 private:
  std::string label_;
};

class ConnectorMetadataRegistryTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
  }

  void TearDown() override {
    ConnectorMetadata::unregisterAllMetadata();
  }
};

TEST_F(ConnectorMetadataRegistryTest, queryCtxFallback) {
  auto globalMetadata = std::make_shared<StubConnectorMetadata>("global");
  ConnectorMetadataRegistry::global().insert("catalog", globalMetadata);

  auto queryCtx = core::QueryCtx::create();
  auto queryRegistry =
      ConnectorMetadataRegistry::create(&ConnectorMetadataRegistry::global());
  auto queryMetadata = std::make_shared<StubConnectorMetadata>("query");
  queryRegistry->insert("catalog", queryMetadata);
  queryCtx->setRegistry(ConnectorMetadataRegistry::kRegistryKey, queryRegistry);

  // Per-query registry takes precedence.
  EXPECT_EQ(
      ConnectorMetadataRegistry::tryGet(*queryCtx, "catalog"), queryMetadata);

  // Global lookup is unaffected by the query-scoped override.
  EXPECT_EQ(ConnectorMetadataRegistry::tryGet("catalog"), globalMetadata);

  // QueryCtx without per-query registry falls back to global.
  auto noRegistryCtx = core::QueryCtx::create();
  EXPECT_EQ(
      ConnectorMetadataRegistry::tryGet(*noRegistryCtx, "catalog"),
      globalMetadata);
}

TEST_F(ConnectorMetadataRegistryTest, getThrowsOnMissing) {
  VELOX_ASSERT_THROW(
      ConnectorMetadataRegistry::get("nonexistent"),
      "ConnectorMetadata not registered: nonexistent");

  auto queryCtx = core::QueryCtx::create();
  VELOX_ASSERT_THROW(
      ConnectorMetadataRegistry::get(*queryCtx, "nonexistent"),
      "ConnectorMetadata not registered: nonexistent");
}

TEST_F(ConnectorMetadataRegistryTest, allMetadataIds) {
  auto globalMetadata = std::make_shared<StubConnectorMetadata>("global");
  ConnectorMetadataRegistry::global().insert("global-catalog", globalMetadata);

  auto queryCtx = core::QueryCtx::create();
  auto queryRegistry =
      ConnectorMetadataRegistry::create(&ConnectorMetadataRegistry::global());
  auto queryMetadata = std::make_shared<StubConnectorMetadata>("query");
  queryRegistry->insert("query-catalog", queryMetadata);
  queryCtx->setRegistry(ConnectorMetadataRegistry::kRegistryKey, queryRegistry);

  EXPECT_THAT(
      ConnectorMetadataRegistry::allMetadataIds(*queryCtx),
      testing::UnorderedElementsAre("global-catalog", "query-catalog"));

  EXPECT_THAT(
      ConnectorMetadataRegistry::allMetadataIds(),
      testing::UnorderedElementsAre("global-catalog"));

  // QueryCtx without registry falls back to global.
  auto noRegistryCtx = core::QueryCtx::create();
  EXPECT_THAT(
      ConnectorMetadataRegistry::allMetadataIds(*noRegistryCtx),
      testing::UnorderedElementsAre("global-catalog"));
}

TEST_F(ConnectorMetadataRegistryTest, duplicateRegistration) {
  auto first = std::make_shared<StubConnectorMetadata>("first");
  auto second = std::make_shared<StubConnectorMetadata>("second");

  ConnectorMetadata::registerMetadata("catalog", first);

  // Duplicate registration is a no-op — first registration wins.
  ConnectorMetadata::registerMetadata("catalog", second);

  auto* result = dynamic_cast<StubConnectorMetadata*>(
      ConnectorMetadata::metadata("catalog"));
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->label(), "first");
}

} // namespace
} // namespace facebook::axiom::connector
