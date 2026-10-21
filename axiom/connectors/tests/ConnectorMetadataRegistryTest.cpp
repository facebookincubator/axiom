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
#include "axiom/connectors/tests/TestConnector.h"
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

  std::vector<std::string> listTableNames(
      const ConnectorSessionPtr& /*session*/,
      const std::string& /*schemaName*/) override {
    return {};
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

  VELOX_ASSERT_THROW(
      ConnectorMetadataRegistry::create()->get("nonexistent"),
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
      queryRegistry->ids(),
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

  const auto* result =
      ConnectorMetadata::metadata("catalog")->as<StubConnectorMetadata>();
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->label(), "first");
}

// Catalog registration inserts the connector and its matching metadata.
TEST_F(ConnectorMetadataRegistryTest, registersCatalogPair) {
  auto connectorRegistry = velox::connector::ConnectorRegistry::create();
  auto metadataRegistry = ConnectorMetadataRegistry::create();
  auto connector = std::make_shared<TestConnector>("catalog");

  ConnectorMetadataRegistry::registerCatalog(
      *connectorRegistry, *metadataRegistry, connector, connector->metadata());

  EXPECT_EQ(connectorRegistry->find("catalog"), connector);
  EXPECT_EQ(metadataRegistry->find("catalog"), connector->metadata());
}

// Registering the same catalog twice throws and preserves the first pair.
TEST_F(ConnectorMetadataRegistryTest, duplicateCatalogPreservesFirstPair) {
  auto connectorRegistry = velox::connector::ConnectorRegistry::create();
  auto metadataRegistry = ConnectorMetadataRegistry::create();
  auto firstConnector = std::make_shared<TestConnector>("catalog");
  auto secondConnector = std::make_shared<TestConnector>("catalog");

  ConnectorMetadataRegistry::registerCatalog(
      *connectorRegistry,
      *metadataRegistry,
      firstConnector,
      firstConnector->metadata());
  VELOX_ASSERT_THROW(
      ConnectorMetadataRegistry::registerCatalog(
          *connectorRegistry,
          *metadataRegistry,
          secondConnector,
          secondConnector->metadata()),
      "Key already registered: catalog");

  EXPECT_EQ(connectorRegistry->find("catalog"), firstConnector);
  EXPECT_EQ(metadataRegistry->find("catalog"), firstConnector->metadata());
}

// A metadata conflict rolls back the connector inserted for the same catalog.
TEST_F(ConnectorMetadataRegistryTest, rollsBackConnectorOnMetadataConflict) {
  auto connectorRegistry = velox::connector::ConnectorRegistry::create();
  auto metadataRegistry = ConnectorMetadataRegistry::create();
  auto existingMetadata = std::make_shared<StubConnectorMetadata>("existing");
  metadataRegistry->insert("catalog", existingMetadata);
  auto connector = std::make_shared<TestConnector>("catalog");

  VELOX_ASSERT_THROW(
      ConnectorMetadataRegistry::registerCatalog(
          *connectorRegistry,
          *metadataRegistry,
          connector,
          connector->metadata()),
      "Key already registered: catalog");

  EXPECT_EQ(connectorRegistry->find("catalog"), nullptr);
  EXPECT_EQ(metadataRegistry->find("catalog"), existingMetadata);
}

// A metadata entry keeps its matching execution connector alive.
TEST_F(ConnectorMetadataRegistryTest, metadataRetainsConnector) {
  auto connectorRegistry = velox::connector::ConnectorRegistry::create();
  auto metadataRegistry = ConnectorMetadataRegistry::create();
  auto connector = std::make_shared<TestConnector>("catalog");
  std::weak_ptr<velox::connector::Connector> weakConnector = connector;

  ConnectorMetadataRegistry::registerCatalog(
      *connectorRegistry, *metadataRegistry, connector, connector->metadata());
  connector.reset();
  connectorRegistry->erase("catalog");

  EXPECT_FALSE(weakConnector.expired());
  metadataRegistry->erase("catalog");
  EXPECT_TRUE(weakConnector.expired());
}

} // namespace
} // namespace facebook::axiom::connector
