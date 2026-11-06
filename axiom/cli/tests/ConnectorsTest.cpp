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

#include "axiom/cli/Connectors.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "axiom/connectors/tests/TestConnector.h"
#include "velox/common/memory/Memory.h"

namespace facebook::axiom {
namespace {

class ConnectorsTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    velox::memory::MemoryManager::testingSetInstance(
        velox::memory::MemoryManager::Options{});
  }

  // Execution connectors that connectors_ registers into.
  std::shared_ptr<velox::connector::ConnectorRegistry::Registry>
      connectorRegistry_{velox::connector::ConnectorRegistry::create()};
  // Catalog metadata that connectors_ registers into.
  std::shared_ptr<connector::ConnectorMetadataRegistry::Registry>
      metadataRegistry_{connector::ConnectorMetadataRegistry::create()};
  // Helper under test.
  Connectors connectors_{*connectorRegistry_, *metadataRegistry_};
};

// Each catalog is registered, plus each built-in catalog no catalog replaces.
TEST_F(ConnectorsTest, registerCatalogs) {
  const auto catalogs = connectors_.registerCatalogs(
      {
          {.catalogName = "tpch", .connectorName = "test"},
          {.catalogName = "other_tpch", .connectorName = "tpch"},
      },
      {
          {.catalogName = "tpch", .connectorName = "tpch"},
          {.catalogName = "test", .connectorName = "test"},
      });

  std::vector<std::string> catalogNames;
  for (const auto& catalog : catalogs) {
    catalogNames.push_back(catalog.catalogName);
  }
  EXPECT_THAT(catalogNames, testing::ElementsAre("tpch", "other_tpch", "test"));
  for (const auto& catalogName : catalogNames) {
    SCOPED_TRACE(catalogName);
    EXPECT_NE(connectorRegistry_->find(catalogName), nullptr);
    EXPECT_NE(metadataRegistry_->find(catalogName), nullptr);
  }
  EXPECT_NE(
      dynamic_cast<connector::TestConnector*>(
          connectorRegistry_->find("tpch").get()),
      nullptr);
}

// Adds a connector name, as an application's own Connectors subclass does.
class CustomConnectors : public Connectors {
 public:
  using Connectors::Connectors;

  std::shared_ptr<velox::connector::Connector> registerConnector(
      std::string_view connectorName,
      const folly::F14FastMap<std::string, std::string>& connectorConfig,
      const std::string& connectorId) override {
    if (connectorName == "custom") {
      return registerTestConnector(connectorId);
    }
    return Connectors::registerConnector(
        connectorName, connectorConfig, connectorId);
  }
};

// registerCatalogs() registers each catalog through the subclass's
// registerConnector().
TEST_F(ConnectorsTest, subclassConnectorNames) {
  CustomConnectors connectors{*connectorRegistry_, *metadataRegistry_};

  connectors.registerCatalogs(
      {{.catalogName = "mine", .connectorName = "custom"}},
      {{.catalogName = "tpch", .connectorName = "tpch"}});

  EXPECT_NE(
      dynamic_cast<connector::TestConnector*>(
          connectorRegistry_->find("mine").get()),
      nullptr);
  EXPECT_NE(connectorRegistry_->find("tpch"), nullptr);
}

} // namespace
} // namespace facebook::axiom
