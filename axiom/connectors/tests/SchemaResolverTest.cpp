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

#include <gtest/gtest.h>

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/SchemaResolver.h"
#include "axiom/connectors/tests/TestConnector.h"
#include "velox/common/base/tests/GTestUtils.h"

using namespace facebook::velox;

namespace facebook::axiom::connector {
namespace {

class SchemaResolverTest : public ::testing::Test {
 public:
  void SetUp() override {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    baseCatalog_ = generateCatalog("base", "baseschema");
    otherCatalog_ = generateCatalog("other", "otherschema");
    resolver_ = std::make_shared<SchemaResolver>(
        ConnectorMetadataRegistry::global(), baseCatalog_.schema);
  }

  void TearDown() override {
    ConnectorMetadataRegistry::global().erase("base");
    ConnectorMetadataRegistry::global().erase("other");
    velox::connector::unregisterConnector("base");
    velox::connector::unregisterConnector("other");
  }

  struct Catalog {
    std::string id;
    std::string schema;
    std::shared_ptr<connector::TestConnector> connector;
  };

  static Catalog generateCatalog(
      const std::string& id,
      const std::string& schema) {
    auto connector = std::make_shared<connector::TestConnector>(id);
    velox::connector::registerConnector(connector);
    ConnectorMetadataRegistry::global().insert(id, connector->metadata());
    connector->metadata()->createSchema(
        nullptr, schema, /*ifNotExists=*/false, {});
    return Catalog{
        .id = id,
        .schema = schema,
        .connector = connector,
    };
  }

  Catalog baseCatalog_;
  Catalog otherCatalog_;
  std::shared_ptr<SchemaResolver> resolver_;
};

TEST_F(SchemaResolverTest, findTable) {
  auto metadata = ConnectorMetadataRegistry::get("base");
  metadata->createTable(
      nullptr,
      {"baseschema", "table"},
      ROW({}),
      {},
      /*ifNotExists=*/false,
      /*explain=*/false);

  auto table = resolver_->findTable("base", {"baseschema", "table"});
  ASSERT_NE(table, nullptr);
}

TEST_F(SchemaResolverTest, findTableInNonDefaultSchema) {
  auto metadata = ConnectorMetadataRegistry::get("base");
  metadata->createSchema(nullptr, "newschema", /*ifNotExists=*/false, {});
  metadata->createTable(
      nullptr,
      {"newschema", "table"},
      ROW({}),
      {},
      /*ifNotExists=*/false,
      /*explain=*/false);

  auto table = resolver_->findTable("base", {"newschema", "table"});
  ASSERT_NE(table, nullptr);
}

TEST_F(SchemaResolverTest, tableNotFound) {
  // No table is created; lookup against an existing catalog/schema must return
  // nullptr.
  auto table = resolver_->findTable("base", {"baseschema", "missing_table"});
  ASSERT_EQ(table, nullptr);
}

TEST_F(SchemaResolverTest, wrongCatalog) {
  // Table exists in "other" catalog but lookup is routed through "base".
  auto metadata = ConnectorMetadataRegistry::get("other");
  metadata->createTable(
      /*session=*/nullptr,
      {"otherschema", "table"},
      ROW({}),
      {},
      /*ifNotExists=*/false,
      /*explain=*/false);
  auto table = resolver_->findTable("base", {"otherschema", "table"});
  ASSERT_EQ(table, nullptr);
}

TEST_F(SchemaResolverTest, unregisteredConnectorThrows) {
  VELOX_ASSERT_THROW(
      resolver_->findTable("nonexistent", {"baseschema", "table"}),
      "ConnectorMetadata not registered: nonexistent");
}

TEST_F(SchemaResolverTest, scopedRegistryLookup) {
  // Create a scoped registry that is a child of the global registry.
  auto scopedRegistry =
      ConnectorMetadataRegistry::create(&ConnectorMetadataRegistry::global());

  // Register a new connector with a table in the scoped registry only.
  auto scopedConnector = std::make_shared<connector::TestConnector>("scoped");
  const auto& scopedMetadata = scopedConnector->metadata();
  scopedMetadata->createSchema(
      nullptr, "scopedschema", /*ifNotExists=*/false, {});
  scopedMetadata->createTable(
      nullptr,
      {"scopedschema", "scoped_orders"},
      ROW({}),
      {},
      /*ifNotExists=*/false,
      /*explain=*/false);
  scopedRegistry->insert("scoped", scopedMetadata);

  auto resolver{SchemaResolver{*scopedRegistry, "scopedschema"}};
  auto table = resolver.findTable("scoped", {"scopedschema", "scoped_orders"});
  ASSERT_NE(table, nullptr);

  // The scoped registration must not have leaked into the global registry.
  ASSERT_EQ(ConnectorMetadataRegistry::tryGet("scoped"), nullptr);
}

TEST_F(SchemaResolverTest, scopedRegistryFallsBackToParent) {
  // Create a scoped registry with no local entries that falls back to global.
  auto scopedRegistry =
      ConnectorMetadataRegistry::create(&ConnectorMetadataRegistry::global());

  // Register a table in the globally-registered "base" catalog.
  auto metadata = ConnectorMetadataRegistry::get("base");
  metadata->createTable(
      nullptr,
      {"baseschema", "parent_table"},
      ROW({}),
      {},
      /*ifNotExists=*/false,
      /*explain=*/false);

  // Resolver using scoped registry should still find the global table.
  auto resolver{SchemaResolver{*scopedRegistry, "baseschema"}};
  auto table = resolver.findTable("base", {"baseschema", "parent_table"});
  ASSERT_NE(table, nullptr);
}

} // namespace
} // namespace facebook::axiom::connector
