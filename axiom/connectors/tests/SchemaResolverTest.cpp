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

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/SchemaResolver.h"
#include "axiom/connectors/tests/TestConnector.h"

using namespace facebook::velox;

namespace facebook::axiom::connector {
namespace {

class SchemaResolverTest : public ::testing::Test {
 public:
  void SetUp() override {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    baseCatalog_ = generateCatalog("base", "baseschema");
    otherCatalog_ = generateCatalog("other", "otherschema");
    resolver_ = std::make_shared<SchemaResolver>(baseCatalog_.schema);
  }

  void TearDown() override {
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
    auto metadata = ConnectorMetadataRegistry::get(id);
    metadata->createSchema(nullptr, schema, /*ifNotExists=*/false, {});
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

TEST_F(SchemaResolverTest, bareTable) {
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

  table = resolver_->findTable("other", {"otherschema", "table"});
  ASSERT_EQ(table, nullptr);
}

TEST_F(SchemaResolverTest, tablePlusSchema) {
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

  table = resolver_->findTable("other", {"newschema", "table"});
  ASSERT_EQ(table, nullptr);
}

TEST_F(SchemaResolverTest, tablePlusSchemaPlusCatalog) {
  auto otherMetadata = ConnectorMetadataRegistry::get("other");
  otherMetadata->createTable(
      /*session=*/nullptr,
      {"otherschema", "other_table"},
      ROW({}),
      {},
      /*ifNotExists=*/false,
      /*explain=*/false);
  auto table = resolver_->findTable("other", {"otherschema", "other_table"});
  ASSERT_NE(table, nullptr);

  auto baseMetadata = ConnectorMetadataRegistry::get("base");
  baseMetadata->createTable(
      nullptr,
      {"baseschema", "base_table"},
      ROW({}),
      {},
      /*ifNotExists=*/false,
      /*explain=*/false);
  table = resolver_->findTable("base", {"baseschema", "base_table"});
  ASSERT_NE(table, nullptr);
}

TEST_F(SchemaResolverTest, catalogMismatch) {
  // Table exists in "other" catalog but not in "base".
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

} // namespace
} // namespace facebook::axiom::connector
