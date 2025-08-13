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

#include "velox/common/base/tests/GTestUtils.h"

#include "axiom/optimizer/SchemaResolver.h"
#include "axiom/optimizer/connectors/ConnectorMetadata.h"
#include "axiom/optimizer/connectors/tests/TestConnector.h"

namespace facebook::velox::optimizer::test {
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
    connector::unregisterConnector("base");
    connector::unregisterConnector("other");
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
    connector::registerConnector(connector);
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
  auto lookup = "table";
  auto expect = "baseschema.table";
  baseCatalog_.connector->createTable(expect);

  auto table = resolver_->findTable("base", lookup);
  ASSERT_NE(table, nullptr);
  ASSERT_EQ(table->name(), expect);

  table = resolver_->findTable("other", lookup);
  ASSERT_EQ(table, nullptr);
}

TEST_F(SchemaResolverTest, invalidName) {
  auto lookup = "table.";
  VELOX_ASSERT_THROW(
      resolver_->findTable("base", lookup),
      fmt::format("Invalid table name: '{}'", lookup));

  lookup = "...";
  VELOX_ASSERT_THROW(
      resolver_->findTable("base", lookup),
      fmt::format("Invalid table name: '{}'", lookup));

  lookup = "catalog.extra.schema.table";
  VELOX_ASSERT_THROW(
      resolver_->findTable("base", lookup),
      fmt::format("Invalid table name: '{}'", lookup));
}

TEST_F(SchemaResolverTest, tablePlusSchema) {
  auto lookup = "newschema.table";
  baseCatalog_.connector->createTable(lookup);
  auto table = resolver_->findTable("base", lookup);
  ASSERT_NE(table, nullptr);
  ASSERT_EQ(table->name(), lookup);

  table = resolver_->findTable("other", lookup);
  ASSERT_EQ(table, nullptr);
}

TEST_F(SchemaResolverTest, tablePlusSchemaPlusCatalog) {
  auto lookup = "other.otherschema.table";
  auto expect = "otherschema.table";
  otherCatalog_.connector->createTable(expect);
  auto table = resolver_->findTable("other", lookup);
  ASSERT_NE(table, nullptr);
  ASSERT_EQ(table->name(), expect);

  lookup = "base.baseschema.table";
  expect = "baseschema.table";
  baseCatalog_.connector->createTable(expect);
  table = resolver_->findTable("base", lookup);
  ASSERT_NE(table, nullptr);
  ASSERT_EQ(table->name(), expect);
}

TEST_F(SchemaResolverTest, catalogMismatch) {
  auto lookupName = "other.otherschema.table";
  otherCatalog_.connector->createTable(lookupName);
  VELOX_ASSERT_THROW(
      resolver_->findTable("base", lookupName),
      "Input catalog must match table catalog specifier");

  lookupName = "otherschema.table";
  auto table = resolver_->findTable("base", lookupName);
  ASSERT_EQ(table, nullptr);
}

} // namespace
} // namespace facebook::velox::optimizer::test
