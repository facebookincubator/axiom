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

#include "axiom/connectors/ConnectorMetadata.h"
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
  baseCatalog_.connector->addTable(expect);

  auto table = resolver_->findTable("base", lookup);
  ASSERT_NE(table, nullptr);
  ASSERT_EQ(table->name(), expect);

  table = resolver_->findTable("other", lookup);
  ASSERT_EQ(table, nullptr);
}

TEST_F(SchemaResolverTest, invalidName) {
  // Names that don't parse as valid catalog.schema.table are passed through
  // to the connector's findTable, which returns nullptr for unknown tables.
  ASSERT_EQ(resolver_->findTable("base", "table."), nullptr);
  ASSERT_EQ(resolver_->findTable("base", "..."), nullptr);
  ASSERT_EQ(
      resolver_->findTable("base", "catalog.extra.schema.table"), nullptr);
}

TEST_F(SchemaResolverTest, multiDotNamePassthrough) {
  // Names with more than 3 dots (e.g., XDB tier paths like
  // "ephemeralxdb.on_demand.ftw.784.tasks") bypass the standard
  // catalog.schema.table parser and are passed directly to the connector.
  auto lookup = "ephemeralxdb.on_demand.ftw.784.tasks";
  baseCatalog_.connector->addTable(lookup);

  auto table = resolver_->findTable("base", lookup);
  ASSERT_NE(table, nullptr);
  ASSERT_EQ(table->name(), lookup);
}

TEST_F(SchemaResolverTest, tablePlusSchema) {
  auto lookup = "newschema.table";
  baseCatalog_.connector->addTable(lookup);
  auto table = resolver_->findTable("base", lookup);
  ASSERT_NE(table, nullptr);
  ASSERT_EQ(table->name(), lookup);

  table = resolver_->findTable("other", lookup);
  ASSERT_EQ(table, nullptr);
}

TEST_F(SchemaResolverTest, tablePlusSchemaPlusCatalog) {
  auto lookup = "other.otherschema.table";
  auto expect = "otherschema.table";
  otherCatalog_.connector->addTable(expect);
  auto table = resolver_->findTable("other", lookup);
  ASSERT_NE(table, nullptr);
  ASSERT_EQ(table->name(), expect);

  lookup = "base.baseschema.table";
  expect = "baseschema.table";
  baseCatalog_.connector->addTable(expect);
  table = resolver_->findTable("base", lookup);
  ASSERT_NE(table, nullptr);
  ASSERT_EQ(table->name(), expect);
}

TEST_F(SchemaResolverTest, catalogMismatch) {
  auto lookupName = "other.otherschema.table";
  otherCatalog_.connector->addTable(lookupName);
  VELOX_ASSERT_THROW(
      resolver_->findTable("base", lookupName),
      "Input catalog must match table catalog specifier");

  lookupName = "otherschema.table";
  auto table = resolver_->findTable("base", lookupName);
  ASSERT_EQ(table, nullptr);
}

} // namespace
} // namespace facebook::axiom::connector
