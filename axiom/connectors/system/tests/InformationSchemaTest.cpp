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

#include <folly/init/Init.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/system/SystemConnectorMetadata.h"
#include "axiom/connectors/tpch/TpchConnectorMetadata.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/functions/prestosql/types/PrestoTypes.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::axiom::connector::system {
namespace {

using namespace facebook::velox;

constexpr std::string_view kSystemConnectorId = "system";
constexpr std::string_view kTpchConnectorId = "tpch";

// Covers information_schema behavior that SQL setup cannot express, including
// views and connector-advertised schema lists.
class InformationSchemaTest : public optimizer::test::QueryTestBase {
 protected:
  void SetUp() override {
    useV2_ = true;
    optimizer::test::QueryTestBase::SetUp();

    registerSystemConnector(velox::PrestoTypes::displayName);

    testConnector_->createView(
        SchemaTableName{kDefaultSchema, "nation_names"},
        ROW("n_name", VARCHAR()),
        "SELECT n_name FROM nation");
  }

  void TearDown() override {
    if (tpchConnector_ != nullptr) {
      ConnectorMetadataRegistry::global().erase(std::string(kTpchConnectorId));
      velox::connector::ConnectorRegistry::global().erase(
          std::string(kTpchConnectorId));
      tpchConnector_.reset();
    }

    ConnectorMetadataRegistry::global().erase(std::string(kSystemConnectorId));
    velox::connector::ConnectorRegistry::global().erase(
        std::string(kSystemConnectorId));
    systemConnector_.reset();

    optimizer::test::QueryTestBase::TearDown();
  }

  // Registers the system connector, replacing one already registered, with
  // 'typeName' spelling the types information_schema.columns reports.
  void registerSystemConnector(InformationSchema::TypeNameFormatter typeName) {
    if (systemConnector_ != nullptr) {
      ConnectorMetadataRegistry::global().erase(
          std::string(kSystemConnectorId));
      velox::connector::ConnectorRegistry::global().erase(
          std::string(kSystemConnectorId));
    }

    systemConnector_ = std::make_shared<SystemConnector>(
        std::string(kSystemConnectorId),
        /*queryInfoProvider=*/nullptr,
        /*sessionPropertiesProvider=*/nullptr,
        std::move(typeName));
    velox::connector::ConnectorRegistry::global().insert(
        std::string(kSystemConnectorId), systemConnector_);
    ConnectorMetadataRegistry::global().insert(
        std::string(kSystemConnectorId),
        std::make_shared<SystemConnectorMetadata>(systemConnector_.get()));
  }

  std::vector<RowVectorPtr> run(std::string_view sql) {
    return runVelox(parseSelect(sql, kTestConnectorId)).results;
  }

  void registerTpchConnector() {
    auto config = std::make_shared<velox::config::ConfigBase>(
        std::unordered_map<std::string, std::string>{});
    tpchConnector_ = std::make_shared<velox::connector::tpch::TpchConnector>(
        std::string(kTpchConnectorId), std::move(config), nullptr);
    velox::connector::ConnectorRegistry::global().insert(
        std::string(kTpchConnectorId), tpchConnector_);
    ConnectorMetadataRegistry::global().insert(
        std::string(kTpchConnectorId),
        std::make_shared<connector::tpch::TpchConnectorMetadata>(
            tpchConnector_.get()));
  }

  std::shared_ptr<SystemConnector> systemConnector_;
  std::shared_ptr<velox::connector::tpch::TpchConnector> tpchConnector_;
};

TEST_F(InformationSchemaTest, schemata) {
  testConnector_->addTable(
      SchemaTableName{"analytics", "events"}, ROW("event_id", BIGINT()));
  // Reports information_schema once when the catalog already lists it.
  testConnector_->addTable(
      SchemaTableName{"information_schema", "reserved"},
      ROW("value", BIGINT()));

  auto results =
      run("SELECT catalog_name, schema_name FROM information_schema.schemata "
          "ORDER BY schema_name");
  velox::test::assertEqualVectors(
      makeRowVector({
          makeFlatVector<std::string>(
              {std::string(kTestConnectorId),
               std::string(kTestConnectorId),
               std::string(kTestConnectorId)}),
          makeFlatVector<std::string>(
              {"analytics", "default", "information_schema"}),
      }),
      results.at(0));

  results = run("SHOW SCHEMAS");
  velox::test::assertEqualVectors(
      makeRowVector(
          {"Schema"},
          {makeFlatVector<std::string>(
              {"analytics", "default", "information_schema"})}),
      results.at(0));

  for (const char* predicate :
       {"schema_name = 'analytics'", "schema_name LIKE 'analytics%'"}) {
    SCOPED_TRACE(predicate);
    results = run(
        std::string(
            "SELECT catalog_name, schema_name FROM information_schema.schemata "
            "WHERE ") +
        predicate);
    velox::test::assertEqualVectors(
        makeRowVector({
            makeFlatVector<std::string>({std::string(kTestConnectorId)}),
            makeFlatVector<std::string>({"analytics"}),
        }),
        results.at(0));
  }
}

TEST_F(InformationSchemaTest, schemataMembership) {
  registerTpchConnector();

  auto results =
      run("SELECT schema_name FROM tpch.information_schema.schemata "
          "ORDER BY schema_name");
  velox::test::assertEqualVectors(
      makeRowVector({makeFlatVector<std::string>({
          "information_schema",
          "sf1",
          "sf100",
          "sf1000",
          "sf10000",
          "sf100000",
          "sf300",
          "sf3000",
          "sf30000",
          "tiny",
      })}),
      results.at(0));

  // TPC-H accepts sf42 but does not advertise it. Schemata excludes
  // valid-but-unadvertised schemas from unfiltered, equality, and pattern
  // queries.
  for (const char* predicate :
       {"schema_name = 'sf42'", "schema_name LIKE 'sf4%'"}) {
    SCOPED_TRACE(predicate);
    EXPECT_THAT(
        run(std::string(
                "SELECT schema_name FROM tpch.information_schema.schemata "
                "WHERE ") +
            predicate),
        testing::IsEmpty());
  }

  results =
      run("SELECT table_name, table_type "
          "FROM tpch.information_schema.tables "
          "WHERE table_schema = 'information_schema' "
          "AND table_name = 'schemata'");
  velox::test::assertEqualVectors(
      makeRowVector({
          makeFlatVector<std::string>({"schemata"}),
          makeFlatVector<std::string>({"BASE TABLE"}),
      }),
      results.at(0));

  results =
      run("SELECT column_name FROM tpch.information_schema.columns "
          "WHERE table_schema = 'information_schema' "
          "AND table_name = 'schemata' ORDER BY ordinal_position");
  velox::test::assertEqualVectors(
      makeRowVector(
          {makeFlatVector<std::string>({"catalog_name", "schema_name"})}),
      results.at(0));
}

TEST_F(InformationSchemaTest, view) {
  // A view reports the text it was defined with, and is not a base table.
  auto results =
      run("SELECT table_name, table_type FROM information_schema.tables "
          "WHERE table_schema = 'default' AND table_name = 'nation_names'");
  velox::test::assertEqualVectors(
      makeRowVector({
          makeFlatVector<std::string>({"nation_names"}),
          makeFlatVector<std::string>({"VIEW"}),
      }),
      results.at(0));

  results =
      run("SELECT view_definition, view_owner FROM information_schema.views "
          "WHERE table_schema = 'default' AND table_name = 'nation_names'");
  velox::test::assertEqualVectors(
      makeRowVector({
          makeFlatVector<std::string>({"SELECT n_name FROM nation"}),
          makeNullableFlatVector<std::string>({std::nullopt}),
      }),
      results.at(0));
}

TEST_F(InformationSchemaTest, complexColumnTypes) {
  testConnector_->addTable(
      "u",
      ROW(
          {{"a", ARRAY(REAL())},
           {"m", MAP(VARCHAR(), BIGINT())},
           {"r", ROW({{"x", BIGINT()}, {"y", VARCHAR()}})}}));

  auto results =
      run("SELECT column_name, data_type "
          "FROM information_schema.columns "
          "WHERE table_schema = 'default' AND table_name = 'u' "
          "ORDER BY ordinal_position");
  velox::test::assertEqualVectors(
      makeRowVector({
          makeFlatVector<std::string>({"a", "m", "r"}),
          makeFlatVector<std::string>(
              {"array(real)",
               "map(varchar, bigint)",
               "row(\"x\" bigint, \"y\" varchar)"}),
      }),
      results.at(0));
}

TEST_F(InformationSchemaTest, columnComments) {
  testConnector_->addTable(
      "u",
      ROW({"a", "b"}, BIGINT()),
      /*hiddenColumns=*/ROW({}),
      /*bucketSpec=*/std::nullopt,
      {{"a", "the first column"}});

  // A column the catalog describes reports its description; one it does not
  // reports null, which a client tells apart from an empty description.
  auto results =
      run("SELECT column_name, comment FROM information_schema.columns "
          "WHERE table_schema = 'default' AND table_name = 'u' "
          "ORDER BY ordinal_position");
  velox::test::assertEqualVectors(
      makeRowVector({
          makeFlatVector<std::string>({"a", "b"}),
          makeNullableFlatVector<std::string>(
              {"the first column", std::nullopt}),
      }),
      results.at(0));
}

TEST_F(InformationSchemaTest, customTypeName) {
  // A dialect that writes types differently registers its own spelling.
  registerSystemConnector(
      [](const velox::Type& /*type*/) { return "list(float)"; });

  testConnector_->addTable("v", ROW({{"a", ARRAY(REAL())}}));

  auto results =
      run("SELECT data_type FROM information_schema.columns "
          "WHERE table_schema = 'default' AND table_name = 'v'");
  velox::test::assertEqualVectors(
      makeRowVector({makeFlatVector<std::string>({"list(float)"})}),
      results.at(0));
}

TEST_F(InformationSchemaTest, viewColumns) {
  // A view's columns are described like a table's, but carry no role.
  auto results =
      run("SELECT column_name, data_type, extra_info "
          "FROM information_schema.columns "
          "WHERE table_schema = 'default' AND table_name = 'nation_names'");
  velox::test::assertEqualVectors(
      makeRowVector({
          makeFlatVector<std::string>({"n_name"}),
          makeFlatVector<std::string>({"varchar"}),
          makeNullableFlatVector<std::string>({std::nullopt}),
      }),
      results.at(0));
}

} // namespace
} // namespace facebook::axiom::connector::system

// Queries run here, and Velox execution reaches folly singletons that a
// gtest-provided main leaves uninitialized.
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
