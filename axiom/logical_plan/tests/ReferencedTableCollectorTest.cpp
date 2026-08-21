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

#include "axiom/logical_plan/ReferencedTableCollector.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook::velox;

namespace facebook::axiom::logical_plan {
namespace {

using ::testing::IsEmpty;
using ::testing::UnorderedElementsAre;

class ReferencedTableCollectorTest : public testing::Test {
 protected:
  static constexpr auto kConnectorId = "test";
  static constexpr auto kOtherConnectorId = "other";
  static constexpr auto kSchema = connector::TestConnector::kDefaultSchema;
  static constexpr auto kOtherSchema = "archive";

  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();

    connector_ = addConnector(kConnectorId);
    otherConnector_ = addConnector(kOtherConnectorId);

    const auto ordersType = ROW({"id", "amount"}, BIGINT());
    connector_->addTable("orders", ordersType);
    connector_->addTable("orders_copy", ordersType);
    connector_->addTable("lineitem", ROW({"id", "quantity"}, BIGINT()));
    connector_->addTable({std::string(kOtherSchema), "orders"}, ordersType);
    otherConnector_->addTable("orders", ordersType);
  }

  void TearDown() override {
    removeConnector(kOtherConnectorId);
    removeConnector(kConnectorId);
    connector_.reset();
    otherConnector_.reset();
  }

  static CatalogSchemaTableName table(
      std::string_view name,
      std::string_view catalogName = kConnectorId,
      std::string_view schema = kSchema) {
    return {
        .catalogName = std::string{catalogName},
        .schemaTableName = {
            .schema = std::string{schema}, .table = std::string{name}}};
  }

  PlanBuilder planBuilder() {
    return PlanBuilder(context_, /*allowAmbiguousOutputNames=*/true);
  }

  static ReferencedTables collect(const LogicalPlanNodePtr& plan) {
    return ReferencedTableCollector::collect(*plan);
  }

  std::shared_ptr<connector::TestConnector> connector_;
  std::shared_ptr<connector::TestConnector> otherConnector_;

  PlanBuilder::Context context_{
      std::string(kConnectorId),
      std::string(kSchema)};

 private:
  static std::shared_ptr<connector::TestConnector> addConnector(
      std::string_view connectorId) {
    auto connector =
        std::make_shared<connector::TestConnector>(std::string(connectorId));
    velox::connector::ConnectorRegistry::global().insert(
        std::string(connectorId), connector);
    connector::ConnectorMetadataRegistry::global().insert(
        std::string(connectorId), connector->metadata());
    return connector;
  }

  static void removeConnector(std::string_view connectorId) {
    connector::ConnectorMetadataRegistry::global().erase(
        std::string(connectorId));
    velox::connector::ConnectorRegistry::global().erase(
        std::string(connectorId));
  }
};

TEST_F(ReferencedTableCollectorTest, scanIsAnInputTable) {
  auto plan = planBuilder().tableScan("orders").build();

  const auto referenced = collect(plan);

  EXPECT_THAT(referenced.inputTables, UnorderedElementsAre(table("orders")));
  EXPECT_FALSE(referenced.outputTable.has_value());
}

TEST_F(ReferencedTableCollectorTest, joinCollectsBothSides) {
  auto plan = planBuilder()
                  .tableScan("orders")
                  .as("o")
                  .join(
                      planBuilder().tableScan("lineitem").as("l"),
                      "o.id = l.id",
                      JoinType::kInner)
                  .build();

  const auto referenced = collect(plan);

  EXPECT_THAT(
      referenced.inputTables,
      UnorderedElementsAre(table("orders"), table("lineitem")));
}

TEST_F(ReferencedTableCollectorTest, selfJoinCollectsOneTable) {
  auto plan = planBuilder()
                  .tableScan("orders")
                  .as("o1")
                  .join(
                      planBuilder().tableScan("orders").as("o2"),
                      "o1.id = o2.id",
                      JoinType::kInner)
                  .build();

  const auto referenced = collect(plan);

  EXPECT_THAT(referenced.inputTables, UnorderedElementsAre(table("orders")));
}

TEST_F(ReferencedTableCollectorTest, unionCollectsEveryBranchOnce) {
  auto plan = planBuilder()
                  .tableScan("orders", {"id"})
                  .unionAll(planBuilder().tableScan("lineitem", {"id"}))
                  .unionAll(planBuilder().tableScan("orders", {"id"}))
                  .build();

  const auto referenced = collect(plan);

  EXPECT_THAT(
      referenced.inputTables,
      UnorderedElementsAre(table("orders"), table("lineitem")));
}

TEST_F(ReferencedTableCollectorTest, sameTableNameInTwoCatalogsStaysApart) {
  auto plan = planBuilder()
                  .tableScan("orders")
                  .unionAll(
                      planBuilder().tableScan(
                          std::string(kOtherConnectorId),
                          std::string(kSchema),
                          std::string("orders")))
                  .build();

  const auto referenced = collect(plan);

  EXPECT_THAT(
      referenced.inputTables,
      UnorderedElementsAre(
          table("orders"), table("orders", kOtherConnectorId)));
}

TEST_F(ReferencedTableCollectorTest, sameTableNameInTwoSchemasStaysApart) {
  auto plan = planBuilder()
                  .tableScan("orders")
                  .unionAll(
                      planBuilder().tableScan(
                          std::string(kOtherSchema), std::string("orders")))
                  .build();

  const auto referenced = collect(plan);

  EXPECT_THAT(
      referenced.inputTables,
      UnorderedElementsAre(
          table("orders"), table("orders", kConnectorId, kOtherSchema)));
}

TEST_F(ReferencedTableCollectorTest, subqueryTableIsAnInputTable) {
  auto plan =
      planBuilder()
          .tableScan("orders")
          .filter(Exists(Subquery(planBuilder().tableScan("lineitem").build())))
          .build();

  const auto referenced = collect(plan);

  EXPECT_THAT(
      referenced.inputTables,
      UnorderedElementsAre(table("orders"), table("lineitem")));
}

TEST_F(ReferencedTableCollectorTest, insertSeparatesSourceFromTarget) {
  auto plan =
      planBuilder()
          .tableScan("orders")
          .tableWrite("orders_copy", WriteKind::kInsert, {"id", "amount"})
          .build();

  const auto referenced = collect(plan);

  EXPECT_THAT(referenced.inputTables, UnorderedElementsAre(table("orders")));
  EXPECT_EQ(referenced.outputTable, table("orders_copy"));
}

TEST_F(ReferencedTableCollectorTest, deleteTargetIsBothReadAndWritten) {
  // Matches what the Presto parser records for DELETE, where the target is
  // scanned to find the rows to remove.
  auto plan = planBuilder().tableScan("orders").tableDelete("orders").build();

  const auto referenced = collect(plan);

  EXPECT_THAT(referenced.inputTables, UnorderedElementsAre(table("orders")));
  EXPECT_EQ(referenced.outputTable, table("orders"));
}

TEST_F(ReferencedTableCollectorTest, secondWriteFails) {
  // A write reaches the collector twice only through a subquery, since a node
  // rejects a write as its input.
  auto write =
      planBuilder()
          .tableScan("orders")
          .tableWrite("orders_copy", WriteKind::kInsert, {"id", "amount"})
          .build();

  auto plan = planBuilder()
                  .tableScan("orders")
                  .filter(Exists(Subquery(write)))
                  .tableWrite("orders", WriteKind::kInsert, {"id", "amount"})
                  .build();

  VELOX_ASSERT_USER_THROW(
      collect(plan),
      "Plan cannot write more than one table: "
      "test.default.orders and test.default.orders_copy");
}

TEST_F(ReferencedTableCollectorTest, planWithoutTablesReferencesNothing) {
  auto plan =
      planBuilder()
          .values(
              ROW({"id"}, BIGINT()), std::vector<Variant>{Variant::row({1LL})})
          .build();

  const auto referenced = collect(plan);

  EXPECT_THAT(referenced.inputTables, IsEmpty());
  EXPECT_FALSE(referenced.outputTable.has_value());
}

} // namespace
} // namespace facebook::axiom::logical_plan
