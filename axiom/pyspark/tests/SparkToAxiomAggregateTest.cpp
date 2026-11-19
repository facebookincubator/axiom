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

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/logical_plan/ExprPrinter.h"
#include "axiom/logical_plan/LogicalPlanNode.h"
#include "axiom/pyspark/Exception.h"
#include "axiom/pyspark/SparkToAxiom.h"
#include "axiom/pyspark/third-party/protos/relations.pb.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/Connector.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook;

namespace axiom::collagen::test {
namespace {

class SparkToAxiomAggregateTest : public ::testing::Test {
 protected:
  void SetUp() override {
    velox::functions::prestosql::registerAllScalarFunctions();
    velox::aggregate::prestosql::registerAllAggregateFunctions();
    velox::memory::MemoryManager::testingSetInstance({});
    pool_ = velox::memory::memoryManager()->addLeafPool();

    auto connector =
        std::make_shared<facebook::axiom::connector::TestConnector>(
            kConnectorId);
    connector->addTable("t", velox::ROW({"a", "b"}, velox::BIGINT()));
    velox::connector::registerConnector(connector);
    facebook::axiom::connector::ConnectorMetadataRegistry::global().insert(
        kConnectorId, connector->metadata());
  }

  void TearDown() override {
    facebook::axiom::connector::ConnectorMetadataRegistry::global().erase(
        kConnectorId);
    velox::connector::unregisterConnector(kConnectorId);
  }

  // Returns an Aggregate relation over table 't' with the given group type.
  // Add grouping keys and aggregates to the result with addGroupingKey and
  // addAggregate.
  static spark::connect::Relation makeAggregate(
      spark::connect::Aggregate::GroupType groupType) {
    spark::connect::Relation relation;
    relation.mutable_common()->set_plan_id(1);
    auto* aggregate = relation.mutable_aggregate();
    aggregate->set_group_type(groupType);
    auto* input = aggregate->mutable_input();
    input->mutable_common()->set_plan_id(2);
    input->mutable_read()->mutable_named_table()->set_unparsed_identifier("t");
    return relation;
  }

  static void addGroupingKey(
      spark::connect::Relation& relation,
      std::string_view column) {
    relation.mutable_aggregate()
        ->add_grouping_expressions()
        ->mutable_unresolved_attribute()
        ->set_unparsed_identifier(std::string(column));
  }

  // Adds 'function(column) AS alias', with DISTINCT if 'distinct' is true.
  static void addAggregate(
      spark::connect::Relation& relation,
      std::string_view function,
      std::string_view column,
      std::string_view alias,
      bool distinct) {
    auto* aliasExpr = relation.mutable_aggregate()
                          ->add_aggregate_expressions()
                          ->mutable_alias();
    aliasExpr->add_name(std::string(alias));
    auto* call = aliasExpr->mutable_expr()->mutable_unresolved_function();
    call->set_function_name(std::string(function));
    call->set_is_distinct(distinct);
    call->add_arguments()
        ->mutable_unresolved_attribute()
        ->set_unparsed_identifier(std::string(column));
  }

  // Converts 'relation' and returns the resulting AggregateNode.
  const facebook::axiom::logical_plan::AggregateNode* toAggregateNode(
      const spark::connect::Relation& relation) {
    SparkToAxiom converter(kConnectorId, "default", pool_.get());
    SparkPlanVisitorContext context;
    converter.visit(relation, context);
    planNode_ = converter.planNode();
    const auto* aggregateNode =
        dynamic_cast<const facebook::axiom::logical_plan::AggregateNode*>(
            planNode_.get());
    VELOX_CHECK_NOT_NULL(aggregateNode);
    return aggregateNode;
  }

  static std::vector<std::string> toText(
      const std::vector<facebook::axiom::logical_plan::ExprPtr>& exprs) {
    std::vector<std::string> texts;
    for (const auto& expr : exprs) {
      texts.push_back(
          facebook::axiom::logical_plan::ExprPrinter::toText(*expr));
    }
    return texts;
  }

  static std::vector<std::string> toText(
      const std::vector<facebook::axiom::logical_plan::AggregateExprPtr>&
          aggregates) {
    return toText(
        std::vector<facebook::axiom::logical_plan::ExprPtr>(
            aggregates.begin(), aggregates.end()));
  }

  static constexpr const char* kConnectorId = "test-connector-aggregate";

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  // Owns the plan returned by the last toAggregateNode call.
  facebook::axiom::logical_plan::LogicalPlanNodePtr planNode_;
};

TEST_F(SparkToAxiomAggregateTest, groupBy) {
  auto relation = makeAggregate(spark::connect::Aggregate::GROUP_TYPE_GROUPBY);
  addGroupingKey(relation, "a");
  addAggregate(relation, "sum", "b", "s", false);
  addAggregate(relation, "count", "b", "c", false);
  addAggregate(relation, "max", "a", "m", false);

  const auto* aggregateNode = toAggregateNode(relation);
  EXPECT_THAT(toText(aggregateNode->groupingKeys()), testing::ElementsAre("a"));
  EXPECT_THAT(aggregateNode->groupingSets(), testing::IsEmpty());
  EXPECT_THAT(
      toText(aggregateNode->aggregates()),
      testing::ElementsAre("sum(b)", "count(b)", "max(a)"));
  EXPECT_THAT(
      aggregateNode->outputNames(), testing::ElementsAre("a", "s", "c", "m"));
}

TEST_F(SparkToAxiomAggregateTest, globalAggregate) {
  auto relation = makeAggregate(spark::connect::Aggregate::GROUP_TYPE_GROUPBY);
  addAggregate(relation, "sum", "b", "s", false);

  const auto* aggregateNode = toAggregateNode(relation);
  EXPECT_THAT(aggregateNode->groupingKeys(), testing::IsEmpty());
  EXPECT_THAT(aggregateNode->groupingSets(), testing::IsEmpty());
  EXPECT_THAT(
      toText(aggregateNode->aggregates()), testing::ElementsAre("sum(b)"));
  EXPECT_THAT(aggregateNode->outputNames(), testing::ElementsAre("s"));
}

TEST_F(SparkToAxiomAggregateTest, distinct) {
  auto relation = makeAggregate(spark::connect::Aggregate::GROUP_TYPE_GROUPBY);
  addGroupingKey(relation, "a");
  addAggregate(relation, "count", "b", "c", true);
  addAggregate(relation, "sum", "b", "s", true);
  addAggregate(relation, "count", "b", "n", false);

  const auto* aggregateNode = toAggregateNode(relation);
  EXPECT_THAT(
      toText(aggregateNode->aggregates()),
      testing::ElementsAre("count(DISTINCT b)", "sum(DISTINCT b)", "count(b)"));
  EXPECT_THAT(
      aggregateNode->outputNames(), testing::ElementsAre("a", "c", "s", "n"));
}

TEST_F(SparkToAxiomAggregateTest, unsupportedGroupTypes) {
  for (auto groupType : {
           spark::connect::Aggregate::GROUP_TYPE_UNSPECIFIED,
           spark::connect::Aggregate::GROUP_TYPE_ROLLUP,
           spark::connect::Aggregate::GROUP_TYPE_CUBE,
           spark::connect::Aggregate::GROUP_TYPE_PIVOT,
       }) {
    SCOPED_TRACE(spark::connect::Aggregate::GroupType_Name(groupType));
    auto relation = makeAggregate(groupType);
    addGroupingKey(relation, "a");
    addAggregate(relation, "count", "b", "c", false);

    EXPECT_THAT(
        [&]() { toAggregateNode(relation); },
        testing::ThrowsMessage<CollagenException>(testing::HasSubstr(
            "Aggregate group type is not supported: " +
            spark::connect::Aggregate::GroupType_Name(groupType))));
  }
}

} // namespace
} // namespace axiom::collagen::test
