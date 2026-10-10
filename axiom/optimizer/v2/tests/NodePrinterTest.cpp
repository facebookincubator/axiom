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

#include <folly/String.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "axiom/optimizer/tests/QueryTestBase.h"
#include "axiom/optimizer/v2/Node.h"
#include "axiom/optimizer/v2/NodePrinter.h"

namespace facebook::axiom::optimizer::v2::test {
namespace {

using testing::ElementsAre;
using testing::Eq;
using testing::HasSubstr;
using testing::StartsWith;

class NodePrinterTest : public optimizer::test::QueryTestBase {
 protected:
  void configureTestConnector() override {
    testConnector_->addTable("t", velox::ROW("a", velox::BIGINT()));
    testConnector_->addLookupTable(
        "lookup", velox::ROW({"k", "v"}, velox::BIGINT()), {"k"});
  }

  std::vector<std::string> toLines(
      NodeCP node,
      const NodePrinter::Options& options = {}) {
    std::vector<std::string> lines;
    folly::split('\n', NodePrinter::toText(node, options), lines);
    return lines;
  }
};

TEST_F(NodePrinterTest, fixedPoint) {
  const auto plan = parseSelect(
      "WITH RECURSIVE counter(n) AS ("
      "SELECT 1 UNION ALL SELECT n + 1 FROM counter WHERE n < 10) "
      "SELECT n FROM counter");

  verifyOptimization(*plan, Optimizer::Pass::kTranslate, [&](NodeCP root) {
    // Names carry NameAllocator ids, so assert structure and drop the tail.
    EXPECT_THAT(
        toLines(root),
        ElementsAre(
            StartsWith(
                "- FixedPoint[name=counter, maxIterations=1000, recursiveNumDrivers=unplanned] ->"),
            Eq("  anchor:"),
            StartsWith("  - Values ->"),
            Eq("  step:"),
            StartsWith("  - Project ->"),
            HasSubstr(":= plus("),
            StartsWith("    - Filter ->"),
            StartsWith("      predicate: lt("),
            StartsWith(
                "      - WorkingTable[name=counter, readMode=latestDelta] ->"),
            Eq("  convergence:"),
            StartsWith("  - Project ->"),
            HasSubstr(":= eq("),
            StartsWith("    - Aggregate ->"),
            Eq("      aggregates: count()"),
            StartsWith(
                "      - WorkingTable[name=counter, readMode=latestDelta] ->"),
            Eq("")));
  });
}

TEST_F(NodePrinterTest, unknownEstimate) {
  const auto plan = parseSelect("SELECT 1 AS a");
  verifyOptimization(*plan, Optimizer::Pass::kTranslate, [&](NodeCP root) {
    EXPECT_THAT(
        toLines(
            root,
            {.estimates =
                 [](NodeCP) { return Estimate{.cardinality = std::nullopt}; }}),
        ElementsAre(
            StartsWith("- Project ->"),
            Eq("  Estimate: unknown"),
            HasSubstr(":= 1"),
            StartsWith("  - Values ->"),
            Eq("    Estimate: unknown"),
            Eq("")));
  });
}

TEST_F(NodePrinterTest, selectivityAndFanout) {
  const auto filter =
      parseSelect("SELECT * FROM (VALUES 1, 2) AS t(x) WHERE x > 1");
  verifyOptimization(*filter, Optimizer::Pass::kTranslate, [&](NodeCP root) {
    EstimateProvider estimateProvider;
    EXPECT_THAT(
        toLines(
            root,
            {.estimates =
                 [&](NodeCP node) { return estimateProvider.estimate(node); }}),
        ElementsAre(
            StartsWith("- Filter ->"),
            Eq("  Estimate: 1 rows, selectivity: 0.5"),
            StartsWith("  predicate:"),
            StartsWith("  - Values ->"),
            Eq("    Estimate: 2 rows"),
            Eq("")));
  });

  const auto join = parseSelect(
      "SELECT * FROM (VALUES 1, 2) AS l(left_key) CROSS JOIN "
      "(VALUES 1, 2, 3) AS r(right_key)");
  verifyOptimization(*join, Optimizer::Pass::kTranslate, [&](NodeCP root) {
    EstimateProvider estimateProvider;
    EXPECT_THAT(
        toLines(
            root,
            {.estimates =
                 [&](NodeCP node) { return estimateProvider.estimate(node); }}),
        ElementsAre(
            StartsWith("- Join[INNER] ->"),
            Eq("  Estimate: 6 rows, left fanout: 3, right fanout: 2"),
            StartsWith("  - Values ->"),
            Eq("    Estimate: 2 rows"),
            StartsWith("  - Values ->"),
            Eq("    Estimate: 3 rows"),
            Eq("")));
  });
}

TEST_F(NodePrinterTest, indexLookupJoin) {
  const auto plan =
      parseSelect("SELECT * FROM t JOIN lookup ON t.a = lookup.k");

  verifyOptimization(*plan, Optimizer::Pass::kPushdownAndPrune, [&](NodeCP root) {
    EXPECT_THAT(
        toLines(root),
        ElementsAre(
            Eq("- IndexLookupJoin[INNER, \"default\".\"lookup\"] -> a:BIGINT, v:BIGINT"),
            Eq("  probeKeys: a"),
            Eq("  lookupKeys: k"),
            Eq("  - Scan[\"default\".\"t\"] -> a:BIGINT"),
            Eq("    handle: \"default\".\"t\""),
            Eq("")));
  });
}

TEST_F(NodePrinterTest, exchangePartitioning) {
  const auto plan = parseSelect("SELECT * FROM t");

  verifyOptimization(*plan, Optimizer::Pass::kTranslate, [&](NodeCP root) {
    Builder builder;
    const ExprVector keys{root->outputColumns().front()};
    NodeCP hash = builder.make<Exchange>(
        {root, Partitioning::hash(PropertyScope::kDriver, keys)});
    EXPECT_THAT(
        toLines(hash),
        ElementsAre(
            Eq("- Exchange -> a:BIGINT"),
            Eq("  partitioning: Driver Partitioned"),
            Eq("  partitionKeys: a"),
            Eq("  - Scan[\"default\".\"t\"] -> a:BIGINT"),
            Eq("")));

    const OrderTypeVector orderTypes{OrderType::kAscNullsFirst};
    NodeCP gatherMerge = builder.make<Exchange>(
        {root,
         Partitioning::gatherMerge(PropertyScope::kGlobal, keys, orderTypes)});
    EXPECT_THAT(
        toLines(gatherMerge),
        ElementsAre(
            Eq("- Exchange -> a:BIGINT"),
            Eq("  partitioning: Global Gather"),
            Eq("  orderKeys: a"),
            Eq("  orderTypes: ASC NULLS FIRST"),
            Eq("  - Scan[\"default\".\"t\"] -> a:BIGINT"),
            Eq("")));
  });
}

} // namespace
} // namespace facebook::axiom::optimizer::v2::test
