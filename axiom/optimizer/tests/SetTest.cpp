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

#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/ParquetTpchTest.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::axiom::optimizer::test {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

class SetTest : public test::QueryTestBase {
 public:
  static void SetUpTestCase() {
    test::QueryTestBase::SetUpTestCase();

    std::string path;
    if (FLAGS_data_path.empty()) {
      gTempDirectory = velox::exec::test::TempDirectoryPath::create();
      path = gTempDirectory->getPath();
      test::ParquetTpchTest::createTables(path);
    } else {
      path = FLAGS_data_path;
      if (FLAGS_create_dataset) {
        test::ParquetTpchTest::createTables(path);
      }
    }

    LocalRunnerTestBase::localDataPath_ = path;
    LocalRunnerTestBase::localFileFormat_ =
        velox::dwio::common::FileFormat::PARQUET;
  }

  static void TearDownTestCase() {
    gTempDirectory.reset();
    test::QueryTestBase::TearDownTestCase();
  }

 private:
  inline static std::shared_ptr<velox::exec::test::TempDirectoryPath>
      gTempDirectory;
};

TEST_F(SetTest, unionAll) {
  auto nationType =
      ROW({"n_nationkey", "n_regionkey", "n_name", "n_comment"},
          {BIGINT(), BIGINT(), VARCHAR(), VARCHAR()});

  const auto connectorId = exec::test::kHiveConnectorId;
  const auto connector = velox::connector::getConnector(connectorId);

  const std::vector<std::string>& names = nationType->names();

  lp::PlanBuilder::Context ctx;
  auto t1 = lp::PlanBuilder(ctx)
                .tableScan(connectorId, "nation", names)
                .filter("n_nationkey < 11");
  auto t2 = lp::PlanBuilder(ctx)
                .tableScan(connectorId, "nation", names)
                .filter("n_nationkey > 13");

  auto logicalPlan = t1.unionAll(t2)
                         .project({"n_regionkey + 1 as rk"})
                         .filter("rk % 3 = 1")
                         .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher =
        core::PlanMatcherBuilder()
            .hiveScan(
                "nation", lte("n_nationkey", 10), "(n_regionkey + 1) % 3 = 1")
            .localPartition(
                core::PlanMatcherBuilder()
                    .hiveScan(
                        "nation",
                        gte("n_nationkey", 14),
                        "(n_regionkey + 1) % 3 = 1")
                    .project()
                    .build())
            .project()
            .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  auto referencePlan = exec::test::PlanBuilder(pool_.get())
                           .tableScan("nation", nationType)
                           .filter("n_nationkey < 11 or n_nationkey > 13")
                           .project({"n_regionkey + 1 as rk"})
                           .filter("rk % 3 = 1")
                           .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(SetTest, unionJoin) {
  auto partType = ROW({"p_partkey", "p_retailprice"}, {BIGINT(), DOUBLE()});
  auto partSuppType = ROW({"ps_partkey", "ps_availqty"}, {BIGINT(), INTEGER()});

  const auto connectorId = exec::test::kHiveConnectorId;
  const auto connector = velox::connector::getConnector(connectorId);

  lp::PlanBuilder::Context ctx;
  auto ps1 =
      lp::PlanBuilder(ctx)
          .tableScan(connectorId, "partsupp", {"ps_partkey", "ps_availqty"})
          .filter("ps_availqty < 1000::int")
          .project({"ps_partkey"});

  auto ps2 =
      lp::PlanBuilder(ctx)
          .tableScan(connectorId, "partsupp", {"ps_partkey", "ps_availqty"})
          .filter("ps_availqty > 2000::int")
          .project({"ps_partkey"});

  auto ps3 =
      lp::PlanBuilder(ctx)
          .tableScan(connectorId, "partsupp", {"ps_partkey", "ps_availqty"})
          .filter("ps_availqty between 1200::int and 1400::int")
          .project({"ps_partkey"});

  // The shape of the partsupp union is ps1 union all (ps2 union all ps3). We
  // verify that a stack of multiple set ops works.
  auto psu2 = ps2.unionAll(ps3);

  auto p1 = lp::PlanBuilder(ctx)
                .tableScan(connectorId, "part", {"p_partkey", "p_retailprice"})
                .filter("p_retailprice < 1100.0");

  auto p2 = lp::PlanBuilder(ctx)
                .tableScan(connectorId, "part", {"p_partkey", "p_retailprice"})
                .filter("p_retailprice > 1200.0");

  auto logicalPlan =
      ps1.unionAll(psu2)
          .join(p1.unionAll(p2), "ps_partkey = p_partkey", lp::JoinType::kInner)
          .aggregate({}, {"sum(1)"})
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher =
        core::PlanMatcherBuilder()
            .hiveScan("partsupp", lte("ps_availqty", 999))
            .localPartition({
                core::PlanMatcherBuilder()
                    .hiveScan("partsupp", gte("ps_availqty", 2001))
                    .project()
                    .build(),
                core::PlanMatcherBuilder()
                    .hiveScan("partsupp", between("ps_availqty", 1200, 1400))
                    .project()
                    .build(),
            })
            .hashJoin(
                core::PlanMatcherBuilder()
                    .hiveScan("part", lt("p_retailprice", 1100.0))
                    .localPartition(
                        core::PlanMatcherBuilder()
                            .hiveScan("part", gt("p_retailprice", 1200.0))
                            .project()
                            .build())
                    .build(),
                core::JoinType::kInner)
            .singleAggregation({}, {"sum(1)"})
            .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  auto idGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto referencePlan =
      exec::test::PlanBuilder(idGenerator)
          .tableScan("partsupp", partSuppType)
          .filter(
              "ps_availqty < 1000::int or ps_availqty > 2000::int "
              "or ps_availqty between 1200::int and 1400::int")
          .hashJoin(
              {"ps_partkey"},
              {"p_partkey"},
              exec::test::PlanBuilder(idGenerator)
                  .tableScan("part", partType)
                  .filter("p_retailprice < 1100.0 or p_retailprice > 1200.0")
                  .planNode(),
              "",
              {"p_partkey"})
          .project({"p_partkey"})
          .localPartition({})
          .singleAggregation({}, {"sum(1)"})
          .planNode();

  // Skip distributed run. Problem with local exchange source with
  // multiple inputs.
  checkSame(logicalPlan, referencePlan, {.numWorkers = 1, .numDrivers = 4});
}

// Checks
// - UNION ALL of two UNION ALL (should be flatten)
// - UNION ALL of two UNION (shouldn't be flatten)
// - UNION of two UNION ALL (should be flatten)
// - UNION of two UNION (should be flatten)
TEST_F(SetTest, unionFlatten) {
  auto nationType =
      ROW({"n_nationkey", "n_regionkey", "n_name", "n_comment"},
          {BIGINT(), BIGINT(), VARCHAR(), VARCHAR()});

  const auto connectorId = exec::test::kHiveConnectorId;
  const auto connector = velox::connector::getConnector(connectorId);

  const std::vector<std::string>& names = nationType->names();

  for (auto [rootType, leftType, rightType] : {
           std::tuple{
               lp::SetOperation::kUnion,
               lp::SetOperation::kUnion,
               lp::SetOperation::kUnion,
           },
           {
               lp::SetOperation::kUnion,
               lp::SetOperation::kUnionAll,
               lp::SetOperation::kUnionAll,
           },
           {
               lp::SetOperation::kUnionAll,
               lp::SetOperation::kUnion,
               lp::SetOperation::kUnion,
           },
           {
               lp::SetOperation::kUnionAll,
               lp::SetOperation::kUnionAll,
               lp::SetOperation::kUnionAll,
           },

       }) {
    lp::PlanBuilder::Context ctx;
    auto makeT1 = [&] {
      return lp::PlanBuilder(ctx)
          .tableScan(connectorId, "nation", names)
          .filter("n_nationkey < 11");
    };
    auto makeT2 = [&] {
      return lp::PlanBuilder(ctx)
          .tableScan(connectorId, "nation", names)
          .filter("n_nationkey > 13");
    };

    SCOPED_TRACE(
        fmt::format(
            "rootType={}, leftType={}, rightType={}",
            rootType,
            leftType,
            rightType));

    auto logicalPlan =
        makeT1()
            .setOperation(leftType, makeT1())
            .setOperation(rootType, makeT2().setOperation(rightType, makeT2()))
            .build();

    auto plan = toSingleNodePlan(logicalPlan);

    if (rootType == lp::SetOperation::kUnion) {
      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan()
              .localPartition({
                  core::PlanMatcherBuilder().tableScan().project().build(),
                  core::PlanMatcherBuilder().tableScan().project().build(),
                  core::PlanMatcherBuilder().tableScan().project().build(),
              })
              .aggregation()
              .build();

      AXIOM_ASSERT_PLAN(plan, matcher);
    } else if (
        leftType == lp::SetOperation::kUnionAll &&
        rightType == lp::SetOperation::kUnionAll) {
      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan()
              .localPartition({
                  core::PlanMatcherBuilder().tableScan().project().build(),
                  core::PlanMatcherBuilder().tableScan().project().build(),
                  core::PlanMatcherBuilder().tableScan().project().build(),
              })
              .build();

      AXIOM_ASSERT_PLAN(plan, matcher);
      continue;
    } else {
      // We cannot flatten UNION inside UNION ALL.
      auto matcher =
          core::PlanMatcherBuilder()
              .tableScan()
              .localPartition(
                  core::PlanMatcherBuilder().tableScan().project().build())
              .aggregation()
              .localPartition(
                  core::PlanMatcherBuilder()
                      .tableScan()
                      .localPartition(
                          core::PlanMatcherBuilder()
                              .tableScan()
                              .project()
                              .build())
                      .aggregation()
                      .project()
                      .build())
              .build();

      AXIOM_ASSERT_PLAN(plan, matcher);
    }

    auto referencePlan =
        exec::test::PlanBuilder(pool_.get())
            .tableScan("nation", nationType)
            .filter("n_nationkey < 11 or n_nationkey > 13")
            .singleAggregation(
                {"n_nationkey", "n_regionkey", "n_name", "n_comment"}, {})
            .planNode();

    // Skip distributed run. Problem with local exchange source with
    // multiple inputs.
    checkSame(logicalPlan, referencePlan, {.numWorkers = 1, .numDrivers = 4});
  }
}

TEST_F(SetTest, intersect) {
  auto nationType =
      ROW({"n_nationkey", "n_regionkey", "n_name", "n_comment"},
          {BIGINT(), BIGINT(), VARCHAR(), VARCHAR()});

  const auto connectorId = exec::test::kHiveConnectorId;
  const auto connector = velox::connector::getConnector(connectorId);

  const std::vector<std::string>& names = nationType->names();

  lp::PlanBuilder::Context ctx;
  auto t1 = lp::PlanBuilder(ctx)
                .tableScan(connectorId, "nation", names)
                .filter("n_nationkey < 21")
                .project({"n_nationkey", "n_regionkey"});
  auto t2 = lp::PlanBuilder(ctx)
                .tableScan(connectorId, "nation", names)
                .filter("n_nationkey > 11")
                .project({"n_nationkey", "n_regionkey"});
  auto t3 = lp::PlanBuilder(ctx)
                .tableScan(connectorId, "nation", names)
                .filter("n_nationkey > 12")
                .project({"n_nationkey", "n_regionkey"});

  auto logicalPlan =
      lp::PlanBuilder(ctx)
          .setOperation(lp::SetOperation::kIntersect, {t1, t2, t3})
          .project({"n_regionkey + 1 as rk"})
          .filter("rk % 3 = 1")
          .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher =
        core::PlanMatcherBuilder()
            // TODO Fix this plan to push down (n_regionkey + 1) % 3
            // = 1 to all branches of 'intersect'.
            .hiveScan(
                "nation", lte("n_nationkey", 20), "(n_regionkey + 1) % 3 = 1")
            .hashJoin(
                core::PlanMatcherBuilder()
                    .hiveScan("nation", gte("n_nationkey", 12))
                    .build(),
                core::JoinType::kLeftSemiFilter)
            .hashJoin(
                core::PlanMatcherBuilder()
                    .hiveScan("nation", gte("n_nationkey", 13))
                    .build(),
                core::JoinType::kLeftSemiFilter)
            .singleAggregation()
            .project()
            .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  auto referencePlan = exec::test::PlanBuilder(pool_.get())
                           .tableScan("nation", nationType)
                           .filter("n_nationkey > 12 and n_nationkey < 21")
                           .project({"n_regionkey + 1 as rk"})
                           .filter("rk % 3 = 1")
                           .planNode();

  checkSame(logicalPlan, referencePlan);
}

TEST_F(SetTest, except) {
  auto nationType =
      ROW({"n_nationkey", "n_regionkey", "n_name", "n_comment"},
          {BIGINT(), BIGINT(), VARCHAR(), VARCHAR()});

  const auto connectorId = exec::test::kHiveConnectorId;
  const auto connector = velox::connector::getConnector(connectorId);

  const std::vector<std::string>& names = nationType->names();

  lp::PlanBuilder::Context ctx;
  auto t1 = lp::PlanBuilder(ctx)
                .tableScan(connectorId, "nation", names)
                .filter("n_nationkey < 21")
                .project({"n_nationkey", "n_regionkey"});
  auto t2 = lp::PlanBuilder(ctx)
                .tableScan(connectorId, "nation", names)
                .filter("n_nationkey > 16")
                .project({"n_nationkey", "n_regionkey"});
  auto t3 = lp::PlanBuilder(ctx)
                .tableScan(connectorId, "nation", names)
                .filter("n_nationkey <= 5")
                .project({"n_nationkey", "n_regionkey"});

  auto logicalPlan = lp::PlanBuilder(ctx)
                         .setOperation(lp::SetOperation::kExcept, {t1, t2, t3})
                         .project({"n_nationkey", "n_regionkey + 1 as rk"})
                         .filter("rk % 3 = 1")
                         .build();

  {
    auto plan = toSingleNodePlan(logicalPlan);
    auto matcher =
        core::PlanMatcherBuilder()
            .hiveScan(
                "nation", lte("n_nationkey", 20), "(n_regionkey + 1) % 3 = 1")
            .hashJoin(
                core::PlanMatcherBuilder()
                    // TODO Fix this plan to push down (n_regionkey + 1) % 3 = 1
                    // to all branches of 'except'.
                    .hiveScan("nation", gte("n_nationkey", 17))
                    .build(),
                core::JoinType::kAnti)
            .hashJoin(
                core::PlanMatcherBuilder()
                    .hiveScan("nation", lte("n_nationkey", 5))
                    .build(),
                core::JoinType::kAnti)
            .singleAggregation()
            .project()
            .build();

    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  auto referencePlan = exec::test::PlanBuilder(pool_.get())
                           .tableScan("nation", nationType)
                           .filter("n_nationkey > 5 and n_nationkey <= 16")
                           .project({"n_nationkey", "n_regionkey + 1 as rk"})
                           .filter("rk % 3 = 1")
                           .planNode();

  checkSame(logicalPlan, referencePlan);
}

} // namespace
} // namespace facebook::axiom::optimizer::test
