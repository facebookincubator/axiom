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

#include "optimizer/Plan.h" //@manual
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include "optimizer/VeloxHistory.h" //@manual
#include "optimizer/tests/ParquetTpchTest.h" //@manual
#include "optimizer/tests/QueryTestBase.h" //@manual
#include "optimizer/tests/Tpch.h" //@manual
#include "velox/common/file/FileSystems.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/exec/tests/utils/TpchQueryBuilder.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"

DEFINE_int32(num_repeats, 1, "Number of repeats for optimization timing");

DECLARE_int32(optimizer_trace);

DECLARE_int32(num_workers);

DECLARE_string(history_save_path);

using namespace facebook::velox;
using namespace facebook::velox::optimizer;
using namespace facebook::velox::optimizer::test;

std::string nodeString(core::PlanNode* node) {
  return node->toString(true, true);
}

class PlanTest : public virtual ParquetTpchTest, public virtual QueryTestBase {
 protected:
  static void SetUpTestCase() {
    ParquetTpchTest::SetUpTestCase();
    LocalRunnerTestBase::testDataPath_ = FLAGS_data_path;
    LocalRunnerTestBase::localFileFormat_ = "parquet";
    connector::unregisterConnector(exec::test::kHiveConnectorId);
    connector::unregisterConnectorFactory("hive");
    LocalRunnerTestBase::SetUpTestCase();
  }

  static void TearDownTestCase() {
    if (!FLAGS_history_save_path.empty()) {
      suiteHistory_->saveToFile(FLAGS_history_save_path);
    }
    LocalRunnerTestBase::TearDownTestCase();
    ParquetTpchTest::TearDownTestCase();
  }

  void SetUp() override {
    ParquetTpchTest::SetUp();
    QueryTestBase::SetUp();
    allocator_ = std::make_unique<HashStringAllocator>(pool_.get());
    context_ = std::make_unique<QueryGraphContext>(*allocator_);
    queryCtx() = context_.get();
    builder_ = std::make_unique<exec::test::TpchQueryBuilder>(
        dwio::common::FileFormat::PARQUET, true);
    builder_->initialize(FLAGS_data_path);
    referenceBuilder_ = std::make_unique<exec::test::TpchQueryBuilder>(
        dwio::common::FileFormat::PARQUET);
    referenceBuilder_->initialize(FLAGS_data_path);
  }

  void TearDown() override {
    context_.reset();
    queryCtx() = nullptr;
    allocator_.reset();
    ParquetTpchTest::TearDown();
    QueryTestBase::TearDown();
  }

  std::string makePlan(
      std::shared_ptr<const core::PlanNode> plan,
      bool partitioned,
      bool ordered,
      int numRepeats = FLAGS_num_repeats) {
    std::string planText;
    std::string errorText;
    for (auto counter = 0; counter < numRepeats; ++counter) {
      optimizerOptions_.traceFlags = FLAGS_optimizer_trace;
      auto result = planVelox(plan, &planText, &errorText);
    }
    return fmt::format(
        "=== {} {}:\n{}\n",
        partitioned ? "Partitioned on PK" : "Not partitioned",
        ordered ? "sorted on PK" : "not sorted",
        planText);
  }

  void checkSame(
      const core::PlanNodePtr& planNode,
      core::PlanNodePtr referencePlan = nullptr,
      std::string* planString = nullptr,
      std::string* veloxPlan = nullptr) {
    auto fragmentedPlan = planVelox(planNode, planString);
    if (veloxPlan) {
      *veloxPlan = veloxString(fragmentedPlan.plan);
    }
    auto reference = referencePlan ? referencePlan : planNode;
    TestResult referenceResult;
    assertSame(reference, fragmentedPlan, &referenceResult);
    auto numWorkers = FLAGS_num_workers;
    if (numWorkers != 1) {
      FLAGS_num_workers = 1;
      auto singlePlan = planVelox(planNode, planString);
      ASSERT_TRUE(singlePlan.plan != nullptr);
      auto singleResult = runFragmentedPlan(singlePlan);
      exec::test::assertEqualResults(
          referenceResult.results, singleResult.results);
      FLAGS_num_workers = numWorkers;
    }
  }

  // Breaks str into tokens at whitespace and punctuation. Returns tokens as
  // string, character position pairs.
  std::vector<std::pair<std::string, int32_t>> tokenize(
      const std::string& str) {
    std::vector<std::pair<std::string, int32_t>> result;
    std::string token;
    for (auto i = 0; i < str.size(); ++i) {
      char c = str[i];
      if (strchr(" \n\t", c)) {
        if (token.empty()) {
          continue;
        }
        auto offset = i - token.size();
        result.push_back(std::make_pair(std::move(token), offset));
      } else if (strchr("()[]*%", c)) {
        if (!token.empty()) {
          auto offset = i - token.size();
          result.push_back(std::make_pair(std::move(token), offset));
        }
        token.resize(1);
        token[0] = c;
        result.push_back(std::make_pair(std::move(token), i));
      } else {
        token.push_back(c);
      }
    }
    return result;
  }

  void expectPlan(const std::string& actual, const std::string& expected) {
    auto expectedTokens = tokenize(expected);
    auto actualTokens = tokenize(expected);
    for (auto i = 0; i < actualTokens.size() && i < expectedTokens.size();
         ++i) {
      if (actualTokens[i].first != expectedTokens[i].first) {
        FAIL() << "Difference at " << i << " position "
               << actualTokens[i].second << "= " << actualTokens[i].first
               << " vs " << expectedTokens[i].first << "\na actual= " << actual
               << "\nexpected=" << expected;
        return;
      }
    }
  }

  void checkTpch(int32_t query, std::string expected = "") {
    auto q = builder_->getQueryPlan(query).plan;
    auto rq = referenceBuilder_->getQueryPlan(query).plan;
    std::string planText;
    checkSame(q, rq, &planText);
    if (!expected.empty()) {
      expectPlan(planText, expected);
    } else {
      std::cout << " -- plan = " << planText << std::endl;
    }
  }

  void appendNames(const RowTypePtr& type, std::vector<std::string>& names) {
    for (auto i = 0; i < type->size(); ++i) {
      names.push_back(type->nameOf(i));
    }
  }

  std::unique_ptr<HashStringAllocator> allocator_;
  std::unique_ptr<QueryGraphContext> context_;
  std::unique_ptr<exec::test::TpchQueryBuilder> builder_;
  std::unique_ptr<exec::test::TpchQueryBuilder> referenceBuilder_;
  static inline bool registered;
};

void printPlan(core::PlanNode* plan, bool r, bool d) {
  std::cout << plan->toString(r, d) << std::endl;
}

TEST_F(PlanTest, queryGraph) {
  TypePtr row1 = ROW({{"c1", ROW({{"c1a", INTEGER()}})}, {"c2", DOUBLE()}});
  TypePtr row2 = row1 =
      ROW({{"c1", ROW({{"c1a", INTEGER()}})}, {"c2", DOUBLE()}});
  TypePtr largeRow = ROW(
      {{"c1", ROW({{"c1a", INTEGER()}})},
       {"c2", DOUBLE()},
       {"m1", MAP(INTEGER(), ARRAY(INTEGER()))}});
  TypePtr differentNames =
      ROW({{"different", ROW({{"c1a", INTEGER()}})}, {"c2", DOUBLE()}});

  auto* dedupRow1 = toType(row1);
  auto* dedupRow2 = toType(row2);
  auto* dedupLargeRow = toType(largeRow);
  auto* dedupDifferentNames = toType(differentNames);

  // dedupped complex types make a copy.
  EXPECT_NE(row1.get(), dedupRow1);

  // Identical types get equal pointers.
  EXPECT_EQ(dedupRow1, dedupRow2);

  // Different names differentiate types.
  EXPECT_NE(dedupDifferentNames, dedupRow1);

  // Shared complex substructure makes equal pointers.
  EXPECT_EQ(dedupRow1->childAt(0).get(), dedupLargeRow->childAt(0).get());

  // Identical child types with different names get equal pointers.
  EXPECT_EQ(dedupRow1->childAt(0).get(), dedupDifferentNames->childAt(0).get());

  auto* path = make<Path>()
                   ->subscript("field")
                   ->subscript(123)
                   ->field("f1")
                   ->cardinality();
  auto interned = queryCtx()->toPath(path);
  EXPECT_EQ(interned, path);
  auto* path2 = make<Path>()
                    ->subscript("field")
                    ->subscript(123)
                    ->field("f1")
                    ->cardinality();
  auto interned2 = queryCtx()->toPath(path2);
  EXPECT_EQ(interned2, interned);
}

TEST_F(PlanTest, q1) {
  checkTpch(1);
}

TEST_F(PlanTest, q2) {
  checkTpch(1);
}

TEST_F(PlanTest, q3) {
  checkTpch(
      3,
      "lineitem t2 shuffle *H  (orders t3*H  (customer t4 broadcast   Build ) shuffle   Build ) PARTIAL agg shuffle  FINAL agg");
}
TEST_F(PlanTest, q4) {
  // Incorrect with distributed plan at larger scales.
  GTEST_SKIP();
  checkTpch(4);
}

TEST_F(PlanTest, q5) {
  checkTpch(5);
}

TEST_F(PlanTest, q6) {
  checkTpch(6);
}

TEST_F(PlanTest, q7) {
  checkTpch(7);
}

TEST_F(PlanTest, q8) {
  checkTpch(8);
}

TEST_F(PlanTest, q9) {
  // Plan does not minimize build size. To adjust build cost and check that
  // import of existences to build side does not affect join cardinality.
  checkTpch(9);
}

TEST_F(PlanTest, q10) {
  checkTpch(10);
}

TEST_F(PlanTest, q11) {
  checkTpch(11);
}

TEST_F(PlanTest, q12) {
  // Fix string in filter
  checkTpch(12);
}

TEST_F(PlanTest, q13) {
  checkTpch(13);
}

TEST_F(PlanTest, q14) {
  checkTpch(14);
}

TEST_F(PlanTest, q15) {
  GTEST_SKIP();
  checkTpch(15);
}

TEST_F(PlanTest, q16) {
  GTEST_SKIP();
  checkTpch(16);
}

TEST_F(PlanTest, q17) {
  GTEST_SKIP();
  checkTpch(17);
}

TEST_F(PlanTest, q18) {
  GTEST_SKIP();
  checkTpch(18);
}

TEST_F(PlanTest, q19) {
  checkTpch(19);
}

TEST_F(PlanTest, q20) {
  GTEST_SKIP();
  checkTpch(20);
}

TEST_F(PlanTest, q21) {
  GTEST_SKIP();
  checkTpch(21);
}

TEST_F(PlanTest, q22) {
  GTEST_SKIP();
  checkTpch(22);
}

TEST_F(PlanTest, filterToJoinEdge) {
  auto nationType = ROW({"n_regionkey"}, {BIGINT()});
  auto regionType = ROW({"r_regionkey"}, {BIGINT()});
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto nested = exec::test::PlanBuilder(planNodeIdGenerator)
                    .tableScan("nation", nationType, {}, {})
                    .nestedLoopJoin(
                        exec::test::PlanBuilder(planNodeIdGenerator)
                            .tableScan("region", regionType, {}, {})
                            .planNode(),
                        {"n_regionkey", "r_regionkey"},
                        core::JoinType::kInner)
                    .filter("n_regionkey + 1 = r_regionkey + 1")
                    .planNode();
  std::string plan;
  checkSame(nested, nullptr, &plan);
  expectPlan(plan, "nation t2*H  (region t3  Build ) project");

  nested =
      exec::test::PlanBuilder(planNodeIdGenerator)
          .tableScan("nation", nationType, {}, {})
          .filter("random() < 2::DOUBLE")
          .nestedLoopJoin(
              exec::test::PlanBuilder(planNodeIdGenerator)
                  .tableScan("region", regionType, {}, {})
                  .filter("random() < 2::DOUBLE")
                  .planNode(),
              {"n_regionkey", "r_regionkey"},
              core::JoinType::kInner)
          .filter("n_regionkey + 1 = r_regionkey + 1 and random() < 2::DOUBLE")
          .planNode();
  checkSame(nested, nullptr, &plan);
  expectPlan(
      plan,
      "nation t5 filter 1 exprs  project 1 columns  project 1 columns *H  (region t8 filter 1 exprs  project 1 columns  project 1 columns  broadcast   Build ) filter 1 exprs  project 2 columns  project 2 columns ");
}

TEST_F(PlanTest, filterImport) {
  auto orderType = ROW({"o_custkey", "o_totalprice"}, {BIGINT(), DOUBLE()});
  auto agg = exec::test::PlanBuilder()
                 .tableScan("orders", orderType, {}, {})
                 .singleAggregation({"o_custkey"}, {"sum(o_totalprice)"})
                 .singleAggregation({"o_custkey"}, {"sum(a0)"})
                 .filter("o_custkey < 100 and a0 > 200.0")
                 .planNode();
  std::string plan;
  checkSame(agg, nullptr, &plan);
  expectPlan(
      plan,
      "orders t3 PARTIAL agg shuffle  FINAL agg project 2 columns  PARTIAL agg FINAL agg filter 1 exprs  project");
}

TEST_F(PlanTest, filterBreakup) {
  const char* filterText =
      "        (\n"
      "                l_partkey = p_partkey\n"
      "                and p_brand = 'Brand#12'\n"
      "                and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\n"
      "                and l_quantity >= 1.0 and l_quantity <= 1.0 + 10.0\n"
      "                and p_size between 1 and 5\n"
      "                and l_shipmode in ('AIR', 'AIR REG')\n"
      "                and l_shipinstruct = 'DELIVER IN PERSON'\n"
      "        )\n"
      "        or\n"
      "        (\n"
      "                p_partkey = l_partkey\n"
      "                and p_brand = 'Brand#23'\n"
      "                and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\n"
      "                and l_quantity >= 10.0 and l_quantity <= 10.0 + 10.0\n"
      "                and p_size between 1 and 10\n"
      "                and l_shipmode in ('AIR', 'AIR REG')\n"
      "                and l_shipinstruct = 'DELIVER IN PERSON'\n"
      "        )\n"
      "        or\n"
      "        (\n"
      "                p_partkey = l_partkey\n"
      "                and p_brand = 'Brand#34'\n"
      "                and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\n"
      "                and l_quantity >= 20.0 and l_quantity <= 20.0 + 10.0\n"
      "                and p_size between 1 and 15\n"
      "                and l_shipmode in ('AIR', 'AIR REG')\n"
      "                and l_shipinstruct = 'DELIVER IN PERSON'\n"
      "        )\n";

  auto lineitemType = ROW(
      {{"l_partkey", BIGINT()},
       {"l_shipmode", VARCHAR()},
       {"l_shipinstruct", VARCHAR()},
       {"l_extendedprice", DOUBLE()},
       {"l_discount", DOUBLE()},
       {"l_quantity", DOUBLE()}});
  auto partType = ROW(
      {{"p_partkey", BIGINT()},
       {"p_brand", VARCHAR()},
       {"p_container", VARCHAR()},
       {"p_size", INTEGER()}});
  std::vector<std::string> allNames;
  appendNames(lineitemType, allNames);
  appendNames(partType, allNames);
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan =
      exec::test::PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan("lineitem", lineitemType)
          .nestedLoopJoin(
              exec::test::PlanBuilder(planNodeIdGenerator)
                  .tableScan("part", partType)
                  .planNode(),
              allNames)
          .filter(filterText)
          .project({"l_extendedprice * (1.0 - l_discount) as part_revenue"})
          .singleAggregation({}, {"sum(part_revenue)"})
          .planNode();
  auto reference = referenceBuilder_->getQueryPlan(19).plan;
  std::string(planString);
  std::string veloxString;
  checkSame(plan, reference, &planString, &veloxString);

  // Expect the per table filters to be extracted from the OR.
  expectRegexp(
      veloxString,
      "lineitem,.*range.*l_shipinstruct,.*l_shipmode.*remaining.*l_quantity.*l_quantity.*l_quantity");
  expectRegexp(veloxString, "part.*p_size.*p_container");
}
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
