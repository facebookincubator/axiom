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

DEFINE_int32(trace, 0, "Enable trace 1=retained plans, 2=abandoned, 3=both");

DEFINE_int32(num_repeats, 1, "Number of repeats for optimization timing");

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
        dwio::common::FileFormat::PARQUET);
    builder_->initialize(FLAGS_data_path);
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
      optimizerOptions_.traceFlags = FLAGS_trace;
      auto result = planVelox(plan, &planText, &errorText);
    }
    return fmt::format(
        "=== {} {}:\n{}\n",
        partitioned ? "Partitioned on PK" : "Not partitioned",
        ordered ? "sorted on PK" : "not sorted",
        planText);
  }

  std::unique_ptr<HashStringAllocator> allocator_;
  std::unique_ptr<QueryGraphContext> context_;
  std::unique_ptr<exec::test::TpchQueryBuilder> builder_;
  static inline bool registered;
};

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

TEST_F(PlanTest, q3) {
  auto q = builder_->getQueryPlan(3).plan;
  auto result = makePlan(q, true, true);
  std::cout << result;
  result = makePlan(q, true, false);
  std::cout << result;
}

TEST_F(PlanTest, q9) {
  auto q = builder_->getQueryPlan(9).plan;
  auto result = makePlan(q, true, true);
  std::cout << result;
  result = makePlan(q, true, false);
  std::cout << result;
}

TEST_F(PlanTest, q17) {
  auto q = builder_->getQueryPlan(17).plan;
  auto result = makePlan(q, true, true);
  std::cout << result;
  result = makePlan(q, true, false);
  std::cout << result;
}

void printPlan(core::PlanNode* plan, bool r, bool d) {
  std::cout << plan->toString(r, d) << std::endl;
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
