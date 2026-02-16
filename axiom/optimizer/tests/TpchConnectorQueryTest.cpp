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
#include <gtest/gtest.h>

#include "axiom/optimizer/tests/QueryTestBase.h"
#include "axiom/optimizer/tests/TpchDataGenerator.h"
#include "axiom/sql/presto/PrestoParser.h"

namespace facebook::axiom::optimizer::test {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class TpchConnectorQueryTest : public QueryTestBase {
 protected:
  static constexpr auto kTpchConnectorId = "tpch";

  void SetUp() override {
    QueryTestBase::SetUp();
    TpchDataGenerator::registerTpchConnector(kTpchConnectorId);
  }

  void TearDown() override {
    TpchDataGenerator::unregisterTpchConnector(kTpchConnectorId);
    QueryTestBase::TearDown();
  }

  lp::LogicalPlanNodePtr parseSql(std::string_view sql) {
    ::axiom::sql::presto::PrestoParser parser(kTpchConnectorId, std::nullopt);
    auto statement = parser.parse(sql);

    VELOX_CHECK(statement->isSelect());
    return statement->as<::axiom::sql::presto::SelectStatement>()->plan();
  }

  // Run "SELECT count(*) FROM <table>" query and return the result.
  int64_t runCountStar(std::string_view tableName) {
    const auto query = fmt::format("SELECT count(*) FROM {}", tableName);

    auto logicalPlan = parseSql(query);
    auto result = runVelox(logicalPlan);
    VELOX_CHECK_EQ(result.results.size(), 1);

    auto rowVector = result.results.at(0);
    VELOX_CHECK_EQ(rowVector->size(), 1);

    const auto value = rowVector->childAt(0)->variantAt(0);
    VELOX_CHECK(!value.isNull());

    return value.value<int64_t>();
  }
};

TEST_F(TpchConnectorQueryTest, lambda) {
  auto query =
      "SELECT n_regionkey, transform(array_agg(n_nationkey), x -> x * 2) FROM nation "
      "WHERE all_match(sequence(1, n_nationkey), x -> x < 3) "
      "GROUP BY 1";

  auto expected = makeRowVector({
      makeFlatVector<int64_t>({0, 1}),
      makeArrayVectorFromJson<int64_t>({"[0]", "[2, 4]"}),
  });
  checkSame(parseSql(query), {expected});
}

TEST_F(TpchConnectorQueryTest, scaleFactors) {
  ASSERT_EQ(15'000, runCountStar("orders"));
  ASSERT_EQ(15'000, runCountStar("tiny.orders"));
  ASSERT_EQ(15'000, runCountStar("tpch.tiny.orders"));

  ASSERT_EQ(1'500'000, runCountStar("sf1.orders"));
  ASSERT_EQ(1'500'000, runCountStar("tpch.sf1.orders"));
}

} // namespace
} // namespace facebook::axiom::optimizer::test
