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

#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;

class ExchangeSerdeTest : public test::QueryTestBase {
 protected:
  void SetUp() override {
    test::QueryTestBase::SetUp();
    testConnector_->addTable("t", ROW({"c", "v"}, {VARCHAR(), BIGINT()}));
  }

  // Plans 'sql' with two workers and the requested exchange settings.
  PlanAndStats planWithThreshold(
      std::string_view sql,
      int32_t minChannels,
      bool remoteOutput) {
    const auto optimizerOptions = OptimizerOptions::from({{
        std::string(OptimizerOptions::kMinColumnarChannelsForCompactRow),
        std::to_string(minChannels),
    }});
    return planVelox(
        parseSelect(sql, kTestConnectorId),
        {
            .maxRemotePartitions = 2,
            .maxLocalPartitions = 2,
            .remoteOutput = remoteOutput,
        },
        optimizerOptions);
  }
};

// The threshold selects zero, one, or two CompactRow exchanges. A
// client-facing result always uses Presto.
TEST_F(ExchangeSerdeTest, selection) {
  struct TestCase {
    int32_t minChannels;
    std::string aggregationSerde;
    std::string gatherSerde;
  };

  const std::vector<TestCase> cases = {
      {6, "Presto", "Presto"},
      {5, "CompactRow", "Presto"},
      {2, "CompactRow", "CompactRow"},
  };
  constexpr std::string_view kSql =
      "SELECT sum(subtotal) "
      "FROM (SELECT c, sum(v) AS subtotal FROM t GROUP BY c)";

  for (const auto& testCase : cases) {
    SCOPED_TRACE(testCase.minChannels);
    for (const bool remoteOutput : {false, true}) {
      SCOPED_TRACE(remoteOutput ? "remote output" : "local output");
      const auto plan =
          planWithThreshold(kSql, testCase.minChannels, remoteOutput);
      auto matcher = matchScan("t")
                         .partialAggregation({"c"}, {"sum(v) as subtotal"})
                         .shuffle({"c"}, testCase.aggregationSerde)
                         .localPartition({"c"})
                         .finalAggregation({"c"}, {"sum(subtotal) as subtotal"})
                         .partialAggregation({}, {"sum(subtotal) as total"})
                         .gather(testCase.gatherSerde)
                         .localGather()
                         .finalAggregation({}, {"sum(total)"})
                         .partitionedOutputSingleIf(remoteOutput, "Presto");
      AXIOM_ASSERT_DISTRIBUTED_PLAN(plan.plan, matcher.build());
    }
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
