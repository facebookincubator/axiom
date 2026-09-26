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

#include <fmt/format.h>

#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/expression/rpc/AsyncRPCFunctionRegistry.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;

class InferenceTest : public test::QueryTestBase {
 protected:
  void SetUp() override {
    test::QueryTestBase::SetUp();

    registerInference(
        "test_inference", "array(real)", {"varchar"}, /*deterministic=*/true);
    registerInference(
        "test_summarize", "varchar", {"varchar"}, /*deterministic=*/true);
    registerInference(
        "test_sample", "array(real)", {"varchar"}, /*deterministic=*/false);
  }

  void TearDown() override {
    exec::rpc::AsyncRPCFunctionRegistry::testingClear();
    test::QueryTestBase::TearDown();
  }

  // Registers an inference function with a single signature.
  static void registerInference(
      std::string_view name,
      std::string_view returnType,
      const std::vector<std::string>& argumentTypes,
      bool deterministic) {
    auto builder =
        exec::FunctionSignatureBuilder().returnType(std::string{returnType});
    for (const auto& argumentType : argumentTypes) {
      builder.argumentType(argumentType);
    }
    exec::rpc::AsyncRPCFunctionRegistry::Metadata metadata;
    metadata.deterministic = deterministic;
    exec::rpc::AsyncRPCFunctionRegistry::registerFunction(
        std::string{name},
        []() -> std::shared_ptr<exec::rpc::AsyncRPCFunction> {
          return nullptr;
        },
        {builder.build()},
        metadata);
  }

  velox::core::PlanNodePtr toSingleNodePlan(std::string_view sql) {
    return QueryTestBase::toSingleNodePlan(
        parseSelect(sql, kTestConnectorId), /*numDrivers=*/1);
  }
};

TEST_F(InferenceTest, singleCall) {
  auto plan = toSingleNodePlan(
      "SELECT test_inference(n_name) AS embedding FROM nation");

  auto matcher = matchScan("nation")
                     .inference("test_inference(n_name) as embedding")
                     .project({"embedding"})
                     .build();
  AXIOM_ASSERT_PLAN_V2(plan, matcher);
}

// A special form holds a call in any input every row reaches.
TEST_F(InferenceTest, underSpecialForm) {
  auto plan = toSingleNodePlan(
      "SELECT CAST(test_inference(n_name) AS ARRAY(DOUBLE)) AS e FROM nation");

  auto matcher = matchScan("nation")
                     .inference("test_inference(n_name) as e")
                     .project({"cast(e as double[])"})
                     .build();
  AXIOM_ASSERT_PLAN_V2(plan, matcher);

  plan = toSingleNodePlan(
      "SELECT n_name FROM nation "
      "WHERE n_nationkey > 10 AND test_inference(n_name)[1] > 0.5");

  matcher =
      matchScan("nation")
          .inference("test_inference(n_name) as e")
          .filter("n_nationkey > 10 AND cast(subscript(e, 1) as DOUBLE) > 0.5")
          .project({"n_name"})
          .build();
  AXIOM_ASSERT_PLAN_V2(plan, matcher);
}

// A branch a form may skip cannot hold a call, which becomes a node that runs
// for every row.
TEST_F(InferenceTest, underGuardedInput) {
  const std::vector<std::pair<std::string, std::string>> cases = {
      {
          "SELECT CASE WHEN n_nationkey > 10 THEN test_inference(n_name) END "
          "FROM nation",
          "SWITCH: test_inference(n_name)",
      },
      {
          "SELECT coalesce(ARRAY[1.0], "
          "       CAST(test_inference(n_name) AS ARRAY(DOUBLE))) "
          "FROM nation",
          "COALESCE: CAST(test_inference(n_name) AS ARRAY<DOUBLE>)",
      },
  };
  for (const auto& [sql, expected] : cases) {
    SCOPED_TRACE(sql);
    VELOX_ASSERT_USER_THROW(
        toSingleNodePlan(sql),
        fmt::format("Inference function is not supported under {}", expected));
  }
}

TEST_F(InferenceTest, nested) {
  auto plan = toSingleNodePlan(
      "SELECT test_inference(test_summarize(n_name)) AS e FROM nation");

  // One node per call, innermost first; each reads the one below it.
  auto matcher = matchScan("nation")
                     .inference("test_summarize(n_name) as summary")
                     .project({"summary"})
                     .inference("test_inference(summary) as embedding")
                     .project({"embedding"})
                     .build();
  AXIOM_ASSERT_PLAN_V2(plan, matcher);
}

// A call appearing more than once is evaluated once.
TEST_F(InferenceTest, repeatedCall) {
  auto plan = toSingleNodePlan(
      "SELECT test_inference(n_name)[1] AS a, test_inference(n_name)[2] AS b "
      "FROM nation");

  auto matcher = matchScan("nation")
                     .inference("test_inference(n_name) as e")
                     .project({"subscript(e, 1) as a", "subscript(e, 2) as b"})
                     .build();
  AXIOM_ASSERT_PLAN_V2(plan, matcher);
}

// A non-deterministic call is evaluated once per occurrence.
TEST_F(InferenceTest, repeatedNonDeterministicCall) {
  auto plan = toSingleNodePlan(
      "SELECT test_sample(n_name)[1] AS a, test_sample(n_name)[2] AS b "
      "FROM nation");

  auto matcher =
      matchScan("nation")
          .inference("test_sample(n_name) as e1")
          .inference("test_sample(n_name) as e2")
          .project({"subscript(e1, 1) as a", "subscript(e2, 2) as b"})
          .build();
  AXIOM_ASSERT_PLAN_V2(plan, matcher);
}

// The call is evaluated below the Window that partitions by it.
TEST_F(InferenceTest, inWindowPartitionKey) {
  auto plan = toSingleNodePlan(
      "SELECT rank() OVER (PARTITION BY test_inference(n_name)[1] "
      "ORDER BY n_name) FROM nation");

  auto matcher =
      matchScan("nation")
          .inference("test_inference(n_name) as e")
          .project({"n_name", "subscript(e, 1) as p"})
          .window({"rank() OVER (PARTITION BY p ORDER BY n_name) as r"})
          .project({"r"})
          .build();
  AXIOM_ASSERT_PLAN_V2(plan, matcher);
}

TEST_F(InferenceTest, inLambda) {
  VELOX_ASSERT_USER_THROW(
      toSingleNodePlan(
          "SELECT transform(ARRAY[n_name], x -> test_inference(x)) FROM nation"),
      "Inference function is not supported inside a lambda");
}

} // namespace
} // namespace facebook::axiom::optimizer
