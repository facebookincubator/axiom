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

#pragma once

#include "axiom/optimizer/tests/DuckParser.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/PrestoParser.h"
#include "axiom/optimizer/tests/QueryTestBase.h"

namespace facebook::velox::optimizer::test {

class HiveQueriesTestBase : public test::QueryTestBase {
 protected:
  static void SetUpTestCase();

  static constexpr auto kTpchConnectorId = "tpch";

  void SetUp() override;

  void TearDown() override;

  static void TearDownTestCase();

  RowTypePtr getSchema(const std::string& tableName);

  void checkResults(
      const std::string& sql,
      const core::PlanNodePtr& referencePlan);

  void checkResults(
      const logical_plan::LogicalPlanNodePtr& logicalPlan,
      const core::PlanNodePtr& referencePlan);

  void checkResults(const PlanAndStats& plan, const test::TestResult& expected);

  void checkSingleNodePlan(
      const PlanAndStats& plan,
      const std::shared_ptr<core::PlanMatcher>& matcher);

  DuckParser& duckParser() {
    return *duckParser_;
  }

  PrestoParser& prestoParser() {
    return *prestoParser_;
  }

 protected:
  static std::shared_ptr<exec::test::TempDirectoryPath> tempDirectory_;

  std::unique_ptr<DuckParser> duckParser_;
  std::unique_ptr<PrestoParser> prestoParser_;
};

} // namespace facebook::velox::optimizer::test
