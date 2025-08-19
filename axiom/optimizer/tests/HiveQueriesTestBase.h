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

#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QuerySqlParser.h"
#include "axiom/optimizer/tests/QueryTestBase.h"

namespace facebook::velox::optimizer::test {

class HiveQueriesTestBase : public test::QueryTestBase {
 protected:
  static void SetUpTestCase();

  void SetUp() override {
    test::QueryTestBase::SetUp();
    parser_ = makeQueryParser();
  }

  static void TearDownTestCase() {
    LocalRunnerTestBase::TearDownTestCase();
  }

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

  QuerySqlParser& parser() {
    return *parser_;
  }

 private:
  std::unique_ptr<QuerySqlParser> makeQueryParser();

  std::unique_ptr<QuerySqlParser> parser_;
};

} // namespace facebook::velox::optimizer::test
