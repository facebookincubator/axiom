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
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "axiom/sql/presto/PrestoParser.h"

namespace facebook::axiom::optimizer::test {

class HiveQueriesTestBase : public test::QueryTestBase {
 protected:
  static void SetUpTestCase();

  /// Creates TPC-H tables in a temp directory using PARQUET file format.
  void SetUp() override;

  void TearDown() override;

  static void TearDownTestCase();

  /// Returns a schema of a TPC-H table.
  velox::RowTypePtr getSchema(std::string_view tableName);

  void checkResults(
      std::string_view sql,
      const velox::core::PlanNodePtr& referencePlan);

  void checkResults(PlanAndStats& plan, const test::TestResult& expected);

  void checkSingleNodePlan(
      const PlanAndStats& plan,
      const std::shared_ptr<velox::core::PlanMatcher>& matcher);

  ::axiom::sql::presto::PrestoParser& prestoParser() {
    return *prestoParser_;
  }

 private:
  inline static std::shared_ptr<velox::exec::test::TempDirectoryPath>
      gTempDirectory;

  std::unique_ptr<::axiom::sql::presto::PrestoParser> prestoParser_;
};

} // namespace facebook::axiom::optimizer::test
