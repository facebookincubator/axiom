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

#include <functional>
#include <unordered_map>
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "axiom/sql/presto/PrestoParser.h"

namespace facebook::axiom::optimizer::test {

struct CheckerKey {
  int32_t queryNo;
  int32_t numWorkers;
  int32_t numDrivers;

  bool operator==(const CheckerKey& other) const {
    return queryNo == other.queryNo && numWorkers == other.numWorkers &&
        numDrivers == other.numDrivers;
  }
};

struct CheckerKeyHash {
  std::size_t operator()(const CheckerKey& key) const {
    // Combine hash values using a simple hash combination technique
    std::size_t h1 = std::hash<int32_t>{}(key.queryNo);
    std::size_t h2 = std::hash<int32_t>{}(key.numWorkers);
    std::size_t h3 = std::hash<int32_t>{}(key.numDrivers);
    return h1 ^ (h2 << 1) ^ (h3 << 2);
  }
};

class HiveQueriesTestBase : public test::QueryTestBase {
 protected:
  using PlanChecker = std::function<void(const PlanAndStats&)>;

  static void SetUpTestCase();

  /// Creates TPC-H tables in a temp directory using PARQUET file format.
  void SetUp() override;

  void TearDown() override;

  static void TearDownTestCase();

  /// Returns a schema of a TPC-H table.
  velox::RowTypePtr getSchema(std::string_view tableName);

  axiom::optimizer::PlanAndStats checkResults(
      std::string_view sql,
      const velox::core::PlanNodePtr& referencePlan);

  void checkResults(PlanAndStats& plan, const test::TestResult& expected);

  void checkSingleNodePlan(
      const PlanAndStats& plan,
      const std::shared_ptr<velox::core::PlanMatcher>& matcher);

  ::axiom::sql::presto::PrestoParser& prestoParser() {
    return *prestoParser_;
  }

  void explain(
      std::string_view sql,
      std::string* shortRel,
      std::string* longRel,
      std::string* graph,
      const runner::MultiFragmentPlan::Options& runnerOptions = {},
      const OptimizerOptions& optimizerOptions = {});

  void setChecker(
      int32_t queryNo,
      int32_t numWorkers,
      int32_t numDrivers,
      PlanChecker checker);

  PlanChecker*
  getChecker(int32_t queryNo, int32_t numWorkers, int32_t numDrivers);

 private:
  inline static std::shared_ptr<velox::exec::test::TempDirectoryPath>
      gTempDirectory;

  std::unique_ptr<::axiom::sql::presto::PrestoParser> prestoParser_;
  std::unordered_map<CheckerKey, PlanChecker, CheckerKeyHash> checkers_;
};

} // namespace facebook::axiom::optimizer::test
