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
#include <fstream>
#include <functional>
#include <sstream>
#include "axiom/connectors/hive/LocalHiveConnectorMetadata.h"
#include "axiom/logical_plan/ExprApi.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "axiom/optimizer/tests/PlanMatcherGenerator.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"

DECLARE_string(record_plans);

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class TpchPlanStability : public virtual test::HiveQueriesTestBase {
 protected:
  static void SetUpTestCase() {
    test::HiveQueriesTestBase::SetUpTestCase();
    gSuiteHistory = std::make_unique<optimizer::VeloxHistory>();
    keepHistoryBetweenTests_ = true;
    // Initialize VeloxHistory from tpch30samples.json
    suiteHistory().updateFromFile("tpch30samples.json");
  }

  static void TearDownTestCase() {
    test::HiveQueriesTestBase::TearDownTestCase();
  }

  void SetUp() override {
    HiveQueriesTestBase::SetUp();

    // Load column stats from tpch30stats.json
    auto* connectorMetadata =
        connector::ConnectorMetadata::metadata(exec::test::kHiveConnectorId);
    auto* hiveMetadata =
        dynamic_cast<connector::hive::LocalHiveConnectorMetadata*>(
            connectorMetadata);
    VELOX_CHECK_NOT_NULL(hiveMetadata);
    hiveMetadata->loadColumnStats("tpch30stats.json");
  }

  void TearDown() override {
    HiveQueriesTestBase::TearDown();
  }

  void recordPlanCheckerStart(int32_t queryNo) {
    if (FLAGS_record_plans.empty()) {
      return;
    }

    auto filename = fmt::format("{}_check_{}.inc", FLAGS_record_plans, queryNo);
    std::ofstream file(filename);

    if (!file.is_open()) {
      LOG(ERROR) << "Failed to open file: " << filename;
      return;
    }

    file << "void " << FLAGS_record_plans << "DefineCheckers" << queryNo
         << "() {\n";
    file.close();
  }

  void recordPlanCheckerEnd(int32_t queryNo) {
    if (FLAGS_record_plans.empty()) {
      return;
    }

    auto filename = fmt::format("{}_check_{}.inc", FLAGS_record_plans, queryNo);
    std::ofstream file(filename, std::ios::app);

    if (!file.is_open()) {
      LOG(ERROR) << "Failed to open file: " << filename;
      return;
    }

    file << "}\n";
    file.close();
  }

  void recordPlanChecker(
      int32_t queryNo,
      const lp::LogicalPlanNodePtr& logicalPlan,
      const PlanAndStats& planAndStats,
      int32_t numWorkers,
      int32_t numDrivers) {
    if (FLAGS_record_plans.empty()) {
      return;
    }

    auto filename = fmt::format("{}_check_{}.inc", FLAGS_record_plans, queryNo);
    std::ofstream file(filename, std::ios::app);

    if (!file.is_open()) {
      LOG(ERROR) << "Failed to open file: " << filename;
      return;
    }

    // Get the short RelationOp representation
    std::string shortRel;
    QueryTestBase::explain(
        logicalPlan,
        &shortRel,
        nullptr,
        nullptr,
        runner::MultiFragmentPlan::Options{
            .numWorkers = numWorkers, .numDrivers = numDrivers});

    file << "// Configuration: numWorkers=" << numWorkers
         << ", numDrivers=" << numDrivers << "\n";
    file << "setChecker(" << queryNo << ", " << numWorkers << ", " << numDrivers
         << ", [](const PlanAndStats& planAndStats) {\n";

    // Add the plan as a comment for readability
    file << "  // Plan:\n";
    std::istringstream planStream(shortRel);
    std::string line;
    while (std::getline(planStream, line)) {
      file << "  // " << line << "\n";
    }
    file << "\n";

    const auto& fragments = planAndStats.plan->fragments();
    for (size_t i = 0; i < fragments.size(); ++i) {
      const auto& fragment = fragments[i];
      const auto& topNode = fragment.fragment.planNode;

      file << "  // Fragment " << i << "\n";
      file << "  {\n";
      file << "    " << velox::core::generatePlanMatcherCode(topNode, "matcher")
           << ";\n";
      file << "    EXPECT_TRUE(matcher->match(planAndStats.plan->fragments()["
           << i << "].fragment.planNode));\n";
      file << "  }\n";
    }

    file << "});\n\n";
    file.close();
  }

  static std::string readSqlFromFile(const std::string& filePath) {
    auto path = velox::test::getDataFilePath("axiom/optimizer/tests", filePath);
    std::ifstream inputFile(path, std::ifstream::binary);

    VELOX_CHECK(inputFile, "Failed to open SQL file: {}", path);

    // Find out file size.
    auto begin = inputFile.tellg();
    inputFile.seekg(0, std::ios::end);
    auto end = inputFile.tellg();

    const auto fileSize = end - begin;
    VELOX_CHECK_GT(fileSize, 0, "SQL file is empty: {}", path);

    // Read the file.
    std::string sql;
    sql.resize(fileSize);

    inputFile.seekg(begin);
    inputFile.read(sql.data(), fileSize);
    inputFile.close();

    return sql;
  }

  std::string readTpchSql(int32_t query) {
    return readSqlFromFile(fmt::format("tpch.queries/q{}.sql", query));
  }

  lp::LogicalPlanNodePtr parseTpchSql(int32_t query) {
    auto sql = readTpchSql(query);

    ::axiom::sql::presto::PrestoParser prestoParser(
        exec::test::kHiveConnectorId, pool());
    auto statement = prestoParser.parse(sql);

    VELOX_CHECK(statement->isSelect());

    auto logicalPlan =
        statement->as<::axiom::sql::presto::SelectStatement>()->plan();
    VELOX_CHECK_NOT_NULL(logicalPlan);

    return logicalPlan;
  }

  void checkPlan(int32_t query) {
    // Configuration: 1 worker, 4 drivers
    const int32_t numWorkers = 1;
    const int32_t numDrivers = 4;

    // Parse the SQL query
    auto logicalPlan = parseTpchSql(query);

    // Generate the plan
    auto planAndStats = planVelox(
        logicalPlan,
        runner::MultiFragmentPlan::Options{
            .numWorkers = numWorkers, .numDrivers = numDrivers});

    // If FLAGS_record_plans is set, record the plan
    if (!FLAGS_record_plans.empty()) {
      recordPlanCheckerStart(query);
      recordPlanChecker(
          query, logicalPlan, planAndStats, numWorkers, numDrivers);
      recordPlanCheckerEnd(query);
    } else {
      // If not recording, check the plan if a checker exists
      auto* checker = getChecker(query, numWorkers, numDrivers);
      if (checker) {
        (*checker)(planAndStats);
      }
    }

    // Plans are not run
  }

#include "h30_check_1.inc"
#include "h30_check_10.inc"
#include "h30_check_11.inc"
#include "h30_check_12.inc"
#include "h30_check_13.inc"
#include "h30_check_14.inc"
#include "h30_check_15.inc"
#include "h30_check_16.inc"
#include "h30_check_17.inc"
#include "h30_check_18.inc"
#include "h30_check_19.inc"
#include "h30_check_2.inc"
#include "h30_check_20.inc"
#include "h30_check_21.inc"
#include "h30_check_22.inc"
#include "h30_check_3.inc"
#include "h30_check_4.inc"
#include "h30_check_5.inc"
#include "h30_check_6.inc"
#include "h30_check_7.inc"
#include "h30_check_8.inc"
#include "h30_check_9.inc"
};

TEST_F(TpchPlanStability, q01) {
  h30DefineCheckers1();
  checkPlan(1);
}

TEST_F(TpchPlanStability, q02) {
  h30DefineCheckers2();
  checkPlan(2);
}

TEST_F(TpchPlanStability, q03) {
  h30DefineCheckers3();
  checkPlan(3);
}

TEST_F(TpchPlanStability, q04) {
  h30DefineCheckers4();
  checkPlan(4);
}

TEST_F(TpchPlanStability, q05) {
  h30DefineCheckers5();
  checkPlan(5);
}

TEST_F(TpchPlanStability, q06) {
  h30DefineCheckers6();
  checkPlan(6);
}

TEST_F(TpchPlanStability, q07) {
  h30DefineCheckers7();
  checkPlan(7);
}

TEST_F(TpchPlanStability, q08) {
  h30DefineCheckers8();
  checkPlan(8);
}

TEST_F(TpchPlanStability, q09) {
  h30DefineCheckers9();
  checkPlan(9);
}

TEST_F(TpchPlanStability, q10) {
  h30DefineCheckers10();
  checkPlan(10);
}

TEST_F(TpchPlanStability, q11) {
  h30DefineCheckers11();
  checkPlan(11);
}

TEST_F(TpchPlanStability, q12) {
  h30DefineCheckers12();
  checkPlan(12);
}

TEST_F(TpchPlanStability, q13) {
  h30DefineCheckers13();
  checkPlan(13);
}

TEST_F(TpchPlanStability, q14) {
  h30DefineCheckers14();
  checkPlan(14);
}

TEST_F(TpchPlanStability, q15) {
  h30DefineCheckers15();
  checkPlan(15);
}

TEST_F(TpchPlanStability, q16) {
  h30DefineCheckers16();
  checkPlan(16);
}

TEST_F(TpchPlanStability, q17) {
  h30DefineCheckers17();
  checkPlan(17);
}

TEST_F(TpchPlanStability, q18) {
  h30DefineCheckers18();
  checkPlan(18);
}

TEST_F(TpchPlanStability, q19) {
  h30DefineCheckers19();
  checkPlan(19);
}

TEST_F(TpchPlanStability, q20) {
  h30DefineCheckers20();
  checkPlan(20);
}

TEST_F(TpchPlanStability, q21) {
  h30DefineCheckers21();
  checkPlan(21);
}

TEST_F(TpchPlanStability, q22) {
  h30DefineCheckers22();
  checkPlan(22);
}

} // namespace

std::string tryParse(char* text) {
  std::string str = text;
  try {
    auto expr = parse::parseExpr(str, {});
    return expr->toString();
  } catch (const std::exception& e) {
    printf("Error %s\n", e.what());
    return "ERROR";
  }
}

} // namespace facebook::axiom::optimizer
