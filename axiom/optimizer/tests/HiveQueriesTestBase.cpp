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

#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/ParquetTpchTest.h"

namespace facebook::axiom::optimizer::test {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

// static
void HiveQueriesTestBase::SetUpTestCase() {
  test::QueryTestBase::SetUpTestCase();

  gTempDirectory = exec::test::TempDirectoryPath::create();
  test::ParquetTpchTest::createTables(gTempDirectory->getPath());

  LocalRunnerTestBase::localDataPath_ = gTempDirectory->getPath();
  LocalRunnerTestBase::localFileFormat_ =
      velox::dwio::common::FileFormat::PARQUET;
}

// static
void HiveQueriesTestBase::TearDownTestCase() {
  gTempDirectory.reset();
  test::QueryTestBase::TearDownTestCase();
}

void HiveQueriesTestBase::SetUp() {
  test::QueryTestBase::SetUp();

  prestoParser_ = std::make_unique<::axiom::sql::presto::PrestoParser>(
      exec::test::kHiveConnectorId, std::nullopt, pool());

  connector_ = velox::connector::getConnector(exec::test::kHiveConnectorId);
  metadata_ = dynamic_cast<connector::hive::LocalHiveConnectorMetadata*>(
      connector::ConnectorMetadata::metadata(exec::test::kHiveConnectorId));
}

void HiveQueriesTestBase::TearDown() {
  metadata_ = nullptr;
  connector_.reset();

  test::QueryTestBase::TearDown();
}

RowTypePtr HiveQueriesTestBase::getSchema(std::string_view tableName) {
  return metadata_->findTable(tableName)->type();
}

velox::core::PlanNodePtr HiveQueriesTestBase::toSingleNodePlan(
    std::string_view sql,
    int32_t numDrivers) {
  auto logicalPlan = parseSelect(sql);

  auto plan =
      planVelox(logicalPlan, {.numWorkers = 1, .numDrivers = numDrivers}).plan;

  EXPECT_EQ(1, plan->fragments().size());
  return plan->fragments().at(0).fragment.planNode;
}

void HiveQueriesTestBase::checkResults(
    std::string_view sql,
    const core::PlanNodePtr& referencePlan) {
  SCOPED_TRACE(sql);
  VELOX_CHECK_NOT_NULL(referencePlan);

  auto logicalPlan = parseSelect(sql);
  checkSame(logicalPlan, referencePlan);
}

void HiveQueriesTestBase::checkResults(
    PlanAndStats& plan,
    const test::TestResult& expected) {
  auto results = runFragmentedPlan(plan);
  exec::test::assertEqualResults(expected.results, results.results);
}

void HiveQueriesTestBase::checkSingleNodePlan(
    const PlanAndStats& plan,
    const std::shared_ptr<core::PlanMatcher>& matcher) {
  SCOPED_TRACE(plan.plan->toString());

  const auto& fragments = plan.plan->fragments();
  ASSERT_EQ(1, fragments.size());

  ASSERT_TRUE(matcher->match(fragments.at(0).fragment.planNode));
}

lp::LogicalPlanNodePtr HiveQueriesTestBase::parseSelect(std::string_view sql) {
  auto statement = prestoParser_->parse(sql);

  VELOX_CHECK(statement->isSelect());
  return statement->as<::axiom::sql::presto::SelectStatement>()->plan();
}

lp::LogicalPlanNodePtr HiveQueriesTestBase::parseInsert(std::string_view sql) {
  auto statement = prestoParser_->parse(sql);

  VELOX_CHECK(statement->isInsert());
  return statement->as<::axiom::sql::presto::InsertStatement>()->plan();
}

void HiveQueriesTestBase::createEmptyTable(
    const std::string& name,
    const RowTypePtr& tableType,
    const folly::F14FastMap<std::string, velox::Variant>& options) {
  metadata_->dropTableIfExists(name);

  auto session = std::make_shared<connector::ConnectorSession>("test");
  auto table = metadata_->createTable(session, name, tableType, options);
  auto handle =
      metadata_->beginWrite(session, table, connector::WriteKind::kCreate);
  metadata_->finishWrite(session, handle, {}).get();
}

void HiveQueriesTestBase::checkTableData(
    const std::string& tableName,
    const std::vector<RowVectorPtr>& expectedData) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto logicalPlan = lp::PlanBuilder(context).tableScan(tableName).build();

  checkSameSingleNode(logicalPlan, expectedData);
}

void HiveQueriesTestBase::createTableFromFiles(
    const std::string& tableName,
    const RowTypePtr& tableType,
    const std::vector<std::string>& filePaths,
    const folly::F14FastMap<std::string, velox::Variant>& options) {
  for (const auto& filePath : filePaths) {
    ASSERT_TRUE(std::filesystem::exists(filePath))
        << "File does not exist: " << filePath;
  }

  auto session = std::make_shared<connector::ConnectorSession>("test");
  metadata_->createTable(session, tableName, tableType, options);

  auto tablePath = metadata_->tablePath(tableName);
  for (const auto& filePath : filePaths) {
    auto fileName = std::filesystem::path(filePath).filename().string();
    std::string targetFilePath = fmt::format("{}/{}", tablePath, fileName);
    std::filesystem::copy_file(
        filePath,
        targetFilePath,
        std::filesystem::copy_options::overwrite_existing);
  }

  metadata_->reloadTableFromPath(tableName);
}

} // namespace facebook::axiom::optimizer::test
