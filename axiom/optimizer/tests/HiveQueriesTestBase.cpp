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
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/hive/HiveMetadataConfig.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/ConstantExprEvaluator.h"
#include "axiom/optimizer/tests/TpchDataGenerator.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"

namespace facebook::axiom::optimizer::test {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

const std::string kDefaultSchema{
    connector::hive::LocalHiveConnectorMetadata::kDefaultSchema};

// static
void HiveQueriesTestBase::SetUpTestCase() {
  test::QueryTestBase::SetUpTestCase();

  gTempDirectory = common::testutil::TempDirectoryPath::create();

  localDataPath_ = gTempDirectory->getPath();
  localFileFormat_ = velox::dwio::common::FileFormat::PARQUET;

  velox::parquet::registerParquetReaderFactory();
  velox::parquet::registerParquetWriterFactory();
}

// static
void HiveQueriesTestBase::TearDownTestCase() {
  velox::parquet::unregisterParquetWriterFactory();
  velox::parquet::unregisterParquetReaderFactory();

  gTempDirectory.reset();
  test::QueryTestBase::TearDownTestCase();
}

void HiveQueriesTestBase::SetUp() {
  test::QueryTestBase::SetUp();

  setupHiveConnector();

  prestoParser_ = std::make_unique<::axiom::sql::presto::PrestoParser>(
      exec::test::kHiveConnectorId,
      std::string(connector::hive::LocalHiveConnectorMetadata::kDefaultSchema));
}

// static
void HiveQueriesTestBase::createTpchTables(
    const std::vector<velox::tpch::Table>& tables) {
  VELOX_CHECK(gTempDirectory != nullptr, "SetUpTestCase not called");
  TpchDataGenerator::createTables(
      tables, gTempDirectory->getPath(), /*scaleFactor=*/0.1, localFileFormat_);
}

void HiveQueriesTestBase::TearDown() {
  hiveMetadata_ = nullptr;
  connector::ConnectorMetadataRegistry::global().erase(
      velox::exec::test::kHiveConnectorId);

  test::QueryTestBase::TearDown();
}

void HiveQueriesTestBase::setupHiveConnector() {
  std::unordered_map<std::string, std::string> configs;
  configs[connector::hive::HiveMetadataConfig::kLocalDataPath] = localDataPath_;
  configs[connector::hive::HiveMetadataConfig::kLocalFileFormat] =
      velox::dwio::common::toString(localFileFormat_);
  configs.insert(hiveConfig_.begin(), hiveConfig_.end());

  resetHiveConnector(
      std::make_shared<velox::config::ConfigBase>(std::move(configs)));

  auto hiveConnector = dynamic_cast<velox::connector::hive::HiveConnector*>(
      velox::connector::getConnector(velox::exec::test::kHiveConnectorId)
          .get());

  auto metadata = std::make_shared<connector::hive::LocalHiveConnectorMetadata>(
      hiveConnector);
  hiveMetadata_ = metadata.get();

  connector::ConnectorMetadataRegistry::global().insert(
      velox::exec::test::kHiveConnectorId, std::move(metadata));
}

RowTypePtr HiveQueriesTestBase::getSchema(std::string_view tableName) {
  return hiveMetadata()
      .findTable({kDefaultSchema, std::string(tableName)})
      ->type();
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

lp::LogicalPlanNodePtr HiveQueriesTestBase::parseInsert(std::string_view sql) {
  auto statement = prestoParser_->parse(sql);

  VELOX_CHECK(statement->isInsert());
  return statement->as<::axiom::sql::presto::InsertStatement>()->plan();
}

void HiveQueriesTestBase::createEmptyTable(
    const std::string& name,
    const RowTypePtr& tableType,
    const folly::F14FastMap<std::string, velox::Variant>& options) {
  hiveMetadata().dropTableIfExists({kDefaultSchema, name});

  auto session = std::make_shared<connector::ConnectorSession>("test");
  auto table = hiveMetadata().createTable(
      session,
      {kDefaultSchema, name},
      tableType,
      options,
      /*ifNotExists=*/false,
      /*explain=*/false);
  VELOX_CHECK_NOT_NULL(table);
  auto handle = hiveMetadata().beginWrite(
      session, table, connector::WriteKind::kCreate, /*explain=*/false);
  hiveMetadata().finishWrite(session, handle, {}, nullptr, {}).get();
}

void HiveQueriesTestBase::checkTableData(
    const std::string& tableName,
    const std::vector<RowVectorPtr>& expectedData) {
  lp::PlanBuilder::Context context(
      exec::test::kHiveConnectorId, kDefaultSchema);
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
  hiveMetadata().createTable(
      session,
      {kDefaultSchema, tableName},
      tableType,
      options,
      /*ifNotExists=*/false,
      /*explain=*/false);

  auto tablePath = hiveMetadata().tablePath({kDefaultSchema, tableName});
  for (const auto& filePath : filePaths) {
    auto fileName = std::filesystem::path(filePath).filename().string();
    std::string targetFilePath = fmt::format("{}/{}", tablePath, fileName);
    std::filesystem::copy_file(
        filePath,
        targetFilePath,
        std::filesystem::copy_options::overwrite_existing);
  }

  hiveMetadata().reloadTableFromPath({kDefaultSchema, tableName});
}

void HiveQueriesTestBase::runCtas(const std::string& sql) {
  auto statement = prestoParser_->parse(sql);
  VELOX_CHECK(statement->isCreateTableAsSelect());

  auto ctasStatement =
      statement->as<::axiom::sql::presto::CreateTableAsSelectStatement>();

  hiveMetadata().dropTableIfExists(ctasStatement->tableName());

  folly::F14FastMap<std::string, Variant> options;
  for (const auto& [key, value] : ctasStatement->properties()) {
    options[key] = ConstantExprEvaluator::evaluateConstantExpr(*value);
  }

  auto session = std::make_shared<connector::ConnectorSession>("test");
  auto table = hiveMetadata().createTable(
      session,
      ctasStatement->tableName(),
      ctasStatement->tableSchema(),
      options,
      /*ifNotExists=*/false,
      /*explain=*/false);
  VELOX_CHECK_NOT_NULL(table);

  connector::SchemaResolver schemaResolver;
  schemaResolver.setTargetTable(
      ctasStatement->connectorId(), ctasStatement->tableName(), table);

  auto plan = planVelox(ctasStatement->plan(), schemaResolver);
  runFragmentedPlan(plan);
}

} // namespace facebook::axiom::optimizer::test
