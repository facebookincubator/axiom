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

#include "axiom/optimizer/tests/ParquetTpchTest.h"
#include <folly/system/HardwareConcurrency.h>
#include "axiom/common/Session.h"
#include "axiom/connectors/SchemaResolver.h"
#include "axiom/connectors/hive/LocalHiveConnectorMetadata.h"
#include "axiom/connectors/tpch/TpchConnectorMetadata.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/ConstantExprEvaluator.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/QueryGraphContext.h"
#include "axiom/optimizer/VeloxHistory.h"
#include "axiom/runner/LocalRunner.h"
#include "axiom/sql/presto/PrestoParser.h"
#include "velox/common/config/Config.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

#include <folly/executors/CPUThreadPoolExecutor.h>

DEFINE_string(
    data_path,
    "",
    "Path to TPC-H data directory. If empty, the test creates a temp directory and deletes it on exit");
DEFINE_bool(create_dataset, true, "Creates the TPC-H tables");
DEFINE_double(tpch_scale, 0.1, "Scale factor");

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::axiom::optimizer::test {

namespace {
void doCreateTables(std::string_view path) {
  auto rootPool = memory::memoryManager()->addRootPool();
  auto pool = rootPool->addLeafChild("leaf");

  LOG(INFO) << "Creating TPC-H tables in " << path;

  for (const auto& table : tpch::tables) {
    const auto tableName = tpch::toTableName(table);
    const auto tableSchema = tpch::getTableSchema(table);

    int32_t numSplits = 1;
    if (tableName != "nation" && tableName != "region" &&
        FLAGS_tpch_scale > 1) {
      numSplits = std::min<int32_t>(FLAGS_tpch_scale, 200);
    }

    const auto tableDirectory = fmt::format("{}/{}", path, tableName);
    auto plan =
        PlanBuilder()
            .tpchTableScan(table, tableSchema->names(), FLAGS_tpch_scale)
            .startTableWriter()
            .outputDirectoryPath(tableDirectory)
            .fileFormat(dwio::common::FileFormat::PARQUET)
            .compressionKind(common::CompressionKind::CompressionKind_SNAPPY)
            .endTableWriter()
            .planNode();

    std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> splits;
    for (auto i = 0; i < numSplits; ++i) {
      splits.push_back(
          std::make_shared<velox::connector::tpch::TpchConnectorSplit>(
              std::string(PlanBuilder::kTpchDefaultConnectorId), numSplits, i));
    }

    const int32_t numDrivers =
        std::min<int32_t>(numSplits, folly::hardware_concurrency());

    LOG(INFO) << "Creating TPC-H table " << tableName
              << " scaleFactor=" << FLAGS_tpch_scale
              << " numSplits=" << numSplits << " numDrivers=" << numDrivers
              << " hw concurrency=" << folly::hardware_concurrency();
    auto rows = AssertQueryBuilder(plan)
                    .splits(std::move(splits))
                    .maxDrivers(numDrivers)
                    .copyResults(pool.get());
  }
}

void registerHiveConnector(const std::string& id) {
  auto emptyConfig = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{});

  velox::connector::hive::HiveConnectorFactory factory;
  velox::connector::registerConnector(factory.newConnector(id, emptyConfig));
}

} // namespace

//  static
void ParquetTpchTest::createTables(std::string_view path) {
  SCOPE_EXIT {
    velox::connector::unregisterConnector(
        std::string(PlanBuilder::kHiveDefaultConnectorId));
    unregisterTpchConnector(std::string(PlanBuilder::kTpchDefaultConnectorId));

    parquet::unregisterParquetWriterFactory();
  };

  filesystems::registerLocalFileSystem();
  dwio::common::registerFileSinks();

  parquet::registerParquetWriterFactory();

  auto emptyConfig = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{});

  velox::connector::hive::HiveConnectorFactory hiveConnectorFactory;
  auto hiveConnector = hiveConnectorFactory.newConnector(
      std::string(PlanBuilder::kHiveDefaultConnectorId), emptyConfig);
  velox::connector::registerConnector(std::move(hiveConnector));

  registerTpchConnector(std::string(PlanBuilder::kTpchDefaultConnectorId));

  doCreateTables(path);
}

// static
void ParquetTpchTest::registerTpchConnector(const std::string& id) {
  auto emptyConfig = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>{});

  velox::connector::tpch::TpchConnectorFactory factory;
  auto connector = factory.newConnector(id, emptyConfig);
  velox::connector::registerConnector(connector);

  connector::ConnectorMetadata::registerMetadata(
      id,
      std::make_shared<connector::tpch::TpchConnectorMetadata>(
          dynamic_cast<velox::connector::tpch::TpchConnector*>(
              connector.get())));
}

// static
void ParquetTpchTest::unregisterTpchConnector(const std::string& id) {
  connector::ConnectorMetadata::unregisterMetadata(id);
  velox::connector::unregisterConnector(id);
}

// static
void ParquetTpchTest::makeBucketedTables(std::string_view path) {
  SCOPE_EXIT {
    connector::ConnectorMetadata::unregisterMetadata(
        PlanBuilder::kHiveDefaultConnectorId);
    velox::connector::unregisterConnector(
        std::string(PlanBuilder::kHiveDefaultConnectorId));
    parquet::unregisterParquetReaderFactory();
    parquet::unregisterParquetWriterFactory();
  };

  filesystems::registerLocalFileSystem();
  dwio::common::registerFileSinks();

  parquet::registerParquetReaderFactory();
  parquet::registerParquetWriterFactory();

  // Register Hive connector with data path
  std::unordered_map<std::string, std::string> configs;
  configs[velox::connector::hive::HiveConfig::kLocalDataPath] =
      std::string(path);
  configs[velox::connector::hive::HiveConfig::kLocalFileFormat] =
      velox::dwio::common::toString(velox::dwio::common::FileFormat::PARQUET);

  auto hiveConfig = std::make_shared<config::ConfigBase>(std::move(configs));

  velox::connector::hive::HiveConnectorFactory hiveConnectorFactory;
  auto hiveConnector = hiveConnectorFactory.newConnector(
      std::string(PlanBuilder::kHiveDefaultConnectorId), hiveConfig);
  velox::connector::registerConnector(std::move(hiveConnector));

  // Register LocalHiveConnectorMetadata
  auto connector = velox::connector::getConnector(
      std::string(PlanBuilder::kHiveDefaultConnectorId));
  connector::ConnectorMetadata::registerMetadata(
      PlanBuilder::kHiveDefaultConnectorId,
      std::make_shared<connector::hive::LocalHiveConnectorMetadata>(
          dynamic_cast<velox::connector::hive::HiveConnector*>(
              connector.get())));

  auto connectorId = std::string(PlanBuilder::kHiveDefaultConnectorId);
  auto metadata = connector::ConnectorMetadata::metadata(connectorId);
  auto* hiveMetadata =
      dynamic_cast<connector::hive::LocalHiveConnectorMetadata*>(metadata);
  VELOX_CHECK_NOT_NULL(hiveMetadata, "Expected LocalHiveConnectorMetadata");

  // Create memory pools
  auto rootPool = memory::memoryManager()->addRootPool();
  auto pool = rootPool->addLeafChild("leaf");

  // Define tables and their bucket columns
  struct TableInfo {
    std::string name;
    std::string bucketColumn;
  };

  std::vector<TableInfo> tables = {
      {"orders", "o_orderkey"},
      {"lineitem", "l_orderkey"},
      {"partsupp", "ps_partkey"},
      {"part", "p_partkey"}};

  for (const auto& tableInfo : tables) {
    const std::string sourceTable = tableInfo.name;
    const std::string targetTable = sourceTable + "_b";
    const std::string bucketColumn = tableInfo.bucketColumn;

    LOG(INFO) << "Creating bucketed table " << targetTable << " from "
              << sourceTable << " bucketed on " << bucketColumn;

    // Get table schema to build explicit column lists
    auto tpchTable = tpch::fromTableName(sourceTable);
    auto tableSchema = tpch::getTableSchema(tpchTable);

    // Build column lists for CREATE TABLE and SELECT
    std::vector<std::string> createColumns;
    std::vector<std::string> selectColumns;

    for (size_t i = 0; i < tableSchema->size(); ++i) {
      const auto& columnName = tableSchema->nameOf(i);
      const auto& columnType = tableSchema->childAt(i);

      // Check if column name ends with "date"
      bool isDateColumn = columnName.size() >= 4 &&
          columnName.substr(columnName.size() - 4) == "date";

      if (isDateColumn) {
        // For date columns, use DATE type in CREATE and CAST in SELECT
        createColumns.push_back(fmt::format("{} date", columnName));
        selectColumns.push_back(
            fmt::format("cast({} as date) {}", columnName, columnName));
        // selectColumns.push_back(columnName);
      } else {
        // For non-date columns, use the original type name
        createColumns.push_back(
            fmt::format("{} {}", columnName, columnType->toString()));
        selectColumns.push_back(columnName);
      }
    }

    std::string createColumnList; // = fmt::format("({})",
                                  // fmt::join(createColumns, ", "));
    std::string selectColumnList =
        fmt::format("{}", fmt::join(selectColumns, ", "));

    // Create SQL statement
    const std::string sql = fmt::format(
        "CREATE TABLE {} {} WITH (bucket_count = 8, bucketed_by = ARRAY['{}']) AS SELECT {} FROM {}",
        targetTable,
        createColumnList,
        bucketColumn,
        selectColumnList,
        sourceTable);

    LOG(INFO) << "SQL: " << sql;

    // Parse SQL
    ::axiom::sql::presto::PrestoParser parser(
        connectorId, std::nullopt, pool.get());
    auto statement = parser.parse(sql);
    VELOX_CHECK(
        statement->isCreateTableAsSelect(),
        "Expected CREATE TABLE AS SELECT statement");

    auto ctasStatement =
        statement->as<::axiom::sql::presto::CreateTableAsSelectStatement>();

    // Create table
    auto session = std::make_shared<connector::ConnectorSession>("test");
    hiveMetadata->dropTableIfExists(targetTable);

    folly::F14FastMap<std::string, velox::Variant> tableOptions;
    for (const auto& [key, value] : ctasStatement->properties()) {
      tableOptions[key] =
          optimizer::ConstantExprEvaluator::evaluateConstantExpr(*value);
    }

    auto table = hiveMetadata->createTable(
        session, targetTable, ctasStatement->tableSchema(), tableOptions);

    // Set up schema resolver
    connector::SchemaResolver schemaResolver;
    schemaResolver.setTargetTable(connectorId, table);

    // Create optimizer context
    auto optimizerPool = rootPool->addLeafChild("optimizer");
    auto allocator =
        std::make_unique<velox::HashStringAllocator>(optimizerPool.get());
    auto context_graph =
        std::make_unique<optimizer::QueryGraphContext>(*allocator);
    optimizer::queryCtx() = context_graph.get();
    SCOPE_EXIT {
      optimizer::queryCtx() = nullptr;
    };

    // Register Presto functions
    optimizer::FunctionRegistry::registerPrestoFunctions();

    // Create executor and QueryCtx
    auto executor = std::make_unique<folly::CPUThreadPoolExecutor>(4);
    auto queryCtx = velox::core::QueryCtx::create(executor.get());

    // Create expression evaluator
    velox::exec::SimpleExpressionEvaluator evaluator(
        queryCtx.get(), optimizerPool.get());

    // Create session
    auto axiomSession =
        std::make_shared<axiom::Session>(fmt::format("create_{}", targetTable));

    // Create history object
    optimizer::VeloxHistory history;

    // Create optimization
    optimizer::OptimizerOptions optimizerOptions;
    runner::MultiFragmentPlan::Options runnerOptions;
    runnerOptions.numWorkers = 1;
    runnerOptions.numDrivers = 1;
    runnerOptions.queryId = fmt::format("create_{}", targetTable);

    optimizer::Optimization opt(
        axiomSession,
        *ctasStatement->plan(),
        schemaResolver,
        history,
        queryCtx,
        evaluator,
        optimizerOptions,
        runnerOptions);

    // Get optimized plan
    auto best = opt.bestPlan();
    auto planAndStats = opt.toVeloxPlan(best->op);

    // Execute the write plan using LocalRunner
    auto runner = std::make_shared<runner::LocalRunner>(
        planAndStats.plan,
        std::move(planAndStats.finishWrite),
        queryCtx,
        std::make_shared<runner::ConnectorSplitSourceFactory>(),
        optimizerPool);

    // Read results
    std::vector<RowVectorPtr> results;
    while (auto result = runner->next()) {
      results.push_back(result);
    }
    runner->waitForCompletion(50000);

    // Verify write completed
    VELOX_CHECK_EQ(1, results.size());
    VELOX_CHECK_EQ(1, results[0]->size());
    const auto& child = results[0]->childAt(0);
    VELOX_CHECK(child);
    VELOX_CHECK_EQ(1, child->size());
    const auto rowCount = child->as<SimpleVector<int64_t>>()->valueAt(0);

    LOG(INFO) << "Created bucketed table " << targetTable << " with "
              << rowCount << " rows";
  }
}

} // namespace facebook::axiom::optimizer::test
