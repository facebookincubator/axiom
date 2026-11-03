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

#include "axiom/connectors/hive/LocalHiveConnectorMetadata.h"
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/tests/TestConnectorContext.h"
#include "axiom/runner/tests/LocalRunnerTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/connectors/hive/HivePartitionFunction.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

#include <folly/coro/BlockingWait.h>
#include <folly/init/Init.h>
#include <gmock/gmock.h>
#include <filesystem>
#include <fstream>

using namespace facebook::velox;
using namespace facebook::axiom::connector;

namespace facebook::axiom::connector::hive {
namespace {

class LocalHiveConnectorMetadataTest
    : public runner::test::LocalRunnerTestBase {
 protected:
  static constexpr int32_t kNumFiles = 5;
  static constexpr int32_t kNumVectors = 5;
  static constexpr int32_t kRowsPerVector = 10000;

  const std::string kDefaultSchema{LocalHiveConnectorMetadata::kDefaultSchema};

  // Returns 'name' in the default schema.
  SchemaTableName qualifiedName(std::string name) const {
    return {kDefaultSchema, std::move(name)};
  }

  static void makeAscending(const RowVectorPtr& rows, int32_t& counter) {
    auto ints = rows->childAt(0)->as<FlatVector<int64_t>>();
    for (auto i = 0; i < ints->size(); ++i) {
      ints->set(i, counter + i);
    }
    counter += ints->size();
  }

  void SetUp() override {
    rowType_ = ROW("c0", BIGINT());

    int32_t counter = 0;
    makeTables({runner::test::TableSpec{
        .name = "t",
        .columns = rowType_,
        .rowsPerVector = kRowsPerVector,
        .numVectorsPerFile = kNumVectors,
        .numFiles = kNumFiles,
        .customizeData = [&counter](const RowVectorPtr& rows) {
          makeAscending(rows, counter);
        }}});

    runner::test::LocalRunnerTestBase::SetUp();
    auto metadataPtr =
        ConnectorMetadataRegistry::tryGet(velox::exec::test::kHiveConnectorId);
    VELOX_CHECK_NOT_NULL(
        metadataPtr,
        "ConnectorMetadata not registered: {}",
        velox::exec::test::kHiveConnectorId);
    metadata_ =
        std::dynamic_pointer_cast<LocalHiveConnectorMetadata>(metadataPtr);
    ASSERT_TRUE(metadata_ != nullptr);
  }

  void TearDown() override {
    metadata_.reset();
    runner::test::LocalRunnerTestBase::TearDown();
  }

  static const LocalHiveTableLayout* getLayout(const TablePtr& table) {
    auto& layouts = table->layouts();
    EXPECT_EQ(1, layouts.size());
    const auto* layout = layouts[0]->as<LocalHiveTableLayout>();
    EXPECT_TRUE(layout != nullptr);
    return layout;
  }

  std::string localTablePath(const SchemaTableName& tableName) const {
    return fmt::format(
        "{}/{}",
        metadata_->hiveMetadataConfig()->localDataPath(),
        tableName.table);
  }

  void compareTableLayout(
      const LocalHiveTableLayout& expected,
      const LocalHiveTableLayout& layout) {
    auto compare = [](const std::vector<const Column*>& v1,
                      const std::vector<const Column*>& v2) {
      return v1.size() == v2.size() &&
          std::equal(
                 v1.begin(),
                 v1.end(),
                 v2.begin(),
                 [](const Column* a, const Column* b) {
                   return (a->name() == b->name()) &&
                       (*a->type() == *b->type());
                 });
    };

    EXPECT_TRUE(compare(expected.columns(), layout.columns()));
    EXPECT_TRUE(
        compare(expected.partitionColumns(), layout.partitionColumns()));
    EXPECT_EQ(expected.numBuckets(), layout.numBuckets());
    EXPECT_TRUE(compare(expected.orderColumns(), layout.orderColumns()));
    EXPECT_TRUE(compare(
        expected.hivePartitionColumns(), layout.hivePartitionColumns()));
    EXPECT_EQ(expected.fileFormat(), layout.fileFormat());
  }

  void compareTableLayout(const TablePtr& expected, const TablePtr& table) {
    compareTableLayout(*getLayout(expected), *getLayout(table));
  }

  static ConnectorSessionPtr makeSession() {
    return std::make_shared<ConnectorSession>(
        /*queryId=*/"q-test",
        /*user=*/"u-test",
        Properties{},
        makeTestStatWriter());
  }

  /// Write the specified data to the table with a TableWrite operation. The
  /// 'kind' specifies the type of write, for which only kCreate (new table) and
  /// kInsert (existing table) writes are supported. 'format' specifies the
  /// storage format to write with. A write to a partitioned table reports one
  /// stats group per partition, as the engine's does (see
  /// ConnectorMetadata::finishWrite).
  void writeToTable(
      const TablePtr& table,
      const RowVectorPtr& values,
      WriteKind kind,
      dwio::common::FileFormat format) {
    std::string outputPath = localTablePath(table->name());
    auto groupingKeys = partitionGroups(table, values);
    auto session = makeSession();
    auto handle = metadata_->beginWrite(
        session,
        table,
        kind,
        /*explain=*/false);

    auto builder = exec::test::PlanBuilder().values({values});
    auto plan = builder.startTableWriter()
                    .outputDirectoryPath(outputPath)
                    .targetColumns(table->type())
                    .insertHandle(handle->veloxHandle())
                    .fileFormat(format)
                    .endTableWriter()
                    .planNode();
    auto result = exec::test::AssertQueryBuilder(plan).copyResults(pool());
    metadata_
        ->finishWrite(
            session,
            handle,
            std::vector<velox::RowVectorPtr>{result},
            groupingKeys,
            std::vector<std::vector<ColumnStatistics>>(
                groupingKeys == nullptr ? 0 : groupingKeys->size()))
        .get();
  }

  // Returns the stats groups of writing 'values' to 'table': for a partitioned
  // table, one row per partition with its keys and "$row_count". Returns
  // nullptr for an unpartitioned table.
  RowVectorPtr partitionGroups(
      const TablePtr& table,
      const RowVectorPtr& values) {
    std::vector<std::string> keys;
    for (const auto* column : getLayout(table)->hivePartitionColumns()) {
      keys.push_back(column->name());
    }
    if (keys.empty()) {
      return nullptr;
    }
    auto plan = exec::test::PlanBuilder()
                    .values({values})
                    .singleAggregation(keys, {"count(1) AS \"$row_count\""})
                    .planNode();
    return exec::test::AssertQueryBuilder(plan).copyResults(pool());
  }

  /// Read the specified files from the table. All the files must belong to
  /// the same partition, or the table must be unpartitioned. 'partitionKeys'
  /// provides the values of any partition keys for the files. 'format' provides
  /// the storage format of the specified files, which must all have the same
  /// format.
  velox::RowVectorPtr readFiles(
      const TablePtr& table,
      const std::vector<std::string>& files,
      const std::unordered_map<std::string, std::optional<std::string>>&
          partitionKeys,
      dwio::common::FileFormat format) {
    std::vector<std::shared_ptr<velox::connector::ConnectorSplit>> splits;
    splits.reserve(files.size());
    for (const auto& file : files) {
      auto hiveSplits = makeHiveConnectorSplits(
          file, /*splitCount=*/1, format, partitionKeys);
      EXPECT_EQ(hiveSplits.size(), 1);
      splits.push_back(std::move(hiveSplits.front()));
    }

    auto tableType = table->type();
    velox::connector::ColumnHandleMap assignments;
    const auto* layout = getLayout(table);
    for (auto i = 0; i < tableType->size(); ++i) {
      assignments[tableType->nameOf(i)] = layout->createColumnHandle(
          /*session=*/nullptr, tableType->nameOf(i));
    }
    auto plan = exec::test::PlanBuilder()
                    .tableScan(
                        tableType,
                        /*subfieldFilters=*/{},
                        /*remainingFilter=*/"",
                        /*dataColumns=*/nullptr,
                        assignments)
                    .planNode();
    return exec::test::AssertQueryBuilder(plan).splits(splits).copyResults(
        pool());
  }

  /// Compare the data present in the table given by 'tableName' against the
  /// provided data, failing the test if the data does not match.
  /// 'partitionKeys' specifies the value of any partition columns in the data
  /// to be read, for which all data must belong to the same partition. 'format'
  /// specifies the storage format of the table.
  void compareTableData(
      const std::string& tableName,
      const RowVectorPtr& expectedData,
      const std::unordered_map<std::string, std::optional<std::string>>&
          partitionKeys,
      dwio::common::FileFormat format) {
    std::string path = localTablePath(qualifiedName(tableName));
    auto files = getDataFiles(path);
    auto table = metadata_->findTable(qualifiedName(tableName));
    auto results = readFiles(table, files, partitionKeys, format);
    exec::test::assertEqualResults({expectedData}, {results});
  }

  static std::vector<std::string> getDataFiles(const std::string& path) {
    EXPECT_TRUE(std::filesystem::is_directory(path));
    std::vector<std::string> files;
    for (const auto& entry :
         std::filesystem::recursive_directory_iterator(path)) {
      auto file = entry.path().string();
      if (entry.is_regular_file() &&
          entry.path().filename().string()[0] != '.') {
        files.push_back(file);
      }
    }
    return files;
  }

  // Creates the vector index 'name' with 'columns' and 'tableProperties'.
  // Returns false if the index exists and 'ifNotExists' is true.
  bool createVectorIndex(
      const std::string& name,
      const RowTypePtr& columns,
      bool ifNotExists,
      bool explain,
      folly::F14FastMap<std::string, Variant> tableProperties = {
          {"partitioned_by", Variant::array({Variant("ds")})}}) {
    return folly::coro::blockingWait(metadata_->co_createVectorIndex(
        makeSession(),
        qualifiedName(name),
        columns,
        vectorIndexProperties(),
        std::move(tableProperties),
        ifNotExists,
        explain));
  }

  // Expects that the codec table and index table of the vector index 'name'
  // do not exist.
  void expectNoVectorIndexTables(const std::string& name) {
    EXPECT_EQ(metadata_->findTable(qualifiedName(name + "__codecs")), nullptr);
    EXPECT_EQ(metadata_->findTable(qualifiedName(name + "__index")), nullptr);
  }

  static VectorIndexProperties vectorIndexProperties() {
    return {
        .metric = common::vectorindex::VectorIndexMetric::kCosine,
        .indexKind = common::vectorindex::VectorIndexKind::kIvfFlat,
        .numClusters = 10,
        .codecPolicy = VectorIndexCodecPolicy::kShared,
        .indexOptions = {{"ivf.nlist", "4"}, {"ivf.nprobe", "2"}},
    };
  }

  static RowTypePtr vectorIndexColumns() {
    return ROW(
        {{"id", BIGINT()}, {"embedding", ARRAY(REAL())}, {"ds", VARCHAR()}});
  }

  static std::vector<std::string> partitionColumnNames(const TablePtr& table) {
    std::vector<std::string> names;
    for (const auto* column : getLayout(table)->hivePartitionColumns()) {
      names.push_back(column->name());
    }
    return names;
  }

  RowTypePtr rowType_;
  std::shared_ptr<LocalHiveConnectorMetadata> metadata_;
};

// An absent bucket count produces all-null statistics.
TEST(HiveTableLayoutTest, bucketStats) {
  for (const auto& [numRows, nullPct] :
       std::vector<std::pair<uint64_t, float>>{{0, 0}, {10, 100}}) {
    SCOPED_TRACE(numRows);
    const auto stats =
        HiveTableLayout::bucketColumnStats(std::nullopt, numRows);
    EXPECT_EQ(stats.name, HiveTable::kBucket);
    EXPECT_EQ(stats.nullPct, nullPct);
    EXPECT_EQ(stats.numDistinct, 0);
    EXPECT_EQ(stats.numValues, 0);
    EXPECT_FALSE(stats.nonNull);
    EXPECT_FALSE(stats.min.has_value());
    EXPECT_FALSE(stats.max.has_value());
  }
}

TEST_F(LocalHiveConnectorMetadataTest, basic) {
  auto table = metadata_->findTable(qualifiedName("t"));
  ASSERT_TRUE(table != nullptr);
  auto column = table->findColumn("c0");
  ASSERT_TRUE(column != nullptr);
  EXPECT_EQ(250'000, table->numRows());
  auto* layout = table->primaryLayout();
  auto columnHandle = layout->createColumnHandle(/*session=*/nullptr, "c0");
  std::vector<velox::connector::ColumnHandlePtr> columns = {columnHandle};
  std::vector<core::TypedExprPtr> filters;
  std::vector<int32_t> rejectedFilterIndices;
  auto ctx = metadata_->connectorQueryCtx();

  auto tableHandle = layout->createTableHandle(
      /*session=*/nullptr,
      columns,
      *ctx->expressionEvaluator(),
      filters,
      rejectedFilterIndices,
      /*dataColumns=*/nullptr,
      /*lookupKeys=*/{});
  EXPECT_TRUE(rejectedFilterIndices.empty());

  const auto deleteLayout =
      metadata_->deleteLayout(makeSession(), tableHandle, /*exact=*/true);
  EXPECT_FALSE(deleteLayout.hasWriter());
  EXPECT_TRUE(deleteLayout.shuffleKeys().empty());
  EXPECT_EQ(deleteLayout.partitionType(), nullptr);
  EXPECT_TRUE(deleteLayout.sortKeys().empty());
  EXPECT_TRUE(deleteLayout.sortOrders().empty());
  VELOX_ASSERT_USER_THROW(
      metadata_->deleteLayout(makeSession(), tableHandle, /*exact=*/false),
      "DELETE requires row-level support from the connector");

  std::vector<common::Subfield> fields;
  auto c0 = common::Subfield::create("c0");
  fields.push_back(std::move(*c0));
  HashStringAllocator allocator(pool_.get());
  std::vector<std::unique_ptr<StatisticsBuilder>> statsBuilders;
  const auto* localLayout = layout->as<LocalHiveTableLayout>();
  ASSERT_NE(localLayout, nullptr);
  auto pair =
      localLayout->sample(tableHandle, 100, fields, &allocator, &statsBuilders);
  EXPECT_EQ(250'000, pair.first);
  EXPECT_EQ(250'000, pair.second);
}

// Verifies that sample() only samples files matching $path filter.
TEST_F(LocalHiveConnectorMetadataTest, sampleWithPathFilter) {
  auto table = metadata_->findTable(qualifiedName("t"));
  ASSERT_TRUE(table != nullptr);
  const auto* layout = table->primaryLayout()->as<LocalHiveTableLayout>();
  ASSERT_TRUE(layout != nullptr);
  const auto& files = layout->files();
  ASSERT_GT(files.size(), 1);

  // Get the first file path for testing.
  const auto& targetFilePath = files[0]->path;

  auto columnHandle = layout->createColumnHandle(/*session=*/nullptr, "c0");
  std::vector<velox::connector::ColumnHandlePtr> columns = {columnHandle};

  auto filterRowType = ROW({{HiveTable::kPath, VARCHAR()}});
  auto pathFilterExpr = parseExpr(
      fmt::format("\"{}\" = '{}'", HiveTable::kPath, targetFilePath),
      filterRowType);
  std::vector<core::TypedExprPtr> filters = {pathFilterExpr};
  std::vector<int32_t> rejectedFilterIndices;
  auto ctx = metadata_->connectorQueryCtx();

  auto tableHandle = layout->createTableHandle(
      /*session=*/nullptr,
      columns,
      *ctx->expressionEvaluator(),
      filters,
      rejectedFilterIndices,
      /*dataColumns=*/nullptr,
      /*lookupKeys=*/{});
  EXPECT_TRUE(rejectedFilterIndices.empty());

  std::vector<common::Subfield> fields;
  auto c0 = common::Subfield::create("c0");
  fields.push_back(std::move(*c0));
  HashStringAllocator allocator(pool_.get());

  // sample() should only sample the file matching $path filter.
  // With 5 files and 50,000 rows per file, filtering to 1 file should
  // result in approximately 50,000 rows sampled (not 250,000).
  std::vector<std::unique_ptr<StatisticsBuilder>> statsBuilders;
  auto pair =
      layout->sample(tableHandle, 100, fields, &allocator, &statsBuilders);
  EXPECT_EQ(kRowsPerVector * kNumVectors, pair.first);
  EXPECT_EQ(kRowsPerVector * kNumVectors, pair.second);
}

TEST_F(LocalHiveConnectorMetadataTest, createTable) {
  auto tableType = ROW(
      {{"key1", BIGINT()},
       {"key2", INTEGER()},
       {"data", BIGINT()},
       {"ds", VARCHAR()}});

  folly::F14FastMap<std::string, velox::Variant> options = {
      {HiveWriteOptions::kBucketedBy, velox::Variant::array({"key1"})},
      {HiveWriteOptions::kBucketCount, 4LL},
      {HiveWriteOptions::kSortedBy,
       velox::Variant::array({"key1", "key2 DESC"})},
      {HiveWriteOptions::kPartitionedBy, velox::Variant::array({"ds"})},
      {HiveWriteOptions::kFormat, "parquet"},
      {HiveWriteOptions::kCompressionKind, "zstd"}};

  auto session = makeSession();
  auto table = folly::coro::blockingWait(metadata_->co_createTable(
      session,
      qualifiedName("test"),
      TableDefinition::create(tableType, options),
      /*ifNotExists=*/false,
      /*explain=*/false));

  // Table should be findable immediately after createTable, before
  // beginWrite/finishWrite. This is the flow for plain CREATE TABLE (no data).
  ASSERT_NE(metadata_->findTable(qualifiedName("test")), nullptr);

  constexpr int32_t kTestSize = 2048;
  auto data = makeRowVector(
      tableType->names(),
      {
          makeFlatVector<int64_t>(kTestSize, [](auto row) { return row; }),
          makeFlatVector<int32_t>(kTestSize, [](auto row) { return row % 10; }),
          makeFlatVector<int64_t>(kTestSize, [](auto row) { return row + 2; }),
          makeFlatVector<StringView>(
              kTestSize, [](auto /*row*/) { return "2022-09-01"; }),
      });
  EXPECT_EQ(data->size(), kTestSize);

  auto expected = getLayout(table);
  EXPECT_EQ(expected->partitionColumns().size(), 1);
  EXPECT_EQ(expected->partitionColumns()[0], expected->columns()[0]);
  EXPECT_EQ(expected->numBuckets().value(), 4);
  EXPECT_EQ(expected->orderColumns().size(), 2);
  EXPECT_EQ(expected->orderColumns()[0], expected->columns()[0]);
  EXPECT_EQ(expected->orderColumns()[1], expected->columns()[1]);
  ASSERT_EQ(expected->sortOrder().size(), 2);
  EXPECT_TRUE(expected->sortOrder()[0].isAscending);
  EXPECT_TRUE(expected->sortOrder()[0].isNullsFirst);
  EXPECT_FALSE(expected->sortOrder()[1].isAscending);
  EXPECT_FALSE(expected->sortOrder()[1].isNullsFirst);
  EXPECT_EQ(expected->hivePartitionColumns().size(), 1);
  EXPECT_EQ(expected->hivePartitionColumns()[0], expected->columns()[3]);
  EXPECT_TRUE(expected->hivePartitionColumns()[0]->includeInExplainIo());
  EXPECT_EQ(table->ioColumnPriority(), (std::vector<std::string>{"ds"}));
  // Non-partition columns should not be included in EXPLAIN IO.
  EXPECT_FALSE(expected->columns()[0]->includeInExplainIo());
  EXPECT_FALSE(expected->columns()[1]->includeInExplainIo());
  EXPECT_FALSE(expected->columns()[2]->includeInExplainIo());
  EXPECT_EQ(expected->fileFormat(), dwio::common::toFileFormat("parquet"));
  EXPECT_EQ(expected->table().options().at("compression_kind"), "zstd");

  writeToTable(
      table, data, WriteKind::kCreate, dwio::common::FileFormat::PARQUET);

  std::string tablePath = localTablePath(qualifiedName("test"));
  std::string partition = "2022-09-01";
  std::string path = fmt::format("{}/ds={}", tablePath, partition);
  auto files = getDataFiles(path);
  EXPECT_GT(files.size(), 0);

  auto numBuckets = 4;
  auto bucketFunction =
      std::make_unique<velox::connector::hive::HivePartitionFunction>(
          numBuckets, std::vector<velox::column_index_t>{0});
  std::unordered_set<int32_t> buckets;
  for (const auto& file : files) {
    // e.g. "/ds=2022-09-01/000000_0_TaskCursorQuery_0.parquet"
    auto pos = file.find(partition);
    ASSERT_NE(pos, std::string::npos);
    auto bucket = stoi(file.substr(pos + partition.size() + 1, 6));
    auto result = readFiles(
        table, {file}, {{"ds", partition}}, dwio::common::FileFormat::PARQUET);
    buckets.insert(bucket);

    std::vector<uint32_t> partitions;
    partitions.resize(result->size());
    bucketFunction->partition(*result, partitions);
    for (auto i = 0; i < result->size(); ++i) {
      EXPECT_EQ(partitions[i], bucket);
    }
  }
  for (auto i = 0; i < numBuckets; i++) {
    EXPECT_TRUE(buckets.contains(i));
  }

  compareTableLayout(table, metadata_->findTable(qualifiedName("test")));
  compareTableData(
      "test", data, {{"ds", partition}}, dwio::common::FileFormat::PARQUET);
}

TEST_F(LocalHiveConnectorMetadataTest, invalidSortedBy) {
  auto tableType = ROW({{"key1", BIGINT()}, {"data", BIGINT()}});

  auto create =
      [&](const folly::F14FastMap<std::string, velox::Variant>& options) {
        folly::coro::blockingWait(metadata_->co_createTable(
            makeSession(),
            qualifiedName("test"),
            TableDefinition::create(tableType, options),
            /*ifNotExists=*/false,
            /*explain=*/false));
      };

  VELOX_ASSERT_USER_THROW(
      create({{HiveWriteOptions::kSortedBy, velox::Variant::array({"key1"})}}),
      "sorted_by requires bucketed_by");

  VELOX_ASSERT_USER_THROW(
      create(
          {{HiveWriteOptions::kBucketedBy, velox::Variant::array({"key1"})},
           {HiveWriteOptions::kBucketCount, 4LL},
           {HiveWriteOptions::kSortedBy,
            velox::Variant::array({"key1 BACKWARDS"})}}),
      "Invalid sort direction: BACKWARDS");
}

TEST_F(LocalHiveConnectorMetadataTest, addColumn) {
  auto tableType =
      ROW({{"id", BIGINT()}, {"name", VARCHAR()}, {"ds", VARCHAR()}});

  folly::F14FastMap<std::string, velox::Variant> options = {
      {HiveWriteOptions::kPartitionedBy, velox::Variant::array({"ds"})},
      {HiveWriteOptions::kFormat, "parquet"}};

  auto session = makeSession();
  auto table = folly::coro::blockingWait(metadata_->co_createTable(
      session,
      qualifiedName("add_col_test"),
      TableDefinition::create(tableType, options),
      /*ifNotExists=*/false,
      /*explain=*/false));

  // Write some data before adding the column.
  auto data = makeRowVector(
      tableType->names(),
      {
          makeFlatVector<int64_t>(100, [](auto row) { return row; }),
          makeFlatVector<StringView>(100, [](auto /*row*/) { return "test"; }),
          makeFlatVector<StringView>(
              100, [](auto /*row*/) { return "2025-01-01"; }),
      });
  writeToTable(
      table, data, WriteKind::kCreate, dwio::common::FileFormat::PARQUET);

  // Add a column.
  auto added = folly::coro::blockingWait(metadata_->co_addColumn(
      session,
      qualifiedName("add_col_test"),
      {.name = "score", .type = DOUBLE()},
      /*ifTableExists=*/false,
      /*ifNotExists=*/false,
      /*explain=*/false));
  ASSERT_TRUE(added.has_value());
  EXPECT_TRUE(*added);

  // Verify schema has the new column.
  auto updated = metadata_->findTable(qualifiedName("add_col_test"));
  ASSERT_NE(updated, nullptr);
  VELOX_EXPECT_EQ_TYPES(
      updated->type(),
      ROW({"id", "name", "score", "ds"},
          {BIGINT(), VARCHAR(), DOUBLE(), VARCHAR()}));

  // Layout should exist (not empty).
  ASSERT_FALSE(updated->layouts().empty());

  // Partition columns should be preserved.
  const auto* layout = updated->primaryLayout()->as<LocalHiveTableLayout>();
  ASSERT_NE(layout, nullptr);
  EXPECT_EQ(1, layout->hivePartitionColumns().size());
  EXPECT_EQ("ds", layout->hivePartitionColumns()[0]->name());

  // Duplicate column without IF NOT EXISTS throws.
  VELOX_ASSERT_THROW(
      folly::coro::blockingWait(metadata_->co_addColumn(
          session,
          qualifiedName("add_col_test"),
          {.name = "score", .type = INTEGER()},
          /*ifTableExists=*/false,
          /*ifNotExists=*/false,
          /*explain=*/false)),
      "already exists");

  // IF NOT EXISTS on existing column is a no-op.
  auto noOp = folly::coro::blockingWait(metadata_->co_addColumn(
      session,
      qualifiedName("add_col_test"),
      {.name = "score", .type = INTEGER()},
      /*ifTableExists=*/false,
      /*ifNotExists=*/true,
      /*explain=*/false));
  ASSERT_TRUE(noOp.has_value());
  EXPECT_FALSE(*noOp);
  VELOX_EXPECT_EQ_TYPES(
      metadata_->findTable(qualifiedName("add_col_test"))->type(),
      ROW({"id", "name", "score", "ds"},
          {BIGINT(), VARCHAR(), DOUBLE(), VARCHAR()}));

  // Non-existent table throws.
  VELOX_ASSERT_THROW(
      folly::coro::blockingWait(metadata_->co_addColumn(
          session,
          qualifiedName("no_such_table"),
          {.name = "col", .type = BIGINT()},
          /*ifTableExists=*/false,
          /*ifNotExists=*/false,
          /*explain=*/false)),
      "does not exist");

  // IF TABLE EXISTS on missing table returns nullopt.
  auto missing = folly::coro::blockingWait(metadata_->co_addColumn(
      session,
      qualifiedName("no_such_table"),
      {.name = "col", .type = BIGINT()},
      /*ifTableExists=*/true,
      /*ifNotExists=*/false,
      /*explain=*/false));
  EXPECT_FALSE(missing.has_value());

  // Schema persists after reload.
  metadata_->reloadTableFromPath(qualifiedName("add_col_test"));
  auto reloaded = metadata_->findTable(qualifiedName("add_col_test"));
  ASSERT_NE(reloaded, nullptr);
  VELOX_EXPECT_EQ_TYPES(
      reloaded->type(),
      ROW({"id", "name", "score", "ds"},
          {BIGINT(), VARCHAR(), DOUBLE(), VARCHAR()}));
}

TEST_F(LocalHiveConnectorMetadataTest, addColumnExplain) {
  auto tableType = ROW({{"id", BIGINT()}, {"name", VARCHAR()}});

  folly::F14FastMap<std::string, velox::Variant> options = {
      {HiveWriteOptions::kFormat, "parquet"}};

  auto session = makeSession();
  folly::coro::blockingWait(metadata_->co_createTable(
      session,
      qualifiedName("explain_add_col_test"),
      TableDefinition::create(tableType, options),
      /*ifNotExists=*/false,
      /*explain=*/false));

  const auto tableName = qualifiedName("explain_add_col_test");

  auto originalType = metadata_->findTable(tableName)->type();

  // Explain mode for a valid add returns true and does not mutate the schema
  // in memory or on disk.
  auto wouldAdd = folly::coro::blockingWait(metadata_->co_addColumn(
      session,
      tableName,
      {.name = "score", .type = DOUBLE()},
      /*ifTableExists=*/false,
      /*ifNotExists=*/false,
      /*explain=*/true));
  ASSERT_TRUE(wouldAdd.has_value());
  EXPECT_TRUE(*wouldAdd);

  auto afterExplain = metadata_->findTable(tableName);
  ASSERT_NE(afterExplain, nullptr);
  EXPECT_EQ(*originalType, *afterExplain->type());

  // Reloading from disk also shows the original schema.
  metadata_->reloadTableFromPath(tableName);
  auto afterReload = metadata_->findTable(tableName);
  ASSERT_NE(afterReload, nullptr);
  EXPECT_EQ(*originalType, *afterReload->type());

  // Explain mode raises the same user error as a real call when a duplicate
  // column is added without IF NOT EXISTS.
  VELOX_ASSERT_THROW(
      folly::coro::blockingWait(metadata_->co_addColumn(
          session,
          tableName,
          {.name = "id", .type = INTEGER()},
          /*ifTableExists=*/false,
          /*ifNotExists=*/false,
          /*explain=*/true)),
      "already exists");

  // Explain mode honors IF NOT EXISTS for an existing column: returns false
  // (no-op) without mutating state.
  auto explainNoOp = folly::coro::blockingWait(metadata_->co_addColumn(
      session,
      tableName,
      {.name = "id", .type = INTEGER()},
      /*ifTableExists=*/false,
      /*ifNotExists=*/true,
      /*explain=*/true));
  ASSERT_TRUE(explainNoOp.has_value());
  EXPECT_FALSE(*explainNoOp);

  // Explain mode raises the same user error for a missing table when
  // IF EXISTS is not set.
  VELOX_ASSERT_THROW(
      folly::coro::blockingWait(metadata_->co_addColumn(
          session,
          qualifiedName("no_such_table"),
          {.name = "col", .type = BIGINT()},
          /*ifTableExists=*/false,
          /*ifNotExists=*/false,
          /*explain=*/true)),
      "does not exist");

  // Explain mode honors IF EXISTS on a missing table: returns nullopt
  // (no-op).
  auto explainMissing = folly::coro::blockingWait(metadata_->co_addColumn(
      session,
      qualifiedName("no_such_table"),
      {.name = "col", .type = BIGINT()},
      /*ifTableExists=*/true,
      /*ifNotExists=*/false,
      /*explain=*/true));
  EXPECT_FALSE(explainMissing.has_value());
}

// An empty data file produces no splits.
TEST_F(LocalHiveConnectorMetadataTest, emptyFile) {
  auto dir = velox::common::testutil::TempDirectoryPath::create();
  const FileInfo empty{.path = dir->getPath() + "/empty"};
  const FileInfo nonEmpty{.path = dir->getPath() + "/non_empty"};
  std::ofstream{empty.path};
  std::ofstream{nonEmpty.path} << "x";

  LocalHiveSplitSource source(
      {&empty, &nonEmpty},
      dwio::common::FileFormat::DWRF,
      "test",
      "t",
      /*serdeParameters=*/{},
      /*partitionType=*/nullptr);
  auto batch = folly::coro::blockingWait(source.co_getSplits(10));
  folly::coro::blockingWait(source.co_close());
  EXPECT_EQ(batch.splits.size(), 1);
  EXPECT_TRUE(batch.noMoreSplits);
}

TEST_F(LocalHiveConnectorMetadataTest, createEmptyTable) {
  auto tableType = ROW(
      {{"key1", BIGINT()},
       {"key2", INTEGER()},
       {"data", BIGINT()},
       {"ts", VARCHAR()},
       {"ds", VARCHAR()}});

  auto session = makeSession();
  auto table = folly::coro::blockingWait(metadata_->co_createTable(
      session,
      qualifiedName("test_empty"),
      TableDefinition::create(tableType, /*properties=*/{}),
      /*ifNotExists=*/false,
      /*explain=*/false));

  auto emptyData = makeRowVector(tableType, 0);
  EXPECT_EQ(emptyData->size(), 0);

  auto expected = getLayout(table);
  EXPECT_EQ(expected->partitionColumns().size(), 0);
  EXPECT_FALSE(expected->numBuckets().has_value());
  EXPECT_EQ(expected->orderColumns().size(), 0);
  EXPECT_EQ(expected->hivePartitionColumns().size(), 0);
  EXPECT_EQ(expected->fileFormat(), dwio::common::toFileFormat("dwrf"));
  EXPECT_EQ(expected->table().options().count("compression_kind"), 0);

  writeToTable(
      table, emptyData, WriteKind::kCreate, dwio::common::FileFormat::DWRF);
  compareTableLayout(table, metadata_->findTable(qualifiedName("test_empty")));
  compareTableData(
      "test_empty",
      emptyData,
      /*partitionKeys=*/{},
      dwio::common::FileFormat::DWRF);
}

// Table and column comments, including the comment of a column added later,
// and the primary key persist in the table's '.schema'.
TEST_F(LocalHiveConnectorMetadataTest, commentsAndPrimaryKey) {
  auto session = makeSession();
  const auto tableName = qualifiedName("commented");
  folly::coro::blockingWait(metadata_->co_createEmptyTable(
      session,
      tableName,
      TableDefinition{
          .columns =
              {{.name = "id", .type = BIGINT(), .comment = "the key"},
               {.name = "ds", .type = VARCHAR()}},
          .comment = "a table",
          .properties = {{"partitioned_by", Variant::array({Variant("ds")})}},
          .primaryKey = {"id"},
      },
      /*ifNotExists=*/false));
  folly::coro::blockingWait(metadata_->co_addColumn(
      session,
      tableName,
      {.name = "note", .type = VARCHAR(), .comment = "free-form remarks"},
      /*ifTableExists=*/false,
      /*ifNotExists=*/false,
      /*explain=*/false));

  metadata_->reloadTableFromPath(tableName);
  auto table = metadata_->findTable(tableName);
  ASSERT_NE(table, nullptr);
  VELOX_EXPECT_EQ_TYPES(
      table->type(),
      ROW({"id", "note", "ds"}, {BIGINT(), VARCHAR(), VARCHAR()}));
  EXPECT_EQ("a table", table->comment());
  EXPECT_THAT(table->primaryKey(), testing::ElementsAre("id"));
  EXPECT_EQ("the key", table->findColumn("id")->comment());
  EXPECT_EQ("free-form remarks", table->findColumn("note")->comment());
  EXPECT_EQ(std::nullopt, table->findColumn("ds")->comment());
}

// A table created empty and then inserted into holds and reports the inserted
// rows, partitioned or not.
TEST_F(LocalHiveConnectorMetadataTest, createThenInsert) {
  auto tableType =
      ROW({{"key1", BIGINT()}, {"key2", BIGINT()}, {"ds", VARCHAR()}});
  constexpr int32_t kInsertSize = 1024;
  auto insertData = makeRowVector(
      tableType->names(),
      {
          makeFlatVector<int64_t>(
              kInsertSize, [](auto row) { return row + 7; }),
          makeFlatVector<int64_t>(
              kInsertSize, [](auto row) { return row % 11; }),
          makeFlatVector<StringView>(
              kInsertSize, [](auto /*row*/) { return "2022-09-01"; }),
      });

  auto session = makeSession();
  TablePtr created;
  for (const bool partitioned : {false, true}) {
    SCOPED_TRACE(partitioned);
    const std::string name =
        partitioned ? "test_insert_partitioned" : "test_insert";
    folly::F14FastMap<std::string, velox::Variant> options;
    if (partitioned) {
      options.emplace(
          HiveWriteOptions::kPartitionedBy, velox::Variant::array({"ds"}));
    }
    auto staged = folly::coro::blockingWait(metadata_->co_createEmptyTable(
        session,
        qualifiedName(name),
        TableDefinition::create(tableType, options),
        /*ifNotExists=*/false));
    created = metadata_->findTable(qualifiedName(name));
    compareTableLayout(staged, created);

    writeToTable(
        created,
        insertData,
        WriteKind::kInsert,
        dwio::common::FileFormat::DWRF);
    EXPECT_EQ(
        metadata_->findTable(qualifiedName(name))->numRows(), kInsertSize);

    std::unordered_map<std::string, std::optional<std::string>> partitionKeys;
    if (partitioned) {
      partitionKeys.emplace("ds", "2022-09-01");
    }
    compareTableData(
        name, insertData, partitionKeys, dwio::common::FileFormat::DWRF);
  }

  VELOX_ASSERT_THROW(
      metadata_->beginWrite(
          session,
          created,
          WriteKind::kUpdate,
          /*explain=*/false),
      "Only CREATE/INSERT supported, not UPDATE");
  VELOX_ASSERT_THROW(
      metadata_->deleteLayout(session, /*scanHandle=*/nullptr, /*exact=*/true),
      "DELETE requires a scan of the table");
}

TEST_F(LocalHiveConnectorMetadataTest, abortCreateWithRetry) {
  auto tableType =
      ROW({{"key1", BIGINT()}, {"key2", BIGINT()}, {"ds", VARCHAR()}});
  auto session = makeSession();
  std::string tablePath = localTablePath(qualifiedName("test_abort"));

  auto table = folly::coro::blockingWait(metadata_->co_createTable(
      session,
      qualifiedName("test_abort"),
      TableDefinition::create(tableType, /*properties=*/{}),
      /*ifNotExists=*/false,
      /*explain=*/false));
  auto handle = metadata_->beginWrite(
      session,
      table,
      WriteKind::kCreate,
      /*explain=*/false);
  EXPECT_TRUE(std::filesystem::exists(tablePath));

  VELOX_ASSERT_THROW(
      folly::coro::blockingWait(metadata_->co_createTable(
          session,
          qualifiedName("test_abort"),
          TableDefinition::create(tableType, /*properties=*/{}),
          /*ifNotExists=*/false,
          /*explain=*/false)),
      "Table already exists: \"default\".\"test_abort\"");
  metadata_->abortWrite(session, handle).get();
  EXPECT_FALSE(std::filesystem::exists(tablePath));

  table = folly::coro::blockingWait(metadata_->co_createTable(
      session,
      qualifiedName("test_abort"),
      TableDefinition::create(tableType, /*properties=*/{}),
      /*ifNotExists=*/false,
      /*explain=*/false));
  handle = metadata_->beginWrite(
      session,
      table,
      WriteKind::kCreate,
      /*explain=*/false);
  metadata_->finishWrite(session, handle, /*writeResults=*/{}, nullptr, {})
      .get();
  EXPECT_TRUE(std::filesystem::exists(tablePath));
  auto created = metadata_->findTable(qualifiedName("test_abort"));
  EXPECT_NE(created, nullptr);
}

TEST_F(LocalHiveConnectorMetadataTest, createTableIfNotExists) {
  auto tableType = ROW({{"col1", BIGINT()}, {"col2", VARCHAR()}});
  auto session = makeSession();

  auto table = folly::coro::blockingWait(metadata_->co_createTable(
      session,
      qualifiedName("test_ine"),
      TableDefinition::create(tableType, /*properties=*/{}),
      /*ifNotExists=*/false,
      /*explain=*/false));
  ASSERT_NE(table, nullptr);

  auto table2 = folly::coro::blockingWait(metadata_->co_createTable(
      session,
      qualifiedName("test_ine"),
      TableDefinition::create(tableType, /*properties=*/{}),
      /*ifNotExists=*/true,
      /*explain=*/false));
  ASSERT_EQ(table2, nullptr);

  VELOX_ASSERT_THROW(
      folly::coro::blockingWait(metadata_->co_createTable(
          session,
          qualifiedName("test_ine"),
          TableDefinition::create(tableType, /*properties=*/{}),
          /*ifNotExists=*/false,
          /*explain=*/false)),
      "Table already exists: \"default\".\"test_ine\"");
}

TEST_F(LocalHiveConnectorMetadataTest, createTableIfNotExistsNewTable) {
  auto tableType = ROW({{"col1", BIGINT()}});
  auto session = makeSession();

  auto table = folly::coro::blockingWait(metadata_->co_createTable(
      session,
      qualifiedName("test_ine_new"),
      TableDefinition::create(tableType, /*properties=*/{}),
      /*ifNotExists=*/true,
      /*explain=*/false));
  ASSERT_NE(table, nullptr);

  auto found = metadata_->findTable(qualifiedName("test_ine_new"));
  ASSERT_NE(found, nullptr);
}

TEST_F(LocalHiveConnectorMetadataTest, createTableIfNotExistsExplain) {
  auto tableType = ROW({{"col1", BIGINT()}});
  auto session = makeSession();

  auto table = folly::coro::blockingWait(metadata_->co_createTable(
      session,
      qualifiedName("test_ine_explain"),
      TableDefinition::create(tableType, /*properties=*/{}),
      /*ifNotExists=*/false,
      /*explain=*/false));
  ASSERT_NE(table, nullptr);

  auto explained = folly::coro::blockingWait(metadata_->co_createTable(
      session,
      qualifiedName("test_ine_explain"),
      TableDefinition::create(tableType, /*properties=*/{}),
      /*ifNotExists=*/true,
      /*explain=*/true));
  ASSERT_EQ(explained, nullptr);

  VELOX_ASSERT_THROW(
      folly::coro::blockingWait(metadata_->co_createTable(
          session,
          qualifiedName("test_ine_explain"),
          TableDefinition::create(tableType, /*properties=*/{}),
          /*ifNotExists=*/false,
          /*explain=*/true)),
      "Table already exists: \"default\".\"test_ine_explain\"");

  ASSERT_NE(metadata_->findTable(qualifiedName("test_ine_explain")), nullptr);
}

TEST_F(LocalHiveConnectorMetadataTest, dropTableIfExists) {
  auto tableType = ROW({{"col1", BIGINT()}});
  auto session = makeSession();

  auto table = folly::coro::blockingWait(metadata_->co_createTable(
      session,
      qualifiedName("test_drop"),
      TableDefinition::create(tableType, /*properties=*/{}),
      /*ifNotExists=*/false,
      /*explain=*/false));
  ASSERT_NE(table, nullptr);

  ASSERT_TRUE(metadata_->dropTable(
      session,
      qualifiedName("test_drop"),
      /*ifExists=*/false,
      /*explain=*/false));
  ASSERT_EQ(metadata_->findTable(qualifiedName("test_drop")), nullptr);

  VELOX_ASSERT_THROW(
      metadata_->dropTable(
          session,
          qualifiedName("test_drop"),
          /*ifExists=*/false,
          /*explain=*/false),
      "Table doesn't exist: \"default\".\"test_drop\"");

  ASSERT_FALSE(metadata_->dropTable(
      session,
      qualifiedName("test_drop"),
      /*ifExists=*/true,
      /*explain=*/false));
}

TEST_F(LocalHiveConnectorMetadataTest, dropTableExplain) {
  auto tableType = ROW({{"col1", BIGINT()}});
  auto session = makeSession();

  auto table = folly::coro::blockingWait(metadata_->co_createTable(
      session,
      qualifiedName("test_drop_explain"),
      TableDefinition::create(tableType, /*properties=*/{}),
      /*ifNotExists=*/false,
      /*explain=*/false));
  ASSERT_NE(table, nullptr);

  ASSERT_TRUE(metadata_->dropTable(
      session,
      qualifiedName("test_drop_explain"),
      /*ifExists=*/false,
      /*explain=*/true));
  ASSERT_NE(metadata_->findTable(qualifiedName("test_drop_explain")), nullptr);

  VELOX_ASSERT_THROW(
      metadata_->dropTable(
          session,
          qualifiedName("test_drop_missing_explain"),
          /*ifExists=*/false,
          /*explain=*/true),
      "Table doesn't exist: \"default\".\"test_drop_missing_explain\"");

  ASSERT_FALSE(metadata_->dropTable(
      session,
      qualifiedName("test_drop_missing_explain"),
      /*ifExists=*/true,
      /*explain=*/true));
}

TEST_F(LocalHiveConnectorMetadataTest, createVectorIndex) {
  auto session = makeSession();
  const auto name = qualifiedName("test_index");
  ASSERT_TRUE(createVectorIndex(
      name.table,
      vectorIndexColumns(),
      /*ifNotExists=*/false,
      /*explain=*/false));

  auto index = metadata_->findVectorIndex(session, name);
  ASSERT_NE(index, nullptr);
  EXPECT_EQ(name, index->name);
  EXPECT_EQ("id", index->idColumn);
  EXPECT_EQ("embedding", index->embeddingColumn);
  EXPECT_EQ(vectorIndexProperties(), index->properties);

  const auto codecTableName = qualifiedName("test_index__codecs");
  const auto indexTableName = qualifiedName("test_index__index");
  EXPECT_EQ(codecTableName, index->codecTable);
  EXPECT_EQ(indexTableName, index->indexTable);

  auto codecTable = metadata_->findTable(codecTableName);
  ASSERT_NE(codecTable, nullptr);
  VELOX_EXPECT_EQ_TYPES(
      codecTable->type(),
      ROW({{"codec", VARBINARY()}, {"ds", VARCHAR()}, {"codec_id", BIGINT()}}));
  EXPECT_THAT(
      partitionColumnNames(codecTable), testing::ElementsAre("ds", "codec_id"));

  auto indexTable = metadata_->findTable(indexTableName);
  ASSERT_NE(indexTable, nullptr);
  VELOX_EXPECT_EQ_TYPES(
      indexTable->type(),
      ROW(
          {{"codec_id", BIGINT()},
           {"cluster_id", BIGINT()},
           {"group_id", BIGINT()},
           {"local_index", VARBINARY()},
           {"ds", VARCHAR()}}));
  EXPECT_THAT(partitionColumnNames(indexTable), testing::ElementsAre("ds"));

  ASSERT_FALSE(createVectorIndex(
      name.table,
      vectorIndexColumns(),
      /*ifNotExists=*/true,
      /*explain=*/false));
  VELOX_ASSERT_THROW(
      createVectorIndex(
          name.table,
          vectorIndexColumns(),
          /*ifNotExists=*/false,
          /*explain=*/false),
      "Vector index already exists");
}

// A table with the name of one of the index's tables fails the create and is
// left as it is.
TEST_F(LocalHiveConnectorMetadataTest, createVectorIndexTableExists) {
  const auto codecTableName = qualifiedName("test_index_exists__codecs");
  folly::coro::blockingWait(metadata_->co_createTable(
      makeSession(),
      codecTableName,
      TableDefinition::create(ROW("x", BIGINT()), /*properties=*/{}),
      /*ifNotExists=*/false,
      /*explain=*/false));
  auto table = metadata_->findTable(codecTableName);
  ASSERT_NE(table, nullptr);

  for (const bool explain : {true, false}) {
    SCOPED_TRACE(explain);
    VELOX_ASSERT_THROW(
        createVectorIndex(
            "test_index_exists",
            vectorIndexColumns(),
            /*ifNotExists=*/false,
            explain),
        "Vector index table already exists: \"default\".\"test_index_exists__codecs\"");
  }
  VELOX_EXPECT_EQ_TYPES(
      metadata_->findTable(codecTableName)->type(), table->type());
  EXPECT_EQ(
      metadata_->findTable(qualifiedName("test_index_exists__index")), nullptr);
}

TEST_F(LocalHiveConnectorMetadataTest, createVectorIndexExplain) {
  ASSERT_TRUE(createVectorIndex(
      "test_index_explain",
      vectorIndexColumns(),
      /*ifNotExists=*/false,
      /*explain=*/true));
  EXPECT_EQ(
      metadata_->findVectorIndex(
          makeSession(), qualifiedName("test_index_explain")),
      nullptr);
  expectNoVectorIndexTables("test_index_explain");
}

TEST_F(LocalHiveConnectorMetadataTest, dropVectorIndex) {
  auto session = makeSession();
  const auto name = qualifiedName("test_index_drop");
  ASSERT_TRUE(createVectorIndex(
      name.table,
      vectorIndexColumns(),
      /*ifNotExists=*/false,
      /*explain=*/false));

  ASSERT_TRUE(metadata_->dropVectorIndex(
      session, name, /*ifExists=*/false, /*explain=*/true));
  ASSERT_NE(metadata_->findVectorIndex(session, name), nullptr);

  ASSERT_TRUE(metadata_->dropVectorIndex(
      session, name, /*ifExists=*/false, /*explain=*/false));
  EXPECT_EQ(metadata_->findVectorIndex(session, name), nullptr);
  expectNoVectorIndexTables(name.table);

  ASSERT_FALSE(metadata_->dropVectorIndex(
      session, name, /*ifExists=*/true, /*explain=*/false));
  VELOX_ASSERT_THROW(
      metadata_->dropVectorIndex(
          session, name, /*ifExists=*/false, /*explain=*/false),
      "Vector index doesn't exist");
}

// Bucketing and sorting are not supported.
TEST_F(LocalHiveConnectorMetadataTest, vectorIndexTableProperties) {
  for (const auto& properties :
       std::vector<folly::F14FastMap<std::string, Variant>>{
           {{"bucketed_by", Variant::array({Variant("ds")})},
            {"bucket_count", Variant(int32_t{4})}},
           {{"sorted_by", Variant::array({Variant("ds")})}},
       }) {
    SCOPED_TRACE(properties.begin()->first);
    VELOX_ASSERT_THROW(
        createVectorIndex(
            "test_index_properties",
            vectorIndexColumns(),
            /*ifNotExists=*/false,
            /*explain=*/false,
            properties),
        "Unsupported table property for a vector index");
  }
}

// The index's columns are checked with or without explain: the partition
// columns must be columns of the index, and the rest a BIGINT id and an
// ARRAY(REAL) embedding.
TEST_F(LocalHiveConnectorMetadataTest, invalidVectorIndexColumns) {
  for (const bool explain : {true, false}) {
    SCOPED_TRACE(explain);
    VELOX_ASSERT_THROW(
        createVectorIndex(
            "test_index_columns",
            ROW({{"embedding", ARRAY(REAL())}, {"ds", VARCHAR()}}),
            /*ifNotExists=*/false,
            explain),
        "A vector index must have exactly two columns besides its partition "
        "columns, a BIGINT id and an ARRAY(REAL) embedding");
    VELOX_ASSERT_THROW(
        createVectorIndex(
            "test_index_columns",
            ROW(
                {{"id", BIGINT()},
                 {"x", BIGINT()},
                 {"embedding", ARRAY(REAL())},
                 {"ds", VARCHAR()}}),
            /*ifNotExists=*/false,
            explain),
        "A vector index must have exactly two columns besides its partition "
        "columns, a BIGINT id and an ARRAY(REAL) embedding");
    VELOX_ASSERT_THROW(
        createVectorIndex(
            "test_index_columns",
            ROW({{"id", BIGINT()}, {"x", BIGINT()}, {"ds", VARCHAR()}}),
            /*ifNotExists=*/false,
            explain),
        "The embedding column of a vector index must be an ARRAY(REAL): x BIGINT");
    VELOX_ASSERT_THROW(
        createVectorIndex(
            "test_index_columns",
            ROW(
                {{"id", VARCHAR()},
                 {"embedding", ARRAY(REAL())},
                 {"ds", VARCHAR()}}),
            /*ifNotExists=*/false,
            explain),
        "A vector index must have a BIGINT id column: id VARCHAR, embedding "
        "ARRAY<REAL>");
    VELOX_ASSERT_THROW(
        createVectorIndex(
            "test_index_columns",
            ROW({{"id", BIGINT()}, {"embedding", ARRAY(REAL())}}),
            /*ifNotExists=*/false,
            explain),
        "Partition column is not a column of the vector index: ds");
  }
}

} // namespace
} // namespace facebook::axiom::connector::hive

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
