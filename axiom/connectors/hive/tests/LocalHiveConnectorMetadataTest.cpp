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
#include "axiom/runner/tests/LocalRunnerTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/hive/HivePartitionFunction.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

#include <folly/init/Init.h>
#include <filesystem>

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
    metadata_ = dynamic_cast<LocalHiveConnectorMetadata*>(
        ConnectorMetadata::metadata(velox::exec::test::kHiveConnectorId));
    ASSERT_TRUE(metadata_ != nullptr);
  }

  static const LocalHiveTableLayout* getLayout(const TablePtr& table) {
    auto& layouts = table->layouts();
    EXPECT_EQ(1, layouts.size());
    auto* layout = dynamic_cast<const LocalHiveTableLayout*>(layouts[0]);
    EXPECT_TRUE(layout != nullptr);
    return layout;
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
                   return (a->name() == b->name()) && (a->type() == b->type());
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

  /// Write the specified data to the table with a TableWrite operation. The
  /// 'kind' specifies the type of write, for which only kCreate (new table) and
  /// kInsert (existing table) writes are supported. 'format' specifies the
  /// storage format to write with.
  void writeToTable(
      const TablePtr& table,
      const RowVectorPtr& values,
      WriteKind kind,
      dwio::common::FileFormat format) {
    std::string outputPath = metadata_->tablePath(table->name());
    auto session = std::make_shared<ConnectorSession>("q-test");
    auto handle =
        metadata_->beginWrite(session, table, kind, /*explain=*/false);

    auto builder = exec::test::PlanBuilder().values({values});
    auto insertHandle = std::make_shared<core::InsertTableHandle>(
        velox::exec::test::kHiveConnectorId, handle->veloxHandle());
    auto plan = builder.startTableWriter()
                    .outputDirectoryPath(outputPath)
                    .outputType(table->type())
                    .insertHandle(insertHandle)
                    .fileFormat(format)
                    .endTableWriter()
                    .planNode();
    auto result = exec::test::AssertQueryBuilder(plan).copyResults(pool());
    metadata_->finishWrite(session, handle, {result}, nullptr, {}).get();
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
    std::string tablePath = metadata_->tablePath({kDefaultSchema, tableName});
    auto files = getDataFiles(tablePath);
    auto table = metadata_->findTable({kDefaultSchema, tableName});
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

  RowTypePtr rowType_;
  LocalHiveConnectorMetadata* metadata_{};
};

TEST_F(LocalHiveConnectorMetadataTest, basic) {
  auto table = metadata_->findTable({kDefaultSchema, "t"});
  ASSERT_TRUE(table != nullptr);
  auto column = table->findColumn("c0");
  ASSERT_TRUE(column != nullptr);
  EXPECT_EQ(250'000, table->numRows());
  auto* layout = table->layouts()[0];
  auto columnHandle = layout->createColumnHandle(/*session=*/nullptr, "c0");
  std::vector<velox::connector::ColumnHandlePtr> columns = {columnHandle};
  std::vector<core::TypedExprPtr> filters;
  std::vector<core::TypedExprPtr> rejectedFilters;
  auto ctx = metadata_->connectorQueryCtx();

  auto tableHandle = layout->createTableHandle(
      /*session=*/nullptr,
      columns,
      *ctx->expressionEvaluator(),
      filters,
      rejectedFilters,
      /*dataColumns=*/nullptr,
      /*lookupKeys=*/{});
  EXPECT_TRUE(rejectedFilters.empty());
  std::vector<common::Subfield> fields;
  auto c0 = common::Subfield::create("c0");
  fields.push_back(std::move(*c0));
  HashStringAllocator allocator(pool_.get());
  std::vector<std::unique_ptr<StatisticsBuilder>> statsBuilders;
  auto* localLayout = dynamic_cast<const LocalHiveTableLayout*>(layout);
  ASSERT_NE(localLayout, nullptr);
  auto pair =
      localLayout->sample(tableHandle, 100, fields, &allocator, &statsBuilders);
  EXPECT_EQ(250'000, pair.first);
  EXPECT_EQ(250'000, pair.second);
}

// Verifies that sample() only samples files matching $path filter.
TEST_F(LocalHiveConnectorMetadataTest, sampleWithPathFilter) {
  auto table = metadata_->findTable({kDefaultSchema, "t"});
  ASSERT_TRUE(table != nullptr);
  auto* layout = dynamic_cast<const LocalHiveTableLayout*>(table->layouts()[0]);
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
  std::vector<core::TypedExprPtr> rejectedFilters;
  auto ctx = metadata_->connectorQueryCtx();

  auto tableHandle = layout->createTableHandle(
      /*session=*/nullptr,
      columns,
      *ctx->expressionEvaluator(),
      filters,
      rejectedFilters,
      /*dataColumns=*/nullptr,
      /*lookupKeys=*/{});
  EXPECT_TRUE(rejectedFilters.empty());

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
      {HiveWriteOptions::kSortedBy, velox::Variant::array({"key1", "key2"})},
      {HiveWriteOptions::kPartitionedBy, velox::Variant::array({"ds"})},
      {HiveWriteOptions::kFileFormat, "parquet"},
      {HiveWriteOptions::kCompressionKind, "zstd"}};

  auto session = std::make_shared<ConnectorSession>("q-test");
  auto table = metadata_->createTable(
      session,
      {kDefaultSchema, "test"},
      tableType,
      options,
      /*ifNotExists=*/false,
      /*explain=*/false);

  // Table should be findable immediately after createTable, before
  // beginWrite/finishWrite. This is the flow for plain CREATE TABLE (no data).
  ASSERT_NE(metadata_->findTable({kDefaultSchema, "test"}), nullptr);

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
  EXPECT_EQ(expected->hivePartitionColumns().size(), 1);
  EXPECT_EQ(expected->hivePartitionColumns()[0], expected->columns()[3]);
  EXPECT_TRUE(expected->hivePartitionColumns()[0]->includeInExplainIo());
  // Non-partition columns should not be included in EXPLAIN IO.
  EXPECT_FALSE(expected->columns()[0]->includeInExplainIo());
  EXPECT_FALSE(expected->columns()[1]->includeInExplainIo());
  EXPECT_FALSE(expected->columns()[2]->includeInExplainIo());
  EXPECT_EQ(expected->fileFormat(), dwio::common::toFileFormat("parquet"));
  EXPECT_EQ(expected->table().options().at("compression_kind"), "zstd");

  writeToTable(
      table, data, WriteKind::kCreate, dwio::common::FileFormat::PARQUET);

  std::string tablePath = metadata_->tablePath({kDefaultSchema, "test"});
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

  compareTableLayout(table, metadata_->findTable({kDefaultSchema, "test"}));
  compareTableData(
      "test", data, {{"ds", partition}}, dwio::common::FileFormat::PARQUET);
}

TEST_F(LocalHiveConnectorMetadataTest, createEmptyTable) {
  auto tableType = ROW(
      {{"key1", BIGINT()},
       {"key2", INTEGER()},
       {"data", BIGINT()},
       {"ts", VARCHAR()},
       {"ds", VARCHAR()}});

  auto session = std::make_shared<ConnectorSession>("q-test");
  auto table = metadata_->createTable(
      session,
      {kDefaultSchema, "test_empty"},
      tableType,
      /*options=*/{},
      /*ifNotExists=*/false,
      /*explain=*/false);

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
  compareTableLayout(
      table, metadata_->findTable({kDefaultSchema, "test_empty"}));
  compareTableData(
      "test_empty",
      emptyData,
      /*partitionKeys=*/{},
      dwio::common::FileFormat::DWRF);
}

TEST_F(LocalHiveConnectorMetadataTest, createThenInsert) {
  auto tableType =
      ROW({{"key1", BIGINT()}, {"key2", BIGINT()}, {"ds", VARCHAR()}});

  auto session = std::make_shared<ConnectorSession>("q-test");
  auto staged = metadata_->createTable(
      session,
      {kDefaultSchema, "test_insert"},
      tableType,
      /*options=*/{},
      /*ifNotExists=*/false,
      /*explain=*/false);
  auto handle = metadata_->beginWrite(
      session, staged, WriteKind::kCreate, /*explain=*/false);
  metadata_->finishWrite(session, handle, /*writeResults=*/{}, nullptr, {})
      .get();

  auto created = metadata_->findTable({kDefaultSchema, "test_insert"});
  compareTableLayout(staged, created);

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
  EXPECT_EQ(insertData->size(), kInsertSize);
  writeToTable(
      created, insertData, WriteKind::kInsert, dwio::common::FileFormat::DWRF);
  compareTableData(
      "test_insert",
      insertData,
      /*partitionKeys=*/{},
      dwio::common::FileFormat::DWRF);

  VELOX_ASSERT_THROW(
      metadata_->beginWrite(
          session, created, WriteKind::kUpdate, /*explain=*/false),
      "Only CREATE/INSERT supported, not UPDATE");
  VELOX_ASSERT_THROW(
      metadata_->beginWrite(
          session, created, WriteKind::kDelete, /*explain=*/false),
      "Only CREATE/INSERT supported, not DELETE");
}

TEST_F(LocalHiveConnectorMetadataTest, abortCreateWithRetry) {
  auto tableType =
      ROW({{"key1", BIGINT()}, {"key2", BIGINT()}, {"ds", VARCHAR()}});
  auto session = std::make_shared<ConnectorSession>("q-test");
  std::string tablePath = metadata_->tablePath({kDefaultSchema, "test_abort"});

  auto table = metadata_->createTable(
      session,
      SchemaTableName{kDefaultSchema, "test_abort"},
      tableType,
      /*options=*/{},
      /*ifNotExists=*/false,
      /*explain=*/false);
  auto handle = metadata_->beginWrite(
      session, table, WriteKind::kCreate, /*explain=*/false);
  EXPECT_TRUE(std::filesystem::exists(tablePath));

  VELOX_ASSERT_THROW(
      metadata_->createTable(
          session,
          SchemaTableName{kDefaultSchema, "test_abort"},
          tableType,
          /*options=*/{},
          /*ifNotExists=*/false,
          /*explain=*/false),
      "Table \"default\".\"test_abort\" already exists");
  metadata_->abortWrite(session, handle).get();
  EXPECT_FALSE(std::filesystem::exists(tablePath));

  table = metadata_->createTable(
      session,
      SchemaTableName{kDefaultSchema, "test_abort"},
      tableType,
      /*options=*/{},
      /*ifNotExists=*/false,
      /*explain=*/false);
  handle = metadata_->beginWrite(
      session, table, WriteKind::kCreate, /*explain=*/false);
  metadata_->finishWrite(session, handle, /*writeResults=*/{}, nullptr, {})
      .get();
  EXPECT_TRUE(std::filesystem::exists(tablePath));
  auto created = metadata_->findTable({kDefaultSchema, "test_abort"});
  EXPECT_NE(created, nullptr);
}

TEST_F(LocalHiveConnectorMetadataTest, createTableIfNotExists) {
  auto tableType = ROW({{"col1", BIGINT()}, {"col2", VARCHAR()}});
  auto session = std::make_shared<ConnectorSession>("q-test");

  auto table = metadata_->createTable(
      session,
      {kDefaultSchema, "test_ine"},
      tableType,
      /*options=*/{},
      /*ifNotExists=*/false,
      /*explain=*/false);
  ASSERT_NE(table, nullptr);

  auto table2 = metadata_->createTable(
      session,
      {kDefaultSchema, "test_ine"},
      tableType,
      /*options=*/{},
      /*ifNotExists=*/true,
      /*explain=*/false);
  ASSERT_EQ(table2, nullptr);

  VELOX_ASSERT_THROW(
      metadata_->createTable(
          session,
          {kDefaultSchema, "test_ine"},
          tableType,
          /*options=*/{},
          /*ifNotExists=*/false,
          /*explain=*/false),
      "already exists");
}

TEST_F(LocalHiveConnectorMetadataTest, createTableIfNotExistsNewTable) {
  auto tableType = ROW({{"col1", BIGINT()}});
  auto session = std::make_shared<ConnectorSession>("q-test");

  auto table = metadata_->createTable(
      session,
      {kDefaultSchema, "test_ine_new"},
      tableType,
      /*options=*/{},
      /*ifNotExists=*/true,
      /*explain=*/false);
  ASSERT_NE(table, nullptr);

  auto found = metadata_->findTable({kDefaultSchema, "test_ine_new"});
  ASSERT_NE(found, nullptr);
}

} // namespace
} // namespace facebook::axiom::connector::hive

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
