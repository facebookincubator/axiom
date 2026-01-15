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
#include "velox/dwio/parquet/RegisterParquetWriter.h"
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

  static void SetUpTestCase() {
    // Creates the data and schema from 'testTables_'. These are created on the
    // first test fixture initialization.
    LocalRunnerTestBase::SetUpTestCase();

    // The lambdas will be run after this scope returns, so make captures
    // static.
    static int32_t counter1;
    // Clear 'counter1' so that --gtest_repeat runs get the same data.
    counter1 = 0;
    auto customize1 = [&](const RowVectorPtr& rows) {
      makeAscending(rows, counter1);
    };

    rowType_ = ROW({"c0"}, {BIGINT()});
    testTables_ = {
        runner::test::TableSpec{
            .name = "T",
            .columns = rowType_,
            .rowsPerVector = kRowsPerVector,
            .numVectorsPerFile = kNumVectors,
            .numFiles = kNumFiles,
            .customizeData = customize1},
    };
    parquet::registerParquetWriterFactory();
  }

  static void TearDownTestCase() {
    parquet::unregisterParquetWriterFactory();
    LocalRunnerTestBase::TearDownTestCase();
  }

  void SetUp() override {
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
    auto handle = metadata_->beginWrite(session, table, kind);

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
    metadata_->finishWrite(session, handle, {result}).get();
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
    std::string tablePath = metadata_->tablePath(tableName);
    auto files = getDataFiles(tablePath);
    auto table = metadata_->findTable(tableName);
    auto results = readFiles(table, files, partitionKeys, format);
    exec::test::assertEqualResults({expectedData}, {results});
  }

  static void makeAscending(const RowVectorPtr& rows, int32_t& counter) {
    auto ints = rows->childAt(0)->as<FlatVector<int64_t>>();
    for (auto i = 0; i < ints->size(); ++i) {
      ints->set(i, counter + i);
    }
    counter += ints->size();
  }

  static std::vector<std::string> getDataFiles(const std::string& path) {
    EXPECT_TRUE(std::filesystem::is_directory(path));
    std::vector<std::string> files;
    for (const auto& entry :
         std::filesystem::recursive_directory_iterator(path)) {
      auto file = entry.path().string();
      if (entry.is_regular_file() &&
          file.find(".schema") == std::string::npos) {
        files.push_back(file);
      }
    }
    return files;
  }

  inline static RowTypePtr rowType_;
  LocalHiveConnectorMetadata* metadata_;
};

TEST_F(LocalHiveConnectorMetadataTest, basic) {
  auto table = metadata_->findTable("T");
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
  std::vector<ColumnStatistics> stats;
  std::vector<common::Subfield> fields;
  auto c0 = common::Subfield::create("c0");
  fields.push_back(std::move(*c0));
  HashStringAllocator allocator(pool_.get());
  auto pair = layout->sample(
      tableHandle, 100, {}, layout->rowType(), fields, &allocator, &stats);
  EXPECT_EQ(250'000, pair.first);
  EXPECT_EQ(250'000, pair.second);
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
  auto table = metadata_->createTable(session, "test", tableType, options);

  constexpr int32_t kTestSize = 2048;
  auto data = makeRowVector(
      tableType->names(),
      {
          makeFlatVector<int64_t>(kTestSize, [](auto row) { return row; }),
          makeFlatVector<int32_t>(kTestSize, [](auto row) { return row % 10; }),
          makeFlatVector<int64_t>(kTestSize, [](auto row) { return row + 2; }),
          makeFlatVector<StringView>(
              kTestSize, [](auto row) { return "2022-09-01"; }),
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
  EXPECT_EQ(expected->fileFormat(), dwio::common::toFileFormat("parquet"));
  EXPECT_EQ(expected->table().options().at("compression_kind"), "zstd");

  writeToTable(
      table, data, WriteKind::kCreate, dwio::common::FileFormat::PARQUET);

  std::string tablePath = metadata_->tablePath("test");
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

  compareTableLayout(table, metadata_->findTable("test"));
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
  auto table =
      metadata_->createTable(session, "test_empty", tableType, /*options=*/{});

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
  compareTableLayout(table, metadata_->findTable("test_empty"));
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
  auto staged =
      metadata_->createTable(session, "test_insert", tableType, /*options=*/{});
  auto handle = metadata_->beginWrite(session, staged, WriteKind::kCreate);
  metadata_->finishWrite(session, handle, /*writeResults=*/{}).get();

  auto created = metadata_->findTable("test_insert");
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
              kInsertSize, [](auto row) { return "2022-09-01"; }),
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
      metadata_->beginWrite(session, created, WriteKind::kUpdate),
      "Only CREATE/INSERT supported, not UPDATE");
  VELOX_ASSERT_THROW(
      metadata_->beginWrite(session, created, WriteKind::kDelete),
      "Only CREATE/INSERT supported, not DELETE");
}

TEST_F(LocalHiveConnectorMetadataTest, abortCreateWithRetry) {
  auto tableType =
      ROW({{"key1", BIGINT()}, {"key2", BIGINT()}, {"ds", VARCHAR()}});
  auto session = std::make_shared<ConnectorSession>("q-test");
  std::string tablePath = metadata_->tablePath("test_abort");

  auto table =
      metadata_->createTable(session, "test_abort", tableType, /*options=*/{});
  auto handle = metadata_->beginWrite(session, table, WriteKind::kCreate);
  EXPECT_TRUE(std::filesystem::exists(tablePath));

  VELOX_ASSERT_THROW(
      metadata_->createTable(session, "test_abort", tableType, /*options=*/{}),
      "Table test_abort already exists");
  metadata_->abortWrite(session, handle).get();
  EXPECT_FALSE(std::filesystem::exists(tablePath));

  table =
      metadata_->createTable(session, "test_abort", tableType, /*options=*/{});
  handle = metadata_->beginWrite(session, table, WriteKind::kCreate);
  metadata_->finishWrite(session, handle, /*writeResults=*/{}).get();
  EXPECT_TRUE(std::filesystem::exists(tablePath));
  auto created = metadata_->findTable("test_abort");
  EXPECT_NE(created, nullptr);
}

TEST_F(LocalHiveConnectorMetadataTest, complexTypeStats) {
  // Create a struct type with array fields
  auto structType = ROW(
      {{"bigints", ARRAY(BIGINT())},
       {"bigints_array", ARRAY(ARRAY(BIGINT()))}});

  // Create table type with complex columns
  auto tableType = ROW(
      {{"array_struct", ARRAY(structType)},
       {"map_data", MAP(INTEGER(), ARRAY(BIGINT()))}});

  auto session = std::make_shared<ConnectorSession>("q-test");
  auto table = metadata_->createTable(
      session, "complex_data", tableType, /*options=*/{});

  constexpr int32_t kTestSize = 100;

  // Create test data using simpler nested structure
  // Build array<struct<bigints:array<bigint>,
  // bigints_array:array<array<bigint>>>>

  // First, create the struct elements
  std::vector<std::vector<int64_t>> bigintsData;
  std::vector<std::vector<std::vector<int64_t>>> bigintsArrayData;

  // Calculate total struct instances needed
  // Array sizes vary 1-4 cyclically: avg = (1+2+3+4)/4 = 2.5
  // For kTestSize rows: ~250 struct instances
  int runningTotal = 0;
  for (int i = 0; i < kTestSize; ++i) {
    runningTotal += ((i % 4) + 1);
  }
  int totalStructs = runningTotal;

  for (int i = 0; i < totalStructs; ++i) {
    // bigints: array of size 1-5
    int bigintsSize = (i % 5) + 1;
    std::vector<int64_t> bigints;
    for (int j = 0; j < bigintsSize; ++j) {
      bigints.push_back(i * 100 + j);
    }
    bigintsData.push_back(bigints);

    // bigints_array: array of arrays, outer size 1-3
    int outerSize = (i % 3) + 1;
    std::vector<std::vector<int64_t>> outerArray;
    for (int k = 0; k < outerSize; ++k) {
      int innerSize = ((i + k) % 4) + 1;
      std::vector<int64_t> innerArray;
      for (int m = 0; m < innerSize; ++m) {
        innerArray.push_back(i * 1000 + k * 10 + m);
      }
      outerArray.push_back(innerArray);
    }
    bigintsArrayData.push_back(outerArray);
  }

  // Create the arrays for struct members
  auto bigintsArray = makeArrayVector<int64_t>(bigintsData);

  // Flatten the nested arrays for makeArrayVector
  std::vector<std::vector<int64_t>> flattenedInnerArrays;
  for (const auto& outer : bigintsArrayData) {
    for (const auto& inner : outer) {
      flattenedInnerArrays.push_back(inner);
    }
  }
  auto innerArrays = makeArrayVector<int64_t>(flattenedInnerArrays);

  // Build offsets for outer array
  std::vector<vector_size_t> outerOffsets = {0};
  for (const auto& outer : bigintsArrayData) {
    outerOffsets.push_back(outerOffsets.back() + outer.size());
  }
  auto bigintsArrayArray = makeArrayVector(outerOffsets, innerArrays);

  auto structVector = makeRowVector(
      {"bigints", "bigints_array"}, {bigintsArray, bigintsArrayArray});

  // Create array of structs with varying sizes 1-4
  std::vector<vector_size_t> structOffsets;
  vector_size_t offset = 0;
  for (int i = 0; i < kTestSize; ++i) {
    structOffsets.push_back(offset);
    offset += ((i % 4) + 1);
  }
  auto arrayStructVector = makeArrayVector(structOffsets, structVector);

  // Create map: integer -> array of bigints
  std::vector<std::vector<std::pair<int32_t, std::vector<int64_t>>>> mapData;
  for (int i = 0; i < kTestSize; ++i) {
    // Map size varies 1-3
    int mapSize = (i % 3) + 1;
    std::vector<std::pair<int32_t, std::vector<int64_t>>> mapRow;
    for (int j = 0; j < mapSize; ++j) {
      int32_t key = (i * 10 + j) % 50;

      // Array value size varies 1-6
      int arraySize = ((i + j) % 6) + 1;
      std::vector<int64_t> arrayValue;
      for (int k = 0; k < arraySize; ++k) {
        arrayValue.push_back(i * 100 + j * 10 + k);
      }
      mapRow.push_back({key, arrayValue});
    }
    mapData.push_back(mapRow);
  }

  // Convert to proper map vector structure
  std::vector<vector_size_t> mapOffsets;
  std::vector<int32_t> allKeys;
  std::vector<std::vector<int64_t>> allValueArrays;

  for (const auto& mapRow : mapData) {
    mapOffsets.push_back(allKeys.size());
    for (const auto& [key, value] : mapRow) {
      allKeys.push_back(key);
      allValueArrays.push_back(value);
    }
  }

  auto mapKeysVector = makeFlatVector<int32_t>(allKeys);
  auto mapValuesVector = makeArrayVector<int64_t>(allValueArrays);
  auto mapVector = makeMapVector(mapOffsets, mapKeysVector, mapValuesVector);

  // Verify sizes match
  EXPECT_EQ(arrayStructVector->size(), kTestSize);
  EXPECT_EQ(mapVector->size(), kTestSize);

  auto data = makeRowVector(tableType->names(), {arrayStructVector, mapVector});
  EXPECT_EQ(data->size(), kTestSize);

  writeToTable(table, data, WriteKind::kCreate, dwio::common::FileFormat::DWRF);

  // Prepare to call sample() with subfields
  auto* layout = getLayout(table);
  auto ctx = metadata_->connectorQueryCtx();

  // Create column handles for all columns
  std::vector<velox::connector::ColumnHandlePtr> columns;
  for (auto i = 0; i < tableType->size(); ++i) {
    columns.push_back(
        layout->createColumnHandle(/*session=*/nullptr, tableType->nameOf(i)));
  }

  std::vector<core::TypedExprPtr> filters;
  std::vector<core::TypedExprPtr> rejectedFilters;

  auto tableHandle = layout->createTableHandle(
      /*session=*/nullptr,
      columns,
      *ctx->expressionEvaluator(),
      filters,
      rejectedFilters,
      /*dataColumns=*/nullptr,
      /*lookupKeys=*/{});
  EXPECT_TRUE(rejectedFilters.empty());

  // Create subfields for array_struct and map_data
  std::vector<common::Subfield> fields;
  auto arrayStructField = common::Subfield::create("array_struct");
  auto mapDataField = common::Subfield::create("map_data");
  fields.push_back(std::move(*arrayStructField));
  fields.push_back(std::move(*mapDataField));

  // Call sample() with ColumnStatistics array
  std::vector<ColumnStatistics> stats;
  HashStringAllocator allocator(pool_.get());
  auto pair = layout->sample(
      tableHandle, 100, {}, layout->rowType(), fields, &allocator, &stats);

  EXPECT_EQ(kTestSize, pair.first);
  EXPECT_EQ(kTestSize, pair.second);

  // Verify statistics were collected
  ASSERT_EQ(2, stats.size());

  // Find statistics by column name (order is undefined)
  ColumnStatistics* arrayStructStats = nullptr;
  ColumnStatistics* mapStats = nullptr;

  for (auto& stat : stats) {
    if (stat.name == "array_struct") {
      arrayStructStats = &stat;
    } else if (stat.name == "map_data") {
      mapStats = &stat;
    }
  }

  ASSERT_NE(arrayStructStats, nullptr) << "array_struct statistics not found";
  ASSERT_NE(mapStats, nullptr) << "map_data statistics not found";

  // Check array_struct statistics
  EXPECT_GT(arrayStructStats->avgLength.value_or(0), 0);
  ASSERT_EQ(
      1, arrayStructStats->children.size()); // array has 1 child (elements)

  // The child is a struct with 2 members
  auto& structStats = arrayStructStats->children[0];
  ASSERT_EQ(2, structStats.children.size());

  // First struct member: bigints (array of bigint)
  auto& bigintsStats = structStats.children[0];
  EXPECT_EQ("bigints", bigintsStats.name);
  EXPECT_GT(bigintsStats.avgLength.value_or(0), 0);
  ASSERT_EQ(1, bigintsStats.children.size()); // array has 1 child (elements)

  // Second struct member: bigints_array (array of array of bigint)
  auto& bigintsArrayStats = structStats.children[1];
  EXPECT_EQ("bigints_array", bigintsArrayStats.name);
  EXPECT_GT(bigintsArrayStats.avgLength.value_or(0), 0);
  ASSERT_EQ(1, bigintsArrayStats.children.size()); // outer array has 1 child

  // The inner array
  auto& innerArrayStats = bigintsArrayStats.children[0];
  EXPECT_GT(innerArrayStats.avgLength.value_or(0), 0);
  ASSERT_EQ(
      1, innerArrayStats.children.size()); // inner array has 1 child (elements)

  // Check map_data statistics
  EXPECT_GT(mapStats->avgLength.value_or(0), 0);
  ASSERT_EQ(2, mapStats->children.size()); // map has 2 children (keys, values)

  // First child: keys (integers) - check that stats were collected
  // Keys are scalars, may not have numValues populated for scalar types
  EXPECT_EQ(2, mapStats->children.size());

  // Second child: values (array of bigints)
  auto& valuesStats = mapStats->children[1];
  EXPECT_GT(valuesStats.avgLength.value_or(0), 0);
  ASSERT_EQ(1, valuesStats.children.size()); // array has 1 child (elements)

  // Verify avgLength matches expected values from data generation
  // array_struct: size varies 1-4, avg should be around 2.5
  EXPECT_GE(arrayStructStats->avgLength.value(), 1);
  EXPECT_LE(arrayStructStats->avgLength.value(), 4);

  // map_data: size varies 1-3, avg should be around 2
  EXPECT_GE(mapStats->avgLength.value(), 1);
  EXPECT_LE(mapStats->avgLength.value(), 3);

  // bigints array: size varies 1-5, avg should be around 3
  EXPECT_GE(bigintsStats.avgLength.value(), 1);
  EXPECT_LE(bigintsStats.avgLength.value(), 5);

  // map values array: size varies 1-6, avg should be around 3.5
  EXPECT_GE(valuesStats.avgLength.value(), 1);
  EXPECT_LE(valuesStats.avgLength.value(), 6);
}

} // namespace
} // namespace facebook::axiom::connector::hive

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
