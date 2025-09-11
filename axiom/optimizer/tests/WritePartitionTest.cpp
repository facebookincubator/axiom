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
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

class WritePartitionTest : public test::HiveQueriesTestBase {
 protected:
  static void SetUpTestCase() {
    test::HiveQueriesTestBase::SetUpTestCase();
  }

  static void TearDownTestCase() {
    test::HiveQueriesTestBase::TearDownTestCase();
  }

  void SetUp() override {
    HiveQueriesTestBase::SetUp();
    connector_ = connector::getConnector(exec::test::kHiveConnectorId);
    metadata_ = dynamic_cast<connector::hive::LocalHiveConnectorMetadata*>(
        connector::ConnectorMetadata::metadata(exec::test::kHiveConnectorId));
    optimizerOptions_.session =
        std::make_shared<connector::hive::HiveConnectorSession>();
    parquet::registerParquetReaderFactory();
    parquet::registerParquetWriterFactory();
  }

  void TearDown() override {
    connector_.reset();
    HiveQueriesTestBase::TearDown();
    parquet::unregisterParquetReaderFactory();
    parquet::unregisterParquetWriterFactory();
  }

  std::vector<RowVectorPtr>
  makeTestData(int32_t numBatches, int32_t batchSize, int32_t dayOffset = 0) {
    std::vector<RowVectorPtr> data;
    for (auto i = 0; i < numBatches; ++i) {
      auto start = i * batchSize;
      std::string str;
      data.push_back(makeRowVector(
          {"key1", "key2", "data", "ds"},
          {
              makeFlatVector<int64_t>(
                  batchSize, [&](auto row) { return row + start; }),
              makeFlatVector<int32_t>(
                  batchSize, [&](auto row) { return (row + start) % 19; }),
              makeFlatVector<int64_t>(
                  batchSize, [&](auto row) { return row + start + 2; }),
              makeFlatVector<StringView>(
                  batchSize,
                  [&](auto row) {
                    str = fmt::format(
                        "2025-09-{}", dayOffset + ((row + start) % 2));
                    return StringView(str);
                  }),
          }));
    }
    return data;
  }

  std::vector<lp::ExprApi> exprs(const std::vector<std::string>& strings) {
    std::vector<lp::ExprApi> exprs;
    for (auto& string : strings) {
      exprs.push_back(lp::Sql(string));
    }
    return exprs;
  }

  std::shared_ptr<connector::Connector> connector_;
  connector::hive::LocalHiveConnectorMetadata* metadata_;
  connector::ConnectorSessionPtr session_{
      std::make_shared<connector::hive::HiveConnectorSession>()};
  RowTypePtr writeOutputType_{
      ROW({"numWrittenRows", "fragment", "tableCommitContext"},
          {BIGINT(), VARBINARY(), VARBINARY()})};
};

TEST_F(WritePartitionTest, write) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);

  constexpr int32_t kTestBatchSize = 2048;

  auto tableType = ROW({
      {"key1", BIGINT()},
      {"key2", INTEGER()},
      {"data", BIGINT()},
      {"data2", VARCHAR()},
      {"ds", VARCHAR()},
  });

  folly::F14FastMap<std::string, std::string> options = {
      {"bucketed_by", "key1,key2"},
      {"bucket_count", "16"},
      {"partitioned_by", "ds"},
      {"file_format", "parquet"},
      {"compression_kind", "snappy"},
  };

  metadata_->createTable("test", tableType, options, session_, false);

  auto data = makeTestData(10, kTestBatchSize);

  auto write1 = lp::PlanBuilder(context)
                    .values({data})
                    .tableWrite(
                        exec::test::kHiveConnectorId,
                        "test",
                        lp::WriteKind::kInsert,
                        {"key1", "key2", "data", "ds"},
                        exprs({"key1", "key2", "data", "ds"}),
                        writeOutputType_)
                    .build();
  runVelox(write1);

  auto countPlan =
      lp::PlanBuilder(context)
          .tableScan(exec::test::kHiveConnectorId, "test", {"key1"})
          .aggregate({}, {"count(1)"})
          .build();

  {
    auto result = runVelox(countPlan);
    EXPECT_EQ(
        kTestBatchSize * 10,
        result.results[0]->childAt(0)->as<FlatVector<int64_t>>()->valueAt(0));
  }
  auto errorData = makeTestData(100, kTestBatchSize, 3);
  auto errorPlan =
      lp::PlanBuilder(context)
          .values(errorData)
          .tableWrite(
              exec::test::kHiveConnectorId,
              "test",
              lp::WriteKind::kInsert,
              {"key1", "key2", "data", "ds"},
              exprs({"key1", "key2", "key1 % (key1 - 200000)", "ds"}),
              writeOutputType_)
          .build();
  VELOX_ASSERT_THROW(runVelox(errorPlan), "ivide by");

  {
    auto result = runVelox(countPlan);
    EXPECT_EQ(
        kTestBatchSize * 10,
        result.results[0]->childAt(0)->as<FlatVector<int64_t>>()->valueAt(0));
  }

  auto readPlan = lp::PlanBuilder(context)
                      .tableScan(
                          exec::test::kHiveConnectorId,
                          "test",
                          {"key1", "key2", "data", "data2", "ds"})
                      .filter("data2 is null")
                      .project({"key1", "key2", "data", "ds"})
                      .build();

  {
    auto result = runVelox(readPlan);
    exec::test::assertEqualResults(data, result.results);
  }

  // Create a second table to copy the first one into. Values runs single node,
  // the copy runs distributed.
  folly::F14FastMap<std::string, std::string> options2 = {
      {"bucketed_by", "key1"},
      {"bucket_count", "16"},
      {"partitioned_by", "ds"},
      {"file_format", "parquet"},
      {"compression_kind", "snappy"},
  };
  metadata_->createTable("test2", tableType, options2, session_, false);

  auto copyPlan = lp::PlanBuilder(context)
                      .tableScan(
                          exec::test::kHiveConnectorId,
                          "test",
                          {"key1", "key2", "data", "data2", "ds"})
                      .tableWrite(
                          exec::test::kHiveConnectorId,
                          "test2",
                          lp::WriteKind::kInsert,
                          {"key1", "key2", "data", "data2", "ds"},
                          exprs({"key1", "key2", "data", "data2", "ds"}),
                          writeOutputType_)
                      .build();
  runVelox(copyPlan);

  readPlan = lp::PlanBuilder(context)
                 .tableScan(
                     exec::test::kHiveConnectorId,
                     "test2",
                     {"key1", "key2", "data", "data2", "ds"})
                 .filter("data2 is null")
                 .project({"key1", "key2", "data", "ds"})
                 .build();

  {
    auto result = runVelox(readPlan);
    exec::test::assertEqualResults(data, result.results);
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
