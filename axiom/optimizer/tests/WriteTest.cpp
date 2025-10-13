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
#include "velox/dwio/parquet/RegisterParquetWriter.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

class WriteTest : public test::HiveQueriesTestBase {
 protected:
  void SetUp() override {
    HiveQueriesTestBase::SetUp();
    connector_ = velox::connector::getConnector(exec::test::kHiveConnectorId);
    metadata_ = dynamic_cast<connector::hive::LocalHiveConnectorMetadata*>(
        connector::ConnectorMetadata::metadata(exec::test::kHiveConnectorId));
    parquet::registerParquetWriterFactory();
  }

  void TearDown() override {
    parquet::unregisterParquetWriterFactory();
    metadata_ = nullptr;
    connector_.reset();
    HiveQueriesTestBase::TearDown();
  }

  void createTable(
      const std::string& name,
      const RowTypePtr& tableType,
      const folly::F14FastMap<std::string, std::string>& options) {
    auto session = std::make_shared<connector::ConnectorSession>("test");
    auto table = metadata_->createTable(session, name, tableType, options);
    auto handle =
        metadata_->beginWrite(session, table, connector::WriteKind::kCreate);
    metadata_->finishWrite(session, handle, {}).get();
  }

  void checkWriteResults(const test::TestResult& result, int64_t expected) {
    ASSERT_EQ(1, result.results.size());
    ASSERT_EQ(1, result.results[0]->size());
    const auto& child = result.results[0]->childAt(0);
    ASSERT_TRUE(child);
    auto* flat = result.results[0]->childAt(0)->as<FlatVector<int64_t>>();
    ASSERT_TRUE(flat);
    ASSERT_EQ(flat->size(), 1);
    ASSERT_TRUE(!flat->isNullAt(0));
    EXPECT_EQ(flat->valueAt(0), expected);
  }

  std::vector<RowVectorPtr> makeTestData(
      size_t numBatches,
      vector_size_t batchSize) {
    std::vector<RowVectorPtr> data;
    for (size_t i = 0; i < numBatches; ++i) {
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
                    str = fmt::format("2025-09-{}", ((row + start) % 2));
                    return StringView(str);
                  }),
          }));
    }
    return data;
  }

  std::shared_ptr<velox::connector::Connector> connector_;
  connector::hive::LocalHiveConnectorMetadata* metadata_;
};

TEST_F(WriteTest, write) {
  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);

  static constexpr vector_size_t kTestBatchSize = 2048;

  auto tableType = ROW({
      {"key1", BIGINT()},
      {"key2", INTEGER()},
      {"data", BIGINT()},
      {"data2", VARCHAR()},
      {"ds", VARCHAR()},
  });

  folly::F14FastMap<std::string, std::string> options = {
      {"file_format", "parquet"},
      {"compression_kind", "snappy"},
  };

  createTable("test", tableType, options);

  auto data = makeTestData(10, kTestBatchSize);

  auto writePlan = lp::PlanBuilder(context)
                       .values({data})
                       .tableWrite(
                           exec::test::kHiveConnectorId,
                           "test",
                           lp::WriteKind::kInsert,
                           {"key1", "key2", "data", "ds"},
                           {"key1", "key2", "data", "ds"})
                       .build();
  checkWriteResults(runVelox(writePlan), kTestBatchSize * 10);

  auto countTestTable = [&] {
    auto countPlan = lp::PlanBuilder(context)
                         .tableScan(exec::test::kHiveConnectorId, "test")
                         .aggregate({}, {"count(1)"})
                         .build();

    auto result = runVelox(countPlan);
    return result.results[0]->childAt(0)->as<FlatVector<int64_t>>()->valueAt(0);
  };

  EXPECT_EQ(kTestBatchSize * 10, countTestTable());

  auto errorData = makeTestData(100, kTestBatchSize);
  auto errorPlan = lp::PlanBuilder(context)
                       .values(errorData)
                       .tableWrite(
                           exec::test::kHiveConnectorId,
                           "test",
                           lp::WriteKind::kInsert,
                           {"key1", "key2", "data", "ds"},
                           {"key1", "key2", "key1 % (key1 - 200000)", "ds"})
                       .build();
  VELOX_ASSERT_THROW(runVelox(errorPlan), "divide by");

  EXPECT_EQ(kTestBatchSize * 10, countTestTable());

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

  // Create a second table to copy the first one into.
  createTable("test2", tableType, options);

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
                          {"key1", "key2", "data", "data2", "ds"})
                      .build();
  checkWriteResults(runVelox(copyPlan), kTestBatchSize * 10);

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
