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
#include "axiom/logical_plan/ExprVisitor.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/expression/Expr.h"

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
      const folly::F14FastMap<std::string, velox::Variant>& options) {
    metadata_->dropTableIfExists(name);

    auto session = std::make_shared<connector::ConnectorSession>("test");
    auto table = metadata_->createTable(session, name, tableType, options);
    auto handle = metadata_->beginWrite(session, table, WriteKind::kCreate);
    metadata_->finishWrite(session, handle, {}).get();
  }

  connector::TablePtr createTable(
      const test::CreateTableAsSelectStatement& statement) {
    metadata_->dropTableIfExists(statement.tableName());

    folly::F14FastMap<std::string, velox::Variant> options;
    for (const auto& [key, value] : statement.properties()) {
      options[key] = evaluateConstantExpr(*value);
    }

    auto session = std::make_shared<connector::ConnectorSession>("test");
    return metadata_->createTable(
        session, statement.tableName(), statement.tableSchema(), options);
  }

  void runCtas(
      const std::string& sql,
      int64_t writtenRows,
      const runner::MultiFragmentPlan::Options& options = {
          .numWorkers = 4,
          .numDrivers = 4,
      }) {
    SCOPED_TRACE(sql);

    test::PrestoParser parser(exec::test::kHiveConnectorId, pool());

    auto statement = parser.parse(sql);
    VELOX_CHECK(statement->isCreateTableAsSelect());

    auto ctasStatement =
        statement->asUnchecked<test::CreateTableAsSelectStatement>();

    auto table = createTable(*ctasStatement);

    connector::SchemaResolver schemaResolver;
    schemaResolver.setTargetTable(exec::test::kHiveConnectorId, table);

    auto result = runVelox(ctasStatement->plan(), schemaResolver, options);

    checkWrittenRows(result, writtenRows);
  }

  static void checkWrittenRows(
      const test::TestResult& result,
      int64_t writtenRows) {
    ASSERT_EQ(1, result.results.size());
    ASSERT_EQ(1, result.results[0]->size());

    const auto& child = result.results[0]->childAt(0);
    ASSERT_TRUE(child);
    ASSERT_EQ(1, child->size());

    const auto value = child->variantAt(0);
    ASSERT_TRUE(!value.isNull());

    ASSERT_EQ(writtenRows, value.value<int64_t>());
  }

  void checkTableData(
      const std::string& tableName,
      const RowVectorPtr& expectedData) {
    auto logicalPlan = lp::PlanBuilder()
                           .tableScan(exec::test::kHiveConnectorId, tableName)
                           .build();

    checkSameSingleNode(logicalPlan, {expectedData});
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

 private:
  velox::Variant evaluateConstantExpr(const lp::Expr& expr);
};

TEST_F(WriteTest, basic) {
  SCOPE_EXIT {
    metadata_->dropTableIfExists("test");
    metadata_->dropTableIfExists("test2");
  };

  auto tableType = ROW({
      {"key1", BIGINT()},
      {"key2", INTEGER()},
      {"data", BIGINT()},
      {"data2", VARCHAR()},
      {"ds", VARCHAR()},
  });

  folly::F14FastMap<std::string, velox::Variant> options = {
      {"file_format", "parquet"},
      {"compression_kind", "snappy"},
  };

  createTable("test", tableType, options);

  static constexpr vector_size_t kTestBatchSize = 2048;
  auto data = makeTestData(10, kTestBatchSize);

  lp::PlanBuilder::Context context(exec::test::kHiveConnectorId);
  auto writePlan = lp::PlanBuilder(context)
                       .values({data})
                       .tableWrite(
                           exec::test::kHiveConnectorId,
                           "test",
                           WriteKind::kInsert,
                           {"key1", "key2", "data", "ds"},
                           {"key1", "key2", "data", "ds"})
                       .build();
  checkWrittenRows(runVelox(writePlan), kTestBatchSize * 10);

  auto countTestTable = [&] {
    auto countPlan = lp::PlanBuilder(context)
                         .tableScan("test")
                         .aggregate({}, {"count(1)"})
                         .build();

    auto result = runVelox(countPlan);
    return result.results[0]->childAt(0)->as<FlatVector<int64_t>>()->valueAt(0);
  };

  EXPECT_EQ(kTestBatchSize * 10, countTestTable());

  auto errorPlan = lp::PlanBuilder(context)
                       .values(makeTestData(100, kTestBatchSize))
                       .tableWrite(
                           exec::test::kHiveConnectorId,
                           "test",
                           WriteKind::kInsert,
                           {"key1", "key2", "data", "ds"},
                           {"key1", "key2", "key1 % (key1 - 200000)", "ds"})
                       .build();
  VELOX_ASSERT_THROW(runVelox(errorPlan), "divide by");

  EXPECT_EQ(kTestBatchSize * 10, countTestTable());

  std::vector<RowVectorPtr> expectedData;
  expectedData.reserve(data.size());
  for (const auto& vector : data) {
    expectedData.emplace_back(makeRowVector({
        vector->childAt(0),
        vector->childAt(1),
        vector->childAt(2),
        /*data2*/ makeAllNullFlatVector<std::string>(vector->size()),
        vector->childAt(3),
    }));
  }

  {
    auto readPlan = lp::PlanBuilder(context).tableScan("test").build();
    checkSameSingleNode(readPlan, expectedData);
  }

  // Create a second table to copy the first one into.
  createTable("test2", tableType, options);

  auto copyPlan = lp::PlanBuilder(context)
                      .tableScan("test")
                      .tableWrite(
                          exec::test::kHiveConnectorId,
                          "test2",
                          WriteKind::kInsert,
                          {"key1", "key2", "data", "data2", "ds"},
                          {"key1", "key2", "data", "data2", "ds"})
                      .build();
  checkWrittenRows(runVelox(copyPlan), kTestBatchSize * 10);

  {
    auto readPlan = lp::PlanBuilder(context).tableScan("test2").build();
    checkSameSingleNode(readPlan, expectedData);
  }
}

TEST_F(WriteTest, insertSql) {
  SCOPE_EXIT {
    metadata_->dropTableIfExists("test");
  };

  createTable(
      "test", ROW({"a", "b", "c"}, {BIGINT(), DOUBLE(), VARCHAR()}), {});

  auto parseSql = [&](std::string_view sql) {
    test::PrestoParser parser(exec::test::kHiveConnectorId, pool());

    auto statement = parser.parse(sql);
    VELOX_CHECK(statement->isInsert());

    return statement->asUnchecked<test::InsertStatement>()->plan();
  };

  {
    auto logicalPlan = parseSql("INSERT INTO test SELECT 1, 0.123, 'foo'");
    checkWrittenRows(runVelox(logicalPlan), 1);
  }

  {
    auto logicalPlan =
        parseSql("INSERT INTO test(c, a, b) SELECT 'bar', 2, 1.23");
    checkWrittenRows(runVelox(logicalPlan), 1);
  }

  {
    auto logicalPlan = parseSql(
        "INSERT INTO test(a, b) "
        "SELECT x, x * 0.1 FROM unnest(array[3, 4, 5]) as t(x)");
    checkWrittenRows(runVelox(logicalPlan), 3);
  }

  checkTableData(
      "test",
      makeRowVector({
          makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
          makeFlatVector<double>({0.123, 1.23, 0.3, 0.4, 0.5}),
          makeNullableFlatVector<std::string>(
              {"foo", "bar", std::nullopt, std::nullopt, std::nullopt}),
      }));
}

TEST_F(WriteTest, createTableAsSelectSql) {
  {
    SCOPE_EXIT {
      metadata_->dropTableIfExists("test");
    };

    runCtas("CREATE TABLE test(a, b, c) AS SELECT 1, 0.123, 'foo'", 1);

    ASSERT_TRUE(metadata_->findTable("test") != nullptr);
    checkTableData(
        "test",
        makeRowVector({
            makeFlatVector<int64_t>({1}),
            makeFlatVector<double>({0.123}),
            makeFlatVector<std::string>({"foo"}),
        }));
  }

  {
    SCOPE_EXIT {
      metadata_->dropTableIfExists("test");
    };

    runCtas(
        "CREATE TABLE test AS "
        "SELECT x, x * 0.1 as y FROM unnest(array[1, 2, 3]) as t(x)",
        3);

    ASSERT_TRUE(metadata_->findTable("test") != nullptr);
    checkTableData(
        "test",
        makeRowVector({
            makeFlatVector<int64_t>({1, 2, 3}),
            makeFlatVector<double>({0.1, 0.2, 0.3}),
        }));
  }

  // Verify that newly created table is deleted if write fails.
  {
    SCOPE_EXIT {
      metadata_->dropTableIfExists("test");
    };

    VELOX_ASSERT_THROW(
        runCtas("CREATE TABLE test(a, b, c) AS SELECT 1, 0.123, 123 % 0", 0),
        "Cannot divide by 0");

    ASSERT_TRUE(metadata_->findTable("test") == nullptr);
  }
}

TEST_F(WriteTest, createTableAsSelectPartitionedSql) {
  SCOPE_EXIT {
    metadata_->dropTableIfExists("test");
  };

  runCtas(
      "CREATE TABLE test WITH (partitioned_by = ARRAY['pk']) AS "
      "SELECT n_nationkey, n_name, n_nationkey % 3 as pk FROM nation",
      25);

  auto table = metadata_->findTable("test");
  ASSERT_TRUE(table != nullptr);

  ASSERT_EQ(1, table->layouts().size());

  auto layout =
      table->layouts().at(0)->as<connector::hive::LocalHiveTableLayout>();
  ASSERT_EQ(1, layout->hivePartitionColumns().size());
  ASSERT_EQ("pk", layout->hivePartitionColumns().at(0)->name());

  ASSERT_EQ(0, layout->partitionColumns().size());
}

TEST_F(WriteTest, createTableAsSelectBucketedSql) {
  {
    SCOPE_EXIT {
      metadata_->dropTableIfExists("test");
    };

    runCtas(
        "CREATE TABLE test WITH (bucket_count = 4, bucketed_by = ARRAY['b']) AS "
        "SELECT n_nationkey, n_name, n_nationkey % 4 as b FROM nation",
        25);

    auto table = metadata_->findTable("test");
    ASSERT_TRUE(table != nullptr);

    ASSERT_EQ(1, table->layouts().size());

    auto layout =
        table->layouts().at(0)->as<connector::hive::LocalHiveTableLayout>();
    ASSERT_EQ(1, layout->partitionColumns().size());
    ASSERT_EQ("b", layout->partitionColumns().at(0)->name());

    ASSERT_EQ(4, layout->numBuckets());

    ASSERT_EQ(0, layout->orderColumns().size());
    ASSERT_EQ(0, layout->sortOrder().size());
    ASSERT_EQ(0, layout->hivePartitionColumns().size());
  }

  {
    SCOPE_EXIT {
      metadata_->dropTableIfExists("test");
    };

    runCtas(
        "CREATE TABLE test WITH (bucket_count = 4, bucketed_by = ARRAY['b'], sorted_by = ARRAY['b']) AS "
        "SELECT n_nationkey, n_name, n_nationkey % 4 as b FROM nation",
        25);

    auto table = metadata_->findTable("test");
    ASSERT_TRUE(table != nullptr);

    ASSERT_EQ(1, table->layouts().size());

    auto layout =
        table->layouts().at(0)->as<connector::hive::LocalHiveTableLayout>();
    ASSERT_EQ(1, layout->partitionColumns().size());
    ASSERT_EQ("b", layout->partitionColumns().at(0)->name());

    ASSERT_EQ(1, layout->orderColumns().size());
    ASSERT_EQ("b", layout->orderColumns().at(0)->name());

    ASSERT_EQ(1, layout->sortOrder().size());
    ASSERT_TRUE(layout->sortOrder().at(0).isAscending);
    ASSERT_TRUE(layout->sortOrder().at(0).isNullsFirst);

    ASSERT_EQ(4, layout->numBuckets());

    ASSERT_EQ(0, layout->hivePartitionColumns().size());
  }
}

class ExprTranslatorContext : public lp::ExprVisitorContext {
 public:
  velox::core::TypedExprPtr veloxExpr;
};

class ExprTranslator : public lp::ExprVisitor {
 public:
  void visit(
      const lp::InputReferenceExpr& expr,
      lp::ExprVisitorContext& context) const {
    auto& myCtx = static_cast<ExprTranslatorContext&>(context);

    myCtx.veloxExpr =
        std::make_shared<core::FieldAccessTypedExpr>(expr.type(), expr.name());
  }

  void visit(const lp::CallExpr& expr, lp::ExprVisitorContext& context) const {
    auto& myCtx = static_cast<ExprTranslatorContext&>(context);

    std::vector<core::TypedExprPtr> inputs;
    inputs.reserve(expr.inputs().size());
    for (const auto& input : expr.inputs()) {
      input->accept(*this, context);
      inputs.push_back(myCtx.veloxExpr);
    }

    myCtx.veloxExpr =
        std::make_shared<core::CallTypedExpr>(expr.type(), inputs, expr.name());
  }

  void visit(const lp::SpecialFormExpr& expr, lp::ExprVisitorContext& context)
      const {
    VELOX_NYI();
  }

  void visit(const lp::AggregateExpr& expr, lp::ExprVisitorContext& context)
      const {
    VELOX_NYI();
  }

  void visit(const lp::WindowExpr& expr, lp::ExprVisitorContext& context)
      const {
    VELOX_NYI();
  }

  void visit(const lp::ConstantExpr& expr, lp::ExprVisitorContext& context)
      const {
    auto& myCtx = static_cast<ExprTranslatorContext&>(context);

    myCtx.veloxExpr =
        std::make_shared<core::ConstantTypedExpr>(expr.type(), *expr.value());
  }

  void visit(const lp::LambdaExpr& expr, lp::ExprVisitorContext& context)
      const {
    VELOX_NYI();
  }

  void visit(const lp::SubqueryExpr& expr, lp::ExprVisitorContext& context)
      const {
    VELOX_NYI();
  }
};

velox::Variant WriteTest::evaluateConstantExpr(const lp::Expr& expr) {
  ExprTranslatorContext context;
  ExprTranslator translator;
  expr.accept(translator, context);

  auto result = velox::exec::tryEvaluateConstantExpression(
      context.veloxExpr, pool(), core::QueryCtx::create());

  VELOX_CHECK_NOT_NULL(result);

  return result->variantAt(0);
}

} // namespace
} // namespace facebook::axiom::optimizer
