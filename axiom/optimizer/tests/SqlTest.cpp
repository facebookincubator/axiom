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

#include <folly/FileUtil.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <filesystem>
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/hive/LocalHiveConnectorMetadata.h"
#include "axiom/optimizer/ConstantExprEvaluator.h"
#include "axiom/optimizer/tests/SqlFile.h"
#include "axiom/optimizer/tests/SqlTestBase.h"
#include "axiom/optimizer/tests/TestDataPath.h"
#include "axiom/runner/LocalRunner.h"
#include "axiom/sql/presto/PrestoParser.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"

namespace facebook::axiom::optimizer::test {
namespace {

using namespace facebook::velox;

// Runs process-global, non-idempotent initialization (function registries,
// SharedArbitrator factory, etc.) exactly once per RUN_ALL_TESTS. The
// per-suite SetUpTestCase below shadows SqlTestBase::SetUpTestCase, so this
// global init cannot live there.
class SqlTestEnvironment : public testing::Environment {
 public:
  void SetUp() override {
    SqlTestBase::SetUpTestCase();
  }

  void TearDown() override {
    exec::test::OperatorTestBase::TearDownTestCase();
  }
};

// Compile-time string wrapper so a .sql file's base name can be passed as a
// non-type template argument. Each distinct string value yields a distinct
// type, which gives every .sql file its own gtest fixture class — and
// therefore its own SetUpTestCase/TearDownTestCase scope.
template <size_t N>
struct FileName {
  // Implicit so 'SqlTest<"basic">' converts the string literal directly.
  /* implicit */ constexpr FileName(const char (&name)[N]) {
    std::copy_n(name, N, value);
  }
  char value[N]{};
};

// Removes a Presto table-property clause ("WITH ( ... )") so the same setup DDL
// can run in DuckDB, which doesn't understand properties like partitioned_by.
// Matches only a "WITH (" table-property clause, not a "WITH cte" CTE.
std::string stripTablePropertiesForDuckDb(const std::string& sql) {
  static constexpr std::string_view kWith = " with (";
  const auto match = std::search(
      sql.begin(), sql.end(), kWith.begin(), kWith.end(), [](char a, char b) {
        return std::tolower(static_cast<unsigned char>(a)) == b;
      });
  if (match == sql.end()) {
    return sql;
  }
  // Position of the space before "WITH" and of the clause's opening '('.
  const size_t pos = static_cast<size_t>(match - sql.begin());
  const size_t open = pos + kWith.size() - 1;
  int depth = 0;
  size_t i = open;
  for (; i < sql.size(); ++i) {
    if (sql[i] == '(') {
      ++depth;
    } else if (sql[i] == ')' && --depth == 0) {
      break;
    }
  }
  return sql.substr(0, pos) + sql.substr(i + 1);
}

// Runs one setup DDL statement against 'metadata's connector: CREATE TABLE
// registers the (possibly partitioned) table with any table properties; INSERT
// / CTAS write data through the connector's write path. Works for any connector
// whose metadata implements createTable (TestConnector, LocalHive). DuckDB
// installation is done by the caller.
void runSetupStatement(
    const std::string& sql,
    const std::string& connectorId,
    const std::string& defaultSchema,
    connector::ConnectorMetadata& metadata,
    const std::function<std::shared_ptr<runner::LocalRunner>(
        const logical_plan::LogicalPlanNodePtr&)>& runnerFactory) {
  ::axiom::sql::presto::PrestoParser parser(
      connectorId,
      defaultSchema,
      std::make_shared<::axiom::sql::presto::ParserSession>(
          /*queryId=*/"test",
          /*user=*/"test",
          ::axiom::sql::presto::ParserOptions{},
          connector::ConnectorProperties{}));
  auto stmt = parser.parse(sql);

  auto session = std::make_shared<connector::ConnectorSession>(
      /*queryId=*/"test", /*user=*/"test", connector::Properties{});

  auto evalOptions = [](const auto& properties) {
    folly::F14FastMap<std::string, velox::Variant> options;
    for (const auto& [key, value] : properties) {
      options[key] =
          optimizer::ConstantExprEvaluator::evaluateConstantExpr(*value);
    }
    return options;
  };

  if (stmt->isCreateTable()) {
    const auto& create =
        *stmt->as<::axiom::sql::presto::CreateTableStatement>();
    metadata.createTable(
        session,
        create.tableName(),
        create.tableSchema(),
        evalOptions(create.properties()),
        /*ifNotExists=*/false,
        /*explain=*/false);
    return;
  }

  logical_plan::LogicalPlanNodePtr plan;
  if (stmt->isInsert()) {
    plan = stmt->as<::axiom::sql::presto::InsertStatement>()->plan();
  } else if (stmt->isCreateTableAsSelect()) {
    const auto& ctas =
        *stmt->as<::axiom::sql::presto::CreateTableAsSelectStatement>();
    metadata.createTable(
        session,
        ctas.tableName(),
        ctas.tableSchema(),
        evalOptions(ctas.properties()),
        /*ifNotExists=*/false,
        /*explain=*/false);
    plan = ctas.plan();
  } else {
    VELOX_USER_FAIL("Unsupported setup statement: {}", sql);
  }

  auto runner = runnerFactory(plan);
  runner->drain([](velox::RowVectorPtr) {});
}

// Per-.sql-file, per-optimizer gtest fixture. Each (file, optimizer) pair is a
// distinct instantiation with its own SetUpTestCase/TearDownTestCase scope and
// its own suite-scoped state: a standalone MemoryManager (separate from the
// per-test singleton that OperatorTestBase resets), a TestConnector, reference
// tables installed once, and a DuckDbQueryRunner. 'UseV2' selects the optimizer
// the queries run through; results are compared against DuckDB either way.
//
// The standalone MemoryManager is the key that lets fixture state survive the
// per-test resetMemory() call: pools created from it are not bound to the
// singleton, so testingSetInstance does not invalidate them.
template <FileName Name, bool UseV2>
class SqlTest : public SqlTestBase {
 public:
  SqlTest(QueryEntry entry, bool syntacticJoinOrder)
      : entry_(std::move(entry)) {
    this->syntacticJoinOrder_ = syntacticJoinOrder;
    this->useV2_ = UseV2;
  }

  // Uses SetUpTestCase / TearDownTestCase rather than the modern
  // SetUpTestSuite / TearDownTestSuite synonyms because SqlTestBase already
  // defines SetUpTestCase via inheritance from OperatorTestBase, and gtest
  // disallows having both the legacy and modern names visible on the same
  // fixture class.
  static void SetUpTestCase() {
    suiteManager_ = std::make_unique<memory::MemoryManager>();
    suiteExecutor_ = std::make_shared<folly::CPUThreadPoolExecutor>(4);
    suiteRootPool_ = suiteManager_->addRootPool(
        fmt::format("sql_test_{}", Name.value),
        memory::kMaxMemory,
        memory::MemoryReclaimer::create());
    suiteOptimizerPool_ = suiteRootPool_->addLeafChild("optimizer");
    suiteDuckDbRunner_ = std::make_unique<exec::test::DuckDbQueryRunner>();

    auto runnerFactory = [](const logical_plan::LogicalPlanNodePtr& plan) {
      const auto buildRunner = UseV2 ? makeLocalRunnerV2 : makeLocalRunner;
      return buildRunner(
          plan,
          suiteExecutor_.get(),
          /*asyncDataCache=*/nullptr,
          suiteRootPool_,
          suiteOptimizerPool_,
          /*numWorkers=*/4,
          /*numDrivers=*/4,
          /*syntacticJoinOrder=*/false);
    };

    connector::ConnectorMetadata* metadata = nullptr;
    if (connectorKind == TestConnectorKind::kLocalHive) {
      setUpHiveConnector();
      metadata = suiteHiveMetadata_;
    } else {
      suiteConnector_ = std::make_shared<connector::TestConnector>(
          kTestConnectorId, /*config=*/nullptr, suiteRootPool_);
      velox::connector::ConnectorRegistry::global().insert(
          kTestConnectorId, suiteConnector_);
      connector::ConnectorMetadataRegistry::global().insert(
          kTestConnectorId, suiteConnector_->metadata());
      metadata = suiteConnector_->metadata().get();
    }

    for (const auto& statement : setupStatements) {
      suiteDuckDbRunner_->execute(stripTablePropertiesForDuckDb(statement));
      runSetupStatement(
          statement, connectorId(), defaultSchema(), *metadata, runnerFactory);
    }
  }

  // Connector id / default schema the file's setup DDL and queries run against.
  static std::string connectorId() {
    return connectorKind == TestConnectorKind::kLocalHive
        ? std::string(velox::exec::test::kHiveConnectorId)
        : std::string(kTestConnectorId);
  }

  static std::string defaultSchema() {
    return connectorKind == TestConnectorKind::kLocalHive
        ? std::string(
              connector::hive::LocalHiveConnectorMetadata::kDefaultSchema)
        : std::string(connector::TestConnector::kDefaultSchema);
  }

  // Builds a suite-scoped LocalHive connector over a temp directory. Setup DDL
  // then runs as real CREATE TABLE / INSERT / CTAS, so data, partition
  // metadata, and write-time stats are on disk for queries to read.
  static void setUpHiveConnector() {
    velox::filesystems::registerLocalFileSystem();
    velox::dwrf::registerDwrfReaderFactory();
    velox::dwrf::registerDwrfWriterFactory();
    velox::dwio::common::registerFileSinks();
    suiteHiveDir_ = velox::common::testutil::TempDirectoryPath::create();

    std::unordered_map<std::string, std::string> config;
    config[connector::hive::HiveMetadataConfig::kLocalDataPath] =
        suiteHiveDir_->getPath();
    config[connector::hive::HiveMetadataConfig::kLocalFileFormat] = "dwrf";

    velox::connector::hive::HiveConnectorFactory factory;
    auto hiveConnector = factory.newConnector(
        std::string(velox::exec::test::kHiveConnectorId),
        std::make_shared<velox::config::ConfigBase>(std::move(config)),
        suiteExecutor_.get());
    velox::connector::ConnectorRegistry::global().insert(
        velox::exec::test::kHiveConnectorId, hiveConnector);

    auto metadata =
        std::make_shared<connector::hive::LocalHiveConnectorMetadata>(
            dynamic_cast<velox::connector::hive::HiveConnector*>(
                hiveConnector.get()),
            suiteManager_->addRootPool("sql_test_hive_metadata"));
    suiteHiveMetadata_ = metadata.get();
    connector::ConnectorMetadataRegistry::global().insert(
        velox::exec::test::kHiveConnectorId, std::move(metadata));
  }

  static void TearDownTestCase() {
    suiteDuckDbRunner_.reset();
    if (connectorKind == TestConnectorKind::kLocalHive) {
      suiteHiveMetadata_ = nullptr;
      connector::ConnectorMetadataRegistry::global().erase(
          velox::exec::test::kHiveConnectorId);
      velox::connector::ConnectorRegistry::global().erase(
          velox::exec::test::kHiveConnectorId);
      velox::dwrf::unregisterDwrfReaderFactory();
      velox::dwrf::unregisterDwrfWriterFactory();
      suiteHiveDir_.reset();
    } else {
      connector::ConnectorMetadataRegistry::global().erase(kTestConnectorId);
      velox::connector::ConnectorRegistry::global().erase(kTestConnectorId);
      suiteConnector_.reset();
    }
    suiteOptimizerPool_.reset();
    suiteRootPool_.reset();
    suiteExecutor_.reset();
    suiteManager_.reset();
  }

  // Setup statements consumed by SetUpTestCase, populated at registration time.
  static std::vector<std::string> setupStatements;

  // Connector the file's queries run against, populated at registration time.
  static TestConnectorKind connectorKind;

 protected:
  exec::test::DuckDbQueryRunner& duckDbRunner() override {
    return *suiteDuckDbRunner_;
  }

  void SetUp() override {
    SqlTestBase::SetUp();
    // connector_ is suite-scoped (null in the hive suite, where queries resolve
    // via the connector registry).
    connector_ = suiteConnector_;
    connectorId_ = connectorId();
    defaultSchema_ = defaultSchema();
  }

  void TearDown() override {
    // Detach but do not unregister; the suite owns the connector.
    connector_.reset();
    SqlTestBase::TearDown();
  }

  void TestBody() override {
    const std::string& expectedError =
        UseV2 ? entry_.expectedErrorV2 : entry_.expectedErrorV1;
    try {
      if (!expectedError.empty()) {
        assertFailure(entry_.sql, expectedError);
        return;
      }
      switch (entry_.type) {
        case QueryEntry::Type::kResults:
          assertResults(entry_.sql, entry_.checkColumnNames, entry_.duckDbSql);
          break;
        case QueryEntry::Type::kOrdered:
          assertOrderedResults(
              entry_.sql, entry_.checkColumnNames, entry_.duckDbSql);
          break;
        case QueryEntry::Type::kCount:
          assertResultCount(entry_.sql, entry_.expectedCount);
          break;
      }
    } catch (const std::exception& e) {
      FAIL() << "Unexpected exception: " << e.what();
    }
  }

 private:
  static std::unique_ptr<memory::MemoryManager> suiteManager_;
  static std::shared_ptr<folly::CPUThreadPoolExecutor> suiteExecutor_;
  static std::shared_ptr<memory::MemoryPool> suiteRootPool_;
  static std::shared_ptr<memory::MemoryPool> suiteOptimizerPool_;
  static std::shared_ptr<connector::TestConnector> suiteConnector_;
  static std::unique_ptr<exec::test::DuckDbQueryRunner> suiteDuckDbRunner_;
  static std::shared_ptr<velox::common::testutil::TempDirectoryPath>
      suiteHiveDir_;
  static connector::hive::LocalHiveConnectorMetadata* suiteHiveMetadata_;

  QueryEntry entry_;
};

template <FileName Name, bool UseV2>
std::unique_ptr<memory::MemoryManager> SqlTest<Name, UseV2>::suiteManager_;

template <FileName Name, bool UseV2>
std::shared_ptr<folly::CPUThreadPoolExecutor>
    SqlTest<Name, UseV2>::suiteExecutor_;

template <FileName Name, bool UseV2>
std::shared_ptr<memory::MemoryPool> SqlTest<Name, UseV2>::suiteRootPool_;

template <FileName Name, bool UseV2>
std::shared_ptr<memory::MemoryPool> SqlTest<Name, UseV2>::suiteOptimizerPool_;

template <FileName Name, bool UseV2>
std::shared_ptr<connector::TestConnector> SqlTest<Name, UseV2>::suiteConnector_;

template <FileName Name, bool UseV2>
std::unique_ptr<exec::test::DuckDbQueryRunner>
    SqlTest<Name, UseV2>::suiteDuckDbRunner_;

template <FileName Name, bool UseV2>
std::vector<std::string> SqlTest<Name, UseV2>::setupStatements;

template <FileName Name, bool UseV2>
TestConnectorKind SqlTest<Name, UseV2>::connectorKind =
    TestConnectorKind::kTest;

template <FileName Name, bool UseV2>
std::shared_ptr<velox::common::testutil::TempDirectoryPath>
    SqlTest<Name, UseV2>::suiteHiveDir_;

template <FileName Name, bool UseV2>
connector::hive::LocalHiveConnectorMetadata*
    SqlTest<Name, UseV2>::suiteHiveMetadata_ = nullptr;

// Registers every query in 'file' as an individual gtest test under the suite
// 'V1/SqlTest_<name>' or 'V2/SqlTest_<name>' (per UseV2). Each query runs under
// both cost-based (default) and syntactic join ordering.
template <FileName Name, bool UseV2>
void registerVariant(const SqlFile& file, const std::string& path) {
  SqlTest<Name, UseV2>::setupStatements = file.setupStatements;
  SqlTest<Name, UseV2>::connectorKind = file.connector;

  const auto suiteName =
      fmt::format("{}/SqlTest_{}", UseV2 ? "V2" : "V1", Name.value);

  for (const auto& entry : file.entries) {
    for (const bool syntacticJoinOrder : {false, true}) {
      auto testName = syntacticJoinOrder
          ? fmt::format("l{}_syntactic", entry.lineNumber)
          : fmt::format("l{}", entry.lineNumber);
      testing::RegisterTest(
          suiteName.c_str(),
          testName.c_str(),
          /*type_param=*/nullptr,
          /*value_param=*/nullptr,
          path.c_str(),
          entry.lineNumber,
          [capturedEntry = entry,
           syntacticJoinOrder]() -> SqlTest<Name, UseV2>* {
            return new SqlTest<Name, UseV2>(capturedEntry, syntacticJoinOrder);
          });
    }
  }
}

// Parses a .sql file and registers its queries. Registers under both the v1 and
// v2 suites by default; when 'v2Only' is set, registers only the v2 suite (for
// features not yet wired in the v1 optimizer).
template <FileName Name>
void registerQueryFile(bool v2Only = false) {
  const std::string baseName = Name.value;
  const auto path = getTestFilePath(fmt::format("sql/{}.sql", baseName));

  std::string content;
  VELOX_CHECK(
      folly::readFile(path.c_str(), content), "Failed to read: {}", path);
  auto baseDir = std::filesystem::path(path).parent_path().string();
  auto file = SqlFile::parse(content, baseDir);

  VELOX_CHECK(
      !file.entries.empty(),
      "SQL test file registered but produced no queries: {}",
      path);

  if (!v2Only) {
    registerVariant<Name, false>(file, path);
  }
  registerVariant<Name, true>(file, path);
}

} // namespace
} // namespace facebook::axiom::optimizer::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);

  using namespace facebook::axiom::optimizer::test;

  testing::AddGlobalTestEnvironment(new SqlTestEnvironment);

  registerQueryFile<"aggregation">();
  registerQueryFile<"basic">();
  registerQueryFile<"coercion">();
  registerQueryFile<"cte">();
  registerQueryFile<"datetime">();
  registerQueryFile<"distinctAggregation">();
  registerQueryFile<"groupingsets">();
  registerQueryFile<"join">();
  registerQueryFile<"limit">();
  registerQueryFile<"metadataAggregate">(/*v2Only=*/true);
  registerQueryFile<"nondeterministic">();
  registerQueryFile<"nullif">();
  registerQueryFile<"set">();
  registerQueryFile<"subfield">();
  registerQueryFile<"subquery">();
  registerQueryFile<"unionAll">();
  registerQueryFile<"unionAllFlatten">();
  registerQueryFile<"window">();

  return RUN_ALL_TESTS();
}
