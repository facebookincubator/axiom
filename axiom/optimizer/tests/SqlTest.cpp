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
#include "axiom/optimizer/tests/SqlFile.h"
#include "axiom/optimizer/tests/SqlTestBase.h"
#include "axiom/optimizer/tests/TestDataPath.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/ConnectorRegistry.h"

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

    suiteConnector_ = std::make_shared<connector::TestConnector>(
        kTestConnectorId,
        /*config=*/nullptr,
        suiteRootPool_);
    velox::connector::ConnectorRegistry::global().insert(
        kTestConnectorId, suiteConnector_);
    connector::ConnectorMetadataRegistry::global().insert(
        kTestConnectorId, suiteConnector_->metadata());

    suiteDuckDbRunner_ = std::make_unique<exec::test::DuckDbQueryRunner>();

    for (const auto& statement : setupStatements) {
      runSetupStatement(
          statement,
          *suiteConnector_,
          *suiteDuckDbRunner_,
          [](const logical_plan::LogicalPlanNodePtr& plan) {
            const auto buildRunner =
                UseV2 ? makeLocalRunnerV2 : makeLocalRunner;
            return buildRunner(
                plan,
                suiteExecutor_.get(),
                /*asyncDataCache=*/nullptr,
                suiteRootPool_,
                suiteOptimizerPool_,
                /*numWorkers=*/4,
                /*numDrivers=*/4,
                /*syntacticJoinOrder=*/false);
          });
    }
  }

  static void TearDownTestCase() {
    suiteDuckDbRunner_.reset();
    connector::ConnectorMetadataRegistry::global().erase(kTestConnectorId);
    velox::connector::ConnectorRegistry::global().erase(kTestConnectorId);
    suiteConnector_.reset();
    suiteOptimizerPool_.reset();
    suiteRootPool_.reset();
    suiteExecutor_.reset();
    suiteManager_.reset();
  }

  // Setup statements consumed by SetUpTestCase, populated at registration time.
  static std::vector<std::string> setupStatements;

 protected:
  bool createsConnectorPerTest() const override {
    return false;
  }

  exec::test::DuckDbQueryRunner& duckDbRunner() override {
    return *suiteDuckDbRunner_;
  }

  void SetUp() override {
    SqlTestBase::SetUp();
    // connector_ is suite-scoped, set once per suite rather than per test.
    connector_ = suiteConnector_;
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

// Registers every query in 'file' as an individual gtest test under the suite
// 'V1/SqlTest_<name>' or 'V2/SqlTest_<name>' (per UseV2). Each query runs under
// both cost-based (default) and syntactic join ordering.
template <FileName Name, bool UseV2>
void registerVariant(const SqlFile& file, const std::string& path) {
  SqlTest<Name, UseV2>::setupStatements = file.setupStatements;

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

// Parses a .sql file and registers its queries under both the v1 and v2
// suites.
template <FileName Name>
void registerQueryFile() {
  const std::string baseName = Name.value;
  const auto path = getTestFilePath(fmt::format("sql/{}.sql", baseName));

  std::string content;
  VELOX_CHECK(
      folly::readFile(path.c_str(), content), "Failed to read: {}", path);
  auto baseDir = std::filesystem::path(path).parent_path().string();
  auto file = SqlFile::parse(content, baseDir);

  registerVariant<Name, false>(file, path);
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
  registerQueryFile<"distinctAggregation">();
  registerQueryFile<"groupingsets">();
  registerQueryFile<"join">();
  registerQueryFile<"limit">();
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
