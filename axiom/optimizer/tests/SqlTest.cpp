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

// Process-wide setup/teardown for the SqlTest binary. SqlTest<Name>'s
// per-suite SetUpTestCase shadows the inherited SqlTestBase::SetUpTestCase,
// so the gtest-driven once-per-process initialization that a non-templated
// SqlTestBase consumer would otherwise get is moved here. gtest's
// Environment lifecycle fires once per RUN_ALL_TESTS invocation and only
// when tests actually run, which makes it the right home for non-idempotent
// global state (function registries, SharedArbitrator factory, etc.).
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

// Per-.sql-file gtest fixture, parameterized by the file's base name. Each
// distinct base name instantiates a distinct fixture class, giving every
// .sql file its own SetUpTestCase/TearDownTestCase scope and its own
// suite-scoped state: a standalone MemoryManager (separate from the
// per-test singleton that OperatorTestBase resets), a TestConnector,
// reference tables installed once, and a DuckDbQueryRunner.
//
// The standalone MemoryManager is the key that lets fixture state survive
// the per-test resetMemory() call: pools created from it are not bound to
// the singleton, so testingSetInstance does not invalidate them.
template <FileName Name>
class SqlTest : public SqlTestBase {
 public:
  explicit SqlTest(QueryEntry entry) : entry_(std::move(entry)) {}

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
            return makeLocalRunner(
                plan,
                suiteExecutor_.get(),
                /*asyncDataCache=*/nullptr,
                suiteRootPool_,
                suiteOptimizerPool_);
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

  // Setup statements parsed once at registration time and consumed by
  // SetUpTestCase. registerQueryFile() populates this when it parses the
  // file to register individual tests. Public so the free-function
  // registerQueryFile() can assign it.
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
    // The base class skipped per-test connector creation because of the
    // override above; point connector_ at the suite-scoped one.
    connector_ = suiteConnector_;
  }

  void TearDown() override {
    // Detach but do not unregister; the suite owns the connector.
    connector_.reset();
    SqlTestBase::TearDown();
  }

  void TestBody() override {
    try {
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
        case QueryEntry::Type::kError:
          assertFailure(entry_.sql, entry_.expectedError);
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

template <FileName Name>
std::unique_ptr<memory::MemoryManager> SqlTest<Name>::suiteManager_;

template <FileName Name>
std::shared_ptr<folly::CPUThreadPoolExecutor> SqlTest<Name>::suiteExecutor_;

template <FileName Name>
std::shared_ptr<memory::MemoryPool> SqlTest<Name>::suiteRootPool_;

template <FileName Name>
std::shared_ptr<memory::MemoryPool> SqlTest<Name>::suiteOptimizerPool_;

template <FileName Name>
std::shared_ptr<connector::TestConnector> SqlTest<Name>::suiteConnector_;

template <FileName Name>
std::unique_ptr<exec::test::DuckDbQueryRunner>
    SqlTest<Name>::suiteDuckDbRunner_;

template <FileName Name>
std::vector<std::string> SqlTest<Name>::setupStatements;

// Registers all queries from a .sql file as individual gtest tests under a
// per-file fixture class 'SqlTest<Name>'. Each test instance picks its
// QueryEntry comes from the closure captured at RegisterTest time.
template <FileName Name>
void registerQueryFile() {
  const std::string baseName = Name.value;
  const auto path = getTestFilePath(fmt::format("sql/{}.sql", baseName));

  std::string content;
  VELOX_CHECK(
      folly::readFile(path.c_str(), content), "Failed to read: {}", path);
  auto baseDir = std::filesystem::path(path).parent_path().string();
  auto file = SqlFile::parse(content, baseDir);

  // Stash the setup statements where SetUpTestCase can find them.
  SqlTest<Name>::setupStatements = std::move(file.setupStatements);

  auto suiteName = fmt::format("SqlTest_{}", baseName);

  for (const auto& entry : file.entries) {
    auto testName = fmt::format("{}_l{}", baseName, entry.lineNumber);
    auto capturedEntry = entry;
    testing::RegisterTest(
        suiteName.c_str(),
        testName.c_str(),
        /*type_param=*/nullptr,
        /*value_param=*/nullptr,
        path.c_str(),
        entry.lineNumber,
        [capturedEntry = std::move(capturedEntry)]() -> SqlTest<Name>* {
          return new SqlTest<Name>(capturedEntry);
        });
  }
}

} // namespace
} // namespace facebook::axiom::optimizer::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);

  using namespace facebook::axiom::optimizer::test;

  testing::AddGlobalTestEnvironment(new SqlTestEnvironment);

  registerQueryFile<"basic">();
  registerQueryFile<"join">();
  registerQueryFile<"subquery">();
  registerQueryFile<"window">();
  registerQueryFile<"set">();
  registerQueryFile<"limit">();
  registerQueryFile<"aggregation">();
  registerQueryFile<"subfield">();
  registerQueryFile<"nullif">();
  registerQueryFile<"coercion">();
  registerQueryFile<"distinctAggregation">();
  registerQueryFile<"unionAll">();
  registerQueryFile<"unionAllFlatten">();

  return RUN_ALL_TESTS();
}
