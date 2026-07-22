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
#pragma once

#include <folly/executors/FunctionScheduler.h>
#include <gtest/gtest.h>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "axiom/cli/SqlQueryRunner.h"
#include "axiom/connectors/tests/TestConnector.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace axiom::sql {

// Shared fixture for tests that drive queries through a SqlQueryRunner backed
// by a TestConnector. makeRunner() selects the optimizer, so a derived fixture
// can parameterize over v1/v2.
class SqlQueryRunnerTestBase : public ::testing::Test,
                               public facebook::velox::test::VectorTestBase {
 protected:
  // Installs the process-wide Velox MemoryManager the runner allocates from.
  static void SetUpTestCase();

  // Builds runner_ for the optimizer selected by useV2_ (default v1). A derived
  // fixture selects v2 by setting useV2_ before calling this.
  void SetUp() override;

  // Destroys runner_ and unregisters every connector makeRunner() created.
  void TearDown() override;

  // Builds a SqlQueryRunner over a freshly created TestConnector registered
  // under 'connectorId', wired as the default connector/schema. The connector
  // is unregistered in TearDown. Selects the optimizer per 'useV2_', which a
  // parameterized fixture sets before calling.
  std::unique_ptr<SqlQueryRunner> makeRunner(
      const std::string& connectorId = "test",
      std::function<std::string()> queryIdGenerator = {},
      PermissionCheck permissionCheck = {});

  // Runs 'sql' on runner_ with default options.
  SqlQueryRunner::SqlResult run(std::string_view sql);

  // Runs 'sql', which must succeed and return exactly one row, and returns it.
  facebook::velox::RowVectorPtr fetchSingleRow(
      std::string_view sql,
      const SqlQueryRunner::RunOptions& options = {});

  // Runs 'sql', which must return a single row and a single column, and returns
  // that value.
  template <typename T>
  T fetchSingleValue(std::string_view sql) {
    return fetchSingleRow(sql)->childAt(0)->variantAt(0).value<T>();
  }

  // Default schema of the TestConnectors makeRunner() creates.
  static const std::string kDefaultSchema;

  // Drives progress polling for runners built by makeRunner(); declared before
  // runner_ so it outlives any reporter a query starts.
  folly::FunctionScheduler progressScheduler_;
  std::unique_ptr<SqlQueryRunner> runner_;
  std::shared_ptr<facebook::axiom::connector::TestConnector> testConnector_;

  // Selects the optimizer makeRunner() builds; a parameterized fixture sets
  // this before calling makeRunner() and branches its expectations on v1 vs v2.
  bool useV2_{false};

 private:
  std::vector<std::string> connectorIds_;
};

} // namespace axiom::sql
