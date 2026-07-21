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

#include "axiom/cli/tests/SqlQueryRunnerTestBase.h"

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "velox/connectors/ConnectorRegistry.h"

using namespace facebook::velox;

namespace axiom::sql {

const std::string SqlQueryRunnerTestBase::kDefaultSchema{
    facebook::axiom::connector::TestConnector::kDefaultSchema};

void SqlQueryRunnerTestBase::SetUpTestCase() {
  memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
}

void SqlQueryRunnerTestBase::SetUp() {
  runner_ = makeRunner();
}

void SqlQueryRunnerTestBase::TearDown() {
  runner_.reset();
  for (const auto& id : connectorIds_) {
    facebook::axiom::connector::ConnectorMetadataRegistry::global().erase(id);
    connector::ConnectorRegistry::global().erase(id);
  }
}

std::unique_ptr<SqlQueryRunner> SqlQueryRunnerTestBase::makeRunner(
    const std::string& connectorId,
    std::function<std::string()> queryIdGenerator,
    PermissionCheck permissionCheck,
    bool useOptimizerV2) {
  auto runner = std::make_unique<SqlQueryRunner>(
      "test_user", &progressScheduler_, useOptimizerV2);

  auto initConnectors = [&]() {
    testConnector_ =
        std::make_shared<facebook::axiom::connector::TestConnector>(
            connectorId);
    connector::ConnectorRegistry::global().insert(
        testConnector_->connectorId(), testConnector_);
    facebook::axiom::connector::ConnectorMetadataRegistry::global().insert(
        testConnector_->connectorId(), testConnector_->metadata());

    connectorIds_.emplace_back(testConnector_->connectorId());

    return std::make_pair(testConnector_->connectorId(), kDefaultSchema);
  };

  runner->initialize(
      initConnectors, std::move(permissionCheck), std::move(queryIdGenerator));

  return runner;
}

SqlQueryRunner::SqlResult SqlQueryRunnerTestBase::run(std::string_view sql) {
  return runner_->run(sql, {});
}

RowVectorPtr SqlQueryRunnerTestBase::fetchSingleRow(
    std::string_view sql,
    const SqlQueryRunner::RunOptions& options) {
  auto result = runner_->run(sql, options);
  VELOX_CHECK(!result.message.has_value(), "Query failed: {}", *result.message);
  VELOX_CHECK_EQ(1, result.results.size());
  VELOX_CHECK_EQ(1, result.results[0]->size());
  return result.results[0];
}

} // namespace axiom::sql
