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

#include "axiom/cli/Connectors.h"
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

std::unique_ptr<SqlQueryRunner> SqlQueryRunnerTestBase::makeRunner(
    const std::string& connectorId,
    std::function<std::string()> queryIdGenerator,
    PermissionCheck permissionCheck,
    LogicalPlanCheck logicalPlanCheck) {
  return makeRunner(
      [&](facebook::axiom::Connectors& connectors) {
        testConnector_ = std::dynamic_pointer_cast<
            facebook::axiom::connector::TestConnector>(
            connectors.registerTestConnector(connectorId));
        VELOX_CHECK_NOT_NULL(testConnector_);

        return std::make_pair(testConnector_->connectorId(), kDefaultSchema);
      },
      std::move(queryIdGenerator),
      std::move(permissionCheck),
      std::move(logicalPlanCheck));
}

std::unique_ptr<SqlQueryRunner> SqlQueryRunnerTestBase::makeRunner(
    const TestConnectorInitializer& initializeConnectors,
    std::function<std::string()> queryIdGenerator,
    PermissionCheck permissionCheck,
    LogicalPlanCheck logicalPlanCheck) {
  auto connectorRegistry =
      facebook::velox::connector::ConnectorRegistry::create(nullptr);
  auto metadataRegistry =
      facebook::axiom::connector::ConnectorMetadataRegistry::create(nullptr);
  auto runner = std::make_unique<SqlQueryRunner>(
      "test_user",
      &progressScheduler_,
      useV2_,
      connectorRegistry,
      metadataRegistry);

  runner->initialize(
      [&]() {
        facebook::axiom::Connectors connectors{
            *connectorRegistry, *metadataRegistry};
        return initializeConnectors(connectors);
      },
      std::move(permissionCheck),
      std::move(logicalPlanCheck),
      std::move(queryIdGenerator));

  facebook::axiom::Connectors connectors{*connectorRegistry, *metadataRegistry};
  connectors.registerSystemConnector(runner->sessionConfig());
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
