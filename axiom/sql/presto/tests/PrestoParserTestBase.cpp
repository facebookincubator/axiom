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

#include "axiom/sql/presto/tests/PrestoParserTestBase.h"
#include "axiom/logical_plan/ExprPrinter.h"
#include "axiom/logical_plan/PlanPrinter.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"

namespace axiom::sql::presto::test {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

void PrestoParserTestBase::SetUpTestCase() {
  memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});

  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  window::prestosql::registerAllWindowFunctions();
}

void PrestoParserTestBase::SetUp() {
  connector_ =
      std::make_shared<facebook::axiom::connector::TestConnector>(kConnectorId);
  facebook::velox::connector::registerConnector(connector_);
  connector_->addTpchTables();
}

void PrestoParserTestBase::TearDown() {
  facebook::velox::connector::unregisterConnector(kConnectorId);
  connector_.reset();
}

SqlStatementPtr PrestoParserTestBase::parseSql(std::string_view sql) {
  SCOPED_TRACE(sql);
  auto parser = makeParser();

  return parser.parse(sql, true);
}

void PrestoParserTestBase::testExplain(
    std::string_view sql,
    lp::test::LogicalPlanMatcherBuilder& matcher) {
  SCOPED_TRACE(sql);
  auto parser = makeParser();

  auto statement = parser.parse(sql);
  ASSERT_TRUE(statement->isExplain());

  auto* explainStatement = statement->as<ExplainStatement>();
  ASSERT_FALSE(explainStatement->isAnalyze());
  ASSERT_TRUE(explainStatement->type() == ExplainStatement::Type::kExecutable);

  if (explainStatement->statement()->isSelect()) {
    auto* selectStatement =
        explainStatement->statement()->as<SelectStatement>();

    auto logicalPlan = selectStatement->plan();
    ASSERT_TRUE(matcher.build()->match(logicalPlan))
        << lp::PlanPrinter::toText(*logicalPlan);
  } else if (explainStatement->statement()->isInsert()) {
    auto* insertStatement =
        explainStatement->statement()->as<InsertStatement>();

    auto logicalPlan = insertStatement->plan();
    ASSERT_TRUE(matcher.build()->match(logicalPlan))
        << lp::PlanPrinter::toText(*logicalPlan);
  } else {
    FAIL() << "Unexpected statement: "
           << explainStatement->statement()->kindName();
  }
}

void PrestoParserTestBase::testSelect(
    std::string_view sql,
    lp::test::LogicalPlanMatcherBuilder& matcher,
    const std::unordered_set<std::string>& views) {
  SCOPED_TRACE(sql);
  auto parser = makeParser();

  auto statement = parser.parse(sql, true);
  ASSERT_TRUE(statement->isSelect());

  auto* selectStatement = statement->as<SelectStatement>();

  auto logicalPlan = selectStatement->plan();
  ASSERT_TRUE(matcher.build()->match(logicalPlan))
      << lp::PlanPrinter::toText(*logicalPlan);

  ASSERT_EQ(views.size(), selectStatement->views().size());

  for (const auto& view : views) {
    ASSERT_TRUE(selectStatement->views().contains({kConnectorId, view}))
        << "Missing view: " << view;
  }
}

void PrestoParserTestBase::testInsert(
    std::string_view sql,
    lp::test::LogicalPlanMatcherBuilder& matcher) {
  SCOPED_TRACE(sql);
  auto parser = makeParser();

  auto statement = parser.parse(sql);
  ASSERT_TRUE(statement->isInsert());

  auto insertStatement = statement->as<InsertStatement>();

  auto logicalPlan = insertStatement->plan();
  ASSERT_TRUE(matcher.build()->match(logicalPlan))
      << lp::PlanPrinter::toText(*logicalPlan);
}

void PrestoParserTestBase::testCtas(
    std::string_view sql,
    const std::string& tableName,
    const RowTypePtr& tableSchema,
    lp::test::LogicalPlanMatcherBuilder& matcher,
    const std::unordered_map<std::string, std::string>& properties) {
  SCOPED_TRACE(sql);
  auto parser = makeParser();

  auto statement = parser.parse(sql);
  ASSERT_TRUE(statement->isCreateTableAsSelect());

  auto ctasStatement = statement->as<CreateTableAsSelectStatement>();

  ASSERT_EQ(ctasStatement->tableName(), tableName);
  ASSERT_TRUE(*ctasStatement->tableSchema() == *tableSchema);

  auto logicalPlan = ctasStatement->plan();
  ASSERT_TRUE(matcher.build()->match(logicalPlan))
      << lp::PlanPrinter::toText(*logicalPlan);

  const auto& actualProperties = ctasStatement->properties();
  ASSERT_EQ(properties.size(), actualProperties.size());

  for (const auto& [key, value] : properties) {
    ASSERT_TRUE(actualProperties.contains(key));
    ASSERT_EQ(lp::ExprPrinter::toText(*actualProperties.at(key)), value);
  }
}

void PrestoParserTestBase::testCreateTable(
    std::string_view sql,
    const std::string& tableName,
    const RowTypePtr& tableSchema,
    const std::unordered_map<std::string, std::string>& properties,
    const std::vector<CreateTableStatement::Constraint>& constraints) {
  SCOPED_TRACE(sql);
  auto parser = makeParser();

  auto statement = parser.parse(sql);
  ASSERT_TRUE(statement->isCreateTable());

  auto* createTable = statement->as<CreateTableStatement>();

  ASSERT_EQ(createTable->tableName(), tableName);
  ASSERT_TRUE(*createTable->tableSchema() == *tableSchema);

  const auto& actualProperties = createTable->properties();
  ASSERT_EQ(properties.size(), actualProperties.size());
  for (const auto& [key, value] : properties) {
    ASSERT_TRUE(actualProperties.contains(key));
    ASSERT_EQ(lp::ExprPrinter::toText(*actualProperties.at(key)), value);
  }

  const auto& actualConstraints = createTable->constraints();
  ASSERT_EQ(constraints.size(), actualConstraints.size());
  for (size_t i = 0; i < constraints.size(); ++i) {
    ASSERT_EQ(constraints[i].name, actualConstraints[i].name);
    ASSERT_EQ(constraints[i].type, actualConstraints[i].type);
    ASSERT_EQ(constraints[i].columns, actualConstraints[i].columns);
  }
}

} // namespace axiom::sql::presto::test
