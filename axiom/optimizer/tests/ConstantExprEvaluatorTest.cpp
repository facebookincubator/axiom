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

#include "axiom/optimizer/ConstantExprEvaluator.h"
#include <gtest/gtest.h>
#include "axiom/connectors/tests/TestConnectorContext.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/sql/presto/PrestoParser.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

namespace facebook::axiom::optimizer {
namespace {

class ConstantExprEvaluatorTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance(
        velox::memory::MemoryManager::Options{});
    velox::functions::prestosql::registerAllScalarFunctions();
  }

  // Evaluates the constant expression 'sql'.
  static velox::Variant evaluate(std::string_view sql) {
    ::axiom::sql::presto::PrestoParser parser(
        "test",
        "default",
        std::make_shared<::axiom::sql::presto::ParserSession>(
            connector::makeTestContext("test"),
            FunctionRegistry::testingCreate(),
            connector::makeTestStatWriter(),
            connector::Properties{},
            ::axiom::sql::presto::ParserOptions{}));
    return ConstantExprEvaluator::evaluateConstantExpr(
        *parser.parseExpression(sql));
  }
};

TEST_F(ConstantExprEvaluatorTest, evaluate) {
  const std::vector<std::pair<std::string, velox::Variant>> cases = {
      {
          "1 + 2",
          velox::Variant(int32_t{3}),
      },
      {
          "CAST('7' AS BIGINT)",
          velox::Variant(int64_t{7}),
      },
      {
          "TRY_CAST('x' AS BIGINT)",
          velox::Variant::null(velox::TypeKind::BIGINT),
      },
      {
          "ROW(1, 'a')",
          velox::Variant::row({
              velox::Variant(int32_t{1}),
              velox::Variant("a"),
          }),
      },
      {
          "CAST(ROW(1 + 1, 'b') AS ROW(x INTEGER, y VARCHAR))",
          velox::Variant::row({
              velox::Variant(int32_t{2}),
              velox::Variant("b"),
          }),
      },
  };
  for (const auto& [sql, expected] : cases) {
    SCOPED_TRACE(sql);
    EXPECT_EQ(expected, evaluate(sql));
  }
}

} // namespace
} // namespace facebook::axiom::optimizer
