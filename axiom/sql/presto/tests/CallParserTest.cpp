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

#include <folly/coro/Task.h>
#include "axiom/logical_plan/Expr.h"
#include "axiom/sql/presto/tests/ExprMatcher.h"
#include "axiom/sql/presto/tests/PrestoParserTestBase.h"
#include "velox/parse/ExpressionsParser.h"

namespace axiom::sql::presto::test {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;
using facebook::axiom::connector::Procedure;
using facebook::axiom::connector::ProcedureParameter;
using facebook::axiom::logical_plan::test::ExprMatcher;

namespace {

// The parser only binds arguments; it never executes the procedure. 'execute'
// throws so that any accidental execution fails the test loudly.
Procedure makeProcedure(std::vector<ProcedureParameter> parameters) {
  Procedure procedure;
  procedure.parameters = std::move(parameters);
  procedure.execute =
      [](const facebook::axiom::connector::ConnectorSessionPtr&,
         const std::vector<Variant>&) -> folly::coro::Task<void> {
    VELOX_FAIL("Procedure must not be executed during parsing");
  };
  return procedure;
}

class CallParserTest : public PrestoParserTestBase {
 protected:
  void SetUp() override {
    PrestoParserTestBase::SetUp();
    // increment(Name VARCHAR, Step BIGINT = 1): a required VARCHAR and an
    // optional BIGINT that defaults to 1. Parameters are declared in mixed case
    // to exercise case-insensitive argument matching.
    connector_->metadata()->addProcedure(
        {std::string(kDefaultSchema), "increment"},
        makeProcedure({
            {"Name", VARCHAR(), std::nullopt},
            {"Step", BIGINT(), Variant(static_cast<int64_t>(1))},
        }));
  }

  // Parses 'sql' and asserts it is a CALL.
  std::shared_ptr<const CallStatement> parseCall(std::string_view sql) {
    auto call = std::dynamic_pointer_cast<const CallStatement>(parseSql(sql));
    EXPECT_NE(call, nullptr);
    return call;
  }

  facebook::velox::core::ExprPtr parseExpr(const std::string& expr) {
    return exprParser_.parseExpr(expr);
  }

  // Parses a CALL and verifies its procedure name and bound arguments. Each
  // expected argument is matched structurally against the parsed expression.
  void verifyCall(
      std::string_view sql,
      const std::string& procedure,
      const std::vector<std::string>& expectedArguments) {
    auto call = parseCall(sql);

    EXPECT_EQ(call->connectorId(), kConnectorId);
    EXPECT_EQ(call->procedureName().schema, kDefaultSchema);
    EXPECT_EQ(call->procedureName().procedure, procedure);

    ASSERT_EQ(call->arguments().size(), expectedArguments.size());
    for (size_t i = 0; i < expectedArguments.size(); ++i) {
      ExprMatcher::match(call->arguments()[i], parseExpr(expectedArguments[i]));
    }
  }

  facebook::velox::parse::DuckSqlExpressionsParser exprParser_;
};

TEST_F(CallParserTest, positional) {
  // Positional arguments bind in order; 'step' (INTEGER literal) is coerced to
  // the declared BIGINT parameter type.
  verifyCall(
      "CALL increment('counter', 5)",
      "increment",
      {"'counter'", "cast(5 as bigint)"});

  // A schema-qualified name resolves the same procedure.
  verifyCall(
      "CALL default.increment('counter', 5)",
      "increment",
      {"'counter'", "cast(5 as bigint)"});

  // The omitted optional 'step' is filled from its default of 1.
  verifyCall("CALL increment('counter')", "increment", {"'counter'", "1"});

  // A constant expression argument is bound unevaluated (folded by the runner)
  // and coerced to the parameter type.
  verifyCall(
      "CALL increment('counter', 1 + 2)",
      "increment",
      {"'counter'", "cast(1 + 2 as bigint)"});

  verifyCall(
      "CALL increment('counter', 1 / 0)",
      "increment",
      {"'counter'", "cast(1 / 0 as bigint)"});

  // An argument whose type cannot be coerced to the parameter type is rejected.
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSql("CALL increment('counter', rand())"),
      "Cannot coerce procedure argument from DOUBLE to BIGINT");
}

TEST_F(CallParserTest, named) {
  // Named arguments bind by name and are reordered to declared order. Checking
  // 'step's value distinguishes it from the default, so dropping it would fail.
  verifyCall(
      "CALL increment(name => 'counter')", "increment", {"'counter'", "1"});

  verifyCall(
      "CALL increment(step => 5, name => 'counter')",
      "increment",
      {"'counter'", "cast(5 as bigint)"});

  verifyCall(
      "CALL increment(step => 1 + 2, name => 'counter')",
      "increment",
      {"'counter'", "cast(1 + 2 as bigint)"});

  // Argument names are matched case-insensitively: lowercase 'name'/'step' and
  // uppercase NAME/STEP both bind to the declared 'Name'/'Step' parameters.
  verifyCall(
      "CALL increment(STEP => 5, NAME => 'counter')",
      "increment",
      {"'counter'", "cast(5 as bigint)"});
}

TEST_F(CallParserTest, unknownProcedure) {
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSql("CALL no_such_proc('counter')"),
      "Procedure not found: default.no_such_proc");
}

TEST_F(CallParserTest, invalidArguments) {
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSql("CALL increment('counter', 'five')"),
      "Cannot coerce procedure argument from VARCHAR to BIGINT");

  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSql("CALL increment('counter', 5, 'extra')"),
      "Too many arguments for procedure default.increment");

  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSql("CALL increment()"),
      "Missing required procedure argument: Name");

  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSql("CALL increment(name => 'counter', bogus => 'counter')"),
      "Unknown argument name for procedure default.increment: bogus");

  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSql("CALL increment(name => 'a', name => 'b')"),
      "Duplicate argument for procedure default.increment: name");

  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSql("CALL increment('counter', step => 5)"),
      "Named and positional procedure arguments cannot be mixed");
}

} // namespace
} // namespace axiom::sql::presto::test
