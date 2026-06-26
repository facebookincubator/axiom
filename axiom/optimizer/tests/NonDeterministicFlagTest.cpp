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

#include <gtest/gtest.h>
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/tests/QueryTestBase.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;

// Verifies that Field and Lambda propagate the non-deterministic flag from
// their sub-expressions. A Field over a struct that hides rand(), or a Lambda
// whose body draws rand(), must still report non-determinism.
class NonDeterministicFlagTest : public test::QueryTestBase {
 protected:
  void configureTestConnector() override {
    testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
  }

  // Returns whether the single projected expression is deterministic.
  // Wraps `expr` in SELECT <expr> AS p FROM t and inspects the query graph
  // directly (before ToVelox) because the flag is not observable in the
  // rendered Velox plan.
  bool isDeterministic(std::string_view expr) {
    auto sql = "SELECT " + std::string(expr) + " AS p FROM t";
    bool result = true;
    auto plan = parseSelect(sql, kTestConnectorId);
    verifyOptimization(*plan, [&](Optimization& opt) {
      const auto& dt = *opt.rootDt();
      ASSERT_EQ(dt.exprs.size(), 1);
      result = !dt.exprs[0]->containsNonDeterministic();
    });
    return result;
  }
};

TEST_F(NonDeterministicFlagTest, fieldWithoutSubfieldPruning) {
  // With a cast, subfield decomposition is blocked, so the Field over the
  // whole struct is conservatively tainted by the rand() in field1 — even
  // when reading the deterministic field0.
  EXPECT_FALSE(
      isDeterministic("cast(row(a, rand()) AS row(x bigint, y double)).x + 1"));

  // With a cast, reading the rand()-bearing field1 is non-deterministic as
  // the whole struct is non-deterministic.
  EXPECT_FALSE(
      isDeterministic("cast(row(a, rand()) AS row(x bigint, y double)).y"));
}

TEST_F(NonDeterministicFlagTest, fieldWithSubfieldPruning) {
  // Without a cast, row_constructor subfield decomposition drops the
  // unaccessed rand() in field1, so reading field0 is deterministic.
  EXPECT_TRUE(isDeterministic("row(a, rand()).field0 + 1"));

  // Without a cast, reading the rand()-bearing field1 is non-deterministic.
  EXPECT_FALSE(isDeterministic("row(a, rand()).field1"));
}

TEST_F(NonDeterministicFlagTest, lambda) {
  // A Lambda whose body draws rand() propagates non-determinism through the
  // enclosing higher-order call.
  EXPECT_FALSE(isDeterministic(
      "transform(array[a], e -> e + cast(rand() * 10 AS bigint))"));

  // Deterministic lambda body reports determinism.
  EXPECT_TRUE(isDeterministic("transform(array[a], e -> e + b)"));
}

} // namespace
} // namespace facebook::axiom::optimizer
