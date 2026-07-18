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

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/sql/presto/tests/PrestoParserTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace axiom::sql::presto::test {

using namespace facebook::velox;
namespace connector = facebook::axiom::connector;

namespace {

class SqlFunctionTest : public PrestoParserTestBase {
 protected:
  void SetUp() override {
    PrestoParserTestBase::SetUp();
    metadata_ = std::make_shared<connector::TestConnectorMetadata>(nullptr);
    connector::ConnectorMetadataRegistry::global().insert("tc", metadata_);
  }

  void TearDown() override {
    connector::ConnectorMetadataRegistry::global().erase("tc");
    metadata_.reset();
    PrestoParserTestBase::TearDown();
  }

  std::shared_ptr<connector::TestConnectorMetadata> metadata_;
};

// A qualified call is replaced in place by its body, with each argument bound
// to the call-site expression, in projections and predicates alike.
TEST_F(SqlFunctionTest, inlines) {
  // f(x) := x
  metadata_->addFunction(
      {"default", "identity"},
      {
          .returnType = BIGINT(),
          .argumentNames = {"x"},
          .argumentTypes = {BIGINT()},
          .body = "x",
          .defaultNullBehavior = false,
      });
  testSelect(
      "SELECT tc.default.identity(n_nationkey) FROM nation",
      matchScan("nation").project({"n_nationkey"}).output());

  testSelect(
      "SELECT tc.default.identity(123) FROM nation",
      matchScan("nation").project({"123::bigint"}).output());

  testSelect(
      "SELECT tc.default.identity(n_nationkey + 123) FROM nation",
      matchScan("nation").project({"n_nationkey + 123::bigint"}).output());

  // f(a, b) := a + b
  metadata_->addFunction(
      {"default", "combine"},
      {
          .returnType = BIGINT(),
          .argumentNames = {"a", "b"},
          .argumentTypes = {BIGINT(), BIGINT()},
          .body = "a + b",
          .defaultNullBehavior = false,
      });
  testSelect(
      "SELECT tc.default.combine(10, tc.default.combine(n_nationkey, n_regionkey)) "
      "FROM nation",
      matchScan("nation")
          .project({"10::bigint + (n_nationkey + n_regionkey)"})
          .output());

  // f(x) := x * x references its argument more than once; each occurrence is
  // bound to the call-site expression.
  metadata_->addFunction(
      {"default", "square"},
      {
          .returnType = BIGINT(),
          .argumentNames = {"x"},
          .argumentTypes = {BIGINT()},
          .body = "x * x",
          .defaultNullBehavior = false,
      });
  testSelect(
      "SELECT tc.default.square(n_nationkey + n_regionkey) FROM nation",
      matchScan("nation")
          .project(
              {"(n_nationkey + n_regionkey) * (n_nationkey + n_regionkey)"})
          .output());

  // f(x) := case when x < 10 then 1 when x < 20 then 2 else 3 end
  metadata_->addFunction(
      {"default", "bucket"},
      {
          .returnType = INTEGER(),
          .argumentNames = {"x"},
          .argumentTypes = {BIGINT()},
          .body = "case when x < 10 then 1 when x < 20 then 2 else 3 end",
          .defaultNullBehavior = false,
      });
  testSelect(
      "SELECT tc.default.bucket(n_nationkey) FROM nation",
      matchScan("nation")
          .project({"case when n_nationkey < 10::bigint then 1 "
                    "     when n_nationkey < 20::bigint then 2 "
                    "     else 3 end"})
          .output());
  testSelect(
      "SELECT * FROM nation "
      "WHERE tc.default.bucket(n_nationkey + n_regionkey) = 2",
      matchScan("nation")
          .filter(
              "(case when n_nationkey + n_regionkey < 10::bigint then 1 "
              "      when n_nationkey + n_regionkey < 20::bigint then 2 "
              "      else 3 end) "
              " = 2")
          .output());

  // Constant. f(x) := array['red', 'green', 'blue']
  metadata_->addFunction(
      {"default", "colors"},
      {
          .returnType = ARRAY(VARCHAR()),
          .body = "array['red', 'green', 'blue']",
          .defaultNullBehavior = false,
      });
  testSelect(
      "SELECT tc.default.colors()[n_nationkey % 3 + 1] FROM nation",
      matchScan("nation")
          .project({"array_constructor('red', 'green', 'blue')"
                    "   [(n_nationkey % 3::bigint) + 1::bigint]"})
          .output());
}

// Each argument is cast to its declared type, and the body is cast to the
// declared return type.
TEST_F(SqlFunctionTest, coercion) {
  metadata_->addFunction(
      {"default", "identity"},
      {
          .returnType = BIGINT(),
          .argumentNames = {"x"},
          .argumentTypes = {BIGINT()},
          .body = "x",
          .defaultNullBehavior = false,
      });
  // The INTEGER argument is coerced to the declared BIGINT.
  testSelect(
      "SELECT tc.default.identity(CAST(n_nationkey AS INTEGER)) FROM nation",
      matchScan("nation")
          .project({"cast(cast(n_nationkey as integer) as bigint)"})
          .output());

  metadata_->addFunction(
      {"default", "narrow"},
      {
          .returnType = BIGINT(),
          .argumentNames = {"x"},
          .argumentTypes = {BIGINT()},
          .body = "cast(x as smallint)",
          .defaultNullBehavior = false,
      });
  // The SMALLINT body is coerced to the declared BIGINT return type.
  testSelect(
      "SELECT tc.default.narrow(n_nationkey) FROM nation",
      matchScan("nation")
          .project({"cast(cast(n_nationkey as smallint) as bigint)"})
          .output());
}

// A RETURNS NULL ON NULL INPUT function has its body wrapped so a null in any
// argument yields null without evaluating the body.
TEST_F(SqlFunctionTest, nullOnNullInput) {
  metadata_->addFunction(
      {"default", "guarded_one"},
      {
          .returnType = BIGINT(),
          .argumentNames = {"x"},
          .argumentTypes = {BIGINT()},
          .body = "x",
          .defaultNullBehavior = true,
      });
  testSelect(
      "SELECT tc.default.guarded_one(n_nationkey) FROM nation",
      matchScan("nation")
          .project({"if(is_null(n_nationkey), null, n_nationkey)"})
          .output());

  metadata_->addFunction(
      {"default", "guarded_two"},
      {
          .returnType = BIGINT(),
          .argumentNames = {"a", "b"},
          .argumentTypes = {BIGINT(), BIGINT()},
          .body = "a + b",
          .defaultNullBehavior = true,
      });
  testSelect(
      "SELECT tc.default.guarded_two(n_nationkey, n_regionkey) FROM nation",
      matchScan("nation")
          .project({"if(is_null(n_nationkey) or is_null(n_regionkey), "
                    "null, n_nationkey + n_regionkey)"})
          .output());

  // A zero-argument function has no argument to be null, so no guard is added.
  metadata_->addFunction(
      {"default", "answer"},
      {
          .returnType = BIGINT(),
          .body = "42",
          .defaultNullBehavior = true,
      });
  testSelect(
      "SELECT tc.default.answer() FROM nation",
      matchScan("nation").project({"42::bigint"}).output());
}

// Malformed calls and definitions fail with clear user errors.
TEST_F(SqlFunctionTest, resolutionErrors) {
  // A well-formed function; the call site, not the definition, is at fault in
  // the first four cases below.
  metadata_->addFunction(
      {"default", "identity"},
      {
          .returnType = BIGINT(),
          .argumentNames = {"x"},
          .argumentTypes = {BIGINT()},
          .body = "x",
      });

  // Unregistered catalog.
  VELOX_ASSERT_THROW(
      parseSelect("SELECT nope.default.identity(n_nationkey) FROM nation"),
      "Catalog not found");

  // No such function.
  VELOX_ASSERT_THROW(
      parseSelect("SELECT tc.default.missing(n_nationkey) FROM nation"),
      "SQL function not found");

  // Wrong number of arguments.
  VELOX_ASSERT_THROW(
      parseSelect(
          "SELECT tc.default.identity(n_nationkey, n_regionkey) FROM nation"),
      "SQL function called with wrong number of arguments");

  // Argument not coercible to the declared type.
  VELOX_ASSERT_THROW(
      parseSelect("SELECT tc.default.identity(n_name) FROM nation"),
      "Cannot coerce SQL function argument");

  // Body does not use every argument.
  metadata_->addFunction(
      {"default", "unused"},
      {
          .returnType = BIGINT(),
          .argumentNames = {"x", "y"},
          .argumentTypes = {BIGINT(), BIGINT()},
          .body = "x",
      });
  VELOX_ASSERT_THROW(
      parseSelect(
          "SELECT tc.default.unused(n_nationkey, n_regionkey) FROM nation"),
      "SQL function does not use all of its arguments");

  // Body references an identifier that is not an argument.
  metadata_->addFunction(
      {"default", "unknown_identifier"},
      {
          .returnType = BIGINT(),
          .argumentNames = {"x"},
          .argumentTypes = {BIGINT()},
          .body = "y",
      });
  VELOX_ASSERT_THROW(
      parseSelect(
          "SELECT tc.default.unknown_identifier(n_nationkey) FROM nation"),
      "Unknown identifier in SQL function body");

  // Body calls itself.
  metadata_->addFunction(
      {"default", "recurse"},
      {
          .returnType = BIGINT(),
          .argumentNames = {"x"},
          .argumentTypes = {BIGINT()},
          .body = "tc.default.recurse(x)",
      });
  VELOX_ASSERT_THROW(
      parseSelect("SELECT tc.default.recurse(n_nationkey) FROM nation"),
      "Recursive SQL function is not supported");

  // Body type is not coercible to the declared return type.
  metadata_->addFunction(
      {"default", "bad_return"},
      {
          .returnType = BIGINT(),
          .body = "array[1, 2, 3]",
      });
  VELOX_ASSERT_THROW(
      parseSelect("SELECT tc.default.bad_return() FROM nation"),
      "SQL function body is not coercible to its declared return type");
}

} // namespace
} // namespace axiom::sql::presto::test
