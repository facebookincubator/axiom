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

#include <fmt/format.h>
#include "axiom/common/SchemaTypeName.h"
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/sql/presto/tests/PrestoParserTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/types/BigintEnumRegistration.h"
#include "velox/functions/prestosql/types/BigintEnumType.h"
#include "velox/functions/prestosql/types/VarcharEnumRegistration.h"
#include "velox/functions/prestosql/types/VarcharEnumType.h"

namespace axiom::sql::presto::test {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

namespace {

// Wraps TestConnectorMetadata to count findType calls.
class CountingTestConnectorMetadata
    : public facebook::axiom::connector::TestConnectorMetadata {
 public:
  using TestConnectorMetadata::TestConnectorMetadata;

  facebook::velox::TypePtr findType(
      const facebook::axiom::SchemaTypeName& typeName) override {
    ++count_;
    return TestConnectorMetadata::findType(typeName);
  }

  int count() const {
    return count_;
  }

  void reset() {
    count_ = 0;
  }

 private:
  int count_{0};
};

class EnumLiteralTest : public PrestoParserTestBase {
 protected:
  static void SetUpTestCase() {
    PrestoParserTestBase::SetUpTestCase();
    facebook::velox::registerBigintEnumType();
    facebook::velox::registerVarcharEnumType();
  }

  void SetUp() override {
    PrestoParserTestBase::SetUp();

    enumMetadata_ = std::make_shared<CountingTestConnectorMetadata>(nullptr);

    statusType_ = BIGINT_ENUM(LongEnumParameter(
        "tc.myschema.status",
        {{"active", 0}, {"inactive", 1}, {"pending", 2}}));
    enumMetadata_->addType({"myschema", "status"}, statusType_);

    colorType_ = VARCHAR_ENUM(VarcharEnumParameter(
        "tc.myschema.color", {{"red", "red_value"}, {"blue", "blue_value"}}));
    enumMetadata_->addType({"myschema", "color"}, colorType_);

    facebook::axiom::connector::ConnectorMetadataRegistry::global().insert(
        "tc", enumMetadata_);
  }

  void TearDown() override {
    facebook::axiom::connector::ConnectorMetadataRegistry::global().erase("tc");
    enumMetadata_.reset();
    PrestoParserTestBase::TearDown();
  }

  std::shared_ptr<CountingTestConnectorMetadata> enumMetadata_;
  TypePtr statusType_;
  TypePtr colorType_;
};

TEST_F(EnumLiteralTest, bigintEnum) {
  // Happy path: bigint enum literal resolves to typed constant.
  testSelect(
      "SELECT tc.myschema.Status.ACTIVE FROM nation",
      matchScan().project({lp::Lit(int64_t{0}, statusType_)}).output());

  // Case-insensitive resolution.
  auto verify = [&](std::string_view sql, int64_t expected) {
    SCOPED_TRACE(sql);
    testSelect(
        fmt::format("SELECT {} FROM nation", sql),
        matchScan().project({lp::Lit(expected, statusType_)}).output());
  };
  verify("tc.myschema.status.active", 0);
  verify("tc.myschema.STATUS.INACTIVE", 1);
  verify("tc.myschema.Status.Pending", 2);

  // Unknown value is an error.
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSelect("SELECT tc.myschema.Status.UNKNOWN FROM nation"),
      "Enum value not found in type: tc.myschema.status");

  // Non-enum type referenced as enum literal produces explicit error.
  enumMetadata_->addType({"myschema", "notstruct"}, ROW({{"x", INTEGER()}}));
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSelect("SELECT tc.myschema.notstruct.x FROM nation"),
      "Type is not an enum type");
}

TEST_F(EnumLiteralTest, varcharEnum) {
  testSelect(
      "SELECT tc.myschema.Color.RED FROM nation",
      matchScan().project({lp::Lit("red_value", colorType_)}).output());
}

TEST_F(EnumLiteralTest, castToEnumType) {
  // CAST resolves dotted type names through connector.
  testSelect(
      "SELECT CAST(n_regionkey AS tc.myschema.Status) FROM nation",
      matchScan()
          .project({lp::Cast(statusType_, lp::Col("n_regionkey"))})
          .output());

  // Case-insensitive CAST.
  testSelect(
      "SELECT CAST(n_regionkey AS TC.MYSCHEMA.STATUS) FROM nation",
      matchScan()
          .project({lp::Cast(statusType_, lp::Col("n_regionkey"))})
          .output());

  // Unknown type in CAST.
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSelect(
          "SELECT CAST(n_regionkey AS tc.myschema.NonExistent) FROM nation"),
      "Cannot resolve type");

  // Unknown schema in CAST.
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSelect(
          "SELECT CAST(n_regionkey AS tc.unknown.SomeType) FROM nation"),
      "Cannot resolve type");
}

TEST_F(EnumLiteralTest, unknownTypeAndCatalog) {
  // Unknown type with known catalog produces explicit error.
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSelect("SELECT tc.myschema.NonExistent.VALUE FROM nation"),
      "near 'tc.myschema.nonexistent': Type not found");

  // Case-insensitive: token is lowercased.
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSelect("SELECT tc.MYSCHEMA.NONEXISTENT.VALUE FROM nation"),
      "near 'tc.myschema.nonexistent': Type not found");

  // Multi-part type name with known catalog.
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSelect("SELECT tc.myschema.foo.bar.VALUE FROM nation"),
      "near 'tc.myschema.foo.bar': Type not found");

  // Unknown catalog falls through to column dereference.
  VELOX_ASSERT_THROW(
      parseSelect("SELECT other.myschema.Status.ACTIVE FROM nation"),
      "Cannot resolve column: other");
}

TEST_F(EnumLiteralTest, columnFirstResolution) {
  // Create a table with a struct column named "tc" whose subfield chain
  // matches a registered enum. Column dereference must win over enum.
  connector_->addTable(
      "structtest",
      ROW({"tc"}, {ROW("myschema", ROW("status", ROW("active", BIGINT())))}));

  auto structDeref =
      lp::Col("active", lp::Col("status", lp::Col("myschema", lp::Col("tc"))));

  testSelect(
      "SELECT tc.myschema.status.active FROM structtest",
      matchScan().project({structDeref}).output());

  // Verify findType was not called — enum resolution was skipped.
  enumMetadata_->reset();
  parseSelect("SELECT tc.myschema.status.active FROM structtest");
  EXPECT_EQ(enumMetadata_->count(), 0);

  // Table alias collision: "tc" is a table alias that matches the catalog name.
  // The alias must win — enum resolution must not be attempted.
  connector_->addTable(
      "aliastest", ROW({"myschema"}, {ROW("status", ROW("active", BIGINT()))}));

  testSelect(
      "SELECT tc.myschema.status.active FROM aliastest AS tc",
      matchScan()
          .project({lp::Col("active", lp::Col("status", lp::Col("myschema")))})
          .output());

  enumMetadata_->reset();
  parseSelect("SELECT tc.myschema.status.active FROM aliastest AS tc");
  EXPECT_EQ(enumMetadata_->count(), 0);
}

TEST_F(EnumLiteralTest, structFieldDereference) {
  // Two-part name: column dereference (< 4 parts, never enum).
  connector_->addTable(
      "structtable", ROW({"s"}, {ROW("nested", ROW("field", BIGINT()))}));

  testSelect(
      "SELECT s.nested FROM structtable",
      matchScan().project({lp::Col("nested", lp::Col("s"))}).output());

  // Three-part name: still a column dereference (< 4 parts).
  testSelect(
      "SELECT s.nested.field FROM structtable",
      matchScan()
          .project({lp::Col("field", lp::Col("nested", lp::Col("s")))})
          .output());
}

TEST_F(EnumLiteralTest, enumLiteralInClauses) {
  {
    SCOPED_TRACE("WHERE");
    testSelect(
        "SELECT n_name FROM nation "
        "WHERE CAST(n_regionkey AS tc.myschema.Status) = "
        "tc.myschema.Status.ACTIVE",
        matchScan().filter().project().output());
  }
  {
    SCOPED_TRACE("GROUP BY");
    testSelect(
        "SELECT tc.myschema.Status.ACTIVE, count(*) FROM nation "
        "GROUP BY tc.myschema.Status.ACTIVE",
        matchScan().aggregate().output());
  }
  {
    SCOPED_TRACE("ORDER BY");
    testSelect(
        "SELECT n_name FROM nation "
        "ORDER BY tc.myschema.Status.ACTIVE",
        matchScan().project().sort().project().output());
  }
  {
    SCOPED_TRACE("CASE WHEN");
    testSelect(
        "SELECT CASE "
        "WHEN CAST(n_regionkey AS tc.myschema.Status) = "
        "tc.myschema.Status.ACTIVE "
        "THEN 'yes' ELSE 'no' END FROM nation",
        matchScan().project().output());
  }
  {
    SCOPED_TRACE("JOIN condition");
    testSelect(
        "SELECT n.n_name FROM nation n "
        "JOIN region r ON CAST(n.n_regionkey AS tc.myschema.Status) = "
        "tc.myschema.Status.ACTIVE",
        matchScan().join(matchScan().build()).project().output());
  }
}

TEST_F(EnumLiteralTest, compareEnumLiterals) {
  testSelect(
      "SELECT tc.myschema.Status.ACTIVE = tc.myschema.Status.INACTIVE "
      "FROM nation",
      matchScan()
          .project({lp::Call(
              "eq",
              lp::Lit(int64_t{0}, statusType_),
              lp::Lit(int64_t{1}, statusType_))})
          .output());
}

TEST_F(EnumLiteralTest, typeofEnumLiteral) {
  testSelect(
      "SELECT typeof(tc.myschema.Status.ACTIVE) FROM nation",
      matchScan().project().output());
}

TEST_F(EnumLiteralTest, multiPartTypeName) {
  auto multiPartType =
      BIGINT_ENUM(LongEnumParameter("tc.b.foo.bar", {{"val", 42}}));
  enumMetadata_->addType({"b", "foo.bar"}, multiPartType);

  // CAST path.
  testSelect(
      "SELECT CAST(n_regionkey AS tc.b.foo.bar) FROM nation",
      matchScan()
          .project({lp::Cast(multiPartType, lp::Col("n_regionkey"))})
          .output());

  // Literal path.
  testSelect(
      "SELECT tc.b.foo.bar.VAL FROM nation",
      matchScan().project({lp::Lit(int64_t{42}, multiPartType)}).output());
}

TEST_F(EnumLiteralTest, castAndLiteralShareCache) {
  enumMetadata_->reset();

  parseSelect(
      "SELECT CAST(n_regionkey AS tc.myschema.Status), "
      "tc.myschema.Status.ACTIVE FROM nation");

  EXPECT_EQ(enumMetadata_->count(), 1);
}

TEST_F(EnumLiteralTest, edgeCaseDotNames) {
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parseSelect("SELECT CAST(n_regionkey AS .a.b) FROM nation"),
      "extraneous input '.'");

  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parseSelect("SELECT CAST(n_regionkey AS a.b.) FROM nation"),
      "extraneous input '.'");

  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parseSelect("SELECT CAST(n_regionkey AS a..b) FROM nation"),
      "missing ')'");
}

TEST_F(EnumLiteralTest, enumLiteralInCreateTableProperty) {
  testCreateTable(
      "CREATE TABLE t (id INTEGER) "
      "WITH (status = tc.myschema.Status.ACTIVE)",
      "t",
      ROW({"id"}, {INTEGER()}),
      {{"status", "0"}});
}

} // namespace
} // namespace axiom::sql::presto::test
