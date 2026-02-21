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
#include "velox/common/base/tests/GTestUtils.h"

namespace axiom::sql::presto::test {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

namespace {

class DdlParserTest : public PrestoParserTestBase {};

TEST_F(DdlParserTest, insertIntoTable) {
  {
    auto matcher = matchScan().tableWrite();
    testInsert("INSERT INTO nation SELECT * FROM nation", matcher);
  }

  {
    auto matcher = matchValues().project().tableWrite();
    testInsert(
        "INSERT INTO nation SELECT 100, 'n-100', 2, 'test comment'", matcher);

    // Omit n_comment. Expect to be filled with default value.
    testInsert(
        "INSERT INTO nation(n_nationkey, n_name, n_regionkey) SELECT 100, 'n-100', 2",
        matcher);

    // Change the order of columns.
    testInsert(
        "INSERT INTO nation(n_nationkey, n_regionkey, n_name) SELECT 100, 2, 'n-100'",
        matcher);
  }

  // Wrong types.
  VELOX_ASSERT_THROW(
      parseSql("INSERT INTO nation SELECT 100, 'n-100', 2, 3"),
      "Wrong column type: INTEGER vs. VARCHAR, column 'n_comment' in table 'nation'");
}

TEST_F(DdlParserTest, createTableAsSelect) {
  {
    auto nationSchema =
        facebook::axiom::connector::ConnectorMetadata::metadata(kConnectorId)
            ->findTable("nation")
            ->type();

    auto matcher = matchScan().tableWrite();
    testCtas(
        "CREATE TABLE t AS SELECT * FROM nation", "t", nationSchema, matcher);
  }

  auto matcher = matchScan().project().tableWrite();

  testCtas(
      "CREATE TABLE t AS SELECT n_nationkey * 100 as a, n_name as b FROM nation",
      "t",
      ROW({"a", "b"}, {BIGINT(), VARCHAR()}),
      matcher);

  // Missing column names.
  VELOX_ASSERT_THROW(
      parseSql(
          "CREATE TABLE t AS SELECT n_nationkey * 100, n_name FROM nation"),
      "Column name not specified at position 1");

  testCtas(
      "CREATE TABLE t(a, b) AS SELECT n_nationkey * 100, n_name FROM nation",
      "t",
      ROW({"a", "b"}, {BIGINT(), VARCHAR()}),
      matcher);

  // Table properties.
  testCtas(
      "CREATE TABLE t WITH (partitioned_by = ARRAY['ds']) AS "
      "SELECT n_nationkey, n_name, '2025-10-04' as ds FROM nation",
      "t",
      ROW({"n_nationkey", "n_name", "ds"}, {BIGINT(), VARCHAR(), VARCHAR()}),
      matcher,
      {
          {"partitioned_by", "array_constructor(ds)"},
      });

  testCtas(
      "CREATE TABLE t WITH (partitioned_by = ARRAY['ds'], bucket_count = 4, bucketed_by = ARRAY['n_nationkey']) AS "
      "SELECT n_nationkey, n_name, '2025-10-04' as ds FROM nation",
      "t",
      ROW({"n_nationkey", "n_name", "ds"}, {BIGINT(), VARCHAR(), VARCHAR()}),
      matcher,
      {
          {"partitioned_by", "array_constructor(ds)"},
          {"bucket_count", "4"},
          {"bucketed_by", "array_constructor(n_nationkey)"},
      });
}

TEST_F(DdlParserTest, createTable) {
  testCreateTable("CREATE TABLE t (id INTEGER)", "t", ROW({"id"}, {INTEGER()}));

  // if not exists
  {
    auto statement = parseSql("CREATE TABLE IF NOT EXISTS t (id BIGINT)");
    ASSERT_TRUE(statement->isCreateTable());

    const auto* createTable = statement->as<CreateTableStatement>();
    ASSERT_EQ("t", createTable->tableName());
    ASSERT_TRUE(createTable->ifNotExists());
  }

  // properties
  testCreateTable(
      "CREATE TABLE t (id INTEGER, ds VARCHAR) "
      "WITH (partitioned_by = ARRAY['ds'], format = 'ORC')",
      "t",
      ROW({"id", "ds"}, {INTEGER(), VARCHAR()}),
      /*properties=*/
      {{"partitioned_by", "array_constructor(ds)"}, {"format", "ORC"}});

  // a variety of different types
  testCreateTable(
      "CREATE TABLE t ("
      "  tiny_col TINYINT,"
      "  small_col SMALLINT,"
      "  int_col INT,"
      "  big_col BIGINT,"
      "  real_col REAL,"
      "  double_col DOUBLE,"
      "  varchar_col VARCHAR,"
      "  bool_col BOOLEAN"
      ")",
      "t",
      ROW({"tiny_col",
           "small_col",
           "int_col",
           "big_col",
           "real_col",
           "double_col",
           "varchar_col",
           "bool_col"},
          {TINYINT(),
           SMALLINT(),
           INTEGER(),
           BIGINT(),
           REAL(),
           DOUBLE(),
           VARCHAR(),
           BOOLEAN()}));

  // complex types
  testCreateTable(
      "CREATE TABLE t (ids ARRAY<INTEGER>)",
      "t",
      ROW({"ids"}, {ARRAY(INTEGER())}));
  testCreateTable(
      "CREATE TABLE t (kv MAP<VARCHAR, BIGINT>)",
      "t",
      ROW({"kv"}, {MAP(VARCHAR(), BIGINT())}));
  testCreateTable(
      "CREATE TABLE t (nested_map MAP<VARCHAR, ARRAY<INTEGER>>)",
      "t",
      ROW({"nested_map"}, {MAP(VARCHAR(), ARRAY(INTEGER()))}));
  testCreateTable(
      "CREATE TABLE t (nested ROW(a INTEGER, b VARCHAR))",
      "t",
      ROW({"nested"}, {ROW({"a", "b"}, {INTEGER(), VARCHAR()})}));
  testCreateTable(
      "CREATE TABLE t (price DECIMAL(10, 2))",
      "t",
      ROW({"price"}, {DECIMAL(10, 2)}));

  // like clause
  {
    auto likeSchema =
        facebook::axiom::connector::ConnectorMetadata::metadata(kConnectorId)
            ->findTable("nation")
            ->type();
    testCreateTable("CREATE TABLE copy (LIKE nation)", "copy", likeSchema);
  }

  // like clause + some more columns
  {
    auto likeSchema =
        facebook::axiom::connector::ConnectorMetadata::metadata(kConnectorId)
            ->findTable("nation")
            ->type();

    // should respect the order of (before, LIKE, after)
    std::vector<std::string> names = {"before"};
    std::vector<TypePtr> types = {INTEGER()};
    for (int i = 0; i < likeSchema->size(); ++i) {
      names.push_back(likeSchema->nameOf(i));
      types.push_back(likeSchema->childAt(i));
    }
    names.push_back("after");
    types.push_back(DOUBLE());

    testCreateTable(
        "CREATE TABLE extended (before INTEGER, LIKE nation, after DOUBLE)",
        "extended",
        ROW(std::move(names), std::move(types)));
  }

  // primary key constraint
  {
    std::vector<CreateTableStatement::Constraint> constraints = {
        {.columns = {"id"},
         .type = CreateTableStatement::Constraint::Type::kPrimaryKey}};
    testCreateTable(
        "CREATE TABLE t (id INTEGER, PRIMARY KEY (id))",
        "t",
        ROW({"id"}, {INTEGER()}),
        /*properties=*/{},
        constraints);
  }

  // unique key constraint
  {
    std::vector<CreateTableStatement::Constraint> constraints = {
        {.name = "unique_name",
         .columns = {"name"},
         .type = CreateTableStatement::Constraint::Type::kUnique}};
    testCreateTable(
        "CREATE TABLE t (id INTEGER, name VARCHAR, CONSTRAINT unique_name UNIQUE (name))",
        "t",
        ROW({"id", "name"}, {INTEGER(), VARCHAR()}),
        /*properties=*/{},
        constraints);
  }

  // duplicate column names
  VELOX_ASSERT_THROW(
      parseSql("CREATE TABLE t (id INTEGER, id VARCHAR)"),
      "Duplicate column name: id");
  VELOX_ASSERT_THROW(
      parseSql("CREATE TABLE t (id INTEGER, ID VARCHAR)"),
      "Duplicate column name: ID");

  // unknown type
  VELOX_ASSERT_THROW(
      parseSql("CREATE TABLE t (id UNKNOWNTYPE)"),
      "Cannot resolve type: UNKNOWNTYPE");

  // invalid decimal type parameters
  VELOX_ASSERT_THROW(
      parseSql("CREATE TABLE t (price DECIMAL(VARCHAR, ARRAY<INTEGER>))"),
      "'VARCHAR' could not be converted to INTEGER_LITERAL");

  // unknown constraint column
  VELOX_ASSERT_THROW(
      parseSql("CREATE TABLE t (id INTEGER, PRIMARY KEY (unknown))"),
      "Constraint on unknown column: unknown");

  // duplicate constraint column
  VELOX_ASSERT_THROW(
      parseSql("CREATE TABLE t (id INTEGER, PRIMARY KEY (id, id))"),
      "Duplicate constraint column: id");
  VELOX_ASSERT_THROW(
      parseSql("CREATE TABLE t (id INTEGER, PRIMARY KEY (id, ID))"),
      "Duplicate constraint column: ID");

  // duplicate table property
  VELOX_ASSERT_THROW(
      parseSql(
          "CREATE TABLE t (id INTEGER) WITH (format = 'ORC', format = 'PARQUET')"),
      "Duplicate property: format");
}

TEST_F(DdlParserTest, dropTable) {
  {
    auto statement = parseSql("DROP TABLE t");
    ASSERT_TRUE(statement->isDropTable());

    const auto* dropTable = statement->as<DropTableStatement>();
    ASSERT_EQ("t", dropTable->tableName());
    ASSERT_FALSE(dropTable->ifExists());
  }

  {
    auto statement = parseSql("DROP TABLE IF EXISTS u");
    ASSERT_TRUE(statement->isDropTable());

    const auto* dropTable = statement->as<DropTableStatement>();
    ASSERT_EQ("u", dropTable->tableName());
    ASSERT_TRUE(dropTable->ifExists());
  }
}

TEST_F(DdlParserTest, view) {
  connector_->createView(
      "view",
      ROW({"n_nationkey", "cnt"}, {BIGINT(), BIGINT()}),
      "SELECT n_regionkey as regionkey, count(*) cnt FROM nation GROUP BY 1");

  SCOPE_EXIT {
    connector_->dropView("view");
  };

  auto matcher = matchScan()
                     .join(matchScan().aggregate().project().build())
                     .filter()
                     .project();
  testSelect(
      "SELECT n_name, n_regionkey, cnt FROM nation n, view v "
      "WHERE n_nationkey = regionkey",
      matcher,
      {"view"});
}

TEST_F(DdlParserTest, createTableAndInsert) {
  auto parser = makeParser();

  // Parse CREATE TABLE and INSERT statements.
  const auto statements = parser.parseMultiple(
      "CREATE TABLE test_table AS SELECT n_nationkey, n_name FROM nation; "
      "INSERT INTO nation SELECT * FROM nation");

  // Verify both statements parsed correctly with expected write types.
  ASSERT_EQ(2, statements.size());
  ASSERT_TRUE(statements[0]->isCreateTableAsSelect());
  ASSERT_TRUE(statements[1]->isInsert());

  const auto* ctasWrite = statements[0]
                              ->as<CreateTableAsSelectStatement>()
                              ->plan()
                              ->as<lp::TableWriteNode>();
  ASSERT_EQ(lp::WriteKind::kCreate, ctasWrite->writeKind());

  const auto* insertWrite =
      statements[1]->as<InsertStatement>()->plan()->as<lp::TableWriteNode>();
  ASSERT_EQ(lp::WriteKind::kInsert, insertWrite->writeKind());
}

} // namespace
} // namespace axiom::sql::presto::test
