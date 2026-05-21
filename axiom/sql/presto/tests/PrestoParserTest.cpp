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

#include "axiom/sql/presto/PrestoSqlError.h"
#include "axiom/sql/presto/tests/PrestoParserTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace axiom::sql::presto::test {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

namespace {

class PrestoParserTest : public PrestoParserTestBase {};

TEST_F(PrestoParserTest, parseMultiple) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple("select 1; select 2");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleWithTrailingSemicolon) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple("select 1; select 2;");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleWithWhitespace) {
  auto parser = makeParser();

  auto statements =
      parser.parseMultiple("  select 1  ;  \n  select 2  ;  \n  select 3  ");
  ASSERT_EQ(3, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
  ASSERT_TRUE(statements[2]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleWithComments) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple(
      "-- First query\nselect 1;\n-- Second query\nselect 2");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleWithMultiByteUtf8Char) {
  // Multi-byte UTF-8 characters anywhere in the input must not cause the
  // lexer to lose subsequent tokens. ANTLR token positions are codepoint
  // indices, but splitStatements slices the original UTF-8 string and so
  // must convert codepoint offsets to byte offsets.
  auto parser = makeParser();

  // 2-byte UTF-8 (U+00F1) in a leading comment.
  {
    auto statements = parser.parseMultiple("-- ñ\nSELECT 1");
    ASSERT_EQ(1, statements.size());
    ASSERT_TRUE(statements[0]->isSelect());
  }

  // 2-byte UTF-8 in a string literal.
  {
    auto statements = parser.parseMultiple("SELECT 'ñ'; SELECT 1");
    ASSERT_EQ(2, statements.size());
    ASSERT_TRUE(statements[0]->isSelect());
    ASSERT_TRUE(statements[1]->isSelect());
  }

  // 3-byte UTF-8 (U+20AC '€') in a comment between statements.
  {
    auto statements = parser.parseMultiple("SELECT 1; -- €\nSELECT 2");
    ASSERT_EQ(2, statements.size());
    ASSERT_TRUE(statements[0]->isSelect());
    ASSERT_TRUE(statements[1]->isSelect());
  }

  // Multiple multi-byte chars to exercise cumulative offset shift.
  {
    auto statements =
        parser.parseMultiple("-- ñ ñ ñ\nSELECT 'ä' AS x; SELECT 'ö' AS y");
    ASSERT_EQ(2, statements.size());
    ASSERT_TRUE(statements[0]->isSelect());
    ASSERT_TRUE(statements[1]->isSelect());
  }

  // 4-byte UTF-8 (U+1F389 '🎉') in a comment between statements.
  {
    auto statements = parser.parseMultiple("SELECT 1; -- 🎉\nSELECT 2");
    ASSERT_EQ(2, statements.size());
    ASSERT_TRUE(statements[0]->isSelect());
    ASSERT_TRUE(statements[1]->isSelect());
  }
}

TEST_F(PrestoParserTest, parseMultipleWithBlockComments) {
  auto parser = makeParser();

  auto statements =
      parser.parseMultiple("/* First */ select 1; /* Second */ select 2");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleWithSingleQuotes) {
  auto parser = makeParser();

  auto statements =
      parser.parseMultiple("select 'hello; world'; select 'foo''bar; baz'");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleWithDoubleQuotes) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple(
      "select 1 as \"col;name\"; select 2 as \"foo\"\"bar; baz\"");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleMixedStatements) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple(
      "select * from nation; "
      "select n_name from nation where n_nationkey = 1; "
      "select 42");
  ASSERT_EQ(3, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
  ASSERT_TRUE(statements[2]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleSingleStatement) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple("select 1");
  ASSERT_EQ(1, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
}

TEST_F(PrestoParserTest, parseMultipleEmptyStatements) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple(";;;");
  ASSERT_EQ(0, statements.size());
}

TEST_F(PrestoParserTest, parseMultipleComplexQuery) {
  auto parser = makeParser();

  auto statements = parser.parseMultiple(
      "select n_nationkey, n_name "
      "from nation "
      "where n_regionkey = 1 "
      "order by n_name; "
      "select count(*) from nation");
  ASSERT_EQ(2, statements.size());

  ASSERT_TRUE(statements[0]->isSelect());
  ASSERT_TRUE(statements[1]->isSelect());
}

TEST_F(PrestoParserTest, unnest) {
  {
    auto matcher = matchValues().unnest().output();
    testSelect("SELECT * FROM unnest(array[1, 2, 3])", matcher);

    testSelect(
        "SELECT * FROM unnest(array[1, 2, 3], array[4, 5]) with ordinality",
        matcher);

    testSelect(
        "SELECT * FROM unnest(map(array[1, 2, 3], array[10, 20, 30]))",
        matcher);
  }

  testSelect(
      "SELECT * FROM unnest(array[1, 2, 3]) as t(x)",
      matchValues().unnest().project().output({"x"}));

  // Alias count must match total column count (including ordinality).
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      testSelect(
          "SELECT * FROM unnest(array[1, 2, 3], array[4, 5]) with ordinality as t(x, y)",
          matchValues().unnest().output()),
      "Column alias list size does not match");

  testSelect(
      "SELECT * FROM unnest(array[1, 2, 3], array[4, 5]) with ordinality as t(x, y, ord)",
      matchValues().unnest().project().output({"x", "y", "ord"}));

  // Ordinality column alias.
  testSelect(
      "SELECT ord FROM unnest(array[1, 2, 3]) with ordinality as t(x, ord)",
      matchValues().unnest().project().project().output({"ord"}));

  {
    auto matcher = matchScan().unnest().output();
    testSelect(
        "SELECT * FROM nation, unnest(array[n_nationkey, n_regionkey])",
        matcher);
  }

  {
    auto matcher = matchScan().unnest().output(
        {"n_nationkey", "n_name", "n_regionkey", "n_comment", "x"});

    testSelect(
        "SELECT * FROM nation, unnest(array[n_nationkey, n_regionkey]) as t(x)",
        matcher);

    testSelect(
        "SELECT * FROM (nation cross join unnest(array[1,2,3]) as t(x))",
        matcher);
  }

  // Cross join unnest with ordinality alias.
  testSelect(
      "SELECT * FROM nation cross join unnest(array[n_nationkey]) with ordinality as t(x, ord)",
      matchScan().unnest().output(
          {"n_nationkey", "n_name", "n_regionkey", "n_comment", "x", "ord"}));

  {
    auto matcher = matchValues().project().unnest().project().output();

    testSelect(
        "WITH a AS (SELECT array[1,2,3] as x) SELECT t.x + 1 FROM a, unnest(A.x) as T(X)",
        matcher);
  }
}

TEST_F(PrestoParserTest, qualifiedColumnAccess) {
  {
    connector_->addTable("t", ROW({"x"}, {INTEGER()}));

    // Qualified and unqualified column access.
    testSelect("SELECT t.x FROM t", matchScan().project().output({"x"}));
    testSelect("SELECT x FROM t", matchScan().project().output({"x"}));

    // Case insensitive column and table alias; output preserves the
    // user-written case.
    testSelect("SELECT t.X FROM t", matchScan().project().output({"X"}));
    testSelect("SELECT T.X FROM t", matchScan().project().output({"X"}));
  }

  // Table alias takes priority over struct column name. Table 'u' has a
  // column 'x' (INTEGER) and a struct column 't' with fields 'x' (VARCHAR)
  // and 'y' (DOUBLE). When aliased as 't':
  //   - 't' resolves to the struct column (ROW).
  //   - 't.x' resolves to the table-qualified column (INTEGER), not the
  //     struct field (VARCHAR), because the table alias takes priority.
  //   - 't.y' resolves to the struct field (DOUBLE), because 'u' has no
  //     column 'y' and the fallback to struct dereference kicks in.
  {
    connector_->addTable(
        "u",
        ROW({"t", "x"}, {ROW({"x", "y"}, {VARCHAR(), DOUBLE()}), INTEGER()}));
    auto matcher =
        matchScan()
            .project([&](const auto& node) {
              const auto& outputType = node->outputType();
              EXPECT_EQ(outputType->childAt(0)->kind(), TypeKind::ROW);
              EXPECT_EQ(outputType->childAt(1)->kind(), TypeKind::INTEGER);
              EXPECT_EQ(outputType->childAt(2)->kind(), TypeKind::DOUBLE);
            })
            .output({"t", "x", "y"});
    testSelect("SELECT t, t.x, t.y FROM u AS t", matcher);

    // Struct field 'x' is shadowed by the table-qualified column. Legacy
    // positional access also fails because the struct has named fields.
    VELOX_ASSERT_THROW(
        parseSql("SELECT t.field0 FROM u AS t"),
        "Cannot access named field using legacy field name");
  }
}

TEST_F(PrestoParserTest, selectStar) {
  {
    auto matcher = matchScan().output(
        {"n_nationkey", "n_name", "n_regionkey", "n_comment"});
    testSelect("SELECT * FROM nation", matcher);
    testSelect("(SELECT * FROM nation)", matcher);
  }

  {
    // SELECT *, * duplicates column names via OutputNode.
    auto matcher = matchScan().project().output(
        {"n_nationkey",
         "n_name",
         "n_regionkey",
         "n_comment",
         "n_nationkey",
         "n_name",
         "n_regionkey",
         "n_comment"});
    testSelect("SELECT *, * FROM nation", matcher);

    // These produce Project → Output (Output renames disambiguated columns
    // back to original names).
    testSelect(
        "SELECT *, n_nationkey FROM nation",
        matchScan().project().output(
            {"n_nationkey",
             "n_name",
             "n_regionkey",
             "n_comment",
             "n_nationkey"}));

    // These produce just Project (output names already match).
    testSelect(
        "SELECT nation.* FROM nation",
        matchScan().project().output(
            {"n_nationkey", "n_name", "n_regionkey", "n_comment"}));
    testSelect(
        "SELECT nation.*, n_nationkey + 1 FROM nation",
        matchScan().project().output());
  }

  {
    auto matcher =
        matchScan().join(matchScan().build()).filter().project().output();
    testSelect(
        "SELECT nation.*, r_regionkey + 1 FROM nation, region WHERE n_regionkey = r_regionkey",
        matcher);
  }

  VELOX_ASSERT_THROW(parseSql("SELECT r.* FROM region"), "Alias not found: r");
}

// Tests for star expansion with duplicate column names from joins.
TEST_F(PrestoParserTest, selectStarDuplicateColumns) {
  auto matchJoin = [&]() {
    return matchScan("nation")
        .join(
            matchScan("nation").build(),
            {
                "a_nk",
                "a_name",
                "a_rk",
                "a_comment",
                "b_nk",
                "b_name",
                "b_rk",
                "b_comment",
            })
        .filter("a_nk = b_nk")
        .project();
  };

  // SELECT *, expr with self-join.
  testSelect(
      "SELECT *, 1 FROM nation a, nation b "
      "WHERE a.n_nationkey = b.n_nationkey",
      matchJoin().output());

  // Qualified star with additional columns.
  testSelect(
      "SELECT a.*, b.n_nationkey FROM nation a, nation b "
      "WHERE a.n_nationkey = b.n_nationkey",
      matchJoin().output());

  // Star expansion includes anonymous columns from subqueries.
  {
    auto plan = parseSelect(
        "SELECT *, 1 FROM (SELECT n_nationkey, n_nationkey + 1 FROM nation)");
    EXPECT_EQ(3, plan->outputType()->size());
  }

  // Nested window function with duplicate column names.
  testSelect(
      "SELECT *, sum(a.n_nationkey) OVER () + 1 FROM nation a, nation b "
      "WHERE a.n_nationkey = b.n_nationkey",
      matchJoin().project().output());

  // ORDER BY ordinal with duplicate column names.
  testSelect(
      "SELECT *, 1 FROM nation a, nation b "
      "WHERE a.n_nationkey = b.n_nationkey ORDER BY 1",
      matchJoin().sort().output());
}

TEST_F(PrestoParserTest, hiddenColumns) {
  connector_->addTable(
      "t", ROW({"a", "b"}, INTEGER()), ROW({"$c", "$d"}, VARCHAR()));

  {
    // SELECT * filters out hidden columns.
    auto matcher = matchScan().project().output({"a", "b"});
    testSelect("SELECT * FROM t", matcher);
  }
  {
    // Hidden columns can be explicitly referenced.
    testSelect(
        R"(SELECT "$c", * FROM t)",
        matchScan().project().output({"$c", "a", "b"}));
    testSelect(
        R"(SELECT a, "$c" FROM t)", matchScan().project().output({"a", "$c"}));
  }

  {
    // Duplicate columns from * produce correct output names.
    auto matcher = matchScan().project().output({"a", "b", "a"});
    testSelect("SELECT *, a FROM t", matcher);
  }

  {
    auto matcher = matchScan().project().output({"a", "b", "a", "b"});
    testSelect("SELECT *, * FROM t", matcher);
  }
}

TEST_F(PrestoParserTest, mixedCaseColumnNames) {
  // Output schema preserves the user-written case of column references.
  testSelect(
      "SELECT N_NAME, n_ReGiOnKeY FROM nation",
      matchScan().project().output({"N_NAME", "n_ReGiOnKeY"}));

  auto matcher = matchScan().project().output({"n_name"});
  testSelect("SELECT nation.n_name FROM nation", matcher);
  testSelect("SELECT NATION.n_name FROM nation", matcher);
  testSelect("SELECT \"NATION\".n_name FROM nation", matcher);
}

TEST_F(PrestoParserTest, withBasic) {
  {
    auto matcher = matchValues().project().output({"x"});
    testSelect("WITH a as (SELECT 1 as x) SELECT * FROM a", matcher);
    testSelect("WITH a as (SELECT 1 as x) SELECT * FROM A", matcher);
    testSelect("WITH A as (SELECT 1 as x) SELECT * FROM a", matcher);
  }

  {
    auto matcher = matchValues().project().project().output({"x"});
    testSelect("WITH a as (SELECT 1 as x) SELECT A.x FROM a", matcher);
  }
}

TEST_F(PrestoParserTest, withShadowingCte) {
  // Inner CTE shadows outer CTE with the same name. The inner CTE uses a table
  // scan (producing a Scan node) while the outer uses VALUES. Without proper
  // shadowing, the subquery would resolve to the outer CTE and produce a Values
  // node instead.
  auto matcher = matchScan().project().output({"n_nationkey"});
  testSelect(
      "WITH t AS (SELECT 1 AS x) "
      "SELECT * FROM (WITH t AS (SELECT n_nationkey FROM nation) SELECT * FROM t) sub",
      matcher);
}

TEST_F(PrestoParserTest, withNoLeaking) {
  // CTE defined inside a subquery is not visible outside.
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSql(
          "SELECT * FROM (WITH t AS (SELECT 1 AS x) SELECT * FROM t) sub "
          "CROSS JOIN t"),
      "Table not found: t");
}

TEST_F(PrestoParserTest, withShadowingBaseTable) {
  // CTE with the same name as a base table it references. The inner reference
  // to 'nation' inside the CTE body must resolve to the base table, not recurse
  // into the CTE itself.
  auto matcher = matchScan().project().output({"n_nationkey"});
  testSelect(
      "WITH nation AS (SELECT n_nationkey FROM nation) "
      "SELECT * FROM nation",
      matcher);
}

TEST_F(PrestoParserTest, withMultipleCtes) {
  // Later CTE references earlier CTE.
  testSelect(
      "WITH a AS (SELECT n_nationkey, n_name FROM nation), "
      "     b AS (SELECT * FROM a) "
      "SELECT * FROM b",
      matchScan().project().output({"n_nationkey", "n_name"}));
}

TEST_F(PrestoParserTest, withColumnAliases) {
  auto matcher =
      lp::test::LogicalPlanMatcherBuilder().values().project().project().output(
          {"a", "b"});
  testSelect("WITH t(a, b) AS (VALUES (1, 'x')) SELECT a, b FROM t", matcher);

  // Mismatched alias-list size is rejected.
  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSql("WITH t(a, b) AS (VALUES (1)) SELECT * FROM t"),
      "Column alias list size does not match the number of output columns");
}

TEST_F(PrestoParserTest, withReferencedMultipleTimes) {
  // Same CTE referenced twice in a JOIN.
  auto matcher = matchScan()
                     .project()
                     .join(matchScan().project().build())
                     .project()
                     .output({"n_nationkey"});
  testSelect(
      "WITH a AS (SELECT n_nationkey FROM nation) "
      "SELECT x.n_nationkey FROM a x JOIN a y ON x.n_nationkey = y.n_nationkey",
      matcher);
}

TEST_F(PrestoParserTest, withRecursiveNotSupported) {
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parseSql("WITH RECURSIVE t AS (SELECT 1 AS x) SELECT * FROM t"),
      "WITH RECURSIVE is not supported");
}

TEST_F(PrestoParserTest, join) {
  {
    auto matcher = matchScan()
                       .join(matchScan().build())
                       .output(
                           {"n_nationkey",
                            "n_name",
                            "n_regionkey",
                            "n_comment",
                            "r_regionkey",
                            "r_name",
                            "r_comment"});

    testSelect("SELECT * FROM nation, region", matcher);

    testSelect(
        "SELECT * FROM nation LEFT JOIN region ON n_regionkey = r_regionkey",
        matcher);

    testSelect(
        "SELECT * FROM nation RIGHT JOIN region ON nation.n_regionkey = region.r_regionkey",
        matcher);

    testSelect(
        "SELECT * FROM nation n LEFT JOIN region r ON n.n_regionkey = r.r_regionkey",
        matcher);

    testSelect(
        "SELECT * FROM nation FULL OUTER JOIN region ON n_regionkey = r_regionkey",
        matcher);
  }

  {
    connector_->addTable("t1", ROW({"id", "a"}, {INTEGER(), VARCHAR()}));
    connector_->addTable("t2", ROW({"id", "b"}, {INTEGER(), VARCHAR()}));
    connector_->addTable(
        "t3", ROW({"x", "y", "z"}, {INTEGER(), INTEGER(), VARCHAR()}));

    // Unqualified reference to a column on both sides is ambiguous.
    VELOX_ASSERT_THROW(
        parseSql("SELECT * FROM t1 JOIN t2 ON id = id"),
        "Cannot resolve column");

    // Qualified references are not ambiguous.
    auto matcher =
        matchScan().join(matchScan().build()).output({"id", "a", "id", "b"});
    testSelect("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id", matcher);

    // Non-existent column in ON clause.
    VELOX_ASSERT_THROW(
        parseSql("SELECT * FROM t1 JOIN t2 ON t1.id = no_such_column"),
        "Cannot resolve column");

    // Correlated subquery in ON clause with unqualified reference to a column
    // that exists on both sides of the join is ambiguous. This exercises the
    // joinScope lambda (resolveJoinColumn) rather than the NameMappings::merge
    // path used for simple ON conditions.
    VELOX_ASSERT_THROW(
        parseSql(
            "SELECT * FROM t1 JOIN t2 "
            "ON t1.id = (SELECT max(x) FROM t3 WHERE t3.y = id)"),
        "Column is ambiguous: id");

    // Correlated subquery referencing a column unique to one side works.
    // Column 'a' exists only on t1, so it resolves unambiguously.
    testSelect(
        "SELECT * FROM t1 JOIN t2 "
        "ON t1.id = (SELECT max(x) FROM t3 WHERE t3.z = a)",
        matcher);

    // Qualified reference to ambiguous column in subquery is not ambiguous.
    testSelect(
        "SELECT * FROM t1 JOIN t2 "
        "ON t1.id = (SELECT max(x) FROM t3 WHERE t3.y = t1.id)",
        matcher);
  }

  {
    auto matcher = matchScan()
                       .join(matchScan().build())
                       .filter()
                       .output(
                           {"n_nationkey",
                            "n_name",
                            "n_regionkey",
                            "n_comment",
                            "r_regionkey",
                            "r_name",
                            "r_comment"});

    testSelect(
        "SELECT * FROM nation, region WHERE n_regionkey = r_regionkey",
        matcher);
  }

  {
    auto matcher = matchScan()
                       .join(matchScan().build())
                       .filter()
                       .project()
                       .output({"n_name", "r_name"});

    testSelect(
        "SELECT n_name, r_name FROM nation, region WHERE n_regionkey = r_regionkey",
        matcher);
  }
}

TEST_F(PrestoParserTest, joinUsing) {
  connector_->addTable("t1", ROW({"id", "a"}, {INTEGER(), VARCHAR()}));
  connector_->addTable("t2", ROW({"id", "b"}, {INTEGER(), VARCHAR()}));
  connector_->addTable("t3", ROW({"id", "c"}, {INTEGER(), VARCHAR()}));

  // INNER JOIN USING.
  {
    auto matcher = matchScan("t1")
                       .join(matchScan("t2").build())
                       .project()
                       .output({"id", "a", "b"});
    testSelect("SELECT * FROM t1 JOIN t2 USING (id)", matcher);
  }

  // Chained JOIN USING.
  {
    auto matcher = matchScan("t1")
                       .join(matchScan("t2").build())
                       .project()
                       .join(matchScan("t3").build())
                       .project()
                       .output({"id", "a", "b", "c"});
    testSelect(
        "SELECT * FROM t1 JOIN t2 USING (id) JOIN t3 USING (id)", matcher);
  }

  // LEFT JOIN USING.
  {
    auto matcher = matchScan("t1")
                       .join(matchScan("t2").build())
                       .project()
                       .output({"id", "a", "b"});
    testSelect("SELECT * FROM t1 LEFT JOIN t2 USING (id)", matcher);
  }

  // RIGHT JOIN USING: OutputNode renames id_0 -> id.
  {
    auto matcher = matchScan("t1")
                       .join(matchScan("t2").build())
                       .project()
                       .output({"id", "a", "b"});
    testSelect("SELECT * FROM t1 RIGHT JOIN t2 USING (id)", matcher);
  }

  // FULL JOIN USING: project coalesces USING columns, OutputNode renames.
  {
    auto matcher =
        matchScan("t1")
            .join(matchScan("t2").build())
            .project([&](const auto& node) {
              auto project =
                  std::dynamic_pointer_cast<const lp::ProjectNode>(node);
              ASSERT_EQ(3, project->expressions().size());
              auto* coalesce = dynamic_cast<const lp::SpecialFormExpr*>(
                  project->expressionAt(0).get());
              ASSERT_NE(nullptr, coalesce);
              EXPECT_EQ(lp::SpecialForm::kCoalesce, coalesce->form());
            })
            .output({"id", "a", "b"});
    testSelect("SELECT * FROM t1 FULL JOIN t2 USING (id)", matcher);
  }

  // Aliased table in JOIN USING. Column aliases from both sides are used as
  // output names. The USING column appears once with the left side's alias,
  // followed by non-USING columns from both sides.
  {
    auto matcher = matchScan("t1")
                       .project()
                       .join(matchScan("t2").project().build())
                       .project()
                       .output({"x", "y", "b"});
    testSelect(
        "SELECT * FROM t1 AS s1(x, y) JOIN t2 AS s2(x, b) USING (x)", matcher);
    testSelect(
        "SELECT * FROM t1 AS s1(x, y) LEFT JOIN t2 AS s2(x, b) USING (x)",
        matcher);
    testSelect(
        "SELECT * FROM t1 AS s1(x, y) RIGHT JOIN t2 AS s2(x, b) USING (x)",
        matcher);
    testSelect(
        "SELECT * FROM t1 AS s1(x, y) FULL JOIN t2 AS s2(x, b) USING (x)",
        matcher);
  }

  // Duplicate column aliases with JOIN USING.
  {
    connector_->addTable(
        "t4", ROW({"x", "y", "z"}, {INTEGER(), INTEGER(), INTEGER()}));
    connector_->addTable(
        "t5", ROW({"x", "y", "z"}, {INTEGER(), INTEGER(), INTEGER()}));
    auto matcher = matchScan("t4")
                       .project()
                       .join(matchScan("t5").project().build())
                       .project()
                       .output({"id", "a", "a", "b", "b"});
    testSelect(
        "SELECT * FROM t4 AS r(id, a, a) JOIN t5 AS t(id, b, b) USING (id)",
        matcher);
  }

  // JOIN USING with type coercion (INTEGER vs BIGINT).
  {
    connector_->addTable("t_int", ROW({"id", "a"}, {INTEGER(), VARCHAR()}));
    connector_->addTable("t_big", ROW({"id", "b"}, {BIGINT(), VARCHAR()}));
    auto matcher = matchScan("t_int")
                       .join(matchScan("t_big").build())
                       .project()
                       .output({"id", "a", "b"});
    testSelect("SELECT * FROM t_int JOIN t_big USING (id)", matcher);
  }
}

TEST_F(PrestoParserTest, joinOnSubquery) {
  auto matcher = matchScan()
                     .join(matchScan().build())
                     .output(
                         {"n_nationkey",
                          "n_name",
                          "n_regionkey",
                          "n_comment",
                          "r_regionkey",
                          "r_name",
                          "r_comment"});

  // Correlated subquery in JOIN ON clause referencing left-side columns works.
  testSelect(
      "SELECT * FROM nation n JOIN region r "
      "ON n.n_regionkey = r.r_regionkey "
      "AND n.n_nationkey IN (SELECT s_nationkey FROM supplier s WHERE s.s_nationkey = n.n_nationkey)",
      matcher);

  // Correlated subquery in JOIN ON clause referencing right-side columns.
  testSelect(
      "SELECT * FROM nation n JOIN region r "
      "ON n.n_regionkey = r.r_regionkey "
      "AND r.r_regionkey IN (SELECT s_nationkey FROM supplier s WHERE s.s_nationkey = r.r_regionkey)",
      matcher);

  // Correlated EXISTS referencing right-side columns.
  testSelect(
      "SELECT * FROM nation n JOIN region r "
      "ON n.n_regionkey = r.r_regionkey "
      "AND EXISTS (SELECT 1 FROM supplier s WHERE s.s_nationkey = r.r_regionkey)",
      matcher);

  // LEFT JOIN with correlated subquery referencing right-side (null-supplying)
  // columns.
  testSelect(
      "SELECT * FROM nation n LEFT JOIN region r "
      "ON n.n_regionkey = r.r_regionkey "
      "AND EXISTS (SELECT 1 FROM supplier s WHERE s.s_nationkey = r.r_regionkey)",
      matcher);
}

TEST_F(PrestoParserTest, unionAll) {
  {
    auto matcher = matchScan()
                       .project()
                       .unionAll(matchScan().project().build())
                       .output({"n_name"});

    testSelect(
        "SELECT n_name FROM nation UNION ALL SELECT r_name FROM region",
        matcher);
  }

  // 3-way UNION ALL produces nested SetNodes.
  {
    auto matcher = matchScan()
                       .project()
                       .unionAll(matchScan().project().build())
                       .unionAll(matchScan().project().build())
                       .output({"n_name"});

    testSelect(
        "SELECT n_name FROM nation "
        "UNION ALL SELECT r_name FROM region "
        "UNION ALL SELECT n_name FROM nation",
        matcher);
  }
}

TEST_F(PrestoParserTest, unionAllFollowedByInSubqueryCoercion) {
  auto matcher = matchValues()
                     .project()
                     .unionAll(matchValues().project().build())
                     .project()
                     .output();
  testSelect(
      "SELECT t.x IN (SELECT 1) "
      "FROM (SELECT CAST(1 AS BIGINT) AS x UNION ALL SELECT CAST(2 AS BIGINT)) t",
      matcher);
}

TEST_F(PrestoParserTest, union) {
  // UNION and UNION DISTINCT are equivalent. Deduplication is part of the
  // kUnion set operation semantics.
  auto matcher = matchScan()
                     .project()
                     .unionDistinct(matchScan().project().build())
                     .output({"n_name"});

  testSelect(
      "SELECT n_name FROM nation UNION SELECT r_name FROM region", matcher);
  testSelect(
      "SELECT n_name FROM nation UNION DISTINCT SELECT r_name FROM region",
      matcher);
}

TEST_F(PrestoParserTest, except) {
  // EXCEPT and EXCEPT DISTINCT are equivalent. Deduplication is part of the
  // kExcept set operation semantics.
  auto matcher = matchScan()
                     .project()
                     .except(matchScan().project().build())
                     .output({"n_name"});

  testSelect(
      "SELECT n_name FROM nation EXCEPT SELECT r_name FROM region", matcher);
  testSelect(
      "SELECT n_name FROM nation EXCEPT DISTINCT SELECT r_name FROM region",
      matcher);

  // EXCEPT ALL uses kExceptAll which preserves duplicates.
  auto matcherAll = matchScan()
                        .project()
                        .exceptAll(matchScan().project().build())
                        .output({"n_name"});

  testSelect(
      "SELECT n_name FROM nation EXCEPT ALL SELECT r_name FROM region",
      matcherAll);
}

TEST_F(PrestoParserTest, intersect) {
  // INTERSECT and INTERSECT DISTINCT are equivalent. Deduplication is part of
  // the kIntersect set operation semantics.
  auto matcher = matchScan()
                     .project()
                     .intersect(matchScan().project().build())
                     .output({"n_name"});

  testSelect(
      "SELECT n_name FROM nation INTERSECT SELECT r_name FROM region", matcher);
  testSelect(
      "SELECT n_name FROM nation INTERSECT DISTINCT SELECT r_name FROM region",
      matcher);

  // INTERSECT ALL uses kIntersectAll which preserves duplicates.
  auto matcherAll = matchScan()
                        .project()
                        .intersectAll(matchScan().project().build())
                        .output({"n_name"});

  testSelect(
      "SELECT n_name FROM nation INTERSECT ALL SELECT r_name FROM region",
      matcherAll);
}

TEST_F(PrestoParserTest, exists) {
  {
    auto matcher =
        matchScan().filter().output({"r_regionkey", "r_name", "r_comment"});

    testSelect(
        "SELECT * FROM region WHERE exists (SELECT * from nation WHERE n_name like 'A%' and r_regionkey = n_regionkey)",
        matcher);

    testSelect(
        "SELECT * FROM region WHERE not exists (SELECT * from nation WHERE n_name like 'A%' and r_regionkey = n_regionkey)",
        matcher);
  }

  {
    auto matcher = matchScan().project().output();

    testSelect(
        "SELECT EXISTS (SELECT * from nation WHERE n_regionkey = r_regionkey) FROM region",
        matcher);
  }
}

TEST_F(PrestoParserTest, structDereferenceInCorrelatedSubquery) {
  connector_->addTable(
      "t",
      ROW({"s", "x"}, {ROW({"a", "b"}, {INTEGER(), VARCHAR()}), INTEGER()}));
  connector_->addTable("u", ROW({"y"}, {INTEGER()}));

  // Correlated subquery references a struct field from the outer query.
  auto matcher = matchScan().filter().output({"s", "x"});
  testSelect(
      "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM u WHERE s.a = y)", matcher);
}

TEST_F(PrestoParserTest, values) {
  {
    auto matcher =
        lp::test::LogicalPlanMatcherBuilder()
            .values(ROW({"c0", "c1", "c2"}, {INTEGER(), DOUBLE(), VARCHAR()}))
            .output();

    testSelect(
        "SELECT * FROM (VALUES (1, 1.1, 'foo'), (2, null, 'bar'))", matcher);

    testSelect(
        "SELECT * FROM (VALUES (1, null, 'foo'), (2, 2.2, 'bar'))", matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder()
                       .values(ROW({"c0"}, {INTEGER()}))
                       .output();
    testSelect("SELECT * FROM (VALUES (1), (2), (3), (4))", matcher);
  }

  {
    auto matcher = lp::test::LogicalPlanMatcherBuilder()
                       .values(ROW({"c0", "c1"}, {REAL(), INTEGER()}))
                       .output();
    testSelect("SELECT * FROM (VALUES (real '1', 1 + 2))", matcher);
  }
}

TEST_F(PrestoParserTest, tablesample) {
  {
    auto matcher = matchScan().sample().output(
        {"n_nationkey", "n_name", "n_regionkey", "n_comment"});

    testSelect("SELECT * FROM nation TABLESAMPLE BERNOULLI (10.0)", matcher);
    testSelect("SELECT * FROM nation TABLESAMPLE SYSTEM (1.5)", matcher);

    testSelect("SELECT * FROM nation TABLESAMPLE BERNOULLI (10)", matcher);
    testSelect("SELECT * FROM nation TABLESAMPLE BERNOULLI (1 + 2)", matcher);
  }

  {
    auto matcher =
        matchScan().aggregate().sample().output({"l_orderkey", "count"});

    testSelect(
        "SELECT * FROM (SELECT l_orderkey, count(*) FROM lineitem GROUP BY 1) "
        "TABLESAMPLE BERNOULLI (1.5)",
        matcher);
  }
}

TEST_F(PrestoParserTest, everything) {
  auto matcher = matchScan()
                     .join(matchScan().build())
                     .filter()
                     .aggregate()
                     .sort({"count desc"})
                     .output({"r_name", "count"});

  testSelect(
      "SELECT r_name, count(*) FROM nation, region "
      "WHERE n_regionkey = r_regionkey "
      "GROUP BY 1 "
      "ORDER BY 2 DESC",
      matcher);
}

TEST_F(PrestoParserTest, explainSelect) {
  {
    auto matcher = matchScan().output(
        {"n_nationkey", "n_name", "n_regionkey", "n_comment"});
    testExplain("EXPLAIN SELECT * FROM nation", matcher);
  }

  auto parser = makeParser();
  {
    auto statement = parser.parse("EXPLAIN ANALYZE SELECT * FROM nation");
    ASSERT_TRUE(statement->isExplain());

    auto explainStatement = statement->as<ExplainStatement>();
    ASSERT_TRUE(explainStatement->isAnalyze());
  }

  {
    auto statement =
        parser.parse("EXPLAIN (TYPE LOGICAL) SELECT * FROM nation", true);
    ASSERT_TRUE(statement->isExplain());

    auto explainStatement = statement->as<ExplainStatement>();
    ASSERT_FALSE(explainStatement->isAnalyze());
    ASSERT_TRUE(explainStatement->type() == ExplainStatement::Type::kLogical);
  }

  {
    auto statement =
        parser.parse("EXPLAIN (TYPE GRAPH) SELECT * FROM nation", true);
    ASSERT_TRUE(statement->isExplain());

    auto explainStatement = statement->as<ExplainStatement>();
    ASSERT_FALSE(explainStatement->isAnalyze());
    ASSERT_TRUE(explainStatement->type() == ExplainStatement::Type::kGraph);
  }

  {
    auto statement =
        parser.parse("EXPLAIN (TYPE OPTIMIZED) SELECT * FROM nation");
    ASSERT_TRUE(statement->isExplain());

    auto explainStatement = statement->as<ExplainStatement>();
    ASSERT_FALSE(explainStatement->isAnalyze());
    ASSERT_TRUE(explainStatement->type() == ExplainStatement::Type::kOptimized);
  }

  {
    auto statement =
        parser.parse("EXPLAIN (TYPE EXECUTABLE) SELECT * FROM nation");
    ASSERT_TRUE(statement->isExplain());

    auto explainStatement = statement->as<ExplainStatement>();
    ASSERT_FALSE(explainStatement->isAnalyze());
    ASSERT_TRUE(
        explainStatement->type() == ExplainStatement::Type::kExecutable);
  }

  {
    auto statement =
        parser.parse("EXPLAIN (TYPE DISTRIBUTED) SELECT * FROM nation");
    ASSERT_TRUE(statement->isExplain());

    auto explainStatement = statement->as<ExplainStatement>();
    ASSERT_FALSE(explainStatement->isAnalyze());
    ASSERT_TRUE(
        explainStatement->type() == ExplainStatement::Type::kExecutable);
  }

  {
    auto statement = parser.parse("EXPLAIN (TYPE IO) SELECT * FROM nation");
    ASSERT_TRUE(statement->isExplain());

    auto explainStatement = statement->as<ExplainStatement>();
    ASSERT_FALSE(explainStatement->isAnalyze());
    ASSERT_EQ(explainStatement->type(), ExplainStatement::Type::kIo);
  }
}

TEST_F(PrestoParserTest, explainFormat) {
  auto parser = makeParser();

  std::vector<std::pair<std::string, ExplainStatement::Format>> formats = {
      {"TEXT", ExplainStatement::Format::kText},
      {"GRAPHVIZ", ExplainStatement::Format::kGraphviz},
      {"JSON", ExplainStatement::Format::kJson},
  };

  for (const auto& [formatName, expectedFormat] : formats) {
    SCOPED_TRACE(formatName);
    auto statement = parser.parse(
        fmt::format(
            "EXPLAIN (TYPE LOGICAL, FORMAT {}) SELECT * FROM nation",
            formatName),
        true);
    ASSERT_TRUE(statement->isExplain());

    auto explainStatement = statement->as<ExplainStatement>();
    ASSERT_EQ(explainStatement->type(), ExplainStatement::Type::kLogical);
    ASSERT_EQ(explainStatement->format(), expectedFormat);
  }

  // Default format is TEXT.
  {
    auto statement =
        parser.parse("EXPLAIN (TYPE LOGICAL) SELECT * FROM nation", true);
    auto explainStatement = statement->as<ExplainStatement>();
    ASSERT_EQ(explainStatement->format(), ExplainStatement::Format::kText);
  }
}

TEST_F(PrestoParserTest, explainShow) {
  auto matcher = matchValues();
  testExplain("EXPLAIN SHOW CATALOGS", matcher);

  testExplain("EXPLAIN SHOW TABLES", matcher);

  testExplain("EXPLAIN SHOW COLUMNS FROM nation", matcher);

  testExplain("EXPLAIN SHOW FUNCTIONS", matcher);

  testExplain("EXPLAIN SHOW STATS FOR nation", matcher);
}

TEST_F(PrestoParserTest, explainInsert) {
  {
    auto matcher = matchScan().tableWrite();
    testExplain("EXPLAIN INSERT INTO region SELECT * FROM region", matcher);
  }

  {
    auto matcher = matchValues().tableWrite();
    testExplain("EXPLAIN INSERT INTO region VALUES (1, 'foo', 'bar')", matcher);
  }
}

TEST_F(PrestoParserTest, explainCreateTable) {
  testExplainDdl(
      "EXPLAIN CREATE TABLE t (id INTEGER)", SqlStatementKind::kCreateTable);
  testExplainDdl(
      "EXPLAIN CREATE TABLE IF NOT EXISTS t (id INTEGER)",
      SqlStatementKind::kCreateTable);
  testExplainDdl(
      "EXPLAIN CREATE TABLE t (id INTEGER, ds VARCHAR) "
      "WITH (partitioned_by = ARRAY['ds'])",
      SqlStatementKind::kCreateTable);
}

TEST_F(PrestoParserTest, explainDropTable) {
  testExplainDdl("EXPLAIN DROP TABLE t", SqlStatementKind::kDropTable);
  testExplainDdl(
      "EXPLAIN DROP TABLE IF EXISTS u", SqlStatementKind::kDropTable);
}

TEST_F(PrestoParserTest, showCatalogs) {
  {
    auto matcher = matchValues();
    testSelect("SHOW CATALOGS", matcher);
  }

  {
    auto matcher = matchValues().filter();
    testSelect("SHOW CATALOGS LIKE 'tpch'", matcher);
  }
}

TEST_F(PrestoParserTest, showTables) {
  testSelect("SHOW TABLES", matchValues());
  testSelect("SHOW TABLES FROM default", matchValues());
  testSelect("SHOW TABLES LIKE 'line%'", matchValues().filter());
  testSelect("SHOW TABLES FROM default LIKE 'nation'", matchValues().filter());

  VELOX_ASSERT_THROW(
      parseSelect("SHOW TABLES FROM a.b.c"), "Invalid schema reference");

  VELOX_ASSERT_THROW(
      parseSelect("SHOW TABLES FROM unknown_catalog.tiny"), "not registered");

  VELOX_ASSERT_THROW(
      parseSelect("SHOW TABLES FROM nonexistent_schema"),
      "Schema does not exist");
}

TEST_F(PrestoParserTest, describe) {
  auto matcher = matchValues();
  testSelect("DESCRIBE nation", matcher);

  testSelect("DESC orders", matcher);

  testSelect("SHOW COLUMNS FROM lineitem", matcher);
}

TEST_F(PrestoParserTest, showFunctions) {
  {
    auto matcher = matchValues();
    testSelect("SHOW FUNCTIONS", matcher);
  }

  {
    auto matcher = matchValues().filter();
    testSelect("SHOW FUNCTIONS LIKE 'array%'", matcher);
  }
}

TEST_F(PrestoParserTest, showStats) {
  auto matcher = matchValues();
  testSelect("SHOW STATS FOR nation", matcher);

  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      parseSelect("SHOW STATS FOR no_such_table"), "Table not found");

  // SHOW STATS FOR (<query>) returns a ShowStatsForQueryStatement wrapping
  // the inner SELECT's logical plan.
  {
    auto statement = parseSql("SHOW STATS FOR (SELECT * FROM nation)");
    ASSERT_TRUE(statement->isShowStatsForQuery());

    auto* showStats = statement->as<ShowStatsForQueryStatement>();
    ASSERT_TRUE(showStats->statement()->isSelect());
  }

  {
    auto statement = parseSql(
        "SHOW STATS FOR (SELECT n_nationkey FROM nation WHERE n_regionkey = 1)");
    ASSERT_TRUE(statement->isShowStatsForQuery());

    auto* showStats = statement->as<ShowStatsForQueryStatement>();
    ASSERT_TRUE(showStats->statement()->isSelect());
  }
}

TEST_F(PrestoParserTest, unqualifiedAccessAfterJoin) {
  auto matcher = matchScan()
                     .join(matchScan().build())
                     .project()
                     .project()
                     .output({"n_name"});
  testSelect(
      "SELECT n_name FROM (SELECT n1.n_name as n_name FROM nation n1, nation n2)",
      matcher);
}

TEST_F(PrestoParserTest, duplicateAliases) {
  {
    auto matcher = matchValues().project().project().output({"x", "x"});
    testSelect(
        "SELECT a as x, b as x FROM (VALUES (1, 2)) AS t(a, b)", matcher);
  }

  {
    auto matcher =
        matchValues().project().unnest().project().output({"x", "x"});
    testSelect(
        "SELECT a as x, u.x FROM (VALUES (1, ARRAY[10, 20])) AS t(a, b) "
        "CROSS JOIN UNNEST(b) AS u(x)",
        matcher);
  }

  // Duplicate aliases with aggregates.
  {
    auto matcher =
        matchValues().project().aggregate().project().output({"x", "x"});
    testSelect(
        "SELECT sum(a) as x, sum(b) as x FROM (VALUES (1, 2)) AS t(a, b)",
        matcher);
  }

  // Referencing a duplicate column shoud fail.
  VELOX_ASSERT_THROW(
      parseSql(
          "SELECT x FROM (SELECT a as x, b as x FROM (VALUES (1, 2)) AS t(a, b))"),
      "Cannot resolve column: x");
}

TEST_F(PrestoParserTest, outputNames) {
  auto test = [&](const std::string& sql,
                  const std::vector<std::string>& expectedNames) {
    SCOPED_TRACE(sql);
    auto plan = parseSelect(sql);
    EXPECT_THAT(
        plan->outputType()->names(), testing::ElementsAreArray(expectedNames));
  };

  // Direct SELECT with empty and duplicate aliases.
  test(R"(SELECT 1 as "", 2 as "", 3 as x, 4 as x)", {"", "", "x", "x"});

  // SELECT * from subquery preserves empty and duplicate names.
  test(
      R"(SELECT * FROM (SELECT 1 as "", 2 as "", 3 as x, 4 as x))",
      {"", "", "x", "x"});

  // SELECT t.* from subquery preserves empty and duplicate names.
  test(
      R"(SELECT t.* FROM (SELECT 1 as "", 2 as "", 3 as x, 4 as x) t)",
      {"", "", "x", "x"});

  // Cross join of subqueries preserves empty and duplicate names.
  test(
      R"(SELECT * FROM (SELECT 1 as ""), (SELECT 2 as ""), )"
      R"((SELECT 3 as x), (SELECT 4 as x))",
      {"", "", "x", "x"});

  // Mixed qualified star preserves empty and duplicate names per alias.
  test(
      R"(SELECT t.*, v.*, w.* FROM )"
      R"((SELECT 1 as "") as t, )"
      R"((SELECT 2 as "") as u, )"
      R"((SELECT 3 as x) as v, )"
      R"((SELECT 4 as x) as w)",
      {"", "x", "x"});

  // Column aliases override empty output names from inner subquery.
  test(R"(SELECT * FROM (SELECT 1 as "", 2 as "") t(a, b))", {"a", "b"});

  // Mixed select with * and explicit columns preserves empty names.
  test(R"(SELECT *, 5 as y FROM (SELECT 1 as ""))", {"", "y"});

  // Double * preserves empty names in both expansions.
  test(R"(SELECT *, * FROM (SELECT 1 as ""))", {"", ""});

  // Double * with explicit columns in between.
  test(
      "SELECT *, 'foo' as x, 'bar' as y, * FROM (SELECT 1 as x)",
      {"x", "x", "y", "x"});

  // Nested subqueries preserve output names through multiple layers.
  test(
      R"(SELECT * FROM (SELECT * FROM (SELECT 1 as "", 2 as x, 3 as x)))",
      {"", "x", "x"});

  // UNION uses left side's output names.
  test(R"(SELECT 1 as "", 2 as x UNION ALL SELECT 3, 4)", {"", "x"});

  // UNNEST with duplicate column aliases.
  test(
      "SELECT * FROM UNNEST(ARRAY[1, 2, 3], ARRAY[1, 2, 3]) AS t(x, x)",
      {"x", "x"});

  // UNNEST with empty column aliases.
  test(
      R"(SELECT * FROM UNNEST(ARRAY[1, 2, 3], ARRAY[1, 2, 3]) AS t("", ""))",
      {"", ""});

  // UNNEST without aliases produces auto-generated names.
  {
    auto plan =
        parseSelect("SELECT * FROM UNNEST(ARRAY[1, 2, 3], ARRAY[1, 2, 3])");
    const auto& names = plan->outputType()->names();
    EXPECT_EQ(2, names.size());
    EXPECT_FALSE(names[0].empty());
    EXPECT_FALSE(names[1].empty());
    EXPECT_NE(names[0], names[1]);
  }

  // Bare expression uses default name; explicit empty alias uses "".
  {
    auto plan = parseSelect(R"(SELECT 1, 1 as "")");
    const auto& names = plan->outputType()->names();
    EXPECT_EQ(2, names.size());
    EXPECT_FALSE(names[0].empty());
    EXPECT_TRUE(names[1].empty());
  }

  // Duplicate qualified star produces duplicate column names.
  test(
      "SELECT nation.*, nation.* FROM nation",
      {"n_nationkey",
       "n_name",
       "n_regionkey",
       "n_comment",
       "n_nationkey",
       "n_name",
       "n_regionkey",
       "n_comment"});

  // Duplicate output column names are preserved as-is (not deduplicated to
  // x, x_0).
  test("SELECT a as x, b as x FROM (VALUES (1, 2)) AS t(a, b)", {"x", "x"});

  // SELECT * over a subquery with duplicate output aliases preserves the
  // duplicate aliases.
  test(
      "SELECT * FROM (SELECT a as x, b as x FROM (VALUES (1, 2)) AS t(a, b))",
      {"x", "x"});
}

// Explicit SELECT aliases preserve the user's original case in the output
// schema, even though identifier matching is case-insensitive.
TEST_F(PrestoParserTest, outputNamesPreserveAliasCase) {
  auto outputNames = [&](const std::string& sql) {
    SCOPED_TRACE(sql);
    return parseSelect(sql)->outputType()->names();
  };

  EXPECT_THAT(
      outputNames("SELECT 1 AS FooBar, 2 AS MyCol"),
      testing::ElementsAre("FooBar", "MyCol"));

  EXPECT_THAT(
      outputNames(R"(SELECT 1 AS "FooBar", 2 AS "MyCol")"),
      testing::ElementsAre("FooBar", "MyCol"));

  // Two aliases with the same canonical form are kept as distinct output
  // columns, each with its user-written case.
  EXPECT_THAT(
      outputNames(R"(SELECT 1 AS "ABC", 2 AS "abc")"),
      testing::ElementsAre("ABC", "abc"));

  EXPECT_THAT(outputNames("SELECT 1 AS foo"), testing::ElementsAre("foo"));

  // Column reference: output name follows the reference's written case,
  // not the source's.
  EXPECT_THAT(
      outputNames("SELECT FooBar FROM (SELECT 1 AS FooBar) s"),
      testing::ElementsAre("FooBar"));

  EXPECT_THAT(
      outputNames("SELECT FOOBAR FROM (SELECT 1 AS FooBar) s"),
      testing::ElementsAre("FOOBAR"));

  EXPECT_THAT(
      outputNames(R"(SELECT "FooBar" FROM (SELECT 1 AS FooBar) s)"),
      testing::ElementsAre("FooBar"));

  // Global aggregation over a multi-column subquery parses and yields a
  // single output column.
  EXPECT_NO_THROW(parseSelect("SELECT sum(z) FROM (SELECT 1 AS x, 2 AS z) s"));

  // Global aggregation preserves the user-written alias case.
  EXPECT_THAT(
      outputNames("SELECT sum(z) AS Total FROM (SELECT 1 AS z) s"),
      testing::ElementsAre("Total"));

  // An IN/scalar subquery in WHERE does not affect the outer SELECT's
  // output names.
  EXPECT_THAT(
      outputNames("SELECT 1 AS OuterCol WHERE 1 IN (SELECT 1 AS InnerCol)"),
      testing::ElementsAre("OuterCol"));

  // Set-operation output names come from the left input.
  EXPECT_THAT(
      outputNames("SELECT 1 AS FooBar UNION ALL SELECT 2 AS Bar"),
      testing::ElementsAre("FooBar"));
  EXPECT_THAT(
      outputNames("SELECT 1 AS FooBar INTERSECT SELECT 2 AS Bar"),
      testing::ElementsAre("FooBar"));
  EXPECT_THAT(
      outputNames("SELECT 1 AS FooBar EXCEPT SELECT 2 AS Bar"),
      testing::ElementsAre("FooBar"));

  // Qualified column reference: output name follows the rightmost field's
  // written case.
  EXPECT_THAT(
      outputNames("SELECT s.FooBar FROM (SELECT 1 AS FooBar) s"),
      testing::ElementsAre("FooBar"));

  // Star expansion propagates the source relation's per-column case.
  EXPECT_THAT(
      outputNames("SELECT * FROM (SELECT 1 AS FooBar) s"),
      testing::ElementsAre("FooBar"));

  EXPECT_THAT(
      outputNames("SELECT s.* FROM (SELECT 1 AS FooBar) s"),
      testing::ElementsAre("FooBar"));

  // CTE star expansion propagates the inner SELECT's case.
  EXPECT_THAT(
      outputNames("WITH cte AS (SELECT 1 AS FooBar) SELECT * FROM cte"),
      testing::ElementsAre("FooBar"));
  EXPECT_THAT(
      outputNames("WITH cte AS (SELECT 1 AS FooBar) SELECT cte.* FROM cte"),
      testing::ElementsAre("FooBar"));

  // CTE with an explicit column-alias list uses the alias-list case.
  EXPECT_THAT(
      outputNames("WITH cte(Bar) AS (SELECT 1 AS FooBar) SELECT * FROM cte"),
      testing::ElementsAre("Bar"));

  // Aliased subquery with an explicit column-alias list uses the
  // alias-list case.
  EXPECT_THAT(
      outputNames("SELECT * FROM (SELECT 1) s(Bar)"),
      testing::ElementsAre("Bar"));

  // Star over a set operation propagates the left side's case (set-op
  // output columns come from the left).
  EXPECT_THAT(
      outputNames(
          "SELECT * FROM ((SELECT 1 AS FooBar) UNION ALL (SELECT 2 AS Bar)) u"),
      testing::ElementsAre("FooBar"));
}

TEST_F(PrestoParserTest, renameToAliasMatchingInnerColumnName) {
  // A SELECT-list rename must be honored even when the alias coincides
  // with a column name appearing in an inner subquery.
  EXPECT_THAT(
      parseSelect(
          "WITH src AS (SELECT 1 AS x_2024),"
          "     renamed AS (SELECT x_2024 AS x FROM src) "
          "SELECT x FROM renamed")
          ->outputType()
          ->names(),
      testing::ElementsAre("x"));
}

TEST_F(PrestoParserTest, qualifiedStarInUnionAfterJoin) {
  auto matcher = matchValues()
                     .project()
                     .unionAll(
                         matchValues()
                             .project()
                             .join(matchValues().project().build())
                             .project()
                             .build())
                     .output({"id"});
  testSelect(
      "SELECT * FROM (VALUES (1)) t(id) "
      "UNION ALL "
      "SELECT a.* FROM (VALUES (1)) a(id) JOIN (VALUES (2)) b(id) ON a.id = b.id",
      matcher);
}

TEST_F(PrestoParserTest, qualifiedStarWithAmbiguousColumnAfterJoin) {
  // SELECT t.* after a JOIN where both sides have columns with the same names.
  // Verifies that t.* correctly resolves ambiguous column names using qualified
  // references.
  testSelect(
      "SELECT n1.* FROM nation n1 JOIN nation n2 ON n1.n_regionkey = n2.n_regionkey",
      matchScan()
          .join(matchScan().build())
          .project({
              "n_nationkey",
              "n_name",
              "n_regionkey",
              "n_comment",
          })
          .output({"n_nationkey", "n_name", "n_regionkey", "n_comment"}));
}

// A SELECT projection alias that collides with a table alias must not be
// substituted for the table alias when used as a dereference qualifier
// later in the same SELECT. The qualifier in a dereference is a scope
// reference, not a value, so lateral column alias substitution must not
// apply to it. The JOIN with same-named columns keeps the qualifier from
// being dropped before reaching the resolution path under test.
TEST_F(PrestoParserTest, lateralAliasCollidingWithTableAlias) {
  testSelect(
      "SELECT a.x + 1 AS a, a.x + 2 "
      "FROM (VALUES (1)) t(x) JOIN (VALUES (1)) a(x) ON t.x = a.x",
      matchValues()
          .project()
          .join(matchValues().project().build(), {"t_x", "a_x"})
          .project({"a_x + 1", "a_x + 2"})
          .output());
}

// FETCH FIRST n ROWS ONLY is equivalent to LIMIT n. Each LIMIT query below is
// paired with a FETCH FIRST query to verify they produce the same plan.
TEST_F(PrestoParserTest, limit) {
  auto nationColumns = std::vector<std::string>{
      "n_nationkey", "n_name", "n_regionkey", "n_comment"};

  {
    auto matcher = matchScan().limit(0, 10).output(nationColumns);
    testSelect("SELECT * FROM nation LIMIT 10", matcher);
    testSelect("SELECT * FROM nation FETCH FIRST 10 ROWS ONLY", matcher);
  }

  {
    auto matcher =
        matchScan().aggregate().limit(0, 5).output({"n_regionkey", "count"});
    testSelect(
        "SELECT n_regionkey, count(1) FROM nation GROUP BY 1 LIMIT 5", matcher);
    testSelect(
        "SELECT n_regionkey, count(1) FROM nation GROUP BY 1 FETCH FIRST 5 ROWS ONLY",
        matcher);
  }

  {
    auto matcher =
        matchScan().sort({"n_name"}).limit(0, 100).output(nationColumns);
    testSelect("SELECT * FROM nation ORDER BY n_name LIMIT 100", matcher);
    testSelect(
        "SELECT * FROM nation ORDER BY n_name FETCH FIRST 100 ROWS ONLY",
        matcher);
  }

  {
    auto matcher = matchScan().limit(0, 2'147'483'647).output(nationColumns);
    testSelect("SELECT * FROM nation LIMIT 2147483647", matcher);
  }

  {
    auto matcher = matchScan().limit(0, 2'147'483'648).output(nationColumns);
    testSelect("SELECT * FROM nation LIMIT 2147483648", matcher);
  }

  {
    auto matcher = matchScan().limit(0, 24'859'023'574).output(nationColumns);
    testSelect("SELECT * FROM nation LIMIT 24859023574", matcher);
  }

  // LIMIT ALL means "no limit" — should not add a LimitNode.
  {
    auto matcher = matchScan().output(nationColumns);
    testSelect("SELECT * FROM nation LIMIT ALL", matcher);
  }

  {
    auto matcher = matchScan().sort({"n_name"}).output(nationColumns);
    testSelect("SELECT * FROM nation ORDER BY n_name LIMIT ALL", matcher);
  }
}

TEST_F(PrestoParserTest, offset) {
  auto nationColumns = std::vector<std::string>{
      "n_nationkey", "n_name", "n_regionkey", "n_comment"};

  {
    auto matcher = matchScan()
                       .limit(5, std::numeric_limits<int64_t>::max())
                       .output(nationColumns);
    testSelect("SELECT * FROM nation OFFSET 5", matcher);
  }

  {
    auto matcher = matchScan()
                       .limit(5, std::numeric_limits<int64_t>::max())
                       .limit(0, 10)
                       .output(nationColumns);
    testSelect("SELECT * FROM nation OFFSET 5 LIMIT 10", matcher);
    testSelect(
        "SELECT * FROM nation OFFSET 5 FETCH FIRST 10 ROWS ONLY", matcher);
  }

  {
    auto matcher = matchScan()
                       .limit(1, std::numeric_limits<int64_t>::max())
                       .output(nationColumns);
    testSelect("SELECT * FROM nation OFFSET 1", matcher);
  }

  {
    auto matcher = matchScan().output(nationColumns);
    testSelect("SELECT * FROM nation OFFSET 0", matcher);
  }

  {
    auto matcher = matchScan().sort({"n_name"}).output(nationColumns);
    testSelect("SELECT * FROM nation ORDER BY n_name OFFSET 0", matcher);
  }

  {
    auto matcher = matchScan().limit(0, 10).output(nationColumns);
    testSelect("SELECT * FROM nation OFFSET 0 LIMIT 10", matcher);
  }

  {
    auto matcher = matchScan().limit(0, 0).output(nationColumns);
    testSelect("SELECT * FROM nation OFFSET 0 LIMIT 0", matcher);
  }
}

TEST_F(PrestoParserTest, use) {
  {
    auto statement = parseSql("USE my_schema");
    ASSERT_TRUE(statement->isUse());

    const auto* use = statement->as<UseStatement>();
    ASSERT_FALSE(use->catalog().has_value());
    ASSERT_EQ("my_schema", use->schema());
  }

  {
    auto statement = parseSql("USE my_catalog.my_schema");
    ASSERT_TRUE(statement->isUse());

    const auto* use = statement->as<UseStatement>();
    ASSERT_TRUE(use->catalog().has_value());
    ASSERT_EQ("my_catalog", use->catalog().value());
    ASSERT_EQ("my_schema", use->schema());
  }
}

TEST_F(PrestoParserTest, windowFunction) {
  // SQL standard default frame when ORDER BY is present without explicit frame
  // is RANGE UNBOUNDED PRECEDING to CURRENT ROW.
  testSelect(
      "SELECT n_name, row_number() OVER (ORDER BY n_nationkey) FROM nation",
      matchScan()
          .project({
              "n_name",
              "row_number() OVER (ORDER BY n_nationkey ASC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
          })
          .output());
}

TEST_F(PrestoParserTest, nestedWindowFunction) {
  connector_->addTable("t", ROW({"a", "b"}, {BIGINT(), BIGINT()}));

  // Window function nested in an arithmetic expression produces two project
  // nodes: the first computes the window function, the second applies the
  // scalar expression using a column reference to the window result.
  testSelect(
      "SELECT sum(a) OVER (PARTITION BY b) * 2 AS doubled FROM t",
      matchScan("t")
          .project({
              "a",
              "b",
              "sum(a) OVER (PARTITION BY b) AS w",
          })
          .project({"w * 2::bigint"})
          .output({"doubled"}));

  // Multiple nested window functions with aliases.
  testSelect(
      "SELECT sum(a) OVER (PARTITION BY b) + 1 AS s, "
      "count(*) OVER () * 2 AS c FROM t",
      matchScan("t")
          .project({
              "a",
              "b",
              "sum(a) OVER (PARTITION BY b) AS s",
              "count() OVER () AS c",
          })
          .project({"s + 1::bigint", "c * 2::bigint"})
          .output({"s", "c"}));

  // Mix of top-level and nested window functions. Only the nested window
  // (inside the multiply) is extracted into its own project. The top-level
  // window stays in the main project alongside the scalar expression.
  testSelect(
      "SELECT row_number() OVER (ORDER BY a) AS rn, "
      "sum(a) OVER (PARTITION BY b) * 2 AS doubled FROM t",
      matchScan("t")
          .project({
              "a",
              "b",
              "sum(a) OVER (PARTITION BY b) AS w",
          })
          .project({
              "row_number() OVER (ORDER BY a ASC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
              "w * 2::bigint",
          })
          .output({"rn", "doubled"}));

  // Window function without alias in expression.
  testSelect(
      "SELECT b, sum(a) OVER (PARTITION BY b) * 2 FROM t",
      matchScan("t")
          .project({
              "a",
              "b",
              "sum(a) OVER (PARTITION BY b) AS w",
          })
          .project({"b", "w * 2::bigint"})
          .output());
}

TEST_F(PrestoParserTest, rangeWindowFrame) {
  connector_->addTable("t", ROW({"a", "b"}, INTEGER()));

  // RANGE BETWEEN n PRECEDING AND n FOLLOWING over ASC ordering rewrites each
  // bound as a materialized boundary value of the ORDER BY type: 'a - n' for
  // PRECEDING, 'a + n' for FOLLOWING.
  testSelect(
      "SELECT a, sum(b) OVER (ORDER BY a RANGE BETWEEN 1 PRECEDING AND 2 FOLLOWING) FROM t",
      matchScan("t")
          .project({
              "a",
              "sum(b) OVER (ORDER BY a ASC NULLS LAST RANGE BETWEEN minus(a, 1) PRECEDING AND plus(a, 2) FOLLOWING)",
          })
          .output());

  // DESC ordering flips the operator selection: PRECEDING uses plus, FOLLOWING
  // uses minus.
  testSelect(
      "SELECT a, sum(b) OVER (ORDER BY a DESC RANGE BETWEEN 1 PRECEDING AND 2 FOLLOWING) FROM t",
      matchScan("t")
          .project({
              "a",
              "sum(b) OVER (ORDER BY a DESC NULLS LAST RANGE BETWEEN plus(a, 1) PRECEDING AND minus(a, 2) FOLLOWING)",
          })
          .output());

  // RANGE without offset bounds is left unchanged.
  testSelect(
      "SELECT a, sum(b) OVER (ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
      matchScan("t")
          .project({
              "a",
              "sum(b) OVER (ORDER BY a ASC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
          })
          .output());

  // ROWS frames are not rewritten; the integer bound passes through.
  testSelect(
      "SELECT a, sum(b) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) FROM t",
      matchScan("t")
          .project({
              "a",
              "sum(b) OVER (ORDER BY a ASC NULLS LAST ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING)",
          })
          .output());

  // RANGE + offset bound requires exactly one ORDER BY key.
  VELOX_ASSERT_THROW(
      parseSql(
          "SELECT a, sum(b) OVER (ORDER BY a, b RANGE BETWEEN 1 PRECEDING AND 2 FOLLOWING) FROM t"),
      "RANGE frame with offset bound requires exactly one ORDER BY key");
}

TEST_F(PrestoParserTest, friendlySqlTrailingCommaSelect) {
  auto statement = makeParser().parse("SELECT 1, 2, FROM nation");
  ASSERT_TRUE(statement->isSelect());

  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      makeStrictParser().parse("SELECT 1, 2, FROM nation"),
      "Trailing commas in SELECT list require Friendly SQL mode");

  // No trailing comma works in both modes.
  statement = makeStrictParser().parse("SELECT 1, 2 FROM nation");
  ASSERT_TRUE(statement->isSelect());
}

TEST_F(PrestoParserTest, friendlySqlTrailingCommaWith) {
  auto statement = makeParser().parse(
      "WITH a AS (SELECT 1), b AS (SELECT 2), SELECT * FROM a, b");
  ASSERT_TRUE(statement->isSelect());

  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      makeStrictParser().parse(
          "WITH a AS (SELECT 1), b AS (SELECT 2), SELECT * FROM a, b"),
      "Trailing commas in WITH clause require Friendly SQL mode");

  // No trailing comma works in both modes.
  statement = makeStrictParser().parse(
      "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a, b");
  ASSERT_TRUE(statement->isSelect());

  // Commas inside subqueries don't count as trailing commas.
  statement =
      makeStrictParser().parse("WITH a AS (SELECT 1, 2, 3) SELECT * FROM a");
  ASSERT_TRUE(statement->isSelect());
}

TEST_F(PrestoParserTest, friendlySqlTrailingCommaValues) {
  auto statement = makeParser().parse("VALUES 1, 2, 3,");
  ASSERT_TRUE(statement->isSelect());

  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      makeStrictParser().parse("VALUES 1, 2, 3,"),
      "Trailing commas in VALUES clause require Friendly SQL mode");

  // No trailing comma works in both modes.
  statement = makeStrictParser().parse("VALUES 1, 2, 3");
  ASSERT_TRUE(statement->isSelect());

  // Commas inside ROW expressions don't count as trailing commas.
  statement = makeStrictParser().parse("VALUES ROW(1, 2), ROW(3, 4)");
  ASSERT_TRUE(statement->isSelect());
}

TEST_F(PrestoParserTest, friendlySqlFromFirst) {
  // FROM-first syntax works with Friendly SQL (default).
  auto parser = makeParser();
  auto statement = parser.parse("FROM nation");
  ASSERT_TRUE(statement->isSelect());

  // With WHERE clause.
  statement = parser.parse("FROM nation WHERE n_regionkey = 1");
  ASSERT_TRUE(statement->isSelect());

  // Rejected when Friendly SQL is disabled.
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      makeStrictParser().parse("FROM nation"),
      "FROM-first syntax requires Friendly SQL mode");
}

TEST_F(PrestoParserTest, syntaxErrorHasMessageTemplate) {
  try {
    parseSql("SELECT * FROM WHERE");
    ADD_FAILURE() << "Expected PrestoSqlError";
  } catch (const PrestoSqlError& e) {
    EXPECT_EQ(e.kind(), PrestoSqlErrorKind::kSyntax);
    EXPECT_FALSE(std::string_view(e.messageTemplate()).empty())
        << "ANTLR syntax errors must have a messageTemplate for Scuba grouping";
  }
}

TEST_F(PrestoParserTest, semanticErrorHasMessageTemplate) {
  try {
    parseSql("SELECT * FROM nonexistent_table_xyz");
    ADD_FAILURE() << "Expected PrestoSqlError";
  } catch (const PrestoSqlError& e) {
    EXPECT_EQ(e.kind(), PrestoSqlErrorKind::kSemantic);
    EXPECT_FALSE(std::string_view(e.messageTemplate()).empty())
        << "Semantic errors must have a messageTemplate for Scuba grouping";
    EXPECT_EQ(std::string(e.messageTemplate()), "Table not found: {}");
  }
}

} // namespace
} // namespace axiom::sql::presto::test
