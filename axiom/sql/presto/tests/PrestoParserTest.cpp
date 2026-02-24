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

#include "axiom/sql/presto/PrestoParseError.h"
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

  testSelect(
      "SELECT * FROM unnest(array[1, 2, 3], array[4, 5]) with ordinality as t(x, y)",
      matchValues().unnest().project().output({"x", "y"}));

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
    auto matcher = matchScan().project().output({"x"});

    // Qualified and unqualified column access.
    testSelect("SELECT t.x FROM t", matcher);
    testSelect("SELECT x FROM t", matcher);

    // Case insensitive column and table alias.
    testSelect("SELECT t.X FROM t", matcher);
    testSelect("SELECT T.X FROM t", matcher);
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

TEST_F(PrestoParserTest, syntaxErrors) {
  auto parser = makeParser();
  EXPECT_THAT(
      [&]() { parser.parse("SELECT * FROM"); },
      ThrowsMessage<axiom::sql::presto::PrestoParseError>(::testing::HasSubstr(
          "Syntax error at 1:13: mismatched input '<EOF>'")));

  EXPECT_THAT(
      [&]() {
        parser.parse(
            "SELECT * FROM nation\n"
            "WHERE");
      },
      ThrowsMessage<axiom::sql::presto::PrestoParseError>(::testing::HasSubstr(
          "Syntax error at 2:5: mismatched input '<EOF>'")));

  EXPECT_THAT(
      [&]() { parser.parse("SELECT * FROM (VALUES 1, 2, 3)) blah..."); },
      ThrowsMessage<axiom::sql::presto::PrestoParseError>(::testing::HasSubstr(
          "Syntax error at 1:30: mismatched input ')' expecting <EOF>")));

  EXPECT_THAT(
      [&]() { parser.parse("CREATE TABLE t (price DECIMAL('abc', 'xyz'))"); },
      ThrowsMessage<axiom::sql::presto::PrestoParseError>(::testing::HasSubstr(
          "Syntax error at 1:30: mismatched input ''abc''")));
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
  {
    auto matcher = matchScan().project().output({"n_name", "n_regionkey"});
    testSelect("SELECT N_NAME, n_ReGiOnKeY FROM nation", matcher);
  }
  {
    auto matcher = matchScan().project().output({"n_name"});
    testSelect("SELECT nation.n_name FROM nation", matcher);
    testSelect("SELECT NATION.n_name FROM nation", matcher);
    testSelect("SELECT \"NATION\".n_name FROM nation", matcher);
  }
}

TEST_F(PrestoParserTest, with) {
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

TEST_F(PrestoParserTest, withShadowing) {
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
  VELOX_ASSERT_THROW(
      parseSql(
          "SELECT * FROM (WITH t AS (SELECT 1 AS x) SELECT * FROM t) sub "
          "CROSS JOIN t"),
      "Table not found: t");
}

TEST_F(PrestoParserTest, orderBy) {
  {
    auto matcher =
        matchScan().aggregate().sort().project().output({"n_regionkey"});

    testSelect(
        "select n_regionkey from nation group by 1 order by count(1)", matcher);
  }

  {
    auto matcher =
        matchScan().aggregate().sort().output({"n_regionkey", "count"});

    testSelect(
        "select n_regionkey, count(1) from nation group by 1 order by count(1)",
        matcher);

    testSelect(
        "select n_regionkey, count(1) from nation group by 1 order by 2",
        matcher);

    testSelect(
        "select n_regionkey, count(1) as c from nation group by 1 order by c",
        matchScan().aggregate().sort().output({"n_regionkey", "c"}));
  }

  {
    auto matcher = matchScan().aggregate().project().sort().output();

    testSelect(
        "select n_regionkey, count(1) * 2 from nation group by 1 order by 2",
        matcher);

    testSelect(
        "select n_regionkey, count(1) * 2 from nation group by 1 order by count(1) * 2",
        matcher);
  }

  {
    auto matcher = matchScan().aggregate().project().sort().project().output();
    testSelect(
        "select n_regionkey, count(1) * 2 from nation group by 1 order by count(1) * 3",
        matcher);
  }
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
  auto matcher = matchScan()
                     .project()
                     .unionAll(matchScan().project().build())
                     .output({"n_name"});

  testSelect(
      "SELECT n_name FROM nation UNION ALL SELECT r_name FROM region", matcher);

  // 3-way UNION ALL produces nested SetNodes.
  auto matcher3 = matchScan()
                      .project()
                      .unionAll(matchScan().project().build())
                      .unionAll(matchScan().project().build())
                      .output({"n_name"});

  testSelect(
      "SELECT n_name FROM nation "
      "UNION ALL SELECT r_name FROM region "
      "UNION ALL SELECT n_name FROM nation",
      matcher3);
}

TEST_F(PrestoParserTest, union) {
  auto matcher = matchScan()
                     .project()
                     .unionAll(matchScan().project().build())
                     .distinct()
                     .output({"n_name"});

  // UNION and UNION DISTINCT are equivalent.
  testSelect(
      "SELECT n_name FROM nation UNION SELECT r_name FROM region", matcher);
  testSelect(
      "SELECT n_name FROM nation UNION DISTINCT SELECT r_name FROM region",
      matcher);
}

TEST_F(PrestoParserTest, except) {
  // EXCEPT and EXCEPT DISTINCT are equivalent. Both add distinct for
  // deduplication.
  auto matcher = matchScan()
                     .project()
                     .except(matchScan().project().build())
                     .distinct()
                     .output({"n_name"});

  testSelect(
      "SELECT n_name FROM nation EXCEPT SELECT r_name FROM region", matcher);
  testSelect(
      "SELECT n_name FROM nation EXCEPT DISTINCT SELECT r_name FROM region",
      matcher);

  // EXCEPT ALL skips deduplication.
  auto matcherAll = matchScan()
                        .project()
                        .except(matchScan().project().build())
                        .output({"n_name"});

  testSelect(
      "SELECT n_name FROM nation EXCEPT ALL SELECT r_name FROM region",
      matcherAll);
}

TEST_F(PrestoParserTest, intersect) {
  // INTERSECT and INTERSECT DISTINCT are equivalent. Both add distinct for
  // deduplication.
  auto matcher = matchScan()
                     .project()
                     .intersect(matchScan().project().build())
                     .distinct()
                     .output({"n_name"});

  testSelect(
      "SELECT n_name FROM nation INTERSECT SELECT r_name FROM region", matcher);
  testSelect(
      "SELECT n_name FROM nation INTERSECT DISTINCT SELECT r_name FROM region",
      matcher);

  // INTERSECT ALL skips deduplication.
  auto matcherAll = matchScan()
                        .project()
                        .intersect(matchScan().project().build())
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
                     .sort()
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
}

TEST_F(PrestoParserTest, explainShow) {
  auto matcher = matchValues();
  testExplain("EXPLAIN SHOW CATALOGS", matcher);

  testExplain("EXPLAIN SHOW COLUMNS FROM nation", matcher);

  testExplain("EXPLAIN SHOW FUNCTIONS", matcher);
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
    auto matcher = matchScan().sort().limit(0, 100).output(nationColumns);
    testSelect("SELECT * FROM nation ORDER BY n_name LIMIT 100", matcher);
    testSelect(
        "SELECT * FROM nation ORDER BY n_name FETCH FIRST 100 ROWS ONLY",
        matcher);
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
  testSelect(
      "SELECT n_name, row_number() OVER (ORDER BY n_nationkey) FROM nation",
      matchScan()
          .project({
              "n_name",
              "row_number() OVER (ORDER BY n_nationkey ASC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
          })
          .output());
}

} // namespace
} // namespace axiom::sql::presto::test
