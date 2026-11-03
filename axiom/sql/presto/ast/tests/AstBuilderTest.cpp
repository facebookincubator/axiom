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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "axiom/sql/presto/ast/AstBuilder.h"
#include "axiom/sql/presto/grammar/SyntaxParser.h"
#include "axiom/sql/presto/tests/ExpectPrestoSqlError.h"

namespace axiom::sql::presto {
namespace {

class AstBuilderTest : public testing::Test {
 protected:
  // Parses 'sql' with graph queries enabled.
  static StatementPtr parse(std::string_view sql) {
    ParserOptions options;
    options.graphQueries = true;
    return parse(sql, options);
  }

  static StatementPtr parse(
      std::string_view sql,
      const ParserOptions& options) {
    SyntaxParser syntaxParser(sql, options.maxExpressionDepth);
    AstBuilder builder(options);
    return std::any_cast<StatementPtr>(builder.visit(syntaxParser.parse()));
  }

  // Returns the AST of a standalone expression.
  static ExpressionPtr parseExpression(std::string_view sql) {
    auto query = std::dynamic_pointer_cast<Query>(
        parse(fmt::format("SELECT {}", sql), ParserOptions{}));
    const auto& item = query->queryBody()
                           ->as<QuerySpecification>()
                           ->select()
                           ->selectItems()[0];
    return item->as<SingleColumn>()->expression();
  }

  // Returns the FROM clause of 'sql'.
  static RelationPtr parseFrom(std::string_view sql) {
    auto query = std::dynamic_pointer_cast<Query>(parse(sql));
    return query->queryBody()->as<QuerySpecification>()->from();
  }

  // Returns the GRAPH_TABLE in the FROM clause of 'sql'.
  static std::shared_ptr<GraphTable> parseGraphTable(std::string_view sql) {
    auto graphTable = std::dynamic_pointer_cast<GraphTable>(parseFrom(sql));
    VELOX_CHECK_NOT_NULL(graphTable);
    return graphTable;
  }

  // Returns the single path pattern of 'match', a MATCH clause body.
  static PathPatternPtr parsePath(std::string_view match) {
    auto graphTable = parseGraphTable(
        fmt::format(
            "SELECT * FROM GRAPH_TABLE (g MATCH {} COLUMNS (1))", match));
    VELOX_CHECK_EQ(1, graphTable->paths().size());
    return graphTable->paths()[0];
  }

  // Returns the label of the single node pattern in 'match'.
  static LabelExpressionPtr parseNodeLabel(std::string_view match) {
    return parsePath(match)->elements()[0]->as<NodePattern>()->label();
  }

  static LabelExpressionPtr name(std::string_view value) {
    return std::make_shared<LabelExpression>(
        NodeLocation{1, 0},
        LabelExpression::Kind::kName,
        std::make_shared<Identifier>(
            NodeLocation{1, 0}, std::string(value), false),
        std::vector<LabelExpressionPtr>{});
  }

  static LabelExpressionPtr wildcard() {
    return std::make_shared<LabelExpression>(
        NodeLocation{1, 0},
        LabelExpression::Kind::kWildcard,
        nullptr,
        std::vector<LabelExpressionPtr>{});
  }

  static LabelExpressionPtr combine(
      LabelExpression::Kind kind,
      std::vector<LabelExpressionPtr> operands) {
    return std::make_shared<LabelExpression>(
        NodeLocation{1, 0}, kind, nullptr, std::move(operands));
  }
};

TEST_F(AstBuilderTest, graphTable) {
  auto graphTable = parseGraphTable(
      "SELECT * FROM GRAPH_TABLE (g "
      "MATCH (a IS A WHERE a.x = 1) -[k IS B]-> (b), (b) <- (c) "
      "WHERE b.y > 2 "
      "COLUMNS (b.x AS d, k.z))");

  EXPECT_EQ("g", graphTable->graph()->fullyQualifiedName());
  EXPECT_EQ(*parseExpression("b.y > 2"), *graphTable->where());
  ASSERT_EQ(2, graphTable->columns().size());
  EXPECT_EQ(*parseExpression("b.x"), *graphTable->columns()[0]->expression());
  EXPECT_EQ("d", graphTable->columns()[0]->alias()->value());
  EXPECT_EQ(*parseExpression("k.z"), *graphTable->columns()[1]->expression());
  EXPECT_EQ(nullptr, graphTable->columns()[1]->alias());

  ASSERT_EQ(2, graphTable->paths().size());
  const auto& elements = graphTable->paths()[0]->elements();
  ASSERT_EQ(3, elements.size());

  const auto* source = elements[0]->as<NodePattern>();
  ASSERT_NE(nullptr, source);
  EXPECT_EQ("a", source->variable()->value());
  EXPECT_EQ(*name("A"), *source->label());
  EXPECT_EQ(*parseExpression("a.x = 1"), *source->where());

  const auto* edge = elements[1]->as<EdgePattern>();
  ASSERT_NE(nullptr, edge);
  EXPECT_EQ(EdgePattern::Direction::kRight, edge->direction());
  EXPECT_EQ("k", edge->variable()->value());
  EXPECT_EQ(*name("B"), *edge->label());
  EXPECT_EQ(nullptr, edge->where());
  EXPECT_FALSE(edge->quantifier().has_value());

  const auto* target = elements[2]->as<NodePattern>();
  ASSERT_NE(nullptr, target);
  EXPECT_EQ("b", target->variable()->value());
  EXPECT_EQ(nullptr, target->label());
  EXPECT_EQ(nullptr, target->where());

  // The second path pattern shares variable 'b' with the first.
  const auto& second = graphTable->paths()[1]->elements();
  ASSERT_EQ(3, second.size());
  EXPECT_EQ("b", second[0]->as<NodePattern>()->variable()->value());
  EXPECT_EQ(
      EdgePattern::Direction::kLeft, second[1]->as<EdgePattern>()->direction());
  EXPECT_EQ("c", second[2]->as<NodePattern>()->variable()->value());
}

std::vector<std::string> names(const std::vector<IdentifierPtr>& identifiers) {
  std::vector<std::string> values;
  values.reserve(identifiers.size());
  for (const auto& identifier : identifiers) {
    values.push_back(identifier->value());
  }
  return values;
}

TEST_F(AstBuilderTest, tableFunction) {
  auto from = parseFrom(
      "SELECT * FROM TABLE(s.f("
      "q => TABLE(SELECT a FROM t) AS x, i => TABLE(u) v(b, c), "
      "d => DESCRIPTOR(a, b), k => 10, 1 + 2)) AS r");
  const auto* aliased = from->as<AliasedRelation>();
  ASSERT_NE(nullptr, aliased);
  EXPECT_EQ("r", aliased->alias()->value());

  const auto* call = aliased->relation()->as<TableFunctionInvocation>();
  ASSERT_NE(nullptr, call);
  EXPECT_EQ("s.f", call->name()->fullyQualifiedName());

  const auto& arguments = call->arguments();
  ASSERT_EQ(5, arguments.size());

  EXPECT_EQ("q", arguments[0]->name()->value());
  const auto* query = arguments[0]->table()->as<AliasedRelation>();
  ASSERT_NE(nullptr, query);
  EXPECT_EQ("x", query->alias()->value());
  EXPECT_TRUE(query->columnNames().empty());
  EXPECT_TRUE(query->relation()->is(NodeType::kTableSubquery));
  EXPECT_EQ(nullptr, arguments[0]->expression());

  EXPECT_EQ("i", arguments[1]->name()->value());
  const auto* table = arguments[1]->table()->as<AliasedRelation>();
  ASSERT_NE(nullptr, table);
  EXPECT_EQ("v", table->alias()->value());
  EXPECT_THAT(names(table->columnNames()), testing::ElementsAre("b", "c"));
  EXPECT_EQ("u", table->relation()->as<Table>()->name()->fullyQualifiedName());

  EXPECT_EQ("d", arguments[2]->name()->value());
  EXPECT_EQ(nullptr, arguments[2]->table());
  EXPECT_EQ(nullptr, arguments[2]->expression());
  EXPECT_THAT(
      names(arguments[2]->descriptor()), testing::ElementsAre("a", "b"));

  EXPECT_EQ("k", arguments[3]->name()->value());
  EXPECT_EQ(nullptr, arguments[3]->table());
  EXPECT_TRUE(arguments[3]->descriptor().empty());
  EXPECT_EQ(*parseExpression("10"), *arguments[3]->expression());

  EXPECT_EQ(nullptr, arguments[4]->name());
  EXPECT_EQ(*parseExpression("1 + 2"), *arguments[4]->expression());
}

TEST_F(AstBuilderTest, edgeDirections) {
  auto directionOf = [](std::string_view edge) {
    auto path = parsePath(fmt::format("(a) {} (b)", edge));
    return path->elements()[1]->as<EdgePattern>()->direction();
  };

  using Direction = EdgePattern::Direction;
  EXPECT_EQ(Direction::kRight, directionOf("-[e]->"));
  EXPECT_EQ(Direction::kRight, directionOf("->"));
  EXPECT_EQ(Direction::kLeft, directionOf("<-[e]-"));
  EXPECT_EQ(Direction::kLeft, directionOf("<-"));
  EXPECT_EQ(Direction::kAnyDirection, directionOf("-[e]-"));
  EXPECT_EQ(Direction::kAnyDirection, directionOf("-"));

  // An abbreviated edge has no variable, label or predicate.
  auto path = parsePath("(a)->(b)");
  const auto* abbreviated = path->elements()[1]->as<EdgePattern>();
  EXPECT_EQ(nullptr, abbreviated->variable());
  EXPECT_EQ(nullptr, abbreviated->label());
  EXPECT_EQ(nullptr, abbreviated->where());
}

TEST_F(AstBuilderTest, labelExpressions) {
  using Kind = LabelExpression::Kind;

  EXPECT_EQ(*wildcard(), *parseNodeLabel("(a IS %)"));
  EXPECT_EQ(
      *combine(Kind::kNegation, {name("A")}), *parseNodeLabel("(a IS !A)"));

  // '!' binds tightest, then '&', then '|'.
  EXPECT_EQ(
      *combine(
          Kind::kDisjunction,
          {name("A"),
           combine(
               Kind::kConjunction,
               {combine(Kind::kNegation, {name("B")}), name("C")})}),
      *parseNodeLabel("(a IS A | !B & C)"));

  EXPECT_EQ(
      *combine(
          Kind::kConjunction,
          {combine(Kind::kDisjunction, {name("A"), name("B")}), name("C")}),
      *parseNodeLabel("(a IS (A | B) & C)"));

  EXPECT_EQ(
      *combine(Kind::kDisjunction, {name("A"), name("B"), name("C")}),
      *parseNodeLabel("(a IS A | B | C)"));
}

TEST_F(AstBuilderTest, quantifiers) {
  auto quantifierOf = [](std::string_view quantifier) {
    auto path =
        parsePath(fmt::format("ANY SHORTEST (a) -[e]->{} (b)", quantifier));
    return path->elements()[1]->as<EdgePattern>()->quantifier().value();
  };

  EXPECT_EQ((GraphQuantifier{0, std::nullopt}), quantifierOf("*"));
  EXPECT_EQ((GraphQuantifier{1, std::nullopt}), quantifierOf("+"));
  EXPECT_EQ((GraphQuantifier{3, 3}), quantifierOf("{3}"));
  EXPECT_EQ((GraphQuantifier{1, 3}), quantifierOf("{1,3}"));
  EXPECT_EQ((GraphQuantifier{0, 3}), quantifierOf("{,3}"));
  EXPECT_EQ((GraphQuantifier{2, std::nullopt}), quantifierOf("{2,}"));
  EXPECT_EQ((GraphQuantifier{0, std::nullopt}), quantifierOf("{,}"));
  EXPECT_EQ((GraphQuantifier{1'000, 1'000}), quantifierOf("{1_000}"));
  // Zero repetitions match an empty path.
  EXPECT_EQ((GraphQuantifier{0, 0}), quantifierOf("{0}"));
  EXPECT_EQ((GraphQuantifier{0, 0}), quantifierOf("{0,0}"));

  AXIOM_EXPECT_PRESTO_SEMANTIC_ERROR(
      quantifierOf("{99999999999999999999}"),
      "Integer literal out of range: 99999999999999999999");
}

TEST_F(AstBuilderTest, pathPrefixes) {
  auto path = parsePath("p = ANY SHORTEST TRAIL PATH (a)->(b)");
  EXPECT_EQ("p", path->variable()->value());
  EXPECT_EQ(
      (PathSelector{PathSelector::Kind::kAnyShortest, std::nullopt}),
      path->selector());
  EXPECT_EQ(PathMode::kTrail, path->mode());

  auto selectorOf = [](std::string_view prefix) {
    return parsePath(fmt::format("{} (a)->(b)", prefix))->selector().value();
  };

  using Kind = PathSelector::Kind;
  EXPECT_EQ((PathSelector{Kind::kAll, std::nullopt}), selectorOf("ALL"));
  EXPECT_EQ((PathSelector{Kind::kAny, std::nullopt}), selectorOf("ANY"));
  EXPECT_EQ((PathSelector{Kind::kAny, 2}), selectorOf("ANY 2"));
  EXPECT_EQ(
      (PathSelector{Kind::kAllShortest, std::nullopt}),
      selectorOf("ALL SHORTEST"));
  EXPECT_EQ((PathSelector{Kind::kShortest, 3}), selectorOf("SHORTEST 3"));
  EXPECT_EQ(
      (PathSelector{Kind::kShortestGroups, 3}), selectorOf("SHORTEST 3 GROUP"));
  EXPECT_EQ(
      (PathSelector{Kind::kShortestGroups, std::nullopt}),
      selectorOf("SHORTEST GROUPS"));

  for (const auto* prefix : {"ANY 0", "SHORTEST 0", "SHORTEST 0 GROUP"}) {
    AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
        selectorOf(prefix), "Path selector count must be positive.");
  }

  auto modeOf = [](std::string_view prefix) {
    auto path = parsePath(fmt::format("{} (a)->(b)", prefix));
    EXPECT_FALSE(path->selector().has_value());
    return path->mode().value();
  };

  EXPECT_EQ(PathMode::kWalk, modeOf("WALK"));
  EXPECT_EQ(PathMode::kTrail, modeOf("TRAIL"));
  EXPECT_EQ(PathMode::kAcyclic, modeOf("ACYCLIC PATH"));
  EXPECT_EQ(PathMode::kSimple, modeOf("SIMPLE PATHS"));

  auto none = parsePath("(a)->(b)");
  EXPECT_EQ(nullptr, none->variable());
  EXPECT_FALSE(none->selector().has_value());
  EXPECT_FALSE(none->mode().has_value());
}

TEST_F(AstBuilderTest, parenthesizedPaths) {
  auto path = parsePath(
      "(a) (q = TRAIL (x) -[e]-> (y) WHERE e.w > 0){0,3} (b) "
      "((c) -> (d))?");
  ASSERT_EQ(4, path->elements().size());

  const auto* quantified = path->elements()[1]->as<ParenthesizedPathPattern>();
  ASSERT_NE(nullptr, quantified);
  EXPECT_EQ("q", quantified->variable()->value());
  EXPECT_EQ(PathMode::kTrail, quantified->mode());
  ASSERT_EQ(3, quantified->elements().size());
  EXPECT_EQ(
      "x", quantified->elements()[0]->as<NodePattern>()->variable()->value());
  EXPECT_EQ(
      "e", quantified->elements()[1]->as<EdgePattern>()->variable()->value());
  EXPECT_EQ(
      "y", quantified->elements()[2]->as<NodePattern>()->variable()->value());
  EXPECT_EQ(*parseExpression("e.w > 0"), *quantified->where());
  EXPECT_EQ((GraphQuantifier{0, 3}), quantified->quantifier());
  EXPECT_FALSE(quantified->isOptional());

  const auto* optional = path->elements()[3]->as<ParenthesizedPathPattern>();
  ASSERT_NE(nullptr, optional);
  EXPECT_EQ(nullptr, optional->variable());
  EXPECT_FALSE(optional->mode().has_value());
  ASSERT_EQ(3, optional->elements().size());
  EXPECT_EQ(
      "c", optional->elements()[0]->as<NodePattern>()->variable()->value());
  EXPECT_EQ(
      "d", optional->elements()[2]->as<NodePattern>()->variable()->value());
  EXPECT_EQ(nullptr, optional->where());
  EXPECT_TRUE(optional->isOptional());
  EXPECT_FALSE(optional->quantifier().has_value());
}

// Elements need not alternate between node and edge patterns.
TEST_F(AstBuilderTest, adjacentElements) {
  EXPECT_EQ(2, parsePath("(a) (b)")->elements().size());
  EXPECT_EQ(4, parsePath("(a) -> -> (b)")->elements().size());
  EXPECT_EQ(1, parsePath("-[e]->")->elements().size());
}

TEST_F(AstBuilderTest, unboundedQuantifierScope) {
  // Allowed under a selector other than ALL, or a path mode other than WALK,
  // including one on an enclosing subpath.
  parsePath("ANY SHORTEST (a) -[e]->* (b)");
  parsePath("ALL TRAIL (a) -[e]->+ (b)");
  parsePath("(a) (ACYCLIC (x) -[e]->+ (y)) (b)");
  parsePath("(a) -[e]->{1,5} (b)");

  const std::string_view kMessage =
      "Unbounded quantifier requires a selector other than ALL or a path "
      "mode other than WALK.";
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(parsePath("(a) -[e]->* (b)"), kMessage);
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(parsePath("ALL (a) -[e]->+ (b)"), kMessage);
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parsePath("WALK (a) -[e]->{2,} (b)"), kMessage);

  // A subpath's own mode does not cover a quantifier on the subpath itself.
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parsePath("(a) (TRAIL (x) -[e]-> (y))* (b)"), kMessage);
}

TEST_F(AstBuilderTest, quantifierBounds) {
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parsePath("(a) -[e]->{3,1} (b)"),
      "Quantifier lower bound must not exceed its upper bound.");
}

TEST_F(AstBuilderTest, colonLabelSyntax) {
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parsePath("(a:A) -> (b)"),
      "Labels are written with IS, as in (a IS Person).");

  // A quoted name may contain ':'.
  auto path = parsePath("(\"a:b\" IS A) -> (b)");
  EXPECT_EQ("a:b", path->elements()[0]->as<NodePattern>()->variable()->value());
}

TEST_F(AstBuilderTest, withGraph) {
  auto query = std::dynamic_pointer_cast<Query>(parse(
      "WITH w AS (SELECT 1), GRAPH g AS ("
      "  VERTEX TABLES ("
      "    t KEY (a) LABEL A PROPERTIES (a, upper(b) AS c),"
      "    v AS x KEY (a),"
      "    y KEY (a) PROPERTIES (b))"
      "  EDGE TABLES ("
      "    u KEY (a, b)"
      "      SOURCE KEY (a) REFERENCES t (a)"
      "      DESTINATION KEY (b) REFERENCES t (a)"
      "      LABEL B LABEL C PROPERTIES (c)))"
      "SELECT * FROM w"));

  const auto& with = query->with();
  ASSERT_EQ(1, with->queries().size());
  ASSERT_EQ(1, with->graphs().size());

  const auto& graph = with->graphs()[0];
  EXPECT_EQ("g", graph->name()->value());
  ASSERT_EQ(3, graph->vertexTables().size());
  ASSERT_EQ(1, graph->edgeTables().size());

  const auto& labeledVertex = graph->vertexTables()[0];
  EXPECT_FALSE(labeledVertex->isEdge());
  EXPECT_EQ("t", labeledVertex->table()->fullyQualifiedName());
  EXPECT_EQ(nullptr, labeledVertex->alias());
  ASSERT_EQ(1, labeledVertex->keyColumns().size());
  EXPECT_EQ("a", labeledVertex->keyColumns()[0]->value());
  ASSERT_EQ(1, labeledVertex->labels().size());
  EXPECT_EQ("A", labeledVertex->labels()[0].name->value());
  ASSERT_TRUE(labeledVertex->labels()[0].properties.has_value());
  const auto& properties = labeledVertex->labels()[0].properties.value();
  ASSERT_EQ(2, properties.size());
  EXPECT_EQ(*parseExpression("a"), *properties[0].expression);
  EXPECT_EQ(nullptr, properties[0].name);
  EXPECT_EQ(*parseExpression("upper(b)"), *properties[1].expression);
  EXPECT_EQ("c", properties[1].name->value());

  // No LABEL clause means one default label exposing all columns.
  const auto& aliasedVertex = graph->vertexTables()[1];
  EXPECT_EQ("x", aliasedVertex->alias()->value());
  EXPECT_TRUE(aliasedVertex->labels().empty());

  // PROPERTIES without LABEL applies to the default label.
  const auto& defaultLabelVertex = graph->vertexTables()[2];
  ASSERT_EQ(1, defaultLabelVertex->labels().size());
  EXPECT_EQ(nullptr, defaultLabelVertex->labels()[0].name);
  ASSERT_EQ(1, defaultLabelVertex->labels()[0].properties->size());
  EXPECT_EQ(
      *parseExpression("b"),
      *defaultLabelVertex->labels()[0].properties->at(0).expression);

  const auto& edgeTable = graph->edgeTables()[0];
  EXPECT_TRUE(edgeTable->isEdge());
  EXPECT_EQ("u", edgeTable->table()->fullyQualifiedName());
  ASSERT_EQ(2, edgeTable->keyColumns().size());
  EXPECT_EQ("a", edgeTable->keyColumns()[0]->value());
  EXPECT_EQ("b", edgeTable->keyColumns()[1]->value());

  const auto& source = edgeTable->source().value();
  EXPECT_EQ("t", source.vertexTable->value());
  ASSERT_EQ(1, source.keyColumns.size());
  EXPECT_EQ("a", source.keyColumns[0]->value());
  ASSERT_EQ(1, source.referencedColumns.size());
  EXPECT_EQ("a", source.referencedColumns[0]->value());

  const auto& destination = edgeTable->destination().value();
  EXPECT_EQ("t", destination.vertexTable->value());
  ASSERT_EQ(1, destination.keyColumns.size());
  EXPECT_EQ("b", destination.keyColumns[0]->value());
  ASSERT_EQ(1, destination.referencedColumns.size());
  EXPECT_EQ("a", destination.referencedColumns[0]->value());

  ASSERT_EQ(2, edgeTable->labels().size());
  EXPECT_EQ("B", edgeTable->labels()[0].name->value());
  EXPECT_FALSE(edgeTable->labels()[0].properties.has_value());
  EXPECT_EQ("C", edgeTable->labels()[1].name->value());
  ASSERT_EQ(1, edgeTable->labels()[1].properties->size());
  EXPECT_EQ(
      *parseExpression("c"),
      *edgeTable->labels()[1].properties->at(0).expression);
}

TEST_F(AstBuilderTest, propertyExpressionRequiresName) {
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parse(
          "WITH GRAPH g AS (VERTEX TABLES ("
          "t KEY (a) PROPERTIES (upper(b)))) SELECT 1"),
      "A property that is not a column reference requires AS name.");
}

TEST_F(AstBuilderTest, endpointColumnCount) {
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parse(
          "WITH GRAPH g AS ("
          "  VERTEX TABLES (t KEY (a))"
          "  EDGE TABLES (u KEY (a, b)"
          "    SOURCE KEY (a, b) REFERENCES t (a)"
          "    DESTINATION KEY (b) REFERENCES t (a))) "
          "SELECT 1"),
      "Edge endpoint must reference as many columns as its key has: 2 vs 1.");
}

TEST_F(AstBuilderTest, graphQueriesOption) {
  const ParserOptions defaultOptions;
  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parse(
          "SELECT * FROM GRAPH_TABLE (g MATCH (a) COLUMNS (a.x))",
          defaultOptions),
      "GRAPH_TABLE requires the graph_queries parser option.");

  AXIOM_EXPECT_PRESTO_SYNTAX_ERROR(
      parse(
          "WITH GRAPH g AS (VERTEX TABLES (t KEY (a))) SELECT 1",
          defaultOptions),
      "WITH GRAPH requires the graph_queries parser option.");
}

} // namespace
} // namespace axiom::sql::presto
