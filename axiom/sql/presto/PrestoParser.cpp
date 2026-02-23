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

#include "axiom/sql/presto/PrestoParser.h"
#include <folly/ScopeGuard.h>
#include <cctype>
#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/sql/presto/ExpressionPlanner.h"
#include "axiom/sql/presto/GroupByPlanner.h"
#include "axiom/sql/presto/PrestoParseError.h"
#include "axiom/sql/presto/TableVisitor.h"
#include "axiom/sql/presto/ast/AstBuilder.h"
#include "axiom/sql/presto/ast/AstPrinter.h"
#include "axiom/sql/presto/ast/DefaultTraversalVisitor.h"
#include "axiom/sql/presto/ast/UpperCaseInputStream.h"
#include "axiom/sql/presto/grammar/PrestoSqlLexer.h"
#include "axiom/sql/presto/grammar/PrestoSqlParser.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/WindowFunction.h"
#include "velox/functions/FunctionRegistry.h"

namespace axiom::sql::presto {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class ErrorListener : public antlr4::BaseErrorListener {
 public:
  void syntaxError(
      antlr4::Recognizer* recognizer,
      antlr4::Token* offendingSymbol,
      size_t line,
      size_t charPositionInLine,
      const std::string& msg,
      std::exception_ptr e) override {
    if (firstError.empty()) {
      firstError = fmt::format(
          "Syntax error at {}:{}: {}", line, charPositionInLine, msg);
    }
  }

  std::string firstError;
};

class ParserHelper {
 public:
  explicit ParserHelper(std::string_view sql)
      : inputStream_(std::make_unique<UpperCaseInputStream>(sql)),
        lexer_(std::make_unique<PrestoSqlLexer>(inputStream_.get())),
        tokenStream_(std::make_unique<antlr4::CommonTokenStream>(lexer_.get())),
        parser_(std::make_unique<PrestoSqlParser>(tokenStream_.get())) {
    lexer_->removeErrorListeners();
    lexer_->addErrorListener(&errorListener_);

    parser_->removeErrorListeners();
    parser_->addErrorListener(&errorListener_);

    // Use SLL prediction mode for faster parsing. SLL is much faster than LL
    // mode and works for most SQL queries. If SLL fails, we fall back to LL.
    parser_->getInterpreter<antlr4::atn::ParserATNSimulator>()
        ->setPredictionMode(antlr4::atn::PredictionMode::SLL);
  }

  PrestoSqlParser& parser() const {
    return *parser_;
  }

  PrestoSqlParser::StatementContext* parse() {
    // Try SLL mode first (fast path).
    try {
      auto ctx = parser_->singleStatement();
      if (parser_->getNumberOfSyntaxErrors() == 0) {
        return ctx->statement();
      }
    } catch (const std::exception&) {
      // SLL mode failed, fall through to LL mode.
    }

    // Fall back to LL mode (slower but handles all valid SQL).
    tokenStream_->seek(0);
    parser_->reset();
    parser_->getInterpreter<antlr4::atn::ParserATNSimulator>()
        ->setPredictionMode(antlr4::atn::PredictionMode::LL);

    auto ctx = parser_->singleStatement();
    if (parser_->getNumberOfSyntaxErrors() > 0) {
      throw PrestoParseError(errorListener_.firstError);
    }

    return ctx->statement();
  }

 private:
  std::unique_ptr<antlr4::ANTLRInputStream> inputStream_;
  std::unique_ptr<PrestoSqlLexer> lexer_;
  std::unique_ptr<antlr4::CommonTokenStream> tokenStream_;
  std::unique_ptr<PrestoSqlParser> parser_;
  ErrorListener errorListener_;
};

std::pair<std::string, std::string> toConnectorTable(
    const QualifiedName& name,
    const std::optional<std::string>& defaultConnectorId,
    const std::optional<std::string>& defaultSchema) {
  const auto& parts = name.parts();
  VELOX_CHECK(!parts.empty(), "Table name cannot be empty");

  const auto& tableName = parts.back();

  if (parts.size() == 1) {
    // name
    VELOX_CHECK(defaultConnectorId.has_value());
    if (defaultSchema.has_value()) {
      return {
          defaultConnectorId.value(),
          fmt::format("{}.{}", defaultSchema.value(), tableName)};
    }
    return {defaultConnectorId.value(), tableName};
  }

  if (parts.size() == 2) {
    // schema.name
    VELOX_CHECK(defaultConnectorId.has_value());
    return {
        defaultConnectorId.value(), fmt::format("{}.{}", parts[0], tableName)};
  }

  // connector.schema.name
  VELOX_CHECK_EQ(3, parts.size());
  return {parts[0], fmt::format("{}.{}", parts[1], tableName)};
}

// Resolves a column reference against both sides of a JOIN. Raises an error
// for unqualified names that exist on both sides.
lp::ExprPtr resolveJoinColumn(
    const lp::PlanBuilder::Scope& leftScope,
    const lp::PlanBuilder::Scope& rightScope,
    const lp::PlanBuilder& leftBuilder,
    const lp::PlanBuilder& rightBuilder,
    const std::optional<std::string>& alias,
    const std::string& name) {
  // Qualified name: try left, then right. No ambiguity since table
  // aliases are unique.
  if (alias.has_value()) {
    if (auto expr = leftScope(alias, name)) {
      return expr;
    }
    return rightScope(alias, name);
  }

  // For unqualified names, check both sides for ambiguity.
  const bool leftHas = leftBuilder.hasColumn(name);
  const bool rightHas = rightBuilder.hasColumn(name);

  // Not found on either side. Delegate to leftScope which chains to the
  // outer scope for correlated subqueries, or throws.
  if (!leftHas && !rightHas) {
    return leftScope(alias, name);
  }

  VELOX_USER_CHECK(leftHas != rightHas, "Column is ambiguous: {}", name);

  // Resolve from the side that has it. Calling leftScope for a name only on
  // the right would throw (unqualified not-found with no outer scope).
  return leftHas ? leftScope(alias, name) : rightScope(alias, name);
}

class RelationPlanner : public AstVisitor {
 public:
  RelationPlanner(
      const std::string& defaultConnectorId,
      const std::optional<std::string>& defaultSchema,
      const std::function<std::shared_ptr<axiom::sql::presto::Statement>(
          std::string_view /*sql*/)>& parseSql)
      : context_{defaultConnectorId, /*queryCtxPtr=*/nullptr,
        /*hook=*/nullptr, std::make_shared<lp::ThrowingSqlExpressionsParser>()},
        defaultSchema_{defaultSchema},
        parseSql_{parseSql},
        builder_(newBuilder()) {}

  lp::LogicalPlanNodePtr plan() {
    return builder_->build();
  }

  const std::unordered_map<std::pair<std::string, std::string>, std::string>&
  views() const {
    return views_;
  }

  lp::PlanBuilder& builder() {
    return *builder_;
  }

  lp::ExprApi toExpr(
      const ExpressionPtr& node,
      std::unordered_map<const core::IExpr*, lp::PlanBuilder::AggregateOptions>*
          aggregateOptions = nullptr) {
    return exprPlanner_.toExpr(node, aggregateOptions);
  }

 private:
  void addFilter(const ExpressionPtr& filter) {
    if (filter != nullptr) {
      builder_->filter(toExpr(filter));
    }
  }

  static lp::JoinType toJoinType(Join::Type type) {
    switch (type) {
      case Join::Type::kCross:
        return lp::JoinType::kInner;
      case Join::Type::kImplicit:
        return lp::JoinType::kInner;
      case Join::Type::kInner:
        return lp::JoinType::kInner;
      case Join::Type::kLeft:
        return lp::JoinType::kLeft;
      case Join::Type::kRight:
        return lp::JoinType::kRight;
      case Join::Type::kFull:
        return lp::JoinType::kFull;
    }

    folly::assume_unreachable();
  }

  static std::optional<std::pair<const Unnest*, const AliasedRelation*>>
  tryGetUnnest(const RelationPtr& relation) {
    if (relation->is(NodeType::kAliasedRelation)) {
      const auto* aliasedRelation = relation->as<AliasedRelation>();
      if (aliasedRelation->relation()->is(NodeType::kUnnest)) {
        return std::make_pair(
            aliasedRelation->relation()->as<Unnest>(), aliasedRelation);
      }
      return std::nullopt;
    }

    if (relation->is(NodeType::kUnnest)) {
      return std::make_pair(relation->as<Unnest>(), nullptr);
    }

    return std::nullopt;
  }

  void addCrossJoinUnnest(
      const Unnest& unnest,
      const AliasedRelation* aliasedRelation) {
    std::vector<lp::ExprApi> inputs;
    for (const auto& expr : unnest.expressions()) {
      inputs.push_back(toExpr(expr));
    }

    if (aliasedRelation) {
      std::vector<std::string> columnNames;
      columnNames.reserve(aliasedRelation->columnNames().size());
      for (const auto& name : aliasedRelation->columnNames()) {
        columnNames.emplace_back(canonicalizeIdentifier(*name));
      }

      builder_->unnest(
          inputs,
          unnest.isWithOrdinality(),
          canonicalizeIdentifier(*aliasedRelation->alias()),
          columnNames);
    } else {
      builder_->unnest(inputs, unnest.isWithOrdinality());
    }
  }

  void processFrom(const RelationPtr& relation) {
    if (relation == nullptr) {
      // SELECT 1; type of query.
      builder_->values(ROW({}), {Variant::row({})});
      return;
    }

    switch (relation->type()) {
      case NodeType::kTable:
        return processTable(*relation->as<Table>());
      case NodeType::kSampledRelation:
        return processSampledRelation(*relation->as<SampledRelation>());
      case NodeType::kAliasedRelation:
        return processAliasedRelation(*relation->as<AliasedRelation>());
      case NodeType::kTableSubquery:
        return processTableSubquery(*relation->as<TableSubquery>());
      case NodeType::kUnnest:
        return processUnnest(*relation->as<Unnest>());
      case NodeType::kJoin:
        return processJoin(*relation->as<Join>());
      default:
        VELOX_NYI(
            "Relation type is not supported yet: {}",
            NodeTypeName::toName(relation->type()));
    }
  }

  void processTable(const Table& table) {
    const auto tableName = canonicalizeName(table.name()->suffix());

    auto withIt = withQueries_.find(table.name()->suffix());
    if (withIt == withQueries_.end()) {
      withIt = withQueries_.find(tableName);
    }

    if (withIt != withQueries_.end()) {
      // TODO Change WithQuery to store Query and not Statement.
      processQuery(dynamic_cast<Query*>(withIt->second->query().get()));
    } else {
      const auto& [connectorId, qualifiedName] = toConnectorTable(
          *table.name(), context_.defaultConnectorId, defaultSchema_);

      auto* metadata =
          facebook::axiom::connector::ConnectorMetadata::metadata(connectorId);

      if (metadata->findTable(qualifiedName) != nullptr) {
        builder_->tableScan(
            connectorId, qualifiedName, /*includeHiddenColumns=*/true);
      } else if (auto view = metadata->findView(qualifiedName)) {
        views_.emplace(
            std::make_pair(connectorId, qualifiedName), view->text());

        VELOX_CHECK_NOT_NULL(parseSql_);
        auto query = parseSql_(view->text());
        processQuery(dynamic_cast<Query*>(query.get()));
      } else {
        VELOX_USER_FAIL(
            "Table not found: {}", table.name()->fullyQualifiedName());
      }
    }

    builder_->as(tableName);
  }

  void processSampledRelation(const SampledRelation& sampledRelation) {
    processFrom(sampledRelation.relation());

    lp::SampleNode::SampleMethod sampleMethod;
    switch (sampledRelation.sampleType()) {
      case SampledRelation::Type::kBernoulli:
        sampleMethod = lp::SampleNode::SampleMethod::kBernoulli;
        break;
      case SampledRelation::Type::kSystem:
        sampleMethod = lp::SampleNode::SampleMethod::kSystem;
        break;
      default:
        VELOX_USER_FAIL("Unsupported sample type");
    }

    auto percentage = toExpr(sampledRelation.samplePercentage());
    builder_->sample(percentage.expr(), sampleMethod);
  }

  void processAliasedRelation(const AliasedRelation& aliasedRelation) {
    processFrom(aliasedRelation.relation());

    const auto& columnAliases = aliasedRelation.columnNames();
    if (!columnAliases.empty()) {
      // Add projection to rename columns.
      const size_t numColumns = columnAliases.size();

      std::vector<lp::ExprApi> renames;
      renames.reserve(numColumns);
      for (auto i = 0; i < numColumns; ++i) {
        renames.push_back(
            lp::Col(builder_->findOrAssignOutputNameAt(i))
                .as(canonicalizeIdentifier(*columnAliases.at(i))));
      }

      builder_->project(renames);
    }

    builder_->as(canonicalizeIdentifier(*aliasedRelation.alias()));
  }

  void processTableSubquery(const TableSubquery& subquery) {
    auto query = subquery.query();

    if (query->is(NodeType::kQuery)) {
      processQuery(query->as<Query>());
      return;
    }

    VELOX_NYI(
        "Subquery type is not supported yet: {}",
        NodeTypeName::toName(query->type()));
  }

  void processUnnest(const Unnest& unnest) {
    std::vector<lp::ExprApi> inputs;
    for (const auto& expr : unnest.expressions()) {
      inputs.push_back(toExpr(expr));
    }

    builder_->unnest(inputs, unnest.isWithOrdinality());
  }

  void processJoin(const Join& join) {
    processFrom(join.left());

    if (auto unnest = tryGetUnnest(join.right())) {
      addCrossJoinUnnest(*unnest->first, unnest->second);
      return;
    }

    auto leftBuilder = builder_;
    auto leftScope = leftBuilder->scope();

    builder_ = newBuilder(leftScope);
    processFrom(join.right());
    auto rightBuilder = builder_;

    // Create a combined scope that can resolve columns from both sides of
    // the join. Subqueries in the ON clause may contain correlated
    // references to either side.
    lp::PlanBuilder::Scope joinScope =
        [leftScope,
         rightScope = rightBuilder->scope(),
         leftBuilder,
         rightBuilder](const auto& alias, const auto& name) {
          return resolveJoinColumn(
              leftScope, rightScope, *leftBuilder, *rightBuilder, alias, name);
        };

    builder_ = newBuilder(joinScope);

    if (const auto& criteria = join.criteria()) {
      if (criteria->is(NodeType::kJoinOn)) {
        std::optional<lp::ExprApi> condition;
        condition = toExpr(criteria->as<JoinOn>()->expression());

        builder_ = leftBuilder;
        builder_->join(*rightBuilder, condition, toJoinType(join.joinType()));
      } else if (criteria->is(NodeType::kJoinUsing)) {
        const auto* joinUsing = criteria->as<JoinUsing>();
        std::vector<std::string> columns;
        columns.reserve(joinUsing->columns().size());
        for (const auto& col : joinUsing->columns()) {
          columns.push_back(canonicalizeIdentifier(*col));
        }

        builder_ = leftBuilder;
        builder_->joinUsing(
            *rightBuilder, columns, toJoinType(join.joinType()));
      } else {
        VELOX_NYI(
            "Join criteria type is not supported yet: {}",
            NodeTypeName::toName(criteria->type()));
      }
    } else {
      builder_ = leftBuilder;
      builder_->join(*rightBuilder, std::nullopt, toJoinType(join.joinType()));
    }
  }

  void addProject(const std::vector<SelectItemPtr>& selectItems) {
    // SELECT * FROM ...
    const bool isSingleSelectStar = selectItems.size() == 1 &&
        selectItems.at(0)->is(NodeType::kAllColumns) &&
        selectItems.at(0)->as<AllColumns>()->prefix() == nullptr;
    if (isSingleSelectStar) {
      builder_->dropHiddenColumns();
      return;
    }

    std::vector<lp::ExprApi> exprs;
    for (const auto& item : selectItems) {
      if (item->is(NodeType::kAllColumns)) {
        auto* allColumns = item->as<AllColumns>();

        std::vector<std::string> columnNames;
        if (allColumns->prefix() != nullptr) {
          // SELECT t.*
          const auto& prefix = allColumns->prefix()->suffix();
          columnNames = builder_->findOrAssignOutputNames(
              /*includeHiddenColumns=*/false, prefix);

          for (const auto& name : columnNames) {
            exprs.push_back(lp::Col(name, lp::Col(prefix)));
          }
        } else {
          // SELECT *
          columnNames =
              builder_->findOrAssignOutputNames(/*includeHiddenColumns=*/false);

          for (const auto& name : columnNames) {
            exprs.push_back(lp::Col(name));
          }
        }
      } else {
        VELOX_CHECK(item->is(NodeType::kSingleColumn));
        auto* singleColumn = item->as<SingleColumn>();

        lp::ExprApi expr = toExpr(singleColumn->expression());

        if (singleColumn->alias() != nullptr) {
          expr = expr.as(canonicalizeIdentifier(*singleColumn->alias()));
        }
        exprs.push_back(expr);
      }
    }

    builder_->project(exprs);
  }

  lp::ExprApi toSortingKey(const ExpressionPtr& expr) {
    if (expr->is(NodeType::kLongLiteral)) {
      const auto n = expr->as<LongLiteral>()->value();
      const auto name = builder_->findOrAssignOutputNameAt(n - 1);

      return lp::Col(name);
    }

    return toExpr(expr);
  }

  void addOrderBy(const OrderByPtr& orderBy) {
    if (orderBy == nullptr) {
      return;
    }

    std::vector<lp::SortKey> keys;

    const auto& sortItems = orderBy->sortItems();
    for (const auto& item : sortItems) {
      auto expr = toSortingKey(item->sortKey());
      keys.emplace_back(expr, item->isAscending(), item->isNullsFirst());
    }

    builder_->sort(keys);
  }

  static int64_t parseInt64(const std::optional<std::string>& value) {
    return std::atol(value.value().c_str());
  }

  void addOffset(const OffsetPtr& offset) {
    if (offset == nullptr) {
      return;
    }

    builder_->offset(std::atol(offset->offset().c_str()));
  }

  void addLimit(const std::optional<std::string>& limit) {
    if (!limit.has_value()) {
      return;
    }

    builder_->limit(parseInt64(limit));
  }

  void processQuery(Query* query) {
    auto savedWithQueries = withQueries_;
    SCOPE_EXIT {
      withQueries_ = std::move(savedWithQueries);
    };

    if (const auto& with = query->with()) {
      for (const auto& query : with->queries()) {
        withQueries_.insert_or_assign(
            canonicalizeIdentifier(*query->name()), query);
      }
    }

    const auto& queryBody = query->queryBody();
    if (queryBody->is(NodeType::kQuerySpecification)) {
      visitQuerySpecification(
          queryBody->as<QuerySpecification>(), query->orderBy());
    } else {
      queryBody->accept(this);
      addOrderBy(query->orderBy());
    }

    addOffset(query->offset());
    addLimit(query->limit());
  }

  void visitQuery(Query* query) override {
    processQuery(query);
  }

  void visitTableSubquery(TableSubquery* node) override {
    node->query()->accept(this);
  }

  void visitQuerySpecification(QuerySpecification* node) override {
    visitQuerySpecification(node, /*orderBy=*/nullptr);
  }

  void visitQuerySpecification(
      QuerySpecification* node,
      const OrderByPtr& orderBy) {
    // FROM t -> builder.tableScan(t)
    processFrom(node->from());

    // WHERE a > 1 -> builder.filter("a > 1")
    addFilter(node->where());

    const auto& selectItems = node->select()->selectItems();
    const bool distinct = node->select()->isDistinct();

    if (auto groupBy = node->groupBy()) {
      VELOX_USER_CHECK(
          !groupBy->isDistinct(),
          "GROUP BY with DISTINCT is not supported yet");
      GroupByPlanner{builder_, exprPlanner_}.plan(
          selectItems, groupBy->groupingElements(), node->having(), orderBy);

      if (distinct) {
        builder_->distinct();
      }
    } else {
      if (GroupByPlanner{builder_, exprPlanner_}.tryPlanGlobalAgg(
              selectItems, node->having())) {
        // Nothing else to do.
      } else {
        // SELECT a, b -> builder.project({a, b})
        addProject(selectItems);
      }

      if (distinct) {
        builder_->distinct();
      }

      addOrderBy(orderBy);
    }
  }

  void visitValues(Values* node) override {
    VELOX_CHECK(!node->rows().empty());

    const auto& firstRow = node->rows().front();
    const bool isRow = firstRow->is(NodeType::kRow);
    const auto numColumns = isRow ? firstRow->as<Row>()->items().size() : 1;

    std::vector<std::vector<lp::ExprApi>> rows;
    rows.reserve(node->rows().size());

    for (const auto& row : node->rows()) {
      std::vector<lp::ExprApi> values;
      if (isRow) {
        const auto& columns = row->as<Row>()->items();
        VELOX_CHECK_EQ(numColumns, columns.size());

        for (const auto& expr : columns) {
          values.emplace_back(toExpr(expr));
        }
      } else {
        values.emplace_back(toExpr(row));
      }

      rows.emplace_back(std::move(values));
    }

    std::vector<std::string> names;
    names.reserve(numColumns);
    for (auto i = 0; i < numColumns; ++i) {
      names.emplace_back(fmt::format("c{}", i));
    }

    builder_->values(names, rows);
  }

  void visitExcept(Except* node) override {
    visitSetOperation(
        lp::SetOperation::kExcept,
        node->left(),
        node->right(),
        node->isDistinct());
  }

  void visitIntersect(Intersect* node) override {
    visitSetOperation(
        lp::SetOperation::kIntersect,
        node->left(),
        node->right(),
        node->isDistinct());
  }

  void visitUnion(Union* node) override {
    visitSetOperation(
        lp::SetOperation::kUnionAll,
        node->left(),
        node->right(),
        node->isDistinct());
  }

  void visitSetOperation(
      lp::SetOperation op,
      const std::shared_ptr<QueryBody>& left,
      const std::shared_ptr<QueryBody>& right,
      bool distinct) {
    left->accept(this);

    auto leftBuilder = builder_;

    auto scope = leftBuilder->scope();

    builder_ = newBuilder(scope);
    right->accept(this);
    auto rightBuilder = builder_;

    builder_ = leftBuilder;
    builder_->setOperation(op, *rightBuilder);

    if (distinct) {
      builder_->distinct();
    }
  }

  std::shared_ptr<lp::PlanBuilder> newBuilder(
      const lp::PlanBuilder::Scope& outerScope = nullptr) {
    return std::make_shared<lp::PlanBuilder>(
        context_,
        /*enableCoercions=*/true,
        /*allowDuplicateAliases=*/true,
        outerScope);
  }

  lp::PlanBuilder::Context context_;
  const std::optional<std::string> defaultSchema_;
  const std::function<std::shared_ptr<axiom::sql::presto::Statement>(
      std::string_view /*sql*/)>
      parseSql_;
  std::shared_ptr<lp::PlanBuilder> builder_;
  ExpressionPlanner exprPlanner_{
      [this](Query* query) -> lp::LogicalPlanNodePtr {
        auto builder = std::move(builder_);
        auto scope = builder->scope();
        builder_ = newBuilder(scope);
        processQuery(query);
        auto subqueryBuilder = builder_;
        builder_ = std::move(builder);
        return subqueryBuilder->build(/*useIds=*/true);
      },
      [this](const ExpressionPtr& expr) -> lp::ExprApi {
        return toSortingKey(expr);
      }};
  std::unordered_map<std::string, std::shared_ptr<WithQuery>> withQueries_;
  std::unordered_map<std::pair<std::string, std::string>, std::string> views_;
};

} // namespace

SqlStatementPtr PrestoParser::parse(std::string_view sql, bool enableTracing) {
  return doParse(sql, enableTracing);
}

std::vector<SqlStatementPtr> PrestoParser::parseMultiple(
    std::string_view sql,
    bool enableTracing) {
  auto statements = splitStatements(sql);
  std::vector<SqlStatementPtr> results;
  results.reserve(statements.size());

  for (const auto& statement : statements) {
    if (!statement.empty()) {
      results.push_back(doParse(statement, enableTracing));
    }
  }

  return results;
}

std::vector<std::string_view> PrestoParser::splitStatements(
    std::string_view sql) {
  std::vector<std::string_view> statements;

  // Use ANTLR lexer to tokenize and find statement boundaries
  const std::string sqlStr(sql);
  UpperCaseInputStream inputStream(sqlStr);
  PrestoSqlLexer lexer(&inputStream);
  antlr4::CommonTokenStream tokenStream(&lexer);
  tokenStream.fill();

  // Get all tokens (default channel only - excludes hidden tokens like
  // whitespace/comments)
  size_t numTokens = tokenStream.size();

  size_t statementStart = 0;
  for (size_t i = 0; i < numTokens; ++i) {
    const auto* token = tokenStream.get(i);

    if (token->getText() == ";") {
      // Find the last token before the semicolon (on default channel)
      if (i > statementStart) {
        size_t startIndex = tokenStream.get(statementStart)->getStartIndex();
        size_t endIndex = tokenStream.get(i - 1)->getStopIndex();

        auto stmt = sql.substr(startIndex, endIndex - startIndex + 1);

        while (!stmt.empty() && std::isspace(stmt.front())) {
          stmt.remove_prefix(1);
        }
        while (!stmt.empty() && std::isspace(stmt.back())) {
          stmt.remove_suffix(1);
        }

        if (!stmt.empty()) {
          statements.push_back(stmt);
        }
      }

      statementStart = i + 1;
    }
  }

  // Handle the last statement (if no trailing semicolon)
  if (statementStart < numTokens) {
    // Skip EOF token (last token in stream)
    size_t lastTokenIdx = numTokens - 1;
    if (lastTokenIdx > 0 && lastTokenIdx >= statementStart) {
      --lastTokenIdx;
    }

    if (lastTokenIdx >= statementStart) {
      size_t startIndex = tokenStream.get(statementStart)->getStartIndex();
      size_t endIndex = tokenStream.get(lastTokenIdx)->getStopIndex();

      auto stmt = sql.substr(startIndex, endIndex - startIndex + 1);

      while (!stmt.empty() && std::isspace(stmt.front())) {
        stmt.remove_prefix(1);
      }
      while (!stmt.empty() && std::isspace(stmt.back())) {
        stmt.remove_suffix(1);
      }

      if (!stmt.empty()) {
        statements.push_back(stmt);
      }
    }
  }

  return statements;
}

lp::ExprPtr PrestoParser::parseExpression(
    std::string_view sql,
    bool enableTracing) {
  auto statement = doParse(fmt::format("SELECT {}", sql), enableTracing);
  VELOX_USER_CHECK(statement->isSelect());

  auto plan = statement->as<SelectStatement>()->plan();

  VELOX_USER_CHECK(plan->is(lp::NodeKind::kProject));

  auto project = plan->as<lp::ProjectNode>();
  VELOX_CHECK_NOT_NULL(project);

  VELOX_USER_CHECK_EQ(1, project->expressions().size());
  return project->expressionAt(0);
}

namespace {
lp::ExprPtr parseSqlExpression(const ExpressionPtr& expr) {
  ExpressionPlanner exprPlanner{/*subqueryPlanner=*/nullptr,
                                /*sortingKeyResolver=*/nullptr};

  auto plan = lp::PlanBuilder()
                  .values(ROW({}), {Variant::row({})})
                  .project({exprPlanner.toExpr(expr)})
                  .build();
  VELOX_USER_CHECK(plan->is(lp::NodeKind::kProject));

  auto project = plan->as<lp::ProjectNode>();
  VELOX_CHECK_NOT_NULL(project);

  VELOX_USER_CHECK_EQ(1, project->expressions().size());

  return project->expressionAt(0);
}

SqlStatementPtr parseExplain(
    const Explain& explain,
    const SqlStatementPtr& sqlStatement) {
  if (explain.isAnalyze()) {
    return std::make_shared<ExplainStatement>(
        sqlStatement,
        /*analyze=*/true);
  }

  ExplainStatement::Type type = ExplainStatement::Type::kExecutable;

  for (const auto& option : explain.options()) {
    if (option->is(NodeType::kExplainType)) {
      const auto explainType = option->as<ExplainType>()->explainType();
      switch (explainType) {
        case ExplainType::Type::kLogical:
          type = ExplainStatement::Type::kLogical;
          break;
        case ExplainType::Type::kGraph:
          type = ExplainStatement::Type::kGraph;
          break;
        case ExplainType::Type::kOptimized:
          type = ExplainStatement::Type::kOptimized;
          break;
        case ExplainType::Type::kExecutable:
          [[fallthrough]];
        case ExplainType::Type::kDistributed:
          type = ExplainStatement::Type::kExecutable;
          break;
        default:
          VELOX_USER_FAIL("Unsupported EXPLAIN type");
      }
    }
  }

  return std::make_shared<ExplainStatement>(
      sqlStatement,
      /*analyze=*/false,
      type);
}

static facebook::axiom::connector::TablePtr findTable(
    const QualifiedName& name,
    const std::string& defaultConnectorId,
    const std::optional<std::string>& defaultSchema) {
  const auto connectorTable =
      toConnectorTable(name, defaultConnectorId, defaultSchema);

  auto table = facebook::axiom::connector::ConnectorMetadata::metadata(
                   connectorTable.first)
                   ->findTable(connectorTable.second);

  VELOX_USER_CHECK_NOT_NULL(
      table, "Table not found: {}", name.fullyQualifiedName());
  return table;
}

lp::ExprApi makeLikeExpr(
    const std::string& name,
    const std::string& pattern,
    const std::optional<std::string>& escape) {
  std::vector<lp::ExprApi> inputs;
  inputs.emplace_back(lp::Col(name));
  inputs.emplace_back(lp::Lit(pattern));
  if (escape.has_value()) {
    inputs.emplace_back(lp::Lit(escape.value()));
  }

  return lp::Call("like", std::move(inputs));
}

SqlStatementPtr parseShowCatalogs(
    const ShowCatalogs& showCatalogs,
    const std::string& defaultConnectorId) {
  const auto& connectors = connector::getAllConnectors();

  std::vector<Variant> data;
  data.reserve(connectors.size());
  for (const auto& [id, _] : connectors) {
    data.emplace_back(Variant::row({id, id, id}));
  }

  lp::PlanBuilder::Context ctx(defaultConnectorId);
  lp::PlanBuilder builder(ctx);
  builder.values(
      ROW({"catalog_name", "connector_id", "connector_name"}, VARCHAR()), data);

  if (showCatalogs.getLikePattern().has_value()) {
    builder.filter(makeLikeExpr(
        "catalog_name",
        showCatalogs.getLikePattern().value(),
        showCatalogs.getEscape()));
  }

  return std::make_shared<SelectStatement>(builder.build());
}

SqlStatementPtr parseShowColumns(
    const ShowColumns& showColumns,
    const std::string& defaultConnectorId,
    const std::optional<std::string>& defaultSchema) {
  const auto schema =
      findTable(*showColumns.table(), defaultConnectorId, defaultSchema)
          ->type();

  std::vector<Variant> data;
  data.reserve(schema->size());
  for (auto i = 0; i < schema->size(); ++i) {
    data.emplace_back(
        Variant::row({schema->nameOf(i), schema->childAt(i)->toString()}));
  }

  lp::PlanBuilder::Context ctx(defaultConnectorId);
  return std::make_shared<SelectStatement>(
      lp::PlanBuilder(ctx)
          .values(ROW({"column", "type"}, VARCHAR()), data)
          .build());
}

SqlStatementPtr parseShowFunctions(
    const ShowFunctions& showFunctions,
    const std::string& defaultConnectorId) {
  std::vector<Variant> rows;

  auto const& allScalarFunctions = getFunctionSignatures();

  for (const auto& [name, signatures] : allScalarFunctions) {
    for (const auto& signature : signatures) {
      rows.emplace_back(
          Variant::row({
              name,
              "scalar",
              signature->toString(),
          }));
    }
  }

  auto const& allAggregateFunctions = exec::getAggregateFunctionSignatures();

  for (const auto& [name, signatures] : allAggregateFunctions) {
    for (const auto& signature : signatures) {
      rows.emplace_back(
          Variant::row({
              name,
              "aggregate",
              signature->toString(),
          }));
    }
  }

  auto const& allWindowFunctions = exec::windowFunctions();

  for (const auto& [name, windowEntry] : allWindowFunctions) {
    // Skip aggregate functions as they have already been processed.
    if (!allAggregateFunctions.contains(name)) {
      for (const auto& signature : windowEntry.signatures) {
        rows.emplace_back(
            Variant::row({
                name,
                "window",
                signature->toString(),
            }));
      }
    }
  }

  lp::PlanBuilder::Context ctx(defaultConnectorId);
  lp::PlanBuilder builder(ctx);
  builder.values(
      ROW(
          {
              "Function",
              "Function Type",
              "Signature",
          },
          VARCHAR()),
      rows);

  if (showFunctions.getLikePattern().has_value()) {
    builder.filter(makeLikeExpr(
        "Function",
        showFunctions.getLikePattern().value(),
        showFunctions.getEscape()));
  }

  return std::make_shared<SelectStatement>(builder.build());
};

std::vector<lp::ExprApi> toColumnExprs(const std::vector<std::string>& names) {
  std::vector<lp::ExprApi> exprs;
  exprs.reserve(names.size());
  for (const auto& name : names) {
    exprs.emplace_back(lp::Col(name));
  }
  return exprs;
}

SqlStatementPtr parseInsert(
    const Insert& insert,
    const std::string& defaultConnectorId,
    const std::optional<std::string>& defaultSchema,
    const std::function<std::shared_ptr<axiom::sql::presto::Statement>(
        std::string_view /*sql*/)>& parseSql) {
  const auto connectorTable =
      toConnectorTable(*insert.target(), defaultConnectorId, defaultSchema);

  const auto table = facebook::axiom::connector::ConnectorMetadata::metadata(
                         connectorTable.first)
                         ->findTable(connectorTable.second);

  VELOX_USER_CHECK_NOT_NULL(
      table, "Table not found: {}", insert.target()->fullyQualifiedName());

  const auto& columns = insert.columns();

  std::vector<std::string> columnNames;
  if (columns.empty()) {
    columnNames = table->type()->names();
  } else {
    columnNames.reserve(columns.size());
    for (const auto& column : columns) {
      columnNames.emplace_back(column->value());
    }
  }

  RelationPlanner planner(defaultConnectorId, defaultSchema, parseSql);
  insert.query()->accept(&planner);

  auto inputColumns = planner.builder().findOrAssignOutputNames();
  VELOX_CHECK_EQ(inputColumns.size(), columnNames.size());

  planner.builder().tableWrite(
      connectorTable.first,
      connectorTable.second,
      lp::WriteKind::kInsert,
      columnNames,
      toColumnExprs(inputColumns));

  return std::make_shared<InsertStatement>(planner.plan(), planner.views());
}

std::unordered_map<std::string, lp::ExprPtr> parseTableProperties(
    const std::vector<std::shared_ptr<Property>>& props) {
  std::unordered_map<std::string, lp::ExprPtr> properties;
  for (const auto& p : props) {
    const auto& name = p->name()->value();
    auto expr = parseSqlExpression(p->value());
    VELOX_USER_CHECK(
        expr->looksConstant(),
        "Property {} = {} is not constant",
        name,
        expr->toString());
    bool ok = properties.emplace(name, std::move(expr)).second;
    VELOX_USER_CHECK(ok, "Duplicate property: {}", name);
  }
  return properties;
}

SqlStatementPtr parseCreateTableAsSelect(
    const CreateTableAsSelect& ctas,
    const std::string& defaultConnectorId,
    const std::optional<std::string>& defaultSchema,
    const std::function<std::shared_ptr<axiom::sql::presto::Statement>(
        std::string_view /*sql*/)>& parseSql) {
  auto connectorTable =
      toConnectorTable(*ctas.name(), defaultConnectorId, defaultSchema);

  RelationPlanner planner(defaultConnectorId, defaultSchema, parseSql);
  ctas.query()->accept(&planner);

  auto properties = parseTableProperties(ctas.properties());

  auto& planBuilder = planner.builder();

  auto columnTypes = planBuilder.outputTypes();

  const auto inputColumns = planBuilder.outputNames();
  const auto numInputColumns = inputColumns.size();

  std::vector<std::string> columnNames;
  if (ctas.columns().empty()) {
    columnNames.reserve(numInputColumns);
    for (auto i = 0; i < numInputColumns; ++i) {
      const auto& name = inputColumns[i];
      VELOX_USER_CHECK(
          name.has_value(), "Column name not specified at position {}", i + 1);
      columnNames.emplace_back(name.value());
    }

    planBuilder.tableWrite(
        connectorTable.first,
        connectorTable.second,
        lp::WriteKind::kCreate,
        columnNames,
        toColumnExprs(columnNames));
  } else {
    VELOX_USER_CHECK_EQ(ctas.columns().size(), numInputColumns);

    columnNames.reserve(numInputColumns);
    for (const auto& column : ctas.columns()) {
      columnNames.emplace_back(column->value());
    }

    planBuilder.tableWrite(
        connectorTable.first,
        connectorTable.second,
        lp::WriteKind::kCreate,
        columnNames,
        toColumnExprs(planBuilder.findOrAssignOutputNames()));
  }

  return std::make_shared<CreateTableAsSelectStatement>(
      connectorTable.first,
      connectorTable.second,
      ROW(std::move(columnNames), std::move(columnTypes)),
      std::move(properties),
      planner.plan(),
      planner.views());
}

SqlStatementPtr parseCreateTable(
    const CreateTable& createTable,
    const std::string& defaultConnectorId,
    const std::optional<std::string>& defaultSchema) {
  auto connectorTable =
      toConnectorTable(*createTable.name(), defaultConnectorId, defaultSchema);

  auto properties = parseTableProperties(createTable.properties());

  std::vector<std::string> names;
  std::vector<TypePtr> types;
  std::vector<CreateTableStatement::Constraint> constraints;

  for (const auto& element : createTable.elements()) {
    switch (element->type()) {
      case NodeType::kColumnDefinition: {
        auto* columnDef = element->as<ColumnDefinition>();
        names.push_back(columnDef->name()->value());

        auto type = parseType(columnDef->columnType());
        VELOX_USER_CHECK_NOT_NULL(
            type,
            "Unknown type specifier: {}",
            columnDef->columnType()->baseName());
        types.push_back(type);
        break;
      }
      case NodeType::kLikeClause: {
        auto* likeClause = element->as<LikeClause>();
        auto table = findTable(
            *likeClause->tableName(), defaultConnectorId, defaultSchema);

        auto schema = table->type();
        for (auto i = 0; i < schema->size(); ++i) {
          names.push_back(schema->nameOf(i));
          types.push_back(schema->childAt(i));
        }
        break;
      }
      case NodeType::kConstraintSpecification: {
        auto* constraintSpec = element->as<ConstraintSpecification>();

        CreateTableStatement::Constraint constraint;
        if (constraintSpec->name()) {
          constraint.name = constraintSpec->name()->value();
        }

        for (const auto& col : constraintSpec->columns()) {
          constraint.columns.push_back(col->value());
        }

        switch (constraintSpec->constraintType()) {
          case ConstraintSpecification::ConstraintType::kPrimaryKey:
            constraint.type =
                CreateTableStatement::Constraint::Type::kPrimaryKey;
            break;
          case ConstraintSpecification::ConstraintType::kUnique:
            constraint.type = CreateTableStatement::Constraint::Type::kUnique;
            break;
        }

        constraints.push_back(std::move(constraint));
        break;
      }
      default:
        VELOX_UNREACHABLE(
            "Unexpected table element type: {}",
            static_cast<int>(element->type()));
    }
  }

  return std::make_shared<CreateTableStatement>(
      connectorTable.first,
      connectorTable.second,
      ROW(std::move(names), std::move(types)),
      std::move(properties),
      createTable.isNotExists(),
      std::move(constraints));
}

SqlStatementPtr parseDropTable(
    const DropTable& dropTable,
    const std::string& defaultConnectorId,
    const std::optional<std::string>& defaultSchema) {
  auto connectorTable = toConnectorTable(
      *dropTable.tableName(), defaultConnectorId, defaultSchema);

  return std::make_shared<DropTableStatement>(
      connectorTable.first, connectorTable.second, dropTable.isExists());
}

SqlStatementPtr doPlan(
    const std::shared_ptr<Statement>& query,
    const std::string& defaultConnectorId,
    const std::optional<std::string>& defaultSchema,
    const std::function<std::shared_ptr<axiom::sql::presto::Statement>(
        std::string_view /*sql*/)>& parseSql) {
  if (query->is(NodeType::kInsert)) {
    return parseInsert(
        *query->as<Insert>(), defaultConnectorId, defaultSchema, parseSql);
  }

  if (query->is(NodeType::kCreateTableAsSelect)) {
    return parseCreateTableAsSelect(
        *query->as<CreateTableAsSelect>(),
        defaultConnectorId,
        defaultSchema,
        parseSql);
  }

  if (query->is(NodeType::kCreateTable)) {
    return parseCreateTable(
        *query->as<CreateTable>(), defaultConnectorId, defaultSchema);
  }

  if (query->is(NodeType::kShowCatalogs)) {
    return parseShowCatalogs(*query->as<ShowCatalogs>(), defaultConnectorId);
  }

  if (query->is(NodeType::kShowColumns)) {
    return parseShowColumns(
        *query->as<ShowColumns>(), defaultConnectorId, defaultSchema);
  }

  if (query->is(NodeType::kShowFunctions)) {
    return parseShowFunctions(*query->as<ShowFunctions>(), defaultConnectorId);
  }

  if (query->is(NodeType::kQuery)) {
    RelationPlanner planner(defaultConnectorId, defaultSchema, parseSql);
    query->accept(&planner);
    return std::make_shared<SelectStatement>(planner.plan(), planner.views());
  }

  VELOX_NYI(
      "Unsupported statement type: {}", NodeTypeName::toName(query->type()));
}
} // namespace

SqlStatementPtr PrestoParser::doParse(
    std::string_view sql,
    bool enableTracing) {
  auto parseSql = [enableTracing](std::string_view sql) {
    ParserHelper helper(sql);
    auto* context = helper.parse();

    AstBuilder astBuilder(enableTracing);
    auto query =
        std::any_cast<std::shared_ptr<Statement>>(astBuilder.visit(context));

    if (enableTracing) {
      std::stringstream astString;
      AstPrinter printer(astString);
      query->accept(&printer);

      std::cout << "AST: " << astString.str() << std::endl;
    }

    return query;
  };

  auto query = parseSql(sql);

  if (query->is(NodeType::kExplain)) {
    auto* explain = query->as<Explain>();
    auto sqlStatement = doPlan(
        explain->statement(), defaultConnectorId_, defaultSchema_, parseSql);
    return parseExplain(*explain, sqlStatement);
  }

  if (query->is(NodeType::kDropTable)) {
    return parseDropTable(
        *query->as<DropTable>(), defaultConnectorId_, defaultSchema_);
  }

  if (query->is(NodeType::kUse)) {
    auto* use = query->as<Use>();
    std::optional<std::string> catalog;
    if (use->catalog()) {
      catalog = use->catalog()->value();
    }
    return std::make_shared<UseStatement>(
        std::move(catalog), use->schema()->value());
  }

  return doPlan(query, defaultConnectorId_, defaultSchema_, parseSql);
}

ReferencedTables PrestoParser::getReferencedTables(std::string_view sql) {
  ParserHelper helper(sql);
  auto* context = helper.parse();

  AstBuilder astBuilder(false);
  auto statement =
      std::any_cast<std::shared_ptr<Statement>>(astBuilder.visit(context));

  TableVisitor visitor(defaultConnectorId_, defaultSchema_);
  visitor.process(statement.get());
  return ReferencedTables{
      .inputTables = visitor.inputTables(),
      .outputTable = visitor.outputTable()};
}

} // namespace axiom::sql::presto
