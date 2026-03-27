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
#include <unordered_set>
#include "axiom/common/CatalogSchemaTableName.h"
#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/sql/presto/ExpressionPlanner.h"
#include "axiom/sql/presto/GroupByPlanner.h"
#include "axiom/sql/presto/PrestoParseError.h"
#include "axiom/sql/presto/ShowStatsBuilder.h"
#include "axiom/sql/presto/SortProjection.h"
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
#include "velox/functions/prestosql/types/PrestoTypes.h"

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

std::pair<std::string, facebook::axiom::SchemaTableName> toConnectorTable(
    const QualifiedName& name,
    const std::string& defaultConnectorId,
    const std::string& defaultSchema) {
  const auto& parts = name.parts();
  VELOX_CHECK(!parts.empty(), "Table name cannot be empty");

  if (parts.size() == 1) {
    // name
    return {defaultConnectorId, {defaultSchema, parts[0]}};
  }

  if (parts.size() == 2) {
    // schema.name
    return {defaultConnectorId, {parts[0], parts[1]}};
  }

  // connector.schema.name
  VELOX_CHECK_EQ(3, parts.size());
  return {parts[0], {parts[1], parts[2]}};
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

// Walks an AST expression and checks whether it contains window function calls
// nested inside other expressions.
class WindowFunctionFinder : public DefaultTraversalVisitor {
 public:
  bool hasWindowFunction() const {
    return hasWindowFunction_;
  }

 protected:
  void visitFunctionCall(FunctionCall* node) override {
    if (node->window() != nullptr) {
      hasWindowFunction_ = true;
      return;
    }
    DefaultTraversalVisitor::visitFunctionCall(node);
  }

  void visitSubqueryExpression(SubqueryExpression* node) override {
    // Window function calls within a subquery do not count.
  }

 private:
  bool hasWindowFunction_{false};
};

// Returns true if the expression contains any window function call.
bool hasWindowFunction(const ExpressionPtr& expr) {
  WindowFunctionFinder finder;
  const_cast<Expression*>(expr.get())->accept(&finder);
  return finder.hasWindowFunction();
}

// Returns true if any select item has a window function nested inside an
// expression (e.g., sum(b) OVER (...) * 2). Top-level window functions
// (e.g., sum(b) OVER (...) AS s) are handled directly by PlanBuilder and
// don't need special treatment.
bool hasNestedWindowFunction(const std::vector<SelectItemPtr>& selectItems) {
  for (const auto& item : selectItems) {
    if (item->is(NodeType::kSingleColumn)) {
      auto* singleColumn = item->as<SingleColumn>();
      const auto& expr = singleColumn->expression();
      // Skip top-level window functions - they already work.
      if (expr->is(NodeType::kFunctionCall) &&
          expr->as<FunctionCall>()->window() != nullptr) {
        continue;
      }
      if (hasWindowFunction(expr)) {
        return true;
      }
    }
  }
  return false;
}

// Finds sub-expressions in an IExpr tree using pointer-identity matching.
// Walks the expression tree and collects ExprPtrs whose raw pointers are
// found in 'targets'. Also appends matches to 'order' in traversal order
// for deterministic plan generation.
void findExprPtrs(
    const core::ExprPtr& expr,
    const std::unordered_map<const core::IExpr*, lp::WindowSpec>& targets,
    std::unordered_map<const core::IExpr*, core::ExprPtr>& found,
    std::vector<const core::IExpr*>& order) {
  if (targets.count(expr.get())) {
    if (found.emplace(expr.get(), expr).second) {
      order.push_back(expr.get());
    }
    return;
  }
  for (const auto& input : expr->inputs()) {
    findExprPtrs(input, targets, found, order);
  }
}

core::ExprPtr replaceInputs(
    const core::ExprPtr& expr,
    const std::unordered_map<const core::IExpr*, core::ExprPtr>& replacements) {
  auto it = replacements.find(expr.get());
  if (it != replacements.end()) {
    return it->second;
  }

  std::vector<core::ExprPtr> newInputs;
  bool changed = false;
  for (const auto& input : expr->inputs()) {
    auto newInput = replaceInputs(input, replacements);
    if (newInput.get() != input.get()) {
      changed = true;
    }
    newInputs.push_back(std::move(newInput));
  }

  return changed ? expr->replaceInputs(std::move(newInputs)) : expr;
}

class RelationPlanner : public AstVisitor {
 public:
  RelationPlanner(
      const std::string& defaultConnectorId,
      const std::string& defaultSchema,
      const std::function<std::shared_ptr<axiom::sql::presto::Statement>(
          std::string_view /*sql*/)>& parseSql,
      bool friendlySql = true)
      : context_{defaultConnectorId, defaultSchema,
        /*queryCtxPtr=*/nullptr,
        /*hook=*/nullptr, std::make_shared<lp::ThrowingSqlExpressionsParser>()},
        defaultSchema_{defaultSchema},
        parseSql_{parseSql},
        builder_(newBuilder()),
        friendlySql_{friendlySql} {}

  lp::LogicalPlanNodePtr plan() {
    return builder_->build();
  }

  const ViewMap& views() const {
    return views_;
  }

  lp::PlanBuilder& builder() {
    return *builder_;
  }

  lp::ExprApi toExpr(
      const ExpressionPtr& node,
      std::unordered_map<const core::IExpr*, lp::PlanBuilder::AggregateOptions>*
          aggregateOptions = nullptr,
      std::unordered_map<const core::IExpr*, lp::WindowSpec>* windowOptions =
          nullptr) {
    return exprPlanner_.toExpr(node, aggregateOptions, windowOptions);
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

    auto toOrdinality = [&]() -> std::optional<lp::ExprApi> {
      if (!unnest.isWithOrdinality()) {
        return std::nullopt;
      }
      return lp::Ordinality();
    };

    if (aliasedRelation) {
      std::vector<std::string> columnNames;
      columnNames.reserve(aliasedRelation->columnNames().size());
      for (const auto& name : aliasedRelation->columnNames()) {
        columnNames.emplace_back(canonicalizeIdentifier(*name));
      }

      auto ordinality = toOrdinality();
      if (ordinality.has_value() && !columnNames.empty()) {
        ordinality = ordinality->as(columnNames.back());
        columnNames.pop_back();
      }

      builder_->unnest(
          inputs,
          ordinality,
          canonicalizeIdentifier(*aliasedRelation->alias()),
          columnNames);
    } else {
      builder_->unnest(inputs, toOrdinality());
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
      // Temporarily remove the CTE from the map while processing its body
      // to prevent infinite recursion when a CTE has the same name as a
      // base table it references (e.g. WITH t AS (SELECT * FROM t)).
      // Non-recursive CTEs cannot reference themselves in Presto.
      auto withEntry = std::move(withIt->second);
      withQueries_.erase(withIt);
      SCOPE_EXIT {
        withQueries_.insert_or_assign(tableName, std::move(withEntry));
      };
      // TODO: Change WithQuery to store Query and not Statement.
      processQuery(dynamic_cast<Query*>(withEntry->query().get()));
    } else {
      const auto [connectorId, connectorTable] = toConnectorTable(
          *table.name(), context_.defaultConnectorId.value(), defaultSchema_);

      auto* metadata =
          facebook::axiom::connector::ConnectorMetadata::metadata(connectorId);

      if (metadata->findTable(connectorTable) != nullptr) {
        builder_->tableScan(
            connectorId,
            connectorTable.schema,
            connectorTable.table,
            /*includeHiddenColumns=*/true);
      } else if (auto view = metadata->findView(connectorTable)) {
        views_.emplace(
            facebook::axiom::CatalogSchemaTableName{
                connectorId, connectorTable},
            view->text());

        VELOX_CHECK_NOT_NULL(parseSql_);
        auto query = parseSql_(view->text());
        processQuery(dynamic_cast<Query*>(query.get()));
      } else {
        VELOX_USER_FAIL(
            "Table not found: {}", table.name()->fullyQualifiedName());
      }
    }

    builder_->findOrAssignOutputNames(/*includeHiddenColumns=*/false);
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

    auto alias = canonicalizeIdentifier(*aliasedRelation.alias());

    const auto& columnAliases = aliasedRelation.columnNames();
    if (!columnAliases.empty()) {
      const size_t numOutput = builder_->numOutput();
      VELOX_USER_CHECK_EQ(
          columnAliases.size(),
          numOutput,
          "Column alias list size does not match the number of columns available for '{}'",
          alias);

      // Add projection to rename columns. Column aliases override output
      // names from the inner subquery.
      std::vector<lp::ExprApi> renames;
      renames.reserve(numOutput);

      for (auto i = 0; i < numOutput; ++i) {
        auto name = canonicalizeIdentifier(*columnAliases.at(i));
        auto column = lp::Col(builder_->findOrAssignOutputNameAt(i));
        renames.push_back(column.as(name));
      }

      builder_->project(renames);
    }

    builder_->findOrAssignOutputNames(/*includeHiddenColumns=*/false);
    builder_->as(alias);
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

    std::optional<lp::ExprApi> ordinality;
    if (unnest.isWithOrdinality()) {
      ordinality = lp::Ordinality();
    }
    builder_->unnest(inputs, ordinality);
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

    if (const auto& criteria = join.criteria()) {
      if (criteria->is(NodeType::kJoinOn)) {
        // Create a combined scope that can resolve columns from both sides of
        // the join. Subqueries in the ON clause may contain correlated
        // references to either side.
        lp::PlanBuilder::Scope joinScope = [leftScope,
                                            rightScope = rightBuilder->scope(),
                                            leftBuilder,
                                            rightBuilder](
                                               const auto& alias,
                                               const auto& name) {
          return resolveJoinColumn(
              leftScope, rightScope, *leftBuilder, *rightBuilder, alias, name);
        };

        builder_ = newBuilder(joinScope);
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

  // Adds a projection that computes window functions. Returns a map from
  // window call IExpr* to column reference ExprPtr for replacing window
  // calls in downstream expressions.
  std::unordered_map<const core::IExpr*, core::ExprPtr> addWindowProjection(
      const std::unordered_map<const core::IExpr*, lp::WindowSpec>&
          windowOptions,
      const std::vector<lp::ExprApi>& exprs) {
    auto inputColumns =
        builder_->findOrAssignOutputNames(/*includeHiddenColumns=*/false);

    // Collect ExprPtrs for the window calls from the expression trees.
    // 'windowOrder' captures matches in left-to-right traversal order
    // for deterministic plan generation.
    std::unordered_map<const core::IExpr*, core::ExprPtr> windowExprPtrs;
    std::vector<const core::IExpr*> windowOrder;
    for (const auto& expr : exprs) {
      findExprPtrs(expr.expr(), windowOptions, windowExprPtrs, windowOrder);
    }

    std::vector<lp::ExprApi> windowProjection;
    windowProjection.reserve(inputColumns.size() + windowOrder.size());
    for (const auto& name : inputColumns) {
      windowProjection.push_back(lp::Col(name));
    }

    // TODO: Deduplicate semantically equivalent window function calls.
    // Currently, each occurrence of the same window expression (e.g.
    // sum(b) OVER (PARTITION BY a)) gets a separate entry in windowOptions
    // and is computed redundantly.
    for (const auto* exprPtr : windowOrder) {
      windowProjection.push_back(
          lp::ExprApi(windowExprPtrs.at(exprPtr))
              .over(windowOptions.at(exprPtr)));
    }

    builder_->project(windowProjection);

    auto outputNames =
        builder_->findOrAssignOutputNames(/*includeHiddenColumns=*/false);

    std::unordered_map<const core::IExpr*, core::ExprPtr> replacements;
    for (size_t i = 0; i < windowOrder.size(); ++i) {
      const auto& name = outputNames.at(inputColumns.size() + i);
      replacements.emplace(windowOrder[i], lp::Col(name).expr());
    }
    return replacements;
  }

  // Converts SELECT items to ExprApi projections. Exits early by returning
  // std::nullopt for a single SELECT *.
  std::optional<std::vector<lp::ExprApi>> buildSelectProjections(
      const std::vector<SelectItemPtr>& selectItems) {
    // SELECT * FROM ...
    const bool isSingleSelectStar = selectItems.size() == 1 &&
        selectItems.at(0)->is(NodeType::kAllColumns) &&
        selectItems.at(0)->as<AllColumns>()->prefix() == nullptr;
    if (isSingleSelectStar) {
      return std::nullopt;
    }

    const bool hasNestedWindow = hasNestedWindowFunction(selectItems);

    // When hasNestedWindow is true, window function calls are collected in
    // windowOptions (keyed by IExpr*) and returned as plain function calls.
    // Post-hoc, we extract them and replace with column references, similar
    // to how GroupByPlanner handles aggregates in expressions.
    std::unordered_map<const core::IExpr*, lp::WindowSpec> windowOptions;

    // Lateral column alias map: alias name → expression. When friendlySql is
    // enabled, aliases defined in earlier SELECT items can be referenced in
    // later items. Populated left-to-right as aliases are encountered.
    // Column names take priority over aliases to preserve backward
    // compatibility (e.g., SELECT a AS b, b FROM t — second b is column b).
    std::unordered_map<std::string, core::ExprPtr> aliasExprs;
    std::unordered_set<std::string> columnNames;
    if (friendlySql_) {
      for (const auto& name : builder_->outputNames()) {
        if (name.has_value()) {
          columnNames.insert(name.value());
        }
      }
      exprPlanner_.setLateralAliases(&aliasExprs, &columnNames);
    }
    SCOPE_EXIT {
      exprPlanner_.clearLateralAliases();
    };

    std::vector<lp::ExprApi> exprs;
    for (const auto& item : selectItems) {
      if (item->is(NodeType::kAllColumns)) {
        auto* allColumns = item->as<AllColumns>();

        std::vector<std::string> columnNames;
        if (allColumns->prefix() != nullptr) {
          // SELECT t.*
          const auto prefix = canonicalizeName(allColumns->prefix()->suffix());
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

        lp::ExprApi expr = toExpr(
            singleColumn->expression(),
            /*aggregateOptions=*/nullptr,
            hasNestedWindow ? &windowOptions : nullptr);

        if (singleColumn->alias() != nullptr) {
          auto alias = canonicalizeIdentifier(*singleColumn->alias());
          if (friendlySql_) {
            aliasExprs[alias] = expr.expr();
          }
          expr = expr.as(alias);
        }
        exprs.push_back(expr);
      }
    }

    if (hasNestedWindow) {
      auto replacements = addWindowProjection(windowOptions, exprs);
      for (auto& expr : exprs) {
        expr =
            lp::ExprApi(replaceInputs(expr.expr(), replacements), expr.alias());
      }
    }

    return exprs;
  }

  // Adds a project node if projection list is non-null (i.e. not SELECT *). If
  // projection list is null, drop hidden columns and exit early.
  void addProject(const std::vector<SelectItemPtr>& selectItems) {
    auto projections = buildSelectProjections(selectItems);
    if (!projections.has_value()) {
      builder_->dropHiddenColumns();
      return;
    }
    builder_->project(projections.value());
  }

  // Build sort key expressions. Ordinals are resolved to the corresponding
  // SELECT projection expression so widenProjectionsForSort can match them
  // by expression identity.
  std::pair<std::vector<lp::ExprApi>, std::vector<size_t>> buildSortKeyExprs(
      const OrderByPtr& orderBy,
      const std::vector<lp::ExprApi>& projections) {
    const size_t numSelectItems = projections.size();
    std::vector<lp::ExprApi> sortKeyExprs;
    std::vector<size_t> preResolved(orderBy->sortItems().size(), 0);
    for (size_t i = 0; i < orderBy->sortItems().size(); ++i) {
      const auto& item = orderBy->sortItems()[i];
      const auto& sortExpr = item->sortKey();
      if (sortExpr->is(NodeType::kLongLiteral)) {
        const auto n = sortExpr->as<LongLiteral>()->value();
        VELOX_USER_CHECK_GE(
            n, 1, "ORDER BY position is not in the select list: {}", n);
        VELOX_USER_CHECK_LE(
            n,
            numSelectItems,
            "ORDER BY position is not in the select list: {}",
            n);
        preResolved[i] = n;
        sortKeyExprs.emplace_back(projections.at(n - 1));
      } else {
        sortKeyExprs.emplace_back(toExpr(sortExpr));
      }
    }
    return {sortKeyExprs, preResolved};
  }

  // Adds project and sort nodes. In order to provide sort with schema-level
  // visibility, we project twice: once with the items in the SELECT list AND
  // table columns referenced by the sort keys and once with just the items in
  // the SELECT list. There are a few edge cases with unique behavior, however.
  // If the projection list is null (SELECT *), we exit early without projecting
  // and if there is no ORDER BY clause, we just project the SELECT list only.
  void addProjectAndOrderBy(
      const std::vector<SelectItemPtr>& selectItems,
      const OrderByPtr& orderBy) {
    auto projections = buildSelectProjections(selectItems);
    if (!projections.has_value()) {
      addOrderBy(orderBy);
      builder_->dropHiddenColumns();
      return;
    }
    if (orderBy == nullptr) {
      builder_->project(projections.value());
      return;
    }

    const size_t numSelectItems = projections->size();
    auto [sortKeyExprs, preResolved] =
        buildSortKeyExprs(orderBy, projections.value());

    auto ordinals = SortProjection::widenProjections(
        sortKeyExprs, preResolved, projections.value());
    builder_->project(projections.value());
    SortProjection::sortAndTrim(
        *builder_, orderBy->sortItems(), ordinals, numSelectItems);
  }

  lp::ExprApi toSortingKey(const ExpressionPtr& expr) {
    if (expr->is(NodeType::kLongLiteral)) {
      const auto n = expr->as<LongLiteral>()->value();
      VELOX_USER_CHECK_GE(
          n, 1, "ORDER BY position is not in the select list: {}", n);
      VELOX_USER_CHECK_LE(
          n,
          builder_->numOutput(),
          "ORDER BY position is not in the select list: {}",
          n);
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
      VELOX_USER_CHECK(!with->isRecursive(), "WITH RECURSIVE is not supported");
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
      GroupByPlanner{builder_, exprPlanner_}.plan(
          groupBy->groupingElements(),
          groupBy->isDistinct(),
          selectItems,
          node->having(),
          orderBy);

      if (distinct) {
        builder_->distinct();
      }
    } else {
      if (GroupByPlanner{builder_, exprPlanner_}.tryPlanGlobalAgg(
              selectItems, node->having())) {
        // Nothing else to do. For aggregation, ORDER BY can only reference
        // SELECT list (aggregates). DISTINCT will be a no-op since global
        // aggregations without a group by are 1 row output.
        addOrderBy(orderBy);
      } else if (distinct) {
        // With DISTINCT, ORDER BY can only reference SELECT-list columns.
        // Project first, then sort.
        addProject(selectItems);
        builder_->distinct();
        addOrderBy(orderBy);
      } else {
        // Widen the projection to include any ORDER BY columns not in the
        // SELECT list, sort, then project again using only the SELECT list.
        addProjectAndOrderBy(selectItems, orderBy);
      }
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
        node->isDistinct() ? lp::SetOperation::kExcept
                           : lp::SetOperation::kExceptAll,
        node->left(),
        node->right());
  }

  void visitIntersect(Intersect* node) override {
    visitSetOperation(
        node->isDistinct() ? lp::SetOperation::kIntersect
                           : lp::SetOperation::kIntersectAll,
        node->left(),
        node->right());
  }

  void visitUnion(Union* node) override {
    visitSetOperation(
        node->isDistinct() ? lp::SetOperation::kUnion
                           : lp::SetOperation::kUnionAll,
        node->left(),
        node->right());
  }

  void visitSetOperation(
      lp::SetOperation op,
      const std::shared_ptr<QueryBody>& left,
      const std::shared_ptr<QueryBody>& right) {
    left->accept(this);

    auto leftBuilder = builder_;

    builder_ = newBuilder(leftBuilder->scope());
    right->accept(this);
    auto rightBuilder = builder_;

    builder_ = leftBuilder;
    builder_->setOperation(op, *rightBuilder);
  }

  std::shared_ptr<lp::PlanBuilder> newBuilder(
      const lp::PlanBuilder::Scope& outerScope = nullptr) {
    return std::make_shared<lp::PlanBuilder>(
        context_,
        /*enableCoercions=*/true,
        /*allowAmbiguousOutputNames=*/true,
        outerScope);
  }

  lp::PlanBuilder::Context context_;
  const std::string defaultSchema_;
  const std::function<std::shared_ptr<axiom::sql::presto::Statement>(
      std::string_view /*sql*/)>
      parseSql_;
  std::shared_ptr<lp::PlanBuilder> builder_;
  ExpressionPlanner exprPlanner_{
      [this](Query* query) -> lp::LogicalPlanNodePtr {
        // Save and restore builder. Correlated subqueries run on the same
        // RelationPlanner and must not overwrite the outer scope's state.
        // userOutputNames_ lives inside the builder, so it is saved and
        // restored automatically.
        auto builder = std::move(builder_);
        SCOPE_EXIT {
          builder_ = std::move(builder);
        };

        builder_ = newBuilder(builder->scope());
        processQuery(query);
        return builder_->planNode();
      },
      [this](const ExpressionPtr& expr) -> lp::ExprApi {
        return toSortingKey(expr);
      }};
  std::unordered_map<std::string, std::shared_ptr<WithQuery>> withQueries_;
  ViewMap views_;
  bool friendlySql_;
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

  // plan() always wraps in OutputNode; look through it.
  auto plan = statement->as<SelectStatement>()->plan()->onlyInput();
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
  ExplainStatement::Type type = ExplainStatement::Type::kExecutable;
  ExplainStatement::Format format = ExplainStatement::Format::kText;

  if (explain.isAnalyze()) {
    return std::make_shared<ExplainStatement>(
        sqlStatement,
        /*analyze=*/true,
        type,
        format);
  }

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
        case ExplainType::Type::kIo:
          type = ExplainStatement::Type::kIo;
          break;
        default:
          VELOX_USER_FAIL("Unsupported EXPLAIN type");
      }
    } else if (option->is(NodeType::kExplainFormat)) {
      const auto explainFormat = option->as<ExplainFormat>()->formatType();
      switch (explainFormat) {
        case ExplainFormat::Type::kText:
          format = ExplainStatement::Format::kText;
          break;
        case ExplainFormat::Type::kGraphviz:
          format = ExplainStatement::Format::kGraphviz;
          break;
        case ExplainFormat::Type::kJson:
          format = ExplainStatement::Format::kJson;
          break;
      }
    }
  }

  return std::make_shared<ExplainStatement>(
      sqlStatement,
      /*analyze=*/false,
      type,
      format);
}

static facebook::axiom::connector::TablePtr findTable(
    const QualifiedName& name,
    const std::string& defaultConnectorId,
    const std::string& defaultSchema) {
  const auto [connectorId, connectorTable] =
      toConnectorTable(name, defaultConnectorId, defaultSchema);

  auto table =
      facebook::axiom::connector::ConnectorMetadata::metadata(connectorId)
          ->findTable(connectorTable);

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
      ROW({"catalog_name", "connector_id", "connector_name"}, VARCHAR()),
      std::move(data));

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
    const std::string& defaultSchema) {
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

// Formats a Variant value as a Presto SQL literal for use in WITH clauses.
std::string variantToSql(const Variant& value) {
  switch (value.kind()) {
    case TypeKind::VARCHAR: {
      auto escaped = value.value<std::string>();
      // Escape single quotes by doubling them per SQL standard.
      size_t pos = 0;
      while ((pos = escaped.find('\'', pos)) != std::string::npos) {
        escaped.replace(pos, 1, "''");
        pos += 2;
      }
      return fmt::format("'{}'", escaped);
    }
    case TypeKind::INTEGER:
      return std::to_string(value.value<int32_t>());
    case TypeKind::BIGINT:
      return std::to_string(value.value<int64_t>());
    case TypeKind::BOOLEAN:
      return value.value<bool>() ? "true" : "false";
    case TypeKind::ARRAY: {
      const auto& elements = value.array();
      std::vector<std::string> formatted;
      formatted.reserve(elements.size());
      for (const auto& element : elements) {
        formatted.push_back(variantToSql(element));
      }
      return fmt::format("ARRAY[{}]", folly::join(", ", formatted));
    }
    default:
      VELOX_UNSUPPORTED(
          "Unsupported Variant kind in WITH clause: {}",
          TypeKindName::toName(value.kind()));
  }
}

SqlStatementPtr parseShowCreateTable(
    const ShowCreateTable& showCreateTable,
    const std::string& defaultConnectorId,
    const std::string& defaultSchema) {
  using facebook::velox::PrestoTypes;

  const auto table =
      findTable(*showCreateTable.name(), defaultConnectorId, defaultSchema);
  const auto& schema = table->type();
  const auto& options = table->options();

  const auto [connectorId, schemaTableName] = toConnectorTable(
      *showCreateTable.name(), defaultConnectorId, defaultSchema);

  std::stringstream ddl;
  ddl << "CREATE TABLE " << connectorId << "."
      << fmt::format("{}", schemaTableName) << " (\n";

  // Column definitions.
  for (auto i = 0; i < schema->size(); ++i) {
    if (i > 0) {
      ddl << ",\n";
    }
    ddl << "   " << schema->nameOf(i) << " "
        << PrestoTypes::toSql(schema->childAt(i));
  }
  ddl << "\n)";

  // WITH clause from table options.
  if (!options.empty()) {
    ddl << "\nWITH (\n";
    bool first = true;
    for (const auto& [key, value] : options) {
      if (!first) {
        ddl << ",\n";
      }
      ddl << "   " << key << " = " << variantToSql(value);
      first = false;
    }
    ddl << "\n)";
  }

  return std::make_shared<SelectStatement>(
      lp::PlanBuilder()
          .values(ROW({"Create Table"}, VARCHAR()), {Variant::row({ddl.str()})})
          .build());
}

SqlStatementPtr parseShowStats(
    const ShowStats& showStats,
    const std::string& defaultConnectorId,
    const std::string& defaultSchema) {
  const auto table =
      findTable(*showStats.table(), defaultConnectorId, defaultSchema);
  const auto schema = table->type();

  ShowStatsBuilder builder(static_cast<int64_t>(table->numRows()));
  for (auto i = 0; i < schema->size(); ++i) {
    const auto* column = table->columnMap().at(schema->nameOf(i));
    const auto* stats = column->stats();

    std::optional<double> nullsFraction;
    std::optional<int64_t> distinctCount;
    std::optional<int64_t> avgLength;
    const Variant* min{nullptr};
    const Variant* max{nullptr};

    if (stats != nullptr) {
      nullsFraction = static_cast<double>(stats->nullPct) / 100.0;
      if (stats->numDistinct.has_value()) {
        distinctCount = static_cast<int64_t>(stats->numDistinct.value());
      }
      avgLength = stats->avgLength;
      if (stats->min.has_value()) {
        min = &stats->min.value();
      }
      if (stats->max.has_value()) {
        max = &stats->max.value();
      }
    }

    builder.addColumn(
        schema->nameOf(i),
        *column->type(),
        nullsFraction,
        distinctCount,
        avgLength,
        min,
        max);
  }

  lp::PlanBuilder::Context ctx(defaultConnectorId);
  return std::make_shared<SelectStatement>(
      lp::PlanBuilder(ctx)
          .values(ShowStatsBuilder::outputType(), builder.rows())
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
    const std::string& defaultSchema,
    const std::function<std::shared_ptr<axiom::sql::presto::Statement>(
        std::string_view /*sql*/)>& parseSql) {
  const auto [connectorId, connectorTable] =
      toConnectorTable(*insert.target(), defaultConnectorId, defaultSchema);

  const auto table =
      facebook::axiom::connector::ConnectorMetadata::metadata(connectorId)
          ->findTable(connectorTable);

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
      connectorId,
      connectorTable.schema,
      connectorTable.table,
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
    const std::string& defaultSchema,
    const std::function<std::shared_ptr<axiom::sql::presto::Statement>(
        std::string_view /*sql*/)>& parseSql) {
  auto [connectorId, connectorTable] =
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
        connectorId,
        connectorTable.schema,
        connectorTable.table,
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
        connectorId,
        connectorTable.schema,
        connectorTable.table,
        lp::WriteKind::kCreate,
        columnNames,
        toColumnExprs(planBuilder.findOrAssignOutputNames()));
  }

  return std::make_shared<CreateTableAsSelectStatement>(
      std::move(connectorId),
      std::move(connectorTable),
      ROW(std::move(columnNames), std::move(columnTypes)),
      std::move(properties),
      planner.plan(),
      planner.views());
}

SqlStatementPtr parseCreateTable(
    const CreateTable& createTable,
    const std::string& defaultConnectorId,
    const std::string& defaultSchema) {
  auto [connectorId, connectorTable] =
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
      std::move(connectorId),
      std::move(connectorTable),
      ROW(std::move(names), std::move(types)),
      std::move(properties),
      createTable.isNotExists(),
      std::move(constraints));
}

SqlStatementPtr parseDropTable(
    const DropTable& dropTable,
    const std::string& defaultConnectorId,
    const std::string& defaultSchema) {
  auto [connectorId, connectorTable] = toConnectorTable(
      *dropTable.tableName(), defaultConnectorId, defaultSchema);

  return std::make_shared<DropTableStatement>(
      std::move(connectorId), std::move(connectorTable), dropTable.isExists());
}

SqlStatementPtr parseCreateSchema(
    const CreateSchema& createSchema,
    const std::string& defaultConnectorId) {
  const auto& parts = createSchema.schemaName()->parts();
  VELOX_USER_CHECK(
      parts.size() == 1 || parts.size() == 2,
      "Invalid schema name: {}",
      createSchema.schemaName()->fullyQualifiedName());

  std::string connectorId = parts.size() == 2 ? parts[0] : defaultConnectorId;
  std::string schemaName = parts.size() == 2 ? parts[1] : parts[0];

  auto properties = parseTableProperties(createSchema.properties());

  return std::make_shared<CreateSchemaStatement>(
      std::move(connectorId),
      std::move(schemaName),
      createSchema.isNotExists(),
      std::move(properties));
}

SqlStatementPtr parseDropSchema(
    const DropSchema& dropSchema,
    const std::string& defaultConnectorId) {
  const auto& parts = dropSchema.schemaName()->parts();
  VELOX_USER_CHECK(
      parts.size() == 1 || parts.size() == 2,
      "Invalid schema name: {}",
      dropSchema.schemaName()->fullyQualifiedName());

  VELOX_USER_CHECK(
      dropSchema.behavior() != DropSchema::DropBehavior::kCascade,
      "DROP SCHEMA CASCADE is not supported");

  std::string connectorId = parts.size() == 2 ? parts[0] : defaultConnectorId;
  std::string schemaName = parts.size() == 2 ? parts[1] : parts[0];

  return std::make_shared<DropSchemaStatement>(
      std::move(connectorId), std::move(schemaName), dropSchema.isExists());
}

SqlStatementPtr parseShowSchemas(
    const ShowSchemas& showSchemas,
    const std::string& defaultConnectorId) {
  const auto connectorId = showSchemas.catalog().value_or(defaultConnectorId);

  auto metadata =
      facebook::axiom::connector::ConnectorMetadata::metadata(connectorId);
  auto session = std::make_shared<facebook::axiom::connector::ConnectorSession>(
      "show-schemas");
  auto schemaNames = metadata->listSchemaNames(session);
  std::sort(schemaNames.begin(), schemaNames.end());

  std::vector<Variant> data;
  data.reserve(schemaNames.size());
  for (const auto& name : schemaNames) {
    data.emplace_back(Variant::row({name}));
  }

  lp::PlanBuilder::Context ctx(defaultConnectorId);
  lp::PlanBuilder builder(ctx);
  builder.values(ROW({"Schema"}, VARCHAR()), std::move(data));

  if (showSchemas.getLikePattern().has_value()) {
    builder.filter(makeLikeExpr(
        "Schema",
        showSchemas.getLikePattern().value(),
        showSchemas.getEscape()));
  }

  return std::make_shared<SelectStatement>(builder.build());
}

// Extracts the literal value from a SET SESSION statement.
SqlStatementPtr parseSetSession(const SetSession* setSession) {
  auto name = setSession->name()->fullyQualifiedName();

  auto* value = setSession->value().get();
  std::string valueString;
  if (value->is(NodeType::kStringLiteral)) {
    valueString = value->as<StringLiteral>()->value();
  } else if (value->is(NodeType::kLongLiteral)) {
    valueString = std::to_string(value->as<LongLiteral>()->value());
  } else if (value->is(NodeType::kBooleanLiteral)) {
    valueString = value->as<BooleanLiteral>()->value() ? "true" : "false";
  } else if (value->is(NodeType::kDoubleLiteral)) {
    valueString = std::to_string(value->as<DoubleLiteral>()->value());
  } else {
    VELOX_USER_FAIL("SET SESSION value must be a literal: {}", name);
  }

  return std::make_shared<SetSessionStatement>(
      std::move(name), std::move(valueString));
}

SqlStatementPtr doPlan(
    const std::shared_ptr<Statement>& query,
    const std::string& defaultConnectorId,
    const std::string& defaultSchema,
    const std::function<std::shared_ptr<axiom::sql::presto::Statement>(
        std::string_view /*sql*/)>& parseSql,
    bool friendlySql = true) {
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

  if (query->is(NodeType::kDropTable)) {
    return parseDropTable(
        *query->as<DropTable>(), defaultConnectorId, defaultSchema);
  }

  if (query->is(NodeType::kCreateSchema)) {
    return parseCreateSchema(*query->as<CreateSchema>(), defaultConnectorId);
  }

  if (query->is(NodeType::kDropSchema)) {
    return parseDropSchema(*query->as<DropSchema>(), defaultConnectorId);
  }

  if (query->is(NodeType::kShowSchemas)) {
    return parseShowSchemas(*query->as<ShowSchemas>(), defaultConnectorId);
  }

  if (query->is(NodeType::kShowCatalogs)) {
    return parseShowCatalogs(*query->as<ShowCatalogs>(), defaultConnectorId);
  }

  if (query->is(NodeType::kShowCreate)) {
    return parseShowCreateTable(
        *query->as<ShowCreateTable>(), defaultConnectorId, defaultSchema);
  }

  if (query->is(NodeType::kShowColumns)) {
    return parseShowColumns(
        *query->as<ShowColumns>(), defaultConnectorId, defaultSchema);
  }

  if (query->is(NodeType::kShowStats)) {
    return parseShowStats(
        *query->as<ShowStats>(), defaultConnectorId, defaultSchema);
  }

  if (query->is(NodeType::kShowStatsForQuery)) {
    auto* showStats = query->as<ShowStatsForQuery>();
    RelationPlanner planner(defaultConnectorId, defaultSchema, parseSql);
    showStats->query()->accept(&planner);
    auto innerStatement =
        std::make_shared<SelectStatement>(planner.plan(), planner.views());
    return std::make_shared<ShowStatsForQueryStatement>(
        std::move(innerStatement));
  }

  if (query->is(NodeType::kShowFunctions)) {
    return parseShowFunctions(*query->as<ShowFunctions>(), defaultConnectorId);
  }

  if (query->is(NodeType::kQuery)) {
    RelationPlanner planner(
        defaultConnectorId, defaultSchema, parseSql, friendlySql);
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
  auto parseSql = [this, enableTracing](std::string_view sql) {
    ParserHelper helper(sql);
    auto* context = helper.parse();

    AstBuilder astBuilder(options_, enableTracing);
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
        explain->statement(),
        defaultConnectorId_,
        defaultSchema_,
        parseSql,
        options_.friendlySql);
    return parseExplain(*explain, sqlStatement);
  }

  if (query->is(NodeType::kShowSession)) {
    auto* showSession = query->as<ShowSession>();
    return std::make_shared<ShowSessionStatement>(showSession->likePattern());
  }

  if (query->is(NodeType::kSetSession)) {
    return parseSetSession(query->as<SetSession>());
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

  return doPlan(
      query,
      defaultConnectorId_,
      defaultSchema_,
      parseSql,
      options_.friendlySql);
}

ReferencedTables PrestoParser::getReferencedTables(std::string_view sql) {
  ParserHelper helper(sql);
  auto* context = helper.parse();

  AstBuilder astBuilder(options_, false);
  auto statement =
      std::any_cast<std::shared_ptr<Statement>>(astBuilder.visit(context));

  TableVisitor visitor(defaultConnectorId_, defaultSchema_);
  visitor.process(statement.get());
  return ReferencedTables{
      .inputTables = visitor.inputTables(),
      .outputTable = visitor.outputTable()};
}

} // namespace axiom::sql::presto
