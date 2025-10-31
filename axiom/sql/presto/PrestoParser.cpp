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
#include <algorithm>
#include <cctype>
#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/sql/presto/ast/AstBuilder.h"
#include "axiom/sql/presto/ast/AstPrinter.h"
#include "axiom/sql/presto/ast/UpperCaseInputStream.h"
#include "axiom/sql/presto/grammar/PrestoSqlLexer.h"
#include "axiom/sql/presto/grammar/PrestoSqlParser.h"
#include "velox/exec/Aggregate.h"

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
  }

  PrestoSqlParser& parser() const {
    return *parser_;
  }

  PrestoSqlParser::StatementContext* parse() const {
    auto ctx = parser_->statement();

    if (parser_->getNumberOfSyntaxErrors() > 0) {
      throw std::runtime_error(errorListener_.firstError);
    }

    return ctx;
  }

 private:
  std::unique_ptr<antlr4::ANTLRInputStream> inputStream_;
  std::unique_ptr<PrestoSqlLexer> lexer_;
  std::unique_ptr<antlr4::CommonTokenStream> tokenStream_;
  std::unique_ptr<PrestoSqlParser> parser_;
  ErrorListener errorListener_;
};

using ExprMap = folly::
    F14FastMap<core::ExprPtr, core::ExprPtr, core::IExprHash, core::IExprEqual>;

// Given an expression, and pairs of search-and-replace sub-expressions,
// produces a new expression with sub-expressions replaced.
core::ExprPtr replaceInputs(
    const core::ExprPtr& expr,
    const ExprMap& replacements) {
  auto it = replacements.find(expr);
  if (it != replacements.end()) {
    return it->second;
  }

  std::vector<core::ExprPtr> newInputs;
  bool hasNewInput = false;
  for (const auto& input : expr->inputs()) {
    auto newInput = replaceInputs(input, replacements);
    if (newInput.get() != input.get()) {
      hasNewInput = true;
    }
    newInputs.push_back(newInput);
  }

  if (hasNewInput) {
    return expr->replaceInputs(std::move(newInputs));
  }

  return expr;
}

// Walks the expression tree looking for aggregate function calls and appending
// these to 'aggregates'.
void findAggregates(
    const core::ExprPtr expr,
    std::vector<lp::ExprApi>& aggregates) {
  switch (expr->kind()) {
    case core::IExpr::Kind::kInput:
      return;
    case core::IExpr::Kind::kFieldAccess:
      return;
    case core::IExpr::Kind::kCall: {
      if (facebook::velox::exec::getAggregateFunctionEntry(
              expr->as<core::CallExpr>()->name())) {
        aggregates.emplace_back(lp::ExprApi(expr));
      } else {
        for (const auto& input : expr->inputs()) {
          findAggregates(input, aggregates);
        }
      }
      return;
    }
    case core::IExpr::Kind::kCast:
      findAggregates(expr->as<core::CastExpr>()->input(), aggregates);
      return;
    case core::IExpr::Kind::kConstant:
      return;
    case core::IExpr::Kind::kLambda:
      // TODO Reject aggregates in lambda expressions.
      return;
    case core::IExpr::Kind::kSubquery:
      // TODO Handle aggregates in subqueries.
      return;
  }
}

bool asQualifiedName(
    const ExpressionPtr& expr,
    std::vector<std::string>& names) {
  if (expr->is(NodeType::kIdentifier)) {
    names.push_back(expr->as<Identifier>()->value());
    return true;
  }

  if (expr->is(NodeType::kDereferenceExpression)) {
    auto* dereference = expr->as<DereferenceExpression>();
    names.push_back(dereference->field()->value());
    return asQualifiedName(dereference->base(), names);
  }

  return false;
}

// Analizes the expression to find out whether there are any aggregate function
// calls and to verify that aggregate calls are not nested, e.g. sum(count(x))
// is not allowed.
class ExprAnalyzer : public AstVisitor {
 public:
  bool hasAggregate() const {
    return numAggregates_ > 0;
  }

 private:
  void defaultVisit(Node* node) override {
    if (dynamic_cast<Literal*>(node) != nullptr) {
      // Literals have no function calls.
      return;
    }

    VELOX_NYI(
        "Not yet supported node type: {}", NodeTypeName::toName(node->type()));
  }

  void visitCast(Cast* node) override {
    node->expression()->accept(this);
  }

  void visitDereferenceExpression(DereferenceExpression* node) override {
    node->base()->accept(this);
  }

  void visitExtract(Extract* node) override {
    node->expression()->accept(this);
  }

  void visitFunctionCall(FunctionCall* node) override {
    const auto& name = node->name()->suffix();
    if (facebook::velox::exec::getAggregateFunctionEntry(name)) {
      VELOX_USER_CHECK(
          !aggregateName_.has_value(),
          "Cannot nest aggregations inside aggregation: {}({})",
          aggregateName_.value(),
          name);

      aggregateName_ = name;
      ++numAggregates_;
    }

    for (const auto& arg : node->arguments()) {
      arg->accept(this);
    }

    aggregateName_.reset();
  }

  void visitArithmeticBinaryExpression(
      ArithmeticBinaryExpression* node) override {
    node->left()->accept(this);
    node->right()->accept(this);
  }

  void visitLogicalBinaryExpression(LogicalBinaryExpression* node) override {
    node->left()->accept(this);
    node->right()->accept(this);
  }

  void visitComparisonExpression(ComparisonExpression* node) override {
    node->left()->accept(this);
    node->right()->accept(this);
  }

  void visitLikePredicate(LikePredicate* node) override {
    node->value()->accept(this);
    node->pattern()->accept(this);
    if (node->escape() != nullptr) {
      node->escape()->accept(this);
    }
  }

  void visitSearchedCaseExpression(SearchedCaseExpression* node) override {
    for (const auto& clause : node->whenClauses()) {
      clause->operand()->accept(this);
      clause->result()->accept(this);
    }

    if (node->defaultValue()) {
      node->defaultValue()->accept(this);
    }
  }

  void visitIdentifier(Identifier* node) override {
    // No function calls.
  }

  size_t numAggregates_{0};
  std::optional<std::string> aggregateName_;
};

class RelationPlanner : public AstVisitor {
 public:
  explicit RelationPlanner(const std::string& defaultConnectorId)
      : context_{defaultConnectorId}, builder_(newBuilder()) {}

  lp::LogicalPlanNodePtr getPlan() {
    return builder_->build();
  }

  lp::PlanBuilder& builder() {
    return *builder_;
  }

  lp::ExprApi toExpr(const ExpressionPtr& node) {
    switch (node->type()) {
      case NodeType::kIdentifier:
        return lp::Col(node->as<Identifier>()->value());

      case NodeType::kDereferenceExpression: {
        std::vector<std::string> names;
        if (asQualifiedName(node, names)) {
          VELOX_USER_CHECK_EQ(2, names.size());
          return lp::Col(names.at(0), lp::Col(names.at(1)));
        }

        auto* dereference = node->as<DereferenceExpression>();
        return lp::Col(
            dereference->field()->value(), toExpr(dereference->base()));
      }

      case NodeType::kSubqueryExpression: {
        auto* subquery = node->as<SubqueryExpression>();
        auto query = subquery->query();

        if (query->is(NodeType::kQuery)) {
          auto builder = std::move(builder_);

          lp::PlanBuilder::Scope scope;
          builder->captureScope(scope);

          builder_ = newBuilder(scope);
          processQuery(query->as<Query>());
          auto subqueryBuider = builder_;

          builder_ = std::move(builder);
          return lp::Subquery(subqueryBuider->build(/*useIds=*/true));
        }

        VELOX_NYI(
            "Subquery type is not supported yet: {}",
            NodeTypeName::toName(query->type()));
      }

      case NodeType::kComparisonExpression: {
        auto* comparison = node->as<ComparisonExpression>();
        return lp::Call(
            toFunctionName(comparison->op()),
            toExpr(comparison->left()),
            toExpr(comparison->right()));
      }

      case NodeType::kNotExpression: {
        auto* negation = node->as<NotExpression>();
        return lp::Call("not", toExpr(negation->value()));
      }

      case NodeType::kLikePredicate: {
        auto* like = node->as<LikePredicate>();

        std::vector<lp::ExprApi> inputs;
        inputs.emplace_back(toExpr(like->value()));
        inputs.emplace_back(toExpr(like->pattern()));
        if (like->escape()) {
          inputs.emplace_back(toExpr(like->escape()));
        }

        return lp::Call("like", std::move(inputs));
      }

      case NodeType::kLogicalBinaryExpression: {
        auto* logical = node->as<LogicalBinaryExpression>();
        auto left = toExpr(logical->left());
        auto right = toExpr(logical->right());

        switch (logical->op()) {
          case LogicalBinaryExpression::Operator::kAnd:
            return left && right;

          case LogicalBinaryExpression::Operator::kOr:
            return left || right;
        }
      }

      case NodeType::kArithmeticBinaryExpression: {
        auto* binary = node->as<ArithmeticBinaryExpression>();
        return lp::Call(
            toFunctionName(binary->op()),
            toExpr(binary->left()),
            toExpr(binary->right()));
      }

      case NodeType::kBetweenPredicate: {
        auto* between = node->as<BetweenPredicate>();
        return lp::Call(
            "between",
            toExpr(between->value()),
            toExpr(between->min()),
            toExpr(between->max()));
      }

      case NodeType::kInPredicate: {
        auto* inPredicate = node->as<InPredicate>();
        const auto& valueList = inPredicate->valueList();

        const auto value = toExpr(inPredicate->value());

        if (valueList->is(NodeType::kInListExpression)) {
          auto inList = valueList->as<InListExpression>();

          std::vector<lp::ExprApi> inputs;
          inputs.reserve(1 + inList->values().size());

          inputs.emplace_back(value);
          for (const auto& expr : inList->values()) {
            inputs.emplace_back(toExpr(expr));
          }

          return lp::Call("in", inputs);
        }

        if (valueList->is(NodeType::kSubqueryExpression)) {
          return lp::Call("in", value, toExpr(valueList));
        }

        VELOX_USER_FAIL(
            "Unexpected IN predicate: {}",
            NodeTypeName::toName(valueList->type()));
      }

      case NodeType::kExistsPredicate: {
        auto* exists = node->as<ExistsPredicate>();
        return lp::Exists(toExpr(exists->subquery()));
      }

      case NodeType::kCast: {
        auto* cast = node->as<Cast>();
        const auto type = parseType(cast->toType());

        if (cast->isSafe()) {
          return lp::TryCast(type, toExpr(cast->expression()));
        } else {
          return lp::Cast(type, toExpr(cast->expression()));
        }
      }

      case NodeType::kSearchedCaseExpression: {
        auto* searchedCase = node->as<SearchedCaseExpression>();

        std::vector<lp::ExprApi> inputs;
        inputs.reserve(1 + searchedCase->whenClauses().size());

        for (const auto& clause : searchedCase->whenClauses()) {
          inputs.emplace_back(toExpr(clause->operand()));
          inputs.emplace_back(toExpr(clause->result()));
        }

        if (searchedCase->defaultValue()) {
          inputs.emplace_back(toExpr(searchedCase->defaultValue()));
        }

        return lp::Call("switch", inputs);
      }

      case NodeType::kExtract: {
        auto* extract = node->as<Extract>();
        auto expr = toExpr(extract->expression());

        switch (extract->field()) {
          case Extract::Field::kYear:
            return lp::Call("year", expr);
          case Extract::Field::kQuarter:
            return lp::Call("quarter", expr);
          case Extract::Field::kMonth:
            return lp::Call("month", expr);
          case Extract::Field::kWeek:
            return lp::Call("week", expr);
          case Extract::Field::kDay:
            [[fallthrough]];
          case Extract::Field::kDayOfMonth:
            return lp::Call("day", expr);
          case Extract::Field::kDow:
            [[fallthrough]];
          case Extract::Field::kDayOfWeek:
            return lp::Call("day_of_week", expr);
          case Extract::Field::kDoy:
            [[fallthrough]];
          case Extract::Field::kDayOfYear:
            return lp::Call("day_of_year", expr);
          case Extract::Field::kYow:
            [[fallthrough]];
          case Extract::Field::kYearOfWeek:
            return lp::Call("year_of_week", expr);
          case Extract::Field::kHour:
            return lp::Call("hour", expr);
          case Extract::Field::kMinute:
            return lp::Call("minute", expr);
          case Extract::Field::kSecond:
            return lp::Call("second", expr);
          case Extract::Field::kTimezoneHour:
            return lp::Call("timezone_hour", expr);
          case Extract::Field::kTimezoneMinute:
            return lp::Call("timezone_minute", expr);
        }
      }

      case NodeType::kNullLiteral:
        return lp::Lit(Variant::null(TypeKind::UNKNOWN));

      case NodeType::kBooleanLiteral:
        return lp::Lit(node->as<BooleanLiteral>()->value());

      case NodeType::kLongLiteral:
        return lp::Lit(node->as<LongLiteral>()->value());

      case NodeType::kDoubleLiteral:
        return lp::Lit(node->as<DoubleLiteral>()->value());

      case NodeType::kDecimalLiteral:
        return parseDecimal(node->as<DecimalLiteral>()->value());

      case NodeType::kStringLiteral:
        return lp::Lit(node->as<StringLiteral>()->value());

      case NodeType::kIntervalLiteral: {
        const auto interval = node->as<IntervalLiteral>();
        const int32_t multiplier =
            interval->sign() == IntervalLiteral::Sign::kPositive ? 1 : -1;

        if (interval->isYearToMonth()) {
          const auto months = parseYearMonthInterval(
              interval->value(), interval->startField(), interval->endField());
          return lp::Lit(multiplier * months, INTERVAL_YEAR_MONTH());
        } else {
          const auto seconds = parseDayTimeInterval(
              interval->value(), interval->startField(), interval->endField());
          return lp::Lit(multiplier * seconds, INTERVAL_DAY_TIME());
        }
      }

      case NodeType::kGenericLiteral: {
        auto literal = node->as<GenericLiteral>();
        return lp::Cast(
            parseType(literal->valueType()), lp::Lit(literal->value()));
      }

      case NodeType::kArrayConstructor: {
        auto* array = node->as<ArrayConstructor>();
        std::vector<lp::ExprApi> values;
        for (const auto& value : array->values()) {
          values.emplace_back(toExpr(value));
        }

        return lp::Call("array_constructor", values);
      }

      case NodeType::kFunctionCall: {
        auto* call = node->as<FunctionCall>();

        std::vector<lp::ExprApi> args;
        for (const auto& arg : call->arguments()) {
          args.push_back(toExpr(arg));
        }
        return lp::Call(call->name()->suffix(), args);
      }

      default:
        VELOX_NYI(
            "Unsupported expression type: {}",
            NodeTypeName::toName(node->type()));
    }
  }

 private:
  static std::string toFunctionName(ComparisonExpression::Operator op) {
    switch (op) {
      case ComparisonExpression::Operator::kEqual:
        return "eq";
      case ComparisonExpression::Operator::kNotEqual:
        return "neq";
      case ComparisonExpression::Operator::kLessThan:
        return "lt";
      case ComparisonExpression::Operator::kLessThanOrEqual:
        return "lte";
      case ComparisonExpression::Operator::kGreaterThan:
        return "gt";
      case ComparisonExpression::Operator::kGreaterThanOrEqual:
        return "gte";
      case ComparisonExpression::Operator::kIsDistinctFrom:
        VELOX_NYI("Not yet supported comparison operator: is_distinct_from");
    }

    folly::assume_unreachable();
  }

  static std::string toFunctionName(ArithmeticBinaryExpression::Operator op) {
    switch (op) {
      case ArithmeticBinaryExpression::Operator::kAdd:
        return "plus";
      case ArithmeticBinaryExpression::Operator::kSubtract:
        return "minus";
      case ArithmeticBinaryExpression::Operator::kMultiply:
        return "multiply";
      case ArithmeticBinaryExpression::Operator::kDivide:
        return "divide";
      case ArithmeticBinaryExpression::Operator::kModulus:
        return "mod";
    }

    folly::assume_unreachable();
  }

  static int32_t parseYearMonthInterval(
      const std::string& value,
      IntervalLiteral::IntervalField start,
      std::optional<IntervalLiteral::IntervalField> end) {
    VELOX_USER_CHECK(
        !end.has_value() || start == end.value(),
        "Multi-part intervals are not supported yet: {}",
        value);

    if (value.empty()) {
      return 0;
    }

    const auto n = atoi(value.c_str());

    switch (start) {
      case IntervalLiteral::IntervalField::kYear:
        return n * 12;
      case IntervalLiteral::IntervalField::kMonth:
        return n;
      default:
        VELOX_UNREACHABLE();
    }
  }

  static int64_t parseDayTimeInterval(
      const std::string& value,
      IntervalLiteral::IntervalField start,
      std::optional<IntervalLiteral::IntervalField> end) {
    VELOX_USER_CHECK(
        !end.has_value() || start == end.value(),
        "Multi-part intervals are not supported yet: {}",
        value);

    if (value.empty()) {
      return 0;
    }

    auto n = atol(value.c_str());

    switch (start) {
      case IntervalLiteral::IntervalField::kDay:
        return n * 24 * 60 * 60;
      case IntervalLiteral::IntervalField::kHour:
        return n * 60 * 60;
      case IntervalLiteral::IntervalField::kMinute:
        return n * 60;
      case IntervalLiteral::IntervalField::kSecond:
        return n;
      default:
        VELOX_UNREACHABLE();
    }
  }

  static lp::ExprApi parseDecimal(std::string_view value) {
    VELOX_USER_CHECK(!value.empty(), "Invalid decimal value: '{}'", value);

    size_t startPos = 0;
    if (value.at(0) == '+' || value.at(0) == '-') {
      startPos = 1;
    }

    int32_t periodPos = -1;
    int32_t firstNonZeroPos = -1;

    for (auto i = startPos; i < value.size(); ++i) {
      if (value.at(i) == '.') {
        VELOX_USER_CHECK_EQ(
            periodPos, -1, "Invalid decimal value: '{}'", value);
        periodPos = i;
      } else {
        VELOX_USER_CHECK(
            std::isdigit(value.at(i)), "Invalid decimal value: '{}'", value);

        if (firstNonZeroPos == -1 && value.at(i) != '0') {
          firstNonZeroPos = i;
        }
      }
    }

    size_t precision;
    size_t scale;
    std::string unscaledValue;

    if (periodPos == -1) {
      if (firstNonZeroPos == -1) {
        // All zeros: 000000. Treat as 0.
        precision = 1;
      } else {
        precision = value.size() - firstNonZeroPos;
      }

      scale = 0;
      unscaledValue = value;
    } else {
      scale = value.size() - periodPos - 1;

      if (firstNonZeroPos == -1 || firstNonZeroPos > periodPos) {
        // All zeros before decimal point. Treat as .0123.
        precision = scale > 0 ? scale : 1;
      } else {
        precision = value.size() - firstNonZeroPos - 1;
      }

      unscaledValue = fmt::format(
          "{}{}", value.substr(0, periodPos), value.substr(periodPos + 1));
    }

    if (precision <= facebook::velox::ShortDecimalType::kMaxPrecision) {
      int64_t v = atol(unscaledValue.c_str());
      return lp::Lit(v, DECIMAL(precision, scale));
    }

    if (precision <= facebook::velox::LongDecimalType::kMaxPrecision) {
      return lp::Lit(
          folly::to<int128_t>(unscaledValue), DECIMAL(precision, scale));
    }

    VELOX_USER_FAIL(
        "Invalid decimal value: '{}'. Precision exceeds maximum: {} > {}.",
        value,
        precision,
        facebook::velox::LongDecimalType::kMaxPrecision);
  }

  static int32_t parseInt(const TypeSignaturePtr& type) {
    VELOX_USER_CHECK_EQ(type->parameters().size(), 0);
    return atoi(type->baseName().c_str());
  }

  static TypePtr parseType(const TypeSignaturePtr& type) {
    auto baseName = type->baseName();
    std::transform(
        baseName.begin(), baseName.end(), baseName.begin(), [](char c) {
          return (std::toupper(c));
        });

    if (baseName == "INT") {
      baseName = "INTEGER";
    }

    std::vector<TypeParameter> parameters;
    if (!type->parameters().empty()) {
      const auto numParams = type->parameters().size();
      parameters.reserve(numParams);

      if (baseName == "ARRAY") {
        VELOX_USER_CHECK_EQ(1, numParams);
        parameters.emplace_back(parseType(type->parameters().at(0)));
      } else if (baseName == "MAP") {
        VELOX_USER_CHECK_EQ(2, numParams);
        parameters.emplace_back(parseType(type->parameters().at(0)));
        parameters.emplace_back(parseType(type->parameters().at(1)));
      } else if (baseName == "ROW") {
        for (const auto& param : type->parameters()) {
          parameters.emplace_back(parseType(param), param->rowFieldName());
        }
      } else if (baseName == "DECIMAL") {
        VELOX_USER_CHECK_EQ(2, numParams);
        parameters.emplace_back(parseInt(type->parameters().at(0)));
        parameters.emplace_back(parseInt(type->parameters().at(1)));

      } else {
        VELOX_USER_FAIL("Unknown parametric type: {}", baseName);
      }
    }

    auto veloxType = getType(baseName, parameters);

    VELOX_CHECK_NOT_NULL(veloxType, "Cannot resolve type: {}", baseName);
    return veloxType;
  }

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
        columnNames.emplace_back(name->value());
      }

      builder_->unnest(
          inputs,
          unnest.isWithOrdinality(),
          aliasedRelation->alias()->value(),
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

    if (relation->is(NodeType::kTable)) {
      auto* table = relation->as<Table>();

      auto withIt = withQueries_.find(table->name()->suffix());
      if (withIt != withQueries_.end()) {
        // TODO Change WithQuery to store Query and not Statement.
        processQuery(dynamic_cast<Query*>(withIt->second->query().get()));
        return;
      }

      builder_->tableScan(table->name()->suffix());
      builder_->as(table->name()->suffix());
      return;
    }

    if (relation->is(NodeType::kAliasedRelation)) {
      auto* aliasedRelation = relation->as<AliasedRelation>();

      processFrom(aliasedRelation->relation());

      const auto& columnAliases = aliasedRelation->columnNames();
      if (!columnAliases.empty()) {
        // Add projection to rename columns.
        const size_t numColumns = columnAliases.size();

        std::vector<lp::ExprApi> renames;
        renames.reserve(numColumns);
        for (auto i = 0; i < numColumns; ++i) {
          renames.push_back(
              lp::Col(builder_->findOrAssignOutputNameAt(i))
                  .as(columnAliases.at(i)->value()));
        }

        builder_->project(renames);
      }

      builder_->as(aliasedRelation->alias()->value());
      return;
    }

    if (relation->is(NodeType::kTableSubquery)) {
      auto* subquery = relation->as<TableSubquery>();
      auto query = subquery->query();

      if (query->is(NodeType::kQuery)) {
        processQuery(query->as<Query>());
        return;
      }

      VELOX_NYI(
          "Subquery type is not supported yet: {}",
          NodeTypeName::toName(query->type()));
    }

    if (relation->is(NodeType::kUnnest)) {
      auto* unnest = relation->as<Unnest>();
      std::vector<lp::ExprApi> inputs;
      for (const auto& expr : unnest->expressions()) {
        inputs.push_back(toExpr(expr));
      }

      builder_->unnest(inputs, unnest->isWithOrdinality());
      return;
    }

    if (relation->is(NodeType::kJoin)) {
      auto* join = relation->as<Join>();
      processFrom(join->left());

      if (auto unnest = tryGetUnnest(join->right())) {
        addCrossJoinUnnest(*unnest->first, unnest->second);
        return;
      }

      auto leftBuilder = builder_;

      lp::PlanBuilder::Scope scope;
      leftBuilder->captureScope(scope);

      builder_ = newBuilder(scope);
      processFrom(join->right());
      auto rightBuilder = builder_;

      builder_ = leftBuilder;

      std::optional<lp::ExprApi> condition;

      if (const auto& criteria = join->criteria()) {
        if (criteria->is(NodeType::kJoinOn)) {
          condition = toExpr(criteria->as<JoinOn>()->expression());
        } else {
          VELOX_NYI(
              "Join criteria type is not supported yet: {}",
              NodeTypeName::toName(criteria->type()));
        }
      }

      builder_->join(*rightBuilder, condition, toJoinType(join->joinType()));
      return;
    }

    VELOX_NYI(
        "Relation type is not supported yet: {}",
        NodeTypeName::toName(relation->type()));
  }

  // Returns true if 'selectItems' contains a single SELECT *.
  static bool isSelectAll(const std::vector<SelectItemPtr>& selectItems) {
    if (selectItems.size() == 1 &&
        selectItems.at(0)->is(NodeType::kAllColumns)) {
      return true;
    }

    return false;
  }

  void addProject(const std::vector<SelectItemPtr>& selectItems) {
    std::vector<lp::ExprApi> exprs;
    for (const auto& item : selectItems) {
      VELOX_CHECK(item->is(NodeType::kSingleColumn));
      auto* singleColumn = item->as<SingleColumn>();

      lp::ExprApi expr = toExpr(singleColumn->expression());

      if (singleColumn->alias() != nullptr) {
        expr = expr.as(singleColumn->alias()->value());
      }
      exprs.push_back(expr);
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

  bool tryAddGlobalAgg(const std::vector<SelectItemPtr>& selectItems) {
    bool hasAggregate = false;
    for (const auto& item : selectItems) {
      VELOX_CHECK(item->is(NodeType::kSingleColumn));
      auto* singleColumn = item->as<SingleColumn>();

      ExprAnalyzer exprAnalyzer;
      singleColumn->expression()->accept(&exprAnalyzer);

      if (exprAnalyzer.hasAggregate()) {
        hasAggregate = true;
        break;
      }
    }

    if (!hasAggregate) {
      return false;
    }

    addGroupBy(selectItems, {}, nullptr);
    return true;
  }

  void addGroupBy(
      const std::vector<SelectItemPtr>& selectItems,
      const std::vector<GroupingElementPtr>& groupingElements,
      const ExpressionPtr& having) {
    // Go over grouping keys and collect expressions. Ordinals refer to output
    // columns (selectItems). Non-ordinals refer to input columns.

    std::vector<lp::ExprApi> groupingKeys;

    for (const auto& groupingElement : groupingElements) {
      VELOX_CHECK_EQ(groupingElement->type(), NodeType::kSimpleGroupBy);
      const auto* simple = groupingElement->as<SimpleGroupBy>();

      for (const auto& expr : simple->expressions()) {
        if (expr->is(NodeType::kLongLiteral)) {
          // 1-based index.
          const auto n = expr->as<LongLiteral>()->value();

          VELOX_CHECK_GE(n, 1);
          VELOX_CHECK_LE(n, selectItems.size());

          const auto& item = selectItems.at(n - 1);
          VELOX_CHECK(item->is(NodeType::kSingleColumn));

          const auto* singleColumn = item->as<SingleColumn>();
          groupingKeys.emplace_back(toExpr(singleColumn->expression()));
        } else {
          groupingKeys.emplace_back(toExpr(expr));
        }
      }
    }

    // Go over SELECT expressions and figure out for each: whether a grouping
    // key, a function of one or more grouping keys, a constant, an aggregate or
    // a function over one or more aggregates and possibly grouping keys.
    //
    // Collect all individual aggregates. A single select item 'sum(x) / sum(y)'
    // will produce 2 aggregates: sum(x), sum(y).

    std::vector<lp::ExprApi> projections;
    std::vector<lp::ExprApi> aggregates;
    for (const auto& item : selectItems) {
      VELOX_CHECK(item->is(NodeType::kSingleColumn));
      auto* singleColumn = item->as<SingleColumn>();

      lp::ExprApi expr = toExpr(singleColumn->expression());
      findAggregates(expr.expr(), aggregates);

      if (!aggregates.empty() &&
          aggregates.back().expr().get() == expr.expr().get()) {
        // Preserve the alias.
        if (singleColumn->alias() != nullptr) {
          aggregates.back() =
              aggregates.back().as(singleColumn->alias()->value());
        }
      }

      if (singleColumn->alias() != nullptr) {
        expr = expr.as(singleColumn->alias()->value());
      }

      projections.emplace_back(expr);
    }

    std::optional<lp::ExprApi> filter;
    if (having != nullptr) {
      lp::ExprApi expr = toExpr(having);
      findAggregates(expr.expr(), aggregates);
      filter = expr;
    }

    std::vector<lp::PlanBuilder::AggregateOptions> options(aggregates.size());
    builder_->aggregate(groupingKeys, aggregates, options);

    const auto outputNames = builder_->findOrAssignOutputNames();

    ExprMap inputs;
    std::vector<core::ExprPtr> flatInputs;

    size_t index = 0;
    for (const auto& key : groupingKeys) {
      flatInputs.emplace_back(lp::Col(outputNames.at(index)).expr());
      inputs.emplace(key.expr(), flatInputs.back());
      ++index;
    }

    for (const auto& agg : aggregates) {
      flatInputs.emplace_back(lp::Col(outputNames.at(index)).expr());
      inputs.emplace(agg.expr(), flatInputs.back());
      ++index;
    }

    if (filter.has_value()) {
      filter = replaceInputs(filter.value().expr(), inputs);
      builder_->filter(filter.value());
    }

    // Go over SELECT expressions and replace sub-expressions matching 'inputs'
    // with column references.

    // TODO Verify that SELECT expressions doesn't depend on anything other than
    // grouping keys and aggregates.

    for (auto i = 0; i < projections.size(); ++i) {
      auto& item = projections.at(i);
      auto newExpr = replaceInputs(item.expr(), inputs);

      item = lp::ExprApi(newExpr, item.name());
    }

    bool identityProjection = (flatInputs.size() == projections.size());
    if (identityProjection) {
      for (auto i = 0; i < projections.size(); ++i) {
        if (i < flatInputs.size()) {
          if (projections.at(i).expr() != flatInputs.at(i)) {
            identityProjection = false;
            break;
          }

          const auto& alias = projections.at(i).alias();
          if (alias.has_value() && alias.value() != outputNames.at(i)) {
            identityProjection = false;
            break;
          }
        }
      }
    }

    if (!identityProjection) {
      builder_->project(projections);
    }
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
    if (const auto& with = query->with()) {
      for (const auto& query : with->queries()) {
        withQueries_.emplace(query->name()->value(), query);
      }
    }

    query->queryBody()->accept(this);

    addOrderBy(query->orderBy());
    addOffset(query->offset());
    addLimit(query->limit());
  }

  void visitQuery(Query* query) override {
    processQuery(query);
  }

  void visitQuerySpecification(QuerySpecification* node) override {
    // FROM t -> builder.tableScan(t)
    processFrom(node->from());

    // WHERE a > 1 -> builder.filter("a > 1")
    addFilter(node->where());

    const auto& selectItems = node->select()->selectItems();

    if (auto groupBy = node->groupBy()) {
      VELOX_USER_CHECK(
          !groupBy->isDistinct(),
          "GROUP BY with DISTINCT is not supported yet");
      addGroupBy(selectItems, groupBy->groupingElements(), node->having());
    } else {
      if (isSelectAll(selectItems)) {
        // SELECT *. No project needed.
      } else if (tryAddGlobalAgg(selectItems)) {
        // Nothing else to do.
      } else {
        // SELECT a, b -> builder.project({a, b})
        addProject(selectItems);
      }
    }

    if (node->select()->isDistinct()) {
      builder_->aggregate(builder_->findOrAssignOutputNames(), {});
    }
  }

  void visitUnion(Union* node) override {
    node->left()->accept(this);

    auto leftBuilder = builder_;

    lp::PlanBuilder::Scope scope;
    leftBuilder->captureScope(scope);

    builder_ = newBuilder(scope);
    node->right()->accept(this);
    auto rightBuilder = builder_;

    builder_ = leftBuilder;
    builder_->unionAll(*rightBuilder);

    if (node->isDistinct()) {
      builder_->aggregate(builder_->findOrAssignOutputNames(), {});
    }
  }

  std::shared_ptr<lp::PlanBuilder> newBuilder(
      const lp::PlanBuilder::Scope& outerScope = nullptr) {
    return std::make_shared<lp::PlanBuilder>(
        context_, /* enableCoersions */ true, outerScope);
  }

  lp::PlanBuilder::Context context_;
  std::shared_ptr<lp::PlanBuilder> builder_;
  std::unordered_map<std::string, std::shared_ptr<WithQuery>> withQueries_;
};

} // namespace

SqlStatementPtr PrestoParser::parse(std::string_view sql, bool enableTracing) {
  return doParse(sql, enableTracing);
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
  RelationPlanner planner("__unused__");

  auto plan =
      lp::PlanBuilder()
          .values(facebook::velox::ROW({}), {facebook::velox::Variant::row({})})
          .project({planner.toExpr(expr)})
          .build();
  VELOX_USER_CHECK(plan->is(lp::NodeKind::kProject));

  auto project = plan->as<lp::ProjectNode>();
  VELOX_CHECK_NOT_NULL(project);

  VELOX_USER_CHECK_EQ(1, project->expressions().size());

  return project->expressionAt(0);
}

SqlStatementPtr parseExplain(
    const Explain& explain,
    const std::string& connectorId) {
  RelationPlanner planner(connectorId);
  explain.statement()->accept(&planner);

  if (explain.isAnalyze()) {
    return std::make_shared<ExplainStatement>(
        std::make_shared<SelectStatement>(planner.getPlan()),
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
      std::make_shared<SelectStatement>(planner.getPlan()),
      /*analyze=*/false,
      type);
}

SqlStatementPtr parseShowColumns(
    const ShowColumns& showColumns,
    const std::string& connectorId) {
  const auto tableName = showColumns.table()->suffix();

  auto table =
      facebook::axiom::connector::ConnectorMetadata::metadata(connectorId)
          ->findTable(tableName);

  VELOX_USER_CHECK_NOT_NULL(table, "Table not found: {}", tableName);

  const auto& schema = table->type();

  std::vector<Variant> data;
  data.reserve(schema->size());
  for (auto i = 0; i < schema->size(); ++i) {
    data.emplace_back(
        Variant::row({schema->nameOf(i), schema->childAt(i)->toString()}));
  }

  lp::PlanBuilder::Context ctx(connectorId);
  return std::make_shared<SelectStatement>(
      lp::PlanBuilder(ctx)
          .values(ROW({"column", "type"}, {VARCHAR(), VARCHAR()}), data)
          .build());
}

SqlStatementPtr parseInsert(
    const Insert& insert,
    const std::string& connectorId) {
  auto tableName = insert.target()->suffix();

  auto table =
      facebook::axiom::connector::ConnectorMetadata::metadata(connectorId)
          ->findTable(tableName);
  VELOX_USER_CHECK_NOT_NULL(table, "Table not found: {}", tableName);

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

  RelationPlanner planner(connectorId);
  insert.query()->accept(&planner);

  auto inputColumns = planner.builder().findOrAssignOutputNames();
  VELOX_CHECK_EQ(inputColumns.size(), columnNames.size());

  planner.builder().tableWrite(
      connectorId,
      tableName,
      lp::WriteKind::kInsert,
      columnNames,
      inputColumns);

  return std::make_shared<InsertStatement>(planner.getPlan());
}

SqlStatementPtr parseCreateTableAsSelect(
    const CreateTableAsSelect& ctas,
    const std::string& connectorId) {
  auto tableName = ctas.name()->suffix();

  RelationPlanner planner(connectorId);
  ctas.query()->accept(&planner);

  std::unordered_map<std::string, lp::ExprPtr> properties;
  for (const auto& p : ctas.properties()) {
    const auto& name = p->name()->value();
    bool ok = properties.emplace(name, parseSqlExpression(p->value())).second;
    VELOX_USER_CHECK(ok, "Duplicate property: {}", name);
  }

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
        tableName,
        lp::WriteKind::kCreate,
        columnNames,
        columnNames);
  } else {
    VELOX_USER_CHECK_EQ(ctas.columns().size(), numInputColumns);

    columnNames.reserve(numInputColumns);
    for (const auto& column : ctas.columns()) {
      columnNames.emplace_back(column->value());
    }

    planBuilder.tableWrite(
        connectorId,
        tableName,
        lp::WriteKind::kCreate,
        columnNames,
        planBuilder.findOrAssignOutputNames());
  }

  return std::make_shared<CreateTableAsSelectStatement>(
      std::move(tableName),
      facebook::velox::ROW(std::move(columnNames), std::move(columnTypes)),
      std::move(properties),
      planner.getPlan());
}

SqlStatementPtr parseDropTable(
    const DropTable& dropTable,
    const std::string& connectorId) {
  auto tableName = dropTable.tableName()->suffix();

  return std::make_shared<DropTableStatement>(
      std::move(tableName), dropTable.isExists());
}

} // namespace

SqlStatementPtr PrestoParser::doParse(
    std::string_view sql,
    bool enableTracing) {
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

  if (query->is(NodeType::kExplain)) {
    return parseExplain(*query->as<Explain>(), defaultConnectorId_);
  }

  if (query->is(NodeType::kShowColumns)) {
    return parseShowColumns(*query->as<ShowColumns>(), defaultConnectorId_);
  }

  if (query->is(NodeType::kInsert)) {
    return parseInsert(*query->as<Insert>(), defaultConnectorId_);
  }

  if (query->is(NodeType::kCreateTableAsSelect)) {
    return parseCreateTableAsSelect(
        *query->as<CreateTableAsSelect>(), defaultConnectorId_);
  }

  if (query->is(NodeType::kDropTable)) {
    return parseDropTable(*query->as<DropTable>(), defaultConnectorId_);
  }

  RelationPlanner planner(defaultConnectorId_);
  query->accept(&planner);
  return std::make_shared<SelectStatement>(planner.getPlan());
}

} // namespace axiom::sql::presto
