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

#include "axiom/optimizer/tests/PrestoParser.h"
#include <algorithm>
#include <cctype>
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/connectors/ConnectorMetadata.h"
#include "axiom/sql/presto/ParserHelper.h"
#include "axiom/sql/presto/ast/AstBuilder.h"
#include "axiom/sql/presto/ast/AstPrinter.h"
#include "velox/exec/Aggregate.h"

namespace lp = facebook::velox::logical_plan;
namespace sql = axiom::sql::presto;

namespace facebook::velox::optimizer::test {
namespace {

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
      if (velox::exec::getAggregateFunctionEntry(
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
    const sql::ExpressionPtr& expr,
    std::vector<std::string>& names) {
  if (expr->is(sql::NodeType::kIdentifier)) {
    names.push_back(expr->as<sql::Identifier>()->value());
    return true;
  }

  if (expr->is(sql::NodeType::kDereferenceExpression)) {
    auto* dereference = expr->as<sql::DereferenceExpression>();
    names.push_back(dereference->field()->value());
    return asQualifiedName(dereference->base(), names);
  }

  return false;
}

// Analizes the expression to find out whether there are any aggregate function
// calls and to verify that aggregate calls are not nested, e.g. sum(count(x))
// is not allowed.
class ExprAnalyzer : public sql::AstVisitor {
 public:
  bool hasAggregate() const {
    return numAggregates_ > 0;
  }

 private:
  void defaultVisit(sql::Node* node) override {
    if (dynamic_cast<sql::Literal*>(node) != nullptr) {
      // Literals have no function calls.
      return;
    }

    VELOX_NYI(
        "Not yet supported node type: {}",
        sql::NodeTypeName::toName(node->type()));
  }

  void visitCast(sql::Cast* node) override {
    node->expression()->accept(this);
  }

  void visitDereferenceExpression(sql::DereferenceExpression* node) override {
    node->base()->accept(this);
  }

  void visitExtract(sql::Extract* node) override {
    node->expression()->accept(this);
  }

  void visitFunctionCall(sql::FunctionCall* node) override {
    const auto& name = node->name()->suffix();
    if (velox::exec::getAggregateFunctionEntry(name)) {
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
      sql::ArithmeticBinaryExpression* node) override {
    node->left()->accept(this);
    node->right()->accept(this);
  }

  void visitLogicalBinaryExpression(
      sql::LogicalBinaryExpression* node) override {
    node->left()->accept(this);
    node->right()->accept(this);
  }

  void visitComparisonExpression(sql::ComparisonExpression* node) override {
    node->left()->accept(this);
    node->right()->accept(this);
  }

  void visitLikePredicate(sql::LikePredicate* node) override {
    node->value()->accept(this);
    node->pattern()->accept(this);
    if (node->escape() != nullptr) {
      node->escape()->accept(this);
    }
  }

  void visitSearchedCaseExpression(sql::SearchedCaseExpression* node) override {
    for (const auto& clause : node->whenClauses()) {
      clause->operand()->accept(this);
      clause->result()->accept(this);
    }

    if (node->defaultValue()) {
      node->defaultValue()->accept(this);
    }
  }

  void visitIdentifier(sql::Identifier* node) override {
    // No function calls.
  }

  size_t numAggregates_{0};
  std::optional<std::string> aggregateName_;
};

class RelationPlanner : public sql::AstVisitor {
 public:
  explicit RelationPlanner(const std::string& defaultConnectorId)
      : context_{defaultConnectorId}, builder_(newBuilder()) {}

  lp::LogicalPlanNodePtr getPlan() {
    return builder_->build();
  }

 private:
  static std::string toFunctionName(sql::ComparisonExpression::Operator op) {
    switch (op) {
      case sql::ComparisonExpression::Operator::kEqual:
        return "eq";
      case sql::ComparisonExpression::Operator::kNotEqual:
        return "neq";
      case sql::ComparisonExpression::Operator::kLessThan:
        return "lt";
      case sql::ComparisonExpression::Operator::kLessThanOrEqual:
        return "lte";
      case sql::ComparisonExpression::Operator::kGreaterThan:
        return "gt";
      case sql::ComparisonExpression::Operator::kGreaterThanOrEqual:
        return "gte";
      case sql::ComparisonExpression::Operator::kIsDistinctFrom:
        VELOX_NYI("Not yet supported comparison operator: is_distinct_from");
    }

    folly::assume_unreachable();
  }

  static std::string toFunctionName(
      sql::ArithmeticBinaryExpression::Operator op) {
    switch (op) {
      case sql::ArithmeticBinaryExpression::Operator::kAdd:
        return "plus";
      case sql::ArithmeticBinaryExpression::Operator::kSubtract:
        return "minus";
      case sql::ArithmeticBinaryExpression::Operator::kMultiply:
        return "multiply";
      case sql::ArithmeticBinaryExpression::Operator::kDivide:
        return "divide";
      case sql::ArithmeticBinaryExpression::Operator::kModulus:
        return "modulus";
    }

    folly::assume_unreachable();
  }

  static int32_t parseYearMonthInterval(
      const std::string& value,
      sql::IntervalLiteral::IntervalField start,
      std::optional<sql::IntervalLiteral::IntervalField> end) {
    VELOX_USER_CHECK(
        !end.has_value() || start == end.value(),
        "Multi-part intervals are not supported yet: {}",
        value);

    if (value.empty()) {
      return 0;
    }

    const auto n = atoi(value.c_str());

    switch (start) {
      case sql::IntervalLiteral::IntervalField::kYear:
        return n * 12;
      case sql::IntervalLiteral::IntervalField::kMonth:
        return n;
      default:
        VELOX_UNREACHABLE();
    }
  }

  static int64_t parseDayTimeInterval(
      const std::string& value,
      sql::IntervalLiteral::IntervalField start,
      std::optional<sql::IntervalLiteral::IntervalField> end) {
    VELOX_USER_CHECK(
        !end.has_value() || start == end.value(),
        "Multi-part intervals are not supported yet: {}",
        value);

    if (value.empty()) {
      return 0;
    }

    auto n = atol(value.c_str());

    switch (start) {
      case sql::IntervalLiteral::IntervalField::kDay:
        return n * 24 * 60 * 60;
      case sql::IntervalLiteral::IntervalField::kHour:
        return n * 60 * 60;
      case sql::IntervalLiteral::IntervalField::kMinute:
        return n * 60;
      case sql::IntervalLiteral::IntervalField::kSecond:
        return n;
      default:
        VELOX_UNREACHABLE();
    }
  }

  static lp::ExprApi parseDecimal(const std::string& value) {
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

      unscaledValue = value.substr(0, periodPos) + value.substr(periodPos + 1);
    }

    if (precision <= velox::ShortDecimalType::kMaxPrecision) {
      int64_t v = atol(unscaledValue.c_str());
      return lp::Lit(v, DECIMAL(precision, scale));
    }

    if (precision <= velox::LongDecimalType::kMaxPrecision) {
      return lp::Lit(
          folly::to<int128_t>(unscaledValue), DECIMAL(precision, scale));
    }

    VELOX_USER_FAIL(
        "Invalid decimal value: '{}'. Precision exceeds maximum: {} > {}.",
        value,
        precision,
        velox::LongDecimalType::kMaxPrecision);
  }

  static int32_t parseInt(const sql::TypeSignaturePtr& type) {
    VELOX_USER_CHECK_EQ(type->parameters().size(), 0);
    return atoi(type->baseName().c_str());
  }

  static TypePtr parseType(const sql::TypeSignaturePtr& type) {
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

  lp::ExprApi toExpr(const sql::ExpressionPtr& node) {
    switch (node->type()) {
      case sql::NodeType::kIdentifier:
        return lp::Col(node->as<sql::Identifier>()->value());

      case sql::NodeType::kDereferenceExpression: {
        std::vector<std::string> names;
        if (asQualifiedName(node, names)) {
          VELOX_USER_CHECK_EQ(2, names.size());
          return lp::Col(names.at(0), lp::Col(names.at(1)));
        }

        auto* dereference = node->as<sql::DereferenceExpression>();
        return lp::Col(
            dereference->field()->value(), toExpr(dereference->base()));
      }

      case sql::NodeType::kSubqueryExpression: {
        auto* subquery = node->as<sql::SubqueryExpression>();
        auto query = subquery->query();

        if (query->is(sql::NodeType::kQuery)) {
          auto builder = std::move(builder_);

          builder_ = newBuilder();
          processQuery(query->as<sql::Query>());
          auto subqueryBuider = builder_;

          builder_ = std::move(builder);
          return lp::Subquery(subqueryBuider->build());
        }

        VELOX_NYI(
            "Subquery type is not supported yet: {}",
            sql::NodeTypeName::toName(query->type()));
      }

      case sql::NodeType::kComparisonExpression: {
        auto* comparison = node->as<sql::ComparisonExpression>();
        return lp::Call(
            toFunctionName(comparison->op()),
            toExpr(comparison->left()),
            toExpr(comparison->right()));
      }

      case sql::NodeType::kNotExpression: {
        auto* negation = node->as<sql::NotExpression>();
        return lp::Call("not", toExpr(negation->value()));
      }

      case sql::NodeType::kLikePredicate: {
        auto* like = node->as<sql::LikePredicate>();

        std::vector<lp::ExprApi> inputs;
        inputs.emplace_back(toExpr(like->value()));
        inputs.emplace_back(toExpr(like->pattern()));
        if (like->escape()) {
          inputs.emplace_back(toExpr(like->escape()));
        }

        return lp::Call("like", std::move(inputs));
      }

      case sql::NodeType::kLogicalBinaryExpression: {
        auto* logical = node->as<sql::LogicalBinaryExpression>();
        auto left = toExpr(logical->left());
        auto right = toExpr(logical->right());

        switch (logical->op()) {
          case sql::LogicalBinaryExpression::Operator::kAnd:
            return left && right;

          case sql::LogicalBinaryExpression::Operator::kOr:
            return left || right;
        }
      }

      case sql::NodeType::kArithmeticBinaryExpression: {
        auto* binary = node->as<sql::ArithmeticBinaryExpression>();
        return lp::Call(
            toFunctionName(binary->op()),
            toExpr(binary->left()),
            toExpr(binary->right()));
      }

      case sql::NodeType::kBetweenPredicate: {
        auto* between = node->as<sql::BetweenPredicate>();
        return lp::Call(
            "between",
            toExpr(between->value()),
            toExpr(between->min()),
            toExpr(between->max()));
      }

      case sql::NodeType::kInPredicate: {
        auto* inPredicate = node->as<sql::InPredicate>();
        const auto& valueList = inPredicate->valueList();

        const auto value = toExpr(inPredicate->value());

        if (valueList->is(sql::NodeType::kInListExpression)) {
          auto inList = valueList->as<sql::InListExpression>();

          std::vector<lp::ExprApi> inputs;
          inputs.reserve(1 + inList->values().size());

          inputs.emplace_back(value);
          for (const auto& expr : inList->values()) {
            inputs.emplace_back(toExpr(expr));
          }

          return lp::Call("in", inputs);
        }

        if (valueList->is(sql::NodeType::kSubqueryExpression)) {
          return lp::Call("in", value, toExpr(valueList));
        }

        VELOX_USER_FAIL(
            "Unexpected IN predicate: {}",
            sql::NodeTypeName::toName(valueList->type()));
      }

      case sql::NodeType::kCast: {
        auto* cast = node->as<sql::Cast>();
        const auto type = parseType(cast->toType());

        if (cast->isSafe()) {
          return lp::TryCast(type, toExpr(cast->expression()));
        } else {
          return lp::Cast(type, toExpr(cast->expression()));
        }
      }

      case sql::NodeType::kSearchedCaseExpression: {
        auto* searchedCase = node->as<sql::SearchedCaseExpression>();

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

      case sql::NodeType::kExtract: {
        auto* extract = node->as<sql::Extract>();
        auto expr = toExpr(extract->expression());

        switch (extract->field()) {
          case sql::Extract::Field::kYear:
            return lp::Call("year", expr);
          case sql::Extract::Field::kQuarter:
            return lp::Call("quarter", expr);
          case sql::Extract::Field::kMonth:
            return lp::Call("month", expr);
          case sql::Extract::Field::kWeek:
            return lp::Call("week", expr);
          case sql::Extract::Field::kDay:
            [[fallthrough]];
          case sql::Extract::Field::kDayOfMonth:
            return lp::Call("day", expr);
          case sql::Extract::Field::kDow:
            [[fallthrough]];
          case sql::Extract::Field::kDayOfWeek:
            return lp::Call("day_of_week", expr);
          case sql::Extract::Field::kDoy:
            [[fallthrough]];
          case sql::Extract::Field::kDayOfYear:
            return lp::Call("day_of_year", expr);
          case sql::Extract::Field::kYow:
            [[fallthrough]];
          case sql::Extract::Field::kYearOfWeek:
            return lp::Call("year_of_week", expr);
          case sql::Extract::Field::kHour:
            return lp::Call("hour", expr);
          case sql::Extract::Field::kMinute:
            return lp::Call("minute", expr);
          case sql::Extract::Field::kSecond:
            return lp::Call("second", expr);
          case sql::Extract::Field::kTimezoneHour:
            return lp::Call("timezone_hour", expr);
          case sql::Extract::Field::kTimezoneMinute:
            return lp::Call("timezone_minute", expr);
        }
      }

      case sql::NodeType::kNullLiteral:
        return lp::Lit(Variant::null(TypeKind::UNKNOWN));

      case sql::NodeType::kLongLiteral:
        return lp::Lit(node->as<sql::LongLiteral>()->value());

      case sql::NodeType::kDoubleLiteral:
        return lp::Lit(node->as<sql::DoubleLiteral>()->value());

      case sql::NodeType::kDecimalLiteral:
        return parseDecimal(node->as<sql::DecimalLiteral>()->value());

      case sql::NodeType::kStringLiteral:
        return lp::Lit(node->as<sql::StringLiteral>()->value());

      case sql::NodeType::kIntervalLiteral: {
        const auto interval = node->as<sql::IntervalLiteral>();
        const int32_t multiplier =
            interval->sign() == sql::IntervalLiteral::Sign::kPositive ? 1 : -1;

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

      case sql::NodeType::kGenericLiteral: {
        auto literal = node->as<sql::GenericLiteral>();
        return lp::Cast(
            parseType(literal->valueType()), lp::Lit(literal->value()));
      }

      case sql::NodeType::kArrayConstructor: {
        auto* array = node->as<sql::ArrayConstructor>();
        std::vector<lp::ExprApi> values;
        for (const auto& value : array->values()) {
          values.emplace_back(toExpr(value));
        }

        return lp::Call("array_constructor", values);
      }

      case sql::NodeType::kFunctionCall: {
        auto* call = node->as<sql::FunctionCall>();

        std::vector<lp::ExprApi> args;
        for (const auto& arg : call->arguments()) {
          args.push_back(toExpr(arg));
        }
        return lp::Call(call->name()->suffix(), args);
      }

      default:
        VELOX_NYI(
            "Unsupported expression type: {}",
            sql::NodeTypeName::toName(node->type()));
    }
  } // namespace

  void addFilter(const sql::ExpressionPtr& filter) {
    if (filter != nullptr) {
      builder_->filter(toExpr(filter));
    }
  }

  static lp::JoinType toJoinType(sql::Join::Type type) {
    switch (type) {
      case sql::Join::Type::kCross:
        return lp::JoinType::kInner;
      case sql::Join::Type::kImplicit:
        return lp::JoinType::kInner;
      case sql::Join::Type::kInner:
        return lp::JoinType::kInner;
      case sql::Join::Type::kLeft:
        return lp::JoinType::kLeft;
      case sql::Join::Type::kRight:
        return lp::JoinType::kRight;
      case sql::Join::Type::kFull:
        return lp::JoinType::kFull;
    }

    folly::assume_unreachable();
  }

  static std::optional<
      std::pair<const sql::Unnest*, const sql::AliasedRelation*>>
  tryGetUnnest(const sql::RelationPtr& relation) {
    if (relation->is(sql::NodeType::kAliasedRelation)) {
      const auto* aliasedRelation = relation->as<sql::AliasedRelation>();
      if (aliasedRelation->relation()->is(sql::NodeType::kUnnest)) {
        return std::make_pair(
            aliasedRelation->relation()->as<sql::Unnest>(), aliasedRelation);
      }
      return std::nullopt;
    }

    if (relation->is(sql::NodeType::kUnnest)) {
      return std::make_pair(relation->as<sql::Unnest>(), nullptr);
    }

    return std::nullopt;
  }

  void addCrossJoinUnnest(
      const sql::Unnest& unnest,
      const sql::AliasedRelation* aliasedRelation) {
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

  void processFrom(const sql::RelationPtr& relation) {
    if (relation == nullptr) {
      // SELECT 1; type of query.
      builder_->values(ROW({}), {Variant::row({})});
      return;
    }

    if (relation->is(sql::NodeType::kTable)) {
      auto* table = relation->as<sql::Table>();
      builder_->tableScan(table->name()->suffix());
      return;
    }

    if (relation->is(sql::NodeType::kAliasedRelation)) {
      auto* aliasedRelation = relation->as<sql::AliasedRelation>();

      processFrom(aliasedRelation->relation());

      const auto& columnAliases = aliasedRelation->columnNames();
      if (!columnAliases.empty()) {
        // Add projection to rename columns.
        const size_t numColumns = columnAliases.size();

        std::vector<lp::ExprApi> renames;
        renames.reserve(numColumns);
        for (auto i = 0; i < numColumns; ++i) {
          renames.push_back(lp::Col(builder_->findOrAssignOutputNameAt(i))
                                .as(columnAliases.at(i)->value()));
        }

        builder_->project(renames);
      }

      builder_->as(aliasedRelation->alias()->value());
      return;
    }

    if (relation->is(sql::NodeType::kTableSubquery)) {
      auto* subquery = relation->as<sql::TableSubquery>();
      auto query = subquery->query();

      if (query->is(sql::NodeType::kQuery)) {
        processQuery(query->as<sql::Query>());
        return;
      }

      VELOX_NYI(
          "Subquery type is not supported yet: {}",
          sql::NodeTypeName::toName(query->type()));
    }

    if (relation->is(sql::NodeType::kUnnest)) {
      auto* unnest = relation->as<sql::Unnest>();
      std::vector<lp::ExprApi> inputs;
      for (const auto& expr : unnest->expressions()) {
        inputs.push_back(toExpr(expr));
      }

      builder_->unnest(inputs, unnest->isWithOrdinality());
      return;
    }

    if (relation->is(sql::NodeType::kJoin)) {
      auto* join = relation->as<sql::Join>();
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
        if (criteria->is(sql::NodeType::kJoinOn)) {
          condition = toExpr(criteria->as<sql::JoinOn>()->expression());
        } else {
          VELOX_NYI(
              "Join criteria type is not supported yet: {}",
              sql::NodeTypeName::toName(criteria->type()));
        }
      }

      builder_->join(*rightBuilder, condition, toJoinType(join->joinType()));
      return;
    }

    VELOX_NYI(
        "Relation type is not supported yet: {}",
        sql::NodeTypeName::toName(relation->type()));
  }

  // Returns true if 'selectItems' contains a single SELECT *.
  static bool isSelectAll(const std::vector<sql::SelectItemPtr>& selectItems) {
    if (selectItems.size() == 1 &&
        selectItems.at(0)->is(sql::NodeType::kAllColumns)) {
      return true;
    }

    return false;
  }

  void addProject(const std::vector<sql::SelectItemPtr>& selectItems) {
    std::vector<lp::ExprApi> exprs;
    for (const auto& item : selectItems) {
      VELOX_CHECK(item->is(sql::NodeType::kSingleColumn));
      auto* singleColumn = item->as<sql::SingleColumn>();

      lp::ExprApi expr = toExpr(singleColumn->expression());

      if (singleColumn->alias() != nullptr) {
        expr = expr.as(singleColumn->alias()->value());
      }
      exprs.push_back(expr);
    }

    builder_->project(exprs);
  }

  lp::ExprApi toSortingKey(const sql::ExpressionPtr& expr) {
    if (expr->is(sql::NodeType::kLongLiteral)) {
      const auto n = expr->as<sql::LongLiteral>()->value();
      const auto name = builder_->findOrAssignOutputNameAt(n - 1);

      return lp::Col(name);
    }

    return toExpr(expr);
  }

  bool tryAddGlobalAgg(const std::vector<sql::SelectItemPtr>& selectItems) {
    bool hasAggregate = false;
    for (const auto& item : selectItems) {
      VELOX_CHECK(item->is(sql::NodeType::kSingleColumn));
      auto* singleColumn = item->as<sql::SingleColumn>();

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

    addGroupBy(selectItems, {});
    return true;
  }

  void addGroupBy(
      const std::vector<sql::SelectItemPtr>& selectItems,
      const std::vector<sql::GroupingElementPtr>& groupingElements) {
    // Go over grouping keys and collect expressions. Ordinals refer to output
    // columns (selectItems). Non-ordinals refer to input columns.

    std::vector<lp::ExprApi> groupingKeys;

    for (const auto& groupingElement : groupingElements) {
      VELOX_CHECK_EQ(groupingElement->type(), sql::NodeType::kSimpleGroupBy);
      const auto* simple = groupingElement->as<sql::SimpleGroupBy>();

      for (const auto& expr : simple->expressions()) {
        if (expr->is(sql::NodeType::kLongLiteral)) {
          // 1-based index.
          const auto n = expr->as<sql::LongLiteral>()->value();

          VELOX_CHECK_GE(n, 1);
          VELOX_CHECK_LE(n, selectItems.size());

          const auto& item = selectItems.at(n - 1);
          VELOX_CHECK(item->is(sql::NodeType::kSingleColumn));

          const auto* singleColumn = item->as<sql::SingleColumn>();
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
      VELOX_CHECK(item->is(sql::NodeType::kSingleColumn));
      auto* singleColumn = item->as<sql::SingleColumn>();

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

      projections.emplace_back(expr);
    }

    builder_->aggregate(groupingKeys, aggregates);

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
        }
      }
    }

    if (!identityProjection) {
      builder_->project(projections);
    }
  }

  void addOrderBy(const sql::OrderByPtr& orderBy) {
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

  void addOffset(const sql::OffsetPtr& offset) {
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

  void processQuery(sql::Query* query) {
    const auto& queryBody = query->queryBody();

    VELOX_CHECK_NOT_NULL(queryBody);
    VELOX_CHECK(queryBody->is(sql::NodeType::kQuerySpecification));
    auto* querySpec = queryBody->as<sql::QuerySpecification>();

    // FROM t -> builder.tableScan(t)
    processFrom(querySpec->from());

    // WHERE a > 1 -> builder.filter("a > 1")
    addFilter(querySpec->where());

    const auto& selectItems = querySpec->select()->selectItems();

    if (auto groupBy = querySpec->groupBy()) {
      VELOX_USER_CHECK(
          !groupBy->isDistinct(),
          "GROUP BY with DISTINCT is not supported yet");
      addGroupBy(selectItems, groupBy->groupingElements());
      addFilter(querySpec->having());
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

    if (querySpec->select()->isDistinct()) {
      builder_->aggregate(builder_->findOrAssignOutputNames(), {});
    }

    addOrderBy(query->orderBy());
    addOffset(query->offset());
    addLimit(query->limit());
  }

  void visitQuery(sql::Query* query) override {
    processQuery(query);
  }

  void visitQuerySpecification(sql::QuerySpecification* node) override {}

  std::shared_ptr<lp::PlanBuilder> newBuilder(
      const lp::PlanBuilder::Scope& outerScope = nullptr) {
    return std::make_shared<lp::PlanBuilder>(
        context_, /* enableCoersions */ true, outerScope);
  }

  lp::PlanBuilder::Context context_;
  std::shared_ptr<lp::PlanBuilder> builder_;
};

} // namespace

SqlStatementPtr PrestoParser::parse(
    const std::string& sql,
    bool enableTracing) {
  return doParse(sql, enableTracing);
}

lp::ExprPtr PrestoParser::parseExpression(
    const std::string& sql,
    bool enableTracing) {
  auto statement = doParse("SELECT " + sql, enableTracing);
  VELOX_USER_CHECK(statement->isSelect());

  auto plan = statement->asUnchecked<SelectStatement>()->plan();

  VELOX_USER_CHECK(plan->is(lp::NodeKind::kProject));

  auto project = plan->asUnchecked<lp::ProjectNode>();
  VELOX_CHECK_NOT_NULL(project);

  VELOX_USER_CHECK_EQ(1, project->expressions().size());
  return project->expressionAt(0);
}

SqlStatementPtr PrestoParser::doParse(
    const std::string& sql,
    bool enableTracing) {
  sql::ParserHelper helper(sql);
  auto* context = helper.parse();

  sql::AstBuilder astBuilder(enableTracing);
  auto query =
      std::any_cast<std::shared_ptr<sql::Statement>>(astBuilder.visit(context));

  if (enableTracing) {
    std::stringstream astString;
    sql::AstPrinter printer(astString);
    query->accept(&printer);

    std::cout << "AST: " << astString.str() << std::endl;
  }

  RelationPlanner planner(defaultConnectorId_);
  if (query->is(sql::NodeType::kExplain)) {
    query->as<sql::Explain>()->statement()->accept(&planner);
    return std::make_shared<ExplainStatement>(
        std::make_shared<SelectStatement>(planner.getPlan()));
  }

  if (query->is(sql::NodeType::kShowColumns)) {
    const auto tableName = query->as<sql::ShowColumns>()->table()->suffix();

    auto table = connector::getConnector(defaultConnectorId_)
                     ->metadata()
                     ->findTable(tableName);

    VELOX_USER_CHECK_NOT_NULL(table, "Table not found: {}", tableName);

    const auto& schema = table->rowType();

    std::vector<Variant> data;
    data.reserve(schema->size());
    for (auto i = 0; i < schema->size(); ++i) {
      data.emplace_back(
          Variant::row({schema->nameOf(i), schema->childAt(i)->toString()}));
    }

    lp::PlanBuilder::Context ctx(defaultConnectorId_);

    return std::make_shared<SelectStatement>(
        lp::PlanBuilder(ctx)
            .values(ROW({"column", "type"}, {VARCHAR(), VARCHAR()}), data)
            .build());
  }

  query->accept(&planner);
  return std::make_shared<SelectStatement>(planner.getPlan());
}

} // namespace facebook::velox::optimizer::test
