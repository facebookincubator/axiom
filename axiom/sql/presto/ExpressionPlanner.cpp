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

#include "axiom/sql/presto/ExpressionPlanner.h"
#include <algorithm>
#include <cctype>
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"

namespace axiom::sql::presto {

using namespace facebook::velox;

namespace {

int32_t parseInt(const TypeSignaturePtr& type) {
  VELOX_USER_CHECK_EQ(type->parameters().size(), 0);
  const auto& str = type->baseName();
  try {
    return folly::to<int32_t>(str);
  } catch (const folly::ConversionError&) {
    VELOX_USER_FAIL("'{}' could not be converted to INTEGER_LITERAL", str);
  }
}

std::string toFunctionName(ComparisonExpression::Operator op) {
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
      return "distinct_from";
  }

  folly::assume_unreachable();
}

std::string toFunctionName(ArithmeticBinaryExpression::Operator op) {
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

int32_t parseYearMonthInterval(
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

int64_t parseDayTimeInterval(
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

lp::ExprApi parseDecimal(std::string_view value) {
  VELOX_USER_CHECK(!value.empty(), "Invalid decimal value: '{}'", value);

  size_t startPos = 0;
  if (value.at(0) == '+' || value.at(0) == '-') {
    startPos = 1;
  }

  int32_t periodPos = -1;
  int32_t firstNonZeroPos = -1;

  for (auto i = startPos; i < value.size(); ++i) {
    if (value.at(i) == '.') {
      VELOX_USER_CHECK_EQ(periodPos, -1, "Invalid decimal value: '{}'", value);
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

  if (precision <= ShortDecimalType::kMaxPrecision) {
    int64_t v = atol(unscaledValue.c_str());
    return lp::Lit(v, DECIMAL(precision, scale));
  }

  if (precision <= LongDecimalType::kMaxPrecision) {
    return lp::Lit(
        folly::to<int128_t>(unscaledValue), DECIMAL(precision, scale));
  }

  VELOX_USER_FAIL(
      "Invalid decimal value: '{}'. Precision exceeds maximum: {} > {}.",
      value,
      precision,
      LongDecimalType::kMaxPrecision);
}

} // namespace

std::string canonicalizeName(const std::string& name) {
  std::string canonicalName;
  canonicalName.resize(name.size());
  std::transform(
      name.begin(), name.end(), canonicalName.begin(), [](unsigned char c) {
        return std::tolower(c);
      });

  return canonicalName;
}

std::string canonicalizeIdentifier(const Identifier& identifier) {
  // TODO: Figure out whether 'delimited' identifiers should be kept as is.
  return canonicalizeName(identifier.value());
}

TypePtr parseType(const TypeSignaturePtr& type) {
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
        auto fieldName = param->rowFieldName();

        // TODO: Extend Velox's RowType to support quoted / delimited field
        // names.
        if (fieldName.has_value()) {
          if (fieldName->starts_with('\"') && fieldName->ends_with('\"') &&
              fieldName->size() >= 2) {
            fieldName = fieldName->substr(1, fieldName->size() - 2);
          }
        }

        parameters.emplace_back(parseType(param), fieldName);
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

lp::ExprApi ExpressionPlanner::toExpr(
    const ExpressionPtr& node,
    std::unordered_map<const core::IExpr*, lp::PlanBuilder::AggregateOptions>*
        aggregateOptions) {
  switch (node->type()) {
    case NodeType::kIdentifier:
      return lp::Col(canonicalizeIdentifier(*node->as<Identifier>()));

    case NodeType::kDereferenceExpression: {
      auto* dereference = node->as<DereferenceExpression>();
      return lp::Col(
          canonicalizeIdentifier(*dereference->field()),
          toExpr(dereference->base(), aggregateOptions));
    }

    case NodeType::kSubqueryExpression: {
      auto* subquery = node->as<SubqueryExpression>();
      auto query = subquery->query();

      if (query->is(NodeType::kQuery)) {
        VELOX_CHECK_NOT_NULL(
            subqueryPlanner_, "Subquery expressions require a SubqueryPlanner");
        return lp::Subquery(subqueryPlanner_(query->as<Query>()));
      }

      VELOX_NYI(
          "Subquery type is not supported yet: {}",
          NodeTypeName::toName(query->type()));
    }

    case NodeType::kComparisonExpression: {
      auto* comparison = node->as<ComparisonExpression>();
      return lp::Call(
          toFunctionName(comparison->op()),
          toExpr(comparison->left(), aggregateOptions),
          toExpr(comparison->right(), aggregateOptions));
    }

    case NodeType::kNotExpression: {
      auto* negation = node->as<NotExpression>();
      return lp::Call("not", toExpr(negation->value(), aggregateOptions));
    }

    case NodeType::kLikePredicate: {
      auto* like = node->as<LikePredicate>();

      std::vector<lp::ExprApi> inputs;
      inputs.emplace_back(toExpr(like->value(), aggregateOptions));
      inputs.emplace_back(toExpr(like->pattern(), aggregateOptions));
      if (like->escape()) {
        inputs.emplace_back(toExpr(like->escape(), aggregateOptions));
      }

      return lp::Call("like", std::move(inputs));
    }

    case NodeType::kLogicalBinaryExpression: {
      auto* logical = node->as<LogicalBinaryExpression>();
      auto left = toExpr(logical->left(), aggregateOptions);
      auto right = toExpr(logical->right(), aggregateOptions);

      switch (logical->op()) {
        case LogicalBinaryExpression::Operator::kAnd:
          return left && right;

        case LogicalBinaryExpression::Operator::kOr:
          return left || right;
      }
    }

    case NodeType::kArithmeticUnaryExpression: {
      auto* unary = node->as<ArithmeticUnaryExpression>();
      if (unary->sign() == ArithmeticUnaryExpression::Sign::kMinus) {
        return lp::Call("negate", toExpr(unary->value(), aggregateOptions));
      }

      return toExpr(unary->value(), aggregateOptions);
    }

    case NodeType::kArithmeticBinaryExpression: {
      auto* binary = node->as<ArithmeticBinaryExpression>();
      return lp::Call(
          toFunctionName(binary->op()),
          toExpr(binary->left(), aggregateOptions),
          toExpr(binary->right(), aggregateOptions));
    }

    case NodeType::kBetweenPredicate: {
      auto* between = node->as<BetweenPredicate>();
      return lp::Call(
          "between",
          toExpr(between->value(), aggregateOptions),
          toExpr(between->min(), aggregateOptions),
          toExpr(between->max(), aggregateOptions));
    }

    case NodeType::kInPredicate: {
      auto* inPredicate = node->as<InPredicate>();
      const auto& valueList = inPredicate->valueList();

      const auto value = toExpr(inPredicate->value(), aggregateOptions);

      if (valueList->is(NodeType::kInListExpression)) {
        auto inList = valueList->as<InListExpression>();

        std::vector<lp::ExprApi> inputs;
        inputs.reserve(1 + inList->values().size());

        inputs.emplace_back(value);
        for (const auto& expr : inList->values()) {
          inputs.emplace_back(toExpr(expr, aggregateOptions));
        }

        return lp::Call("in", inputs);
      }

      if (valueList->is(NodeType::kSubqueryExpression)) {
        return lp::Call("in", value, toExpr(valueList, aggregateOptions));
      }

      VELOX_USER_FAIL(
          "Unexpected IN predicate: {}",
          NodeTypeName::toName(valueList->type()));
    }

    case NodeType::kExistsPredicate: {
      auto* exists = node->as<ExistsPredicate>();
      return lp::Exists(toExpr(exists->subquery(), aggregateOptions));
    }

    case NodeType::kCast: {
      auto* cast = node->as<Cast>();
      const auto type = parseType(cast->toType());

      if (cast->isSafe()) {
        return lp::TryCast(type, toExpr(cast->expression(), aggregateOptions));
      } else {
        return lp::Cast(type, toExpr(cast->expression(), aggregateOptions));
      }
    }

    case NodeType::kAtTimeZone: {
      auto* atTimeZone = node->as<AtTimeZone>();
      return lp::Call(
          "at_timezone",
          toExpr(atTimeZone->value(), aggregateOptions),
          toExpr(atTimeZone->timeZone(), aggregateOptions));
    }

    case NodeType::kSimpleCaseExpression: {
      auto* simpleCase = node->as<SimpleCaseExpression>();

      const auto operand = toExpr(simpleCase->operand(), aggregateOptions);

      std::vector<lp::ExprApi> inputs;
      inputs.reserve(1 + simpleCase->whenClauses().size());

      for (const auto& clause : simpleCase->whenClauses()) {
        inputs.emplace_back(
            lp::Call(
                "eq", operand, toExpr(clause->operand(), aggregateOptions)));
        inputs.emplace_back(toExpr(clause->result(), aggregateOptions));
      }

      if (simpleCase->defaultValue()) {
        inputs.emplace_back(
            toExpr(simpleCase->defaultValue(), aggregateOptions));
      }

      return lp::Call("switch", inputs);
    }

    case NodeType::kSearchedCaseExpression: {
      auto* searchedCase = node->as<SearchedCaseExpression>();

      std::vector<lp::ExprApi> inputs;
      inputs.reserve(1 + searchedCase->whenClauses().size());

      for (const auto& clause : searchedCase->whenClauses()) {
        inputs.emplace_back(toExpr(clause->operand(), aggregateOptions));
        inputs.emplace_back(toExpr(clause->result(), aggregateOptions));
      }

      if (searchedCase->defaultValue()) {
        inputs.emplace_back(
            toExpr(searchedCase->defaultValue(), aggregateOptions));
      }

      return lp::Call("switch", inputs);
    }

    case NodeType::kExtract: {
      auto* extract = node->as<Extract>();
      auto expr = toExpr(extract->expression(), aggregateOptions);

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

    case NodeType::kLongLiteral: {
      const auto value = node->as<LongLiteral>()->value();
      if (value >= std::numeric_limits<int32_t>::min() &&
          value <= std::numeric_limits<int32_t>::max()) {
        return lp::Lit(static_cast<int32_t>(value));
      } else {
        return lp::Lit(value);
      }
    }

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
        return lp::Lit(multiplier * seconds * 1'000, INTERVAL_DAY_TIME());
      }
    }

    case NodeType::kGenericLiteral: {
      auto literal = node->as<GenericLiteral>();
      return lp::Cast(
          parseType(literal->valueType()), lp::Lit(literal->value()));
    }

    case NodeType::kTimestampLiteral: {
      auto literal = node->as<TimestampLiteral>();

      auto timestamp = util::fromTimestampWithTimezoneString(
          literal->value().c_str(),
          literal->value().size(),
          util::TimestampParseMode::kPrestoCast);

      VELOX_USER_CHECK(
          !timestamp.hasError(),
          "Not a valid timestamp literal: {} - {}",
          literal->value(),
          timestamp.error());

      if (timestamp.value().timeZone != nullptr) {
        return lp::Cast(TIMESTAMP_WITH_TIME_ZONE(), lp::Lit(literal->value()));
      } else {
        return lp::Cast(TIMESTAMP(), lp::Lit(literal->value()));
      }
    }

    case NodeType::kArrayConstructor: {
      auto* array = node->as<ArrayConstructor>();
      std::vector<lp::ExprApi> values;
      for (const auto& value : array->values()) {
        values.emplace_back(toExpr(value, aggregateOptions));
      }

      return lp::Call("array_constructor", values);
    }

    case NodeType::kRow: {
      auto* row = node->as<Row>();
      std::vector<lp::ExprApi> items;
      for (const auto& item : row->items()) {
        items.emplace_back(toExpr(item, aggregateOptions));
      }

      return lp::Call("row_constructor", items);
    }

    case NodeType::kFunctionCall: {
      auto* call = node->as<FunctionCall>();

      std::vector<lp::ExprApi> args;
      for (const auto& arg : call->arguments()) {
        args.push_back(toExpr(arg, aggregateOptions));
      }

      const auto& funcName = call->name()->suffix();
      const auto lowerFuncName = canonicalizeName(funcName);

      // TODO: Verify that NULLIF is semantically equivalent with IF(a = b,
      // null, a). https://github.com/prestodb/presto/issues/27024
      if (lowerFuncName == "nullif") {
        VELOX_USER_CHECK_EQ(
            args.size(), 2, "NULLIF requires exactly 2 arguments");
        return lp::Call(
            "if",
            lp::Call("eq", args[0], args[1]),
            lp::Lit(Variant::null(TypeKind::UNKNOWN)),
            args[0]);
      }

      auto callExpr = lp::Call(funcName, args);

      if (call->isDistinct() || call->filter() != nullptr ||
          call->orderBy() != nullptr) {
        VELOX_CHECK_NOT_NULL(aggregateOptions);

        core::ExprPtr filterExpr;
        if (call->filter() != nullptr) {
          filterExpr = toExpr(call->filter()).expr();
        }

        std::vector<lp::SortKey> sortingKeys;
        if (call->orderBy() != nullptr) {
          const auto& sortItems = call->orderBy()->sortItems();
          for (const auto& item : sortItems) {
            VELOX_CHECK_NOT_NULL(
                sortingKeyResolver_,
                "Sorting key resolution requires a SortingKeyResolver");
            sortingKeys.emplace_back(
                sortingKeyResolver_(item->sortKey()),
                item->isAscending(),
                item->isNullsFirst());
          }
        }

        bool inserted =
            aggregateOptions
                ->emplace(
                    callExpr.expr().get(),
                    lp::PlanBuilder::AggregateOptions(
                        filterExpr, sortingKeys, call->isDistinct()))
                .second;
        VELOX_CHECK(inserted);
      }

      if (call->window() != nullptr) {
        auto windowSpec = convertWindow(call->window(), aggregateOptions);
        if (call->ignoreNulls()) {
          windowSpec.ignoreNulls();
        }
        return callExpr.over(windowSpec);
      }

      return callExpr;
    }

    case NodeType::kLambdaExpression: {
      auto* lambda = node->as<LambdaExpression>();

      std::vector<std::string> names;
      names.reserve(lambda->arguments().size());
      for (const auto& arg : lambda->arguments()) {
        names.emplace_back(arg->name()->value());
      }

      return lp::Lambda(names, toExpr(lambda->body(), aggregateOptions));
    }

    case NodeType::kSubscriptExpression: {
      auto* subscript = node->as<SubscriptExpression>();
      return lp::Call(
          "subscript",
          toExpr(subscript->base(), aggregateOptions),
          toExpr(subscript->index(), aggregateOptions));
    }

    case NodeType::kIsNullPredicate: {
      auto* isNull = node->as<IsNullPredicate>();
      return lp::Call("is_null", toExpr(isNull->value(), aggregateOptions));
    }

    case NodeType::kIsNotNullPredicate: {
      auto* isNull = node->as<IsNotNullPredicate>();
      return lp::Call(
          "not",
          lp::Call("is_null", toExpr(isNull->value(), aggregateOptions)));
    }

    default:
      VELOX_NYI(
          "Unsupported expression type: {}",
          NodeTypeName::toName(node->type()));
  }
}

namespace {

lp::WindowExpr::BoundType toWindowBoundType(FrameBound::Type type) {
  switch (type) {
    case FrameBound::Type::kUnboundedPreceding:
      return lp::WindowExpr::BoundType::kUnboundedPreceding;
    case FrameBound::Type::kPreceding:
      return lp::WindowExpr::BoundType::kPreceding;
    case FrameBound::Type::kCurrentRow:
      return lp::WindowExpr::BoundType::kCurrentRow;
    case FrameBound::Type::kFollowing:
      return lp::WindowExpr::BoundType::kFollowing;
    case FrameBound::Type::kUnboundedFollowing:
      return lp::WindowExpr::BoundType::kUnboundedFollowing;
  }
  VELOX_UNREACHABLE();
}

} // namespace

lp::WindowSpec ExpressionPlanner::convertWindow(
    const std::shared_ptr<Window>& window,
    std::unordered_map<
        const facebook::velox::core::IExpr*,
        lp::PlanBuilder::AggregateOptions>* aggregateOptions) {
  lp::WindowSpec spec;

  if (!window->partitionBy().empty()) {
    std::vector<lp::ExprApi> partitionKeys;
    partitionKeys.reserve(window->partitionBy().size());
    for (const auto& key : window->partitionBy()) {
      partitionKeys.push_back(toExpr(key, aggregateOptions));
    }
    spec.partitionBy(std::move(partitionKeys));
  }

  if (window->orderBy() != nullptr) {
    std::vector<lp::SortKey> orderByKeys;
    const auto& sortItems = window->orderBy()->sortItems();
    orderByKeys.reserve(sortItems.size());
    for (const auto& item : sortItems) {
      orderByKeys.emplace_back(
          toExpr(item->sortKey(), aggregateOptions),
          item->isAscending(),
          item->isNullsFirst());
    }
    spec.orderBy(std::move(orderByKeys));
  }

  if (window->frame() != nullptr) {
    const auto& frame = window->frame();

    auto startType = toWindowBoundType(frame->start()->boundType());
    std::optional<lp::ExprApi> startValue;
    if (frame->start()->value().has_value()) {
      startValue = toExpr(frame->start()->value().value(), aggregateOptions);
    }

    auto endType = frame->end() != nullptr
        ? toWindowBoundType(frame->end()->boundType())
        : lp::WindowExpr::BoundType::kCurrentRow;
    std::optional<lp::ExprApi> endValue;
    if (frame->end() != nullptr && frame->end()->value().has_value()) {
      endValue = toExpr(frame->end()->value().value(), aggregateOptions);
    }

    switch (frame->frameType()) {
      case WindowFrame::Type::kRows:
        spec.rows(
            startType, std::move(startValue), endType, std::move(endValue));
        break;
      case WindowFrame::Type::kRange:
        spec.range(
            startType, std::move(startValue), endType, std::move(endValue));
        break;
      case WindowFrame::Type::kGroups:
        VELOX_NYI("GROUPS frame type is not supported yet");
    }
  }

  return spec;
}

} // namespace axiom::sql::presto
