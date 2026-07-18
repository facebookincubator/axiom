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

#include <folly/ScopeGuard.h>
#include <folly/String.h>

#include <algorithm>
#include <cctype>

#include "axiom/common/SchemaTypeName.h"
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/sql/presto/GroupByPlanner.h"
#include "axiom/sql/presto/PrestoSqlError.h"
#include "axiom/sql/presto/SpecialAggregates.h"
#include "axiom/sql/presto/ast/DefaultTraversalVisitor.h"
#include "velox/exec/Aggregate.h"
#include "velox/functions/prestosql/types/BigintEnumType.h"
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/functions/prestosql/types/VarcharEnumType.h"
#include "velox/parse/Expressions.h"

namespace axiom::sql::presto {

using namespace facebook::velox;

namespace {

// Translates a Presto call already recognized as the metadata aggregate 'kind'
// into a SpecialFormAggCallExpr carrying its fallback. Enforces the modifier
// semantics: window / DISTINCT / FILTER are rejected; ORDER BY is accepted and
// ignored (these aggregates are order-insensitive).
lp::ExprApi makeMetadataAggregate(
    lp::SpecialAggregateKind kind,
    const FunctionCall* call,
    const std::vector<lp::ExprApi>& args,
    const std::string& funcName) {
  AXIOM_PRESTO_SEMANTIC_CHECK_NULL(
      call->window(),
      call->location(),
      funcName,
      "Metadata aggregate cannot be used as a window function: {}",
      funcName);
  AXIOM_PRESTO_SEMANTIC_CHECK(
      !call->isDistinct(),
      call->location(),
      funcName,
      "Metadata aggregate does not support DISTINCT: {}",
      funcName);
  AXIOM_PRESTO_SEMANTIC_CHECK_NULL(
      call->filter(),
      call->location(),
      funcName,
      "Metadata aggregate does not support FILTER: {}",
      funcName);

  const size_t numExpectedArgs =
      kind == lp::SpecialAggregateKind::kMetadataRowCount ? 0 : 1;
  AXIOM_PRESTO_SEMANTIC_CHECK(
      args.size() == numExpectedArgs,
      call->location(),
      funcName,
      "Metadata aggregate has wrong number of arguments: {} expects {}, got {}",
      funcName,
      numExpectedArgs,
      args.size());

  std::vector<core::ExprPtr> inputs;
  inputs.reserve(args.size());
  for (const auto& arg : args) {
    inputs.push_back(arg.expr());
  }

  // Each kind supplies its fallback, used when the connector cannot answer the
  // aggregate from metadata, expressed as count / count_if.
  const auto make = [&](const lp::ExprApi& fallback) {
    return lp::ExprApi{std::make_shared<core::SpecialFormAggCallExpr>(
        kind,
        std::move(inputs),
        fallback.expr(),
        /*alias=*/std::nullopt,
        funcName)};
  };

  switch (kind) {
    case lp::SpecialAggregateKind::kMetadataRowCount:
      return make(lp::Call("count"));
    case lp::SpecialAggregateKind::kMetadataNonNullCount:
      return make(lp::Call("count", args));
    case lp::SpecialAggregateKind::kMetadataNullCount:
      return make(lp::Call("count_if", lp::Call("is_null", args)));
  }
}

// Returns the set of relation aliases directly visible in a simple
// FROM clause (a single Table or AliasedRelation). Returns nullopt
// for shapes we don't analyze (joins, etc.). A null `from` (no FROM
// clause) returns an empty set.
std::optional<folly::F14FastSet<std::string>> simpleFromAliases(
    const RelationPtr& from) {
  folly::F14FastSet<std::string> aliases;
  if (from == nullptr) {
    return aliases;
  }
  if (from->is(NodeType::kTable)) {
    const auto& parts = from->as<Table>()->name()->parts();
    aliases.insert(canonicalizeName(parts.back()));
    return aliases;
  }
  if (from->is(NodeType::kAliasedRelation)) {
    aliases.insert(
        canonicalizeIdentifier(*from->as<AliasedRelation>()->alias()));
    return aliases;
  }
  return std::nullopt;
}

// Walks an aggregate's argument expression to classify column refs as
// inner-scope (qualifier matches `innerAliases`, or unqualified when
// `innerAliases` is non-empty) vs outer-scope (qualifier doesn't
// match, or unqualified when there is no FROM).
class AggregateArgScopeClassifier : public DefaultTraversalVisitor {
 public:
  explicit AggregateArgScopeClassifier(
      const folly::F14FastSet<std::string>& innerAliases)
      : innerAliases_(innerAliases) {}

  bool anyInner() const {
    return anyInner_;
  }
  bool anyOuter() const {
    return anyOuter_;
  }

 protected:
  void visitIdentifier(Identifier* /*node*/) override {
    if (innerAliases_.empty()) {
      anyOuter_ = true;
    } else {
      anyInner_ = true;
    }
  }

  void visitDereferenceExpression(DereferenceExpression* node) override {
    if (node->base()->is(NodeType::kIdentifier)) {
      const auto qualifier =
          canonicalizeIdentifier(*node->base()->as<Identifier>());
      if (innerAliases_.count(qualifier) > 0) {
        anyInner_ = true;
      } else {
        anyOuter_ = true;
      }
      return;
    }
    DefaultTraversalVisitor::visitDereferenceExpression(node);
  }

  void visitSubqueryExpression(SubqueryExpression* /*node*/) override {}

 private:
  const folly::F14FastSet<std::string>& innerAliases_;
  bool anyInner_{false};
  bool anyOuter_{false};
};

// Walks an expression looking for an aggregate FunctionCall whose
// args reference at least one outer column and no inner-scope column.
class OuterScopeAggregateScanner : public DefaultTraversalVisitor {
 public:
  explicit OuterScopeAggregateScanner(
      const folly::F14FastSet<std::string>& innerAliases)
      : innerAliases_(innerAliases) {}

  bool foundAny() const {
    return foundAny_;
  }

 protected:
  void visitFunctionCall(FunctionCall* node) override {
    const auto name = canonicalizeName(node->name()->suffix());
    if ((exec::getAggregateFunctionEntry(name) != nullptr ||
         specialAggregateKind(name).has_value()) &&
        node->window() == nullptr && !node->isDistinct() &&
        node->filter() == nullptr && node->orderBy() == nullptr &&
        !node->ignoreNulls() && !node->arguments().empty()) {
      AggregateArgScopeClassifier classifier(innerAliases_);
      for (const auto& arg : node->arguments()) {
        arg->accept(&classifier);
      }
      if (classifier.anyOuter() && !classifier.anyInner()) {
        foundAny_ = true;
        return;
      }
    }
    DefaultTraversalVisitor::visitFunctionCall(node);
  }

  void visitSubqueryExpression(SubqueryExpression* /*node*/) override {}

 private:
  const folly::F14FastSet<std::string>& innerAliases_;
  bool foundAny_{false};
};

// Walks `expr`. Sets `anyMatch` if any column reference's name is in
// `names`; sets `anyOther` if any column reference's name is not.
void classifyColumnRefs(
    const lp::ExprPtr& expr,
    const folly::F14FastSet<std::string>& names,
    bool& anyMatch,
    bool& anyOther) {
  if (expr->kind() == lp::ExprKind::kInputReference) {
    if (names.count(expr->as<lp::InputReferenceExpr>()->name()) > 0) {
      anyMatch = true;
    } else {
      anyOther = true;
    }
    return;
  }
  for (const auto& input : expr->inputs()) {
    classifyColumnRefs(input, names, anyMatch, anyOther);
  }
}

// True if `plan` (or any descendant) is an AggregateNode containing an
// aggregate call whose arguments reference at least one outer column
// and no inner-source column.
bool hasOuterScopeAggregate(const lp::LogicalPlanNodePtr& plan) {
  if (plan->kind() == lp::NodeKind::kAggregate) {
    const auto* aggregateNode = plan->as<lp::AggregateNode>();
    const auto& source = aggregateNode->inputs().front();
    folly::F14FastSet<std::string> innerColumns(
        source->outputType()->names().begin(),
        source->outputType()->names().end());
    for (const auto& aggregateCall : aggregateNode->aggregates()) {
      bool anyInner = false;
      bool anyOuter = false;
      for (const auto& argExpr : aggregateCall->inputs()) {
        classifyColumnRefs(argExpr, innerColumns, anyInner, anyOuter);
        if (anyInner) {
          break;
        }
      }
      if (anyOuter && !anyInner) {
        return true;
      }
    }
  }
  for (const auto& input : plan->inputs()) {
    if (hasOuterScopeAggregate(input)) {
      return true;
    }
  }
  return false;
}

int32_t parseInt(const TypeSignaturePtr& type) {
  AXIOM_PRESTO_SEMANTIC_CHECK(
      type->parameters().size() == 0,
      type->location(),
      type->baseName(),
      "Type must not have parameters");
  const auto& str = type->baseName();
  try {
    return folly::to<int32_t>(str);
  } catch (const folly::ConversionError&) {
    AXIOM_PRESTO_SEMANTIC_FAIL(
        type->location(), str, "Could not be converted to INTEGER_LITERAL");
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
    std::optional<IntervalLiteral::IntervalField> end,
    NodeLocation location) {
  AXIOM_PRESTO_SEMANTIC_CHECK(
      !end.has_value() || start == end.value(),
      location,
      value,
      "Multi-part intervals are not supported yet");

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
    std::optional<IntervalLiteral::IntervalField> end,
    NodeLocation location) {
  AXIOM_PRESTO_SEMANTIC_CHECK(
      !end.has_value() || start == end.value(),
      location,
      value,
      "Multi-part intervals are not supported yet");

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

lp::ExprApi parseDecimal(std::string_view value, NodeLocation location) {
  AXIOM_PRESTO_SEMANTIC_CHECK(
      !value.empty(), location, std::string(value), "Invalid decimal value");

  size_t startPos = 0;
  if (value.at(0) == '+' || value.at(0) == '-') {
    startPos = 1;
  }

  int32_t periodPos = -1;
  int32_t firstNonZeroPos = -1;

  for (auto i = startPos; i < value.size(); ++i) {
    if (value.at(i) == '.') {
      AXIOM_PRESTO_SEMANTIC_CHECK(
          periodPos == -1,
          location,
          std::string(value),
          "Invalid decimal value");
      periodPos = i;
    } else {
      AXIOM_PRESTO_SEMANTIC_CHECK(
          std::isdigit(value.at(i)),
          location,
          std::string(value),
          "Invalid decimal value");

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

  AXIOM_PRESTO_SEMANTIC_FAIL(
      location,
      std::string(value),
      "Invalid decimal value. Precision exceeds maximum: {} > {}.",
      precision,
      LongDecimalType::kMaxPrecision);
}

// Flattens a nested DereferenceExpression chain into dot-separated parts.
// Returns true if the entire chain consists of identifiers and dereferences.
// Returns false and clears 'parts' if any node is a different expression type.
bool extractQualifiedParts(
    const ExpressionPtr& node,
    std::vector<std::string>& parts) {
  if (node->is(NodeType::kIdentifier)) {
    auto* identifier = node->as<Identifier>();
    parts.push_back(canonicalizeName(identifier->value()));
    return true;
  }
  if (node->is(NodeType::kDereferenceExpression)) {
    auto* deref = node->as<DereferenceExpression>();
    if (!extractQualifiedParts(deref->base(), parts)) {
      return false;
    }
    parts.push_back(canonicalizeName(deref->field()->value()));
    return true;
  }
  parts.clear();
  return false;
}

// Resolves a user-defined type from ConnectorMetadata. Returns the TypePtr,
// or nullptr if not found. Uses the per-query cache to avoid repeated lookups.
TypePtr findQualifiedType(
    const std::string& catalog,
    const facebook::axiom::SchemaTypeName& typeName,
    folly::F14FastMap<std::string, TypePtr>& typeCache) {
  auto qualifiedName =
      fmt::format("{}.{}.{}", catalog, typeName.schema, typeName.type);

  auto it = typeCache.find(qualifiedName);
  if (it != typeCache.end()) {
    return it->second;
  }

  auto metadata =
      facebook::axiom::connector::ConnectorMetadataRegistry::tryGet(catalog);
  if (metadata == nullptr) {
    typeCache.emplace(qualifiedName, nullptr);
    return nullptr;
  }

  auto type = metadata->findType(typeName);
  typeCache.emplace(qualifiedName, type);
  return type;
}

// Tries to resolve a dotted base name (e.g., "CATALOG.SCHEMA.TYPE") as a
// connector-provided user-defined type. Returns nullptr if the name has
// fewer than two dots or the connector does not know the type.
TypePtr tryConnectorBasedTypeResolution(
    std::string_view baseName,
    folly::F14FastMap<std::string, TypePtr>& typeCache) {
  auto firstDot = baseName.find('.');
  if (firstDot == std::string_view::npos) {
    return nullptr;
  }
  auto secondDot = baseName.find('.', firstDot + 1);
  if (secondDot == std::string_view::npos) {
    return nullptr;
  }
  auto catalog = canonicalizeName(std::string{baseName.substr(0, firstDot)});
  auto schema = canonicalizeName(
      std::string{baseName.substr(firstDot + 1, secondDot - firstDot - 1)});
  auto typeName = canonicalizeName(std::string{baseName.substr(secondDot + 1)});
  if (catalog.empty() || schema.empty() || typeName.empty()) {
    return nullptr;
  }
  return findQualifiedType(
      catalog, {std::move(schema), std::move(typeName)}, typeCache);
}

// Attempts to resolve a qualified name (e.g., "catalog.schema.status.active")
// as an enum literal. Returns nullopt if the catalog is not registered. Throws
// PrestoSqlError if the catalog is registered but the type is not found, if the
// value is not a valid enum member, or if the resolved type is not an enum
// type.
std::optional<lp::ExprApi> tryResolveEnumLiteral(
    const std::vector<std::string>& parts,
    folly::F14FastMap<std::string, TypePtr>& typeCache,
    NodeLocation location) {
  if (parts.size() < 4) {
    return std::nullopt;
  }

  const auto& catalog = parts[0];
  facebook::axiom::SchemaTypeName schemaTypeName{
      parts[1], folly::join('.', parts.begin() + 2, parts.end() - 1)};
  // Enum types store their keys uppercase (the canonical form), so uppercase
  // the referenced key for a case-insensitive lookup. The type and namespace
  // parts stay lowercased.
  std::string valueName = parts.back();
  std::transform(
      valueName.begin(),
      valueName.end(),
      valueName.begin(),
      [](unsigned char character) { return std::toupper(character); });

  auto type = findQualifiedType(catalog, schemaTypeName, typeCache);
  if (type == nullptr) {
    if (facebook::axiom::connector::ConnectorMetadataRegistry::tryGet(
            catalog) != nullptr) {
      AXIOM_PRESTO_SEMANTIC_FAIL(
          location,
          fmt::format("{}.{}", catalog, schemaTypeName),
          "Type not found");
    }
    return std::nullopt;
  }

  if (isBigintEnumType(*type)) {
    auto enumType = asBigintEnum(type);
    auto value = enumType->valueAt(valueName);
    AXIOM_PRESTO_SEMANTIC_CHECK(
        value.has_value(),
        location,
        valueName,
        "Enum value not found in type: {}",
        enumType->enumName());
    return lp::Lit(*value, type);
  }

  if (isVarcharEnumType(*type)) {
    auto enumType = asVarcharEnum(type);
    auto value = enumType->valueAt(valueName);
    AXIOM_PRESTO_SEMANTIC_CHECK(
        value.has_value(),
        location,
        valueName,
        "Enum value not found in type: {}",
        enumType->enumName());
    return lp::Lit(std::move(*value), type);
  }

  AXIOM_PRESTO_SEMANTIC_FAIL(
      location, valueName, "Type is not an enum type: {}", type->toString());
}

// Resolves a TypeSignature to a Velox built-in type. Returns nullptr for
// unknown non-parametric types. Nested types (ARRAY, MAP, etc.) use parseType
// (declared in the header) which throws on failure.
TypePtr tryResolveBuiltinType(const TypeSignaturePtr& type) {
  auto baseName = type->baseName();
  std::transform(
      baseName.begin(), baseName.end(), baseName.begin(), [](char c) {
        return (std::toupper(c));
      });

  if (baseName == "INT") {
    baseName = "INTEGER";
  }

  std::vector<TypeParameter> parameters;
  if (baseName == "DECIMAL" && type->parameters().empty()) {
    // DECIMAL without explicit precision/scale defaults to DECIMAL(38, 0),
    // matching Presto's behavior.
    parameters.emplace_back(static_cast<int32_t>(38));
    parameters.emplace_back(static_cast<int32_t>(0));
  } else if (!type->parameters().empty()) {
    const auto numParams = type->parameters().size();
    parameters.reserve(numParams);

    if (baseName == "ARRAY") {
      AXIOM_PRESTO_SEMANTIC_CHECK_EQ(
          numParams,
          static_cast<size_t>(1),
          type->location(),
          baseName,
          "ARRAY expects 1 parameter");
      parameters.emplace_back(parseType(type->parameters().at(0)));
    } else if (baseName == "MAP") {
      AXIOM_PRESTO_SEMANTIC_CHECK_EQ(
          numParams,
          static_cast<size_t>(2),
          type->location(),
          baseName,
          "MAP expects 2 parameters");
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
      AXIOM_PRESTO_SEMANTIC_CHECK_EQ(
          numParams,
          static_cast<size_t>(2),
          type->location(),
          baseName,
          "DECIMAL expects 2 parameters");
      parameters.emplace_back(parseInt(type->parameters().at(0)));
      parameters.emplace_back(parseInt(type->parameters().at(1)));
    } else if (baseName == "TDIGEST" || baseName == "QDIGEST") {
      AXIOM_PRESTO_SEMANTIC_CHECK_EQ(
          numParams,
          static_cast<size_t>(1),
          type->location(),
          baseName,
          "Expects 1 parameter");
      parameters.emplace_back(parseType(type->parameters().at(0)));
    } else {
      AXIOM_PRESTO_SEMANTIC_FAIL(
          type->location(), baseName, "Unknown parametric type");
    }
  }

  return getType(baseName, parameters);
}

} // namespace

bool isOuterScopeAggregateLiftCandidate(Query* query) {
  if (query == nullptr || query->with() != nullptr ||
      query->orderBy() != nullptr || query->offset() != nullptr ||
      query->limit().has_value() || query->queryBody() == nullptr ||
      !query->queryBody()->is(NodeType::kQuerySpecification)) {
    return false;
  }
  const auto* spec = query->queryBody()->as<QuerySpecification>();
  if (spec->groupBy() != nullptr || spec->having() != nullptr ||
      spec->orderBy() != nullptr || spec->offset() != nullptr ||
      spec->limit().has_value() || spec->select() == nullptr ||
      spec->select()->isDistinct() ||
      spec->select()->selectItems().size() != 1) {
    return false;
  }
  const auto& item = spec->select()->selectItems().front();
  if (!item->is(NodeType::kSingleColumn) ||
      item->as<SingleColumn>()->expression() == nullptr) {
    return false;
  }
  auto innerAliases = simpleFromAliases(spec->from());
  if (!innerAliases.has_value()) {
    return false;
  }
  OuterScopeAggregateScanner scanner(*innerAliases);
  item->as<SingleColumn>()->expression()->accept(&scanner);
  return scanner.foundAny();
}

void ExpressionPlanner::findWindowExprs(
    const core::ExprPtr& expr,
    std::unordered_map<const core::IExpr*, core::ExprPtr>& windowExprs,
    std::vector<const core::IExpr*>& traversalOrder) {
  if (expr->is(core::IExpr::Kind::kWindow)) {
    if (windowExprs.emplace(expr.get(), expr).second) {
      traversalOrder.push_back(expr.get());
    }
    return;
  }
  for (const auto& input : expr->inputs()) {
    findWindowExprs(input, windowExprs, traversalOrder);
  }
}

void ExpressionPlanner::findNestedWindowExprs(
    const std::vector<lp::ExprApi>& exprs,
    std::unordered_map<const core::IExpr*, core::ExprPtr>& windowExprs,
    std::vector<const core::IExpr*>& traversalOrder) {
  for (const auto& expr : exprs) {
    if (!expr.expr()->is(core::IExpr::Kind::kWindow)) {
      findWindowExprs(expr.expr(), windowExprs, traversalOrder);
    }
  }
}

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
  // Presto matches identifiers case-insensitively regardless of quoting.
  // Quoting only affects which characters are permitted and which case is
  // preserved in the output schema; it does NOT alter matching semantics.
  return canonicalizeName(identifier.value());
}

TypePtr parseType(const TypeSignaturePtr& type) {
  auto veloxType = tryResolveBuiltinType(type);
  AXIOM_PRESTO_SEMANTIC_CHECK(
      veloxType != nullptr,
      type->location(),
      type->baseName(),
      "Cannot resolve type");
  return veloxType;
}

TypePtr ExpressionPlanner::resolveType(const TypeSignaturePtr& type) {
  auto veloxType = tryResolveBuiltinType(type);
  if (veloxType == nullptr) {
    veloxType = tryConnectorBasedTypeResolution(type->baseName(), typeCache_);
  }
  AXIOM_PRESTO_SEMANTIC_CHECK(
      veloxType != nullptr,
      type->location(),
      type->baseName(),
      "Cannot resolve type");
  return veloxType;
}

lp::ExprApi ExpressionPlanner::toExpr(
    const ExpressionPtr& node,
    ExprOptions options) {
  switch (node->type()) {
    case NodeType::kIdentifier: {
      auto name = canonicalizeIdentifier(*node->as<Identifier>());
      // Lateral column alias: resolve to the alias expression if the name
      // matches an alias and is NOT a known column (columns take priority).
      if (aliasExprs_ != nullptr) {
        bool isColumn = columnNames_ != nullptr && columnNames_->contains(name);
        if (!isColumn) {
          if (auto it = aliasExprs_->find(name); it != aliasExprs_->end()) {
            return lp::ExprApi(it->second);
          }
        }
      }
      return lp::Col(name);
    }

    case NodeType::kDereferenceExpression: {
      auto* dereference = node->as<DereferenceExpression>();

      // Try to resolve as an enum literal (e.g., catalog.schema.Type.VALUE).
      // When columnResolver_ is nullptr (e.g., parseSqlExpression for CREATE
      // TABLE property values), there is no column scope to compete with — a
      // 4-part dotted name can only be an enum literal.
      std::vector<std::string> parts;
      if (extractQualifiedParts(node, parts) && parts.size() >= 4) {
        bool isColumn =
            columnResolver_ != nullptr && columnResolver_(parts[0], parts[1]);

        if (!isColumn) {
          auto resolved =
              tryResolveEnumLiteral(parts, typeCache_, node->location());
          if (resolved.has_value()) {
            return *resolved;
          }
        }
      }

      auto field = canonicalizeIdentifier(*dereference->field());
      // The base of a dereference is a scope qualifier (table alias or
      // struct-typed column), not a value-producing expression. Lateral
      // column alias substitution must not apply to it; bypass toExpr to
      // build the qualifier directly when the base is a bare identifier.
      //
      // TODO: extend the resolver so lateral aliases are a third fallback
      // scope after table.column and struct dereference. Requires moving
      // lateral substitution from here into PlanBuilder::resolveInputName.
      if (dereference->base()->is(NodeType::kIdentifier)) {
        auto qualifier =
            canonicalizeIdentifier(*dereference->base()->as<Identifier>());
        // A lambda parameter shadows any outer alias of the same name, so
        // the qualifier must be preserved for ExprResolver to dereference it
        // as a struct field of the parameter. Search innermost-first to
        // match SQL lexical scoping.
        const bool isLambdaParam = std::find(
                                       lambdaParamScope_.rbegin(),
                                       lambdaParamScope_.rend(),
                                       qualifier) != lambdaParamScope_.rend();
        // Strip table qualifier when safe (not a struct field, name is
        // unambiguous).
        if (!isLambdaParam && shouldDropQualifier_ &&
            shouldDropQualifier_(qualifier, field)) {
          return lp::Col(field);
        }
        return lp::Col(field, lp::Col(qualifier));
      }
      return lp::Col(field, toExpr(dereference->base(), options));
    }

    case NodeType::kSubqueryExpression: {
      return planSubquery(node->as<SubqueryExpression>(), /*scalar=*/true);
    }

    case NodeType::kComparisonExpression: {
      auto* comparison = node->as<ComparisonExpression>();
      return lp::Call(
          toFunctionName(comparison->op()),
          toExpr(comparison->left(), options),
          toExpr(comparison->right(), options));
    }

    case NodeType::kNotExpression: {
      auto* negation = node->as<NotExpression>();
      return lp::Call("not", toExpr(negation->value(), options));
    }

    case NodeType::kLikePredicate: {
      auto* like = node->as<LikePredicate>();

      std::vector<lp::ExprApi> inputs;
      inputs.emplace_back(toExpr(like->value(), options));
      inputs.emplace_back(toExpr(like->pattern(), options));
      if (like->escape()) {
        inputs.emplace_back(toExpr(like->escape(), options));
      }

      return lp::Call("like", std::move(inputs));
    }

    case NodeType::kLogicalBinaryExpression: {
      auto* logical = node->as<LogicalBinaryExpression>();
      auto left = toExpr(logical->left(), options);
      auto right = toExpr(logical->right(), options);

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
        return lp::Call("negate", toExpr(unary->value(), options));
      }

      return toExpr(unary->value(), options);
    }

    case NodeType::kArithmeticBinaryExpression: {
      auto* binary = node->as<ArithmeticBinaryExpression>();
      return lp::Call(
          toFunctionName(binary->op()),
          toExpr(binary->left(), options),
          toExpr(binary->right(), options));
    }

    case NodeType::kBetweenPredicate: {
      auto* between = node->as<BetweenPredicate>();
      return lp::Call(
          "between",
          toExpr(between->value(), options),
          toExpr(between->min(), options),
          toExpr(between->max(), options));
    }

    case NodeType::kInPredicate: {
      auto* inPredicate = node->as<InPredicate>();
      const auto& valueList = inPredicate->valueList();

      const auto value = toExpr(inPredicate->value(), options);

      if (valueList->is(NodeType::kInListExpression)) {
        auto inList = valueList->as<InListExpression>();

        std::vector<lp::ExprApi> inputs;
        inputs.reserve(1 + inList->values().size());

        inputs.emplace_back(value);
        for (const auto& expr : inList->values()) {
          inputs.emplace_back(toExpr(expr, options));
        }

        return lp::Call("in", inputs);
      }

      if (valueList->is(NodeType::kSubqueryExpression)) {
        return lp::Call("in", value, toExpr(valueList, options));
      }

      AXIOM_PRESTO_SEMANTIC_FAIL(
          valueList->location(),
          std::nullopt,
          "Unexpected IN predicate: {}",
          NodeTypeName::toName(valueList->type()));
    }

    case NodeType::kExistsPredicate: {
      auto* exists = node->as<ExistsPredicate>();
      return lp::Exists(planSubquery(
          exists->subquery()->as<SubqueryExpression>(), /*scalar=*/false));
    }

    case NodeType::kCast: {
      auto* cast = node->as<Cast>();
      const auto type = resolveType(cast->toType());

      if (cast->isSafe()) {
        return lp::TryCast(type, toExpr(cast->expression(), options));
      } else {
        return lp::Cast(type, toExpr(cast->expression(), options));
      }
    }

    case NodeType::kAtTimeZone: {
      auto* atTimeZone = node->as<AtTimeZone>();
      return lp::Call(
          "at_timezone",
          toExpr(atTimeZone->value(), options),
          toExpr(atTimeZone->timeZone(), options));
    }

    case NodeType::kSimpleCaseExpression: {
      auto* simpleCase = node->as<SimpleCaseExpression>();

      const auto operand = toExpr(simpleCase->operand(), options);

      std::vector<lp::ExprApi> inputs;
      inputs.reserve(1 + simpleCase->whenClauses().size());

      for (const auto& clause : simpleCase->whenClauses()) {
        inputs.emplace_back(
            lp::Call("eq", operand, toExpr(clause->operand(), options)));
        inputs.emplace_back(toExpr(clause->result(), options));
      }

      if (simpleCase->defaultValue()) {
        inputs.emplace_back(toExpr(simpleCase->defaultValue(), options));
      }

      return lp::Call("switch", inputs);
    }

    case NodeType::kSearchedCaseExpression: {
      auto* searchedCase = node->as<SearchedCaseExpression>();

      std::vector<lp::ExprApi> inputs;
      inputs.reserve(1 + searchedCase->whenClauses().size());

      for (const auto& clause : searchedCase->whenClauses()) {
        // A NULL condition can never be true. Drop the clause.
        if (clause->operand()->is(NodeType::kNullLiteral)) {
          continue;
        }
        inputs.emplace_back(toExpr(clause->operand(), options));
        inputs.emplace_back(toExpr(clause->result(), options));
      }

      if (searchedCase->defaultValue()) {
        inputs.emplace_back(toExpr(searchedCase->defaultValue(), options));
      }

      // All WHEN clauses were dropped (all had NULL conditions).
      if (inputs.empty()) {
        return lp::Lit(Variant::null(TypeKind::UNKNOWN));
      }

      // Only the ELSE value remains.
      if (inputs.size() == 1) {
        return inputs[0];
      }

      return lp::Call("switch", inputs);
    }

    case NodeType::kExtract: {
      auto* extract = node->as<Extract>();
      auto expr = toExpr(extract->expression(), options);

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
      return parseDecimal(
          node->as<DecimalLiteral>()->value(),
          node->as<DecimalLiteral>()->location());

    case NodeType::kStringLiteral:
      return lp::Lit(node->as<StringLiteral>()->value());

    case NodeType::kBinaryLiteral: {
      auto hexString = node->as<BinaryLiteral>()->value();
      std::erase_if(hexString, [](char c) { return std::isspace(c); });
      std::string bytes;
      AXIOM_PRESTO_SEMANTIC_CHECK(
          hexString.size() % 2 == 0,
          node->location(),
          hexString,
          "Binary literal must contain an even number of digits");
      AXIOM_PRESTO_SEMANTIC_CHECK(
          folly::unhexlify(hexString, bytes),
          node->location(),
          hexString,
          "Binary literal can only contain hexadecimal digits");
      return lp::Lit(Variant::binary(std::move(bytes)));
    }

    case NodeType::kIntervalLiteral: {
      const auto interval = node->as<IntervalLiteral>();
      const int32_t multiplier =
          interval->sign() == IntervalLiteral::Sign::kPositive ? 1 : -1;

      if (interval->isYearToMonth()) {
        const auto months = parseYearMonthInterval(
            interval->value(),
            interval->startField(),
            interval->endField(),
            interval->location());
        return lp::Lit(multiplier * months, INTERVAL_YEAR_MONTH());
      } else {
        const auto seconds = parseDayTimeInterval(
            interval->value(),
            interval->startField(),
            interval->endField(),
            interval->location());
        return lp::Lit(multiplier * seconds * 1'000, INTERVAL_DAY_TIME());
      }
    }

    case NodeType::kGenericLiteral: {
      auto literal = node->as<GenericLiteral>();
      auto type = resolveType(literal->valueType());
      // JSON literals use json_parse, not CAST. CAST(VARCHAR AS JSON)
      // wraps the value as a JSON string, while json_parse interprets the
      // value as JSON.
      if (facebook::velox::isJsonType(type)) {
        return lp::Call("json_parse", lp::Lit(literal->value()));
      }
      return lp::Cast(type, lp::Lit(literal->value()));
    }

    case NodeType::kTimestampLiteral: {
      auto literal = node->as<TimestampLiteral>();

      auto timestamp = util::fromTimestampWithTimezoneString(
          literal->value().c_str(),
          literal->value().size(),
          util::TimestampParseMode::kPrestoCast);

      AXIOM_PRESTO_SEMANTIC_CHECK(
          !timestamp.hasError(),
          literal->location(),
          literal->value(),
          "Not a valid timestamp literal: {}",
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
        values.emplace_back(toExpr(value, options));
      }

      return lp::Call("array_constructor", values);
    }

    case NodeType::kRow: {
      auto* row = node->as<Row>();
      std::vector<lp::ExprApi> items;
      for (const auto& item : row->items()) {
        items.emplace_back(toExpr(item, options));
      }

      return lp::Call("row_constructor", items);
    }

    case NodeType::kNamedRow: {
      auto* row = node->as<NamedRow>();
      std::vector<core::ExprPtr> childExprs;
      childExprs.reserve(row->items().size());
      for (const auto& item : row->items()) {
        childExprs.push_back(toExpr(item, options).expr());
      }
      return lp::ExprApi{std::make_shared<core::ConcatExpr>(
          row->fieldNames(), std::move(childExprs))};
    }

    case NodeType::kFunctionCall: {
      auto* call = node->as<FunctionCall>();

      auto args = translateArgs(call->arguments(), options);

      const auto& funcName = call->name()->suffix();
      const auto lowerFuncName = canonicalizeName(funcName);

      if (lowerFuncName == "nullif") {
        AXIOM_PRESTO_SEMANTIC_CHECK(
            args.size() == 2,
            node->location(),
            lowerFuncName,
            "NULLIF requires exactly 2 arguments, got {}",
            args.size());
        return lp::Call("nullif", args[0], args[1]);
      }

      if (auto kind = specialAggregateKind(lowerFuncName)) {
        return makeMetadataAggregate(*kind, call, args, lowerFuncName);
      }

      if (call->isDistinct() || call->filter() || call->orderBy()) {
        return toAggregateCallExpr(call, funcName, args, options);
      }

      // A qualified (multi-part) name denotes a connector-defined SQL-invoked
      // function. Encode it with a leading '.' so downstream resolution can
      // distinguish it from a builtin; a plain builtin keeps its bare name.
      auto callExpr = lp::Call(
          call->name()->parts().size() > 1
              ? "." + call->name()->fullyQualifiedName()
              : funcName,
          args);

      if (call->window() != nullptr) {
        auto windowSpec = convertWindow(call->window(), options);
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
        names.emplace_back(canonicalizeName(arg->name()->value()));
      }

      const auto scopeBegin = lambdaParamScope_.size();
      lambdaParamScope_.insert(
          lambdaParamScope_.end(), names.begin(), names.end());
      SCOPE_EXIT {
        lambdaParamScope_.resize(scopeBegin);
      };

      return lp::Lambda(names, toExpr(lambda->body(), options));
    }

    case NodeType::kSubscriptExpression: {
      auto* subscript = node->as<SubscriptExpression>();
      return lp::Call(
          "subscript",
          toExpr(subscript->base(), options),
          toExpr(subscript->index(), options));
    }

    case NodeType::kIsNullPredicate: {
      auto* isNull = node->as<IsNullPredicate>();
      return lp::Call("is_null", toExpr(isNull->value(), options));
    }

    case NodeType::kIsNotNullPredicate: {
      auto* isNull = node->as<IsNotNullPredicate>();
      return lp::Call(
          "not", lp::Call("is_null", toExpr(isNull->value(), options)));
    }

    case NodeType::kCurrentTime: {
      auto* currentTime = node->as<CurrentTime>();

      AXIOM_PRESTO_SEMANTIC_CHECK(
          !currentTime->precision().has_value(),
          node->location(),
          std::nullopt,
          "Precision for date/time functions is not supported yet.");

      switch (currentTime->function()) {
        case CurrentTime::Function::kDate:
          return lp::Call("current_date");
        case CurrentTime::Function::kTime:
          return lp::Call("current_time");
        case CurrentTime::Function::kTimestamp:
          return lp::Call("current_timestamp");
        case CurrentTime::Function::kLocaltime:
          return lp::Call("localtime");
        case CurrentTime::Function::kLocaltimestamp:
          return lp::Call("localtimestamp");
      }
      VELOX_UNREACHABLE();
    }

    case NodeType::kGroupingOperation: {
      AXIOM_PRESTO_SEMANTIC_CHECK(
          options.allowGrouping,
          node->location(),
          std::nullopt,
          "GROUPING() is not allowed in this context");
      const auto* groupingOp = node->as<GroupingOperation>();
      std::vector<lp::ExprApi> columnArgs;
      columnArgs.reserve(groupingOp->groupingColumns().size());
      for (const auto& column : groupingOp->groupingColumns()) {
        columnArgs.push_back(toExpr(column, options));
      }
      return lp::Call(
          std::string(GroupByPlanner::kGroupingFunctionName), columnArgs);
    }

    case NodeType::kCurrentUser: {
      AXIOM_PRESTO_SEMANTIC_CHECK(
          !user_.empty(),
          node->location(),
          std::nullopt,
          "CURRENT_USER is not available in this context (no session user set)");
      return lp::Lit(user_);
    }

    default:
      AXIOM_PRESTO_SYNTAX_FAIL(
          node->location(),
          std::nullopt,
          "Unsupported expression type: {}",
          NodeTypeName::toName(node->type()));
  }
}

namespace {

core::WindowCallExpr::BoundType toWindowBoundType(FrameBound::Type type) {
  switch (type) {
    case FrameBound::Type::kUnboundedPreceding:
      return core::WindowCallExpr::BoundType::kUnboundedPreceding;
    case FrameBound::Type::kPreceding:
      return core::WindowCallExpr::BoundType::kPreceding;
    case FrameBound::Type::kCurrentRow:
      return core::WindowCallExpr::BoundType::kCurrentRow;
    case FrameBound::Type::kFollowing:
      return core::WindowCallExpr::BoundType::kFollowing;
    case FrameBound::Type::kUnboundedFollowing:
      return core::WindowCallExpr::BoundType::kUnboundedFollowing;
  }
  VELOX_UNREACHABLE();
}

// AST-shape matcher for a scalar subquery that may be a candidate
// for the outer-scope aggregate lift. Match is necessary but not
// sufficient — the caller must still verify at the LP level that each
// aggregate's arguments reference only outer columns.
struct LiftablePureOuterAggregate {
  Query* query;
  const QuerySpecification* spec;

  // Accepts a body of the form
  //   SELECT <expr> FROM <inner> [WHERE <pred>]
  // with no other top-level clauses. <expr> may itself contain
  // aggregate function calls; per-aggregate validity is verified at
  // the LP level by the caller.
  static std::optional<LiftablePureOuterAggregate> match(Query* query) {
    if (!isOuterScopeAggregateLiftCandidate(query)) {
      return std::nullopt;
    }
    return LiftablePureOuterAggregate{
        query, query->queryBody()->as<QuerySpecification>()};
  }

  // Plans `SELECT 1 FROM <body.from> [WHERE body.where]` for use as
  // the EXISTS gate's source. The planned body's pre-aggregate input
  // can't be reused because in the no-FROM case it has zero output
  // columns and `lp::Subquery` requires at least one.
  //
  //   Before: SELECT count(t.a) FROM u WHERE u.a > 999
  //   After:  SELECT 1           FROM u WHERE u.a > 999
  lp::LogicalPlanNodePtr planExistsBody(
      const ExpressionPlanner::SubqueryPlanner& planner) const {
    const auto& selectLocation = spec->select()->location();
    auto liftedSelect = std::make_shared<Select>(
        selectLocation,
        /*distinct=*/false,
        std::vector<SelectItemPtr>{std::make_shared<SingleColumn>(
            selectLocation,
            std::make_shared<LongLiteral>(selectLocation, 1),
            /*alias=*/nullptr)});
    auto liftedQuery = std::make_shared<Query>(
        query->location(),
        /*with=*/nullptr,
        std::make_shared<QuerySpecification>(
            spec->location(), liftedSelect, spec->from(), spec->where()));
    return planner(liftedQuery.get()).plan;
  }
};

std::vector<core::ExprPtr> toCoreExprs(const std::vector<lp::ExprApi>& args) {
  std::vector<core::ExprPtr> result;
  result.reserve(args.size());
  for (const auto& arg : args) {
    result.push_back(arg.expr());
  }
  return result;
}

// Replaces every aggregate-function `CallExpr` in `expr` with an
// `AggregateCallExpr` that has `filter` injected. Returns nullptr if
// the tree contains a pre-existing `AggregateCallExpr` or
// `WindowCallExpr` whose options can't be composed with the injected
// filter (DISTINCT, FILTER, ORDER BY, window). Caller must pass an
// expression translated outside an aggregation context; aggregates
// must appear as `CallExpr`, not as `AggregateCallExpr`.
core::ExprPtr wrapAggregatesWithFilter(
    const core::ExprPtr& expr,
    const core::ExprPtr& filter) {
  if (expr->is(core::IExpr::Kind::kAggregate) ||
      expr->is(core::IExpr::Kind::kWindow)) {
    return nullptr;
  }
  if (expr->is(core::IExpr::Kind::kCall)) {
    const auto* call = expr->as<core::CallExpr>();
    if (exec::getAggregateFunctionEntry(canonicalizeName(call->name())) !=
        nullptr) {
      return std::make_shared<core::AggregateCallExpr>(
          call->name(),
          call->inputs(),
          /*distinct=*/false,
          filter,
          std::vector<core::SortKey>{});
    }
  }
  std::vector<core::ExprPtr> newInputs;
  newInputs.reserve(expr->inputs().size());
  bool changed{false};
  for (const auto& input : expr->inputs()) {
    auto rewritten = wrapAggregatesWithFilter(input, filter);
    if (rewritten == nullptr) {
      return nullptr;
    }
    if (rewritten != input) {
      changed = true;
    }
    newInputs.push_back(std::move(rewritten));
  }
  if (!changed) {
    return expr;
  }
  return expr->replaceInputs(std::move(newInputs));
}

} // namespace

lp::ExprApi ExpressionPlanner::planSubquery(
    const SubqueryExpression* subquery,
    bool scalar) {
  auto query = subquery->query();

  if (!query->is(NodeType::kQuery)) {
    AXIOM_PRESTO_SYNTAX_FAIL(
        query->location(),
        std::string(NodeTypeName::toName(query->type())),
        "Subquery type is not supported yet");
  }

  VELOX_CHECK_NOT_NULL(
      subqueryPlanner_, "Subquery expressions require a SubqueryPlanner");

  // Look up first; if not cached, plan and insert. Do not hold an
  // iterator across subqueryPlanner_ because it may recursively plan
  // a subquery that mutates the cache and invalidates iterators.
  if (auto it = subqueryCache_.find(query); it != subqueryCache_.end()) {
    return it->second;
  }

  // Lateral column aliases belong to the enclosing SELECT block only. Clear
  // them while the subquery is planned so a bare column reference inside the
  // subquery resolves against the subquery's own FROM (and then the outer
  // scope's columns for correlation), not an outer SELECT alias.
  const auto* savedAliasExprs = aliasExprs_;
  const auto* savedColumnNames = columnNames_;
  aliasExprs_ = nullptr;
  columnNames_ = nullptr;
  SCOPE_EXIT {
    aliasExprs_ = savedAliasExprs;
    columnNames_ = savedColumnNames;
  };

  auto result = subqueryPlanner_(query->as<Query>());

  if (scalar) {
    if (auto lifted = tryLiftPureOuterAggregate(query->as<Query>(), result)) {
      return *lifted;
    }
    AXIOM_PRESTO_SEMANTIC_CHECK_EQ(
        result.plan->outputType()->size(),
        1,
        query->location(),
        "",
        "Scalar subquery must return exactly one column");
    AXIOM_PRESTO_SEMANTIC_CHECK(
        !hasOuterScopeAggregate(result.plan),
        query->location(),
        "",
        "Outer-scope aggregate could not be lifted. Lift supports a "
        "subquery whose body is `SELECT <expr> FROM <inner> [WHERE "
        "<pred>]` where every aggregate in <expr> takes only outer-"
        "column arguments and binds to the immediate parent scope.");
  } else {
    // Outer-scope aggregate inside an EXISTS body silently flips
    // EXISTS to true (the aggregation produces one row even over an
    // empty body), so the parser must reject.
    AXIOM_PRESTO_SEMANTIC_CHECK(
        !hasOuterScopeAggregate(result.plan),
        query->location(),
        "",
        "Outer-scope aggregate in an EXISTS body is not supported.");
  }

  auto expr = lp::Subquery(result.plan);
  // Correlated subqueries bake in physical column names from the outer
  // scope they were planned in; caching would let a second outer
  // context reuse a plan whose references point at the first context's
  // (now stale) names.
  if (!result.touchedOuterScope) {
    subqueryCache_.emplace(query, expr);
  }
  return expr;
}

std::optional<lp::ExprApi> ExpressionPlanner::tryLiftPureOuterAggregate(
    Query* query,
    const SubqueryPlanResult& bodyResult) {
  const auto shape = LiftablePureOuterAggregate::match(query);
  if (!shape.has_value()) {
    return std::nullopt;
  }

  // The body's planned plan must contain a top-level AggregateNode,
  // either as the root (bare aggregate) or as the immediate input of a
  // ProjectNode (aggregate wrapped in an expression).
  const lp::AggregateNode* aggregateNode = nullptr;
  if (bodyResult.plan->kind() == lp::NodeKind::kAggregate) {
    aggregateNode = bodyResult.plan->as<lp::AggregateNode>();
  } else if (
      bodyResult.plan->kind() == lp::NodeKind::kProject &&
      !bodyResult.plan->inputs().empty() &&
      bodyResult.plan->inputs().front()->kind() == lp::NodeKind::kAggregate) {
    aggregateNode = bodyResult.plan->inputs().front()->as<lp::AggregateNode>();
  }
  if (aggregateNode == nullptr || !aggregateNode->groupingKeys().empty()) {
    return std::nullopt;
  }

  // Every aggregate in the body must be a plain aggregate (no
  // DISTINCT/FILTER/ORDER BY) whose arguments reference at least one
  // outer column and no inner-source column.
  const auto& innerSource = aggregateNode->inputs().front();
  folly::F14FastSet<std::string> innerColumns(
      innerSource->outputType()->names().begin(),
      innerSource->outputType()->names().end());
  for (const auto& aggregateCall : aggregateNode->aggregates()) {
    if (aggregateCall->isDistinct() || aggregateCall->filter() != nullptr ||
        !aggregateCall->ordering().empty() || aggregateCall->inputs().empty()) {
      return std::nullopt;
    }
    bool anyInner = false;
    bool anyOuter = false;
    for (const auto& argExpr : aggregateCall->inputs()) {
      classifyColumnRefs(argExpr, innerColumns, anyInner, anyOuter);
      if (anyInner) {
        break;
      }
    }
    if (anyInner || !anyOuter) {
      return std::nullopt;
    }
  }

  auto existsCheck =
      lp::Exists(lp::Subquery(shape->planExistsBody(subqueryPlanner_)));

  // Translate the body's SELECT expression in the outer scope and
  // inject the EXISTS filter on every aggregate function call within
  // it.
  const auto& selectExpr = shape->spec->select()
                               ->selectItems()
                               .front()
                               ->as<SingleColumn>()
                               ->expression();
  auto translated = toExpr(selectExpr);
  auto wrapped =
      wrapAggregatesWithFilter(translated.expr(), existsCheck.expr());
  if (wrapped == nullptr) {
    return std::nullopt;
  }
  return lp::ExprApi(wrapped);
}

std::vector<lp::ExprApi> ExpressionPlanner::translateArgs(
    const std::vector<ExpressionPtr>& arguments,
    ExprOptions options) {
  std::vector<lp::ExprApi> args;
  args.reserve(arguments.size());
  for (const auto& arg : arguments) {
    args.push_back(toExpr(arg, options));
  }
  return args;
}

lp::ExprApi ExpressionPlanner::toAggregateCallExpr(
    const FunctionCall* call,
    const std::string& funcName,
    const std::vector<lp::ExprApi>& args,
    ExprOptions /*options*/) {
  core::ExprPtr filterExpr;
  if (call->filter() != nullptr) {
    filterExpr = toExpr(call->filter(), {}).expr();
  }

  std::vector<core::SortKey> sortKeys;
  if (call->orderBy() != nullptr) {
    const auto& sortItems = call->orderBy()->sortItems();
    for (const auto& item : sortItems) {
      VELOX_CHECK_NOT_NULL(
          sortingKeyResolver_,
          "Sorting key resolution requires a SortingKeyResolver");
      auto keyExpr = sortingKeyResolver_(item->sortKey(), {});
      sortKeys.push_back(
          {keyExpr.expr(), item->isAscending(), item->isNullsFirst()});
    }
  }

  return lp::ExprApi(
      std::make_shared<core::AggregateCallExpr>(
          funcName,
          toCoreExprs(args),
          call->isDistinct(),
          std::move(filterExpr),
          std::move(sortKeys)));
}

lp::WindowSpec ExpressionPlanner::convertWindow(
    const std::shared_ptr<Window>& window,
    ExprOptions options) {
  lp::WindowSpec spec;

  if (!window->partitionBy().empty()) {
    std::vector<lp::ExprApi> partitionKeys;
    partitionKeys.reserve(window->partitionBy().size());
    for (const auto& key : window->partitionBy()) {
      partitionKeys.push_back(toExpr(key, options));
    }
    spec.partitionBy(std::move(partitionKeys));
  }

  std::vector<lp::SortKey> orderByKeys;
  if (window->orderBy() != nullptr) {
    const auto& sortItems = window->orderBy()->sortItems();
    orderByKeys.reserve(sortItems.size());
    for (const auto& item : sortItems) {
      orderByKeys.emplace_back(
          toExpr(item->sortKey(), options),
          item->isAscending(),
          item->isNullsFirst());
    }
    spec.orderBy(orderByKeys);
  }

  if (window->frame() != nullptr) {
    const auto& frame = window->frame();

    auto startType = toWindowBoundType(frame->start()->boundType());
    std::optional<lp::ExprApi> startValue;
    if (frame->start()->value().has_value()) {
      startValue = toExpr(frame->start()->value().value(), options);
    }

    auto endType = frame->end() != nullptr
        ? toWindowBoundType(frame->end()->boundType())
        : core::WindowCallExpr::BoundType::kCurrentRow;
    std::optional<lp::ExprApi> endValue;
    if (frame->end() != nullptr && frame->end()->value().has_value()) {
      endValue = toExpr(frame->end()->value().value(), options);
    }

    // For RANGE n PRECEDING / n FOLLOWING the execution engine compares the
    // frame bound directly against the ORDER BY column, so the bound must
    // evaluate to a value of the ORDER BY type. Rewrite each offset bound as
    // <order_by> +/- <offset>: SUBTRACT when bound direction agrees with sort
    // direction (PRECEDING + ASC or FOLLOWING + DESC), ADD otherwise.
    if (frame->frameType() == WindowFrame::Type::kRange &&
        (startValue.has_value() || endValue.has_value())) {
      VELOX_USER_CHECK_EQ(
          orderByKeys.size(),
          1,
          "RANGE frame with offset bound requires exactly one ORDER BY key");
      const auto& orderKey = orderByKeys[0].expr;
      const bool ascending = orderByKeys[0].ascending;

      auto rewriteBound = [&](std::optional<lp::ExprApi>& bound,
                              core::WindowCallExpr::BoundType boundType) {
        if (!bound.has_value() ||
            (boundType != core::WindowCallExpr::BoundType::kPreceding &&
             boundType != core::WindowCallExpr::BoundType::kFollowing)) {
          return;
        }
        const bool isPreceding =
            boundType == core::WindowCallExpr::BoundType::kPreceding;
        const std::string functionName =
            (isPreceding == ascending) ? "minus" : "plus";
        bound = lp::Call(functionName, lp::ExprApi(orderKey), bound.value());
      };

      rewriteBound(startValue, startType);
      rewriteBound(endValue, endType);
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
        AXIOM_PRESTO_SYNTAX_FAIL(
            window->location(),
            "GROUPS",
            "GROUPS frame type is not supported yet");
    }
  }

  return spec;
}

} // namespace axiom::sql::presto
