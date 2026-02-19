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

#include <velox/common/base/Exceptions.h>
#include <iostream>
#include <ranges>
#include "axiom/logical_plan/ExprPrinter.h"
#include "axiom/logical_plan/PlanPrinter.h"
#include "axiom/optimizer/Filters.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/PlanUtils.h"
#include "axiom/optimizer/SubfieldTracker.h"
#include "axiom/runner/LocalRunner.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregateFunctionRegistry.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/expression/Expr.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/FunctionRegistry.h"

namespace lp = facebook::axiom::logical_plan;

namespace facebook::axiom::optimizer {
namespace {

Value toValue(const velox::TypePtr& type, float cardinality) {
  return clampCardinality(Value{toType(type), cardinality});
}

Value toConstantValue(const velox::TypePtr& type) {
  return toValue(type, 1);
}

OrderType toOrderType(lp::SortOrder sort) {
  if (sort.isAscending()) {
    return sort.isNullsFirst() ? OrderType::kAscNullsFirst
                               : OrderType::kAscNullsLast;
  }
  return sort.isNullsFirst() ? OrderType::kDescNullsFirst
                             : OrderType::kDescNullsLast;
}

/// Trace info to add to exception messages.
struct ToGraphContext {
  explicit ToGraphContext(const lp::Expr* e) : expr{e} {}

  explicit ToGraphContext(const lp::LogicalPlanNode* n) : node{n} {}

  const lp::Expr* expr{nullptr};
  const lp::LogicalPlanNode* node{nullptr};
};

std::string toGraphMessage(
    velox::VeloxException::Type exceptionType,
    void* arg) {
  auto ctx = reinterpret_cast<ToGraphContext*>(arg);
  if (ctx->expr != nullptr) {
    return fmt::format("Expr: {}", lp::ExprPrinter::toText(*ctx->expr));
  }
  if (ctx->node != nullptr) {
    return fmt::format(
        "Node: [{}] {}\n",
        ctx->node->id(),
        lp::PlanPrinter::summarizeToText(*ctx->node));
  }
  return "";
}

velox::ExceptionContext makeExceptionContext(ToGraphContext* ctx) {
  velox::ExceptionContext e;
  e.messageFunc = toGraphMessage;
  e.arg = ctx;
  return e;
}
} // namespace

ToGraph::ToGraph(
    const connector::SchemaResolver& schema,
    velox::core::ExpressionEvaluator& evaluator,
    const OptimizerOptions& options)
    : schema_{schema},
      evaluator_{evaluator},
      options_{options},
      functionNames_{queryCtx()->functionNames()} {
  auto* registry = FunctionRegistry::instance();

  const auto& reversibleFunctions = registry->reversibleFunctions();
  for (const auto& [name, reverseName] : reversibleFunctions) {
    reversibleFunctions_[toName(name)] = toName(reverseName);
    reversibleFunctions_[toName(reverseName)] = toName(name);
  }

  reversibleFunctions_[SpecialFormCallNames::kAnd] = SpecialFormCallNames::kAnd;
  reversibleFunctions_[SpecialFormCallNames::kOr] = SpecialFormCallNames::kOr;
}

void ToGraph::addDtColumn(DerivedTableP dt, std::string_view name) {
  const auto* inner = translateColumn(name);
  dt->exprs.push_back(inner);

  ColumnCP outer = nullptr;
  if (inner->isColumn() && inner->as<Column>()->relation() == dt &&
      inner->as<Column>()->outputName() == name) {
    outer = inner->as<Column>();
  } else {
    const auto* columnName = toName(name);
    outer = make<Column>(columnName, dt, inner->value(), columnName);
  }
  dt->columns.push_back(outer);
  renames_[name] = outer;
}

namespace {

std::shared_ptr<velox::core::QueryCtx> constantQueryCtx(
    const velox::core::QueryCtx& original) {
  std::atomic<int64_t> kQueryCounter;

  std::unordered_map<std::string, std::string> empty;
  return velox::core::QueryCtx::create(
      original.executor(),
      velox::core::QueryConfig(std::move(empty)),
      original.connectorSessionProperties(),
      original.cache(),
      original.pool()->shared_from_this(),
      nullptr,
      fmt::format("constant_fold:{}", ++kQueryCounter));
}

std::vector<velox::RowVectorPtr> runConstantPlan(
    PlanAndStats& veloxPlan,
    velox::memory::MemoryPool* pool) {
  auto runner = std::make_shared<runner::LocalRunner>(
      veloxPlan.plan,
      std::move(veloxPlan.finishWrite),
      constantQueryCtx(*queryCtx()->optimization()->veloxQueryCtx()));

  std::vector<velox::RowVectorPtr> results;
  while (auto rows = runner->next()) {
    VELOX_CHECK_GT(rows->size(), 0);
    results.push_back(
        std::dynamic_pointer_cast<velox::RowVector>(
            velox::BaseVector::copy(*rows, pool)));
  }
  runner->waitForCompletion(1'000'000);
  return results;
}

std::unique_ptr<connector::DiscretePredicates> allDiscreteColumns(
    const ColumnVector& columns,
    const connector::TableLayout& layout) {
  const auto& discreteColumns = layout.discretePredicateColumns();
  if (discreteColumns.empty()) {
    return {nullptr, {}};
  }

  folly::F14FastMap<std::string_view, const connector::Column*>
      discreteColumnMap;
  for (const auto* column : discreteColumns) {
    discreteColumnMap.emplace(column->name(), column);
  }

  std::vector<const connector::Column*> connectorColumns;
  connectorColumns.reserve(columns.size());

  for (auto* column : columns) {
    auto it = discreteColumnMap.find(column->schemaColumn()->name());
    if (it == discreteColumnMap.end()) {
      return {nullptr, {}};
    }

    connectorColumns.emplace_back(it->second);
  }

  return layout.discretePredicates(connectorColumns);
}

velox::RowTypePtr toRowType(const ColumnVector& columns) {
  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;
  names.reserve(columns.size());
  types.reserve(columns.size());
  for (auto* column : columns) {
    names.emplace_back(column->name());
    types.emplace_back(toTypePtr(column->value().type));
  }

  return ROW(std::move(names), std::move(types));
}

std::vector<velox::Variant> toValues(
    connector::DiscretePredicates& discretePredicates) {
  std::vector<velox::Variant> valueRows;
  for (;;) {
    auto rows = discretePredicates.next();
    if (rows.empty()) {
      break;
    }

    valueRows.reserve(valueRows.size() + rows.size());

    for (auto& row : rows) {
      valueRows.emplace_back(std::move(row));
    }
  }

  return valueRows;
}

// Constant folds a derived table that represents global aggregation over a base
// table and uses only discrete-predicate columns. In addition, aggregate
// functions must ignore duplicate inputs or aggregation must be over distinct
// inputs (e.g. max(x) or agg(distinct x)).
lp::ValuesNodePtr tryFoldConstantDt(
    DerivedTableP dt,
    velox::memory::MemoryPool* pool) {
  if (dt->tables.size() > 1 || !dt->tables[0]->is(PlanType::kTableNode)) {
    return nullptr;
  }

  if (!dt->hasAggregation() || !dt->aggregation->groupingKeys().empty()) {
    return nullptr;
  }

  // DT has a single BaseTable with a global aggregation.
  const auto* aggPlan = dt->aggregation;

  // Check if aggregation ignores duplicate inputs.
  for (const auto* agg : aggPlan->aggregates()) {
    if (!agg->functions().contains(FunctionSet::kIgnoreDuplicatesAggregate) &&
        !agg->isDistinct()) {
      return nullptr;
    }
  }

  // Check if aggregation uses only 'discretePredicate' columns.
  auto* baseTable = dt->tables[0]->as<BaseTable>();

  std::unique_ptr<connector::DiscretePredicates> discretePredicates;

  for (auto* layout : baseTable->schemaTable->connectorTable->layouts()) {
    if (auto predicates = allDiscreteColumns(baseTable->columns, *layout)) {
      discretePredicates = std::move(predicates);
      break;
    }
  }

  if (discretePredicates == nullptr) {
    return nullptr;
  }

  dt->distributeConjuncts();

  VELOX_CHECK(dt->conjuncts.empty());
  VELOX_CHECK_NULL(dt->write);

  // TODO Remove this check to allow SELECT count(1) FROM (SELECT distinct ds
  // FROM t) queries.
  VELOX_CHECK(!dt->columns.empty());

  if (dt->hasOrderBy() || dt->hasLimit()) {
    // TODO Add support for these. Order-by can be ignored. Global agg produces
    // a single row, hence, no need to sort. Limit >= 1 can be ignored as well.
    // Limit 0 should be optimized.
    return nullptr;
  }

  // Create and run Velox plan.
  const auto values = toValues(*discretePredicates);
  auto* valuesTable =
      make<ValuesTable>(toType(toRowType(baseTable->columns)), &values);
  valuesTable->cname = dt->cname;
  valuesTable->columns = baseTable->columns;

  RelationOpPtr plan = make<Values>(*valuesTable, valuesTable->columns);

  if (!baseTable->columnFilters.empty() || !baseTable->filter.empty()) {
    auto combinedFilters = baseTable->columnFilters;
    if (!baseTable->filter.empty()) {
      combinedFilters.reserve(
          baseTable->columnFilters.size() + baseTable->filter.size());
      combinedFilters.insert(
          combinedFilters.end(),
          baseTable->filter.begin(),
          baseTable->filter.end());
    }
    plan = make<Filter>(plan, combinedFilters);
  }

  plan = Optimization::planSingleAggregation(dt, plan);

  if (!dt->having.empty()) {
    plan = make<Filter>(plan, dt->having);
  }

  if (!Project::isRedundant(plan, dt->exprs, dt->columns)) {
    plan = make<Project>(
        plan,
        dt->exprs,
        dt->columns,
        /*redundantProject=*/false);
  }

  auto veloxPlan = queryCtx()->optimization()->toVeloxPlan(plan);
  auto results = runConstantPlan(veloxPlan, pool);
  if (results.empty()) {
    VELOX_CHECK_EQ(1, veloxPlan.plan->fragments().size());
    const auto& rowType =
        veloxPlan.plan->fragments().front().fragment.planNode->outputType();
    return std::make_shared<lp::ValuesNode>(
        dt->cname, rowType, std::vector<velox::Variant>{});
  }

  return std::make_shared<lp::ValuesNode>(dt->cname, std::move(results));
}

} // namespace

void ToGraph::setDtUsedOutput(
    DerivedTableP dt,
    const lp::LogicalPlanNode& node) {
  const auto& type = *node.outputType();
  for (auto i : usedChannels(node)) {
    addDtColumn(dt, type.nameOf(i));
  }
}

std::vector<int32_t> ToGraph::usedChannels(const lp::LogicalPlanNode& node) {
  const auto& control = controlSubfields_.nodeFields[&node];
  const auto& payload = payloadSubfields_.nodeFields[&node];
  std::vector<int32_t> result;
  std::ranges::set_union(
      control.resultPaths | std::views::keys,
      payload.resultPaths | std::views::keys,
      std::back_inserter(result));
  return result;
}

namespace {
bool isConstantTrue(ExprCP expr) {
  if (expr->isNot(PlanType::kLiteralExpr)) {
    return false;
  }

  const auto& variant = expr->as<Literal>()->literal();
  return variant.kind() == velox::TypeKind::BOOLEAN && !variant.isNull() &&
      variant.value<bool>();
}
} // namespace

void ToGraph::translateConjuncts(const lp::ExprPtr& input, ExprVector& flat) {
  if (!input) {
    return;
  }
  if (isSpecialForm(input, lp::SpecialForm::kAnd)) {
    for (auto& child : input->inputs()) {
      translateConjuncts(child, flat);
    }
  } else {
    auto translatedExpr = translateExpr(input);
    if (!isConstantTrue(translatedExpr)) {
      flat.push_back(translatedExpr);
    }
  }
}

lp::ConstantExprPtr ToGraph::tryFoldConstant(const lp::ExprPtr& expr) {
  if (expr->isConstant()) {
    return std::static_pointer_cast<const lp::ConstantExpr>(expr);
  }

  if (expr->looksConstant()) {
    auto literal = translateExpr(expr);
    if (literal->is(PlanType::kLiteralExpr)) {
      return std::make_shared<lp::ConstantExpr>(
          toTypePtr(literal->value().type),
          std::make_shared<velox::Variant>(literal->as<Literal>()->literal()));
    }
  }
  return nullptr;
}

ExprCP ToGraph::tryFoldConstant(
    const velox::TypePtr& returnType,
    std::string_view callName,
    const ExprVector& literals) {
  try {
    auto* call = make<Call>(
        toName(callName), toConstantValue(returnType), literals, FunctionSet());
    auto typedExpr = queryCtx()->optimization()->toTypedExpr(call);
    auto exprSet = evaluator_.compile(typedExpr);
    const auto& first = *exprSet->exprs().front();
    if (!first.isConstant()) {
      return nullptr;
    }
    const auto& constantExpr =
        static_cast<const velox::exec::ConstantExpr&>(first);
    auto typed = std::make_shared<lp::ConstantExpr>(
        constantExpr.type(),
        std::make_shared<velox::Variant>(constantExpr.value()->variantAt(0)));
    return makeConstant(*typed);
  } catch (const std::exception&) {
    // Swallow exception.
  }

  return nullptr;
}

bool ToGraph::isSubfield(
    const lp::ExprPtr& expr,
    Step& step,
    lp::ExprPtr& input) {
  if (isSpecialForm(expr, lp::SpecialForm::kDereference)) {
    step.kind = StepKind::kField;
    auto maybeIndex =
        maybeIntegerLiteral(expr->inputAt(1)->as<lp::ConstantExpr>());
    Name name = nullptr;
    int64_t id = 0;
    auto& rowType = expr->inputAt(0)->type()->as<velox::TypeKind::ROW>();
    if (maybeIndex.has_value()) {
      id = maybeIndex.value();
    } else {
      auto& field = expr->inputAt(1)->as<lp::ConstantExpr>()->value();
      name = toName(field->value<velox::TypeKind::VARCHAR>());
      id = rowType.getChildIdx(name);
    }
    step.field = name;
    step.id = id;
    input = expr->inputAt(0);
    return true;
  }

  if (expr->isCall()) {
    const auto* call = expr->as<lp::CallExpr>();
    auto name = toName(call->name());
    if (name == functionNames_.subscript || name == functionNames_.elementAt) {
      auto subscript = translateExpr(call->inputAt(1));
      if (subscript->is(PlanType::kLiteralExpr)) {
        step.kind = StepKind::kSubscript;
        auto& literal = subscript->as<Literal>()->literal();
        switch (subscript->value().type->kind()) {
          case velox::TypeKind::VARCHAR:
            step.field = toName(literal.value<velox::TypeKind::VARCHAR>());
            break;
          case velox::TypeKind::BIGINT:
          case velox::TypeKind::INTEGER:
          case velox::TypeKind::SMALLINT:
          case velox::TypeKind::TINYINT:
            step.id = integerValue(&literal);
            break;
          default:
            VELOX_UNREACHABLE();
        }
        input = expr->inputAt(0);
        return true;
      }
      return false;
    }
    if (name == functionNames_.cardinality) {
      step.kind = StepKind::kCardinality;
      input = expr->inputAt(0);
      return true;
    }
  }
  return false;
}

void ToGraph::getExprForField(
    const lp::InputReferenceExpr* field,
    lp::ExprPtr& resultExpr,
    ColumnCP& resultColumn,
    const lp::LogicalPlanNode*& context) {
  VELOX_CHECK_NOT_NULL(context);

  auto lookupName = [&](const std::string& name) -> ExprCP {
    auto it = renames_.find(name);
    if (it != renames_.end()) {
      return it->second;
    }

    if (allowCorrelations_ && correlations_ != nullptr) {
      if (auto it = correlations_->find(name); it != correlations_->end()) {
        return it->second;
      }
    }

    return nullptr;
  };

  while (context) {
    const auto& name = field->name();

    const auto ordinal = context->outputType()->getChildIdx(name);
    if (context->is(lp::NodeKind::kProject)) {
      const auto* project = context->as<lp::ProjectNode>();
      auto& def = project->expressions()[ordinal];
      context = context->inputAt(0).get();
      if (def->isInputReference()) {
        field = def->as<lp::InputReferenceExpr>();
        continue;
      }
      resultExpr = def;
      return;
    }

    if (context->is(lp::NodeKind::kAggregate)) {
      auto it = renames_.find(name);
      VELOX_CHECK(it != renames_.end());
      VELOX_CHECK(it->second->is(PlanType::kColumnExpr));
      resultColumn = it->second->as<Column>();
      resultExpr = nullptr;
      return;
    }

    // This is a band-aid. Revisit the logic in this method.
    //
    // The problem with current logic is that translated expression may belong
    // to a derived table that's not directly referenced from the currentDt_.
    // When this happens, the downstream processing breaks because the core
    // invariant is broken: all expressions in a dericed table must reference
    // relations from DerivedTable::tables.
    {
      if (auto expr = lookupName(name)) {
        if (expr != nullptr && expr->is(PlanType::kColumnExpr)) {
          resultColumn = expr->as<Column>();
          resultExpr = nullptr;
          return;
        }
      }
    }

    const auto& sources = context->inputs();

    const bool checkInContext = [&] {
      if (context->is(lp::NodeKind::kUnnest)) {
        const auto* unnest = context->as<lp::UnnestNode>();
        return ordinal >= unnest->onlyInput()->outputType()->size();
      }
      return sources.empty();
    }();

    if (checkInContext) {
      const auto* leaf = findLeaf(context);
      auto it = renames_.find(name);
      VELOX_CHECK(it != renames_.end());
      const auto* maybeColumn = it->second;
      VELOX_CHECK(maybeColumn->is(PlanType::kColumnExpr));
      resultColumn = maybeColumn->as<Column>();
      resultExpr = nullptr;
      context = nullptr;
      const auto* relation = resultColumn->relation();
      VELOX_CHECK_NOT_NULL(relation);
      if (relation->is(PlanType::kTableNode) ||
          relation->is(PlanType::kValuesTableNode) ||
          relation->is(PlanType::kUnnestTableNode)) {
        VELOX_CHECK(leaf == relation);
      }
      return;
    }

    context = nullptr;
    for (const auto& source : sources) {
      const auto& row = source->outputType();
      if (auto maybe = row->getChildIdxIfExists(name)) {
        context = source.get();
        break;
      }
    }

    VELOX_CHECK_NOT_NULL(context, "Cannot find source for column: {}", name);
  }
  VELOX_FAIL();
}

std::optional<ExprCP> ToGraph::translateSubfield(const lp::ExprPtr& inputExpr) {
  std::vector<Step> steps;
  const lp::LogicalPlanNode* source = nullptr;
  auto expr = inputExpr;

  for (;;) {
    lp::ExprPtr input;
    Step step;
    VELOX_CHECK_NOT_NULL(expr);
    bool isStep = isSubfield(expr, step, input);
    if (!isStep) {
      if (steps.empty()) {
        return std::nullopt;
      }

      // If this is a field we follow to the expr assigning the field if any.
      ColumnCP column = nullptr;
      if (expr->isInputReference()) {
        const auto* field = expr->as<lp::InputReferenceExpr>();

        if (auto it = lambdaSignature_.find(field->name());
            it != lambdaSignature_.end()) {
          column = it->second;
          expr = nullptr;
        } else {
          if (source == nullptr) {
            const auto& name = field->name();

            for (const auto* exprSource : exprSources_) {
              if (exprSource->outputType()->getChildIdxIfExists(name)) {
                source = exprSource;
                break;
              }
            }
          }
          VELOX_CHECK_NOT_NULL(source);
          getExprForField(field, expr, column, source);
          if (expr) {
            continue;
          }
        }
      }

      SubfieldProjections* skyline = nullptr;
      if (column) {
        auto it = allColumnSubfields_.find(column);
        if (it != allColumnSubfields_.end()) {
          skyline = &it->second;
        }
      } else {
        skyline = ensureFunctionSubfields(expr);
      }

      // 'steps is a path. 'skyline' is a map from path to Expr. If no prefix
      // of steps occurs in skyline, then the item referenced by steps is not
      // materialized. Otherwise, the prefix that matches one in skyline is
      // replaced by the Expr from skyline and the tail of 'steps' are tagged
      // on the Expr. If skyline is empty, then 'steps' simply becomes a
      // nested sequence of getters.
      auto originalExprSources = exprSources_;
      SCOPE_EXIT {
        exprSources_ = originalExprSources;
      };

      // 'source' can be null if 'inputExpr' is a subfield over a function call.
      if (source != nullptr) {
        exprSources_ = {source};
      }

      return makeGettersOverSkyline(steps, skyline, expr, column);
    }
    steps.push_back(step);
    expr = input;
  }
}

namespace {

velox::Variant* subscriptLiteral(velox::TypeKind kind, const Step& step) {
  switch (kind) {
    case velox::TypeKind::VARCHAR:
      return registerVariant(std::string{step.field});
    case velox::TypeKind::BIGINT:
      return registerVariant(static_cast<int64_t>(step.id));
    case velox::TypeKind::INTEGER:
      return registerVariant(static_cast<int32_t>(step.id));
    case velox::TypeKind::SMALLINT:
      return registerVariant(static_cast<int16_t>(step.id));
    case velox::TypeKind::TINYINT:
      return registerVariant(static_cast<int8_t>(step.id));
    default:
      VELOX_FAIL("Unsupported key type");
  }
}

ExprCP FOLLY_NULLABLE intersectWithSkyline(
    std::span<const Step> steps,
    const SubfieldProjections& skyline,
    int32_t& last) {
  // We see how many trailing (inner) steps fall below skyline, i.e. address
  // enclosing containers that are not materialized.

  last = static_cast<int32_t>(steps.size() - 1);
  for (; last >= 0; --last) {
    auto inner = toPath(steps.subspan(last), /*reverse=*/true);
    auto it = skyline.pathToExpr.find(inner);
    if (it != skyline.pathToExpr.end()) {
      return it->second;
    }
  }

  // The path is not materialized. Need a longer path to intersect skyline.
  return nullptr;
}

} // namespace

ExprCP ToGraph::makeGettersOverSkyline(
    std::span<const Step> steps,
    const SubfieldProjections* skyline,
    const lp::ExprPtr& base,
    ColumnCP column) {
  if (skyline) {
    int32_t last;
    if (auto expr = intersectWithSkyline(steps, *skyline, last)) {
      return makeGetters(std::span(steps).subspan(0, last), expr);
    }

    // The path is not materialized. Need a longer path to intersect skyline.
    return nullptr;
  }

  ExprCP expr;
  if (column) {
    expr = column;
  } else {
    trace(OptimizerOptions::kPreprocess, [&]() {
      std::cout << "Complex function with no skyline: steps="
                << toPath(steps)->toString() << std::endl;
      std::cout << "base=" << lp::ExprPrinter::toText(*base) << std::endl;
    });
    expr = translateExpr(base);
  }

  return makeGetters(steps, expr);
}

ExprCP ToGraph::makeGetters(std::span<const Step> steps, ExprCP base) {
  ExprCP expr = base;
  for (int32_t i = steps.size() - 1; i >= 0; --i) {
    const auto& step = steps[i];

    // We make a getter over expr made so far with 'steps[i]' as first.
    PathExpr pathExpr{step, expr};
    auto it = deduppedGetters_.find(pathExpr);
    if (it != deduppedGetters_.end()) {
      expr = it->second;
    } else {
      expr = makeGetter(step, expr);
      deduppedGetters_[pathExpr] = expr;
    }
  }

  return expr;
}

ExprCP ToGraph::makeGetter(const Step& step, ExprCP base) {
  const auto& inputType = base->value().type;
  switch (step.kind) {
    case StepKind::kField: {
      if (step.field) {
        auto childType = toType(inputType->asRow().findChild(step.field));
        return make<Field>(childType, base, step.field);
      } else {
        auto childType = toType(inputType->childAt(step.id));
        return make<Field>(childType, base, step.id);
      }
    }

    case StepKind::kSubscript: {
      // Type of array element or map value.
      auto valueType = inputType->childAt(inputType->isArray() ? 0 : 1);

      // Type of array index or map key.
      auto subscriptType =
          inputType->isArray() ? velox::INTEGER() : inputType->childAt(0);

      ExprVector args{
          base,
          make<Literal>(
              toConstantValue(subscriptType),
              subscriptLiteral(subscriptType->kind(), step)),
      };

      return make<Call>(
          functionNames_.subscript,
          toConstantValue(valueType),
          std::move(args),
          FunctionSet());
    }

    case StepKind::kCardinality: {
      return make<Call>(
          functionNames_.cardinality,
          toConstantValue(velox::BIGINT()),
          ExprVector{base},
          FunctionSet());
    }
    default:
      VELOX_NYI();
  }
}

PathSet ToGraph::functionSubfields(const lp::CallExpr* call) {
  PathSet subfields;
  if (auto maybe = payloadSubfields_.findSubfields(call)) {
    subfields = maybe.value();
  }

  if (auto maybe = controlSubfields_.findSubfields(call)) {
    subfields.unionSet(maybe.value());
  }

  Path::subfieldSkyline(subfields);
  return subfields;
}

SubfieldProjections* ToGraph::ensureFunctionSubfields(const lp::ExprPtr& expr) {
  if (expr->isCall()) {
    const auto* call = expr->as<lp::CallExpr>();
    if (functionMetadata(velox::exec::sanitizeName(call->name()))) {
      if (!translatedSubfieldFuncs_.contains(call)) {
        translateExpr(expr);
      }
    }

    auto it = functionSubfields_.find(call);
    if (it != functionSubfields_.end()) {
      return &it->second;
    }
  }

  return nullptr;
}

namespace {

// If we should reverse the sides of a binary expression to canonicalize it. We
// invert in two cases:
//
//  #1. If there is a literal in the left and something else in the right:
//    f("literal", col) => f(col, "literal")
//
//  #2. If none are literal, but the id on the left is higher.
bool shouldInvert(ExprCP left, ExprCP right) {
  if (left->is(PlanType::kLiteralExpr) &&
      right->isNot(PlanType::kLiteralExpr)) {
    return true;
  }

  if (left->isNot(PlanType::kLiteralExpr) &&
      right->isNot(PlanType::kLiteralExpr) && (left->id() > right->id())) {
    return true;
  }

  return false;
}

} // namespace

void ToGraph::canonicalizeCall(Name& name, ExprVector& args) {
  if (args.size() != 2) {
    return;
  }

  auto it = reversibleFunctions_.find(name);
  if (it == reversibleFunctions_.end()) {
    return;
  }

  if (shouldInvert(args[0], args[1])) {
    std::swap(args[0], args[1]);
    name = it->second;
  }
}

ExprCP ToGraph::deduppedCall(
    Name name,
    Value value,
    ExprVector args,
    FunctionSet flags) {
  canonicalizeCall(name, args);
  ExprDedupKey key = {name, args};

  auto [it, emplaced] = functionDedup_.try_emplace(key);
  if (it->second) {
    return it->second;
  }
  auto* call = make<Call>(name, value, std::move(args), flags);
  if (emplaced && !call->containsNonDeterministic()) {
    it->second = call;
  }
  return call;
}

namespace {

bool isJoinEquality(
    const FunctionNames& funcs,
    ExprCP expr,
    PlanObjectCP leftTable,
    PlanObjectCP rightTable,
    ExprCP& left,
    ExprCP& right) {
  if (expr->is(PlanType::kCallExpr)) {
    auto call = expr->as<Call>();
    if (call->name() == funcs.equality) {
      left = call->argAt(0);
      right = call->argAt(1);

      auto* lt = left->singleTable();
      auto* rt = right->singleTable();

      if (lt == rightTable && rt == leftTable) {
        std::swap(left, right);
        return true;
      }

      if (lt == leftTable && rt == rightTable) {
        return true;
      }
    }
  }
  return false;
}

bool areAllJoinEqualities(
    const FunctionNames& funcs,
    const ExprVector& conjuncts) {
  for (const auto* conjunct : conjuncts) {
    const auto tables = conjunct->allTables().toObjects();
    ExprCP left = nullptr;
    ExprCP right = nullptr;
    if (!isJoinEquality(funcs, conjunct, tables[0], tables[1], left, right)) {
      return false;
    }
  }
  return true;
}

} // namespace

bool ToGraph::isJoinEquality(
    ExprCP expr,
    PlanObjectCP leftTable,
    PlanObjectCP rightTable,
    ExprCP& left,
    ExprCP& right) const {
  return optimizer::isJoinEquality(
      functionNames_, expr, leftTable, rightTable, left, right);
}

ExprCP ToGraph::makeConstant(const lp::ConstantExpr& constant) {
  TypedVariant temp{toType(constant.type()), constant.value()};
  auto it = constantDedup_.find(temp);
  if (it != constantDedup_.end()) {
    return it->second;
  }

  Value value(temp.type, 1);
  // For scalar types, set min and max to the literal value
  if (temp.type->isPrimitiveType()) {
    // Scalar/primitive type - set min and max to the variant
    value.min = temp.value.get();
    value.max = temp.value.get();
  }

  auto* literal = make<Literal>(value, temp.value.get());

  constantDedup_[std::move(temp)] = literal;
  return literal;
}

namespace {
// Returns bits describing function 'name'.
FunctionSet functionBits(Name name, bool specialForm) {
  if (auto* md = functionMetadata(name)) {
    return md->functionSet;
  }

  FunctionSet bits;

  if (specialForm) {
    bits = bits | FunctionSet::kNonDefaultNullBehavior;
  } else {
    const auto deterministic = velox::isDeterministic(name);
    VELOX_CHECK(deterministic.has_value(), "Function not found: {}", name);
    if (!deterministic.value()) {
      bits = bits | FunctionSet::kNonDeterministic;
    }

    const auto defaultNullBehavior = velox::isDefaultNullBehavior(name);
    VELOX_CHECK(
        defaultNullBehavior.has_value(), "Function not found: {}", name);
    if (!defaultNullBehavior.value()) {
      bits = bits | FunctionSet::kNonDefaultNullBehavior;
    }
  }

  return bits;
}

} // namespace

ExprCP ToGraph::translateExpr(const lp::ExprPtr& expr) {
  if (expr->isInputReference()) {
    return translateColumn(expr->as<lp::InputReferenceExpr>()->name());
  }

  if (expr->isConstant()) {
    return makeConstant(*expr->as<lp::ConstantExpr>());
  }

  if (auto path = translateSubfield(expr)) {
    return path.value();
  }

  if (expr->isLambda()) {
    return translateLambda(expr->as<lp::LambdaExpr>());
  }

  auto it = subqueries_.find(expr);
  if (it != subqueries_.end()) {
    return it->second;
  }

  ToGraphContext ctx(expr.get());
  velox::ExceptionContextSetter exceptionContext(makeExceptionContext(&ctx));

  const auto* call = expr->isCall() ? expr->as<lp::CallExpr>() : nullptr;
  std::string callName;
  if (call) {
    callName = velox::exec::sanitizeName(call->name());
    auto* metadata = functionMetadata(callName);
    if (metadata && metadata->processSubfields()) {
      auto translated = translateSubfieldFunction(call, metadata);
      if (translated.has_value()) {
        return translated.value();
      }
    }
  }

  const auto* specialForm =
      expr->isSpecialForm() ? expr->as<lp::SpecialFormExpr>() : nullptr;

  if (call || specialForm) {
    FunctionSet funcs;
    const auto& inputs = expr->inputs();
    ExprVector args;
    args.reserve(inputs.size());
    float cardinality = 1;
    bool allConstant = true;

    for (const auto& input : inputs) {
      auto arg = translateExpr(input);
      args.emplace_back(arg);
      allConstant &= arg->is(PlanType::kLiteralExpr);
      cardinality = std::max(cardinality, arg->value().cardinality);
      if (arg->is(PlanType::kCallExpr)) {
        funcs = funcs | arg->as<Call>()->functions();
      }
    }

    auto name = call ? toName(callName)
                     : SpecialFormCallNames::toCallName(specialForm->form());
    if (allConstant) {
      if (auto literal = tryFoldConstant(expr->type(), name, args)) {
        return literal;
      }
    }

    auto* exprType = toType(expr->type());

    // Drop redundant cast.
    //    CAST(x as t) ==> x if typeof(x) == t.
    if (specialForm && specialForm->form() == lp::SpecialForm::kCast) {
      if (args[0]->value().type == exprType) {
        return args[0];
      }
    }

    funcs = funcs | functionBits(name, specialForm != nullptr);
    auto* callExpr = deduppedCall(
        name, Value(exprType, cardinality), std::move(args), funcs);
    return callExpr;
  }

  VELOX_NYI();
  return nullptr;
}

ExprCP ToGraph::translateLambda(const lp::LambdaExpr* lambda) {
  const auto& signature = *lambda->signature();
  auto lambdaSignature = lambdaSignature_;
  SCOPE_EXIT {
    lambdaSignature_ = std::move(lambdaSignature);
  };
  ColumnVector args;
  args.reserve(signature.size());
  for (uint32_t i = 0; i < signature.size(); ++i) {
    const auto& name = signature.nameOf(i);
    const auto* column = make<Column>(
        toName(name), nullptr, toConstantValue(signature.childAt(i)));
    args.push_back(column);
    lambdaSignature_[name] = column;
  }
  const auto* body = translateExpr(lambda->body());
  return make<Lambda>(std::move(args), toType(lambda->type()), body);
}

namespace {

constexpr uint64_t kAllAllowedInDt = ~uint64_t{0};

// Returns a mask that allows 'op' in the same derived table.
constexpr uint64_t allow(lp::NodeKind op) {
  return uint64_t{1} << static_cast<uint64_t>(op);
}

// True if 'op' is in 'mask.
constexpr bool contains(uint64_t mask, lp::NodeKind op) {
  return mask & allow(op);
}

// Removes 'op' from the set of operators allowed in the current derived
// table. makeQueryGraph() starts a new derived table if it finds an operator
// that does not belong to the mask.
template <typename... T>
constexpr uint64_t deny(uint64_t mask, T... op) {
  return (mask & ... & ~allow(op));
}

} // namespace

std::optional<ExprCP> ToGraph::translateSubfieldFunction(
    const lp::CallExpr* call,
    const FunctionMetadata* metadata) {
  translatedSubfieldFuncs_.insert(call);

  auto subfields = functionSubfields(call);
  if (subfields.empty()) {
    // The function is accessed as a whole.
    return std::nullopt;
  }

  std::vector<PathCP> paths;
  subfields.forEachPath([&](PathCP path) { paths.push_back(path); });

  PathSet usedArgs;
  bool allUsed = false;

  const auto& argOrginal = metadata->argOrdinal;
  if (argOrginal.empty()) {
    allUsed = true;
  } else {
    for (auto i = 0; i < paths.size(); ++i) {
      if (std::find(argOrginal.begin(), argOrginal.end(), i) ==
          argOrginal.end()) {
        // This argument is not a source of subfields over some field
        // of the return value. Compute this in any case.
        usedArgs.add(i);
        continue;
      }

      const auto& step = paths[i]->steps()[0];
      if (auto maybeArg = SubfieldTracker::stepToArg(step, metadata)) {
        usedArgs.add(maybeArg.value());
      }
    }
  }

  const auto& inputs = call->inputs();
  ExprVector args(inputs.size());
  float cardinality = 1;
  FunctionSet funcs;
  for (auto i = 0; i < inputs.size(); ++i) {
    const auto& input = inputs[i];
    if (allUsed || usedArgs.contains(i)) {
      args[i] = translateExpr(input);
      cardinality = std::max(cardinality, args[i]->value().cardinality);
      if (args[i]->is(PlanType::kCallExpr)) {
        funcs = funcs | args[i]->as<Call>()->functions();
      }
    } else {
      // Make a null of the type for the unused arg to keep the tree valid.
      const auto& inputType = input->type();
      args[i] = make<Literal>(
          toConstantValue(inputType), registerVariant(inputType->kind()));
    }
  }

  auto* name = toName(velox::exec::sanitizeName(call->name()));
  funcs = funcs | functionBits(name, /*specialForm=*/false);

  if (metadata->explode) {
    auto map = metadata->explode(call, paths);
    folly::F14FastMap<PathCP, ExprCP> translated;
    for (const auto& [path, expr] : map) {
      translated[path] = translateExpr(expr);
    }

    trace(OptimizerOptions::kPreprocess, [&]() {
      std::cout << "Explode=" << lp::ExprPrinter::toText(*call) << std::endl;
      std::cout << "num paths=" << paths.size() << std::endl;
      std::cout << "translated=" << map.size() << std::endl;
      if (!translated.empty()) {
        std::cout << "Set function skyline=" << translated.size() << " "
                  << map.size() << std::endl;
      }
    });

    if (!translated.empty()) {
      functionSubfields_[call] =
          SubfieldProjections{.pathToExpr = std::move(translated)};
      return nullptr;
    }
  }
  auto* callExpr =
      make<Call>(name, toValue(call->type(), cardinality), args, funcs);
  return callExpr;
}

ExprCP ToGraph::translateColumn(std::string_view name) const {
  if (auto it = lambdaSignature_.find(name); it != lambdaSignature_.end()) {
    return it->second;
  }

  if (auto it = renames_.find(name); it != renames_.end()) {
    return it->second;
  }

  if (allowCorrelations_ && correlations_ != nullptr) {
    if (auto it = correlations_->find(name); it != correlations_->end()) {
      return it->second;
    }
  }

  VELOX_FAIL("Cannot resolve column name: {}", name);
}

ExprVector ToGraph::translateExprs(const std::vector<lp::ExprPtr>& source) {
  ExprVector result{source.size()};
  for (auto i = 0; i < source.size(); ++i) {
    result[i] = translateExpr(source[i]); // NOLINT
  }
  return result;
}

void ToGraph::addUnnest(const lp::UnnestNode& unnest) {
  exprSources_.push_back(unnest.onlyInput().get());
  SCOPE_EXIT {
    exprSources_.pop_back();
  };

  auto* unnestDt = currentDt_;
  const bool needsSeparateUnnest = unnestDt->hasAggregation() ||
      unnestDt->hasOrderBy() || unnestDt->hasLimit();
  if (needsSeparateUnnest) {
    finalizeDt(*unnest.onlyInput());
  }

  PlanObjectCP leftTable = nullptr;
  ExprVector unnestExprs;
  unnestExprs.reserve(unnest.unnestExpressions().size());
  float maxCardinality = 0;
  for (size_t i = 0; i < unnest.unnestExpressions().size(); ++i) {
    const auto* unnestExpr = translateExpr(unnest.unnestExpressions()[i]);
    unnestExprs.push_back(unnestExpr);
    if (i == 0) {
      leftTable = unnestExpr->singleTable();
    } else if (leftTable && leftTable != unnestExpr->singleTable()) {
      leftTable = nullptr;
    }
    maxCardinality = std::max(maxCardinality, unnestExpr->value().cardinality);
  }

  if (!leftTable) {
    leftTable = unnestDt;
    if (!needsSeparateUnnest) {
      finalizeDt(*unnest.onlyInput());
      unnestDt->exportExprs(unnestExprs);
    }
  }

  auto* unnestTable = make<UnnestTable>();
  unnestTable->cname = newCName("ut");
  unnestTable->columns.reserve(
      unnest.outputType()->size() - unnest.onlyInput()->outputType()->size());
  for (size_t i = 0; i < unnestExprs.size(); ++i) {
    const auto* unnestExpr = unnestExprs[i];
    const auto& unnestedNames = unnest.unnestedNames()[i];
    for (size_t j = 0; j < unnestedNames.size(); ++j) {
      const auto& unnestedType = unnestExpr->value().type->childAt(j);
      // TODO Value cardinality also should be multiplied by the max from all
      // columns average expected number of elements per unnested element.
      // Other Value properties also should be computed.
      const auto* columnName = toName(unnestedNames[j]);
      auto* column = make<Column>(
          columnName,
          unnestTable,
          toValue(unnestedType, maxCardinality),
          columnName);
      unnestTable->columns.push_back(column);
      renames_[columnName] = column;
    }
  }
  if (unnest.ordinalityName().has_value()) {
    auto channels = usedChannels(unnest);
    // Add the ordinality column; if it is needed for further operations.
    if (!channels.empty() &&
        channels.back() == unnest.outputType()->size() - 1) {
      const auto* columnName = toName(unnest.ordinalityName().value());
      auto* column = make<Column>(
          columnName,
          unnestTable,
          toValue(velox::BIGINT(), maxCardinality),
          columnName);
      renames_[columnName] = unnestTable->ordinalityColumn = column;
    }
  }

  auto* edge =
      JoinEdge::makeUnnest(leftTable, unnestTable, std::move(unnestExprs));

  planLeaves_[&unnest] = unnestTable;
  currentDt_->addTable(unnestTable);
  currentDt_->joins.push_back(edge);
}

namespace {
struct AggregateDedupKey {
  Name func;
  bool isDistinct;
  ExprCP condition;
  std::span<const ExprCP> args;
  std::span<const ExprCP> orderKeys;
  std::span<const OrderType> orderTypes;

  bool operator==(const AggregateDedupKey& other) const {
    return func == other.func && isDistinct == other.isDistinct &&
        condition == other.condition && std::ranges::equal(args, other.args) &&
        std::ranges::equal(orderKeys, other.orderKeys) &&
        std::ranges::equal(orderTypes, other.orderTypes);
  }
};

struct AggregateDedupHasher {
  size_t operator()(const AggregateDedupKey& key) const {
    size_t hash =
        folly::hasher<uintptr_t>()(reinterpret_cast<uintptr_t>(key.func));

    hash = velox::bits::hashMix(hash, folly::hasher<bool>()(key.isDistinct));

    if (key.condition != nullptr) {
      hash = velox::bits::hashMix(hash, folly::hasher<ExprCP>()(key.condition));
    }

    for (auto& a : key.args) {
      hash = velox::bits::hashMix(hash, folly::hasher<ExprCP>()(a));
    }

    for (auto& k : key.orderKeys) {
      hash = velox::bits::hashMix(hash, folly::hasher<ExprCP>()(k));
    }

    for (auto& t : key.orderTypes) {
      hash = velox::bits::hashMix(hash, folly::hasher<OrderType>()(t));
    }

    return hash;
  }
};
} // namespace

AggregationPlanCP ToGraph::translateAggregation(const lp::AggregateNode& agg) {
  const auto& input = *agg.onlyInput();

  exprSources_.push_back(&input);
  SCOPE_EXIT {
    exprSources_.pop_back();
  };

  for (const auto& key : agg.groupingKeys()) {
    processSubqueries(input, key, /*filter=*/false);
  }

  ColumnVector columns;

  ExprVector deduppedGroupingKeys;
  deduppedGroupingKeys.reserve(agg.groupingKeys().size());

  auto newRenames = renames_;

  folly::F14FastMap<ExprCP, ColumnCP> uniqueGroupingKeys;
  for (auto i = 0; i < agg.groupingKeys().size(); ++i) {
    auto name = toName(agg.outputType()->nameOf(i));
    auto* key = translateExpr(agg.groupingKeys()[i]);

    auto it = uniqueGroupingKeys.try_emplace(key).first;
    if (it->second) {
      newRenames[name] = it->second;
    } else {
      if (key->is(PlanType::kColumnExpr)) {
        columns.push_back(key->as<Column>());
      } else {
        auto* column = make<Column>(name, currentDt_, key->value(), name);
        columns.push_back(column);
      }

      deduppedGroupingKeys.emplace_back(key);
      it->second = columns.back();
      newRenames[name] = columns.back();
    }
  }

  AggregateVector deduppedAggregates;
  folly::F14FastMap<AggregateDedupKey, ColumnCP, AggregateDedupHasher>
      uniqueAggregates;

  // The keys for intermediate are the same as for final.
  ColumnVector intermediateColumns = columns;
  for (auto channel : usedChannels(agg)) {
    if (channel < agg.groupingKeys().size()) {
      continue;
    }

    const auto i = channel - agg.groupingKeys().size();
    const auto& aggregate = agg.aggregates()[i];
    ExprVector args = translateExprs(aggregate->inputs());

    FunctionSet funcs;
    std::vector<velox::TypePtr> argTypes;
    for (auto& arg : args) {
      funcs = funcs | arg->functions();
      argTypes.push_back(toTypePtr(arg->value().type));
    }
    ExprCP condition = nullptr;
    if (aggregate->filter()) {
      condition = translateExpr(aggregate->filter());
    }

    auto aggName = toName(aggregate->name());

    const auto& metadata =
        velox::exec::getAggregateFunctionMetadata(aggregate->name());

    if (metadata.ignoreDuplicates) {
      funcs = funcs | FunctionSet::kIgnoreDuplicatesAggregate;
    } else {
      if ((aggName == toName("min") || aggName == toName("max")) &&
          args.size() == 1) {
        // Presto's min/max are not marked 'ignoreDuplicates' because while
        // min(x) and max(x) do ignore duplicates, min(x, n) and max(x, n) do
        // not.
        // TODO Figure out a better way.
        funcs = funcs | FunctionSet::kIgnoreDuplicatesAggregate;
      }
    }

    if (metadata.orderSensitive) {
      funcs = funcs | FunctionSet::kOrderSensitiveAggregate;
    }

    const bool isDistinct =
        !metadata.ignoreDuplicates && aggregate->isDistinct();

    ExprVector orderKeys;
    OrderTypeVector orderTypes;
    if (metadata.orderSensitive) {
      std::tie(orderKeys, orderTypes) = dedupOrdering(aggregate->ordering());
    }

    if (isDistinct && !orderKeys.empty()) {
      VELOX_FAIL(
          "DISTINCT with ORDER BY in same aggregation expression isn't supported yet");
    }

    if (isDistinct) {
      const auto& options = queryCtx()->optimization()->runnerOptions();
      VELOX_CHECK(
          options.numWorkers == 1 && options.numDrivers == 1,
          "DISTINCT option for aggregation is supported only in single worker, single thread mode");
    }

    auto name = toName(agg.outputNames()[channel]);

    AggregateDedupKey key{
        aggName, isDistinct, condition, args, orderKeys, orderTypes};

    auto it = uniqueAggregates.try_emplace(key).first;
    if (it->second) {
      newRenames[name] = it->second;
    } else {
      auto accumulatorType = toType(
          velox::exec::resolveIntermediateType(aggregate->name(), argTypes));
      Value finalValue = toConstantValue(aggregate->type());

      AggregateCP aggregateExpr = make<Aggregate>(
          aggName,
          finalValue,
          std::move(args),
          funcs,
          isDistinct,
          condition,
          accumulatorType,
          std::move(orderKeys),
          std::move(orderTypes));

      auto* column =
          make<Column>(name, currentDt_, aggregateExpr->value(), name);
      columns.push_back(column);

      auto intermediateValue = aggregateExpr->value();
      intermediateValue.type = accumulatorType;
      auto* intermediateColumn =
          make<Column>(name, currentDt_, intermediateValue, name);
      intermediateColumns.push_back(intermediateColumn);

      deduppedAggregates.push_back(aggregateExpr);
      it->second = column;
      newRenames[name] = column;
    }
  }

  renames_ = std::move(newRenames);

  return make<AggregationPlan>(
      std::move(deduppedGroupingKeys),
      std::move(deduppedAggregates),
      std::move(columns),
      std::move(intermediateColumns));
}

void ToGraph::addOrderBy(const lp::SortNode& order) {
  exprSources_.push_back(order.onlyInput().get());
  SCOPE_EXIT {
    exprSources_.pop_back();
  };

  auto [deduppedOrderKeys, deduppedOrderTypes] =
      dedupOrdering(order.ordering());

  currentDt_->orderKeys = std::move(deduppedOrderKeys);
  currentDt_->orderTypes = std::move(deduppedOrderTypes);
}

namespace {

// Fills 'leftKeys' and 'rightKeys's from 'conjuncts' so that
// equalities with one side only depending on 'right' go to
// 'rightKeys' and the other side not depending on 'right' goes to
// 'leftKeys'. The left side may depend on more than one table. The
// tables 'leftKeys' depend on are returned in 'allLeft'. The
// conjuncts that are not equalities or have both sides depending
// on right and something else are left in 'conjuncts'.
void extractNonInnerJoinEqualities(
    Name eq,
    ExprVector& conjuncts,
    PlanObjectCP right,
    ExprVector& leftKeys,
    ExprVector& rightKeys,
    PlanObjectSet& allLeft) {
  for (auto i = 0; i < conjuncts.size(); ++i) {
    const auto* conjunct = conjuncts[i];
    if (isCallExpr(conjunct, eq)) {
      const auto* eq = conjunct->as<Call>();
      const auto leftTables = eq->argAt(0)->allTables();
      const auto rightTables = eq->argAt(1)->allTables();

      if (leftTables.empty() || rightTables.empty()) {
        continue;
      }

      if (rightTables.size() == 1 && rightTables.contains(right) &&
          !leftTables.contains(right)) {
        allLeft.unionSet(leftTables);
        leftKeys.push_back(eq->argAt(0));
        rightKeys.push_back(eq->argAt(1));
        conjuncts.erase(conjuncts.begin() + i);
        --i;
      } else if (
          leftTables.size() == 1 && leftTables.contains(right) &&
          !rightTables.contains(right)) {
        allLeft.unionSet(rightTables);
        leftKeys.push_back(eq->argAt(1));
        rightKeys.push_back(eq->argAt(0));
        conjuncts.erase(conjuncts.begin() + i);
        --i;
      }
    }
  }
}

} // namespace

void ToGraph::addJoinColumns(
    const logical_plan::LogicalPlanNode& joinSide,
    ColumnVector& columns,
    ExprVector& exprs) {
  const auto& names = joinSide.outputType()->names();
  for (auto channel : usedChannels(joinSide)) {
    const auto& name = names[channel];
    auto* expr = translateColumn(name);

    Name alias = nullptr;
    if (expr->isColumn()) {
      alias = expr->as<Column>()->alias();
    }

    auto* column = make<Column>(toName(name), currentDt_, expr->value(), alias);
    renames_[name] = column;

    columns.push_back(column);
    exprs.push_back(expr);
  }
}

void ToGraph::translateJoin(
    const lp::LogicalPlanNodePtr& left,
    const lp::LogicalPlanNodePtr& right,
    lp::JoinType joinType,
    const lp::ExprPtr& condition,
    lp::JoinType originalJoinType) {
  if (joinType == lp::JoinType::kInner) {
    // Inner join is a cross join with a filter. Both sides are already in the
    // DT. Process the condition as a filter. This handles subqueries using the
    // same infrastructure as WHERE clause predicates.
    if (condition) {
      exprSources_.push_back(right.get());
      SCOPE_EXIT {
        exprSources_.pop_back();
      };

      addFilter(*left, condition);
    }
    return;
  }

  exprSources_.push_back(left.get());
  exprSources_.push_back(right.get());
  SCOPE_EXIT {
    exprSources_.pop_back();
    exprSources_.pop_back();
  };

  ExprVector conjuncts;
  translateConjuncts(condition, conjuncts);

#ifndef NDEBUG
  // Sanity check. The join condition should not depend on the output of the
  // current DT.
  for (const auto* conjunct : conjuncts) {
    VELOX_DCHECK(
        !conjunct->allTables().contains(currentDt_),
        "Cannot add a join that depends on DT's output: {}",
        conjunct->toString());
  }
#endif

  const bool leftOptional =
      joinType == lp::JoinType::kRight || joinType == lp::JoinType::kFull;
  const bool rightOptional =
      joinType == lp::JoinType::kLeft || joinType == lp::JoinType::kFull;

  // If non-inner, and many tables on the right they are one dt. If a single
  // table then this too is the last in 'tables'.
  auto rightTable = currentDt_->tables.back();

  // For LEFT JOIN, push down conjuncts that reference only the right
  // (null-supplying) side. Pre-filtering the right side is semantically
  // equivalent: it reduces which right rows are candidates for matching, while
  // left rows that don't find a match still appear with NULLs.
  if (rightOptional && !leftOptional) {
    for (auto it = conjuncts.begin(); it != conjuncts.end();) {
      auto* conjunct = *it;
      if (conjunct->singleTable() != rightTable) {
        ++it;
        continue;
      }

      if (rightTable->is(PlanType::kDerivedTableNode)) {
        auto* rightDt =
            const_cast<DerivedTable*>(rightTable->as<DerivedTable>());
        if (!rightDt->addFilter(conjunct)) {
          ++it;
          continue;
        }
      } else if (rightTable->is(PlanType::kTableNode)) {
        const_cast<BaseTable*>(rightTable->as<BaseTable>())
            ->addFilter(conjunct);
      } else {
        // TODO: Support ValuesTable and UnnestTable.
        ++it;
        continue;
      }

      it = conjuncts.erase(it);
    }
  }

  ExprVector leftKeys;
  ExprVector rightKeys;
  PlanObjectSet leftTables;
  extractNonInnerJoinEqualities(
      functionNames_.equality,
      conjuncts,
      rightTable,
      leftKeys,
      rightKeys,
      leftTables);

  JoinEdge::Spec joinSpec{
      .filter = std::move(conjuncts),
      .leftOptional = leftOptional,
      .rightOptional = rightOptional,
  };

  if (leftOptional) {
    addJoinColumns(*left, joinSpec.leftColumns, joinSpec.leftExprs);
  }

  if (rightOptional) {
    addJoinColumns(*right, joinSpec.rightColumns, joinSpec.rightExprs);
  }

  if (leftTables.empty()) {
    VELOX_CHECK_EQ(
        2,
        currentDt_->tables.size(),
        "The left of a non-inner join is expected to be one table");
    leftTables.add(currentDt_->tables[0]);
  }

  auto* edge = make<JoinEdge>(
      leftTables.size() == 1 ? leftTables.onlyObject() : nullptr,
      rightTable,
      std::move(joinSpec),
      originalJoinType);
  currentDt_->joins.push_back(edge);
  for (auto i = 0; i < leftKeys.size(); ++i) {
    edge->addEquality(leftKeys[i], rightKeys[i]);
  }
}

DerivedTableP ToGraph::newDt() {
  auto* dt = make<DerivedTable>();
  dt->cname = newCName("dt");
  return dt;
}

void ToGraph::wrapInDt(const lp::LogicalPlanNode& node) {
  auto* outerDt = std::exchange(currentDt_, newDt());
  makeQueryGraph(node, kAllAllowedInDt);
  finalizeDt(node, outerDt);
}

void ToGraph::finalizeDt(
    const lp::LogicalPlanNode& node,
    DerivedTableP outerDt) {
  VELOX_CHECK_EQ(0, correlatedConjuncts_.size());

  if (!outerDt) {
    outerDt = newDt();
  }
  VELOX_DCHECK_NOT_NULL(currentDt_);

  DerivedTableP dt = currentDt_;
  setDtUsedOutput(dt, node);

  currentDt_ = outerDt;
  currentDt_->addTable(dt);
}

ColumnCP ToGraph::makeCountStarWrapper(DerivedTableP inputDt) {
  VELOX_CHECK_NOT_NULL(
      functionNames_.count, "Count aggregate function not registered");

  auto* wrapperDt = newDt();
  wrapperDt->addTable(inputDt);

  auto accumulatorType = toType(
      velox::exec::resolveIntermediateType(
          std::string{functionNames_.count}, {}));

  AggregateCP countAggregate = make<Aggregate>(
      functionNames_.count,
      toConstantValue(velox::BIGINT()),
      ExprVector{},
      FunctionSet(),
      /*isDistinct=*/false,
      /*condition=*/nullptr,
      accumulatorType,
      ExprVector{},
      OrderTypeVector{});

  auto* countColumnName = newCName("__count");
  auto* countColumn =
      make<Column>(countColumnName, wrapperDt, countAggregate->value());

  auto* intermediateColumn =
      make<Column>(countColumnName, wrapperDt, Value{accumulatorType, 1});

  wrapperDt->aggregation = make<AggregationPlan>(
      ExprVector{},
      AggregateVector{countAggregate},
      ColumnVector{countColumn},
      ColumnVector{intermediateColumn});

  wrapperDt->columns = {countColumn};
  wrapperDt->exprs = {countColumn};

  currentDt_->addTable(wrapperDt);

  renames_[countColumnName] = countColumn;

  return countColumn;
}

namespace {
const velox::Type* pathType(const velox::Type* type, PathCP path) {
  for (auto& step : path->steps()) {
    switch (step.kind) {
      case StepKind::kField:
        if (step.field) {
          type = type->childAt(type->as<velox::TypeKind::ROW>().getChildIdx(
                                   step.field))
                     .get();
          break;
        }
        type = type->childAt(step.id).get();
        break;
      case StepKind::kSubscript:
        type =
            type->childAt(type->kind() == velox::TypeKind::ARRAY ? 0 : 1).get();
        break;
      default:
        VELOX_NYI();
    }
  }
  return type;
}

SubfieldProjections makeSubfieldColumns(
    BaseTable& baseTable,
    ColumnCP column,
    const PathSet& paths) {
  const float cardinality =
      baseTable.schemaTable->cardinality * baseTable.filterSelectivity;

  SubfieldProjections projections;
  paths.forEachPath([&](PathCP path) {
    auto type = pathType(column->value().type, path);
    Value value(type, cardinality);
    auto name = fmt::format("{}.{}", column->name(), path->toString());
    auto* subcolumn = make<Column>(
        toName(name),
        &baseTable,
        value,
        /*alias=*/nullptr,
        /*nameInTable=*/nullptr,
        column,
        path);
    baseTable.columns.push_back(subcolumn);
    projections.pathToExpr[path] = subcolumn;
  });

  return projections;
}
} // namespace

void ToGraph::makeBaseTable(const lp::TableScanNode& tableScan) {
  const auto* schemaTable =
      schema_.findTable(tableScan.connectorId(), tableScan.tableName());
  VELOX_CHECK_NOT_NULL(
      schemaTable,
      "Table not found: {} via connector {}",
      tableScan.tableName(),
      tableScan.connectorId());

  auto* baseTable = make<BaseTable>();
  baseTable->cname = newCName("t");
  baseTable->schemaTable = schemaTable;
  planLeaves_[&tableScan] = baseTable;

  auto channels = usedChannels(tableScan);
  const auto& type = tableScan.outputType();
  const auto& names = tableScan.columnNames();
  for (auto i : channels) {
    VELOX_DCHECK_LT(i, type->size());

    const auto& name = names[i];
    const auto* columnName = toName(name);
    auto schemaColumn = schemaTable->findColumn(columnName);
    auto value = schemaColumn->value();
    auto* column = make<Column>(
        columnName,
        baseTable,
        value,
        toName(type->nameOf(i)),
        schemaColumn->name());
    baseTable->columns.push_back(column);

    const auto kind = column->value().type->kind();
    if (kind == velox::TypeKind::ARRAY || kind == velox::TypeKind::ROW ||
        kind == velox::TypeKind::MAP) {
      PathSet allPaths;
      if (controlSubfields_.hasColumn(&tableScan, i)) {
        baseTable->controlSubfields.ids.push_back(column->id());
        allPaths = controlSubfields_.nodeFields[&tableScan].resultPaths[i];
        baseTable->controlSubfields.subfields.push_back(allPaths);
      }
      if (payloadSubfields_.hasColumn(&tableScan, i)) {
        baseTable->payloadSubfields.ids.push_back(column->id());
        auto payloadPaths =
            payloadSubfields_.nodeFields[&tableScan].resultPaths[i];
        baseTable->payloadSubfields.subfields.push_back(payloadPaths);
        allPaths.unionSet(payloadPaths);
      }
      if (options_.pushdownSubfields) {
        Path::subfieldSkyline(allPaths);
        if (!allPaths.empty()) {
          trace(OptimizerOptions::kPreprocess, [&]() {
            std::cout << "Subfields: " << baseTable->cname << "."
                      << baseTable->schemaTable->name() << " " << column->name()
                      << ":" << allPaths.size() << std::endl;
          });
          allColumnSubfields_[column] =
              makeSubfieldColumns(*baseTable, column, allPaths);
        }
      }
    }

    renames_[type->nameOf(i)] = column;
  }

  auto* optimization = queryCtx()->optimization();

  optimization->filterUpdated(baseTable, false);

  ColumnVector top;
  folly::F14FastMap<ColumnCP, velox::TypePtr> map;
  auto scanType = optimization->subfieldPushdownScanType(
      baseTable, baseTable->columns, top, map);

  optimization->estimateLeafSelectivity(*baseTable, scanType);
  currentDt_->addTable(baseTable);
}

void ToGraph::makeValuesTable(const lp::ValuesNode& values) {
  ValuesTable::Data data;

  if (const auto* rows =
          std::get_if<lp::ValuesNode::Variants>(&values.data())) {
    data = rows;
  } else if (
      const auto* rows = std::get_if<lp::ValuesNode::Vectors>(&values.data())) {
    data = rows;
  } else if (
      const auto* rows = std::get_if<lp::ValuesNode::Exprs>(&values.data())) {
    std::vector<velox::Variant> variants;
    variants.reserve(rows->size());
    for (const auto& row : *rows) {
      std::vector<velox::Variant> rowVariants;
      rowVariants.reserve(row.size());
      for (const auto& expr : row) {
        auto literal = translateExpr(expr);
        VELOX_USER_CHECK(
            literal->is(PlanType::kLiteralExpr),
            "Expressions used in Values node must be constant-foldable: {}",
            expr->toString());

        rowVariants.emplace_back(literal->as<Literal>()->literal());
      }
      variants.emplace_back(velox::Variant::row(std::move(rowVariants)));
    }

    data =
        &registerVariant(velox::Variant::array(std::move(variants)))->array();
  }

  auto* valuesTable = makeValuesTable(values, std::move(data));
  planLeaves_[&values] = valuesTable;
  currentDt_->addTable(valuesTable);
}

ValuesTable* ToGraph::makeValuesTable(
    const lp::LogicalPlanNode& node,
    ValuesTable::Data data) {
  auto* valuesTable = make<ValuesTable>(toType(node.outputType()), data);
  valuesTable->cname = newCName("vt");

  const auto& type = node.outputType();
  const auto& names = type->names();
  const auto cardinality = valuesTable->cardinality();
  for (auto i : usedChannels(node)) {
    VELOX_DCHECK_LT(i, type->size());

    const auto& name = names[i];
    const auto* columnName = toName(name);
    auto* column = make<Column>(
        columnName,
        valuesTable,
        toValue(type->childAt(i), cardinality),
        columnName);
    valuesTable->columns.push_back(column);

    renames_[name] = column;
  }

  return valuesTable;
}

void ToGraph::addProjection(const lp::ProjectNode& project) {
  const auto& input = *project.onlyInput();

  exprSources_.push_back(&input);
  SCOPE_EXIT {
    exprSources_.pop_back();
  };

  const auto& names = project.names();
  const auto& exprs = project.expressions();
  auto channels = usedChannels(project);
  trace(OptimizerOptions::kPreprocess, [&]() {
    for (auto i = 0; i < exprs.size(); ++i) {
      if (std::ranges::find(channels, i) == channels.end()) {
        std::cout << "P=" << project.id()
                  << " dropped projection name=" << names[i] << " = "
                  << lp::ExprPrinter::toText(*exprs[i]) << std::endl;
      }
    }
  });

  for (auto i : channels) {
    processSubqueries(input, exprs[i], /*filter=*/false);
  }

  for (auto i : channels) {
    if (exprs[i]->isInputReference()) {
      const auto& name = exprs[i]->as<lp::InputReferenceExpr>()->name();
      // A variable projected to itself adds no renames. Inputs contain this
      // all the time.
      if (name == names[i]) {
        continue;
      }
    }

    auto expr = translateExpr(exprs.at(i));
    renames_[names[i]] = expr;
  }
}

namespace {

struct Subqueries {
  std::vector<lp::SubqueryExprPtr> scalars;
  std::vector<lp::ExprPtr> inPredicates;
  std::vector<lp::ExprPtr> exists;

  bool empty() const {
    return scalars.empty() && inPredicates.empty() && exists.empty();
  }
};

void extractSubqueries(const lp::ExprPtr& expr, Subqueries& subqueries) {
  if (expr->isSubquery()) {
    subqueries.scalars.push_back(
        std::static_pointer_cast<const lp::SubqueryExpr>(expr));
    return;
  }

  if (expr->isSpecialForm()) {
    const auto* specialForm = expr->as<lp::SpecialFormExpr>();
    if (specialForm->form() == lp::SpecialForm::kIn &&
        specialForm->inputAt(1)->isSubquery()) {
      subqueries.inPredicates.push_back(expr);
      return;
    }

    if (specialForm->form() == lp::SpecialForm::kExists) {
      subqueries.exists.push_back(expr);
      return;
    }
  }

  for (const auto& input : expr->inputs()) {
    extractSubqueries(input, subqueries);
  }
}
} // namespace

DerivedTableP ToGraph::translateSubquery(
    const logical_plan::LogicalPlanNode& node,
    bool finalize) {
  auto originalRenames = std::move(renames_);
  renames_.clear();

  correlations_ = &originalRenames;
  SCOPE_EXIT {
    correlations_ = nullptr;
  };

  VELOX_CHECK(correlatedConjuncts_.empty());

  auto* outerDt = std::exchange(currentDt_, newDt());
  makeQueryGraph(node, kAllAllowedInDt);
  auto* subqueryDt = currentDt_;

  setDtUsedOutput(subqueryDt, node);
  currentDt_ = outerDt;
  if (finalize) {
    currentDt_->addTable(subqueryDt);
  }

  renames_ = std::move(originalRenames);

  for (const auto* column : subqueryDt->columns) {
    renames_[column->name()] = column;
  }

  return subqueryDt;
}

ColumnCP ToGraph::addMarkColumn() {
  auto* markColumn = makeColumn("__mark", toValue(velox::BOOLEAN(), 2));
  renames_[markColumn->name()] = markColumn;
  return markColumn;
}

namespace {

// Holds the extracted join keys and filters from correlated conjuncts.
// Used when decorrelating subqueries.
struct DecorrelatedJoin {
  PlanObjectSet leftTables;
  ExprVector leftKeys;
  ExprVector rightKeys;
  ExprVector filter;
};

DecorrelatedJoin extractDecorrelatedJoin(
    const FunctionNames& funcs,
    const ExprVector& correlatedConjuncts,
    DerivedTableP subqueryDt) {
  DecorrelatedJoin result;

  for (const auto* conjunct : correlatedConjuncts) {
    conjunct = subqueryDt->exportExpr(conjunct);

    auto tables = conjunct->allTables();
    tables.erase(subqueryDt);

    VELOX_CHECK_EQ(
        1,
        tables.size(),
        "Correlated conjuncts referencing multiple outer tables are not supported: {}",
        conjunct->toString());
    auto outerTable = tables.onlyObject();

    result.leftTables.add(outerTable);

    ExprCP left = nullptr;
    ExprCP right = nullptr;
    if (isJoinEquality(funcs, conjunct, outerTable, subqueryDt, left, right)) {
      result.leftKeys.push_back(left);
      result.rightKeys.push_back(right);
    } else {
      result.filter.push_back(conjunct);
    }
  }

  return result;
}

// Holds extracted correlation keys from correlatedConjuncts.
// Used internally during scalar subquery processing.
struct CorrelationKeys {
  PlanObjectCP leftTable{nullptr};
  ExprVector leftKeys;
  ExprVector rightKeys;
  ExprVector nonEquiConjuncts;
};

CorrelationKeys extractCorrelationKeys(
    const FunctionNames& funcs,
    const ExprVector& correlatedConjuncts,
    DerivedTableP subqueryDt) {
  CorrelationKeys result;

  for (const auto* conjunct : correlatedConjuncts) {
    const auto tables = conjunct->allTables().toObjects();
    VELOX_CHECK_EQ(2, tables.size());

    ExprCP left = nullptr;
    ExprCP right = nullptr;
    if (isJoinEquality(funcs, conjunct, tables[0], tables[1], left, right)) {
      if (tables[1] == subqueryDt || subqueryDt->tableSet.contains(tables[1])) {
        result.leftKeys.push_back(left);
        result.rightKeys.push_back(right);
        result.leftTable = tables[0];
      } else {
        result.leftKeys.push_back(right);
        result.rightKeys.push_back(left);
        result.leftTable = tables[1];
      }
    } else {
      result.nonEquiConjuncts.push_back(conjunct);
      if (tables[1] == subqueryDt || subqueryDt->tableSet.contains(tables[1])) {
        result.leftTable = tables[0];
      } else {
        result.leftTable = tables[1];
      }
    }
  }

  return result;
}

} // namespace

void ToGraph::appendArbitraryAggregates(
    const lp::LogicalPlanNode& input,
    AggregateVector& aggregates,
    ColumnVector& columns) {
  VELOX_CHECK_NOT_NULL(
      functionNames_.arbitrary, "Arbitrary aggregate function not registered");

  for (auto channel : usedChannels(input)) {
    const auto& name = input.outputType()->nameOf(channel);
    auto expr = renames_.at(name);

    auto intermediateType = toType(
        velox::exec::resolveIntermediateType(
            std::string{functionNames_.arbitrary},
            {toTypePtr(expr->value().type)}));
    auto* arbitraryAgg = make<Aggregate>(
        functionNames_.arbitrary,
        expr->value(),
        ExprVector{expr},
        FunctionSet(),
        /*isDistinct=*/false,
        /*condition=*/nullptr,
        intermediateType,
        ExprVector{},
        OrderTypeVector{});
    aggregates.push_back(arbitraryAgg);

    auto* column = make<Column>(newCName("__agg"), currentDt_, expr->value());
    columns.push_back(column);

    renames_[name] = column;
  }
}

AggregationPlanCP ToGraph::processCorrelatedAggregation(AggregationPlanCP agg) {
  if (correlatedConjuncts_.empty()) {
    return agg;
  }

  // For EXISTS subqueries with DISTINCT (aggregation with no aggregate
  // functions), the DISTINCT can be dropped since EXISTS only checks row
  // existence. Skip adding grouping keys from correlation and just keep
  // the correlation conjuncts for later processing.
  if (agg->aggregates().empty()) {
    // This is a DISTINCT (aggregation with only grouping keys, no
    // aggregates). For EXISTS, we can skip it entirely.
    return nullptr;
  }

  VELOX_CHECK(agg->groupingKeys().empty());

  // Check if all conjuncts are equalities. If any are non-equi, we'll
  // handle decorrelation at the outer level using AssignUniqueId.
  if (!areAllJoinEqualities(functionNames_, correlatedConjuncts_)) {
    // Non-equi correlation: keep the aggregation as-is (global agg).
    // The decorrelation will be handled at the outer level using
    // AssignUniqueId + LEFT JOIN + outer aggregation.
    return agg;
  }

  // Expect all conjuncts to have the form of f(outer) = g(inner).
  ExprVector groupingKeys;
  ColumnVector columns;
  ExprVector leftKeys;
  for (const auto* conjunct : correlatedConjuncts_) {
    const auto tables = conjunct->allTables().toObjects();
    VELOX_CHECK_EQ(2, tables.size());

    ExprCP left = nullptr;
    ExprCP right = nullptr;
    ExprCP key = nullptr;
    if (isJoinEquality(conjunct, tables[0], tables[1], left, right)) {
      if (currentDt_->tableSet.contains(tables[0])) {
        key = left;
        leftKeys.push_back(right);
      } else {
        key = right;
        leftKeys.push_back(left);
      }

      if (key->is(PlanType::kColumnExpr)) {
        columns.push_back(key->as<Column>());
      } else {
        auto* column = make<Column>(newCName("__gk"), currentDt_, key->value());
        columns.push_back(column);
      }

      groupingKeys.push_back(key);
    }
  }

  correlatedConjuncts_.clear();
  for (auto i = 0; i < leftKeys.size(); ++i) {
    currentDt_->exprs.push_back(columns[i]);
    currentDt_->columns.push_back(
        make<Column>(newCName("__gk"), currentDt_, columns[i]->value()));

    correlatedConjuncts_.push_back(
        make<Call>(
            functionNames_.equality,
            toValue(velox::BOOLEAN(), 2),
            ExprVector{leftKeys[i], currentDt_->columns.back()},
            FunctionSet()));
  }

  auto intermediateColumns = columns;

  for (const auto* column : agg->columns()) {
    columns.push_back(column);
  }

  for (const auto* column : agg->intermediateColumns()) {
    intermediateColumns.push_back(column);
  }

  return make<AggregationPlan>(
      groupingKeys, agg->aggregates(), columns, intermediateColumns);
}

void ToGraph::processSubqueries(
    const lp::LogicalPlanNode& input,
    const lp::ExprPtr& expr,
    bool filter) {
  Subqueries subqueries;
  extractSubqueries(expr, subqueries);

  if (filter && currentDt_->hasLimit()) {
    // Filter cannot be added after LIMIT.
    finalizeDt(input);
  } else if (currentDt_->hasAggregation() && !subqueries.empty()) {
    // Scalar subqueries are placed as joins. Joins cannot be added after
    // aggregation.
    finalizeDt(input);
  } else if (currentDt_->hasUnnestTable() && !subqueries.empty()) {
    // Subqueries are placed as joins. Joins cannot be placed directly on unnest
    // tables because unnest tables are on the right side of directed
    // cross-joins.
    // TODO: Optimize to only wrap when subquery actually references an unnest
    // table column.
    finalizeDt(input);
  }

  processScalarSubqueries(input, subqueries.scalars);
  processInPredicates(subqueries.inPredicates);
  processExistsSubqueries(subqueries.exists);
}

void ToGraph::processScalarSubqueries(
    const lp::LogicalPlanNode& input,
    const std::vector<lp::SubqueryExprPtr>& scalars) {
  for (const auto& subquery : scalars) {
    auto subqueryDt = translateSubquery(*subquery->subquery());

    ExprCP column;
    if (correlatedConjuncts_.empty()) {
      column = processUncorrelatedScalarSubquery(subqueryDt);
    } else {
      column = processCorrelatedScalarSubquery(input, subqueryDt);
    }
    subqueries_.emplace(subquery, column);
  }
}

ExprCP ToGraph::processUncorrelatedScalarSubquery(DerivedTableP subqueryDt) {
  VELOX_CHECK_EQ(1, subqueryDt->columns.size());

  if (auto valuesNode = tryFoldConstantDt(subqueryDt, evaluator_.pool())) {
    VELOX_CHECK_EQ(1, valuesNode->outputType()->size());
    if (valuesNode->cardinality() == 1) {
      // Replace subquery with a constant value.
      const auto value =
          std::get<std::vector<velox::RowVectorPtr>>(valuesNode->data())
              .front()
              ->childAt(0)
              ->variantAt(0);
      const auto* literal = make<Literal>(
          toConstantValue(valuesNode->outputType()->childAt(0)),
          registerVariant(value));

      currentDt_->removeLastTable(subqueryDt);
      return literal;
    }
  }

  subqueryDt->ensureSingleRow();
  return subqueryDt->columns.front();
}

namespace {
// Returns a Literal for the result of 'agg' over empty input, or nullptr
// if the aggregate returns NULL for empty input. Used to wrap count-like
// aggregates with COALESCE when decorrelating scalar subqueries.
Literal* tryMakeLiteralForEmptyInput(AggregateCP agg) {
  auto result = FunctionRegistry::instance()->aggregateResultForEmptyInput(
      agg->name(), agg->rawInputType());
  if (result.isNull()) {
    return nullptr;
  }
  return make<Literal>(Value(agg->value().type, 1), registerVariant(result));
}
} // namespace

ExprCP ToGraph::processCorrelatedScalarSubquery(
    const lp::LogicalPlanNode& input,
    DerivedTableP subqueryDt) {
  auto correlation =
      extractCorrelationKeys(functionNames_, correlatedConjuncts_, subqueryDt);

  const bool hasAggregation = subqueryDt->hasAggregation();

  if (!correlation.nonEquiConjuncts.empty()) {
    // For non-equi correlation, expect only 1 column (the aggregate result).
    VELOX_CHECK_EQ(1, subqueryDt->columns.size());
  } else if (hasAggregation) {
    // For equi-only correlation with aggregation, expect grouping keys +
    // aggregate result.
    VELOX_CHECK_EQ(correlatedConjuncts_.size() + 1, subqueryDt->columns.size());
  } else {
    // For equi-only correlation without aggregation, expect only 1 column (the
    // subquery result).
    VELOX_CHECK_EQ(1, subqueryDt->columns.size());
  }

  correlatedConjuncts_.clear();

  if (correlation.nonEquiConjuncts.empty() && hasAggregation) {
    // Equi-join only with aggregation: create LEFT join with correlation keys.
    auto* join = make<JoinEdge>(
        correlation.leftTable,
        subqueryDt,
        JoinEdge::Spec{.rightOptional = true});
    for (auto i = 0; i < correlation.leftKeys.size(); ++i) {
      join->addEquality(correlation.leftKeys[i], correlation.rightKeys[i]);
    }

    currentDt_->joins.push_back(join);

    auto* aggregateResult = subqueryDt->columns.back();

    auto* agg = subqueryDt->aggregation->aggregates().back();
    if (auto* literal = tryMakeLiteralForEmptyInput(agg)) {
      // Wrap with COALESCE for aggregates that return non-NULL for empty input.
      // LEFT JOIN returns NULL for outer rows with no matches, but count-like
      // aggregates should return 0 (or similar default) instead.
      return make<Call>(
          SpecialFormCallNames::kCoalesce,
          aggregateResult->value(),
          ExprVector{aggregateResult, literal},
          FunctionSet());
    }

    return aggregateResult;
  }

  if (!hasAggregation) {
    // Correlation without aggregation: use AssignUniqueId + LEFT JOIN +
    // EnforceDistinct to validate single-row semantics.
    auto* rowNumberColumn = makeColumn(
        "__rownum",
        toValue(velox::BIGINT(), std::numeric_limits<int64_t>::max()));

    ExprVector exportedFilter;
    for (auto* conjunct : correlation.nonEquiConjuncts) {
      exportedFilter.push_back(subqueryDt->exportExpr(conjunct));
    }

    auto* join = make<JoinEdge>(
        correlation.leftTable,
        subqueryDt,
        JoinEdge::Spec{
            .filter = exportedFilter,
            .rightOptional = true,
            .rowNumberColumn = rowNumberColumn,
            .multipleMatchesError =
                toName("Scalar sub-query has returned multiple rows"),
        });

    for (auto i = 0; i < correlation.leftKeys.size(); ++i) {
      join->addEquality(
          correlation.leftKeys[i],
          subqueryDt->exportExpr(correlation.rightKeys[i]));
    }

    currentDt_->joins.push_back(join);

    // The first column is the subquery's original output (the SELECT
    // expression). Subsequent columns are correlation keys added via
    // exportExpr.
    return subqueryDt->columns.front();
  }

  // Non-equi correlation with aggregation: use AssignUniqueId + LEFT JOIN +
  // Aggregation.
  auto exportedAgg = subqueryDt->exportSingleAggregate(newCName("__mark"));

  auto* rowNumberColumn = makeColumn(
      "__rownum",
      toValue(velox::BIGINT(), std::numeric_limits<int64_t>::max()));

  ExprVector exportedFilter;
  for (auto* conjunct : correlation.nonEquiConjuncts) {
    exportedFilter.push_back(subqueryDt->exportExpr(conjunct));
  }

  auto* join = make<JoinEdge>(
      correlation.leftTable,
      subqueryDt,
      JoinEdge::Spec{
          .filter = exportedFilter,
          .rightOptional = true,
          .rowNumberColumn = rowNumberColumn,
      });

  // Add any equi-join keys.
  for (auto i = 0; i < correlation.leftKeys.size(); ++i) {
    join->addEquality(
        subqueryDt->exportExpr(correlation.leftKeys[i]),
        subqueryDt->exportExpr(correlation.rightKeys[i]));
  }
  currentDt_->joins.push_back(join);

  auto* aggResultColumn = makeColumn("__agg", exportedAgg->value());

  AggregateVector allAggregates{exportedAgg};
  ColumnVector allColumns{rowNumberColumn, aggResultColumn};
  appendArbitraryAggregates(input, allAggregates, allColumns);

  currentDt_->aggregation = make<AggregationPlan>(
      ExprVector{rowNumberColumn}, allAggregates, allColumns, allColumns);

  return aggResultColumn;
}

void ToGraph::processInPredicates(
    const std::vector<lp::ExprPtr>& inPredicates) {
  for (const auto& predicate : inPredicates) {
    auto subqueryDt = translateSubquery(
        *predicate->inputAt(1)->as<lp::SubqueryExpr>()->subquery());
    VELOX_CHECK_EQ(1, subqueryDt->columns.size());

    // Add a join edge and replace 'expr' with 'mark' column in the join output.
    const auto* markColumn = addMarkColumn();

    auto leftKey = translateExpr(predicate->inputAt(0));
    auto leftTable = leftKey->singleTable();
    VELOX_CHECK_NOT_NULL(
        leftTable,
        "<expr> IN <subquery> with multi-table <expr> is not supported yet");

    ExprCP column;
    if (correlatedConjuncts_.empty()) {
      column = processUncorrelatedInPredicate(
          subqueryDt, markColumn, leftKey, leftTable);
    } else {
      column = processCorrelatedInPredicate(
          subqueryDt, markColumn, leftKey, leftTable);
    }
    subqueries_.emplace(predicate, column);
  }
}

ExprCP ToGraph::processUncorrelatedInPredicate(
    DerivedTableP subqueryDt,
    ColumnCP markColumn,
    ExprCP leftKey,
    PlanObjectCP leftTable) {
  // Uncorrelated IN subquery: create semi-join with IN equality only.
  auto* edge = JoinEdge::makeExists(
      leftTable,
      subqueryDt,
      markColumn,
      /*filter=*/{},
      /*nullAwareIn=*/true);

  currentDt_->joins.push_back(edge);
  edge->addEquality(leftKey, subqueryDt->columns.front());

  return markColumn;
}

ExprCP ToGraph::processCorrelatedInPredicate(
    DerivedTableP subqueryDt,
    ColumnCP markColumn,
    ExprCP leftKey,
    PlanObjectCP leftTable) {
  // Correlated IN subquery: process correlation conditions and create
  // semi-join with both correlation equalities and IN equality.
  auto decorrelated =
      extractDecorrelatedJoin(functionNames_, correlatedConjuncts_, subqueryDt);
  decorrelated.leftTables.add(leftTable);
  correlatedConjuncts_.clear();

  PlanObjectCP joinLeftTable = nullptr;
  if (decorrelated.leftTables.size() == 1) {
    joinLeftTable = decorrelated.leftTables.onlyObject();
  }

  auto* edge = JoinEdge::makeExists(
      joinLeftTable,
      subqueryDt,
      markColumn,
      std::move(decorrelated.filter),
      /*nullAwareIn=*/true);
  currentDt_->joins.push_back(edge);

  // Add correlation equalities.
  for (auto i = 0; i < decorrelated.leftKeys.size(); ++i) {
    edge->addEquality(decorrelated.leftKeys[i], decorrelated.rightKeys[i]);
  }

  // Add IN equality.
  edge->addEquality(leftKey, subqueryDt->columns.front());

  return markColumn;
}

void ToGraph::processExistsSubqueries(const std::vector<lp::ExprPtr>& exists) {
  for (const auto& existsExpr : exists) {
    auto subqueryDt = translateSubquery(
        *existsExpr->inputAt(0)->as<lp::SubqueryExpr>()->subquery(),
        /*finalize=*/false);

    ExprCP column;
    if (correlatedConjuncts_.empty()) {
      column = processUncorrelatedExists(subqueryDt);
    } else {
      column = processCorrelatedExists(subqueryDt);
    }
    subqueries_.emplace(existsExpr, column);
  }
}

namespace {
// Creates a comparison expression: expr != 0, i.e. NOT(expr == 0).
ExprCP makeNotEqualsZero(const FunctionNames& funcs, ExprCP expr) {
  auto* zero = make<Literal>(
      toConstantValue(velox::BIGINT()), registerVariant(int64_t{0}));
  // <expr> = 0
  auto* equalsZero = make<Call>(
      funcs.equality,
      toConstantValue(velox::BOOLEAN()),
      ExprVector{expr, zero},
      FunctionSet());
  // NOT(<expr> = 0)
  return make<Call>(
      funcs.negation,
      toConstantValue(velox::BOOLEAN()),
      ExprVector{equalsZero},
      FunctionSet());
}
} // namespace

ExprCP ToGraph::processUncorrelatedExists(DerivedTableP subqueryDt) {
  // Uncorrelated EXISTS: transform to cross join with NOT(COUNT(*) == 0).
  //
  // For efficiency, first apply LIMIT 1 to the subquery (we only need to
  // know if at least one row exists), then wrap with COUNT(*) aggregation
  // which produces 0 or 1. Cross join this with the outer query and replace
  // the EXISTS expression with `NOT(count == 0)`.
  if (subqueryDt->limit != 0) {
    subqueryDt->limit = 1;
  }

  auto* countColumn = makeCountStarWrapper(subqueryDt);

  return makeNotEqualsZero(functionNames_, countColumn);
}

ExprCP ToGraph::processCorrelatedExists(DerivedTableP subqueryDt) {
  // Correlated EXISTS: create mark join.
  // Finalize the subqueryDt (add to currentDt_).
  currentDt_->addTable(subqueryDt);

  auto decorrelated =
      extractDecorrelatedJoin(functionNames_, correlatedConjuncts_, subqueryDt);
  if (decorrelated.leftKeys.empty()) {
    VELOX_CHECK_EQ(decorrelated.leftTables.size(), 1);
  }
  correlatedConjuncts_.clear();

  const auto* markColumn = addMarkColumn();

  PlanObjectCP leftTable = nullptr;
  if (decorrelated.leftTables.size() == 1) {
    leftTable = decorrelated.leftTables.onlyObject();
  }

  auto* existsEdge = JoinEdge::makeExists(
      leftTable, subqueryDt, markColumn, std::move(decorrelated.filter));
  currentDt_->joins.push_back(existsEdge);

  for (auto i = 0; i < decorrelated.leftKeys.size(); ++i) {
    existsEdge->addEquality(
        decorrelated.leftKeys[i], decorrelated.rightKeys[i]);
  }

  return markColumn;
}

void ToGraph::applySampling(
    const lp::SampleNode& sample,
    uint64_t allowedInDt) {
  auto constantPercentageExpr = tryFoldConstant(sample.percentage());
  VELOX_USER_CHECK_NOT_NULL(
      constantPercentageExpr,
      "Sampling percentage must be constant: {}",
      sample.percentage()->toString());

  const auto& percentageValue =
      constantPercentageExpr->as<lp::ConstantExpr>()->value();
  VELOX_USER_CHECK(
      !percentageValue->isNull(), "Sampling percentage must not be null");
  VELOX_USER_CHECK_EQ(
      percentageValue->kind(),
      velox::TypeKind::DOUBLE,
      "Sampling percentage must be a double");

  const auto percentage = percentageValue->value<double>();
  VELOX_USER_CHECK_GE(percentage, 0, "Sampling percentage must be >= 0");
  VELOX_USER_CHECK_LE(percentage, 100, "Sampling percentage must be <= 100");

  if (percentage == 100) {
    makeQueryGraph(*sample.onlyInput(), allowedInDt);
    return;
  }

  // TODO Optimize the case when percentage == 0.
  // TODO Figure out how to avoid hard-coding "rand" and "lt".

  switch (sample.sampleMethod()) {
    case lp::SampleNode::SampleMethod::kSystem:
      VELOX_NYI("SYSTEM sampling is not supported yet");
      break;
    case lp::SampleNode::SampleMethod::kBernoulli: {
      // Implement using filter(rand() < percentage / 100.0).
      auto predicate = std::make_shared<lp::CallExpr>(
          velox::BOOLEAN(),
          "lt",
          std::make_shared<lp::CallExpr>(velox::DOUBLE(), "rand"),
          std::make_shared<lp::ConstantExpr>(
              velox::DOUBLE(),
              std::make_shared<velox::Variant>(percentage / 100.0)));

      const auto& input = *sample.onlyInput();
      auto* outerDt = std::exchange(currentDt_, newDt());
      makeQueryGraph(input, kAllAllowedInDt);
      addFilter(input, predicate);
      finalizeDt(sample, outerDt);
      break;
    }
  }
}

void ToGraph::addFilter(
    const lp::LogicalPlanNode& input,
    const lp::ExprPtr& predicate) {
  exprSources_.push_back(&input);
  SCOPE_EXIT {
    exprSources_.pop_back();
  };

  processSubqueries(input, predicate, /*filter=*/true);

  ExprVector flat;
  {
    allowCorrelations_ = true;
    SCOPE_EXIT {
      allowCorrelations_ = false;
    };

    translateConjuncts(predicate, flat);
  }
  {
    PlanObjectSet tables = currentDt_->tableSet;
    tables.add(currentDt_);
    std::erase_if(flat, [&](const auto* conjunct) {
      if (conjunct->allTables().isSubset(tables)) {
        return false;
      }
      correlatedConjuncts_.push_back(conjunct);
      return true;
    });
  }

  if (currentDt_->hasAggregation()) {
    currentDt_->having.insert(
        currentDt_->having.end(), flat.begin(), flat.end());
  } else {
    currentDt_->conjuncts.insert(
        currentDt_->conjuncts.end(), flat.begin(), flat.end());
  }
}

void ToGraph::addLimit(const lp::LimitNode& limit) {
  if (currentDt_->hasLimit()) {
    currentDt_->offset += limit.offset();

    if (currentDt_->limit <= limit.offset()) {
      currentDt_->limit = 0;
    } else {
      currentDt_->limit =
          std::min(limit.count(), currentDt_->limit - limit.offset());
    }
  } else {
    currentDt_->limit = limit.count();
    currentDt_->offset = limit.offset();
  }
}

void ToGraph::makeEmptyValuesTable(const lp::LogicalPlanNode& node) {
  auto* emptyData = &registerVariant(velox::Variant::array({}))->array();
  auto* valuesTable = makeValuesTable(node, emptyData);
  currentDt_->addTable(valuesTable);
}

void ToGraph::addWrite(const lp::TableWriteNode& tableWrite) {
  const auto writeKind =
      static_cast<connector::WriteKind>(tableWrite.writeKind());
  if (writeKind != connector::WriteKind::kInsert &&
      writeKind != connector::WriteKind::kCreate) {
    VELOX_NYI("Only INSERT supported for TableWrite");
  }
  VELOX_CHECK_NULL(
      currentDt_->write, "Only one TableWrite per DerivedTable is allowed");
  const auto* schemaTable =
      schema_.findTable(tableWrite.connectorId(), tableWrite.tableName());
  VELOX_CHECK_NOT_NULL(
      schemaTable,
      "Table not found: {} via connector {}",
      tableWrite.tableName(),
      tableWrite.connectorId());
  const auto* connectorTable = schemaTable->connectorTable;
  VELOX_DCHECK_NOT_NULL(connectorTable);
  const auto& tableSchema = *connectorTable->type();

  ExprVector columnExprs;
  columnExprs.reserve(tableSchema.size());
  for (uint32_t i = 0; i < tableSchema.size(); ++i) {
    const auto& columnName = tableSchema.nameOf(i);

    auto it = std::ranges::find(tableWrite.columnNames(), columnName);
    if (it != tableWrite.columnNames().end()) {
      const auto nth = it - tableWrite.columnNames().begin();
      const auto& columnExpr = tableWrite.columnExpressions()[nth];
      columnExprs.push_back(translateExpr(columnExpr));
    } else {
      const auto* tableColumn = connectorTable->findColumn(columnName);
      VELOX_DCHECK_NOT_NULL(tableColumn);
      columnExprs.push_back(
          make<Literal>(
              toConstantValue(tableColumn->type()),
              &tableColumn->defaultValue()));
    }
    VELOX_DCHECK(
        *tableSchema.childAt(i) == *columnExprs.back()->value().type,
        "Wrong column type: {}, {} vs. {}",
        columnName,
        tableSchema.childAt(i)->toString(),
        columnExprs.back()->value().type->toString());
  }

  renames_.clear();
  auto& outputType = *tableWrite.outputType();
  for (uint32_t i = 0; i < outputType.size(); ++i) {
    const auto& outputName = outputType.nameOf(i);
    const auto* outputColumn = toName(outputName);
    renames_[outputName] = make<Column>(
        outputColumn,
        currentDt_,
        toConstantValue(outputType.childAt(i)),
        outputColumn);
  }

  currentDt_->write =
      make<WritePlan>(*connectorTable, writeKind, std::move(columnExprs));
}

namespace {

bool hasNondeterministic(const lp::ExprPtr& expr) {
  if (expr->isCall()) {
    const auto* call = expr->as<lp::CallExpr>();
    if (functionBits(toName(call->name()), /*specialForm=*/false)
            .contains(FunctionSet::kNonDeterministic)) {
      return true;
    }
  }
  return std::ranges::any_of(expr->inputs(), hasNondeterministic);
}

bool hasSubquery(const lp::ExprPtr& expr) {
  if (expr->isSubquery()) {
    return true;
  }

  for (const auto& input : expr->inputs()) {
    if (hasSubquery(input)) {
      return true;
    }
  }

  return false;
}

} // namespace

void ToGraph::translateSetJoin(const lp::SetNode& set) {
  auto* setDt = currentDt_;
  for (auto& input : set.inputs()) {
    wrapInDt(*input);
  }

  const bool exists = set.operation() == lp::SetOperation::kIntersect;
  const bool anti = set.operation() == lp::SetOperation::kExcept;

  VELOX_CHECK(exists || anti);

  const auto* left = setDt->tables[0]->as<DerivedTable>();

  for (auto i = 1; i < setDt->tables.size(); ++i) {
    const auto* right = setDt->tables[i]->as<DerivedTable>();

    auto* joinEdge = exists ? JoinEdge::makeExists(left, right)
                            : JoinEdge::makeNotExists(left, right);
    for (auto i = 0; i < left->columns.size(); ++i) {
      joinEdge->addEquality(left->columns[i], right->columns[i]);
    }

    setDt->joins.push_back(joinEdge);
  }

  const auto& type = set.outputType();
  ExprVector exprs;
  ColumnVector columns;
  for (auto i = 0; i < type->size(); ++i) {
    exprs.push_back(left->columns[i]);
    const auto* columnName = toName(type->nameOf(i));
    columns.push_back(
        make<Column>(columnName, setDt, exprs.back()->value(), columnName));
    renames_[type->nameOf(i)] = columns.back();
  }

  setDt->aggregation =
      make<AggregationPlan>(exprs, AggregateVector{}, columns, columns);
  for (auto& c : columns) {
    setDt->exprs.push_back(c);
  }
  setDt->columns = columns;
}

namespace {

void translateSetOperationInput(
    const lp::LogicalPlanNode& input,
    const std::function<bool(const lp::LogicalPlanNode&)>& shouldFlatten,
    const std::function<void(const lp::LogicalPlanNode&)>& translateInput) {
  if (shouldFlatten(input)) {
    for (const auto& child : input.inputs()) {
      translateSetOperationInput(*child, shouldFlatten, translateInput);
    }
  } else {
    translateInput(input);
  }
}

} // namespace

void ToGraph::translateUnion(const lp::SetNode& set) {
  auto* setDt = currentDt_;
  setDt->setOp = set.operation();

  auto shouldFlatten = [&](const lp::LogicalPlanNode& input) {
    if (input.kind() != lp::NodeKind::kSet) {
      return false;
    }
    const auto inputSetOp = input.as<lp::SetNode>()->operation();
    const auto parentSetOp = setDt->setOp;
    if (inputSetOp == parentSetOp) {
      // Same set operation can be flattened.
      return true;
    }
    if (inputSetOp == lp::SetOperation::kUnionAll &&
        parentSetOp == lp::SetOperation::kUnion) {
      // UNION ALL can be flattened into UNION.
      return true;
    }
    return false;
  };

  auto renames = std::move(renames_);
  bool isFirstInput = true;

  auto translateUnionInput = [&](const lp::LogicalPlanNode& input) {
    renames_ = renames;
    currentDt_ = newDt();
    makeQueryGraph(input, kAllAllowedInDt);
    auto* newDt = std::exchange(currentDt_, setDt);

    const auto& type = input.outputType();

    if (isFirstInput) {
      // This is the first input of a union tree.
      for (auto i : usedChannels(input)) {
        const auto& name = type->nameOf(i);

        ExprCP inner = translateColumn(name);
        newDt->exprs.push_back(inner);

        // The top dt has the same columns as all the unioned dts.
        const auto* columnName = toName(name);
        auto* outer =
            make<Column>(columnName, setDt, inner->value(), columnName);
        setDt->columns.push_back(outer);
        newDt->columns.push_back(outer);
      }

      isFirstInput = false;
    } else {
      for (auto i : usedChannels(input)) {
        ExprCP inner = translateColumn(type->nameOf(i));
        newDt->exprs.push_back(inner);
      }

      // Same outward facing columns as the top dt of union.
      newDt->columns = setDt->columns;
    }

    setDt->children.push_back(newDt);
  };

  translateSetOperationInput(set, shouldFlatten, translateUnionInput);

  renames_ = std::move(renames);
  for (const auto* column : setDt->columns) {
    renames_[column->name()] = column;
  }
}

namespace {

// Normalized join with left/right sides and join type. RIGHT joins are
// converted to LEFT joins by swapping sides.
struct NormalizedJoin {
  lp::LogicalPlanNodePtr left;
  lp::LogicalPlanNodePtr right;
  lp::JoinType joinType;
};

// Normalizes a join by converting RIGHT join to LEFT join (swapping sides).
NormalizedJoin normalizeLogicalJoin(const lp::JoinNode& join) {
  auto left = join.left();
  auto right = join.right();
  auto joinType = join.joinType();
  if (joinType == lp::JoinType::kRight) {
    std::swap(left, right);
    joinType = lp::JoinType::kLeft;
  }
  return {std::move(left), std::move(right), joinType};
}

} // namespace

DerivedTableP ToGraph::makeQueryGraph(const lp::LogicalPlanNode& logicalPlan) {
  std::tie(controlSubfields_, payloadSubfields_) =
      SubfieldTracker([&](const auto& expr) {
        return tryFoldConstant(expr);
      }).markAll(logicalPlan);
  currentDt_ = newDt();
  makeQueryGraph(logicalPlan, kAllAllowedInDt);
  setDtUsedOutput(currentDt_, logicalPlan);

  // TODO Try constant fold the dt.

  return currentDt_;
}

void ToGraph::makeQueryGraph(
    const lp::LogicalPlanNode& node,
    uint64_t allowedInDt,
    bool excludeOuterJoins) {
  if (!contains(allowedInDt, node.kind())) {
    wrapInDt(node);
    return;
  }

  if (excludeOuterJoins && node.is(lp::NodeKind::kJoin) &&
      node.as<lp::JoinNode>()->joinType() != lp::JoinType::kInner) {
    wrapInDt(node);
    return;
  }

  ToGraphContext ctx{&node};
  velox::ExceptionContextSetter exceptionContext{makeExceptionContext(&ctx)};
  switch (node.kind()) {
    case lp::NodeKind::kValues:
      makeValuesTable(*node.as<lp::ValuesNode>());
      break;
    case lp::NodeKind::kTableScan:
      makeBaseTable(*node.as<lp::TableScanNode>());
      break;
    case lp::NodeKind::kSample:
      applySampling(*node.as<lp::SampleNode>(), allowedInDt);
      break;
    case lp::NodeKind::kFilter: {
      const auto& input = *node.onlyInput();
      const auto& filter = *node.as<lp::FilterNode>();

      if (hasSubquery(filter.predicate())) {
        excludeOuterJoins = true;
      }

      if (hasNondeterministic(filter.predicate())) {
        auto* outerDt = std::exchange(currentDt_, newDt());
        makeQueryGraph(input, kAllAllowedInDt, excludeOuterJoins);
        addFilter(input, filter.predicate());
        finalizeDt(node, outerDt);
        break;
      }

      makeQueryGraph(input, allowedInDt, excludeOuterJoins);
      addFilter(input, filter.predicate());
    } break;
    case lp::NodeKind::kProject: {
      makeQueryGraph(*node.onlyInput(), allowedInDt, excludeOuterJoins);
      addProjection(*node.as<lp::ProjectNode>());
    } break;
    case lp::NodeKind::kAggregate: {
      const auto& input = *node.onlyInput();
      makeQueryGraph(input, allowedInDt, excludeOuterJoins);
      if (currentDt_->hasAggregation() || currentDt_->hasLimit()) {
        finalizeDt(input);
      } else if (currentDt_->hasOrderBy()) {
        currentDt_->orderKeys.clear();
        currentDt_->orderTypes.clear();
      }

      auto* agg = translateAggregation(*node.as<lp::AggregateNode>());
      if (auto* result = processCorrelatedAggregation(agg)) {
        currentDt_->aggregation = result;
      }

    } break;
    case lp::NodeKind::kJoin: {
      const auto& join = *node.as<lp::JoinNode>();

      // TODO Allow mixing Unnest with Join in a single DT.
      // https://github.com/facebookincubator/axiom/issues/286
      allowedInDt = deny(
          allowedInDt,
          lp::NodeKind::kUnnest,
          lp::NodeKind::kAggregate,
          lp::NodeKind::kLimit,
          lp::NodeKind::kFilter,
          lp::NodeKind::kSort);
      if (join.joinType() != lp::JoinType::kInner) {
        allowedInDt = deny(allowedInDt, lp::NodeKind::kJoin);
      }

      auto [left, right, joinType] = normalizeLogicalJoin(join);

      makeQueryGraph(*left, allowedInDt, /*excludeOuterJoins=*/true);

      if (queryCtx()->optimization()->options().syntacticJoinOrder) {
        allowedInDt = deny(allowedInDt, lp::NodeKind::kJoin);
      }
      makeQueryGraph(*right, allowedInDt, /*excludeOuterJoins=*/true);

      translateJoin(left, right, joinType, join.condition(), join.joinType());
    } break;
    case lp::NodeKind::kSort: {
      const auto& input = *node.onlyInput();
      makeQueryGraph(input, allowedInDt);
      if (currentDt_->hasLimit()) {
        finalizeDt(input);
      }
      addOrderBy(*node.as<lp::SortNode>());
    } break;
    case lp::NodeKind::kLimit: {
      const auto& limit = *node.as<lp::LimitNode>();
      if (limit.count() == 0) {
        makeEmptyValuesTable(limit);
        break;
      }
      makeQueryGraph(*node.onlyInput(), allowedInDt);
      addLimit(limit);
      // After combining limits, if the result is 0 rows, replace with empty
      // values. This handles cases like OFFSET >= inner LIMIT.
      if (currentDt_->limit == 0) {
        currentDt_ = newDt();
        makeEmptyValuesTable(limit);
      }
    } break;
    case lp::NodeKind::kSet: {
      auto* outerDt = std::exchange(currentDt_, newDt());
      const auto& set = *node.as<lp::SetNode>();
      if (set.operation() == lp::SetOperation::kUnion ||
          set.operation() == lp::SetOperation::kUnionAll) {
        translateUnion(set);
      } else {
        translateSetJoin(set);
      }
      outerDt->addTable(currentDt_);
      currentDt_ = outerDt;
    } break;
    case lp::NodeKind::kUnnest: {
      makeQueryGraph(
          *node.onlyInput(), allowedInDt, /*excludeOuterJoins=*/true);
      addUnnest(*node.as<lp::UnnestNode>());
    } break;
    case lp::NodeKind::kTableWrite: {
      VELOX_DCHECK_EQ(allowedInDt, kAllAllowedInDt);
      wrapInDt(*node.onlyInput());
      addWrite(*node.as<lp::TableWriteNode>());
    } break;
    default:
      VELOX_NYI(
          "Unsupported PlanNode {}", lp::NodeKindName::toName(node.kind()));
  }
}

std::pair<ExprVector, OrderTypeVector> ToGraph::dedupOrdering(
    const std::vector<lp::SortingField>& ordering) {
  ExprVector deduppedOrderKeys;
  OrderTypeVector deduppedOrderTypes;
  deduppedOrderKeys.reserve(ordering.size());
  deduppedOrderTypes.reserve(ordering.size());

  folly::F14FastSet<ExprCP> uniqueOrderKeys;
  for (const auto& field : ordering) {
    const auto* key = translateExpr(field.expression);
    if (!uniqueOrderKeys.emplace(key).second) {
      continue;
    }
    deduppedOrderKeys.push_back(key);
    deduppedOrderTypes.push_back(toOrderType(field.order));
  }

  return {std::move(deduppedOrderKeys), std::move(deduppedOrderTypes)};
}

// Debug helper functions. Must be extern to be callable from debugger.

extern std::string leString(const lp::Expr* e) {
  return lp::ExprPrinter::toText(*e);
}

extern std::string lpString(const lp::LogicalPlanNode* p) {
  return lp::PlanPrinter::toText(*p);
}

} // namespace facebook::axiom::optimizer
