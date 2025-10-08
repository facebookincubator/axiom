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
#include "axiom/logical_plan/ExprPrinter.h"
#include "axiom/logical_plan/PlanPrinter.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/PlanUtils.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregateFunctionRegistry.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/expression/Expr.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/FunctionRegistry.h"

namespace facebook::axiom::optimizer {
namespace {

namespace lp = facebook::axiom::logical_plan;

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
    const Schema& schema,
    velox::core::ExpressionEvaluator& evaluator,
    const OptimizerOptions& options)
    : schema_{schema},
      evaluator_{evaluator},
      options_{options},
      equality_{toName(FunctionRegistry::instance()->equality())} {
  auto* registry = FunctionRegistry::instance();

  const auto& reversibleFunctions = registry->reversibleFunctions();
  for (const auto& [name, reverseName] : reversibleFunctions) {
    reversibleFunctions_[toName(name)] = toName(reverseName);
    reversibleFunctions_[toName(reverseName)] = toName(name);
  }

  reversibleFunctions_[SpecialFormCallNames::kAnd] = SpecialFormCallNames::kAnd;
  reversibleFunctions_[SpecialFormCallNames::kOr] = SpecialFormCallNames::kOr;

  if (auto elementAt = registry->elementAt()) {
    elementAt_ = toName(elementAt.value());
  }

  if (auto subscript = registry->subscript()) {
    subscript_ = toName(subscript.value());
  }

  if (auto cardinality = registry->cardinality()) {
    cardinality_ = toName(cardinality.value());
  }
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

void ToGraph::setDtOutput(DerivedTableP dt, const lp::LogicalPlanNode& node) {
  const auto& type = *node.outputType();
  for (const auto& name : type.names()) {
    addDtColumn(dt, name);
  }
}

void ToGraph::setDtUsedOutput(
    DerivedTableP dt,
    const lp::LogicalPlanNode& node) {
  const auto& type = *node.outputType();
  for (auto i : usedChannels(node)) {
    addDtColumn(dt, type.nameOf(i));
  }
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

ExprCP ToGraph::tryFoldConstant(
    const velox::TypePtr& returnType,
    std::string_view callName,
    const ExprVector& literals) {
  try {
    Value value(toType(returnType), 1);
    auto* veraxExpr = make<Call>(
        PlanType::kCallExpr, toName(callName), value, literals, FunctionSet());
    auto typedExpr = queryCtx()->optimization()->toTypedExpr(veraxExpr);
    auto exprSet = evaluator_.compile(typedExpr);
    auto first = exprSet->exprs().front().get();
    if (auto constantExpr =
            dynamic_cast<const velox::exec::ConstantExpr*>(first)) {
      auto typed = std::make_shared<lp::ConstantExpr>(
          constantExpr->type(),
          std::make_shared<velox::Variant>(
              constantExpr->value()->variantAt(0)));

      return makeConstant(*typed);
    }
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
        maybeIntegerLiteral(expr->inputAt(1)->asUnchecked<lp::ConstantExpr>());
    Name name = nullptr;
    int64_t id = 0;
    auto& rowType = expr->inputAt(0)->type()->as<velox::TypeKind::ROW>();
    if (maybeIndex.has_value()) {
      id = maybeIndex.value();
      name = toName(rowType.nameOf(maybeIndex.value()));
    } else {
      auto& field = expr->inputAt(1)->asUnchecked<lp::ConstantExpr>()->value();
      name = toName(field->value<velox::TypeKind::VARCHAR>());
      id = rowType.getChildIdx(name);
    }
    step.field = name;
    step.id = id;
    input = expr->inputAt(0);
    return true;
  }

  if (expr->isCall()) {
    const auto* call = expr->asUnchecked<lp::CallExpr>();
    auto name = toName(call->name());
    if (name == subscript_ || name == elementAt_) {
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
    if (name == cardinality_) {
      step.kind = StepKind::kCardinality;
      input = expr->inputAt(0);
      return true;
    }
  }
  return false;
}

void ToGraph::getExprForField(
    const lp::Expr* field,
    lp::ExprPtr& resultExpr,
    ColumnCP& resultColumn,
    const lp::LogicalPlanNode*& context) {
  while (context) {
    const auto& name = field->asUnchecked<lp::InputReferenceExpr>()->name();
    auto ordinal = context->outputType()->getChildIdx(name);
    if (context->is(lp::NodeKind::kProject)) {
      const auto* project = context->asUnchecked<lp::ProjectNode>();
      auto& def = project->expressions()[ordinal];
      context = context->inputAt(0).get();
      if (def->isInputReference()) {
        const auto* innerField = def->asUnchecked<lp::InputReferenceExpr>();
        field = innerField;
        continue;
      }
      resultExpr = def;
      return;
    }

    const auto& sources = context->inputs();

    const bool checkInContext = [&] {
      if (context->is(lp::NodeKind::kUnnest)) {
        const auto* unnest = context->asUnchecked<lp::UnnestNode>();
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

    for (const auto& source : sources) {
      const auto& row = source->outputType();
      if (auto maybe = row->getChildIdxIfExists(name)) {
        context = source.get();
        break;
      }
    }
  }
  VELOX_FAIL();
}

std::optional<ExprCP> ToGraph::translateSubfield(const lp::ExprPtr& inputExpr) {
  std::vector<Step> steps;
  auto* source = exprSource_;
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

      // if this is a field we follow to the expr assigning the field if any.
      ColumnCP column = nullptr;
      if (expr->isInputReference()) {
        getExprForField(expr.get(), expr, column, source);
        if (expr) {
          continue;
        }
      }

      SubfieldProjections* skyline = nullptr;
      if (column) {
        auto it = allColumnSubfields_.find(column);
        if (it != allColumnSubfields_.end()) {
          skyline = &it->second;
        }
      } else {
        ensureFunctionSubfields(expr);
        auto call = expr->asUnchecked<lp::CallExpr>();
        auto it = functionSubfields_.find(call);
        if (it != functionSubfields_.end()) {
          skyline = &it->second;
        }
      }

      // 'steps is a path. 'skyline' is a map from path to Expr. If no prefix
      // of steps occurs in skyline, then the item referenced by steps is not
      // materialized. Otherwise, the prefix that matches one in skyline is
      // replaced by the Expr from skyline and the tail of 'steps' are tagged
      // on the Expr. If skyline is empty, then 'steps' simply becomes a
      // nested sequence of getters.
      auto originalExprSource = exprSource_;
      SCOPE_EXIT {
        exprSource_ = originalExprSource;
      };
      exprSource_ = source;
      return makeGettersOverSkyline(steps, skyline, expr, column);
    }
    steps.push_back(step);
    expr = input;
  }
}

namespace {
PathCP innerPath(std::span<const Step> steps, int32_t last) {
  return toPath(steps.subspan(last), true);
}

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

} // namespace

ExprCP ToGraph::makeGettersOverSkyline(
    const std::vector<Step>& steps,
    const SubfieldProjections* skyline,
    const lp::ExprPtr& base,
    ColumnCP column) {
  auto last = static_cast<int32_t>(steps.size() - 1);
  ExprCP expr = nullptr;
  if (skyline) {
    // We see how many trailing (inner) steps fall below skyline, i.e. address
    // enclosing containers that are not materialized.
    bool found = false;
    for (; last >= 0; --last) {
      auto inner = innerPath(steps, last);
      auto it = skyline->pathToExpr.find(inner);
      if (it != skyline->pathToExpr.end()) {
        expr = it->second;
        found = true;
        break;
      }
    }
    if (!found) {
      // The path is not materialized. Need a longer path to intersect skyline.
      return nullptr;
    }
  } else {
    if (column) {
      expr = column;
    } else {
      trace(OptimizerOptions::kPreprocess, [&]() {
        std::cout << "Complex function with no skyline: steps="
                  << toPath(steps)->toString() << std::endl;
        std::cout << "base=" << lp::ExprPrinter::toText(*base) << std::endl;
        std::cout << "Columns=";
        for (auto& name : exprSource_->outputType()->names()) {
          std::cout << name << " ";
        }
        std::cout << std::endl;
      });
      expr = translateExpr(base);
    }
    last = static_cast<int32_t>(steps.size());
  }

  for (int32_t i = last - 1; i >= 0; --i) {
    // We make a getter over expr made so far with 'steps[i]' as first.
    PathExpr pathExpr{steps[i], nullptr, expr};
    auto it = deduppedGetters_.find(pathExpr);
    if (it != deduppedGetters_.end()) {
      expr = it->second;
    } else {
      const auto& step = steps[i];
      auto inputType = expr->value().type;
      switch (step.kind) {
        case StepKind::kField: {
          if (step.field) {
            auto childType = toType(inputType->asRow().findChild(step.field));
            expr = make<Field>(childType, expr, step.field);
          } else {
            auto childType = toType(inputType->childAt(step.id));
            expr = make<Field>(childType, expr, step.id);
          }
          break;
        }

        case StepKind::kSubscript: {
          // Type of array element or map value.
          auto valueType =
              toType(inputType->childAt(inputType->isArray() ? 0 : 1));

          // Type of array index or map key.
          auto subscriptType = toType(
              inputType->isArray() ? velox::INTEGER() : inputType->childAt(0));

          ExprVector args{
              expr,
              make<Literal>(
                  Value(subscriptType, 1),
                  subscriptLiteral(subscriptType->kind(), step)),
          };

          expr = make<Call>(
              subscript_, Value(valueType, 1), std::move(args), FunctionSet());
          break;
        }

        case StepKind::kCardinality: {
          expr = make<Call>(
              cardinality_,
              Value(toType(velox::BIGINT()), 1),
              ExprVector{expr},
              FunctionSet());
          break;
        }
        default:
          VELOX_NYI();
      }

      deduppedGetters_[pathExpr] = expr;
    }
  }
  return expr;
}

namespace {
std::optional<BitSet> findSubfields(
    const PlanSubfields& fields,
    const lp::CallExpr* call) {
  auto it = fields.argFields.find(call);
  if (it == fields.argFields.end()) {
    return std::nullopt;
  }
  auto& paths = it->second.resultPaths;
  auto it2 = paths.find(ResultAccess::kSelf);
  if (it2 == paths.end()) {
    return {};
  }
  return it2->second;
}
} // namespace

BitSet ToGraph::functionSubfields(
    const lp::CallExpr* call,
    bool controlOnly,
    bool payloadOnly) {
  BitSet subfields;
  if (!controlOnly) {
    auto maybe = findSubfields(payloadSubfields_, call);
    if (maybe.has_value()) {
      subfields = maybe.value();
    }
  }
  if (!payloadOnly) {
    auto maybe = findSubfields(controlSubfields_, call);
    if (maybe.has_value()) {
      subfields.unionSet(maybe.value());
    }
  }
  Path::subfieldSkyline(subfields);
  return subfields;
}

void ToGraph::ensureFunctionSubfields(const lp::ExprPtr& expr) {
  if (expr->isCall()) {
    const auto* call = expr->asUnchecked<lp::CallExpr>();
    if (functionMetadata(velox::exec::sanitizeName(call->name()))) {
      if (!translatedSubfieldFuncs_.contains(call)) {
        translateExpr(expr);
      }
    }
  }
}

namespace {

/// If we should reverse the sides of a binary expression to canonicalize it. We
/// invert in two cases:
///
///  #1. If there is a literal in the left and something else in the right:
///    f("literal", col) => f(col, "literal")
///
///  #2. If none are literal, but the id on the left is higher.
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

bool ToGraph::isJoinEquality(
    ExprCP expr,
    std::vector<PlanObjectP>& tables,
    ExprCP& left,
    ExprCP& right) const {
  if (expr->is(PlanType::kCallExpr)) {
    auto call = expr->as<Call>();
    if (call->name() == equality_) {
      left = call->argAt(0);
      right = call->argAt(1);

      auto leftTable = left->singleTable();
      auto rightTable = right->singleTable();
      if (!leftTable || !rightTable) {
        return false;
      }

      if (leftTable == tables[1]) {
        std::swap(left, right);
      }
      return true;
    }
  }
  return false;
}

ExprCP ToGraph::makeConstant(const lp::ConstantExpr& constant) {
  TypedVariant temp{toType(constant.type()), constant.value()};
  auto it = constantDedup_.find(temp);
  if (it != constantDedup_.end()) {
    return it->second;
  }

  auto* literal = make<Literal>(Value(temp.type, 1), temp.value.get());

  constantDedup_[std::move(temp)] = literal;
  return literal;
}

namespace {
// Returns bits describing function 'name'.
FunctionSet functionBits(Name name) {
  if (auto* md = functionMetadata(name)) {
    return md->functionSet;
  }

  const auto deterministic = velox::isDeterministic(name);
  if (deterministic.has_value() && !deterministic.value()) {
    return FunctionSet(FunctionSet::kNonDeterministic);
  }

  return FunctionSet(0);
}

} // namespace

ExprCP ToGraph::translateExpr(const lp::ExprPtr& expr) {
  if (expr->isInputReference()) {
    return translateColumn(expr->asUnchecked<lp::InputReferenceExpr>()->name());
  }

  if (expr->isConstant()) {
    return makeConstant(*expr->asUnchecked<lp::ConstantExpr>());
  }

  if (auto path = translateSubfield(expr)) {
    return path.value();
  }

  if (expr->isLambda()) {
    return translateLambda(expr->asUnchecked<lp::LambdaExpr>());
  }

  ToGraphContext ctx(expr.get());
  velox::ExceptionContextSetter exceptionContext(makeExceptionContext(&ctx));

  const auto* call =
      expr->isCall() ? expr->asUnchecked<lp::CallExpr>() : nullptr;
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

  const auto* specialForm = expr->isSpecialForm()
      ? expr->asUnchecked<lp::SpecialFormExpr>()
      : nullptr;

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

    funcs = funcs | functionBits(name);
    auto* callExpr = deduppedCall(
        name, Value(toType(expr->type()), cardinality), std::move(args), funcs);
    return callExpr;
  }

  VELOX_NYI();
  return nullptr;
}

ExprCP ToGraph::translateLambda(const lp::LambdaExpr* lambda) {
  auto savedRenames = renames_;
  const auto& row = lambda->signature();
  toType(row);
  toType(lambda->type());
  ColumnVector args;
  for (auto i = 0; i < row->size(); ++i) {
    auto col = make<Column>(
        toName(row->nameOf(i)), nullptr, Value(toType(row->childAt(i)), 1));
    args.push_back(col);
    renames_[row->nameOf(i)] = col;
  }
  auto body = translateExpr(lambda->body());
  renames_ = std::move(savedRenames);
  return make<Lambda>(std::move(args), toType(lambda->type()), body);
}

namespace {
// Returns a mask that allows 'op' in the same derived table.
uint64_t allow(PlanType op) {
  return 1UL << static_cast<uint32_t>(op);
}

// True if 'op' is in 'mask.
bool contains(uint64_t mask, PlanType op) {
  return 0 != (mask & (1UL << static_cast<uint32_t>(op)));
}
} // namespace

std::optional<ExprCP> ToGraph::translateSubfieldFunction(
    const lp::CallExpr* call,
    const FunctionMetadata* metadata) {
  translatedSubfieldFuncs_.insert(call);

  auto subfields = functionSubfields(call, false, false);
  if (subfields.empty()) {
    // The function is accessed as a whole.
    return std::nullopt;
  }

  auto* ctx = queryCtx();
  std::vector<PathCP> paths;
  subfields.forEach([&](auto id) { paths.push_back(ctx->pathById(id)); });

  BitSet usedArgs;
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
      if (auto maybeArg = stepToArg(step, metadata)) {
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
          Value(toType(inputType), 1),
          make<velox::Variant>(velox::Variant::null(inputType->kind())));
    }
  }

  auto* name = toName(velox::exec::sanitizeName(call->name()));
  funcs = funcs | functionBits(name);

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
      make<Call>(name, Value(toType(call->type()), cardinality), args, funcs);
  return callExpr;
}

ExprCP ToGraph::translateColumn(std::string_view name) {
  auto it = renames_.find(name);
  if (it != renames_.end()) {
    return it->second;
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

void ToGraph::translateUnnest(const lp::UnnestNode& unnest, bool isNewDt) {
  if (unnest.ordinalityName().has_value()) {
    VELOX_NYI(
        "Unnest ordinality column is not supported in Verax optimizer. Unnest node: {}",
        unnest.id());
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
    leftTable = currentDt_;
    if (!isNewDt) {
      finalizeDt(*unnest.onlyInput());
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
      const auto* unnestedType = unnestExpr->value().type->childAt(j).get();
      // TODO Value cardinality also should be multiplied by the max from all
      // columns average expected number of elements per unnested element.
      // Other Value properties also should be computed.
      Value value{unnestedType, maxCardinality};
      const auto* columnName = toName(unnestedNames[j]);
      auto* column = make<Column>(columnName, unnestTable, value, columnName);
      unnestTable->columns.push_back(column);
      renames_[columnName] = column;
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

    const auto& metadata =
        velox::exec::getAggregateFunctionMetadata(aggregate->name());

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

    if (!orderKeys.empty()) {
      const auto& options = queryCtx()->optimization()->runnerOptions();
      VELOX_CHECK(
          options.numWorkers == 1 && options.numDrivers == 1,
          "ORDER BY option for aggregation is supported only in single worker, single thread mode");
    }

    auto aggName = toName(aggregate->name());
    auto name = toName(agg.outputNames()[channel]);

    AggregateDedupKey key{
        aggName, isDistinct, condition, args, orderKeys, orderTypes};

    auto it = uniqueAggregates.try_emplace(key).first;
    if (it->second) {
      newRenames[name] = it->second;
    } else {
      auto accumulatorType = toType(
          velox::exec::resolveAggregateFunction(aggregate->name(), argTypes)
              .second);
      Value finalValue(toType(aggregate->type()), 1);

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

PlanObjectP ToGraph::addOrderBy(const lp::SortNode& order) {
  auto [deduppedOrderKeys, deduppedOrderTypes] =
      dedupOrdering(order.ordering());

  currentDt_->orderKeys = std::move(deduppedOrderKeys);
  currentDt_->orderTypes = std::move(deduppedOrderTypes);

  return currentDt_;
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

void ToGraph::translateJoin(const lp::JoinNode& join) {
  const auto& joinLeft = join.left();
  const auto& joinRight = join.right();

  const auto joinType = join.joinType();
  const bool isInner = joinType == lp::JoinType::kInner;

  // TODO Allow mixing Unnest with Join in a single DT.
  // https://github.com/facebookexperimental/verax/issues/286
  const auto allowedInDt = allow(PlanType::kJoinNode);
  makeQueryGraph(*joinLeft, allowedInDt);

  // For an inner join a join tree on the right can be flattened, for all other
  // kinds it must be kept together in its own dt.

  DerivedTableP previousDt = nullptr;
  if (isNondeterministicWrap_) {
    previousDt = currentDt_;
    currentDt_ = newDt();

    isNondeterministicWrap_ = false;
  }
  makeQueryGraph(
      *joinRight,
      (isInner && !queryCtx()->optimization()->options().syntacticJoinOrder)
          ? allowedInDt
          : 0);

  if (previousDt) {
    finalizeDt(*joinRight, previousDt);
  }

  ExprVector conjuncts;
  translateConjuncts(join.condition(), conjuncts);

  if (isInner) {
    currentDt_->conjuncts.insert(
        currentDt_->conjuncts.end(), conjuncts.begin(), conjuncts.end());
  } else {
    const bool leftOptional =
        joinType == lp::JoinType::kRight || joinType == lp::JoinType::kFull;
    const bool rightOptional =
        joinType == lp::JoinType::kLeft || joinType == lp::JoinType::kFull;

    // If non-inner, and many tables on the right they are one dt. If a single
    // table then this too is the last in 'tables'.
    auto rightTable = currentDt_->tables.back();

    ExprVector leftKeys;
    ExprVector rightKeys;
    PlanObjectSet leftTables;
    extractNonInnerJoinEqualities(
        equality_, conjuncts, rightTable, leftKeys, rightKeys, leftTables);

    auto leftTableVector = leftTables.toObjects();

    auto* edge = make<JoinEdge>(
        leftTableVector.size() == 1 ? leftTableVector[0] : nullptr,
        rightTable,
        JoinEdge::Spec{
            .filter = std::move(conjuncts),
            .leftOptional = leftOptional,
            .rightOptional = rightOptional});
    currentDt_->joins.push_back(edge);
    for (auto i = 0; i < leftKeys.size(); ++i) {
      edge->addEquality(leftKeys[i], rightKeys[i]);
    }
  }
}

DerivedTableP ToGraph::newDt() {
  auto* dt = make<DerivedTable>();
  dt->cname = newCName("dt");
  return dt;
}

PlanObjectP ToGraph::wrapInDt(const lp::LogicalPlanNode& node) {
  DerivedTableP previousDt = currentDt_;

  currentDt_ = newDt();
  makeQueryGraph(node, kAllAllowedInDt);

  finalizeDt(node, previousDt);

  return currentDt_;
}

void ToGraph::finalizeDt(
    const lp::LogicalPlanNode& node,
    DerivedTableP outerDt) {
  DerivedTableP dt = currentDt_;
  setDtUsedOutput(dt, node);

  currentDt_ = outerDt != nullptr ? outerDt : newDt();
  currentDt_->addTable(dt);

  dt->makeInitialPlan();
}

PlanObjectP ToGraph::makeBaseTable(const lp::TableScanNode& tableScan) {
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
    auto schemaColumn = schemaTable->findColumn(name);
    auto value = schemaColumn->value();
    auto* column = make<Column>(
        toName(name),
        baseTable,
        value,
        toName(type->nameOf(i)),
        schemaColumn->name());
    baseTable->columns.push_back(column);

    const auto kind = column->value().type->kind();
    if (kind == velox::TypeKind::ARRAY || kind == velox::TypeKind::ROW ||
        kind == velox::TypeKind::MAP) {
      BitSet allPaths;
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
                      << baseTable->schemaTable->name << " " << column->name()
                      << ":" << allPaths.size() << std::endl;
          });
          makeSubfieldColumns(baseTable, column, allPaths);
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

  optimization->setLeafSelectivity(*baseTable, scanType);
  currentDt_->addTable(baseTable);

  return baseTable;
}

PlanObjectP ToGraph::makeValuesTable(const lp::ValuesNode& values) {
  auto* valuesTable = make<ValuesTable>(values);
  valuesTable->cname = newCName("vt");
  planLeaves_[&values] = valuesTable;

  auto channels = usedChannels(values);
  const auto& type = values.outputType();
  const auto& names = values.outputType()->names();
  const auto cardinality = valuesTable->cardinality();
  for (auto i : channels) {
    VELOX_DCHECK_LT(i, type->size());

    const auto& name = names[i];
    Value value{toType(type->childAt(i)), cardinality};
    const auto* columnName = toName(name);
    auto* column = make<Column>(columnName, valuesTable, value, columnName);
    valuesTable->columns.push_back(column);

    renames_[name] = column;
  }

  currentDt_->addTable(valuesTable);

  return valuesTable;
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
} // namespace

void ToGraph::makeSubfieldColumns(
    BaseTable* baseTable,
    ColumnCP column,
    const BitSet& paths) {
  SubfieldProjections projections;
  auto* ctx = queryCtx();
  float card =
      baseTable->schemaTable->cardinality * baseTable->filterSelectivity;
  paths.forEach([&](auto id) {
    auto* path = ctx->pathById(id);
    auto type = pathType(column->value().type, path);
    Value value(type, card);
    auto name = fmt::format("{}.{}", column->name(), path->toString());
    auto* subcolumn = make<Column>(
        toName(name), baseTable, value, nullptr, nullptr, column, path);
    baseTable->columns.push_back(subcolumn);
    projections.pathToExpr[path] = subcolumn;
  });
  allColumnSubfields_[column] = std::move(projections);
}

PlanObjectP ToGraph::addProjection(const lp::ProjectNode* project) {
  exprSource_ = project->onlyInput().get();
  const auto& names = project->names();
  const auto& exprs = project->expressions();
  auto channels = usedChannels(*project);
  trace(OptimizerOptions::kPreprocess, [&]() {
    for (auto i = 0; i < exprs.size(); ++i) {
      if (std::ranges::find(channels, i) == channels.end()) {
        std::cout << "P=" << project->id()
                  << " dropped projection name=" << names[i] << " = "
                  << lp::ExprPrinter::toText(*exprs[i]) << std::endl;
      }
    }
  });

  for (auto i : channels) {
    if (exprs[i]->isInputReference()) {
      const auto& name =
          exprs[i]->asUnchecked<lp::InputReferenceExpr>()->name();
      // A variable projected to itself adds no renames. Inputs contain this
      // all the time.
      if (name == names[i]) {
        continue;
      }
    }

    auto expr = translateExpr(exprs.at(i));
    renames_[names[i]] = expr;
  }

  return currentDt_;
}

PlanObjectP ToGraph::addFilter(const lp::FilterNode* filter) {
  exprSource_ = filter->onlyInput().get();

  ExprVector flat;
  translateConjuncts(filter->predicate(), flat);

  if (currentDt_->hasAggregation()) {
    currentDt_->having.insert(
        currentDt_->having.end(), flat.begin(), flat.end());
  } else {
    currentDt_->conjuncts.insert(
        currentDt_->conjuncts.end(), flat.begin(), flat.end());
  }

  return currentDt_;
}

PlanObjectP ToGraph::addAggregation(const lp::AggregateNode& aggNode) {
  currentDt_->aggregation = translateAggregation(aggNode);
  return currentDt_;
}

PlanObjectP ToGraph::addLimit(const lp::LimitNode& limitNode) {
  if (currentDt_->hasLimit()) {
    currentDt_->offset += limitNode.offset();

    if (currentDt_->limit <= limitNode.offset()) {
      currentDt_->limit = 0;
    } else {
      currentDt_->limit =
          std::min(limitNode.count(), currentDt_->limit - limitNode.offset());
    }
  } else {
    currentDt_->limit = limitNode.count();
    currentDt_->offset = limitNode.offset();
  }

  return currentDt_;
}

PlanObjectP ToGraph::addWrite(const lp::TableWriteNode& tableWrite) {
  const auto writeKind =
      static_cast<connector::WriteKind>(tableWrite.writeKind());
  if (writeKind != connector::WriteKind::kInsert) {
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
      columnExprs.push_back(make<Literal>(
          Value{toType(tableColumn->type()), 1}, &tableColumn->defaultValue()));
    }
    VELOX_DCHECK(*tableSchema.childAt(i) == *columnExprs.back()->value().type);
  }

  renames_.clear();
  auto& outputType = *tableWrite.outputType();
  for (uint32_t i = 0; i < outputType.size(); ++i) {
    const auto& outputName = outputType.nameOf(i);
    const auto* outputColumn = toName(outputName);
    renames_[outputName] = make<Column>(
        outputColumn,
        currentDt_,
        Value{toType(outputType.childAt(i)), 1},
        outputColumn);
  }

  currentDt_->write =
      make<WritePlan>(*connectorTable, writeKind, std::move(columnExprs));

  return currentDt_;
}

namespace {

bool hasNondeterministic(const lp::ExprPtr& expr) {
  if (expr->isCall()) {
    const auto* call = expr->asUnchecked<lp::CallExpr>();
    if (functionBits(toName(call->name()))
            .contains(FunctionSet::kNonDeterministic)) {
      return true;
    }
  }
  return std::ranges::any_of(expr->inputs(), hasNondeterministic);
}

} // namespace

DerivedTableP ToGraph::translateSetJoin(
    const lp::SetNode& set,
    DerivedTableP setDt) {
  auto previousDt = currentDt_;
  currentDt_ = setDt;
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
  setDt->makeInitialPlan();
  currentDt_ = previousDt;
  return setDt;
}

void ToGraph::makeUnionDistributionAndStats(
    DerivedTableP setDt,
    DerivedTableP innerDt) {
  if (setDt->distribution == nullptr) {
    setDt->distribution = make<Distribution>();
  }
  if (innerDt == nullptr) {
    innerDt = setDt;
  }
  if (innerDt->children.empty()) {
    VELOX_CHECK_EQ(
        innerDt->columns.size(),
        setDt->columns.size(),
        "Union inputs must have same arity also after pruning");

    auto plan = innerDt->bestInitialPlan()->op;

    setDt->cardinality += plan->resultCardinality();
    for (auto i = 0; i < setDt->columns.size(); ++i) {
      // The Column is created in setDt before all branches are planned so the
      // value is mutated here.
      auto mutableValue =
          const_cast<float*>(&setDt->columns[i]->value().cardinality);
      *mutableValue += plan->columns()[i]->value().cardinality;
    }
  } else {
    for (auto& child : innerDt->children) {
      makeUnionDistributionAndStats(setDt, child);
    }
  }
}

DerivedTableP ToGraph::translateUnion(
    const lp::SetNode& set,
    DerivedTableP setDt,
    bool isTopLevel,
    bool& isLeftLeaf) {
  auto initialRenames = std::move(renames_);
  QGVector<DerivedTableP> children;
  DerivedTableP previousDt = currentDt_;
  for (auto& input : set.inputs()) {
    renames_ = initialRenames;

    currentDt_ = newDt();

    auto& newDt = currentDt_;

    auto isUnionLike =
        [](const lp::LogicalPlanNode& node) -> const lp::SetNode* {
      if (node.kind() == lp::NodeKind::kSet) {
        const auto* set = node.asUnchecked<lp::SetNode>();
        if (set->operation() == lp::SetOperation::kUnion ||
            set->operation() == lp::SetOperation::kUnionAll) {
          return set;
        }
      }

      return nullptr;
    };

    if (auto* setNode = isUnionLike(*input)) {
      auto inner = translateUnion(*setNode, setDt, false, isLeftLeaf);
      children.push_back(inner);
    } else {
      makeQueryGraph(*input, kAllAllowedInDt);

      const auto& type = input->outputType();

      if (isLeftLeaf) {
        // This is the left leaf of a union tree.
        for (auto i : usedChannels(*input)) {
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
        isLeftLeaf = false;
      } else {
        for (auto i : usedChannels(*input)) {
          ExprCP inner = translateColumn(type->nameOf(i));
          newDt->exprs.push_back(inner);
        }

        // Same outward facing columns as the top dt of union.
        newDt->columns = setDt->columns;
      }

      newDt->makeInitialPlan();
      children.push_back(newDt);
    }
  }

  currentDt_ = previousDt;
  if (isTopLevel) {
    setDt->children = std::move(children);
    setDt->setOp = set.operation();

    makeUnionDistributionAndStats(setDt);

    renames_ = std::move(initialRenames);
    for (const auto* column : setDt->columns) {
      renames_[column->name()] = column;
    }
  } else {
    setDt = newDt();
    setDt->children = std::move(children);
    setDt->setOp = set.operation();
  }
  return setDt;
}

DerivedTableP ToGraph::makeQueryGraph(const lp::LogicalPlanNode& logicalPlan) {
  markAllSubfields(logicalPlan);

  currentDt_ = newDt();
  makeQueryGraph(logicalPlan, kAllAllowedInDt);
  return currentDt_;
}

namespace {
// Removes 'op' from the set of operators allowed in the current derived
// table. makeQueryGraph() starts a new derived table if it finds an operator
// that does not belong to the mask.
uint64_t makeDtIf(uint64_t mask, PlanType op) {
  return mask & ~(1UL << static_cast<uint32_t>(op));
}
} // namespace

PlanObjectP ToGraph::makeQueryGraph(
    const lp::LogicalPlanNode& node,
    uint64_t allowedInDt) {
  ToGraphContext ctx{&node};
  velox::ExceptionContextSetter exceptionContext{makeExceptionContext(&ctx)};
  switch (node.kind()) {
    case lp::NodeKind::kValues:
      return makeValuesTable(*node.asUnchecked<lp::ValuesNode>());

    case lp::NodeKind::kTableScan:
      return makeBaseTable(*node.asUnchecked<lp::TableScanNode>());

    case lp::NodeKind::kFilter: {
      // Multiple filters are allowed before a limit. If DT has a groupBy, then
      // filter is added to 'having', otherwise, to 'conjuncts'.
      const auto* filter = node.asUnchecked<lp::FilterNode>();

      if (!isNondeterministicWrap_ &&
          hasNondeterministic(filter->predicate())) {
        // Force wrap the filter and its input inside a dt so the filter
        // does not get mixed with parent nodes.
        makeQueryGraph(*node.onlyInput(), allowedInDt);

        if (currentDt_->hasLimit()) {
          finalizeDt(*node.onlyInput());
        }

        addFilter(filter);
        finalizeDt(node);

        isNondeterministicWrap_ = true;
        return currentDt_;
      }

      isNondeterministicWrap_ = false;
      makeQueryGraph(*node.onlyInput(), allowedInDt);

      if (currentDt_->hasLimit()) {
        finalizeDt(*node.onlyInput());
      }
      return addFilter(filter);
    }

    case lp::NodeKind::kProject:
      // A project is always allowed in a DT. Multiple projects are combined.
      makeQueryGraph(*node.onlyInput(), allowedInDt);
      return addProjection(node.asUnchecked<lp::ProjectNode>());

    case lp::NodeKind::kAggregate:
      if (!contains(allowedInDt, PlanType::kAggregationNode)) {
        return wrapInDt(node);
      }

      // A single groupBy is allowed before a limit. If arrives after orderBy,
      // then orderBy is dropped. If arrives after limit, then starts a new DT.

      makeQueryGraph(*node.onlyInput(), allowedInDt);

      if (currentDt_->hasAggregation() || currentDt_->hasLimit()) {
        finalizeDt(*node.onlyInput());
      } else if (currentDt_->hasOrderBy()) {
        currentDt_->orderKeys.clear();
        currentDt_->orderTypes.clear();
      }

      addAggregation(*node.asUnchecked<lp::AggregateNode>());

      return currentDt_;

    case lp::NodeKind::kJoin:
      if (!contains(allowedInDt, PlanType::kJoinNode)) {
        return wrapInDt(node);
      }

      translateJoin(*node.asUnchecked<lp::JoinNode>());
      return currentDt_;

    case lp::NodeKind::kSort:
      // Multiple orderBys are allowed before a limit. Last one wins. Previous
      // are dropped. If arrives after limit, then starts a new DT.

      makeQueryGraph(*node.onlyInput(), allowedInDt);

      if (currentDt_->hasLimit()) {
        finalizeDt(*node.onlyInput());
      }

      return addOrderBy(*node.asUnchecked<lp::SortNode>());

    case lp::NodeKind::kLimit: {
      // Multiple limits are allowed. If already present, then it is combined
      // with the new limit.
      makeQueryGraph(*node.onlyInput(), allowedInDt);
      return addLimit(*node.asUnchecked<lp::LimitNode>());
    }

    case lp::NodeKind::kSet: {
      auto* setDt = newDt();

      auto* set = node.asUnchecked<lp::SetNode>();
      if (set->operation() == lp::SetOperation::kUnion ||
          set->operation() == lp::SetOperation::kUnionAll) {
        bool isLeftLeaf = true;
        translateUnion(*set, setDt, true, isLeftLeaf);
      } else {
        translateSetJoin(*set, setDt);
      }
      currentDt_->addTable(setDt);
      return currentDt_;
    }

    case lp::NodeKind::kUnnest: {
      if (!contains(allowedInDt, PlanType::kUnnestTableNode)) {
        return wrapInDt(node);
      }

      // Multiple unnest is allowed in a DT.
      // If arrives after groupBy, orderBy, limit then starts a new DT.
      const auto& input = *node.onlyInput();
      makeQueryGraph(input, allowedInDt);

      const bool isNewDt = currentDt_->hasAggregation() ||
          currentDt_->hasOrderBy() || currentDt_->hasLimit();
      if (isNewDt) {
        finalizeDt(input);
      }
      translateUnnest(*node.asUnchecked<lp::UnnestNode>(), isNewDt);
      return currentDt_;
    }

    case lp::NodeKind::kTableWrite:
      wrapInDt(*node.onlyInput());
      return addWrite(*node.asUnchecked<lp::TableWriteNode>());

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
