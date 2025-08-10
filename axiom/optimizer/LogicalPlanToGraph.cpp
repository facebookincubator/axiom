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

#include "axiom/logical_plan/ExprPrinter.h"
#include "axiom/logical_plan/PlanPrinter.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/PlanUtils.h"
#include "velox/exec/AggregateFunctionRegistry.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/vector/VariantToVector.h"

namespace facebook::velox::optimizer {

namespace lp = facebook::velox::logical_plan;

void Optimization::setDerivedTableOutput(
    DerivedTableP dt,
    const lp::LogicalPlanNode& planNode) {
  const auto& outputType = planNode.outputType();
  for (auto i = 0; i < outputType->size(); ++i) {
    const auto& fieldType = outputType->childAt(i);
    const auto& fieldName = outputType->nameOf(i);

    auto expr = translateColumn(fieldName);
    Value value(toType(fieldType), 0);
    auto* column = make<Column>(toName(fieldName), dt, value);
    dt->columns.push_back(column);
    dt->exprs.push_back(expr);
    renames_[fieldName] = column;
  }
}

DerivedTableP Optimization::makeQueryGraphFromLogical() {
  markAllSubfields(logicalPlan_->outputType().get(), logicalPlan_);

  root_ = newDt();
  currentSelect_ = root_;

  makeQueryGraph(*logicalPlan_, kAllAllowedInDt);
  return root_;
}

void Optimization::translateConjuncts(
    const lp::ExprPtr& input,
    ExprVector& flat) {
  if (!input) {
    return;
  }
  if (isSpecialForm(input.get(), lp::SpecialForm::kAnd)) {
    for (auto& child : input->inputs()) {
      translateConjuncts(child, flat);
    }
  } else {
    flat.push_back(translateExpr(input));
  }
}

std::shared_ptr<const exec::ConstantExpr> Optimization::foldConstant(
    const core::TypedExprPtr& typedExpr) {
  auto exprSet = evaluator_.compile(typedExpr);
  const auto& first = exprSet->exprs().front();

  if (first->isConstant()) {
    return std::dynamic_pointer_cast<exec::ConstantExpr>(first);
  }
  return nullptr;
}

ExprCP Optimization::tryFoldConstant(
    const lp::CallExpr* call,
    const lp::SpecialFormExpr* cast,
    const ExprVector& literals) {
  try {
    Value value(call ? toType(call->type()) : toType(cast->type()), 1);
    auto* veraxExpr = make<Call>(
        PlanType::kCall,
        cast ? toName("cast") : toName(call->name()),
        value,
        literals,
        FunctionSet());
    auto typedExpr = toTypedExpr(veraxExpr);
    auto exprSet = evaluator_.compile(typedExpr);
    auto first = exprSet->exprs().front().get();
    if (auto constantExpr = dynamic_cast<const exec::ConstantExpr*>(first)) {
      auto typed = std::make_shared<lp::ConstantExpr>(
          constantExpr->type(),
          std::make_shared<Variant>(vectorToVariant(constantExpr->value(), 0)));

      return makeConstant(*typed);
    }
  } catch (const std::exception&) {
    // Swallow exception.
  }

  return nullptr;
}

bool Optimization::isSubfield(
    const lp::Expr* expr,
    Step& step,
    lp::ExprPtr& input) {
  if (isSpecialForm(expr, lp::SpecialForm::kDereference)) {
    step.kind = StepKind::kField;
    auto maybeIndex =
        maybeIntegerLiteral(expr->inputAt(1)->asUnchecked<lp::ConstantExpr>());
    Name name = nullptr;
    int64_t id = 0;
    auto& rowType = expr->inputAt(0)->type()->as<TypeKind::ROW>();
    if (maybeIndex.has_value()) {
      id = maybeIndex.value();
      name = toName(rowType.nameOf(maybeIndex.value()));
    } else {
      auto& field = expr->inputAt(1)->asUnchecked<lp::ConstantExpr>()->value();
      name = toName(field->value<TypeKind::VARCHAR>());
      id = rowType.getChildIdx(name);
    }
    step.field = name;
    step.id = id;
    input = expr->inputAt(0);
    return true;
  }

  if (const auto* call = expr->asUnchecked<lp::CallExpr>()) {
    auto name = call->name();
    if (name == "subscript" || name == "element_at") {
      auto subscript = translateExpr(call->inputAt(1));
      if (subscript->type() == PlanType::kLiteral) {
        step.kind = StepKind::kSubscript;
        auto& literal = subscript->as<Literal>()->literal();
        switch (subscript->value().type->kind()) {
          case TypeKind::VARCHAR:
            step.field = toName(literal.value<TypeKind::VARCHAR>());
            break;
          case TypeKind::BIGINT:
          case TypeKind::INTEGER:
          case TypeKind::SMALLINT:
          case TypeKind::TINYINT:
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
    if (name == "cardinality") {
      step.kind = StepKind::kCardinality;
      input = expr->inputAt(0);
      return true;
    }
  }
  return false;
}

void Optimization::getExprForField(
    const lp::Expr* field,
    lp::ExprPtr& resultExpr,
    ColumnCP& resultColumn,
    const lp::LogicalPlanNode*& context) {
  for (;;) {
    auto& name = field->asUnchecked<lp::InputReferenceExpr>()->name();
    auto ordinal = context->outputType()->getChildIdx(name);
    if (const auto* project = context->asUnchecked<lp::ProjectNode>()) {
      auto& def = project->expressions()[ordinal];
      if (const auto* innerField = def->asUnchecked<lp::InputReferenceExpr>()) {
        context = context->inputAt(0).get();
        field = innerField;
        continue;
      }
      resultExpr = def;
      context = project->inputAt(0).get();
      return;
    }

    const auto& sources = context->inputs();
    if (sources.empty()) {
      auto leaf = findLeaf(context);
      auto it = renames_.find(name);
      VELOX_CHECK(it != renames_.end());
      auto maybeColumn = it->second;
      VELOX_CHECK(maybeColumn->type() == PlanType::kColumn);
      resultColumn = maybeColumn->as<Column>();
      resultExpr = nullptr;
      context = nullptr;
      const auto* relation = resultColumn->relation();
      VELOX_CHECK_NOT_NULL(relation);
      if (relation->type() == PlanType::kTable ||
          relation->type() == PlanType::kValuesTable) {
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
}

std::optional<ExprCP> Optimization::translateSubfield(
    const lp::ExprPtr& inputExpr) {
  std::vector<Step> steps;
  auto* source = logicalExprSource_;
  auto expr = inputExpr;
  for (;;) {
    lp::ExprPtr input;
    Step step;
    VELOX_CHECK_NOT_NULL(expr);
    bool isStep = isSubfield(expr.get(), step, input);
    if (!isStep) {
      if (steps.empty()) {
        return std::nullopt;
      }
      // if this is a field we follow to the expr assigning the field if any.
      Step ignore;
      lp::ExprPtr ignore2;
      if (!isSubfield(expr.get(), ignore, ignore2)) {
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
          auto it = logicalFunctionSubfields_.find(call);
          if (it != logicalFunctionSubfields_.end()) {
            skyline = &it->second;
          }
        }
        // 'steps is a path. 'skyline' is a map from path to Expr. If no prefix
        // of steps occurs in skyline, then the item referenced by steps is not
        // materialized. Otherwise, the prefix that matches one in skyline is
        // replaced by the Expr from skyline and the tail of 'steps' are tagged
        // on the Expr. If skyline is empty, then 'steps' simply becomes a
        // nested sequence of getters.
        if (steps.empty()) {
          return std::nullopt;
        }
        return makeGettersOverSkyline(steps, skyline, expr, column);
      }
    }
    steps.push_back(step);
    expr = input;
  }
}

namespace {
PathCP innerPath(const std::vector<Step>& steps, int32_t last) {
  std::vector<Step> reverse;
  for (int32_t i = steps.size() - 1; i >= last; --i) {
    reverse.push_back(steps[i]);
  }
  return toPath(std::move(reverse));
}
} // namespace

Variant* subscriptLiteral(TypeKind kind, const Step& step) {
  auto* ctx = queryCtx();
  switch (kind) {
    case TypeKind::VARCHAR:
      return ctx->registerVariant(
          std::make_unique<Variant>(std::string(step.field)));
    case TypeKind::BIGINT:
      return ctx->registerVariant(
          std::make_unique<Variant>(static_cast<int64_t>(step.id)));
    case TypeKind::INTEGER:
      return ctx->registerVariant(
          std::make_unique<Variant>(static_cast<int32_t>(step.id)));
    case TypeKind::SMALLINT:
      return ctx->registerVariant(
          std::make_unique<Variant>(static_cast<int16_t>(step.id)));
    case TypeKind::TINYINT:
      return ctx->registerVariant(
          std::make_unique<Variant>(static_cast<int8_t>(step.id)));
    default:
      VELOX_FAIL("Unsupported key type");
  }
}

ExprCP Optimization::makeGettersOverSkyline(
    const std::vector<Step>& steps,
    const SubfieldProjections* skyline,
    const lp::ExprPtr& base,
    ColumnCP column) {
  int32_t last = steps.size() - 1;
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
      // The path is not materialized. Need a longer path. to intersect skyline.
      return nullptr;
    }
  } else {
    if (column) {
      expr = column;
    } else {
      expr = translateExpr(base);
    }
    last = steps.size();
  }

  std::vector<Step> reverse;
  for (int32_t i = last - 1; i >= 0; --i) {
    // We make a getter over expr made so far with 'steps[i]' as first.
    PathExpr pathExpr = {steps[i], nullptr, expr};
    auto it = deduppedGetters_.find(pathExpr);
    if (it != deduppedGetters_.end()) {
      expr = it->second;
    } else {
      auto& step = steps[i];
      switch (step.kind) {
        case StepKind::kField: {
          if (!step.field) {
            auto* type = toType(expr->value().type->childAt(step.id));
            expr = make<Field>(type, expr, step.id);
            break;
          }
          auto* type = toType(expr->value().type->childAt(
              expr->value().type->as<TypeKind::ROW>().getChildIdx(step.field)));
          expr = make<Field>(type, expr, step.field);
          break;
        }
        case StepKind::kSubscript: {
          auto inputType = expr->value().type;
          const Type* type = toType(
              inputType->childAt(inputType->kind() == TypeKind::ARRAY ? 0 : 1));
          auto subscriptType = toType(
              inputType->kind() == TypeKind::ARRAY
                  ? INTEGER()
                  : inputType->as<TypeKind::MAP>().childAt(0));
          auto subscriptKind = subscriptType->kind();
          ExprVector args;
          args.push_back(expr);
          args.push_back(make<Literal>(
              Value(subscriptType, 1), subscriptLiteral(subscriptKind, step)));
          expr = make<Call>(
              toName("subscript"),
              Value(type, 1),
              std::move(args),
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
    const LogicalPlanSubfields& fields,
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

BitSet Optimization::functionSubfields(
    const lp::CallExpr* call,
    bool controlOnly,
    bool payloadOnly) {
  BitSet subfields;
  if (!controlOnly) {
    auto maybe = findSubfields(logicalPayloadSubfields_, call);
    if (maybe.has_value()) {
      subfields = maybe.value();
    }
  }
  if (!payloadOnly) {
    auto maybe = findSubfields(logicalControlSubfields_, call);
    if (maybe.has_value()) {
      subfields.unionSet(maybe.value());
    }
  }
  Path::subfieldSkyline(subfields);
  return subfields;
}

void Optimization::ensureFunctionSubfields(const lp::ExprPtr& expr) {
  if (const auto* call = expr->asUnchecked<lp::CallExpr>()) {
    auto metadata = functionMetadata(exec::sanitizeName(call->name()));
    if (!metadata) {
      return;
    }
    if (!logicalTranslatedSubfieldFuncs_.count(call)) {
      translateExpr(expr);
    }
  }
}

BuiltinNames::BuiltinNames()
    : eq(toName("eq")),
      lt(toName("lt")),
      lte(toName("lte")),
      gt(toName("gt")),
      gte(toName("gte")),
      plus(toName("plus")),
      multiply(toName("multiply")),
      _and(toName("and")),
      _or(toName("or")) {
  canonicalizable.insert(eq);
  canonicalizable.insert(lt);
  canonicalizable.insert(lte);
  canonicalizable.insert(gt);
  canonicalizable.insert(gte);
  canonicalizable.insert(plus);
  canonicalizable.insert(multiply);
  canonicalizable.insert(_and);
  canonicalizable.insert(_or);
}

Name BuiltinNames::reverse(Name name) const {
  if (name == lt) {
    return gt;
  }
  if (name == lte) {
    return gte;
  }
  if (name == gt) {
    return lt;
  }
  if (name == gte) {
    return lte;
  }
  return name;
}

BuiltinNames& Optimization::builtinNames() {
  if (!builtinNames_) {
    builtinNames_ = std::make_unique<BuiltinNames>();
  }
  return *builtinNames_;
}

void Optimization::canonicalizeCall(Name& name, ExprVector& args) {
  auto& names = builtinNames();
  if (!names.isCanonicalizable(name)) {
    return;
  }
  VELOX_CHECK_EQ(args.size(), 2, "Expecting binary op {}", name);
  if ((args[0]->type() == PlanType::kLiteral &&
       args[1]->type() != PlanType::kLiteral) ||
      args[0]->id() > args[1]->id()) {
    std::swap(args[0], args[1]);
    name = names.reverse(name);
  }
}

ExprCP Optimization::deduppedCall(
    Name name,
    Value value,
    ExprVector args,
    FunctionSet flags) {
  if (args.size() == 2) {
    canonicalizeCall(name, args);
  }
  ExprDedupKey key = {name, &args};
  auto it = functionDedup_.find(key);
  if (it != functionDedup_.end()) {
    return it->second;
  }
  auto* call =
      make<Call>(name, std::move(value), std::move(args), std::move(flags));
  if (!call->containsNonDeterministic()) {
    key.args = &call->args();
    functionDedup_[key] = call;
  }
  return call;
}

ExprCP Optimization::makeConstant(const lp::ConstantExpr& constant) {
  auto temp = constant.value();
  auto it = constantDedup_.find(temp);
  if (it != constantDedup_.end()) {
    return it->second;
  }
  auto* literal = make<Literal>(Value(toType(constant.type()), 1), temp.get());
  // The variant will stay live for the optimization duration.
  reverseConstantDedup_[literal] = temp;
  constantDedup_[std::move(temp)] = literal;
  return literal;
}

namespace {
const char* specialFormCallName(const lp::SpecialFormExpr* form) {
  switch (form->form()) {
    case lp::SpecialForm::kAnd:
      return "and";
    case lp::SpecialForm::kOr:
      return "or";
    case lp::SpecialForm::kCast:
      return "cast";
    case lp::SpecialForm::kTryCast:
      return "trycast";
    case lp::SpecialForm::kCoalesce:
      return "coalesce";
    case lp::SpecialForm::kIf:
      return "if";
    case lp::SpecialForm::kSwitch:
      return "switch";
    default:
      VELOX_UNREACHABLE(lp::SpecialFormName::toName(form->form()));
  }
}

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

ExprCP Optimization::translateExpr(const lp::ExprPtr& expr) {
  if (expr->isInputReference()) {
    return translateColumn(expr->asUnchecked<lp::InputReferenceExpr>()->name());
  }

  if (expr->isConstant()) {
    return makeConstant(*expr->asUnchecked<lp::ConstantExpr>());
  }
  auto path = translateSubfield(expr);
  if (path.has_value()) {
    return path.value();
  }

  const auto* call = expr->asUnchecked<lp::CallExpr>();
  std::string callName;
  if (call) {
    callName = exec::sanitizeName(call->name());
    auto* metadata = functionMetadata(callName);
    if (metadata && metadata->processSubfields()) {
      auto translated = translateSubfieldFunction(call, metadata);
      if (translated.has_value()) {
        return translated.value();
      }
    }
  }
  auto isCast = isSpecialForm(expr.get(), lp::SpecialForm::kCast);
  const lp::SpecialFormExpr* cast =
      isCast ? expr->asUnchecked<lp::SpecialFormExpr>() : nullptr;
  if (!isCast && !call) {
    if (expr->isLambda()) {
      return translateLambda(expr->asUnchecked<lp::LambdaExpr>());
    }
  }
  ExprVector args{expr->inputs().size()};
  PlanObjectSet columns;
  FunctionSet funcs;
  auto& inputs = expr->inputs();
  float cardinality = 1;
  bool allConstant = true;

  for (auto i = 0; i < inputs.size(); ++i) {
    args[i] = translateExpr(inputs[i]);
    allConstant &= args[i]->type() == PlanType::kLiteral;
    cardinality = std::max(cardinality, args[i]->value().cardinality);
    if (args[i]->type() == PlanType::kCall) {
      funcs = funcs | args[i]->as<Call>()->functions();
    }
  }

  if (allConstant && (call || cast)) {
    auto literal = tryFoldConstant(call, cast, args);
    if (literal) {
      return literal;
    }
  }
  if (call || expr->isSpecialForm()) {
    auto name = call
        ? toName(callName)
        : toName(specialFormCallName(expr->asUnchecked<lp::SpecialFormExpr>()));
    funcs = funcs | functionBits(name);
    auto* callExpr = deduppedCall(
        name, Value(toType(expr->type()), cardinality), std::move(args), funcs);
    return callExpr;
  }
  if (cast) {
    auto name = toName("cast");
    funcs = funcs | functionBits(name);

    auto* callExpr = deduppedCall(
        name, Value(toType(cast->type()), cardinality), std::move(args), funcs);
    return callExpr;
  }

  VELOX_NYI();
  return nullptr;
}

ExprCP Optimization::translateLambda(const lp::LambdaExpr* lambda) {
  auto savedRenames = renames_;
  auto row = lambda->signature();
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
  renames_ = savedRenames;
  return make<Lambda>(std::move(args), toType(lambda->type()), body);
}

std::optional<ExprCP> Optimization::translateSubfieldFunction(
    const lp::CallExpr* call,
    const FunctionMetadata* metadata) {
  logicalTranslatedSubfieldFuncs_.insert(call);
  auto subfields = functionSubfields(call, false, false);
  if (subfields.empty()) {
    // The function is accessed as a whole.
    return std::nullopt;
  }
  auto* ctx = queryCtx();
  std::vector<PathCP> paths;
  subfields.forEach([&](auto id) { paths.push_back(ctx->pathById(id)); });
  ExprVector args(call->inputs().size());
  BitSet usedArgs;
  bool allUsed = false;
  if (metadata->argOrdinal.empty()) {
    allUsed = true;
  } else {
    for (auto i = 0; i < paths.size(); ++i) {
      if (std::find(
              metadata->argOrdinal.begin(), metadata->argOrdinal.end(), i) ==
          metadata->argOrdinal.end()) {
        // This argument is not a source of subfields over some field
        // of the return value. Compute this in any case. accessed
        usedArgs.add(i);
        continue;
      }
      auto& step = paths[i]->steps()[0];
      auto maybeArg = stepToArg(step, metadata);
      if (maybeArg.has_value()) {
        usedArgs.add(maybeArg.value());
      }
    }
  }

  float cardinality = 1;
  FunctionSet funcs;
  for (auto i = 0; i < call->inputs().size(); ++i) {
    if (allUsed || usedArgs.contains(i)) {
      args[i] = translateExpr(call->inputs()[i]);
      cardinality = std::max(cardinality, args[i]->value().cardinality);
      if (args[i]->type() == PlanType::kCall) {
        funcs = funcs | args[i]->as<Call>()->functions();
      }
    } else {
      // make a null of the type for the unused arg to keep the tree valid.
      args[i] = make<Literal>(
          Value(toType(call->inputs()[i]->type()), 1),
          make<Variant>(Variant::null(call->inputs()[i]->type()->kind())));
    }
  }
  auto* name = toName(exec::sanitizeName(call->name()));
  funcs = funcs | functionBits(name);
  if (metadata->logicalExplode) {
    auto map = metadata->logicalExplode(call, paths);
    std::unordered_map<PathCP, ExprCP> translated;
    for (auto& pair : map) {
      translated[pair.first] = translateExpr(pair.second);
    }
    if (!translated.empty()) {
      logicalFunctionSubfields_[call] =
          SubfieldProjections{.pathToExpr = std::move(translated)};
      return nullptr;
    }
  }
  auto* callExpr =
      make<Call>(name, Value(toType(call->type()), cardinality), args, funcs);
  return callExpr;
}

ExprCP Optimization::translateColumn(const std::string& name) {
  auto it = renames_.find(name);
  if (it != renames_.end()) {
    return it->second;
  }
  VELOX_FAIL("Cannot resolve column name: {}", name);
}

ExprVector Optimization::translateColumns(
    const std::vector<lp::ExprPtr>& source) {
  ExprVector result{source.size()};
  for (auto i = 0; i < source.size(); ++i) {
    result[i] = translateExpr(source[i]); // NOLINT
  }
  return result;
}

AggregationP Optimization::translateAggregation(
    const lp::AggregateNode& logicalAgg) {
  auto* aggregation =
      make<Aggregation>(nullptr, translateColumns(logicalAgg.groupingKeys()));

  for (auto i = 0; i < logicalAgg.groupingKeys().size(); ++i) {
    auto name = toName(logicalAgg.outputType()->nameOf(i));
    auto* key = aggregation->grouping[i];

    if (key->type() == PlanType::kColumn) {
      aggregation->mutableColumns().push_back(key->as<Column>());
    } else {
      toType(logicalAgg.outputType()->childAt(i));

      auto* column = make<Column>(name, currentSelect_, key->value());
      aggregation->mutableColumns().push_back(column);
    }

    renames_[name] = aggregation->mutableColumns().back();
  }

  // The keys for intermediate are the same as for final.
  aggregation->intermediateColumns = aggregation->columns();
  for (auto channel : usedChannels(&logicalAgg)) {
    if (channel < logicalAgg.groupingKeys().size()) {
      continue;
    }

    const auto i = channel - logicalAgg.groupingKeys().size();
    const auto& aggregate = logicalAgg.aggregates()[i];
    ExprVector args = translateColumns(aggregate->inputs());

    FunctionSet funcs;
    std::vector<TypePtr> argTypes;
    for (auto& arg : args) {
      funcs = funcs | arg->functions();
      argTypes.push_back(toTypePtr(arg->value().type));
    }
    ExprCP condition = nullptr;
    if (aggregate->filter()) {
      condition = translateExpr(aggregate->filter());
    }
    VELOX_CHECK(aggregate->ordering().empty());

    Name aggName = toName(aggregate->name());
    auto accumulatorType = toType(
        exec::resolveAggregateFunction(aggregate->name(), argTypes).second);
    Value finalValue = Value(toType(aggregate->type()), 1);
    auto* agg = make<Aggregate>(
        aggName,
        finalValue,
        args,
        funcs,
        false,
        condition,
        false,
        accumulatorType);
    auto name = toName(logicalAgg.outputNames()[channel]);
    auto* column = make<Column>(name, currentSelect_, agg->value());
    aggregation->mutableColumns().push_back(column);
    auto intermediateValue = agg->value();
    intermediateValue.type = accumulatorType;
    auto* intermediateColumn =
        make<Column>(name, currentSelect_, intermediateValue);
    aggregation->intermediateColumns.push_back(intermediateColumn);
    auto dedupped = queryCtx()->dedup(agg);
    aggregation->aggregates.push_back(dedupped->as<Aggregate>());

    renames_[name] = aggregation->columns().back();
  }

  return aggregation;
}

PlanObjectP Optimization::addOrderBy(const lp::SortNode& order) {
  OrderTypeVector orderType;
  ExprVector keys;
  for (auto& field : order.ordering()) {
    auto sort = field.order;
    orderType.push_back(
        sort.isAscending() ? (sort.isNullsFirst() ? OrderType::kAscNullsFirst
                                                  : OrderType::kAscNullsLast)
                           : (sort.isNullsFirst() ? OrderType::kDescNullsFirst
                                                  : OrderType::kDescNullsLast));

    keys.push_back(translateExpr(field.expression));
  }

  currentSelect_->orderBy = make<OrderBy>(nullptr, keys, orderType);
  return currentSelect_;
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
    ExprVector& conjuncts,
    PlanObjectCP right,
    ExprVector& leftKeys,
    ExprVector& rightKeys,
    PlanObjectSet& allLeft) {
  const auto* eq = toName("eq");

  for (auto i = 0; i < conjuncts.size(); ++i) {
    const auto* conjunct = conjuncts[i];
    if (isCallExpr(conjunct, eq)) {
      const auto* eq = conjunct->as<Call>();
      const auto leftTables = eq->argAt(0)->allTables();
      const auto rightTables = eq->argAt(1)->allTables();
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

void Optimization::translateJoin(const lp::JoinNode& join) {
  const auto& joinLeft = join.left();
  const auto& joinRight = join.right();

  const auto joinType = join.joinType();
  const bool isInner = joinType == lp::JoinType::kInner;

  makeQueryGraph(*joinLeft, allow(PlanType::kJoin));

  // For an inner join a join tree on the right can be flattened, for all other
  // kinds it must be kept together in its own dt.
  makeQueryGraph(*joinRight, isInner ? allow(PlanType::kJoin) : 0);

  ExprVector conjuncts;
  translateConjuncts(join.condition(), conjuncts);

  if (isInner) {
    currentSelect_->conjuncts.insert(
        currentSelect_->conjuncts.end(), conjuncts.begin(), conjuncts.end());
  } else {
    const bool leftOptional =
        joinType == lp::JoinType::kRight || joinType == lp::JoinType::kFull;
    const bool rightOptional =
        joinType == lp::JoinType::kLeft || joinType == lp::JoinType::kFull;

    // If non-inner, and many tables on the right they are one dt. If a single
    // table then this too is the last in 'tables'.
    auto rightTable = currentSelect_->tables.back();

    ExprVector leftKeys;
    ExprVector rightKeys;
    PlanObjectSet leftTables;
    extractNonInnerJoinEqualities(
        conjuncts, rightTable, leftKeys, rightKeys, leftTables);

    std::vector<PlanObjectCP> leftTableVector;
    leftTableVector.reserve(leftTables.size());
    leftTables.forEach(
        [&](PlanObjectCP table) { leftTableVector.push_back(table); });

    auto* edge = make<JoinEdge>(
        leftTableVector.size() == 1 ? leftTableVector[0] : nullptr,
        rightTable,
        conjuncts,
        leftOptional,
        rightOptional,
        false,
        false);
    currentSelect_->joins.push_back(edge);
    for (auto i = 0; i < leftKeys.size(); ++i) {
      edge->addEquality(leftKeys[i], rightKeys[i]);
    }
  }
}

DerivedTableP Optimization::newDt() {
  auto* dt = make<DerivedTable>();
  dt->cname = newCName("dt");
  return dt;
}

PlanObjectP Optimization::wrapInDt(const lp::LogicalPlanNode& node) {
  DerivedTableP previousDt = currentSelect_;
  auto* dt = newDt();

  currentSelect_ = dt;
  makeQueryGraph(node, kAllAllowedInDt);

  currentSelect_ = previousDt;

  const auto& type = node.outputType();
  for (auto i : usedChannels(&node)) {
    const auto& name = type->nameOf(i);

    const auto* inner = translateColumn(name);
    dt->exprs.push_back(inner);

    const auto* outer = make<Column>(toName(name), dt, inner->value());
    dt->columns.push_back(outer);
    renames_[name] = outer;
  }

  currentSelect_->tables.push_back(dt);
  currentSelect_->tableSet.add(dt);
  dt->makeInitialPlan();

  return dt;
}

PlanObjectP Optimization::makeBaseTable(const lp::TableScanNode& tableScan) {
  const auto* schemaTable = schema_.findTable(tableScan.tableName());
  VELOX_CHECK_NOT_NULL(
      schemaTable, "Table not found: {}", tableScan.tableName());

  auto* baseTable = make<BaseTable>();
  baseTable->cname = newCName("t");
  baseTable->schemaTable = schemaTable;
  logicalPlanLeaves_[&tableScan] = baseTable;

  auto channels = usedChannels(&tableScan);
  const auto& type = tableScan.outputType();
  const auto& names = tableScan.columnNames();
  for (auto i : channels) {
    VELOX_DCHECK_LT(i, type->size());

    const auto& name = names[i];
    auto schemaColumn = schemaTable->findColumn(name);
    auto value = schemaColumn->value();
    auto* column =
        make<Column>(toName(name), baseTable, value, schemaColumn->name());
    baseTable->columns.push_back(column);

    const auto kind = column->value().type->kind();
    if (kind == TypeKind::ARRAY || kind == TypeKind::ROW ||
        kind == TypeKind::MAP) {
      BitSet allPaths;
      if (logicalControlSubfields_.hasColumn(&tableScan, i)) {
        baseTable->controlSubfields.ids.push_back(column->id());
        allPaths =
            logicalControlSubfields_.nodeFields[&tableScan].resultPaths[i];
        baseTable->controlSubfields.subfields.push_back(allPaths);
      }
      if (logicalPayloadSubfields_.hasColumn(&tableScan, i)) {
        baseTable->payloadSubfields.ids.push_back(column->id());
        auto payloadPaths =
            logicalPayloadSubfields_.nodeFields[&tableScan].resultPaths[i];
        baseTable->payloadSubfields.subfields.push_back(payloadPaths);
        allPaths.unionSet(payloadPaths);
      }
      if (opts_.pushdownSubfields) {
        Path::subfieldSkyline(allPaths);
        if (!allPaths.empty()) {
          makeSubfieldColumns(baseTable, column, allPaths);
        }
      }
    }

    renames_[type->nameOf(i)] = column;
  }

  ColumnVector top;
  std::unordered_map<ColumnCP, TypePtr> map;
  filterUpdated(baseTable, false);
  auto scanType =
      subfieldPushdownScanType(baseTable, baseTable->columns, top, map);
  setLeafSelectivity(*baseTable, scanType);
  currentSelect_->tables.push_back(baseTable);
  currentSelect_->tableSet.add(baseTable);
  return baseTable;
}

PlanObjectP Optimization::makeValuesTable(const lp::ValuesNode& values) {
  auto* valuesTable = make<ValuesTable>(values);
  valuesTable->cname = newCName("vt");
  logicalPlanLeaves_[&values] = valuesTable;

  auto channels = usedChannels(&values);
  const auto& type = values.outputType();
  const auto& names = values.outputType()->names();
  const auto cardinality = valuesTable->cardinality();
  for (auto i : channels) {
    VELOX_DCHECK_LT(i, type->size());

    const auto& name = names[i];
    Value value{type->childAt(i).get(), cardinality};
    auto* column = make<Column>(toName(name), valuesTable, value);
    valuesTable->columns.push_back(column);

    renames_[name] = column;
  }

  currentSelect_->tables.push_back(valuesTable);
  currentSelect_->tableSet.add(valuesTable);
  return valuesTable;
}

namespace {
const Type* pathType(const Type* type, PathCP path) {
  for (auto& step : path->steps()) {
    switch (step.kind) {
      case StepKind::kField:
        if (step.field) {
          type =
              type->childAt(type->as<TypeKind::ROW>().getChildIdx(step.field))
                  .get();
          break;
        }
        type = type->childAt(step.id).get();
        break;
      case StepKind::kSubscript:
        type = type->childAt(type->kind() == TypeKind::ARRAY ? 0 : 1).get();
        break;
      default:
        VELOX_NYI();
    }
  }
  return type;
}
} // namespace

void Optimization::makeSubfieldColumns(
    BaseTable* baseTable,
    ColumnCP column,
    const BitSet& paths) {
  SubfieldProjections projections;
  auto* ctx = queryCtx();
  float card =
      baseTable->schemaTable->columnGroups[0]->distribution().cardinality *
      baseTable->filterSelectivity;
  paths.forEach([&](auto id) {
    auto* path = ctx->pathById(id);
    auto type = pathType(column->value().type, path);
    Value value(type, card);
    auto name = fmt::format("{}.{}", column->name(), path->toString());
    auto* subcolumn =
        make<Column>(toName(name), baseTable, value, nullptr, column, path);
    baseTable->columns.push_back(subcolumn);
    projections.pathToExpr[path] = subcolumn;
  });
  allColumnSubfields_[column] = std::move(projections);
}

PlanObjectP Optimization::addProjection(const lp::ProjectNode* project) {
  logicalExprSource_ = project->onlyInput().get();
  const auto& names = project->names();
  const auto& exprs = project->expressions();
  for (auto i : usedChannels(project)) {
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

  return currentSelect_;
}

PlanObjectP Optimization::addFilter(const lp::FilterNode* filter) {
  logicalExprSource_ = filter->onlyInput().get();

  ExprVector flat;
  translateConjuncts(filter->predicate(), flat);
  if (logicalExprSource_->kind() == lp::NodeKind::kAggregate) {
    VELOX_CHECK(
        currentSelect_->having.empty(),
        "Must have all of HAVING in one filter");
    currentSelect_->having = flat;
  } else {
    currentSelect_->conjuncts.insert(
        currentSelect_->conjuncts.end(), flat.begin(), flat.end());
  }

  return currentSelect_;
}

PlanObjectP Optimization::addAggregation(const lp::AggregateNode& aggNode) {
  aggFinalType_ = aggNode.outputType();

  auto* aggPlan = make<AggregationPlan>(translateAggregation(aggNode));
  currentSelect_->aggregation = aggPlan;

  return currentSelect_;
}

PlanObjectP Optimization::addLimit(const lp::LimitNode& limitNode) {
  currentSelect_->limit = limitNode.count();
  currentSelect_->offset = limitNode.offset();
  return currentSelect_;
}

namespace {
bool hasNondeterministic(const lp::ExprPtr& expr) {
  if (const auto* call = expr->asUnchecked<lp::CallExpr>()) {
    if (functionBits(toName(call->name()))
            .contains(FunctionSet::kNonDeterministic)) {
      return true;
    }
  }
  for (auto& in : expr->inputs()) {
    if (hasNondeterministic(in)) {
      return true;
    }
  }
  return false;
}
} // namespace

DerivedTableP Optimization::translateSetJoin(
    const lp::SetNode& set,
    DerivedTableP setDt) {
  auto previousDt = currentSelect_;
  currentSelect_ = setDt;
  for (auto& input : set.inputs()) {
    wrapInDt(*input);
  }

  const bool exists = set.operation() == lp::SetOperation::kIntersect;
  const bool anti = set.operation() == lp::SetOperation::kExcept;

  const auto* left = setDt->tables[0]->as<DerivedTable>();

  for (auto i = 1; i < setDt->tables.size(); ++i) {
    const auto* right = setDt->tables[i]->as<DerivedTable>();

    auto* joinEdge =
        make<JoinEdge>(left, right, ExprVector{}, false, false, exists, anti);
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
    columns.push_back(
        make<Column>(toName(type->nameOf(i)), setDt, exprs.back()->value()));
    renames_[type->nameOf(i)] = columns.back();
  }

  auto agg = make<Aggregation>(nullptr, exprs);
  agg->mutableColumns() = columns;
  agg->intermediateColumns = columns;

  setDt->aggregation = make<AggregationPlan>(agg);
  for (auto& c : columns) {
    setDt->exprs.push_back(c);
  }
  setDt->columns = columns;
  setDt->makeInitialPlan();
  currentSelect_ = previousDt;
  return setDt;
}

void Optimization::makeUnionDistributionAndStats(
    DerivedTableP setDt,
    DerivedTableP innerDt) {
  if (setDt->distribution == nullptr) {
    DistributionType empty;
    setDt->distribution = make<Distribution>(empty, 0, ExprVector{});
  }
  if (innerDt == nullptr) {
    innerDt = setDt;
  }
  if (innerDt->children.empty()) {
    VELOX_CHECK_EQ(
        innerDt->columns.size(),
        setDt->columns.size(),
        "Union inputs must have same arity also after pruning");

    MemoKey key;
    key.firstTable = innerDt;
    key.tables.add(innerDt);
    for (auto& column : innerDt->columns) {
      key.columns.add(column);
    }

    auto it = memo_.find(key);
    VELOX_CHECK(it != memo_.end(), "Expecting to find a plan for union branch");

    bool ignore;
    Distribution emptyDistribution;
    auto plan = it->second.best(emptyDistribution, ignore)->op;
    setDt->distribution->cardinality += plan->distribution().cardinality;
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

DerivedTableP Optimization::translateUnion(
    const lp::SetNode& set,
    DerivedTableP setDt,
    bool isTopLevel,
    bool& isLeftLeaf) {
  auto initialRenames = renames_;
  std::vector<DerivedTableP, QGAllocator<DerivedTable*>> children;
  bool isFirst = true;
  DerivedTableP previousDt = currentSelect_;
  for (auto& input : set.inputs()) {
    if (!isFirst) {
      renames_ = initialRenames;
    } else {
      isFirst = false;
    }

    currentSelect_ = newDt();

    auto& newDt = currentSelect_;

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
        for (auto i : usedChannels(input.get())) {
          const auto& name = type->nameOf(i);

          ExprCP inner = translateColumn(name);
          newDt->exprs.push_back(inner);

          // The top dt has the same columns as all the unioned dts.
          auto* outer = make<Column>(toName(name), setDt, inner->value());
          setDt->columns.push_back(outer);
          newDt->columns.push_back(outer);
        }
        isLeftLeaf = false;
      } else {
        for (auto i : usedChannels(input.get())) {
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

  currentSelect_ = previousDt;
  if (isTopLevel) {
    setDt->children = std::move(children);
    setDt->setOp = set.operation();

    makeUnionDistributionAndStats(setDt);

    renames_ = initialRenames;
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

PlanObjectP Optimization::makeQueryGraph(
    const lp::LogicalPlanNode& node,
    uint64_t allowedInDt) {
  switch (node.kind()) {
    case lp::NodeKind::kValues:
      return makeValuesTable(*node.asUnchecked<lp::ValuesNode>());

    case lp::NodeKind::kTableScan:
      return makeBaseTable(*node.asUnchecked<lp::TableScanNode>());

    case lp::NodeKind::kFilter: {
      if (!contains(allowedInDt, PlanType::kFilter)) {
        return wrapInDt(node);
      }

      const auto* filter = node.asUnchecked<lp::FilterNode>();
      if (!isNondeterministicWrap_ &&
          hasNondeterministic(filter->predicate())) {
        // Force wrap the filter and its input inside a dt so the filter
        // does not get mixed with parrent nodes.
        isNondeterministicWrap_ = true;
        return makeQueryGraph(node, 0);
      }

      isNondeterministicWrap_ = false;
      makeQueryGraph(*node.onlyInput(), allowedInDt);
      return addFilter(filter);
    }

    case lp::NodeKind::kProject:
      makeQueryGraph(*node.onlyInput(), allowedInDt);
      return addProjection(node.asUnchecked<lp::ProjectNode>());

    case lp::NodeKind::kAggregate:
      if (!contains(allowedInDt, PlanType::kAggregation)) {
        return wrapInDt(node);
      }

      makeQueryGraph(
          *node.onlyInput(), makeDtIf(allowedInDt, PlanType::kAggregation));
      return addAggregation(*node.asUnchecked<lp::AggregateNode>());

    case lp::NodeKind::kJoin:
      if (!contains(allowedInDt, PlanType::kJoin)) {
        return wrapInDt(node);
      }

      translateJoin(*node.asUnchecked<lp::JoinNode>());
      return currentSelect_;

    case lp::NodeKind::kSort:
      if (!contains(allowedInDt, PlanType::kOrderBy)) {
        return wrapInDt(node);
      }

      makeQueryGraph(
          *node.onlyInput(), makeDtIf(allowedInDt, PlanType::kOrderBy));
      return addOrderBy(*node.asUnchecked<lp::SortNode>());

    case lp::NodeKind::kLimit: {
      // TODO Allow multiple limits.
      //
      // SELECT * FROM (SELECT * FROM t LIMIT 10 OFFSET 5) LIMIT 10 OFFSET 5
      // is equivalent to
      //    SELECT * FROM t LIMIT 5 OFFSET 10
      if (!contains(allowedInDt, PlanType::kLimit)) {
        return wrapInDt(node);
      }

      makeQueryGraph(
          *node.onlyInput(), makeDtIf(allowedInDt, PlanType::kLimit));
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
      currentSelect_->tables.push_back(setDt);
      currentSelect_->tableSet.add(setDt);
      return currentSelect_;
    }
    case lp::NodeKind::kUnnest:
    default:
      VELOX_NYI(
          "Unsupported PlanNode {}",
          logical_plan::NodeKindName::toName(node.kind()));
  }
}

// Debug helper functions. Must be in a namespace to be callable from gdb.
std::string leString(const lp::Expr* e) {
  return lp::ExprPrinter::toText(*e);
}

std::string pString(const lp::LogicalPlanNode* p) {
  return lp::PlanPrinter::toText(*p);
}

} // namespace facebook::velox::optimizer
