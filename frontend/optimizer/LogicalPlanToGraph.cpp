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

#include "optimizer/FunctionRegistry.h" //@manual
#include "optimizer/Plan.h" //@manual
#include "optimizer/PlanUtils.h" //@manual
#include "velox/exec/Aggregate.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/SignatureBinder.h"

namespace facebook::velox::optimizer {

using namespace facebook::velox;

namespace lp = facebook::velox::logical_plan;

void Optimization::setDerivedTableOutput(
    DerivedTableP dt,
    const velox::logical_plan::LogicalPlanNode& planNode) {
  auto& outputType = planNode.outputType();
  for (auto i = 0; i < outputType->size(); ++i) {
    auto fieldType = outputType->childAt(i);
    auto fieldName = outputType->nameOf(i);
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
  auto* root = make<DerivedTable>();
  root_ = root;
  currentSelect_ = root_;
  root->cname = toName(fmt::format("dt{}", ++nameCounter_));
  makeQueryGraph(*logicalPlan_, kAllAllowedInDt);
  return root_;
}

const std::string* columnName(const lp::Expr& expr) {
  if (expr.isInputReference()) {
    return &expr.asUnchecked<lp::InputReferenceExpr>()->name();
  }
  return nullptr;
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

template <TypeKind kind>
const variant* toVariant(BaseVector& constantVector) {
  using T = typename TypeTraits<kind>::NativeType;
  if (auto typed = dynamic_cast<ConstantVector<T>*>(&constantVector)) {
    return queryCtx()->registerVariant(
        std::make_unique<variant>(typed->valueAt(0)));
  }
  VELOX_FAIL("Literal not of foldable type");
}

std::shared_ptr<const exec::ConstantExpr> Optimization::foldConstant(
    const core::TypedExprPtr& typedExpr) {
  auto exprSet = evaluator_.compile(typedExpr);
  auto first = exprSet->exprs().front().get();
  if (dynamic_cast<const exec::ConstantExpr*>(first)) {
    return std::dynamic_pointer_cast<exec::ConstantExpr>(
        exprSet->exprs().front());
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
      lp::ConstantExprPtr typed;
      auto kind = constantExpr->type()->kind();
      switch (kind) {
        case TypeKind::ARRAY:
        case TypeKind::ROW:
        case TypeKind::MAP:
          VELOX_NYI("Need complex type to variant conversion");
          break;
        default: {
          auto* variantLiteral = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
              toVariant,
              constantExpr->value()->typeKind(),
              *constantExpr->value());
          typed = std::make_shared<lp::ConstantExpr>(
              constantExpr->type(), *variantLiteral);
          break;
        }
      }
      return makeConstant(*typed);
    }
    return nullptr;
  } catch (const std::exception&) {
    return nullptr;
  }
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
      name = toName(field.value<TypeKind::VARCHAR>());
      id = rowType.getChildIdx(name);
    }
    step.field = name;
    step.id = id;
    input = expr->inputAt(0);
    return true;
  }
  if (auto* call = dynamic_cast<const lp::CallExpr*>(expr)) {
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
    auto row = context->outputType();
    auto ordinal = row->getChildIdx(name);
    if (auto* project = dynamic_cast<const lp::ProjectNode*>(context)) {
      auto& def = project->expressions()[ordinal];
      if (auto* innerField =
              dynamic_cast<const lp::InputReferenceExpr*>(def.get())) {
        context = context->inputAt(0).get();
        field = innerField;
        continue;
      }
      resultExpr = def;
      context = project->inputAt(0).get();
      return;
    }
    auto& sources = context->inputs();
    if (sources.empty()) {
      auto leaf = findLeaf(context);
      auto it = renames_.find(name);
      VELOX_CHECK(it != renames_.end());
      auto maybeColumn = it->second;
      VELOX_CHECK(maybeColumn->type() == PlanType::kColumn);
      resultColumn = maybeColumn->as<Column>();
      resultExpr = nullptr;
      context = nullptr;
#if 0
      auto internedName = toName(name);
      if (auto* table = dynamic_cast<BaseTableCP>(leaf)) {
	for (auto i = 0; i < table->columns.size(); ++i) {
          if (table->columns[i]->name() == internedName) {
            resultColumn = table->columns[i];
            break;
          }
        }
    } else {
        VELOX_NYI("Leaf node is not a table");
      }
#else
      VELOX_CHECK_NOT_NULL(resultColumn->relation());
      if (resultColumn->relation()->type() == PlanType::kTable) {
        VELOX_CHECK(leaf == resultColumn->relation());
      }
#endif
      return;
    }
    for (auto i = 0; i < sources.size(); ++i) {
      auto& row = sources[i]->outputType();
      auto maybe = row->getChildIdxIfExists(name);
      if (maybe.has_value()) {
        context = sources[i].get();
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

variant* subscriptLiteral(TypeKind kind, const Step& step) {
  auto* ctx = queryCtx();
  switch (kind) {
    case TypeKind::VARCHAR:
      return ctx->registerVariant(
          std::make_unique<variant>(std::string(step.field)));
    case TypeKind::BIGINT:
      return ctx->registerVariant(
          std::make_unique<variant>(static_cast<int64_t>(step.id)));
    case TypeKind::INTEGER:
      return ctx->registerVariant(
          std::make_unique<variant>(static_cast<int32_t>(step.id)));
    case TypeKind::SMALLINT:
      return ctx->registerVariant(
          std::make_unique<variant>(static_cast<int16_t>(step.id)));
    case TypeKind::TINYINT:
      return ctx->registerVariant(
          std::make_unique<variant>(static_cast<int8_t>(step.id)));
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
  if (auto* call = dynamic_cast<const lp::CallExpr*>(expr.get())) {
    auto metadata = FunctionRegistry::instance()->metadata(
        exec::sanitizeName(call->name()));
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
  if (!call->containsFunction(FunctionSet::kNondeterministic)) {
    key.args = &call->args();
    functionDedup_[key] = call;
  }
  return call;
}

ExprCP Optimization::makeConstant(const lp::ConstantExpr& constant) {
  auto temp = constant.valueShared();
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
      VELOX_UNREACHABLE(
          "Bad special form {}", static_cast<int32_t>(form->form()));
  }
}

ExprCP Optimization::translateExpr(const lp::ExprPtr& expr) {
  if (auto name = columnName(*expr)) {
    return translateColumn(*name);
  }
  if (expr->isConstant()) {
    return makeConstant(*expr->asUnchecked<lp::ConstantExpr>());
  }
  auto path = translateSubfield(expr);
  if (path.has_value()) {
    return path.value();
  }
  auto call = dynamic_cast<const lp::CallExpr*>(expr.get());
  std::string callName;
  if (call) {
    callName = exec::sanitizeName(call->name());
    auto* metadata = FunctionRegistry::instance()->metadata(callName);
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
          make<variant>(variant::null(call->inputs()[i]->type()->kind())));
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
  auto column = renames_.find(name);
  if (column != renames_.end()) {
    return column->second;
  }
  VELOX_FAIL("could not resolve name {}", name);
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
    const lp::AggregateNode& source) {
  auto* aggregation =
      make<Aggregation>(nullptr, translateColumns(source.groupingKeys()));
  std::unordered_map<std::string, ExprCP> keyRenames;

  for (auto i = 0; i < source.groupingKeys().size(); ++i) {
    if (aggregation->grouping[i]->type() == PlanType::kColumn) {
      aggregation->mutableColumns().push_back(
          aggregation->grouping[i]->as<Column>());
    } else {
      auto name = toName(source.outputType()->nameOf(i));
      toType(source.outputType()->childAt(i));

      auto* column =
          make<Column>(name, currentSelect_, aggregation->grouping[i]->value());
      aggregation->mutableColumns().push_back(column);
      keyRenames[name] = column;
    }
  }
  // The keys for intermediate are the same as for final.
  aggregation->intermediateColumns = aggregation->columns();
  for (auto channel : usedChannels(&source)) {
    if (channel < source.groupingKeys().size()) {
      continue;
    }
    auto i = channel - source.groupingKeys().size();
    auto aggregate = source.aggregates()[i];
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

    auto accumulatorType =
        toType(exec::Aggregate::intermediateType(aggregate->name(), argTypes));
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
    auto name = toName(source.outputNames()[channel]);
    auto* column = make<Column>(name, currentSelect_, agg->value());
    aggregation->mutableColumns().push_back(column);
    auto intermediateValue = agg->value();
    intermediateValue.type = accumulatorType;
    auto* intermediateColumn =
        make<Column>(name, currentSelect_, intermediateValue);
    aggregation->intermediateColumns.push_back(intermediateColumn);
    auto dedupped = queryCtx()->dedup(agg);
    aggregation->aggregates.push_back(dedupped->as<Aggregate>());
    auto resultName = toName(source.outputNames()[i]);
    renames_[resultName] = aggregation->columns().back();
  }
  for (auto& pair : keyRenames) {
    renames_[pair.first] = pair.second;
  }
  return aggregation;
}

OrderByP Optimization::translateOrderBy(const lp::SortNode& order) {
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
  auto* orderBy = QGC_MAKE_IN_ARENA(OrderBy)(nullptr, keys, orderType, {});
  return orderBy;
}

void Optimization::translateJoin(const lp::JoinNode& join) {
  auto joinLeft = join.left();
  auto joinRight = join.right();

  auto joinType = join.joinType();
  bool isInner = joinType == lp::JoinType::kInner;
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
    bool leftOptional =
        joinType == lp::JoinType::kRight || joinType == lp::JoinType::kFull;
    bool rightOptional =
        joinType == lp::JoinType::kLeft || joinType == lp::JoinType::kFull;
    ExprVector leftKeys;
    ExprVector rightKeys;
    PlanObjectSet leftTables;
    // If non-inner, and many tables on the right they are one dt. If a single
    // table then this too is the last in 'tables'.
    auto rightTable = currentSelect_->tables.back();
    extractNonInnerJoinEqualities(
        conjuncts, rightTable, leftKeys, rightKeys, leftTables);
    std::vector<PlanObjectCP> leftTableVector;
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

bool isJoin(const lp::LogicalPlanNode& node) {
  auto kind = node.kind();
  if (kind == lp::NodeKind::kJoin) {
    return true;
  }
  if (kind == lp::NodeKind::kFilter || kind == lp::NodeKind::kProject) {
    return isJoin(*node.inputAt(0));
  }
  return false;
}

bool isDirectOver(const lp::LogicalPlanNode& node, lp::NodeKind kind) {
  auto source = node.inputAt(0);
  return source && source->kind() == kind;
}

PlanObjectP Optimization::wrapInDt(const lp::LogicalPlanNode& node) {
  DerivedTableP previousDt = currentSelect_;
  auto* newDt = make<DerivedTable>();
  auto cname = toName(fmt::format("dt{}", ++nameCounter_));
  newDt->cname = cname;
  currentSelect_ = newDt;
  makeQueryGraph(node, kAllAllowedInDt);

  currentSelect_ = previousDt;
  velox::RowTypePtr type = node.outputType();
  // node.name() == "Aggregation" ? aggFinalType_ : node.outputType();
  for (auto i : usedChannels(&node)) {
    ExprCP inner = translateColumn(type->nameOf(i));
    newDt->exprs.push_back(inner);
    auto* outer = make<Column>(toName(type->nameOf(i)), newDt, inner->value());
    newDt->columns.push_back(outer);
    renames_[type->nameOf(i)] = outer;
  }
  currentSelect_->tables.push_back(newDt);
  currentSelect_->tableSet.add(newDt);
  newDt->makeInitialPlan();

  return newDt;
}

PlanObjectP Optimization::makeBaseTable(const lp::TableScanNode* tableScan) {
  auto schemaTable = schema_.findTable(tableScan->tableName());
  auto cname = fmt::format("t{}", ++nameCounter_);

  auto* baseTable = make<BaseTable>();
  baseTable->cname = toName(cname);
  baseTable->schemaTable = schemaTable;
  logicalPlanLeaves_[tableScan] = baseTable;
  auto channels = usedChannels(tableScan);
  auto type = tableScan->outputType();
  auto& names = tableScan->columnNames();
  for (auto i = 0; i < type->size(); ++i) {
    if (std::find(channels.begin(), channels.end(), i) == channels.end()) {
      continue;
    }
    auto schemaColumn = schemaTable->findColumn(names[i]);
    auto value = schemaColumn->value();
    auto* column =
        make<Column>(toName(names[i]), baseTable, value, schemaColumn->name());
    baseTable->columns.push_back(column);
    auto kind = column->value().type->kind();
    if (kind == TypeKind::ARRAY || kind == TypeKind::ROW ||
        kind == TypeKind::MAP) {
      BitSet allPaths;
      if (logicalControlSubfields_.hasColumn(tableScan, i)) {
        baseTable->controlSubfields.ids.push_back(column->id());
        allPaths =
            logicalControlSubfields_.nodeFields[tableScan].resultPaths[i];
        baseTable->controlSubfields.subfields.push_back(allPaths);
      }
      if (logicalPayloadSubfields_.hasColumn(tableScan, i)) {
        baseTable->payloadSubfields.ids.push_back(column->id());
        auto payloadPaths =
            logicalPayloadSubfields_.nodeFields[tableScan].resultPaths[i];
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

void Optimization::addProjection(const lp::ProjectNode* project) {
  logicalExprSource_ = project->inputAt(0).get();
  auto names = project->names();
  auto exprs = project->expressions();
  for (auto i : usedChannels(project)) {
    if (exprs[i]->isInputReference()) {
      auto name = exprs[i]->asUnchecked<lp::InputReferenceExpr>()->name();
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

void Optimization::addFilter(const lp::FilterNode* filter) {
  logicalExprSource_ = filter->inputAt(0).get();
  ExprVector flat;
  translateConjuncts(filter->predicate(), flat);
  if (isDirectOver(*filter, lp::NodeKind::kAggregate)) {
    VELOX_CHECK(
        currentSelect_->having.empty(),
        "Must have aall of HAVING in one filter");
    currentSelect_->having = flat;
  } else {
    currentSelect_->conjuncts.insert(
        currentSelect_->conjuncts.end(), flat.begin(), flat.end());
  }
}

PlanObjectP Optimization::addAggregation(
    const lp::AggregateNode& aggNode,
    uint64_t allowedInDt) {
  using AggregateNode = lp::AggregateNode;
  if (!contains(allowedInDt, PlanType::kAggregation)) {
    return wrapInDt(aggNode);
  }
  aggFinalType_ = aggNode.outputType();
  makeQueryGraph(
      *aggNode.inputAt(0), makeDtIf(allowedInDt, PlanType::kAggregation));
  auto agg = translateAggregation(aggNode);
  if (agg) {
    auto* aggPlan = make<AggregationPlan>(agg);
    currentSelect_->aggregation = aggPlan;
  }
  return currentSelect_;
}

bool hasNondeterministic(const lp::ExprPtr& expr) {
  if (auto* call = dynamic_cast<const lp::CallExpr*>(expr.get())) {
    if (functionBits(toName(call->name()))
            .contains(FunctionSet::kNondeterministic)) {
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

PlanObjectP Optimization::makeQueryGraph(
    const lp::LogicalPlanNode& node,
    uint64_t allowedInDt) {
  auto kind = node.kind();
  if (kind == lp::NodeKind::kFilter &&
      !contains(allowedInDt, PlanType::kFilter)) {
    return wrapInDt(node);
  }

  if (kind == lp::NodeKind::kJoin && !contains(allowedInDt, PlanType::kJoin)) {
    return wrapInDt(node);
  }
  if (kind == lp::NodeKind::kTableScan) {
    return makeBaseTable(reinterpret_cast<const lp::TableScanNode*>(&node));
  }
  if (kind == lp::NodeKind::kProject) {
    makeQueryGraph(*node.inputAt(0), allowedInDt);
    addProjection(reinterpret_cast<const lp::ProjectNode*>(&node));
    return currentSelect_;
  }
  if (kind == lp::NodeKind::kFilter) {
    auto filter = reinterpret_cast<const lp::FilterNode*>(&node);
    if (!isNondeterministicWrap_ && hasNondeterministic(filter->predicate())) {
      // Force wrap the filter and its input inside a dt so the filter
      // does not get mixed with parrent nodes.
      isNondeterministicWrap_ = true;
      return makeQueryGraph(node, 0);
    }
    isNondeterministicWrap_ = false;
    makeQueryGraph(*node.inputAt(0), allowedInDt);
    addFilter(filter);
    return currentSelect_;
  }
  if (kind == lp::NodeKind::kJoin) {
    if (!contains(allowedInDt, PlanType::kJoin)) {
      return wrapInDt(node);
    }
    translateJoin(*reinterpret_cast<const lp::JoinNode*>(&node));
    return currentSelect_;
  }
  if (kind == lp::NodeKind::kAggregate) {
    return addAggregation(
        *reinterpret_cast<const lp::AggregateNode*>(&node), allowedInDt);
  }
  if (kind == lp::NodeKind::kSort) {
    if (!contains(allowedInDt, PlanType::kOrderBy)) {
      return wrapInDt(node);
    }
    makeQueryGraph(*node.inputAt(0), makeDtIf(allowedInDt, PlanType::kOrderBy));
    currentSelect_->orderBy =
        translateOrderBy(*reinterpret_cast<const lp::SortNode*>(&node));
    return currentSelect_;
  }
  if (kind == lp::NodeKind::kLimit) {
    if (!contains(allowedInDt, PlanType::kLimit)) {
      return wrapInDt(node);
    }
    makeQueryGraph(*node.inputAt(0), makeDtIf(allowedInDt, PlanType::kLimit));
    auto limit = reinterpret_cast<const lp::LimitNode*>(&node);
    currentSelect_->limit = limit->count();
    currentSelect_->offset = limit->offset();
  } else {
    VELOX_NYI("Unsupported PlanNode {}", static_cast<int32_t>(kind));
  }
  return currentSelect_;
}

std::string leString(const lp::Expr* e) {
  return lp::ExprPrinter::toText(*e);
}

std::string pString(const lp::LogicalPlanNode* p) {
  return lp::PlanPrinter::toText(*p);
}

} // namespace facebook::velox::optimizer
