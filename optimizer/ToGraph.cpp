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

std::string veloxToString(const core::PlanNode* plan) {
  return plan->toString(true, true);
}

void Optimization::setDerivedTableOutput(
    DerivedTableP dt,
    const velox::core::PlanNode& planNode) {
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

DerivedTableP Optimization::makeQueryGraph() {
  markAllSubfields(inputPlan_.outputType().get(), &inputPlan_);
  auto* root = make<DerivedTable>();
  root_ = root;
  currentSelect_ = root_;
  root->cname = toName(fmt::format("dt{}", ++nameCounter_));
  makeQueryGraph(inputPlan_, kAllAllowedInDt);
  return root_;
}

const std::string* columnName(const core::TypedExprPtr& expr) {
  if (auto column =
          dynamic_cast<const core::FieldAccessTypedExpr*>(expr.get())) {
    if (column->inputs().empty() ||
        dynamic_cast<const core::InputTypedExpr*>(column->inputs()[0].get())) {
      return &column->name();
    }
  }
  return nullptr;
}

bool isCall(const core::TypedExprPtr& expr, const std::string& name) {
  if (auto call = std::dynamic_pointer_cast<const core::CallTypedExpr>(expr)) {
    return exec::sanitizeName(call->name()) == name;
  }
  return false;
}

void Optimization::translateConjuncts(
    const core::TypedExprPtr& input,
    ExprVector& flat) {
  if (!input) {
    return;
  }
  if (isCall(input, "and")) {
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
    const core::CallTypedExpr* call,
    const core::CastTypedExpr* cast,
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
      core::ConstantTypedExprPtr typed;
      auto kind = constantExpr->type()->kind();
      switch (kind) {
        case TypeKind::ARRAY:
        case TypeKind::ROW:
        case TypeKind::MAP:
          typed =
              std::make_shared<core::ConstantTypedExpr>(constantExpr->value());
          break;
        default: {
          auto* variantLiteral = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
              toVariant,
              constantExpr->value()->typeKind(),
              *constantExpr->value());
          typed = std::make_shared<core::ConstantTypedExpr>(
              constantExpr->value()->type(), *variantLiteral);
          break;
        }
      }
      return makeConstant(typed);
    }
    return nullptr;
  } catch (const std::exception&) {
    return nullptr;
  }
}

bool Optimization::isSubfield(
    const core::ITypedExpr* expr,
    Step& step,
    core::TypedExprPtr& input) {
  if (auto* field = dynamic_cast<const core::FieldAccessTypedExpr*>(expr)) {
    input = field->inputs().empty() ? nullptr : field->inputs()[0];
    if (!input || dynamic_cast<const core::InputTypedExpr*>(input.get())) {
      return false;
    }
    step.kind = StepKind::kField;
    step.field = toName(field->name());
    return true;
  }
  if (auto deref = dynamic_cast<const core::DereferenceTypedExpr*>(expr)) {
    step = {.kind = StepKind::kField, .id = deref->index()};
    input = deref->inputs()[0];
    auto& type = input->type();
    VELOX_CHECK_EQ(type->kind(), TypeKind::ROW);
    auto& name = type->as<TypeKind::ROW>().nameOf(step.id);
    // There can be field index-only field accesses over functions
    // that hav row values without field names. These are not suitable
    // for subfield pruning in columns though, so fill in the name if
    // there is one.
    if (!name.empty()) {
      step.field = toName(name);
    }
    return true;
  }
  if (auto* call = dynamic_cast<const core::CallTypedExpr*>(expr)) {
    auto name = call->name();
    if (name == "subscript" || name == "element_at") {
      auto subscript = translateExpr(call->inputs()[1]);
      if (subscript->type() == PlanType::kLiteral) {
        step.kind = StepKind::kSubscript;
        auto& literal = subscript->as<Literal>()->literal();
        switch (subscript->value().type->kind()) {
          case TypeKind::VARCHAR:
            step.field = toName(literal.value<TypeKind::VARCHAR>());
            break;
          case TypeKind::BIGINT:
            step.id = literal.value<TypeKind::BIGINT>();
            break;
          case TypeKind::INTEGER:
            step.id = literal.value<TypeKind::INTEGER>();
            break;
          case TypeKind::SMALLINT:
            step.id = literal.value<TypeKind::SMALLINT>();
            break;
          case TypeKind::TINYINT:
            step.id = literal.value<TypeKind::TINYINT>();
            break;
          default:
            VELOX_UNREACHABLE();
        }
        input = expr->inputs()[0];
        return true;
      }
      return false;
    }
    if (name == "cardinality") {
      step.kind = StepKind::kCardinality;
      input = expr->inputs()[0];
      return true;
    }
  }
  return false;
}

void Optimization::getExprForField(
    const core::FieldAccessTypedExpr* field,
    core::TypedExprPtr& resultExpr,
    ColumnCP& resultColumn,
    const core::PlanNode*& context) {
  for (;;) {
    auto& name = field->name();
    auto row = context->outputType();
    auto ordinal = row->getChildIdx(name);
    if (auto* project = dynamic_cast<const core::ProjectNode*>(context)) {
      auto& def = project->projections()[ordinal];
      if (auto* innerField =
              dynamic_cast<const core::FieldAccessTypedExpr*>(def.get())) {
        context = context->sources()[0].get();
        field = innerField;
        continue;
      }
      resultExpr = def;
      context = project->sources()[0].get();
      return;
    }
    auto& sources = context->sources();
    if (sources.empty()) {
      auto leaf = findLeaf(context);
      auto internedName = toName(name);
      resultExpr = nullptr;
      if (auto* table = dynamic_cast<BaseTableCP>(leaf)) {
        for (auto i = 0; i < table->columns.size(); ++i) {
          if (table->columns[i]->name() == internedName) {
            resultColumn = table->columns[i];
            break;
          }
        }
        context = nullptr;
        return;
      } else {
        VELOX_NYI("Leaf node is not a table");
      }
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

bool isLeafField(const core::ITypedExpr* expr) {
  if (auto* field = dynamic_cast<const core::FieldAccessTypedExpr*>(expr)) {
    if (field->inputs().empty() ||
        dynamic_cast<const core::InputTypedExpr*>(field->inputs()[0].get())) {
      return true;
    }
  }
  return false;
}

std::optional<ExprCP> Optimization::translateSubfield(
    const core::TypedExprPtr& inputExpr) {
  std::vector<Step> steps;
  auto* source = exprSource_;
  auto expr = inputExpr;
  for (;;) {
    core::TypedExprPtr input;
    Step step;
    VELOX_CHECK_NOT_NULL(expr);
    bool isStep = isSubfield(expr.get(), step, input);
    if (!isStep) {
      if (steps.empty()) {
        return std::nullopt;
      }
      // if this is a field we follow to the expr assigning the field if any.
      Step ignore;
      core::TypedExprPtr ignore2;
      if (!isSubfield(expr.get(), ignore, ignore2)) {
        ColumnCP column = nullptr;
        if (isLeafField(expr.get())) {
          getExprForField(
              reinterpret_cast<const core::FieldAccessTypedExpr*>(expr.get()),
              expr,
              column,
              source);
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
          auto it = functionSubfields_.find(expr.get());
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

PathCP innerPath(const std::vector<Step>& steps, int32_t last) {
  std::vector<Step> reverse;
  for (int32_t i = steps.size() - 1; i >= last; --i) {
    reverse.push_back(steps[i]);
  }
  return toPath(std::move(reverse));
}

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
    const core::TypedExprPtr& base,
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
    const PlanSubfields& fields,
    const core::CallTypedExpr* call) {
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
    const core::CallTypedExpr* call,
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

void Optimization::ensureFunctionSubfields(const core::TypedExprPtr& expr) {
  if (auto* call = dynamic_cast<const core::CallTypedExpr*>(expr.get())) {
    auto metadata = FunctionRegistry::instance()->metadata(
        exec::sanitizeName(call->name()));
    if (!metadata) {
      return;
    }
    if (!translatedSubfieldFuncs_.count(call)) {
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

ExprCP Optimization::makeConstant(const core::ConstantTypedExprPtr& constant) {
  auto it = exprDedup_.find(constant.get());
  if (it != exprDedup_.end()) {
    return it->second;
  }

  Literal* literal;
  if (constant->hasValueVector()) {
    auto dedupped = queryCtx()->toVector(constant->valueVector());
    literal = make<Literal>(Value(toType(constant->type()), 1), dedupped);
  } else {
    literal = make<Literal>(
        Value(toType(constant->type()), 1),
        queryCtx()->registerVariant(
            std::make_unique<variant>(constant->value())));
  }
  // Keep the key live for the optimization duration.
  tempExprs_.push_back(constant);
  exprDedup_[constant.get()] = literal;
  return literal;
}

ExprCP Optimization::translateExpr(const core::TypedExprPtr& expr) {
  if (auto name = columnName(expr)) {
    return translateColumn(*name);
  }
  if (auto constant =
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(expr)) {
    return makeConstant(constant);
  }
  auto path = translateSubfield(expr);
  if (path.has_value()) {
    return path.value();
  }
  auto call = dynamic_cast<const core::CallTypedExpr*>(expr.get());
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
  auto cast = dynamic_cast<const core::CastTypedExpr*>(expr.get());
  if (!cast && !call) {
    if (auto* lambda = dynamic_cast<const core::LambdaTypedExpr*>(expr.get())) {
      return translateLambda(lambda);
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
  if (call) {
    auto name = toName(callName);
    funcs = funcs | functionBits(name);
    auto* callExpr = deduppedCall(
        name, Value(toType(call->type()), cardinality), std::move(args), funcs);
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

ExprCP Optimization::translateLambda(const core::LambdaTypedExpr* lambda) {
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
    const core::CallTypedExpr* call,
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
  if (metadata->explode) {
    auto map = metadata->explode(call, paths);
    std::unordered_map<PathCP, ExprCP> translated;
    for (auto& pair : map) {
      translated[pair.first] = translateExpr(pair.second);
    }
    if (!translated.empty()) {
      functionSubfields_[call] =
          SubfieldProjections{.pathToExpr = std::move(translated)};
      return nullptr;
    }
  }
  auto* callExpr =
      make<Call>(name, Value(toType(call->type()), cardinality), args, funcs);
  exprDedup_[call] = callExpr;
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
    const std::vector<core::FieldAccessTypedExprPtr>& source) {
  ExprVector result{source.size()};
  for (auto i = 0; i < source.size(); ++i) {
    result[i] = translateColumn(source[i]->name()); // NOLINT
  }
  return result;
}

TypePtr intermediateType(const core::CallTypedExprPtr& call) {
  std::vector<TypePtr> types;
  for (auto& arg : call->inputs()) {
    types.push_back(arg->type());
  }
  return exec::Aggregate::intermediateType(
      exec::sanitizeName(call->name()), types);
}

TypePtr finalType(const core::CallTypedExprPtr& call) {
  std::vector<TypePtr> types;
  for (auto& arg : call->inputs()) {
    types.push_back(arg->type());
  }
  return exec::Aggregate::finalType(exec::sanitizeName(call->name()), types);
}

AggregationP Optimization::translateAggregation(
    const core::AggregationNode& source) {
  using velox::core::AggregationNode;
  if (source.step() == AggregationNode::Step::kPartial ||
      source.step() == AggregationNode::Step::kSingle) {
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

        auto* column = make<Column>(
            name, currentSelect_, aggregation->grouping[i]->value());
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
      auto rawFunc = translateExpr(source.aggregates()[i].call)->as<Call>();
      ExprCP condition = nullptr;
      if (source.aggregates()[i].mask) {
        condition = translateExpr(source.aggregates()[i].mask);
      }
      VELOX_CHECK(source.aggregates()[i].sortingKeys.empty());
      // rawFunc is either a single or a partial aggregation. We need
      // both final and intermediate types. The type of rawFunc itself
      // is one or the other so resolve the types using the registered
      // signatures.
      auto accumulatorType =
          toType(intermediateType(source.aggregates()[i].call));
      Value finalValue = rawFunc->value();
      finalValue.type = toType(finalType(source.aggregates()[i].call));
      auto* agg = make<Aggregate>(
          rawFunc->name(),
          finalValue,
          rawFunc->args(),
          rawFunc->functions(),
          false,
          condition,
          false,
          accumulatorType);
      auto name = toName(source.aggregateNames()[i]);
      auto* column = make<Column>(name, currentSelect_, agg->value());
      aggregation->mutableColumns().push_back(column);
      auto intermediateValue = agg->value();
      intermediateValue.type = accumulatorType;
      auto* intermediateColumn =
          make<Column>(name, currentSelect_, intermediateValue);
      aggregation->intermediateColumns.push_back(intermediateColumn);
      auto dedupped = queryCtx()->dedup(agg);
      aggregation->aggregates.push_back(dedupped->as<Aggregate>());
      auto resultName = toName(source.aggregateNames()[i]);
      renames_[resultName] = aggregation->columns().back();
    }
    for (auto& pair : keyRenames) {
      renames_[pair.first] = pair.second;
    }
    return aggregation;
  }
  return nullptr;
}

OrderByP Optimization::translateOrderBy(const core::OrderByNode& order) {
  OrderTypeVector orderType;
  for (auto& sort : order.sortingOrders()) {
    orderType.push_back(
        sort.isAscending() ? (sort.isNullsFirst() ? OrderType::kAscNullsFirst
                                                  : OrderType::kAscNullsLast)
                           : (sort.isNullsFirst() ? OrderType::kDescNullsFirst
                                                  : OrderType::kDescNullsLast));
  }
  auto keys = translateColumns(order.sortingKeys());
  auto* orderBy = QGC_MAKE_IN_ARENA(OrderBy)(nullptr, keys, orderType, {});
  return orderBy;
}

ColumnCP Optimization::makeMark(const core::AbstractJoinNode& join) {
  auto type = join.outputType();
  auto name = toName(type->nameOf(type->size() - 1));
  Value value(toType(type->childAt(type->size() - 1)), 2);
  auto* column = make<Column>(name, currentSelect_, value);
  return column;
}

void Optimization::translateJoin(const core::AbstractJoinNode& join) {
  bool isInner = join.isInnerJoin();
  auto joinLeft = join.sources()[0];
  auto joinLeftKeys = join.leftKeys();
  auto joinRight = join.sources()[1];
  auto joinRightKeys = join.rightKeys();
  auto joinType = join.joinType();
  // Normalize right exists to left exists swapping the sides.
  if (joinType == core::JoinType::kRightSemiFilter ||
      joinType == core::JoinType::kRightSemiProject) {
    std::swap(joinLeft, joinRight);
    std::swap(joinLeftKeys, joinRightKeys);
    joinType = joinType == core::JoinType::kRightSemiFilter
        ? core::JoinType::kLeftSemiFilter
        : core::JoinType::kLeftSemiProject;
  }
  makeQueryGraph(*joinLeft, allow(PlanType::kJoin));
  auto leftKeys = translateColumns(joinLeftKeys);
  // For an inner join a join tree on the right can be flattened, for all other
  // kinds it must be kept together in its own dt.
  makeQueryGraph(*joinRight, isInner ? allow(PlanType::kJoin) : 0);
  auto rightKeys = translateColumns(joinRightKeys);
  ExprVector conjuncts;
  translateConjuncts(join.filter(), conjuncts);
  if (isInner) {
    // Every column to column equality adds to an equivalence class and is an
    // independent bidirectional join edge.
    for (auto i = 0; i < leftKeys.size(); ++i) {
      auto l = leftKeys[i];
      auto r = rightKeys.at(i);
      if (l->type() == PlanType::kColumn && r->type() == PlanType::kColumn) {
        l->as<Column>()->equals(r->as<Column>());
        currentSelect_->addJoinEquality(l, r, {}, false, false, false, false);
      } else {
        currentSelect_->addJoinEquality(l, r, {}, false, false, false, false);
      }
    }
    currentSelect_->conjuncts.insert(
        currentSelect_->conjuncts.end(), conjuncts.begin(), conjuncts.end());
  } else {
    bool leftOptional =
        joinType == core::JoinType::kRight || joinType == core::JoinType::kFull;
    bool rightOptional =
        joinType == core::JoinType::kLeft || joinType == core::JoinType::kFull;
    bool rightExists = joinType == core::JoinType::kLeftSemiFilter;
    bool rightNotExists = joinType == core::JoinType::kAnti;
    ColumnCP markColumn =
        joinType == core::JoinType::kLeftSemiProject ? makeMark(join) : nullptr;
    ;

    PlanObjectSet leftTables;
    PlanObjectCP rightTable = nullptr;

    for (auto i = 0; i < leftKeys.size(); ++i) {
      auto l = leftKeys[i];
      leftTables.unionSet(l->allTables());
      auto r = rightKeys.at(i);
      auto rightKeyTable = r->singleTable();
      if (rightTable) {
        VELOX_CHECK(rightKeyTable == rightTable);
      } else {
        rightTable = rightKeyTable;
      }
    }
    VELOX_CHECK(rightTable, "No right side in join");
    std::vector<PlanObjectCP> leftTableVector;
    leftTables.forEach(
        [&](PlanObjectCP table) { leftTableVector.push_back(table); });
    auto* edge = make<JoinEdge>(
        leftTableVector.size() == 1 ? leftTableVector[0] : nullptr,
        rightTable,
        conjuncts,
        leftOptional,
        rightOptional,
        rightExists,
        rightNotExists,
        markColumn);
    if (markColumn) {
      renames_[markColumn->name()] = markColumn;
    }
    currentSelect_->joins.push_back(edge);
    for (auto i = 0; i < leftKeys.size(); ++i) {
      edge->addEquality(leftKeys[i], rightKeys[i]);
    }
  }
}

void Optimization::translateNonEqualityJoin(
    const core::NestedLoopJoinNode& join) {
  auto joinType = join.joinType();
  bool isInner = joinType == core::JoinType::kInner;
  makeQueryGraph(*join.sources()[0], allow(PlanType::kJoin));
  // For an inner join a join tree on the right can be flattened, for all other
  // kinds it must be kept together in its own dt.
  makeQueryGraph(*join.sources()[1], isInner ? allow(PlanType::kJoin) : 0);
  ExprVector conjuncts;
  translateConjuncts(join.joinCondition(), conjuncts);
  if (conjuncts.empty()) {
    // Inner cross product. Join conditions may be added from
    // conjuncts of the enclosing DerivedTable.
    return;
  }
  PlanObjectSet tables;
  for (auto& conjunct : conjuncts) {
    tables.unionColumns(conjunct);
  }
  std::vector<PlanObjectCP> tableVector;
  tables.forEach([&](PlanObjectCP table) { tableVector.push_back(table); });
  if (tableVector.size() == 2) {
    auto* edge = make<JoinEdge>(
        tableVector[0], tableVector[1], conjuncts, false, false, false, false);
    edge->guessFanout();
    currentSelect_->joins.push_back(edge);

  } else {
    VELOX_NYI("Multiway non-equality join not supported");
    currentSelect_->conjuncts.insert(
        currentSelect_->conjuncts.end(), conjuncts.begin(), conjuncts.end());
  }
}

bool isJoin(const core::PlanNode& node) {
  auto name = node.name();
  if (name == "HashJoin" || name == "MergeJoin" || name == "NestedLoopJoin") {
    return true;
  }
  if (name == "Project" || name == "Filter") {
    return isJoin(*node.sources()[0]);
  }
  return false;
}

bool isDirectOver(const core::PlanNode& node, const std::string& name) {
  auto source = node.sources()[0];
  if (source && source->name() == name) {
    return true;
  }
  return false;
}

PlanObjectP Optimization::wrapInDt(const core::PlanNode& node) {
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

PlanObjectP Optimization::makeBaseTable(const core::TableScanNode* tableScan) {
  auto tableHandle = tableScan->tableHandle().get();
  auto assignments = tableScan->assignments();
  auto schemaTable = schema_.findTable(tableHandle->name());
  auto cname = fmt::format("t{}", ++nameCounter_);

  auto* baseTable = make<BaseTable>();
  baseTable->cname = toName(cname);
  baseTable->schemaTable = schemaTable;
  planLeaves_[tableScan] = baseTable;
  auto channels = usedChannels(tableScan);

  for (auto& pair : assignments) {
    auto idx = tableScan->outputType()->getChildIdx(pair.second->name());
    if (std::find(channels.begin(), channels.end(), idx) == channels.end()) {
      continue;
    }
    auto schemaColumn = schemaTable->findColumn(pair.second->name());
    auto value = schemaColumn->value();
    auto* column = make<Column>(toName(pair.second->name()), baseTable, value);
    baseTable->columns.push_back(column);
    auto kind = column->value().type->kind();
    if (kind == TypeKind::ARRAY || kind == TypeKind::ROW ||
        kind == TypeKind::MAP) {
      BitSet allPaths;
      if (controlSubfields_.hasColumn(tableScan, idx)) {
        baseTable->controlSubfields.ids.push_back(column->id());
        allPaths = controlSubfields_.nodeFields[tableScan].resultPaths[idx];
        baseTable->controlSubfields.subfields.push_back(allPaths);
      }
      if (payloadSubfields_.hasColumn(tableScan, idx)) {
        baseTable->payloadSubfields.ids.push_back(column->id());
        auto payloadPaths =
            payloadSubfields_.nodeFields[tableScan].resultPaths[idx];
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
    renames_[pair.first] = column;
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
        make<Column>(toName(name), baseTable, value, column, path);
    baseTable->columns.push_back(subcolumn);
    projections.pathToExpr[path] = subcolumn;
  });
  allColumnSubfields_[column] = std::move(projections);
}

void Optimization::addProjection(const core::ProjectNode* project) {
  exprSource_ = project->sources()[0].get();
  auto names = project->names();
  auto exprs = project->projections();
  for (auto i : usedChannels(project)) {
    if (auto field = dynamic_cast<const core::FieldAccessTypedExpr*>(
            exprs.at(i).get())) {
      // A variable projected to itself adds no renames. Inputs contain this
      // all the time.
      if (field->name() == names[i]) {
        continue;
      }
    }
    auto expr = translateExpr(exprs.at(i));
    renames_[names[i]] = expr;
  }
}

void Optimization::addFilter(const core::FilterNode* filter) {
  exprSource_ = filter->sources()[0].get();
  ExprVector flat;
  translateConjuncts(filter->filter(), flat);
  if (isDirectOver(*filter, "Aggregation")) {
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
    const core::AggregationNode& aggNode,
    uint64_t allowedInDt) {
  using AggregationNode = velox::core::AggregationNode;
  if (aggNode.step() == AggregationNode::Step::kPartial ||
      aggNode.step() == AggregationNode::Step::kSingle) {
    if (!contains(allowedInDt, PlanType::kAggregation)) {
      return wrapInDt(aggNode);
    }
    if (aggNode.step() == AggregationNode::Step::kSingle) {
      aggFinalType_ = aggNode.outputType();
    }
    makeQueryGraph(
        *aggNode.sources()[0], makeDtIf(allowedInDt, PlanType::kAggregation));
    auto agg = translateAggregation(aggNode);
    if (agg) {
      auto* aggPlan = make<AggregationPlan>(agg);
      currentSelect_->aggregation = aggPlan;
    }
  } else {
    if (aggNode.step() == AggregationNode::Step::kFinal) {
      aggFinalType_ = aggNode.outputType();
    }
    makeQueryGraph(*aggNode.sources()[0], allowedInDt);
  }
  return currentSelect_;
}

bool hasNondeterministic(const core::TypedExprPtr& expr) {
  if (auto* call = dynamic_cast<const core::CallTypedExpr*>(expr.get())) {
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
    const core::PlanNode& node,
    uint64_t allowedInDt) {
  auto name = node.name();
  if (name == "Filter" && !contains(allowedInDt, PlanType::kFilter)) {
    return wrapInDt(node);
  }

  if (isJoin(node) && !contains(allowedInDt, PlanType::kJoin)) {
    return wrapInDt(node);
  }
  if (name == "TableScan") {
    return makeBaseTable(reinterpret_cast<const core::TableScanNode*>(&node));
  }
  if (name == "Project") {
    makeQueryGraph(*node.sources()[0], allowedInDt);
    addProjection(reinterpret_cast<const core::ProjectNode*>(&node));
    return currentSelect_;
  }
  if (name == "Filter") {
    auto filter = reinterpret_cast<const core::FilterNode*>(&node);
    if (!isNondeterministicWrap_ && hasNondeterministic(filter->filter())) {
      // Force wrap the filter and its input inside a dt so the filter
      // does not get mixed with parrent nodes.
      isNondeterministicWrap_ = true;
      return makeQueryGraph(node, 0);
    }
    isNondeterministicWrap_ = false;
    makeQueryGraph(*node.sources()[0], allowedInDt);
    addFilter(filter);
    return currentSelect_;
  }
  if (name == "HashJoin" || name == "MergeJoin") {
    if (!contains(allowedInDt, PlanType::kJoin)) {
      return wrapInDt(node);
    }
    translateJoin(*reinterpret_cast<const core::AbstractJoinNode*>(&node));
    return currentSelect_;
  }
  if (name == "NestedLoopJoin") {
    if (!contains(allowedInDt, PlanType::kJoin)) {
      return wrapInDt(node);
    }
    translateNonEqualityJoin(
        *reinterpret_cast<const core::NestedLoopJoinNode*>(&node));
    return currentSelect_;
  }
  if (name == "LocalPartition") {
    makeQueryGraph(*node.sources()[0], allowedInDt);
    return currentSelect_;
  }
  if (name == "Aggregation") {
    return addAggregation(
        *reinterpret_cast<const core::AggregationNode*>(&node), allowedInDt);
  }
  if (name == "OrderBy") {
    if (!contains(allowedInDt, PlanType::kOrderBy)) {
      return wrapInDt(node);
    }
    makeQueryGraph(
        *node.sources()[0], makeDtIf(allowedInDt, PlanType::kOrderBy));
    currentSelect_->orderBy =
        translateOrderBy(*reinterpret_cast<const core::OrderByNode*>(&node));
    return currentSelect_;
  }
  if (name == "Limit") {
    if (!contains(allowedInDt, PlanType::kLimit)) {
      return wrapInDt(node);
    }
    makeQueryGraph(*node.sources()[0], makeDtIf(allowedInDt, PlanType::kLimit));
    auto limit = reinterpret_cast<const core::LimitNode*>(&node);
    currentSelect_->limit = limit->count();
    currentSelect_->offset = limit->offset();
  } else {
    VELOX_NYI("Unsupported PlanNode {}", name);
  }
  return currentSelect_;
}

} // namespace facebook::velox::optimizer
