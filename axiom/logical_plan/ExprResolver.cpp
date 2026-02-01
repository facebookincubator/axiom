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
#include "axiom/logical_plan/ExprResolver.h"
#include "axiom/logical_plan/ExprApi.h"
#include "axiom/logical_plan/LogicalPlanNode.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregateFunctionRegistry.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/Expr.h"
#include "velox/expression/ExprConstants.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/vector/VariantToVector.h"

namespace facebook::axiom::logical_plan {

namespace {

std::string toString(
    const std::string& functionName,
    const std::vector<velox::TypePtr>& argTypes) {
  std::ostringstream signature;
  signature << functionName << "(";
  for (auto i = 0; i < argTypes.size(); i++) {
    if (i > 0) {
      signature << ", ";
    }
    signature << argTypes[i]->toString();
  }
  signature << ")";
  return signature.str();
}

std::string toString(
    const std::vector<const velox::exec::FunctionSignature*>& signatures) {
  std::stringstream out;
  for (auto i = 0; i < signatures.size(); ++i) {
    if (i > 0) {
      out << ", ";
    }
    out << signatures[i]->toString();
  }
  return out.str();
}

void applyCoercions(
    std::vector<ExprPtr>& inputs,
    const std::vector<velox::TypePtr>& coercions) {
  if (coercions.empty()) {
    return;
  }

  for (auto i = 0; i < inputs.size(); ++i) {
    if (const auto& coersion = coercions.at(i)) {
      inputs[i] = std::make_shared<SpecialFormExpr>(
          coersion, SpecialForm::kCast, inputs[i]);
    }
  }
}

[[noreturn]] void throwCannotResolveScalarFunction(
    const std::string& name,
    const std::vector<velox::TypePtr>& argTypes) {
  auto allSignatures = velox::getFunctionSignatures();
  auto it = allSignatures.find(name);
  if (it == allSignatures.end()) {
    VELOX_USER_FAIL("Scalar function doesn't exist: {}.", name);
  } else {
    const auto& functionSignatures = it->second;
    VELOX_USER_FAIL(
        "Scalar function signature is not supported: {}. Supported signatures: {}.",
        toString(name, argTypes),
        toString(functionSignatures));
  }
}

velox::TypePtr resolveScalarFunction(
    const std::string& name,
    const std::vector<velox::TypePtr>& argTypes,
    bool allowCoercions,
    std::vector<velox::TypePtr>& coercions) {
  if (allowCoercions) {
    if (auto type = velox::resolveFunctionOrCallableSpecialFormWithCoercions(
            name, argTypes, coercions)) {
      return type;
    }
  } else {
    if (auto type =
            velox::resolveFunctionOrCallableSpecialForm(name, argTypes)) {
      return type;
    }
  }

  throwCannotResolveScalarFunction(name, argTypes);
}

std::vector<velox::TypePtr> toTypes(const std::vector<ExprPtr>& exprs) {
  std::vector<velox::TypePtr> types;
  types.reserve(exprs.size());
  for (auto& expr : exprs) {
    types.push_back(expr->type());
  }

  return types;
}

// Resolves a scalar function with the given name and arguments. Applies
// coercions if 'allowCoercions' is true.
//
// If 'name' is a lambda function and 'allowCoercions' is true, the lambda may
// need to be resolved again after applying coercions to non-lambda arguments.
// If that's the case, this function applies coercion to non-lambda arguments
// and returns nullptr. The caller is expected to resolve lambda arguments again
// using modified non-lambda arguments.
velox::TypePtr resolveScalarFunction(
    const std::string& name,
    std::vector<ExprPtr>& args,
    bool allowCoercions) {
  const auto argTypes = toTypes(args);

  std::vector<velox::TypePtr> coercions;
  auto returnType =
      resolveScalarFunction(name, argTypes, allowCoercions, coercions);

  bool hasFunctionCoercion = false;
  bool hasNonFunctionCoercion = false;
  if (!coercions.empty()) {
    for (auto i = 0; i < args.size(); ++i) {
      if (coercions[i] != nullptr) {
        if (args[i]->type()->isFunction()) {
          hasFunctionCoercion = true;
          coercions[i] = nullptr;
        } else {
          hasNonFunctionCoercion = true;
        }
      }
    }
  }

  if (hasFunctionCoercion && !hasNonFunctionCoercion) {
    throwCannotResolveScalarFunction(name, argTypes);
  }

  applyCoercions(args, coercions);

  if (hasFunctionCoercion) {
    return nullptr;
  }

  return returnType;
}

ExprPtr resolveSpecialFormWithCoercions(
    SpecialForm form,
    const std::string& name,
    std::vector<ExprPtr>& inputs) {
  std::vector<velox::TypePtr> coercions;
  auto returnType =
      resolveCallableSpecialFormWithCoercions(name, toTypes(inputs), coercions);
  VELOX_CHECK_NOT_NULL(returnType);

  applyCoercions(inputs, coercions);
  return std::make_shared<SpecialFormExpr>(returnType, form, inputs);
}

ExprPtr applyCoercion(
    const ExprPtr& input,
    const velox::TypePtr& type,
    const std::shared_ptr<velox::core::PlanNodeIdGenerator>&
        planNodeIdGenerator) {
  if (input->isSpecialForm() &&
      input->as<SpecialFormExpr>()->form() == SpecialForm::kCast) {
    return std::make_shared<SpecialFormExpr>(
        type, SpecialForm::kCast, input->inputAt(0));
  }

  if (input->isSubquery()) {
    const auto* subqueryExpr = input->as<SubqueryExpr>();
    const auto& subquery = subqueryExpr->subquery();

    VELOX_CHECK_EQ(subquery->outputType()->size(), 1);

    return std::make_shared<SubqueryExpr>(std::make_shared<ProjectNode>(
        planNodeIdGenerator->next(),
        subquery,
        std::vector<std::string>{subquery->outputType()->nameOf(0)},
        std::vector<ExprPtr>{applyCoercion(
            std::make_shared<InputReferenceExpr>(
                subquery->outputType()->childAt(0),
                subquery->outputType()->nameOf(0)),
            type,
            planNodeIdGenerator)}));
  }

  return std::make_shared<SpecialFormExpr>(type, SpecialForm::kCast, input);
}

namespace {

int64_t toIntegerValue(const ConstantExpr& expr) {
  switch (expr.type()->kind()) {
    case velox::TypeKind::BIGINT:
      return expr.value()->value<int64_t>();
    case velox::TypeKind::INTEGER:
      return expr.value()->value<int32_t>();
    case velox::TypeKind::SMALLINT:
      return expr.value()->value<int16_t>();
    case velox::TypeKind::TINYINT:
      return expr.value()->value<int8_t>();
    default:
      VELOX_USER_FAIL(
          "Expected an integer value: {} - {}",
          expr.type()->toString(),
          expr.toString());
  }
}
} // namespace

ExprPtr tryResolveSpecialForm(
    const std::string& name,
    std::vector<ExprPtr>& resolvedInputs,
    bool allowCoercions,
    const std::shared_ptr<velox::core::PlanNodeIdGenerator>&
        planNodeIdGenerator) {
  if (name == "and") {
    return std::make_shared<SpecialFormExpr>(
        velox::BOOLEAN(), SpecialForm::kAnd, resolvedInputs);
  }

  if (name == "or") {
    return std::make_shared<SpecialFormExpr>(
        velox::BOOLEAN(), SpecialForm::kOr, resolvedInputs);
  }

  if (name == "try") {
    return std::make_shared<SpecialFormExpr>(
        resolvedInputs.at(0)->type(), SpecialForm::kTry, resolvedInputs);
  }

  if (name == "coalesce") {
    if (allowCoercions) {
      return resolveSpecialFormWithCoercions(
          SpecialForm::kCoalesce, velox::expression::kCoalesce, resolvedInputs);
    }

    return std::make_shared<SpecialFormExpr>(
        resolvedInputs.at(0)->type(), SpecialForm::kCoalesce, resolvedInputs);
  }

  if (name == "if") {
    if (resolvedInputs.size() == 2) {
      // Add null ELSE clause.
      const auto& type = resolvedInputs.back()->type();
      resolvedInputs.emplace_back(
          std::make_shared<ConstantExpr>(
              type, std::make_shared<velox::Variant>(type->kind())));
    }

    if (allowCoercions) {
      return resolveSpecialFormWithCoercions(
          SpecialForm::kIf, velox::expression::kIf, resolvedInputs);
    }

    return std::make_shared<SpecialFormExpr>(
        resolvedInputs.at(1)->type(), SpecialForm::kIf, resolvedInputs);
  }

  if (name == "switch") {
    if (allowCoercions) {
      return resolveSpecialFormWithCoercions(
          SpecialForm::kSwitch, velox::expression::kSwitch, resolvedInputs);
    }
    return std::make_shared<SpecialFormExpr>(
        resolvedInputs.at(1)->type(), SpecialForm::kSwitch, resolvedInputs);
  }

  if (name == "subscript" && resolvedInputs.at(0)->type()->isRow()) {
    VELOX_USER_CHECK_EQ(2, resolvedInputs.size());

    const auto& rowType = resolvedInputs.at(0)->type()->asRow();

    const auto& fieldExpr = resolvedInputs.at(1);
    VELOX_USER_CHECK(fieldExpr->isConstant());

    const auto index = toIntegerValue(*fieldExpr->as<ConstantExpr>());
    VELOX_USER_CHECK_GE(index, 1, "Subscript index must be 1-based");
    VELOX_USER_CHECK_LE(index, rowType.size(), "Subscript index out of range");

    const auto zeroBasedIndex = static_cast<int32_t>(index - 1);

    std::vector<ExprPtr> newInputs = {
        resolvedInputs.at(0),
        std::make_shared<ConstantExpr>(
            velox::INTEGER(),
            std::make_shared<velox::Variant>(zeroBasedIndex))};

    return std::make_shared<SpecialFormExpr>(
        rowType.childAt(zeroBasedIndex), SpecialForm::kDereference, newInputs);
  }

  if (name == "in") {
    if (allowCoercions) {
      VELOX_USER_CHECK_GE(
          resolvedInputs.size(), 2, "IN must have at least two inputs");

      auto type = resolvedInputs.at(0)->type();
      for (auto i = 1; i < resolvedInputs.size(); ++i) {
        const auto& newType = resolvedInputs.at(i)->type();
        if (type->equivalent(*newType)) {
          continue;
        }

        if (velox::TypeCoercer::coercible(newType, type)) {
          resolvedInputs[i] =
              applyCoercion(resolvedInputs[i], type, planNodeIdGenerator);
          continue;
        }

        if (velox::TypeCoercer::coercible(type, newType)) {
          type = newType;

          for (auto j = 0; j < i; ++j) {
            resolvedInputs[j] =
                applyCoercion(resolvedInputs[j], type, planNodeIdGenerator);
          }
        }
      }
    }

    return std::make_shared<SpecialFormExpr>(
        velox::BOOLEAN(), SpecialForm::kIn, resolvedInputs);
  }

  if (name == "exists") {
    return std::make_shared<SpecialFormExpr>(
        velox::BOOLEAN(), SpecialForm::kExists, resolvedInputs);
  }

  return nullptr;
}
} // namespace

ExprPtr ExprResolver::resolveLambdaExpr(
    const velox::core::LambdaExpr& lambdaExpr,
    const std::vector<velox::TypePtr>& lambdaInputTypes,
    const InputNameResolver& inputNameResolver) const {
  const auto& names = lambdaExpr.arguments();
  const auto& body = lambdaExpr.body();

  VELOX_CHECK_LE(names.size(), lambdaInputTypes.size());
  std::vector<velox::TypePtr> types;
  types.reserve(names.size());
  for (auto i = 0; i < names.size(); ++i) {
    types.push_back(lambdaInputTypes[i]);
  }

  auto signature =
      ROW(std::vector<std::string>(names), std::vector<velox::TypePtr>(types));
  auto lambdaResolver = [inputNameResolver, signature](
                            const std::optional<std::string>& alias,
                            const std::string& fieldName) -> ExprPtr {
    if (!alias.has_value()) {
      auto maybeIdx = signature->getChildIdxIfExists(fieldName);
      if (maybeIdx.has_value()) {
        return std::make_shared<InputReferenceExpr>(
            signature->childAt(maybeIdx.value()), fieldName);
      }
    }
    return inputNameResolver(alias, fieldName);
  };

  return std::make_shared<LambdaExpr>(
      signature, resolveScalarTypes(body, lambdaResolver));
}

namespace {
bool isLambdaArgument(const velox::exec::TypeSignature& typeSignature) {
  return typeSignature.baseName() == "function";
}

bool hasLambdaArgument(const velox::exec::FunctionSignature& signature) {
  return std::ranges::any_of(signature.argumentTypes(), isLambdaArgument);
}

bool isLambdaArgument(
    const velox::exec::TypeSignature& typeSignature,
    size_t numInputs) {
  return isLambdaArgument(typeSignature) &&
      typeSignature.parameters().size() == numInputs + 1;
}

bool isLambdaSignature(
    const velox::exec::FunctionSignature* signature,
    const std::shared_ptr<const velox::core::CallExpr>& callExpr) {
  if (!hasLambdaArgument(*signature)) {
    return false;
  }

  const auto numArguments = callExpr->inputs().size();

  if (numArguments != signature->argumentTypes().size()) {
    return false;
  }

  bool match = true;
  for (size_t i = 0; i < numArguments; ++i) {
    if (auto lambda = dynamic_cast<const velox::core::LambdaExpr*>(
            callExpr->inputAt(i).get())) {
      const auto numLambdaInputs = lambda->arguments().size();
      const auto& argumentType = signature->argumentTypes()[i];
      if (!isLambdaArgument(argumentType, numLambdaInputs)) {
        match = false;
        break;
      }
    }
  }

  return match;
}

const velox::exec::FunctionSignature* FOLLY_NULLABLE findLambdaSignature(
    const std::vector<std::shared_ptr<velox::exec::AggregateFunctionSignature>>&
        signatures,
    const std::shared_ptr<const velox::core::CallExpr>& callExpr) {
  const velox::exec::FunctionSignature* matchingSignature = nullptr;
  for (const auto& signature : signatures) {
    if (isLambdaSignature(signature.get(), callExpr)) {
      VELOX_CHECK_NULL(
          matchingSignature,
          "Cannot resolve ambiguous lambda function signatures for {}.",
          callExpr->name());
      matchingSignature = signature.get();
    }
  }

  return matchingSignature;
}

const velox::exec::FunctionSignature* FOLLY_NULLABLE findLambdaSignature(
    const std::vector<const velox::exec::FunctionSignature*>& signatures,
    const std::shared_ptr<const velox::core::CallExpr>& callExpr) {
  const velox::exec::FunctionSignature* matchingSignature = nullptr;
  for (const auto& signature : signatures) {
    if (isLambdaSignature(signature, callExpr)) {
      VELOX_CHECK_NULL(
          matchingSignature,
          "Cannot resolve ambiguous lambda function signatures for {}.",
          callExpr->name());
      matchingSignature = signature;
    }
  }

  return matchingSignature;
}

const velox::exec::FunctionSignature* findLambdaSignature(
    const std::shared_ptr<const velox::core::CallExpr>& callExpr) {
  const auto name = velox::exec::sanitizeName(callExpr->name());

  // Look for a scalar lambda function.
  auto scalarSignatures = velox::getFunctionSignatures(name);
  if (!scalarSignatures.empty()) {
    return findLambdaSignature(scalarSignatures, callExpr);
  }

  // Look for an aggregate lambda function.
  if (auto signatures = velox::exec::getAggregateFunctionSignatures(name)) {
    return findLambdaSignature(signatures.value(), callExpr);
  }

  return nullptr;
}
} // namespace

bool ExprResolver::resolveLambdaArguments(
    const std::vector<velox::core::ExprPtr>& inputs,
    const velox::exec::FunctionSignature& signature,
    std::vector<ExprPtr>& resolvedInputs,
    const InputNameResolver& inputNameResolver) const {
  const auto numArgs = resolvedInputs.size();
  VELOX_CHECK_EQ(numArgs, inputs.size());

  std::vector<velox::TypePtr> argTypes(numArgs);
  for (auto i = 0; i < numArgs; ++i) {
    if (resolvedInputs[i] != nullptr) {
      argTypes[i] = resolvedInputs[i]->type();
    }
  }

  velox::exec::SignatureBinder binder(signature, argTypes);
  binder.tryBind();
  for (auto i = 0; i < numArgs; ++i) {
    auto argSignature = signature.argumentTypes()[i];
    if (isLambdaArgument(argSignature)) {
      std::vector<velox::TypePtr> lambdaTypes;
      for (auto j = 0; j < argSignature.parameters().size() - 1; ++j) {
        auto type = binder.tryResolveType(argSignature.parameters()[j]);
        if (type == nullptr) {
          return false;
        }
        lambdaTypes.push_back(type);
      }

      resolvedInputs[i] = resolveLambdaExpr(
          dynamic_cast<const velox::core::LambdaExpr&>(*inputs[i]),
          lambdaTypes,
          inputNameResolver);
    }
  }

  return true;
}

ExprPtr ExprResolver::tryResolveCallWithLambdas(
    const std::shared_ptr<const velox::core::CallExpr>& callExpr,
    const InputNameResolver& inputNameResolver) const {
  if (callExpr == nullptr) {
    return nullptr;
  }

  if (!std::ranges::any_of(callExpr->inputs(), [](const auto& input) {
        return input->is(velox::core::IExpr::Kind::kLambda);
      })) {
    // Not lambda inputs.
    return nullptr;
  }

  auto signature = findLambdaSignature(callExpr);
  VELOX_CHECK_NOT_NULL(
      signature, "Cannot resolve lambda function: {}", callExpr->toString());

  // Resolve non-lambda arguments first.
  const auto numArgs = callExpr->inputs().size();
  std::vector<ExprPtr> children(numArgs);
  for (auto i = 0; i < numArgs; ++i) {
    if (!isLambdaArgument(signature->argumentTypes()[i])) {
      children[i] = resolveScalarTypes(callExpr->inputAt(i), inputNameResolver);
    }
  }

  // Resolve lambda arguments.
  if (!resolveLambdaArguments(
          callExpr->inputs(), *signature, children, inputNameResolver)) {
    return nullptr;
  }

  const auto name = velox::exec::sanitizeName(callExpr->name());

  auto returnType = resolveScalarFunction(name, children, enableCoercions_);
  if (returnType == nullptr) {
    VELOX_CHECK(enableCoercions_);
    if (!resolveLambdaArguments(
            callExpr->inputs(), *signature, children, inputNameResolver)) {
      return nullptr;
    }

    returnType =
        resolveScalarFunction(name, children, /*allowCoercions=*/false);
    VELOX_CHECK_NOT_NULL(returnType);
  }

  return std::make_shared<CallExpr>(returnType, name, children);
}

velox::core::TypedExprPtr ExprResolver::makeConstantTypedExpr(
    const ExprPtr& expr) const {
  auto vector = variantToVector(
      expr->type(), *expr->as<ConstantExpr>()->value(), pool_.get());
  return std::make_shared<velox::core::ConstantTypedExpr>(vector);
}

ExprPtr ExprResolver::makeConstant(const velox::VectorPtr& vector) const {
  auto variant = std::make_shared<velox::Variant>(vector->variantAt(0));
  return std::make_shared<ConstantExpr>(vector->type(), std::move(variant));
}

ExprPtr ExprResolver::tryFoldCall(
    const velox::TypePtr& type,
    const std::string& name,
    const std::vector<ExprPtr>& inputs) const {
  if (!queryCtx_) {
    return nullptr;
  }
  for (const auto& arg : inputs) {
    if (arg->kind() != ExprKind::kConstant) {
      return nullptr;
    }
  }
  std::vector<velox::core::TypedExprPtr> args;
  args.reserve(inputs.size());
  for (const auto& arg : inputs) {
    args.push_back(makeConstantTypedExpr(arg));
  }
  auto vector = velox::exec::tryEvaluateConstantExpression(
      std::make_shared<velox::core::CallTypedExpr>(type, std::move(args), name),
      pool_.get(),
      queryCtx_,
      true);
  if (vector) {
    return makeConstant(vector);
  }
  return nullptr;
}

ExprPtr ExprResolver::tryFoldSpecialForm(
    const std::string& name,
    const std::vector<ExprPtr>& inputs) const {
  if (!queryCtx_) {
    return nullptr;
  }
  if (name == "in" && inputs.at(0)->isConstant() &&
      !inputs.at(1)->isSubquery()) {
    auto elementType = inputs[0]->type();

    std::vector<velox::Variant> arrayElements;
    arrayElements.reserve(inputs.size() - 1);
    for (size_t i = 1; i < inputs.size(); i++) {
      VELOX_USER_CHECK(inputs.at(i)->isConstant());
      arrayElements.push_back(*inputs.at(i)->as<ConstantExpr>()->value());
    }

    auto arrayConstant = std::make_shared<ConstantExpr>(
        ARRAY(elementType),
        std::make_shared<velox::Variant>(velox::Variant::array(arrayElements)));

    return tryFoldCall(velox::BOOLEAN(), "in", {inputs[0], arrayConstant});
  }
  return nullptr;
}

ExprPtr ExprResolver::tryFoldCast(
    const velox::TypePtr& type,
    const ExprPtr& input) const {
  if (!queryCtx_ || input->kind() != ExprKind::kConstant) {
    return nullptr;
  }
  auto vector = velox::exec::tryEvaluateConstantExpression(
      std::make_shared<velox::core::CastTypedExpr>(
          type, makeConstantTypedExpr(input), false),
      pool_.get(),
      queryCtx_,
      true);
  if (vector) {
    return makeConstant(vector);
  }
  return nullptr;
}

namespace {
std::optional<std::string> tryGetRootName(const velox::core::ExprPtr& expr) {
  if (const auto* fieldAccess =
          dynamic_cast<const velox::core::FieldAccessExpr*>(expr.get())) {
    if (fieldAccess->isRootColumn()) {
      return fieldAccess->name();
    }
  }

  return std::nullopt;
}

int32_t parseLegacyRowFieldOrdinal(
    const std::string& name,
    const velox::RowType& rowType) {
  VELOX_USER_CHECK(
      boost::istarts_with(name, "field"),
      "Invalid field name: {}. Available names are: {}",
      name,
      folly::join(", ", rowType.names()));

  const auto* start = name.c_str() + 5;
  const auto* end = name.c_str() + name.size();

  int32_t ordinal;
  auto parseResult = std::from_chars(start, end, ordinal);

  VELOX_USER_CHECK(
      parseResult.ec == std::errc{}, "Invalid legacy field name: {}", name);
  VELOX_USER_CHECK(
      parseResult.ptr == end, "Invalid legacy field name: {}", name);

  VELOX_USER_CHECK_GE(ordinal, 0, "Invalid legacy field name: {}", name);
  VELOX_USER_CHECK_LT(
      ordinal, rowType.size(), "Invalid legacy field name: {}", name);

  VELOX_USER_CHECK(
      rowType.nameOf(ordinal).empty(),
      "Cannot access named field using legacy field name: {} vs. {}",
      name,
      rowType.nameOf(ordinal));

  return ordinal;
}

} // namespace

ExprPtr ExprResolver::resolveScalarTypes(
    const velox::core::ExprPtr& expr,
    const InputNameResolver& inputNameResolver) const {
  if (const auto* fieldAccess =
          dynamic_cast<const velox::core::FieldAccessExpr*>(expr.get())) {
    const auto& name = fieldAccess->name();

    if (fieldAccess->isRootColumn()) {
      return inputNameResolver(std::nullopt, name);
    }

    if (auto rootName = tryGetRootName(fieldAccess->input())) {
      if (auto resolved = inputNameResolver(rootName, name)) {
        return resolved;
      }
    }

    auto input = resolveScalarTypes(fieldAccess->input(), inputNameResolver);

    VELOX_USER_CHECK_EQ(
        input->type()->kind(),
        velox::TypeKind::ROW,
        "Expected a struct, but got {}",
        input->type()->toString());

    const auto& inputRowType = input->type()->asRow();

    std::optional<int32_t> index;
    for (auto i = 0; i < inputRowType.size(); ++i) {
      if (boost::iequals(inputRowType.nameOf(i), name)) {
        index = i;
        break;
      }
    }

    if (index.has_value()) {
      return std::make_shared<SpecialFormExpr>(
          inputRowType.childAt(index.value()),
          SpecialForm::kDereference,
          std::vector<ExprPtr>{
              input,
              std::make_shared<ConstantExpr>(
                  velox::VARCHAR(),
                  std::make_shared<velox::Variant>(
                      inputRowType.nameOf(index.value())))});
    }

    const auto ordinal = parseLegacyRowFieldOrdinal(name, inputRowType);
    return std::make_shared<SpecialFormExpr>(
        inputRowType.childAt(ordinal),
        SpecialForm::kDereference,
        std::vector<ExprPtr>{
            input,
            std::make_shared<ConstantExpr>(
                velox::INTEGER(), std::make_shared<velox::Variant>(ordinal))});
  }

  if (const auto& constant =
          dynamic_cast<const velox::core::ConstantExpr*>(expr.get())) {
    return std::make_shared<ConstantExpr>(
        constant->type(), std::make_shared<velox::Variant>(constant->value()));
  }

  if (auto lambdaCall = tryResolveCallWithLambdas(
          std::dynamic_pointer_cast<const velox::core::CallExpr>(expr),
          inputNameResolver)) {
    return lambdaCall;
  }

  std::vector<ExprPtr> inputs;
  inputs.reserve(expr->inputs().size());
  for (const auto& input : expr->inputs()) {
    inputs.push_back(resolveScalarTypes(input, inputNameResolver));
  }

  if (const auto* call =
          dynamic_cast<const velox::core::CallExpr*>(expr.get())) {
    const auto name = velox::exec::sanitizeName(call->name());

    if (hook_ != nullptr) {
      auto result = hook_(name, inputs);
      if (result != nullptr) {
        return result;
      }
    }

    if (auto specialForm = tryResolveSpecialForm(
            name, inputs, enableCoercions_, planNodeIdGenerator_)) {
      if (auto folded = tryFoldSpecialForm(name, inputs)) {
        return folded;
      }
      return specialForm;
    }

    auto type = resolveScalarFunction(name, inputs, enableCoercions_);
    VELOX_CHECK_NOT_NULL(type);

    auto folded = tryFoldCall(type, name, inputs);
    if (folded != nullptr) {
      return folded;
    }

    return std::make_shared<CallExpr>(type, name, inputs);
  }

  if (const auto* cast =
          dynamic_cast<const velox::core::CastExpr*>(expr.get())) {
    auto folded = tryFoldCast(cast->type(), inputs[0]);
    if (folded != nullptr) {
      return folded;
    }

    return std::make_shared<SpecialFormExpr>(
        cast->type(),
        cast->isTryCast() ? SpecialForm::kTryCast : SpecialForm::kCast,
        inputs);
  }

  if (const auto* subquery =
          dynamic_cast<const velox::core::SubqueryExpr*>(expr.get())) {
    return std::make_shared<SubqueryExpr>(subquery->subquery());
  }

  VELOX_NYI("Can't resolve {}", expr->toString());
}

AggregateExprPtr ExprResolver::resolveAggregateTypes(
    const velox::core::ExprPtr& expr,
    const InputNameResolver& inputNameResolver,
    const ExprPtr& filter,
    const std::vector<SortingField>& ordering,
    bool distinct) const {
  const auto* call = dynamic_cast<const velox::core::CallExpr*>(expr.get());
  VELOX_USER_CHECK_NOT_NULL(
      call, "Aggregate must be a call expression: {}", expr->toString());

  const auto name = velox::exec::sanitizeName(call->name());

  std::vector<ExprPtr> inputs;
  inputs.reserve(expr->inputs().size());
  for (const auto& input : expr->inputs()) {
    inputs.push_back(resolveScalarTypes(input, inputNameResolver));
  }

  std::vector<velox::TypePtr> inputTypes;
  inputTypes.reserve(inputs.size());
  for (const auto& input : inputs) {
    inputTypes.push_back(input->type());
  }

  velox::TypePtr type;
  if (enableCoercions_) {
    std::vector<velox::TypePtr> coercions;
    type = velox::exec::resolveResultTypeWithCoercions(
        name, inputTypes, coercions);
    applyCoercions(inputs, coercions);
  } else {
    type = velox::exec::resolveResultType(name, inputTypes);
  }

  return std::make_shared<AggregateExpr>(
      type, name, inputs, filter, ordering, distinct);
}

ExprResolver::WindowResolveResult ExprResolver::resolveWindowTypes(
    const velox::core::ExprPtr& expr,
    const InputNameResolver& inputNameResolver) const {
  const auto* call = dynamic_cast<const velox::core::CallExpr*>(expr.get());
  VELOX_USER_CHECK_NOT_NULL(
      call, "Window function must be a call expression: {}", expr->toString());

  const auto& name = call->name();

  std::vector<ExprPtr> inputs;
  inputs.reserve(expr->inputs().size());
  for (const auto& input : expr->inputs()) {
    inputs.push_back(resolveScalarTypes(input, inputNameResolver));
  }

  std::vector<velox::TypePtr> inputTypes;
  inputTypes.reserve(inputs.size());
  for (const auto& input : inputs) {
    inputTypes.push_back(input->type());
  }

  // First try window function signatures
  auto windowSignatures = velox::exec::getWindowFunctionSignatures(name);
  if (windowSignatures.has_value()) {
    for (const auto& signature : windowSignatures.value()) {
      velox::exec::SignatureBinder binder(*signature, inputTypes);
      if (binder.tryBind()) {
        if (auto type = binder.tryResolveReturnType()) {
          return {type, name, inputs};
        }
      }
    }
  }

  auto aggregateSignatures = velox::exec::getAggregateFunctionSignatures(name);
  if (aggregateSignatures.has_value()) {
    for (const auto& signature : aggregateSignatures.value()) {
      velox::exec::SignatureBinder binder(*signature, inputTypes);
      if (binder.tryBind()) {
        if (auto type = binder.tryResolveReturnType()) {
          return {type, name, inputs};
        }
      }
    }
  }

  if (!windowSignatures.has_value() && !aggregateSignatures.has_value()) {
    VELOX_USER_FAIL("Window Function doesn't exist: {}.", name);
  } else {
    std::string errorMsg = "Window function signature is not supported: " +
        toString(name, inputTypes) + ". ";
    if (windowSignatures.has_value()) {
      errorMsg +=
          "Window function signatures: " + toString(windowSignatures.value()) +
          ". ";
    }
    if (aggregateSignatures.has_value()) {
      errorMsg += "Aggregate function signatures: " +
          toString(aggregateSignatures.value()) + ".";
    }
    VELOX_USER_FAIL(errorMsg);
  }

  VELOX_UNREACHABLE();
}

} // namespace facebook::axiom::logical_plan
