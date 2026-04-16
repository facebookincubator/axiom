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

#include "axiom/optimizer/tests/ExpressionMatcher.h"
#include <gtest/gtest.h>
#include "velox/core/Expressions.h"
#include "velox/type/Type.h"

namespace facebook::velox::core {

struct ExpressionMatcher::Impl {
  enum class Kind { kAny, kField, kConstant, kCall, kCast };

  Kind kind{Kind::kAny};
  std::string name;
  std::shared_ptr<const ConstantTypedExpr> constantExpr;
  TypePtr castType;
  std::vector<ExpressionMatcher> inputs;
};

bool ExpressionMatcher::matchImpl(
    const Impl& pattern,
    const ITypedExpr* actual) {
  switch (pattern.kind) {
    case Impl::Kind::kAny:
      return true;

    case Impl::Kind::kField: {
      auto* af = dynamic_cast<const FieldAccessTypedExpr*>(actual);
      if (!af) {
        return false;
      }
      return af->name() == pattern.name;
    }

    case Impl::Kind::kConstant: {
      auto* ac = dynamic_cast<const ConstantTypedExpr*>(actual);
      if (!ac) {
        return false;
      }
      return *ac == *pattern.constantExpr;
    }

    case Impl::Kind::kCall: {
      auto* aCall = dynamic_cast<const CallTypedExpr*>(actual);
      if (!aCall) {
        return false;
      }
      if (aCall->name() != pattern.name) {
        return false;
      }
      if (aCall->inputs().size() != pattern.inputs.size()) {
        return false;
      }
      for (size_t i = 0; i < pattern.inputs.size(); ++i) {
        if (!matchImpl(*pattern.inputs[i].impl_, aCall->inputs()[i].get())) {
          return false;
        }
      }
      return true;
    }

    case Impl::Kind::kCast: {
      auto* aCast = dynamic_cast<const CastTypedExpr*>(actual);
      if (!aCast) {
        return false;
      }
      if (!aCast->type()->equivalent(*pattern.castType)) {
        return false;
      }
      if (aCast->inputs().size() != pattern.inputs.size()) {
        return false;
      }
      for (size_t i = 0; i < pattern.inputs.size(); ++i) {
        if (!matchImpl(*pattern.inputs[i].impl_, aCast->inputs()[i].get())) {
          return false;
        }
      }
      return true;
    }
  }
  VELOX_UNREACHABLE();
}

std::string ExpressionMatcher::describePattern(const Impl& pattern) {
  switch (pattern.kind) {
    case Impl::Kind::kAny:
      return "<any>";
    case Impl::Kind::kField:
      return pattern.name;
    case Impl::Kind::kConstant:
      return pattern.constantExpr->toString();
    case Impl::Kind::kCall: {
      std::string result = pattern.name + "(";
      for (size_t i = 0; i < pattern.inputs.size(); ++i) {
        if (i > 0) {
          result += ", ";
        }
        result += describePattern(*pattern.inputs[i].impl_);
      }
      result += ")";
      return result;
    }
    case Impl::Kind::kCast: {
      std::string result = "cast(";
      if (!pattern.inputs.empty()) {
        result += describePattern(*pattern.inputs[0].impl_);
      }
      result += " as " + pattern.castType->toString() + ")";
      return result;
    }
  }
  VELOX_UNREACHABLE();
}

ExpressionMatcher::ExpressionMatcher(std::shared_ptr<const Impl> impl)
    : impl_(std::move(impl)) {}

ExpressionMatcher::ExpressionMatcher(int32_t value)
    : ExpressionMatcher(constant(value)) {}
ExpressionMatcher::ExpressionMatcher(int64_t value)
    : ExpressionMatcher(constant(value)) {}
ExpressionMatcher::ExpressionMatcher(double value)
    : ExpressionMatcher(constant(value)) {}

ExpressionMatcher ExpressionMatcher::any() {
  auto impl = std::make_shared<Impl>();
  impl->kind = Impl::Kind::kAny;
  return ExpressionMatcher(std::move(impl));
}

ExpressionMatcher ExpressionMatcher::col(const std::string& name) {
  auto impl = std::make_shared<Impl>();
  impl->kind = Impl::Kind::kField;
  impl->name = name;
  return ExpressionMatcher(std::move(impl));
}

ExpressionMatcher ExpressionMatcher::constant(int64_t value) {
  auto impl = std::make_shared<Impl>();
  impl->kind = Impl::Kind::kConstant;
  impl->constantExpr =
      std::make_shared<ConstantTypedExpr>(BIGINT(), Variant(value));
  return ExpressionMatcher(std::move(impl));
}

ExpressionMatcher ExpressionMatcher::constant(int32_t value) {
  auto impl = std::make_shared<Impl>();
  impl->kind = Impl::Kind::kConstant;
  impl->constantExpr =
      std::make_shared<ConstantTypedExpr>(INTEGER(), Variant(value));
  return ExpressionMatcher(std::move(impl));
}

ExpressionMatcher ExpressionMatcher::constant(float value) {
  auto impl = std::make_shared<Impl>();
  impl->kind = Impl::Kind::kConstant;
  impl->constantExpr =
      std::make_shared<ConstantTypedExpr>(REAL(), Variant(value));
  return ExpressionMatcher(std::move(impl));
}

ExpressionMatcher ExpressionMatcher::constant(double value) {
  auto impl = std::make_shared<Impl>();
  impl->kind = Impl::Kind::kConstant;
  impl->constantExpr =
      std::make_shared<ConstantTypedExpr>(DOUBLE(), Variant(value));
  return ExpressionMatcher(std::move(impl));
}

ExpressionMatcher ExpressionMatcher::constant(bool value) {
  auto impl = std::make_shared<Impl>();
  impl->kind = Impl::Kind::kConstant;
  impl->constantExpr =
      std::make_shared<ConstantTypedExpr>(BOOLEAN(), Variant(value));
  return ExpressionMatcher(std::move(impl));
}

ExpressionMatcher ExpressionMatcher::constant(const std::string& value) {
  auto impl = std::make_shared<Impl>();
  impl->kind = Impl::Kind::kConstant;
  impl->constantExpr =
      std::make_shared<ConstantTypedExpr>(VARCHAR(), Variant(value));
  return ExpressionMatcher(std::move(impl));
}

ExpressionMatcher ExpressionMatcher::constant(const char* value) {
  return constant(std::string(value));
}

ExpressionMatcher ExpressionMatcher::constant(
    const TypePtr& type,
    const std::string& value) {
  auto impl = std::make_shared<Impl>();
  impl->kind = Impl::Kind::kConstant;
  if (type->isDate()) {
    impl->constantExpr = std::make_shared<ConstantTypedExpr>(
        type, Variant(DATE()->toDays(value)));
  } else {
    VELOX_UNSUPPORTED(
        "constant(TypePtr, string) not yet supported for {}", type->toString());
  }
  return ExpressionMatcher(std::move(impl));
}

ExpressionMatcher ExpressionMatcher::call(
    const std::string& name,
    std::vector<ExpressionMatcher> inputs) {
  auto impl = std::make_shared<Impl>();
  impl->kind = Impl::Kind::kCall;
  impl->name = name;
  impl->inputs = std::move(inputs);
  return ExpressionMatcher(std::move(impl));
}

ExpressionMatcher ExpressionMatcher::cast(
    TypePtr targetType,
    ExpressionMatcher input) {
  auto impl = std::make_shared<Impl>();
  impl->kind = Impl::Kind::kCast;
  impl->castType = std::move(targetType);
  impl->inputs = {std::move(input)};
  return ExpressionMatcher(std::move(impl));
}

ExpressionMatcher ExpressionMatcher::eq(ExpressionMatcher rhs) const {
  return call("eq", {*this, std::move(rhs)});
}

ExpressionMatcher ExpressionMatcher::neq(ExpressionMatcher rhs) const {
  return call("neq", {*this, std::move(rhs)});
}

ExpressionMatcher ExpressionMatcher::lt(ExpressionMatcher rhs) const {
  return call("lt", {*this, std::move(rhs)});
}

ExpressionMatcher ExpressionMatcher::lte(ExpressionMatcher rhs) const {
  return call("lte", {*this, std::move(rhs)});
}

ExpressionMatcher ExpressionMatcher::gt(ExpressionMatcher rhs) const {
  return call("gt", {*this, std::move(rhs)});
}

ExpressionMatcher ExpressionMatcher::gte(ExpressionMatcher rhs) const {
  return call("gte", {*this, std::move(rhs)});
}

ExpressionMatcher ExpressionMatcher::between(
    ExpressionMatcher low,
    ExpressionMatcher high) const {
  return call("between", {*this, std::move(low), std::move(high)});
}

ExpressionMatcher ExpressionMatcher::and_(ExpressionMatcher rhs) const {
  return call("and", {*this, std::move(rhs)});
}

ExpressionMatcher ExpressionMatcher::or_(ExpressionMatcher rhs) const {
  return call("or", {*this, std::move(rhs)});
}

ExpressionMatcher ExpressionMatcher::not_() const {
  return call("not", {*this});
}

ExpressionMatcher ExpressionMatcher::like(ExpressionMatcher pattern) const {
  return call("like", {*this, std::move(pattern)});
}

bool ExpressionMatcher::match(const TypedExprPtr& actual) const {
  EXPECT_NE(actual, nullptr)
      << "Expected expression matching: " << describePattern(*impl_);
  if (actual == nullptr) {
    return false;
  }
  bool result = matchImpl(*impl_, actual.get());
  EXPECT_TRUE(result) << "Expression mismatch:\n  actual:   "
                      << actual->toString()
                      << "\n  expected: " << describePattern(*impl_);
  return result;
}

} // namespace facebook::velox::core
