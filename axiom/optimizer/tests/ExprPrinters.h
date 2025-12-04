/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
#pragma once

#include "velox/core/Expressions.h"
#include "velox/core/ITypedExpr.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/IExpr.h"

namespace facebook::velox::core {

/// Printer for ITypedExpr expressions.
/// Converts typed expressions to their textual representation.
class ITypedExprPrinter {
 public:
  /// Converts a typed expression to its textual representation.
  /// Uses the kind enum to dispatch to the appropriate subclass printer.
  static std::string toText(const ITypedExpr& expr);

 private:
  static std::string toTextImpl(const InputTypedExpr& expr);
  static std::string toTextImpl(const ConstantTypedExpr& expr);
  static std::string toTextImpl(const CallTypedExpr& expr);
  static std::string toTextImpl(const FieldAccessTypedExpr& expr);
  static std::string toTextImpl(const DereferenceTypedExpr& expr);
  static std::string toTextImpl(const ConcatTypedExpr& expr);
  static std::string toTextImpl(const LambdaTypedExpr& expr);
  static std::string toTextImpl(const CastTypedExpr& expr);
};

/// Printer for IExpr expressions.
/// Converts implicitly-typed expressions to their textual representation.
class IExprPrinter {
 public:
  /// Converts an expression to its textual representation.
  /// Uses dynamic_cast to dispatch to the appropriate subclass printer.
  static std::string toText(const IExpr& expr);

 private:
  static std::string toTextImpl(const InputExpr& expr);
  static std::string toTextImpl(const FieldAccessExpr& expr);
  static std::string toTextImpl(const CallExpr& expr);
  static std::string toTextImpl(const ConstantExpr& expr);
  static std::string toTextImpl(const CastExpr& expr);
  static std::string toTextImpl(const LambdaExpr& expr);
};

} // namespace facebook::velox::core
