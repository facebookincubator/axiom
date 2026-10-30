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

#pragma once

#include <folly/container/F14Map.h>

#include "axiom/optimizer/ExprFactory.h"
#include "velox/core/ITypedExpr.h"

namespace facebook::axiom::optimizer {

/// Converts Velox expressions into IR under fixed input-column bindings.
/// Construct one importer for expressions that share bindings; lambda
/// parameters shadow those bindings only while importing the lambda body.
///
///   VeloxExprImporter importer(builder, columns);
///   ExprCP expr = importer.importExpr(veloxExpr);
class VeloxExprImporter {
 public:
  VeloxExprImporter(
      Builder& builder,
      folly::F14FastMap<std::string_view, ColumnCP> columns);

  /// Converts `expr` into IR. Reading a column outside the bindings supplied
  /// to the constructor fails.
  ExprCP importExpr(const velox::core::TypedExprPtr& expr);

 private:
  Builder& builder_;
  ExprFactory exprFactory_;
  folly::F14FastMap<std::string_view, ColumnCP> columns_;
  folly::F14FastMap<std::string_view, Name> specialFormNames_;
};

} // namespace facebook::axiom::optimizer
