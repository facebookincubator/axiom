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

#include "axiom/optimizer/v1/DerivedTable.h"
#include "folly/container/F14Map.h"

namespace facebook::axiom::optimizer::v1 {

using ExprMapping = folly::F14FastMap<ExprCP, ExprCP>;

/// Supports inlining one DerivedTable into another. When a DT is a trivial
/// wrapper around a single child (e.g., after UNION ALL collapses to one
/// leg, or when isWrapOnly), the child's fields can be copied directly into
/// the parent to eliminate unnecessary nesting. After the copy, columns that
/// referenced the child DT become dangling. This class fixes those
/// references.
class DerivedTableFlattener {
 public:
  /// Reconstructs columns in 'dt' that have relation_ == 'oldDt' so they
  /// reference 'dt' instead. Columns produced by aggregation, window, and
  /// outer join layers may have relation_ pointing to oldDt (the DT that
  /// originally owned them). After inlining, oldDt is no longer in dt's
  /// tableSet, so these columns become dangling. This method recreates
  /// them with relation_ == dt and rewrites all expressions that reference
  /// the old columns.
  ///
  /// Processes DT layers bottom-up: joins (2), filters (3),
  /// aggregation (4), having (5), window (6), projection expressions (7).
  static void reconstructColumns(DerivedTable* dt, const DerivedTable* oldDt);

  /// Replaces column references in an expression tree. Columns found in
  /// 'source' are replaced with the corresponding entry in 'target'.
  /// Handles all expression types including WindowFunction.
  static ExprCP replaceInputs(
      ExprCP expr,
      const ColumnVector& source,
      const ExprVector& target);

  static ExprCP replaceInputs(
      ExprCP expr,
      const ExprVector& source,
      const ColumnVector& target);

 private:
  static ExprCP replaceInputs(ExprCP expr, const ExprMapping& mapping);
};

} // namespace facebook::axiom::optimizer::v1
