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

#include "axiom/optimizer/DerivedTable.h"
#include "axiom/optimizer/RelationOp.h"

namespace facebook::axiom::optimizer {

/// Builder-style class to generate an optional ProjectOp node that computes
/// expressions needed by nodes that cannot evaluate expressions directly.
///
/// The only Velox plan nodes that can evaluate expressions directly are Filter
/// and Project. Other nodes require expressions precomputed. Some nodes allow
/// constant expressions.
///
/// Aggregation node requires grouping keys, aggregate masks and inputs to
/// aggregates precomputed. It allows constant expressions for inputs to
/// aggregates, but not for grouping keys or masks.
///
/// OrderBy node requires sorting keys precomputed.
///
/// HashJoin node requires join keys precomputed. It allows arbitrary
/// expressions for the extra filter.
///
/// Unnest node requires replicated and unnest columns precomputed.
///
/// In addition, Aggregation allows to specify output name for the aggregate
/// columns, but not for grouping keys. It projects grouping keys as is. Hence,
/// it may be necessary to add Project node before Aggregation to rename
/// grouping keys. For example, this would be the case for "SELECT k as key,
/// count(1) FROM t GROUP BY 1" query.
///
/// The basic usage of this class is the following:
///   - Create an instance by providing an input RelationOp and a DerivedTable
///   it comes from.
///   - Call toColumn(expr) or toColumns(exprs) for all expressions used in the
///   relation.
///   - Call maybeProject() to retrieve the new input RelationOp (either
///   original one or the one with ProjectOp on top).
///   - Re-write the relation to replace expressions with columns from the new
///   input.
///
/// By default, all input columns are projected out alongside new expressions.
/// To project only necessary columns, set 'projectAllInputs' to false in the
/// ctor. (Used for Unnest and Aggregation.)
///
/// By default, all non-column expressions are replaced with projections. To
/// preserve both column and literal expressions, set 'preserveLiterals' to true
/// when calling toColumn(s). (Used for Aggregation.)
///
/// toColumn(s) methods take an optional 'alias(es)' parameter to specify that
/// the expression must be accessible with a particular name. This is needed to
/// ensure that grouping keys projected out of an Aggregation have specific
/// names. (TODO: Extend AggregationNode in Velox to allow specifying output
/// names for grouping keys.)
class PrecomputeProjection {
 public:
  /// @param input Input relation.
  /// @param dt DerivedTable the relations belong to. Used to specify DT for
  /// newly-created columns.
  /// TODO Do not require a DerivedTable here. Allow for creating ephemeral
  /// columns that are not associated with a BaseTable or a DerivedTable.
  PrecomputeProjection(
      const RelationOpPtr& input,
      DerivedTableCP dt,
      bool projectAllInputs = true);

  /// @param alias Optional column that specifies the required alias for the
  /// expression.
  ExprCP toColumn(
      ExprCP expr,
      ColumnCP alias = nullptr,
      bool preserveLiterals = false);

  /// @param aliases Optional list of columns that specify the required aliases
  /// for the expression. If specified, must correspond 1:1 to 'exprs'. May have
  /// more entries than 'exprs'.
  ExprVector toColumns(
      const ExprVector& exprs,
      const ColumnVector* aliases = nullptr,
      bool preserveLiterals = false);

  /// @returns the original 'input' with an optional ProjectOp on top.
  RelationOpPtr maybeProject() && {
    if (needsProject_) {
      return make<Project>(input_, projectExprs_, projectColumns_);
    }

    return input_;
  }

 private:
  void addToProject(ExprCP expr, ColumnCP column);

  const RelationOpPtr& input_;
  DerivedTableCP const dt_;

  // A list of expressions to project. Populated with input columns in the ctor
  // (if projectAllInputs is true). Apppended to by 'toColumn(s)' methods.
  ExprVector projectExprs_;

  // A list of columns to project. 1:1 with 'projectExprs_'.
  ColumnVector projectColumns_;

  // A mapping of expressions already present in 'projectExprs_'. The key is an
  // expression from 'projectExprs_'. The value is the corresponding column from
  // 'projectColumns_'.
  folly::F14FastMap<ExprCP, ColumnCP> projections_;

  // True if there is a non-trivial expression over inputs or an input column
  // needs to be projected with a different name.
  bool needsProject_{false};
};

} // namespace facebook::axiom::optimizer
