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

#include <folly/ConstexprMath.h>
#include <folly/container/F14Map.h>
#include <folly/container/HeterogeneousAccess.h>
#include <folly/lang/Hint.h>
#include <stddef.h>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/sql/presto/ast/AstExpressions.h"
#include "axiom/sql/presto/ast/AstNodesAll.h"
#include "axiom/sql/presto/ast/AstRelations.h"

namespace axiom::sql::presto {

namespace lp = facebook::axiom::logical_plan;

/// Tracks per-query display names as the AST is walked. Held by
/// 'RelationPlanner' and updated at SELECT and relation boundaries.
///
/// Example walk-through for `SELECT * FROM (SELECT a AS Foo FROM t) AS sub`:
///   1. Inner SELECT finishes; 'lastNames' is set to ["Foo"].
///   2. 'accumulate(builder, "sub")' writes ["Foo"] into 'accumulatedNames'
///      under both {"sub", "foo"} and {nullopt, "foo"}.
///   3. Outer 'SELECT *' calls 'captureLastNames(builder)', which reads
///      'accumulatedNames[{nullopt, "foo"}]' and sets 'lastNames' to ["Foo"].
///   4. 'plan()' passes 'lastNames' to PlanBuilder so the OutputNode preserves
///      the user-written case 'Foo'.
struct DisplayNames {
  /// Per-column display names of the most recently processed
  /// SELECT/relation.
  std::vector<std::optional<std::string>> lastNames;

  /// User-written case captured at relation boundaries (subqueries, CTEs,
  /// AliasedRelation) as they finish planning. Star and COLUMNS expansions
  /// in an enclosing SELECT consult this to recover the original case.
  ///
  /// Keyed by {relation alias, canonical column name}: an alias-qualified
  /// relation (e.g. 'AS t') stores each column twice — once under its alias
  /// and once under std::nullopt — so both 't.col' and bare 'col' lookups
  /// find the override. An unaliased source stores only under std::nullopt.
  /// The canonical column name is the lowercase form (matches what
  /// 'PlanBuilder::outputNames()' returns).
  folly::F14FastMap<
      std::pair<std::optional<std::string>, std::string>,
      std::string>
      accumulatedNames;

  /// User-written display name for a SingleColumn SELECT item, or nullopt
  /// if the expression has no obvious source name (e.g., an arithmetic
  /// expression with no alias).
  static std::optional<std::string> displayName(
      const SingleColumn& singleColumn);

  /// Returns the display-case override for a column, if any. Falls back to
  /// the unaliased key so a prefix-less lookup can find an entry recorded
  /// under an explicit alias.
  std::optional<std::string> displayName(
      const std::optional<std::string>& alias,
      const std::string& name) const;

  /// Captures 'lastNames' by looking up each of 'builder' columns in
  /// 'accumulatedNames'. Used when a SELECT passes columns through
  /// unchanged (bare 'SELECT *') and there are no SELECT-item-derived
  /// names to use.
  void captureLastNames(const lp::PlanBuilder& builder);

  /// Captures 'lastNames' from a user-provided column-alias list (e.g. CTE
  /// 'WITH t(a, b) AS ...' or AliasedRelation '... AS sub(a, b)').
  void captureLastNames(const std::vector<IdentifierPtr>& aliases);

  /// Captures 'lastNames' from SELECT items. All items must be SingleColumn
  /// (the global-aggregate path enforces this).
  void captureLastNames(const std::vector<SelectItemPtr>& items);

  /// Captures 'lastNames' into 'accumulatedNames' as the display names of
  /// 'builder' columns under 'relationAlias', so an enclosing SELECT's star
  /// or COLUMNS expansion can recover them. Must not register new output
  /// names on 'builder'.
  void accumulate(
      const lp::PlanBuilder& builder,
      const std::optional<std::string>& relationAlias);

  /// Captures display names for 'builder' columns starting at 'firstColumn'.
  /// Used when a relation appends columns to an existing input.
  void accumulateFrom(
      const lp::PlanBuilder& builder,
      const std::optional<std::string>& relationAlias,
      size_t firstColumn);
};

} // namespace axiom::sql::presto
