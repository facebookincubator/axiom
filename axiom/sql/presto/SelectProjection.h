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

#include <functional>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

#include <folly/container/F14Map.h>

#include "axiom/logical_plan/ExprApi.h"
#include "axiom/logical_plan/NameAllocator.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/sql/presto/ast/AstSupport.h"

namespace axiom::sql::presto {

/// Plans what one query block's SELECT list computes below its final
/// projection:
///
/// - A window function nested inside another expression, e.g. the window in
///   `x / sum(x) OVER ()`. PlanBuilder projects a window function only as a
///   whole projection.
/// - A SELECT item read through its output alias, from ORDER BY or from a
///   later SELECT item (a lateral column alias), or through its position in
///   GROUP BY. Every reader reads the same value: the item is evaluated once.
///
/// While the SELECT list is translated, a reference to an item is a
/// placeholder column. 'lowerBelowAggregate' and 'lower' replace the
/// placeholders: an item read once is evaluated where it is read; an item read
/// more than once is computed in a projection below its readers, which read
/// its column.
///
/// Example:
///
///   SELECT random() AS r FROM t ORDER BY r + 1
///
///   Translation:  r + 1   -> plus(<reference to item 0>, 1)
///   lower:        with(rand() AS r); the sort key becomes plus(r, 1).
///
/// Invariants:
/// - Placeholder names come from the statement's name allocator, so no column
///   carries one.
/// - Placeholders never reach PlanBuilder: 'lower' replaces every one in the
///   SELECT list and ORDER BY, and 'lowerBelowAggregate' every one in the
///   grouping keys and aggregates.
class SelectProjection {
 public:
  explicit SelectProjection(
      std::shared_ptr<facebook::axiom::logical_plan::NameAllocator>
          nameAllocator);

  /// Returns the 1-based SELECT position that 'expr' names in 'clause' when it
  /// is an integer literal, or nullopt. Fails when the position is not in
  /// 1..numItems.
  static std::optional<size_t>
  position(const Expression& expr, size_t numItems, std::string_view clause);

  /// An ORDER BY key resolved against the SELECT list.
  struct SortKey {
    /// 1-based position of the SELECT item the key names: by position, by
    /// output alias, or by the same expression once aliases are expanded. 0
    /// for any other expression.
    size_t item{0};

    /// The key's expression when 'item' is 0.
    std::optional<facebook::axiom::logical_plan::ExprApi> expr;
  };

  /// Resolves ORDER BY key 'sortKey' against SELECT 'items'. 'translate'
  /// translates the key with the output aliases in scope.
  SortKey resolveSortKey(
      const Expression& sortKey,
      const std::vector<facebook::axiom::logical_plan::ExprApi>& items,
      const std::function<facebook::axiom::logical_plan::ExprApi()>& translate)
      const;

  /// Returns the placeholder for SELECT item 'item' (0-based), the same on
  /// every call.
  facebook::velox::core::ExprPtr reference(size_t item);

  /// Returns the SELECT item (0-based) 'expr' refers to if it is a
  /// placeholder.
  std::optional<size_t> itemOf(
      const facebook::velox::core::ExprPtr& expr) const;

  /// Returns true if 'expr' holds a placeholder.
  bool readsItem(const facebook::velox::core::ExprPtr& expr) const;

  /// Returns 'expr' with each placeholder replaced by the expression of the
  /// SELECT item in 'items' it refers to.
  facebook::velox::core::ExprPtr inlineReferences(
      const facebook::velox::core::ExprPtr& expr,
      const std::vector<facebook::axiom::logical_plan::ExprApi>& items) const;

  /// Replaces the placeholders in 'readers', which are evaluated below an
  /// aggregation: its grouping keys and aggregates. 'items' are the SELECT
  /// items, evaluated above it, and are left unchanged. 'prepare' readies an
  /// item computed here for projection over the builder's current output.
  ///
  /// Returns, for each item, the expression its readers read: its column, or
  /// its expression when it is read once; nullptr for an item not read.
  std::vector<facebook::velox::core::ExprPtr> lowerBelowAggregate(
      facebook::axiom::logical_plan::PlanBuilder& builder,
      const std::vector<facebook::axiom::logical_plan::ExprApi>& items,
      std::vector<facebook::axiom::logical_plan::ExprApi>& readers,
      const std::function<facebook::velox::core::ExprPtr(
          const facebook::velox::core::ExprPtr&)>& prepare) const;

  /// Computes, over the builder's current output, each window function nested
  /// in 'items' or 'readers' and each SELECT item a placeholder refers to, and
  /// replaces them with their columns. A computed item is replaced in 'items'
  /// with its column under its alias. Afterwards PlanBuilder can project
  /// 'items' and 'readers'.
  ///
  /// 'items' and 'readers' must read only the builder's current output and
  /// placeholders.
  void lower(
      facebook::axiom::logical_plan::PlanBuilder& builder,
      std::vector<facebook::axiom::logical_plan::ExprApi>& items,
      std::vector<facebook::axiom::logical_plan::ExprApi>& readers) const;

  /// As above, with no readers other than the items.
  void lower(
      facebook::axiom::logical_plan::PlanBuilder& builder,
      std::vector<facebook::axiom::logical_plan::ExprApi>& items) const;

 private:
  // Where the expressions being lowered are evaluated.
  enum class Level {
    // Below an aggregation: the items are not evaluated there.
    kBelowAggregate,
    // Below the final projection, which evaluates every item.
    kBelowProjection,
  };

  std::vector<facebook::velox::core::ExprPtr> lower(
      Level level,
      facebook::axiom::logical_plan::PlanBuilder& builder,
      std::vector<facebook::axiom::logical_plan::ExprApi>& items,
      std::vector<facebook::axiom::logical_plan::ExprApi>& readers,
      const std::function<facebook::velox::core::ExprPtr(
          const facebook::velox::core::ExprPtr&)>& prepare) const;

  std::shared_ptr<facebook::axiom::logical_plan::NameAllocator> nameAllocator_;

  // Placeholder of each SELECT item, by item; nullptr for an item no
  // reference was created for.
  std::vector<facebook::velox::core::ExprPtr> placeholders_;

  // SELECT item of each placeholder, by placeholder name.
  folly::F14FastMap<std::string, size_t> itemByPlaceholder_;
};

} // namespace axiom::sql::presto
