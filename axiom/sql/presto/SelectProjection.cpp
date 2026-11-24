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
#include "axiom/sql/presto/SelectProjection.h"

#include "axiom/sql/presto/ExpressionPlanner.h"
#include "axiom/sql/presto/PrestoSqlError.h"
#include "axiom/sql/presto/ast/AstLiterals.h"
#include "velox/parse/Expressions.h"

namespace axiom::sql::presto {

namespace lp = facebook::axiom::logical_plan;
namespace core = facebook::velox::core;

SelectProjection::SelectProjection(
    std::shared_ptr<lp::NameAllocator> nameAllocator)
    : nameAllocator_{std::move(nameAllocator)} {
  VELOX_CHECK_NOT_NULL(nameAllocator_);
}

// static
std::optional<size_t> SelectProjection::position(
    const Expression& expr,
    size_t numItems,
    std::string_view clause) {
  const auto position = LongLiteral::asSelectPosition(expr);
  if (!position.has_value()) {
    return std::nullopt;
  }
  const auto n = position.value();
  AXIOM_PRESTO_SEMANTIC_CHECK_GE(
      n,
      static_cast<int64_t>(1),
      expr.location(),
      std::to_string(n),
      "{} position is not in the select list",
      clause);
  AXIOM_PRESTO_SEMANTIC_CHECK_LE(
      n,
      static_cast<int64_t>(numItems),
      expr.location(),
      std::to_string(n),
      "{} position is not in the select list",
      clause);
  return static_cast<size_t>(n);
}

SelectProjection::SortKey SelectProjection::resolveSortKey(
    const Expression& sortKey,
    const std::vector<lp::ExprApi>& items,
    const std::function<lp::ExprApi()>& translate) const {
  if (const auto item = position(sortKey, items.size(), "ORDER BY")) {
    return {.item = item.value()};
  }
  auto expr = translate();
  if (const auto item = itemOf(expr.expr())) {
    return {.item = item.value() + 1};
  }
  const auto inlined = inlineReferences(expr.expr(), items);
  core::IExprEqual equal;
  for (size_t i = 0; i < items.size(); ++i) {
    if (equal(inlined, inlineReferences(items[i].expr(), items))) {
      return {.item = i + 1};
    }
  }
  return {.expr = std::move(expr)};
}

core::ExprPtr SelectProjection::reference(size_t item) {
  if (placeholders_.size() <= item) {
    placeholders_.resize(item + 1);
  }
  auto& placeholder = placeholders_[item];
  if (placeholder == nullptr) {
    auto name = nameAllocator_->newName("__alias");
    placeholder = lp::Col(name).expr();
    itemByPlaceholder_.emplace(std::move(name), item);
  }
  return placeholder;
}

std::optional<size_t> SelectProjection::itemOf(
    const core::ExprPtr& expr) const {
  const auto* field = core::FieldAccessExpr::tryAsRootColumn(expr);
  if (field == nullptr) {
    return std::nullopt;
  }
  const auto it = itemByPlaceholder_.find(field->name());
  if (it == itemByPlaceholder_.end()) {
    return std::nullopt;
  }
  return it->second;
}

namespace {

// Calls 'onReference' with the SELECT item of each placeholder in 'expr'.
void forEachReference(
    const SelectProjection& projection,
    const core::ExprPtr& expr,
    const std::function<void(size_t item)>& onReference) {
  if (const auto item = projection.itemOf(expr)) {
    onReference(item.value());
    return;
  }
  ExpressionPlanner::rewriteInputs(expr, [&](const core::ExprPtr& input) {
    forEachReference(projection, input, onReference);
    return input;
  });
}

// Returns the window functions nested in 'expr', in traversal order.
std::vector<core::ExprPtr> nestedWindows(const lp::ExprApi& expr) {
  std::unordered_map<const core::IExpr*, core::ExprPtr> windows;
  std::vector<const core::IExpr*> order;
  ExpressionPlanner::findNestedWindowExprs({expr}, windows, order);

  std::vector<core::ExprPtr> result;
  result.reserve(order.size());
  for (const auto* window : order) {
    result.push_back(windows.at(window));
  }
  return result;
}

bool isColumn(const core::ExprPtr& expr) {
  return core::FieldAccessExpr::tryAsRootColumn(expr) != nullptr;
}

} // namespace

bool SelectProjection::readsItem(const core::ExprPtr& expr) const {
  bool found{false};
  forEachReference(*this, expr, [&](size_t) { found = true; });
  return found;
}

core::ExprPtr SelectProjection::inlineReferences(
    const core::ExprPtr& expr,
    const std::vector<lp::ExprApi>& items) const {
  if (const auto item = itemOf(expr)) {
    VELOX_CHECK_LT(item.value(), items.size());
    return inlineReferences(items[item.value()].expr(), items);
  }
  return ExpressionPlanner::rewriteInputs(
      expr, [&](const core::ExprPtr& input) {
        return inlineReferences(input, items);
      });
}

std::vector<core::ExprPtr> SelectProjection::lowerBelowAggregate(
    lp::PlanBuilder& builder,
    const std::vector<lp::ExprApi>& items,
    std::vector<lp::ExprApi>& readers,
    const std::function<core::ExprPtr(const core::ExprPtr&)>& prepare) const {
  auto itemsCopy = items;
  return lower(Level::kBelowAggregate, builder, itemsCopy, readers, prepare);
}

void SelectProjection::lower(
    lp::PlanBuilder& builder,
    std::vector<lp::ExprApi>& items,
    std::vector<lp::ExprApi>& readers) const {
  lower(Level::kBelowProjection, builder, items, readers, nullptr);
}

void SelectProjection::lower(
    lp::PlanBuilder& builder,
    std::vector<lp::ExprApi>& items) const {
  std::vector<lp::ExprApi> readers;
  lower(builder, items, readers);
}

namespace {

// One run of SelectProjection::lower. Each round computes, in one projection,
// every nested window and referenced item that reads nothing still pending.
class Lowering {
 public:
  Lowering(
      const SelectProjection& projection,
      bool belowProjection,
      std::vector<lp::ExprApi>& items,
      std::vector<lp::ExprApi>& readers)
      : projection_{projection},
        belowProjection_{belowProjection},
        items_{items},
        readers_{readers},
        numOwnUses_{belowProjection ? 1u : 0u},
        numUses_(items.size(), numOwnUses_),
        values_(items.size()) {
    countUses();
  }

  // Runs the rounds and returns what the readers of each item read.
  std::vector<core::ExprPtr> run(
      lp::PlanBuilder& builder,
      const std::function<core::ExprPtr(const core::ExprPtr&)>& prepare) {
    while (computeRound(builder, prepare)) {
    }
    for (size_t i = 0; i < items_.size(); ++i) {
      VELOX_CHECK(
          !pending(i),
          "SELECT item read through an alias was not computed: {}",
          i);
    }
    substituteAll(readers_);
    return std::move(values_);
  }

 private:
  // Counts how many times each item is evaluated: by the final projection, by
  // the readers, and by the items that read it. A lateral alias refers only to
  // an earlier item, so counting from the last item covers the chains.
  void countUses() {
    const auto count = [&](const core::ExprPtr& expr) {
      forEachReference(projection_, expr, [&](size_t item) {
        VELOX_CHECK_LT(item, items_.size());
        ++numUses_[item];
      });
    };
    for (const auto& reader : readers_) {
      count(reader.expr());
    }
    for (size_t i = items_.size(); i-- > 0;) {
      if (numUses_[i] > 0) {
        count(items_[i].expr());
      }
    }
  }

  bool pending(size_t item) const {
    return numUses_[item] > numOwnUses_ && values_[item] == nullptr;
  }

  bool readsPendingItem(const core::ExprPtr& expr) const {
    bool found{false};
    forEachReference(projection_, expr, [&](size_t item) {
      found = found || pending(item);
    });
    return found;
  }

  // Nested windows that read no pending item, without duplicates.
  std::vector<core::ExprPtr> readyWindows() const {
    std::vector<core::ExprPtr> windows;
    if (!belowProjection_) {
      return windows;
    }
    core::ExprSet seen;
    for (const auto* exprs : {&items_, &readers_}) {
      for (const auto& expr : *exprs) {
        for (const auto& window : nestedWindows(expr)) {
          if (!readsPendingItem(window) && seen.insert(window).second) {
            windows.push_back(window);
          }
        }
      }
    }
    return windows;
  }

  // Pending items that read no pending item and hold no nested window.
  std::vector<size_t> readyItems() const {
    std::vector<size_t> ready;
    for (size_t i = 0; i < items_.size(); ++i) {
      if (pending(i) && !readsPendingItem(items_[i].expr()) &&
          (!belowProjection_ || nestedWindows(items_[i]).empty())) {
        ready.push_back(i);
      }
    }
    return ready;
  }

  // Returns false when nothing was ready.
  bool computeRound(
      lp::PlanBuilder& builder,
      const std::function<core::ExprPtr(const core::ExprPtr&)>& prepare) {
    // Decided before anything of this round gets a value: what reads a value
    // of this round waits for the next one.
    const auto windows = readyWindows();
    const auto ready = readyItems();
    if (windows.empty() && ready.empty()) {
      return false;
    }

    // An item read once is evaluated where it is read. One read more than
    // once is computed here, unless it is a column.
    std::vector<lp::ExprApi> computed{windows.begin(), windows.end()};
    std::vector<size_t> computedItems;
    for (const auto i : ready) {
      auto expr = substitute(items_[i].expr());
      if (numUses_[i] > 1 && !isColumn(expr) && prepare != nullptr) {
        expr = prepare(expr);
      }
      if (numUses_[i] == 1 || isColumn(expr)) {
        values_[i] = std::move(expr);
      } else {
        computedItems.push_back(i);
        computed.emplace_back(std::move(expr), items_[i].alias());
      }
    }

    if (!computed.empty()) {
      builder.with(computed);
      const auto first = builder.numOutput() - computed.size();
      const auto columnAt = [&](size_t index) {
        return builder.findOrAssignOutputNameAt(first + index).toCol().expr();
      };
      for (size_t k = 0; k < windows.size(); ++k) {
        windowColumns_.emplace(windows[k], columnAt(k));
      }
      for (size_t k = 0; k < computedItems.size(); ++k) {
        values_[computedItems[k]] = columnAt(windows.size() + k);
      }
    }

    if (belowProjection_) {
      substituteAll(items_);
      substituteAll(readers_);
      for (const auto i : computedItems) {
        items_[i] = lp::ExprApi(values_[i], items_[i].alias());
      }
    }
    return true;
  }

  core::ExprPtr substitute(const core::ExprPtr& expr) const {
    if (const auto item = projection_.itemOf(expr)) {
      const auto& value = values_[item.value()];
      return value != nullptr ? value : expr;
    }
    // Inputs first: a window can match a computed one only once the
    // placeholders in it are replaced.
    auto rewritten = ExpressionPlanner::rewriteInputs(
        expr, [&](const core::ExprPtr& input) { return substitute(input); });
    if (rewritten->is(core::IExpr::Kind::kWindow)) {
      if (auto it = windowColumns_.find(rewritten);
          it != windowColumns_.end()) {
        return it->second;
      }
    }
    return rewritten;
  }

  void substituteAll(std::vector<lp::ExprApi>& exprs) const {
    for (auto& expr : exprs) {
      expr = lp::ExprApi(substitute(expr.expr()), expr.alias());
    }
  }

  const SelectProjection& projection_;
  // True for the final projection, which evaluates every item.
  const bool belowProjection_;
  std::vector<lp::ExprApi>& items_;
  std::vector<lp::ExprApi>& readers_;
  const size_t numOwnUses_;
  // How many times each item is evaluated.
  std::vector<size_t> numUses_;
  // What the readers of each referenced item read; nullptr until known.
  std::vector<core::ExprPtr> values_;
  // Column of each nested window computed so far.
  core::ExprMap<core::ExprPtr> windowColumns_;
};

} // namespace

std::vector<core::ExprPtr> SelectProjection::lower(
    Level level,
    lp::PlanBuilder& builder,
    std::vector<lp::ExprApi>& items,
    std::vector<lp::ExprApi>& readers,
    const std::function<core::ExprPtr(const core::ExprPtr&)>& prepare) const {
  return Lowering{*this, level == Level::kBelowProjection, items, readers}.run(
      builder, prepare);
}

} // namespace axiom::sql::presto
