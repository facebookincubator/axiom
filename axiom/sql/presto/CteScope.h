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

#include <folly/ScopeGuard.h>
#include <folly/container/F14Map.h>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/sql/presto/ast/AstNodesAll.h"

namespace axiom::sql::presto {

namespace lp = facebook::axiom::logical_plan;

/// Resolves CTE names while parsing one query. As the parser descends
/// through nested WITH clauses, tracks what each in-scope name refers to.
///
/// A table name resolves in order:
///   1. the recursive anchor, if inside a recursive CTE's step body;
///   2. the in-scope CTE binding;
///   3. otherwise a base table (resolved by the caller).
///
/// Bindings stack per name: a nested WITH that reuses a name pushes a
/// binding on top of the enclosing one, which reappears when the nested
/// scope is popped. That is what lets a non-recursive CTE's own body
/// resolve a same-name reference to the enclosing CTE -- the body hides
/// only its own binding.
///
/// Usage:
///   auto scope = ctes.enterScope();   // once per query body
///   ctes.bind(name, {withQuery, isRecursive});
///   if (auto hidden = ctes.hide(name)) { translate hidden.entry()'s body }
///
/// Invariant: a name is a key iff it has a binding in scope.
class CteScope {
 public:
  /// One CTE binding. 'isRecursive' is true when the body refers to itself.
  /// A recursive body is re-translated at each reference site.
  struct Entry {
    std::shared_ptr<WithQuery> withQuery;
    bool isRecursive{false};
  };

  /// Hides a name's in-scope binding for the guard's lifetime, so the
  /// CTE is invisible within its own body. The hidden binding is available
  /// through entry(). Restores the binding when the guard is destroyed. The
  /// guard is false when the name had no binding to hide.
  class [[nodiscard]] Hidden {
   public:
    Hidden() = default;

    Hidden(CteScope* owner, std::string name, Entry entry)
        : owner_{owner}, name_{std::move(name)}, entry_{std::move(entry)} {}

    Hidden(Hidden&& other) noexcept
        : owner_{std::exchange(other.owner_, nullptr)},
          name_{std::move(other.name_)},
          entry_{std::move(other.entry_)} {}

    Hidden& operator=(Hidden&& other) noexcept {
      if (this != &other) {
        restore();
        owner_ = std::exchange(other.owner_, nullptr);
        name_ = std::move(other.name_);
        entry_ = std::move(other.entry_);
      }
      return *this;
    }

    ~Hidden() {
      restore();
    }

    explicit operator bool() const {
      return owner_ != nullptr;
    }

    const Entry& entry() const {
      return entry_;
    }

   private:
    void restore() {
      if (owner_ != nullptr) {
        owner_->bindings_[name_].push_back(std::move(entry_));
        owner_ = nullptr;
      }
    }

    CteScope* owner_{nullptr};
    std::string name_;
    Entry entry_;
  };

  /// Saves all bindings and anchors, and restores them when the guard dies.
  /// Use one per query body so a nested WITH cannot leak outward.
  [[nodiscard]] auto enterScope() {
    return folly::makeGuard(
        [this, bindings = bindings_, anchors = anchors_]() mutable {
          bindings_ = std::move(bindings);
          anchors_ = std::move(anchors);
        });
  }

  /// Registers a new CTE definition for 'name'. If an outer CTE already uses
  /// that name, this definition takes precedence until the current query scope
  /// ends.
  /// A recursive CTE of the same name is normally matched before bindings, so
  /// if one is being expanded, its anchor is cleared to let this binding be
  /// found instead.
  void bind(std::string name, Entry entry) {
    anchors_.erase(name);
    bindings_[std::move(name)].push_back(std::move(entry));
  }

  /// Hides 'name's in-scope binding and returns it as a Hidden guard. The guard
  /// is false when 'name' has no binding in scope.
  [[nodiscard]] Hidden hide(const std::string& name) {
    auto it = bindings_.find(name);
    if (it == bindings_.end()) {
      return Hidden{};
    }
    auto entry = std::move(it->second.back());
    it->second.pop_back();
    if (it->second.empty()) {
      bindings_.erase(it);
    }
    return Hidden{this, name, std::move(entry)};
  }

  /// Returns the anchor plan node for a recursive CTE currently being expanded,
  /// or nullptr when 'name' is not such a CTE.
  const lp::PlanBuilder* anchorFor(const std::string& name) const {
    auto it = anchors_.find(name);
    return it == anchors_.end() ? nullptr : it->second;
  }

  bool hasAnchors() const {
    return !anchors_.empty();
  }

  /// Marks 'name' as a recursive CTE being expanded, with 'anchor' as its
  /// loop-back point, so references to it resolve as self-references. The
  /// record is removed when the returned guard dies.
  [[nodiscard]] auto pushAnchor(
      std::string name,
      const lp::PlanBuilder* anchor) {
    anchors_[name] = anchor;
    return folly::makeGuard(
        [this, name = std::move(name)]() mutable { anchors_.erase(name); });
  }

  /// Hides all anchors for as long as the returned guard lives. An independent
  /// query, such as a view body, then does not resolve against the caller's
  /// in-flight recursive anchors. Bindings stay visible.
  [[nodiscard]] auto suppressAnchors() {
    auto saved = std::move(anchors_);
    anchors_.clear();
    return folly::makeGuard([this, saved = std::move(saved)]() mutable {
      anchors_ = std::move(saved);
    });
  }

 private:
  using BindingMap = folly::F14FastMap<std::string, std::vector<Entry>>;
  using AnchorMap = folly::F14FastMap<std::string, const lp::PlanBuilder*>;

  BindingMap bindings_;
  AnchorMap anchors_;
};

} // namespace axiom::sql::presto
