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

#include "axiom/optimizer/v2/LimitAndOrderPass.h"

#include <algorithm>
#include <limits>
#include <optional>
#include <utility>

#include "axiom/optimizer/v2/NodeRewriter.h"

namespace facebook::axiom::optimizer::v2 {
namespace {

// A count of this value means "no row bound" (a pure OFFSET).
constexpr int64_t kNoCount = std::numeric_limits<int64_t>::max();

// A row bound: skip 'offset' rows, then emit up to 'count' (kNoCount means
// unbounded).
struct LimitBound {
  int64_t offset;
  int64_t count;
};

// Composes two bounds: 'inner' is applied first, then 'outer' on its output.
// inner yields 'inner.count' rows after skipping 'inner.offset'; outer skips
// 'outer.offset' more of those and takes 'outer.count'.
LimitBound collapse(const LimitBound& outer, const LimitBound& inner) {
  const int64_t offset = inner.offset + outer.offset;
  int64_t count;
  if (inner.count == kNoCount) {
    // Inner unbounded: outer's bound governs the rows after outer.offset.
    count = outer.count;
  } else {
    const int64_t afterOuterOffset =
        std::max<int64_t>(0, inner.count - outer.offset);
    count = outer.count == kNoCount ? afterOuterOffset
                                    : std::min(outer.count, afterOuterOffset);
  }
  return {offset, count};
}

// State threaded top-down.
struct LimitContext {
  // The row bound the consumer imposes on the current node's output, placed as
  // low as the per-node rules allow.
  std::optional<LimitBound> pending;

  // Whether the row order produced at the current node is preserved all the
  // way to the query output. A bare Sort reached with this false is dropped.
  bool orderPreserved{true};
};

// Top-down rewriter that threads the pending row bound and the
// order-preservation flag (see `LimitContext`) and applies the per-node
// limit/order rules described on `LimitAndOrderPass`.
class LimitAndOrderRewriter : public NodeRewriter<LimitContext> {
 public:
  using NodeRewriter::NodeRewriter;

 protected:
  // A Limit folds into the pending bound (collapse) and is dropped here; the
  // bound flows down to be fused into a Sort/TopN or materialized at a barrier.
  NodeCP rewriteLimit(const Limit* node, LimitContext& context) override {
    const LimitBound here{node->offset(), node->count()};
    const LimitBound bound =
        context.pending ? collapse(*context.pending, here) : here;
    if (bound.count == 0) {
      return builder().makeEmptyValues(node->outputColumns());
    }
    context.pending = bound;
    return rewrite(node->input(), context);
  }

  // A bounded pending bound fuses into a TopN. With no bound, a Sort whose
  // order is not observed (orderPreserved == false) is dropped — this covers a
  // Sort feeding an order-non-preserving operator and Sort-over-Sort. An
  // unbounded bound (pure OFFSET) keeps the Sort (the offset selects specific
  // sorted rows) under a materialized Limit. Nothing pushes below a Sort.
  NodeCP rewriteSort(const Sort* node, LimitContext& context) override {
    const auto pending = std::exchange(context.pending, std::nullopt);
    const bool dropSort = !pending && !context.orderPreserved;
    // A Sort establishes its own order, so its input's order is not preserved.
    context.orderPreserved = false;
    const NodeCP newInput = rewrite(node->input(), context);
    if (pending && pending->count != kNoCount) {
      return builder().make<TopN>(
          {newInput,
           node->orderKeys(),
           node->orderTypes(),
           pending->offset,
           pending->count});
    }
    if (dropSort) {
      return newInput;
    }
    const NodeCP sort = newInput == node->input()
        ? node
        : builder().make<Sort>(
              {newInput, node->orderKeys(), node->orderTypes()});
    return materialize(pending, sort);
  }

  // A pending bound folds into the TopN's bound; nothing pushes below a TopN.
  NodeCP rewriteTopN(const TopN* node, LimitContext& context) override {
    const auto pending = std::exchange(context.pending, std::nullopt);
    // A TopN establishes its own order, so its input's order is not preserved.
    context.orderPreserved = false;
    const NodeCP newInput = rewrite(node->input(), context);
    if (pending) {
      const LimitBound folded =
          collapse(*pending, {node->offset(), node->count()});
      if (folded.count == 0) {
        return builder().makeEmptyValues(node->outputColumns());
      }
      return builder().make<TopN>(
          {newInput,
           node->orderKeys(),
           node->orderTypes(),
           folded.offset,
           folded.count});
    }
    if (newInput == node->input()) {
      return node;
    }
    return builder().make<TopN>(
        {newInput,
         node->orderKeys(),
         node->orderTypes(),
         node->offset(),
         node->count()});
  }

  // UnionAll: copy a bounded bound into every leg (each leg needs at most
  // offset + count rows) and keep the original bound above the union.
  NodeCP rewriteUnionAll(const UnionAll* node, LimitContext& context) override {
    const auto pending = std::exchange(context.pending, std::nullopt);
    std::optional<LimitBound> legBound;
    if (pending && pending->count != kNoCount) {
      legBound = LimitBound{0, pending->offset + pending->count};
    }

    NodeVector newInputs;
    newInputs.reserve(node->inputs().size());
    bool changed = false;
    for (const NodeCP input : node->inputs()) {
      LimitContext legContext;
      legContext.pending = legBound;
      // UnionAll does not preserve any leg's row order.
      legContext.orderPreserved = false;
      const NodeCP newInput = rewrite(input, legContext);
      changed |= newInput != input;
      newInputs.push_back(newInput);
    }

    const NodeCP newUnion = changed
        ? builder().make<UnionAll>(
              {std::move(newInputs), node->legColumns(), node->outputColumns()})
        : node;
    return materialize(pending, newUnion);
  }

  // EnforceSingleRow yields <=1 row and throws if its input has more. A bound
  // with offset 0 keeps that row, so it is redundant and dropped. A positive
  // offset yields zero rows, but the node must still run to enforce its
  // single-row check, so the bound is materialized above it rather than
  // replacing the node (which would skip the check).
  NodeCP rewriteEnforceSingleRow(
      const EnforceSingleRow* node,
      LimitContext& context) override {
    const auto pending = std::exchange(context.pending, std::nullopt);
    const auto bound =
        (pending && pending->offset > 0) ? pending : std::nullopt;
    return materialize(
        bound, NodeRewriter::rewriteEnforceSingleRow(node, context));
  }

  // Barriers: a bound cannot move below them, so materialize it above and
  // rewrite the inputs with no pending bound. 'preservesOrder' says whether the
  // node passes its input's order through (drives the bare-Sort drop below).
  //
  // The multi-input barriers (Join, Apply) delegate to the base rewriter, which
  // rewrites all inputs with this one shared context. That is safe because the
  // macro clears `pending` (via the std::exchange below) and sets
  // `orderPreserved` false *before* delegating, so no input is rewritten with
  // the bound and none re-enables order preservation.
#define LIMIT_BARRIER(NodeT, RewriteMethod, preservesOrder)                  \
  NodeCP RewriteMethod(const NodeT* node, LimitContext& context) override {  \
    const auto pending = std::exchange(context.pending, std::nullopt);       \
    if (!(preservesOrder)) {                                                 \
      context.orderPreserved = false;                                        \
    }                                                                        \
    return materialize(pending, NodeRewriter::RewriteMethod(node, context)); \
  }

  LIMIT_BARRIER(Scan, rewriteScan, true)
  LIMIT_BARRIER(Values, rewriteValues, true)
  LIMIT_BARRIER(Filter, rewriteFilter, true)
  LIMIT_BARRIER(Aggregate, rewriteAggregate, false)
  LIMIT_BARRIER(GroupId, rewriteGroupId, false)
  LIMIT_BARRIER(Unnest, rewriteUnnest, false)
  LIMIT_BARRIER(Join, rewriteJoin, false)
  LIMIT_BARRIER(Window, rewriteWindow, false)
  LIMIT_BARRIER(Apply, rewriteApply, false)
  LIMIT_BARRIER(EnforceDistinct, rewriteEnforceDistinct, false)
  LIMIT_BARRIER(TopNRowNumber, rewriteTopNRowNumber, false)
  LIMIT_BARRIER(Exchange, rewriteExchange, false)

#undef LIMIT_BARRIER

  // Project, AssignUniqueId, and MarkDistinct are row-preserving (1:1), so the
  // base rewrites — which thread the pending bound into the input — push the
  // bound below them unchanged.

 private:
  // Wraps 'node' in a Limit carrying the bound, or returns it unchanged when
  // there is no bound.
  NodeCP materialize(const std::optional<LimitBound>& pending, NodeCP node) {
    if (!pending) {
      return node;
    }
    return builder().make<Limit>({node, pending->offset, pending->count});
  }
};

} // namespace

NodeCP LimitAndOrderPass::run(NodeCP root, Builder& builder) {
  LimitContext context;
  return LimitAndOrderRewriter(builder).rewrite(root, context);
}

} // namespace facebook::axiom::optimizer::v2
