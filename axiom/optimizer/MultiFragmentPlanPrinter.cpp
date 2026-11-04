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

#include "axiom/optimizer/MultiFragmentPlanPrinter.h"

#include <cctype>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <folly/container/F14Set.h>
#include "velox/core/FixedPointPlanNodes.h"
#include "velox/core/PlanNode.h"

namespace facebook::axiom::optimizer {

namespace {

// The estimate fields of a node's prediction. See toSummaryJson.
folly::dynamic toJson(const NodePrediction& prediction) {
  folly::dynamic estimate = folly::dynamic::object;
  if (prediction.numRawInputRows.has_value()) {
    estimate["rawInputRows"] =
        static_cast<int64_t>(*prediction.numRawInputRows);
  }
  if (prediction.numRawInputBytesPerRow.has_value()) {
    estimate["rawInputBytesPerRow"] = *prediction.numRawInputBytesPerRow;
  }
  if (prediction.numSplits.has_value()) {
    estimate["splits"] = static_cast<int64_t>(*prediction.numSplits);
  }
  estimate["outputRows"] = prediction.cardinality;
  if (prediction.numOutputBytesPerRow.has_value()) {
    estimate["outputBytesPerRow"] = *prediction.numOutputBytesPerRow;
  }
  return estimate;
}

// How a fragment's output reaches its consumer: hash, gather, broadcast or
// arbitrary.
std::string_view distribution(const velox::core::Stage& producer) {
  const auto* output = producer.plan->as<velox::core::PartitionedOutputNode>();
  VELOX_CHECK_NOT_NULL(
      output, "A fragment read by an exchange ends in PartitionedOutput");
  if (output->isBroadcast()) {
    return "broadcast";
  }
  if (output->isArbitrary()) {
    return "arbitrary";
  }
  return output->keys().empty() || output->numPartitions() == 1 ? "gather"
                                                                : "hash";
}

// The word an exchange line starts with: Shuffle for hash partitioning,
// otherwise Gather, Broadcast or Arbitrary.
std::string exchangeWord(const velox::core::Stage& producer) {
  const auto kind = distribution(producer);
  if (kind == "hash") {
    return "Shuffle";
  }
  std::string word{kind};
  word[0] = static_cast<char>(std::toupper(word[0]));
  return word;
}

struct SummaryContext : velox::core::PlanNodeVisitorContext {
  const velox::core::Stage* fragment{nullptr};
  // The estimate of the nearest node above that the summary leaves out: an
  // optimizer operator can lower to a control node under a projection, and
  // the estimate is keyed by the outermost node.
  const NodePrediction* pending{nullptr};
  // Text: the line's depth. JSON: the array the node goes into.
  size_t depth{0};
  folly::dynamic* nodes{nullptr};
};

// Walks a fragment's plan and hands each control node to the hook for its
// kind; other nodes are skipped. Every Velox node type has an overload, so a
// new one has to be classified before this compiles.
class ControlNodeVisitor : public velox::core::PlanNodeVisitor {
 public:
  ControlNodeVisitor(
      const MultiFragmentPlan& plan,
      const NodePredictionMap* prediction)
      : prediction_(prediction) {
    for (const auto& fragment : plan.stages()) {
      fragments_.emplace(fragment.id, &fragment);
    }
  }

  using Ctx = velox::core::PlanNodeVisitorContext;

  void visit(const velox::core::AggregationNode& node, Ctx& ctx)
      const override {
    visitAggregation(node, estimate(node, ctx), summary(ctx));
  }

  void visit(const velox::core::ArrowStreamNode& node, Ctx& ctx)
      const override {
    skip(node, ctx);
  }

  void visit(const velox::core::AssignUniqueIdNode& node, Ctx& ctx)
      const override {
    skip(node, ctx);
  }

  void visit(const velox::core::EnforceSingleRowNode& node, Ctx& ctx)
      const override {
    visitOther(node, std::nullopt, estimate(node, ctx), summary(ctx));
  }

  void visit(const velox::core::ExchangeNode& node, Ctx& ctx) const override {
    exchange(node, ctx);
  }

  void visit(const velox::core::ExpandNode& node, Ctx& ctx) const override {
    skip(node, ctx);
  }

  void visit(const velox::core::FilterNode& node, Ctx& ctx) const override {
    skip(node, ctx);
  }

  void visit(const velox::core::GroupIdNode& node, Ctx& ctx) const override {
    visitOther(node, std::nullopt, estimate(node, ctx), summary(ctx));
  }

  void visit(const velox::core::HashJoinNode& node, Ctx& ctx) const override {
    visitJoin(
        node,
        "HashJoin",
        node.joinType(),
        node.leftKeys().size(),
        estimate(node, ctx),
        summary(ctx));
  }

  void visit(const velox::core::IndexLookupJoinNode& node, Ctx& ctx)
      const override {
    visitJoin(
        node,
        "IndexLookupJoin",
        node.joinType(),
        node.leftKeys().size(),
        estimate(node, ctx),
        summary(ctx));
  }

  void visit(const velox::core::LimitNode& node, Ctx& ctx) const override {
    visitOther(node, node.count(), estimate(node, ctx), summary(ctx));
  }

  void visit(const velox::core::LocalMergeNode& node, Ctx& ctx) const override {
    localExchange(node, ctx);
  }

  void visit(const velox::core::LocalPartitionNode& node, Ctx& ctx)
      const override {
    localExchange(node, ctx);
  }

  void visit(const velox::core::MarkDistinctNode& node, Ctx& ctx)
      const override {
    visitOther(node, std::nullopt, estimate(node, ctx), summary(ctx));
  }

  void visit(const velox::core::EnforceDistinctNode& node, Ctx& ctx)
      const override {
    visitOther(node, std::nullopt, estimate(node, ctx), summary(ctx));
  }

  void visit(const velox::core::MarkSortedNode& node, Ctx& ctx) const override {
    skip(node, ctx);
  }

  void visit(const velox::core::MergeExchangeNode& node, Ctx& ctx)
      const override {
    exchange(node, ctx);
  }

  void visit(const velox::core::MergeJoinNode& node, Ctx& ctx) const override {
    visitJoin(
        node,
        "MergeJoin",
        node.joinType(),
        node.leftKeys().size(),
        estimate(node, ctx),
        summary(ctx));
  }

  void visit(const velox::core::NestedLoopJoinNode& node, Ctx& ctx)
      const override {
    visitJoin(
        node,
        "NestedLoopJoin",
        node.joinType(),
        0,
        estimate(node, ctx),
        summary(ctx));
  }

  void visit(const velox::core::SpatialJoinNode& node, Ctx& ctx)
      const override {
    visitJoin(
        node,
        "SpatialJoin",
        node.joinType(),
        0,
        estimate(node, ctx),
        summary(ctx));
  }

  void visit(const velox::core::VectorSearchJoinNode& node, Ctx& ctx)
      const override {
    visitJoin(
        node,
        "VectorSearchJoin",
        node.joinType(),
        node.leftKeys().size(),
        estimate(node, ctx),
        summary(ctx));
  }

  void visit(const velox::core::OrderByNode& node, Ctx& ctx) const override {
    visitOther(node, std::nullopt, estimate(node, ctx), summary(ctx));
  }

  void visit(const velox::core::PartitionedOutputNode& node, Ctx& ctx)
      const override {
    skip(node, ctx);
  }

  void visit(const velox::core::ProjectNode& node, Ctx& ctx) const override {
    skip(node, ctx);
  }

  void visit(const velox::core::ParallelProjectNode& node, Ctx& ctx)
      const override {
    skip(node, ctx);
  }

  void visit(const velox::core::RowNumberNode& node, Ctx& ctx) const override {
    visitOther(node, std::nullopt, estimate(node, ctx), summary(ctx));
  }

  void visit(const velox::core::TableScanNode& node, Ctx& ctx) const override {
    visitScan(node, estimate(node, ctx), summary(ctx));
  }

  void visit(const velox::core::TableWriteNode& node, Ctx& ctx) const override {
    visitOther(node, std::nullopt, estimate(node, ctx), summary(ctx));
  }

  void visit(const velox::core::TableWriteMergeNode& node, Ctx& ctx)
      const override {
    skip(node, ctx);
  }

  void visit(const velox::core::TopNNode& node, Ctx& ctx) const override {
    visitOther(node, node.count(), estimate(node, ctx), summary(ctx));
  }

  void visit(const velox::core::TopNRowNumberNode& node, Ctx& ctx)
      const override {
    visitOther(node, std::nullopt, estimate(node, ctx), summary(ctx));
  }

  void visit(const velox::core::TraceScanNode& node, Ctx& ctx) const override {
    skip(node, ctx);
  }

  void visit(const velox::core::UnnestNode& node, Ctx& ctx) const override {
    visitOther(node, std::nullopt, estimate(node, ctx), summary(ctx));
  }

  void visit(const velox::core::ValuesNode& node, Ctx& ctx) const override {
    visitOther(node, std::nullopt, estimate(node, ctx), summary(ctx));
  }

  void visit(const velox::core::WindowNode& node, Ctx& ctx) const override {
    visitWindow(node, estimate(node, ctx), summary(ctx));
  }

  void visit(const velox::core::MixedUnionNode& node, Ctx& ctx) const override {
    visitUnion(node, estimate(node, ctx), summary(ctx));
  }

  void visit(const velox::core::FixedPointNode& node, Ctx& ctx) const override {
    skip(node, ctx);
  }

  void visit(const velox::core::StateSourceNode& node, Ctx& ctx)
      const override {
    skip(node, ctx);
  }

  void visit(const velox::core::StateHashJoinNode& node, Ctx& ctx)
      const override {
    skip(node, ctx);
  }

  void visit(const velox::core::PlanNode& node, Ctx& ctx) const override {
    skip(node, ctx);
  }

 protected:
  // Hooks for the control nodes. 'estimate' is the optimizer's, or null.
  virtual void visitScan(
      const velox::core::TableScanNode& node,
      const NodePrediction* estimate,
      SummaryContext& ctx) const = 0;

  virtual void visitJoin(
      const velox::core::PlanNode& node,
      std::string_view algorithm,
      velox::core::JoinType joinType,
      size_t numKeys,
      const NodePrediction* estimate,
      SummaryContext& ctx) const = 0;

  virtual void visitAggregation(
      const velox::core::AggregationNode& node,
      const NodePrediction* estimate,
      SummaryContext& ctx) const = 0;

  // An exchange reading 'producers', usually one fragment; reading several
  // makes it a union of them.
  virtual void visitExchange(
      const velox::core::PlanNode& node,
      const std::vector<const velox::core::Stage*>& producers,
      const NodePrediction* estimate,
      SummaryContext& ctx) const = 0;

  // A union of the node's inputs.
  virtual void visitUnion(
      const velox::core::PlanNode& node,
      const NodePrediction* estimate,
      SummaryContext& ctx) const = 0;

  virtual void visitWindow(
      const velox::core::WindowNode& node,
      const NodePrediction* estimate,
      SummaryContext& ctx) const = 0;

  // Any other control node, by name; 'count' is a TopN's or Limit's row count.
  virtual void visitOther(
      const velox::core::PlanNode& node,
      std::optional<int64_t> count,
      const NodePrediction* estimate,
      SummaryContext& ctx) const = 0;

  const velox::core::Stage& fragment(int32_t id) const {
    return *fragments_.at(id);
  }

 private:
  static SummaryContext& summary(Ctx& ctx) {
    return static_cast<SummaryContext&>(ctx);
  }

  const NodePrediction* estimate(const velox::core::PlanNode& node, Ctx& ctx)
      const {
    if (prediction_ != nullptr) {
      if (auto it = prediction_->find(node.id()); it != prediction_->end()) {
        return &it->second;
      }
    }
    return summary(ctx).pending;
  }

  void skip(const velox::core::PlanNode& node, Ctx& ctx) const {
    const auto* saved = summary(ctx).pending;
    summary(ctx).pending = estimate(node, ctx);
    visitSources(node, ctx);
    summary(ctx).pending = saved;
  }

  // A local exchange gathering two or more inputs is a union; one with a
  // single input only repartitions it.
  void localExchange(const velox::core::PlanNode& node, Ctx& ctx) const {
    if (node.sources().size() > 1) {
      visitUnion(node, estimate(node, ctx), summary(ctx));
    } else {
      skip(node, ctx);
    }
  }

  void exchange(const velox::core::PlanNode& node, Ctx& ctx) const {
    std::vector<const velox::core::Stage*> producers;
    for (const auto& input : summary(ctx).fragment->inputs) {
      if (input.exchangeId == node.id()) {
        producers.push_back(&fragment(input.producerStageId));
      }
    }
    visitExchange(node, producers, estimate(node, ctx), summary(ctx));
  }

  const NodePredictionMap* prediction_;
  folly::F14FastMap<int32_t, const velox::core::Stage*> fragments_;
};

folly::dynamic jsonNode(
    const velox::core::PlanNode& node,
    std::string_view kind,
    const NodePrediction* estimate) {
  folly::dynamic result =
      folly::dynamic::object("nodeId", node.id())("kind", kind);
  if (estimate != nullptr) {
    result["estimate"] = toJson(*estimate);
  }
  return result;
}

// Collects each fragment's control nodes, nested, for toSummaryJson.
class JsonTreeVisitor : public ControlNodeVisitor {
 public:
  using ControlNodeVisitor::ControlNodeVisitor;

  folly::dynamic tree(const velox::core::Stage& fragment) const {
    folly::dynamic nodes = folly::dynamic::array;
    SummaryContext ctx;
    ctx.fragment = &fragment;
    ctx.nodes = &nodes;
    fragment.plan->accept(*this, ctx);
    return nodes;
  }

 protected:
  void visitScan(
      const velox::core::TableScanNode& node,
      const NodePrediction* estimate,
      SummaryContext& ctx) const override {
    auto entry = jsonNode(node, "Scan", estimate);
    entry["table"] = node.tableHandle()->name();
    add(std::move(entry), node, ctx);
  }

  void visitJoin(
      const velox::core::PlanNode& node,
      std::string_view algorithm,
      velox::core::JoinType joinType,
      size_t numKeys,
      const NodePrediction* estimate,
      SummaryContext& ctx) const override {
    auto entry = jsonNode(node, "Join", estimate);
    entry["algorithm"] = algorithm;
    entry["joinType"] = velox::core::JoinTypeName::toName(joinType);
    entry["numJoinKeys"] = static_cast<int64_t>(numKeys);
    add(std::move(entry), node, ctx);
  }

  void visitAggregation(
      const velox::core::AggregationNode& node,
      const NodePrediction* estimate,
      SummaryContext& ctx) const override {
    auto entry = jsonNode(node, "Agg", estimate);
    entry["step"] = velox::core::AggregationNode::toName(node.step());
    entry["numGroupingKeys"] = static_cast<int64_t>(node.groupingKeys().size());
    add(std::move(entry), node, ctx);
  }

  void visitExchange(
      const velox::core::PlanNode& node,
      const std::vector<const velox::core::Stage*>& producers,
      const NodePrediction* estimate,
      SummaryContext& ctx) const override {
    folly::dynamic legs = folly::dynamic::array;
    for (const auto* producer : producers) {
      auto leg = jsonNode(
          node, "Exchange", producers.size() == 1 ? estimate : nullptr);
      leg["distribution"] = distribution(*producer);
      leg["producerFragmentId"] = producer->id;
      legs.push_back(std::move(leg));
    }
    if (producers.size() == 1) {
      ctx.nodes->push_back(std::move(legs[0]));
      return;
    }
    auto entry = jsonNode(node, "UnionAll", estimate);
    entry["children"] = std::move(legs);
    ctx.nodes->push_back(std::move(entry));
  }

  void visitUnion(
      const velox::core::PlanNode& node,
      const NodePrediction* estimate,
      SummaryContext& ctx) const override {
    add(jsonNode(node, "UnionAll", estimate), node, ctx);
  }

  void visitWindow(
      const velox::core::WindowNode& node,
      const NodePrediction* estimate,
      SummaryContext& ctx) const override {
    auto entry = jsonNode(node, "Window", estimate);
    entry["numPartitionKeys"] =
        static_cast<int64_t>(node.partitionKeys().size());
    entry["numOrderKeys"] = static_cast<int64_t>(node.sortingKeys().size());
    add(std::move(entry), node, ctx);
  }

  void visitOther(
      const velox::core::PlanNode& node,
      std::optional<int64_t> count,
      const NodePrediction* estimate,
      SummaryContext& ctx) const override {
    auto entry = jsonNode(node, node.name(), estimate);
    if (count.has_value()) {
      entry["count"] = count.value();
    }
    add(std::move(entry), node, ctx);
  }

 private:
  // Adds 'entry', the JSON of 'node', with the control nodes under it.
  void add(
      folly::dynamic entry,
      const velox::core::PlanNode& node,
      SummaryContext& ctx) const {
    folly::dynamic children = folly::dynamic::array;
    SummaryContext childCtx;
    childCtx.fragment = ctx.fragment;
    childCtx.nodes = &children;
    visitSources(node, childCtx);
    if (!children.empty()) {
      entry["children"] = std::move(children);
    }
    ctx.nodes->push_back(std::move(entry));
  }
};

// 1234567 as "1,234,567".
std::string withCommas(int64_t value) {
  auto digits = std::to_string(value < 0 ? -value : value);
  for (int i = static_cast<int>(digits.size()) - 3; i > 0; i -= 3) {
    digits.insert(i, ",");
  }
  return value < 0 ? "-" + digits : digits;
}

// A fragment's type and, where the plan fixes it, how many workers run it,
// e.g. `FIXED, 4 workers`. A SOURCE fragment runs on as many workers as its
// splits need, which planning does not decide.
std::string fragmentKind(const velox::core::Stage& fragment) {
  const auto type = velox::core::StageTypeName::toName(fragment.type);
  if (fragment.type == velox::core::StageType::kSingle) {
    return fmt::format("{}, 1 worker", type);
  }
  if (fragment.numRemotePartitions.has_value()) {
    return fmt::format(
        "{}, {} workers", type, fragment.numRemotePartitions.value());
  }
  return std::string{type};
}

std::string keys(size_t numKeys) {
  return fmt::format("{} key{}", numKeys, numKeys == 1 ? "" : "s");
}

// Writes the plan as one indented tree for toSummaryText. At an exchange the
// tree continues into the fragment it reads.
class TextVisitor : public ControlNodeVisitor {
 public:
  using ControlNodeVisitor::ControlNodeVisitor;

  void write(const velox::core::Stage& fragment, size_t depth) const {
    SummaryContext ctx;
    ctx.fragment = &fragment;
    ctx.depth = depth;
    fragment.plan->accept(*this, ctx);
  }

  std::string text() const {
    return fmt::format("{}\n", fmt::join(lines_, "\n"));
  }

 protected:
  void visitScan(
      const velox::core::TableScanNode& node,
      const NodePrediction* estimate,
      SummaryContext& ctx) const override {
    add(fmt::format("Scan {}", node.tableHandle()->name()), estimate, ctx);
    writeSources(node, ctx);
  }

  void visitJoin(
      const velox::core::PlanNode& node,
      std::string_view algorithm,
      velox::core::JoinType joinType,
      size_t numKeys,
      const NodePrediction* estimate,
      SummaryContext& ctx) const override {
    add(fmt::format(
            "{} ({}){}",
            algorithm,
            velox::core::JoinTypeName::toName(joinType),
            numKeys == 0 ? "" : " " + keys(numKeys)),
        estimate,
        ctx);
    writeSources(node, ctx);
  }

  void visitAggregation(
      const velox::core::AggregationNode& node,
      const NodePrediction* estimate,
      SummaryContext& ctx) const override {
    const auto numKeys = node.groupingKeys().size();
    add(fmt::format(
            "Agg ({}) {}",
            velox::core::AggregationNode::toName(node.step()),
            numKeys == 0 ? "global" : keys(numKeys)),
        estimate,
        ctx);
    writeSources(node, ctx);
  }

  void visitExchange(
      const velox::core::PlanNode& /*node*/,
      const std::vector<const velox::core::Stage*>& producers,
      const NodePrediction* estimate,
      SummaryContext& ctx) const override {
    size_t depth = ctx.depth;
    if (producers.size() > 1) {
      add("UnionAll", estimate, ctx);
      ++depth;
    }
    for (const auto* producer : producers) {
      SummaryContext legCtx;
      legCtx.fragment = ctx.fragment;
      legCtx.depth = depth;
      add(fmt::format(
              "{} F{} ({}) → F{}",
              exchangeWord(*producer),
              producer->id,
              fragmentKind(*producer),
              ctx.fragment->id),
          producers.size() == 1 ? estimate : nullptr,
          legCtx);
      write(*producer, depth + 1);
    }
  }

  void visitUnion(
      const velox::core::PlanNode& node,
      const NodePrediction* estimate,
      SummaryContext& ctx) const override {
    add("UnionAll", estimate, ctx);
    writeSources(node, ctx);
  }

  void visitWindow(
      const velox::core::WindowNode& node,
      const NodePrediction* estimate,
      SummaryContext& ctx) const override {
    add(fmt::format(
            "Window partition {}, order {}",
            keys(node.partitionKeys().size()),
            keys(node.sortingKeys().size())),
        estimate,
        ctx);
    writeSources(node, ctx);
  }

  void visitOther(
      const velox::core::PlanNode& node,
      std::optional<int64_t> count,
      const NodePrediction* estimate,
      SummaryContext& ctx) const override {
    add(count.has_value() ? fmt::format("{} {}", node.name(), count.value())
                          : std::string{node.name()},
        estimate,
        ctx);
    writeSources(node, ctx);
  }

 private:
  // Adds the line 'text' at the context's depth, and the estimate's line
  // indented twice as deep as a child.
  void add(
      const std::string& text,
      const NodePrediction* estimate,
      const SummaryContext& ctx) const {
    const std::string indent(2 * ctx.depth, ' ');
    lines_.push_back(indent + text);
    if (estimate != nullptr) {
      lines_.push_back(
          fmt::format(
              "{}    Estimate: {} rows",
              indent,
              withCommas(std::llround(estimate->cardinality))));
    }
  }

  void writeSources(const velox::core::PlanNode& node, SummaryContext& ctx)
      const {
    SummaryContext childCtx;
    childCtx.fragment = ctx.fragment;
    childCtx.depth = ctx.depth + 1;
    visitSources(node, childCtx);
  }

  mutable std::vector<std::string> lines_;
};

// Renders 'plan', with estimates from 'prediction' unless it is null.
folly::dynamic toGraph(
    const MultiFragmentPlan& plan,
    const NodePredictionMap* prediction) {
  // A StageInput names the producer, so invert the edges once to find each
  // fragment's consumer.
  folly::F14FastMap<int32_t, int32_t> consumerOf;
  for (const auto& fragment : plan.stages()) {
    for (const auto& input : fragment.inputs) {
      consumerOf[input.producerStageId] = fragment.id;
    }
  }

  const JsonTreeVisitor treeVisitor{plan, prediction};
  folly::dynamic fragments = folly::dynamic::array;
  for (const auto& fragment : plan.stages()) {
    folly::dynamic entry = folly::dynamic::object;
    entry["id"] = fragment.id;
    entry["type"] =
        std::string{velox::core::StageTypeName::toName(fragment.type)};
    if (fragment.numRemotePartitions.has_value()) {
      entry["numRemotePartitions"] = fragment.numRemotePartitions.value();
    }

    entry["tree"] = treeVisitor.tree(fragment);

    if (const auto* output =
            fragment.plan->as<velox::core::PartitionedOutputNode>()) {
      folly::dynamic outputEntry = folly::dynamic::object;
      outputEntry["nodeId"] = output->id();
      if (auto it = consumerOf.find(fragment.id); it != consumerOf.end()) {
        outputEntry["consumerFragmentId"] = it->second;
      }
      entry["output"] = std::move(outputEntry);
    }

    fragments.push_back(std::move(entry));
  }

  return folly::dynamic::object("fragments", std::move(fragments));
}

std::string toText(
    const MultiFragmentPlan& plan,
    const NodePredictionMap* prediction) {
  folly::F14FastSet<int32_t> producers;
  for (const auto& fragment : plan.stages()) {
    for (const auto& input : fragment.inputs) {
      producers.insert(input.producerStageId);
    }
  }

  const TextVisitor visitor{plan, prediction};
  for (const auto& fragment : plan.stages()) {
    if (!producers.contains(fragment.id)) {
      visitor.write(fragment, 0);
    }
  }
  return visitor.text();
}

} // namespace

std::string MultiFragmentPlanPrinter::toSummaryText(
    const MultiFragmentPlan& plan) {
  return toText(plan, nullptr);
}

std::string MultiFragmentPlanPrinter::toSummaryText(
    const MultiFragmentPlan& plan,
    const NodePredictionMap& prediction) {
  return toText(plan, &prediction);
}

folly::dynamic MultiFragmentPlanPrinter::toSummaryJson(
    const MultiFragmentPlan& plan,
    const NodePredictionMap& prediction) {
  return toGraph(plan, &prediction);
}

folly::dynamic MultiFragmentPlanPrinter::toSummaryJson(
    const MultiFragmentPlan& plan) {
  return toGraph(plan, nullptr);
}

} // namespace facebook::axiom::optimizer
