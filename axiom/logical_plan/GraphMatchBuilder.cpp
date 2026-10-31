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

#include "axiom/logical_plan/GraphMatchBuilder.h"

#include <set>

#include <folly/container/F14Set.h>

#include "axiom/logical_plan/NameMappings.h"

namespace facebook::axiom::logical_plan {

using GraphMatchBuilder = PlanBuilder::GraphMatchBuilder;

GraphMatchBuilder PlanBuilder::startGraphMatch(PropertyGraphPtr graph) {
  VELOX_USER_CHECK_NULL(node_, "Graph match node must be the leaf node");
  VELOX_USER_CHECK_NOT_NULL(graph, "Graph match has no graph");
  return GraphMatchBuilder(*this, std::move(graph));
}

GraphMatchBuilder::GraphMatchBuilder(
    PlanBuilder& builder,
    PropertyGraphPtr graph)
    : builder_{builder}, graph_{std::move(graph)} {}

GraphMatchBuilder& GraphMatchBuilder::path(std::string name) {
  return path(std::move(name), GraphPathSelector{}, GraphPathMode::kWalk);
}

GraphMatchBuilder& GraphMatchBuilder::path(
    std::string name,
    GraphPathSelector selector) {
  return path(std::move(name), selector, GraphPathMode::kWalk);
}

GraphMatchBuilder& GraphMatchBuilder::path(
    std::string name,
    GraphPathMode mode) {
  return path(std::move(name), GraphPathSelector{}, mode);
}

GraphMatchBuilder& GraphMatchBuilder::path(
    std::string name,
    GraphPathSelector selector,
    GraphPathMode mode) {
  flush();
  VELOX_USER_CHECK(
      subpaths_.empty(), "A path cannot start inside an open subpath");
  if (!name.empty()) {
    pathNames_.insert(name);
  }
  paths_.push_back({
      .name = std::move(name),
      .selector = selector,
      .mode = mode,
  });
  return *this;
}

void GraphMatchBuilder::addElement(
    const std::string& name,
    const GraphLabelExpressionPtr& label,
    bool edge) {
  if (name.empty()) {
    return;
  }
  auto tables = edge ? graph_->matchingEdgeTables(label.get())
                     : graph_->matchingVertexTables(label.get());
  auto [it, inserted] =
      elementNames_.emplace(name, ElementName{.edge = edge, .tables = tables});
  if (!inserted && it->second.edge == edge) {
    PropertyGraph::intersectTables(it->second.tables, tables);
  }
  for (auto& subpath : subpaths_) {
    subpath.names.insert(name);
  }
}

void GraphMatchBuilder::flush() {
  if (!pending_.has_value()) {
    return;
  }
  auto term = std::move(pending_.value());
  pending_.reset();

  GraphPatternTermPtr built;
  switch (term.kind) {
    case GraphPatternTerm::Kind::kVertex:
      built = std::make_shared<GraphVertexPattern>(
          std::move(term.name),
          std::move(term.label),
          std::move(term.predicate));
      break;
    case GraphPatternTerm::Kind::kEdge:
      built = std::make_shared<GraphEdgePattern>(
          std::move(term.name),
          std::move(term.label),
          std::move(term.predicate),
          term.direction,
          term.repetition);
      break;
    case GraphPatternTerm::Kind::kSubpath:
      built = std::make_shared<GraphSubpathPattern>(
          std::move(term.name),
          term.mode,
          std::move(term.terms),
          std::move(term.predicate),
          term.repetition,
          term.optional);
      break;
  }

  if (!subpaths_.empty()) {
    subpaths_.back().terms.push_back(std::move(built));
    return;
  }
  if (paths_.empty()) {
    paths_.push_back({});
  }
  paths_.back().terms.push_back(std::move(built));
}

GraphMatchBuilder& GraphMatchBuilder::vertex(
    std::string name,
    GraphLabelExpressionPtr label,
    std::optional<ExprApi> predicate) {
  flush();
  addElement(name, label, /*edge=*/false);
  auto resolved =
      predicate.has_value() ? resolve(predicate.value(), {name}) : nullptr;
  pending_ = PendingTerm{
      .kind = GraphPatternTerm::Kind::kVertex,
      .name = std::move(name),
      .label = std::move(label),
      .predicate = std::move(resolved),
  };
  return *this;
}

namespace {

GraphLabelExpressionPtr labelOf(std::string_view label) {
  if (label.empty()) {
    return nullptr;
  }
  return std::make_shared<GraphLabelName>(std::string(label));
}

} // namespace

GraphMatchBuilder& GraphMatchBuilder::vertex(
    std::string name,
    std::string_view label,
    std::string_view predicate) {
  return vertex(std::move(name), labelOf(label), parse(predicate));
}

GraphMatchBuilder& GraphMatchBuilder::vertex(
    std::string name,
    std::string_view label) {
  return vertex(std::move(name), labelOf(label), std::nullopt);
}

GraphMatchBuilder& GraphMatchBuilder::vertex(
    std::string name,
    GraphLabelExpressionPtr label) {
  return vertex(std::move(name), std::move(label), std::nullopt);
}

GraphMatchBuilder& GraphMatchBuilder::vertex(std::string name) {
  return vertex(std::move(name), nullptr, std::nullopt);
}

GraphMatchBuilder& GraphMatchBuilder::addEdge(
    std::string name,
    GraphLabelExpressionPtr label,
    std::optional<ExprApi> predicate,
    GraphEdgePattern::Direction direction) {
  flush();
  addElement(name, label, /*edge=*/true);
  auto resolved =
      predicate.has_value() ? resolve(predicate.value(), {name}) : nullptr;
  pending_ = PendingTerm{
      .kind = GraphPatternTerm::Kind::kEdge,
      .name = std::move(name),
      .label = std::move(label),
      .predicate = std::move(resolved),
      .direction = direction,
  };
  return *this;
}

GraphMatchBuilder& GraphMatchBuilder::rightEdge(
    std::string name,
    GraphLabelExpressionPtr label,
    std::optional<ExprApi> predicate) {
  return addEdge(
      std::move(name),
      std::move(label),
      std::move(predicate),
      GraphEdgePattern::Direction::kRight);
}

GraphMatchBuilder& GraphMatchBuilder::rightEdge(
    std::string name,
    std::string_view label,
    std::string_view predicate) {
  return rightEdge(std::move(name), labelOf(label), parse(predicate));
}

GraphMatchBuilder& GraphMatchBuilder::rightEdge(
    std::string name,
    std::string_view label) {
  return rightEdge(std::move(name), labelOf(label), std::nullopt);
}

GraphMatchBuilder& GraphMatchBuilder::rightEdge(
    std::string name,
    GraphLabelExpressionPtr label) {
  return rightEdge(std::move(name), std::move(label), std::nullopt);
}

GraphMatchBuilder& GraphMatchBuilder::rightEdge(std::string name) {
  return rightEdge(std::move(name), nullptr, std::nullopt);
}

GraphMatchBuilder& GraphMatchBuilder::leftEdge(
    std::string name,
    GraphLabelExpressionPtr label,
    std::optional<ExprApi> predicate) {
  return addEdge(
      std::move(name),
      std::move(label),
      std::move(predicate),
      GraphEdgePattern::Direction::kLeft);
}

GraphMatchBuilder& GraphMatchBuilder::leftEdge(
    std::string name,
    std::string_view label,
    std::string_view predicate) {
  return leftEdge(std::move(name), labelOf(label), parse(predicate));
}

GraphMatchBuilder& GraphMatchBuilder::leftEdge(
    std::string name,
    std::string_view label) {
  return leftEdge(std::move(name), labelOf(label), std::nullopt);
}

GraphMatchBuilder& GraphMatchBuilder::leftEdge(
    std::string name,
    GraphLabelExpressionPtr label) {
  return leftEdge(std::move(name), std::move(label), std::nullopt);
}

GraphMatchBuilder& GraphMatchBuilder::leftEdge(std::string name) {
  return leftEdge(std::move(name), nullptr, std::nullopt);
}

GraphMatchBuilder& GraphMatchBuilder::anyEdge(
    std::string name,
    GraphLabelExpressionPtr label,
    std::optional<ExprApi> predicate) {
  return addEdge(
      std::move(name),
      std::move(label),
      std::move(predicate),
      GraphEdgePattern::Direction::kAnyDirection);
}

GraphMatchBuilder& GraphMatchBuilder::anyEdge(
    std::string name,
    std::string_view label,
    std::string_view predicate) {
  return anyEdge(std::move(name), labelOf(label), parse(predicate));
}

GraphMatchBuilder& GraphMatchBuilder::anyEdge(
    std::string name,
    std::string_view label) {
  return anyEdge(std::move(name), labelOf(label), std::nullopt);
}

GraphMatchBuilder& GraphMatchBuilder::anyEdge(
    std::string name,
    GraphLabelExpressionPtr label) {
  return anyEdge(std::move(name), std::move(label), std::nullopt);
}

GraphMatchBuilder& GraphMatchBuilder::anyEdge(std::string name) {
  return anyEdge(std::move(name), nullptr, std::nullopt);
}

GraphMatchBuilder& GraphMatchBuilder::repeats(
    int64_t min,
    std::optional<int64_t> max) {
  VELOX_USER_CHECK(
      pending_.has_value() && pending_->kind != GraphPatternTerm::Kind::kVertex,
      "Only an edge or a subpath repeats");
  pending_->repetition = GraphRepetition{.min = min, .max = max};
  return *this;
}

GraphMatchBuilder& GraphMatchBuilder::optional() {
  VELOX_USER_CHECK(
      pending_.has_value() &&
          pending_->kind == GraphPatternTerm::Kind::kSubpath,
      "Only a subpath is optional");
  pending_->optional = true;
  return *this;
}

GraphMatchBuilder& GraphMatchBuilder::startSubpath(
    std::string name,
    GraphPathMode mode) {
  flush();
  if (!name.empty()) {
    pathNames_.insert(name);
  }
  subpaths_.push_back({.name = std::move(name), .mode = mode});
  return *this;
}

GraphMatchBuilder& GraphMatchBuilder::startSubpath(std::string name) {
  return startSubpath(std::move(name), GraphPathMode::kWalk);
}

GraphMatchBuilder& GraphMatchBuilder::endSubpath() {
  return endSubpath(std::optional<ExprApi>{});
}

GraphMatchBuilder& GraphMatchBuilder::endSubpath(
    std::optional<ExprApi> predicate) {
  flush();
  VELOX_USER_CHECK(!subpaths_.empty(), "There is no open subpath to end");
  auto subpath = std::move(subpaths_.back());
  subpaths_.pop_back();
  pending_ = PendingTerm{
      .kind = GraphPatternTerm::Kind::kSubpath,
      .name = std::move(subpath.name),
      .predicate = predicate.has_value()
          ? resolve(predicate.value(), subpath.names)
          : nullptr,
      .mode = subpath.mode,
      .terms = std::move(subpath.terms),
  };
  return *this;
}

GraphMatchBuilder& GraphMatchBuilder::endSubpath(std::string_view predicate) {
  return endSubpath(parse(predicate));
}

GraphMatchBuilder& GraphMatchBuilder::filter(const ExprApi& predicate) {
  flush();
  VELOX_USER_CHECK_NULL(predicate_, "Graph match already has a condition");
  folly::F14FastSet<std::string> names;
  for (const auto& [name, _] : elementNames_) {
    names.insert(name);
  }
  predicate_ = resolve(predicate, names);
  return *this;
}

GraphMatchBuilder& GraphMatchBuilder::filter(std::string_view predicate) {
  return filter(parse(predicate).value());
}

GraphMatchBuilder& GraphMatchBuilder::project(
    const std::vector<ExprApi>& exprs) {
  flush();
  VELOX_USER_CHECK(outputs_.empty(), "Graph match is already projected");
  VELOX_USER_CHECK(!exprs.empty(), "Graph match projects no expressions");
  outputs_ = exprs;
  return *this;
}

GraphMatchBuilder& GraphMatchBuilder::project(
    const std::vector<std::string>& exprs) {
  std::vector<ExprApi> parsed;
  parsed.reserve(exprs.size());
  for (const auto& expr : exprs) {
    parsed.push_back(parse(expr).value());
  }
  return project(parsed);
}

std::optional<ExprApi> GraphMatchBuilder::parse(std::string_view sql) const {
  if (sql.empty()) {
    return std::nullopt;
  }
  return ExprApi(
      builder_.context_.sqlParser->parseScalarOrWindowExpr(std::string(sql)));
}

ExprPtr GraphMatchBuilder::resolve(
    const ExprApi& expr,
    const folly::F14FastSet<std::string>& names) const {
  return builder_.resolver_.resolveScalarTypes(
      expr.expr(),
      [&](const std::optional<std::string>& alias,
          const std::string& name) -> ExprPtr {
        // A qualified name resolves as a field of the pattern name.
        if (alias.has_value()) {
          return nullptr;
        }
        VELOX_USER_CHECK(
            names.contains(name),
            "Pattern predicate reads a name it cannot see: {}",
            name);
        return std::make_shared<InputReferenceExpr>(
            PropertyGraph::properties(elementNames_.at(name).tables), name);
      });
}

namespace {

// Returns the pattern and property 'expr' reads if it is pattern.property.
std::optional<std::pair<std::string, std::string>> asPropertyRef(
    const velox::core::ExprPtr& expr) {
  const auto* field =
      dynamic_cast<const velox::core::FieldAccessExpr*>(expr.get());
  if (field == nullptr || field->isRootColumn()) {
    return std::nullopt;
  }
  const auto* root =
      velox::core::FieldAccessExpr::tryAsRootColumn(field->input());
  if (root == nullptr) {
    return std::nullopt;
  }
  return std::make_pair(root->name(), field->name());
}

// Returns the pattern 'expr' reads if it is path_length(pattern).
std::optional<std::string> asPathLength(const velox::core::ExprPtr& expr) {
  const auto* call = dynamic_cast<const velox::core::CallExpr*>(expr.get());
  if (call == nullptr ||
      call->name() != GraphMatchBuilder::kPathLengthFunctionName ||
      call->inputs().size() != 1) {
    return std::nullopt;
  }
  const auto* root =
      velox::core::FieldAccessExpr::tryAsRootColumn(call->inputAt(0));
  if (root == nullptr) {
    return std::nullopt;
  }
  return root->name();
}

} // namespace

PlanBuilder& GraphMatchBuilder::endGraphMatch() {
  flush();
  VELOX_USER_CHECK(
      subpaths_.empty(), "Graph match has a subpath that was not ended");

  // A node output: pattern and property, or a path pattern and an empty
  // property for its length.
  using Output = std::pair<std::string, std::string>;

  // Returns the node output 'expr' reads directly, if any.
  auto asOutput =
      [&](const velox::core::ExprPtr& expr) -> std::optional<Output> {
    if (auto ref = asPropertyRef(expr);
        ref.has_value() && elementNames_.contains(ref->first)) {
      return ref;
    }
    if (auto pattern = asPathLength(expr);
        pattern.has_value() && pathNames_.contains(pattern.value())) {
      return Output{pattern.value(), ""};
    }
    return std::nullopt;
  };

  std::vector<GraphMatchOutputPtr> nodeOutputs;
  std::vector<std::string> nodeOutputNames;
  folly::F14FastMap<std::string, std::string> outputIds;
  auto mapping = std::make_shared<NameMappings>();

  // Adds 'output' to the node once, under a name from 'hint', and returns its
  // column name.
  auto addOutput = [&](const Output& output, const std::string& hint) {
    const auto& [pattern, property] = output;
    const auto key = pattern + "." + property;
    if (const auto it = outputIds.find(key); it != outputIds.end()) {
      return it->second;
    }
    if (property.empty()) {
      nodeOutputs.push_back(std::make_shared<GraphPathLengthOutput>(pattern));
    } else {
      nodeOutputs.push_back(
          std::make_shared<GraphPropertyOutput>(pattern, property));
    }
    nodeOutputNames.push_back(builder_.newName(hint));
    outputIds.emplace(key, nodeOutputNames.back());
    return nodeOutputNames.back();
  };

  // Returns the name an output gets when its expression has no alias.
  auto defaultName = [](const Output& output) {
    return output.second.empty() ? std::string(kPathLengthFunctionName)
                                 : output.second;
  };

  // Outputs that each read a distinct property or path length, under distinct
  // names, need no projection.
  std::set<Output> reads;
  folly::F14FastSet<std::string> names;
  const bool direct =
      std::all_of(outputs_.begin(), outputs_.end(), [&](const auto& output) {
        const auto read = asOutput(output.expr());
        return read.has_value() && reads.insert(read.value()).second &&
            names.insert(output.alias().value_or(defaultName(read.value())))
                .second;
      });

  if (direct) {
    for (const auto& output : outputs_) {
      const auto read = asOutput(output.expr()).value();
      const auto name = output.alias().value_or(defaultName(read));
      mapping->add(name, addOutput(read, name));
    }
  } else {
    // Replaces each property and path length with a reference to a node
    // output.
    std::function<velox::core::ExprPtr(const velox::core::ExprPtr&)> rewrite =
        [&](const velox::core::ExprPtr& expr) -> velox::core::ExprPtr {
      if (const auto read = asOutput(expr)) {
        const auto id = addOutput(read.value(), defaultName(read.value()));
        if (!mapping->lookup(id).has_value()) {
          mapping->add(id, id);
        }
        return std::make_shared<velox::core::FieldAccessExpr>(
            id, expr->alias());
      }
      if (const auto* root =
              velox::core::FieldAccessExpr::tryAsRootColumn(expr)) {
        VELOX_USER_FAIL(
            "Graph match output references an unknown pattern name: {}",
            root->name());
      }
      std::vector<velox::core::ExprPtr> inputs;
      for (const auto& input : expr->inputs()) {
        inputs.push_back(rewrite(input));
      }
      return expr->replaceInputs(std::move(inputs));
    };
    std::vector<ExprApi> projections;
    for (const auto& output : outputs_) {
      const auto read = asOutput(output.expr());
      projections.emplace_back(
          rewrite(output.expr()),
          read.has_value() ? output.alias().value_or(defaultName(read.value()))
                           : output.alias());
    }
    outputs_ = std::move(projections);
  }

  builder_.node_ = std::make_shared<GraphMatchNode>(
      builder_.nextId(),
      graph_,
      std::move(paths_),
      std::move(predicate_),
      std::move(nodeOutputs),
      std::move(nodeOutputNames));
  builder_.outputMapping_ = std::move(mapping);

  if (!direct) {
    builder_.project(outputs_);
  }
  return builder_;
}

} // namespace facebook::axiom::logical_plan
