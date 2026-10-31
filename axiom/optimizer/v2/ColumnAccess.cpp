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

#include "axiom/optimizer/v2/ColumnAccess.h"

#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/PlanUtils.h"
#include "axiom/optimizer/v2/AppendAll.h"
#include "axiom/optimizer/v2/NodeVisitor.h"

namespace facebook::axiom::optimizer::v2 {

namespace {

// A dereference selector is an ordinal by the time it reaches here, so the
// field name comes from the struct being read.
// Returns null for a field with no name, which no subfield can address.
Name fieldNameAt(ExprCP base, const velox::Variant& selector) {
  const int32_t ordinal = selector.value<velox::TypeKind::INTEGER>();
  const auto& name = base->value().type->asRow().nameOf(ordinal);
  return name.empty() ? nullptr : toName(name);
}

// Fills in a subscript's key, or marks the step as reaching every entry when
// the key has a type no subfield can name.
void setKey(Step& step, const velox::Variant& key) {
  if (key.isNull()) {
    step.allFields = true;
    return;
  }
  switch (key.kind()) {
    case velox::TypeKind::VARCHAR:
      step.field = toName(key.value<velox::TypeKind::VARCHAR>());
      return;
    case velox::TypeKind::BIGINT:
    case velox::TypeKind::INTEGER:
    case velox::TypeKind::SMALLINT:
    case velox::TypeKind::TINYINT:
      step.id = integerValue(&key);
      return;
    default:
      step.allFields = true;
  }
}

// A step that reaches every entry says nothing once nothing follows it: the
// level it stands for is read in full either way. Dropping it lets a path that
// is nothing but wildcards reduce to the whole column.
void dropTrailingWildcards(std::vector<Step>& steps) {
  while (!steps.empty() && steps.back().allFields) {
    steps.pop_back();
  }
}

// Splits 'tail' by its leading field step, so each argument of a constructed
// row is visited with the paths read from the field it supplies. A path that
// does not start with a field reads the row itself, which reads every
// argument whole.
template <typename Func>
void forEachArgumentField(const Call& call, const PathSet& tail, Func func) {
  const auto& rowType = call.value().type->asRow();
  folly::F14FastMap<int32_t, PathSet> byArgument;
  bool wholeRow = false;

  tail.forEachPath([&](PathCP path) {
    const auto& steps = path->steps();
    if (steps.empty() || steps.front().kind != StepKind::kField) {
      wholeRow = true;
      return;
    }
    const auto ordinal = rowType.getChildIdx(steps.front().field);
    byArgument[ordinal].add(
        toPath(std::span<const Step>(steps).subspan(1))->id());
  });

  if (wholeRow) {
    for (ExprCP arg : call.args()) {
      func(arg, PathSet{});
    }
    return;
  }

  for (const auto& [ordinal, paths] : byArgument) {
    func(call.args()[ordinal], paths);
  }
}

Name registeredName(const std::optional<std::string>& name) {
  return name.has_value() ? toName(name.value()) : nullptr;
}

} // namespace

ColumnAccess::ColumnAccess()
    : subscript_(registeredName(queryCtx()->functionRegistry().subscript())),
      elementAt_(registeredName(queryCtx()->functionRegistry().elementAt())) {}

void ColumnAccess::add(ExprCP expr) {
  std::vector<Step> steps;
  // An empty tail means the reader takes the whole result.
  const PathSet wholeResult;
  addSteps(expr, steps, wholeResult);
}

void ColumnAccess::addProducing(ExprCP expr, ColumnCP resultColumn) {
  const auto it = byColumn_.find(resultColumn);
  if (it == byColumn_.end()) {
    // Nothing above narrowed the result, so the expression is read whole.
    add(expr);
    return;
  }
  // Copied, not referenced: the walk inserts into 'byColumn_', which can
  // rehash and move the set this came from.
  const PathSet tail = it->second;
  std::vector<Step> steps;
  addSteps(expr, steps, tail);
}

void ColumnAccess::addAll(const ExprVector& exprs) {
  for (ExprCP expr : exprs) {
    add(expr);
  }
}

PathSet ColumnAccess::subfieldsOf(ColumnCP column) const {
  const auto it = byColumn_.find(column);
  if (it == byColumn_.end()) {
    return PathSet{};
  }
  PathSet paths = it->second;
  Path::subfieldSkyline(paths);
  return paths;
}

void ColumnAccess::addSteps(
    ExprCP expr,
    std::vector<Step>& steps,
    const PathSet& tail) {
  if (expr->is(PlanType::kColumnExpr)) {
    addColumn(expr->as<Column>(), steps, tail);
    return;
  }

  if (!expr->is(PlanType::kCallExpr) && !expr->is(PlanType::kAggregateExpr) &&
      !expr->is(PlanType::kWindowExpr)) {
    addWhole(expr);
    return;
  }

  const auto* call = expr->as<Call>();
  if (call->isRowConstructor() && addRowConstructor(call, steps, tail)) {
    return;
  }

  if (addPathStep(call, steps, tail)) {
    return;
  }

  addCallInputs(call);
}

void ColumnAccess::addColumn(
    ColumnCP column,
    const std::vector<Step>& steps,
    const PathSet& tail) {
  // Reaching a column reverses 'steps' to get the path read from it.
  const std::vector<Step> prefix(steps.rbegin(), steps.rend());
  PathSet& paths = byColumn_[column];

  auto addExtendedBy = [&](std::span<const Step> suffix) {
    std::vector<Step> full = prefix;
    full.insert(full.end(), suffix.begin(), suffix.end());
    dropTrailingWildcards(full);
    paths.add(toPath(full)->id());
  };

  if (tail.empty()) {
    addExtendedBy({});
    return;
  }

  // Each path the reader takes from the result extends the path taken to
  // reach this column.
  tail.forEachPath([&](PathCP suffix) { addExtendedBy(suffix->steps()); });
}

bool ColumnAccess::addRowConstructor(
    const Call* call,
    std::vector<Step>& steps,
    const PathSet& tail) {
  // Reading field 'i' of a constructed row reads the argument that supplies
  // it. The field step arrives in 'tail' when a projection produced the row,
  // and in 'steps' when the row is dereferenced where it is built.
  if (!steps.empty() && steps.back().kind == StepKind::kField) {
    const auto ordinal =
        call->value().type->asRow().getChildIdx(steps.back().field);
    Step field = steps.back();
    steps.pop_back();
    addSteps(call->args()[ordinal], steps, tail);
    steps.push_back(field);
    return true;
  }

  if (!tail.empty()) {
    // 'tail' is read from the row this call builds, so its leading field step
    // selects an argument. A path in 'steps' would sit between the two and
    // change what the field names.
    VELOX_CHECK(
        steps.empty(),
        "Row constructor reached with both a path to it and paths from it");
    forEachArgumentField(*call, tail, [&](ExprCP arg, const PathSet& rest) {
      addSteps(arg, steps, rest);
    });
    return true;
  }

  return false;
}

bool ColumnAccess::addPathStep(
    const Call* call,
    std::vector<Step>& steps,
    const PathSet& tail) {
  if (call->args().size() != 2) {
    return false;
  }
  const Name name = call->name();
  const bool isSubscript = name == subscript_ || name == elementAt_;
  const bool isDereference = name == SpecialFormCallNames::kDereference;
  if (!isSubscript && !isDereference) {
    return false;
  }

  const StepKind subscriptKind =
      name == elementAt_ ? StepKind::kElementAt : StepKind::kSubscript;
  ExprCP operand = call->args()[0];

  Step step;
  if (call->args()[1]->is(PlanType::kLiteralExpr)) {
    const auto& key = call->literalAt(1);
    if (isDereference) {
      Name field = fieldNameAt(operand, key);
      if (field == nullptr) {
        // An unnamed field cannot be addressed, so the struct is read whole.
        addWhole(operand);
        return true;
      }
      step = {.kind = StepKind::kField, .field = field};
    } else {
      step = {.kind = subscriptKind};
      setKey(step, key);
      // A non-positive array index counts from the end of the array, which no
      // prefix bound can express, and Velox rejects it outright. Reading every
      // element is the only sound answer.
      if (operand->value().type->isArray() && !step.allFields && step.id <= 0) {
        step.allFields = true;
        // The key no longer selects anything, and leaving it set would make
        // two steps that mean the same thing intern as different paths.
        step.id = 0;
      }
    }
  } else if (isSubscript) {
    // A key computed at run time selects no entry, so every entry at this
    // level is read; anything below it still prunes. The key expression is a
    // read of its own.
    step = {.kind = subscriptKind, .allFields = true};
    add(call->args()[1]);
  } else {
    return false;
  }

  steps.push_back(step);
  addSteps(operand, steps, tail);
  steps.pop_back();
  return true;
}

void ColumnAccess::addCallInputs(const Call* call) {
  // The arguments are re-rooted here as whole-result reads, so what this adds
  // depends only on 'call'. A shared subexpression is reached once per path
  // through the expression, which is exponential in its depth without this.
  if (!callInputsAdded_.insert(call).second) {
    return;
  }
  ExprVector reads(call->args().begin(), call->args().end());
  if (const auto* aggregate = dynamic_cast<const optimizer::Aggregate*>(call)) {
    if (aggregate->condition() != nullptr) {
      reads.push_back(aggregate->condition());
    }
    appendAll(reads, aggregate->orderKeys());
  } else if (
      const auto* window =
          dynamic_cast<const optimizer::WindowFunction*>(call)) {
    appendAll(reads, window->partitionKeys());
    appendAll(reads, window->orderKeys());
    if (window->frame().startValue != nullptr) {
      reads.push_back(window->frame().startValue);
    }
    if (window->frame().endValue != nullptr) {
      reads.push_back(window->frame().endValue);
    }
  }

  PlanObjectSet decomposed;
  for (ExprCP read : reads) {
    decomposed.unionSet(read->columns());
    add(read);
  }

  // Every column a call reads is reached by one of the expressions above. A
  // call that holds one somewhere else must be added there; recording it here
  // instead would leave the column pruned to whatever another read recorded.
  PlanObjectSet unreached = call->columns();
  unreached.except(decomposed);
  VELOX_CHECK(
      unreached.empty(),
      "Call reads a column no walked expression reaches: {}",
      call->name());
}

void ColumnAccess::addWhole(ExprCP expr) {
  expr->columns().forEach<Column>(
      [&](ColumnCP column) { byColumn_[column].add(toPath({})->id()); });
}

namespace {

struct CollectContext : public NodeVisitorContext {
  explicit CollectContext(ColumnAccess& access) : access(access) {}

  ColumnAccess& access;
};

// Feeds every expression one node evaluates to `ColumnAccess`. A node whose
// only inputs are columns reads those columns whole, which is what an
// unrecorded column already reports, so it contributes nothing here.
class Collector : public NodeVisitor {
 public:
  void visit(const Scan& node, NodeVisitorContext& context) const override {}

  void visit(const Filter& node, NodeVisitorContext& context) const override {
    addAll(context, node.predicates());
  }

  // Project expressions and aggregate calls are recorded by the pass, once per
  // return path, from what that path keeps: an expression it prunes must not
  // contribute the paths it reads.
  void visit(const Project& node, NodeVisitorContext& context) const override {}

  void visit(const Limit& node, NodeVisitorContext& context) const override {}

  void visit(const Sort& node, NodeVisitorContext& context) const override {
    addAll(context, node.orderKeys());
  }

  void visit(const TopN& node, NodeVisitorContext& context) const override {
    addAll(context, node.orderKeys());
  }

  // Grouping keys are never pruned, so they are recorded here.
  void visit(const Aggregate& node, NodeVisitorContext& context)
      const override {
    addAll(context, node.groupingKeys());
  }

  void visit(const GroupId& node, NodeVisitorContext& context) const override {
    addAll(context, node.groupingKeys());
    addAll(context, node.aggregationInputs());
  }

  void visit(const MarkDistinct& node, NodeVisitorContext& context)
      const override {
    addAll(context, node.distinctKeys());
  }

  void visit(const Values& node, NodeVisitorContext& context) const override {}

  void visit(const Unnest& node, NodeVisitorContext& context) const override {
    addAll(context, node.unnestExpressions());
  }

  // A leg's column feeds an output column with no expression between them, so
  // nothing else records that the leg column is read. Leaving it unrecorded
  // would be safe only while no other part of the plan narrows the same
  // `Column*`, which a rewrite that duplicates a subtree can arrange.
  void visit(const UnionAll& node, NodeVisitorContext& context) const override {
    auto& access = accessOf(context);
    for (const auto& leg : node.legColumns()) {
      for (size_t i = 0; i < leg.size(); ++i) {
        access.addProducing(leg[i], node.outputColumns()[i]);
      }
    }
  }

  void visit(const Join& node, NodeVisitorContext& context) const override {
    addAll(context, node.leftKeys());
    addAll(context, node.rightKeys());
    addAll(context, node.filter());
  }

  void visit(const IndexLookupJoin& node, NodeVisitorContext& context)
      const override {
    addAll(context, node.probeKeys());
    addAll(context, node.filter());
  }

  void visit(const VectorSearch& node, NodeVisitorContext& context)
      const override {
    addAll(context, node.probeKeys());
    addAll(context, node.buildKeys());
    add(context, node.probeEmbedding());
    add(context, node.buildIndex());
  }

  void visit(const Window& node, NodeVisitorContext& context) const override {
    addAll(context, node.partitionKeys());
    addAll(context, node.orderKeys());
    for (const auto& function : node.functions()) {
      add(context, function.call);
      add(context, function.frame.startValue);
      add(context, function.frame.endValue);
    }
  }

  void visit(const Inference& node, NodeVisitorContext& context)
      const override {
    add(context, node.call());
  }

  void visit(const RowNumber& node, NodeVisitorContext& context)
      const override {
    addAll(context, node.partitionKeys());
  }

  void visit(const TopNRowNumber& node, NodeVisitorContext& context)
      const override {
    addAll(context, node.partitionKeys());
    addAll(context, node.orderKeys());
  }

  void visit(const Apply& node, NodeVisitorContext& context) const override {
    addAll(context, node.filter());
    add(context, node.inLhs());
    add(context, node.inBodyKey());
  }

  void visit(const EnforceSingleRow& node, NodeVisitorContext& context)
      const override {}

  void visit(const AssignUniqueId& node, NodeVisitorContext& context)
      const override {}

  void visit(const EnforceDistinct& node, NodeVisitorContext& context)
      const override {
    addAll(context, node.distinctKeys());
  }

  void visit(const Exchange& node, NodeVisitorContext& context) const override {
    addAll(context, node.partitioning().keys);
    addAll(context, node.partitioning().orderKeys);
  }

  void visit(const TableWrite& node, NodeVisitorContext& context)
      const override {
    addAll(context, node.columnExprs());
  }

  // Each column a read presents is produced from the state column at its
  // position, so the read's paths are paths of the state's column.
  void visit(const WorkingTable& node, NodeVisitorContext& context)
      const override {
    for (size_t i = 0; i < node.outputColumns().size(); ++i) {
      accessOf(context).addProducing(
          node.stateColumns()[i], node.outputColumns()[i]);
    }
  }

  void visit(const FixedPoint& node, NodeVisitorContext& context)
      const override {}

  void visit(const Repeat& node, NodeVisitorContext& context) const override {
    addAll(context, node.startKeys());
    addAll(context, node.hopSourceKeys());
    addAll(context, node.hopDestinationKeys());
    addAll(context, node.endKeys());
  }

 protected:
  static ColumnAccess& accessOf(NodeVisitorContext& context) {
    return static_cast<CollectContext&>(context).access;
  }

  static void add(NodeVisitorContext& context, ExprCP expr) {
    if (expr != nullptr) {
      accessOf(context).add(expr);
    }
  }

  static void addAll(NodeVisitorContext& context, const ExprVector& exprs) {
    accessOf(context).addAll(exprs);
  }
};

} // namespace

void ColumnAccess::add(const Node& node) {
  CollectContext context{*this};
  node.accept(Collector{}, context);
}

namespace {

// Adds what `Collector` leaves to the pass, for a subtree the pass has not
// walked yet.
class SubtreeCollector : public Collector {
 public:
  void visit(const Project& node, NodeVisitorContext& context) const override {
    auto& access = accessOf(context);
    for (size_t i = 0; i < node.exprs().size(); ++i) {
      access.addProducing(node.exprs()[i], node.outputColumns()[i]);
    }
  }

  void visit(const Aggregate& node, NodeVisitorContext& context)
      const override {
    Collector::visit(node, context);
    auto& access = accessOf(context);
    for (const auto* call : node.aggregates()) {
      access.add(call);
    }
  }
};

} // namespace

void ColumnAccess::addSubtree(const Node& node) {
  CollectContext context{*this};
  node.accept(SubtreeCollector{}, context);
  for (NodeCP input : node.inputs()) {
    addSubtree(*input);
  }
}

} // namespace facebook::axiom::optimizer::v2
