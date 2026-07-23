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

#include "axiom/optimizer/v2/FoldEmptyInputsPass.h"

#include <algorithm>
#include <optional>

#include "axiom/optimizer/v2/NodeRewriter.h"

namespace facebook::axiom::optimizer::v2 {
namespace {

enum class JoinSide {
  kLeft,
  kRight,
};

bool sameColumns(const ColumnVector& left, const ColumnVector& right) {
  if (left.size() != right.size()) {
    return false;
  }
  for (size_t i = 0; i < left.size(); ++i) {
    if (left[i] != right[i]) {
      return false;
    }
  }
  return true;
}

ExprVector toExprs(const ColumnVector& columns) {
  ExprVector exprs;
  exprs.reserve(columns.size());
  for (ColumnCP column : columns) {
    exprs.push_back(column);
  }
  return exprs;
}

bool isEmptyValues(NodeCP node) {
  if (!node->is(NodeType::kValues)) {
    return false;
  }
  const auto* values = node->as<Values>();
  if (values->source() != nullptr) {
    return false;
  }
  return values->rows() == nullptr || values->rows()->array().empty();
}

bool isEmptyPreservingAggregation(AggregateCP node) {
  return !node->groupingKeys().empty() && node->globalGroupingSets().empty();
}

class Folder : public NodeRewriter<NoContext> {
 public:
  // Creates a bottom-up empty-input folder using 'builder' for rewritten nodes.
  explicit Folder(Builder& builder) : NodeRewriter(builder) {}

 private:
  // Produces an empty Values node with the same output columns as 'node'.
  NodeCP makeEmptyLike(NodeCP node) {
    return builder().makeEmptyValues(node->outputColumns());
  }

  // Folds a unary operator whose empty input implies empty output.
  NodeCP foldUnaryEmptyPreserving(NodeCP original, NodeCP rewrittenInput) {
    if (isEmptyValues(rewrittenInput)) {
      return makeEmptyLike(original);
    }
    return nullptr;
  }

  // Returns a replacement if a unary node folds, or the original node when its
  // input is unchanged.
  NodeCP tryFoldUnary(NodeCP original, NodeCP originalInput, NodeCP newInput) {
    if (NodeCP folded = foldUnaryEmptyPreserving(original, newInput)) {
      return folded;
    }
    if (newInput == originalInput) {
      return original;
    }
    return nullptr;
  }

  // Replaces connector-proven empty scans with schema-preserving empty Values.
  NodeCP rewriteScan(const Scan* node, NoContext& /*context*/) override {
    if (node->baseTable()->knownEmpty) {
      return makeEmptyLike(node);
    }
    return node;
  }

  // Rewrites a filter, preserving emptiness from its input.
  NodeCP rewriteFilter(const Filter* node, NoContext& context) override {
    NodeCP newInput = rewrite(node->input(), context);
    if (NodeCP replacement = tryFoldUnary(node, node->input(), newInput)) {
      return replacement;
    }
    return builder().make<Filter>({newInput, node->predicates()});
  }

  // Rewrites a project, preserving the project's output schema on empty input.
  NodeCP rewriteProject(const Project* node, NoContext& context) override {
    NodeCP newInput = rewrite(node->input(), context);
    if (NodeCP replacement = tryFoldUnary(node, node->input(), newInput)) {
      return replacement;
    }
    return builder().make<Project>(
        {newInput, node->exprs(), node->outputColumns()});
  }

  // Rewrites a limit, preserving emptiness from its input.
  NodeCP rewriteLimit(const Limit* node, NoContext& context) override {
    NodeCP newInput = rewrite(node->input(), context);
    if (NodeCP replacement = tryFoldUnary(node, node->input(), newInput)) {
      return replacement;
    }
    return builder().make<Limit>({newInput, node->offset(), node->count()});
  }

  // Rewrites a sort, preserving emptiness from its input.
  NodeCP rewriteSort(const Sort* node, NoContext& context) override {
    NodeCP newInput = rewrite(node->input(), context);
    if (NodeCP replacement = tryFoldUnary(node, node->input(), newInput)) {
      return replacement;
    }
    return builder().make<Sort>(
        {newInput, node->orderKeys(), node->orderTypes()});
  }

  // Rewrites a TopN, preserving emptiness from its input.
  NodeCP rewriteTopN(const TopN* node, NoContext& context) override {
    NodeCP newInput = rewrite(node->input(), context);
    if (NodeCP replacement = tryFoldUnary(node, node->input(), newInput)) {
      return replacement;
    }
    return builder().make<TopN>(
        {newInput,
         node->orderKeys(),
         node->orderTypes(),
         node->offset(),
         node->count()});
  }

  // Folds only aggregate forms that produce no rows over empty input.
  NodeCP rewriteAggregate(const Aggregate* node, NoContext& context) override {
    NodeCP newInput = rewrite(node->input(), context);
    if (isEmptyValues(newInput) && isEmptyPreservingAggregation(node)) {
      return makeEmptyLike(node);
    }
    if (newInput == node->input()) {
      return node;
    }
    return builder().make<Aggregate>(Aggregate::Key{
        .input = newInput,
        .groupingKeys = node->groupingKeys(),
        .aggregates = node->aggregates(),
        .outputColumns = node->outputColumns(),
        .step = node->step(),
        .groupId = node->groupId(),
        .globalGroupingSets = node->globalGroupingSets()});
  }

  // Rewrites GroupId, preserving emptiness from its input.
  NodeCP rewriteGroupId(const GroupId* node, NoContext& context) override {
    NodeCP newInput = rewrite(node->input(), context);
    if (NodeCP replacement = tryFoldUnary(node, node->input(), newInput)) {
      return replacement;
    }
    return builder().make<GroupId>(
        {newInput,
         node->groupingKeys(),
         node->aggregationInputs(),
         node->groupingSets(),
         node->groupingKeyColumns(),
         node->groupId(),
         node->outputColumns()});
  }

  // Rewrites MarkDistinct, preserving emptiness from its input.
  NodeCP rewriteMarkDistinct(const MarkDistinct* node, NoContext& context)
      override {
    NodeCP newInput = rewrite(node->input(), context);
    if (NodeCP replacement = tryFoldUnary(node, node->input(), newInput)) {
      return replacement;
    }
    return builder().make<MarkDistinct>(
        {newInput,
         node->markers(),
         node->distinctKeys(),
         node->masks(),
         node->outputColumns()});
  }

  // Drops empty UNION ALL legs and remaps a single survivor to union outputs.
  NodeCP rewriteUnionAll(const UnionAll* node, NoContext& context) override {
    NodeVector inputs;
    QGVector<ColumnVector> legColumns;
    bool changed = false;
    for (size_t i = 0; i < node->inputs().size(); ++i) {
      NodeCP newInput = rewrite(node->inputs()[i], context);
      changed |= (newInput != node->inputs()[i]);
      if (!isEmptyValues(newInput)) {
        ColumnVector newLegColumns = node->legColumns()[i];
        // Avoid a redundant column-remapping Project.
        if (auto composed = composeColumnRemap(newInput, newLegColumns)) {
          newInput = composed->first;
          newLegColumns = std::move(composed->second);
          changed = true;
        }
        inputs.push_back(newInput);
        legColumns.push_back(std::move(newLegColumns));
      } else {
        changed = true;
      }
    }

    if (inputs.empty()) {
      return makeEmptyLike(node);
    }

    if (inputs.size() == 1) {
      return projectColumnRemap(
          inputs[0], legColumns[0], node->outputColumns());
    }

    if (!changed) {
      return node;
    }
    return builder().make<UnionAll>(
        {std::move(inputs), std::move(legColumns), node->outputColumns()});
  }

  // Rewrites Unnest, preserving emptiness from its input.
  NodeCP rewriteUnnest(const Unnest* node, NoContext& context) override {
    NodeCP newInput = rewrite(node->input(), context);
    if (NodeCP replacement = tryFoldUnary(node, node->input(), newInput)) {
      return replacement;
    }
    return builder().make<Unnest>(
        {newInput,
         node->unnestExpressions(),
         node->replicatedColumns(),
         node->unnestColumns(),
         node->ordinalityColumn(),
         node->outputColumns()});
  }

  // Folds joins according to each join type's empty-side semantics.
  NodeCP rewriteJoin(const Join* node, NoContext& context) override {
    NodeCP newLeft = rewrite(node->left(), context);
    NodeCP newRight = rewrite(node->right(), context);
    const bool leftEmpty = isEmptyValues(newLeft);
    const bool rightEmpty = isEmptyValues(newRight);

    if (!leftEmpty && !rightEmpty) {
      if (newLeft == node->left() && newRight == node->right()) {
        return node;
      }
      return rebuildJoin(node, newLeft, newRight);
    }

    switch (node->joinType()) {
      case velox::core::JoinType::kInner:
        return makeEmptyLike(node);

      case velox::core::JoinType::kLeft:
        return leftEmpty ? makeEmptyLike(node)
                         : projectJoinOutput(node, newLeft, JoinSide::kLeft);

      case velox::core::JoinType::kRight:
        return rightEmpty ? makeEmptyLike(node)
                          : projectJoinOutput(node, newRight, JoinSide::kRight);

      case velox::core::JoinType::kFull:
        if (leftEmpty && rightEmpty) {
          return makeEmptyLike(node);
        }
        return leftEmpty ? projectJoinOutput(node, newRight, JoinSide::kRight)
                         : projectJoinOutput(node, newLeft, JoinSide::kLeft);

      case velox::core::JoinType::kLeftSemiFilter:
      case velox::core::JoinType::kCountingLeftSemiFilter:
        return makeEmptyLike(node);

      case velox::core::JoinType::kRightSemiFilter:
        return makeEmptyLike(node);

      case velox::core::JoinType::kLeftSemiProject:
        return leftEmpty ? makeEmptyLike(node)
                         : projectJoinOutput(
                               node,
                               newLeft,
                               JoinSide::kLeft,
                               /*semiProjectMarkValue=*/false);

      case velox::core::JoinType::kRightSemiProject:
        return rightEmpty ? makeEmptyLike(node)
                          : projectJoinOutput(
                                node,
                                newRight,
                                JoinSide::kRight,
                                /*semiProjectMarkValue=*/false);

      case velox::core::JoinType::kAnti:
      case velox::core::JoinType::kCountingAnti:
        return leftEmpty ? makeEmptyLike(node)
                         : projectJoinOutput(node, newLeft, JoinSide::kLeft);

      case velox::core::JoinType::kNumJoinTypes:
        break;
    }
    VELOX_UNREACHABLE();
  }

  // Rewrites Window, preserving emptiness from its input.
  NodeCP rewriteWindow(const Window* node, NoContext& context) override {
    NodeCP newInput = rewrite(node->input(), context);
    if (NodeCP replacement = tryFoldUnary(node, node->input(), newInput)) {
      return replacement;
    }
    return builder().make<Window>(
        {newInput,
         node->functions(),
         node->partitionKeys(),
         node->orderKeys(),
         node->orderTypes(),
         node->outputColumns()});
  }

  // Rewrites TopNRowNumber, preserving emptiness from its input.
  NodeCP rewriteTopNRowNumber(const TopNRowNumber* node, NoContext& context)
      override {
    NodeCP newInput = rewrite(node->input(), context);
    if (NodeCP replacement = tryFoldUnary(node, node->input(), newInput)) {
      return replacement;
    }
    return builder().make<TopNRowNumber>(
        {newInput,
         node->rankFunction(),
         node->partitionKeys(),
         node->orderKeys(),
         node->orderTypes(),
         node->limit(),
         node->rankColumn(),
         node->outputColumns()});
  }

  // Rewrites AssignUniqueId, preserving emptiness from its input.
  NodeCP rewriteAssignUniqueId(const AssignUniqueId* node, NoContext& context)
      override {
    NodeCP newInput = rewrite(node->input(), context);
    if (NodeCP replacement = tryFoldUnary(node, node->input(), newInput)) {
      return replacement;
    }
    return builder().make<AssignUniqueId>({newInput, node->idColumn()});
  }

  // Rewrites EnforceDistinct, preserving emptiness from its input.
  NodeCP rewriteEnforceDistinct(const EnforceDistinct* node, NoContext& context)
      override {
    NodeCP newInput = rewrite(node->input(), context);
    if (NodeCP replacement = tryFoldUnary(node, node->input(), newInput)) {
      return replacement;
    }
    return builder().make<EnforceDistinct>(
        {newInput, node->distinctKeys(), node->errorMessage()});
  }

  // Rebuilds a join with rewritten children and the original join semantics.
  NodeCP rebuildJoin(const Join* node, NodeCP left, NodeCP right) {
    return builder().make<Join>(
        {left,
         right,
         node->joinType(),
         node->leftKeys(),
         node->rightKeys(),
         node->filter(),
         node->nullAware(),
         node->nullAsValue(),
         node->outputColumns()});
  }

  // Creates a Project unless the input already exposes the requested columns.
  NodeCP project(
      NodeCP input,
      ExprVector exprs,
      ColumnVector outputColumns,
      bool force = false) {
    if (!force && sameColumns(input->outputColumns(), outputColumns)) {
      return input;
    }
    return builder().make<Project>(
        {input, std::move(exprs), std::move(outputColumns)});
  }

  // Removes a column-renaming Project by collapsing the renaming as output
  // columns of the Project's input.
  std::optional<std::pair<NodeCP, ColumnVector>> composeColumnRemap(
      NodeCP projection,
      const ColumnVector& inputColumns) {
    if (!projection->is(NodeType::kProject)) {
      return std::nullopt;
    }

    const auto* project = projection->as<Project>();
    for (ExprCP expr : project->exprs()) {
      if (!expr->isColumn()) {
        return std::nullopt;
      }
    }

    ColumnVector composedColumns;
    composedColumns.reserve(inputColumns.size());
    const auto& projectOutputs = project->outputColumns();
    for (ColumnCP inputColumn : inputColumns) {
      auto it =
          std::find(projectOutputs.begin(), projectOutputs.end(), inputColumn);
      VELOX_CHECK(
          it != projectOutputs.end(),
          "Column remap references a column not produced by input: {}",
          inputColumn->toString());
      composedColumns.push_back(
          project->exprs()[std::distance(projectOutputs.begin(), it)]
              ->as<Column>());
    }
    return std::pair{project->input(), std::move(composedColumns)};
  }

  // Projects a surviving union input's leg columns into union output columns.
  NodeCP projectColumnRemap(
      NodeCP input,
      const ColumnVector& legColumns,
      const ColumnVector& outputColumns) {
    if (sameColumns(legColumns, outputColumns) &&
        sameColumns(input->outputColumns(), outputColumns)) {
      return input;
    }

    return project(input, toExprs(legColumns), outputColumns, /*force=*/true);
  }

  // Projects a surviving join side into the original join output schema.
  NodeCP projectJoinOutput(
      JoinCP join,
      NodeCP input,
      JoinSide survivingSide,
      std::optional<bool> semiProjectMarkValue = std::nullopt) {
    const auto& survivingColumns = survivingSide == JoinSide::kLeft
        ? join->left()->outputColumns()
        : join->right()->outputColumns();
    const auto& missingColumns = survivingSide == JoinSide::kLeft
        ? join->right()->outputColumns()
        : join->left()->outputColumns();
    const auto survivingSet = PlanObjectSet::fromObjects(survivingColumns);
    const auto missingSet = PlanObjectSet::fromObjects(missingColumns);

    ExprVector exprs;
    exprs.reserve(join->outputColumns().size());
    for (ColumnCP output : join->outputColumns()) {
      if (survivingSet.contains(output)) {
        exprs.push_back(output);
      } else if (missingSet.contains(output)) {
        exprs.push_back(builder().makeNull(output->value().type));
      } else {
        VELOX_CHECK(
            semiProjectMarkValue.has_value(),
            "Join output column is not produced by either input: {}",
            output->toString());
        exprs.push_back(builder().makeBoolean(*semiProjectMarkValue));
      }
    }

    return project(input, std::move(exprs), join->outputColumns());
  }
};

} // namespace

NodeCP FoldEmptyInputsPass::run(NodeCP root, Builder& builder) {
  Folder folder(builder);
  return folder.rewrite(root);
}

} // namespace facebook::axiom::optimizer::v2
