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

#include "axiom/optimizer/AppendAll.h"
#include "axiom/optimizer/NodeSimplifier.h"
#include "axiom/optimizer/NonNullOutput.h"
#include "axiom/optimizer/PlanUtils.h"
#include "axiom/optimizer/PrecomputeProjections.h"

namespace facebook::axiom::optimizer {

/// Simplifies one join over simplified inputs. The phases run in order, and
/// any of them can end with a result that replaces the join:
///
/// 1. Empty input: an empty input leaves no rows, or the rows that match
///    nothing.
/// 2. Filter: the filter reads through the input substitutions.
/// 3. Keys: the keys read through the input substitutions. A key pair with a
///    literal becomes a filter on an input, a join filter or a substitution.
///    A counting join keeps one key and only the columns its keys read.
/// 4. Output substitutions: outputs that read a key read the surviving key
///    instead, and the filter is simplified through them.
/// 5. A left or right join whose other input is one constant row becomes a
///    projection of the preserved input.
/// 6. Outputs: one output per distinct source; literal outputs become
///    substitutions.
/// 7. An inner cross join with one row without columns becomes the other
///    input. A semi or anti join with such a build side becomes the probe, or
///    no rows.
/// 8. The join, or for a null-aware semi project whose mark depends on the
///    build side having rows, a join that marks existence and a projection of
///    the mark.
class NodeSimplifier::JoinSimplifier {
 public:
  JoinSimplifier(
      NodeSimplifier& nodes,
      Join::Key key,
      SimplifiedNode left,
      SimplifiedNode right);

  SimplifiedNode simplify();

 private:
  // The node below any Projects over 'node', which has the same row count.
  static NodeCP cardinalitySource(NodeCP node);

  // True if 'node' produces one row without columns.
  static bool isSingleRowNoColumns(NodeCP node) {
    return node->outputColumns().empty() &&
        Values::isSingleRowNoColumns(cardinalitySource(node));
  }

  // The result with no rows, in the join's original layout.
  SimplifiedNode empty() const {
    return {nullptr, {}, originalOutputColumns_};
  }

  // True if an empty input leaves the join with no rows.
  bool emptyInputMatchesNothing() const;

  // Source columns 'input' provides: its node's output columns and the
  // columns its substitutions replace.
  ColumnVector providedColumns(const SimplifiedNode& input) const;

  // The rows of 'preserved' with the other side's outputs NULL.
  SimplifiedNode paddedInput(SimplifiedNode& preserved);

  // The rows of a full join when nothing matches: each input padded with
  // NULLs for the other.
  SimplifiedNode unmatchedFullInputs();

  // The probe side with 'marker' as the mark column.
  SimplifiedNode probeWithMarker(ExprCP marker);

  // The result when no pair of rows matches.
  SimplifiedNode noMatch();

  // Simplifies the filter through the input substitutions. Returns false if
  // no row can match.
  bool simplifyFilter();

  // Simplifies the keys through the input substitutions, and turns each key
  // pair with a literal into a filter or a substitution. Returns false if no
  // row can match.
  bool simplifyKeys();

  // Handles key pair 'i', which has a literal, appending the pair to
  // 'leftKeys' and 'rightKeys' when it stays a key. Returns false if no row
  // can match.
  bool
  simplifyLiteralKey(size_t i, ExprVector& leftKeys, ExprVector& rightKeys);

  // Returns the predicate a key pair stands for.
  ExprCP keyPredicate(ExprCP leftKey, ExprCP rightKey) const;

  // Filters 'input' by 'predicate'. Returns false if no row is left.
  bool filterInput(SimplifiedNode& input, ExprCP predicate);

  // Restores the key a counting join keeps when every key was a literal.
  void restoreCountingKey(ExprVector& leftKeys, ExprVector& rightKeys);

  // Restores the column 'replacement' stands for in 'input', for a counting
  // join's kept key.
  ExprCP
  restoreCountingKeyColumn(NodeCP& input, ExprCP original, ExprCP replacement);

  // Keeps only the columns of 'input' that 'keys' read.
  void retainCountingKeyColumns(NodeCP& input, const ExprVector& keys);

  // Builds the output substitutions the keys establish and simplifies the
  // filter through them. Returns false if no row can match.
  bool substituteOutputs();

  // Replaces a left or right join whose other input is one constant row with
  // a projection of the preserved input.
  std::optional<SimplifiedNode> eliminateSingleConstantOuterInput();

  // Keeps one output per distinct source, and outputs that are not literals.
  void pruneOutputs();

  // 'result' with the join's output substitutions, which already include the
  // input substitutions and so supersede the surviving input's.
  SimplifiedNode withOutputSubstitutions(SimplifiedNode result);

  // Replaces an inner cross join with 'singleRow', one row without columns,
  // by 'other' filtered with the join filter.
  std::optional<SimplifiedNode> eliminateSingleRowCrossJoin(
      NodeCP singleRow,
      NodeCP other);

  // Replaces a keyless, filterless semi or anti join whose build side is one
  // row without columns.
  std::optional<SimplifiedNode> eliminateSingleRowBuild();

  // Builds the join of a null-aware semi project whose mark depends on the
  // build side having rows.
  SimplifiedNode makeJoinWithMarker();

  SimplifiedNode makeJoin();

  Builder& builder_;
  ExprFactory& exprs_;
  ExprSimplifier& simplifier_;
  NodeSimplifier& nodes_;

  Join::Key key_;
  SimplifiedNode left_;
  SimplifiedNode right_;
  const ColumnVector originalOutputColumns_;

  const bool isSemiFilter_;
  const bool isSemiProject_;
  const bool isAnti_;
  const bool isCountingSemi_;
  const bool isCountingAnti_;
  const bool isCounting_;
  const bool probeIsLeft_;

  // Substitutions both inputs carry.
  PlanSubstitutions inputSubstitutions_;
  // Substitutions the literal keys establish.
  PlanSubstitutions joinSubstitutions_;
  // Substitutions for the join's outputs.
  PlanSubstitutions outputSubstitutions_;
  // The keys before simplification.
  ExprVector originalLeftKeys_;
  ExprVector originalRightKeys_;
  // The literal key a counting join keeps when it has no other key.
  std::optional<size_t> countingKeyToKeep_;
  // The mark of a null-aware semi project when the build side has rows.
  ExprCP markerWhenBuildHasRows_{nullptr};
  // The join's source columns after the keys are simplified.
  ColumnVector boundarySources_;
};

NodeSimplifier::JoinSimplifier::JoinSimplifier(
    NodeSimplifier& nodes,
    Join::Key key,
    SimplifiedNode left,
    SimplifiedNode right)
    : builder_{nodes.builder_},
      exprs_{nodes.exprs_},
      simplifier_{nodes.simplifier_},
      nodes_{nodes},
      key_{std::move(key)},
      left_{std::move(left)},
      right_{std::move(right)},
      originalOutputColumns_{key_.outputColumns},
      isSemiFilter_{
          key_.joinType == velox::core::JoinType::kLeftSemiFilter ||
          key_.joinType == velox::core::JoinType::kRightSemiFilter},
      isSemiProject_{
          key_.joinType == velox::core::JoinType::kLeftSemiProject ||
          key_.joinType == velox::core::JoinType::kRightSemiProject},
      isAnti_{
          key_.joinType == velox::core::JoinType::kAnti ||
          key_.joinType == velox::core::JoinType::kRightAnti},
      isCountingSemi_{
          key_.joinType == velox::core::JoinType::kCountingLeftSemiFilter},
      isCountingAnti_{key_.joinType == velox::core::JoinType::kCountingAnti},
      isCounting_{isCountingSemi_ || isCountingAnti_},
      probeIsLeft_{
          key_.joinType != velox::core::JoinType::kRightSemiFilter &&
          key_.joinType != velox::core::JoinType::kRightSemiProject &&
          key_.joinType != velox::core::JoinType::kRightAnti} {
  VELOX_CHECK(
      key_.joinType == velox::core::JoinType::kInner ||
      key_.joinType == velox::core::JoinType::kLeft ||
      key_.joinType == velox::core::JoinType::kRight ||
      key_.joinType == velox::core::JoinType::kFull || isSemiFilter_ ||
      isSemiProject_ || isAnti_ || isCounting_);
}

NodeSimplifier::SimplifiedNode NodeSimplifier::JoinSimplifier::simplify() {
  if (left_.empty() || right_.empty()) {
    return emptyInputMatchesNothing() ? empty() : noMatch();
  }

  if (!simplifyFilter() || !simplifyKeys() || !substituteOutputs()) {
    return noMatch();
  }

  if (auto simplified = eliminateSingleConstantOuterInput()) {
    return std::move(*simplified);
  }

  if (!isCounting_) {
    pruneOutputs();
  }

  if (key_.joinType == velox::core::JoinType::kInner && key_.leftKeys.empty()) {
    if (auto simplified = eliminateSingleRowCrossJoin(key_.left, key_.right)) {
      return std::move(*simplified);
    }
    if (auto simplified = eliminateSingleRowCrossJoin(key_.right, key_.left)) {
      return std::move(*simplified);
    }
  }

  if (auto simplified = eliminateSingleRowBuild()) {
    return std::move(*simplified);
  }

  return makeJoin();
}

// static
NodeCP NodeSimplifier::JoinSimplifier::cardinalitySource(NodeCP node) {
  while (node->is(NodeType::kProject)) {
    node = node->as<Project>()->input();
  }
  return node;
}

bool NodeSimplifier::JoinSimplifier::emptyInputMatchesNothing() const {
  const bool probeEmpty = probeIsLeft_ ? left_.empty() : right_.empty();
  return key_.joinType == velox::core::JoinType::kInner || isSemiFilter_ ||
      (isSemiProject_ && probeEmpty) || (isAnti_ && probeEmpty) ||
      (isCounting_ && left_.empty()) || (isCountingSemi_ && right_.empty()) ||
      (left_.empty() && key_.joinType == velox::core::JoinType::kLeft) ||
      (right_.empty() && key_.joinType == velox::core::JoinType::kRight);
}

ColumnVector NodeSimplifier::JoinSimplifier::providedColumns(
    const SimplifiedNode& input) const {
  ColumnVector provided = input.node->outputColumns();
  for (ColumnCP column : key_.effectiveSourceColumns()) {
    if (input.substitutions.apply(column, exprs_) != column) {
      provided.push_back(column);
    }
  }
  return provided;
}

NodeSimplifier::SimplifiedNode NodeSimplifier::JoinSimplifier::paddedInput(
    SimplifiedNode& preserved) {
  VELOX_CHECK(!preserved.empty());
  const auto expressions = builder_.paddedExpressions(
      providedColumns(preserved),
      key_.effectiveSourceColumns(),
      /*falsePadding=*/false);
  PlanSubstitutions substitutions;
  for (size_t i = 0; i < key_.outputColumns.size(); ++i) {
    if (key_.outputColumns[i] != expressions[i]) {
      substitutions.add(key_.outputColumns[i], expressions[i]);
    }
  }
  substitutions.merge(preserved.substitutions);
  substitutions.retainVisible(preserved.node->outputColumns(), exprs_);
  return {preserved.node, std::move(substitutions)};
}

NodeSimplifier::SimplifiedNode
NodeSimplifier::JoinSimplifier::unmatchedFullInputs() {
  if (left_.empty()) {
    return right_.empty() ? empty() : paddedInput(right_);
  }
  if (right_.empty()) {
    return paddedInput(left_);
  }

  NodeVector legs;
  QGVector<ColumnVector> legColumns;
  legs.reserve(2);
  legColumns.reserve(2);
  for (SimplifiedNode* input : {&left_, &right_}) {
    auto expressions = builder_.paddedExpressions(
        providedColumns(*input),
        key_.effectiveSourceColumns(),
        /*falsePadding=*/false);
    expressions = input->substitutions.apply(expressions, exprs_);
    NodeCP legInput = input->node;
    ColumnVector outputs;
    outputs.reserve(key_.outputColumns.size());
    for (ColumnCP output : key_.outputColumns) {
      outputs.push_back(
          optimizer::make<Column>(
              output->name(),
              /*relation=*/nullptr,
              output->value(),
              /*alias=*/nullptr));
    }
    legs.push_back(
        PrecomputeProjections::makeProject(
            legInput, std::move(expressions), outputs, builder_, simplifier_));
    legColumns.push_back(std::move(outputs));
  }
  return {
      builder_.make<UnionAll>(
          {std::move(legs),
           std::move(legColumns),
           std::move(key_.outputColumns)}),
      {}};
}

NodeSimplifier::SimplifiedNode NodeSimplifier::JoinSimplifier::probeWithMarker(
    ExprCP marker) {
  SimplifiedNode& probe = probeIsLeft_ ? left_ : right_;
  VELOX_CHECK(!probe.empty());
  const auto probeColumns = PlanObjectSet::fromObjects(providedColumns(probe));
  const auto sourceColumns = key_.effectiveSourceColumns();
  PlanSubstitutions substitutions;
  for (size_t i = 0; i < key_.outputColumns.size(); ++i) {
    ExprCP replacement = probeColumns.contains(sourceColumns[i])
        ? probe.substitutions.apply(sourceColumns[i], exprs_)
        : marker;
    if (key_.outputColumns[i] != replacement) {
      substitutions.add(key_.outputColumns[i], replacement);
    }
  }
  substitutions.retainVisible(probe.node->outputColumns(), exprs_);
  return {probe.node, std::move(substitutions)};
}

NodeSimplifier::SimplifiedNode NodeSimplifier::JoinSimplifier::noMatch() {
  if (key_.joinType == velox::core::JoinType::kInner || isSemiFilter_ ||
      isCountingSemi_) {
    return empty();
  }
  if (isSemiProject_) {
    return probeWithMarker(builder_.makeBoolean(false));
  }
  if (isAnti_) {
    return probeIsLeft_ ? left_ : right_;
  }
  if (isCountingAnti_) {
    return left_;
  }
  if (key_.joinType == velox::core::JoinType::kFull) {
    return unmatchedFullInputs();
  }
  return paddedInput(
      key_.joinType == velox::core::JoinType::kLeft ? left_ : right_);
}

bool NodeSimplifier::JoinSimplifier::simplifyFilter() {
  inputSubstitutions_ = left_.substitutions;
  inputSubstitutions_.merge(right_.substitutions);
  ExprVector filters;
  for (ExprCP filter : inputSubstitutions_.apply(key_.filter, exprs_)) {
    if (simplifier_.simplifyFilter(filter, filters)) {
      return false;
    }
  }
  key_.filter = std::move(filters);
  return true;
}

ExprCP NodeSimplifier::JoinSimplifier::keyPredicate(
    ExprCP leftKey,
    ExprCP rightKey) const {
  return key_.nullAsValue ? exprs_.makeNotDistinctFrom(leftKey, rightKey)
                          : exprs_.makeEq(leftKey, rightKey);
}

bool NodeSimplifier::JoinSimplifier::filterInput(
    SimplifiedNode& input,
    ExprCP predicate) {
  NodeCP node = input.node;
  input =
      nodes_.make(Filter::Key{node, ExprVector{predicate}}, std::move(input));
  return !input.empty();
}

bool NodeSimplifier::JoinSimplifier::simplifyKeys() {
  originalLeftKeys_ = key_.leftKeys;
  originalRightKeys_ = key_.rightKeys;
  key_.leftKeys = left_.substitutions.apply(key_.leftKeys, exprs_);
  key_.rightKeys = right_.substitutions.apply(key_.rightKeys, exprs_);
  NonNullOutput nonNullOutput{simplifier_};
  for (ExprCP& leftKey : key_.leftKeys) {
    leftKey = simplifier_.simplify(leftKey, nonNullOutput.get(left_.node));
  }
  for (ExprCP& rightKey : key_.rightKeys) {
    rightKey = simplifier_.simplify(rightKey, nonNullOutput.get(right_.node));
  }
  VELOX_CHECK_EQ(key_.leftKeys.size(), key_.rightKeys.size());

  ExprVector leftKeys;
  ExprVector rightKeys;
  leftKeys.reserve(key_.leftKeys.size());
  rightKeys.reserve(key_.rightKeys.size());
  for (size_t i = 0; i < key_.leftKeys.size(); ++i) {
    if (!key_.leftKeys[i]->is(PlanType::kLiteralExpr) &&
        !key_.rightKeys[i]->is(PlanType::kLiteralExpr)) {
      leftKeys.push_back(key_.leftKeys[i]);
      rightKeys.push_back(key_.rightKeys[i]);
      continue;
    }
    if (!simplifyLiteralKey(i, leftKeys, rightKeys)) {
      return false;
    }
  }
  if (isCounting_ && leftKeys.empty() && countingKeyToKeep_.has_value()) {
    restoreCountingKey(leftKeys, rightKeys);
  }
  if (isCounting_) {
    retainCountingKeyColumns(left_.node, leftKeys);
    retainCountingKeyColumns(right_.node, rightKeys);
    key_.outputColumns = left_.node->outputColumns();
    key_.sourceColumns.clear();
  }
  key_.leftKeys = std::move(leftKeys);
  key_.rightKeys = std::move(rightKeys);
  key_.left = left_.node;
  key_.right = right_.node;
  return true;
}

bool NodeSimplifier::JoinSimplifier::simplifyLiteralKey(
    size_t i,
    ExprVector& leftKeys,
    ExprVector& rightKeys) {
  ExprCP leftKey = key_.leftKeys[i];
  ExprCP rightKey = key_.rightKeys[i];
  const bool leftLiteral = leftKey->is(PlanType::kLiteralExpr);
  const bool rightLiteral = rightKey->is(PlanType::kLiteralExpr);
  if (leftLiteral && rightLiteral) {
    if (isAnti_ && key_.nullAware &&
        (isConstantNull(leftKey) || isConstantNull(rightKey))) {
      key_.nullAware = false;
      return true;
    }
    if (isSemiProject_ && key_.nullAware &&
        (isConstantNull(leftKey) || isConstantNull(rightKey))) {
      markerWhenBuildHasRows_ = builder_.makeNull(toType(velox::BOOLEAN()));
      return true;
    }
    ExprVector remaining;
    if (simplifier_.simplifyFilter(
            keyPredicate(leftKey, rightKey), remaining)) {
      return false;
    }
    VELOX_CHECK(remaining.empty());
    if (isCounting_) {
      countingKeyToKeep_ = i;
    }
    if (isAnti_ && key_.nullAware) {
      key_.nullAware = false;
    }
    return true;
  }

  ExprCP literal = leftLiteral ? leftKey : rightKey;
  if (isSemiProject_ && key_.nullAware && isConstantNull(literal)) {
    markerWhenBuildHasRows_ = builder_.makeNull(toType(velox::BOOLEAN()));
    return true;
  }
  if (!key_.nullAsValue && isConstantNull(literal) &&
      !(isAnti_ && key_.nullAware)) {
    return false;
  }

  ExprCP predicate = leftLiteral ? keyPredicate(rightKey, leftKey)
                                 : keyPredicate(leftKey, rightKey);
  if (isCounting_) {
    if (isCountingAnti_ && rightLiteral) {
      leftKeys.push_back(leftKey);
      rightKeys.push_back(rightKey);
      return true;
    }
    if (!filterInput(leftLiteral ? right_ : left_, predicate)) {
      return false;
    }
    if (isCountingSemi_ && rightLiteral) {
      joinSubstitutions_.add(leftKey, literal);
    }
    countingKeyToKeep_ = i;
    return true;
  }
  const bool literalOnBuildSide = probeIsLeft_ ? rightLiteral : leftLiteral;
  if (isAnti_) {
    ExprCP nonLiteral = leftLiteral ? rightKey : leftKey;
    SimplifiedNode& build = probeIsLeft_ ? right_ : left_;
    if (key_.nullAware) {
      VELOX_CHECK_EQ(key_.leftKeys.size(), 1);
      key_.nullAware = false;
      if (isConstantNull(literal)) {
        return true;
      }
      ExprCP orNull = exprs_.makeOr(predicate, exprs_.makeIsNull(nonLiteral));
      if (literalOnBuildSide) {
        key_.filter.push_back(orNull);
        return true;
      }
      return filterInput(build, orNull);
    }
    if (literalOnBuildSide) {
      key_.filter.push_back(predicate);
      return true;
    }
    return filterInput(build, predicate);
  }
  if (isSemiProject_) {
    if (literalOnBuildSide) {
      if (key_.nullAware) {
        markerWhenBuildHasRows_ = predicate;
        return true;
      }
      key_.filter.push_back(predicate);
      return true;
    }
    if (key_.nullAware) {
      ExprCP buildKey = leftLiteral ? rightKey : leftKey;
      if (!filterInput(
              leftLiteral ? right_ : left_,
              exprs_.makeOr(predicate, exprs_.makeIsNull(buildKey)))) {
        return false;
      }
      leftKeys.push_back(leftKey);
      rightKeys.push_back(rightKey);
      return true;
    }
  }
  if (key_.joinType == velox::core::JoinType::kFull) {
    leftKeys.push_back(leftKey);
    rightKeys.push_back(rightKey);
    return true;
  }
  const bool literalOnPreservedSide =
      (leftLiteral && key_.joinType == velox::core::JoinType::kLeft) ||
      (rightLiteral && key_.joinType == velox::core::JoinType::kRight);
  if (key_.joinType != velox::core::JoinType::kInner && !isSemiFilter_ &&
      !isSemiProject_ && !literalOnPreservedSide) {
    key_.filter.push_back(predicate);
    return true;
  }

  if (key_.joinType == velox::core::JoinType::kInner ||
      (isSemiFilter_ && literalOnBuildSide)) {
    joinSubstitutions_.add(leftLiteral ? rightKey : leftKey, literal);
  }
  return filterInput(leftLiteral ? right_ : left_, predicate);
}

void NodeSimplifier::JoinSimplifier::restoreCountingKey(
    ExprVector& leftKeys,
    ExprVector& rightKeys) {
  const size_t index = *countingKeyToKeep_;
  leftKeys.push_back(restoreCountingKeyColumn(
      left_.node, originalLeftKeys_[index], key_.leftKeys[index]));
  rightKeys.push_back(restoreCountingKeyColumn(
      right_.node, originalRightKeys_[index], key_.rightKeys[index]));
}

ExprCP NodeSimplifier::JoinSimplifier::restoreCountingKeyColumn(
    NodeCP& input,
    ExprCP original,
    ExprCP replacement) {
  if (original->isColumn()) {
    return nodes_.restoreColumnPositions(
        input, ExprVector{original}, ExprVector{replacement})[0];
  }
  auto* column = optimizer::make<Column>(
      queryCtx()->newName("counting_key"),
      /*relation=*/nullptr,
      replacement->value(),
      /*alias=*/nullptr);
  ExprVector expressions = toExprs(input->outputColumns());
  expressions.push_back(replacement);
  ColumnVector outputs = input->outputColumns();
  outputs.push_back(column);
  input = builder_.make<Project>(
      {input, std::move(expressions), std::move(outputs)});
  return column;
}

void NodeSimplifier::JoinSimplifier::retainCountingKeyColumns(
    NodeCP& input,
    const ExprVector& keys) {
  PlanObjectSet required;
  for (ExprCP expression : keys) {
    required.unionColumns(expression);
  }
  ColumnVector retained;
  for (ColumnCP column : input->outputColumns()) {
    if (required.contains(column)) {
      retained.push_back(column);
    }
  }
  if (retained.size() != input->outputColumns().size()) {
    input =
        builder_.make<Project>({input, toExprs(retained), std::move(retained)});
  }
}

bool NodeSimplifier::JoinSimplifier::substituteOutputs() {
  outputSubstitutions_ = std::move(inputSubstitutions_);
  outputSubstitutions_.merge(joinSubstitutions_);
  PlanSubstitutions sourceToOutput;
  boundarySources_ = key_.effectiveSourceColumns();
  for (size_t i = 0; i < boundarySources_.size(); ++i) {
    sourceToOutput.addIfAbsent(
        outputSubstitutions_.apply(boundarySources_[i], exprs_),
        outputSubstitutions_.apply(key_.outputColumns[i], exprs_));
  }
  ExprVector substitutionLeftKeys = sourceToOutput.apply(
      outputSubstitutions_.apply(key_.leftKeys, exprs_), exprs_);
  ExprVector substitutionRightKeys = sourceToOutput.apply(
      outputSubstitutions_.apply(key_.rightKeys, exprs_), exprs_);
  if (key_.joinType == velox::core::JoinType::kInner) {
    // The left key survives, and an output reading the right key reads it
    // instead. When the left input does not produce the left key as a column,
    // the right key survives.
    const PlanObjectSet visibleOutputs = PlanObjectSet::fromObjects(
        outputSubstitutions_.apply(toExprs(key_.outputColumns), exprs_));
    const PlanObjectSet leftColumns =
        PlanObjectSet::fromObjects(left_.node->outputColumns());
    for (size_t i = 0; i < substitutionLeftKeys.size(); ++i) {
      if (!visibleOutputs.contains(substitutionLeftKeys[i]) &&
          !leftColumns.contains(substitutionLeftKeys[i]) &&
          visibleOutputs.contains(substitutionRightKeys[i])) {
        std::swap(substitutionLeftKeys[i], substitutionRightKeys[i]);
      }
    }
  }
  nodes_.addJoinKeySubstitutions(
      key_.joinType,
      substitutionLeftKeys,
      substitutionRightKeys,
      outputSubstitutions_);
  ExprVector simplifiedFilters;
  for (ExprCP filter : outputSubstitutions_.apply(key_.filter, exprs_)) {
    if (simplifier_.simplifyFilter(filter, simplifiedFilters)) {
      return false;
    }
  }
  key_.filter = std::move(simplifiedFilters);
  return true;
}

std::optional<NodeSimplifier::SimplifiedNode>
NodeSimplifier::JoinSimplifier::eliminateSingleConstantOuterInput() {
  const bool constantOnRight = key_.joinType == velox::core::JoinType::kLeft;
  if (!constantOnRight && key_.joinType != velox::core::JoinType::kRight) {
    return std::nullopt;
  }
  if (!key_.leftKeys.empty()) {
    return std::nullopt;
  }
  SimplifiedNode& preserved = constantOnRight ? left_ : right_;
  SimplifiedNode& constant = constantOnRight ? right_ : left_;
  if (preserved.originalColumns.empty() || constant.originalColumns.empty()) {
    return std::nullopt;
  }
  for (ColumnCP column : constant.originalColumns) {
    if (!constant.substitutions.apply(column, exprs_)
             ->is(PlanType::kLiteralExpr)) {
      return std::nullopt;
    }
  }
  if (!isSingleRowNoColumns(constant.node)) {
    return std::nullopt;
  }
  const auto preservedOutputs =
      PlanObjectSet::fromObjects(preserved.node->outputColumns());
  for (ExprCP filter : key_.filter) {
    if (!preservedOutputs.containsColumns(filter)) {
      return std::nullopt;
    }
  }

  ExprCP matched = key_.filter.empty() ? builder_.makeBoolean(true)
                                       : exprs_.andAll(key_.filter);
  const auto preservedSources =
      PlanObjectSet::fromObjects(preserved.originalColumns);
  const auto constantSources =
      PlanObjectSet::fromObjects(constant.originalColumns);
  bool hasConstantOutput{false};
  for (ExprCP source : boundarySources_) {
    if (constantSources.contains(source)) {
      hasConstantOutput = true;
      if (!outputSubstitutions_.apply(source, exprs_)
               ->is(PlanType::kLiteralExpr)) {
        return std::nullopt;
      }
    } else if (!preservedSources.contains(source)) {
      return std::nullopt;
    }
  }
  if (!hasConstantOutput) {
    return std::nullopt;
  }
  ExprVector expressions;
  expressions.reserve(boundarySources_.size());
  for (size_t i = 0; i < boundarySources_.size(); ++i) {
    ExprCP replacement =
        outputSubstitutions_.apply(boundarySources_[i], exprs_);
    if (constantSources.contains(boundarySources_[i])) {
      replacement = exprs_.makeIf(
          matched, replacement, builder_.makeNull(replacement->value().type));
    }
    expressions.push_back(replacement);
  }
  return SimplifiedNode{
      PrecomputeProjections::makeProject(
          preserved.node,
          std::move(expressions),
          key_.outputColumns,
          builder_,
          simplifier_),
      {}};
}

void NodeSimplifier::JoinSimplifier::pruneOutputs() {
  const ColumnVector sourceColumns = key_.effectiveSourceColumns();
  ColumnVector keptOutputs;
  ColumnVector keptSources;
  PlanObjectSet addedOutputs;
  folly::F14FastMap<ColumnCP, ColumnCP> nullExtendedOutputs;
  keptOutputs.reserve(key_.outputColumns.size());
  keptSources.reserve(sourceColumns.size());
  for (size_t i = 0; i < key_.outputColumns.size(); ++i) {
    ExprCP replacement = outputSubstitutions_.apply(sourceColumns[i], exprs_);
    if (!replacement->isColumn()) {
      if (replacement->is(PlanType::kLiteralExpr) &&
          (key_.outputColumns[i] == sourceColumns[i] ||
           isConstantNull(replacement) || isCounting_)) {
        outputSubstitutions_.addIfAbsent(key_.outputColumns[i], replacement);
        continue;
      }
      ExprCP leftReplacement =
          left_.substitutions.apply(sourceColumns[i], exprs_);
      SimplifiedNode* input =
          leftReplacement != sourceColumns[i] ? &left_ : &right_;
      replacement = nodes_.restoreColumnPositions(
          input->node,
          ExprVector{sourceColumns[i]},
          ExprVector{replacement})[0];
      input->substitutions.erase(sourceColumns[i]);
      outputSubstitutions_.erase(sourceColumns[i]);
    }
    VELOX_CHECK(replacement->isColumn());
    ColumnCP source = replacement->as<Column>();
    ExprCP outputReplacement =
        outputSubstitutions_.apply(key_.outputColumns[i], exprs_);
    if (outputReplacement->is(PlanType::kLiteralExpr)) {
      outputSubstitutions_.addIfAbsent(
          key_.outputColumns[i], outputReplacement);
      continue;
    }
    VELOX_CHECK(outputReplacement->isColumn());
    ColumnCP output = outputReplacement->as<Column>();
    if (key_.outputColumns[i] != sourceColumns[i]) {
      auto [it, inserted] = nullExtendedOutputs.try_emplace(source, output);
      if (inserted &&
          (source != sourceColumns[i] ||
           key_.outputColumns[i]->outputName() != source->outputName())) {
        it->second = Column::createForNullExtendedValue(source);
      }
      output = it->second;
      if (output != key_.outputColumns[i]) {
        outputSubstitutions_.add(key_.outputColumns[i], output);
      }
    }
    if (addedOutputs.contains(output)) {
      continue;
    }
    addedOutputs.add(output);
    keptOutputs.push_back(output);
    keptSources.push_back(source);
  }
  key_.outputColumns = std::move(keptOutputs);
  key_.sourceColumns = std::move(keptSources);
  key_.left = left_.node;
  key_.right = right_.node;
}

NodeSimplifier::SimplifiedNode
NodeSimplifier::JoinSimplifier::withOutputSubstitutions(SimplifiedNode result) {
  if (!result.empty()) {
    PlanSubstitutions dropped{outputSubstitutions_};
    dropped.retainVisible(result.node->outputColumns(), exprs_);
    result.substitutions.setAll(dropped);
  }
  return result;
}

std::optional<NodeSimplifier::SimplifiedNode>
NodeSimplifier::JoinSimplifier::eliminateSingleRowCrossJoin(
    NodeCP singleRow,
    NodeCP other) {
  if (!Values::isSingleRowNoColumns(singleRow)) {
    return std::nullopt;
  }
  SimplifiedNode result{other, {}};
  if (!key_.filter.empty()) {
    result = nodes_.make(Filter::Key{other, key_.filter}, std::move(result));
  }
  if (result.empty()) {
    return empty();
  }
  NodeCP resultNode = result.node;
  result = nodes_.make(
      Project::Key{
          resultNode,
          toExprs(key_.effectiveSourceColumns()),
          key_.outputColumns},
      std::move(result));
  return withOutputSubstitutions(std::move(result));
}

std::optional<NodeSimplifier::SimplifiedNode>
NodeSimplifier::JoinSimplifier::eliminateSingleRowBuild() {
  if (!(isSemiFilter_ || isSemiProject_ || isAnti_) || !key_.leftKeys.empty() ||
      !key_.filter.empty()) {
    return std::nullopt;
  }
  const SimplifiedNode& build = probeIsLeft_ ? right_ : left_;
  if (!isSingleRowNoColumns(build.node)) {
    return std::nullopt;
  }
  if (isAnti_) {
    return empty();
  }
  if (isSemiFilter_) {
    return withOutputSubstitutions(probeIsLeft_ ? left_ : right_);
  }
  return withOutputSubstitutions(probeWithMarker(
      markerWhenBuildHasRows_ != nullptr ? markerWhenBuildHasRows_
                                         : builder_.makeBoolean(true)));
}

NodeSimplifier::SimplifiedNode NodeSimplifier::JoinSimplifier::makeJoin() {
  if (key_.leftKeys.empty()) {
    ColumnVector requiredColumns = key_.effectiveSourceColumns();
    if (markerWhenBuildHasRows_ != nullptr) {
      PlanObjectSet required = PlanObjectSet::fromObjects(requiredColumns);
      markerWhenBuildHasRows_->columns().forEach<Column>([&](ColumnCP column) {
        if (required.contains(column)) {
          return;
        }
        required.add(column);
        requiredColumns.push_back(column);
      });
    }
    nodes_.precomputeCrossJoinFilter(
        key_.left, key_.right, key_.filter, requiredColumns);
  }
  if (markerWhenBuildHasRows_ != nullptr) {
    return makeJoinWithMarker();
  }
  NodeCP output = builder_.make<Join>(std::move(key_));
  outputSubstitutions_.retainVisible(output->outputColumns(), exprs_);
  return {output, std::move(outputSubstitutions_)};
}

NodeSimplifier::SimplifiedNode
NodeSimplifier::JoinSimplifier::makeJoinWithMarker() {
  VELOX_CHECK(isSemiProject_);
  VELOX_CHECK(!key_.outputColumns.empty());
  PlanSubstitutions markerSubstitutions{left_.substitutions};
  markerSubstitutions.merge(right_.substitutions);
  markerSubstitutions.merge(joinSubstitutions_);
  markerWhenBuildHasRows_ = simplifier_.simplify(
      markerSubstitutions.apply(markerWhenBuildHasRows_, exprs_));
  NodeCP buildSource = cardinalitySource((probeIsLeft_ ? right_ : left_).node);
  if (buildSource->is(NodeType::kValues) &&
      buildSource->as<Values>()->cardinality() > 0) {
    return withOutputSubstitutions(probeWithMarker(markerWhenBuildHasRows_));
  }
  ColumnVector finalOutputColumns{key_.outputColumns};
  PlanObjectSet existenceOutputs =
      PlanObjectSet::fromObjects(key_.outputColumns);
  markerWhenBuildHasRows_->columns().forEach<Column>([&](ColumnCP column) {
    if (existenceOutputs.contains(column)) {
      return;
    }
    key_.outputColumns.insert(key_.outputColumns.end() - 1, column);
    if (!key_.sourceColumns.empty()) {
      key_.sourceColumns.insert(key_.sourceColumns.end() - 1, column);
    }
    existenceOutputs.add(column);
  });
  auto* existenceMark = optimizer::make<Column>(
      key_.outputColumns.back()->name(),
      /*relation=*/nullptr,
      key_.outputColumns.back()->value(),
      /*alias=*/nullptr);
  key_.outputColumns.back() = existenceMark;
  if (!key_.sourceColumns.empty()) {
    key_.sourceColumns.back() = existenceMark;
  }
  key_.nullAware = false;
  NodeCP existence = builder_.make<Join>(std::move(key_));
  ExprVector expressions;
  expressions.reserve(finalOutputColumns.size());
  expressions.insert(
      expressions.end(),
      existence->outputColumns().begin(),
      existence->outputColumns().begin() + finalOutputColumns.size() - 1);
  expressions.push_back(exprs_.makeIf(
      existenceMark, markerWhenBuildHasRows_, builder_.makeBoolean(false)));
  NodeCP output = builder_.make<Project>(
      {existence, std::move(expressions), std::move(finalOutputColumns)});
  outputSubstitutions_.retainVisible(output->outputColumns(), exprs_);
  return {output, std::move(outputSubstitutions_)};
}

NodeSimplifier::SimplifiedNode
NodeSimplifier::make(Join::Key key, SimplifiedNode left, SimplifiedNode right) {
  return JoinSimplifier(
             *this, std::move(key), std::move(left), std::move(right))
      .simplify();
}

} // namespace facebook::axiom::optimizer
