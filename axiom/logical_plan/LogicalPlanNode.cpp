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

#include "axiom/logical_plan/LogicalPlanNode.h"
#include "axiom/logical_plan/PlanNodeVisitor.h"

namespace facebook::velox::logical_plan {

namespace {
folly::F14FastMap<NodeKind, std::string> nodeKindNames() {
  return {
      {NodeKind::kValues, "VALUES"},
      {NodeKind::kTableScan, "TABLE_SCAN"},
      {NodeKind::kFilter, "FILTER"},
      {NodeKind::kProject, "PROJECT"},
      {NodeKind::kAggregate, "AGGREGATE"},
      {NodeKind::kJoin, "JOIN"},
      {NodeKind::kSort, "SORT"},
      {NodeKind::kLimit, "LIMIT"},
      {NodeKind::kSet, "SET"},
      {NodeKind::kUnnest, "UNNEST"},
  };
}
} // namespace

VELOX_DEFINE_ENUM_NAME(NodeKind, nodeKindNames)

namespace {

class UniqueNameChecker {
 public:
  const std::string& add(const std::string& name) {
    VELOX_USER_CHECK(!name.empty(), "Name must not be empty");
    VELOX_USER_CHECK(names_.insert(name).second, "Duplicate name: {}", name);
    return name;
  }

  void addAll(const std::vector<std::string>& names) {
    for (const auto& name : names) {
      add(name);
    }
  }

  static void check(const std::vector<std::string>& names) {
    UniqueNameChecker{}.addAll(names);
  }

 private:
  std::unordered_set<std::string> names_;
};

RowTypePtr getType(const std::vector<RowVectorPtr>& values) {
  VELOX_USER_CHECK(!values.empty(), "Values must not be empty");
  return values.front()->rowType();
}

} // namespace

ValuesNode::ValuesNode(
    const std::string& id,
    const RowTypePtr& rowType,
    Rows rows)
    : LogicalPlanNode{NodeKind::kValues, id, {}, rowType},
      cardinality_{rows.size()},
      data_{std::move(rows)} {
  UniqueNameChecker::check(rowType->names());

  for (const auto& row : std::get<Rows>(data_)) {
    VELOX_USER_CHECK(
        row.isTypeCompatible(rowType),
        "All rows should have compatible types: {} vs. {}",
        row.inferType()->toString(),
        rowType->toString());
  }
}

ValuesNode::ValuesNode(const std::string& id, Values values)
    : LogicalPlanNode{NodeKind::kValues, id, {}, getType(values)},
      cardinality_{[&] {
        uint64_t cardinality = 0;
        for (const auto& value : values) {
          VELOX_USER_CHECK_NOT_NULL(value);
          VELOX_USER_CHECK(
              outputType()->equivalent(*value->type()),
              "All values should have equivalent types: {} vs. {}",
              outputType()->toString(),
              value->type()->toString());
          cardinality += value->size();
        }
        return cardinality;
      }()},
      data_{std::move(values)} {
  UniqueNameChecker::check(outputType()->names());
}

void ValuesNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

void TableScanNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

void FilterNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

// static
RowTypePtr ProjectNode::makeOutputType(
    const std::vector<std::string>& names,
    const std::vector<ExprPtr>& expressions) {
  VELOX_USER_CHECK_EQ(names.size(), expressions.size());

  UniqueNameChecker::check(names);

  std::vector<TypePtr> types;
  types.reserve(names.size());
  for (const auto& expression : expressions) {
    VELOX_USER_CHECK_NOT_NULL(expression);
    types.push_back(expression->type());
  }

  return ROW(names, std::move(types));
}

void ProjectNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

// static
RowTypePtr AggregateNode::makeOutputType(
    const std::vector<ExprPtr>& groupingKeys,
    const std::vector<GroupingSet>& groupingSets,
    const std::vector<AggregateExprPtr>& aggregates,
    const std::vector<std::string>& outputNames) {
  const auto size =
      groupingKeys.size() + aggregates.size() + (groupingSets.empty() ? 0 : 1);

  VELOX_USER_CHECK_EQ(outputNames.size(), size);

  std::vector<std::string> names = outputNames;
  std::vector<TypePtr> types;
  types.reserve(size);

  for (const auto& groupingKey : groupingKeys) {
    types.push_back(groupingKey->type());
  }

  for (const auto& aggregate : aggregates) {
    types.push_back(aggregate->type());
  }

  if (!groupingSets.empty()) {
    types.push_back(BIGINT());
  }

  UniqueNameChecker::check(names);

  return ROW(std::move(names), std::move(types));
}

void AggregateNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

namespace {
folly::F14FastMap<JoinType, std::string> joinTypeNames() {
  return {
      {JoinType::kInner, "INNER"},
      {JoinType::kLeft, "LEFT"},
      {JoinType::kRight, "RIGHT"},
      {JoinType::kFull, "FULL"},
  };
}
} // namespace

VELOX_DEFINE_ENUM_NAME(JoinType, joinTypeNames)

// static
RowTypePtr JoinNode::makeOutputType(
    const LogicalPlanNodePtr& left,
    const LogicalPlanNodePtr& right) {
  auto type = left->outputType()->unionWith(right->outputType());

  UniqueNameChecker::check(type->names());

  return type;
}

void JoinNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

void SortNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

void LimitNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

namespace {
folly::F14FastMap<SetOperation, std::string> setOperationNames() {
  return {
      {SetOperation::kUnion, "UNION"},
      {SetOperation::kUnionAll, "UNION ALL"},
      {SetOperation::kIntersect, "INTERSECT"},
      {SetOperation::kExcept, "EXCEPT"},
  };
}
} // namespace

VELOX_DEFINE_ENUM_NAME(SetOperation, setOperationNames)

SetNode::SetNode(
    const std::string& id,
    const std::vector<LogicalPlanNodePtr>& inputs,
    SetOperation operation)
    : LogicalPlanNode(NodeKind::kSet, id, inputs, inputs.at(0)->outputType()),
      operation_{operation} {
  VELOX_USER_CHECK_GE(
      inputs.size(), 2, "Set operation requires at least 2 inputs");

  const auto firstRowType = inputs.at(0)->outputType();

  for (auto i = 1; i < inputs.size(); ++i) {
    const auto& rowType = inputs.at(i)->outputType();

    // The names are different, but types must be the same.
    VELOX_USER_CHECK(
        firstRowType->equivalent(*rowType),
        "Output schemas of all inputs to a Set operation must match");

    // Individual column types must match exactly.
    for (auto j = 0; j < firstRowType->size(); ++j) {
      VELOX_USER_CHECK(
          *firstRowType->childAt(j) == *rowType->childAt(j),
          "Output schemas of all inputs to a Set operation must match: {} vs. {} at {}.{}",
          firstRowType->childAt(j)->toSummaryString(),
          rowType->childAt(j)->toSummaryString(),
          j,
          firstRowType->nameOf(j));
    }
  }
}

void SetNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

// static
RowTypePtr UnnestNode::makeOutputType(
    const LogicalPlanNodePtr& input,
    const std::vector<ExprPtr>& unnestExpressions,
    const std::vector<std::vector<std::string>>& unnestedNames,
    const std::optional<std::string>& ordinalityName,
    bool flattenArrayOfRows) {
  VELOX_USER_CHECK_EQ(unnestedNames.size(), unnestExpressions.size());
  VELOX_USER_CHECK_GT(
      unnestedNames.size(),
      0,
      "Unnest requires at least one ARRAY or MAP to expand");

  auto size = input->outputType()->size();
  for (const auto& names : unnestedNames) {
    size += names.size();
  }

  std::vector<std::string> names;
  names.reserve(size);

  std::vector<TypePtr> types;
  types.reserve(size);

  names = input->outputType()->names();
  types = input->outputType()->children();

  const auto numUnnest = unnestExpressions.size();
  for (auto i = 0; i < numUnnest; ++i) {
    const auto& type = unnestExpressions.at(i)->type();

    VELOX_USER_CHECK(
        type->isArray() || type->isMap(),
        "A column to unnest must be an ARRAY or a MAP: {}",
        type->toString());

    const auto& outputNames = unnestedNames.at(i);
    const auto& numOutput = outputNames.size();

    if (flattenArrayOfRows && type->isArray() && type->childAt(0)->isRow()) {
      const auto& rowType = type->childAt(0);
      VELOX_USER_CHECK_EQ(numOutput, rowType->size());

      for (auto j = 0; j < numOutput; ++j) {
        names.push_back(outputNames.at(j));
        types.push_back(rowType->childAt(j));
      }
    } else {
      VELOX_USER_CHECK_EQ(numOutput, type->size());
      for (auto j = 0; j < numOutput; ++j) {
        names.push_back(outputNames.at(j));
        types.push_back(type->childAt(j));
      }
    }
  }

  if (ordinalityName.has_value()) {
    names.push_back(ordinalityName.value());
    types.push_back(BIGINT());
  }

  UniqueNameChecker::check(names);

  return ROW(std::move(names), std::move(types));
}

void UnnestNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

} // namespace facebook::velox::logical_plan
