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

namespace facebook::axiom::logical_plan {

namespace {
const auto& nodeKindNames() {
  static const folly::F14FastMap<NodeKind, std::string_view> kNames = {
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
      {NodeKind::kTableWrite, "TABLE_WRITE"},
  };
  return kNames;
}
} // namespace

AXIOM_DEFINE_ENUM_NAME(NodeKind, nodeKindNames)

namespace {

class UniqueNameChecker {
 public:
  static void check(std::span<const std::string> names) {
    UniqueNameChecker{}.addAll(names);
  }

 private:
  void add(std::string_view name) {
    VELOX_USER_CHECK(!name.empty(), "Name must not be empty");
    VELOX_USER_CHECK(names_.emplace(name).second, "Duplicate name: {}", name);
  }

  void addAll(std::span<const std::string> names) {
    for (const auto& name : names) {
      add(name);
    }
  }

  folly::F14FastSet<std::string_view> names_;
};

velox::RowTypePtr getType(const std::vector<velox::RowVectorPtr>& values) {
  VELOX_USER_CHECK(!values.empty(), "Values must not be empty");
  return values.front()->rowType();
}

} // namespace

ValuesNode::ValuesNode(std::string id, velox::RowTypePtr rowType, Rows rows)
    : LogicalPlanNode{NodeKind::kValues, std::move(id), {}, std::move(rowType)},
      cardinality_{rows.size()},
      data_{std::move(rows)} {
  UniqueNameChecker::check(outputType_->names());

  for (const auto& row : std::get<Rows>(data_)) {
    VELOX_USER_CHECK(
        row.isTypeCompatible(outputType_),
        "All rows should have compatible types: {} vs. {}",
        row.inferType()->toString(),
        outputType_->toString());
  }
}

ValuesNode::ValuesNode(std::string id, Values values)
    : LogicalPlanNode{NodeKind::kValues, std::move(id), {}, getType(values)},
      cardinality_{[&] {
        uint64_t cardinality = 0;
        for (const auto& value : values) {
          VELOX_USER_CHECK_NOT_NULL(value);
          VELOX_USER_CHECK(
              outputType_->equivalent(*value->type()),
              "All values should have equivalent types: {} vs. {}",
              outputType_->toString(),
              value->type()->toString());
          cardinality += value->size();
        }
        return cardinality;
      }()},
      data_{std::move(values)} {
  UniqueNameChecker::check(outputType_->names());
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
velox::RowTypePtr ProjectNode::makeOutputType(
    const std::vector<std::string>& names,
    const std::vector<ExprPtr>& expressions) {
  VELOX_USER_CHECK_EQ(names.size(), expressions.size());

  UniqueNameChecker::check(names);

  std::vector<velox::TypePtr> types;
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
velox::RowTypePtr AggregateNode::makeOutputType(
    const std::vector<ExprPtr>& groupingKeys,
    const std::vector<GroupingSet>& groupingSets,
    const std::vector<AggregateExprPtr>& aggregates,
    const std::vector<std::string>& outputNames) {
  const auto size =
      groupingKeys.size() + aggregates.size() + (groupingSets.empty() ? 0 : 1);

  VELOX_USER_CHECK_EQ(outputNames.size(), size);

  std::vector<std::string> names = outputNames;
  std::vector<velox::TypePtr> types;
  types.reserve(size);

  for (const auto& groupingKey : groupingKeys) {
    types.push_back(groupingKey->type());
  }

  for (const auto& aggregate : aggregates) {
    types.push_back(aggregate->type());
  }

  if (!groupingSets.empty()) {
    types.push_back(velox::BIGINT());
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
const auto& joinTypeNames() {
  static const folly::F14FastMap<JoinType, std::string_view> kNames = {
      {JoinType::kInner, "INNER"},
      {JoinType::kLeft, "LEFT"},
      {JoinType::kRight, "RIGHT"},
      {JoinType::kFull, "FULL"},
  };
  return kNames;
}
} // namespace

AXIOM_DEFINE_ENUM_NAME(JoinType, joinTypeNames)

// static
velox::RowTypePtr JoinNode::makeOutputType(
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
const auto& setOperationNames() {
  static const folly::F14FastMap<SetOperation, std::string_view> kNames = {
      {SetOperation::kUnion, "UNION"},
      {SetOperation::kUnionAll, "UNION ALL"},
      {SetOperation::kIntersect, "INTERSECT"},
      {SetOperation::kExcept, "EXCEPT"},
  };
  return kNames;
}
} // namespace

AXIOM_DEFINE_ENUM_NAME(SetOperation, setOperationNames)

// static
velox::RowTypePtr SetNode::makeOutputType(
    const std::vector<LogicalPlanNodePtr>& inputs) {
  VELOX_USER_CHECK_GE(
      inputs.size(), 2, "Set operation requires at least 2 inputs");

  const auto firstRowType = inputs[0]->outputType();

  for (size_t i = 1; i < inputs.size(); ++i) {
    const auto& rowType = inputs[i]->outputType();

    // The names are different, but types must be the same.
    VELOX_USER_CHECK(
        firstRowType->equivalent(*rowType),
        "Output schemas of all inputs to a Set operation must match");

    // Individual column types must match exactly.
    for (uint32_t j = 0; j < firstRowType->size(); ++j) {
      VELOX_USER_CHECK(
          *firstRowType->childAt(j) == *rowType->childAt(j),
          "Output schemas of all inputs to a Set operation must match: {} vs. {} at {}.{}",
          firstRowType->childAt(j)->toSummaryString(),
          rowType->childAt(j)->toSummaryString(),
          j,
          firstRowType->nameOf(j));
    }
  }

  return firstRowType;
}

void SetNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

// static
velox::RowTypePtr UnnestNode::makeOutputType(
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

  velox::RowTypePtr inputType;
  if (input != nullptr) {
    inputType = input->outputType();
  } else {
    inputType = velox::ROW({});
  }

  auto size = inputType->size();
  for (const auto& names : unnestedNames) {
    size += names.size();
  }

  std::vector<std::string> names;
  names.reserve(size);

  std::vector<velox::TypePtr> types;
  types.reserve(size);

  names = inputType->names();
  types = inputType->children();

  const auto numUnnest = unnestExpressions.size();
  for (size_t i = 0; i < numUnnest; ++i) {
    const auto& type = unnestExpressions[i]->type();

    VELOX_USER_CHECK(
        type->isArray() || type->isMap(),
        "A column to unnest must be an ARRAY or a MAP: {}",
        type->toString());

    const auto& outputNames = unnestedNames[i];
    const auto& numOutput = outputNames.size();

    if (flattenArrayOfRows && type->isArray() && type->childAt(0)->isRow()) {
      const auto& rowType = type->childAt(0);
      VELOX_USER_CHECK_EQ(numOutput, rowType->size());

      for (size_t j = 0; j < numOutput; ++j) {
        names.push_back(outputNames[j]);
        types.push_back(rowType->childAt(j));
      }
    } else {
      VELOX_USER_CHECK_EQ(numOutput, type->size());
      for (size_t j = 0; j < numOutput; ++j) {
        names.push_back(outputNames[j]);
        types.push_back(type->childAt(j));
      }
    }
  }

  if (ordinalityName.has_value()) {
    names.push_back(ordinalityName.value());
    types.push_back(velox::BIGINT());
  }

  UniqueNameChecker::check(names);

  return ROW(std::move(names), std::move(types));
}

void UnnestNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

TableWriteNode::TableWriteNode(
    std::string id,
    LogicalPlanNodePtr input,
    std::string connectorId,
    std::string tableName,
    WriteKind writeKind,
    std::vector<std::string> columnNames,
    std::vector<ExprPtr> columnExpressions,
    velox::RowTypePtr outputType,
    folly::F14FastMap<std::string, std::string> options)
    : LogicalPlanNode{NodeKind::kTableWrite, std::move(id), {std::move(input)}, std::move(outputType)},
      connectorId_{std::move(connectorId)},
      tableName_{std::move(tableName)},
      writeKind_{writeKind},
      columnNames_{std::move(columnNames)},
      columnExpressions_{std::move(columnExpressions)},
      options_{std::move(options)} {
  VELOX_USER_CHECK(!connectorId_.empty());
  VELOX_USER_CHECK(!tableName_.empty());
  VELOX_USER_CHECK_EQ(columnNames_.size(), columnExpressions_.size());
  UniqueNameChecker::check(columnNames_);
  VELOX_USER_CHECK(
      writeKind_ == WriteKind::kCreate || options_.empty(),
      "Options can be needed only for create table");
}

void TableWriteNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

namespace {

folly::F14FastMap<WriteKind, std::string_view> writeKindNames() {
  static const folly::F14FastMap<WriteKind, std::string_view> kNames = {
      {WriteKind::kCreate, "CREATE"},
      {WriteKind::kInsert, "INSERT"},
      {WriteKind::kUpdate, "UPDATE"},
      {WriteKind::kDelete, "DELETE"},
  };

  return kNames;
}

} // namespace

VELOX_DEFINE_ENUM_NAME(WriteKind, writeKindNames);

} // namespace facebook::axiom::logical_plan
