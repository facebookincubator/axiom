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
#include "axiom/logical_plan/PlanPrinter.h"

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
      {NodeKind::kSample, "SAMPLE"},
      {NodeKind::kOutput, "OUTPUT"},
  };
  return kNames;
}

/// Helper to serialize a vector of items to a folly::dynamic array.
template <typename T, typename Serializer>
folly::dynamic serializeVector(const std::vector<T>& items, Serializer&& fn) {
  folly::dynamic arr = folly::dynamic::array;
  for (const auto& item : items) {
    arr.push_back(fn(item));
  }
  return arr;
}

/// Helper to deserialize the "inputs" field from a folly::dynamic object.
std::vector<LogicalPlanNodePtr> deserializeNodeInputs(
    const folly::dynamic& obj,
    void* context) {
  std::vector<LogicalPlanNodePtr> result;
  if (obj.count("inputs")) {
    const auto& inputs = obj["inputs"];
    result.reserve(inputs.size());
    for (const auto& input : inputs) {
      result.push_back(
          velox::ISerializable::deserialize<LogicalPlanNode>(input, context));
    }
  }
  return result;
}

/// Helper to deserialize the "outputType" field from a folly::dynamic object.
velox::RowTypePtr deserializeOutputType(const folly::dynamic& obj) {
  return std::dynamic_pointer_cast<const velox::RowType>(
      velox::ISerializable::deserialize<velox::Type>(obj["outputType"]));
}

/// Helper to deserialize an Expr from a folly::dynamic object.
ExprPtr deserializeExpr(const folly::dynamic& obj, void* context) {
  return velox::ISerializable::deserialize<Expr>(obj, context);
}

/// Helper to deserialize expressions from a field.
std::vector<ExprPtr> deserializeExprs(
    const folly::dynamic& obj,
    const std::string& fieldName,
    void* context) {
  std::vector<ExprPtr> result;
  if (obj.count(fieldName)) {
    const auto& exprs = obj[fieldName];
    result.reserve(exprs.size());
    for (const auto& expr : exprs) {
      result.push_back(deserializeExpr(expr, context));
    }
  }
  return result;
}

/// Helper to deserialize a vector of strings.
std::vector<std::string> deserializeStringVector(
    const folly::dynamic& obj,
    const std::string& fieldName) {
  std::vector<std::string> result;
  if (obj.count(fieldName)) {
    const auto& items = obj[fieldName];
    result.reserve(items.size());
    for (const auto& item : items) {
      result.push_back(item.asString());
    }
  }
  return result;
}

} // namespace

AXIOM_DEFINE_ENUM_NAME(NodeKind, nodeKindNames)

std::string LogicalPlanNode::toString() const {
  return PlanPrinter::toText(*this);
}

folly::dynamic LogicalPlanNode::serializeBase(std::string_view name) const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = name;
  obj["id"] = id_;
  obj["outputType"] = outputType_->serialize();
  if (!inputs_.empty()) {
    obj["inputs"] = serializeVector(
        inputs_, [](const LogicalPlanNodePtr& n) { return n->serialize(); });
  }
  return obj;
}

// static
void LogicalPlanNode::registerSerDe() {
  auto& registry = velox::DeserializationWithContextRegistryForSharedPtr();
  registry.Register("ValuesNode", ValuesNode::create);
  registry.Register("TableScanNode", TableScanNode::create);
  registry.Register("FilterNode", FilterNode::create);
  registry.Register("ProjectNode", ProjectNode::create);
  registry.Register("AggregateNode", AggregateNode::create);
  registry.Register("JoinNode", JoinNode::create);
  registry.Register("SortNode", SortNode::create);
  registry.Register("LimitNode", LimitNode::create);
  registry.Register("SetNode", SetNode::create);
  registry.Register("UnnestNode", UnnestNode::create);
  registry.Register("TableWriteNode", TableWriteNode::create);
  registry.Register("SampleNode", SampleNode::create);
  registry.Register("OutputNode", OutputNode::create);
}

folly::dynamic ValuesNode::serialize() const {
  auto obj = serializeBase("ValuesNode");
  obj["cardinality"] = cardinality_;

  // Serialize the data variant
  if (std::holds_alternative<Variants>(data_)) {
    obj["dataType"] = "Variants";
    obj["data"] = serializeVector(
        std::get<Variants>(data_),
        [](const velox::Variant& v) { return v.serialize(); });
  } else if (std::holds_alternative<Vectors>(data_)) {
    obj["dataType"] = "Vectors";
    // Serialize each RowVector as an array of variants to preserve batch
    // structure
    obj["data"] = serializeVector(
        std::get<Vectors>(data_), [](const velox::RowVectorPtr& vector) {
          return serializeVector(
              vector->toVariants(),
              [](const velox::Variant& v) { return v.serialize(); });
        });
  } else {
    obj["dataType"] = "Exprs";
    folly::dynamic rows = folly::dynamic::array;
    for (const auto& row : std::get<Exprs>(data_)) {
      rows.push_back(serializeVector(
          row, [](const ExprPtr& e) { return e->serialize(); }));
    }
    obj["data"] = std::move(rows);
  }
  return obj;
}

// static
LogicalPlanNodePtr ValuesNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto id = obj["id"].asString();
  auto outputType = deserializeOutputType(obj);
  auto dataType = obj["dataType"].asString();

  if (dataType == "Variants") {
    Variants rows;
    for (const auto& v : obj["data"]) {
      rows.push_back(velox::Variant::create(v));
    }
    return std::make_shared<ValuesNode>(
        std::move(id), outputType, std::move(rows));
  } else if (dataType == "Vectors") {
    VELOX_CHECK_NOT_NULL(
        context,
        "Memory pool required in context for deserializing ValuesNode with Vectors");
    auto* pool = static_cast<velox::memory::MemoryPool*>(context);
    const auto& data = obj["data"];
    Vectors vectors;
    vectors.reserve(data.size());
    for (const auto& batch : data) {
      std::vector<velox::Variant> variants;
      variants.reserve(batch.size());
      for (const auto& v : batch) {
        variants.push_back(velox::Variant::create(v));
      }
      auto vector = std::dynamic_pointer_cast<velox::RowVector>(
          velox::BaseVector::createFromVariants(outputType, variants, pool));
      vectors.push_back(std::move(vector));
    }
    return std::make_shared<ValuesNode>(std::move(id), std::move(vectors));
  } else if (dataType == "Exprs") {
    const auto& data = obj["data"];
    Exprs rows;
    rows.reserve(data.size());
    for (const auto& row : data) {
      std::vector<ExprPtr> exprs;
      exprs.reserve(row.size());
      for (const auto& e : row) {
        exprs.push_back(deserializeExpr(e, context));
      }
      rows.push_back(std::move(exprs));
    }
    return std::make_shared<ValuesNode>(
        std::move(id), outputType, std::move(rows));
  } else {
    VELOX_FAIL("Unknown ValuesNode dataType: {}", dataType);
  }
}

folly::dynamic TableScanNode::serialize() const {
  auto obj = serializeBase("TableScanNode");
  obj["connectorId"] = connectorId_;
  obj["tableName"] = tableName_;
  obj["columnNames"] =
      serializeVector(columnNames_, [](const std::string& s) { return s; });
  return obj;
}

// static
LogicalPlanNodePtr TableScanNode::create(
    const folly::dynamic& obj,
    void* /*context*/) {
  return std::make_shared<TableScanNode>(
      obj["id"].asString(),
      deserializeOutputType(obj),
      obj["connectorId"].asString(),
      obj["tableName"].asString(),
      deserializeStringVector(obj, "columnNames"));
}

folly::dynamic FilterNode::serialize() const {
  auto obj = serializeBase("FilterNode");
  obj["predicate"] = predicate_->serialize();
  return obj;
}

// static
LogicalPlanNodePtr FilterNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto inputs = deserializeNodeInputs(obj, context);
  VELOX_CHECK_EQ(inputs.size(), 1);
  return std::make_shared<FilterNode>(
      obj["id"].asString(),
      inputs[0],
      deserializeExpr(obj["predicate"], context));
}

folly::dynamic ProjectNode::serialize() const {
  auto obj = serializeBase("ProjectNode");
  obj["names"] =
      serializeVector(names_, [](const std::string& s) { return s; });
  obj["expressions"] = serializeVector(
      expressions_, [](const ExprPtr& e) { return e->serialize(); });
  return obj;
}

// static
LogicalPlanNodePtr ProjectNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto inputs = deserializeNodeInputs(obj, context);
  VELOX_CHECK_EQ(inputs.size(), 1);
  return std::make_shared<ProjectNode>(
      obj["id"].asString(),
      inputs[0],
      deserializeStringVector(obj, "names"),
      deserializeExprs(obj, "expressions", context));
}

folly::dynamic AggregateNode::serialize() const {
  auto obj = serializeBase("AggregateNode");
  obj["groupingKeys"] = serializeVector(
      groupingKeys_, [](const ExprPtr& e) { return e->serialize(); });
  obj["groupingSets"] =
      serializeVector(groupingSets_, [](const GroupingSet& gs) {
        folly::dynamic arr = folly::dynamic::array;
        for (const auto& idx : gs) {
          arr.push_back(idx);
        }
        return arr;
      });
  obj["aggregates"] = serializeVector(
      aggregates_, [](const AggregateExprPtr& e) { return e->serialize(); });
  obj["outputNames"] =
      serializeVector(outputNames_, [](const std::string& s) { return s; });
  return obj;
}

// static
LogicalPlanNodePtr AggregateNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto inputs = deserializeNodeInputs(obj, context);
  VELOX_CHECK_EQ(inputs.size(), 1);

  std::vector<ExprPtr> groupingKeys =
      deserializeExprs(obj, "groupingKeys", context);

  std::vector<GroupingSet> groupingSets;
  if (obj.count("groupingSets")) {
    for (const auto& gs : obj["groupingSets"]) {
      GroupingSet set;
      for (const auto& idx : gs) {
        set.push_back(idx.asInt());
      }
      groupingSets.push_back(std::move(set));
    }
  }

  std::vector<AggregateExprPtr> aggregates;
  if (obj.count("aggregates")) {
    for (const auto& e : obj["aggregates"]) {
      aggregates.push_back(
          std::dynamic_pointer_cast<const AggregateExpr>(
              deserializeExpr(e, context)));
    }
  }

  return std::make_shared<AggregateNode>(
      obj["id"].asString(),
      inputs[0],
      std::move(groupingKeys),
      std::move(groupingSets),
      std::move(aggregates),
      deserializeStringVector(obj, "outputNames"));
}

folly::dynamic JoinNode::serialize() const {
  auto obj = serializeBase("JoinNode");
  obj["joinType"] = JoinTypeName::toName(joinType_);
  if (condition_) {
    obj["condition"] = condition_->serialize();
  }
  return obj;
}

// static
LogicalPlanNodePtr JoinNode::create(const folly::dynamic& obj, void* context) {
  auto inputs = deserializeNodeInputs(obj, context);
  VELOX_CHECK_EQ(inputs.size(), 2);
  ExprPtr condition = nullptr;
  if (obj.count("condition")) {
    condition = deserializeExpr(obj["condition"], context);
  }
  return std::make_shared<JoinNode>(
      obj["id"].asString(),
      inputs[0],
      inputs[1],
      JoinTypeName::toJoinType(obj["joinType"].asString()),
      std::move(condition));
}

folly::dynamic SortNode::serialize() const {
  auto obj = serializeBase("SortNode");
  obj["ordering"] = serializeVector(
      ordering_, [](const SortingField& f) { return f.serialize(); });
  return obj;
}

// static
LogicalPlanNodePtr SortNode::create(const folly::dynamic& obj, void* context) {
  auto inputs = deserializeNodeInputs(obj, context);
  VELOX_CHECK_EQ(inputs.size(), 1);
  std::vector<SortingField> ordering;
  if (obj.count("ordering")) {
    for (const auto& f : obj["ordering"]) {
      ordering.push_back(SortingField::deserialize(f, context));
    }
  }
  return std::make_shared<SortNode>(
      obj["id"].asString(), inputs[0], std::move(ordering));
}

folly::dynamic LimitNode::serialize() const {
  auto obj = serializeBase("LimitNode");
  obj["offset"] = offset_;
  obj["count"] = count_;
  return obj;
}

// static
LogicalPlanNodePtr LimitNode::create(const folly::dynamic& obj, void* context) {
  auto inputs = deserializeNodeInputs(obj, context);
  VELOX_CHECK_EQ(inputs.size(), 1);
  return std::make_shared<LimitNode>(
      obj["id"].asString(),
      inputs[0],
      obj["offset"].asInt(),
      obj["count"].asInt());
}

folly::dynamic SetNode::serialize() const {
  auto obj = serializeBase("SetNode");
  obj["operation"] = SetOperationName::toName(operation_);
  return obj;
}

// static
LogicalPlanNodePtr SetNode::create(const folly::dynamic& obj, void* context) {
  auto inputs = deserializeNodeInputs(obj, context);
  return std::make_shared<SetNode>(
      obj["id"].asString(),
      inputs,
      SetOperationName::toSetOperation(obj["operation"].asString()));
}

folly::dynamic UnnestNode::serialize() const {
  auto obj = serializeBase("UnnestNode");
  obj["unnestExpressions"] = serializeVector(
      unnestExpressions_, [](const ExprPtr& e) { return e->serialize(); });
  obj["unnestedNames"] = serializeVector(
      unnestedNames_, [](const std::vector<std::string>& names) {
        folly::dynamic arr = folly::dynamic::array;
        for (const auto& name : names) {
          arr.push_back(name);
        }
        return arr;
      });
  if (ordinalityName_.has_value()) {
    obj["ordinalityName"] = ordinalityName_.value();
  }
  obj["flattenArrayOfRows"] = flattenArrayOfRows_;
  return obj;
}

// static
LogicalPlanNodePtr UnnestNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto inputs = deserializeNodeInputs(obj, context);
  VELOX_CHECK_EQ(inputs.size(), 1);

  std::vector<std::vector<std::string>> unnestedNames;
  if (obj.count("unnestedNames")) {
    for (const auto& names : obj["unnestedNames"]) {
      std::vector<std::string> nameVec;
      for (const auto& name : names) {
        nameVec.push_back(name.asString());
      }
      unnestedNames.push_back(std::move(nameVec));
    }
  }

  std::optional<std::string> ordinalityName;
  if (obj.count("ordinalityName")) {
    ordinalityName = obj["ordinalityName"].asString();
  }

  return std::make_shared<UnnestNode>(
      obj["id"].asString(),
      inputs[0],
      deserializeExprs(obj, "unnestExpressions", context),
      std::move(unnestedNames),
      std::move(ordinalityName),
      obj["flattenArrayOfRows"].asBool());
}

folly::dynamic TableWriteNode::serialize() const {
  auto obj = serializeBase("TableWriteNode");
  obj["connectorId"] = connectorId_;
  obj["tableName"] = tableName_;
  obj["writeKind"] = WriteKindName::toName(writeKind_);
  obj["columnNames"] =
      serializeVector(columnNames_, [](const std::string& s) { return s; });
  obj["columnExpressions"] = serializeVector(
      columnExpressions_, [](const ExprPtr& e) { return e->serialize(); });
  if (!options_.empty()) {
    folly::dynamic optionsObj = folly::dynamic::object;
    for (const auto& [key, value] : options_) {
      optionsObj[key] = value;
    }
    obj["options"] = std::move(optionsObj);
  }
  return obj;
}

// static
LogicalPlanNodePtr TableWriteNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto inputs = deserializeNodeInputs(obj, context);
  VELOX_CHECK_EQ(inputs.size(), 1);

  folly::F14FastMap<std::string, std::string> options;
  if (obj.count("options")) {
    for (const auto& [key, value] : obj["options"].items()) {
      options[key.asString()] = value.asString();
    }
  }

  return std::make_shared<TableWriteNode>(
      obj["id"].asString(),
      inputs[0],
      obj["connectorId"].asString(),
      obj["tableName"].asString(),
      WriteKindName::toWriteKind(obj["writeKind"].asString()),
      deserializeStringVector(obj, "columnNames"),
      deserializeExprs(obj, "columnExpressions", context),
      std::move(options));
}

folly::dynamic SampleNode::serialize() const {
  auto obj = serializeBase("SampleNode");
  obj["percentage"] = percentage_->serialize();
  obj["sampleMethod"] = SampleNode::toName(sampleMethod_);
  return obj;
}

// static
LogicalPlanNodePtr SampleNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto inputs = deserializeNodeInputs(obj, context);
  VELOX_CHECK_EQ(inputs.size(), 1);
  return std::make_shared<SampleNode>(
      obj["id"].asString(),
      inputs[0],
      deserializeExpr(obj["percentage"], context),
      SampleNode::toSampleMethod(obj["sampleMethod"].asString()));
}

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

ValuesNode::ValuesNode(std::string id, velox::RowTypePtr rowType, Variants rows)
    : LogicalPlanNode{NodeKind::kValues, std::move(id), {}, std::move(rowType)},
      cardinality_{rows.size()},
      data_{std::move(rows)} {
  UniqueNameChecker::check(outputType_->names());

  for (const auto& row : std::get<Variants>(data_)) {
    VELOX_USER_CHECK(
        row.isTypeCompatible(outputType_),
        "All rows should have compatible types: {} vs. {}",
        row.inferType()->toString(),
        outputType_->toString());
  }
}

ValuesNode::ValuesNode(std::string id, Vectors values)
    : LogicalPlanNode{NodeKind::kValues, std::move(id), {}, getType(values)},
      cardinality_{[&] {
        uint64_t cardinality = 0;
        for (const auto& value : values) {
          VELOX_USER_CHECK_NOT_NULL(value);
          VELOX_USER_CHECK(
              outputType_->equivalent(*value->type()),
              "All values should have equivalent types: {} vs. {}",
              value->type()->toString(),
              outputType_->toString());
          cardinality += value->size();
        }
        return cardinality;
      }()},
      data_{std::move(values)} {
  UniqueNameChecker::check(outputType_->names());
}

ValuesNode::ValuesNode(
    std::string id,
    velox::RowTypePtr rowType,
    std::vector<std::vector<ExprPtr>> rows)
    : LogicalPlanNode{NodeKind::kValues, std::move(id), {}, std::move(rowType)},
      cardinality_{rows.size()},
      data_{std::move(rows)} {
  UniqueNameChecker::check(outputType_->names());

  for (const auto& row : std::get<Exprs>(data_)) {
    VELOX_USER_CHECK_EQ(row.size(), outputType_->size());
    for (auto i = 0; i < row.size(); ++i) {
      const auto& type = row[i]->type();
      VELOX_USER_CHECK(
          outputType_->childAt(i)->equivalent(*type),
          "All values should have equivalent types: {} vs. {}",
          type->toString(),
          outputType_->toString());
    }
  }
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
    folly::F14FastMap<std::string, std::string> options)
    : LogicalPlanNode{NodeKind::kTableWrite, std::move(id), {std::move(input)}, velox::ROW("rows", velox::BIGINT())},
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
      writeKind_ == WriteKind::kCreate || writeKind == WriteKind::kInsert ||
          options_.empty(),
      "Options are supported only for create or insert");
}

void TableWriteNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

namespace {

const auto& writeKindNames() {
  static const folly::F14FastMap<WriteKind, std::string_view> kNames = {
      {WriteKind::kCreate, "CREATE"},
      {WriteKind::kInsert, "INSERT"},
      {WriteKind::kUpdate, "UPDATE"},
      {WriteKind::kDelete, "DELETE"},
  };

  return kNames;
}

} // namespace

AXIOM_DEFINE_ENUM_NAME(WriteKind, writeKindNames);

namespace {

const auto& sampleMethodNames() {
  static const folly::F14FastMap<SampleNode::SampleMethod, std::string_view>
      kNames = {
          {SampleNode::SampleMethod::kBernoulli, "BERNOULLI"},
          {
              SampleNode::SampleMethod::kSystem,
              "SYSTEM",
          }};

  return kNames;
}

} // namespace

AXIOM_DEFINE_EMBEDDED_ENUM_NAME(SampleNode, SampleMethod, sampleMethodNames);

void SampleNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

namespace {
velox::RowTypePtr makeOutputType(
    const LogicalPlanNode& input,
    const std::vector<OutputNode::Entry>& entries) {
  const auto& inputType = input.outputType();

  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;
  names.reserve(entries.size());
  types.reserve(entries.size());

  for (const auto& entry : entries) {
    VELOX_USER_CHECK_GE(entry.index, 0);
    VELOX_USER_CHECK_LT(
        entry.index, inputType->size(), "Output column index is out of range");
    names.push_back(entry.name);
    types.push_back(inputType->childAt(entry.index));
  }

  return velox::ROW(std::move(names), std::move(types));
}
} // namespace

OutputNode::OutputNode(
    std::string id,
    LogicalPlanNodePtr input,
    std::vector<Entry> entries)
    : LogicalPlanNode{NodeKind::kOutput, std::move(id), {input}, makeOutputType(*input, entries)},
      entries_{std::move(entries)} {
  VELOX_USER_CHECK(
      !entries_.empty(), "Output node must have at least one entry");
}

void OutputNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

folly::dynamic OutputNode::serialize() const {
  auto obj = serializeBase("OutputNode");
  obj["entries"] = serializeVector(entries_, [](const Entry& entry) {
    folly::dynamic obj = folly::dynamic::object;
    obj["index"] = entry.index;
    obj["name"] = entry.name;
    return obj;
  });
  return obj;
}

// static
LogicalPlanNodePtr OutputNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto inputs = deserializeNodeInputs(obj, context);
  VELOX_CHECK_EQ(inputs.size(), 1);

  std::vector<OutputNode::Entry> entries;
  if (obj.count("entries")) {
    for (const auto& entry : obj["entries"]) {
      entries.push_back(
          {static_cast<int32_t>(entry["index"].asInt()),
           entry["name"].asString()});
    }
  }

  return std::make_shared<OutputNode>(
      obj["id"].asString(), inputs[0], std::move(entries));
}

} // namespace facebook::axiom::logical_plan
