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

#include <folly/container/F14Set.h>

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
      {NodeKind::kFixedPoint, "FIXED_POINT"},
      {NodeKind::kRecursiveReference, "RECURSIVE_REFERENCE"},
      {NodeKind::kLateralJoin, "LATERAL_JOIN"},
      {NodeKind::kGraphMatch, "GRAPH_MATCH"},
      {NodeKind::kVectorSearch, "VECTOR_SEARCH"},
  };
  return kNames;
}

// Visits the plan of each subquery in a node's expressions.
class SubqueryPlanVisitor : public PlanNodeVisitor {
 public:
  struct Context : public PlanNodeVisitorContext {
    explicit Context(folly::FunctionRef<void(const LogicalPlanNode&)> visit)
        : visit{visit} {}

    folly::FunctionRef<void(const LogicalPlanNode&)> visit;
  };

  void visit(const ValuesNode& node, PlanNodeVisitorContext& context)
      const override {
    if (const auto* rows = std::get_if<ValuesNode::Exprs>(&node.data())) {
      for (const auto& row : *rows) {
        visitExpressions(row, context);
      }
    }
  }

  void visit(const TableScanNode& /*node*/, PlanNodeVisitorContext& /*context*/)
      const override {}

  void visit(const FilterNode& node, PlanNodeVisitorContext& context)
      const override {
    visitExpression(*node.predicate(), context);
  }

  void visit(const ProjectNode& node, PlanNodeVisitorContext& context)
      const override {
    visitExpressions(node.expressions(), context);
  }

  void visit(const AggregateNode& node, PlanNodeVisitorContext& context)
      const override {
    visitExpressions(node.groupingKeys(), context);
    for (const auto& aggregate : node.aggregates()) {
      visitExpression(*aggregate, context);
    }
  }

  void visit(const JoinNode& node, PlanNodeVisitorContext& context)
      const override {
    visitOptionalExpression(node.condition(), context);
  }

  void visit(const LateralJoinNode& node, PlanNodeVisitorContext& context)
      const override {
    visitOptionalExpression(node.condition(), context);
  }

  void visit(const VectorSearchNode& node, PlanNodeVisitorContext& context)
      const override {}

  void visit(const SortNode& node, PlanNodeVisitorContext& context)
      const override {
    for (const auto& field : node.ordering()) {
      visitExpression(*field.expression, context);
    }
  }

  void visit(const LimitNode& node, PlanNodeVisitorContext& context)
      const override {}

  void visit(const SetNode& node, PlanNodeVisitorContext& context)
      const override {}

  void visit(const UnnestNode& node, PlanNodeVisitorContext& context)
      const override {
    visitExpressions(node.unnestExpressions(), context);
  }

  void visit(const TableWriteNode& node, PlanNodeVisitorContext& context)
      const override {
    visitExpressions(node.columnExpressions(), context);
  }

  void visit(const SampleNode& node, PlanNodeVisitorContext& context)
      const override {
    visitExpression(*node.percentage(), context);
  }

  void visit(const OutputNode& node, PlanNodeVisitorContext& context)
      const override {}

  void visit(const FixedPointNode& node, PlanNodeVisitorContext& context)
      const override {}

  void visit(
      const RecursiveReferenceNode& /*node*/,
      PlanNodeVisitorContext& /*context*/) const override {}

  void visit(const GraphMatchNode& node, PlanNodeVisitorContext& context)
      const override {
    node.forEachExpression(
        [&](const Expr& expression) { visitExpression(expression, context); });
  }

 private:
  static void visitOptionalExpression(
      const ExprPtr& expression,
      PlanNodeVisitorContext& context) {
    if (expression != nullptr) {
      visitExpression(*expression, context);
    }
  }

  static void visitExpression(
      const Expr& expression,
      PlanNodeVisitorContext& context) {
    auto& visit = static_cast<Context&>(context).visit;
    expression.forEachSubquery(
        [&](const SubqueryExpr& subquery) { visit(*subquery.subquery()); });
  }

  static void visitExpressions(
      const std::vector<ExprPtr>& expressions,
      PlanNodeVisitorContext& context) {
    for (const auto& expression : expressions) {
      visitExpression(*expression, context);
    }
  }
};

// Invokes 'visit' for the plan of each subquery in 'node''s expressions.
void forEachSubqueryPlan(
    const LogicalPlanNode& node,
    folly::FunctionRef<void(const LogicalPlanNode&)> visit) {
  const SubqueryPlanVisitor visitor;
  SubqueryPlanVisitor::Context context{visit};
  node.accept(visitor, context);
}

struct TraversalContext {
  folly::FunctionRef<void(const LogicalPlanNode&)> visit;
  folly::F14FastSet<const LogicalPlanNode*> visited;
};

void traversePlan(const LogicalPlanNode& plan, TraversalContext& context) {
  if (!context.visited.insert(&plan).second) {
    return;
  }
  context.visit(plan);
  forEachSubqueryPlan(plan, [&](const LogicalPlanNode& subquery) {
    traversePlan(subquery, context);
  });
  for (const auto& input : plan.inputs()) {
    traversePlan(*input, context);
  }
  for (const auto& nested : plan.nestedPlans()) {
    traversePlan(*nested, context);
  }
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

// Compares two vectors of LogicalPlanNodePtr for structural equality.
bool equalNodeVectors(
    const std::vector<LogicalPlanNodePtr>& lhs,
    const std::vector<LogicalPlanNodePtr>& rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (size_t i = 0; i < lhs.size(); ++i) {
    VELOX_CHECK_NOT_NULL(lhs[i]);
    VELOX_CHECK_NOT_NULL(rhs[i]);
    if (*lhs[i] != *rhs[i]) {
      return false;
    }
  }
  return true;
}

} // namespace

AXIOM_DEFINE_ENUM_NAME(NodeKind, nodeKindNames)

void LogicalPlanNode::forEachNode(
    folly::FunctionRef<void(const LogicalPlanNode&)> visit) const {
  TraversalContext context{visit};
  traversePlan(*this, context);
}

bool LogicalPlanNode::operator==(const LogicalPlanNode& other) const {
  if (this == &other) {
    return true;
  }
  if (kind_ != other.kind_) {
    return false;
  }
  if (*outputType_ != *other.outputType_) {
    return false;
  }
  if (!equalNodeVectors(inputs_, other.inputs_)) {
    return false;
  }
  return equalTo(other);
}

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
  // Keys are prefixed with "Logical" because this registry is shared with
  // velox::core::PlanNode, whose physical nodes claim TableScanNode,
  // FilterNode, ProjectNode, LimitNode, TableWriteNode and UnnestNode. Without
  // the prefix whichever registered last would win, leaving a process able to
  // deserialize logical plans or physical ones but never both. The prefix must
  // stay in step with the names serializeBase() writes.
  auto& registry = velox::DeserializationWithContextRegistryForSharedPtr();
  registry.Register("LogicalValuesNode", ValuesNode::create);
  registry.Register("LogicalTableScanNode", TableScanNode::create);
  registry.Register("LogicalFilterNode", FilterNode::create);
  registry.Register("LogicalProjectNode", ProjectNode::create);
  registry.Register("LogicalAggregateNode", AggregateNode::create);
  registry.Register("LogicalJoinNode", JoinNode::create);
  registry.Register("LogicalLateralJoinNode", LateralJoinNode::create);
  registry.Register("LogicalVectorSearchNode", VectorSearchNode::create);
  registry.Register("LogicalSortNode", SortNode::create);
  registry.Register("LogicalLimitNode", LimitNode::create);
  registry.Register("LogicalSetNode", SetNode::create);
  registry.Register("LogicalUnnestNode", UnnestNode::create);
  registry.Register("LogicalTableWriteNode", TableWriteNode::create);
  registry.Register("LogicalSampleNode", SampleNode::create);
  registry.Register("LogicalOutputNode", OutputNode::create);
  registry.Register("LogicalFixedPointNode", FixedPointNode::create);
  registry.Register(
      "LogicalRecursiveReferenceNode", RecursiveReferenceNode::create);
  registry.Register("LogicalGraphMatchNode", GraphMatchNode::create);
}

folly::dynamic ValuesNode::serialize() const {
  auto obj = serializeBase("LogicalValuesNode");
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
  auto obj = serializeBase("LogicalTableScanNode");
  obj["connectorId"] = connectorId_;
  obj["tableName"] = tableName_.table;
  obj["schema"] = tableName_.schema;
  obj["columnNames"] =
      serializeVector(columnNames_, [](const std::string& s) { return s; });
  return obj;
}

// static
LogicalPlanNodePtr TableScanNode::create(
    const folly::dynamic& obj,
    void* /*context*/) {
  SchemaTableName tableName{
      obj.getDefault("schema", "").asString(), obj["tableName"].asString()};
  return std::make_shared<TableScanNode>(
      obj["id"].asString(),
      deserializeOutputType(obj),
      obj["connectorId"].asString(),
      std::move(tableName),
      deserializeStringVector(obj, "columnNames"));
}

folly::dynamic FilterNode::serialize() const {
  auto obj = serializeBase("LogicalFilterNode");
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
  auto obj = serializeBase("LogicalProjectNode");
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
  auto obj = serializeBase("LogicalAggregateNode");
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
  auto obj = serializeBase("LogicalJoinNode");
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
  auto obj = serializeBase("LogicalSortNode");
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
  auto obj = serializeBase("LogicalLimitNode");
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
  auto obj = serializeBase("LogicalSetNode");
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
  auto obj = serializeBase("LogicalUnnestNode");
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
  auto obj = serializeBase("LogicalTableWriteNode");
  obj["connectorId"] = connectorId_;
  obj["tableName"] = tableName_.table;
  obj["schema"] = tableName_.schema;
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

  SchemaTableName tableName{
      obj.getDefault("schema", "").asString(), obj["tableName"].asString()};

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
      std::move(tableName),
      WriteKindName::toWriteKind(obj["writeKind"].asString()),
      deserializeStringVector(obj, "columnNames"),
      deserializeExprs(obj, "columnExpressions", context),
      std::move(options));
}

folly::dynamic SampleNode::serialize() const {
  auto obj = serializeBase("LogicalSampleNode");
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

bool ValuesNode::equalTo(const LogicalPlanNode& other) const {
  const auto* rhs = other.as<ValuesNode>();
  if (cardinality_ != rhs->cardinality_) {
    return false;
  }
  if (data_.index() != rhs->data_.index()) {
    return false;
  }
  // TODO: support order-insensitive comparison.
  return std::visit(
      [&](const auto& lhsData) -> bool {
        using T = std::decay_t<decltype(lhsData)>;
        const auto& rhsData = std::get<T>(rhs->data_);
        if constexpr (std::is_same_v<T, ValuesNode::Variants>) {
          return lhsData == rhsData;
        } else if constexpr (std::is_same_v<T, ValuesNode::Vectors>) {
          if (lhsData.size() != rhsData.size()) {
            return false;
          }
          for (size_t i = 0; i < lhsData.size(); ++i) {
            if (lhsData[i]->size() != rhsData[i]->size()) {
              return false;
            }
            for (velox::vector_size_t row = 0; row < lhsData[i]->size();
                 ++row) {
              if (!lhsData[i]->equalValueAt(rhsData[i].get(), row, row)) {
                return false;
              }
            }
          }
          return true;
        } else {
          if (lhsData.size() != rhsData.size()) {
            return false;
          }
          for (size_t i = 0; i < lhsData.size(); ++i) {
            if (!Expr::equalExprVectors(lhsData[i], rhsData[i])) {
              return false;
            }
          }
          return true;
        }
      },
      data_);
}

void TableScanNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

bool TableScanNode::equalTo(const LogicalPlanNode& other) const {
  const auto* rhs = other.as<TableScanNode>();
  return connectorId_ == rhs->connectorId_ && tableName_ == rhs->tableName_ &&
      columnNames_ == rhs->columnNames_;
}

void FilterNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

bool FilterNode::equalTo(const LogicalPlanNode& other) const {
  return *predicate_ == *other.as<FilterNode>()->predicate_;
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

bool ProjectNode::equalTo(const LogicalPlanNode& other) const {
  const auto* rhs = other.as<ProjectNode>();
  return names_ == rhs->names_ &&
      Expr::equalExprVectors(expressions_, rhs->expressions_);
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

bool AggregateNode::equalTo(const LogicalPlanNode& other) const {
  const auto* rhs = other.as<AggregateNode>();
  return outputNames_ == rhs->outputNames_ &&
      groupingSets_ == rhs->groupingSets_ &&
      Expr::equalExprVectors(groupingKeys_, rhs->groupingKeys_) &&
      Expr::equalExprVectors(aggregates_, rhs->aggregates_);
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

bool JoinNode::equalTo(const LogicalPlanNode& other) const {
  const auto* rhs = other.as<JoinNode>();
  return joinType_ == rhs->joinType_ &&
      Expr::equalNullableExprs(condition_, rhs->condition_);
}

folly::dynamic LateralJoinNode::serialize() const {
  auto obj = serializeBase("LogicalLateralJoinNode");
  obj["right"] = right()->serialize();
  obj["joinType"] = JoinTypeName::toName(joinType_);
  if (condition_) {
    obj["condition"] = condition_->serialize();
  }
  return obj;
}

// static
LogicalPlanNodePtr LateralJoinNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto inputs = deserializeNodeInputs(obj, context);
  VELOX_CHECK_EQ(inputs.size(), 1);
  ExprPtr condition = nullptr;
  if (obj.count("condition")) {
    condition = deserializeExpr(obj["condition"], context);
  }
  return std::make_shared<LateralJoinNode>(
      obj["id"].asString(),
      inputs[0],
      velox::ISerializable::deserialize<LogicalPlanNode>(obj["right"], context),
      JoinTypeName::toJoinType(obj["joinType"].asString()),
      std::move(condition));
}

// static
velox::RowTypePtr LateralJoinNode::makeOutputType(
    const LogicalPlanNodePtr& left,
    const LogicalPlanNodePtr& right) {
  auto type = left->outputType()->unionWith(right->outputType());

  UniqueNameChecker::check(type->names());

  return type;
}

void LateralJoinNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

bool LateralJoinNode::equalTo(const LogicalPlanNode& other) const {
  const auto* rhs = other.as<LateralJoinNode>();
  return equalNodeVectors(nestedPlans(), rhs->nestedPlans()) &&
      joinType_ == rhs->joinType_ &&
      Expr::equalNullableExprs(condition_, rhs->condition_);
}

namespace {

// Returns the probe columns, the build columns except 'buildIndex', then the
// BIGINT 'idName' and the REAL 'scoreName' columns.
velox::RowTypePtr makeVectorSearchOutputType(
    const LogicalPlanNodePtr& probe,
    const LogicalPlanNodePtr& build,
    std::string_view buildIndex,
    std::string_view idName,
    std::string_view scoreName) {
  auto names = probe->outputType()->names();
  auto types = probe->outputType()->children();
  const auto& buildType = build->outputType();
  for (uint32_t i = 0; i < buildType->size(); ++i) {
    if (buildType->nameOf(i) != buildIndex) {
      names.push_back(buildType->nameOf(i));
      types.push_back(buildType->childAt(i));
    }
  }
  names.emplace_back(idName);
  types.push_back(velox::BIGINT());
  names.emplace_back(scoreName);
  types.push_back(velox::REAL());

  UniqueNameChecker::check(names);
  return velox::ROW(std::move(names), std::move(types));
}

// Returns the type of 'column', which must be a column of 'node'.
const velox::TypePtr& vectorSearchColumnType(
    const LogicalPlanNodePtr& node,
    std::string_view column,
    std::string_view side) {
  const auto& type = node->outputType();
  VELOX_USER_CHECK(
      type->containsChild(column),
      "Vector search {} column not found: {}",
      side,
      column);
  return type->findChild(column);
}

} // namespace

VectorSearchNode::VectorSearchNode(
    std::string id,
    const LogicalPlanNodePtr& probe,
    const LogicalPlanNodePtr& build,
    JoinType joinType,
    std::vector<std::string> probeKeys,
    std::vector<std::string> buildKeys,
    std::string probeEmbedding,
    std::string buildIndex,
    velox::common::vectorindex::VectorIndexMetric metric,
    int64_t k,
    std::vector<std::pair<std::string, std::string>> searchOptions,
    std::string idName,
    std::string scoreName)
    : LogicalPlanNode{NodeKind::kVectorSearch, std::move(id), {probe, build}, makeVectorSearchOutputType(probe, build, buildIndex, idName, scoreName)},
      joinType_{joinType},
      probeKeys_{std::move(probeKeys)},
      buildKeys_{std::move(buildKeys)},
      probeEmbedding_{std::move(probeEmbedding)},
      buildIndex_{std::move(buildIndex)},
      metric_{metric},
      k_{k},
      searchOptions_{std::move(searchOptions)},
      idName_{std::move(idName)},
      scoreName_{std::move(scoreName)} {
  VELOX_USER_CHECK(
      joinType_ == JoinType::kInner || joinType_ == JoinType::kLeft,
      "Vector search supports only INNER and LEFT join types: {}",
      JoinTypeName::toName(joinType_));
  VELOX_USER_CHECK_EQ(
      probeKeys_.size(),
      buildKeys_.size(),
      "Vector search needs as many probe keys as build keys");
  for (size_t i = 0; i < probeKeys_.size(); ++i) {
    const auto& probeType =
        vectorSearchColumnType(probe, probeKeys_[i], "probe key");
    const auto& buildType =
        vectorSearchColumnType(build, buildKeys_[i], "build key");
    VELOX_USER_CHECK(
        probeType->equivalent(*buildType),
        "Vector search key types differ: {} {}, {} {}",
        probeKeys_[i],
        probeType->toString(),
        buildKeys_[i],
        buildType->toString());
  }

  const auto& embeddingType =
      vectorSearchColumnType(probe, probeEmbedding_, "probe embedding");
  VELOX_USER_CHECK(
      embeddingType->equivalent(*velox::ARRAY(velox::REAL())),
      "Vector search probe embedding must be an ARRAY(REAL): {} {}",
      probeEmbedding_,
      embeddingType->toString());

  const auto& indexType =
      vectorSearchColumnType(build, buildIndex_, "build index");
  VELOX_USER_CHECK(
      indexType->isVarbinary(),
      "Vector search build index must be a VARBINARY: {} {}",
      buildIndex_,
      indexType->toString());

  VELOX_USER_CHECK_GE(k_, 1, "Vector search k must be at least 1");
}

folly::dynamic VectorSearchNode::serialize() const {
  auto obj = serializeBase("LogicalVectorSearchNode");
  obj["joinType"] = JoinTypeName::toName(joinType_);
  obj["probeKeys"] = velox::ISerializable::serialize(probeKeys_);
  obj["buildKeys"] = velox::ISerializable::serialize(buildKeys_);
  obj["probeEmbedding"] = probeEmbedding_;
  obj["buildIndex"] = buildIndex_;
  obj["metric"] = std::string{
      velox::common::vectorindex::VectorIndexMetricName::toName(metric_)};
  obj["k"] = k_;
  folly::dynamic options = folly::dynamic::array;
  for (const auto& [name, value] : searchOptions_) {
    options.push_back(folly::dynamic::array(name, value));
  }
  obj["searchOptions"] = std::move(options);
  obj["idName"] = idName_;
  obj["scoreName"] = scoreName_;
  return obj;
}

// static
LogicalPlanNodePtr VectorSearchNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto inputs = deserializeNodeInputs(obj, context);
  VELOX_CHECK_EQ(inputs.size(), 2);
  std::vector<std::pair<std::string, std::string>> searchOptions;
  for (const auto& option : obj["searchOptions"]) {
    searchOptions.emplace_back(option[0].asString(), option[1].asString());
  }
  return std::make_shared<VectorSearchNode>(
      obj["id"].asString(),
      inputs[0],
      inputs[1],
      JoinTypeName::toJoinType(obj["joinType"].asString()),
      velox::ISerializable::deserialize<std::vector<std::string>>(
          obj["probeKeys"]),
      velox::ISerializable::deserialize<std::vector<std::string>>(
          obj["buildKeys"]),
      obj["probeEmbedding"].asString(),
      obj["buildIndex"].asString(),
      velox::common::vectorindex::VectorIndexMetricName::toVectorIndexMetric(
          obj["metric"].asString()),
      obj["k"].asInt(),
      std::move(searchOptions),
      obj["idName"].asString(),
      obj["scoreName"].asString());
}

void VectorSearchNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

bool VectorSearchNode::equalTo(const LogicalPlanNode& other) const {
  const auto* rhs = other.as<VectorSearchNode>();
  return joinType_ == rhs->joinType_ && probeKeys_ == rhs->probeKeys_ &&
      buildKeys_ == rhs->buildKeys_ &&
      probeEmbedding_ == rhs->probeEmbedding_ &&
      buildIndex_ == rhs->buildIndex_ && metric_ == rhs->metric_ &&
      k_ == rhs->k_ && searchOptions_ == rhs->searchOptions_ &&
      idName_ == rhs->idName_ && scoreName_ == rhs->scoreName_;
}

void SortNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

bool SortNode::equalTo(const LogicalPlanNode& other) const {
  return ordering_ == other.as<SortNode>()->ordering_;
}

void LimitNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

bool LimitNode::equalTo(const LogicalPlanNode& other) const {
  const auto* rhs = other.as<LimitNode>();
  return offset_ == rhs->offset_ && count_ == rhs->count_;
}

namespace {
const auto& setOperationNames() {
  static const folly::F14FastMap<SetOperation, std::string_view> kNames = {
      {SetOperation::kUnion, "UNION"},
      {SetOperation::kUnionAll, "UNION ALL"},
      {SetOperation::kIntersect, "INTERSECT"},
      {SetOperation::kIntersectAll, "INTERSECT ALL"},
      {SetOperation::kExcept, "EXCEPT"},
      {SetOperation::kExceptAll, "EXCEPT ALL"},
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

bool SetNode::equalTo(const LogicalPlanNode& other) const {
  return operation_ == other.as<SetNode>()->operation_;
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

bool UnnestNode::equalTo(const LogicalPlanNode& other) const {
  const auto* rhs = other.as<UnnestNode>();
  return flattenArrayOfRows_ == rhs->flattenArrayOfRows_ &&
      ordinalityName_ == rhs->ordinalityName_ &&
      unnestedNames_ == rhs->unnestedNames_ &&
      Expr::equalExprVectors(unnestExpressions_, rhs->unnestExpressions_);
}

TableWriteNode::TableWriteNode(
    std::string id,
    LogicalPlanNodePtr input,
    std::string connectorId,
    SchemaTableName tableName,
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
  VELOX_USER_CHECK(!tableName_.table.empty());
  VELOX_USER_CHECK_EQ(columnNames_.size(), columnExpressions_.size());

  UniqueNameChecker::check(columnNames_);
  VELOX_USER_CHECK(
      writeKind_ == WriteKind::kCreate || writeKind == WriteKind::kInsert ||
          options_.empty(),
      "Options are supported only for create or insert");
  VELOX_USER_CHECK(
      writeKind_ != WriteKind::kDelete || columnNames_.empty(),
      "Delete writes no columns");
}

void TableWriteNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

bool TableWriteNode::equalTo(const LogicalPlanNode& other) const {
  const auto* rhs = other.as<TableWriteNode>();
  return connectorId_ == rhs->connectorId_ && tableName_ == rhs->tableName_ &&
      writeKind_ == rhs->writeKind_ && columnNames_ == rhs->columnNames_ &&
      options_ == rhs->options_ &&
      Expr::equalExprVectors(columnExpressions_, rhs->columnExpressions_);
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

bool SampleNode::equalTo(const LogicalPlanNode& other) const {
  const auto* rhs = other.as<SampleNode>();
  return sampleMethod_ == rhs->sampleMethod_ &&
      *percentage_ == *rhs->percentage_;
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

bool OutputNode::equalTo(const LogicalPlanNode& other) const {
  return entries_ == other.as<OutputNode>()->entries_;
}

folly::dynamic OutputNode::serialize() const {
  auto obj = serializeBase("LogicalOutputNode");
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

namespace {

// Invokes 'visit' for each child plan of 'node' whose recursive references
// resolve against the same fixed point as 'node': its inputs, its subqueries'
// plans and its nested plans. A nested FixedPointNode's step and convergence
// resolve against that inner loop, so they are skipped.
void forEachChildInScope(
    const LogicalPlanNode& node,
    folly::FunctionRef<void(const LogicalPlanNode&)> visit) {
  for (const auto& input : node.inputs()) {
    visit(*input);
  }
  forEachSubqueryPlan(node, visit);
  if (node.is(NodeKind::kFixedPoint)) {
    return;
  }
  for (const auto& nested : node.nestedPlans()) {
    visit(*nested);
  }
}

// Checks every RecursiveReferenceNode reachable from 'node': it must name
// 'name' and carry 'stateSchema'. Returns true if at least one is seen.
bool checkRecursiveReferences(
    const LogicalPlanNode& node,
    const std::string& name,
    const velox::RowTypePtr& stateSchema) {
  if (node.is(NodeKind::kRecursiveReference)) {
    const auto* ref = node.as<RecursiveReferenceNode>();
    VELOX_USER_CHECK_EQ(
        ref->name(),
        name,
        "RecursiveReferenceNode names an unknown fixed-point state");
    VELOX_USER_CHECK(
        ref->outputType()->equivalent(*stateSchema),
        "RecursiveReferenceNode schema does not match state '{}': {} vs. {}",
        name,
        ref->outputType()->toString(),
        stateSchema->toString());
    return true;
  }
  bool found{false};
  forEachChildInScope(node, [&](const LogicalPlanNode& child) {
    found |= checkRecursiveReferences(child, name, stateSchema);
  });
  return found;
}

// Rejects any RecursiveReferenceNode reachable from 'node' that names
// 'name'. Used on the anchor, which produces seed rows before any
// iteration and so cannot reference its own enclosing FixedPointNode.
void rejectAnchorRecursiveReference(
    const LogicalPlanNode& node,
    const std::string& name) {
  if (node.is(NodeKind::kRecursiveReference)) {
    const auto* ref = node.as<RecursiveReferenceNode>();
    VELOX_USER_CHECK_NE(
        ref->name(),
        name,
        "FixedPointNode anchor must not reference its own state");
    return;
  }
  forEachChildInScope(node, [&](const LogicalPlanNode& child) {
    rejectAnchorRecursiveReference(child, name);
  });
}
} // namespace

namespace {

std::vector<LogicalPlanNodePtr> iterationPlans(
    const LogicalPlanNodePtr& step,
    const LogicalPlanNodePtr& convergence) {
  if (convergence == nullptr) {
    return {step};
  }
  return {step, convergence};
}

const auto& outputModeNames() {
  static const folly::F14FastMap<FixedPointNode::OutputMode, std::string_view>
      kNames = {
          {FixedPointNode::OutputMode::kAppend, "APPEND"},
          {FixedPointNode::OutputMode::kReplace, "REPLACE"},
      };
  return kNames;
}

} // namespace

AXIOM_DEFINE_EMBEDDED_ENUM_NAME(FixedPointNode, OutputMode, outputModeNames);

FixedPointNode::FixedPointNode(
    std::string id,
    std::string name,
    LogicalPlanNodePtr anchor,
    LogicalPlanNodePtr step,
    OutputMode outputMode,
    LogicalPlanNodePtr convergence,
    std::optional<int32_t> maxIterations,
    bool failAtBound,
    std::vector<std::string> keyColumns)
    : LogicalPlanNode{NodeKind::kFixedPoint, std::move(id), {anchor}, iterationPlans(step, convergence), anchor->outputType()},
      name_{std::move(name)},
      outputMode_{outputMode},
      maxIterations_{maxIterations},
      failAtBound_{failAtBound},
      keyColumns_{std::move(keyColumns)} {
  VELOX_USER_CHECK(!name_.empty(), "FixedPointNode name must be non-empty");
  VELOX_USER_CHECK_NOT_NULL(step, "FixedPointNode requires a step plan");
  const auto& stateType = anchor->outputType();
  VELOX_USER_CHECK(
      stateType->equivalent(*step->outputType()),
      "Anchor and step branches of FixedPointNode must have equivalent output schemas: {} vs. {}",
      stateType->toString(),
      step->outputType()->toString());

  rejectAnchorRecursiveReference(*anchor, name_);
  VELOX_USER_CHECK(
      checkRecursiveReferences(*step, name_, stateType),
      "FixedPointNode step must contain at least one RecursiveReferenceNode named '{}'",
      name_);

  if (convergence != nullptr) {
    VELOX_USER_CHECK(
        checkRecursiveReferences(*convergence, name_, stateType),
        "FixedPointNode convergence must contain at least one RecursiveReferenceNode named '{}'",
        name_);
    const auto& convergenceType = convergence->outputType();
    VELOX_USER_CHECK_EQ(
        convergenceType->size(),
        1,
        "FixedPointNode convergence must produce one column: {}",
        convergenceType->toString());
    VELOX_USER_CHECK(
        convergenceType->childAt(0)->isBoolean(),
        "FixedPointNode convergence must produce a BOOLEAN column: {}",
        convergenceType->childAt(0)->toString());
  }

  if (maxIterations_.has_value()) {
    VELOX_USER_CHECK_GT(
        maxIterations_.value(),
        0,
        "FixedPointNode maxIterations must be positive");
  }

  VELOX_USER_CHECK(
      keyColumns_.empty() || outputMode_ == OutputMode::kReplace,
      "FixedPointNode keyColumns require replace mode");
  folly::F14FastSet<std::string_view> seen;
  for (const auto& column : keyColumns_) {
    VELOX_USER_CHECK(
        stateType->containsChild(column),
        "FixedPointNode key column not found in the state: {}",
        column);
    VELOX_USER_CHECK(
        seen.insert(column).second,
        "FixedPointNode key column appears more than once: {}",
        column);
  }
}

// static
std::shared_ptr<const FixedPointNode> FixedPointNode::recursiveCte(
    std::string id,
    std::string name,
    LogicalPlanNodePtr anchor,
    LogicalPlanNodePtr step) {
  return std::make_shared<FixedPointNode>(
      std::move(id),
      std::move(name),
      std::move(anchor),
      std::move(step),
      OutputMode::kAppend,
      /*convergence=*/nullptr,
      /*maxIterations=*/std::nullopt,
      /*failAtBound=*/true,
      /*keyColumns=*/std::vector<std::string>{});
}

// static
std::shared_ptr<const FixedPointNode> FixedPointNode::iterate(
    std::string id,
    std::string name,
    LogicalPlanNodePtr anchor,
    LogicalPlanNodePtr step,
    int32_t numIterations,
    std::vector<std::string> keyColumns) {
  return std::make_shared<FixedPointNode>(
      std::move(id),
      std::move(name),
      std::move(anchor),
      std::move(step),
      OutputMode::kReplace,
      /*convergence=*/nullptr,
      numIterations,
      /*failAtBound=*/false,
      std::move(keyColumns));
}

// static
std::shared_ptr<const FixedPointNode> FixedPointNode::iterateUntil(
    std::string id,
    std::string name,
    LogicalPlanNodePtr anchor,
    LogicalPlanNodePtr step,
    LogicalPlanNodePtr convergence,
    int32_t maxIterations,
    bool failAtBound,
    std::vector<std::string> keyColumns) {
  VELOX_USER_CHECK_NOT_NULL(convergence);
  return std::make_shared<FixedPointNode>(
      std::move(id),
      std::move(name),
      std::move(anchor),
      std::move(step),
      OutputMode::kReplace,
      std::move(convergence),
      maxIterations,
      failAtBound,
      std::move(keyColumns));
}

void FixedPointNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

bool FixedPointNode::equalTo(const LogicalPlanNode& other) const {
  // LogicalPlanNode::operator== compares the anchor before delegating here.
  const auto* otherFixedPoint = other.as<FixedPointNode>();
  return equalNodeVectors(nestedPlans(), otherFixedPoint->nestedPlans()) &&
      name_ == otherFixedPoint->name_ &&
      outputMode_ == otherFixedPoint->outputMode_ &&
      maxIterations_ == otherFixedPoint->maxIterations_ &&
      failAtBound_ == otherFixedPoint->failAtBound_ &&
      keyColumns_ == otherFixedPoint->keyColumns_;
}

folly::dynamic FixedPointNode::serialize() const {
  auto obj = serializeBase("LogicalFixedPointNode");
  obj["recursiveName"] = name_;
  obj["step"] = step()->serialize();
  if (convergence() != nullptr) {
    obj["convergence"] = convergence()->serialize();
  }
  obj["outputMode"] = toName(outputMode_);
  if (maxIterations_.has_value()) {
    obj["maxIterations"] = maxIterations_.value();
  }
  obj["failAtBound"] = failAtBound_;
  if (!keyColumns_.empty()) {
    obj["keyColumns"] = velox::ISerializable::serialize(keyColumns_);
  }
  return obj;
}

// static
LogicalPlanNodePtr FixedPointNode::create(
    const folly::dynamic& obj,
    void* context) {
  auto inputs = deserializeNodeInputs(obj, context);
  VELOX_USER_CHECK_EQ(
      inputs.size(), 1, "FixedPointNode requires exactly one input (anchor)");
  LogicalPlanNodePtr convergence;
  if (obj.count("convergence")) {
    convergence = velox::ISerializable::deserialize<LogicalPlanNode>(
        obj["convergence"], context);
  }
  std::optional<int32_t> maxIterations;
  if (obj.count("maxIterations")) {
    maxIterations = static_cast<int32_t>(obj["maxIterations"].asInt());
  }
  std::vector<std::string> keyColumns;
  if (obj.count("keyColumns")) {
    keyColumns = velox::ISerializable::deserialize<std::vector<std::string>>(
        obj["keyColumns"]);
  }
  return std::make_shared<FixedPointNode>(
      obj["id"].asString(),
      obj["recursiveName"].asString(),
      std::move(inputs[0]),
      velox::ISerializable::deserialize<LogicalPlanNode>(obj["step"], context),
      toOutputMode(obj["outputMode"].asString()),
      std::move(convergence),
      maxIterations,
      obj["failAtBound"].asBool(),
      std::move(keyColumns));
}

RecursiveReferenceNode::RecursiveReferenceNode(
    std::string id,
    std::string name,
    velox::RowTypePtr outputType)
    : LogicalPlanNode{NodeKind::kRecursiveReference, std::move(id), {}, std::move(outputType)},
      name_{std::move(name)} {
  VELOX_USER_CHECK(
      !name_.empty(), "RecursiveReferenceNode name must be non-empty");
}

void RecursiveReferenceNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

bool RecursiveReferenceNode::equalTo(const LogicalPlanNode& other) const {
  return id() == other.id();
}

folly::dynamic RecursiveReferenceNode::serialize() const {
  auto obj = serializeBase("LogicalRecursiveReferenceNode");
  obj["recursiveName"] = name_;
  return obj;
}

// static
LogicalPlanNodePtr RecursiveReferenceNode::create(
    const folly::dynamic& obj,
    void* /*context*/) {
  return std::make_shared<RecursiveReferenceNode>(
      obj["id"].asString(),
      obj["recursiveName"].asString(),
      deserializeOutputType(obj));
}

namespace {

using ElementTable = PropertyGraph::ElementTable;

// What GraphMatchNode knows about one named vertex or edge pattern.
struct NamedElement {
  GraphPatternTerm::Kind kind{GraphPatternTerm::Kind::kVertex};

  // True if the pattern is under a repetition, so it binds once per
  // repetition.
  bool group{false};

  // Element tables the name can match: those every pattern with the name can
  // match.
  std::vector<const ElementTable*> tables;
};

// Returns the data type of 'property' for an element that can match 'tables'.
// Fails if none of those tables defines it, or if two of them define it with
// different data types.
velox::TypePtr propertyType(
    const std::vector<const ElementTable*>& tables,
    const std::string& property) {
  auto type = PropertyGraph::propertyType(tables, property);
  VELOX_USER_CHECK_NOT_NULL(
      type,
      "Property is not defined by any table the element can match: {}",
      property);
  return type;
}

void checkSelector(const GraphPathSelector& selector) {
  using Kind = GraphPathSelector::Kind;
  const bool allowsCount = selector.kind == Kind::kAny ||
      selector.kind == Kind::kShortest ||
      selector.kind == Kind::kShortestGroups;
  VELOX_USER_CHECK(
      allowsCount || !selector.count.has_value(),
      "Path selector does not take a count: {}",
      GraphPathSelector::toName(selector.kind));
  VELOX_USER_CHECK(
      selector.kind != Kind::kShortest || selector.count.has_value(),
      "SHORTEST requires a count");
  if (selector.count.has_value()) {
    VELOX_USER_CHECK_GT(
        selector.count.value(), 0, "Path selector count must be positive");
  }
}

// Collects the pattern names of a GraphMatchNode's patterns and checks the
// rules on their use.
class GraphPatternNameCollector {
 public:
  explicit GraphPatternNameCollector(const PropertyGraph& graph)
      : graph_{graph} {}

  void addPath(const GraphPathPattern& path) {
    VELOX_USER_CHECK(!path.terms.empty(), "Path pattern has no terms");
    checkSelector(path.selector);
    if (!path.name.empty()) {
      addPathName(path.name, /*group=*/false);
    }
    addTerms(path.terms, /*group=*/false);
  }

  const folly::F14FastMap<std::string, NamedElement>& elementNames() const {
    return elementNames_;
  }

  // Path and subpath names, each mapped to whether it is under a
  // repetition.
  const folly::F14FastMap<std::string, bool>& pathNames() const {
    return pathNames_;
  }

  // Checks that the ROW inputs of the predicates in 'terms' read properties the
  // named patterns' tables define, with those data types. Call after adding
  // every path.
  void checkPredicates(const std::vector<GraphPatternTermPtr>& terms) const {
    for (const auto& term : terms) {
      if (term->predicate() != nullptr) {
        term->predicate()->forEachInputReference(
            [&](const InputReferenceExpr& input) { checkInput(input); });
      }
      if (term->is(GraphPatternTerm::Kind::kSubpath)) {
        checkPredicates(term->as<GraphSubpathPattern>()->terms());
      }
    }
  }

  // Checks that 'predicate', a condition on a whole match, is BOOLEAN and
  // reads vertex and edge patterns that are not under a repetition.
  void checkMatchPredicate(const Expr& predicate) const {
    VELOX_USER_CHECK_EQ(
        predicate.type()->kind(),
        velox::TypeKind::BOOLEAN,
        "Graph match predicate is not BOOLEAN: {}",
        predicate.type()->toString());
    predicate.forEachInputReference([&](const InputReferenceExpr& input) {
      const auto it = elementNames_.find(input.name());
      VELOX_USER_CHECK(
          it != elementNames_.end(),
          "Graph match predicate reads an unknown pattern name: {}",
          input.name());
      VELOX_USER_CHECK(
          !it->second.group,
          "Graph match predicate reads a pattern under a repetition: {}",
          input.name());
      VELOX_USER_CHECK(
          input.type()->isRow(),
          "Graph match predicate reads a pattern name as a non-ROW: {}",
          input.name());
      checkInput(input);
    });
  }

 private:
  void addPathName(const std::string& name, bool group) {
    VELOX_USER_CHECK(
        !elementNames_.contains(name),
        "Pattern name is used for both a path and an element: {}",
        name);
    VELOX_USER_CHECK(
        pathNames_.emplace(name, group).second,
        "Path name is used more than once: {}",
        name);
  }

  void addTerms(const std::vector<GraphPatternTermPtr>& terms, bool group) {
    for (const auto& term : terms) {
      VELOX_USER_CHECK_NOT_NULL(term, "Path pattern term is null");
      addTerm(*term, group);
    }
  }

  // Adds 'term'. 'group' is true if the term is under a repetition.
  void addTerm(const GraphPatternTerm& term, bool group) {
    if (term.is(GraphPatternTerm::Kind::kSubpath)) {
      const auto* subpath = term.as<GraphSubpathPattern>();
      const bool subpathGroup = group || subpath->repetition().has_value();
      if (!term.name().empty()) {
        addPathName(term.name(), subpathGroup);
      }
      addTerms(subpath->terms(), subpathGroup);
      return;
    }

    const auto& element = *term.as<GraphElementPattern>();
    const bool elementGroup = group || term.repetition().has_value();
    checkLabel(element);

    if (!term.name().empty()) {
      addElementName(element, elementGroup, element.matchingTables(graph_));
    }
  }

  // Checks that each field of 'input', which names a vertex or edge pattern,
  // is a property its tables define with the field's data type.
  void checkInput(const InputReferenceExpr& input) const {
    const auto& tables = elementNames_.at(input.name()).tables;
    const auto& row = input.type()->asRow();
    for (auto i = 0; i < row.size(); ++i) {
      const auto& property = row.nameOf(i);
      const auto type = propertyType(tables, property);
      VELOX_USER_CHECK(
          type->equivalent(*row.childAt(i)),
          "Pattern predicate reads a property with a different data type: "
          "{}.{}, {} and {}",
          input.name(),
          property,
          row.childAt(i)->toString(),
          type->toString());
    }
  }

  // Checks that every label name in 'element' names a vertex of the graph,
  // or an edge for an edge pattern.
  void checkLabel(const GraphElementPattern& element) const {
    if (element.label() == nullptr) {
      return;
    }
    const bool isVertexPattern = element.is(GraphPatternTerm::Kind::kVertex);
    element.label()->forEachName([&](std::string_view name) {
      VELOX_USER_CHECK(
          isVertexPattern ? graph_.isVertex(name) : graph_.isEdge(name),
          "{} not found in graph: {}",
          isVertexPattern ? "Vertex" : "Edge",
          name);
    });
  }

  void addElementName(
      const GraphElementPattern& element,
      bool group,
      std::vector<const ElementTable*> tables) {
    const auto& name = element.name();
    VELOX_USER_CHECK(
        !pathNames_.contains(name),
        "Pattern name is used for both a path and an element: {}",
        name);

    auto [it, inserted] = elementNames_.emplace(
        name,
        NamedElement{.kind = element.kind(), .group = group, .tables = tables});
    if (inserted) {
      return;
    }

    auto& named = it->second;
    VELOX_USER_CHECK(
        !named.group && !group,
        "A pattern name under a repetition is used more than once: {}",
        name);
    VELOX_USER_CHECK(
        named.kind == element.kind(),
        "Pattern name is used for both a vertex and an edge: {}",
        name);

    // Every occurrence binds the same element, so it matches only tables all
    // occurrences can match.
    std::erase_if(named.tables, [&](const ElementTable* table) {
      return std::find(tables.begin(), tables.end(), table) == tables.end();
    });
  }

  const PropertyGraph& graph_;

  folly::F14FastMap<std::string, NamedElement> elementNames_;
  folly::F14FastMap<std::string, bool> pathNames_;
};

// Returns the data type of 'output'.
velox::TypePtr graphMatchOutputType(
    const GraphPatternNameCollector& collector,
    const GraphMatchOutput& output) {
  const auto& pattern = output.pattern();
  if (output.is(GraphMatchOutput::Kind::kPathLength)) {
    const auto it = collector.pathNames().find(pattern);
    VELOX_USER_CHECK(
        it != collector.pathNames().end(),
        "PATH_LENGTH requires a path or subpath name: {}",
        pattern);
    VELOX_USER_CHECK(
        !it->second,
        "PATH_LENGTH of a pattern under a repetition is not supported: {}",
        pattern);
    return velox::BIGINT();
  }

  const auto it = collector.elementNames().find(pattern);
  VELOX_USER_CHECK(
      it != collector.elementNames().end(),
      "Graph match output references an unknown pattern name: {}",
      pattern);
  const auto& named = it->second;
  auto type =
      propertyType(named.tables, output.as<GraphPropertyOutput>()->property());
  return named.group ? velox::ARRAY(std::move(type)) : type;
}

// Checks the patterns and outputs of a GraphMatchNode and returns its output
// type.
velox::RowTypePtr makeGraphMatchOutputType(
    const PropertyGraphPtr& graph,
    const std::vector<GraphPathPattern>& paths,
    const ExprPtr& predicate,
    const std::vector<GraphMatchOutputPtr>& outputs,
    const std::vector<std::string>& outputNames) {
  VELOX_USER_CHECK_NOT_NULL(graph, "GraphMatchNode has no graph");
  VELOX_USER_CHECK(!paths.empty(), "GraphMatchNode has no path patterns");
  VELOX_USER_CHECK(!outputs.empty(), "GraphMatchNode has no outputs");
  VELOX_USER_CHECK_EQ(
      outputs.size(),
      outputNames.size(),
      "GraphMatchNode outputs and output names are not aligned");
  UniqueNameChecker::check(outputNames);

  GraphPatternNameCollector collector(*graph);
  for (const auto& path : paths) {
    collector.addPath(path);
  }
  for (const auto& path : paths) {
    collector.checkPredicates(path.terms);
  }
  if (predicate != nullptr) {
    collector.checkMatchPredicate(*predicate);
  }

  std::vector<velox::TypePtr> types;
  for (const auto& output : outputs) {
    VELOX_USER_CHECK_NOT_NULL(output, "Graph match output is null");
    types.push_back(graphMatchOutputType(collector, *output));
  }
  return velox::ROW(outputNames, std::move(types));
}

const auto& graphMatchOutputKindNames() {
  static const folly::F14FastMap<GraphMatchOutput::Kind, std::string_view>
      kNames = {
          {GraphMatchOutput::Kind::kProperty, "PROPERTY"},
          {GraphMatchOutput::Kind::kPathLength, "PATH_LENGTH"},
      };
  return kNames;
}

} // namespace

AXIOM_DEFINE_EMBEDDED_ENUM_NAME(
    GraphMatchOutput,
    Kind,
    graphMatchOutputKindNames);

GraphMatchOutput::GraphMatchOutput(Kind kind, std::string pattern)
    : kind_{kind}, pattern_{std::move(pattern)} {
  VELOX_USER_CHECK(!pattern_.empty(), "Graph match output has no pattern");
}

bool GraphMatchOutput::operator==(const GraphMatchOutput& other) const {
  return kind_ == other.kind_ && pattern_ == other.pattern_ && equals(other);
}

folly::dynamic GraphMatchOutput::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["kind"] = toName(kind_);
  obj["pattern"] = pattern_;
  return obj;
}

// static
GraphMatchOutputPtr GraphMatchOutput::create(const folly::dynamic& obj) {
  auto pattern = obj["pattern"].asString();
  switch (toKind(obj["kind"].asString())) {
    case Kind::kProperty:
      return std::make_shared<GraphPropertyOutput>(
          std::move(pattern), obj["property"].asString());
    case Kind::kPathLength:
      return std::make_shared<GraphPathLengthOutput>(std::move(pattern));
  }
  VELOX_UNREACHABLE();
}

GraphPropertyOutput::GraphPropertyOutput(
    std::string pattern,
    std::string property)
    : GraphMatchOutput{Kind::kProperty, std::move(pattern)},
      property_{std::move(property)} {
  VELOX_USER_CHECK(!property_.empty(), "Graph property output has no property");
}

folly::dynamic GraphPropertyOutput::serialize() const {
  auto obj = GraphMatchOutput::serialize();
  obj["property"] = property_;
  return obj;
}

bool GraphPropertyOutput::equals(const GraphMatchOutput& other) const {
  return property_ == static_cast<const GraphPropertyOutput&>(other).property_;
}

namespace {

std::vector<LogicalPlanNodePtr> tablePlans(const PropertyGraphPtr& graph) {
  VELOX_USER_CHECK_NOT_NULL(graph);
  std::vector<LogicalPlanNodePtr> plans;
  for (const auto& table : graph->vertexTables()) {
    plans.push_back(table.plan);
  }
  for (const auto& table : graph->edgeTables()) {
    plans.push_back(table.plan);
  }
  return plans;
}

} // namespace

GraphMatchNode::GraphMatchNode(
    std::string id,
    PropertyGraphPtr graph,
    std::vector<GraphPathPattern> paths,
    ExprPtr predicate,
    std::vector<GraphMatchOutputPtr> outputs,
    std::vector<std::string> outputNames)
    : LogicalPlanNode{NodeKind::kGraphMatch, std::move(id), {}, tablePlans(graph), makeGraphMatchOutputType(graph, paths, predicate, outputs, outputNames)},
      graph_{std::move(graph)},
      paths_{std::move(paths)},
      predicate_{std::move(predicate)},
      outputs_{std::move(outputs)},
      outputNames_{std::move(outputNames)} {}

namespace {

void forEachPredicate(
    const std::vector<GraphPatternTermPtr>& terms,
    folly::FunctionRef<void(const Expr&)> visit) {
  for (const auto& term : terms) {
    if (term->predicate() != nullptr) {
      visit(*term->predicate());
    }
    if (term->is(GraphPatternTerm::Kind::kSubpath)) {
      forEachPredicate(term->as<GraphSubpathPattern>()->terms(), visit);
    }
  }
}

} // namespace

void GraphMatchNode::forEachExpression(
    folly::FunctionRef<void(const Expr&)> visit) const {
  auto visitProperties = [&](const PropertyGraph::ElementTable& table) {
    for (const auto& [_, expression] : table.properties) {
      visit(*expression);
    }
  };
  for (const auto& table : graph_->vertexTables()) {
    visitProperties(table);
  }
  for (const auto& table : graph_->edgeTables()) {
    visitProperties(table);
  }
  for (const auto& path : paths_) {
    forEachPredicate(path.terms, visit);
  }
  if (predicate_ != nullptr) {
    visit(*predicate_);
  }
}

void GraphMatchNode::accept(
    const PlanNodeVisitor& visitor,
    PlanNodeVisitorContext& context) const {
  visitor.visit(*this, context);
}

bool GraphMatchNode::equalTo(const LogicalPlanNode& other) const {
  const auto* rhs = other.as<GraphMatchNode>();
  return *graph_ == *rhs->graph_ && paths_ == rhs->paths_ &&
      Expr::equalNullableExprs(predicate_, rhs->predicate_) &&
      std::equal(
          outputs_.begin(),
          outputs_.end(),
          rhs->outputs_.begin(),
          rhs->outputs_.end(),
          [](const auto& left, const auto& right) {
            return *left == *right;
          }) &&
      outputNames_ == rhs->outputNames_;
}

folly::dynamic GraphMatchNode::serialize() const {
  auto obj = serializeBase("LogicalGraphMatchNode");
  obj["graph"] = graph_->serialize();
  obj["paths"] = serializeVector(
      paths_, [](const GraphPathPattern& path) { return path.serialize(); });
  if (predicate_ != nullptr) {
    obj["predicate"] = predicate_->serialize();
  }
  obj["outputs"] = serializeVector(
      outputs_,
      [](const GraphMatchOutputPtr& output) { return output->serialize(); });
  obj["outputNames"] = velox::ISerializable::serialize(outputNames_);
  return obj;
}

// static
LogicalPlanNodePtr GraphMatchNode::create(
    const folly::dynamic& obj,
    void* context) {
  std::vector<GraphPathPattern> paths;
  for (const auto& path : obj["paths"]) {
    paths.push_back(GraphPathPattern::create(path, context));
  }
  std::vector<GraphMatchOutputPtr> outputs;
  for (const auto& output : obj["outputs"]) {
    outputs.push_back(GraphMatchOutput::create(output));
  }
  return std::make_shared<GraphMatchNode>(
      obj["id"].asString(),
      PropertyGraph::create(obj["graph"], context),
      std::move(paths),
      obj.count("predicate")
          ? velox::ISerializable::deserialize<Expr>(obj["predicate"], context)
          : nullptr,
      std::move(outputs),
      velox::ISerializable::deserialize<std::vector<std::string>>(
          obj["outputNames"]));
}

} // namespace facebook::axiom::logical_plan
