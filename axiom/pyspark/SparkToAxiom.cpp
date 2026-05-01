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

#include <folly/String.h>
#include <string>

#include "axiom/common/SchemaTableName.h"
#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/logical_plan/Expr.h"
#include "axiom/logical_plan/ExprPrinter.h"
#include "axiom/logical_plan/PlanPrinter.h"
#include "axiom/pyspark/ArrowIpcLib.h"
#include "axiom/pyspark/Exception.h"
#include "axiom/pyspark/SparkToAxiom.h"
#include "axiom/pyspark/SparkVeloxConverter.h"
#include "velox/exec/AggregateFunctionRegistry.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"

using namespace facebook;

namespace axiom::collagen {
namespace {

bool isGeneratorFunction(const std::string& functionName) {
  return functionName == "explode" || functionName == "posexplode" ||
      functionName == "explode_outer" || functionName == "posexplode_outer";
}

bool isOuterGenerator(const std::string& functionName) {
  return functionName == "explode_outer" || functionName == "posexplode_outer";
}

/// Extracts the generator function name from an expression (direct or aliased).
/// Returns empty string if no generator is found.
std::string findGeneratorFunction(
    const spark::connect::Expression& expression) {
  if (expression.has_unresolved_function() &&
      isGeneratorFunction(expression.unresolved_function().function_name())) {
    return expression.unresolved_function().function_name();
  }
  if (expression.has_alias() &&
      expression.alias().expr().has_unresolved_function() &&
      isGeneratorFunction(
          expression.alias().expr().unresolved_function().function_name())) {
    return expression.alias().expr().unresolved_function().function_name();
  }
  return "";
}

// Tries to resolve a function signature. Returns the return type if the
// signature was found, or throws a descriptive error message.
velox::TypePtr resolveFunction(
    const std::string& functionName,
    const std::vector<velox::TypePtr>& paramTypes,
    std::vector<velox::TypePtr>& coercions) {
  if (auto returnType = velox::resolveFunctionWithCoercions(
          functionName, paramTypes, coercions)) {
    return returnType;
  }

  // Signature doesn't exist. Return a descriptive error message.
  auto availableSignatures = velox::getFunctionSignatures(functionName);

  if (availableSignatures.empty()) {
    COLLAGEN_USER_FAIL("Scalar function does not exist: '{}'", functionName);
  }

  std::vector<std::string> strings;
  strings.reserve(paramTypes.size());
  for (const auto& type : paramTypes) {
    strings.push_back(type->toString());
  }
  auto signature = folly::join(", ", strings);
  strings.clear();

  for (const auto& item : availableSignatures) {
    strings.push_back(item->toString());
  }

  COLLAGEN_USER_FAIL(
      "Scalar function signature not supported: '{}({})'. Supported signatures: '{}'",
      functionName,
      signature,
      folly::join(", ", strings));
}

} // namespace

void SparkToAxiom::setPlanNode(
    const facebook::axiom::logical_plan::LogicalPlanNodePtr& planNode,
    int64_t planId) {
  sparkPlanIdToPlanNode_.emplace(planId, planNode);
  planNode_ = planNode;
}

void SparkToAxiom::visit(
    const spark::connect::Read_NamedTable& namedTable,
    SparkPlanVisitorContext& context) {
  // This is a leaf, so there should not be an existing plan node.
  COLLAGEN_CHECK_NULL(planNode_);

  const auto& identifier = namedTable.unparsed_identifier();
  COLLAGEN_CHECK(
      identifier.find('.') == std::string::npos,
      "Schema-qualified table names are not supported: {}",
      identifier);

  facebook::axiom::SchemaTableName tableName{schema_, identifier};

  LOG(INFO) << "Creating table scan node [" << context.planId << "] for '"
            << tableName.toString() << "'.";

  // Lookup table metadata on the connector.
  auto* metadata =
      facebook::axiom::connector::ConnectorMetadata::metadata(catalog_);
  auto table = metadata->findTable(tableName);

  COLLAGEN_CHECK_NOT_NULL(table);

  auto planNode =
      std::make_shared<facebook::axiom::logical_plan::TableScanNode>(
          nextId(), table->type(), catalog_, tableName, table->type()->names());
  setPlanNode(planNode, context.planId);
  sparkAliasToPlanNode_.emplace(namedTable.unparsed_identifier(), planNode_);
}

void SparkToAxiom::visit(
    const spark::connect::LocalRelation& localRelation,
    SparkPlanVisitorContext& context) {
  LOG(INFO) << "Creating a local relation node [" << context.planId << "]";
  LOG(INFO) << "      Schema: " << localRelation.schema();

  auto veloxType = velox::asRowType(
      jsonToVeloxType(std::string_view(localRelation.schema())));
  if (veloxType == nullptr) {
    COLLAGEN_USER_FAIL("Top-level type must be a of type 'struct'");
  }

  // Deserialize the Arrow IPC payload into Velox Vectors.
  auto arrowBuffer = arrow::Buffer::Wrap(
      localRelation.data().data(), localRelation.data().size());
  auto vectorInput = fromArrowIpc(arrowBuffer, pool_);

  // If no data was found in the Arrow input, create one empty vector to
  // carry the type.
  if (vectorInput.empty()) {
    vectorInput.push_back(velox::RowVector::createEmpty(veloxType, pool_));
  }

  auto planNode = std::make_shared<facebook::axiom::logical_plan::ValuesNode>(
      nextId(), std::move(vectorInput));
  setPlanNode(planNode, context.planId);
}

void SparkToAxiom::visit(
    const spark::connect::Join& join,
    SparkPlanVisitorContext& context) {
  LOG(INFO) << "Creating a join node [" << context.planId
            << "] (type=" << join.join_type() << "):";

  // First traverse and generate the right (build) subtree.
  SparkPlanVisitor::visit(join.right(), context);
  auto rightSubtree = std::move(planNode_);

  // Then traverse the left (current) subtree.
  SparkPlanVisitor::visit(join.left(), context);

  // TODO: Some joins set join.using_columns() instead of join_condition.
  visit(join.join_condition(), context);

  // Add the actual join node.
  auto planNode = std::make_shared<facebook::axiom::logical_plan::JoinNode>(
      nextId(),
      std::move(planNode_),
      std::move(rightSubtree),
      toAxiomJoinType(join.join_type()),
      std::move(expr_));
  setPlanNode(planNode, context.planId);
}

void SparkToAxiom::visit(
    const spark::connect::Project& project,
    SparkPlanVisitorContext& context) {
  for (const auto& expression : project.expressions()) {
    if (!findGeneratorFunction(expression).empty()) {
      visitProjectWithGenerator(project, context);
      return;
    }
  }

  // Note that we first visit the project input subtree, before visiting the
  // expressions themselves. This ensures that by the time we visit the
  // expressions, we are ready to bind attributes to the output of the current
  // planNode in `planNode_`.
  SparkPlanVisitor::visit(project.input(), context);

  LOG(INFO) << "Creating a project node [" << context.planId << "]";
  std::vector<facebook::axiom::logical_plan::ExprPtr> exprs;
  std::vector<std::string> exprNames;

  exprs.reserve(project.expressions().size());
  exprNames.reserve(project.expressions().size());

  for (const auto& expression : project.expressions()) {
    visit(expression, context);

    // If no expression name is available, PySpark seems to fall back to use the
    // type as the name.
    auto exprName = std::move(exprName_);
    if (exprName.empty()) {
      exprName = expr_->type()->toString();
      folly::toLowerAscii(exprName);
    }

    exprNames.push_back(std::move(exprName));
    exprs.push_back(std::move(expr_));
  }

  auto planNode = std::make_shared<facebook::axiom::logical_plan::ProjectNode>(
      nextId(), std::move(planNode_), std::move(exprNames), std::move(exprs));
  setPlanNode(planNode, context.planId);
}

void SparkToAxiom::visitProjectWithGenerator(
    const spark::connect::Project& project,
    SparkPlanVisitorContext& context) {
  SparkPlanVisitor::visit(project.input(), context);

  LOG(INFO) << "Creating unnest node from generator in project ["
            << context.planId << "]";

  const spark::connect::Expression* generatorExpr = nullptr;
  std::string generatorName;
  std::vector<std::string> aliasNames;
  int generatorCount = 0;

  std::vector<const spark::connect::Expression*> nonGeneratorExprs;

  for (const auto& expression : project.expressions()) {
    auto genFunc = findGeneratorFunction(expression);
    if (!genFunc.empty()) {
      ++generatorCount;
      if (generatorExpr == nullptr) {
        generatorExpr = &expression;
        generatorName = genFunc;

        if (expression.has_alias()) {
          for (const auto& name : expression.alias().name()) {
            aliasNames.push_back(name);
          }
        }
      }
    } else {
      nonGeneratorExprs.push_back(&expression);
    }
  }

  COLLAGEN_CHECK(
      generatorExpr != nullptr,
      "Expected generator expression in project but none found");

  // Spark rejects multiple generators in a single SELECT.
  COLLAGEN_CHECK(
      generatorCount == 1,
      "Only one generator allowed per select clause but found {}. "
      "Use separate select() calls for multiple explode operations.",
      generatorCount);

  COLLAGEN_CHECK(
      !isOuterGenerator(generatorName),
      "{} is not supported yet. UnnestNode uses inner semantics "
      "(rows with empty arrays/maps are dropped). Use explode/posexplode instead.",
      generatorName);

  const spark::connect::Expression::UnresolvedFunction* genFuncPtr;
  if (generatorExpr->has_alias()) {
    genFuncPtr = &generatorExpr->alias().expr().unresolved_function();
  } else {
    genFuncPtr = &generatorExpr->unresolved_function();
  }

  COLLAGEN_CHECK(
      genFuncPtr->arguments().size() >= 1,
      "{} requires at least 1 argument",
      generatorName);

  visit(genFuncPtr->arguments(0), context);
  auto unnestExpr = std::move(expr_);
  auto unnestExprType = unnestExpr->type();

  std::vector<std::vector<std::string>> unnestedNames;
  const bool isPosExplode = generatorName == "posexplode";
  std::optional<std::string> ordinalityName;

  // Spark's posexplode alias order is (pos, element[, value]). The first alias
  // names the position column; subsequent aliases name the unnested columns.
  // For plain explode, all aliases name the unnested columns.
  const size_t aliasOffset = isPosExplode ? 1 : 0;
  if (isPosExplode) {
    ordinalityName = !aliasNames.empty() ? aliasNames[0] : "pos";
  }

  const auto aliasAt = [&](size_t index, std::string_view fallback) {
    return index < aliasNames.size() ? aliasNames[index]
                                     : std::string{fallback};
  };

  if (unnestExprType->isArray()) {
    unnestedNames.push_back({aliasAt(aliasOffset, "col")});
  } else if (unnestExprType->isMap()) {
    unnestedNames.push_back(
        {aliasAt(aliasOffset, "key"), aliasAt(aliasOffset + 1, "value")});
  } else {
    COLLAGEN_NYI(
        "{} on type {} is not supported",
        generatorName,
        unnestExprType->toString());
  }

  auto unnestNode = std::make_shared<facebook::axiom::logical_plan::UnnestNode>(
      nextId(),
      std::move(planNode_),
      std::vector<ExprPtr>{unnestExpr},
      unnestedNames,
      ordinalityName);

  // posexplode requires a wrapping ProjectNode to convert Velox's 1-based
  // ordinality to Spark's 0-based position.
  const bool needsWrapper = !nonGeneratorExprs.empty() || isPosExplode;
  if (!needsWrapper) {
    setPlanNode(unnestNode, context.planId);
    return;
  }

  planNode_ = unnestNode;

  std::vector<ExprPtr> projExprs;
  std::vector<std::string> projNames;

  for (const auto* expr : nonGeneratorExprs) {
    visit(*expr, context);
    auto name = std::move(exprName_);
    if (name.empty()) {
      name = expr_->type()->toString();
      folly::toLowerAscii(name);
    }
    projNames.emplace_back(std::move(name));
    projExprs.emplace_back(std::move(expr_));
  }

  // UnnestNode emits all input columns followed by unnested columns, with the
  // ordinality column last when present.
  auto unnestOutputType = unnestNode->outputType();
  auto numOutputCols = static_cast<uint32_t>(unnestOutputType->size());
  auto numInputCols =
      static_cast<uint32_t>(unnestNode->onlyInput()->outputType()->size());
  const uint32_t ordinalityIdx =
      isPosExplode ? numOutputCols - 1 : numOutputCols;

  for (uint32_t i = numInputCols; i < numOutputCols; ++i) {
    auto colType = unnestOutputType->childAt(i);
    std::string colName{unnestOutputType->nameOf(i)};
    if (i == ordinalityIdx) {
      // Convert Velox's 1-based ordinality to Spark's 0-based position by
      // subtracting 1.
      auto posRef =
          std::make_shared<facebook::axiom::logical_plan::InputReferenceExpr>(
              colType, colName);
      auto oneConst =
          std::make_shared<facebook::axiom::logical_plan::ConstantExpr>(
              velox::BIGINT(),
              std::make_shared<velox::variant>(velox::variant(int64_t{1})));
      projExprs.emplace_back(
          std::make_shared<facebook::axiom::logical_plan::CallExpr>(
              colType,
              "minus",
              std::vector<ExprPtr>{std::move(posRef), std::move(oneConst)}));
    } else {
      projExprs.emplace_back(
          std::make_shared<facebook::axiom::logical_plan::InputReferenceExpr>(
              colType, colName));
    }
    projNames.emplace_back(std::move(colName));
  }

  auto projectNode =
      std::make_shared<facebook::axiom::logical_plan::ProjectNode>(
          nextId(),
          std::move(planNode_),
          std::move(projNames),
          std::move(projExprs));
  setPlanNode(projectNode, context.planId);
}

void SparkToAxiom::visit(
    const spark::connect::Filter& filter,
    SparkPlanVisitorContext& context) {
  SparkPlanVisitor::visit(filter.input(), context);
  visit(filter.condition(), context);

  LOG(INFO) << "Creating a filter node [" << context.planId << "]: "
            << facebook::axiom::logical_plan::ExprPrinter::toText(*expr_);
  auto planNode = std::make_shared<facebook::axiom::logical_plan::FilterNode>(
      nextId(), std::move(planNode_), std::move(expr_));
  COLLAGEN_CHECK_NULL(expr_);
  setPlanNode(planNode, context.planId);
}

void SparkToAxiom::visit(
    const spark::connect::Aggregate& aggregate,
    SparkPlanVisitorContext& context) {
  // First visit the input relation
  SparkPlanVisitor::visit(aggregate.input(), context);

  LOG(INFO) << "Creating an aggregate node [" << context.planId << "]";

  // init the vectors
  std::vector<facebook::axiom::logical_plan::ExprPtr> groupingKeys;
  groupingKeys.reserve(aggregate.grouping_expressions().size());

  std::vector<facebook::axiom::logical_plan::AggregateExprPtr> aggregates;
  aggregates.reserve(aggregate.aggregate_expressions().size());

  std::vector<std::string> outputNames;
  outputNames.reserve(
      aggregate.grouping_expressions().size() +
      aggregate.aggregate_expressions().size());

  // Clear previous expression and name.
  expr_.reset();
  exprName_.clear();

  // Process grouping expressions
  for (const auto& groupingExpr : aggregate.grouping_expressions()) {
    visit(groupingExpr, context);
    groupingKeys.push_back(std::move(expr_));
    auto exprName = std::move(exprName_);
    if (exprName.empty()) {
      // TODO: find out the pyspark convention if exprName is empty
      exprName = "grouping_key_" + std::to_string(outputNames.size());
    }
    outputNames.push_back(std::move(exprName));
  }

  // Process each aggregate expression
  COLLAGEN_CHECK_NULL(expr_);
  COLLAGEN_CHECK(exprName_.empty());
  for (const auto& aggExpr : aggregate.aggregate_expressions()) {
    context.isAggregate = true;
    visit(aggExpr, context);
    context.isAggregate = false;

    auto resolvedAggrExpr = std::dynamic_pointer_cast<
        const facebook::axiom::logical_plan::AggregateExpr>(expr_);
    COLLAGEN_CHECK(
        resolvedAggrExpr != nullptr,
        "expr_ must be a facebook::axiom::logical_plan::AggregateExpr, got: {}",
        expr_ ? facebook::axiom::logical_plan::ExprPrinter::toText(*expr_)
              : "null");
    aggregates.push_back(resolvedAggrExpr);

    // Use expression name if available, otherwise use function name
    auto exprName = std::move(exprName_);
    if (exprName.empty()) {
      exprName = resolvedAggrExpr->name();
    }
    outputNames.push_back(std::move(exprName));

    // Clear expression after processing
    expr_.reset();
    exprName_.clear();
  }

  // Create the aggregate node
  // For now, we only support simple GROUP BY (no grouping sets)
  std::vector<facebook::axiom::logical_plan::AggregateNode::GroupingSet>
      groupingSets;

  auto planNode =
      std::make_shared<facebook::axiom::logical_plan::AggregateNode>(
          nextId(),
          std::move(planNode_),
          std::move(groupingKeys),
          std::move(groupingSets),
          std::move(aggregates),
          std::move(outputNames));

  setPlanNode(planNode, context.planId);
}

void SparkToAxiom::visit(
    const spark::connect::Limit& limit,
    SparkPlanVisitorContext& context) {
  SparkPlanVisitor::visit(limit.input(), context);
  auto planNode = std::make_shared<facebook::axiom::logical_plan::LimitNode>(
      nextId(), std::move(planNode_), 0, limit.limit());
  setPlanNode(planNode, context.planId);
}

void SparkToAxiom::visit(
    const spark::connect::Offset& offset,
    SparkPlanVisitorContext& context) {
  SparkPlanVisitor::visit(offset.input(), context);
  auto planNode = std::make_shared<facebook::axiom::logical_plan::LimitNode>(
      nextId(),
      std::move(planNode_),
      offset.offset(),
      std::numeric_limits<int64_t>::max());
  setPlanNode(planNode, context.planId);
}

void SparkToAxiom::visit(
    const spark::connect::ToDF& toDF,
    SparkPlanVisitorContext& context) {
  SparkPlanVisitor::visit(toDF.input(), context);

  LOG(INFO) << "Creating a project node [" << context.planId
            << "] for toDF node.";
  std::vector<ExprPtr> exprs;
  std::vector<std::string> exprNames;

  exprs.reserve(toDF.column_names().size());
  exprNames.reserve(toDF.column_names().size());

  for (const auto& columnName : toDF.column_names()) {
    auto exprType = planNode_->outputType()->findChild(columnName);
    exprs.push_back(
        std::make_shared<facebook::axiom::logical_plan::InputReferenceExpr>(
            exprType, columnName));
    exprNames.push_back(columnName);
    LOG(INFO) << "    " << columnName;
  }

  auto planNode = std::make_shared<facebook::axiom::logical_plan::ProjectNode>(
      nextId(), std::move(planNode_), std::move(exprNames), std::move(exprs));
  setPlanNode(planNode, context.planId);
}

void SparkToAxiom::visit(
    const spark::connect::Expression::Alias& alias,
    SparkPlanVisitorContext& context) {
  COLLAGEN_CHECK_EQ(
      alias.name().size(),
      1,
      "Expected exactly one alias name, got {}",
      alias.name().size());
  visit(alias.expr(), context);
  exprName_ = *alias.name().begin();
}

void SparkToAxiom::visit(
    const spark::connect::Expression::UnresolvedStar& /*unresolvedStar*/,
    SparkPlanVisitorContext& /*context*/) {
  // UnresolvedStar represents "*" in expressions like count(*)

  // For aggregate functions like count(*), we create a literal true
  // expression since count(*) means count all rows regardless of column
  // values
  // TODO: handle cases where the "*" needs to actually be expanded to a list
  // of column references. Fix this when Axiom supports SpecialFormExpr of type
  // kStar
  LOG(INFO) << "-- UnresolvedStar (*)";
  CHECK_EQ(expr_, nullptr);

  // Create a literal TRUE expression to represent * in count(*)
  expr_ = std::make_shared<facebook::axiom::logical_plan::ConstantExpr>(
      velox::BOOLEAN(), std::make_shared<velox::variant>(velox::variant(true)));

  exprName_ = "*";
}

void SparkToAxiom::visit(
    const spark::connect::SubqueryAlias& subqueryAlias,
    SparkPlanVisitorContext& context) {
  SparkPlanVisitor::visit(subqueryAlias.input(), context);
  sparkAliasToPlanNode_.emplace(subqueryAlias.alias(), planNode_);
}

void SparkToAxiom::visit(
    const spark::connect::WriteOperation& writeOperation,
    SparkPlanVisitorContext& context) {
  SparkPlanVisitor::visit(writeOperation.input(), context);

  // Only handle write to tables, other options like writing to
  // path are not supported yet
  if (writeOperation.save_type_case() !=
      spark::connect::WriteOperation::kTable) {
    COLLAGEN_NYI(
        "Unsupported write operation type: {}",
        writeOperation.save_type_case());
  }

  const auto& saveTable = writeOperation.table();
  const auto& rawTableName = saveTable.table_name();
  COLLAGEN_CHECK(
      rawTableName.find('.') == std::string::npos,
      "Schema-qualified table names are not supported: {}",
      rawTableName);

  facebook::axiom::SchemaTableName tableName{schema_, rawTableName};

  LOG(INFO) << "Creating table write node [" << context.planId
            << "] for table '" << tableName.toString() << "'";

  facebook::axiom::logical_plan::WriteKind writeKind;
  switch (writeOperation.mode()) {
    case spark::connect::WriteOperation::SAVE_MODE_APPEND:
      // APPEND mode: Insert rows into existing table
      writeKind = facebook::axiom::logical_plan::WriteKind::kInsert;
      break;

    case spark::connect::WriteOperation::SAVE_MODE_ERROR_IF_EXISTS:
      // ERROR_IF_EXISTS mode: Create new table, error if exists
      writeKind = facebook::axiom::logical_plan::WriteKind::kCreate;
      break;

    case spark::connect::WriteOperation::SAVE_MODE_OVERWRITE:
      // OVERWRITE mode: Not yet implemented
      COLLAGEN_NYI("SAVE_MODE_OVERWRITE is not yet supported for table writes");

    case spark::connect::WriteOperation::SAVE_MODE_IGNORE:
      // IGNORE mode: Not yet implemented
      COLLAGEN_NYI("SAVE_MODE_IGNORE is not yet supported for table writes");

    default:
      COLLAGEN_NYI("Unknown save mode: {}", writeOperation.mode());
  }

  // Get the input type to determine column names and create column
  // expressions
  const auto inputType = planNode_->outputType();
  std::vector<std::string> columnNames;
  std::vector<facebook::axiom::logical_plan::ExprPtr> columnExpressions;

  columnNames.reserve(inputType->size());
  columnExpressions.reserve(inputType->size());

  for (uint32_t i = 0; i < inputType->size(); ++i) {
    const auto& columnName = inputType->nameOf(i);
    auto columnType = inputType->childAt(i);

    columnNames.push_back(columnName);
    columnExpressions.push_back(
        std::make_shared<facebook::axiom::logical_plan::InputReferenceExpr>(
            columnType, columnName));
  }

  // Convert options map
  folly::F14FastMap<std::string, std::string> options;
  for (const auto& [key, value] : writeOperation.options()) {
    options.emplace(key, value);
  }

  auto planNode =
      std::make_shared<facebook::axiom::logical_plan::TableWriteNode>(
          nextId(),
          std::move(planNode_),
          catalog_,
          tableName,
          writeKind,
          std::move(columnNames),
          std::move(columnExpressions),
          std::move(options));

  setPlanNode(planNode, context.planId);

  // TODO: handle partitioning, bucketing, and sorting.
}

void SparkToAxiom::visit(
    const spark::connect::Sort& sort,
    SparkPlanVisitorContext& context) {
  SparkPlanVisitor::visit(sort.input(), context);

  LOG(INFO) << "Creating a sort node [" << context.planId << "]";

  std::vector<facebook::axiom::logical_plan::SortingField> sortKeys;
  sortKeys.reserve(sort.order().size());

  for (const auto& sortOrder : sort.order()) {
    visit(sortOrder.child(), context);
    auto sortKey = expr_;

    // Convert sort direction and null ordering
    bool ascending = true;
    bool nullsFirst = false;

    switch (sortOrder.direction()) {
      case spark::connect::Expression::SortOrder::SORT_DIRECTION_ASCENDING:
        ascending = true;
        break;
      case spark::connect::Expression::SortOrder::SORT_DIRECTION_DESCENDING:
        ascending = false;
        break;
      default:
        ascending = true;
        break;
    }

    switch (sortOrder.null_ordering()) {
      case spark::connect::Expression::SortOrder::SORT_NULLS_FIRST:
        nullsFirst = true;
        break;
      case spark::connect::Expression::SortOrder::SORT_NULLS_LAST:
        nullsFirst = false;
        break;
      default:
        nullsFirst = false;
        break;
    }

    facebook::axiom::logical_plan::SortOrder order(ascending, nullsFirst);
    sortKeys.push_back(
        facebook::axiom::logical_plan::SortingField{sortKey, order});
    expr_.reset();
  }

  auto planNode = std::make_shared<facebook::axiom::logical_plan::SortNode>(
      nextId(), std::move(planNode_), std::move(sortKeys));
  setPlanNode(planNode, context.planId);
}

void SparkToAxiom::visit(
    const spark::connect::Range& range,
    SparkPlanVisitorContext& context) {
  // This is a leaf node, so there should not be an existing plan node.
  COLLAGEN_CHECK_NULL(planNode_);

  // Extract range parameters
  int64_t start = range.has_start() ? range.start() : 0;
  int64_t end = range.end();
  int64_t step = range.step();

  LOG(INFO) << "Creating range node [" << context.planId
            << "] with start=" << start << ", end=" << end << ", step=" << step;

  // Validate step parameter
  COLLAGEN_CHECK_NE(step, 0, "Range step cannot be zero");

  // Calculate the number of elements in the range
  int32_t numElements;
  if (step > 0) {
    numElements = (end > start) ? (end - start + step - 1) / step : 0;
  } else {
    numElements = (start > end) ? (start - end - step - 1) / (-step) : 0;
  }

  // Create the output type: single BIGINT column named "id"
  auto outputType = velox::ROW({"id"}, {velox::BIGINT()});

  // Generate the range values as Velox vectors
  std::vector<velox::RowVectorPtr> values;

  if (numElements > 0) {
    // Create a flat vector for the range values
    auto rangeVector =
        velox::BaseVector::create(velox::BIGINT(), numElements, pool_);
    auto flatVector = rangeVector->asFlatVector<int64_t>();

    // Populate the vector with range values
    int64_t currentValue = start;
    for (int32_t i = 0; i < numElements; ++i) {
      flatVector->set(i, currentValue);
      currentValue += step;
    }

    // Create a row vector containing the range values
    std::vector<velox::VectorPtr> children = {rangeVector};
    auto rowVector = std::make_shared<velox::RowVector>(
        pool_, outputType, nullptr, numElements, std::move(children));
    values.push_back(rowVector);
  } else {
    // Create an empty vector with the correct type
    values.push_back(velox::RowVector::createEmpty(outputType, pool_));
  }

  // Create the ValuesNode
  auto planNode = std::make_shared<facebook::axiom::logical_plan::ValuesNode>(
      nextId(), std::move(values));
  setPlanNode(planNode, context.planId);
}

namespace {

struct Identifier {
  std::string name;
  std::string parent;
  std::string connectorId;

  void updateReference(
      const facebook::axiom::logical_plan::LogicalPlanNodePtr& planNode) {
    if (auto tableScan = std::dynamic_pointer_cast<
            const facebook::axiom::logical_plan::TableScanNode>(planNode)) {
      parent = tableScan->tableName().toString();
      connectorId = tableScan->connectorId();
    }
  }
};

Identifier parseIdentifier(const std::string& input) {
  std::vector<std::string> parts;
  folly::split('.', input, parts, true); // true = ignore empty tokens

  if (parts.size() == 1) {
    return {.name = parts[0]};
  } else if (parts.size() == 2) {
    return {.name = parts[1], .parent = parts[0]};
  } else if (parts.size() == 3) {
    return {.name = parts[2], .parent = parts[1], .connectorId = parts[0]};
  }
  COLLAGEN_FAIL("Unable to parse identifier: '{}'", input);
}

} // namespace

void SparkToAxiom::visit(
    const spark::connect::Expression::UnresolvedAttribute& unresolvedAttribute,
    SparkPlanVisitorContext& context) {
  auto referencePlanId = unresolvedAttribute.plan_id();
  LOG(INFO) << "-- Unresolved attribute: "
            << unresolvedAttribute.unparsed_identifier() << " - "
            << referencePlanId;
  COLLAGEN_CHECK_NULL(
      expr_,
      "Found unexpected expression '{}'",
      facebook::axiom::logical_plan::ExprPrinter::toText(*expr_));
  COLLAGEN_CHECK_NOT_NULL(planNode_);

  velox::TypePtr exprType;
  auto identifier = parseIdentifier(unresolvedAttribute.unparsed_identifier());
  exprName_ = identifier.name;

  // First check if the input tree has an explicit plan id set; if it does,
  // check the output of that plan node.
  if (unresolvedAttribute.has_plan_id()) {
    // Look up the output type of the plan id referenced by this attribute
    // node.
    auto it = sparkPlanIdToPlanNode_.find(referencePlanId);
    if (it == sparkPlanIdToPlanNode_.end()) {
      COLLAGEN_USER_FAIL(
          "Unable to find Spark Connect plan id: {}", referencePlanId);
    }
    exprType = it->second->outputType()->findChild(exprName_);
    identifier.updateReference(it->second);
  } else if (!identifier.parent.empty()) {
    // Look up the output type of the plan id referenced by the alias name.
    auto it = sparkAliasToPlanNode_.find(identifier.parent);
    if (it == sparkAliasToPlanNode_.end()) {
      COLLAGEN_USER_FAIL(
          "Unable to find Spark Connect reference: '{}'", identifier.parent);
    }
    exprType = it->second->outputType()->findChild(exprName_);
    identifier.updateReference(it->second);
  }
  // If an explicit plan id wasn't set, try to resolve the attribute in the
  // output of the last plan node.
  else {
    if (auto idx = planNode_->outputType()->getChildIdxIfExists(exprName_)) {
      exprType = planNode_->outputType()->childAt(idx.value());
    } else {
      COLLAGEN_USER_FAIL(
          "Unable to resolve reference '{}'.",
          unresolvedAttribute.unparsed_identifier());
    }
  }
  expr_ = std::make_shared<facebook::axiom::logical_plan::InputReferenceExpr>(
      exprType, identifier.name);
}

void SparkToAxiom::visit(
    const spark::connect::Expression::UnresolvedFunction& unresolvedFunction,
    SparkPlanVisitorContext& context) {
  const auto& functionName = unresolvedFunction.function_name();
  LOG(INFO) << "-- Unresolved function: " << functionName;
  CHECK_EQ(expr_, nullptr);

  std::vector<facebook::axiom::logical_plan::ExprPtr> params;
  std::vector<velox::TypePtr> paramTypes;

  params.reserve(unresolvedFunction.arguments().size());
  paramTypes.reserve(unresolvedFunction.arguments().size());

  // Check if this is a higher-order function that takes a lambda.
  // For these functions, we need to infer lambda parameter types from
  // the array argument before visiting the lambda.
  // Higher-order functions that take lambda arguments.
  // These functions are handled specially to infer lambda parameter types
  // from the array/collection argument types.
  const bool isHigherOrderFunction = functionName == "transform" ||
      functionName == "filter" || functionName == "zip_with" ||
      functionName == "map_zip_with" || functionName == "transform_values" ||
      functionName == "transform_keys" || functionName == "exists" ||
      functionName == "forall" || functionName == "map_filter" ||
      functionName == "find_first_index" || functionName == "aggregate";

  if (isHigherOrderFunction && unresolvedFunction.arguments().size() >= 2) {
    // For higher-order functions, first visit the array/collection argument
    // to determine the element type for lambda parameter inference.
    visitHigherOrderFunction(unresolvedFunction, context, params, paramTypes);
    return;
  }

  for (const auto& argument : unresolvedFunction.arguments()) {
    visit(argument, context);
    paramTypes.push_back(expr_->type());
    params.push_back(std::move(expr_));
  }

  if (context.isAggregate) {
    // Try to resolve as aggregate function.
    const auto veloxFunctionName = toVeloxFunctionName(functionName);
    auto finalType =
        velox::exec::resolveResultType(veloxFunctionName, paramTypes);

    LOG(INFO) << "Successfully resolved '" << veloxFunctionName
              << "' as aggregate function";
    expr_ = std::make_shared<facebook::axiom::logical_plan::AggregateExpr>(
        finalType, veloxFunctionName, std::move(params));
    return;
  }

  // Check if it's a conjunct. Conjuncts (AND/OR) need to generate a special
  // form expression node.
  //
  // TODO: handle other types of special forms.
  if (functionName == "and") {
    expr_ = std::make_shared<facebook::axiom::logical_plan::SpecialFormExpr>(
        velox::BOOLEAN(),
        facebook::axiom::logical_plan::SpecialForm::kAnd,
        std::move(params));
  } else if (functionName == "or") {
    expr_ = std::make_shared<facebook::axiom::logical_plan::SpecialFormExpr>(
        velox::BOOLEAN(),
        facebook::axiom::logical_plan::SpecialForm::kOr,
        std::move(params));
  } else if (functionName == "coalesce") {
    // coalesce returns the first non-null value
    // The return type is the same as the first argument
    COLLAGEN_CHECK(!params.empty(), "coalesce requires at least one argument");
    // @lint-ignore CLANGTIDY facebook-hte-LocalUncheckedArrayBounds
    auto resultType = params[0]->type();
    expr_ = std::make_shared<facebook::axiom::logical_plan::SpecialFormExpr>(
        resultType,
        facebook::axiom::logical_plan::SpecialForm::kCoalesce,
        std::move(params));
  } else if (functionName == "if" || functionName == "when") {
    // IF/WHEN expression handling:
    // Simple: if(condition, then_value, else_value) -> SpecialForm::kIf
    // Chained: when(c1, v1, c2, v2, ..., else) -> SpecialForm::kSwitch
    COLLAGEN_CHECK(
        params.size() >= 2,
        "if/when requires at least 2 arguments (condition, then_value)");

    // Capture type before move since evaluation order is unspecified
    // @lint-ignore CLANGTIDY facebook-hte-LocalUncheckedArrayBounds
    auto resultType = params[1]->type();

    // Chained when() with more than 3 args uses SWITCH
    // Format: switch(cond1, val1, cond2, val2, ..., else_val)
    if (params.size() > 3) {
      expr_ = std::make_shared<facebook::axiom::logical_plan::SpecialFormExpr>(
          resultType,
          facebook::axiom::logical_plan::SpecialForm::kSwitch,
          std::move(params));
    } else {
      // Simple when/otherwise uses IF
      expr_ = std::make_shared<facebook::axiom::logical_plan::SpecialFormExpr>(
          resultType,
          facebook::axiom::logical_plan::SpecialForm::kIf,
          std::move(params));
    }
  } else if (functionName == "isnotnull") {
    // isnotnull(x) = not(is_null(x))
    COLLAGEN_CHECK(params.size() == 1, "isnotnull requires exactly 1 argument");
    auto isNullExpr = std::make_shared<facebook::axiom::logical_plan::CallExpr>(
        velox::BOOLEAN(), "is_null", std::move(params));
    expr_ = std::make_shared<facebook::axiom::logical_plan::CallExpr>(
        velox::BOOLEAN(),
        "not",
        std::vector<facebook::axiom::logical_plan::ExprPtr>{isNullExpr});
  } else {
    const auto veloxFunctionName = toVeloxFunctionName(functionName);
    std::vector<velox::TypePtr> coercions;

    auto returnType =
        collagen::resolveFunction(veloxFunctionName, paramTypes, coercions);

    if (!coercions.empty()) {
      for (auto i = 0; i < params.size(); i++) {
        if (const auto& coercion = coercions.at(i)) {
          params[i] =
              std::make_shared<facebook::axiom::logical_plan::SpecialFormExpr>(
                  coercion,
                  facebook::axiom::logical_plan::SpecialForm::kCast,
                  params[i]);
        }
      }
    }
    expr_ = std::make_shared<facebook::axiom::logical_plan::CallExpr>(
        returnType, veloxFunctionName, std::move(params));
  }
}

void SparkToAxiom::visit(
    const spark::connect::Expression::UnresolvedExtractValue&
        unresolvedExtractValue,
    SparkPlanVisitorContext& context) {
  LOG(INFO) << "-- Unresolved extract value: ";
  std::vector<facebook::axiom::logical_plan::ExprPtr> params;

  // First visit the child column.
  visit(unresolvedExtractValue.child(), context);
  auto childType = expr_->type();
  params.emplace_back(std::move(expr_));

  // Then visit the "extract" modifier.
  visit(unresolvedExtractValue.extraction(), context);
  params.emplace_back(std::move(expr_));

  // Array subscript will return the type of the array elements.
  if (childType->isArray()) {
    expr_ = std::make_shared<facebook::axiom::logical_plan::CallExpr>(
        childType->childAt(0), "subscript", std::move(params));
  }
  // Map subscript will return the type of map values.
  else if (childType->isMap()) {
    expr_ = std::make_shared<facebook::axiom::logical_plan::CallExpr>(
        childType->childAt(1), "subscript", std::move(params));
  }
  // Row field subscript will return the type of the field.
  else if (childType->isRow()) {
    if (params.back()->type() == velox::VARCHAR() &&
        params.back()->kind() ==
            facebook::axiom::logical_plan::ExprKind::kConstant) {
      auto constant = std::dynamic_pointer_cast<
          const facebook::axiom::logical_plan::ConstantExpr>(params.back());
      COLLAGEN_CHECK_NOT_NULL(constant);
      auto fieldName = constant->value()->value<velox::TypeKind::VARCHAR>();

      expr_ = std::make_shared<facebook::axiom::logical_plan::SpecialFormExpr>(
          childType->asRow().findChild(fieldName),
          facebook::axiom::logical_plan::SpecialForm::kDereference,
          std::move(params));
    } else {
      COLLAGEN_USER_FAIL("Unexpected extract modifier for subfield.");
    }
  } else {
    COLLAGEN_USER_FAIL(
        "Wrong child type for extract value: '{}'", childType->toString());
  }
}

void SparkToAxiom::visit(
    const spark::connect::WithColumnsRenamed& withColumnsRenamed,
    SparkPlanVisitorContext& context) {
  SparkPlanVisitor::visit(withColumnsRenamed.input(), context);

  LOG(INFO) << "Creating a project node [" << context.planId
            << "] for WithColumnsRenamed node.";

  const auto& renameMap = withColumnsRenamed.rename_columns_map();
  std::vector<ExprPtr> exprs;
  std::vector<std::string> exprNames;

  const auto inputType = planNode_->outputType();
  exprs.reserve(inputType->size());
  exprNames.reserve(inputType->size());

  for (uint32_t i = 0; i < inputType->size(); ++i) {
    const auto& originalName = inputType->nameOf(i);

    // Check if this column should be renamed
    std::string newName = originalName;
    auto it = renameMap.find(originalName);
    if (it != renameMap.end()) {
      newName = it->second;
      LOG(INFO) << "    Renaming column '" << originalName << "' to '"
                << newName << "'";
    }

    auto exprType = inputType->childAt(i);
    exprs.push_back(
        std::make_shared<facebook::axiom::logical_plan::InputReferenceExpr>(
            exprType, originalName));
    exprNames.push_back(newName);
  }

  auto planNode = std::make_shared<facebook::axiom::logical_plan::ProjectNode>(
      nextId(), std::move(planNode_), std::move(exprNames), std::move(exprs));
  setPlanNode(planNode, context.planId);
}

void SparkToAxiom::visit(
    const spark::connect::WithColumns& withColumns,
    SparkPlanVisitorContext& context) {
  SparkPlanVisitor::visit(withColumns.input(), context);

  LOG(INFO) << "Creating a project node [" << context.planId
            << "] for WithColumns node.";

  std::vector<ExprPtr> exprs;
  std::vector<std::string> exprNames;

  const auto inputType = planNode_->outputType();

  // Build a map of column names to their new expressions from the aliases.
  // This allows us to replace existing columns or add new ones.
  std::unordered_map<std::string, std::pair<ExprPtr, std::string>> newColumns;
  for (const auto& alias : withColumns.aliases()) {
    // Visit the expression to get the expr_
    visit(alias.expr(), context);

    // The alias should have exactly one name
    COLLAGEN_CHECK(
        alias.name_size() == 1,
        "Expected exactly one name in WithColumns alias, got {}",
        alias.name_size());

    const auto& colName = alias.name(0);
    newColumns[colName] = {std::move(expr_), colName};
    LOG(INFO) << "    Adding/replacing column '" << colName << "'";
  }

  // First, add all existing columns (replacing if there's a new expression)
  for (uint32_t i = 0; i < inputType->size(); ++i) {
    const auto& originalName = inputType->nameOf(i);

    auto it = newColumns.find(originalName);
    if (it != newColumns.end()) {
      // Replace this column with the new expression
      exprs.push_back(std::move(it->second.first));
      exprNames.push_back(it->second.second);
      newColumns.erase(it); // Remove from map so we don't add it again
    } else {
      // Keep the original column
      auto exprType = inputType->childAt(i);
      exprs.push_back(
          std::make_shared<facebook::axiom::logical_plan::InputReferenceExpr>(
              exprType, originalName));
      exprNames.push_back(originalName);
    }
  }

  // Then, add any remaining new columns that weren't replacements
  for (auto& [colName, colData] : newColumns) {
    exprs.push_back(std::move(colData.first));
    exprNames.push_back(colData.second);
  }

  auto planNode = std::make_shared<facebook::axiom::logical_plan::ProjectNode>(
      nextId(), std::move(planNode_), std::move(exprNames), std::move(exprs));
  setPlanNode(planNode, context.planId);
}

void SparkToAxiom::visit(
    const spark::connect::Expression::Literal& literal,
    SparkPlanVisitorContext& context) {
  expr_ = std::make_shared<facebook::axiom::logical_plan::ConstantExpr>(
      toVeloxType(literal), toVeloxVariant(literal));
}

void SparkToAxiom::visit(
    const spark::connect::Expression::Cast& cast,
    SparkPlanVisitorContext& context) {
  LOG(INFO) << "-- Cast expression";

  // First, visit the expression being cast
  visit(cast.expr(), context);
  auto inputExpr = std::move(expr_);

  // Determine the target type
  velox::TypePtr targetType;
  if (cast.has_type()) {
    targetType = toVeloxType(cast.type());
  } else if (cast.has_type_str()) {
    // Parse the type string (e.g., "long", "string")
    const auto& typeStr = cast.type_str();
    if (typeStr == "long" || typeStr == "bigint") {
      targetType = velox::BIGINT();
    } else if (typeStr == "int" || typeStr == "integer") {
      targetType = velox::INTEGER();
    } else if (typeStr == "short" || typeStr == "smallint") {
      targetType = velox::SMALLINT();
    } else if (typeStr == "byte" || typeStr == "tinyint") {
      targetType = velox::TINYINT();
    } else if (typeStr == "double") {
      targetType = velox::DOUBLE();
    } else if (typeStr == "float" || typeStr == "real") {
      targetType = velox::REAL();
    } else if (typeStr == "string" || typeStr == "varchar") {
      targetType = velox::VARCHAR();
    } else if (typeStr == "boolean" || typeStr == "bool") {
      targetType = velox::BOOLEAN();
    } else if (typeStr == "binary" || typeStr == "varbinary") {
      targetType = velox::VARBINARY();
    } else if (typeStr == "date") {
      targetType = velox::DATE();
    } else if (typeStr == "timestamp") {
      targetType = velox::TIMESTAMP();
    } else {
      COLLAGEN_NYI("Cast type string not supported: '{}'", typeStr);
    }
  } else {
    COLLAGEN_FAIL("Cast expression must have either type or type_str set");
  }

  // Create a CAST special form expression
  expr_ = std::make_shared<facebook::axiom::logical_plan::SpecialFormExpr>(
      targetType, facebook::axiom::logical_plan::SpecialForm::kCast, inputExpr);
}

void SparkToAxiom::visit(
    const spark::connect::SetOperation& setOperation,
    SparkPlanVisitorContext& context) {
  LOG(INFO) << "Creating a set operation node [" << context.planId
            << "] (type=" << setOperation.set_op_type() << ")";

  // Visit the left input relation first
  SparkPlanVisitor::visit(setOperation.left_input(), context);
  auto leftSubtree = std::move(planNode_);

  // Visit the right input relation
  SparkPlanVisitor::visit(setOperation.right_input(), context);
  auto rightSubtree = std::move(planNode_);

  // Create the inputs vector for the set operation
  std::vector<facebook::axiom::logical_plan::LogicalPlanNodePtr> inputs;
  inputs.push_back(std::move(leftSubtree));
  inputs.push_back(std::move(rightSubtree));

  // Determine if it's an ALL operation (preserve duplicates)
  bool isAll = setOperation.has_is_all() && setOperation.is_all();

  // Convert Spark set operation type to Axiom set operation type
  auto operation = toAxiomSetOperation(setOperation.set_op_type(), isAll);

  // Create the set operation node
  auto planNode = std::make_shared<facebook::axiom::logical_plan::SetNode>(
      nextId(), inputs, operation);

  setPlanNode(planNode, context.planId);
}

std::string SparkToAxiom::toString() {
  if (planNode_ == nullptr) {
    return "<empty>";
  }
  return facebook::axiom::logical_plan::PlanPrinter::toText(*planNode_);
}

namespace {

// Builds a dot-separated variable name from the name_parts of a lambda
// variable protobuf message.
std::string buildLambdaVariableName(
    const spark::connect::Expression::UnresolvedNamedLambdaVariable& var) {
  std::string name;
  for (const auto& part : var.name_parts()) {
    if (!name.empty()) {
      name += ".";
    }
    name += part;
  }
  return name;
}

} // namespace

void SparkToAxiom::visitHigherOrderFunction(
    const spark::connect::Expression::UnresolvedFunction& unresolvedFunction,
    SparkPlanVisitorContext& context,
    std::vector<facebook::axiom::logical_plan::ExprPtr>& params,
    std::vector<velox::TypePtr>& paramTypes) {
  const auto& functionName = unresolvedFunction.function_name();
  LOG(INFO) << "-- Higher-order function: " << functionName;

  // First, visit the array/collection argument to get its type.
  const auto& firstArg = unresolvedFunction.arguments(0);
  visit(firstArg, context);
  auto arrayType = expr_->type();
  paramTypes.push_back(arrayType);
  params.push_back(std::move(expr_));

  // Determine the lambda parameter type(s) based on the function and array
  // type.
  velox::TypePtr lambdaElementType = velox::UNKNOWN();

  if (arrayType->isArray()) {
    // For array functions, the lambda parameter is the array element type.
    lambdaElementType = arrayType->childAt(0);
  } else if (arrayType->isMap()) {
    // For map functions, we'll handle key/value types separately below.
    lambdaElementType = arrayType->childAt(1); // value type by default
  }

  // Process remaining arguments, inferring lambda parameter types.
  int lambdaIndex = 0;
  for (int i = 1; i < unresolvedFunction.arguments().size(); ++i) {
    const auto& argument = unresolvedFunction.arguments(i);

    if (argument.has_lambda_function()) {
      // This is a lambda argument - set up the lambda parameter types before
      // visiting.
      const auto& lambdaFunc = argument.lambda_function();
      const auto numLambdaParams = lambdaFunc.arguments().size();

      // Collect inferred lambda variable types.
      std::vector<std::pair<std::string, velox::TypePtr>> inferredVars;
      for (const auto& arg : lambdaFunc.arguments()) {
        auto paramName = buildLambdaVariableName(arg);

        // Determine the type for this lambda parameter based on the function.
        // Currently only transform, filter, zip_with, and map_zip_with are
        // supported.
        velox::TypePtr paramType = velox::UNKNOWN();

        if (functionName == "transform" || functionName == "filter" ||
            functionName == "exists" || functionName == "forall" ||
            functionName == "find_first_index") {
          // Single-parameter lambdas: element type
          if (numLambdaParams == 1) {
            paramType = lambdaElementType;
          } else if (numLambdaParams == 2) {
            // Some functions have (element, index) signature
            // First param is element, second is index (BIGINT)
            if (&arg == &lambdaFunc.arguments(0)) {
              paramType = lambdaElementType;
            } else {
              paramType = velox::BIGINT();
            }
          }
        } else if (functionName == "zip_with") {
          // zip_with(array1, array2, (x, y) -> ...)
          // Lambda takes elements from both arrays
          if (numLambdaParams == 2 && params.size() >= 2) {
            auto secondArrayType = params[1]->type();
            if (&arg == &lambdaFunc.arguments(0)) {
              paramType = lambdaElementType; // first array element type
            } else if (secondArrayType->isArray()) {
              paramType =
                  secondArrayType->childAt(0); // second array element type
            }
          }
        } else if (functionName == "map_zip_with") {
          // map_zip_with(map1, map2, (k, v1, v2) -> ...)
          // Lambda takes key and values from both maps
          if (numLambdaParams == 3 && params.size() >= 2) {
            auto firstMapType = arrayType; // First map type
            auto secondMapType = params[1]->type();
            if (&arg == &lambdaFunc.arguments(0)) {
              // First param is key type (from first map)
              if (firstMapType->isMap()) {
                paramType = firstMapType->childAt(0);
              }
            } else if (&arg == &lambdaFunc.arguments(1)) {
              // Second param is value from first map
              if (firstMapType->isMap()) {
                paramType = firstMapType->childAt(1);
              }
            } else {
              // Third param is value from second map
              if (secondMapType->isMap()) {
                paramType = secondMapType->childAt(1);
              }
            }
          }
        } else if (
            functionName == "transform_values" ||
            functionName == "transform_keys") {
          // transform_values(map, (k, v) -> ...) or transform_keys(map, (k, v)
          // -> ...) Lambda takes key and value from the map
          if (numLambdaParams == 2 && arrayType->isMap()) {
            if (&arg == &lambdaFunc.arguments(0)) {
              // First param is key type
              paramType = arrayType->childAt(0);
            } else {
              // Second param is value type
              paramType = arrayType->childAt(1);
            }
          }
        } else if (functionName == "map_filter") {
          // map_filter(map, (key, value) -> boolean)
          // Lambda takes key and value from the map, returns boolean
          if (numLambdaParams == 2 && arrayType->isMap()) {
            if (&arg == &lambdaFunc.arguments(0)) {
              // First param is key type
              paramType = arrayType->childAt(0);
            } else {
              // Second param is value type
              paramType = arrayType->childAt(1);
            }
          }
        } else if (functionName == "aggregate") {
          // aggregate(array(T), S, function(S, T, S), function(S, R))
          // PySpark sends "aggregate"; mapped to Velox "reduce".
          // Lambda 0 (merge): params are (state S, element T)
          // Lambda 1 (output): param is (state S)
          // State type S comes from the initial value (params[1]).
          velox::TypePtr stateType =
              params.size() >= 2 ? params[1]->type() : velox::UNKNOWN();
          if (lambdaIndex == 0) {
            // Merge lambda: (S, T) -> S
            if (&arg == &lambdaFunc.arguments(0)) {
              paramType = stateType;
            } else {
              paramType = lambdaElementType;
            }
          } else {
            // Output lambda: (S) -> R
            paramType = stateType;
          }
        }

        inferredVars.emplace_back(paramName, paramType);
        LOG(INFO) << "  Lambda param '" << paramName << "' inferred type: "
                  << (paramType ? paramType->toString() : "UNKNOWN");
      }

      // Create scope to pre-register inferred types. The scope ensures
      // proper cleanup and supports nested lambda shadowing.
      LambdaScope preScope(context, inferredVars);

      // Now visit the lambda with the types already set up.
      visit(argument, context);
      paramTypes.push_back(expr_->type());
      params.push_back(std::move(expr_));
      ++lambdaIndex;
    } else {
      // Regular argument, visit normally.
      visit(argument, context);
      paramTypes.push_back(expr_->type());
      params.push_back(std::move(expr_));
    }
  }

  // Now resolve the function as usual.
  const auto veloxFunctionName = toVeloxFunctionName(functionName);
  std::vector<velox::TypePtr> coercions;

  auto returnType =
      collagen::resolveFunction(veloxFunctionName, paramTypes, coercions);

  if (!coercions.empty()) {
    for (auto i = 0; i < params.size(); i++) {
      if (const auto& coercion = coercions.at(i)) {
        params[i] =
            std::make_shared<facebook::axiom::logical_plan::SpecialFormExpr>(
                coercion,
                facebook::axiom::logical_plan::SpecialForm::kCast,
                params[i]);
      }
    }
  }
  expr_ = std::make_shared<facebook::axiom::logical_plan::CallExpr>(
      returnType, veloxFunctionName, std::move(params));
}

void SparkToAxiom::visit(
    const spark::connect::Expression::LambdaFunction& lambdaFunction,
    SparkPlanVisitorContext& context) {
  LOG(INFO) << "-- Lambda function with " << lambdaFunction.arguments().size()
            << " arguments";
  COLLAGEN_CHECK_NULL(
      expr_,
      "Found unexpected expression '{}'",
      facebook::axiom::logical_plan::ExprPrinter::toText(*expr_));

  // Build the lambda signature from the arguments.
  std::vector<std::string> parameterNames;
  std::vector<velox::TypePtr> parameterTypes;
  std::vector<std::pair<std::string, velox::TypePtr>> scopeVars;

  for (const auto& arg : lambdaFunction.arguments()) {
    auto paramName = buildLambdaVariableName(arg);
    parameterNames.push_back(paramName);

    // Check if the type was pre-registered by visitHigherOrderFunction.
    // If not, fall back to UNKNOWN.
    velox::TypePtr paramType;
    auto it = context.lambdaVariables.find(paramName);
    if (it != context.lambdaVariables.end() && it->second != nullptr &&
        it->second->kind() != velox::TypeKind::UNKNOWN) {
      paramType = it->second;
      LOG(INFO) << "  Using pre-inferred type for '" << paramName
                << "': " << it->second->toString();
    } else {
      paramType = velox::UNKNOWN();
      LOG(INFO) << "  No pre-inferred type for '" << paramName
                << "', using UNKNOWN";
    }
    parameterTypes.push_back(paramType);
    scopeVars.emplace_back(paramName, paramType);
  }

  // Create the signature as a RowType.
  auto signature =
      velox::ROW(std::move(parameterNames), std::move(parameterTypes));

  // Create a lambda scope for proper variable scoping. This supports nested
  // lambdas by saving/restoring any shadowed variables on scope exit.
  LambdaScope scope(context, scopeVars);

  // Visit the lambda body expression.
  visit(lambdaFunction.function(), context);
  auto bodyExpr = std::move(expr_);

  // Create the LambdaExpr.
  expr_ = std::make_shared<facebook::axiom::logical_plan::LambdaExpr>(
      signature, std::move(bodyExpr));
}

void SparkToAxiom::visit(
    const spark::connect::Expression::UnresolvedNamedLambdaVariable&
        unresolvedNamedLambdaVariable,
    SparkPlanVisitorContext& context) {
  auto varName = buildLambdaVariableName(unresolvedNamedLambdaVariable);

  LOG(INFO) << "-- Unresolved named lambda variable: " << varName;
  COLLAGEN_CHECK_NULL(
      expr_,
      "Found unexpected expression '{}'",
      facebook::axiom::logical_plan::ExprPrinter::toText(*expr_));

  // Look up the variable type from the lambda context.
  auto it = context.lambdaVariables.find(varName);
  if (it != context.lambdaVariables.end()) {
    // Create an InputReferenceExpr for the lambda variable.
    expr_ = std::make_shared<facebook::axiom::logical_plan::InputReferenceExpr>(
        it->second, varName);
  } else {
    COLLAGEN_USER_FAIL(
        "Lambda variable '{}' not found in current context", varName);
  }
  exprName_ = varName;
}

} // namespace axiom::collagen
