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

#include "axiom/spark_connect/SparkToAxiom.h"

#include <boost/algorithm/string/predicate.hpp>
#include <folly/String.h>

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/logical_plan/Expr.h"
#include "axiom/logical_plan/NameMappings.h"
#include "axiom/logical_plan/PlanPrinter.h"
#include "axiom/spark_connect/ArrowIpc.h"
#include "axiom/spark_connect/SparkVeloxConverter.h"
#include "velox/common/base/Exceptions.h"
#include "velox/functions/prestosql/coercion/PrestoCoercions.h"
#include "velox/vector/FlatVector.h"

using namespace facebook;

namespace axiom::spark_connect {

namespace lp = facebook::axiom::logical_plan;

namespace {

// Resolves Spark's 'x[key]', which reaches the resolver as a subscript call
// once the type of 'x' is known:
//  - an array subscript reads a 0-based index, Spark's get();
//  - a map subscript reads a key, Spark's element_at();
//  - a struct subscript with a field name accesses the field.
// Returns nullptr for any other call.
lp::ExprPtr resolveSubscript(
    const std::string& name,
    const std::vector<lp::ExprPtr>& args) {
  if (name != "subscript" || args.size() != 2) {
    return nullptr;
  }

  const auto& type = args[0]->type();
  if (type->isArray()) {
    return std::make_shared<lp::CallExpr>(type->childAt(0), "get", args);
  }
  if (type->isMap()) {
    return std::make_shared<lp::CallExpr>(type->childAt(1), "element_at", args);
  }
  if (!type->isRow() || !args[1]->isConstant() ||
      args[1]->type() != velox::VARCHAR()) {
    return nullptr;
  }

  const auto& rowType = type->asRow();
  const auto field = args[1]
                         ->as<lp::ConstantExpr>()
                         ->value()
                         ->value<velox::TypeKind::VARCHAR>();
  for (uint32_t i = 0; i < rowType.size(); ++i) {
    if (boost::iequals(rowType.nameOf(i), field)) {
      return std::make_shared<lp::SpecialFormExpr>(
          rowType.childAt(i),
          lp::SpecialForm::kDereference,
          args[0],
          std::make_shared<lp::ConstantExpr>(
              velox::VARCHAR(),
              std::make_shared<velox::Variant>(rowType.nameOf(i))));
    }
  }
  VELOX_USER_FAIL("Struct field not found: {}", field);
}

// Returns the Velox function for a Spark operator, or 'name' for a function,
// which Velox registers under its Spark name.
std::string toFunctionName(const std::string& name) {
  static const folly::F14FastMap<std::string, std::string> kOperators = {
      {"+", "add"},
      {"-", "subtract"},
      {"*", "multiply"},
      {"/", "divide"},
      {"%", "remainder"},
      {"==", "equalto"},
      {"<=>", "equalnullsafe"},
      {">", "greaterthan"},
      {"<", "lessthan"},
      {">=", "greaterthanorequal"},
      {"<=", "lessthanorequal"},
  };
  auto it = kOperators.find(name);
  return it == kOperators.end() ? name : it->second;
}

bool isGeneratorFunction(const std::string& functionName) {
  return functionName == "explode" || functionName == "posexplode" ||
      functionName == "explode_outer" || functionName == "posexplode_outer";
}

bool isOuterGenerator(const std::string& functionName) {
  return functionName == "explode_outer" || functionName == "posexplode_outer";
}

// Returns the name of the generator function 'expression' calls, directly or
// under an alias, or an empty string.
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

// Returns the Axiom write kind for a Spark save mode.
lp::WriteKind toWriteKind(spark::connect::WriteOperation::SaveMode mode) {
  switch (mode) {
    case spark::connect::WriteOperation::SAVE_MODE_APPEND:
      return lp::WriteKind::kInsert;
    case spark::connect::WriteOperation::SAVE_MODE_ERROR_IF_EXISTS:
      return lp::WriteKind::kCreate;
    case spark::connect::WriteOperation::SAVE_MODE_OVERWRITE:
      VELOX_NYI("SAVE_MODE_OVERWRITE is not yet supported for table writes");
    case spark::connect::WriteOperation::SAVE_MODE_IGNORE:
      VELOX_NYI("SAVE_MODE_IGNORE is not yet supported for table writes");
    default:
      VELOX_NYI(
          "Unknown save mode: {}",
          spark::connect::WriteOperation::SaveMode_Name(mode));
  }
}

// Returns the Axiom join type for a Spark join type.
lp::JoinType toJoinType(spark::connect::Join::JoinType joinType) {
  switch (joinType) {
    case spark::connect::Join::JOIN_TYPE_INNER:
      return lp::JoinType::kInner;

    case spark::connect::Join::JOIN_TYPE_LEFT_OUTER:
      return lp::JoinType::kLeft;

    case spark::connect::Join::JOIN_TYPE_RIGHT_OUTER:
      return lp::JoinType::kRight;

    case spark::connect::Join::JOIN_TYPE_FULL_OUTER:
      return lp::JoinType::kFull;

    default:
      VELOX_NYI(
          "Unable to convert join type: {}",
          spark::connect::Join::JoinType_Name(joinType));
  }
}

// Returns the Axiom set operation for a Spark one. 'isAll' keeps duplicates.
lp::SetOperation toSetOperation(
    spark::connect::SetOperation::SetOpType setOpType,
    bool isAll) {
  switch (setOpType) {
    case spark::connect::SetOperation::SET_OP_TYPE_UNION:
      return isAll ? lp::SetOperation::kUnionAll : lp::SetOperation::kUnion;

    case spark::connect::SetOperation::SET_OP_TYPE_INTERSECT:
      return isAll ? lp::SetOperation::kIntersectAll
                   : lp::SetOperation::kIntersect;

    case spark::connect::SetOperation::SET_OP_TYPE_EXCEPT:
      return isAll ? lp::SetOperation::kExceptAll : lp::SetOperation::kExcept;

    default:
      VELOX_NYI(
          "Unable to convert set operation type: {}",
          spark::connect::SetOperation::SetOpType_Name(setOpType));
  }
}

} // namespace

SparkToAxiom::SparkToAxiom(
    std::string catalog,
    std::string schema,
    MemoryPool* pool)
    : catalog_(std::move(catalog)),
      schema_(std::move(schema)),
      pool_(pool),
      builderContext_{
          catalog_,
          schema_,
          /*queryCtxPtr=*/nullptr,
          &resolveSubscript,
          std::make_shared<lp::ThrowingSqlExpressionsParser>(),
          &velox::functions::prestosql::typeCoercer()} {
  builderContext_.columnsById = true;
  builderContext_.connectorContext = std::make_shared<
      facebook::axiom::connector::ConnectorContext>(
      /*queryId=*/"collagen",
      /*user=*/"collagen",
      facebook::axiom::connector::ConnectorProperties{},
      facebook::axiom::connector::ConnectorContext::noopStatWriterProvider(),
      facebook::axiom::connector::ConnectorMetadataRegistry::global());
}

SparkToAxiom::PlanBuilder SparkToAxiom::newBuilder() const {
  return PlanBuilder(builderContext_, /*allowAmbiguousOutputNames=*/true);
}

SparkToAxiom::Columns SparkToAxiom::columnsOf(const PlanBuilder& builder) {
  const auto& outputType = builder.planNode()->outputType();
  const auto names = builder.outputNames(/*includeHiddenColumns=*/true);
  const auto* mapping = builder.outputMapping();

  Columns columns;
  columns.reserve(outputType->size());
  for (uint32_t i = 0; i < outputType->size(); ++i) {
    const auto& id = outputType->nameOf(i);
    if (mapping->isHidden(id)) {
      continue;
    }
    columns.push_back(
        {.name = names[i].value_or(""),
         .id = id,
         .type = outputType->childAt(i)});
  }
  return columns;
}

SparkToAxiom::ExprApi SparkToAxiom::toCol(const Column& column) {
  auto col = lp::Col(column.id);
  return column.name.empty() ? col : col.as(column.name);
}

void SparkToAxiom::finishRelation(const SparkPlanVisitorContext& context) {
  scope_ = columnsOf(*builder_);
  if (context.planId >= 0) {
    planIdOutputs_[context.planId].push_back(scope_);
  }
}

SparkToAxiom::PlanBuilder SparkToAxiom::visitInput(
    const spark::connect::Relation& relation,
    SparkPlanVisitorContext& context) {
  SparkPlanVisitor::visit(relation, context);
  VELOX_CHECK(builder_.has_value());
  auto builder = std::move(builder_.value());
  builder_.reset();
  return builder;
}

SparkToAxiom::ExprApi SparkToAxiom::toExpr(
    const spark::connect::Expression& expression,
    SparkPlanVisitorContext& context) {
  expr_.reset();
  visit(expression, context);
  VELOX_CHECK(expr_.has_value());
  auto expr = std::move(expr_.value());
  expr_.reset();
  return expr;
}

SparkToAxiom::LogicalPlanNodePtr SparkToAxiom::planNode() {
  if (plan_ == nullptr) {
    VELOX_CHECK(builder_.has_value(), "No relation was visited");
    plan_ = isWrite_ ? builder_->planNode() : builder_->build();
  }
  return plan_;
}

std::string SparkToAxiom::toString() {
  if (!builder_.has_value()) {
    return "<empty>";
  }
  return lp::PlanPrinter::toText(*planNode());
}

namespace {

// Splits a Spark table name, such as "t", "schema.t" or
// "`catalog`.`schema`.`t`", into its parts. A part in backticks may contain
// dots, and a doubled backtick in it is a literal backtick.
std::vector<std::string> splitTableName(std::string_view name) {
  std::vector<std::string> parts(1);
  bool quoted = false;
  for (size_t i = 0; i < name.size(); ++i) {
    const char c = name[i];
    if (c == '`' && quoted && i + 1 < name.size() && name[i + 1] == '`') {
      parts.back() += '`';
      ++i;
    } else if (c == '`') {
      quoted = !quoted;
    } else if (c == '.' && !quoted) {
      parts.emplace_back();
    } else {
      parts.back() += c;
    }
  }
  VELOX_USER_CHECK(
      !quoted, "Table name has an unterminated backtick: {}", name);
  for (const auto& part : parts) {
    VELOX_USER_CHECK(!part.empty(), "Table name has an empty part: {}", name);
  }
  VELOX_USER_CHECK_LE(
      parts.size(), 3, "Table name must have at most 3 parts: {}", name);
  return parts;
}

} // namespace

void SparkToAxiom::visit(
    const spark::connect::Read_NamedTable& namedTable,
    SparkPlanVisitorContext& context) {
  const auto& identifier = namedTable.unparsed_identifier();
  const auto parts = splitTableName(identifier);
  const auto& table = parts.back();
  const auto& schema = parts.size() > 1 ? parts[parts.size() - 2] : schema_;
  const auto& catalog = parts.size() > 2 ? parts[0] : catalog_;

  const auto metadata =
      facebook::axiom::connector::ConnectorMetadataRegistry::tryGet(catalog);
  VELOX_USER_CHECK_NOT_NULL(metadata, "Catalog not found: {}", catalog);

  builder_.emplace(newBuilder());
  builder_->tableScan(
      catalog,
      metadata->normalizeIdentifier(schema),
      metadata->normalizeIdentifier(table));
  finishRelation(context);
  aliasOutputs_[table].push_back(scope_);
}

void SparkToAxiom::visit(
    const spark::connect::LocalRelation& localRelation,
    SparkPlanVisitorContext& context) {
  auto type = velox::asRowType(
      SparkVeloxConverter::parseJsonType(localRelation.schema()));
  VELOX_USER_CHECK_NOT_NULL(type, "Top-level type must be a of type 'struct'");

  auto arrowBuffer = arrow::Buffer::Wrap(
      localRelation.data().data(), localRelation.data().size());
  auto vectors = ArrowIpc::deserialize(arrowBuffer, pool_);

  // An empty relation still carries its type.
  if (vectors.empty()) {
    vectors.push_back(velox::RowVector::createEmpty(type, pool_));
  }

  builder_.emplace(newBuilder());
  builder_->values(vectors);
  finishRelation(context);
}

void SparkToAxiom::visit(
    const spark::connect::Range& range,
    SparkPlanVisitorContext& context) {
  const int64_t start = range.has_start() ? range.start() : 0;
  const int64_t end = range.end();
  const int64_t step = range.step();
  VELOX_USER_CHECK_NE(step, 0, "Range step cannot be zero");

  int32_t numElements;
  if (step > 0) {
    numElements = (end > start) ? (end - start + step - 1) / step : 0;
  } else {
    numElements = (start > end) ? (start - end - step - 1) / (-step) : 0;
  }

  auto rangeVector = velox::BaseVector::create<velox::FlatVector<int64_t>>(
      velox::BIGINT(), numElements, pool_);
  int64_t value = start;
  for (int32_t i = 0; i < numElements; ++i) {
    rangeVector->set(i, value);
    value += step;
  }

  auto rowVector = std::make_shared<velox::RowVector>(
      pool_,
      velox::ROW("id", velox::BIGINT()),
      nullptr,
      numElements,
      std::vector<velox::VectorPtr>{rangeVector});

  builder_.emplace(newBuilder());
  builder_->values({rowVector});
  finishRelation(context);
}

void SparkToAxiom::visit(
    const spark::connect::Join& join,
    SparkPlanVisitorContext& context) {
  VELOX_NYI_CHECK(
      join.using_columns().empty(),
      "Join with USING columns is not supported yet");

  auto right = visitInput(join.right(), context);
  SparkPlanVisitor::visit(join.left(), context);

  // The condition reads columns of both inputs.
  const auto rightColumns = columnsOf(right);
  scope_.insert(scope_.end(), rightColumns.begin(), rightColumns.end());

  std::optional<ExprApi> condition;
  if (join.has_join_condition()) {
    condition = toExpr(join.join_condition(), context);
  }

  builder_->join(right, condition, toJoinType(join.join_type()));
  finishRelation(context);
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

  SparkPlanVisitor::visit(project.input(), context);

  std::vector<ExprApi> projections;
  projections.reserve(project.expressions().size());
  for (const auto& expression : project.expressions()) {
    projections.push_back(toExpr(expression, context));
  }

  builder_->project(projections);
  finishRelation(context);
}

void SparkToAxiom::visit(
    const spark::connect::Filter& filter,
    SparkPlanVisitorContext& context) {
  SparkPlanVisitor::visit(filter.input(), context);
  builder_->filter(toExpr(filter.condition(), context));
  finishRelation(context);
}

void SparkToAxiom::visit(
    const spark::connect::Aggregate& aggregate,
    SparkPlanVisitorContext& context) {
  SparkPlanVisitor::visit(aggregate.input(), context);

  std::vector<ExprApi> groupingKeys;
  groupingKeys.reserve(aggregate.grouping_expressions().size());
  for (const auto& expression : aggregate.grouping_expressions()) {
    groupingKeys.push_back(toExpr(expression, context));
  }

  std::vector<ExprApi> aggregates;
  aggregates.reserve(aggregate.aggregate_expressions().size());
  for (const auto& expression : aggregate.aggregate_expressions()) {
    aggregates.push_back(toExpr(expression, context));
  }

  builder_->aggregate(groupingKeys, aggregates);
  finishRelation(context);
}

void SparkToAxiom::visit(
    const spark::connect::Limit& limit,
    SparkPlanVisitorContext& context) {
  SparkPlanVisitor::visit(limit.input(), context);
  builder_->limit(limit.limit());
  finishRelation(context);
}

void SparkToAxiom::visit(
    const spark::connect::Offset& offset,
    SparkPlanVisitorContext& context) {
  SparkPlanVisitor::visit(offset.input(), context);
  builder_->offset(offset.offset());
  finishRelation(context);
}

void SparkToAxiom::visit(
    const spark::connect::ToDF& toDF,
    SparkPlanVisitorContext& context) {
  SparkPlanVisitor::visit(toDF.input(), context);

  VELOX_USER_CHECK_EQ(
      toDF.column_names().size(), scope_.size(), "toDF must name every column");

  std::vector<ExprApi> projections;
  projections.reserve(scope_.size());
  for (size_t i = 0; i < scope_.size(); ++i) {
    projections.push_back(lp::Col(scope_[i].id).as(toDF.column_names(i)));
  }

  builder_->project(projections);
  finishRelation(context);
}

void SparkToAxiom::visit(
    const spark::connect::ShowString& showString,
    SparkPlanVisitorContext& context) {
  SparkPlanVisitor::visit(showString.input(), context);

  std::vector<ExprApi> projections;
  projections.reserve(scope_.size());
  for (const auto& column : scope_) {
    auto value = lp::Col(column.id);
    if (!column.type->isVarbinary()) {
      value = lp::Cast(velox::VARCHAR(), value);
    }
    projections.push_back(value.as(column.name));
  }

  builder_->project(projections);
  builder_->limit(std::max<int64_t>(showString.num_rows(), 0) + 1);
  finishRelation(context);
}

void SparkToAxiom::visit(
    const spark::connect::SubqueryAlias& subqueryAlias,
    SparkPlanVisitorContext& context) {
  SparkPlanVisitor::visit(subqueryAlias.input(), context);
  finishRelation(context);
  aliasOutputs_[subqueryAlias.alias()].push_back(scope_);
}

void SparkToAxiom::visit(
    const spark::connect::WriteOperation& writeOperation,
    SparkPlanVisitorContext& context) {
  SparkPlanVisitor::visit(writeOperation.input(), context);

  VELOX_NYI_CHECK(
      writeOperation.save_type_case() == spark::connect::WriteOperation::kTable,
      "Unsupported write operation type: {}",
      fmt::underlying(writeOperation.save_type_case()));

  const auto& rawTableName = writeOperation.table().table_name();
  VELOX_NYI_CHECK(
      rawTableName.find('.') == std::string::npos,
      "Schema-qualified table names are not supported: {}",
      rawTableName);

  const auto writeKind = toWriteKind(writeOperation.mode());

  const auto metadata =
      facebook::axiom::connector::ConnectorMetadataRegistry::global().get(
          catalog_);

  std::vector<std::string> columnNames;
  std::vector<ExprApi> columnExprs;
  columnNames.reserve(scope_.size());
  columnExprs.reserve(scope_.size());
  for (const auto& column : scope_) {
    columnNames.push_back(metadata->normalizeIdentifier(column.name));
    columnExprs.push_back(toCol(column));
  }

  folly::F14FastMap<std::string, std::string> options;
  for (const auto& [key, value] : writeOperation.options()) {
    options.emplace(key, value);
  }

  // TODO: Handle partitioning, bucketing, and sorting.
  builder_->tableWrite(
      catalog_,
      metadata->normalizeIdentifier(schema_),
      metadata->normalizeIdentifier(rawTableName),
      writeKind,
      std::move(columnNames),
      columnExprs,
      std::move(options));
  isWrite_ = true;
}

void SparkToAxiom::visit(
    const spark::connect::Sort& sort,
    SparkPlanVisitorContext& context) {
  SparkPlanVisitor::visit(sort.input(), context);

  std::vector<lp::SortKey> sortKeys;
  sortKeys.reserve(sort.order().size());
  for (const auto& sortOrder : sort.order()) {
    const bool ascending = sortOrder.direction() !=
        spark::connect::Expression::SortOrder::SORT_DIRECTION_DESCENDING;
    const bool nullsFirst = sortOrder.null_ordering() ==
        spark::connect::Expression::SortOrder::SORT_NULLS_FIRST;
    sortKeys.emplace_back(
        toExpr(sortOrder.child(), context), ascending, nullsFirst);
  }

  builder_->sort(sortKeys);
  finishRelation(context);
}

void SparkToAxiom::visit(
    const spark::connect::WithColumnsRenamed& withColumnsRenamed,
    SparkPlanVisitorContext& context) {
  SparkPlanVisitor::visit(withColumnsRenamed.input(), context);

  const auto& renames = withColumnsRenamed.rename_columns_map();

  std::vector<ExprApi> projections;
  projections.reserve(scope_.size());
  for (const auto& column : scope_) {
    auto it = renames.find(column.name);
    projections.push_back(
        it == renames.end() ? toCol(column)
                            : lp::Col(column.id).as(it->second));
  }

  builder_->project(projections);
  finishRelation(context);
}

void SparkToAxiom::visit(
    const spark::connect::WithColumns& withColumns,
    SparkPlanVisitorContext& context) {
  SparkPlanVisitor::visit(withColumns.input(), context);

  // A column named like an input column replaces it in place. The others
  // follow the input columns.
  std::vector<std::pair<std::string, ExprApi>> newColumns;
  for (const auto& alias : withColumns.aliases()) {
    VELOX_USER_CHECK_EQ(
        alias.name_size(),
        1,
        "Expected exactly one name in WithColumns alias, got {}",
        alias.name_size());
    const auto& name = alias.name(0);
    newColumns.emplace_back(name, toExpr(alias.expr(), context).as(name));
  }

  std::vector<ExprApi> projections;
  projections.reserve(scope_.size() + newColumns.size());
  for (const auto& column : scope_) {
    auto it = std::find_if(
        newColumns.begin(), newColumns.end(), [&](const auto& newColumn) {
          return newColumn.first == column.name;
        });
    if (it == newColumns.end()) {
      projections.push_back(toCol(column));
    } else {
      projections.push_back(std::move(it->second));
      newColumns.erase(it);
    }
  }
  for (auto& [name, expr] : newColumns) {
    projections.push_back(std::move(expr));
  }

  builder_->project(projections);
  finishRelation(context);
}

void SparkToAxiom::visit(
    const spark::connect::SetOperation& setOperation,
    SparkPlanVisitorContext& context) {
  auto left = visitInput(setOperation.left_input(), context);
  const auto leftColumns = columnsOf(left);
  auto right = visitInput(setOperation.right_input(), context);
  const auto rightColumns = columnsOf(right);

  // unionByName matches the right input's columns to the left input's by name.
  // With allowMissingColumns, a column one input lacks reads as NULL there, and
  // the right input's extra columns follow the left input's.
  if (setOperation.has_by_name() && setOperation.by_name()) {
    VELOX_USER_CHECK(
        setOperation.set_op_type() ==
            spark::connect::SetOperation::SET_OP_TYPE_UNION,
        "Only UNION can match columns by name: {}",
        spark::connect::SetOperation::SetOpType_Name(
            setOperation.set_op_type()));
    const bool allowMissingColumns = setOperation.has_allow_missing_columns() &&
        setOperation.allow_missing_columns();

    const auto findColumn = [](const Columns& columns,
                               const std::string& name) -> const Column* {
      auto it =
          std::find_if(columns.begin(), columns.end(), [&](const auto& c) {
            return c.name == name;
          });
      return it == columns.end() ? nullptr : &*it;
    };

    // The output columns: the left input's, then the right input's extra
    // ones.
    Columns outputColumns;
    for (const auto& column : leftColumns) {
      VELOX_USER_CHECK(
          allowMissingColumns || findColumn(rightColumns, column.name),
          "Column of the left input is missing from the right input: {}",
          column.name);
      outputColumns.push_back(column);
    }
    for (const auto& column : rightColumns) {
      if (!findColumn(leftColumns, column.name)) {
        VELOX_USER_CHECK(
            allowMissingColumns,
            "Column of the right input is missing from the left input: {}",
            column.name);
        outputColumns.push_back(column);
      }
    }

    // Projects 'columns' onto the output columns, by name. A column 'columns'
    // lacks reads as NULL.
    const auto projectByName = [&](const Columns& columns) {
      std::vector<ExprApi> projections;
      projections.reserve(outputColumns.size());
      for (const auto& output : outputColumns) {
        const auto* column = findColumn(columns, output.name);
        projections.push_back(
            column == nullptr ? lp::NullLit(output.type).as(output.name)
                              : lp::Col(column->id).as(output.name));
      }
      return projections;
    };

    if (outputColumns.size() > leftColumns.size()) {
      left.project(projectByName(leftColumns));
    }
    right.project(projectByName(rightColumns));
  }

  const bool isAll = setOperation.has_is_all() && setOperation.is_all();
  builder_.emplace(std::move(left));
  builder_->setOperation(
      toSetOperation(setOperation.set_op_type(), isAll), right);
  finishRelation(context);
}

void SparkToAxiom::visitProjectWithGenerator(
    const spark::connect::Project& project,
    SparkPlanVisitorContext& context) {
  SparkPlanVisitor::visit(project.input(), context);

  const spark::connect::Expression* generatorExpr = nullptr;
  std::string generatorName;
  int numGenerators = 0;
  std::vector<const spark::connect::Expression*> otherExprs;
  for (const auto& expression : project.expressions()) {
    auto name = findGeneratorFunction(expression);
    if (name.empty()) {
      otherExprs.push_back(&expression);
      continue;
    }
    ++numGenerators;
    if (generatorExpr == nullptr) {
      generatorExpr = &expression;
      generatorName = std::move(name);
    }
  }

  VELOX_USER_CHECK_EQ(
      numGenerators,
      1,
      "Only one generator allowed per select clause. "
      "Use separate select() calls for multiple explode operations.");

  VELOX_NYI_CHECK(
      !isOuterGenerator(generatorName),
      "{} is not supported yet. UnnestNode uses inner semantics "
      "(rows with empty arrays/maps are dropped). Use explode/posexplode instead.",
      generatorName);

  std::vector<std::string> aliases;
  if (generatorExpr->has_alias()) {
    aliases.assign(
        generatorExpr->alias().name().begin(),
        generatorExpr->alias().name().end());
  }
  const auto& generator = generatorExpr->has_alias()
      ? generatorExpr->alias().expr().unresolved_function()
      : generatorExpr->unresolved_function();
  VELOX_USER_CHECK_GE(
      generator.arguments().size(),
      1,
      "{} requires at least 1 argument",
      generatorName);

  const bool isPosExplode = generatorName == "posexplode";
  const auto numInputColumns = builder_->numOutput();
  auto unnestExpr = toExpr(generator.arguments(0), context);
  builder_->unnest(
      {unnestExpr},
      isPosExplode ? std::make_optional(lp::Ordinality()) : std::nullopt);

  // Unnest emits the input columns, then the unnested columns, then the
  // ordinality column, if any.
  const auto outputType = builder_->planNode()->outputType();
  const auto numUnnested =
      outputType->size() - numInputColumns - (isPosExplode ? 1 : 0);

  // For posexplode, the first alias names the position.
  const size_t aliasOffset = isPosExplode ? 1 : 0;
  const auto aliasAt = [&](size_t index, std::string_view fallback) {
    return index < aliases.size() ? aliases[index] : std::string{fallback};
  };

  std::vector<std::string> unnestedNames;
  if (numUnnested == 1) {
    unnestedNames.push_back(aliasAt(aliasOffset, "col"));
  } else {
    unnestedNames.push_back(aliasAt(aliasOffset, "key"));
    unnestedNames.push_back(aliasAt(aliasOffset + 1, "value"));
  }

  scope_ = columnsOf(*builder_);

  std::vector<ExprApi> projections;
  projections.reserve(otherExprs.size());
  for (const auto* expression : otherExprs) {
    projections.push_back(toExpr(*expression, context));
  }
  if (isPosExplode) {
    // Spark's position is 0-based; the ordinality is 1-based.
    projections.push_back(
        lp::Call(
            "subtract",
            lp::Col(outputType->nameOf(outputType->size() - 1)),
            lp::Lit(int64_t{1}))
            .as(aliasAt(0, "pos")));
  }
  for (size_t i = 0; i < numUnnested; ++i) {
    projections.push_back(
        lp::Col(outputType->nameOf(numInputColumns + i)).as(unnestedNames[i]));
  }

  builder_->project(projections);
  finishRelation(context);
}

void SparkToAxiom::visit(
    const spark::connect::Expression::Literal& literal,
    SparkPlanVisitorContext& /*context*/) {
  expr_ = lp::Lit(
      *SparkVeloxConverter::toVariant(literal),
      SparkVeloxConverter::toVeloxType(literal));
}

namespace {

struct Identifier {
  std::string name;
  std::string parent;
};

// Splits 'column', 'parent.column' or 'catalog.parent.column'.
Identifier parseIdentifier(const std::string& input) {
  std::vector<std::string> parts;
  folly::split('.', input, parts, /*ignoreEmpty=*/true);
  if (parts.size() == 1) {
    return {.name = parts[0]};
  }
  if (parts.size() == 2) {
    return {.name = parts[1], .parent = parts[0]};
  }
  if (parts.size() == 3) {
    return {.name = parts[2], .parent = parts[1]};
  }
  VELOX_USER_FAIL("Unable to parse identifier: '{}'", input);
}

} // namespace

SparkToAxiom::ExprApi SparkToAxiom::bindColumn(std::string_view name) const {
  const Column* match = nullptr;
  for (const auto& column : scope_) {
    if (column.name == name) {
      VELOX_USER_CHECK_NULL(match, "Reference is ambiguous: {}", name);
      match = &column;
    }
  }
  VELOX_USER_CHECK_NOT_NULL(match, "Unable to resolve reference: {}", name);
  return toCol(*match);
}

SparkToAxiom::ExprApi SparkToAxiom::bindColumn(
    std::string_view name,
    const std::vector<Columns>& outputs,
    std::string_view reference) const {
  bool found = false;
  const Column* match = nullptr;
  for (const auto& columns : outputs) {
    for (const auto& column : columns) {
      if (column.name != name) {
        continue;
      }
      found = true;
      auto it = std::find_if(scope_.begin(), scope_.end(), [&](const auto& c) {
        return c.id == column.id;
      });
      if (it == scope_.end() || (match != nullptr && match->id == column.id)) {
        continue;
      }
      VELOX_USER_CHECK_NULL(match, "Reference is ambiguous: {}", reference);
      match = &*it;
    }
  }
  VELOX_USER_CHECK(found, "Unable to resolve reference: {}", reference);
  VELOX_USER_CHECK_NOT_NULL(
      match, "Column is not available in the current input: {}", reference);
  return toCol(*match);
}

void SparkToAxiom::visit(
    const spark::connect::Expression::UnresolvedAttribute& unresolvedAttribute,
    SparkPlanVisitorContext& /*context*/) {
  const auto& text = unresolvedAttribute.unparsed_identifier();
  const auto identifier = parseIdentifier(text);

  if (unresolvedAttribute.has_plan_id()) {
    const auto planId = unresolvedAttribute.plan_id();
    auto it = planIdOutputs_.find(planId);
    VELOX_USER_CHECK(
        it != planIdOutputs_.end(),
        "Unable to find Spark Connect plan id: {}",
        planId);
    expr_ = bindColumn(identifier.name, it->second, text);
  } else if (!identifier.parent.empty()) {
    auto it = aliasOutputs_.find(identifier.parent);
    VELOX_USER_CHECK(
        it != aliasOutputs_.end(),
        "Unable to find Spark Connect reference: {}",
        identifier.parent);
    expr_ = bindColumn(identifier.name, it->second, text);
  } else {
    expr_ = bindColumn(identifier.name);
  }
}

void SparkToAxiom::visit(
    const spark::connect::Expression::UnresolvedFunction& unresolvedFunction,
    SparkPlanVisitorContext& context) {
  const auto& functionName = unresolvedFunction.function_name();

  std::vector<ExprApi> args;
  args.reserve(unresolvedFunction.arguments().size());
  for (const auto& argument : unresolvedFunction.arguments()) {
    args.push_back(toExpr(argument, context));
  }

  // Spark's analyzer passes size() its legacy flag: a NULL has size -1.
  if (functionName == "size" && args.size() == 1) {
    args.push_back(lp::Lit(true));
  }

  // when(c1, v1, c2, v2, ..., else) with more than one condition is a switch.
  if (functionName == "if" || functionName == "when") {
    VELOX_USER_CHECK_GE(
        args.size(),
        2,
        "if/when requires at least 2 arguments (condition, then_value)");
    expr_ = lp::Call(args.size() > 3 ? "switch" : "if", args);
    return;
  }

  auto call = lp::Call(toFunctionName(functionName), args);
  expr_ = unresolvedFunction.is_distinct() ? call.distinct() : call;
}

void SparkToAxiom::visit(
    const spark::connect::Expression::Alias& alias,
    SparkPlanVisitorContext& context) {
  VELOX_USER_CHECK_EQ(
      alias.name().size(),
      1,
      "Expected exactly one alias name, got {}",
      alias.name().size());
  expr_ = toExpr(alias.expr(), context).as(*alias.name().begin());
}

void SparkToAxiom::visit(
    const spark::connect::Expression::UnresolvedStar& /*unresolvedStar*/,
    SparkPlanVisitorContext& /*context*/) {
  // '*' appears as the argument of count(*), which counts every row.
  // TODO: Expand '*' to the input columns elsewhere.
  expr_ = lp::Lit(true);
}

void SparkToAxiom::visit(
    const spark::connect::Expression::UnresolvedExtractValue&
        unresolvedExtractValue,
    SparkPlanVisitorContext& context) {
  // Resolved by 'resolveSubscript' once the type of the child is known.
  auto child = toExpr(unresolvedExtractValue.child(), context);
  auto extraction = toExpr(unresolvedExtractValue.extraction(), context);
  expr_ = lp::Call("subscript", child, extraction);
}

void SparkToAxiom::visit(
    const spark::connect::Expression::Cast& cast,
    SparkPlanVisitorContext& context) {
  auto input = toExpr(cast.expr(), context);

  velox::TypePtr targetType;
  if (cast.has_type()) {
    targetType = SparkVeloxConverter::toVeloxType(cast.type());
  } else if (cast.has_type_str()) {
    static const folly::F14FastMap<std::string, velox::TypePtr> kTypes = {
        {"long", velox::BIGINT()},         {"bigint", velox::BIGINT()},
        {"int", velox::INTEGER()},         {"integer", velox::INTEGER()},
        {"short", velox::SMALLINT()},      {"smallint", velox::SMALLINT()},
        {"byte", velox::TINYINT()},        {"tinyint", velox::TINYINT()},
        {"double", velox::DOUBLE()},       {"float", velox::REAL()},
        {"real", velox::REAL()},           {"string", velox::VARCHAR()},
        {"varchar", velox::VARCHAR()},     {"boolean", velox::BOOLEAN()},
        {"bool", velox::BOOLEAN()},        {"binary", velox::VARBINARY()},
        {"varbinary", velox::VARBINARY()}, {"date", velox::DATE()},
        {"timestamp", velox::TIMESTAMP()},
    };
    auto it = kTypes.find(cast.type_str());
    VELOX_NYI_CHECK(
        it != kTypes.end(),
        "Cast type string not supported: '{}'",
        cast.type_str());
    targetType = it->second;
  } else {
    VELOX_FAIL("Cast expression must have either type or type_str set");
  }

  expr_ = lp::Cast(targetType, input);
}

namespace {

// Joins the name parts of a lambda variable with dots.
std::string lambdaVariableName(
    const spark::connect::Expression::UnresolvedNamedLambdaVariable& variable) {
  return folly::join(".", variable.name_parts());
}

} // namespace

void SparkToAxiom::visit(
    const spark::connect::Expression::LambdaFunction& lambdaFunction,
    SparkPlanVisitorContext& context) {
  std::vector<std::string> names;
  names.reserve(lambdaFunction.arguments().size());
  for (const auto& argument : lambdaFunction.arguments()) {
    names.push_back(lambdaVariableName(argument));
  }

  lambdaVariables_.insert(lambdaVariables_.end(), names.begin(), names.end());
  auto body = toExpr(lambdaFunction.function(), context);
  lambdaVariables_.resize(lambdaVariables_.size() - names.size());

  expr_ = lp::Lambda(std::move(names), body);
}

void SparkToAxiom::visit(
    const spark::connect::Expression::UnresolvedNamedLambdaVariable&
        unresolvedNamedLambdaVariable,
    SparkPlanVisitorContext& /*context*/) {
  auto name = lambdaVariableName(unresolvedNamedLambdaVariable);
  VELOX_USER_CHECK(
      std::find(lambdaVariables_.begin(), lambdaVariables_.end(), name) !=
          lambdaVariables_.end(),
      "Lambda variable not found in current context: {}",
      name);
  expr_ = lp::Col(name);
}

} // namespace axiom::spark_connect
