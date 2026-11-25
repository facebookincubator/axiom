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

#pragma once

#include <glog/logging.h>
#include <string>

#include "axiom/spark_connect/third-party/protos/base.pb.h" // @manual=fbcode//axiom/spark_connect/third-party/protos:spark_connect_proto-cpp
#include "axiom/spark_connect/third-party/protos/commands.pb.h" // @manual=fbcode//axiom/spark_connect/third-party/protos:spark_connect_proto-cpp
#include "axiom/spark_connect/third-party/protos/expressions.pb.h" // @manual=fbcode//axiom/spark_connect/third-party/protos:spark_connect_proto-cpp
#include "axiom/spark_connect/third-party/protos/relations.pb.h" // @manual=fbcode//axiom/spark_connect/third-party/protos:spark_connect_proto-cpp
#include "velox/common/base/Exceptions.h"
#include "velox/type/Type.h"

namespace axiom::spark_connect {

class SparkPlanVisitorContext {
 public:
  virtual ~SparkPlanVisitorContext() = default;

  /// Plan ID of the relation being visited, or -1 if it has none.
  int64_t planId{-1};
};

class SparkPlanVisitor {
 public:
  virtual ~SparkPlanVisitor() = default;

  // 'Relation' visitor methods:

  virtual void visit(
      const spark::connect::Read_NamedTable& namedTable,
      SparkPlanVisitorContext& context) {}

  virtual void visit(
      const spark::connect::Read_DataSource& dataSource,
      SparkPlanVisitorContext& context) {}

  virtual void visit(
      const spark::connect::Read& read,
      SparkPlanVisitorContext& context) {
    switch (read.read_type_case()) {
      case spark::connect::Read::kNamedTable:
        visit(read.named_table(), context);
        break;

      case spark::connect::Read::kDataSource:
        visit(read.data_source(), context);
        break;

      case spark::connect::Read::READ_TYPE_NOT_SET:
        throw std::runtime_error("Read type not set on read relation.");
    }
  }

  virtual void visit(
      const spark::connect::LocalRelation& localRelation,
      SparkPlanVisitorContext& context) {}

  virtual void visit(
      const spark::connect::Project& project,
      SparkPlanVisitorContext& context) {
    visit(project.input(), context);
  }

  virtual void visit(
      const spark::connect::Filter& filter,
      SparkPlanVisitorContext& context) {
    visit(filter.input(), context);
  }

  virtual void visit(
      const spark::connect::Join& join,
      SparkPlanVisitorContext& context) {
    visit(join.join_condition(), context);
    visit(join.left(), context);
    visit(join.right(), context);
  }

  virtual void visit(
      const spark::connect::Limit& limit,
      SparkPlanVisitorContext& context) {
    visit(limit.input(), context);
  }

  virtual void visit(
      const spark::connect::Offset& offset,
      SparkPlanVisitorContext& context) {
    visit(offset.input(), context);
  }

  virtual void visit(
      const spark::connect::SQL& sql,
      SparkPlanVisitorContext& context) {}

  virtual void visit(
      const spark::connect::SubqueryAlias& subqueryAlias,
      SparkPlanVisitorContext& context) {
    visit(subqueryAlias.input(), context);
  }

  virtual void visit(
      const spark::connect::ToDF& toDF,
      SparkPlanVisitorContext& context) {
    visit(toDF.input(), context);
  }

  virtual void visit(
      const spark::connect::ShowString& showString,
      SparkPlanVisitorContext& context) {
    visit(showString.input(), context);
  }

  virtual void visit(
      const spark::connect::WithColumnsRenamed& withColumnsRenamed,
      SparkPlanVisitorContext& context) {
    visit(withColumnsRenamed.input(), context);
  }

  virtual void visit(
      const spark::connect::WithColumns& withColumns,
      SparkPlanVisitorContext& context) {
    visit(withColumns.input(), context);
    for (const auto& alias : withColumns.aliases()) {
      visit(alias, context);
    }
  }

  virtual void visit(
      const spark::connect::Aggregate& aggregate,
      SparkPlanVisitorContext& context) {
    // Visit the input relation first.
    visit(aggregate.input(), context);

    // Visit all grouping expressions.
    for (const auto& groupingExpr : aggregate.grouping_expressions()) {
      visit(groupingExpr, context);
    }

    // Visit all aggregate expressions.
    for (const auto& aggExpr : aggregate.aggregate_expressions()) {
      visit(aggExpr, context);
    }

    // Visit pivot if present.
    if (aggregate.has_pivot()) {
      const auto& pivot = aggregate.pivot();

      // Visit the pivot column expression.
      visit(pivot.col(), context);

      // Visit all pivot values
      for (const auto& value : pivot.values()) {
        visit(value, context);
      }
    }
  }

  virtual void visit(
      const spark::connect::SetOperation& setOperation,
      SparkPlanVisitorContext& context) {
    visit(setOperation.left_input(), context);
    visit(setOperation.right_input(), context);
  }

  virtual void visit(
      const spark::connect::Sort& sort,
      SparkPlanVisitorContext& context) {
    visit(sort.input(), context);
    for (const auto& sortOrder : sort.order()) {
      visit(sortOrder.child(), context);
    }
  }

  virtual void visit(
      const spark::connect::Range& range,
      SparkPlanVisitorContext& context) {}

  // RAII abstraction to ensure the right planIds are attached to the context
  // while traversing the plan tree.
  struct RelationScope {
    RelationScope(SparkPlanVisitorContext& context, int64_t planId)
        : context_(context), parentPlanId_(context.planId) {
      context_.planId = planId;
    }

    ~RelationScope() {
      context_.planId = parentPlanId_;
    }

   private:
    SparkPlanVisitorContext& context_;
    const int64_t parentPlanId_;
  };

  virtual void visit(
      const spark::connect::Relation& relation,
      SparkPlanVisitorContext& context) {
    RelationScope scope{
        context,
        relation.common().has_plan_id() ? relation.common().plan_id() : -1};

    switch (relation.rel_type_case()) {
      case spark::connect::Relation::kRead:
        visit(relation.read(), context);
        break;

      case spark::connect::Relation::kProject:
        visit(relation.project(), context);
        break;

      case spark::connect::Relation::kFilter:
        visit(relation.filter(), context);
        break;

      case spark::connect::Relation::kJoin:
        visit(relation.join(), context);
        break;

      case spark::connect::Relation::kLimit:
        visit(relation.limit(), context);
        break;

      case spark::connect::Relation::kOffset:
        visit(relation.offset(), context);
        break;

      case spark::connect::Relation::kLocalRelation:
        visit(relation.local_relation(), context);
        break;

      case spark::connect::Relation::kSql:
        visit(relation.sql(), context);
        break;

      case spark::connect::Relation::kSubqueryAlias:
        visit(relation.subquery_alias(), context);
        break;

      case spark::connect::Relation::kToDf:
        visit(relation.to_df(), context);
        break;

      case spark::connect::Relation::kShowString:
        visit(relation.show_string(), context);
        break;

      case spark::connect::Relation::kAggregate:
        visit(relation.aggregate(), context);
        break;

      case spark::connect::Relation::kSort:
        visit(relation.sort(), context);
        break;

      case spark::connect::Relation::kSetOp:
        visit(relation.set_op(), context);
        break;

      case spark::connect::Relation::kWithColumnsRenamed:
        visit(relation.with_columns_renamed(), context);
        break;

      case spark::connect::Relation::kWithColumns:
        visit(relation.with_columns(), context);
        break;

      case spark::connect::Relation::kRange:
        visit(relation.range(), context);
        break;

      default:
        VELOX_NYI(
            "Relation type not supported yet: {}",
            fmt::underlying(relation.rel_type_case()));
    }
  }

  // 'Plan' visitor methods:

  virtual void visit(
      const spark::connect::Plan& plan,
      SparkPlanVisitorContext& context) {
    switch (plan.op_type_case()) {
      case spark::connect::Plan::kRoot:
        visit(plan.root(), context);
        break;

      case spark::connect::Plan::kCommand:
        visit(plan.command(), context);
        break;

      default:
        VELOX_NYI(
            "Plan type not implemented: {}",
            fmt::underlying(plan.op_type_case()));
    }
  }

  // 'Command' visitor methods:

  virtual void visit(
      const spark::connect::Command& command,
      SparkPlanVisitorContext& context) {
    switch (command.command_type_case()) {
      case spark::connect::Command::kWriteOperation:
        visit(command.write_operation(), context);
        break;

      default:
        VELOX_NYI(
            "Command type not implemented: {}",
            fmt::underlying(command.command_type_case()));
    }
  }

  virtual void visit(
      const spark::connect::WriteOperation& writeOperation,
      SparkPlanVisitorContext& context) {
    visit(writeOperation.input(), context);
  }

  // 'Expression' visitor methods:

  virtual void visit(
      const spark::connect::Expression& expression,
      SparkPlanVisitorContext& context) {
    switch (expression.expr_type_case()) {
      case spark::connect::Expression::kLiteral:
        visit(expression.literal(), context);
        break;

      case spark::connect::Expression::kUnresolvedAttribute:
        visit(expression.unresolved_attribute(), context);
        break;

      case spark::connect::Expression::kUnresolvedFunction:
        visit(expression.unresolved_function(), context);
        break;

      case spark::connect::Expression::kAlias:
        visit(expression.alias(), context);
        break;

      case spark::connect::Expression::kUnresolvedStar:
        visit(expression.unresolved_star(), context);
        break;

      case spark::connect::Expression::kUnresolvedExtractValue:
        visit(expression.unresolved_extract_value(), context);
        break;

      case spark::connect::Expression::kSortOrder:
        visit(expression.sort_order(), context);
        break;

      case spark::connect::Expression::kLambdaFunction:
        visit(expression.lambda_function(), context);
        break;

      case spark::connect::Expression::kUnresolvedNamedLambdaVariable:
        visit(expression.unresolved_named_lambda_variable(), context);
        break;

      case spark::connect::Expression::kCast:
        visit(expression.cast(), context);
        break;

      case spark::connect::Expression::EXPR_TYPE_NOT_SET:
        VELOX_USER_FAIL(
            "Expression type not set for plan node '{}'.", context.planId);

      default:
        VELOX_NYI(
            "Do not know how to handle expression type '{}'.",
            fmt::underlying(expression.expr_type_case()));
    }
  }

  virtual void visit(
      const spark::connect::Expression::Literal& literal,
      SparkPlanVisitorContext& context) {}

  virtual void visit(
      const spark::connect::Expression::UnresolvedAttribute&
          unresolvedAttribute,
      SparkPlanVisitorContext& context) {}

  virtual void visit(
      const spark::connect::Expression::UnresolvedFunction& unresolvedFunction,
      SparkPlanVisitorContext& context) {
    for (const auto& argument : unresolvedFunction.arguments()) {
      visit(argument, context);
    }
  }

  virtual void visit(
      const spark::connect::Expression::Alias& alias,
      SparkPlanVisitorContext& context) {
    visit(alias.expr(), context);
  }

  virtual void visit(
      const spark::connect::Expression::UnresolvedStar& /*unresolvedStar*/,
      SparkPlanVisitorContext& /*context*/) {}

  virtual void visit(
      const spark::connect::Expression::UnresolvedExtractValue&
          unresolvedExtractValue,
      SparkPlanVisitorContext& context) {
    visit(unresolvedExtractValue.child(), context);
    visit(unresolvedExtractValue.extraction(), context);
  }

  virtual void visit(
      const spark::connect::Expression::SortOrder& sortOrder,
      SparkPlanVisitorContext& context) {
    visit(sortOrder.child(), context);
  }

  virtual void visit(
      const spark::connect::Expression::LambdaFunction& lambdaFunction,
      SparkPlanVisitorContext& context) {
    // Visit the lambda body (function expression).
    visit(lambdaFunction.function(), context);
  }

  virtual void visit(
      const spark::connect::Expression::UnresolvedNamedLambdaVariable&
      /*unresolvedNamedLambdaVariable*/,
      SparkPlanVisitorContext& /*context*/) {}

  virtual void visit(
      const spark::connect::Expression::Cast& cast,
      SparkPlanVisitorContext& context) {
    // Visit the expression being cast
    visit(cast.expr(), context);
  }
};

} // namespace axiom::spark_connect
