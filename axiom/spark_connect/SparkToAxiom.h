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

#include <span>

#include <folly/container/F14Map.h>
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/spark_connect/SparkPlanVisitor.h"
#include "axiom/spark_connect/third-party/protos/relations.grpc.pb.h" // @manual=fbcode//axiom/spark_connect/third-party/protos:spark_connect_proto-cpp

namespace axiom::spark_connect {

/// Converts a Spark Connect plan into an Axiom logical plan.
///
/// Builds the plan with a PlanBuilder that resolves columns by ID, and binds
/// Spark column references to column IDs itself:
///  - col("x") names a column of the current input.
///  - df["x"] carries the plan ID of 'df'. It names the column 'df' output as
///    'x', which must still be in the current input.
///  - col("t.x") names the column 't' output as 'x', where 't' is a
///    DataFrame alias or a table name.
///
/// Usage:
///
///   SparkToAxiom converter(catalog, schema, pool);
///   SparkPlanVisitorContext context;
///   converter.visit(plan, context);
///   auto logicalPlan = converter.planNode();
class SparkToAxiom : public SparkPlanVisitor {
 public:
  using LogicalPlanNodePtr =
      ::facebook::axiom::logical_plan::LogicalPlanNodePtr;
  using MemoryPool = ::facebook::velox::memory::MemoryPool;

  /// @param catalog Connector to read and write tables in.
  /// @param schema Schema to read and write tables in.
  /// @param pool Memory pool for local relation and range data.
  SparkToAxiom(std::string catalog, std::string schema, MemoryPool* pool);

  void visit(
      const spark::connect::Read_NamedTable& namedTable,
      SparkPlanVisitorContext& context) override;

  void visit(
      const spark::connect::LocalRelation& localRelation,
      SparkPlanVisitorContext& context) override;

  void visit(const spark::connect::Join& join, SparkPlanVisitorContext& context)
      override;

  void visit(
      const spark::connect::Project& project,
      SparkPlanVisitorContext& context) override;

  void visit(
      const spark::connect::Filter& filter,
      SparkPlanVisitorContext& context) override;

  void visit(
      const spark::connect::Aggregate& aggregate,
      SparkPlanVisitorContext& context) override;

  void visit(
      const spark::connect::Limit& limit,
      SparkPlanVisitorContext& context) override;

  void visit(
      const spark::connect::Offset& offset,
      SparkPlanVisitorContext& context) override;

  void visit(const spark::connect::ToDF& toDF, SparkPlanVisitorContext& context)
      override;

  /// Translates the input of DataFrame.show() as its first num_rows + 1 rows,
  /// each column cast to VARCHAR as Spark does before formatting. A VARBINARY
  /// column stays as is; ShowStringFormatter renders it.
  void visit(
      const spark::connect::ShowString& showString,
      SparkPlanVisitorContext& context) override;

  void visit(
      const spark::connect::SubqueryAlias& subqueryAlias,
      SparkPlanVisitorContext& context) override;

  void visit(
      const spark::connect::Expression& expression,
      SparkPlanVisitorContext& context) override {
    SparkPlanVisitor::visit(expression, context);
  }

  void visit(
      const spark::connect::WriteOperation& writeOperation,
      SparkPlanVisitorContext& context) override;

  void visit(const spark::connect::Sort& sort, SparkPlanVisitorContext& context)
      override;

  void visit(
      const spark::connect::Range& range,
      SparkPlanVisitorContext& context) override;

  void visit(const spark::connect::Plan& plan, SparkPlanVisitorContext& context)
      override {
    SparkPlanVisitor::visit(plan, context);
  }

  void visit(
      const spark::connect::Relation& relation,
      SparkPlanVisitorContext& context) override {
    SparkPlanVisitor::visit(relation, context);
  }

  void visit(
      const spark::connect::Expression::Literal& literal,
      SparkPlanVisitorContext& context) override;

  void visit(
      const spark::connect::Expression::UnresolvedAttribute&
          unresolvedAttribute,
      SparkPlanVisitorContext& context) override;

  void visit(
      const spark::connect::Expression::UnresolvedFunction& unresolvedFunction,
      SparkPlanVisitorContext& context) override;

  void visit(
      const spark::connect::Expression::Alias& alias,
      SparkPlanVisitorContext& context) override;

  void visit(
      const spark::connect::Expression::UnresolvedStar& unresolvedStar,
      SparkPlanVisitorContext& context) override;

  void visit(
      const spark::connect::Expression::UnresolvedExtractValue&
          unresolvedExtractValue,
      SparkPlanVisitorContext& context) override;

  void visit(
      const spark::connect::Expression::LambdaFunction& lambdaFunction,
      SparkPlanVisitorContext& context) override;

  void visit(
      const spark::connect::Expression::UnresolvedNamedLambdaVariable&
          unresolvedNamedLambdaVariable,
      SparkPlanVisitorContext& context) override;

  void visit(
      const spark::connect::Expression::Cast& cast,
      SparkPlanVisitorContext& context) override;

  void visit(
      const spark::connect::Expression::ExpressionString& expressionString,
      SparkPlanVisitorContext& context) override;

  void visit(
      const spark::connect::SetOperation& setOperation,
      SparkPlanVisitorContext& context) override;

  void visit(
      const spark::connect::WithColumnsRenamed& withColumnsRenamed,
      SparkPlanVisitorContext& context) override;

  void visit(
      const spark::connect::WithColumns& withColumns,
      SparkPlanVisitorContext& context) override;

  /// Returns the logical plan of the visited relation or command. A query plan
  /// ends in an OutputNode carrying the user-visible column names.
  LogicalPlanNodePtr planNode();

  /// Returns the text of planNode(), or "<empty>" before a visit.
  std::string toString();

 private:
  using PlanBuilder = ::facebook::axiom::logical_plan::PlanBuilder;
  using ExprApi = ::facebook::axiom::logical_plan::ExprApi;

  // An output column: the user-visible name, the column ID and the type.
  struct Column {
    std::string name;
    std::string id;
    facebook::velox::TypePtr type;
  };

  using Columns = std::vector<Column>;

  // Returns a builder with no plan that shares IDs and names with the others.
  PlanBuilder newBuilder() const;

  // Makes the output of 'builder_' the scope of later expressions, and records
  // it as the output of the relation with the context's plan ID.
  void finishRelation(const SparkPlanVisitorContext& context);

  // Returns a reference to 'column'. The reference carries the column's name,
  // which PlanBuilder reads as a reference rather than a rename.
  static ExprApi toCol(const Column& column);

  // Returns the output columns of 'builder', without hidden columns.
  static Columns columnsOf(const PlanBuilder& builder);

  // Visits 'relation' and returns the builder holding its plan.
  PlanBuilder visitInput(
      const spark::connect::Relation& relation,
      SparkPlanVisitorContext& context);

  // Visits 'expression' and returns it with the name it was given, if any.
  ExprApi toExpr(
      const spark::connect::Expression& expression,
      SparkPlanVisitorContext& context);

  // Returns the column of 'scope_' with the given name.
  ExprApi bindColumn(std::string_view name) const;

  // Returns the column of 'scope_' that one of 'outputs' named 'name'.
  // 'reference' describes the reference in error messages.
  ExprApi bindColumn(
      std::string_view name,
      const std::vector<Columns>& outputs,
      std::string_view reference) const;

  // Returns the column a name refers to: a, a column of a DataFrame alias or
  // table, t.a or s.t.a, or a struct field, a.f or t.a.f.
  ExprApi bindName(std::span<const std::string> nameParts) const;

  // Translates a Project that has a generator (explode, posexplode) into an
  // Unnest followed by a Project. Spark allows one generator per Project. The
  // Project outputs the non-generator expressions, then for posexplode the
  // 0-based position, named 'pos' unless aliased, then the unnested columns,
  // named 'col' (array) or 'key' and 'value' (map) unless aliased.
  void visitProjectWithGenerator(
      const spark::connect::Project& project,
      SparkPlanVisitorContext& context);

  const std::string catalog_;
  const std::string schema_;
  MemoryPool* const pool_;

  // Shared by all builders, so column IDs are unique across the plan.
  PlanBuilder::Context builderContext_;

  // Plan of the relation being translated.
  std::optional<PlanBuilder> builder_;

  // Columns that expressions being translated can reference.
  Columns scope_;

  // Translated expression, set by the expression visit methods.
  std::optional<ExprApi> expr_;

  // Output columns of each relation with a plan ID. A plan ID appears more
  // than once when the client reuses a DataFrame, as in a self-join.
  folly::F14FastMap<int64_t, std::vector<Columns>> planIdOutputs_;

  // Output columns of each DataFrame alias and table name.
  folly::F14FastMap<std::string, std::vector<Columns>> aliasOutputs_;

  // Lambda variables in scope, innermost last: the name in the Spark plan and
  // the parameter name allocated for it, which differs from every column ID.
  std::vector<std::pair<std::string, std::string>> lambdaVariables_;

  // True if the plan is a table write, which has no OutputNode.
  bool isWrite_{false};

  // Plan returned by planNode(), built on first use.
  LogicalPlanNodePtr plan_;
};

} // namespace axiom::spark_connect
