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

#include "axiom/logical_plan/LogicalPlanNode.h"
#include "axiom/pyspark/SparkPlanVisitor.h"
#include "axiom/pyspark/third-party/protos/relations.grpc.pb.h" // @manual=fbcode//axiom/pyspark/third-party/protos:collagen_proto-cpp
#include "velox/parse/PlanNodeIdGenerator.h"

namespace axiom::collagen {

/// Converts a Spark Connect json type representation into a Velox type.
facebook::velox::TypePtr jsonToVeloxType(const std::string& jsonInput);

/// Spark visitor implementation that converts the input protobuf Spark Connect
/// plan into a axiom/logical_plan structure which can serve as an input to the
/// Axiom optimizer.
class SparkToAxiom : public SparkPlanVisitor {
 public:
  using LogicalPlanNodePtr =
      ::facebook::axiom::logical_plan::LogicalPlanNodePtr;
  using ExprPtr = ::facebook::axiom::logical_plan::ExprPtr;
  using PlanNodeIdGenerator = ::facebook::velox::core::PlanNodeIdGenerator;
  using MemoryPool = ::facebook::velox::memory::MemoryPool;

  SparkToAxiom(std::string catalog, std::string schema, MemoryPool* pool)
      : catalog_(std::move(catalog)), schema_(std::move(schema)), pool_(pool) {}

  virtual ~SparkToAxiom() = default;

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

  virtual void visit(
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

  virtual void visit(
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
      const spark::connect::SetOperation& setOperation,
      SparkPlanVisitorContext& context) override;

  void visit(
      const spark::connect::WithColumnsRenamed& withColumnsRenamed,
      SparkPlanVisitorContext& context) override;

  void visit(
      const spark::connect::WithColumns& withColumns,
      SparkPlanVisitorContext& context) override;

  const LogicalPlanNodePtr& planNode() const {
    return planNode_;
  }

  const ExprPtr& expr() const {
    return expr_;
  }

  const std::string& exprName() const {
    return exprName_;
  }

  std::string toString();

 private:
  std::string nextId() {
    return planNodeIdGenerator_->next();
  }

  void setPlanNode(const LogicalPlanNodePtr& planNode, int64_t planId);

  // Catalog to use for table resolution.
  const std::string catalog_;

  // Namespace to use for table resolution and creation.
  const std::string schema_;

  // Current plan node and expr objects being constructed when traversing the
  // tree.
  LogicalPlanNodePtr planNode_;
  ExprPtr expr_;
  std::string exprName_;

  std::shared_ptr<PlanNodeIdGenerator> planNodeIdGenerator_{
      std::make_shared<PlanNodeIdGenerator>()};

  MemoryPool* pool_;

  /// Maintain mappings between the Spark Connect plan id and aliases to the
  /// Axiom logical plan created as the plan tree is traversed.
  std::unordered_map<int64_t, LogicalPlanNodePtr> sparkPlanIdToPlanNode_;
  std::unordered_map<std::string, LogicalPlanNodePtr> sparkAliasToPlanNode_;

  /// Helper method for processing higher-order functions that take lambdas.
  /// Handles type inference for lambda parameters based on the array/collection
  /// type before visiting the lambda expression.
  void visitHigherOrderFunction(
      const spark::connect::Expression::UnresolvedFunction& unresolvedFunction,
      SparkPlanVisitorContext& context,
      std::vector<ExprPtr>& params,
      std::vector<facebook::velox::TypePtr>& paramTypes);

  /// Handles a Project containing a generator function (explode/posexplode)
  /// by creating an UnnestNode instead of a ProjectNode.
  void visitProjectWithGenerator(
      const spark::connect::Project& project,
      SparkPlanVisitorContext& context);
};

} // namespace axiom::collagen
