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

#include <velox/exec/Driver.h>
#include <velox/exec/Operator.h>
#include "velox/expression/Expr.h"

namespace facebook::velox::exec {

/// Variant of ProjectNode that computes projections in
/// parallel. The exprs are given in groups, so that all exprs in
/// one group run together and all groups run in parallel. If lazies
/// are loaded, each lazy must be loaded by exactly one group. If
/// there are identity projections in the groups, possible lazies
/// are loaded as part of processing the group. One can additionally
/// specify 'noLoadIdentities' which are identity projected through
/// without loading. This last set must be disjoint from all columns
/// accessed by the exprs. The output type has 'names' first and
/// then 'noLoadIdentities'. The ith name corresponds to the ith
/// expr when exprs is flattened. Inherits core::ProjectNode in order to reuse
/// the summary functions.
class ParallelProjectNode : public core::AbstractProjectNode {
 public:
  ParallelProjectNode(
      const core::PlanNodeId& id,
      std::vector<std::string> names,
      std::vector<std::vector<core::TypedExprPtr>> exprs,
      std::vector<std::string> noLoadIdentities,
      core::PlanNodePtr input)
      : AbstractProjectNode(
            id,
            allNames(names, noLoadIdentities),
            flatExprs(exprs, noLoadIdentities, input),
            input),
        exprNames_(std::move(names)),
        exprs_(std::move(exprs)),
        noLoadIdentities_(std::move(noLoadIdentities)) {}

  std::string_view name() const override {
    return "ParallelProject";
  }

  const std::vector<std::string>& exprNames() const {
    return exprNames_;
  }

  const std::vector<std::vector<core::TypedExprPtr>>& exprs() const {
    return exprs_;
  }

  const std::vector<std::string> noLoadIdentities() const {
    return noLoadIdentities_;
  }

 private:
  // makes a list of all names for use in the ProjectNode.
  static std::vector<std::string> allNames(
      const std::vector<std::string>& names,
      const std::vector<std::string>& moreNames);

  // Flattens out projection exprs and adds dummy  exprs for noLoadIdentities.
  // Used to fill in ProjectNode members for use in the summary functions.
  static std::vector<core::TypedExprPtr> flatExprs(
      std::vector<std::vector<core::TypedExprPtr>>& exprs,
      const std::vector<std::string> moreNames,
      const core::PlanNodePtr& input);

  void addDetails(std::stringstream& stream) const override;

  std::vector<std::string> exprNames_;
  std::vector<std::vector<core::TypedExprPtr>> exprs_;
  std::vector<std::string> noLoadIdentities_;
};

class ParallelProject : public Operator {
 public:
  ParallelProject(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const ParallelProjectNode>& node);

  bool isFilter() const override {
    return false;
  }

  bool preservesOrder() const override {
    return true;
  }

  bool needsInput() const override {
    return !input_;
  }

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  BlockingReason isBlocked(ContinueFuture* /* unused */) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override;

  void close() override {
    Operator::close();
    for (auto& work : work_) {
      if (work.exprSet) {
        work.exprSet->clear();
      }
    }
  }

  void initialize() override;

 private:
  struct WorkUnit {
    // Maps from result channel of exprSet to channel in the node's output type.
    std::vector<IdentityProjection> resultProjections;
    // Positions in input which are to be loaded by this group.
    std::vector<column_index_t> loadOnly;
    std::unique_ptr<core::ExecCtx> execCtx;
    std::shared_ptr<ExprSet> exprSet;
  };

  struct WorkResult {
    WorkResult(std::exception_ptr e) : error(std::move(e)) {}
    std::exception_ptr error;
  };

  // Tests if 'numProcessedRows_' equals to the length of input_ and clears
  // outstanding references to input_ if done. Returns true if getOutput
  // should return nullptr.
  bool allInputProcessed();

  std::unique_ptr<WorkResult> doWork(
      int32_t workIdx,
      std::vector<VectorPtr>& result);

  // Cached Parallelproject node for lazy initialization. After
  // initialization, they will be reset, and initialized_ will be set to true.
  std::shared_ptr<const ParallelProjectNode> node_;

  bool initialized_{false};

  std::vector<WorkUnit> work_;
  SelectivityVector allRows_;
  int32_t numProcessedInputRows_{0};
};

} // namespace facebook::velox::exec
