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

#include "optimizer/FunctionRegistry.h" //@manual
#include "optimizer/ParallelProject.h" //@manual
#include "optimizer/Plan.h" //@manual
#include "optimizer/PlanUtils.h" //@manual
#include "velox/core/Expressions.h"

namespace facebook::velox::optimizer {

struct LevelData {
  int32_t exprCount{0};
  float levelCost{0};
  PlanObjectSet exprs;
};

int32_t definitionLevel(std::vector<LevelData>& levels, ExprCP expr) {
  for (auto i = 0; i < levels.size(); ++i) {
    if (levels[i].exprs.contains(expr)) {
      return i;
    }
  }
  VELOX_UNREACHABLE();
}

void makeExprLevels(
    PlanObjectSet exprs,
    std::vector<LevelData>& levelData,
    std::unordered_map<ExprCP, int32_t>& refCount) {
  PlanObjectSet counted;
  for (;;) {
    PlanObjectSet inputs;
    levelData.emplace_back();
    int32_t levelIdx = levelData.size() - 1;
    exprs.forEach([&](PlanObjectCP o) {
      auto* expr = o->as<Expr>();
      float self = selfCost(expr);
      if (counted.contains(expr)) {
        auto i = definitionLevel(levelData, expr);
        levelData[i].exprs.erase(expr);
        levelData[i].levelCost -= self;
      }
      levelData[levelIdx].exprs.add(expr);
      levelData[levelIdx].levelCost += self;
      counted.add(expr);
      if (expr->type() == PlanType::kCall) {
        for (auto& input : expr->as<Call>()->args()) {
          if (input->type() == PlanType::kLiteral) {
            continue;
          }
          ++refCount[input];
          inputs.add(input);
        }
      }
    });
    if (inputs.empty()) {
      return;
    }
    exprs = std::move(inputs);
  }
}

PlanObjectSet makeCseBorder(
    std::vector<LevelData> levelData,
    PlanObjectSet& placed,
    std::unordered_map<ExprCP, int32_t>& refCount) {
  PlanObjectSet border;
  for (int32_t leafLevel = levelData.size() - 1; leafLevel >= 0; --leafLevel) {
    levelData[leafLevel].exprs.forEach([&](PlanObjectCP o) {
      ExprCP expr = o->as<const Expr>();
      if (placed.contains(expr)) {
        return;
      }
      if (refCount[expr] > 1) {
        auto subexprs = expr->subexpressions();
        subexprs.intersect(border);
        if (!subexprs.empty()) {
          // Is a multiply refd over another multiply refd in the same border.
          // Not a member.
          return;
        }
        border.add(expr);
      }
    });
  }
  return border;
}

core::PlanNodePtr Optimization::makeParallelProject(
    core::PlanNodePtr input,
    const PlanObjectSet& topExprs,
    const PlanObjectSet& placed,
    const PlanObjectSet& extraColumns) {
  std::vector<std::string> names;
  std::vector<int32_t> indices;
  std::vector<float> costs;
  std::vector<ExprCP> exprs;
  float totalCost = 0;
  topExprs.forEach([&](PlanObjectCP o) {
    exprs.push_back(o->as<Expr>());
    indices.push_back(indices.size());
    costs.push_back(costWithChildren(o->as<Expr>(), placed));
    totalCost += costs.back();
  });
  std::sort(indices.begin(), indices.end(), [&](int32_t l, int32_t r) {
    return costs[l] < costs[r];
  });

  // Sorted lowest cost first. Make even size groups.
  float targetCost = totalCost / opts_.parallelProjectWidth;
  float groupCost = 0;
  std::vector<std::vector<core::TypedExprPtr>> groups;
  groups.emplace_back();
  for (auto nth = 0; nth < indices.size(); ++nth) {
    auto i = indices[nth];
    groupCost += costs[i];
    groups.back().push_back(toTypedExpr(exprs[i]));
    names.push_back(fmt::format("__temp{}", exprs[i]->id()));
    auto fieldAccess = std::make_shared<core::FieldAccessTypedExpr>(
        groups.back().back()->type(), names.back());
    projectedExprs_[exprs[i]] = fieldAccess;
    if (groupCost > targetCost) {
      if (nth == indices.size() - 1) {
        break;
      }
      // Start new group after placing target cost worth.
      groups.emplace_back();
      groupCost = 0;
    }
  }

  std::vector<std::string> extra;
  extraColumns.forEach([&](PlanObjectCP o) {
    auto e = toTypedExpr(o->as<Expr>());
    if (auto* field =
            dynamic_cast<const core::FieldAccessTypedExpr*>(e.get())) {
      extra.push_back(field->name());
    } else {
      VELOX_UNREACHABLE();
    }
  });
  return std::make_shared<exec::ParallelProjectNode>(
      idGenerator().next(),
      std::move(names),
      std::move(groups),
      std::move(extra),
      input);
}

// Returns the columns used by Exprs in 'top', excluding columns only referenced
// from 'placed'.
void columnBorder(
    ExprCP expr,
    const PlanObjectSet& placed,
    PlanObjectSet& result) {
  if (placed.contains(expr)) {
    result.add(expr);
    return;
  }
  switch (expr->type()) {
    case PlanType::kColumn:
      result.add(expr);
      return;
    case PlanType::kCall: {
      for (auto& in : expr->as<Call>()->args()) {
        columnBorder(in, placed, result);
      }
      return;
    }
    case PlanType::kAggregate:
      VELOX_UNREACHABLE();
    default:
      return;
  }
}

PlanObjectSet columnBorder(
    const PlanObjectSet& top,
    const PlanObjectSet& placed) {
  PlanObjectSet result;
  top.forEach(
      [&](PlanObjectCP o) { columnBorder(o->as<Expr>(), placed, result); });

  return result;
}

float parallelBorder(
    ExprCP expr,
    const PlanObjectSet& placed,
    PlanObjectSet& result) {
  // Cost returned for a subexpressoin that is parallelized. Siblings of these
  // that are themselves not split should b members of the border.
  constexpr float kSplit = -1;
  constexpr float kTargetCost = 50;
  if (placed.contains(expr)) {
    return 0;
  }
  switch (expr->type()) {
    case PlanType::kColumn:
      return selfCost(expr);
    case PlanType::kCall: {
      float cost = selfCost(expr);
      auto call = expr->as<Call>();
      BitSet splitArgs;
      auto args = call->args();
      float allArgsCost = 0;
      float highestArgCost = 0;
      for (auto i = 0; i < args.size(); ++i) {
        auto arg = args[i];
        auto argCost = parallelBorder(arg, placed, result);
        if (argCost > highestArgCost) {
          highestArgCost = argCost;
        }
        if (argCost == kSplit) {
          splitArgs.add(i);
        }
        allArgsCost += argCost;
      }
      if (!splitArgs.empty()) {
        // If some arg produced parallel pieces, the non-parallelized siblings
        // are added to the border.
        for (auto i = 0; i < args.size(); ++i) {
          if (!splitArgs.contains(i)) {
            result.add(args[i]);
          }
        }
        return kSplit;
      }
      if (allArgsCost > kTargetCost && highestArgCost < allArgsCost / 2) {
        // The args are above the target and the biggest is less than half the
        // total. Add the args to the border.
        for (auto i = 0; i < args.size(); ++i) {
          result.add(args[i]);
        }
        return kSplit;
      }
      return cost + allArgsCost;
    }

    case PlanType::kAggregate:
      VELOX_UNREACHABLE();
    default:
      return 0;
  }
}

core::PlanNodePtr Optimization::maybeParallelProject(
    Project* project,
    core::PlanNodePtr input) {
  PlanObjectSet top;
  PlanObjectSet allColumns;
  PlanObjectSet placed;
  auto& exprs = project->exprs();
  auto& columns = project->columns();
  for (auto e : exprs) {
    allColumns.unionSet(e->columns());
    top.add(e);
  }
  std::vector<LevelData> levelData;
  std::unordered_map<ExprCP, int32_t> refCount;
  makeExprLevels(top, levelData, refCount);

  for (;;) {
    auto cses = makeCseBorder(levelData, placed, refCount);
    if (cses.empty()) {
      break;
    }
    auto previousPlaced = placed;
    cses.forEach([&](PlanObjectCP o) {
      placed.unionSet(o->as<Expr>()->subexpressions());
    });
    placed.unionSet(cses);
    auto extraColumns = columnBorder(top, placed);
    extraColumns.except(cses);
    input = makeParallelProject(input, cses, previousPlaced, extraColumns);
  }

  // Common prerequisites are placed, the expressions that are left  are a tree
  // with no order
  PlanObjectSet parallel;
  top.forEach(
      [&](PlanObjectCP o) { parallelBorder(o->as<Expr>(), placed, parallel); });
  auto previousPlaced = placed;
  parallel.forEach([&](PlanObjectCP o) {
    placed.unionSet(o->as<Expr>()->subexpressions());
  });
  placed.unionSet(parallel);
  auto extra = columnBorder(top, placed);
  // The projected through columns are loaded here, so these go into the
  // parallel exprs and not in the 'noLoadIdentities'.
  parallel.unionSet(extra);
  PlanObjectSet empty;
  input = makeParallelProject(input, parallel, previousPlaced, empty);
  // one final project for the renames and final functions.
  std::vector<std::string> names;
  std::vector<core::TypedExprPtr> finalExprs;
  for (auto i = 0; i < exprs.size(); ++i) {
    names.push_back(columns[i]->toString());
    finalExprs.push_back(toTypedExpr(exprs[i]));
  }
  return std::make_shared<core::ProjectNode>(
      idGenerator().next(), std::move(names), std::move(finalExprs), input);
}

} // namespace facebook::velox::optimizer
