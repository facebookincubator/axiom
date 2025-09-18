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

#include <ranges>

#include "axiom/optimizer/ToVelox.h"
#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"

namespace facebook::axiom::optimizer {

namespace {

struct LevelData {
  PlanObjectSet exprs;
};

size_t levelOf(std::vector<LevelData>& levels, ExprCP expr) {
  for (auto i = 0; i < levels.size(); ++i) {
    if (levels[i].exprs.contains(expr)) {
      return i;
    }
  }
  VELOX_UNREACHABLE();
}

void pushdownExpr(
    ExprCP expr,
    int32_t level,
    std::vector<LevelData>& levelData) {
  const auto defined = levelOf(levelData, expr);
  if (defined >= level) {
    return;
  }

  if (level >= levelData.size()) {
    levelData.resize(level + 1);
  }
  levelData[defined].exprs.erase(expr);
  levelData[level].exprs.add(expr);

  if (expr->is(PlanType::kCallExpr)) {
    for (auto& input : expr->as<Call>()->args()) {
      if (!input->is(PlanType::kLiteralExpr)) {
        pushdownExpr(input, level + 1, levelData);
      }
    }
  }
}

void makeLevelsInner(
    ExprCP expr,
    int32_t level,
    std::vector<LevelData>& levelData,
    folly::F14FastMap<ExprCP, int32_t>& refCount,
    PlanObjectSet& counted) {
  if (expr->is(PlanType::kLiteralExpr)) {
    return;
  }

  if (counted.contains(expr)) {
    ++refCount[expr];
    pushdownExpr(expr, level, levelData);
    return;
  }

  if (level >= levelData.size()) {
    levelData.resize(level + 1);
  }

  counted.add(expr);
  ++refCount[expr];
  levelData[level].exprs.add(expr);
  if (expr->is(PlanType::kCallExpr)) {
    for (auto& input : expr->as<Call>()->args()) {
      makeLevelsInner(input, level + 1, levelData, refCount, counted);
    }
  }
}

void makeExprLevels(
    const PlanObjectSet& exprs,
    std::vector<LevelData>& levelData,
    folly::F14FastMap<ExprCP, int32_t>& refCount) {
  PlanObjectSet counted;
  exprs.forEach<Expr>([&](ExprCP expr) {
    makeLevelsInner(expr, 0, levelData, refCount, counted);
  });
}

PlanObjectSet makeCseBorder(
    const std::vector<LevelData>& levelData,
    PlanObjectSet& placed,
    folly::F14FastMap<ExprCP, int32_t>& refCount) {
  PlanObjectSet border;
  for (const auto& data : levelData | std::views::reverse) {
    data.exprs.forEach<Expr>([&](auto expr) {
      if (placed.contains(expr)) {
        return;
      }
      if (refCount[expr] > 1) {
        auto subexprs = expr->subexpressions();
        subexprs.intersect(border);
        if (!subexprs.empty()) {
          // Multiply referenced over another multiply referenced in the
          // same border. Not a member.
          return;
        }
        border.add(expr);
      }
    });
  }
  return border;
}

} // namespace

velox::core::PlanNodePtr ToVelox::makeParallelProject(
    const velox::core::PlanNodePtr& input,
    const PlanObjectSet& topExprs,
    const PlanObjectSet& placed,
    const PlanObjectSet& extraColumns) {
  std::vector<uint32_t> indices;
  std::vector<float> costs;
  std::vector<ExprCP> exprs;
  float totalCost = 0;
  topExprs.forEach<Expr>([&](auto expr) {
    exprs.push_back(expr);
    indices.push_back(indices.size());
    costs.push_back(costWithChildren(expr, placed));
    totalCost += costs.back();
  });
  std::ranges::sort(
      indices, [&](auto l, auto r) { return costs[l] < costs[r]; });

  // Sorted lowest cost first. Make even size groups.
  const float targetCost =
      totalCost / static_cast<float>(optimizerOptions_.parallelProjectWidth);

  std::vector<std::vector<velox::core::TypedExprPtr>> groups;
  groups.emplace_back();

  auto* group = &groups.back();
  float groupCost = 0;

  std::vector<std::string> names;
  for (auto i : indices) {
    if (groupCost > targetCost) {
      // Start new group after placing target cost worth.
      groups.emplace_back();

      group = &groups.back();
      groupCost = 0;
    }

    auto expr = exprs[i];

    groupCost += costs[i];
    group->emplace_back(toTypedExpr(expr));

    if (expr->is(PlanType::kColumnExpr)) {
      names.push_back(outputName(expr->as<Column>()));
    } else {
      names.push_back(fmt::format("__temp{}", expr->id()));
    }

    auto fieldAccess = std::make_shared<velox::core::FieldAccessTypedExpr>(
        group->back()->type(), names.back());
    projectedExprs_[expr] = fieldAccess;
  }

  std::vector<std::string> extra;
  extraColumns.forEach<Expr>([&](ExprCP expr) {
    auto veloxExpr = toTypedExpr(expr);
    VELOX_CHECK(veloxExpr->isFieldAccessKind());
    extra.push_back(
        veloxExpr->asUnchecked<velox::core::FieldAccessTypedExpr>()->name());
  });

  return std::make_shared<velox::core::ParallelProjectNode>(
      nextId(), std::move(names), std::move(groups), std::move(extra), input);
}

namespace {

// Returns the columns used by Exprs in 'top', excluding columns only referenced
// from 'placed'.
void columnBorder(
    ExprCP expr,
    const PlanObjectSet& placed,
    PlanObjectSet& result) {
  if (expr->is(PlanType::kLiteralExpr)) {
    return;
  }

  if (placed.contains(expr)) {
    result.add(expr);
    return;
  }

  switch (expr->type()) {
    case PlanType::kColumnExpr:
      result.add(expr);
      return;

    case PlanType::kCallExpr:
      for (auto arg : expr->as<Call>()->args()) {
        columnBorder(arg, placed, result);
      }
      return;

    case PlanType::kAggregateExpr:
      VELOX_UNREACHABLE();
    default:
      return;
  }
}

PlanObjectSet columnBorder(
    const PlanObjectSet& top,
    const PlanObjectSet& placed) {
  PlanObjectSet result;
  top.forEach<Expr>([&](auto expr) { columnBorder(expr, placed, result); });
  return result;
}

float parallelBorder(
    ExprCP expr,
    const PlanObjectSet& placed,
    PlanObjectSet& result) {
  // Cost returned for a subexpression that is parallelized. Siblings of these
  // that are themselves not split should b members of the border.
  constexpr float kSplit = -1;
  constexpr float kTargetCost = 50;
  if (placed.contains(expr)) {
    return 0;
  }

  switch (expr->type()) {
    case PlanType::kColumnExpr:
      return selfCost(expr);

    case PlanType::kCallExpr: {
      const float cost = selfCost(expr);
      auto call = expr->as<Call>();
      BitSet splitArgs;
      auto args = call->args();
      float allArgsCost = 0;
      float highestArgCost = 0;
      for (auto i = 0; i < args.size(); ++i) {
        auto arg = args[i];
        auto argCost = parallelBorder(arg, placed, result);
        highestArgCost = std::max(highestArgCost, argCost);
        if (argCost == kSplit) {
          splitArgs.add(i);
        }
        allArgsCost += argCost;
      }

      if (!splitArgs.empty()) {
        // If some arg produced parallel pieces, the non-parallelized siblings
        // are added to the border.
        for (auto i = 0; i < args.size(); ++i) {
          if (!splitArgs.contains(i) && !args[i]->is(PlanType::kLiteralExpr)) {
            result.add(args[i]);
          }
        }
        return kSplit;
      }

      if (allArgsCost > kTargetCost && highestArgCost < allArgsCost / 2) {
        // The args are above the target and the biggest is less than half the
        // total. Add the args to the border.
        for (auto arg : args) {
          if (!arg->is(PlanType::kLiteralExpr)) {
            result.add(arg);
          }
        }

        return kSplit;
      }
      return cost + allArgsCost;
    }

    case PlanType::kAggregateExpr:
      VELOX_UNREACHABLE();

    default:
      return 0;
  }
}
} // namespace

velox::core::PlanNodePtr ToVelox::maybeParallelProject(
    const Project* project,
    velox::core::PlanNodePtr input) {
  PlanObjectSet top;
  PlanObjectSet allColumns;
  const auto& exprs = project->exprs();
  for (auto expr : exprs) {
    allColumns.unionSet(expr->columns());
    top.add(expr);
  }

  std::vector<LevelData> levelData;
  folly::F14FastMap<ExprCP, int32_t> refCount;
  makeExprLevels(top, levelData, refCount);

  PlanObjectSet placed;

  for (;;) {
    auto cses = makeCseBorder(levelData, placed, refCount);
    if (cses.empty()) {
      break;
    }

    auto previousPlaced = placed;
    cses.forEach<Expr>(
        [&](auto expr) { placed.unionSet(expr->subexpressions()); });
    placed.unionSet(cses);

    auto extraColumns = columnBorder(top, placed);
    extraColumns.except(cses);
    input = makeParallelProject(input, cses, previousPlaced, extraColumns);
  }

  // Common prerequisites are placed, the expressions that are left are a tree
  // with no order.
  PlanObjectSet parallel;
  top.forEach<Expr>([&](auto expr) { parallelBorder(expr, placed, parallel); });

  if (!parallel.empty()) {
    auto previousPlaced = placed;
    parallel.forEach<Expr>(
        [&](auto expr) { placed.unionSet(expr->subexpressions()); });
    placed.unionSet(parallel);

    auto extra = columnBorder(top, placed);
    // The projected through columns are loaded here, so these go into the
    // parallel exprs and not in the 'noLoadIdentities'.
    parallel.unionSet(extra);

    PlanObjectSet empty;
    input = makeParallelProject(input, parallel, previousPlaced, empty);
  }
  // One final project for the renames and final functions.
  auto& columns = project->columns();

  std::vector<std::string> names;
  std::vector<velox::core::TypedExprPtr> finalExprs;
  names.reserve(exprs.size());
  finalExprs.reserve(exprs.size());

  for (auto i = 0; i < exprs.size(); ++i) {
    names.emplace_back(outputName(columns[i]));
    finalExprs.emplace_back(toTypedExpr(exprs[i]));
  }

  return std::make_shared<velox::core::ProjectNode>(
      nextId(), std::move(names), std::move(finalExprs), input);
}

} // namespace facebook::axiom::optimizer
