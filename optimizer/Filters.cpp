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

#include "optimizer/Plan.h" //@manual
#include "optimizer/PlanUtils.h" //@manual
#include "optimizer/QueryGraph.h" //@manual
#include "velox/common/base/SimdUtil.h"
#include "velox/common/base/SuccinctPrinter.h"

namespace facebook::velox::optimizer {

Cost filterCost(CPSpan<Expr> conjuncts) {
  return Cost();
}

void flattenAll(ExprCP expr, Name func, ExprVector& flat) {
  if (expr->type() != PlanType::kCall || expr->as<Call>()->name() != func) {
    flat.push_back(expr);
    return;
  }
  for (auto arg : expr->as<Call>()->args()) {
    flattenAll(arg, func, flat);
  }
}

bool isCallExpr(ExprCP expr, Name name) {
  return expr->type() == PlanType::kCall && expr->as<Call>()->name() == name;
}

ExprCP Optimization::combineLeftDeep(
    Name func,
    const ExprVector& set1,
    const ExprVector& set2) {
  ExprVector all = set1;
  all.insert(all.end(), set2.begin(), set2.end());
  std::sort(all.begin(), all.end(), [&](ExprCP left, ExprCP right) {
    return left->id() < right->id();
  });
  ExprCP result = all[0];
  for (auto i = 1; i < all.size(); ++i) {
    result = deduppedCall(
        func,
        result->value(),
        ExprVector{result, all[i]},
        result->functions() | all[i]->functions());
  }
  return result;
}

// 'disjuncts' is an OR of ANDs. If each disjunct depends on the same tables and
// if each conjunct inside the ANDs in the OR depends on a single table, then
// return for each distinct table an OR of ANDs. The disjuncts are the top
// vector the conjuncts are the inner vector.
ExprVector extractPerTable(
    const ExprVector& disjuncts,
    std::vector<ExprVector>& orOfAnds) {
  PlanObjectSet tables;
  auto optimization = queryCtx()->optimization();
  auto& names = optimization->builtinNames();
  tables = disjuncts[0]->allTables();
  if (tables.size() <= 1) {
    // All must depend on the same set of more than 1 table.
    return {};
  }
  std::unordered_map<int32_t, std::vector<ExprVector>> perTable;

  for (auto i = 0; i < disjuncts.size(); ++i) {
    if (i > 0 && disjuncts[i]->allTables() != tables) {
      // Does not  depend on the same tables as the other disjuncts.
      return {};
    }
    std::unordered_map<int32_t, ExprVector> perTableAnd;
    ExprVector& inner = orOfAnds[i];
    // do the inner conjuncts each depend on a single table?
    for (auto j = 0; j < inner.size(); ++j) {
      auto single = singleTable(inner[j]);
      if (!single) {
        return {};
      }
      perTableAnd[single->id()].push_back(inner[j]);
    }
    for (auto& pair : perTableAnd) {
      perTable[pair.first].push_back(pair.second);
    }
  }
  ExprVector conjuncts;
  for (auto& pair : perTable) {
    ExprVector tableAnds;
    for (auto& tableAnd : pair.second) {
      tableAnds.push_back(
          optimization->combineLeftDeep(names._and, tableAnd, {}));
    }
    conjuncts.push_back(
        optimization->combineLeftDeep(names._or, tableAnds, {}));
  }
  return conjuncts;
}

/// Analyzes an OR. Returns top level conjuncts that this has
/// inferred from the disjuncts. For example if all have an AND
/// inside and each AND has the same condition then this condition
/// is returned and removed from the disjuncts. 'disjuncts' is
/// changed in place. If 'replacement' is set, then this replaces
/// the whole OR frfrom which 'disjuncts' was flattened.
ExprVector extractCommon(ExprVector& disjuncts, ExprCP* replacement) {
  std::unordered_set<ExprCP> distinct;
  PlanObjectSet tableSet;
  auto optimization = queryCtx()->optimization();
  auto& names = optimization->builtinNames();
  ExprVector result;
  // Remove duplicates.
  bool changeOriginal = false;
  for (auto i = 0; i < disjuncts.size(); ++i) {
    auto disjunct = disjuncts[i];
    if (distinct.find(disjunct) != distinct.end()) {
      disjuncts.erase(disjuncts.begin() + i);
      --i;
      changeOriginal = true;
      continue;
    }
    distinct.insert(disjunct);
  }
  if (disjuncts.size() == 1) {
    *replacement = disjuncts[0];
    return {};
  }

  // The conjuncts  in each of the disjuncts.
  std::vector<ExprVector> flat;
  for (auto i = 0; i < disjuncts.size(); ++i) {
    flat.emplace_back();
    flattenAll(disjuncts[i], names._and, flat.back());
  }
  // Check if the flat conjuncts lists have any element that occurs in all.
  // Remove all the elememts that are in all.
  for (auto j = 0; j < flat[0].size(); ++j) {
    auto item = flat[0][j];
    bool inAll = true;
    for (auto i = 1; i < flat.size(); ++i) {
      if (std::find(flat[i].begin(), flat[i].end(), item) == flat[i].end()) {
        inAll = false;
        break;
      }
    }
    if (inAll) {
      changeOriginal = true;
      result.push_back(item);
      flat[0].erase(flat[0].begin() + j);
      --j;
      for (auto i = 1; i < flat.size(); ++i) {
        flat[i].erase(std::find(flat[i].begin(), flat[i].end(), item));
      }
    }
  }
  auto perTable = extractPerTable(disjuncts, flat);
  if (!perTable.empty()) {
    // The per-table extraction does not alter the original but can surface
    // things to push down.
    result.insert(result.end(), perTable.begin(), perTable.end());
  }
  if (changeOriginal) {
    ExprVector ands;
    for (auto inner : flat) {
      ands.push_back(optimization->combineLeftDeep(names._and, inner, {}));
    }
    *replacement = optimization->combineLeftDeep(names._or, ands, {});
  }

  return result;
}

void DerivedTable::expandConjuncts() {
  auto& names = queryCtx()->optimization()->builtinNames();
  bool any;
  std::unordered_set<int32_t> processed;
  int32_t firstUnprocessed = numCanonicalConjuncts;
  do {
    any = false;
    int32_t numProcessed = conjuncts.size();
    auto end = conjuncts.size();
    for (auto i = firstUnprocessed; i < end; ++i) {
      auto conjunct = conjuncts[i];
      if (isCallExpr(conjunct, names._or) &&
          !conjunct->containsFunction(FunctionSet::kNondeterministic)) {
        ExprVector flat;
        flattenAll(conjunct, names._or, flat);
        ExprCP replace = nullptr;
        ExprVector common = extractCommon(flat, &replace);
        if (replace) {
          any = true;
          conjuncts[i] = replace;
        }
        if (!common.empty()) {
          any = true;
          conjuncts.insert(conjuncts.end(), common.begin(), common.end());
        }
      }
    }
    firstUnprocessed = numProcessed;
    numCanonicalConjuncts = numProcessed;
  } while (any);
}

} // namespace facebook::velox::optimizer
