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

#include "axiom/sql/presto/ColumnsExpansion.h"

#include <re2/re2.h>
#include "axiom/sql/presto/PrestoSqlError.h"
#include "velox/common/base/Exceptions.h"

namespace axiom::sql::presto {

namespace lp = facebook::axiom::logical_plan;
using namespace facebook::velox;

namespace {

// Recursively replaces expression tree nodes per the replacement map.
core::ExprPtr replaceInputs(
    const core::ExprPtr& expr,
    const std::unordered_map<const core::IExpr*, core::ExprPtr>& replacements) {
  auto it = replacements.find(expr.get());
  if (it != replacements.end()) {
    return it->second;
  }

  std::vector<core::ExprPtr> newInputs;
  bool changed = false;
  for (const auto& input : expr->inputs()) {
    auto newInput = replaceInputs(input, replacements);
    if (newInput.get() != input.get()) {
      changed = true;
    }
    newInputs.push_back(std::move(newInput));
  }

  return changed ? expr->replaceInputs(std::move(newInputs)) : expr;
}

// Searches an expression tree for COLUMNS('regex') pseudo-function calls.
// Appends each found call (node pointer + regex pattern) to 'results'.
void findColumnsCalls(
    const core::ExprPtr& expr,
    std::vector<std::pair<const core::IExpr*, std::string>>& results) {
  if (expr->is(core::IExpr::Kind::kCall)) {
    auto* callExpr = expr->as<core::CallExpr>();
    if (callExpr->name() == "COLUMNS") {
      // The grammar guarantees COLUMNS takes a single string literal argument.
      VELOX_CHECK_EQ(callExpr->inputs().size(), 1);

      const auto& arg = callExpr->inputAt(0);
      VELOX_CHECK(arg->is(core::IExpr::Kind::kConstant));

      auto* constExpr = arg->as<core::ConstantExpr>();
      VELOX_CHECK_EQ(constExpr->type()->kind(), TypeKind::VARCHAR);
      VELOX_CHECK(!constExpr->value().isNull());

      results.emplace_back(expr.get(), constExpr->value().value<std::string>());
      return;
    }
  }

  for (const auto& input : expr->inputs()) {
    findColumnsCalls(input, results);
  }
}

} // namespace

std::vector<lp::PlanBuilder::OutputColumnName> ColumnsExpansion::matchByRegex(
    const lp::PlanBuilder& builder,
    const std::string& pattern,
    const std::optional<std::string>& prefix,
    NodeLocation location) {
  re2::RE2 regex(pattern);
  AXIOM_PRESTO_SEMANTIC_CHECK(
      regex.ok(),
      location,
      pattern,
      "Invalid regex pattern: {}",
      regex.error());

  auto columns =
      builder.findOrAssignOutputNames(/*includeHiddenColumns=*/false, prefix);
  std::erase_if(columns, [&](const auto& column) {
    return !re2::RE2::FullMatch(column.name, regex);
  });
  AXIOM_PRESTO_SEMANTIC_CHECK(
      !columns.empty(),
      location,
      pattern,
      "COLUMNS('{}') matched no columns",
      pattern);

  return columns;
}

std::vector<lp::ExprApi> ColumnsExpansion::expand(
    const lp::ExprApi& expr,
    const lp::PlanBuilder& builder,
    NodeLocation location) {
  std::vector<std::pair<const core::IExpr*, std::string>> columnsCalls;
  findColumnsCalls(expr.expr(), columnsCalls);
  if (columnsCalls.empty()) {
    return {};
  }

  // Resolve matched columns for each COLUMNS() call.
  std::vector<std::vector<lp::PlanBuilder::OutputColumnName>>
      matchedColumnsPerCall;
  matchedColumnsPerCall.reserve(columnsCalls.size());
  for (const auto& [callNode, pattern] : columnsCalls) {
    matchedColumnsPerCall.push_back(
        matchByRegex(builder, pattern, /*prefix=*/std::nullopt, location));
  }

  // All COLUMNS() calls must match the same number of columns.
  const size_t numColumns = matchedColumnsPerCall[0].size();
  for (size_t i = 1; i < matchedColumnsPerCall.size(); ++i) {
    VELOX_USER_CHECK_EQ(
        matchedColumnsPerCall[i].size(),
        numColumns,
        "All COLUMNS() calls in a single expression must match "
        "the same number of columns. COLUMNS('{}') matched {} columns, "
        "but COLUMNS('{}') matched {} columns",
        columnsCalls[0].second,
        numColumns,
        columnsCalls[i].second,
        matchedColumnsPerCall[i].size());
  }

  // Expand pairwise: for each column index, replace all COLUMNS() calls
  // simultaneously.
  std::vector<lp::ExprApi> result;
  result.reserve(numColumns);
  for (size_t i = 0; i < numColumns; ++i) {
    std::unordered_map<const core::IExpr*, core::ExprPtr> replacements;
    for (size_t callIdx = 0; callIdx < columnsCalls.size(); ++callIdx) {
      replacements.emplace(
          columnsCalls[callIdx].first,
          matchedColumnsPerCall[callIdx][i].toCol().expr());
    }
    result.push_back(lp::ExprApi(replaceInputs(expr.expr(), replacements)));
  }

  return result;
}

} // namespace axiom::sql::presto
