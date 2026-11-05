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
#include "axiom/sql/presto/VectorSearchPlanner.h"

#include <algorithm>

#include <folly/Conv.h>
#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>

#include "axiom/sql/presto/ExpressionPlanner.h"
#include "axiom/sql/presto/PrestoSqlError.h"
#include "velox/common/vectorindex/LocalVectorIndexOptions.h"
#include "velox/common/vectorindex/VectorIndexMetric.h"

namespace axiom::sql::presto {

namespace lp = facebook::axiom::logical_plan;
using facebook::velox::ARRAY;
using facebook::velox::REAL;

namespace {

// Returns 'plan' with its visible columns, and the position of 'column', an
// ARRAY(REAL) column. 'argument', named 'name', is what 'plan' plans;
// 'embeddings' is the argument that names 'column'.
std::pair<lp::PlanBuilder, size_t> withEmbedding(
    lp::PlanBuilder plan,
    const TableFunctionArgument& argument,
    std::string_view name,
    const TableFunctionArgument& embeddings,
    std::string_view column) {
  const auto columns = plan.findOrAssignOutputNames();
  std::vector<lp::ExprApi> projections;
  projections.reserve(columns.size());
  for (const auto& outputColumn : columns) {
    projections.push_back(outputColumn.toCol());
  }
  plan.project(projections);

  const auto it =
      std::find_if(columns.begin(), columns.end(), [&](const auto& candidate) {
        return candidate.outputName() == column;
      });
  AXIOM_PRESTO_SEMANTIC_CHECK(
      it != columns.end(),
      embeddings.location(),
      "embeddings",
      "vector_search column not found: {}.{}",
      name,
      column);
  const auto position = static_cast<size_t>(it - columns.begin());
  AXIOM_PRESTO_SEMANTIC_CHECK(
      plan.outputTypes()[position]->equivalent(*ARRAY(REAL())),
      argument.location(),
      std::string(name),
      "vector_search column must be ARRAY(REAL): {}.{}",
      name,
      column);
  return {std::move(plan), position};
}

// Returns the hints of the 'hints' argument, a MAP(ARRAY[...], ARRAY[...]) of
// VARCHAR literals: index, the name of the index to search, num_probes, a
// positive integer, and the options of the index's local indexes.
std::vector<std::pair<std::string, std::string>> searchHints(
    const TableFunctionArgument& argument) {
  const auto stringLiterals = [](const ExpressionPtr& expression)
      -> std::optional<std::vector<std::string>> {
    const auto* array = expression->as<ArrayConstructor>();
    if (array == nullptr) {
      return std::nullopt;
    }
    std::vector<std::string> values;
    for (const auto& value : array->values()) {
      const auto* literal = value->as<StringLiteral>();
      if (literal == nullptr) {
        return std::nullopt;
      }
      values.push_back(literal->value());
    }
    return values;
  };

  const auto checkMap = [&](bool condition) {
    AXIOM_PRESTO_SEMANTIC_CHECK(
        condition,
        argument.location(),
        "hints",
        "vector_search argument hints must be MAP(ARRAY[...], ARRAY[...]) of VARCHAR literals");
  };
  checkMap(argument.expression() != nullptr);
  const auto* map = argument.expression()->as<FunctionCall>();
  checkMap(map != nullptr);
  checkMap(canonicalizeName(map->name()->suffix()) == "map");
  checkMap(map->arguments().size() == 2);
  const auto keys = stringLiterals(map->arguments()[0]);
  checkMap(keys.has_value());
  const auto values = stringLiterals(map->arguments()[1]);
  checkMap(values.has_value());
  AXIOM_PRESTO_SEMANTIC_CHECK(
      keys->size() == values->size(),
      argument.location(),
      "hints",
      "vector_search argument hints must have one value per name");

  std::vector<std::pair<std::string, std::string>> hints;
  std::vector<std::pair<std::string_view, std::string_view>> localOptions;
  hints.reserve(keys->size());
  for (size_t i = 0; i < keys->size(); ++i) {
    hints.emplace_back((*keys)[i], (*values)[i]);
    const auto& [name, value] = hints.back();
    if (lp::VectorSearchNode::isLocalIndexOption(name)) {
      localOptions.emplace_back(name, value);
      continue;
    }
    if (name == lp::VectorSearchNode::kIndex) {
      continue;
    }
    const auto checkNumProbes = [&](bool condition) {
      AXIOM_PRESTO_SEMANTIC_CHECK(
          condition,
          argument.location(),
          "hints",
          "vector_search hint num_probes must be a positive integer: {}",
          value);
    };
    const auto numProbes = folly::tryTo<int64_t>(value);
    checkNumProbes(numProbes.hasValue());
    checkNumProbes(numProbes.value() > 0);
  }
  facebook::velox::common::vectorindex::LocalVectorIndexOptions::parseSearch(
      localOptions);
  return hints;
}

} // namespace

// static
lp::PlanBuilder VectorSearchPlanner::plan(
    const TableFunctionInvocation& call,
    const TablePlanner& planTable,
    const RowPlanner& planRow) {
  static const folly::F14FastSet<std::string> kNames{
      "queries",
      "query",
      "corpus",
      "embeddings",
      "metric",
      "k",
      "score",
      "hints"};
  folly::F14FastMap<std::string, const TableFunctionArgument*> arguments;
  for (const auto& argument : call.arguments()) {
    AXIOM_PRESTO_SEMANTIC_CHECK(
        argument->name() != nullptr,
        argument->location(),
        "vector_search",
        "vector_search arguments must be named");
    const auto name = canonicalizeName(argument->name()->value());
    AXIOM_PRESTO_SEMANTIC_CHECK(
        kNames.contains(name),
        argument->location(),
        name,
        "Unknown vector_search argument: {}",
        name);
    AXIOM_PRESTO_SEMANTIC_CHECK(
        arguments.emplace(name, argument.get()).second,
        argument->location(),
        name,
        "Duplicate vector_search argument: {}",
        name);
  }

  const auto find =
      [&](const std::string& name) -> const TableFunctionArgument* {
    const auto it = arguments.find(name);
    return it == arguments.end() ? nullptr : it->second;
  };
  const auto require = [&](const std::string& name) {
    const auto* argument = find(name);
    AXIOM_PRESTO_SEMANTIC_CHECK(
        argument != nullptr,
        call.location(),
        "vector_search",
        "Missing vector_search argument: {}",
        name);
    return argument;
  };
  const auto positiveInteger = [&](const TableFunctionArgument* argument,
                                   const std::string& name) {
    const auto check = [&](bool condition) {
      AXIOM_PRESTO_SEMANTIC_CHECK(
          condition,
          argument->location(),
          name,
          "vector_search argument must be a positive integer: {}",
          name);
    };
    const auto& value = argument->expression();
    check(value != nullptr);
    check(value->is(NodeType::kLongLiteral));
    const auto integer = value->as<LongLiteral>()->value();
    check(integer > 0);
    return integer;
  };
  const auto descriptorColumn = [&](const TableFunctionArgument& argument,
                                    const std::string& name) {
    AXIOM_PRESTO_SEMANTIC_CHECK(
        argument.descriptor().size() == 1,
        argument.location(),
        name,
        "vector_search argument must be DESCRIPTOR(column): {}",
        name);
    return canonicalizeIdentifier(*argument.descriptor().front());
  };
  const auto table = [&](const TableFunctionArgument& argument,
                         const std::string& name,
                         const TableFunctionArgument& embeddings,
                         std::string_view column) {
    AXIOM_PRESTO_SEMANTIC_CHECK(
        argument.table() != nullptr,
        argument.location(),
        name,
        "vector_search argument must be a table: {}",
        name);
    return withEmbedding(
        planTable(argument.table()), argument, name, embeddings, column);
  };

  const auto k = positiveInteger(require("k"), "k");
  std::vector<std::pair<std::string, std::string>> hints;
  if (const auto* argument = find("hints")) {
    hints = searchHints(*argument);
  }

  const auto* queriesArgument = find("queries");
  const auto* queryArgument = find("query");
  AXIOM_PRESTO_SEMANTIC_CHECK(
      (queriesArgument == nullptr) != (queryArgument == nullptr),
      call.location(),
      "vector_search",
      "vector_search takes one of queries and query");
  if (queryArgument != nullptr) {
    AXIOM_PRESTO_SEMANTIC_CHECK(
        queryArgument->expression() != nullptr,
        queryArgument->location(),
        "query",
        "vector_search argument query must be an expression");
  }

  // 'embeddings' names the queries' embedding column, then the corpus's. One
  // name names both, or only the corpus's when the query is an expression.
  const auto& embeddings = *require("embeddings");
  if (queriesArgument != nullptr) {
    const auto checkEmbeddings = [&](bool condition) {
      AXIOM_PRESTO_SEMANTIC_CHECK(
          condition,
          embeddings.location(),
          "embeddings",
          "vector_search argument embeddings must be DESCRIPTOR(column) or DESCRIPTOR(query column, corpus column)");
    };
    checkEmbeddings(!embeddings.descriptor().empty());
    checkEmbeddings(embeddings.descriptor().size() <= 2);
  } else {
    AXIOM_PRESTO_SEMANTIC_CHECK(
        embeddings.descriptor().size() == 1,
        embeddings.location(),
        "embeddings",
        "vector_search argument embeddings must be DESCRIPTOR(column) with query");
  }
  const auto queryColumn =
      canonicalizeIdentifier(*embeddings.descriptor().front());
  const auto corpusColumn =
      canonicalizeIdentifier(*embeddings.descriptor().back());

  const auto& metricArgument = *require("metric");
  const auto* metricName = metricArgument.expression() != nullptr
      ? metricArgument.expression()->as<StringLiteral>()
      : nullptr;
  AXIOM_PRESTO_SEMANTIC_CHECK(
      metricName != nullptr,
      metricArgument.location(),
      "metric",
      "vector_search argument metric must be a VARCHAR literal");
  const auto metric = facebook::velox::common::vectorindex::
      VectorIndexMetricName::tryToVectorIndexMetric(metricName->value());
  AXIOM_PRESTO_SEMANTIC_CHECK(
      metric.has_value(),
      metricArgument.location(),
      "metric",
      "Unknown vector_search metric: {}",
      metricName->value());

  std::optional<std::string> scoreName;
  if (const auto* scoreArgument = find("score")) {
    scoreName = descriptorColumn(*scoreArgument, "score");
  }
  const auto queryRow = [&]() {
    auto row = planRow(queryArgument->expression(), "embedding");
    AXIOM_PRESTO_SEMANTIC_CHECK(
        row.outputTypes().front()->equivalent(*ARRAY(REAL())),
        queryArgument->location(),
        "query",
        "vector_search argument query must be ARRAY(REAL)");
    return withEmbedding(
        std::move(row), *queryArgument, "query", embeddings, "embedding");
  };
  auto [queries, queryEmbedding] = queriesArgument != nullptr
      ? table(*queriesArgument, "queries", embeddings, queryColumn)
      : queryRow();
  auto [corpus, corpusEmbedding] =
      table(*require("corpus"), "corpus", embeddings, corpusColumn);

  const auto queryColumns = queries.findOrAssignOutputNames();
  const auto corpusColumns = corpus.findOrAssignOutputNames();
  queries.vectorSearch(
      corpus,
      /*condition=*/std::nullopt,
      lp::JoinType::kInner,
      {
          .probeEmbedding = queryColumns[queryEmbedding].name,
          .buildEmbedding = corpusColumns[corpusEmbedding].name,
          .metric = *metric,
          .k = k,
          .scoreName = scoreName,
          .hints = std::move(hints),
      });

  // The score comes first.
  const auto numSearched = queryColumns.size() + corpusColumns.size();
  std::vector<size_t> positions;
  positions.reserve(numSearched + 1);
  if (scoreName.has_value()) {
    positions.push_back(numSearched);
  }
  for (size_t i = 0; i < numSearched; ++i) {
    positions.push_back(i);
  }
  queries.keepColumns(positions);
  return queries;
}

} // namespace axiom::sql::presto
