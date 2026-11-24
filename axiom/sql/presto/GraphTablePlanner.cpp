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

#include "axiom/sql/presto/GraphTablePlanner.h"

#include "axiom/logical_plan/GraphBuilder.h"

namespace axiom::sql::presto {

namespace {

std::string nameOf(const IdentifierPtr& identifier) {
  return identifier != nullptr ? canonicalizeIdentifier(*identifier) : "";
}

std::vector<std::string> namesOf(
    const std::vector<IdentifierPtr>& identifiers) {
  std::vector<std::string> names;
  names.reserve(identifiers.size());
  for (const auto& identifier : identifiers) {
    names.push_back(canonicalizeIdentifier(*identifier));
  }
  return names;
}

std::string tableName(const GraphElementTable& table) {
  return table.alias() != nullptr ? canonicalizeIdentifier(*table.alias())
                                  : canonicalizeName(table.table()->suffix());
}

lp::GraphBuilder::Endpoint toEndpoint(const EdgeEndpoint& endpoint) {
  return {
      .table = nameOf(endpoint.vertexTable),
      .columns = namesOf(endpoint.keyColumns),
      .referencedColumns = namesOf(endpoint.referencedColumns),
  };
}

// Returns the name of the column 'expr' reads. The AST builder requires a
// property that is not a column to have a name.
std::string columnName(const lp::ExprApi& expr) {
  VELOX_CHECK(
      facebook::velox::core::FieldAccessExpr::tryAsRootColumn(expr.expr()) !=
              nullptr &&
          expr.name().has_value(),
      "Graph property needs a name");
  return expr.name().value();
}

} // namespace

lp::PropertyGraphPtr GraphTablePlanner::buildGraph(
    const WithGraph& graph,
    const lp::PlanBuilder::Context& context,
    const TablePlanner& planTable) {
  lp::GraphBuilder builder(context, nameOf(graph.name()));

  auto addTable = [&](const GraphElementTable& table) {
    const auto name = tableName(table);
    const auto keyColumns = namesOf(table.keyColumns());

    std::vector<GraphLabel> labels = table.labels();
    if (labels.empty()) {
      labels.push_back({});
    }

    // Each label's property expressions, which read the table's columns.
    std::vector<std::vector<lp::ExprApi>> labelExpressions(labels.size());
    auto plan = planTable(table, [&]() {
      for (size_t i = 0; i < labels.size(); ++i) {
        if (labels[i].properties.has_value()) {
          for (const auto& property : labels[i].properties.value()) {
            labelExpressions[i].push_back(
                exprPlanner_.toExpr(property.expression));
          }
        }
      }
    });

    for (size_t i = 0; i < labels.size(); ++i) {
      const auto& label = labels[i];
      auto labelName = label.name != nullptr ? nameOf(label.name) : name;

      // A label without a property list exposes every column.
      std::vector<std::string> propertyNames;
      std::vector<lp::ExprApi> expressions;
      if (label.properties.has_value()) {
        for (size_t j = 0; j < label.properties->size(); ++j) {
          const auto& property = label.properties.value()[j];
          const auto& expr = labelExpressions[i][j];
          auto propertyName = property.name != nullptr ? nameOf(property.name)
                                                       : columnName(expr);
          propertyNames.push_back(propertyName);
          expressions.emplace_back(expr.expr(), propertyName);
        }
      } else {
        for (const auto& column : plan.outputNames(false)) {
          if (column.has_value()) {
            propertyNames.push_back(column.value());
          }
        }
      }

      if (table.isEdge()) {
        builder.edge(std::move(labelName), std::move(propertyNames))
            .backedBy(
                name,
                plan,
                keyColumns,
                toEndpoint(table.source().value()),
                toEndpoint(table.destination().value()),
                std::move(expressions));
      } else {
        builder.vertex(std::move(labelName), std::move(propertyNames))
            .backedBy(name, plan, keyColumns, std::move(expressions));
      }
    }
  };

  for (const auto& table : graph.vertexTables()) {
    addTable(*table);
  }
  for (const auto& table : graph.edgeTables()) {
    addTable(*table);
  }
  return builder.build();
}

namespace {

lp::GraphLabelExpressionPtr toLabel(const LabelExpressionPtr& label) {
  if (label == nullptr) {
    return nullptr;
  }
  std::vector<lp::GraphLabelExpressionPtr> operands;
  for (const auto& operand : label->operands()) {
    operands.push_back(toLabel(operand));
  }
  switch (label->kind()) {
    case LabelExpression::Kind::kName:
      return std::make_shared<lp::GraphLabelName>(nameOf(label->name()));
    case LabelExpression::Kind::kWildcard:
      return std::make_shared<lp::GraphLabelWildcard>();
    case LabelExpression::Kind::kNegation:
      return std::make_shared<lp::GraphLabelNegation>(operands.at(0));
    case LabelExpression::Kind::kConjunction:
      return std::make_shared<lp::GraphLabelConjunction>(std::move(operands));
    case LabelExpression::Kind::kDisjunction:
      return std::make_shared<lp::GraphLabelDisjunction>(std::move(operands));
  }
  VELOX_UNREACHABLE();
}

lp::GraphPathMode toMode(const std::optional<PathMode>& mode) {
  if (!mode.has_value()) {
    return lp::GraphPathMode::kWalk;
  }
  switch (mode.value()) {
    case PathMode::kWalk:
      return lp::GraphPathMode::kWalk;
    case PathMode::kTrail:
      return lp::GraphPathMode::kTrail;
    case PathMode::kAcyclic:
      return lp::GraphPathMode::kAcyclic;
    case PathMode::kSimple:
      return lp::GraphPathMode::kSimple;
  }
  VELOX_UNREACHABLE();
}

lp::GraphPathSelector toSelector(const std::optional<PathSelector>& selector) {
  if (!selector.has_value()) {
    return lp::GraphPathSelector{};
  }
  using Kind = lp::GraphPathSelector::Kind;
  Kind kind{Kind::kAll};
  switch (selector->kind) {
    case PathSelector::Kind::kAll:
      kind = Kind::kAll;
      break;
    case PathSelector::Kind::kAny:
      kind = Kind::kAny;
      break;
    case PathSelector::Kind::kAnyShortest:
      kind = Kind::kAnyShortest;
      break;
    case PathSelector::Kind::kAllShortest:
      kind = Kind::kAllShortest;
      break;
    case PathSelector::Kind::kShortest:
      kind = Kind::kShortest;
      break;
    case PathSelector::Kind::kShortestGroups:
      kind = Kind::kShortestGroups;
      break;
  }
  return lp::GraphPathSelector{.kind = kind, .count = selector->count};
}

} // namespace

std::optional<lp::ExprApi> GraphTablePlanner::toOptionalExpr(
    const ExpressionPtr& expr) {
  if (expr == nullptr) {
    return std::nullopt;
  }
  return exprPlanner_.toExpr(expr);
}

namespace {

void addRepetition(
    const std::optional<GraphQuantifier>& quantifier,
    lp::PlanBuilder::GraphMatchBuilder& match) {
  if (quantifier.has_value()) {
    match.repeats(quantifier->lower, quantifier->upper);
  }
}

} // namespace

void GraphTablePlanner::addElements(
    const std::vector<PathElementPtr>& elements,
    lp::PlanBuilder::GraphMatchBuilder& match) {
  for (const auto& element : elements) {
    switch (element->type()) {
      case NodeType::kNodePattern: {
        const auto* vertex = element->as<NodePattern>();
        match.vertex(
            nameOf(vertex->variable()),
            toLabel(vertex->label()),
            toOptionalExpr(vertex->where()));
        break;
      }
      case NodeType::kEdgePattern: {
        const auto* edge = element->as<EdgePattern>();
        auto name = nameOf(edge->variable());
        auto label = toLabel(edge->label());
        auto predicate = toOptionalExpr(edge->where());
        switch (edge->direction()) {
          case EdgePattern::Direction::kRight:
            match.rightEdge(std::move(name), std::move(label), predicate);
            break;
          case EdgePattern::Direction::kLeft:
            match.leftEdge(std::move(name), std::move(label), predicate);
            break;
          case EdgePattern::Direction::kAnyDirection:
            match.anyEdge(std::move(name), std::move(label), predicate);
            break;
        }
        addRepetition(edge->quantifier(), match);
        break;
      }
      case NodeType::kParenthesizedPathPattern: {
        const auto* subpath = element->as<ParenthesizedPathPattern>();
        match.startSubpath(
            nameOf(subpath->variable()), toMode(subpath->mode()));
        addElements(subpath->elements(), match);
        match.endSubpath(toOptionalExpr(subpath->where()));
        addRepetition(subpath->quantifier(), match);
        if (subpath->isOptional()) {
          match.optional();
        }
        break;
      }
      default:
        VELOX_UNREACHABLE(
            "Unexpected path element: {}",
            NodeTypeName::toName(element->type()));
    }
  }
}

void GraphTablePlanner::planGraphTable(
    const GraphTable& graphTable,
    lp::PropertyGraphPtr graph,
    lp::PlanBuilder& builder) {
  auto match = builder.startGraphMatch(std::move(graph));
  for (const auto& path : graphTable.paths()) {
    match.path(
        nameOf(path->variable()),
        toSelector(path->selector()),
        toMode(path->mode()));
    addElements(path->elements(), match);
  }
  if (const auto& where = graphTable.where()) {
    match.filter(exprPlanner_.toExpr(where));
  }
  std::vector<lp::ExprApi> columns;
  for (const auto& column : graphTable.columns()) {
    auto expr = exprPlanner_.toExpr(column->expression());
    columns.emplace_back(
        expr.expr(),
        column->alias() != nullptr ? std::optional(nameOf(column->alias()))
                                   : expr.alias());
  }
  match.project(columns);
  match.endGraphMatch();
}

} // namespace axiom::sql::presto
