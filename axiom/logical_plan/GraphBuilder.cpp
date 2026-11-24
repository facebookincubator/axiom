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

#include "axiom/logical_plan/GraphBuilder.h"

#include <folly/container/F14Map.h>

namespace facebook::axiom::logical_plan {

GraphBuilder::GraphBuilder(
    const PlanBuilder::Context& context,
    std::string name)
    : context_{context}, name_{std::move(name)} {}

GraphBuilder& GraphBuilder::defineLabel(Label label) {
  for (auto i = 0; i < labels_.size(); ++i) {
    auto& existing = labels_[i];
    if (existing.name != label.name) {
      continue;
    }
    VELOX_USER_CHECK(
        existing.edge == label.edge && existing.properties == label.properties,
        "Graph vertex or edge is defined twice differently: {}",
        label.name);
    lastLabel_ = i;
    return *this;
  }
  lastLabel_ = labels_.size();
  labels_.push_back(std::move(label));
  return *this;
}

GraphBuilder& GraphBuilder::vertex(
    std::string name,
    std::vector<std::string> properties) {
  return defineLabel(
      {.name = std::move(name), .properties = std::move(properties)});
}

GraphBuilder& GraphBuilder::edge(
    std::string name,
    std::vector<std::string> properties) {
  return defineLabel({
      .name = std::move(name),
      .edge = true,
      .properties = std::move(properties),
  });
}

std::vector<ExprApi> GraphBuilder::parse(
    const std::vector<std::string>& sqls) const {
  std::vector<ExprApi> exprs;
  exprs.reserve(sqls.size());
  for (const auto& sql : sqls) {
    exprs.emplace_back(context_.sqlParser->parseScalarOrWindowExpr(sql));
  }
  return exprs;
}

GraphBuilder& GraphBuilder::backedBy(
    std::string table,
    std::vector<std::string> keyColumns) {
  return backedBy(std::move(table), std::move(keyColumns), {});
}

GraphBuilder& GraphBuilder::backedBy(
    std::string table,
    std::vector<std::string> keyColumns,
    const std::vector<std::string>& properties) {
  auto plan = PlanBuilder(context_).tableScan(table);
  return addBacking(
      std::move(table),
      std::move(plan),
      std::move(keyColumns),
      std::nullopt,
      std::nullopt,
      parse(properties));
}

GraphBuilder& GraphBuilder::backedBy(
    std::string table,
    std::string sourceTable,
    std::vector<std::string> sourceColumns,
    std::string destinationTable,
    std::vector<std::string> destinationColumns) {
  return backedBy(
      std::move(table),
      std::move(sourceTable),
      std::move(sourceColumns),
      std::move(destinationTable),
      std::move(destinationColumns),
      {});
}

GraphBuilder& GraphBuilder::backedBy(
    std::string table,
    std::string sourceTable,
    std::vector<std::string> sourceColumns,
    std::string destinationTable,
    std::vector<std::string> destinationColumns,
    const std::vector<std::string>& properties) {
  auto keyColumns = sourceColumns;
  keyColumns.insert(
      keyColumns.end(), destinationColumns.begin(), destinationColumns.end());
  auto plan = PlanBuilder(context_).tableScan(table);
  return addBacking(
      std::move(table),
      std::move(plan),
      std::move(keyColumns),
      Endpoint{
          .table = std::move(sourceTable), .columns = std::move(sourceColumns)},
      Endpoint{
          .table = std::move(destinationTable),
          .columns = std::move(destinationColumns)},
      parse(properties));
}

GraphBuilder& GraphBuilder::backedBy(
    std::string table,
    PlanBuilder plan,
    std::vector<std::string> keyColumns,
    std::vector<ExprApi> properties) {
  return addBacking(
      std::move(table),
      std::move(plan),
      std::move(keyColumns),
      std::nullopt,
      std::nullopt,
      std::move(properties));
}

GraphBuilder& GraphBuilder::backedBy(
    std::string table,
    PlanBuilder plan,
    std::vector<std::string> keyColumns,
    Endpoint source,
    Endpoint destination,
    std::vector<ExprApi> properties) {
  return addBacking(
      std::move(table),
      std::move(plan),
      std::move(keyColumns),
      std::move(source),
      std::move(destination),
      std::move(properties));
}

GraphBuilder& GraphBuilder::addBacking(
    std::string table,
    PlanBuilder plan,
    std::vector<std::string> keyColumns,
    std::optional<Endpoint> source,
    std::optional<Endpoint> destination,
    std::vector<ExprApi> properties) {
  VELOX_USER_CHECK(
      lastLabel_.has_value(),
      "Define a vertex or an edge before its tables: {}",
      table);
  auto& label = labels_[lastLabel_.value()];
  VELOX_USER_CHECK_EQ(
      label.edge,
      source.has_value(),
      "An edge table names its endpoints and a vertex table does not: {}",
      table);

  const auto index = findTable(table);
  if (index.has_value()) {
    const auto& existing = tables_[index.value()];
    VELOX_USER_CHECK(
        existing.keyColumns == keyColumns && existing.edge == label.edge &&
            existing.source == source && existing.destination == destination,
        "A table backs graph elements with different keys or endpoints: {}",
        table);
  } else {
    tables_.push_back({
        .name = table,
        .plan = std::move(plan),
        .keyColumns = std::move(keyColumns),
        .edge = label.edge,
        .source = std::move(source),
        .destination = std::move(destination),
    });
  }

  auto& backing = tables_[index.value_or(tables_.size() - 1)];
  for (auto& property : properties) {
    VELOX_USER_CHECK(
        property.alias().has_value(),
        "Graph property expression needs a property name: {}, {}",
        table,
        property.expr()->toString());
    auto name = property.alias().value();
    backing.properties.emplace_back(std::move(name), std::move(property));
  }
  label.tables.push_back(std::move(table));
  return *this;
}

namespace {

// Table plan with its key and endpoint columns and its properties resolved.
struct ResolvedTable {
  LogicalPlanNodePtr plan;

  // ID of each key or endpoint column, by name.
  folly::F14FastMap<std::string, std::string> columnIds;

  // Expression of each property, by name.
  std::map<std::string, ExprPtr> properties;
};

// Returns a reference to each output column of 'plan', by name, in the way
// 'plan' resolves columns.
folly::F14FastMap<std::string, ExprApi> columnsByName(const PlanBuilder& plan) {
  folly::F14FastMap<std::string, ExprApi> columns;
  for (const auto& column : plan.findOrAssignOutputNames()) {
    columns.emplace(column.outputName(), column.toCol());
  }
  return columns;
}

// Returns the reference 'references' has for the column 'column' of the
// element table 'table'. Throws a user error if the table has none.
const ExprApi& columnOf(
    const folly::F14FastMap<std::string, ExprApi>& references,
    std::string_view table,
    const std::string& column) {
  const auto it = references.find(column);
  VELOX_USER_CHECK(
      it != references.end(),
      "Graph element table has no column: {}.{}",
      table,
      column);
  return it->second;
}

// Projects 'plan' to 'properties', each aliased with its name, and to
// 'columns' that no property already provides under the same name. A column
// whose name a property uses for another value gets an internal name.
ResolvedTable resolveTable(
    std::string_view table,
    PlanBuilder plan,
    const std::vector<std::string>& columns,
    const std::vector<std::pair<std::string, ExprApi>>& properties) {
  std::vector<ExprApi> projections;
  folly::F14FastMap<std::string, size_t> propertyIndex;
  for (const auto& [name, expr] : properties) {
    propertyIndex.emplace(name, projections.size());
    projections.emplace_back(expr.expr(), name);
  }

  const auto references = columnsByName(plan);
  folly::F14FastMap<std::string, size_t> columnIndex;
  for (const auto& column : columns) {
    if (columnIndex.contains(column)) {
      continue;
    }
    const auto it = propertyIndex.find(column);
    const auto& reference = columnOf(references, table, column);
    if (it != propertyIndex.end() &&
        *properties[it->second].second.expr() == *reference.expr()) {
      columnIndex.emplace(column, it->second);
      continue;
    }
    columnIndex.emplace(column, projections.size());
    projections.push_back(
        it != propertyIndex.end() ? reference.as("__" + column) : reference);
  }
  plan.project(projections);

  ResolvedTable resolved{.plan = plan.planNode()};
  const auto& outputType = resolved.plan->outputType();
  for (const auto& [column, index] : columnIndex) {
    resolved.columnIds.emplace(column, outputType->nameOf(index));
  }
  for (auto i = 0; i < properties.size(); ++i) {
    resolved.properties.emplace(
        properties[i].first,
        std::make_shared<InputReferenceExpr>(
            outputType->childAt(i), outputType->nameOf(i)));
  }
  return resolved;
}

std::vector<std::string> columnIds(
    const ResolvedTable& table,
    const std::vector<std::string>& names) {
  std::vector<std::string> ids;
  ids.reserve(names.size());
  for (const auto& name : names) {
    ids.push_back(table.columnIds.at(name));
  }
  return ids;
}

} // namespace

std::optional<size_t> GraphBuilder::findTable(std::string_view name) const {
  for (auto i = 0; i < tables_.size(); ++i) {
    if (tables_[i].name == name) {
      return i;
    }
  }
  return std::nullopt;
}

std::vector<std::vector<std::pair<std::string, ExprApi>>>
GraphBuilder::propertiesByTable() const {
  // Each table's properties, with the expression a backing gave, if any.
  std::vector<std::vector<std::pair<std::string, std::optional<ExprApi>>>>
      declared(tables_.size());
  auto find = [](auto& properties, const std::string& name) {
    return std::find_if(
        properties.begin(), properties.end(), [&](const auto& entry) {
          return entry.first == name;
        });
  };

  for (const auto& label : labels_) {
    for (const auto& table : label.tables) {
      auto& properties = declared[findTable(table).value()];
      for (const auto& property : label.properties) {
        if (find(properties, property) == properties.end()) {
          properties.emplace_back(property, std::nullopt);
        }
      }
    }
  }

  for (size_t i = 0; i < tables_.size(); ++i) {
    auto& properties = declared[i];
    for (const auto& [name, expr] : tables_[i].properties) {
      const auto defined = find(properties, name);
      VELOX_USER_CHECK(
          defined != properties.end(),
          "Graph element table gives an expression to an undeclared "
          "property: {}.{}",
          tables_[i].name,
          name);
      VELOX_USER_CHECK(
          !defined->second.has_value() ||
              *defined->second->expr() == *expr.expr(),
          "Graph element table gives a property two expressions: {}.{}",
          tables_[i].name,
          name);
      defined->second = expr;
    }
  }

  // A property no backing gave an expression is the column of its name.
  std::vector<std::vector<std::pair<std::string, ExprApi>>> result(
      tables_.size());
  for (size_t i = 0; i < tables_.size(); ++i) {
    const auto references = columnsByName(tables_[i].plan);
    for (const auto& [name, expr] : declared[i]) {
      result[i].emplace_back(
          name,
          expr.has_value() ? *expr
                           : columnOf(references, tables_[i].name, name));
    }
  }
  return result;
}

std::pair<std::vector<std::string>, size_t> GraphBuilder::resolveEndpoint(
    const Table& edge,
    const Endpoint& endpoint) const {
  const auto index = findTable(endpoint.table);
  VELOX_USER_CHECK(
      index.has_value() && !tables_[index.value()].edge,
      "Edge table references an unknown vertex table: {}, {}",
      edge.name,
      endpoint.table);
  const auto& key = tables_[index.value()].keyColumns;
  const auto& referenced =
      endpoint.referencedColumns.empty() ? key : endpoint.referencedColumns;
  VELOX_USER_CHECK(
      endpoint.columns.size() == key.size() &&
          referenced.size() == key.size() &&
          std::is_permutation(key.begin(), key.end(), referenced.begin()),
      "Edge endpoint must reference the key of its vertex table: {}, {}",
      edge.name,
      endpoint.table);
  std::vector<std::string> columns;
  for (const auto& keyColumn : key) {
    const auto position =
        std::find(referenced.begin(), referenced.end(), keyColumn) -
        referenced.begin();
    columns.push_back(endpoint.columns[position]);
  }
  return {std::move(columns), index.value()};
}

PropertyGraphPtr GraphBuilder::build() {
  const auto tableProperties = propertiesByTable();

  std::vector<ResolvedTable> resolved;
  resolved.reserve(tables_.size());
  std::vector<PropertyGraph::VertexTable> vertexTables;
  std::vector<PropertyGraph::EdgeTable> edgeTables;
  for (auto i = 0; i < tables_.size(); ++i) {
    const auto& table = tables_[i];
    std::vector<std::string> columns = table.keyColumns;
    std::vector<std::string> sourceColumns;
    std::vector<std::string> destinationColumns;
    size_t sourceTable{0};
    size_t destinationTable{0};
    if (table.edge) {
      std::tie(sourceColumns, sourceTable) =
          resolveEndpoint(table, table.source.value());
      std::tie(destinationColumns, destinationTable) =
          resolveEndpoint(table, table.destination.value());
      columns.insert(columns.end(), sourceColumns.begin(), sourceColumns.end());
      columns.insert(
          columns.end(), destinationColumns.begin(), destinationColumns.end());
    }

    resolved.push_back(
        resolveTable(table.name, table.plan, columns, tableProperties[i]));
    PropertyGraph::ElementTable element{
        .id = static_cast<int32_t>(i),
        .plan = resolved[i].plan,
        .keyColumns = columnIds(resolved[i], table.keyColumns),
        .properties = resolved[i].properties,
    };
    if (table.edge) {
      edgeTables.push_back({
          std::move(element),
          columnIds(resolved[i], sourceColumns),
          static_cast<int32_t>(sourceTable),
          columnIds(resolved[i], destinationColumns),
          static_cast<int32_t>(destinationTable),
      });
    } else {
      vertexTables.push_back({std::move(element)});
    }
  }

  std::vector<PropertyGraph::Label> vertices;
  std::vector<PropertyGraph::Label> edges;
  for (const auto& label : labels_) {
    VELOX_USER_CHECK(
        !label.tables.empty(),
        "Graph vertex or edge is not backed by any table: {}",
        label.name);
    std::set<int32_t> tableIds;
    for (const auto& name : label.tables) {
      tableIds.insert(static_cast<int32_t>(findTable(name).value()));
    }
    const auto& first = resolved[findTable(label.tables[0]).value()];
    std::vector<velox::TypePtr> types;
    for (const auto& property : label.properties) {
      types.push_back(first.properties.at(property)->type());
    }
    (label.edge ? edges : vertices)
        .push_back({
            .name = label.name,
            .properties = velox::ROW(label.properties, std::move(types)),
            .tableIds = std::move(tableIds),
        });
  }

  return std::make_shared<PropertyGraph>(
      name_,
      std::move(vertices),
      std::move(edges),
      std::move(vertexTables),
      std::move(edgeTables));
}

} // namespace facebook::axiom::logical_plan
