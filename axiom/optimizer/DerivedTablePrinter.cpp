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

#include "axiom/optimizer/DerivedTablePrinter.h"
#include <sstream>
#include "axiom/optimizer/PlanUtils.h"

namespace facebook::axiom::optimizer {

namespace {

std::string columnNames(const ColumnVector& columns) {
  std::stringstream out;
  int32_t i = 0;
  for (auto column : columns) {
    if (i > 0) {
      out << ", ";
    }
    i++;
    out << column->name();
  }
  return out.str();
}

std::string exprsToString(const ExprVector& exprs) {
  std::stringstream out;
  int32_t i = 0;
  for (auto expr : exprs) {
    if (i > 0) {
      out << ", ";
    }
    i++;
    out << expr->toString();
  }
  return out.str();
}

std::string tableName(PlanObjectCP table) {
  switch (table->type()) {
    case PlanType::kTableNode:
      return table->as<BaseTable>()->cname;
    case PlanType::kValuesTableNode:
      return table->as<ValuesTable>()->cname;
    case PlanType::kUnnestTableNode:
      return table->as<UnnestTable>()->cname;
    case PlanType::kDerivedTableNode:
      return table->as<DerivedTable>()->cname;
    default:
      VELOX_FAIL();
  }
}

std::string visitBaseTable(const BaseTable& table) {
  std::stringstream out;
  out << table.cname << ": " << columnNames(table.columns) << std::endl;
  out << "  table: " << table.schemaTable->name << std::endl;
  if (!table.columnFilters.empty()) {
    out << "  single-column filters: " << conjunctsToString(table.columnFilters)
        << std::endl;
  }
  if (!table.filter.empty()) {
    out << "  multi-column filters: " << conjunctsToString(table.filter)
        << std::endl;
  }
  return out.str();
}

std::string visitValuesTable(const ValuesTable& values) {
  std::stringstream out;
  out << values.cname << ": " << columnNames(values.columns) << std::endl;
  return out.str();
}

std::string visitUnnestTable(const UnnestTable& unnest) {
  std::stringstream out;
  out << unnest.cname << ": " << columnNames(unnest.columns) << std::endl;
  return out.str();
}

std::string visitJoinEdge(const JoinEdge& edge) {
  std::stringstream out;
  if (edge.leftTable() != nullptr) {
    out << tableName(edge.leftTable());
  } else {
    out << "<multiple tables>";
  }

  if (edge.isSemi()) {
    out << " SEMI ";
  } else if (edge.isAnti()) {
    out << " ANTI ";
  } else if (edge.leftOptional() && edge.rightOptional()) {
    out << " FULL OUTER ";
  } else if (edge.leftOptional()) {
    out << " RIGHT ";
  } else if (edge.rightOptional()) {
    out << " LEFT ";
  } else if (edge.directed()) {
    out << " UNNEST ";
  } else {
    out << " INNER ";
  }

  out << tableName(edge.rightTable());

  out << " ON ";

  for (size_t i = 0; i < edge.leftKeys().size(); ++i) {
    if (i > 0) {
      out << " AND ";
    }
    out << edge.leftKeys()[i]->toString();
    if (i < edge.rightKeys().size()) {
      out << " = " << edge.rightKeys()[i]->toString();
    }
  }

  if (!edge.filter().empty()) {
    out << " FILTER " << conjunctsToString(edge.filter());
  }

  return out.str();
}

std::string visitDerivedTable(const DerivedTable& dt) {
  std::stringstream out;
  out << dt.cname << ": " << columnNames(dt.columns) << std::endl;

  if (dt.setOp.has_value()) {
    VELOX_CHECK_EQ(0, dt.exprs.size());
  } else {
    VELOX_CHECK_EQ(dt.columns.size(), dt.exprs.size());
  }

  out << "  output:" << std::endl;
  for (auto i = 0; i < dt.columns.size(); ++i) {
    if (dt.setOp.has_value()) {
      out << "    " << dt.columns.at(i)->name() << std::endl;
    } else {
      out << "    " << dt.columns.at(i)->name()
          << " := " << dt.exprs.at(i)->toString() << std::endl;
    }
  }

  if (!dt.tables.empty()) {
    out << "  tables: ";
    int32_t i = 0;
    for (const auto& table : dt.tables) {
      if (i > 0) {
        out << ", ";
      }
      i++;
      out << tableName(table);
    }
    out << std::endl;
  }

  if (!dt.joins.empty()) {
    out << "  joins:" << std::endl;
    for (const auto& joinEdge : dt.joins) {
      out << "    " << visitJoinEdge(*joinEdge) << std::endl;
    }
  }

  if (dt.hasAggregation()) {
    if (!dt.aggregation->aggregates().empty()) {
      const auto numGroupingKeys = dt.aggregation->groupingKeys().size();

      out << "  aggregates: ";
      int32_t i = 0;
      for (auto agg : dt.aggregation->aggregates()) {
        if (i > 0) {
          out << ", ";
        }

        out << agg->toString() << " AS "
            << dt.aggregation->columns().at(i + numGroupingKeys)->name();

        i++;
      }
      out << std::endl;
    }

    if (!dt.aggregation->groupingKeys().empty()) {
      out << "  grouping keys: "
          << exprsToString(dt.aggregation->groupingKeys()) << std::endl;
    }

    if (!dt.having.empty()) {
      out << "  having: " << conjunctsToString(dt.having) << std::endl;
    }
  }

  if (!dt.conjuncts.empty()) {
    out << "  filter: " << conjunctsToString(dt.conjuncts) << std::endl;
  }

  if (dt.hasOrderBy()) {
    out << "  orderBy: " << orderByToString(dt.orderKeys, dt.orderTypes)
        << std::endl;
  }

  if (dt.hasLimit()) {
    if (dt.offset > 0) {
      out << "  offset: " << dt.offset << std::endl;
    }
    out << "  limit: " << dt.limit << std::endl;
  }

  for (const auto& table : dt.tables) {
    out << std::endl;
    switch (table->type()) {
      case PlanType::kTableNode:
        out << visitBaseTable(*table->as<BaseTable>());
        break;
      case PlanType::kValuesTableNode:
        out << visitValuesTable(*table->as<ValuesTable>());
        break;
      case PlanType::kUnnestTableNode:
        out << visitUnnestTable(*table->as<UnnestTable>());
        break;
      case PlanType::kDerivedTableNode:
        out << visitDerivedTable(*table->as<DerivedTable>());
        break;
      default:
        VELOX_FAIL();
    }
  }

  return out.str();
}
} // namespace

// static
std::string DerivedTablePrinter::toText(const DerivedTable& root) {
  return visitDerivedTable(root);
}

} // namespace facebook::axiom::optimizer
