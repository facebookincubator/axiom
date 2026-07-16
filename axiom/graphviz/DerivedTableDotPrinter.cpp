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

#include "axiom/graphviz/DerivedTableDotPrinter.h"

#include "axiom/graphviz/Html.h"
#include "axiom/graphviz/Palette.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/Schema.h"

namespace facebook::axiom::graphviz {

using namespace optimizer; // NOLINT(google-build-using-namespace)

namespace {

// Maximum length for filter expressions in the visualization.
// Longer expressions are truncated with "...".
constexpr size_t kMaxFilterLength = 80;

std::string truncate(
    const std::string& s,
    size_t maxLength = kMaxFilterLength) {
  if (s.size() <= maxLength) {
    return s;
  }
  return s.substr(0, maxLength - 3) + "...";
}

std::string nodeId(PlanObjectCP table) {
  return fmt::format("node_{}", table->id());
}

// Starts a table node with the given table and header text.
void printTableStart(
    std::ostream& out,
    PlanObjectCP table,
    const std::string& header) {
  out << "  " << nodeId(table) << " [shape=none, margin=0, label=<\n";
  out << "    <TABLE BORDER=\"1\" CELLBORDER=\"0\" CELLSPACING=\"0\" "
         "CELLPADDING=\"4\" COLOR=\""
      << kPalette.text << "\">\n";
  out << "      <TR><TD BGCOLOR=\"" << kPalette.header << "\"><B>"
      << escapeHtml(header) << "</B></TD></TR>\n";
}

// Ends a table node.
void printTableEnd(std::ostream& out) {
  out << "    </TABLE>\n";
  out << "  >];\n";
}

// Prints a table row with left-aligned text.
void printRow(std::ostream& out, const std::string& text) {
  out << "        <TR><TD ALIGN=\"LEFT\">" << text << "</TD></TR>\n";
}

// Prints a table row with highlighted background.
void printHighlightedRow(std::ostream& out, const std::string& text) {
  out << "        <TR><TD BGCOLOR=\"" << kPalette.highlight
      << "\" ALIGN=\"LEFT\">" << text << "</TD></TR>\n";
}

// Prints a labeled section with items. If there's only one item, prints it
// inline (e.g., "GROUP BY: expr"). Otherwise, prints a header followed by
// indented items.
//
// The formatItem function receives (0-based index, item) allowing callers
// to access related arrays or include numbering (index + 1) if needed.
//
// Example with one item:   "GROUP BY: col1"
// Example with multiple:   "GROUP BY:"
//                          "    col1"
//                          "    col2"
// Example with numbering:  "JOINS:"
//                          "    1. condition1"
//                          "    2. condition2"
template <typename TContainer, typename TFormatOne>
void printSection(
    std::ostream& out,
    const std::string& label,
    const TContainer& items,
    const TFormatOne& formatItem) {
  if (items.empty()) {
    return;
  }

  if (items.size() == 1) {
    printHighlightedRow(out, label + ": " + formatItem(0, items[0]));
  } else {
    printHighlightedRow(out, label + ":");
    for (size_t i = 0; i < items.size(); ++i) {
      printHighlightedRow(out, "    " + formatItem(i, items[i]));
    }
  }
}

std::string joinTypeLabel(const JoinEdge& edge) {
  if (edge.isSemi()) {
    return "SEMI";
  } else if (edge.isAnti()) {
    return "ANTI";
  } else if (edge.leftOptional() && edge.rightOptional()) {
    return "FULL";
  } else if (edge.leftOptional()) {
    return "RIGHT";
  } else if (edge.rightOptional()) {
    return "LEFT";
  } else if (edge.directed()) {
    return "UNNEST";
  } else {
    return "INNER";
  }
}

std::string joinCondition(const JoinEdge& edge) {
  std::string keys;
  for (size_t i = 0; i < edge.leftKeys().size(); ++i) {
    if (i > 0) {
      keys += ", ";
    }
    keys += escapeHtml(edge.leftKeys()[i]->toString());
    if (i < edge.rightKeys().size()) {
      keys += " = " + escapeHtml(edge.rightKeys()[i]->toString());
    }
  }
  return keys;
}

void printBaseTable(const BaseTable& table, std::ostream& out) {
  std::string headerText = table.cname;
  if (table.schemaTable != nullptr) {
    headerText += fmt::format(" ({})", table.schemaTable->name());
  }

  printTableStart(out, &table, headerText);

  for (auto* col : table.columns) {
    printRow(out, escapeHtml(col->name()));
  }

  for (const auto& expr : table.columnFilters) {
    printHighlightedRow(out, escapeHtml(truncate(expr->toString())));
  }

  for (const auto& expr : table.filter) {
    printHighlightedRow(out, escapeHtml(truncate(expr->toString())));
  }

  printTableEnd(out);
}

void printValuesTable(const ValuesTable& table, std::ostream& out) {
  printTableStart(out, &table, std::string(table.cname) + " (VALUES)");

  for (auto* col : table.columns) {
    printRow(out, escapeHtml(col->name()));
  }

  // Show cardinality.
  printHighlightedRow(
      out,
      fmt::format("{} rows", static_cast<int>(table.cardinality().value())));

  printTableEnd(out);
}

void printUnnestTable(const UnnestTable& table, std::ostream& out) {
  printTableStart(out, &table, std::string(table.cname) + " (UNNEST)");

  for (auto* col : table.columns) {
    if (col == table.ordinalityColumn) {
      continue;
    }
    printRow(out, escapeHtml(col->name()));
  }

  if (table.ordinalityColumn != nullptr) {
    printRow(out, escapeHtml(table.ordinalityColumn->name()) + " (ordinality)");
  }

  printTableEnd(out);
}

// Prints one table ID node inside a DerivedTable cluster: a small shape
// representing a child table participating in joins or union legs.
// - Circles for base tables.
// - Rounded squares for nested DerivedTables (including union legs).
void printTableIdNode(
    const DerivedTable& parent,
    PlanObjectCP table,
    std::ostream& out) {
  out << "    dt" << parent.id() << "_" << cname(table) << " [label=\""
      << cname(table) << "\", ";
  if (table->type() == PlanType::kDerivedTableNode) {
    out << "shape=rect, style=\"filled,rounded\", width=0.4, height=0.4, ";
  } else {
    out << "shape=circle, width=0.4, fixedsize=true, style=filled, ";
  }
  out << "fillcolor=\"" << kPalette.circles << "\", color=\"" << kPalette.text
      << "\"];\n";
}

// Prints an invisible edge from the cluster header to the middle table ID
// node to keep the nodes positioned below the header.
void printHeaderAnchor(
    const DerivedTable& parent,
    const PlanObjectVector& children,
    std::ostream& out) {
  if (children.empty()) {
    return;
  }
  out << "    dt" << parent.id() << "_header -> dt" << parent.id() << "_"
      << cname(children[children.size() / 2]) << " [style=invis];\n\n";
}

// Arranges 'children' as horizontally aligned table ID nodes inside the
// 'parent' cluster.
void printTableIdRow(
    const DerivedTable& parent,
    const PlanObjectVector& children,
    std::ostream& out) {
  out << "    {rank=same;";
  for (auto* child : children) {
    out << " dt" << parent.id() << "_" << cname(child) << ";";
  }
  out << "}\n\n";

  for (auto* child : children) {
    printTableIdNode(parent, child, out);
  }
  out << "\n";
}

// Prints table ID nodes and join edges inside a regular DerivedTable
// cluster. Join edges connect table ID nodes with ordinal numbers matching
// the JOINS list in the header.
void printJoinEdges(const DerivedTable& dt, std::ostream& out) {
  printTableIdRow(dt, dt.tables, out);
  printHeaderAnchor(dt, dt.tables, out);

  int joinNum = 1;
  for (auto* join : dt.joins) {
    if (join->leftTable() == nullptr) {
      // Skip hyper edges.
      continue;
    }
    const bool commutative = !join->isNonCommutative();

    out << "    dt" << dt.id() << "_" << cname(join->leftTable()) << " -> dt"
        << dt.id() << "_" << cname(join->rightTable());
    out << " [";
    if (commutative) {
      out << "dir=none, ";
    }
    out << "color=\"" << kPalette.lines << "\", penwidth=2, xlabel=\""
        << joinNum << "\", fontcolor=\"" << kPalette.text
        << "\", fontsize=9];\n";
    ++joinNum;
  }
}

// Emits the shared header row + cluster preamble (subgraph, styling,
// header cell). Returns after the "<B>..." title cell so the caller can
// append set-specific text before closing the title.
void printClusterHeaderStart(
    const DerivedTable& dt,
    bool isRoot,
    size_t numChildren,
    std::ostream& out) {
  out << "  // DerivedTable cluster for " << dt.cname << "\n";
  out << "  subgraph cluster_" << dt.id() << " {\n";
  out << "    label=\"\";\n";
  out << "    style=rounded;\n";
  out << "    color=\"" << kPalette.text << "\";\n";
  out << "    bgcolor=white;\n";
  out << "    margin=8;\n\n";

  if (isRoot) {
    out << "    penwidth=3;\n";
  }

  int headerWidth = std::max(300, static_cast<int>(numChildren) * 70);

  out << "    dt" << dt.id() << "_header [shape=none, margin=0, label=<\n";
  out << "      <TABLE BORDER=\"0\" CELLBORDER=\"0\" CELLSPACING=\"0\" "
         "CELLPADDING=\"4\">\n";
  out << "        <TR><TD BGCOLOR=\"" << kPalette.header << "\" WIDTH=\""
      << headerWidth << "\"><B>" << escapeHtml(dt.cname);
}

// Closes the header cell and the outer HTML table opened by
// printClusterHeaderStart.
void printClusterHeaderEnd(std::ostream& out) {
  out << "      </TABLE>\n";
  out << "    >];\n\n";
}

void printDerivedTableCluster(
    const DerivedTable& dt,
    bool isRoot,
    std::ostream& out) {
  printClusterHeaderStart(dt, isRoot, dt.tables.size(), out);
  out << "</B></TD></TR>\n";

  // Output columns: "col := expr" when the expression differs from the
  // column name.
  for (size_t i = 0; i < dt.columns.size(); ++i) {
    auto* col = dt.columns[i];
    auto* expr = dt.exprs[i];
    if (expr->toString() != col->name()) {
      printRow(
          out,
          escapeHtml(col->name()) +
              " := " + escapeHtml(truncate(expr->toString())));
    } else {
      printRow(out, escapeHtml(col->name()));
    }
  }

  if (dt.aggregation != nullptr) {
    // GROUP BY.
    const auto& groupingKeys = dt.aggregation->groupingKeys();
    printSection(out, "GROUP BY", groupingKeys, [](size_t, auto* key) {
      return escapeHtml(truncate(key->toString()));
    });

    // Aggregates.
    const auto& aggregates = dt.aggregation->aggregates();
    const auto& columns = dt.aggregation->columns();
    const auto numGroupingKeys = groupingKeys.size();
    printSection(out, "AGG", aggregates, [&](size_t i, auto* agg) {
      return escapeHtml(columns[numGroupingKeys + i]->name()) +
          " := " + escapeHtml(truncate(agg->toString()));
    });
  }

  // ORDER BY.
  printSection(out, "ORDER BY", dt.orderKeys, [&](size_t i, auto* key) {
    std::string result = escapeHtml(key->toString());
    if (i < dt.orderTypes.size()) {
      result += " " + std::string(OrderTypeName::toName(dt.orderTypes[i]));
    }
    return result;
  });

  // JOINS section header and numbered join conditions. Only show join type
  // for non-INNER joins to save space.
  printSection(out, "JOIN", dt.joins, [](size_t i, auto* join) {
    auto type = joinTypeLabel(*join);
    auto condition = joinCondition(*join);
    std::string result = std::to_string(i + 1) + ". ";
    if (type != "INNER") {
      result += type + ": ";
    }
    return result + condition;
  });

  printSection(out, "FILTER", dt.conjuncts, [](size_t, auto* expr) {
    return escapeHtml(truncate(expr->toString()));
  });

  printClusterHeaderEnd(out);

  printJoinEdges(dt, out);

  out << "  }\n\n";
}

// Prints a SetDt (UNION ALL / UNION DISTINCT) as a cluster. Each union leg
// is represented by a table ID node; the leg's own DerivedTable cluster is
// printed separately by the caller.
void printSetDtCluster(
    const SetDt& setDt,
    const PlanObjectVector& legs,
    bool isRoot,
    std::ostream& out) {
  printClusterHeaderStart(setDt, isRoot, legs.size(), out);
  out << (setDt.isUnionAll() ? " UNION ALL" : " UNION DISTINCT")
      << "</B></TD></TR>\n";

  // Output columns (names only; per-leg exprs live on the leg DTs).
  for (auto* col : setDt.columns) {
    printRow(out, escapeHtml(col->name()));
  }

  printClusterHeaderEnd(out);

  printTableIdRow(setDt, legs, out);
  printHeaderAnchor(setDt, legs, out);

  out << "  }\n\n";
}

} // namespace

void printAllTables(PlanObjectCP table, std::ostream& out);

// Returns the anchor point for layout edges.
// - BaseTable: Uses the node ID directly (the table box itself).
// - DerivedTable: Uses the cluster's header node (dt{id}_header) since
//   clusters cannot be directly referenced in edge constraints.
std::string layoutId(PlanObjectCP table) {
  if (table->type() == PlanType::kDerivedTableNode) {
    auto* dt = table->as<DerivedTable>();
    return fmt::format("dt{}_header", dt->id());
  }
  return nodeId(table);
}

// Arranges child tables (BaseTable boxes and nested DerivedTable clusters)
// in a 2-column grid using invisible edges.
//
// Example layout for 5 tables:
//   ┌──────────┐  ┌──────────┐
//   │ Table 0  │  │ Table 1  │   Row 0
//   └──────────┘  └──────────┘
//         ↓ (invis)
//   ┌──────────┐  ┌──────────┐
//   │ Table 2  │  │ Table 3  │   Row 1
//   └──────────┘  └──────────┘
//         ↓ (invis)
//   ┌──────────┐
//   │ Table 4  │                 Row 2
//   └──────────┘
//
// Three types of invisible edges:
// 1. Vertical ordering: connects first table in each row to first table in
//    next row (Table[0] → Table[2] → Table[4]...)
// 2. Horizontal ordering: connects pairs within each row
//    (Table[0] → Table[1], Table[2] → Table[3]...)
// 3. Cluster-to-table anchor: connects the middle table ID node from the
//    parent cluster to the first child table with high weight to pull the
//    children below the parent.
void printTableLayout(
    int32_t parentId,
    const PlanObjectVector& tables,
    std::ostream& out) {
  if (tables.empty()) {
    return;
  }

  out << "\n";

  // Invisible edges for vertical ordering - connect first table in each row
  // to first table in the next row.
  for (size_t i = 0; i + 2 < tables.size(); i += 2) {
    out << "  " << layoutId(tables[i]) << " -> " << layoutId(tables[i + 2])
        << " [style=invis];\n";
  }

  // Invisible edges for horizontal ordering within rows.
  // Use constraint=false to prevent vertical shift; the edge direction
  // (left -> right) enforces horizontal order.
  for (size_t i = 0; i + 1 < tables.size(); i += 2) {
    out << "  {rank=same; " << layoutId(tables[i]) << " -> "
        << layoutId(tables[i + 1]) << " [style=invis, constraint=false];}\n";
  }

  // Connect DT cluster to first table with invisible edge for layout.
  size_t mid = tables.size() / 2;
  out << "  dt" << parentId << "_" << cname(tables[mid]) << " -> "
      << layoutId(tables[0]) << " [style=invis, weight=100];\n";
}

void printDerivedTableWithChildren(
    const DerivedTable& dt,
    bool isRoot,
    std::ostream& out) {
  if (const auto* setDt = dt.asUnion()) {
    PlanObjectVector legs;
    legs.reserve(setDt->inputs.size());
    for (auto* leg : setDt->inputs) {
      legs.push_back(leg);
    }

    printSetDtCluster(*setDt, legs, isRoot, out);

    for (auto* leg : legs) {
      printAllTables(leg, out);
    }
    printTableLayout(dt.id(), legs, out);
    return;
  }

  printDerivedTableCluster(dt, isRoot, out);

  for (auto* table : dt.tables) {
    printAllTables(table, out);
  }

  printTableLayout(dt.id(), dt.tables, out);
}

void printAllTables(PlanObjectCP table, std::ostream& out) {
  switch (table->type()) {
    case PlanType::kDerivedTableNode:
      // Nested DerivedTables are not root.
      printDerivedTableWithChildren(
          *table->as<DerivedTable>(), /*isRoot=*/false, out);
      break;
    case PlanType::kTableNode:
      printBaseTable(*table->as<BaseTable>(), out);
      break;
    case PlanType::kValuesTableNode:
      printValuesTable(*table->as<ValuesTable>(), out);
      break;
    case PlanType::kUnnestTableNode:
      printUnnestTable(*table->as<UnnestTable>(), out);
      break;
    default:
      VELOX_UNREACHABLE("Unknown table type: {}", table->typeName());
  }
}

// static
void DerivedTableDotPrinter::print(
    const DerivedTable& root,
    std::ostream& out) {
  out << "digraph QueryGraph {\n";
  out << "  newrank=true;\n";
  out << "  rankdir=TB;\n";
  out << "  splines=curved;\n";
  out << "  nodesep=0.6;\n";
  out << "  ranksep=0.8;\n";
  out << "  node [fontname=\"Helvetica\", fontsize=10, fontcolor=\""
      << kPalette.text << "\"];\n";
  out << "  edge [fontname=\"Helvetica\", fontsize=10];\n";
  out << "\n";

  // Print the DerivedTable and all nested tables recursively.
  // The root DerivedTable gets a thicker border.
  printDerivedTableWithChildren(root, /*isRoot=*/true, out);

  out << "}\n";
}

} // namespace facebook::axiom::graphviz
