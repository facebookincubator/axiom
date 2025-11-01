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
#include "axiom/optimizer/DerivedTable.h"
#include "axiom/optimizer/DerivedTablePrinter.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/PlanUtils.h"

namespace facebook::axiom::optimizer {
namespace {

// Adds an equijoin edge between 'left' and 'right'.
void addJoinEquality(ExprCP left, ExprCP right, JoinEdgeVector& joins) {
  auto leftTable = left->singleTable();
  auto rightTable = right->singleTable();

  VELOX_CHECK_NOT_NULL(leftTable);
  VELOX_CHECK_NOT_NULL(rightTable);
  VELOX_CHECK(leftTable != rightTable);

  for (auto& join : joins) {
    if (join->leftTable() == leftTable && join->rightTable() == rightTable) {
      join->addEquality(left, right);
      return;
    }

    if (join->rightTable() == leftTable && join->leftTable() == rightTable) {
      join->addEquality(right, left);
      return;
    }
  }

  auto* join = JoinEdge::makeInner(leftTable, rightTable);
  join->addEquality(left, right);
  joins.push_back(join);
}

// Set of pairs of column IDs. Each pair represents a join equality condition.
// Pairs are canonicalized so that first ID is < second ID.
using EdgeSet = folly::F14FastSet<std::pair<int32_t, int32_t>>;

bool addEdge(EdgeSet& edges, PlanObjectCP left, PlanObjectCP right) {
  if (left->id() == right->id()) {
    return false;
  }

  if (left->id() < right->id()) {
    return edges.emplace(left->id(), right->id()).second;
  } else {
    return edges.emplace(right->id(), left->id()).second;
  }
}

void fillJoins(
    PlanObjectCP column,
    const Equivalence& equivalence,
    EdgeSet& edges,
    DerivedTableP dt) {
  for (auto& other : equivalence.columns) {
    if (addEdge(edges, column, other)) {
      addJoinEquality(column->as<Column>(), other->as<Column>(), dt->joins);
    }
  }
}
} // namespace

void DerivedTable::addImpliedJoins() {
  EdgeSet edges;
  for (auto& join : joins) {
    if (join->isInner()) {
      for (size_t i = 0; i < join->numKeys(); ++i) {
        const auto* leftKey = join->leftKeys()[i];
        const auto* rightKey = join->rightKeys()[i];
        if (leftKey->isColumn() && rightKey->isColumn()) {
          addEdge(edges, leftKey, rightKey);
        }
      }
    }
  }

  // The loop appends to 'joins', so loop over a copy.
  JoinEdgeVector joinsCopy = joins;
  for (auto& join : joinsCopy) {
    if (join->isInner()) {
      for (size_t i = 0; i < join->numKeys(); ++i) {
        const auto* leftKey = join->leftKeys()[i];
        const auto* rightKey = join->rightKeys()[i];
        if (leftKey->isColumn() && rightKey->isColumn()) {
          auto leftEq = leftKey->as<Column>()->equivalence();
          auto rightEq = rightKey->as<Column>()->equivalence();
          if (rightEq && leftEq) {
            for (auto& left : leftEq->columns) {
              fillJoins(left, *rightEq, edges, this);
            }
          } else if (leftEq) {
            fillJoins(rightKey, *leftEq, edges, this);
          } else if (rightEq) {
            fillJoins(leftKey, *rightEq, edges, this);
          }
        }
      }
    }
  }
}

namespace {

bool isSingleRowDt(PlanObjectCP object) {
  if (object->is(PlanType::kDerivedTableNode)) {
    auto dt = object->as<DerivedTable>();
    return dt->limit == 1 ||
        (dt->aggregation && dt->aggregation->groupingKeys().empty());
  }
  return false;
}

// @return a subset of 'tables' that contain single row tables from
// non-correlated scalar subqueries.
PlanObjectSet findSingleRowDts(
    const PlanObjectSet& tables,
    const JoinEdgeVector& joins) {
  PlanObjectSet singleRowDts;

  // Remove tables that are joined to other tables.
  auto tablesCopy = tables;
  int32_t numSingle = 0;
  for (auto& join : joins) {
    tablesCopy.erase(join->rightTable());
    for (auto& key : join->leftKeys()) {
      tablesCopy.except(key->allTables());
    }
    for (auto& filter : join->filter()) {
      tablesCopy.except(filter->allTables());
    }
  }

  tablesCopy.forEach([&](PlanObjectCP object) {
    if (isSingleRowDt(object)) {
      ++numSingle;
      singleRowDts.add(object);
    }
  });

  // If everything is a single row dt, then process these as cross products and
  // not as placed with filters.
  if (numSingle == tables.size()) {
    return PlanObjectSet();
  }

  return singleRowDts;
}
} // namespace

void DerivedTable::setStartTables() {
  singleRowDts = findSingleRowDts(tableSet, joins);
  startTables = tableSet;
  startTables.except(singleRowDts);
  for (auto join : joins) {
    if (join->isNonCommutative()) {
      startTables.erase(join->rightTable());
    }
  }
}

namespace {
// Returns a right exists (semijoin) with 'table' on the left and one of
// 'tables' on the right.
JoinEdgeP makeExists(PlanObjectCP table, const PlanObjectSet& tables) {
  for (auto join : joinedBy(table)) {
    if (join->leftTable() == table) {
      if (!tables.contains(join->rightTable())) {
        continue;
      }
      auto* exists = JoinEdge::makeExists(table, join->rightTable());
      for (size_t i = 0; i < join->numKeys(); ++i) {
        exists->addEquality(join->leftKeys()[i], join->rightKeys()[i]);
      }
      return exists;
    }

    if (join->rightTable() == table) {
      if (!join->leftTable() || !tables.contains(join->leftTable())) {
        continue;
      }

      auto* exists = JoinEdge::makeExists(table, join->leftTable());
      for (size_t i = 0; i < join->numKeys(); ++i) {
        exists->addEquality(join->rightKeys()[i], join->leftKeys()[i]);
      }
      return exists;
    }
  }
  VELOX_UNREACHABLE("No join to make an exists build side restriction");
}

} // namespace

void DerivedTable::linkTablesToJoins() {
  setStartTables();

  // All tables directly mentioned by a join link to the join. A non-inner
  // that depends on multiple left tables has no leftTable but is still linked
  // from all the tables it depends on.
  for (auto join : joins) {
    PlanObjectSet tables;
    if (join->isInner() && join->directed()) {
      tables.add(join->leftTable());
    } else {
      for (auto key : join->leftKeys()) {
        tables.unionSet(key->allTables());
      }
      for (auto key : join->rightKeys()) {
        tables.unionSet(key->allTables());
      }
      for (auto conjunct : join->filter()) {
        tables.unionSet(conjunct->allTables());
      }
    }
    tables.forEachMutable([&](PlanObjectP table) {
      if (table->is(PlanType::kTableNode)) {
        table->as<BaseTable>()->addJoinedBy(join);
      } else if (table->is(PlanType::kValuesTableNode)) {
        table->as<ValuesTable>()->addJoinedBy(join);
      } else if (table->is(PlanType::kUnnestTableNode)) {
        table->as<UnnestTable>()->addJoinedBy(join);
      } else {
        VELOX_CHECK(table->is(PlanType::kDerivedTableNode));
        table->as<DerivedTable>()->addJoinedBy(join);
      }
    });
  }
}

namespace {
std::pair<DerivedTableP, JoinEdgeP> makeExistsDtAndJoin(
    const DerivedTable& super,
    PlanObjectCP firstTable,
    float existsFanout,
    PlanObjectVector& existsTables,
    JoinEdgeP existsJoin) {
  auto firstExistsTable = existsJoin->rightKeys()[0]->singleTable();
  VELOX_CHECK(firstExistsTable);

  MemoKey existsDtKey;
  existsDtKey.firstTable = firstExistsTable;
  existsDtKey.tables.unionObjects(existsTables);
  for (auto& column : existsJoin->rightKeys()) {
    existsDtKey.columns.unionColumns(column);
  }

  auto optimization = queryCtx()->optimization();
  auto it = optimization->existenceDts().find(existsDtKey);
  DerivedTableP existsDt{};
  if (it == optimization->existenceDts().end()) {
    existsDt = make<DerivedTable>();
    existsDt->cname = optimization->newCName("edt");
    existsDt->import(super, firstExistsTable, existsDtKey.tables, {});
    for (auto& k : existsJoin->rightKeys()) {
      auto* existsColumn = make<Column>(
          toName(fmt::format("{}.{}", existsDt->cname, k->toString())),
          existsDt,
          k->value());
      existsDt->columns.push_back(existsColumn);
      existsDt->exprs.push_back(k);
    }
    existsDt->noImportOfExists = true;
    existsDt->makeInitialPlan();
    optimization->existenceDts()[existsDtKey] = existsDt;
  } else {
    existsDt = it->second;
  }
  auto* joinWithDt = JoinEdge::makeExists(firstTable, existsDt);
  joinWithDt->setFanouts(existsFanout, 1);
  for (size_t i = 0; i < existsJoin->numKeys(); ++i) {
    joinWithDt->addEquality(existsJoin->leftKeys()[i], existsDt->columns[i]);
  }
  return std::make_pair(existsDt, joinWithDt);
}
} // namespace

bool DerivedTable::hasWindows() const {
  return exprs.hasWindows() || orderKeys.hasWindows();
}

void DerivedTable::import(
    const DerivedTable& super,
    PlanObjectCP firstTable,
    const PlanObjectSet& superTables,
    const std::vector<PlanObjectSet>& existences,
    float existsFanout) {
  tableSet = superTables;
  tables = superTables.toObjects();

  for (auto id : super.joinOrder) {
    if (tableSet.BitSet::contains(id)) {
      joinOrder.push_back(id);
    }
  }

  for (auto join : super.joins) {
    if (superTables.contains(join->rightTable()) && join->leftTable() &&
        superTables.contains(join->leftTable())) {
      joins.push_back(join);
    }
  }

  if (!existences.empty()) {
    if (!queryCtx()->optimization()->options().syntacticJoinOrder) {
      for (auto& exists : existences) {
        // We filter the derived table by importing reducing semijoins.
        // These are based on joins on the outer query but become
        // existences so as not to change cardinality. The reducing join
        // is against one or more tables. If more than one table, the join
        // of these tables goes into its own derived table which is joined
        // with exists to the main table(s) in the 'this'.
        importedExistences.unionSet(exists);
        auto existsTables = exists.toObjects();
        auto existsJoin = makeExists(firstTable, exists);
        if (existsTables.size() > 1) {
          // There is a join on the right of exists. Needs its own dt.
          auto [existsDt, joinWithDt] = makeExistsDtAndJoin(
              super, firstTable, existsFanout, existsTables, existsJoin);
          joins.push_back(joinWithDt);
          addTable(existsDt);
        } else {
          joins.push_back(existsJoin);
          VELOX_DCHECK(!existsTables.empty());
          addTable(existsTables[0]);
        }
      }
    }

    noImportOfExists = true;
  }

  if (firstTable->is(PlanType::kDerivedTableNode)) {
    importJoinsIntoFirstDt(firstTable->as<DerivedTable>());
  } else {
    fullyImported = superTables;
  }
  linkTablesToJoins();
}

namespace {
template <typename V, typename E>
void eraseFirst(V& set, E element) {
  auto it = std::find(set.begin(), set.end(), element);
  VELOX_CHECK(it != set.end());
  set.erase(it);
}

JoinEdgeP importedDtJoin(
    JoinEdgeP join,
    DerivedTableP dt,
    ExprCP innerKey,
    bool fullyImported) {
  auto left = innerKey->singleTable();
  VELOX_CHECK(left);
  auto otherKey = dt->columns[0];
  auto* newJoin = !fullyImported ? JoinEdge::makeExists(left, dt)
                                 : JoinEdge::makeInner(left, dt);
  newJoin->addEquality(innerKey, otherKey);
  return newJoin;
}

bool isProjected(PlanObjectCP table, const PlanObjectSet& columns) {
  bool projected = false;
  columns.forEach<Column>(
      [&](auto column) { projected |= column->relation() == table; });
  return projected;
}

// True if 'join'  has max 1 match for a row of 'side'.
bool isUnique(JoinEdgeP join, PlanObjectCP side) {
  return join->sideOf(side, true).isUnique;
}

// Returns a join partner of 'startin 'joins' ' where the partner is
// not in 'visited' Sets 'isFullyImported' to false if the partner is
// not guaranteed n:1 reducing or has columns that are projected out.
PlanObjectCP nextJoin(
    PlanObjectCP start,
    const JoinEdgeVector& joins,
    const PlanObjectSet& columns,
    const PlanObjectSet& visited,
    bool& fullyImported) {
  for (auto& join : joins) {
    auto other = join->otherSide(start);
    if (!other) {
      continue;
    }
    if (visited.contains(other)) {
      continue;
    }
    if (!isUnique(join, other) || isProjected(other, columns)) {
      fullyImported = false;
    }
    return other;
  }
  return nullptr;
}

void joinChain(
    PlanObjectCP start,
    const JoinEdgeVector& joins,
    const PlanObjectSet& columns,
    PlanObjectSet visited,
    bool& fullyImported,
    std::vector<PlanObjectCP>& path) {
  auto next = nextJoin(start, joins, columns, visited, fullyImported);
  if (!next) {
    return;
  }
  visited.add(next);
  path.push_back(next);
  joinChain(next, joins, columns, visited, fullyImported, path);
}

JoinEdgeP importedJoin(
    JoinEdgeP join,
    PlanObjectCP other,
    ExprCP innerKey,
    bool fullyImported) {
  auto left = innerKey->singleTable();
  VELOX_CHECK(left);
  auto otherKey = join->sideOf(other).keys[0];
  auto* newJoin = !fullyImported ? JoinEdge::makeExists(left, other)
                                 : JoinEdge::makeInner(left, other);
  newJoin->addEquality(innerKey, otherKey);
  return newJoin;
}

// Returns a copy of 'expr', replacing instances of columns in 'source' with
// the corresponding expression from 'target'
// @tparam T ColumnVector or ExprVector
// @tparam U ColumnVector or ExprVector
// @param source Columns to replace. 1:1 with 'target.
// @param target Replacements.
template <typename T, typename U>
ExprCP replaceInputs(ExprCP expr, const T& source, const U& target) {
  if (!expr) {
    return nullptr;
  }

  switch (expr->type()) {
    case PlanType::kColumnExpr:
      for (auto i = 0; i < source.size(); ++i) {
        if (source[i] == expr) {
          return target[i];
        }
      }
      return expr;
    case PlanType::kLiteralExpr:
      return expr;
    case PlanType::kCallExpr: {
      auto children = expr->children();
      ExprVector newChildren(children.size());
      FunctionSet functions;
      bool anyChange = false;
      for (auto i = 0; i < children.size(); ++i) {
        newChildren[i] = replaceInputs(children[i]->as<Expr>(), source, target);
        anyChange |= newChildren[i] != children[i];
        if (newChildren[i]->isFunction()) {
          functions = functions | newChildren[i]->as<Call>()->functions();
        }
      }

      if (!anyChange) {
        return expr;
      }

      const auto* call = expr->as<Call>();
      return make<Call>(
          call->name(), call->value(), std::move(newChildren), functions);
    }
    default:
      VELOX_UNREACHABLE("{}", expr->toString());
  }
}

} // namespace

ExprCP DerivedTable::exportExpr(ExprCP expr) {
  return replaceInputs(expr, exprs, columns);
}

ExprCP DerivedTable::importExpr(ExprCP expr) {
  return replaceInputs(expr, columns, exprs);
}

void DerivedTable::importJoinsIntoFirstDt(const DerivedTable* firstDt) {
  if (tables.size() == 1 && tables[0]->is(PlanType::kDerivedTableNode)) {
    flattenDt(tables[0]->as<DerivedTable>());
    return;
  }
  auto initialTables = tables;
  if (firstDt->hasLimit() || firstDt->hasOrderBy()) {
    // tables can't be imported but are marked as used so not tried again.
    for (auto i = 1; i < tables.size(); ++i) {
      importedExistences.add(tables[i]);
    }
    return;
  }
  auto& outer = firstDt->columns;
  auto& inner = firstDt->exprs;
  PlanObjectSet projected;
  for (auto& expr : exprs) {
    projected.unionColumns(expr);
  }

  auto* newFirst = make<DerivedTable>(*firstDt->as<DerivedTable>());
  for (auto& join : joins) {
    auto other = join->otherSide(firstDt);
    if (!other) {
      continue;
    }
    if (!tableSet.contains(other)) {
      // Already placed in some previous join chain.
      continue;
    }
    auto side = join->sideOf(firstDt);
    if (side.keys.size() > 1 || !join->filter().empty()) {
      continue;
    }
    auto innerKey = replaceInputs(side.keys[0], outer, inner);
    VELOX_DCHECK(innerKey);
    if (innerKey->containsFunction(FunctionSet::kAggregate)) {
      // If the join key is an aggregate, the join can't be moved below the agg.
      continue;
    }
    auto otherSide = join->sideOf(firstDt, true);
    PlanObjectSet visited;
    visited.add(firstDt);
    visited.add(other);
    std::vector<PlanObjectCP> path;
    bool fullyImported = otherSide.isUnique;
    joinChain(other, joins, projected, visited, fullyImported, path);
    if (path.empty()) {
      if (other->is(PlanType::kDerivedTableNode)) {
        const_cast<PlanObject*>(other)->as<DerivedTable>()->makeInitialPlan();
      }

      newFirst->addTable(other);
      newFirst->joins.push_back(
          importedJoin(join, other, innerKey, fullyImported));
      if (fullyImported) {
        newFirst->fullyImported.add(other);
      }
    } else {
      auto* chainDt = make<DerivedTable>();
      PlanObjectSet chainSet;
      chainSet.add(other);
      if (fullyImported) {
        newFirst->fullyImported.add(other);
      }
      for (auto& object : path) {
        chainSet.add(object);
        if (fullyImported) {
          newFirst->fullyImported.add(object);
        }
      }
      chainDt->makeProjection(otherSide.keys);
      chainDt->import(*this, other, chainSet, {});
      chainDt->makeInitialPlan();
      newFirst->addTable(chainDt);
      newFirst->joins.push_back(
          importedDtJoin(join, chainDt, innerKey, fullyImported));
    }
    eraseFirst(tables, other);
    tableSet.erase(other);
    for (auto& table : path) {
      eraseFirst(tables, table);
      tableSet.erase(table);
    }
  }

  VELOX_CHECK_EQ(tables.size(), 1);
  for (auto i = 0; i < initialTables.size(); ++i) {
    if (!newFirst->fullyImported.contains(initialTables[i])) {
      newFirst->importedExistences.add(initialTables[i]);
    }
  }
  tables[0] = newFirst;
  flattenDt(newFirst);
}

void DerivedTable::flattenDt(const DerivedTable* dt) {
  tables = dt->tables;
  cname = dt->cname;
  tableSet = dt->tableSet;
  joins = dt->joins;
  joinOrder = dt->joinOrder;
  columns = dt->columns;
  exprs = dt->exprs;
  fullyImported = dt->fullyImported;
  importedExistences.unionSet(dt->importedExistences);
  aggregation = dt->aggregation;
  having = dt->having;
}

void DerivedTable::makeProjection(const ExprVector& exprs) {
  auto optimization = queryCtx()->optimization();
  for (auto& expr : exprs) {
    auto* column =
        make<Column>(optimization->newCName("ec"), this, expr->value());
    columns.push_back(column);
    this->exprs.push_back(expr);
  }
}

namespace {

// Finds a JoinEdge between tables[0] and tables[1]. Sets tables[0] to the
// left and [1] to the right table of the found join. Returns the JoinEdge. If
// 'create' is true and no edge is found, makes a new edge with tables[0] as
// left and [1] as right.
JoinEdgeP
findJoin(DerivedTableP dt, std::vector<PlanObjectP>& tables, bool create) {
  for (auto& join : dt->joins) {
    if (join->leftTable() == tables[0] && join->rightTable() == tables[1]) {
      return join;
    }
    if (join->leftTable() == tables[1] && join->rightTable() == tables[0]) {
      std::swap(tables[0], tables[1]);
      return join;
    }
  }
  if (create) {
    auto* join = JoinEdge::makeInner(tables[0], tables[1]);
    dt->joins.push_back(join);
    return join;
  }
  return nullptr;
}

// Check if a non-UNION DT has a limit or one of the children of a UNION DT has
// a limit.
bool dtHasLimit(const DerivedTable& dt) {
  if (dt.setOp.has_value()) {
    for (const auto& child : dt.children) {
      if (child->is(PlanType::kDerivedTableNode) &&
          child->as<DerivedTable>()->hasLimit()) {
        return true;
      }
    }

    return false;
  }

  return dt.hasLimit();
}

void flattenAll(ExprCP expr, Name func, ExprVector& flat) {
  if (expr->isNot(PlanType::kCallExpr) || expr->as<Call>()->name() != func) {
    flat.push_back(expr);
    return;
  }
  for (auto arg : expr->as<Call>()->args()) {
    flattenAll(arg, func, flat);
  }
}

// 'disjuncts' is an OR of ANDs. If each disjunct depends on the same tables
// and if each conjunct inside the ANDs in the OR depends on a single table,
// then return for each distinct table an OR of ANDs. The disjuncts are the
// top vector the conjuncts are the inner vector.
//
// For example, given two disjuncts:
//    (t.a = 1 AND u.x = 2) OR (t.b = 3 AND u.y = 4)
//
// extracts per-table filters:
//    t: a = 1 OR b = 3
//    u: x = 2 OR y = 4
//
// These filters can be pushed down into individual table scans to reduce the
// cardinality. The original filter still needs to be evaluated on the results
// of the join.
//
// This pattern appears in TPC-H q9.
ExprVector extractPerTable(
    const ExprVector& disjuncts,
    std::vector<ExprVector>& orOfAnds) {
  PlanObjectSet tables = disjuncts[0]->allTables();
  if (tables.size() <= 1) {
    // All must depend on the same set of more than 1 table.
    return {};
  }

  // Mapping keyed on a table ID. The value is a list of conjuncts that depend
  // only on that table.
  folly::F14FastMap<int32_t, std::vector<ExprVector>> perTable;
  for (auto i = 0; i < disjuncts.size(); ++i) {
    if (i > 0 && disjuncts[i]->allTables() != tables) {
      // Does not  depend on the same tables as the other disjuncts.
      return {};
    }
    folly::F14FastMap<int32_t, ExprVector> perTableAnd;
    ExprVector& inner = orOfAnds[i];
    // Do the inner conjuncts each depend on a single table?
    for (const auto& conjunct : inner) {
      auto single = conjunct->singleTable();
      if (!single) {
        return {};
      }
      perTableAnd[single->id()].push_back(conjunct);
    }
    for (auto& pair : perTableAnd) {
      perTable[pair.first].push_back(pair.second);
    }
  }

  auto optimization = queryCtx()->optimization();
  ExprVector conjuncts;
  for (auto& pair : perTable) {
    ExprVector tableAnds;
    for (auto& tableAnd : pair.second) {
      tableAnds.push_back(
          optimization->combineLeftDeep(SpecialFormCallNames::kAnd, tableAnd));
    }
    conjuncts.push_back(
        optimization->combineLeftDeep(SpecialFormCallNames::kOr, tableAnds));
  }

  return conjuncts;
}

// Analyzes an OR. Returns top level conjuncts that this has inferred from the
// disjuncts. For example if all have an AND inside and each AND has the same
// condition then this condition is returned and removed from the disjuncts.
// 'disjuncts' is changed in place. If 'replacement' is set, then this replaces
// the whole OR from which 'disjuncts' was flattened.
//
// In other words,
//    (x AND y) OR (x AND z) => x AND (y OR z)
//    (x AND y) OR (x AND y) => x AND y
//
// This pattern appears in TPC-H q9.
ExprVector extractCommon(ExprVector& disjuncts, ExprCP* replacement) {
  // Remove duplicates.
  folly::F14FastSet<ExprCP> uniqueDisjuncts;
  bool changeOriginal = false;
  for (auto i = 0; i < disjuncts.size(); ++i) {
    auto disjunct = disjuncts[i];
    if (!uniqueDisjuncts.emplace(disjunct).second) {
      disjuncts.erase(disjuncts.begin() + i);
      --i;
      changeOriginal = true;
    }
  }

  if (disjuncts.size() == 1) {
    *replacement = disjuncts[0];
    return {};
  }

  // The conjuncts in each of the disjuncts.
  std::vector<ExprVector> flat;
  for (auto i = 0; i < disjuncts.size(); ++i) {
    flat.emplace_back();
    flattenAll(disjuncts[i], SpecialFormCallNames::kAnd, flat.back());
  }

  // Check if the flat conjuncts lists have any element that occurs in all.
  // Remove all the elememts that are in all.
  ExprVector result;
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
    auto optimization = queryCtx()->optimization();
    ExprVector ands;
    for (const auto& inner : flat) {
      ands.push_back(
          optimization->combineLeftDeep(SpecialFormCallNames::kAnd, inner));
    }
    *replacement =
        optimization->combineLeftDeep(SpecialFormCallNames::kOr, ands);
  }

  return result;
}

// Extracts implied conjuncts and removes duplicates from 'conjuncts' and
// updates 'conjuncts'. Extracted conjuncts may allow extra pushdown or allow
// create join edges. May be called repeatedly, each e.g. after pushing down
// conjuncts from outer DTs.
void expandConjuncts(ExprVector& conjuncts) {
  bool any = false;
  auto firstUnprocessed = 0;
  do {
    any = false;

    const auto end = conjuncts.size();
    for (auto i = firstUnprocessed; i < end; ++i) {
      const auto& conjunct = conjuncts[i];
      if (isCallExpr(conjunct, SpecialFormCallNames::kOr) &&
          !conjunct->containsNonDeterministic()) {
        ExprVector flat;
        flattenAll(conjunct, SpecialFormCallNames::kOr, flat);
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
    firstUnprocessed = end;
  } while (any);
}

} // namespace

void DerivedTable::distributeConjuncts() {
  std::vector<DerivedTableP> changedDts;
  if (!having.empty()) {
    VELOX_CHECK_NOT_NULL(aggregation);

    // Push HAVING clause that uses only grouping keys below the aggregation.
    //
    // SELECT a, sum(b) FROM t GROUP BY a HAVING a > 0
    //   =>
    //     SELECT a, sum(b) FROM t WHERE a > 0 GROUP BY a

    // Gather the columns of grouping expressions. If a having depends
    // on these alone it can move below the aggregation and gets
    // translated from the aggregation output columns to the columns
    // inside the agg. Consider both the grouping expr and its rename
    // after the aggregation.
    PlanObjectSet grouping;
    for (auto i = 0; i < aggregation->groupingKeys().size(); ++i) {
      grouping.unionSet(aggregation->columns()[i]->columns());
      grouping.unionSet(aggregation->groupingKeys()[i]->columns());
    }

    for (auto i = 0; i < having.size(); ++i) {
      // No pushdown of non-deterministic.
      if (having[i]->containsNonDeterministic()) {
        continue;
      }
      // having that refers to no aggregates goes below the
      // aggregation. Translate from names after agg to pre-agg
      // names. Pre/post agg names may differ for dts in set
      // operations. If already in pre-agg names, no-op.
      if (having[i]->columns().isSubset(grouping)) {
        conjuncts.push_back(replaceInputs(
            having[i], aggregation->columns(), aggregation->groupingKeys()));
        having.erase(having.begin() + i);
        --i;
      }
    }
  }

  expandConjuncts(conjuncts);

  // A nondeterminstic filter can be pushed down past a cardinality
  // neutral border. This is either a single leaf table or a union all
  // of dts.
  const bool allowNondeterministic = tables.size() == 1 &&
      (tables[0]->is(PlanType::kTableNode) ||
       (tables[0]->is(PlanType::kDerivedTableNode) &&
        tables[0]->as<DerivedTable>()->setOp.has_value() &&
        tables[0]->as<DerivedTable>()->setOp.value() ==
            logical_plan::SetOperation::kUnionAll));

  for (auto i = 0; i < conjuncts.size(); ++i) {
    // No pushdown of non-deterministic except if only pushdown target is a
    // union all.
    if (conjuncts[i]->containsNonDeterministic() && !allowNondeterministic) {
      continue;
    }

    PlanObjectSet tableSet = conjuncts[i]->allTables();
    std::vector<PlanObjectP> tables;
    tableSet.forEachMutable([&](auto table) { tables.push_back(table); });
    if (tables.size() == 1) {
      if (tables[0] == this) {
        continue; // the conjunct depends on containing dt, like grouping or
                  // existence flags. Leave in place.
      }

      if (tables[0]->is(PlanType::kValuesTableNode)) {
        continue; // ValuesTable does not have filter push-down.
      }

      if (tables[0]->is(PlanType::kUnnestTableNode)) {
        continue; // UnnestTable does not have filter push-down.
      }

      if (tables[0]->is(PlanType::kDerivedTableNode)) {
        // Translate the column names and add the condition to the conjuncts in
        // the dt. If the inner is a set operation, add the filter to children.
        auto innerDt = tables[0]->as<DerivedTable>();
        if (innerDt->hasWindows() || dtHasLimit(*innerDt)) {
          continue;
        }

        auto numChildren =
            innerDt->children.empty() ? 1 : innerDt->children.size();
        for (auto childIdx = 0; childIdx < numChildren; ++childIdx) {
          auto childDt =
              numChildren == 1 ? innerDt : innerDt->children[childIdx];
          auto imported = childDt->importExpr(conjuncts[i]);
          if (childDt->aggregation) {
            childDt->having.push_back(imported);
          } else {
            childDt->conjuncts.push_back(imported);
          }
          if (std::find(changedDts.begin(), changedDts.end(), childDt) ==
              changedDts.end()) {
            changedDts.push_back(childDt);
          }
        }
      } else {
        VELOX_CHECK(tables[0]->is(PlanType::kTableNode));
        tables[0]->as<BaseTable>()->addFilter(conjuncts[i]);
      }
      conjuncts.erase(conjuncts.begin() + i);
      --i;
      continue;
    }

    if (tables.size() == 2) {
      ExprCP left = nullptr;
      ExprCP right = nullptr;
      // expr depends on 2 tables. If it is left = right or right = left and
      // there is no edge or the edge is inner, add the equality. For other
      // cases, leave the conjunct in place, to be evaluated when its
      // dependences are known.
      if (queryCtx()->optimization()->isJoinEquality(
              conjuncts[i], tables[0], tables[1], left, right)) {
        auto join = findJoin(this, tables, true);
        if (join->isInner()) {
          if (left->is(PlanType::kColumnExpr) &&
              right->is(PlanType::kColumnExpr)) {
            left->as<Column>()->equals(right->as<Column>());
          }
          if (join->leftTable() == tables[0]) {
            join->addEquality(left, right);
          } else {
            join->addEquality(right, left);
          }
          conjuncts.erase(conjuncts.begin() + i);

          --i;
        }
      }
    }
  }

  // Remake initial plan for changedDTs. Calls distributeConjuncts
  // recursively for further pushdown of pushed down items. Replans
  // on returning edge of recursion, so everybody's initial plan is
  // up to date after all pushdowns.
  for (auto* changed : changedDts) {
    changed->makeInitialPlan();
  }
}

void DerivedTable::makeInitialPlan() {
  MemoKey key;
  key.firstTable = this;
  key.tables.add(this);
  key.columns.unionObjects(columns);

  distributeConjuncts();
  addImpliedJoins();
  linkTablesToJoins();
  for (auto& join : joins) {
    join->guessFanout();
  }
  setStartTables();

  auto optimization = queryCtx()->optimization();
  PlanState state(*optimization, this);
  state.targetExprs.unionObjects(exprs);

  optimization->makeJoins(state);

  auto plan = state.plans.best()->op;
  this->cardinality = plan->resultCardinality();

  optimization->memo()[key] = std::move(state.plans);
}

PlanP DerivedTable::bestInitialPlan() const {
  MemoKey key;
  key.firstTable = this;
  key.tables.add(this);
  key.columns.unionObjects(columns);

  auto& memo = queryCtx()->optimization()->memo();
  auto it = memo.find(key);
  VELOX_CHECK(it != memo.end(), "Expecting to find a plan for union branch");

  return it->second.best();
}

std::string DerivedTable::toString() const {
  return DerivedTablePrinter::toText(*this);
}

void DerivedTable::addJoinedBy(JoinEdgeP join) {
  pushBackUnique(joinedBy, join);
}

} // namespace facebook::axiom::optimizer
