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
    // A join equality requires columns from different tables. Two columns
    // in the same equivalence class can belong to the same table, e.g. after
    // UNNEST.
    if (column->as<Expr>()->singleTable() == other->as<Expr>()->singleTable()) {
      continue;
    }
    if (addEdge(edges, column, other)) {
      addJoinEquality(column->as<Column>(), other->as<Column>(), dt->joins);
    }
  }
}

// For a single-key semi/anti-join edge, creates implied edges through
// equivalence classes. If the edge goes from A to DT with left key A.x, and
// A.x is equivalent to B.y (from inner join A.x = B.y), creates an implied
// edge from B to DT with left key B.y.
void addImpliedNonInnerEdge(
    JoinEdgeP join,
    const PlanObjectSet& tableSet,
    EdgeSet& edges,
    JoinEdgeVector& joins) {
  if (!join->filter().empty() || join->numKeys() != 1) {
    return;
  }
  auto* leftKey = join->leftKeys()[0];
  if (!leftKey->isColumn()) {
    return;
  }
  auto* equiv = leftKey->as<Column>()->equivalence();
  if (!equiv) {
    return;
  }
  auto* leftTable = leftKey->as<Expr>()->singleTable();

  for (auto* candidate : equiv->columns) {
    auto* candidateTable = candidate->as<Expr>()->singleTable();
    if (candidateTable == leftTable || !tableSet.contains(candidateTable)) {
      continue;
    }
    if (!addEdge(edges, candidate, join->rightKeys()[0])) {
      continue;
    }

    JoinEdgeP implied;
    if (join->isSemi()) {
      implied = JoinEdge::makeExists(
          candidateTable,
          join->rightTable(),
          join->markColumn(),
          {},
          join->isNullAwareIn());
    } else {
      implied = JoinEdge::makeNotExists(candidateTable, join->rightTable());
    }
    implied->addEquality(candidate, join->rightKeys()[0]);
    joins.push_back(implied);
  }
}
} // namespace

void DerivedTable::checkSetOpConsistency() const {
  VELOX_CHECK(
      setOp.value() == logical_plan::SetOperation::kUnion ||
          setOp.value() == logical_plan::SetOperation::kUnionAll,
      "setOp must be union or unionAll: {}, {}",
      cname,
      logical_plan::SetOperationName::toName(setOp.value()));
  VELOX_CHECK_EQ(
      tables.size(), 0, "tables must be empty for set operation: {}", cname);
  VELOX_CHECK(
      exprs.empty(), "exprs must be empty for set operation: {}", cname);
  VELOX_CHECK_GE(
      children.size(),
      2,
      "set operation must have at least 2 children: {}",
      cname);

  // Union DT cannot have post-processing.
  VELOX_CHECK_NULL(
      aggregation, "union DT must not have aggregation: {}", cname);
  VELOX_CHECK_NULL(windowPlan, "union DT must not have windowPlan: {}", cname);
  VELOX_CHECK(having.empty(), "union DT must not have having: {}", cname);
  VELOX_CHECK(conjuncts.empty(), "union DT must not have conjuncts: {}", cname);
  VELOX_CHECK(joins.empty(), "union DT must not have joins: {}", cname);
  VELOX_CHECK(
      startTables.empty(), "union DT must not have startTables: {}", cname);
  VELOX_CHECK(
      singleRowDts.empty(), "union DT must not have singleRowDts: {}", cname);
  VELOX_CHECK(
      importedExistences.empty(),
      "union DT must not have importedExistences: {}",
      cname);

  // Union DT: outputColumns must equal columns (no intermediate columns).
  VELOX_CHECK_EQ(
      outputColumns.size(),
      columns.size(),
      "union DT outputColumns must match columns: {}",
      cname);
}

void DerivedTable::checkConsistency() const {
  VELOX_CHECK_NOT_NULL(cname);

  if (setOp.has_value()) {
    checkSetOpConsistency();
    return;
  }

  // Verifies that all tables referenced by expressions in 'exprs' are in
  // tableSet or 'this'.
  auto checkTableReferences = [&](const ExprVector& expressions,
                                  const char* context) {
    for (auto* expr : expressions) {
      expr->allTables().forEach([&](PlanObjectCP table) {
        VELOX_CHECK(
            tableSet.contains(table) || table == this,
            "{} references table not in tableSet or 'this': {}, {}",
            context,
            cname,
            expr->toString());
      });
    }
  };

  // Columns and exprs must be 1:1 when exprs is non-empty.
  if (!exprs.empty()) {
    VELOX_CHECK_EQ(
        columns.size(),
        exprs.size(),
        "Size mismatch between columns and exprs: {}",
        cname);
    for (size_t i = 0; i < columns.size(); ++i) {
      auto columnType = columns[i]->value().type;
      auto exprType = exprs[i]->value().type;
      VELOX_CHECK(
          columnType->equivalent(*exprType),
          "Type mismatch between column and expr: {}, {}, {} vs {}",
          cname,
          i,
          columnType->toString(),
          exprType->toString());
    }

    // exprs must reference only tables in tableSet or 'this'.
    checkTableReferences(exprs, "expr");
  }

  // outputColumns must be a subset of columns with no duplicates.
  {
    PlanObjectSet columnSet;
    columnSet.unionObjects(columns);
    PlanObjectSet seen;
    for (const auto* column : outputColumns) {
      VELOX_CHECK(
          columnSet.contains(column),
          "outputColumn not found in columns: {}, {}",
          column->toString(),
          cname);
      VELOX_CHECK(
          !seen.contains(column),
          "Duplicate in outputColumns: {}, {}",
          column->toString(),
          cname);
      seen.add(column);
    }
  }

  VELOX_CHECK_EQ(
      children.size(),
      0,
      "children must be empty for non-set-operation: {}",
      cname);
  VELOX_CHECK_GT(
      tables.size(),
      0,
      "tables must not be empty for non-set-operation: {}",
      cname);

  // All entries in tables must be tables. tableSet must contain exactly the
  // elements in tables.
  VELOX_CHECK_EQ(
      tables.size(),
      tableSet.size(),
      "Size mismatch between tables and tableSet: {}",
      cname);
  for (auto* table : tables) {
    VELOX_CHECK(
        table->isTable(),
        "Entry in tables is not a table: {}, {}",
        cname,
        table->typeName());
    VELOX_CHECK(
        tableSet.contains(table),
        "tableSet is missing a table: {}, {}",
        cname,
        optimizer::cname(table));
  }

  // joinOrder must match tables.
  VELOX_CHECK_EQ(
      joinOrder.size(),
      tables.size(),
      "Size mismatch between joinOrder and tables: {}",
      cname);
  for (auto id : joinOrder) {
    VELOX_CHECK(
        tableSet.BitSet::contains(id),
        "joinOrder references unknown table ID {} in {}",
        id,
        cname);
  }

  // At least one side of each join must be in tableSet. The other side may
  // reference a table outside this DT via implied joins from equivalence
  // classes. Neither side can be 'this'.
  for (auto* join : joins) {
    VELOX_CHECK(
        (join->leftTable() && tableSet.contains(join->leftTable())) ||
            tableSet.contains(join->rightTable()),
        "Neither side of a join is in tableSet: {}, {}",
        cname,
        join->toString());
    VELOX_CHECK(
        join->rightTable() != this,
        "Join rightTable is 'this': {}, {}",
        cname,
        join->toString());
    if (join->leftTable()) {
      VELOX_CHECK(
          join->leftTable() != this,
          "Join leftTable is 'this': {}, {}",
          cname,
          join->toString());
    }
  }

  // Each entry in joinedBy must have 'this' as an end point.
  for (auto* join : joinedBy) {
    VELOX_CHECK(
        join->leftTable() == this || join->rightTable() == this,
        "joinedBy entry does not reference this: {}, {}",
        cname,
        join->toString());
  }

  // startTables must be a subset of tableSet.
  startTables.forEach([&](PlanObjectCP table) {
    VELOX_CHECK(
        tableSet.contains(table),
        "startTables entry not in tableSet: {}, {}",
        cname,
        optimizer::cname(table));
  });

  // singleRowDts must be a subset of tableSet.
  singleRowDts.forEach([&](PlanObjectCP table) {
    VELOX_CHECK(
        tableSet.contains(table),
        "singleRowDts entry not in tableSet: {}, {}",
        cname,
        optimizer::cname(table));
  });

  // orderKeys and orderTypes must have the same size.
  VELOX_CHECK_EQ(
      orderKeys.size(),
      orderTypes.size(),
      "Size mismatch between orderKeys and orderTypes: {}",
      cname);

  // orderKeys must reference only tables in tableSet or 'this'.
  checkTableReferences(orderKeys, "orderKey");

  // Aggregation and window plan consistency.
  if (aggregation) {
    aggregation->checkConsistency(*this);
  }

  if (windowPlan) {
    windowPlan->checkConsistency(*this);
  }

  // having requires aggregation. having must reference only columns produced
  // by the aggregation.
  if (!having.empty()) {
    VELOX_CHECK_NOT_NULL(
        aggregation, "having is not empty but aggregation is null: {}", cname);
    auto aggColumns = PlanObjectSet::fromObjects(aggregation->columns());
    for (auto* expr : having) {
      VELOX_CHECK(
          expr->columns().isSubset(aggColumns),
          "having references columns not produced by aggregation: {}, {}",
          cname,
          expr->toString());
    }
  }

  // conjuncts must reference only tables in tableSet or 'this'.
  checkTableReferences(conjuncts, "conjunct");

  // importedExistences must be a subset of tableSet.
  importedExistences.forEach([&](PlanObjectCP table) {
    VELOX_CHECK(
        tableSet.contains(table),
        "importedExistences entry not in tableSet: {}, {}",
        cname,
        optimizer::cname(table));
  });

  // limit and offset.
  VELOX_CHECK_GE(limit, -1, "limit must be >= -1: {}", cname);
  VELOX_CHECK_NE(limit, 0, "limit must not be 0: {}", cname);
  VELOX_CHECK_GE(offset, 0, "offset must be >= 0: {}", cname);
}

MemoKey DerivedTable::memoKey() const {
  return MemoKey::create(
      this, PlanObjectSet::fromObjects(columns), PlanObjectSet::single(this));
}

void DerivedTable::initializePlans() {
  // Pass 1 (top-down): push conjuncts down the entire DT tree.
  distributeAllConjuncts();

  // Pass 2 (batch): estimate filter selectivity for all base tables. Connectors
  // that support co_estimateStats have their requests issued concurrently.
  queryCtx()->optimization()->estimateAllBaseTableSelectivity(*this);

  // Pass 3 (bottom-up): finalize joins and build initial plans.
  finalizeJoinsAndMakePlans();
}

void DerivedTable::distributeAllConjuncts() {
  distributeConjuncts();

  if (!setOp.has_value()) {
    for (auto* table : tables) {
      if (table->is(PlanType::kDerivedTableNode)) {
        const_cast<PlanObject*>(table)
            ->as<DerivedTable>()
            ->distributeAllConjuncts();
      }
    }
  } else {
    for (auto* child : children) {
      child->distributeAllConjuncts();
    }
  }
}

void DerivedTable::finalizeJoinsAndMakePlans() {
  if (!setOp.has_value()) {
    VELOX_CHECK(!tables.empty());
    VELOX_CHECK(children.empty());

    for (auto* table : tables) {
      if (table->is(PlanType::kDerivedTableNode)) {
        const_cast<PlanObject*>(table)
            ->as<DerivedTable>()
            ->finalizeJoinsAndMakePlans();
      }
    }
  } else {
    VELOX_CHECK(tables.empty());
    VELOX_CHECK(!children.empty());

    for (auto* child : children) {
      child->finalizeJoinsAndMakePlans();
    }

    // Set initial cardinality as the sum of children's cardinalities so that
    // makeInitialPlan can use it when estimating groups for makeDistinct.
    cardinality = 0;
    for (const auto* child : children) {
      cardinality += child->cardinality;
    }
  }

  finalizeJoins();

#ifndef NDEBUG
  checkConsistency();
#endif
  auto plan = queryCtx()->optimization()->makeInitialPlan(*this);
  updateConstraints(*plan);
}

void DerivedTable::updateConstraints(const RelationOp& plan) {
  cardinality = plan.resultCardinality();

  if (plan.relType() == RelType::kTableWrite) {
    // TableWrite columns are the data columns being written, not the DT's
    // output columns (e.g. 'rows').
    return;
  }

  // A DT may have zero output columns (e.g. a child DT whose parent only
  // needs a row count). Only cardinality is useful for such DTs.
  if (outputColumns.empty()) {
    return;
  }

  const auto& planColumns = plan.columns();
  const auto& constraints = plan.constraints();
  VELOX_CHECK_EQ(outputColumns.size(), planColumns.size());
  for (size_t i = 0; i < outputColumns.size(); ++i) {
    auto it = constraints.find(planColumns[i]->id());
    VELOX_CHECK(
        it != constraints.end(),
        "Missing constraint for column: {}",
        planColumns[i]->toString());
    const_cast<Value&>(outputColumns[i]->value()) = it->second;
  }
}

void DerivedTable::finalizeJoins() {
  addImpliedJoins();
  linkTablesToJoins();
  setStartTables();
  for (auto& join : joins) {
    join->guessFanout();
  }
}

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
    } else if (join->isSemi() || join->isAnti()) {
      addImpliedNonInnerEdge(join, tableSet, edges, joins);
    }
  }
}

namespace {

bool isSingleRowDt(PlanObjectCP object) {
  if (object->is(PlanType::kDerivedTableNode)) {
    auto dt = object->as<DerivedTable>();
    // A global aggregation (no grouping keys) always returns exactly one row,
    // but only if there's no HAVING clause that could filter it out.
    return (
        dt->aggregation && dt->aggregation->groupingKeys().empty() &&
        dt->having.empty() && dt->limit != 0 && dt->offset == 0);
  }
  return false;
}

// @return a subset of 'tables' that contain single row tables from
// non-correlated scalar subqueries.
PlanObjectSet findSingleRowDts(
    const PlanObjectSet& tables,
    const JoinEdgeVector& joins) {
  // Remove tables that are joined to other tables.
  auto tablesCopy = tables;
  int32_t numSingle = 0;
  for (auto& join : joins) {
    tablesCopy.erase(join->rightTable());
    for (auto& key : join->leftKeys()) {
      tablesCopy.except(key->allTables());
    }
    // An outer cross join can have a left table with no left keys and no
    // filter.
    if (join->leftTable()) {
      tablesCopy.erase(join->leftTable());
    }
    for (auto& filter : join->filter()) {
      tablesCopy.except(filter->allTables());
    }
  }

  PlanObjectSet singleRowDts;
  tablesCopy.forEach([&](PlanObjectCP object) {
    if (isSingleRowDt(object)) {
      ++numSingle;
      singleRowDts.add(object);
    }
  });

  // If everything is a single row dt, then process these as cross products
  // and not as placed with filters.
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
      // There can be an edge that has no columns for a qualified cross join.
      // Add the end points unconditionally.
      tables.add(join->rightTable());
      if (join->leftTable()) {
        tables.add(join->leftTable());
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
std::pair<DerivedTableCP, JoinEdgeP> makeExistsDtAndJoin(
    const DerivedTable& super,
    PlanObjectCP firstTable,
    float existsFanout,
    PlanObjectVector& existsTables,
    JoinEdgeP existsJoin) {
  VELOX_DCHECK_LT(existsFanout, 1.0f);
  const auto& rightKeys = existsJoin->rightKeys();

  MemoKey existsDtKey = [&]() {
    auto firstExistsTable = rightKeys[0]->singleTable();
    VELOX_CHECK(firstExistsTable);

    PlanObjectSet existsDtColumns;
    existsDtColumns.unionColumns(rightKeys);

    return MemoKey::create(
        firstExistsTable,
        std::move(existsDtColumns),
        PlanObjectSet::fromObjects(existsTables));
  }();

  auto existsDt = [&]() -> DerivedTableCP {
    auto optimization = queryCtx()->optimization();
    auto it = optimization->existenceDts().find(existsDtKey);
    if (it != optimization->existenceDts().end()) {
      return it->second;
    }

    auto newExistsDt = make<DerivedTable>();
    newExistsDt->cname = optimization->newCName("edt");
    newExistsDt->import(
        super, existsDtKey.tables, existsDtKey.firstTable, {}, 1);
    for (auto& key : rightKeys) {
      auto* existsColumn = make<Column>(
          toName(fmt::format("{}.{}", newExistsDt->cname, key->toString())),
          newExistsDt,
          key->value());
      newExistsDt->columns.push_back(existsColumn);
      newExistsDt->exprs.push_back(key);
      newExistsDt->outputColumns.push_back(existsColumn);
    }
    newExistsDt->noImportOfExists = true;
    newExistsDt->initializePlans();
    optimization->existenceDts()[existsDtKey] = newExistsDt;

    return newExistsDt;
  }();

  auto* joinWithDt = JoinEdge::makeExists(firstTable, existsDt);
  joinWithDt->setFanouts(existsFanout, 1);
  for (size_t i = 0; i < existsJoin->numKeys(); ++i) {
    joinWithDt->addEquality(existsJoin->leftKeys()[i], existsDt->columns[i]);
  }
  return std::make_pair(existsDt, joinWithDt);
}
} // namespace

void DerivedTable::copySubset(
    const DerivedTable& super,
    const PlanObjectSet& subsetTables) {
  tableSet = subsetTables;
  tables = subsetTables.toObjects();

  for (auto id : super.joinOrder) {
    if (tableSet.BitSet::contains(id)) {
      joinOrder.push_back(id);
    }
  }

  for (auto join : super.joins) {
    if (subsetTables.contains(join->rightTable()) && join->leftTable() &&
        subsetTables.contains(join->leftTable())) {
      joins.push_back(join);
    }
  }
}

void DerivedTable::addExistences(
    const DerivedTable& super,
    PlanObjectCP primaryTable,
    const std::vector<PlanObjectSet>& existences,
    float existsFanout) {
  for (auto& exists : existences) {
    importedExistences.unionSet(exists);
    auto existsTables = exists.toObjects();
    auto existsJoin = makeExists(primaryTable, exists);
    if (existsTables.size() > 1) {
      auto [existsDt, joinWithDt] = makeExistsDtAndJoin(
          super, primaryTable, existsFanout, existsTables, existsJoin);
      joins.push_back(joinWithDt);
      addTable(existsDt);
    } else {
      joins.push_back(existsJoin);
      VELOX_DCHECK(!existsTables.empty());
      addTable(existsTables[0]);
    }
  }
}

void DerivedTable::import(
    const DerivedTable& super,
    const PlanObjectSet& superTables,
    PlanObjectCP primaryTable) {
  import(super, superTables, primaryTable, {}, 1);
}

void DerivedTable::import(
    const DerivedTable& super,
    const PlanObjectSet& superTables,
    PlanObjectCP primaryTable,
    const std::vector<PlanObjectSet>& existences,
    float existsFanout) {
  VELOX_DCHECK(tables.empty());
  VELOX_DCHECK(joins.empty());
  VELOX_DCHECK(!superTables.empty());
  VELOX_DCHECK(superTables.contains(primaryTable));
  VELOX_DCHECK(!super.setOp.has_value(), "Cannot import from a union DT");

  copySubset(super, superTables);

  if (!existences.empty()) {
    if (!queryCtx()->optimization()->options().syntacticJoinOrder) {
      addExistences(super, primaryTable, existences, existsFanout);
    }
    noImportOfExists = true;
  }

  if (primaryTable->is(PlanType::kDerivedTableNode)) {
    auto& primaryDt = *primaryTable->as<DerivedTable>();
    if (isWrapOnly()) {
      flattenDt(&primaryDt);
    } else {
      pushExistencesIntoSubquery(primaryDt);
    }
  }

  linkTablesToJoins();
  setStartTables();
}

void DerivedTable::importUnionChild(PlanObjectCP child) {
  VELOX_DCHECK(tables.empty());
  VELOX_DCHECK(joins.empty());
  VELOX_DCHECK(child->is(PlanType::kDerivedTableNode));

  tableSet.add(child);
  tables = tableSet.toObjects();
  flattenDt(child->as<DerivedTable>());
  linkTablesToJoins();
  setStartTables();
}

namespace {
template <typename V, typename E>
void eraseFirst(V& set, E element) {
  auto it = std::find(set.begin(), set.end(), element);
  VELOX_CHECK(it != set.end());
  set.erase(it);
}

// Returns a join partner of starting 'joins' where the partner is not in
// 'visited'. Sets 'fullyImported' to false if the partner is not guaranteed
// n:1 reducing or has columns that are projected out.
PlanObjectCP nextJoin(
    PlanObjectCP start,
    const JoinEdgeVector& joins,
    const PlanObjectSet& visited) {
  for (auto& join : joins) {
    auto other = join->otherSide(start);
    if (!other) {
      continue;
    }
    if (visited.contains(other)) {
      continue;
    }
    return other;
  }
  return nullptr;
}

void joinChain(
    PlanObjectCP start,
    const JoinEdgeVector& joins,
    PlanObjectSet visited,
    std::vector<PlanObjectCP>& path) {
  auto next = nextJoin(start, joins, visited);
  if (!next) {
    return;
  }
  visited.add(next);
  path.push_back(next);
  joinChain(next, joins, std::move(visited), path);
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
    case PlanType::kCallExpr:
    case PlanType::kAggregateExpr: {
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

      if (expr->type() == PlanType::kAggregateExpr) {
        const auto* aggregate = expr->as<Aggregate>();
        auto* newCondition =
            replaceInputs(aggregate->condition(), source, target);

        ExprVector newOrderKeys;
        newOrderKeys.reserve(aggregate->orderKeys().size());
        for (const auto* orderKey : aggregate->orderKeys()) {
          newOrderKeys.push_back(replaceInputs(orderKey, source, target));
        }

        anyChange |= newCondition != aggregate->condition();
        anyChange |= newOrderKeys != aggregate->orderKeys();

        if (!anyChange) {
          return expr;
        }

        return make<Aggregate>(
            aggregate->name(),
            aggregate->value(),
            std::move(newChildren),
            functions,
            aggregate->isDistinct(),
            newCondition,
            aggregate->intermediateType(),
            std::move(newOrderKeys),
            aggregate->orderTypes());
      }

      if (!anyChange) {
        return expr;
      }

      const auto* call = expr->as<Call>();
      return make<Call>(
          call->name(), call->value(), std::move(newChildren), functions);
    }
    case PlanType::kFieldExpr: {
      auto* field = expr->as<Field>();
      auto* newBase = replaceInputs(field->base(), source, target);
      if (newBase != field->base()) {
        return make<Field>(field->value().type, newBase, field->field());
      }

      return expr;
    }
    case PlanType::kLambdaExpr: {
      auto* lambda = expr->as<Lambda>();
      auto* body = lambda->body();
      auto* newBody = replaceInputs(body, source, target);
      if (body == newBody) {
        return expr;
      }

      return make<Lambda>(lambda->args(), lambda->value().type, newBody);
    }
    default:
      VELOX_UNREACHABLE(
          "Unexpected expression: {} - {}", expr->typeName(), expr->toString());
  }
}

template <typename T, typename U>
void replaceInputs(ExprVector& exprs, const T& source, const U& target) {
  for (auto i = 0; i < exprs.size(); ++i) {
    exprs[i] = replaceInputs(exprs[i], source, target);
  }
}

template <typename T, typename U>
AggregationPlanCP
replaceInputs(AggregationPlanCP aggregation, const T& source, const U& target) {
  if (!aggregation) {
    return nullptr;
  }

  ExprVector newGroupingKeys = aggregation->groupingKeys();
  replaceInputs(newGroupingKeys, source, target);

  AggregateVector newAggregates;
  newAggregates.reserve(aggregation->aggregates().size());
  for (const auto* aggregate : aggregation->aggregates()) {
    newAggregates.push_back(
        replaceInputs(aggregate, source, target)->template as<Aggregate>());
  }

  if (newGroupingKeys == aggregation->groupingKeys() &&
      newAggregates == aggregation->aggregates()) {
    return aggregation;
  }

  return make<AggregationPlan>(
      std::move(newGroupingKeys),
      std::move(newAggregates),
      aggregation->columns(),
      aggregation->intermediateColumns());
}
} // namespace

void DerivedTable::replaceJoinOutputs(
    const ColumnVector& source,
    const ExprVector& target) {
  replaceInputs(exprs, source, target);
  replaceInputs(conjuncts, source, target);
  replaceInputs(orderKeys, source, target);
  aggregation = replaceInputs(aggregation, source, target);
}

bool DerivedTable::isWrapOnly() const {
  return tables.size() == 1 && tables[0]->is(PlanType::kDerivedTableNode) &&
      !hasLimit() && !hasOrderBy() && conjuncts.empty() && !hasAggregation() &&
      !hasWindow() && exprs.empty();
}

bool DerivedTable::hasNonInnerJoin() const {
  return std::ranges::any_of(
      joins, [](JoinEdgeP join) { return !join->isInner(); });
}

void DerivedTable::ensureSingleRow() {
  // Global aggregation (no grouping keys) without HAVING clause guarantees
  // exactly one output row.
  if (aggregation && aggregation->groupingKeys().empty() && having.empty()) {
    return;
  }

  // A single VALUES row with no filtering or aggregation guarantees exactly
  // one output row.
  if (tables.size() == 1 && tables[0]->is(PlanType::kValuesTableNode) &&
      conjuncts.empty() && !hasAggregation() &&
      tables[0]->as<ValuesTable>()->cardinality() == 1) {
    return;
  }

  enforceSingleRow = true;
}

AggregateCP DerivedTable::exportSingleAggregate(Name markName) {
  VELOX_CHECK(hasAggregation());
  VELOX_CHECK_EQ(0, aggregation->groupingKeys().size());
  VELOX_CHECK_EQ(0, having.size());
  VELOX_CHECK_EQ(1, aggregation->aggregates().size());

  VELOX_CHECK(!hasLimit());
  VELOX_CHECK(!hasOrderBy());

  // Clear outputColumns before export. The old output columns (aggregate
  // results) become invalid once aggregation is cleared. exportExpr and the
  // mark column addition below will repopulate outputColumns with only the
  // columns this DT can produce post-export.
  outputColumns.clear();

  const Value constantBoolean{toType(velox::BOOLEAN()), 1};

  auto* markColumn = make<Column>(markName, this, constantBoolean);

  auto* trueLiteral =
      make<Literal>(constantBoolean, registerVariant(velox::Variant(true)));
  columns.push_back(markColumn);
  exprs.push_back(trueLiteral);
  outputColumns.push_back(markColumn);

  const auto* onlyAgg = aggregation->aggregates().front();

  ExprVector exportedArgs;
  for (const auto* arg : onlyAgg->args()) {
    exportedArgs.push_back(exportExpr(arg));
  }

  ExprVector exportedOrderKeys;
  for (const auto* orderKey : onlyAgg->orderKeys()) {
    exportedOrderKeys.push_back(exportExpr(orderKey));
  }

  ExprCP combinedCondition = markColumn;
  if (onlyAgg->condition() != nullptr) {
    combinedCondition = queryCtx()->optimization()->combineLeftDeep(
        SpecialFormCallNames::kAnd,
        ExprVector{combinedCondition, onlyAgg->condition()});
  }

  aggregation = nullptr;

  return make<Aggregate>(
      onlyAgg->name(),
      onlyAgg->value(),
      exportedArgs,
      onlyAgg->functions(),
      onlyAgg->isDistinct(),
      /*condition=*/combinedCondition,
      onlyAgg->intermediateType(),
      exportedOrderKeys,
      onlyAgg->orderTypes());
}

ExprCP DerivedTable::exportExpr(ExprCP expr) {
  // Only update outputColumns if it has been initialized (by setDtUsedOutput or
  // exportSingleAggregate). When uninitialized (empty), adding a single column
  // would create a partial output specification missing the other columns.
  const bool hasOutputColumns = !outputColumns.empty();

  expr->columns().forEach<Column>([&](auto* column) {
    if (tableSet.contains(column->relation())) {
      if (pushBackUnique(exprs, column)) {
        auto outer = make<Column>(
            column->name(), this, column->value(), column->alias());
        columns.push_back(outer);
        if (hasOutputColumns) {
          outputColumns.push_back(outer);
        }
      }
    }
  });

  return replaceInputs(expr, exprs, columns);
}

void DerivedTable::exportExprs(ExprVector& exprs) {
  for (auto& expr : exprs) {
    expr = exportExpr(expr);
  }
}

ExprCP DerivedTable::importExpr(ExprCP expr) const {
  return replaceInputs(expr, columns, exprs);
}

void DerivedTable::removeTables(
    PlanObjectCP table,
    const std::vector<PlanObjectCP>& chain) {
  eraseFirst(tables, table);
  tableSet.erase(table);
  for (auto& chainTable : chain) {
    eraseFirst(tables, chainTable);
    tableSet.erase(chainTable);
  }
}

bool DerivedTable::validatePushdown(
    const DerivedTable& subquery,
    JoinEdgeVector& validJoins,
    ExprVector& innerKeys) {
  validJoins.clear();
  innerKeys.clear();

  if (subquery.hasLimit() || subquery.hasOrderBy()) {
    return false;
  }

  // Collect valid pushdown columns from the subquery. A join key that
  // references the subquery must resolve to one of these for pushdown
  // to be valid. Grouping keys can be filtered before aggregation;
  // window partition keys can be filtered before the window computation.
  // When both aggregation and window are present, the valid set is the
  // intersection (the key must be safe for both operations).
  PlanObjectSet validPushdownColumns;
  if (subquery.hasAggregation()) {
    validPushdownColumns.unionColumns(subquery.aggregation->groupingKeys());
  }
  if (subquery.hasWindow()) {
    // Compute partition keys common to ALL window functions.
    PlanObjectSet windowKeys;
    bool first = true;
    for (const auto* func : subquery.windowPlan->functions()) {
      if (func->partitionKeys().empty()) {
        windowKeys.clear();
        break;
      }
      if (first) {
        windowKeys.unionColumns(func->partitionKeys());
        first = false;
      } else {
        PlanObjectSet funcColumns;
        funcColumns.unionColumns(func->partitionKeys());
        windowKeys.intersect(funcColumns);
      }
    }
    if (subquery.hasAggregation()) {
      // Both agg and window: intersect.
      validPushdownColumns.intersect(windowKeys);
    } else {
      validPushdownColumns = std::move(windowKeys);
    }
  }

  // No valid pushdown columns but the subquery has an operation boundary
  // (aggregation or window). No join key can be pushed below it.
  if (validPushdownColumns.empty() &&
      (subquery.hasAggregation() || subquery.hasWindow())) {
    return false;
  }

  const auto& outer = subquery.columns;
  const auto& inner = subquery.exprs;

  // Check if all tables can be pushed. The existences come as a single package
  // with a single fanout estimate. Partial pushdown may not achieve the
  // expected cardinality reduction, so we push all or none.
  for (auto& join : joins) {
    auto other = join->otherSide(&subquery);
    if (!other || !tableSet.contains(other)) {
      continue;
    }

    auto side = join->sideOf(&subquery);
    if (side.keys.size() > 1 || !join->filter().empty()) {
      return false;
    }

    // Resolve the join key through column mappings to get the inner
    // expression.
    auto key = replaceInputs(side.keys[0], columns, exprs);
    auto innerKey = replaceInputs(key, outer, inner);
    VELOX_DCHECK(innerKey);

    // Verify the key is a valid pushdown column.
    if (innerKey->is(PlanType::kColumnExpr) &&
        innerKey->as<Column>()->relation() == &subquery) {
      if (!validPushdownColumns.contains(innerKey)) {
        return false;
      }
    }

    auto innerTable = innerKey->singleTable();
    if (!innerTable) {
      return false;
    }

    // Skip if the join key resolves to an unnest table column. To support
    // this, we would need to wrap the base table and its unnest into a new
    // DerivedTable and create the existence semijoin on that DT. Not yet
    // implemented.
    if (innerTable->is(PlanType::kUnnestTableNode)) {
      return false;
    }

    validJoins.push_back(join);
    innerKeys.push_back(innerKey);
  }

  return true;
}

void DerivedTable::pushExistencesIntoSubquery(const DerivedTable& subquery) {
  JoinEdgeVector validJoins;
  ExprVector innerKeys;
  if (!validatePushdown(subquery, validJoins, innerKeys)) {
    return;
  }

  auto initialTables = tables;
  auto* newFirst = make<DerivedTable>(subquery);

  auto* optimization = queryCtx()->optimization();
  const int32_t previousNumJoins = newFirst->joins.size();
  for (auto i = 0; i < validJoins.size(); ++i) {
    auto join = validJoins[i];
    auto innerKey = innerKeys[i];

    auto otherSide = join->sideOf(&subquery, true);
    auto other = otherSide.table;
    auto otherKey = otherSide.keys[0];

    PlanObjectSet visited;
    visited.add(&subquery);
    visited.add(other);
    std::vector<PlanObjectCP> path;
    joinChain(other, joins, visited, path);

    auto innerTable = innerKey->singleTable();
    VELOX_DCHECK_NOT_NULL(innerTable);

    auto makeExistsJoin = [&](PlanObjectCP table, ExprCP key) {
      auto* existsJoin = JoinEdge::makeExists(innerTable, table);
      existsJoin->addEquality(innerKey, key);
      return existsJoin;
    };

    if (path.empty()) {
      if (other->is(PlanType::kDerivedTableNode)) {
        optimization->memo().erase(other->as<DerivedTable>()->memoKey());
        const_cast<PlanObject*>(other)->as<DerivedTable>()->initializePlans();
      }

      newFirst->addTable(other);
      newFirst->joins.push_back(makeExistsJoin(other, otherKey));
    } else {
      auto* chainDt = make<DerivedTable>();
      chainDt->cname = toName(optimization->newCName("rdt"));

      auto chainSet = PlanObjectSet::fromObjects(path);
      chainSet.add(other);

      auto column = make<Column>(
          optimization->newCName("ec"), chainDt, otherKey->value());
      chainDt->columns.push_back(column);
      chainDt->exprs.push_back(otherKey);

      chainDt->import(*this, chainSet, other);
      chainDt->outputColumns = chainDt->columns;
      chainDt->initializePlans();

      newFirst->addTable(chainDt);
      newFirst->joins.push_back(makeExistsJoin(chainDt, column));
    }

    removeTables(other, path);
  }

  for (auto i = previousNumJoins; i < newFirst->joins.size(); ++i) {
    newFirst->joins[i]->guessFanout();
  }

  VELOX_CHECK_EQ(tables.size(), 1);
  newFirst->importedExistences.unionObjects(initialTables);
  tables[0] = newFirst;
  flattenDt(newFirst);
}

void DerivedTable::flattenDt(const DerivedTable* dt) {
  tables = dt->tables;
  cname = dt->cname;
  tableSet = dt->tableSet;
  joins = dt->joins;
  joinOrder = dt->joinOrder;
  conjuncts = dt->conjuncts;
  columns = dt->columns;
  exprs = dt->exprs;
  outputColumns = dt->outputColumns;
  importedExistences.unionSet(dt->importedExistences);
  aggregation = dt->aggregation;
  windowPlan = dt->windowPlan;
  having = dt->having;
  limit = dt->limit;
  offset = dt->offset;
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

// Check if a non-UNION DT has a limit or one of the children of a UNION DT
// has a limit.
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

// Factors out common conjuncts from OR disjuncts and extracts per-table
// filters that can be pushed down into individual table scans.
//
// Examples:
//    (x AND y) OR (x AND z) => x AND (y OR z)
//    (x AND y) OR (x AND y) => x AND y
//    A OR (A AND B) => A
//    (n1='FR' AND n2='DE') OR (n1='DE' AND n2='FR')
//        => n1 IN ('FR', 'DE') AND n2 IN ('DE', 'FR')
//           AND ((n1='FR' AND n2='DE') OR (n1='DE' AND n2='FR'))
//
// Returns extracted conjuncts (common factors and per-table filters) to be
// added alongside the OR. The caller must also handle the OR itself based on
// two mutually exclusive signals:
//
//  - 'orReplacement' set: The OR simplifies to a new expression (after
//     deduplication or after factoring out common conjuncts). Replace the OR
//     with 'orReplacement'.
//  - 'orSubsumed' true: A disjunct was fully subsumed by the common factors,
//     e.g. A OR (A AND B) => A. The OR is trivially true. Drop the OR.
//  - Neither: The OR is unchanged. Keep it as-is.
//
// This pattern appears in TPC-H q7 and q9.
ExprVector extractCommonFromDisjuncts(
    ExprVector& disjuncts,
    ExprCP* orReplacement,
    bool& orSubsumed) {
  *orReplacement = nullptr;
  orSubsumed = false;

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
    *orReplacement = disjuncts[0];
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

  // If any disjunct was fully subsumed by the common factors, the OR is
  // trivially true. Return just the common factors.
  for (const auto& disjunct : flat) {
    if (disjunct.empty()) {
      orSubsumed = true;
      return result;
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
    *orReplacement =
        optimization->combineLeftDeep(SpecialFormCallNames::kOr, ands);
  }

  return result;
}

// Extracts implied conjuncts and removes duplicates from 'conjuncts' and
// updates 'conjuncts'. Extracted conjuncts may allow extra pushdown or allow
// create join edges. May be called repeatedly, each e.g. after pushing down
// conjuncts from outer DTs.
void expandConjuncts(ExprVector& conjuncts) {
  bool changed = false;
  auto firstUnprocessed = 0;
  do {
    changed = false;

    auto end = conjuncts.size();
    for (auto i = firstUnprocessed; i < end; ++i) {
      const auto& conjunct = conjuncts[i];
      if (isCallExpr(conjunct, SpecialFormCallNames::kOr) &&
          !conjunct->containsNonDeterministic()) {
        ExprVector flat;
        flattenAll(conjunct, SpecialFormCallNames::kOr, flat);

        ExprCP orReplacement = nullptr;
        bool orSubsumed = false;
        ExprVector common =
            extractCommonFromDisjuncts(flat, &orReplacement, orSubsumed);
        VELOX_CHECK(
            !orReplacement || !orSubsumed,
            "orReplacement and orSubsumed are mutually exclusive");
        if (orReplacement) {
          changed = true;
          conjuncts[i] = orReplacement;
        } else if (orSubsumed) {
          // A disjunct was fully subsumed by the common conjuncts, e.g.
          // A OR (A AND B) => A. The OR is trivially true and can be dropped.
          changed = true;
          conjuncts.erase(conjuncts.begin() + i);
          --i;
          --end;
        }

        if (!common.empty()) {
          changed = true;
          conjuncts.insert(conjuncts.end(), common.begin(), common.end());
        }
      }
    }
    firstUnprocessed = end;
  } while (changed);
}

// Converts a LEFT join to an INNER join, preserving the join keys.
// Used when a filter on the right side columns eliminates rows where the
// right side is NULL, making the LEFT join equivalent to an INNER join. Note:
// The caller must handle the optional filter from the left join separately
// (e.g., add it to conjuncts) as it is not copied to the new inner join.
JoinEdgeP toInnerJoin(JoinEdgeCP leftJoin) {
  auto innerJoin =
      JoinEdge::makeInner(leftJoin->leftTable(), leftJoin->rightTable());

  for (int j = 0; j < leftJoin->leftKeys().size(); j++) {
    innerJoin->addEquality(leftJoin->leftKeys()[j], leftJoin->rightKeys()[j]);
  }

  return innerJoin;
}

// Converts a FULL join to a LEFT join, preserving the filter, join keys, and
// right-side output columns/expressions.
// Used when a filter on the left side columns eliminates rows where the left
// side is NULL (i.e., right-only rows don't survive the filter).
JoinEdgeP toLeftJoin(JoinEdgeCP fullJoin) {
  JoinEdge::Spec joinSpec{
      .filter = fullJoin->filter(),
      .leftOptional = false,
      .rightOptional = true,
  };

  joinSpec.rightColumns = fullJoin->rightColumns();
  joinSpec.rightExprs = fullJoin->rightExprs();

  auto leftJoin = make<JoinEdge>(
      fullJoin->leftTable(),
      fullJoin->rightTable(),
      std::move(joinSpec),
      logical_plan::JoinType::kLeft);

  for (int j = 0; j < fullJoin->leftKeys().size(); j++) {
    leftJoin->addEquality(fullJoin->leftKeys()[j], fullJoin->rightKeys()[j]);
  }

  return leftJoin;
}

// Converts a FULL join to a "normalized" RIGHT join by swapping the table
// sides. This eliminates rows where the left side (of the original join) is
// NULL, keeping only rows where the right side matches or has no match on the
// left. The normalization swaps left and right tables so the result can be
// stored using the standard LEFT join representation (with swapped sides).
JoinEdgeP toNormalizedRightJoin(JoinEdgeCP fullJoin) {
  JoinEdge::Spec joinSpec{
      .filter = fullJoin->filter(),
      .leftOptional = false,
      .rightOptional = true,
  };

  joinSpec.rightColumns = fullJoin->leftColumns();
  joinSpec.rightExprs = fullJoin->leftExprs();

  auto leftJoin = make<JoinEdge>(
      fullJoin->rightTable(),
      fullJoin->leftTable(),
      std::move(joinSpec),
      logical_plan::JoinType::kRight);

  for (int j = 0; j < fullJoin->leftKeys().size(); j++) {
    leftJoin->addEquality(fullJoin->rightKeys()[j], fullJoin->leftKeys()[j]);
  }

  return leftJoin;
}

// Returns true if 'name' is a ranking window function (row_number, rank,
// dense_rank).
bool isRankingFunction(Name name, const FunctionNames& names) {
  return name == names.rowNumber || name == names.rank ||
      name == names.denseRank;
}

// Extracts the ranking limit from a predicate of the form column <= N,
// column < N, or column = 1, where 'windowColumn' is the output of
// 'windowFunction' which must be a ranking function (row_number, rank, or
// dense_rank). Returns std::nullopt if the predicate does not match.
std::optional<int32_t> extractRankingLimit(
    ExprCP expr,
    const FunctionNames& names,
    ColumnCP windowColumn,
    WindowFunctionCP windowFunction) {
  if (!expr->is(PlanType::kCallExpr)) {
    return std::nullopt;
  }

  const auto* call = expr->as<Call>();
  const auto& args = call->args();
  if (args.size() != 2 || args[0] != windowColumn ||
      !args[1]->is(PlanType::kLiteralExpr)) {
    return std::nullopt;
  }

  if (!isRankingFunction(windowFunction->name(), names)) {
    return std::nullopt;
  }

  const auto& literal = args[1];
  const auto& literalType = literal->value().type;
  if (!literalType->isTinyint() && !literalType->isSmallint() &&
      !literalType->isInteger() && !literalType->isBigint()) {
    return std::nullopt;
  }

  auto value = integerValue(&literal->as<Literal>()->literal());
  auto callName = call->name();
  // ranking_func() <= N → limit = N. Valid when N > 0.
  if (callName == names.lte && value > 0) {
    return static_cast<int32_t>(value);
  }

  // ranking_func() < N → limit = N - 1. Valid when N > 1.
  if (callName == names.lt && value > 1) {
    return static_cast<int32_t>(value - 1);
  }

  // ranking_func() = 1 → limit = 1. TopNRowNumber returns rows 1..limit,
  // so = N for N > 1 cannot be converted to a limit.
  if (callName == names.equality && value == 1) {
    return 1;
  }

  return std::nullopt;
}

} // namespace

bool DerivedTable::isPartitionKeyFilter(ExprCP imported) const {
  VELOX_CHECK_NOT_NULL(windowPlan);
  for (const auto* func : windowPlan->functions()) {
    if (func->partitionKeys().empty()) {
      return false;
    }
    PlanObjectSet partitionKeyColumns;
    partitionKeyColumns.unionColumns(func->partitionKeys());
    if (!imported->columns().isSubset(partitionKeyColumns)) {
      return false;
    }
  }
  return true;
}

bool DerivedTable::isZeroRows() const {
  return tables.size() == 1 && tables[0]->is(PlanType::kValuesTableNode) &&
      tables[0]->as<ValuesTable>()->cardinality() == 0;
}

void DerivedTable::clearState() {
  columns.clear();
  exprs.clear();
  outputColumns.clear();
  tables.clear();
  tableSet = PlanObjectSet{};
  joins.clear();
  conjuncts.clear();
  having.clear();
  aggregation = nullptr;
  windowPlan = nullptr;
  children.clear();
  setOp = std::nullopt;
  orderKeys.clear();
  orderTypes.clear();
  limit = -1;
  offset = 0;
}

void DerivedTable::makeEmpty() {
  auto* emptyData = &registerVariant(velox::Variant::array({}))->array();

  // Build ValuesTable matching outputColumns schema.
  auto savedOutputColumns = outputColumns;

  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;
  names.reserve(savedOutputColumns.size());
  types.reserve(savedOutputColumns.size());
  for (auto* column : savedOutputColumns) {
    names.push_back(std::string(column->name()));
    types.push_back(toTypePtr(column->value().type));
  }

  auto* rowType = toType(velox::ROW(std::move(names), std::move(types)));
  auto* valuesTable = make<ValuesTable>(rowType, emptyData);
  valuesTable->cname = queryCtx()->optimization()->newCName("vt");

  ExprVector newExprs;
  for (auto* column : savedOutputColumns) {
    auto* valuesColumn = make<Column>(
        column->name(), valuesTable, Value(column->value().type, 0));
    valuesTable->columns.push_back(valuesColumn);
    newExprs.push_back(valuesColumn);
  }

  clearState();

  columns = savedOutputColumns;
  exprs = std::move(newExprs);
  outputColumns = savedOutputColumns;
  addTable(valuesTable);
}

DerivedTable* DerivedTable::wrapChildWithFilter(
    DerivedTable* child,
    ExprCP conjunct) {
  auto* wrapper = make<DerivedTable>();
  wrapper->cname = queryCtx()->optimization()->newCName("wdt");

  // In a set operation, all children share Column objects whose relation_
  // points to the parent set DT. Create new intermediate columns with relation_
  // = child so that the wrapper's importExpr translates the conjunct into
  // child-column space. Without this, the conjunct would reference the set DT
  // (not in the wrapper's tables), causing an infinite pushdown loop between
  // the wrapper and the set DT.
  auto sharedColumns = child->columns;

  ColumnVector childColumns;
  childColumns.reserve(sharedColumns.size());
  for (const auto* col : sharedColumns) {
    childColumns.push_back(
        make<Column>(col->name(), child, col->value(), col->alias()));
  }

  child->columns = childColumns;
  child->outputColumns = childColumns;

  wrapper->addTable(child);
  wrapper->columns = sharedColumns;
  wrapper->exprs.assign(childColumns.begin(), childColumns.end());
  wrapper->outputColumns = std::move(sharedColumns);
  wrapper->conjuncts.push_back(wrapper->importExpr(conjunct));
  return wrapper;
}

bool DerivedTable::addFilter(ExprCP conjunct) {
  // TODO: Support pushing conjuncts below LIMIT by wrapping in a DT.
  if (dtHasLimit(*this)) {
    return false;
  }

  if (setOp.has_value()) {
    for (auto i = 0; i < children.size(); ++i) {
      if (!children[i]->addFilter(conjunct)) {
        // The child cannot accept the filter (e.g. it has a window or limit).
        // Wrap the child in a new DT that holds the filter above it.
        children[i] = wrapChildWithFilter(children[i], conjunct);
      }
    }

    if (setOp.value() == logical_plan::SetOperation::kUnionAll) {
      // Drop children that became zero-rows after filter pushdown.
      std::erase_if(
          children, [](const auto* child) { return child->isZeroRows(); });

      if (children.size() == 1) {
        auto* only = children[0];
        children.clear();
        setOp = std::nullopt;
        flattenDt(only);
      } else if (children.empty()) {
        makeEmpty();
      }
    }

    return true;
  }

  auto imported = importExpr(conjunct);

  // Fold constant filters. Eliminate constant-true. Replace the DT with an
  // empty ValuesTable for constant-false (the branch produces no rows).
  if (auto* folded = queryCtx()->optimization()->tryFoldConstant(imported)) {
    if (!isConstantTrue(folded)) {
      makeEmpty();
    }
    return true;
  }

  if (windowPlan) {
    if (isPartitionKeyFilter(imported)) {
      // Filter on partition keys of all window functions can be pushed below.
    } else {
      if (windowPlan->functions().size() == 1 &&
          imported->columns().size() == 1 &&
          imported->columns().contains(windowPlan->columns()[0])) {
        if (auto rankingLimit = extractRankingLimit(
                imported,
                queryCtx()->functionNames(),
                windowPlan->columns()[0],
                windowPlan->functions()[0])) {
          windowPlan = windowPlan->withRankingLimit(*rankingLimit);
          return true;
        }
      }
      return false;
    }
  }

  if (aggregation) {
    having.push_back(imported);
  } else {
    conjuncts.push_back(imported);
  }
  return true;
}

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

  tryConvertOuterJoins(allowNondeterministic);

  for (auto i = 0; i < conjuncts.size(); ++i) {
    auto* conjunct = conjuncts[i];

    // No pushdown of non-deterministic except if only pushdown target is a
    // union all.
    if (conjunct->containsNonDeterministic() && !allowNondeterministic) {
      continue;
    }

    PlanObjectSet tableSet = conjunct->allTables();
    std::vector<PlanObjectP> tables;
    tableSet.forEachMutable([&](auto table) { tables.push_back(table); });
    if (tables.size() == 1) {
      if (tables[0] == this) {
        continue; // the conjunct depends on containing dt, like grouping or
                  // existence flags. Leave in place.
      }

      if (!tryPushdownConjunct(conjunct, tables[0], changedDts)) {
        continue;
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
              conjunct, tables[0], tables[1], left, right) &&
          !tables[0]->is(PlanType::kUnnestTableNode) &&
          !tables[1]->is(PlanType::kUnnestTableNode)) {
        auto join = findJoin(this, tables, true);
        if (join->isInner() && !join->isUnnest()) {
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
}

void DerivedTable::tryConvertOuterJoins(bool allowNondeterministic) {
  for (auto i = 0; i < conjuncts.size(); ++i) {
    auto* conjunct = conjuncts[i];

    // No pushdown of non-deterministic except if only pushdown target is a
    // union all.
    if (conjunct->containsNonDeterministic() && !allowNondeterministic) {
      continue;
    }

    if (conjunct->containsFunction(FunctionSet::kNonDefaultNullBehavior)) {
      continue;
    }

    for (auto joinIndex = 0; joinIndex < joins.size(); ++joinIndex) {
      auto join = joins[joinIndex];
      if (!join->leftOptional() && !join->rightOptional()) {
        continue;
      }

      auto rightJoinColumns = PlanObjectSet::fromObjects(join->rightColumns());
      auto leftJoinColumns = PlanObjectSet::fromObjects(join->leftColumns());

      // Check if conjunct references any column from the optional side of the
      // join. If so, and the conjunct has default null behavior, it is
      // null-rejecting for that side, even if it also references other tables.
      bool referencesRight =
          conjunct->columns().hasIntersection(rightJoinColumns);
      bool referencesLeft =
          conjunct->columns().hasIntersection(leftJoinColumns);

      // Special case: If conjunct references BOTH sides of a FULL join, it
      // rejects NULLs on both sides, so convert directly to INNER join.
      if (referencesRight && referencesLeft && join->leftOptional() &&
          join->rightOptional()) {
        joins[joinIndex] = toInnerJoin(join);

        conjuncts.insert(
            conjuncts.end(), join->filter().begin(), join->filter().end());

        replaceJoinOutputs(join->rightColumns(), join->rightExprs());
        replaceJoinOutputs(join->leftColumns(), join->leftExprs());
        break;
      }

      if (referencesRight) {
        if (!join->leftOptional()) {
          joins[joinIndex] = toInnerJoin(join);

          conjuncts.insert(
              conjuncts.end(), join->filter().begin(), join->filter().end());
        } else {
          // Convert FULL join to (normalized) RIGHT join. The filter
          // references right-side columns and has default null behavior, so
          // it eliminates left-only rows (which have NULLs for right
          // columns). This is equivalent to a RIGHT join. The result is
          // normalized by swapping sides to avoid storing RIGHT joins
          // directly.
          joins[joinIndex] = toNormalizedRightJoin(join);
        }

        replaceJoinOutputs(join->rightColumns(), join->rightExprs());
        break;
      }

      if (referencesLeft) {
        // Convert FULL join to LEFT join. The filter references left-side
        // columns and has default null behavior, so it eliminates right-only
        // rows (which have NULLs for left columns). This is equivalent to a
        // LEFT join.
        joins[joinIndex] = toLeftJoin(join);

        replaceJoinOutputs(join->leftColumns(), join->leftExprs());
        break;
      }
    }
  }
}

bool DerivedTable::tryPushdownConjunct(
    ExprCP conjunct,
    PlanObjectP table,
    std::vector<DerivedTableP>& changedDts) {
  if (table->is(PlanType::kValuesTableNode)) {
    return false; // ValuesTable does not have filter push-down.
  }

  if (table->is(PlanType::kUnnestTableNode)) {
    return false; // UnnestTable does not have filter push-down.
  }

  if (conjunct->containsFunction(FunctionSet::kWindow)) {
    return false;
  }

  if (table->is(PlanType::kDerivedTableNode)) {
    auto innerDt = table->as<DerivedTable>();
    if (!innerDt->addFilter(conjunct)) {
      return false;
    }

    if (innerDt->setOp.has_value()) {
      for (auto* child : innerDt->children) {
        pushBackUnique(changedDts, child);
      }
    } else {
      pushBackUnique(changedDts, innerDt);
    }
    return true;
  }

  VELOX_CHECK(table->is(PlanType::kTableNode));
  table->as<BaseTable>()->addFilter(conjunct);
  return true;
}

std::string DerivedTable::toString() const {
  return DerivedTablePrinter::toText(*this);
}

void DerivedTable::addJoinedBy(JoinEdgeP join) {
  pushBackUnique(joinedBy, join);
}

} // namespace facebook::axiom::optimizer
