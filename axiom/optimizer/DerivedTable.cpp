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

MemoKey memoKey(const DerivedTable& dt) {
  return MemoKey::create(
      &dt, PlanObjectSet::fromObjects(dt.columns), PlanObjectSet::single(&dt));
}
} // namespace

void DerivedTable::initializePlans() {
  // Pre-order (top-down): push conjuncts to children.
  distributeConjuncts();

  const bool isUnion = setOp.has_value() &&
      (setOp.value() == logical_plan::SetOperation::kUnion ||
       setOp.value() == logical_plan::SetOperation::kUnionAll);

  if (!isUnion) {
    VELOX_CHECK(!tables.empty());
    VELOX_CHECK(children.empty());

    // Recurse to child DerivedTables in 'tables'.
    for (auto* table : tables) {
      if (table->is(PlanType::kDerivedTableNode)) {
        const_cast<PlanObject*>(table)->as<DerivedTable>()->initializePlans();
      }
    }
  } else {
    VELOX_CHECK(tables.empty());
    VELOX_CHECK(!children.empty());

    // Recurse to children of set operations (UNION, UNION ALL).
    for (auto* child : children) {
      child->initializePlans();
    }
  }

  // Post-order (bottom-up): finalize joins and compute plans.
  finalizeJoins();

  auto optimization = queryCtx()->optimization();
  auto& memo = optimization->memo();

  if (!isUnion) {
    PlanState state(*optimization, this);
    state.targetExprs.unionObjects(exprs);

    optimization->makeJoins(state);

    auto plan = state.plans.best()->op;
    this->cardinality = plan->resultCardinality();

    MemoKey key = memoKey(*this);
    memo.insert(key, std::move(state.plans));

  } else {
    for (const auto* childDt : children) {
      MemoKey childKey = memoKey(*childDt);

      auto* plans = memo.find(childKey);
      VELOX_CHECK(
          plans != nullptr, "Expecting to find a plan for union branch");

      const auto& childPlan = *plans->best()->op;
      this->cardinality += childPlan.resultCardinality();

      for (size_t i = 0; i < columns.size(); ++i) {
        const auto& setValue = columns[i]->value().cardinality;
        const auto& childValue = childPlan.columns()[i]->value().cardinality;
        const_cast<float&>(setValue) += childValue;
      }
    }
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
std::pair<DerivedTableP, JoinEdgeP> makeExistsDtAndJoin(
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
    for (auto& column : rightKeys) {
      existsDtColumns.unionColumns(column);
    }
    return MemoKey::create(
        firstExistsTable,
        std::move(existsDtColumns),
        PlanObjectSet::fromObjects(existsTables));
  }();

  auto optimization = queryCtx()->optimization();
  auto it = optimization->existenceDts().find(existsDtKey);
  DerivedTableP existsDt{};
  if (it == optimization->existenceDts().end()) {
    existsDt = make<DerivedTable>();
    existsDt->cname = optimization->newCName("edt");
    existsDt->import(super, existsDtKey.firstTable, existsDtKey.tables, {});
    for (auto& key : rightKeys) {
      auto* existsColumn = make<Column>(
          toName(fmt::format("{}.{}", existsDt->cname, key->toString())),
          existsDt,
          key->value());
      existsDt->columns.push_back(existsColumn);
      existsDt->exprs.push_back(key);
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
  }

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

JoinEdgeP importedDtJoin(JoinEdgeP join, DerivedTableP dt, ExprCP innerKey) {
  auto left = innerKey->singleTable();
  VELOX_CHECK(left);
  auto otherKey = dt->columns[0];
  auto* newJoin = JoinEdge::makeExists(left, dt);
  newJoin->addEquality(innerKey, otherKey);
  return newJoin;
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
  joinChain(next, joins, visited, path);
}

JoinEdgeP importedJoin(JoinEdgeP join, PlanObjectCP other, ExprCP innerKey) {
  auto left = innerKey->singleTable();
  VELOX_CHECK(left);
  auto otherKey = join->sideOf(other).keys[0];
  auto* newJoin = JoinEdge::makeExists(left, other);
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
      exprs.empty();
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

  const Value constantBoolean{toType(velox::BOOLEAN()), 1};

  auto* markColumn = make<Column>(markName, this, constantBoolean);

  auto* trueLiteral =
      make<Literal>(constantBoolean, registerVariant(velox::Variant(true)));
  columns.push_back(markColumn);
  exprs.push_back(trueLiteral);

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
  expr->columns().forEach<Column>([&](auto* column) {
    if (tableSet.contains(column->relation())) {
      if (pushBackUnique(exprs, column)) {
        auto outer = make<Column>(
            column->name(), this, column->value(), column->alias());
        columns.push_back(outer);
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

ExprCP DerivedTable::importExpr(ExprCP expr) {
  return replaceInputs(expr, columns, exprs);
}

void DerivedTable::importJoinsIntoFirstDt(const DerivedTable* firstDt) {
  if (isWrapOnly()) {
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

  auto* newFirst = make<DerivedTable>(*firstDt->as<DerivedTable>());

  const int32_t previousNumJoins = newFirst->joins.size();
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
      // If the join key is an aggregate, the join can't be moved below the
      // agg.
      continue;
    }

    auto otherSide = join->sideOf(firstDt, true);

    PlanObjectSet visited;
    visited.add(firstDt);
    visited.add(other);
    std::vector<PlanObjectCP> path;
    joinChain(other, joins, visited, path);
    if (path.empty()) {
      if (other->is(PlanType::kDerivedTableNode)) {
        queryCtx()->optimization()->memo().erase(
            memoKey(*other->as<DerivedTable>()));
        const_cast<PlanObject*>(other)->as<DerivedTable>()->makeInitialPlan();
      }

      newFirst->addTable(other);
      newFirst->joins.push_back(importedJoin(join, other, innerKey));
    } else {
      auto* chainDt = make<DerivedTable>();
      chainDt->cname = toName(queryCtx()->optimization()->newCName("rdt"));

      PlanObjectSet chainSet;
      chainSet.add(other);
      chainSet.unionObjects(path);
      chainDt->makeProjection(otherSide.keys);
      chainDt->import(*this, other, chainSet, {});
      chainDt->makeInitialPlan();
      newFirst->addTable(chainDt);
      newFirst->joins.push_back(importedDtJoin(join, chainDt, innerKey));
    }
    eraseFirst(tables, other);
    tableSet.erase(other);
    for (auto& table : path) {
      eraseFirst(tables, table);
      tableSet.erase(table);
    }
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
  importedExistences.unionSet(dt->importedExistences);
  aggregation = dt->aggregation;
  having = dt->having;
  limit = dt->limit;
  offset = dt->offset;
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
              conjunct, tables[0], tables[1], left, right)) {
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

  if (table->is(PlanType::kDerivedTableNode)) {
    // Translate the column names and add the condition to the conjuncts in
    // the dt. If the inner is a set operation, add the filter to children.
    auto innerDt = table->as<DerivedTable>();
    if (dtHasLimit(*innerDt)) {
      return false;
    }

    auto numChildren = innerDt->children.empty() ? 1 : innerDt->children.size();
    for (auto childIdx = 0; childIdx < numChildren; ++childIdx) {
      auto childDt = numChildren == 1 ? innerDt : innerDt->children[childIdx];
      auto imported = childDt->importExpr(conjunct);
      if (childDt->aggregation) {
        childDt->having.push_back(imported);
      } else {
        childDt->conjuncts.push_back(imported);
      }

      pushBackUnique(changedDts, childDt);
    }
    return true;
  }

  VELOX_CHECK(table->is(PlanType::kTableNode));
  table->as<BaseTable>()->addFilter(conjunct);
  return true;
}

void DerivedTable::makeInitialPlan() {
  finalizeJoins();

  auto optimization = queryCtx()->optimization();
  PlanState state(*optimization, this);
  state.targetExprs.unionObjects(exprs);

  optimization->makeJoins(state);

  auto plan = state.plans.best()->op;
  this->cardinality = plan->resultCardinality();

  MemoKey key = memoKey(*this);
  optimization->memo().insert(key, std::move(state.plans));
}

std::string DerivedTable::toString() const {
  return DerivedTablePrinter::toText(*this);
}

void DerivedTable::addJoinedBy(JoinEdgeP join) {
  pushBackUnique(joinedBy, join);
}

} // namespace facebook::axiom::optimizer
