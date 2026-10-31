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

#include "axiom/optimizer/v2/LowerRepeatPass.h"

#include "axiom/optimizer/v2/AppendAll.h"
#include "axiom/optimizer/v2/EstimateProvider.h"
#include "axiom/optimizer/v2/ExprFactory.h"
#include "axiom/optimizer/v2/NodeRewriter.h"
#include "axiom/optimizer/v2/PlanSubstitutions.h"

namespace facebook::axiom::optimizer::v2 {
namespace {

// A Repeat as seen from the side its walks leave from.
struct Walk {
  NodeCP from;
  ExprVector fromKeys;
  // Hop keys of the vertex a hop leaves from and the vertex it reaches, in
  // the direction of the walk.
  ExprVector hopFrom;
  ExprVector hopTo;
  // Null when the walk ends at a row of 'from'.
  NodeCP to;
  ExprVector toKeys;
  // True when the walk leaves from the Repeat's end, so it meets the hops in
  // reverse order.
  bool fromEnd{false};
};

// The columns of the hop the result reads: 'values' for their value on the
// walk's one hop, and 'collected' for their values on all hops, collected into
// arrays. 'arrayTypes' holds the array type of each of 'collected'.
struct HopColumns {
  ColumnVector values;
  ColumnVector collected;
  std::vector<TypeCP> arrayTypes;

  static HopColumns of(const Repeat& repeat) {
    const auto hopColumns =
        PlanObjectSet::fromObjects(repeat.hop()->outputColumns());
    const auto& sources = repeat.effectiveSourceColumns();
    HopColumns result;
    PlanObjectSet seen;
    for (size_t i = 0; i < sources.size(); ++i) {
      ColumnCP source = sources[i];
      if (!hopColumns.contains(source) || seen.contains(source)) {
        continue;
      }
      seen.add(source);
      const auto* type = repeat.outputColumns()[i]->value().type;
      if (type->equivalent(*source->value().type)) {
        result.values.push_back(source);
      } else {
        result.collected.push_back(source);
        result.arrayTypes.push_back(type);
      }
    }
    return result;
  }
};

// The walks' columns: the carried columns, the current vertex, the values of
// the hop columns the result reads, the arrays it collects, and the hop count,
// in that order. Walks of at most one hop count their hops only when the
// result reads the count; walks that loop read no hop values.
struct WalkState {
  ColumnVector columns;
  ColumnVector carried;
  ColumnVector vertex;
  ColumnVector hopValues;
  ColumnVector arrays;
  ColumnCP hops{nullptr};

  static WalkState of(
      ColumnVector columns,
      size_t numCarried,
      size_t numKeys,
      size_t numHopValues,
      size_t numArrays) {
    WalkState state;
    auto it = columns.begin();
    state.carried.assign(it, it + numCarried);
    it += numCarried;
    state.vertex.assign(it, it + numKeys);
    it += numKeys;
    state.hopValues.assign(it, it + numHopValues);
    it += numHopValues;
    state.arrays.assign(it, it + numArrays);
    it += numArrays;
    if (it != columns.end()) {
      state.hops = *it;
    }
    state.columns = std::move(columns);
    return state;
  }
};

// Appends the columns 'exprs' read that 'columns' does not hold yet.
void appendColumnsRead(const ExprVector& exprs, ColumnVector& columns) {
  PlanObjectSet read;
  read.unionColumns(exprs);
  read.eraseAll(columns);
  read.forEach<Column>([&](ColumnCP column) { columns.push_back(column); });
}

// Returns a new column with an unknown value for each of 'columns'.
ColumnVector createWithUnknownValues(const ColumnVector& columns) {
  ColumnVector copies;
  copies.reserve(columns.size());
  for (ColumnCP column : columns) {
    copies.push_back(Column::createWithUnknownValue(column));
  }
  return copies;
}

ExprVector toExprs(const ColumnVector& columns) {
  return ExprVector(columns.begin(), columns.end());
}

class Lowering : public NodeRewriter<NoContext> {
 public:
  explicit Lowering(Builder& builder)
      : NodeRewriter(builder), exprs_(builder) {}

  using NodeRewriter::rewrite;

 protected:
  NodeCP rewriteRepeat(const Repeat* node, NoContext& context) override {
    // Lowers the Repeats inside the inputs first.
    const auto* repeat =
        NodeRewriter::rewriteRepeat(node, context)->as<Repeat>();
    return lower(*repeat, walkOf(*repeat));
  }

 private:
  // Walks from the end when it is present and has fewer estimated rows.
  Walk walkOf(const Repeat& repeat) {
    bool fromEnd{false};
    if (repeat.end() != nullptr) {
      const auto startRows = estimates_.estimate(repeat.start()).cardinality;
      const auto endRows = estimates_.estimate(repeat.end()).cardinality;
      fromEnd =
          startRows.has_value() && endRows.has_value() && *endRows < *startRows;
    }
    if (fromEnd) {
      return {
          .from = repeat.end(),
          .fromKeys = repeat.endKeys(),
          .hopFrom = repeat.hopDestinationKeys(),
          .hopTo = repeat.hopSourceKeys(),
          .to = repeat.start(),
          .toKeys = repeat.startKeys(),
          .fromEnd = true,
      };
    }
    return {
        .from = repeat.start(),
        .fromKeys = repeat.startKeys(),
        .hopFrom = repeat.hopSourceKeys(),
        .hopTo = repeat.hopDestinationKeys(),
        .to = repeat.end(),
        .toKeys = repeat.endKeys(),
    };
  }

  ExprCP bigint(int64_t value) {
    return builder().makeLiteral(
        velox::Variant(value), toType(velox::BIGINT()));
  }

  // Returns an empty array of each of 'hops.arrayTypes'.
  ExprVector emptyArrays(const HopColumns& hops) {
    ExprVector arrays;
    for (TypeCP type : hops.arrayTypes) {
      arrays.push_back(builder().makeEmptyArray(type));
    }
    return arrays;
  }

  // Returns the FixedPoint over 'walk' and the walks it accumulates, filtered
  // and shaped as 'repeat's output.
  NodeCP lower(const Repeat& repeat, const Walk& walk) {
    // The walks carry the columns of 'from' the result reads, the current
    // vertex, the hop columns the result reads, and, when they loop or the
    // result reads it, the number of hops so far.
    ColumnVector carried =
        walk.from->outputsAmong(repeat.effectiveSourceColumns());
    if (walk.to == nullptr) {
      appendColumnsRead(walk.toKeys, carried);
    }
    const auto hops = HopColumns::of(repeat);
    const size_t numKeys = walk.fromKeys.size();
    const bool loops = repeat.maxHops() > 1;
    const bool counts = loops || repeat.hopCount() != nullptr;
    NodeCP anchor = makeAnchor(walk, carried, hops, counts);
    const auto& anchorColumns = anchor->outputColumns();

    WalkState state;
    NodeCP walks{nullptr};
    if (loops) {
      state = WalkState::of(
          createWithUnknownValues(anchorColumns),
          carried.size(),
          numKeys,
          /*numHopValues=*/0,
          hops.collected.size());
      walks = makeFixedPoint(repeat, walk, anchor, anchorColumns, state, hops);
    } else {
      auto [node, columns] = makeShortWalks(
          repeat, walk, anchor, anchorColumns, carried, hops, counts);
      walks = node;
      state = WalkState::of(
          std::move(columns),
          carried.size(),
          numKeys,
          hops.values.size(),
          hops.collected.size());
    }
    return makeResult(repeat, walk, walks, state, carried, hops);
  }

  // Returns the walks of zero hops: 'carried', the vertex 'from' leaves from,
  // NULL for each of 'hops.values', an empty array for each of
  // 'hops.collected', and, when 'counts', a hop count of 0.
  NodeCP makeAnchor(
      const Walk& walk,
      const ColumnVector& carried,
      const HopColumns& hops,
      bool counts) {
    ExprVector exprs = toExprs(carried);
    ColumnVector columns = carried;
    for (ExprCP key : walk.fromKeys) {
      exprs.push_back(key);
      columns.push_back(Column::create("walk_vertex", key->value()));
    }
    for (ColumnCP column : hops.values) {
      exprs.push_back(builder().makeNull(column->value().type));
      columns.push_back(Column::create("walk_hop", column->value()));
    }
    for (ExprCP array : emptyArrays(hops)) {
      exprs.push_back(array);
      columns.push_back(Column::create("walk_array", array->value()));
    }
    if (counts) {
      exprs.push_back(bigint(0));
      columns.push_back(
          Column::create("walk_hops", Value(toType(velox::BIGINT()))));
    }
    return builder().make<Project>(
        {walk.from, std::move(exprs), std::move(columns)});
  }

  // Returns 'walks' with at least 'minHops' hops, joined with 'to' or ending
  // at 'from' when 'to' is null, shaped as 'repeat's output. 'carried' are the
  // columns of 'from' and 'hops' those of the hop that 'state' carries.
  NodeCP makeResult(
      const Repeat& repeat,
      const Walk& walk,
      NodeCP walks,
      const WalkState& state,
      const ColumnVector& carried,
      const HopColumns& hops) {
    ExprVector predicates;
    if (repeat.minHops() > 0) {
      predicates.push_back(
          exprs_.makeGreaterThanOrEqual(state.hops, bigint(repeat.minHops())));
    }
    PlanSubstitutions toState;
    for (size_t i = 0; i < carried.size(); ++i) {
      toState.add(carried[i], state.carried[i]);
    }
    for (size_t i = 0; i < hops.values.size(); ++i) {
      toState.add(hops.values[i], state.hopValues[i]);
    }
    for (size_t i = 0; i < hops.collected.size(); ++i) {
      toState.add(hops.collected[i], state.arrays[i]);
    }
    if (walk.to == nullptr) {
      for (size_t i = 0; i < state.vertex.size(); ++i) {
        predicates.push_back(exprs_.makeEq(
            state.vertex[i], toState.apply(walk.toKeys[i], exprs_)));
      }
    }
    if (!predicates.empty()) {
      walks = builder().make<Filter>({walks, std::move(predicates)});
    }

    if (walk.to != nullptr) {
      ColumnVector joined = state.carried;
      appendAll(joined, state.hopValues);
      appendAll(joined, state.arrays);
      if (repeat.hopCount() != nullptr) {
        joined.push_back(state.hops);
      }
      appendAll(joined, walk.to->outputsAmong(repeat.effectiveSourceColumns()));
      walks = builder().make<Join>({
          .left = walks,
          .right = walk.to,
          .joinType = velox::core::JoinType::kInner,
          .leftKeys = toExprs(state.vertex),
          .rightKeys = walk.toKeys,
          .outputColumns = std::move(joined),
      });
    }

    ExprVector outputExprs;
    outputExprs.reserve(repeat.outputColumns().size());
    for (ColumnCP source : repeat.effectiveSourceColumns()) {
      outputExprs.push_back(
          source == repeat.hopCount() ? state.hops
                                      : toState.apply(source, exprs_));
    }
    return builder().make<Project>(
        {walks, std::move(outputExprs), repeat.outputColumns()});
  }

  // Returns the FixedPoint that accumulates the walks from 'anchor' into
  // 'state'.
  NodeCP makeFixedPoint(
      const Repeat& repeat,
      const Walk& walk,
      NodeCP anchor,
      const ColumnVector& anchorColumns,
      const WalkState& state,
      const HopColumns& hops) {
    const Name name = queryCtx()->newName("walk");
    return builder().make<FixedPoint>({
        .anchor = anchor,
        .step = makeStep(repeat, walk, name, state, hops, anchorColumns),
        .name = name,
        .outputColumns = state.columns,
        .sourceColumns = anchorColumns,
        // The step adds no rows once every walk has 'maxHops' hops, which
        // the loop sees one iteration later.
        .maxIterations = repeat.maxHops() + 1,
    });
  }

  // Returns the walks of at most one hop and their columns: 'anchor' for zero
  // hops, and 'from' joined with the hop for one. 'minHops' is 0: one hop
  // exactly is a join.
  std::pair<NodeCP, ColumnVector> makeShortWalks(
      const Repeat& repeat,
      const Walk& walk,
      NodeCP anchor,
      const ColumnVector& anchorColumns,
      const ColumnVector& carried,
      const HopColumns& hops,
      bool counts) {
    NodeVector legs{anchor};
    QGVector<ColumnVector> legColumns{anchorColumns};
    if (repeat.maxHops() == 1) {
      NodeCP hopped = extendByOneHop(
          repeat,
          walk,
          walk.from,
          walk.fromKeys,
          carried,
          hops.values,
          hops.collected,
          emptyArrays(hops),
          counts ? bigint(0) : nullptr,
          anchorColumns);
      legs.push_back(hopped);
      legColumns.push_back(hopped->outputColumns());
    }
    if (legs.size() == 1) {
      return {legs.front(), std::move(legColumns.front())};
    }
    ColumnVector columns = createWithUnknownValues(anchorColumns);
    return {
        builder().make<UnionAll>(
            {std::move(legs), std::move(legColumns), columns}),
        columns};
  }

  // Extends the latest walks that have fewer than 'maxHops' hops by one hop.
  NodeCP makeStep(
      const Repeat& repeat,
      const Walk& walk,
      Name name,
      const WalkState& state,
      const HopColumns& hops,
      const ColumnVector& anchorColumns) {
    ColumnVector readColumns;
    readColumns.reserve(state.columns.size());
    for (ColumnCP column : state.columns) {
      readColumns.push_back(Column::createWithUnknownValue(column));
    }
    const auto read = WalkState::of(
        readColumns,
        state.carried.size(),
        state.vertex.size(),
        state.hopValues.size(),
        state.arrays.size());
    NodeCP latest = builder().make<WorkingTable>({
        .name = name,
        .stateColumns = state.columns,
        .outputColumns = read.columns,
        .readMode = WorkingTableReadMode::kLatestDelta,
    });
    NodeCP extensible = builder().make<Filter>(
        {latest, {exprs_.makeLessThan(read.hops, bigint(repeat.maxHops()))}});
    return extendByOneHop(
        repeat,
        walk,
        extensible,
        toExprs(read.vertex),
        read.carried,
        /*hopValues=*/{},
        hops.collected,
        toExprs(read.arrays),
        read.hops,
        anchorColumns);
  }

  // Joins 'walks' with the hop on 'vertex' and the hop's source, keeping all of
  // the walks' columns so the join's input needs no projection. Returns the
  // walks' 'carried' columns, the hop's destination as the current vertex,
  // 'hopValues', 'arrays' extended by the hop's value of each of
  // 'collected', and 'hops' plus one when set, as new columns named like
  // 'anchorColumns'.
  NodeCP extendByOneHop(
      const Repeat& repeat,
      const Walk& walk,
      NodeCP walks,
      ExprVector vertex,
      const ColumnVector& carried,
      const ColumnVector& hopValues,
      const ColumnVector& collected,
      const ExprVector& arrays,
      ExprCP hops,
      const ColumnVector& anchorColumns) {
    ColumnVector joined = walks->outputColumns();
    appendColumnsRead(walk.hopTo, joined);
    appendColumnsRead(toExprs(hopValues), joined);
    appendColumnsRead(toExprs(collected), joined);
    NodeCP extended = builder().make<Join>({
        .left = walks,
        .right = repeat.hop(),
        .joinType = velox::core::JoinType::kInner,
        .leftKeys = std::move(vertex),
        .rightKeys = walk.hopFrom,
        .outputColumns = std::move(joined),
    });

    ExprVector exprs = toExprs(carried);
    appendAll(exprs, walk.hopTo);
    appendAll(exprs, toExprs(hopValues));
    // A walk from the end meets the hops in reverse, so it prepends.
    for (size_t i = 0; i < arrays.size(); ++i) {
      exprs.push_back(
          walk.fromEnd ? exprs_.makeArrayPrepend(collected[i], arrays[i])
                       : exprs_.makeArrayAppend(arrays[i], collected[i]));
    }
    if (hops != nullptr) {
      exprs.push_back(exprs_.makeBigintAdd(hops, bigint(1)));
    }
    return builder().make<Project>(
        {extended, std::move(exprs), createWithUnknownValues(anchorColumns)});
  }

  ExprFactory exprs_;
  EstimateProvider estimates_;
};

} // namespace

NodeCP LowerRepeatPass::run(NodeCP root, Builder& builder) {
  return Lowering(builder).rewrite(root);
}

} // namespace facebook::axiom::optimizer::v2
