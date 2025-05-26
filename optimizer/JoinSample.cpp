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

#include "optimizer/Plan.h" //@manual
#include "velox/runner/LocalRunner.h"
#include "optimizer/connectors/ConnectorSplitSource.h" //@manual

namespace facebook::velox::optimizer {

// Counter for making unique query ids for sampling.
int64_t sampleQueryCounter;

Value bigintValue() {
  return Value(toType(BIGINT()), 1);
}

ExprCP bigintLit(int64_t n) {
  return make<Literal>(
      bigintValue(), queryCtx()->registerVariant(std::make_unique<variant>(n)));
}

// Returns an int64 hash with low 28 bits set.
ExprCP makeHash(ExprCP expr) {
  switch (expr->value().type->kind()) {
    case TypeKind::BIGINT:
      break;
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
      expr = make<Call>(toName("cast"), bigintValue(), ExprVector{expr}, FunctionSet());
      break;
    default: {
      ExprVector castArgs;
      castArgs.push_back(make<Call>(
          toName("cast"),
          Value(toType(VARCHAR()), 1),
          castArgs,
          FunctionSet()));

      ExprVector args;
      args.push_back(make<Call>(
          toName("cast"),
          Value(toType(VARBINARY()), 1),
          castArgs,
          FunctionSet()));
      ExprVector final;
      final.push_back(make<Call>(
          toName("crc32"), Value(toType(INTEGER()), 1), args, FunctionSet()));
      expr = make<Call>(toName("cast"), bigintValue(), final, FunctionSet());
    }
  }
  ExprVector andArgs;
  andArgs.push_back(expr);
  andArgs.push_back(bigintLit(0xfffffff));
  return make<Call>(
      toName("bitwise_and"), bigintValue(), andArgs, FunctionSet());
}

std::shared_ptr<core::QueryCtx> sampleQueryCtx(
					       connector::Connector* connector) {
  return connector->metadata()->makeQueryCtx(
      fmt::format("sample:{}", ++sampleQueryCounter));
}

std::shared_ptr<runner::Runner> prepareSanpleRunner(
    SchemaTableCP table,
    const ExprVector& keys,
    int64_t mod,
    int64_t lim) {
  auto base = make<BaseTable>();
  base->schemaTable = table;
  PlanObjectSet sampleColumns;
  for (auto& e : keys) {
    sampleColumns.unionSet(e->columns());
  }
  ColumnVector columns;
  sampleColumns.forEach(
      [&](PlanObjectCP c) { columns.push_back(c->as<Column>()); });
  auto index = chooseLeafIndex(base)[0];
  auto* scan = make<TableScan>(
      nullptr,
      TableScan::outputDistribution(base, index, columns),
      base,
      index,
      index->distribution().cardinality,
      columns);
  ExprCP hash = makeHash(keys[0]);
  for (auto i = 1; i < keys.size(); ++i) {
    auto other = makeHash(keys[i]);
    hash = make<Call>(
        toName("bitwise_xor"),
        bigintValue(),
        ExprVector{hash, other},
        FunctionSet());
  }
  hash = make<Call>(
      toName("multiply"),
      bigintValue(),
      ExprVector{hash, bigintLit(1815531889)},
      FunctionSet());
  ColumnCP hashCol =
    make<Column>(toName("hash"), nullptr, bigintValue(), nullptr);
  RelationOpPtr proj = make<Project>(scan, ExprVector{hash}, ColumnVector{hashCol});
  ExprCP hashMod = make<Call>(
      toName("mod"),
      bigintValue(),
      ExprVector{hash, bigintLit(mod)},
      FunctionSet());
  ExprCP filterExpr = make<Call>(
				 toName("lt"), Value(toType(BOOLEAN()), 1), ExprVector{hashMod, bigintLit(lim)}, FunctionSet());
  RelationOpPtr filter = make<Filter>(proj, ExprVector{filterExpr});

  runner::MultiFragmentPlan::Options options;
  auto plan = queryCtx()->optimization()->toVeloxPlan(filter, options);
  auto* layout = table->columnGroups[0]->layout;
  auto connector = layout->connector();
  return std::make_shared<runner::LocalRunner>(
      plan,
      sampleQueryCtx(connector),
      std::make_shared<connector::ConnectorSplitSourceFactory>());
}

std::pair<float, float> sampleJoin(
    SchemaTableCP left,
    const ColumnVector& leftKeys,
    SchemaTableCP right,
    const ColumnVector& rightColumns) {}

  folly::F14FastMap<uint32_t, uint32_t> runJoinSample(runner::Runner& runner) {
  folly::F14FastMap<uint32_t, uint32_t> result;
  while (auto rows = runner.next()) {
    auto h = rows->childAt(0)->as<FlatVector<int64_t>>();
    for (auto i = 0; i < h->size(); ++i) {
      if (!h->isNullAt(i)) {
        ++result[static_cast<uint32_t>(h->valueAt(i))];
      }
    }
  }
  return result;
}

} // namespace facebook::velox::optimizer
