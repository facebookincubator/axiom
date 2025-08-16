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

#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/connectors/ConnectorSplitSource.h"
#include "axiom/runner/LocalRunner.h"
#include "velox/common/base/AsyncSource.h"

namespace facebook::velox::optimizer {

namespace {
// Counter for making unique query ids for sampling.
int64_t sampleQueryCounter;

Value bigintValue() {
  return Value(toType(BIGINT()), 1);
}

Value intValue() {
  return Value(toType(INTEGER()), 1);
}

ExprCP bigintLit(int64_t n) {
  return make<Literal>(
      bigintValue(), queryCtx()->registerVariant(std::make_unique<variant>(n)));
}

ExprCP intLit(int32_t n) {
  return make<Literal>(
      intValue(), queryCtx()->registerVariant(std::make_unique<variant>(n)));
}

// Returns an int64 hash with low 28 bits set.
ExprCP makeHash(ExprCP expr) {
  switch (expr->value().type->kind()) {
    case TypeKind::BIGINT:
      break;
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
      expr = make<Call>(
          toName("cast"), bigintValue(), ExprVector{expr}, FunctionSet());
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
  andArgs.push_back(bigintLit(0x7fffffff));
  return make<Call>(
      toName("bitwise_and"), bigintValue(), andArgs, FunctionSet());
}

ExprCP scaleTo32(ExprCP value) {
  ExprVector andArgs;
  andArgs.push_back(value);
  andArgs.push_back(bigintLit(0x7fffffff));
  return make<Call>(
      toName("bitwise_and"), bigintValue(), andArgs, FunctionSet());
}

ExprCP mul(ExprCP a, int64_t b) {
  return scaleTo32(make<Call>(
      toName("multiply"),
      bigintValue(),
      ExprVector{a, bigintLit(b)},
      FunctionSet()));
}

ExprCP rightShift(ExprCP a, int32_t s) {
  return make<Call>(
      toName("bitwise_right_shift"),
      bigintValue(),
      ExprVector{a, intLit(s)},
      FunctionSet());
}

ExprCP xor64(ExprCP a, ExprCP b) {
  return make<Call>(
      toName("bitwise_xor"), bigintValue(), ExprVector{a, b}, FunctionSet());
}

std::shared_ptr<core::QueryCtx> sampleQueryCtx(
    std::shared_ptr<core::QueryCtx> original) {
  std::unordered_map<std::string, std::string> empty;
  return core::QueryCtx::create(
      original->executor(),
      core::QueryConfig(std::move(empty)),
      original->connectorSessionProperties(),
      original->cache(),
      original->pool()->shared_from_this(),
      nullptr,
      fmt::format("sample:{}", ++sampleQueryCounter));
}

using KeyFreq = folly::F14FastMap<uint32_t, uint32_t>;

std::shared_ptr<axiom::runner::Runner> prepareSampleRunner(
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
  auto index = base->chooseLeafIndex()[0];
  auto* scan = make<TableScan>(
      nullptr,
      TableScan::outputDistribution(base, index, columns),
      base,
      index,
      index->table->cardinality,
      columns);

  ExprCP hash = makeHash(keys[0]);
  if (keys.size() == 1) {
    hash = mul(hash, 1815531889);
  } else {
    auto kMul = 0xeb382d69ULL;

    for (auto i = 1; i < keys.size(); ++i) {
      auto other = makeHash(keys[i]);
      auto a = mul(xor64(hash, other), kMul);
      a = xor64(a, rightShift(a, 15));
      auto b = mul(xor64(other, a), kMul);
      b = xor64(b, rightShift(b, 13));
      hash = mul(b, kMul);
    }
  }
  ColumnCP hashCol =
      make<Column>(toName("hash"), nullptr, bigintValue(), nullptr);
  RelationOpPtr proj =
      make<Project>(scan, ExprVector{hash}, ColumnVector{hashCol});
  ExprCP hashMod = make<Call>(
      toName("mod"),
      bigintValue(),
      ExprVector{hashCol, bigintLit(mod)},
      FunctionSet());
  ExprCP filterExpr = make<Call>(
      toName("lt"),
      Value(toType(BOOLEAN()), 1),
      ExprVector{hashMod, bigintLit(lim)},
      FunctionSet());
  RelationOpPtr filter = make<Filter>(proj, ExprVector{filterExpr});

  auto plan = queryCtx()->optimization()->toVeloxPlan(filter);
  return std::make_shared<axiom::runner::LocalRunner>(
      plan.plan,
      sampleQueryCtx(queryCtx()->optimization()->queryCtxShared()),
      std::make_shared<connector::ConnectorSplitSourceFactory>());
}

std::unique_ptr<KeyFreq> runJoinSample(
    axiom::runner::Runner& runner,
    int32_t maxRows = 0) {
  auto result = std::make_unique<folly::F14FastMap<uint32_t, uint32_t>>();
  int32_t rowCount = 0;
  while (auto rows = runner.next()) {
    rowCount += rows->size();
    auto h = rows->childAt(0)->as<FlatVector<int64_t>>();
    for (auto i = 0; i < h->size(); ++i) {
      if (!h->isNullAt(i)) {
        ++(*result)[static_cast<uint32_t>(h->valueAt(i))];
      }
    }
    if (maxRows && rowCount > maxRows) {
      runner.abort();
      break;
    }
  }
  runner.waitForCompletion(1000000);
  return result;
}

float freqs(KeyFreq& l, KeyFreq& r) {
  if (l.empty()) {
    return 0;
  }
  float hits = 0;
  for (auto& pair : l) {
    auto it = r.find(pair.first);
    if (it != r.end()) {
      hits += it->second;
    }
  }
  return hits / l.size();
}

float keyCardinality(const ExprVector& keys) {
  float card = 1;
  for (auto& key : keys) {
    card *= key->value().cardinality;
  }
  return card;
}
} // namespace

std::pair<float, float> sampleJoin(
    SchemaTableCP left,
    const ExprVector& leftKeys,
    SchemaTableCP right,
    const ExprVector& rightKeys) {
  uint64_t leftRows = left->numRows();
  uint64_t rightRows = right->numRows();
  auto leftCard = keyCardinality(leftKeys);
  auto rightCard = keyCardinality(rightKeys);
  int32_t fraction = 10000;
  if (leftRows < 10000 && rightRows < 10000) {
    // sample all.
  } else if (leftCard > 10000 && rightCard > 10000) {
    // Keys have many values, sample a fraction.
    auto smaller = std::min(leftRows, rightRows);
    float ratio = smaller / 10000.0;
    fraction = std::max<int32_t>(2, 10000 / ratio);
  } else {
    return std::make_pair(0, 0);
  }

  auto leftRunner = prepareSampleRunner(left, leftKeys, 10000, fraction);
  auto rightRunner = prepareSampleRunner(right, rightKeys, 10000, fraction);
  auto leftRun = std::make_shared<AsyncSource<KeyFreq>>(
      [leftRunner]() { return runJoinSample(*leftRunner); });
  auto rightRun = std::make_shared<AsyncSource<KeyFreq>>(
      [rightRunner]() { return runJoinSample(*rightRunner); });
  auto executor = queryCtx()->optimization()->queryCtxShared()->executor();
  if (executor) {
    executor->add([leftRun]() { leftRun->prepare(); });
    executor->add([rightRun]() { rightRun->prepare(); });
  }

  auto leftFreq = leftRun->move();
  auto rightFreq = rightRun->move();
  return std::make_pair(
      freqs(*rightFreq, *leftFreq), freqs(*leftFreq, *rightFreq));
}

} // namespace facebook::velox::optimizer
