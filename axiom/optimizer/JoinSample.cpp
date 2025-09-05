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

#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/connectors/ConnectorSplitSource.h"
#include "axiom/runner/LocalRunner.h"
#include "velox/common/base/AsyncSource.h"
#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"

namespace facebook::velox::optimizer {

namespace {

template <typename TExec>
struct HashMix {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const int64_t& firstHash,
      const arg_type<Variadic<int64_t>>& moreHashes) {
    result = firstHash;
    for (const auto& hash : moreHashes) {
      result = bits::hashMix(result, hash.value());
    }
  }
};

template <typename... T>
ExprCP makeCall(std::string_view name, const TypePtr& type, T... inputs) {
  return make<Call>(
      toName(name),
      Value(toType(type), 1),
      ExprVector{inputs...},
      FunctionSet{});
}

ExprCP makeCast(const TypePtr& type, ExprCP expr) {
  return makeCall(SpecialFormCallNames::kCast, type, expr);
}

Value bigintValue() {
  return Value(toType(BIGINT()), 1);
}

ExprCP bigintLit(int64_t n) {
  return make<Literal>(
      bigintValue(), queryCtx()->registerVariant(std::make_unique<Variant>(n)));
}

// Returns an int64 hash with low 28 bits set.
ExprCP makeHash(ExprCP expr) {
  switch (expr->value().type->kind()) {
    case TypeKind::BIGINT:
      break;
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
      expr = makeCast(BIGINT(), expr);
      break;
    default: {
      auto toVarchar = makeCast(VARCHAR(), expr);
      auto toVarbinary = makeCast(VARBINARY(), toVarchar);
      auto crc32 = makeCall("crc32", INTEGER(), toVarbinary);
      expr = makeCast(BIGINT(), crc32);
    }
  }

  return makeCall("bitwise_and", BIGINT(), expr, bigintLit(0x7fffffff));
}

std::shared_ptr<core::QueryCtx> sampleQueryCtx(const core::QueryCtx& original) {
  std::atomic<int64_t> kQueryCounter;

  std::unordered_map<std::string, std::string> empty;
  return core::QueryCtx::create(
      original.executor(),
      core::QueryConfig(std::move(empty)),
      original.connectorSessionProperties(),
      original.cache(),
      original.pool()->shared_from_this(),
      nullptr,
      fmt::format("sample:{}", ++kQueryCounter));
}

std::shared_ptr<axiom::runner::Runner> prepareSampleRunner(
    SchemaTableCP table,
    const ExprVector& keys,
    int64_t mod,
    int64_t lim) {
  static folly::once_flag kInitialized;
  static const char* kHashMix = "$internal$hash_mix";

  folly::call_once(kInitialized, []() {
    registerFunction<HashMix, int64_t, int64_t, Variadic<int64_t>>({kHashMix});
  });

  auto base = make<BaseTable>();
  base->schemaTable = table;

  PlanObjectSet sampleColumns;
  for (auto key : keys) {
    sampleColumns.unionSet(key->columns());
  }

  auto columns = sampleColumns.toObjects<Column>();
  auto index = base->chooseLeafIndex()[0];
  auto* scan = make<TableScan>(
      nullptr,
      TableScan::outputDistribution(base, index, columns),
      base,
      index,
      index->table->cardinality,
      columns);

  ExprVector hashes;
  hashes.reserve(keys.size());
  for (const auto& key : keys) {
    hashes.emplace_back(makeHash(key));
  }

  ExprCP hash =
      make<Call>(toName(kHashMix), bigintValue(), hashes, FunctionSet{});

  ColumnCP hashColumn = make<Column>(toName("hash"), nullptr, hash->value());
  RelationOpPtr project =
      make<Project>(scan, ExprVector{hash}, ColumnVector{hashColumn});

  // (hash % mod) < lim
  ExprCP hashMod = makeCall("mod", BIGINT(), hashColumn, bigintLit(mod));
  ExprCP filterExpr = makeCall("lt", BOOLEAN(), hashMod, bigintLit(lim));
  RelationOpPtr filter = make<Filter>(project, ExprVector{filterExpr});

  auto plan = queryCtx()->optimization()->toVeloxPlan(filter);
  return std::make_shared<axiom::runner::LocalRunner>(
      plan.plan,
      sampleQueryCtx(*queryCtx()->optimization()->veloxQueryCtx()),
      std::make_shared<connector::ConnectorSplitSourceFactory>());
}

// Maps hash value to number of times it appears in a table.
using KeyFreq = folly::F14FastMap<uint32_t, uint32_t>;

std::unique_ptr<KeyFreq> runJoinSample(
    axiom::runner::Runner& runner,
    int32_t maxRows = 0) {
  auto result = std::make_unique<folly::F14FastMap<uint32_t, uint32_t>>();

  int32_t rowCount = 0;
  while (auto rows = runner.next()) {
    rowCount += rows->size();
    auto hashes = rows->childAt(0)->as<FlatVector<int64_t>>();
    for (auto i = 0; i < hashes->size(); ++i) {
      if (!hashes->isNullAt(i)) {
        ++(*result)[static_cast<uint32_t>(hashes->valueAt(i))];
      }
    }
    if (maxRows && rowCount > maxRows) {
      runner.abort();
      break;
    }
  }

  runner.waitForCompletion(1'000'000);
  return result;
}

float freqs(const KeyFreq& left, const KeyFreq& right) {
  if (left.empty()) {
    return 0;
  }

  float hits = 0;
  for (const auto& [hash, _] : left) {
    auto it = right.find(hash);
    if (it != right.end()) {
      hits += static_cast<float>(it->second);
    }
  }
  return hits / static_cast<float>(left.size());
}

float keyCardinality(const ExprVector& keys) {
  float cardinality = 1;
  for (auto& key : keys) {
    cardinality *= key->value().cardinality;
  }
  return cardinality;
}
} // namespace

std::pair<float, float> sampleJoin(
    SchemaTableCP left,
    const ExprVector& leftKeys,
    SchemaTableCP right,
    const ExprVector& rightKeys) {
  const uint64_t leftRows = left->numRows();
  const uint64_t rightRows = right->numRows();

  const auto leftCard = keyCardinality(leftKeys);
  const auto rightCard = keyCardinality(rightKeys);

  static const auto kMaxCardinality = 10'000;

  int32_t fraction = kMaxCardinality;
  if (leftRows < kMaxCardinality && rightRows < kMaxCardinality) {
    // Sample all.
  } else if (leftCard > kMaxCardinality && rightCard > kMaxCardinality) {
    // Keys have many values, sample a fraction.
    const auto smaller = static_cast<float>(std::min(leftRows, rightRows));
    const float ratio = smaller / (float)kMaxCardinality;
    fraction =
        static_cast<int32_t>(std::max(2.F, (float)kMaxCardinality / ratio));
  } else {
    return std::make_pair(0, 0);
  }

  auto leftRunner =
      prepareSampleRunner(left, leftKeys, kMaxCardinality, fraction);
  auto rightRunner =
      prepareSampleRunner(right, rightKeys, kMaxCardinality, fraction);

  auto leftRun = std::make_shared<AsyncSource<KeyFreq>>(
      [leftRunner]() { return runJoinSample(*leftRunner); });
  auto rightRun = std::make_shared<AsyncSource<KeyFreq>>(
      [rightRunner]() { return runJoinSample(*rightRunner); });

  if (auto executor = queryCtx()->optimization()->veloxQueryCtx()->executor()) {
    executor->add([leftRun]() { leftRun->prepare(); });
    executor->add([rightRun]() { rightRun->prepare(); });
  }

  auto leftFreq = leftRun->move();
  auto rightFreq = rightRun->move();
  return std::make_pair(
      freqs(*rightFreq, *leftFreq), freqs(*leftFreq, *rightFreq));
}

} // namespace facebook::velox::optimizer
