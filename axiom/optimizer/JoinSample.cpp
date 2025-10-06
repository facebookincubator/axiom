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
#include "axiom/runner/LocalRunner.h"
#include "velox/common/base/AsyncSource.h"
#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"

namespace facebook::axiom::optimizer {
namespace {

// Returns an int64 hash with low 28 bits set.
template <typename TExec>
struct Hash {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<velox::Any>& value) {
    result = value.hash() & 0x7fffffff;
  }
};

template <typename TExec>
struct HashMix {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const int64_t& firstHash,
      const arg_type<velox::Variadic<int64_t>>& moreHashes) {
    result = firstHash;
    for (const auto& hash : moreHashes) {
      result = velox::bits::hashMix(result, hash.value());
    }
  }
};

template <typename TExec>
struct Sample {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  FOLLY_ALWAYS_INLINE void call(
      bool& result,
      const int64_t& value,
      const int64_t& mod,
      const int64_t& limit) {
    result = (value % mod) < limit;
  }
};

template <typename... T>
ExprCP
makeCall(std::string_view name, const velox::TypePtr& type, T... inputs) {
  return make<Call>(
      toName(name),
      Value(toType(type), 1),
      ExprVector{inputs...},
      FunctionSet{});
}

Value bigintValue() {
  return Value(toType(velox::BIGINT()), 1);
}

ExprCP bigintLit(int64_t n) {
  return make<Literal>(
      bigintValue(),
      queryCtx()->registerVariant(std::make_unique<velox::Variant>(n)));
}

std::shared_ptr<velox::core::QueryCtx> sampleQueryCtx(
    const velox::core::QueryCtx& original) {
  std::atomic<int64_t> kQueryCounter;

  std::unordered_map<std::string, std::string> empty;
  return velox::core::QueryCtx::create(
      original.executor(),
      velox::core::QueryConfig(std::move(empty)),
      original.connectorSessionProperties(),
      original.cache(),
      original.pool()->shared_from_this(),
      nullptr,
      fmt::format("sample:{}", ++kQueryCounter));
}

std::shared_ptr<runner::Runner> prepareSampleRunner(
    SchemaTableCP table,
    const ExprVector& keys,
    int64_t mod,
    int64_t lim) {
  static folly::once_flag kInitialized;
  static const char* kHash = "$internal$hash";
  static const char* kHashMix = "$internal$hash_mix";
  static const char* kSample = "$internal$sample";

  folly::call_once(kInitialized, []() {
    velox::registerFunction<Hash, int64_t, velox::Any>({kHash});
    velox::
        registerFunction<HashMix, int64_t, int64_t, velox::Variadic<int64_t>>(
            {kHashMix});
    velox::registerFunction<
        Sample,
        bool,
        int64_t,
        velox::Constant<int64_t>,
        velox::Constant<int64_t>>({kSample});
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
    hashes.emplace_back(makeCall(kHash, velox::BIGINT(), key));
  }

  ExprCP hash =
      make<Call>(toName(kHashMix), bigintValue(), hashes, FunctionSet{});

  ColumnCP hashColumn = make<Column>(toName("hash"), nullptr, hash->value());
  RelationOpPtr project = make<Project>(
      scan, ExprVector{hash}, ColumnVector{hashColumn}, /*redundant=*/false);

  // (hash % mod) < lim
  ExprCP filterExpr = makeCall(
      kSample, velox::BOOLEAN(), hashColumn, bigintLit(mod), bigintLit(lim));
  RelationOpPtr filter = make<Filter>(project, ExprVector{filterExpr});

  auto plan = queryCtx()->optimization()->toVeloxPlan(filter);
  return std::make_shared<runner::LocalRunner>(
      plan.plan, sampleQueryCtx(*queryCtx()->optimization()->veloxQueryCtx()));
}

// Maps hash value to number of times it appears in a table.
using KeyFreq = folly::F14FastMap<uint32_t, uint32_t>;

std::unique_ptr<KeyFreq> runJoinSample(
    runner::Runner& runner,
    int32_t maxRows = 0) {
  auto result = std::make_unique<folly::F14FastMap<uint32_t, uint32_t>>();

  int32_t rowCount = 0;
  while (auto rows = runner.next()) {
    rowCount += rows->size();
    auto hashes = rows->childAt(0)->as<velox::FlatVector<int64_t>>();
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

  auto leftRun = std::make_shared<velox::AsyncSource<KeyFreq>>(
      [leftRunner]() { return runJoinSample(*leftRunner); });
  auto rightRun = std::make_shared<velox::AsyncSource<KeyFreq>>(
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

} // namespace facebook::axiom::optimizer
