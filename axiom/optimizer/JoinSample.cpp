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

#include <folly/executors/CPUThreadPoolExecutor.h>
#include "axiom/logical_plan/ExprApi.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/Plan.h"
#include "axiom/optimizer/QueryGraphContext.h"
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
  return {toType(velox::BIGINT()), 1};
}

ExprCP bigintLit(int64_t n) {
  return make<Literal>(bigintValue(), registerVariant(n));
}

struct SampleContext {
  std::shared_ptr<folly::CPUThreadPoolExecutor> executor;
  std::shared_ptr<velox::memory::MemoryPool> samplePool;
  std::shared_ptr<velox::core::QueryCtx> queryCtx;

  // Constructor
  SampleContext(
      std::shared_ptr<folly::CPUThreadPoolExecutor> executor_,
      std::shared_ptr<velox::memory::MemoryPool> samplePool_,
      std::shared_ptr<velox::core::QueryCtx> queryCtx_)
      : executor(std::move(executor_)),
        samplePool(std::move(samplePool_)),
        queryCtx(std::move(queryCtx_)) {}

  // Move constructor
  SampleContext(SampleContext&& other) noexcept
      : executor(std::move(other.executor)),
        samplePool(std::move(other.samplePool)),
        queryCtx(std::move(other.queryCtx)) {
    other.executor = nullptr;
    other.samplePool = nullptr;
    other.queryCtx = nullptr;
  }

  // Move assignment operator
  SampleContext& operator=(SampleContext&& other) noexcept {
    if (this != &other) {
      // Clean up current resources first
      if (executor) {
        executor->join();
      }
      queryCtx.reset();
      samplePool.reset();
      executor.reset();

      // Move from other
      executor = std::move(other.executor);
      samplePool = std::move(other.samplePool);
      queryCtx = std::move(other.queryCtx);

      // Nullify source pointers
      other.executor = nullptr;
      other.samplePool = nullptr;
      other.queryCtx = nullptr;
    }
    return *this;
  }

  ~SampleContext() {
    // Ensure executor has nothing running before freeing
    if (executor) {
      executor->join();
    }
    // Free in reverse order: queryCtx, samplePool, then executor
    queryCtx.reset();
    samplePool.reset();
    executor.reset();
  }
};

SampleContext createSampleContext(
    const velox::core::QueryCtx& original,
    int32_t numThreads) {
  static std::atomic<int64_t> kQueryCounter{0};

  auto queryId = fmt::format("sample:{}", ++kQueryCounter);
  auto pool = velox::memory::memoryManager()->addRootPool(queryId);

  // Create CPU thread pool executor with specified number of threads
  auto executor =
      std::make_shared<folly::CPUThreadPoolExecutor>(std::max(1, numThreads));

  std::unordered_map<std::string, std::string> empty;
  auto queryCtx = velox::core::QueryCtx::create(
      executor.get(),
      velox::core::QueryConfig(std::move(empty)),
      original.connectorSessionProperties(),
      original.cache(),
      pool,
      nullptr,
      queryId);

  return SampleContext(
      std::move(executor), std::move(pool), std::move(queryCtx));
}

/// Recursively traverses an Expr tree and collects all Column names.
void collectColumnNames(ExprCP expr, folly::F14FastSet<std::string>& names) {
  if (expr->is(PlanType::kColumnExpr)) {
    auto* column = expr->as<Column>();
    names.insert(std::string(column->name()));
    return;
  }

  if (expr->is(PlanType::kCallExpr)) {
    auto* call = expr->as<Call>();
    for (auto arg : call->args()) {
      collectColumnNames(arg, names);
    }
    return;
  }

  if (expr->is(PlanType::kFieldExpr)) {
    auto* field = expr->as<Field>();
    collectColumnNames(field->base(), names);
    return;
  }

  // For literals and other types, no columns to collect
}

struct SampleRunner {
  SampleContext context;
  std::shared_ptr<runner::Runner> runner;
};

SampleRunner prepareSampleRunner(
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

  using namespace logical_plan;

  // Collect column names from keys by recursing through the expression tree
  folly::F14FastSet<std::string> columnNames;
  for (auto key : keys) {
    collectColumnNames(key, columnNames);
  }
  std::vector<std::string> columnVec(columnNames.begin(), columnNames.end());

  // Build the logical plan
  PlanBuilder builder;

  // Get connector ID from the first column group's layout
  const auto& connectorId = table->columnGroups[0]->layout->connectorId();

  // Table scan that extracts the key columns
  builder.tableScan(connectorId, table->name(), columnVec);

  // Project that calculates the hash of each key
  // Since we can't easily convert optimizer Expr to logical_plan ExprApi,
  // we hash the individual columns and let the hash mix handle the combination
  std::vector<ExprApi> hashExprs;
  hashExprs.reserve(columnNames.size());
  for (const auto& colName : columnNames) {
    hashExprs.push_back(logical_plan::Call(kHash, {Col(colName)}));
  }

  // Mix all the hashes together
  ExprApi mixedHash = hashExprs.size() == 1
      ? hashExprs[0]
      : ExprApi(logical_plan::Call(kHashMix, hashExprs));

  // Project the mixed hash as "hash"
  builder.project({mixedHash.as("hash")});

  // Filter that calls sample with the hash and mod
  builder.filter(
      logical_plan::Call(kSample, {Col("hash"), Lit(mod), Lit(lim)}));

  // Project to return only the hash column
  builder.project({"hash"});

  // Build the logical plan
  auto logicalPlan = builder.build();

  // Get the memory pool
  auto* pool = queryCtx()->optimization()->evaluator()->pool();
  auto options = queryCtx()->optimization()->options();
  options.traceFlags = 0;
  // Translate to runnable plan using Optimization::toVeloxPlan
  auto planAndStats = Optimization::toVeloxPlan(
      *logicalPlan,
      *pool,
      options,
      queryCtx()->optimization()->runnerOptions(),
      &queryCtx()->optimization()->history());

  auto context = createSampleContext(
      *queryCtx()->optimization()->veloxQueryCtx(),
      queryCtx()->optimization()->runnerOptions().numDrivers);

  auto runner = std::make_shared<runner::LocalRunner>(
      std::move(planAndStats.plan),
      std::move(planAndStats.finishWrite),
      context.queryCtx);

  return SampleRunner{std::move(context), std::move(runner)};
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
    auto hashes = rows->childAt(0)->as<velox::SimpleVector<int64_t>>();
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

std::pair<float, float> sampleJoinByPartitions(
    SchemaTableCP left,
    const ExprVector& leftKeys,
    SchemaTableCP right,
    const ExprVector& rightKeys) {
  const auto leftRows = left->cardinality;
  const auto rightRows = right->cardinality;

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

  auto leftSampleRunner =
      prepareSampleRunner(left, leftKeys, kMaxCardinality, fraction);
  auto rightSampleRunner =
      prepareSampleRunner(right, rightKeys, kMaxCardinality, fraction);

  auto leftRun = std::make_shared<velox::AsyncSource<KeyFreq>>(
      [runner = leftSampleRunner.runner]() { return runJoinSample(*runner); });
  auto rightRun = std::make_shared<velox::AsyncSource<KeyFreq>>(
      [runner = rightSampleRunner.runner]() { return runJoinSample(*runner); });

  if (auto executor = queryCtx()->optimization()->veloxQueryCtx()->executor()) {
    executor->add([leftRun]() { leftRun->prepare(); });
    executor->add([rightRun]() { rightRun->prepare(); });
  }

  auto leftFreq = leftRun->move();
  auto rightFreq = rightRun->move();
  return std::make_pair(
      freqs(*rightFreq, *leftFreq), freqs(*leftFreq, *rightFreq));
}

std::pair<float, float> sampleJoin(
    SchemaTableCP left,
    const ExprVector& leftKeys,
    SchemaTableCP right,
    const ExprVector& rightKeys) {
  return sampleJoinByPartitions(left, leftKeys, right, rightKeys);
}

} // namespace facebook::axiom::optimizer
