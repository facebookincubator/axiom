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

#include "axiom/optimizer/tests/FeatureGen.h"
#include <cmath>
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/tests/utils/DfFunctions.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace facebook::axiom::optimizer::test {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

RowTypePtr makeRowType(
    const std::vector<RowVectorPtr>& vectors,
    int32_t column) {
  folly::F14FastSet<int32_t> keys;
  TypePtr valueType;
  for (auto& row : vectors) {
    auto map = row->childAt(column)->as<MapVector>();
    auto keyVector = map->mapKeys()->as<FlatVector<int32_t>>();
    if (!valueType) {
      valueType = map->type()->childAt(1);
    }
    for (auto i = 0; i < keyVector->size(); ++i) {
      keys.insert(keyVector->valueAt(i));
    }
  }
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (auto key : keys) {
    names.push_back(fmt::format("{}", key));
    types.push_back(valueType);
  }
  return ROW(std::move(names), std::move(types));
}

BufferPtr evenOffsets(int32_t numRows, int32_t step, memory::MemoryPool* pool) {
  auto buffer = AlignedBuffer::allocate<int32_t>(numRows, pool);
  for (auto i = 0; i < numRows; ++i) {
    buffer->asMutable<int32_t>()[i] = i * step;
  }
  return buffer;
}

BufferPtr evenSizes(int32_t numRows, int32_t step, memory::MemoryPool* pool) {
  auto buffer = AlignedBuffer::allocate<int32_t>(numRows, pool);
  for (auto i = 0; i < numRows; ++i) {
    buffer->asMutable<int32_t>()[i] = step;
  }
  return buffer;
}

std::vector<RowVectorPtr> makeFeatures(
    int32_t numBatches,
    int32_t batchSize,
    FeatureOptions& opts,
    memory::MemoryPool* pool) {
  std::vector<RowVectorPtr> result;
  velox::test::VectorMaker vectorMaker(pool);

  for (auto batchIdx = 0; batchIdx < numBatches; ++batchIdx) {
    auto uids = vectorMaker.flatVector<int64_t>(
        batchSize, [&](auto row) { return row + batchIdx * batchSize; });
    auto tss = vectorMaker.flatVector<int64_t>(batchSize, [&](auto row) {
      return 100 * (row + batchIdx * batchSize);
    });
    auto floatKeys = vectorMaker.flatVector<int32_t>(
        batchSize * opts.numFloat,
        [&](int32_t row) { return (row % opts.numFloat) * 100 + 10000; });
    auto floats = vectorMaker.flatVector<float>(
        batchSize * opts.numFloat,
        [&](int32_t row) { return (row % 123) / 100.0; });

    auto floatFeatures = std::make_shared<MapVector>(
        pool,
        MAP(INTEGER(), REAL()),
        nullptr,
        batchSize,
        evenOffsets(batchSize, opts.numFloat, pool),
        evenSizes(batchSize, opts.numFloat, pool),
        floatKeys,
        floats);

    std::vector<int32_t> idListSize(opts.numIdList);
    if (opts.expCardinalities && opts.numIdList > 1) {
      // Exponential distribution with alternating small/large cardinalities.
      // f(n) gives the nth value on exponential curve from min to max.
      // sizes[0] = f(0), sizes[1] = f(numFeatures-1), sizes[2] = f(2),
      // sizes[3] = f(numFeatures-2), etc.
      auto f = [&](int32_t n) -> int32_t {
        double t =
            static_cast<double>(n) / static_cast<double>(opts.numIdList - 1);
        if (opts.idListMinCard <= 0) {
          // Linear interpolation when min is 0 or negative
          return static_cast<int32_t>(t * opts.idListMaxCard);
        }
        double ratio = static_cast<double>(opts.idListMaxCard) /
            static_cast<double>(opts.idListMinCard);
        return static_cast<int32_t>(opts.idListMinCard * std::pow(ratio, t));
      };
      for (auto i = 0; i < opts.numIdList; ++i) {
        int32_t expIndex;
        if (i % 2 == 0) {
          // Even indices: 0, 2, 4, ... map to f(0), f(2), f(4), ...
          expIndex = i;
        } else {
          // Odd indices: 1, 3, 5, ... map to f(n-1), f(n-2), f(n-3), ...
          expIndex = opts.numIdList - 1 - (i / 2);
        }
        idListSize[i] = f(expIndex);
      }
    } else {
      for (auto i = 0; i < opts.numIdList; ++i) {
        idListSize[i] = opts.idListMinCard +
            i * ((opts.idListMaxCard - opts.idListMinCard) / opts.numIdList);
      }
    }

    auto idLists = vectorMaker.arrayVector<int64_t>(
        batchSize * opts.numIdList,
        [&](auto row) { return idListSize[row % idListSize.size()]; },
        [&](auto row) { return static_cast<int64_t>(row) * 100 + 1; });
    auto idListKeys = vectorMaker.flatVector<int32_t>(
        batchSize * opts.numIdList,
        [&](auto row) { return (row % opts.numIdList) * 200 + 200000; });
    auto idListFeatures = std::make_shared<MapVector>(
        pool,
        MAP(INTEGER(), ARRAY(BIGINT())),
        nullptr,
        batchSize,
        evenOffsets(batchSize, opts.numIdList, pool),
        evenSizes(batchSize, opts.numIdList, pool),
        idListKeys,
        idLists);
    auto scoreKeys = vectorMaker.flatVector<int32_t>(
        batchSize * opts.numIdScoreList,
        [&](auto row) { return (row % opts.numIdScoreList) * 200 + 200000; });

    auto scores = vectorMaker.mapVector<int64_t, float>(
        batchSize * opts.numIdScoreList,
        [&](auto row) { return idListSize[row % opts.numIdScoreList]; },
        [&](int32_t row, int32_t idx) {
          auto nthArray = (row / opts.numIdScoreList) * opts.numIdList;
          return idLists->elements()->as<SimpleVector<int64_t>>()->valueAt(
              idLists->offsetAt(nthArray) + idx);
        },
        [&](int32_t row, int32_t idx) { return 1.2 * row / idx; });
    auto scoreListFeatures = std::make_shared<MapVector>(
        pool,
        MAP(INTEGER(), MAP(BIGINT(), REAL())),
        nullptr,
        batchSize,
        evenOffsets(batchSize, opts.numIdScoreList, pool),
        evenSizes(batchSize, opts.numIdScoreList, pool),
        scoreKeys,
        scores);
    auto row = vectorMaker.rowVector(
        {"uid",
         "ts",
         "float_features",
         "id_list_features",
         "id_score_list_features"},
        {uids, tss, floatFeatures, idListFeatures, scoreListFeatures});
    result.push_back(std::move(row));
  }
  opts.floatStruct = makeRowType(result, 2);
  opts.idListStruct = makeRowType(result, 3);
  opts.idScoreListStruct = makeRowType(result, 4);
  return result;
}

core::TypedExprPtr floatFeatures() {
  return std::make_shared<core::FieldAccessTypedExpr>(
      MAP(INTEGER(), REAL()), "float_features");
}

core::TypedExprPtr intLiteral(int32_t i) {
  return std::make_shared<core::ConstantTypedExpr>(INTEGER(), variant(i));
}

core::TypedExprPtr floatLiteral(float i) {
  return std::make_shared<core::ConstantTypedExpr>(REAL(), variant(i));
}

core::TypedExprPtr rand() {
  core::TypedExprPtr r = std::make_shared<core::CallTypedExpr>(
      INTEGER(), std::vector<core::TypedExprPtr>{intLiteral(5)}, "rand");
  return std::make_shared<core::CastTypedExpr>(REAL(), r, false);
}

core::TypedExprPtr plus(core::TypedExprPtr x, core::TypedExprPtr y) {
  return std::make_shared<core::CallTypedExpr>(
      REAL(), std::vector<core::TypedExprPtr>{x, y}, "plus");
}

core::TypedExprPtr floatFeature(const FeatureOptions& opts) {
  int32_t id = atoi(
      opts.floatStruct
          ->nameOf(folly::Random::rand32(opts.rng) % opts.floatStruct->size())
          .c_str());
  std::vector<core::TypedExprPtr> args{floatFeatures(), intLiteral(id)};

  return std::make_shared<core::CallTypedExpr>(
      REAL(), std::move(args), "subscript");
}

core::TypedExprPtr plusOne(core::TypedExprPtr expr) {
  std::vector<core::TypedExprPtr> args{expr, floatLiteral(1)};
  return std::make_shared<core::CallTypedExpr>(REAL(), std::move(args), "plus");
}
core::TypedExprPtr uid() {
  return std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "uid");
}

core::TypedExprPtr bigintMod(core::TypedExprPtr x, int64_t y) {
  core::TypedExprPtr lit =
      std::make_shared<core::ConstantTypedExpr>(BIGINT(), variant(y));
  return std::make_shared<core::CallTypedExpr>(
      BIGINT(), std::vector<core::TypedExprPtr>{x, lit}, "mod");
}

core::TypedExprPtr bigintEq(core::TypedExprPtr x, int64_t y) {
  core::TypedExprPtr lit =
      std::make_shared<core::ConstantTypedExpr>(BIGINT(), variant(y));
  return std::make_shared<core::CallTypedExpr>(
      BOOLEAN(), std::vector<core::TypedExprPtr>{x, lit}, "eq");
}

core::TypedExprPtr uidCond(core::TypedExprPtr f) {
  auto cond = bigintEq(bigintMod(uid(), 10), 0);
  return std::make_shared<core::CallTypedExpr>(
      REAL(), std::vector<core::TypedExprPtr>{cond, f, plusOne(f)}, "if");
}

core::TypedExprPtr bucketize(
    core::TypedExprPtr expr,
    const FeatureOptions& opts) {
  // Generate 10-30 evenly spaced values between 0 and 2
  auto numBuckets = 10 + (folly::Random::rand32(opts.rng) % 21); // 10 to 30
  std::vector<core::TypedExprPtr> bucketBoundaries;
  for (int i = 0; i < numBuckets; ++i) {
    float value = 2.0f * i / (numBuckets - 1);
    bucketBoundaries.push_back(floatLiteral(value));
  }

  auto boundariesArray = std::make_shared<core::CallTypedExpr>(
      ARRAY(REAL()), std::move(bucketBoundaries), "array_constructor");

  return std::make_shared<core::CallTypedExpr>(
      INTEGER(),
      std::vector<core::TypedExprPtr>{expr, boundariesArray},
      "bucketize");
}

core::TypedExprPtr makeFloatExpr(const FeatureOptions& opts) {
  auto f = floatFeature(opts);
  if (opts.coinToss(opts.plusOnePct)) {
    f = plusOne(f);
  }
  if (opts.coinToss(opts.randomPct)) {
    f = plus(f, rand());
  }
  if (opts.coinToss(opts.multiColumnPct)) {
    auto g = floatFeature(opts);
    if (opts.coinToss(opts.plusOnePct)) {
      g = plusOne(g);
    }
    f = plus(f, g);
  }
  if (opts.coinToss(opts.uidPct)) {
    f = uidCond(f);
  }
  if (opts.coinToss(opts.bucketizePct)) {
    f = bucketize(f, opts);
  }
  return f;
}

void makeExprs(
    const FeatureOptions& opts,
    std::vector<std::string>& names,
    std::vector<core::TypedExprPtr>& exprs) {
  names = {"uid"};
  exprs = {uid()};
  auto numFloatExprs = (opts.floatStruct->size() * opts.floatExprsPct) / 100.0;
  std::vector<core::TypedExprPtr> floatExprs;
  std::vector<TypePtr> floatTypes;
  for (auto cnt = 0; cnt < numFloatExprs; ++cnt) {
    auto expr = makeFloatExpr(opts);
    floatExprs.push_back(expr);
    floatTypes.push_back(expr->type());
  }
  if (!floatExprs.empty()) {
    names.push_back("floats");
    exprs.push_back(
        std::make_shared<core::CallTypedExpr>(
            ROW(std::move(floatTypes)),
            std::move(floatExprs),
            "row_constructor"));
  }
}

namespace lpe {

lp::ExprPtr floatFeatures() {
  return std::make_shared<lp::InputReferenceExpr>(
      MAP(INTEGER(), REAL()), "float_features");
}

lp::ExprPtr intLiteral(int32_t i) {
  return std::make_shared<lp::ConstantExpr>(
      INTEGER(), std::make_shared<Variant>(i));
}

lp::ExprPtr floatLiteral(float i) {
  return std::make_shared<lp::ConstantExpr>(
      REAL(), std::make_shared<Variant>(i));
}

lp::ExprPtr rand() {
  lp::ExprPtr r = std::make_shared<lp::CallExpr>(
      INTEGER(), "rand", std::vector<lp::ExprPtr>{intLiteral(5)});
  return std::make_shared<lp::SpecialFormExpr>(
      REAL(), lp::SpecialForm::kCast, std::vector<lp::ExprPtr>{r});
}

lp::ExprPtr plus(lp::ExprPtr x, lp::ExprPtr y) {
  return std::make_shared<lp::CallExpr>(
      REAL(), "plus", std::vector<lp::ExprPtr>{x, y});
}

lp::ExprPtr floatFeature(const FeatureOptions& opts) {
  int32_t id = atoi(
      opts.floatStruct
          ->nameOf(folly::Random::rand32(opts.rng) % opts.floatStruct->size())
          .c_str());
  std::vector<lp::ExprPtr> args{floatFeatures(), intLiteral(id)};

  return std::make_shared<lp::CallExpr>(REAL(), "subscript", std::move(args));
}

lp::ExprPtr plusOne(lp::ExprPtr expr) {
  std::vector<lp::ExprPtr> args{expr, floatLiteral(1)};
  return std::make_shared<lp::CallExpr>(REAL(), "plus", std::move(args));
}
lp::ExprPtr uid() {
  return std::make_shared<lp::InputReferenceExpr>(BIGINT(), "uid");
}

lp::ExprPtr bigintMod(lp::ExprPtr x, int64_t y) {
  lp::ExprPtr lit = std::make_shared<lp::ConstantExpr>(
      BIGINT(), std::make_shared<Variant>(y));
  return std::make_shared<lp::CallExpr>(
      BIGINT(), "mod", std::vector<lp::ExprPtr>{x, lit});
}

lp::ExprPtr bigintEq(lp::ExprPtr x, int64_t y) {
  lp::ExprPtr lit = std::make_shared<lp::ConstantExpr>(
      BIGINT(), std::make_shared<Variant>(y));
  return std::make_shared<lp::CallExpr>(
      BOOLEAN(), "eq", std::vector<lp::ExprPtr>{x, lit});
}

lp::ExprPtr uidCond(lp::ExprPtr f) {
  auto cond = bigintEq(bigintMod(uid(), 10), 0);
  return std::make_shared<lp::SpecialFormExpr>(
      REAL(),
      lp::SpecialForm::kIf,
      std::vector<lp::ExprPtr>{cond, f, plusOne(f)});
}

lp::ExprPtr bucketize(lp::ExprPtr expr, const FeatureOptions& opts) {
  // Generate 10-30 evenly spaced values between 0 and 2
  auto numBuckets = 10 + (folly::Random::rand32(opts.rng) % 21); // 10 to 30
  std::vector<lp::ExprPtr> bucketBoundaries;
  for (int i = 0; i < numBuckets; ++i) {
    float value = 2.0f * i / (numBuckets - 1);
    bucketBoundaries.push_back(floatLiteral(value));
  }

  auto boundariesArray = std::make_shared<lp::CallExpr>(
      ARRAY(REAL()), "array_constructor", std::move(bucketBoundaries));

  return std::make_shared<lp::CallExpr>(
      INTEGER(), "bucketize", std::vector<lp::ExprPtr>{expr, boundariesArray});
}

lp::ExprPtr makeFloatExpr(const FeatureOptions& opts) {
  auto f = lpe::floatFeature(opts);
  if (opts.coinToss(opts.plusOnePct)) {
    f = plusOne(f);
  }
  if (opts.coinToss(opts.randomPct)) {
    f = plus(f, rand());
  }
  if (opts.coinToss(opts.multiColumnPct)) {
    auto g = lpe::floatFeature(opts);
    if (opts.coinToss(opts.plusOnePct)) {
      g = plusOne(g);
    }
    f = plus(f, g);
  }
  if (opts.coinToss(opts.uidPct)) {
    f = uidCond(f);
  }
  if (opts.coinToss(opts.bucketizePct)) {
    f = bucketize(f, opts);
  }
  return f;
}
} // namespace lpe

void makeLogicalExprs(
    const FeatureOptions& opts,
    std::vector<std::string>& names,
    std::vector<lp::ExprPtr>& exprs) {
  names = {"uid"};
  exprs = {lpe::uid()};
  auto numFloatExprs = (opts.floatStruct->size() * opts.floatExprsPct) / 100.0;
  std::vector<lp::ExprPtr> floatExprs;
  std::vector<TypePtr> floatTypes;
  for (auto cnt = 0; cnt < numFloatExprs; ++cnt) {
    auto expr = lpe::makeFloatExpr(opts);
    floatExprs.push_back(expr);
    floatTypes.push_back(expr->type());
  }
  if (!floatExprs.empty()) {
    names.push_back("floats");
    exprs.push_back(
        std::make_shared<lp::CallExpr>(
            ROW(std::move(floatTypes)),
            "row_constructor",
            std::move(floatExprs)));
  }
}

lp::LogicalPlanNodePtr makeIdScoreListPipeline(
    const FeatureOptions& opts,
    lp::LogicalPlanNodePtr source,
    const std::vector<int32_t>& featureKeys,
    const std::vector<std::string>& passthroughColumns) {
  if (featureKeys.empty()) {
    return source;
  }

  // Helper lambda to add passthrough columns to projection lists
  auto addPassthroughColumns = [&](std::vector<std::string>& names,
                                   std::vector<lp::ExprPtr>& exprs) {
    for (const auto& colName : passthroughColumns) {
      // Get the type from the source node's output type
      auto sourceType = source->outputType();
      auto colType = sourceType->findChild(colName);
      names.push_back(colName);
      exprs.push_back(
          std::make_shared<lp::InputReferenceExpr>(colType, colName));
    }
  };

  // Get id_score_list_features field
  auto idScoreListFeatures = std::make_shared<lp::InputReferenceExpr>(
      MAP(INTEGER(), MAP(BIGINT(), REAL())), "id_score_list_features");

  // Split features: first half through convert_format, second half through
  // first_x
  auto midpoint = featureKeys.size() / 2;

  std::vector<std::string> projNames;
  std::vector<lp::ExprPtr> projExprs;

  // Add passthrough columns
  addPassthroughColumns(projNames, projExprs);

  // First half: convert_format path
  // Build row_constructor with coalesced maps
  for (size_t i = 0; i < midpoint; ++i) {
    auto featureKey = featureKeys[i];
    auto keyExpr = std::make_shared<lp::ConstantExpr>(
        INTEGER(), std::make_shared<Variant>(featureKey));

    // id_score_list_features[featureKey]
    auto subscript = std::make_shared<lp::CallExpr>(
        MAP(BIGINT(), REAL()),
        "subscript",
        std::vector<lp::ExprPtr>{idScoreListFeatures, keyExpr});

    // coalesce(id_score_list_features[featureKey], <empty>)
    auto emptyMap = std::make_shared<lp::ConstantExpr>(
        MAP(BIGINT(), REAL()),
        std::make_shared<Variant>(variant::null(TypeKind::MAP)));
    auto coalesced = std::make_shared<lp::SpecialFormExpr>(
        MAP(BIGINT(), REAL()),
        lp::SpecialForm::kCoalesce,
        std::vector<lp::ExprPtr>{subscript, emptyMap});

    auto tempName = fmt::format("__temp_map_{}", featureKey);
    projNames.push_back(tempName);
    projExprs.push_back(coalesced);
  }

  // Second half: first_x + extract_sparse_key path
  for (size_t i = midpoint; i < featureKeys.size(); ++i) {
    auto featureKey = featureKeys[i];
    auto keyExpr = std::make_shared<lp::ConstantExpr>(
        INTEGER(), std::make_shared<Variant>(featureKey));

    // id_score_list_features[featureKey]
    auto subscript = std::make_shared<lp::CallExpr>(
        MAP(BIGINT(), REAL()),
        "subscript",
        std::vector<lp::ExprPtr>{idScoreListFeatures, keyExpr});

    // coalesce(id_score_list_features[featureKey], <empty>)
    auto emptyMap = std::make_shared<lp::ConstantExpr>(
        MAP(BIGINT(), REAL()),
        std::make_shared<Variant>(variant::null(TypeKind::MAP)));
    auto coalesced = std::make_shared<lp::SpecialFormExpr>(
        MAP(BIGINT(), REAL()),
        lp::SpecialForm::kCoalesce,
        std::vector<lp::ExprPtr>{subscript, emptyMap});

    auto tempName = fmt::format("__temp_coalesced_{}", featureKey);
    projNames.push_back(tempName);
    projExprs.push_back(coalesced);
  }

  // First projection: access and coalesce
  source = std::make_shared<lp::ProjectNode>(
      source->id(), source, std::move(projNames), std::move(projExprs));

  // Second projection: convert_format for first half
  projNames.clear();
  projExprs.clear();

  // Add passthrough columns
  addPassthroughColumns(projNames, projExprs);

  for (size_t i = 0; i < midpoint; ++i) {
    auto featureKey = featureKeys[i];
    auto tempName = fmt::format("__temp_map_{}", featureKey);

    // Build row_constructor from the coalesced map
    auto tempRef = std::make_shared<lp::InputReferenceExpr>(
        MAP(BIGINT(), REAL()), tempName);

    // convert_format(row_constructor(coalesced_map))
    // Input: ROW("map": MAP<BIGINT,REAL>)
    // Output: ROW("map": ARRAY<ROW<"":BIGINT,"":REAL>>)
    auto rowCtor = std::make_shared<lp::CallExpr>(
        ROW({"map"}, std::vector<TypePtr>{MAP(BIGINT(), REAL())}),
        "row_constructor",
        std::vector<lp::ExprPtr>{tempRef});

    auto converted = std::make_shared<lp::CallExpr>(
        ROW({"map"},
            std::vector<TypePtr>{
                ARRAY(ROW({"", ""}, std::vector<TypePtr>{BIGINT(), REAL()}))}),
        "convert_format",
        std::vector<lp::ExprPtr>{rowCtor});

    // Extract the "map" field from the result ROW to get just the ARRAY
    auto arrayExtracted = std::make_shared<lp::SpecialFormExpr>(
        ARRAY(ROW({"", ""}, std::vector<TypePtr>{BIGINT(), REAL()})),
        lp::SpecialForm::kDereference,
        converted,
        std::make_shared<lp::ConstantExpr>(
            VARCHAR(), std::make_shared<Variant>("map")));

    // coalesce(arrayExtracted, empty_array)
    auto emptyArray = std::make_shared<lp::ConstantExpr>(
        ARRAY(ROW({"", ""}, std::vector<TypePtr>{BIGINT(), REAL()})),
        std::make_shared<Variant>(variant::null(TypeKind::ARRAY)));
    auto coalesced = std::make_shared<lp::SpecialFormExpr>(
        ARRAY(ROW({"", ""}, std::vector<TypePtr>{BIGINT(), REAL()})),
        lp::SpecialForm::kCoalesce,
        std::vector<lp::ExprPtr>{arrayExtracted, emptyArray});

    auto arrayName = fmt::format("__temp_array_{}", featureKey);
    projNames.push_back(arrayName);
    projExprs.push_back(coalesced);
  }

  // Pass through coalesced maps for second half
  for (size_t i = midpoint; i < featureKeys.size(); ++i) {
    auto featureKey = featureKeys[i];
    auto tempName = fmt::format("__temp_coalesced_{}", featureKey);
    projNames.push_back(tempName);
    projExprs.push_back(
        std::make_shared<lp::InputReferenceExpr>(
            MAP(BIGINT(), REAL()), tempName));
  }

  source = std::make_shared<lp::ProjectNode>(
      source->id(), source, std::move(projNames), std::move(projExprs));

  // Third projection: extract_sparse_key(first_x(...)) for second half
  projNames.clear();
  projExprs.clear();

  // Add passthrough columns
  addPassthroughColumns(projNames, projExprs);

  // Pass through arrays from first half
  for (size_t i = 0; i < midpoint; ++i) {
    auto featureKey = featureKeys[i];
    auto arrayName = fmt::format("__temp_array_{}", featureKey);
    projNames.push_back(arrayName);
    projExprs.push_back(
        std::make_shared<lp::InputReferenceExpr>(
            ARRAY(ROW({BIGINT(), REAL()})), arrayName));
  }

  // Process second half with first_x
  for (size_t i = midpoint; i < featureKeys.size(); ++i) {
    auto featureKey = featureKeys[i];
    auto tempName = fmt::format("__temp_coalesced_{}", featureKey);

    auto tempRef = std::make_shared<lp::InputReferenceExpr>(
        MAP(BIGINT(), REAL()), tempName);

    // map_keys to get array of ids
    auto mapKeys = std::make_shared<lp::CallExpr>(
        ARRAY(BIGINT()), "map_keys", std::vector<lp::ExprPtr>{tempRef});

    // first_x(map_keys(...), 50)
    auto limitExpr = std::make_shared<lp::ConstantExpr>(
        INTEGER(), std::make_shared<Variant>(50));
    auto firstX = std::make_shared<lp::CallExpr>(
        ARRAY(BIGINT()),
        "first_x",
        std::vector<lp::ExprPtr>{mapKeys, limitExpr});

    auto idsName = fmt::format("__temp_ids_{}", featureKey);
    projNames.push_back(idsName);
    projExprs.push_back(firstX);
  }

  source = std::make_shared<lp::ProjectNode>(
      source->id(), source, std::move(projNames), std::move(projExprs));

  // Fourth projection: sigrid_hash for second half
  projNames.clear();
  projExprs.clear();

  // Add passthrough columns
  addPassthroughColumns(projNames, projExprs);

  // Output arrays from first half
  for (size_t i = 0; i < midpoint; ++i) {
    auto featureKey = featureKeys[i];
    auto arrayName = fmt::format("__temp_array_{}", featureKey);
    auto outputName = fmt::format("feature_{}_array", featureKey);
    projNames.push_back(outputName);
    projExprs.push_back(
        std::make_shared<lp::InputReferenceExpr>(
            ARRAY(ROW({BIGINT(), REAL()})), arrayName));
  }

  // Hash second half
  for (size_t i = midpoint; i < featureKeys.size(); ++i) {
    auto featureKey = featureKeys[i];
    auto idsName = fmt::format("__temp_ids_{}", featureKey);

    auto idsRef =
        std::make_shared<lp::InputReferenceExpr>(ARRAY(BIGINT()), idsName);

    // sigrid_hash_into_i32_velox_debug_only(ids, 0, seed)
    auto zero = std::make_shared<lp::ConstantExpr>(
        INTEGER(), std::make_shared<Variant>(0));
    auto seed = std::make_shared<lp::ConstantExpr>(
        INTEGER(), std::make_shared<Variant>(featureKey * 1000));

    auto hashed = std::make_shared<lp::CallExpr>(
        ARRAY(INTEGER()),
        "sigrid_hash_into_i32_velox_debug_only",
        std::vector<lp::ExprPtr>{idsRef, zero, seed});

    auto outputName = fmt::format("feature_{}_hashed", featureKey);
    projNames.push_back(outputName);
    projExprs.push_back(hashed);
  }

  source = std::make_shared<lp::ProjectNode>(
      source->id(), source, std::move(projNames), std::move(projExprs));

  return source;
}

lp::LogicalPlanNodePtr makeFeaturePipeline(
    const FeatureOptions& opts,
    std::string_view connectorId) {
  lp::PlanBuilder::Context ctx(
      std::string(connectorId), nullptr, resolveDfFunction);

  // Table scan selecting required columns
  auto source = lp::PlanBuilder(ctx)
                    .tableScan(
                        "features",
                        {"ts",
                         "uid",
                         "float_features",
                         "id_list_features",
                         "id_score_list_features"})
                    .build();

  // First projection: padded_make_row_from_map for all map columns
  std::vector<std::string> projNames;
  std::vector<lp::ExprPtr> projExprs;

  // Pass through uid and ts
  projNames.push_back("uid");
  projExprs.push_back(
      std::make_shared<lp::InputReferenceExpr>(BIGINT(), "uid"));
  projNames.push_back("ts");
  projExprs.push_back(std::make_shared<lp::InputReferenceExpr>(BIGINT(), "ts"));

  // Helper to create array of integer literals as ConstantExpr
  auto makeIntArray = [](const std::vector<int32_t>& values) -> lp::ExprPtr {
    std::vector<variant> elements;
    for (auto val : values) {
      elements.push_back(variant(val));
    }
    return std::make_shared<lp::ConstantExpr>(
        ARRAY(INTEGER()), std::make_shared<Variant>(variant::array(elements)));
  };

  // Helper to create array of string literals as ConstantExpr
  auto makeStringArray =
      [](const std::vector<std::string>& values) -> lp::ExprPtr {
    std::vector<variant> elements;
    for (const auto& val : values) {
      elements.push_back(variant(val));
    }
    return std::make_shared<lp::ConstantExpr>(
        ARRAY(VARCHAR()), std::make_shared<Variant>(variant::array(elements)));
  };

  // Create padded struct types with 'n' prefix
  RowTypePtr paddedFloatStruct;
  RowTypePtr paddedIdListStruct;
  RowTypePtr paddedIdScoreListStruct;

  // Process float_features
  {
    std::vector<int32_t> featureIds;
    std::vector<std::string> featureNames;
    std::vector<TypePtr> featureTypes;
    for (size_t i = 0; i < opts.floatStruct->size(); ++i) {
      auto name = opts.floatStruct->nameOf(i);
      int32_t id = std::stoi(name);
      featureIds.push_back(id);
      featureNames.push_back("n" + name);
      featureTypes.push_back(opts.floatStruct->childAt(i));
    }

    paddedFloatStruct = ROW(featureNames, featureTypes);

    auto floatFeaturesRef = std::make_shared<lp::InputReferenceExpr>(
        MAP(INTEGER(), REAL()), "float_features");
    auto idsArray = makeIntArray(featureIds);
    auto namesArray = makeStringArray(featureNames);

    auto paddedRow = std::make_shared<lp::CallExpr>(
        paddedFloatStruct,
        "padded_make_row_from_map",
        std::vector<lp::ExprPtr>{floatFeaturesRef, idsArray, namesArray});

    projNames.push_back("float_features_1");
    projExprs.push_back(paddedRow);
  }

  // Process id_list_features
  {
    std::vector<int32_t> featureIds;
    std::vector<std::string> featureNames;
    std::vector<TypePtr> featureTypes;
    for (size_t i = 0; i < opts.idListStruct->size(); ++i) {
      auto name = opts.idListStruct->nameOf(i);
      int32_t id = std::stoi(name);
      featureIds.push_back(id);
      featureNames.push_back("n" + name);
      featureTypes.push_back(opts.idListStruct->childAt(i));
    }

    paddedIdListStruct = ROW(featureNames, featureTypes);

    auto idListFeaturesRef = std::make_shared<lp::InputReferenceExpr>(
        MAP(INTEGER(), ARRAY(BIGINT())), "id_list_features");
    auto idsArray = makeIntArray(featureIds);
    auto namesArray = makeStringArray(featureNames);

    auto paddedRow = std::make_shared<lp::CallExpr>(
        paddedIdListStruct,
        "padded_make_row_from_map",
        std::vector<lp::ExprPtr>{idListFeaturesRef, idsArray, namesArray});

    projNames.push_back("id_list_features_1");
    projExprs.push_back(paddedRow);
  }

  // Process id_score_list_features
  {
    std::vector<int32_t> featureIds;
    std::vector<std::string> featureNames;
    std::vector<TypePtr> featureTypes;
    for (size_t i = 0; i < opts.idScoreListStruct->size(); ++i) {
      auto name = opts.idScoreListStruct->nameOf(i);
      int32_t id = std::stoi(name);
      featureIds.push_back(id);
      featureNames.push_back("n" + name);
      featureTypes.push_back(opts.idScoreListStruct->childAt(i));
    }

    paddedIdScoreListStruct = ROW(featureNames, featureTypes);

    auto idScoreListFeaturesRef = std::make_shared<lp::InputReferenceExpr>(
        MAP(INTEGER(), MAP(BIGINT(), REAL())), "id_score_list_features");
    auto idsArray = makeIntArray(featureIds);
    auto namesArray = makeStringArray(featureNames);

    auto paddedRow = std::make_shared<lp::CallExpr>(
        paddedIdScoreListStruct,
        "padded_make_row_from_map",
        std::vector<lp::ExprPtr>{idScoreListFeaturesRef, idsArray, namesArray});

    projNames.push_back("id_score_list_features_1");
    projExprs.push_back(paddedRow);
  }

  // Pass through the original float_features MAP for use in float expressions
  projNames.push_back("float_features");
  projExprs.push_back(
      std::make_shared<lp::InputReferenceExpr>(
          MAP(INTEGER(), REAL()), "float_features"));

  // Pass through the original id_score_list_features MAP for use in
  // id_score_list pipeline
  projNames.push_back("id_score_list_features");
  projExprs.push_back(
      std::make_shared<lp::InputReferenceExpr>(
          MAP(INTEGER(), MAP(BIGINT(), REAL())), "id_score_list_features"));

  source = std::make_shared<lp::ProjectNode>(
      ctx.planNodeIdGenerator->next(),
      source,
      std::move(projNames),
      std::move(projExprs));

  // Second projection: make_named_row with float expressions
  projNames.clear();
  projExprs.clear();

  // Pass through uid, ts, id_list_features_1, id_score_list_features_1,
  // id_score_list_features
  projNames.push_back("uid");
  projExprs.push_back(
      std::make_shared<lp::InputReferenceExpr>(BIGINT(), "uid"));
  projNames.push_back("ts");
  projExprs.push_back(std::make_shared<lp::InputReferenceExpr>(BIGINT(), "ts"));
  projNames.push_back("id_list_features_1");
  projExprs.push_back(
      std::make_shared<lp::InputReferenceExpr>(
          paddedIdListStruct, "id_list_features_1"));
  projNames.push_back("id_score_list_features_1");
  projExprs.push_back(
      std::make_shared<lp::InputReferenceExpr>(
          paddedIdScoreListStruct, "id_score_list_features_1"));
  projNames.push_back("id_score_list_features");
  projExprs.push_back(
      std::make_shared<lp::InputReferenceExpr>(
          MAP(INTEGER(), MAP(BIGINT(), REAL())), "id_score_list_features"));

  // Generate float expressions (1.2x the number of float features)
  std::vector<std::string> floatExprNames;
  std::vector<lp::ExprPtr> floatExprs;
  int32_t numFloatExprs = static_cast<int32_t>(opts.floatStruct->size() * 1.2);

  for (int32_t i = 0; i < numFloatExprs; ++i) {
    auto expr = lpe::makeFloatExpr(opts);
    floatExprNames.push_back(fmt::format("f{}", i));
    floatExprs.push_back(expr);
  }

  // Create make_named_row for float expressions
  std::vector<lp::ExprPtr> nameValuePairs;
  for (size_t i = 0; i < floatExprNames.size(); ++i) {
    nameValuePairs.push_back(
        std::make_shared<lp::ConstantExpr>(
            VARCHAR(), std::make_shared<Variant>(floatExprNames[i])));
    nameValuePairs.push_back(floatExprs[i]);
  }

  // Use resolveDfFunction to convert make_named_row to row_constructor
  auto floatRow = resolveDfFunction("make_named_row", nameValuePairs);

  projNames.push_back("float_exprs");
  projExprs.push_back(floatRow);

  source = std::make_shared<lp::ProjectNode>(
      ctx.planNodeIdGenerator->next(),
      source,
      std::move(projNames),
      std::move(projExprs));

  // Third projection: make_named_row with id_list expressions
  projNames.clear();
  projExprs.clear();

  // Pass through uid, ts, float_exprs, id_score_list_features_1,
  // id_score_list_features
  projNames.push_back("uid");
  projExprs.push_back(
      std::make_shared<lp::InputReferenceExpr>(BIGINT(), "uid"));
  projNames.push_back("ts");
  projExprs.push_back(std::make_shared<lp::InputReferenceExpr>(BIGINT(), "ts"));
  projNames.push_back("float_exprs");
  projExprs.push_back(
      std::make_shared<lp::InputReferenceExpr>(
          ROW(floatExprNames,
              std::vector<TypePtr>(floatExprs.size(), INTEGER())),
          "float_exprs"));
  projNames.push_back("id_score_list_features_1");
  projExprs.push_back(
      std::make_shared<lp::InputReferenceExpr>(
          paddedIdScoreListStruct, "id_score_list_features_1"));
  projNames.push_back("id_score_list_features");
  projExprs.push_back(
      std::make_shared<lp::InputReferenceExpr>(
          MAP(INTEGER(), MAP(BIGINT(), REAL())), "id_score_list_features"));

  // Generate id_list expressions
  std::vector<std::string> idListExprNames;
  std::vector<lp::ExprPtr> idListExprs;

  for (size_t i = 0; i < paddedIdListStruct->size(); ++i) {
    auto fieldName = paddedIdListStruct->nameOf(i);

    // Access id_list_features_1.nXXXX (fieldName already has 'n' prefix)
    auto fieldRef = std::make_shared<lp::SpecialFormExpr>(
        ARRAY(BIGINT()),
        lp::SpecialForm::kDereference,
        std::make_shared<lp::InputReferenceExpr>(
            paddedIdListStruct, "id_list_features_1"),
        std::make_shared<lp::ConstantExpr>(
            VARCHAR(), std::make_shared<Variant>(fieldName)));

    // coalesce(ff, array[])
    auto emptyArray = std::make_shared<lp::ConstantExpr>(
        ARRAY(BIGINT()),
        std::make_shared<Variant>(variant::null(TypeKind::ARRAY)));
    auto coalesced = std::make_shared<lp::SpecialFormExpr>(
        ARRAY(BIGINT()),
        lp::SpecialForm::kCoalesce,
        std::vector<lp::ExprPtr>{fieldRef, emptyArray});

    // first_x(coalesced, 800)
    auto limit = std::make_shared<lp::ConstantExpr>(
        INTEGER(), std::make_shared<Variant>(800));
    auto firstX = std::make_shared<lp::CallExpr>(
        ARRAY(BIGINT()), "first_x", std::vector<lp::ExprPtr>{coalesced, limit});

    // sigrid_hash_velox_debug_only(first_x(...), 0, 800) - salt=0, maxValue=800
    auto salt = std::make_shared<lp::ConstantExpr>(
        INTEGER(), std::make_shared<Variant>(0));
    auto hashed = std::make_shared<lp::CallExpr>(
        ARRAY(BIGINT()),
        "sigrid_hash_velox_debug_only",
        std::vector<lp::ExprPtr>{firstX, salt, limit});

    idListExprNames.push_back(fieldName);
    idListExprs.push_back(hashed);
  }

  // Create make_named_row for id_list expressions
  nameValuePairs.clear();
  for (size_t i = 0; i < idListExprNames.size(); ++i) {
    nameValuePairs.push_back(
        std::make_shared<lp::ConstantExpr>(
            VARCHAR(), std::make_shared<Variant>(idListExprNames[i])));
    nameValuePairs.push_back(idListExprs[i]);
  }

  // Use resolveDfFunction to convert make_named_row to row_constructor
  auto idListRow = resolveDfFunction("make_named_row", nameValuePairs);

  projNames.push_back("id_list_exprs");
  projExprs.push_back(idListRow);

  source = std::make_shared<lp::ProjectNode>(
      ctx.planNodeIdGenerator->next(),
      source,
      std::move(projNames),
      std::move(projExprs));

  // Fourth projection: select final columns before id_score_list processing
  projNames.clear();
  projExprs.clear();

  projNames.push_back("ts");
  projExprs.push_back(std::make_shared<lp::InputReferenceExpr>(BIGINT(), "ts"));
  projNames.push_back("uid");
  projExprs.push_back(
      std::make_shared<lp::InputReferenceExpr>(BIGINT(), "uid"));
  projNames.push_back("float_exprs");
  projExprs.push_back(
      std::make_shared<lp::InputReferenceExpr>(
          ROW(floatExprNames,
              std::vector<TypePtr>(floatExprs.size(), INTEGER())),
          "float_exprs"));
  projNames.push_back("id_list_exprs");
  projExprs.push_back(
      std::make_shared<lp::InputReferenceExpr>(
          ROW(idListExprNames,
              std::vector<TypePtr>(idListExprs.size(), ARRAY(BIGINT()))),
          "id_list_exprs"));
  projNames.push_back("id_score_list_features");
  projExprs.push_back(
      std::make_shared<lp::InputReferenceExpr>(
          MAP(INTEGER(), MAP(BIGINT(), REAL())), "id_score_list_features"));

  source = std::make_shared<lp::ProjectNode>(
      ctx.planNodeIdGenerator->next(),
      source,
      std::move(projNames),
      std::move(projExprs));

  // Get feature keys for id_score_list processing
  std::vector<int32_t> idScoreListKeys;
  for (size_t i = 0; i < opts.idScoreListStruct->size(); ++i) {
    auto name = opts.idScoreListStruct->nameOf(i);
    idScoreListKeys.push_back(std::stoi(name));
  }

  // Call makeIdScoreListPipeline with passthrough columns
  std::vector<std::string> passthroughColumns = {
      "ts", "uid", "float_exprs", "id_list_exprs"};

  source = makeIdScoreListPipeline(
      opts, source, idScoreListKeys, passthroughColumns);

  return source;
}

} // namespace facebook::axiom::optimizer::test
