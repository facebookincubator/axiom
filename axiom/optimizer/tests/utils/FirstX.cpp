// Copyright 2004-present Facebook. All Rights Reserved.

#include "axiom/optimizer/tests/utils/FirstX.h"
#include "axiom/optimizer/tests/utils/Utils.h"

#include <velox/expression/EvalCtx.h>
#include "axiom/optimizer/Filters.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/PlanUtils.h"
#include "axiom/optimizer/QueryGraph.h"
#include "velox/expression/DecodedArgs.h"
#include "velox/expression/VectorFunction.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/SelectivityVector.h"
#include "velox/vector/VectorEncoding.h"

namespace facebook::axiom::velox_udf {

using namespace facebook::velox;

namespace {

class VectorFirstXFunction : public exec::VectorFunction {
 public:
  explicit VectorFirstXFunction(vector_size_t numToKeep, bool sortByScore)
      : numToKeep_(numToKeep), sortByScore_(sortByScore) {}

  void apply(
      const SelectivityVector& selector,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& resultFinal) const override {
    vector_size_t resultSize = selector.end();

    if (!resultCached_) {
      BaseVector::ensureWritable(
          selector, outputType, context.pool(), resultCached_);
    } else {
      VELOX_CHECK(resultCached_.unique());
      resultCached_->prepareForReuse();
      resultCached_->resize(resultSize);
    }
    VELOX_CHECK(args.size() >= 2);

    ArrayVector* resultArrayVector = resultCached_->as<ArrayVector>();
    resultArrayVector->resize(resultSize);

    const auto& inputElementType = args[0]->type()->asArray().elementType();
    switch (inputElementType->kind()) {
      case TypeKind::INTEGER:
      case TypeKind::BIGINT: {
        if (args[0]->encoding() == VectorEncoding::Simple::ARRAY &&
            !args[0]->mayHaveNulls()) {
          auto& elements = args[0]->asUnchecked<ArrayVector>()->elements();
          if (elements->encoding() == VectorEncoding::Simple::FLAT &&
              !elements->mayHaveNulls()) {
            if (inputElementType->kind() == TypeKind::INTEGER) {
              processIdListFastPath<int32_t>(
                  selector,
                  *args[0]->asUnchecked<ArrayVector>(),
                  *resultArrayVector);
            } else {
              processIdListFastPath<int64_t>(
                  selector,
                  *args[0]->asUnchecked<ArrayVector>(),
                  *resultArrayVector);
            }
            break;
          }
        }
        exec::DecodedArgs decodedArgs(selector, args, context);
        processIdList(selector, decodedArgs.at(0), resultArrayVector);
        break;
      }
      case TypeKind::ARRAY: {
        exec::DecodedArgs decodedArgs(selector, args, context);
        processListOfIdList(selector, decodedArgs.at(0), resultArrayVector);
        break;
      }
      case TypeKind::ROW: {
        exec::DecodedArgs decodedArgs(selector, args, context);
        if (sortByScore_) {
          processIdScorePairList(
              selector, decodedArgs.at(0), resultArrayVector);
        } else {
          processIdList(selector, decodedArgs.at(0), resultArrayVector);
        }
        break;
      }

      default:
        folly::assume_unreachable();
    }
    context.moveOrCopyResult(resultCached_, selector, resultFinal);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    std::vector<std::string> featureTypes = {
        "array(integer)",
        "array(bigint)",
        "array(row(integer, real))",
        "array(row(bigint, real))",
        "array(array(integer))",
        "array(array(bigint))"};

    std::vector<std::string> constTypes = {"integer", "bigint"};

    std::vector<std::shared_ptr<exec::FunctionSignature>> ret;
    for (const auto& featureType : featureTypes) {
      for (const auto& constType : constTypes) {
        ret.push_back(
            exec::FunctionSignatureBuilder()
                .returnType(featureType)
                .argumentType(featureType)
                .argumentType(constType)
                .build());

        ret.push_back(
            exec::FunctionSignatureBuilder()
                .returnType(featureType)
                .argumentType(featureType)
                .argumentType(constType)
                .argumentType("boolean")
                .build());
      }
    }

    return ret;
  }

 private:
  void keepNums(
      const SelectivityVector& selector,
      ArrayVector* resultArrayVector,
      const DecodedVector* inputArrays) const {
    vector_size_t resultSize = selector.end();
    const ArrayVector* inputArrayPtr = inputArrays->base()->as<ArrayVector>();

    resultArrayVector->resize(resultSize);
    uint64_t* rawNulls = resultArrayVector->mutableRawNulls();
    vector_size_t* rawOffsets = resultArrayVector->mutableOffsets(resultSize)
                                    ->asMutable<vector_size_t>();
    vector_size_t* rawSizes =
        resultArrayVector->mutableSizes(resultSize)->asMutable<vector_size_t>();

    std::vector<BaseVector::CopyRange> ranges;
    ranges.reserve(resultSize);

    vector_size_t curOffset = 0;
    selector.applyToSelected([&](vector_size_t row) {
      auto arrayInnerRow = inputArrays->index(row);
      if (inputArrayPtr->isNullAt(arrayInnerRow)) {
        bits::setNull(rawNulls, row);
      } else {
        rawOffsets[row] = curOffset;
        auto argOffset = inputArrayPtr->offsetAt(arrayInnerRow);
        auto argCount =
            std::min(numToKeep_, inputArrayPtr->sizeAt(arrayInnerRow));

        ranges.push_back({argOffset, curOffset, argCount});
        curOffset += argCount;
        rawSizes[row] = curOffset - rawOffsets[row];
        bits::clearNull(rawNulls, row);
      }
    });

    auto elements = resultArrayVector->elements();
    elements->resize(curOffset);
    elements->copyRanges(inputArrayPtr->elements().get(), ranges);
  }

  void processIdList(
      const SelectivityVector& selector,
      const DecodedVector* inputArrays,
      ArrayVector* resultArrayVector) const {
    keepNums(selector, resultArrayVector, inputArrays);
  }

  // Fast path when input array is flat and null free.
  template <typename T>
  void processIdListFastPath(
      const SelectivityVector& selector,
      const ArrayVector& inputArray,
      ArrayVector& resultArrayVector) const {
    resultArrayVector.clearAllNulls();
    resultArrayVector.elements()->clearAllNulls();

    // Resize the elements vector (this does over-resize but we downsize at the
    // end).
    resultArrayVector.elements()->asFlatVector<T>()->resize(
        selector.size() * numToKeep_);

    auto* rawOutputElements =
        resultArrayVector.elements()->asFlatVector<T>()->mutableRawValues();
    vector_size_t* rawOutputOffsets =
        resultArrayVector.mutableOffsets(selector.end())
            ->asMutable<vector_size_t>();
    vector_size_t* rawOutputSizes =
        resultArrayVector.mutableSizes(selector.end())
            ->asMutable<vector_size_t>();

    vector_size_t curOffset = 0;

    auto* rawInputElements =
        inputArray.elements()->asFlatVector<T>()->rawValues();
    selector.applyToSelected([&](vector_size_t row) {
      rawOutputOffsets[row] = curOffset;
      auto argOffset = inputArray.offsetAt(row);
      auto size = std::min(numToKeep_, inputArray.sizeAt(row));

      for (int i = 0; i < size; i++) {
        rawOutputElements[curOffset + i] = rawInputElements[argOffset + i];
      }
      curOffset += size;
      rawOutputSizes[row] = size;
    });

    // donwsize elements to the actual size needed.
    resultArrayVector.elements()->asFlatVector<T>()->resize(curOffset);
  }

  void processListOfIdList(
      const SelectivityVector& selector,
      const DecodedVector* nestedInputArrays,
      ArrayVector* resultNestedArrayVector) const {
    vector_size_t resultSize = selector.end();

    uint64_t* resultNestedRawNulls = resultNestedArrayVector->mutableRawNulls();
    vector_size_t* resultNestedRawOffsets =
        resultNestedArrayVector->mutableOffsets(resultSize)
            ->asMutable<vector_size_t>();
    vector_size_t* resultNestedRawSizes =
        resultNestedArrayVector->mutableSizes(resultSize)
            ->asMutable<vector_size_t>();

    auto nestedInputArraysPtr = nestedInputArrays->base()->as<ArrayVector>();

    selector.applyToSelected([&](vector_size_t row) {
      auto innerRow = nestedInputArrays->index(row);
      if (nestedInputArrays->isNullAt(innerRow)) {
        bits::setNull(resultNestedRawNulls, row);
      } else {
        bits::clearNull(resultNestedRawNulls, row);
        resultNestedRawOffsets[row] = nestedInputArraysPtr->offsetAt(innerRow);
        resultNestedRawSizes[row] = nestedInputArraysPtr->sizeAt(innerRow);
      }
    });

    ArrayVector* resultArrayVector =
        resultNestedArrayVector->elements()->as<ArrayVector>();
    auto innerSelector =
        SelectivityVector(nestedInputArraysPtr->elements()->size());
    DecodedVector inputArrays(
        *(nestedInputArraysPtr->elements()), innerSelector);

    keepNums(innerSelector, resultArrayVector, &inputArrays);
  }

  void buildCopyRanges(
      std::vector<BaseVector::CopyRange>& ranges,
      std::vector<vector_size_t>& indices,
      const vector_size_t targetIndex) const {
    std::sort(indices.begin(), indices.end());
    std::vector<BaseVector::CopyRange> ret;
    vector_size_t curStart = -1;
    vector_size_t curCount = 0;

    for (auto i : indices) {
      if (curStart < 0) {
        curStart = i;
        curCount = 1;
      } else if (i == curStart + curCount) {
        ++curCount;
      } else {
        ranges.push_back(
            BaseVector::CopyRange{curStart, targetIndex, curCount});
        curStart = i;
        curCount = 1;
      }
    }
    ranges.push_back(BaseVector::CopyRange{curStart, targetIndex, curCount});
  }

  void getCopyRanges(
      std::vector<BaseVector::CopyRange>& ranges,
      const VectorPtr& weights,
      const vector_size_t targetIndex,
      const vector_size_t lo,
      const vector_size_t hi) const {
    std::vector<std::pair<vector_size_t, float>>
        weightedIndices; // vector of (rowIndex, weight) pairs
    weightedIndices.reserve(weights->size());

    const SimpleVector<float>* weightsPtr = weights->as<SimpleVector<float>>();

    for (auto i = lo; i < hi; ++i) {
      auto weightedIndex = std::make_pair(i, weightsPtr->valueAt(i));
      weightedIndices.push_back(weightedIndex);
    }

    std::sort(
        weightedIndices.begin(),
        weightedIndices.end(),
        [](std::pair<vector_size_t, float> lhs,
           std::pair<vector_size_t, float> rhs) {
          return lhs.second > rhs.second;
        });

    vector_size_t numIndices =
        std::min(numToKeep_, (vector_size_t)weightedIndices.size());
    std::vector<vector_size_t> indices;
    indices.reserve(numIndices);
    for (vector_size_t n = 0; n < numIndices; ++n) {
      indices.push_back(weightedIndices[n].first);
    }
    buildCopyRanges(ranges, indices, targetIndex);
  }

  void processIdScorePairList(
      const SelectivityVector& selector,
      const DecodedVector* inputArrays,
      ArrayVector* resultArrayVector) const {
    vector_size_t resultSize = selector.end();

    uint64_t* resultRawNulls = resultArrayVector->mutableRawNulls();
    vector_size_t* resultRawOffsets =
        resultArrayVector->mutableOffsets(resultSize)
            ->asMutable<vector_size_t>();
    vector_size_t* resultRawSizes =
        resultArrayVector->mutableSizes(resultSize)->asMutable<vector_size_t>();

    auto resultRowElementsPtr = resultArrayVector->elements()->as<RowVector>();

    auto inputArraysPtr = inputArrays->base()->as<ArrayVector>();
    auto inputRowElementsPtr = inputArraysPtr->elements()->as<RowVector>();
    auto weightsVector = inputRowElementsPtr->childAt(1);

    std::vector<BaseVector::CopyRange> ranges;

    vector_size_t curOffset = 0;
    selector.applyToSelected([&](vector_size_t row) {
      auto innerRow = inputArrays->index(row);
      if (inputArrays->isNullAt(innerRow)) {
        bits::setNull(resultRawNulls, row);
      } else {
        bits::clearNull(resultRawNulls, row);
        resultRawOffsets[row] = curOffset;
        resultRawSizes[row] =
            std::min(inputArraysPtr->sizeAt(innerRow), numToKeep_);

        auto lo = inputArraysPtr->offsetAt(innerRow);
        auto hi = lo + inputArraysPtr->sizeAt(innerRow);
        getCopyRanges(ranges, weightsVector, curOffset, lo, hi);
        curOffset += resultRawSizes[row];
      }
    });

    resultRowElementsPtr->resize(curOffset);
    resultRowElementsPtr->copyRanges(inputRowElementsPtr, ranges);
  }

 private:
  mutable VectorPtr resultCached_;
  vector_size_t numToKeep_;
  bool sortByScore_;
};

} // anonymous namespace

void registerFirstX() {
  exec::registerStatefulVectorFunction(
      "first_x",
      VectorFirstXFunction::signatures(),
      [](const auto&,
         const std::vector<exec::VectorFunctionArg>& constantInputArgs,
         const velox::core::QueryConfig& /*config*/) {
        vector_size_t numToKeep = readIntegerConstant(constantInputArgs[1]);
        auto sortByScore = true;
        if (constantInputArgs.size() >= 3) {
          sortByScore = readBooleanConstant(constantInputArgs[2]);
        }
        return std::make_shared<VectorFirstXFunction>(numToKeep, sortByScore);
      });

  // Register optimizer metadata
  auto* registry = optimizer::FunctionRegistry::instance();
  auto metadata = std::make_unique<optimizer::FunctionMetadata>();

  // Constraint function: returns the Value of the first argument, but with size
  // limited by the second argument
  metadata->functionConstraint =
      [](optimizer::ExprCP expr,
         optimizer::PlanState& state) -> std::optional<optimizer::Value> {
    using namespace optimizer;

    // Check if expr is a call before casting
    if (expr->type() != PlanType::kCallExpr) {
      return std::nullopt;
    }
    auto* call = expr->as<Call>();
    if (call->args().size() < 2) {
      return std::nullopt;
    }

    const auto& firstArg = call->args()[0];
    const auto& secondArg = call->args()[1];

    // Second argument must be a literal with an integer value
    if (secondArg->type() != PlanType::kLiteralExpr) {
      VELOX_USER_FAIL(
          "first_x: second argument must be a literal integer value");
    }

    auto* literal = secondArg->as<Literal>();

    // Use integerValue() utility to extract the integer
    int64_t limit = integerValue(&literal->literal());

    // Get the Value from the first argument
    Value result = value(state, firstArg);

    // If the size from the first argument > the second argument,
    // then the size is the second argument
    if (result.size > static_cast<float>(limit)) {
      const_cast<float&>(result.size) = static_cast<float>(limit);
    }

    return result;
  };

  registry->registerFunction("first_x", std::move(metadata));
}

} // namespace facebook::axiom::velox_udf
