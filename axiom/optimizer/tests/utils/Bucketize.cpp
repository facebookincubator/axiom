#include "axiom/optimizer/tests/utils/Bucketize.h"
#include <velox/type/Type.h>
#include <velox/vector/TypeAliases.h>
#include <memory>

#include "axiom/optimizer/Filters.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/tests/utils/BucketizeImpl.h"
#include "velox/expression/VectorFunction.h"

namespace facebook::axiom::velox_udf {
using namespace facebook::velox;

namespace {
template <typename T>
int32_t computeBucketId(const T& borders, float val) {
  auto index = borders.find(val);
  if (index >= borders.count() - 1) {
    return index;
  }
  return val < borders[index + 1] ? index : index + 1;
}

template <typename T, typename BordersIndex, bool AsList>
class BucketizeScalar : public exec::VectorFunction {
 public:
  explicit BucketizeScalar(const std::vector<std::vector<float>>& borders) {
    for (const auto& border : borders) {
      borders_.emplace_back(std::make_unique<BordersIndex>(border));
    }
  }

  void apply(
      const SelectivityVector& selector,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 2);

    if constexpr (AsList) {
      if (args[0]->isConstantEncoding()) {
        // Get the constant value as a float.
        float val =
            static_cast<float>(args[0]->as<ConstantVector<T>>()->valueAt(0));

        // Construct the elements of the constant result array.
        auto resultElements = std::make_shared<FlatVector<int64_t>>(
            context.pool(),
            BIGINT(),
            nullptr,
            borders_.size(),
            AlignedBuffer::allocate<int64_t>(borders_.size(), context.pool()),
            std::vector<BufferPtr>{});
        int64_t* rawElements = resultElements->mutableRawValues();

        // Compute the bucket IDs and populate the elements.
        size_t offset = 0;
        size_t startIndex = 0;
        for (const auto& border : borders_) {
          rawElements[offset++] = computeBucketId(*border, val) + startIndex;
          startIndex += border->count() + 1;
        }

        // Construct the constant result array.
        auto resultArray = std::make_shared<ArrayVector>(
            context.pool(),
            outputType,
            nullptr,
            1,
            AlignedBuffer::allocate<vector_size_t>(1, context.pool(), 0),
            AlignedBuffer::allocate<vector_size_t>(
                1, context.pool(), borders_.size()),
            resultElements);
        context.moveOrCopyResult(
            BaseVector::wrapInConstant(selector.end(), 0, resultArray),
            selector,
            result);

        return;
      }

      // Since this UDF takes 2 arguments, and one is always constant, we can
      // assume the other is constant or flat.
      VELOX_CHECK_EQ(args[0]->encoding(), VectorEncoding::Simple::FLAT);

      BaseVector::ensureWritable(selector, outputType, context.pool(), result);
      auto* resultArray = result->as<ArrayVector>();
      auto* resultOffsets = resultArray->mutableOffsets(selector.end())
                                ->asMutable<vector_size_t>();
      auto* resultSizes =
          resultArray->mutableSizes(selector.end())->asMutable<vector_size_t>();
      auto* resultElements = resultArray->elements()->as<FlatVector<int64_t>>();
      resultElements->resize(selector.countSelected() * borders_.size());
      int64_t* rawElements = resultElements->mutableRawValues();

      auto* input = args[0]->as<FlatVector<T>>()->rawValues();
      size_t elementsOffset = 0;
      selector.applyToSelected([&](auto row) {
        float val = static_cast<float>(input[row]);
        resultOffsets[row] = elementsOffset;
        resultSizes[row] = borders_.size();
        size_t startIndex = 0;
        for (const auto& border : borders_) {
          rawElements[elementsOffset++] =
              computeBucketId(*border, val) + startIndex;
          startIndex += border->count() + 1;
        }
      });
    } else {
      if (args[0]->isConstantEncoding()) {
        // Get the constant value as a float.
        float val =
            static_cast<float>(args[0]->as<ConstantVector<T>>()->valueAt(0));
        int64_t resultVal = computeBucketId(*borders_[0], val);

        context.moveOrCopyResult(
            BaseVector::createConstant(
                BIGINT(), resultVal, selector.end(), context.pool()),
            selector,
            result);

        return;
      }

      // Since this UDF takes 2 arguments, and one is always constant, we can
      // assume the other is constant or flat.
      VELOX_CHECK_EQ(args[0]->encoding(), VectorEncoding::Simple::FLAT);

      BaseVector::ensureWritable(selector, outputType, context.pool(), result);
      result->resize(selector.end());

      auto* input = args[0]->as<FlatVector<T>>()->rawValues();

      // Handle both BIGINT and INTEGER return types based on what Velox
      // allocated
      if (outputType->kind() == TypeKind::BIGINT) {
        int64_t* rawResults = result->values()->asMutable<int64_t>();
        selector.applyToSelected([&](auto row) {
          float val = static_cast<float>(input[row]);
          rawResults[row] = computeBucketId(*borders_[0], val);
        });
      } else {
        // INTEGER return type
        int32_t* rawResults = result->values()->asMutable<int32_t>();
        selector.applyToSelected([&](auto row) {
          float val = static_cast<float>(input[row]);
          rawResults[row] = computeBucketId(*borders_[0], val);
        });
      }
    }
  }

 private:
  std::vector<std::unique_ptr<BordersIndex>> borders_;
};

template <typename T, typename BordersIndex>
class BucketizeArray : public exec::VectorFunction {
 public:
  explicit BucketizeArray(const std::vector<std::vector<float>>& borders) {
    for (const auto& border : borders) {
      borders_.emplace_back(std::make_unique<BordersIndex>(border));
    }
  }

  void apply(
      const SelectivityVector& selector,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 2);

    if (args[0]->isConstantEncoding()) {
      // Get the constant value as an array.
      auto* constantArray = args[0]->as<ConstantVector<ComplexType>>();
      const auto* flatArray = constantArray->valueVector()->as<ArrayVector>();
      const auto flatIndex = constantArray->index();
      const auto arrayOffset = flatArray->offsetAt(flatIndex);
      const auto arraySize = flatArray->sizeAt(flatIndex);

      exec::LocalSelectivityVector innerSelector(
          context, flatArray->elements()->size());
      innerSelector->clearAll();
      innerSelector->setValidRange(arrayOffset, arrayOffset + arraySize, true);
      exec::LocalDecodedVector decodedElements(
          context, *flatArray->elements(), *innerSelector);
      auto* elements = decodedElements->base()->as<SimpleVector<T>>();

      // Construct the elements of the constant result array.
      auto resultElements = std::make_shared<FlatVector<int64_t>>(
          context.pool(),
          BIGINT(),
          nullptr,
          arraySize,
          AlignedBuffer::allocate<int64_t>(arraySize, context.pool()),
          std::vector<BufferPtr>{});
      int64_t* rawElements = resultElements->mutableRawValues();

      // Compute the bucket IDs and populate the elements.
      size_t offset = 0;
      for (size_t i = arrayOffset; i < arrayOffset + arraySize; ++i) {
        rawElements[offset++] = computeBucketId(
            *borders_[0], static_cast<float>(elements->valueAt(i)));
      }

      // Construct the constant result array.
      auto resultArray = std::make_shared<ArrayVector>(
          context.pool(),
          outputType,
          nullptr,
          1,
          AlignedBuffer::allocate<vector_size_t>(1, context.pool(), 0),
          AlignedBuffer::allocate<vector_size_t>(1, context.pool(), arraySize),
          resultElements);
      context.moveOrCopyResult(
          BaseVector::wrapInConstant(selector.end(), 0, resultArray),
          selector,
          result);

      return;
    }

    // Since this UDF takes 2 arguments, and one is always constant, we can
    // assume the other is constant or flat.
    VELOX_CHECK_EQ(args[0]->encoding(), VectorEncoding::Simple::ARRAY);

    auto* inputArray = args[0]->as<ArrayVector>();
    exec::LocalSelectivityVector innerSelector{
        context, inputArray->elements()->size()};
    innerSelector->setAll();
    exec::LocalDecodedVector decodedElements(
        context, *inputArray->elements(), *innerSelector);
    auto* elements = decodedElements->base()->as<SimpleVector<T>>();

    BaseVector::ensureWritable(selector, outputType, context.pool(), result);
    auto* resultArray = result->as<ArrayVector>();
    auto* resultOffsets =
        resultArray->mutableOffsets(selector.end())->asMutable<vector_size_t>();
    auto* resultSizes =
        resultArray->mutableSizes(selector.end())->asMutable<vector_size_t>();
    auto* resultElements = resultArray->elements()->as<FlatVector<int64_t>>();
    auto baseResultElementsSize = resultElements->size();
    resultElements->resize(
        baseResultElementsSize + inputArray->elements()->size());
    int64_t* rawElements = resultElements->mutableRawValues();

    size_t elementsOffset = baseResultElementsSize;
    selector.applyToSelected([&](auto row) {
      resultOffsets[row] = elementsOffset;
      resultSizes[row] = inputArray->sizeAt(row);
      for (size_t i = inputArray->offsetAt(row);
           i < inputArray->offsetAt(row) + inputArray->sizeAt(row);
           ++i) {
        rawElements[elementsOffset++] = computeBucketId(
            *borders_[0], static_cast<float>(elements->valueAt(i)));
      }
    });
  }

 private:
  std::vector<std::unique_ptr<BordersIndex>> borders_;
};

template <typename T>
inline void
validateBordersSpec(T* borders, vector_size_t offset, vector_size_t size) {
  VELOX_USER_CHECK_GT(size, 0, "Borders should not be empty.");
  for (auto i = offset + 1; i < offset + size; ++i) {
    VELOX_USER_CHECK_GE(
        borders[i],
        borders[i - 1],
        "Borders should have non-decreasing sequence.");
    if (i > offset + 1 && (borders[i] == borders[i - 1])) {
      if (!(borders[i - 2] < borders[i])) {
        std::string err_detail;
        for (auto j = offset; j < offset + size; j++) {
          err_detail += folly::to<std::string>(borders[j]) + ", ";
        }
        VELOX_USER_FAIL(
            fmt::format(
                "Borders should not have more than 2 repeated values, got: loc {}, array: {}",
                i,
                err_detail));
      }
    }
  }
}

template <typename T>
std::vector<float> populateBorders(
    const VectorPtr& borders,
    vector_size_t offset,
    vector_size_t size) {
  auto* rawBorders = borders->asFlatVector<T>()->rawValues();
  validateBordersSpec(rawBorders, offset, size);

  std::vector<float> result;
  for (size_t i = offset; i < offset + size; i++) {
    const auto v = util::Converter<TypeKind::REAL>::tryCast(rawBorders[i])
                       .thenOrThrow(folly::identity, [&](const Status& status) {
                         VELOX_USER_FAIL(status.message());
                       });
    result.push_back(v);
  }

  return result;
}

template <typename T>
std::unique_ptr<velox::exec::VectorFunction> createBucketizeScalar(
    const std::vector<std::vector<float>>& borders,
    bool asList) {
  if (asList) {
    return std::make_unique<
        BucketizeScalar<T, df4ai::detail::BordersIndexAvx2<0>, true>>(borders);
  }

  auto count = borders[0].size();
  if (count < df4ai::detail::BordersIndexAvx2<1>::Traits::kMaxCount) {
    return std::make_unique<
        BucketizeScalar<T, df4ai::detail::BordersIndexAvx2<1>, false>>(borders);
  } else if (count < df4ai::detail::BordersIndexAvx2<2>::Traits::kMaxCount) {
    return std::make_unique<
        BucketizeScalar<T, df4ai::detail::BordersIndexAvx2<2>, false>>(borders);
  } else if (count < df4ai::detail::BordersIndexAvx2<3>::Traits::kMaxCount) {
    return std::make_unique<
        BucketizeScalar<T, df4ai::detail::BordersIndexAvx2<3>, false>>(borders);
  } else if (count < df4ai::detail::BordersIndexAvx2<4>::Traits::kMaxCount) {
    return std::make_unique<
        BucketizeScalar<T, df4ai::detail::BordersIndexAvx2<4>, false>>(borders);
  } else {
    return std::make_unique<
        BucketizeScalar<T, df4ai::detail::BordersIndexAvx2<0>, false>>(borders);
  }
}

template <typename T>
std::unique_ptr<velox::exec::VectorFunction> createBucketizeArray(
    const std::vector<std::vector<float>>& borders) {
  auto count = borders[0].size();
  if (count < df4ai::detail::BordersIndexAvx2<1>::Traits::kMaxCount) {
    return std::make_unique<
        BucketizeArray<T, df4ai::detail::BordersIndexAvx2<1>>>(borders);
  } else if (count < df4ai::detail::BordersIndexAvx2<2>::Traits::kMaxCount) {
    return std::make_unique<
        BucketizeArray<T, df4ai::detail::BordersIndexAvx2<2>>>(borders);
  } else if (count < df4ai::detail::BordersIndexAvx2<3>::Traits::kMaxCount) {
    return std::make_unique<
        BucketizeArray<T, df4ai::detail::BordersIndexAvx2<3>>>(borders);
  } else if (count < df4ai::detail::BordersIndexAvx2<4>::Traits::kMaxCount) {
    return std::make_unique<
        BucketizeArray<T, df4ai::detail::BordersIndexAvx2<4>>>(borders);
  } else {
    return std::make_unique<
        BucketizeArray<T, df4ai::detail::BordersIndexAvx2<0>>>(borders);
  }
}

void addBorder(
    const ArrayVector* array,
    vector_size_t offset,
    std::vector<std::vector<float>>& borders) {
  switch (array->elements()->typeKind()) {
    case TypeKind::REAL:
      borders.emplace_back(
          populateBorders<float>(
              array->elements(),
              array->offsetAt(offset),
              array->sizeAt(offset)));
      break;
    case TypeKind::DOUBLE:
      borders.emplace_back(
          populateBorders<double>(
              array->elements(),
              array->offsetAt(offset),
              array->sizeAt(offset)));
      break;
    case TypeKind::INTEGER:
      borders.emplace_back(
          populateBorders<int32_t>(
              array->elements(),
              array->offsetAt(offset),
              array->sizeAt(offset)));
      break;
    case TypeKind::BIGINT:
      borders.emplace_back(
          populateBorders<int64_t>(
              array->elements(),
              array->offsetAt(offset),
              array->sizeAt(offset)));
      break;
    default:
      VELOX_FAIL("Unsupported type {}", array->elements()->type()->toString());
  }
}
} // anonymous namespace

void registerBucketize(const std::string& prefix) {
  auto kFunctionName = prefix + "bucketize";
  std::vector<std::string> supportedFeatureTypes{"real", "integer", "bigint"};
  std::vector<std::string> supportedBucketTypes{
      "real", "double", "integer", "bigint"};

  std::vector<exec::FunctionSignaturePtr> signatures;
  for (const auto& featureType : supportedFeatureTypes) {
    for (const auto& bucketType : supportedBucketTypes) {
      signatures.push_back(
          exec::FunctionSignatureBuilder()
              .returnType("bigint")
              .argumentType(featureType)
              .argumentType(fmt::format("array({})", bucketType))
              .build());

      signatures.push_back(
          exec::FunctionSignatureBuilder()
              .returnType("array(bigint)")
              .argumentType(featureType)
              .argumentType(fmt::format("array(array({}))", bucketType))
              .build());

      signatures.push_back(
          exec::FunctionSignatureBuilder()
              .returnType("array(bigint)")
              .argumentType(fmt::format("array({})", featureType))
              .argumentType(fmt::format("array({})", bucketType))
              .build());
    }
  }

  exec::registerStatefulVectorFunction(
      kFunctionName,
      std::move(signatures),
      [](const std::string& /* functionName */,
         const std::vector<exec::VectorFunctionArg>& inputArgs,
         const velox::core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::VectorFunction> {
        VELOX_CHECK_EQ(inputArgs.size(), 2);
        VELOX_CHECK_NOT_NULL(inputArgs[1].constantValue);

        std::vector<std::vector<float>> borders;

        auto bordersVector =
            inputArgs[1].constantValue->as<ConstantVector<ComplexType>>();
        auto offset = bordersVector->index();
        auto* bordersArrayVector =
            bordersVector->valueVector()->as<ArrayVector>();
        bool bordersIsArray =
            bordersArrayVector->elements()->typeKind() == TypeKind::ARRAY;
        if (bordersIsArray) {
          auto* innerBorders =
              bordersArrayVector->elements()->as<ArrayVector>();

          for (auto i = bordersArrayVector->offsetAt(offset);
               i < bordersArrayVector->offsetAt(offset) +
                   bordersArrayVector->sizeAt(offset);
               i++) {
            addBorder(innerBorders, i, borders);
          }
        } else {
          addBorder(bordersArrayVector, offset, borders);
        }

        if (inputArgs[0].type->kind() == TypeKind::ARRAY) {
          switch (inputArgs[0].type->childAt(0)->kind()) {
            case TypeKind::REAL:
              return createBucketizeArray<float>(borders);
            case TypeKind::INTEGER:
              return createBucketizeArray<int32_t>(borders);
            case TypeKind::BIGINT:
              return createBucketizeArray<int64_t>(borders);
            default:
              VELOX_UNSUPPORTED(
                  "Unsupported type {}", inputArgs[0].type->toString());
          }
        } else {
          switch (inputArgs[0].type->kind()) {
            case TypeKind::REAL:
              return createBucketizeScalar<float>(borders, bordersIsArray);
            case TypeKind::INTEGER:
              return createBucketizeScalar<int32_t>(borders, bordersIsArray);
            case TypeKind::BIGINT:
              return createBucketizeScalar<int64_t>(borders, bordersIsArray);
            default:
              VELOX_UNSUPPORTED(
                  "Unsupported type {}", inputArgs[0].type->toString());
          }
        }
      });

  // Register optimizer metadata
  auto* registry = optimizer::FunctionRegistry::instance();
  auto metadata = std::make_unique<optimizer::FunctionMetadata>();

  // Cost function: 0.2 * number of elements in the array (second argument)
  metadata->costFunc =
      [](const optimizer::Call* call,
         const optimizer::ConstraintMap* constraints) -> float {
    using namespace optimizer;

    if (call->args().size() < 2) {
      return 0.2f;
    }

    const auto& secondArg = call->args()[1];
    // Check if it's a literal with an array
    if (secondArg->type() != PlanType::kLiteralExpr) {
      return 0.2f;
    }

    auto* literal = secondArg->as<Literal>();
    const auto& literalValue = literal->literal();

    // Get the array size
    if (literalValue.kind() == velox::TypeKind::ARRAY) {
      const auto& array = literalValue.value<velox::TypeKind::ARRAY>();
      return 0.2f * static_cast<float>(array.size());
    }

    return 0.2f;
  };

  // Constraint function: returns a Value with cardinality = size of array in
  // second argument
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

    const auto& secondArg = call->args()[1];
    // Check if it's a literal with an array
    if (secondArg->type() != PlanType::kLiteralExpr) {
      return std::nullopt;
    }

    auto* literal = secondArg->as<Literal>();
    const auto& literalValue = literal->literal();

    // Get the array size to determine cardinality
    float cardinality = 1.0f;
    if (literalValue.kind() == velox::TypeKind::ARRAY) {
      const auto& array = literalValue.value<velox::TypeKind::ARRAY>();
      cardinality = static_cast<float>(array.size());
    }

    // Create a Value with the function's return type and the determined
    // cardinality
    Value result(value(state, expr).type, cardinality);

    return result;
  };

  registry->registerFunction(kFunctionName, std::move(metadata));
}
} // namespace facebook::axiom::velox_udf
