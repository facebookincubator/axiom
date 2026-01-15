// Copyright 2004-present Facebook. All Rights Reserved.

#include "axiom/optimizer/tests/utils/SigridHash.h"

#include <velox/expression/DecodedArgs.h>
#include <velox/expression/VectorFunction.h>

#include "axiom/optimizer/Filters.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/tests/utils/ComputeSigridHash.h"
#include "axiom/optimizer/tests/utils/Utils.h"

namespace facebook::axiom::velox_udf {

using namespace facebook::velox;

namespace {

using uint128_t = unsigned __int128;

std::tuple<uint64_t, int> computeMultiplierAndShift(
    int64_t divisor,
    int precision) {
  constexpr int N = 64;
  int l = ceil(std::log2(divisor));
  uint128_t low = (static_cast<uint128_t>(1) << (N + l)) / divisor;
  uint128_t high = ((static_cast<uint128_t>(1) << (N + l)) +
                    ((static_cast<uint128_t>(1) << (N + l - precision)))) /
      divisor;
  while (low / 2 < high / 2 && l > 0) {
    low = low / 2;
    high = high / 2;
    --l;
  }
  return std::make_tuple((uint64_t)high, l);
}

struct State {
  int64_t salt;
  int64_t maxValue;
  uint64_t multiplier;
  int shift;
  memory::MemoryPool* pool;
};

template <typename OutputT>
struct SigridHash : exec::VectorFunction {
  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    std::vector<const char*> inputs;
    std::vector<const char*> outputs;
    if constexpr (sizeof(OutputT) == 4) {
      inputs = {
          "ARRAY(INTEGER)",
          "ARRAY(BIGINT)",
          "ARRAY(ROW(INTEGER, REAL))",
          "ARRAY(ROW(BIGINT, REAL))",
          "ARRAY(ARRAY(BIGINT))",
      };
      outputs = {
          "ARRAY(INTEGER)",
          "ARRAY(INTEGER)",
          "ARRAY(ROW(INTEGER, REAL))",
          "ARRAY(ROW(INTEGER, REAL))",
          "ARRAY(ARRAY(INTEGER))",
      };
    } else {
      inputs = outputs = {
          "ARRAY(BIGINT)",
          "ARRAY(ROW(BIGINT, REAL))",
          "ARRAY(ARRAY(BIGINT))",
      };
    }
    VELOX_DCHECK_EQ(inputs.size(), outputs.size());
    std::vector<std::shared_ptr<exec::FunctionSignature>> ans;
    for (const char* salt : {"INTEGER", "BIGINT"}) {
      for (const char* maxValue : {"INTEGER", "BIGINT"}) {
        for (int i = 0; i < inputs.size(); ++i) {
          ans.push_back(
              exec::FunctionSignatureBuilder()
                  .returnType(outputs[i])
                  .argumentType(inputs[i])
                  .argumentType(salt)
                  .argumentType(maxValue)
                  .build());
        }
      }
    }
    return ans;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 3);
    exec::DecodedArgs decoded(rows, args, context);
    auto salt = readIntegerConstant(*decoded.at(1));
    auto maxValue = readIntegerConstant(*decoded.at(2));
    uint64_t multiplier = 1;
    int shift = 0;
    if (maxValue > 0) {
      std::tie(multiplier, shift) = computeMultiplierAndShift(maxValue, 63);
    }
    State state{
        .salt = salt,
        .maxValue = maxValue,
        .multiplier = multiplier,
        .shift = shift,
        .pool = context.pool(),
    };
    auto& input = *decoded.at(0)->base()->asUnchecked<ArrayVector>();
    auto& inputElementType = *input.type()->asArray().elementType();
    VectorPtr localResult;
    switch (inputElementType.kind()) {
      case TypeKind::INTEGER:
        localResult = processIdList<int32_t>(state, input, outputType);
        break;
      case TypeKind::BIGINT:
        localResult = processIdList<int64_t>(state, input, outputType);
        break;
      case TypeKind::ROW:
        switch (inputElementType.asRow().childAt(0)->kind()) {
          case TypeKind::INTEGER:
            localResult = processIdScoreList<int32_t>(state, input, outputType);
            break;
          case TypeKind::BIGINT:
            localResult = processIdScoreList<int64_t>(state, input, outputType);
            break;
          default:
            VELOX_UNREACHABLE();
        }
        break;
      case TypeKind::ARRAY:
        VELOX_DCHECK_EQ(
            inputElementType.asArray().elementType()->kind(), TypeKind::BIGINT);
        localResult = processEventIdList<int64_t>(state, input, outputType);
        break;
      default:
        VELOX_UNREACHABLE();
    }
    if (!decoded.at(0)->isIdentityMapping()) {
      localResult = decoded.at(0)->wrap(localResult, *args[0], rows);
    }
    context.moveOrCopyResult(localResult, rows, result);
  }

 private:
  template <typename InputT>
  static VectorPtr processIdList(
      const State& state,
      const ArrayVector& input,
      const TypePtr& resultType) {
    return std::make_shared<ArrayVector>(
        state.pool,
        resultType,
        input.nulls(),
        input.size(),
        input.offsets(),
        input.sizes(),
        processIds<InputT>(state, *input.elements()));
  }

  template <typename InputT>
  static VectorPtr processIdScoreList(
      const State& state,
      const ArrayVector& input,
      const TypePtr& resultType) {
    auto& rows = *input.elements()->asUnchecked<RowVector>();
    auto ids = processIds<InputT>(state, *rows.childAt(0));
    return std::make_shared<ArrayVector>(
        state.pool,
        resultType,
        input.nulls(),
        input.size(),
        input.offsets(),
        input.sizes(),
        std::make_shared<RowVector>(
            state.pool,
            resultType->childAt(0),
            rows.nulls(),
            rows.size(),
            std::vector<VectorPtr>({ids, rows.childAt(1)})));
  }

  template <typename InputT>
  static VectorPtr processEventIdList(
      const State& state,
      const ArrayVector& input,
      const TypePtr& resultType) {
    auto& innerArray = *input.elements()->asUnchecked<ArrayVector>();
    return std::make_shared<ArrayVector>(
        state.pool,
        resultType,
        input.nulls(),
        input.size(),
        input.offsets(),
        input.sizes(),
        std::make_shared<ArrayVector>(
            state.pool,
            resultType->childAt(0),
            innerArray.nulls(),
            innerArray.size(),
            innerArray.offsets(),
            innerArray.sizes(),
            processIds<InputT>(state, *innerArray.elements())));
  }

  template <typename InputT>
  static VectorPtr processIds(const State& state, const BaseVector& input) {
    auto [salt, maxValue, multiplier, shift, pool] = state;
    VectorPtr output;
    SelectivityVector rows(input.size());
    DecodedVector decoded(input, rows);
    if (decoded.base()->isConstantEncoding()) {
      auto rawInput =
          decoded.base()->asUnchecked<ConstantVector<InputT>>()->rawValues();
      OutputT outVal;
      ComputeSigridHash<InputT, OutputT>::scalar({
          .rows = 1,
          .input = rawInput,
          .output = &outVal,
          .salt = salt,
          .maxValue = maxValue,
          .multiplier = multiplier,
          .shift = shift,
      });
      output = std::make_shared<ConstantVector<OutputT>>(
          pool, 1, false, CppToType<OutputT>::create(), std::move(outVal));
    } else {
      size_t length = decoded.base()->size();
      output = std::make_shared<FlatVector<OutputT>>(
          pool,
          CppToType<OutputT>::create(),
          nullptr,
          length,
          AlignedBuffer::allocate<OutputT>(length, pool),
          std::vector<BufferPtr>());
      auto rawInput = decoded.base()->asFlatVector<InputT>()->rawValues();
      auto rawOutput = output->asFlatVector<OutputT>()->mutableRawValues();
      // AVX2 is not much faster than generic so we omit it here.
#ifdef __AVX512F__
      using ArchList = xsimd::arch_list<xsimd::avx512bw, xsimd::generic>;
#else
      using ArchList = xsimd::arch_list<xsimd::generic>;
#endif
      ComputeSigridHash<InputT, OutputT> compute;
      typename ComputeSigridHash<InputT, OutputT>::Parameters params{
          .rows = length,
          .input = rawInput,
          .output = rawOutput,
          .salt = salt,
          .maxValue = maxValue,
          .multiplier = multiplier,
          .shift = shift,
      };
      xsimd::dispatch<ArchList>(compute)(params);
    }
    if (!decoded.isIdentityMapping()) {
      output = decoded.wrap(output, input, rows);
    }
    return output;
  }
};

} // namespace

// Velox native sigrid_hash implementation currently contains logic that
// diverges from the Koski UDF. Do not use it until this issue is resolved.
// Context in
// https://fb.workplace.com/groups/2059176377881758/permalink/2221400434992684/.
void registerSigridHash(const std::string& prefix) {
  constexpr auto kSuffix = "_velox_debug_only";

  exec::registerVectorFunction(
      prefix + "sigrid_hash" + kSuffix,
      SigridHash<int64_t>::signatures(),
      std::make_unique<SigridHash<int64_t>>());
  exec::registerVectorFunction(
      prefix + "sigrid_hash_into_i32" + kSuffix,
      SigridHash<int32_t>::signatures(),
      std::make_unique<SigridHash<int32_t>>());

  // Register optimizer metadata for both variants
  auto* registry = optimizer::FunctionRegistry::instance();

  // Helper lambda for creating constraint functions
  auto makeConstraint = [](bool isInt32) {
    return [isInt32](
               optimizer::ExprCP expr,
               optimizer::PlanState& state) -> std::optional<optimizer::Value> {
      using namespace optimizer;

      // Check if expr is a call before casting
      if (expr->type() != PlanType::kCallExpr) {
        return std::nullopt;
      }
      auto* call = expr->as<Call>();
      if (call->args().empty()) {
        return std::nullopt;
      }

      // Get the Value from the first argument
      const auto& firstArgValue = value(state, call->args()[0]);

      // Create a Value with the function's return type
      Value result = value(state, expr);

      // Create children with one value representing the element type
      auto* childValues = make<ChildValues>();
      childValues->values.reserve(1);

      // Create a value for the array element (int64 or int32)
      const velox::Type* elementType = isInt32
          ? static_cast<const velox::Type*>(INTEGER().get())
          : static_cast<const velox::Type*>(BIGINT().get());
      Value elementValue(elementType, firstArgValue.size);

      childValues->values.push_back(elementValue);

      const_cast<const ChildValues*&>(result.children) = childValues;
      const_cast<float&>(result.size) = firstArgValue.size;

      return result;
    };
  };

  {
    auto metadata = std::make_unique<optimizer::FunctionMetadata>();
    metadata->functionConstraint = makeConstraint(false);
    registry->registerFunction(
        prefix + "sigrid_hash" + kSuffix, std::move(metadata));
  }

  {
    auto metadata = std::make_unique<optimizer::FunctionMetadata>();
    metadata->functionConstraint = makeConstraint(true);
    registry->registerFunction(
        prefix + "sigrid_hash_into_i32" + kSuffix, std::move(metadata));
  }
}

} // namespace facebook::axiom::velox_udf
