// Copyright 2004-present Facebook. All Rights Reserved.

#include "axiom/optimizer/tests/utils/ConvertFormat.h"

#include <velox/expression/VectorFunction.h>
#include <velox/vector/ComplexVector.h>

#include "axiom/optimizer/Filters.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/QueryGraph.h"

namespace facebook::axiom::velox_udf {

using namespace facebook::velox;

namespace {

// Convert MAP(KEY, VALUE) to ARRAY(ROW(KEY, VALUE))
// This is a zero-copy metadata operation that reuses the underlying data
// vectors
class VectorConvertFormatFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& selector,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 1, "convert_format expects 1 argument");
    VELOX_CHECK_EQ(
        args[0]->typeKind(), TypeKind::ROW, "convert_format expects ROW input");

    std::vector<VectorPtr> children;
    exec::LocalDecodedVector decodedInputRow(context, *args[0], selector);
    auto* inputRow = decodedInputRow->base()->as<RowVector>();

    // Process each child MAP in the input ROW
    // Convert each MAP<K,V> to ARRAY<ROW<K,V>>
    for (size_t i = 0; i < inputRow->children().size(); ++i) {
      auto& child = inputRow->children()[i];

      exec::LocalDecodedVector decodedMap(context, *child, selector);
      auto* childMap = decodedMap->base()->as<MapVector>();

      // Create a ROW vector with the map's keys and values as children
      // This is zero-copy - we're reusing the existing key and value vectors
      VectorPtr row = std::make_shared<RowVector>(
          context.pool(),
          outputType->childAt(i)->childAt(0), // ARRAY's element type is ROW
          nullptr, // no nulls at the ROW level
          childMap->mapKeys()->size(),
          std::vector<VectorPtr>{childMap->mapKeys(), childMap->mapValues()});

      // Wrap the ROW in an ARRAY using the MAP's offsets and sizes
      // This maintains the same structure: each row in the original MAP
      // becomes a row in the output ARRAY, with the same number of elements
      VectorPtr array = std::make_shared<ArrayVector>(
          context.pool(),
          outputType->childAt(i), // ARRAY type
          allocateNulls(childMap->size(), context.pool()),
          childMap->size(),
          childMap->offsets(), // Reuse MAP's offsets
          childMap->sizes(), // Reuse MAP's sizes
          std::move(row));

      // Handle any encoding transformations (dictionary, constant, etc.)
      if (!decodedMap->isIdentityMapping()) {
        array = decodedMap->wrap(array, *child, selector);
      }

      children.emplace_back(std::move(array));
    }

    // Create the output ROW with all converted children
    VectorPtr localResult = std::make_shared<RowVector>(
        context.pool(),
        outputType,
        inputRow->nulls(),
        inputRow->size(),
        std::move(children));

    // Handle any encoding transformations on the outer ROW
    if (!decodedInputRow->isIdentityMapping()) {
      localResult = decodedInputRow->wrap(localResult, *args[0], selector);
    }

    context.moveOrCopyResult(localResult, selector, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // We need to generate signatures for common patterns
    // ROW<MAP<BIGINT,REAL>, ...> -> ROW<ARRAY<ROW<BIGINT,REAL>>, ...>
    // The keys are BIGINT and the values are REAL

    std::vector<std::shared_ptr<exec::FunctionSignature>> sigs;

    // Signature that accepts ROW of MAP<BIGINT,REAL> and returns ROW of
    // ARRAY<ROW<BIGINT,REAL>> The return type will be determined dynamically
    sigs.push_back(
        exec::FunctionSignatureBuilder()
            .returnType("row(array(row(bigint,real)))")
            .argumentType("row(map(bigint,real))")
            .variableArity()
            .build());

    return sigs;
  }
};

// Helper to resolve the output type from input type
TypePtr resolveConvertFormatType(const TypePtr& inputType) {
  VELOX_CHECK_EQ(
      inputType->kind(),
      TypeKind::ROW,
      "convert_format expects ROW input, got {}",
      inputType->toString());

  auto& rowType = inputType->asRow();
  std::vector<TypePtr> outputChildTypes;
  std::vector<std::string> outputNames;

  for (size_t i = 0; i < rowType.size(); ++i) {
    const auto& childType = rowType.childAt(i);

    VELOX_CHECK_EQ(
        childType->kind(),
        TypeKind::MAP,
        "convert_format expects all children to be MAPs, got {} at position {}",
        childType->toString(),
        i);

    auto& mapType = childType->asMap();

    // Create ARRAY<ROW<key_type, value_type>>
    auto rowElementType = ROW({mapType.keyType(), mapType.valueType()});
    auto arrayType = ARRAY(rowElementType);

    outputChildTypes.push_back(arrayType);
    outputNames.push_back(rowType.nameOf(i));
  }

  return ROW(std::move(outputNames), std::move(outputChildTypes));
}

} // namespace

void registerConvertFormat(const std::string& prefix) {
  auto functionName = prefix + "convert_format";

  exec::registerStatefulVectorFunction(
      functionName,
      VectorConvertFormatFunction::signatures(),
      [](const std::string& /* functionName */,
         const std::vector<exec::VectorFunctionArg>& inputArgs,
         const velox::core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::VectorFunction> {
        // Validate and resolve the output type
        VELOX_CHECK_EQ(
            inputArgs.size(), 1, "convert_format expects exactly 1 argument");

        // The output type is determined by the input type
        // This happens at query compilation time
        return std::make_unique<VectorConvertFormatFunction>();
      });

  // Register optimizer metadata
  auto* registry = optimizer::FunctionRegistry::instance();
  auto metadata = std::make_unique<optimizer::FunctionMetadata>();

  // Constraint function: returns a Value with the same structure as the return
  // type The Value::size for the array of row<key, value> is the Value::size
  // from the corresponding map argument
  metadata->functionConstraint =
      [](optimizer::ExprCP expr,
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

    const auto& firstArg = call->args()[0];
    const auto& firstArgValue = value(state, firstArg);

    // Result must be a ROW type (convert_format returns ROW)
    const auto& exprValue = value(state, expr);
    if (exprValue.type->kind() != velox::TypeKind::ROW) {
      return std::nullopt;
    }

    // First argument must also be a ROW type containing MAPs
    if (firstArgValue.type->kind() != velox::TypeKind::ROW) {
      return std::nullopt;
    }

    auto& inputRowType = firstArgValue.type->as<velox::TypeKind::ROW>();
    auto& outputRowType = exprValue.type->as<velox::TypeKind::ROW>();

    // Create a Value for the result with children
    Value result = exprValue;

    // If the first argument has children set
    if (firstArgValue.children) {
      auto* childValues = make<ChildValues>();
      childValues->names.reserve(outputRowType.size());
      childValues->values.reserve(outputRowType.size());

      // For each field in the input ROW (which should be MAPs)
      for (size_t i = 0; i < inputRowType.size() && i < outputRowType.size();
           ++i) {
        childValues->names.push_back(toName(outputRowType.nameOf(i)));

        // Get the Value from the corresponding input map
        Value mapValue(inputRowType.childAt(i).get(), 1.0f);
        if (i < firstArgValue.children->values.size()) {
          mapValue = firstArgValue.children->values[i];
        }

        // Create a Value for the output ARRAY<ROW<K,V>> with the same size as
        // the input MAP The size represents the number of key-value pairs
        Value arrayValue(outputRowType.childAt(i).get(), mapValue.size);

        childValues->values.push_back(arrayValue);
      }

      const_cast<const ChildValues*&>(result.children) = childValues;
    }

    return result;
  };

  registry->registerFunction(functionName, std::move(metadata));
}

} // namespace facebook::axiom::velox_udf
