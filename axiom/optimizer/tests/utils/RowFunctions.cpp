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

#include "axiom/optimizer/tests/utils/DfFunctions.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/FunctionRegistry.h"

namespace facebook::velox::optimizer::test {

class MakeRowFromMapFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_UNREACHABLE();
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {
        exec::FunctionSignatureBuilder()
            .returnType("row(real)")
            .argumentType("map(integer, real)")
            .build(),
        exec::FunctionSignatureBuilder()
            .returnType("row(real)")
            .argumentType("map(integer, array(bigint))")
            .build(),
        exec::FunctionSignatureBuilder()

            .returnType("ROW(real)")
            .argumentType("map(integer, map(bigint, real))")
            .build()};
  }
};

VELOX_DECLARE_VECTOR_FUNCTION_WITH_METADATA(
    udf_makeRowFromMap,
    MakeRowFromMapFunction::signatures(),
    exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
    std::make_unique<MakeRowFromMapFunction>());

void registerRowUdfs() {
  VELOX_REGISTER_VECTOR_FUNCTION(udf_makeRowFromMap, "makeRowFromMap");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_makeRowFromMap, "padded_makeRowFromMap");
}

} // namespace facebook::velox::optimizer::test
