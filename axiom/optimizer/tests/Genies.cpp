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

#include "axiom/optimizer/tests/Genies.h"
#include "velox/expression/VectorFunction.h"

namespace facebook::velox::optimizer::test {

RowTypePtr makeGenieType() {
  return ROW(
      {"uid", "ff", "idlf", "idslf"},
      {BIGINT(),
       MAP(INTEGER(), REAL()),
       MAP(INTEGER(), ARRAY(BIGINT())),
       MAP(INTEGER(), MAP(BIGINT(), REAL()))});
}

class GenieFunction : public exec::VectorFunction {
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
    auto type = makeGenieType();
    return {
        exec::FunctionSignatureBuilder()
            .returnType(
                "row(userid bigint, ff map(integer, real), idlf map(integer, array(bigint)), idsf map(integer, map(bigint, real)))")
            .argumentType("bigint")
            .argumentType("map(integer, real)")
            .argumentType("map(integer, array(bigint))")
            .argumentType("map(integer, map(bigint, real))")
            .build()};
  }
};

VELOX_DECLARE_VECTOR_FUNCTION_WITH_METADATA(
    udf_genie,
    GenieFunction::signatures(),
    exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
    std::make_unique<GenieFunction>());

class IdentityFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 1);

    auto& input = args[0];
    if (!result) {
      // If no result vector provided, just return the input vector directly
      result = input;
    } else {
      // If result vector is provided, copy input to result
      BaseVector::ensureWritable(rows, input->type(), context.pool(), result);
      result->copy(input.get(), rows, nullptr);
    }
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // T -> T (accepts any type and returns the same type)
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("T")
                .argumentType("T")
                .build()};
  }
};

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_identity,
    IdentityFunction::signatures(),
    std::make_unique<IdentityFunction>());

void registerGenieUdfs() {
  VELOX_REGISTER_VECTOR_FUNCTION(udf_genie, "genie");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_genie, "exploding_genie");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_identity, "identity");
}

} // namespace facebook::velox::optimizer::test
