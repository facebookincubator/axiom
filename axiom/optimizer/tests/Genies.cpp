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

void registerGenieUdfs() {
  VELOX_REGISTER_VECTOR_FUNCTION(udf_genie, "genie");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_genie, "exploding_genie");
}

} // namespace facebook::velox::optimizer::test
