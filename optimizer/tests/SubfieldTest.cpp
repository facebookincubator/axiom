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

#include "optimizer/FunctionRegistry.h" //@manual
#include "optimizer/tests/FeatureGen.h" //@manual
#include "optimizer/tests/QueryTestBase.h" //@manual
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/parse/Expressions.h"

using namespace facebook::velox;
using namespace facebook::velox::optimizer;
using namespace facebook::velox::optimizer::test;
using namespace facebook::velox::exec::test;

TypePtr makeGenieType() {
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
                "row(uid bigint, ff map(integer, real), idlf map(integer, array(bigint)), idslf map(integer, map(bigint, real)))")
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

class SubfieldTest : public QueryTestBase {
 protected:
  void SetUp() override {
    QueryTestBase::SetUp();
    core::Expressions::setFieldAccessHook(fieldIndexHook);
  }

  void TearDown() override {
    QueryTestBase::TearDown();
    core::Expressions::setFieldAccessHook(nullptr);
  }

  // Converts names like __[nn to DereferenceTypedExpr with index nn. Other
  // cases are unchanged.
  static core::TypedExprPtr fieldIndexHook(
      std::shared_ptr<const core::FieldAccessExpr> fae,
      std::vector<core::TypedExprPtr>& children) {
    auto name = fae->getFieldName();
    if (name.size() < 3 || name[0] != '_' || name[1] != '_') {
      return nullptr;
    }
    int32_t idx = -1;
    if (1 != sscanf(name.c_str() + 2, "%d", &idx)) {
      return nullptr;
    }
    VELOX_CHECK_EQ(children.size(), 1);
    VELOX_CHECK_GE(idx, 0);
    VELOX_CHECK_LT(idx, children[0]->type()->size());
    return std::make_shared<core::DereferenceTypedExpr>(
        children[0]->type()->as<TypeKind::ROW>().childAt(idx),
        children[0],
        idx);
  }

  void declareGenies() {
    TypePtr genieType = makeGenieType();
    std::vector<TypePtr> genieArgs = {
        genieType->childAt(0),
        genieType->childAt(1),
        genieType->childAt(2),
        genieType->childAt(3)};
    planner_->registerScalarFunction("genie", genieArgs, genieType);
    planner_->registerScalarFunction("exploding_genie", genieArgs, genieType);
    VELOX_REGISTER_VECTOR_FUNCTION(udf_genie, "genie");
    VELOX_REGISTER_VECTOR_FUNCTION(udf_genie, "exploding_genie");

    auto metadata = std::make_unique<FunctionMetadata>();
    metadata->fieldIndexForArg = {1, 2, 3};
    metadata->argOrdinal = {1, 2, 3};
    auto* instance = FunctionRegistry::instance();
    auto explodingMetadata = std::make_unique<FunctionMetadata>(*metadata);
    instance->registerFunction("genie", std::move(metadata));

    explodingMetadata->explode = explodeGenie;
    instance->registerFunction("exploding_genie", std::move(explodingMetadata));
  }

  static std::unordered_map<PathCP, core::TypedExprPtr> explodeGenie(
      const core::CallTypedExpr* call,
      std::vector<PathCP>& paths) {
    // This function understands paths like .__1[cc], .__2[cc],
    // .__3[cc] where __x is an ordinal field reference and cc is an integer
    // constant. If there is an empty path or a path with just one step, this
    // returns empty, meaning nothing is exploded. If the paths are longer, e.g.
    // idslf[11][1], then the trailing part is ignored. The returned map will
    // have the expression for each distinct path that begins with one of .__1,
    // .__2, .__3 followed by an integer subscript.
    std::unordered_map<PathCP, core::TypedExprPtr> result;
    for (auto& path : paths) {
      auto& steps = path->steps();
      if (steps.size() < 2) {
        return {};
      }

      std::vector<Step> prefixSteps = {steps[0], steps[1]};
      auto prefixPath = toPath(std::move(prefixSteps));
      if (result.count(prefixPath)) {
        // There already is an expression for this path.
        continue;
      }
      VELOX_CHECK(steps.front().kind == StepKind::kField);
      auto nth = steps.front().id;
      VELOX_CHECK_LE(nth, 3);
      auto args = call->inputs();

      // Here, for the sake of example, we make every odd key return identity.
      if (steps[1].id % 2 == 1) {
        result[prefixPath] = stepToGetter(steps[1], args[nth]);
        continue;
      }

      // For changed float_features, we add the feature id to the value.
      if (nth == 1) {
        result[prefixPath] = std::make_shared<core::CallTypedExpr>(
            REAL(),
            std::vector<core::TypedExprPtr>{
                stepToGetter(steps[1], args[nth]),
                std::make_shared<core::ConstantTypedExpr>(
                    REAL(), variant(static_cast<float>(steps[1].id)))},
            "plus");
        continue;
      }

      // For changed id list features, we do array_distinct on the list.
      if (nth == 2) {
        result[prefixPath] = std::make_shared<core::CallTypedExpr>(
            ARRAY(BIGINT()),
            std::vector<core::TypedExprPtr>{stepToGetter(steps[1], args[nth])},
            "array_distinct");
        continue;
      }

      // Access to idslf. Identity.
      result[prefixPath] = stepToGetter(steps[1], args[nth]);
    }
    return result;
  }
};

TEST_F(SubfieldTest, structs) {
  auto structType =
      ROW({"s1", "s2", "s3"},
          {BIGINT(), ROW({"s2s1"}, {BIGINT()}), ARRAY(BIGINT())});
  auto rowType = ROW({"s", "i"}, {structType, BIGINT()});
  auto vectors = makeVectors(rowType, 10, 10);
  auto fs = filesystems::getFileSystem(files_->getPath(), {});
  fs->mkdir(files_->getPath() + "/structs");
  auto filePath = files_->getPath() + "/structs/structs.dwrf";
  writeToFile(filePath, vectors);
  tablesCreated();

  auto builder = PlanBuilder()
                     .tableScan("structs", rowType)
                     .project({"s.s1 as a", "s.s3[0] as arr0"});

  auto plan = veloxString(planVelox(builder.planNode()));
  expectRegexp(plan, "s.*Subfields.*s.s3\\[0\\]");
  expectRegexp(plan, "s.*Subfields.*s.s1");
}

TEST_F(SubfieldTest, maps) {
  FeatureOptions opts;
  auto vectors = makeFeatures(5, 100, opts, pool_.get());
  auto rowType = std::dynamic_pointer_cast<const RowType>(vectors[0]->type());
  auto fs = filesystems::getFileSystem(files_->getPath(), {});
  fs->mkdir(files_->getPath() + "/features");
  auto filePath = files_->getPath() + "/features/features.dwrf";
  writeToFile(filePath, vectors);
  tablesCreated();
  std::vector<RowVectorPtr> results;
  std::string plan;
  auto builder =
      PlanBuilder()
          .tableScan("features", rowType)
          .project(
              {"float_features[10::INTEGER] as f1",
               "float_features[20::INTEGER] as f2",
               "id_score_list_features[10000::INTEGER][100000::INTEGER]"});
  plan = veloxString(planVelox(builder.planNode()));
  expectRegexp(plan, "float_features.*Subfields.*float_features\\[10\\]");
  expectRegexp(plan, "float_features.*Subfields.*float_features\\[20\\]");
  expectRegexp(
      plan,
      "id_score_list_features.*Subfields.* id_score_list_features\\[10000\\]\\[100000\\]");
  expectRegexp(plan, "id_list", false);

  builder = PlanBuilder()
                .tableScan("features", rowType)
                .project(
                    {"float_features[10000::INTEGER] as ff",
                     "id_score_list_features[10000::INTEGER] as sc1",
                     "id_list_features as idlf"})
                .project({"sc1[1::INTEGER] + 1::REAL as score"});
  plan = veloxString(planVelox(builder.planNode()));
  expectRegexp(
      plan,
      "id_score_list_features.*Subfields:.*\\[ id_score_list_features\\[10000\\]\\[1\\]");
  expectRegexp(plan, "id_list", false);
  expectRegexp(plan, "float_f", false);

  builder = PlanBuilder()
                .tableScan("features", rowType)
                .project(
                    {"float_features[10000::INTEGER] as ff",
                     "id_score_list_features[10000::INTEGER] as sc1",
                     "id_list_features as idlf",
                     "uid"})
                .project(
                    {"sc1[1::INTEGER] + 1::REAL as score",
                     "idlf[cast(uid % 100 as INTEGER)] as any"});
  plan = veloxString(planVelox(builder.planNode()));
  expectRegexp(
      plan, "id_list_features.*Subfields:.* id_list_features\\[\\*\\]");

  declareGenies();

  // Selected fields of genie are accessed. The uid and idslf args are not
  // accessed and should not be in the table scan.
  builder =
      PlanBuilder()
          .tableScan("features", rowType)
          .project(
              {"genie(uid, float_features, id_list_features, id_score_list_features) as g"})
          .project(
              {"g.__1[2::INTEGER] as f2",
               "g.__1[11::INTEGER] as f11",
               "g.__1[2::INTEGER] + 22::REAL  as f2b",
               "g.__2[100::INTEGER] as idl100"});

  plan = veloxString(planVelox(builder.planNode()));
  expectRegexp(plan, "float_features.*Subfield.*float_features\\[2\\]");
  expectRegexp(plan, "id_list_features.*Subfields.*id_list_features\\[100\\]");

  // All of genie is returned.
  builder =
      PlanBuilder()
          .tableScan("features", rowType)
          .project(
              {"genie(uid, float_features, id_list_features, id_score_list_features) as g"})
          .project(
              {"g",
               "g.__1[10::INTEGER] as f10",
               "g.__1[2::INTEGER] as f2",
               "g.__2[100::INTEGER] as idl100"});

  plan = veloxString(planVelox(builder.planNode()));
  std::cout << plan << std::endl;

  // We expect the genie to explode and the filters to be first.
  builder =
      PlanBuilder()
          .tableScan("features", rowType)
          .project(
              {"exploding_genie(uid, float_features, id_list_features, id_score_list_features) as g"})
          .project({"g.__1 as ff", "g as gg"})
          .project(
              {"ff[10::INTEGER] as f10",
               "ff[11::INTEGER] as f11",
               "ff[2::INTEGER] as f2",
               "gg.__1[2::INTEGER] + 22::REAL as f2b",
               "gg.__2[100::INTEGER] as idl100"})
          .filter("f10 < 10::REAL and f11 < 10::REAL");

  plan = veloxString(planVelox(builder.planNode()));
  std::cout << plan << std::endl;
}
