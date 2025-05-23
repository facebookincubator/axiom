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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/parse/Expressions.h"
#include "velox/vector/tests/utils/VectorMaker.h"

DEFINE_string(subfield_data_path, "", "Data directory for subfield test data");

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
                "row(bigint, map(integer, real), map(integer, array(bigint)), map(integer, map(bigint, real)))")
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

class SubfieldTest : public QueryTestBase,
                     public testing::WithParamInterface<int32_t> {
 protected:
  static void SetUpTestCase() {
    testDataPath_ = FLAGS_subfield_data_path;
    LocalRunnerTestBase::localFileFormat_ = "dwrf";
    LocalRunnerTestBase::SetUpTestCase();
  }

  static void TearDownTestCase() {
    LocalRunnerTestBase::TearDownTestCase();
  }

  void SetUp() override {
    QueryTestBase::SetUp();
    core::Expressions::setFieldAccessHook(fieldIndexHook);
    switch (GetParam()) {
      case 1:
        optimizerOptions_ = OptimizerOptions();
        break;
      case 2:
        optimizerOptions_ = OptimizerOptions{.pushdownSubfields = true};
        break;
      case 3:
        optimizerOptions_ = OptimizerOptions{.pushdownSubfields = true};
        optimizerOptions_.mapAsStruct["features"] = {
            "float_features", "id_list_features", "id_score_list_features"};
        break;
      default:
        FAIL();
        break;
    }
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

  std::vector<RowVectorPtr> extractAndIncrementIdList(
      const std::vector<RowVectorPtr>& vectors,
      int32_t key) {
    std::vector<RowVectorPtr> result;
    facebook::velox::test::VectorMaker vectorMaker(pool_.get());

    for (auto& row : vectors) {
      auto* idList = row->childAt(3)->as<MapVector>();
      auto* keys = idList->mapKeys()->as<FlatVector<int32_t>>();
      auto* values = idList->mapValues()->as<ArrayVector>();
      auto idsShared =
          BaseVector::create(values->type(), row->size(), values->pool());
      auto* ids = idsShared->as<ArrayVector>();
      for (auto i = 0; i < idList->size(); ++i) {
        bool found = false;
        for (auto k = idList->offsetAt(i);
             k < idList->offsetAt(i) + idList->sizeAt(i);
             ++k) {
          if (keys->valueAt(k) == key) {
            ids->copy(values, i, k, 1);
            auto* elt = ids->elements()->as<FlatVector<int64_t>>();
            for (auto e = ids->offsetAt(i);
                 e < ids->offsetAt(i) + ids->sizeAt(i);
                 ++e) {
              elt->set(e, elt->valueAt(e) + 1);
            }
            found = true;
            break;
          }
        }
        if (!found) {
          ids->setNull(i, true);
        }
      }
      result.push_back(vectorMaker.rowVector({idsShared}));
    }

    return result;
  }

  void testParallelExpr(FeatureOptions& opts, const RowTypePtr& rowType) {
    std::vector<std::string> names;
    std::vector<core::TypedExprPtr> exprs;

    // No randoms in test expr, different runs must come out the same.
    opts.randomPct = 0;
    makeExprs(opts, names, exprs);

    optimizerOptions_.parallelProjectWidth = 8;
    auto builder = PlanBuilder()
                       .tableScan("features", rowType)
                       .addNode([&](std::string id, auto node) {
                         return std::make_shared<core::ProjectNode>(
                             id, std::move(names), std::move(exprs), node);
                       });
    auto fragmentedPlan = planVelox(builder.planNode());
    auto plan = veloxString(fragmentedPlan);
    expectRegexp(plan, "ParallelProject");
    std::cout << plan;
    assertSame(builder.planNode(), fragmentedPlan);
  }
};

TEST_P(SubfieldTest, structs) {
  auto structType =
      ROW({"s1", "s2", "s3"},
          {BIGINT(), ROW({"s2s1"}, {BIGINT()}), ARRAY(BIGINT())});
  auto rowType = ROW({"s", "i"}, {structType, BIGINT()});
  auto vectors = makeVectors(rowType, 10, 10);
  auto fs = filesystems::getFileSystem(testDataPath_, {});
  fs->mkdir(testDataPath_ + "/structs");
  auto filePath = testDataPath_ + "/structs/structs.dwrf";
  writeToFile(filePath, vectors);
  tablesCreated();

  auto builder = PlanBuilder()
                     .tableScan("structs", rowType)
                     .project({"s.s1 as a", "s.s3[0] as arr0"});

  auto plan = veloxString(planVelox(builder.planNode()));
  expectRegexp(plan, "s.*Subfields.*s.s3\\[0\\]");
  expectRegexp(plan, "s.*Subfields.*s.s1");
}

TEST_P(SubfieldTest, maps) {
  FeatureOptions opts;
  opts.rng.seed(1);
  auto vectors = makeFeatures(1, 100, opts, pool_.get());
  auto rowType = std::dynamic_pointer_cast<const RowType>(vectors[0]->type());
  auto fs = filesystems::getFileSystem(testDataPath_, {});
  fs->mkdir(testDataPath_ + "/features");
  auto filePath = testDataPath_ + "/features/features.dwrf";
  auto config = std::make_shared<dwrf::Config>();
  config->set(dwrf::Config::FLATTEN_MAP, true);
  config->set<const std::vector<uint32_t>>(
      dwrf::Config::MAP_FLAT_COLS, {2, 3, 4});

  writeToFile(filePath, vectors, config);
  tablesCreated();

  std::string plan;
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto builder =
      PlanBuilder(planNodeIdGenerator)
          .tableScan("features", rowType)
          .project({"uid", "float_features as ff"})
          .hashJoin(
              {"uid"},
              {"opt_uid"},
              PlanBuilder(planNodeIdGenerator)
                  .tableScan("features", rowType)
                  .filter(
                      "uid % 2 = 1 and cast(float_features[10300::INTEGER] as integer) % 2 = 0")
                  .project({"uid as opt_uid", "float_features as opt_ff"})
                  .planNode(),
              "",
              {"ff", "uid", "opt_uid", "opt_ff"},
              core::JoinType::kLeft)
          .project(
              {"uid",
               "opt_uid",
               "ff[10100::INTEGER] as f10",
               "ff[10200::INTEGER] as f20",
               "opt_ff[10100::INTEGER] as o10",
               "opt_ff[10200::INTEGER] as o20"});

  plan = veloxString(planVelox(builder.planNode()));
  std::cout << plan << std::endl;

  builder =
      PlanBuilder()
          .tableScan("features", rowType)
          .project(
              {"float_features[10100::INTEGER] as f1",
               "float_features[10200::INTEGER] as f2",
               "id_score_list_features[200800::INTEGER][100000::INTEGER]"});
  plan = veloxString(planVelox(builder.planNode()));
  expectRegexp(plan, "float_features.*Subfields.*float_features.10100.");
  expectRegexp(plan, "float_features.*Subfields.*float_features.10200.");
  expectRegexp(
      plan,
      "id_score_list_features.*Subfields.* id_score_list_features.200800.*\\[100000\\]");
  expectRegexp(plan, "ubfield.*id_list", false);

  builder = PlanBuilder()
                .tableScan("features", rowType)
                .project(
                    {"float_features[10000::INTEGER] as ff",
                     "id_score_list_features[200800::INTEGER] as sc1",
                     "id_list_features as idlf"})
                .project({"sc1[1::INTEGER] + 1::REAL as score"});
  plan = veloxString(planVelox(builder.planNode()));
  expectRegexp(
      plan,
      "id_score_list_features.*Subfields:.*\\[ id_score_list_features.200800.*\\[1\\]");
  expectRegexp(plan, "ubfield.*id_list", false);
  expectRegexp(plan, "ubfield.*float_f", false);

  builder = PlanBuilder()
                .tableScan("features", rowType)
                .project(
                    {"float_features[10100::INTEGER] as ff",
                     "id_score_list_features[200800::INTEGER] as sc1",
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
              {"g.__1[10200::INTEGER] as f2",
               "g.__1[10100::INTEGER] as f11",
               "g.__1[10200::INTEGER] + 22::REAL  as f2b",
               "g.__2[201600::INTEGER] as idl100"});

  plan = veloxString(planVelox(builder.planNode()));
  expectRegexp(plan, "float_features.*Subfield.*float_features.10200");
  expectRegexp(plan, "id_list_features.*Subfields.*id_list_features.201600");

  // All of genie is returned.
  builder =
      PlanBuilder()
          .tableScan("features", rowType)
          .project(
              {"genie(uid, float_features, id_list_features, id_score_list_features) as g"})
          .project(
              {"g",
               "g.__1[10100::INTEGER] as f10",
               "g.__1[10200::INTEGER] as f2",
               "g.__2[200600::INTEGER] as idl100"});

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
              {"ff[10100::INTEGER] as f10",
               "ff[10100::INTEGER] as f11",
               "ff[10200::INTEGER] as f2",
               "gg.__1[10200::INTEGER] + 22::REAL as f2b",
               "gg.__2[200600::INTEGER] as idl100"})
          .filter("f10 < 10::REAL and f11 < 10::REAL");

  plan = veloxString(planVelox(builder.planNode()));
  std::cout << plan << std::endl;

  builder =
      PlanBuilder()
          .tableScan("features", rowType)
          .project(
              {"transform(id_list_features[201800::INTEGER], x -> x + 1) as ids"});

  auto result = runVelox(builder.planNode());
  auto expected = extractAndIncrementIdList(vectors, 201800);
  assertEqualResults(expected, result.results);

  testParallelExpr(opts, rowType);
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    SubfieldTests,
    SubfieldTest,
    testing::ValuesIn(std::vector<int32_t>{1, 2, 3}));
