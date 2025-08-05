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

#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/tests/FeatureGen.h"
#include "axiom/optimizer/tests/Genies.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "axiom/optimizer/tests/utils/DfFunctions.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/parse/Expressions.h"
#include "velox/vector/tests/utils/VectorMaker.h"

DEFINE_string(subfield_data_path, "", "Data directory for subfield test data");

using namespace facebook::velox;
using namespace facebook::velox::optimizer;
using namespace facebook::velox::optimizer::test;
using namespace facebook::velox::exec::test;
namespace lp = facebook::velox::logical_plan;

class LogicalSubfieldTest : public QueryTestBase,
                            public testing::WithParamInterface<int32_t> {
 protected:
  static void SetUpTestCase() {
    testDataPath_ = FLAGS_subfield_data_path;
    LocalRunnerTestBase::localFileFormat_ = "dwrf";
    LocalRunnerTestBase::SetUpTestCase();
    registerDfFunctions();
    registerRowUdfs();
  }

  static void TearDownTestCase() {
    LocalRunnerTestBase::TearDownTestCase();
  }

  void SetUp() override {
    QueryTestBase::SetUp();
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
  }

  void declareGenies() {
    TypePtr genieType = makeGenieType();
    std::vector<TypePtr> genieArgs = {
        genieType->childAt(0),
        genieType->childAt(1),
        genieType->childAt(2),
        genieType->childAt(3)};
    planner().registerScalarFunction("genie", genieArgs, genieType);
    planner().registerScalarFunction("exploding_genie", genieArgs, genieType);
    registerGenieUdfs();

    auto metadata = std::make_unique<FunctionMetadata>();
    metadata->fieldIndexForArg = {1, 2, 3};
    metadata->argOrdinal = {1, 2, 3};
    auto* instance = FunctionRegistry::instance();
    auto explodingMetadata = std::make_unique<FunctionMetadata>(*metadata);
    instance->registerFunction("genie", std::move(metadata));

    explodingMetadata->logicalExplode = logicalExplodeGenie;
    instance->registerFunction("exploding_genie", std::move(explodingMetadata));
  }

  static std::unordered_map<PathCP, lp::ExprPtr> logicalExplodeGenie(
      const lp::CallExpr* call,
      std::vector<PathCP>& paths) {
    // This function understands paths like [1][cc], [2][cc],
    // .__3[cc] where __x is an ordinal field reference and cc is an integer
    // constant. If there is an empty path or a path with just one step, this
    // returns empty, meaning nothing is exploded. If the paths are longer, e.g.
    // idslf[11][1], then the trailing part is ignored. The returned map will
    // have the expression for each distinct path that begins with one of [1],
    // [2], [3] followed by an integer subscript.
    std::unordered_map<PathCP, lp::ExprPtr> result;
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
        result[prefixPath] = stepToLogicalPlanGetter(steps[1], args[nth]);
        continue;
      }

      // For changed float_features, we add the feature id to the value.
      if (nth == 1) {
        result[prefixPath] = std::make_shared<lp::CallExpr>(
            REAL(),
            "plus",
            std::vector<lp::ExprPtr>{
                stepToLogicalPlanGetter(steps[1], args[nth]),
                std::make_shared<lp::ConstantExpr>(
                    REAL(),
                    std::make_shared<variant>(
                        static_cast<float>(steps[1].id)))});
        continue;
      }

      // For changed id list features, we do array_distinct on the list.
      if (nth == 2) {
        result[prefixPath] = std::make_shared<lp::CallExpr>(
            ARRAY(BIGINT()),
            "array_distinct",
            std::vector<lp::ExprPtr>{
                stepToLogicalPlanGetter(steps[1], args[nth])});
        continue;
      }

      // Access to idslf. Identity.
      result[prefixPath] = stepToLogicalPlanGetter(steps[1], args[nth]);
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

  std::vector<std::string> fieldNames(const RowTypePtr& type) {
    std::vector<std::string> result;
    for (auto i = 0; i < type->size(); ++i) {
      result.push_back(type->nameOf(i));
    }
    return result;
  }

  void testParallelExpr(FeatureOptions& opts, const RowTypePtr& rowType) {
    core::PlanNodePtr veloxPlan;
    // No randoms in test expr, different runs must come out the same.
    opts.randomPct = 0;

    {
      std::vector<std::string> names;
      std::vector<core::TypedExprPtr> exprs;

      opts.rng.seed(1);
      makeExprs(opts, names, exprs);

      auto builder = PlanBuilder()
                         .tableScan("features", rowType)
                         .addNode([&](std::string id, auto node) {
                           return std::make_shared<core::ProjectNode>(
                               id, std::move(names), std::move(exprs), node);
                         });
      veloxPlan = builder.planNode();
    }

    std::vector<std::string> names;
    std::vector<lp::ExprPtr> exprs;

    opts.rng.seed(1);
    makeLogicalExprs(opts, names, exprs);
    lp::PlanBuilder::Context ctx;
    auto builder = lp::PlanBuilder(ctx).tableScan(
        kHiveConnectorId, "features", fieldNames(rowType));
    lp::LogicalPlanNodePtr logicalPlan = std::make_shared<lp::ProjectNode>(
        ctx.planNodeIdGenerator->next(),
        builder.build(),
        std::move(names),
        std::move(exprs));

    optimizerOptions_.parallelProjectWidth = 8;
    auto fragmentedPlan = planVelox(logicalPlan);
    auto plan = veloxString(fragmentedPlan.plan);

    expectRegexp(plan, "ParallelProject");
    std::cout << plan;
    assertSame(veloxPlan, fragmentedPlan);
  }

  void testMakeRowFromMap() {
    lp::PlanBuilder::Context ctx(getQueryCtx(), resolveDfFunction);
    auto logical =
        lp::PlanBuilder(ctx)
            .tableScan(
                exec::test::kHiveConnectorId,
                "features",
                {"float_features", "id_list_features"})
            .unionAll(lp::PlanBuilder(ctx).tableScan(
                exec::test::kHiveConnectorId,
                "features",
                {"float_features", "id_list_features"}))

            .project(
                {"make_row_from_map(float_features, array[10010, 10020, 10030], array['f1', 'f2', 'f3']) as r"})
            .project(
                {"make_named_row('f1b', r.f1 + 1::REAL, 'f2b', r.f2 + 2::REAL) as named"})
            .filter("named.f1b < 10000::REAL")
            .project({"make_named_row('rf2', named.f2b * 2::REAL) as fin"})
            .build();

    std::string planString;
    auto plan = planVelox(logical, &planString);
    expectPlan(
        planString,
        "(features t4 project 1 columns  union all features t6 project 1 columns ) project 1 columns ");
    auto exe = veloxString(plan.plan);
    // Filters should be pushed down to scan.
    expectRegexp(
        exe,
        "remaining filter: .lt.plus.subscript.*float_features.*10010..1..10000");
    expectRegexp(
        exe, "requiredSubfields.*float_features.10010.*float_features.10020");
    // 10030 is not referenced, expect not in plan.
    expectRegexp(exe, "10030", false);
  }
};

TEST_P(LogicalSubfieldTest, structs) {
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

  auto builder =
      lp::PlanBuilder()
          .tableScan(kHiveConnectorId, "structs", fieldNames(rowType))
          .project({"s.s1 as a", "s.s3[0] as arr0"});

  auto plan = veloxString(planVelox(builder.build()).plan);
  expectRegexp(plan, "s.*Subfields.*s.s3\\[0\\]");
  expectRegexp(plan, "s.*Subfields.*s.s1");
}

TEST_P(LogicalSubfieldTest, maps) {
  FeatureOptions opts;
  opts.rng.seed(1);
  auto vectors = makeFeatures(1, 100, opts, pool_.get());
  auto rowType = std::dynamic_pointer_cast<const RowType>(vectors[0]->type());
  auto fields = fieldNames(rowType);
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

  testMakeRowFromMap();
  {
    lp::PlanBuilder::Context ctx;
    auto builder =
        lp::PlanBuilder(ctx)
            .tableScan(kHiveConnectorId, "features", fields)
            .project({"uid", "float_features as ff"})
            .join(
                lp::PlanBuilder(ctx)
                    .tableScan(kHiveConnectorId, "features", fields)
                    .filter(
                        "uid % 2 = 1 and cast(float_features[10300::INTEGER] as integer) % 2::INTEGER = 0::INTEGER")
                    .project({"uid as opt_uid", "float_features as opt_ff"}),
                "uid = opt_uid",
                lp::JoinType::kLeft)
            .project(
                {"uid",
                 "opt_uid",
                 "ff[10100::INTEGER] as f10",
                 "ff[10200::INTEGER] as f20",
                 "opt_ff[10100::INTEGER] as o10",
                 "opt_ff[10200::INTEGER] as o20"});

    plan = veloxString(planVelox(builder.build()).plan);
    std::cout << plan << std::endl;
  }
  {
    auto builder =
        lp::PlanBuilder()
            .tableScan(kHiveConnectorId, "features", fields)
            .project(
                {"float_features[10100::INTEGER] as f1",
                 "float_features[10200::INTEGER] as f2",
                 "id_score_list_features[200800::INTEGER][100000::BIGINT]"});
    plan = veloxString(planVelox(builder.build()).plan);
    expectRegexp(plan, "float_features.*Subfields.*float_features.10100.");
    expectRegexp(plan, "float_features.*Subfields.*float_features.10200.");
    expectRegexp(
        plan,
        "id_score_list_features.*Subfields.* id_score_list_features.200800.*\\[100000\\]");
    expectRegexp(plan, "ubfield.*id_list", false);
  }
  {
    auto builder = lp::PlanBuilder()
                       .tableScan(kHiveConnectorId, "features", fields)
                       .project(
                           {"float_features[10000::INTEGER] as ff",
                            "id_score_list_features[200800::INTEGER] as sc1",
                            "id_list_features as idlf"})
                       .project({"sc1[1::BIGINT] + 1::REAL as score"});
    plan = veloxString(planVelox(builder.build()).plan);
    expectRegexp(
        plan,
        "id_score_list_features.*Subfields:.*\\[ id_score_list_features.200800.*\\[1\\]");
    expectRegexp(plan, "ubfield.*id_list", false);
    expectRegexp(plan, "ubfield.*float_f", false);
  }
  {
    auto builder = lp::PlanBuilder()
                       .tableScan(kHiveConnectorId, "features", fields)
                       .project(
                           {"float_features[10100::INTEGER] as ff",
                            "id_score_list_features[200800::INTEGER] as sc1",
                            "id_list_features as idlf",
                            "uid"})
                       .project(
                           {"sc1[1::BIGINT] + 1::REAL as score",
                            "idlf[cast(uid % 100 as INTEGER)] as any"});
    plan = veloxString(planVelox(builder.build()).plan);
    expectRegexp(
        plan, "id_list_features.*Subfields:.* id_list_features\\[\\*\\]");
  }
  declareGenies();

  // Selected fields of genie are accessed. The uid and idslf args are not
  // accessed and should not be in the table scan.
  {
    auto builder =
        lp::PlanBuilder()
            .tableScan(kHiveConnectorId, "features", fields)
            .project(
                {"genie(uid, float_features, id_list_features, id_score_list_features) as g"})
            // Access some fields of the genie by name, others by index.
            .project(
                {"g.ff[10200::INTEGER] as f2",
                 "g[2][10100::INTEGER] as f11",
                 "g[2][10200::INTEGER] + 22::REAL  as f2b",
                 "g.idlf[201600::INTEGER] as idl100"});

    plan = veloxString(planVelox(builder.build()).plan);
    expectRegexp(plan, "float_features.*Subfield.*float_features.10200");
    expectRegexp(plan, "id_list_features.*Subfields.*id_list_features.201600");
  }
  // All of genie is returned.
  {
    auto builder =
        lp::PlanBuilder()
            .tableScan(kHiveConnectorId, "features", fields)
            .project(
                {"genie(uid, float_features, id_list_features, id_score_list_features) as g"})
            .project(
                {"g",
                 "g[2][10100::INTEGER] as f10",
                 "g[2][10200::INTEGER] as f2",
                 "g[3][200600::INTEGER] as idl100"});

    plan = veloxString(planVelox(builder.build()).plan);
    std::cout << plan << std::endl;
  }

  // We expect the genie to explode and the filters to be first.
  {
    auto builder =
        lp::PlanBuilder()
            .tableScan(kHiveConnectorId, "features", fields)
            .project(
                {"exploding_genie(uid, float_features, id_list_features, id_score_list_features) as g"})
            .project({"g[2] as ff", "g as gg"})
            .project(
                {"ff[10100::INTEGER] as f10",
                 "ff[10100::INTEGER] as f11",
                 "ff[10200::INTEGER] as f2",
                 "gg[2][10200::INTEGER] + 22::REAL as f2b",
                 "gg[3][200600::INTEGER] as idl100"})
            .filter("f10 < 10::REAL and f11 < 10::REAL");

    plan = veloxString(planVelox(builder.build()).plan);
    std::cout << plan << std::endl;
  }
  {
    auto builder =
        lp::PlanBuilder()
            .tableScan(kHiveConnectorId, "features", fields)
            .project(
                {"transform(id_list_features[201800::INTEGER], x -> x + 1) as ids"});

    auto result = runVelox(builder.build());
    auto expected = extractAndIncrementIdList(vectors, 201800);
    assertEqualResults(expected, result.results);
  }

  testParallelExpr(opts, rowType);
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    LogicalSubfieldTests,
    LogicalSubfieldTest,
    testing::ValuesIn(std::vector<int32_t>{1, 2, 3}));
