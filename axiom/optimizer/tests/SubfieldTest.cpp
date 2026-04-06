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

#include "axiom/connectors/hive/HiveMetadataConfig.h"
#include "axiom/connectors/hive/LocalTableMetadata.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/FunctionRegistry.h"
#include "axiom/optimizer/tests/FeatureGen.h"
#include "axiom/optimizer/tests/Genies.h"
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/utils/DfFunctions.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/vector/tests/utils/VectorMaker.h"

DECLARE_uint32(optimizer_trace);

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;
using namespace facebook::velox::exec::test;
using namespace facebook::axiom::optimizer::test;
namespace lp = facebook::axiom::logical_plan;

template <typename T>
lp::ExprPtr makeKey(const TypePtr& type, T value) {
  return std::make_shared<lp::ConstantExpr>(
      type, std::make_shared<Variant>(value));
}

lp::ExprPtr stepToLogicalPlanGetter(Step step, const lp::ExprPtr& arg) {
  const auto& argType = arg->type();
  switch (step.kind) {
    case StepKind::kField: {
      lp::ExprPtr key;
      const TypePtr* type{};
      if (step.field) {
        key = makeKey(VARCHAR(), step.field);
        type = &argType->asRow().findChild(step.field);
      } else {
        key = makeKey(INTEGER(), static_cast<int32_t>(step.id));
        type = &argType->childAt(step.id);
      }

      return std::make_shared<lp::SpecialFormExpr>(
          *type, lp::SpecialForm::kDereference, arg, key);
    }

    case StepKind::kSubscript:
    case StepKind::kElementAt: {
      const auto* funcName =
          step.kind == StepKind::kElementAt ? "element_at" : "subscript";
      if (argType->kind() == TypeKind::ARRAY) {
        return std::make_shared<lp::CallExpr>(
            argType->childAt(0),
            funcName,
            arg,
            makeKey(INTEGER(), static_cast<int32_t>(step.id)));
      }

      lp::ExprPtr key;
      switch (argType->childAt(0)->kind()) {
        case TypeKind::VARCHAR:
          key = makeKey(VARCHAR(), step.field);
          break;
        case TypeKind::BIGINT:
          key = makeKey(BIGINT(), step.id);
          break;
        case TypeKind::INTEGER:
          key = makeKey(INTEGER(), static_cast<int32_t>(step.id));
          break;
        case TypeKind::SMALLINT:
          key = makeKey(SMALLINT(), static_cast<int16_t>(step.id));
          break;
        case TypeKind::TINYINT:
          key = makeKey(TINYINT(), static_cast<int8_t>(step.id));
          break;
        default:
          VELOX_FAIL("Unsupported key type");
      }

      return std::make_shared<lp::CallExpr>(
          argType->childAt(1), funcName, arg, key);
    }

    default:
      VELOX_NYI();
  }
}

class SubfieldTest : public HiveQueriesTestBase,
                     public testing::WithParamInterface<int32_t> {
 protected:
  static void SetUpTestCase() {
    HiveQueriesTestBase::SetUpTestCase();

    localFileFormat_ = velox::dwio::common::FileFormat::DWRF;
    registerDfFunctions();
  }

  void SetUp() override {
    HiveQueriesTestBase::SetUp();
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
    optimizerOptions_.traceFlags = FLAGS_optimizer_trace;
  }

  void declareGenies() {
    registerGenieUdfs();

    auto metadata = std::make_unique<FunctionMetadata>();
    metadata->fieldIndexForArg = {1, 2, 3};
    metadata->argOrdinal = {1, 2, 3};

    auto* registry = FunctionRegistry::instance();
    registry->registerFunction(
        "genie", std::make_unique<FunctionMetadata>(*metadata));

    auto explodingMetadata = std::make_unique<FunctionMetadata>(*metadata);
    explodingMetadata->explode = explodeGenie;
    registry->registerFunction("exploding_genie", std::move(explodingMetadata));
  }

  static folly::F14FastMap<PathCP, lp::ExprPtr> explodeGenie(
      const lp::CallExpr* call,
      std::vector<PathCP>& paths) {
    // This function understands paths like [1][cc], [2][cc],
    // .__3[cc] where __x is an ordinal field reference and cc is an integer
    // constant. If there is an empty path or a path with just one step, this
    // returns empty, meaning nothing is exploded. If the paths are longer, e.g.
    // idslf[11][1], then the trailing part is ignored. The returned map will
    // have the expression for each distinct path that begins with one of [1],
    // [2], [3] followed by an integer subscript.
    folly::F14FastMap<PathCP, lp::ExprPtr> result;
    for (auto& path : paths) {
      const auto& steps = path->steps();
      if (steps.size() < 2) {
        return {};
      }

      const auto* prefixPath = toPath({steps.data(), 2});
      auto [it, emplaced] = result.try_emplace(prefixPath);
      if (!emplaced) {
        // There already is an expression for this path.
        continue;
      }
      VELOX_CHECK(steps.front().kind == StepKind::kField);
      auto nth = steps.front().id;
      VELOX_CHECK_LE(nth, 3);
      auto args = call->inputs();

      // Here, for the sake of example, we make every odd key return identity.
      if (steps[1].id % 2 == 1) {
        it->second = stepToLogicalPlanGetter(steps[1], args[nth]);
        continue;
      }

      // For changed float_features, we add the feature id to the value.
      if (nth == 1) {
        it->second = std::make_shared<lp::CallExpr>(
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
        it->second = std::make_shared<lp::CallExpr>(
            ARRAY(BIGINT()),
            "array_distinct",
            std::vector<lp::ExprPtr>{
                stepToLogicalPlanGetter(steps[1], args[nth])});
        continue;
      }

      // Access to idslf. Identity.
      it->second = stepToLogicalPlanGetter(steps[1], args[nth]);
    }
    return result;
  }

  std::vector<RowVectorPtr> extractAndIncrementIdList(
      const std::vector<RowVectorPtr>& vectors,
      int32_t key) {
    std::vector<RowVectorPtr> result;
    velox::test::VectorMaker vectorMaker(pool());

    for (auto& row : vectors) {
      const auto* map = row->childAt("id_list_features")->as<MapVector>();
      const auto* keys = map->mapKeys()->as<FlatVector<int32_t>>();
      const auto* values = map->mapValues()->as<ArrayVector>();

      auto ids =
          BaseVector::create<ArrayVector>(ARRAY(BIGINT()), row->size(), pool());
      auto* elements = ids->elements()->as<FlatVector<int64_t>>();
      for (auto i = 0; i < row->size(); ++i) {
        bool found = false;
        const auto mapOffset = map->offsetAt(i);
        const auto mapSize = map->sizeAt(i);
        for (auto k = mapOffset; k < mapOffset + mapSize; ++k) {
          if (keys->valueAt(k) == key) {
            ids->copy(values, i, k, 1);

            const auto arrayOffset = ids->offsetAt(i);
            const auto arraySize = ids->sizeAt(i);
            for (auto e = arrayOffset; e < arrayOffset + arraySize; ++e) {
              elements->set(e, elements->valueAt(e) + 1);
            }
            found = true;
            break;
          }
        }
        if (!found) {
          ids->setNull(i, true);
        }
      }
      result.push_back(vectorMaker.rowVector({ids}));
    }

    return result;
  }

  std::string subfield(std::string_view first, std::string_view rest = "")
      const {
    return GetParam() == 3 ? fmt::format(".{}{}", first, rest)
                           : fmt::format("[{}]{}", first, rest);
  };

  void testMakeRowFromMap() {
    lp::PlanBuilder::Context ctx(
        exec::test::kHiveConnectorId,
        kDefaultSchema,
        getQueryCtx(),
        resolveDfFunction);
    auto logicalPlan =
        lp::PlanBuilder(ctx)
            .tableScan("features")
            .unionAll(lp::PlanBuilder(ctx).tableScan("features"))
            .project({"float_features as float_features_1"})
            .project({"float_features_1 as float_features_2"})
            .project(
                {"make_row_from_map(float_features_2, array[10010, 10020, 10030], array['f1', 'f2', 'f3']) as r"})
            .project({"r as r1"})
            .project({"r1 as r2"})
            .project(
                {"make_named_row('f1b', r2.f1 + 1::REAL, 'f2b', r2.f2 + 2::REAL + cast(rand() as real)) as named"})
            .project({"named as named1"})
            .project(
                {"make_named_row('f1b', named1.f1b, 'f2b', named1.f2b + 3::REAL) as named3"})
            .project({"named3 as named2"})
            .filter("named2.f1b < 10000::REAL")
            .project({"make_named_row('rf2', named2.f2b * 2::REAL) as fin"})
            .build();

    const auto plan = toSingleNodePlan(logicalPlan);

    verifyRequiredSubfields(
        plan, {{"float_features", {subfield("10010"), subfield("10020")}}});

    auto matcher =
        core::PlanMatcherBuilder()
            .hiveScan("features", {}, "float_features[10010] + 1 < 10000")
            .localPartition(
                core::PlanMatcherBuilder()
                    .hiveScan(
                        "features", {}, "float_features[10010] + 1 < 10000")
                    .project()
                    .build())
            .project()
            .build();

    ASSERT_TRUE(matcher->match(plan));
  }

  void createTable(
      const std::string& name,
      const std ::vector<RowVectorPtr>& vectors,
      const std::shared_ptr<dwrf::Config>& config =
          std::make_shared<dwrf::Config>()) {
    auto fs = filesystems::getFileSystem(localDataPath_, {});
    const auto tablePath = fmt::format("{}/{}", localDataPath_, name);
    fs->mkdir(tablePath);

    const auto filePath = fmt::format("{}/{}.dwrf", tablePath, name);
    writeToFile(filePath, vectors, config);

    // Write .schema and .stats metadata so that
    // LocalHiveConnectorMetadata::loadTable() can load the table.
    connector::hive::writeSchemaFile(
        tablePath, vectors[0]->rowType(), dwio::common::FileFormat::DWRF);

    uint64_t totalRows = 0;
    for (const auto& vector : vectors) {
      totalRows += vector->size();
    }
    connector::hive::PersistedStats::write(tablePath, {totalRows, {}});

    // Re-read the data directory to pick up the new table.
    hiveMetadata().reinitialize();
  }

  // TODO Move to PlanMatcher.
  static void verifyRequiredSubfields(
      const core::PlanNodePtr& plan,
      const folly::F14FastMap<std::string, std::vector<std::string>>&
          expectedSubfields) {
    auto* scanNode = core::PlanNode::findFirstNode(
        plan.get(), [](const core::PlanNode* node) {
          auto scan = dynamic_cast<const core::TableScanNode*>(node);
          return scan != nullptr;
        });

    ASSERT_TRUE(scanNode != nullptr);

    SCOPED_TRACE(scanNode->toString(true, true));

    const auto& assignments =
        dynamic_cast<const core::TableScanNode*>(scanNode)->assignments();
    ASSERT_EQ(assignments.size(), expectedSubfields.size());

    for (const auto& [_, handle] : assignments) {
      auto hiveHandle =
          dynamic_cast<const velox::connector::hive::HiveColumnHandle*>(
              handle.get());
      ASSERT_TRUE(hiveHandle != nullptr);

      const auto& name = hiveHandle->name();
      const auto& subfields = hiveHandle->requiredSubfields();

      auto it = expectedSubfields.find(name);
      ASSERT_TRUE(it != expectedSubfields.end())
          << "Unexpected column: " << name;
      const auto& expected = it->second;

      ASSERT_EQ(subfields.size(), expected.size()) << hiveHandle->toString();

      for (auto i = 0; i < subfields.size(); ++i) {
        EXPECT_EQ(
            subfields[i].toString(), fmt::format("{}{}", name, expected[i]));
      }
    }
  }

  static core::PlanNodePtr extractPlanNode(const PlanAndStats& plan) {
    return plan.plan->fragments().at(0).fragment.planNode;
  }

  std::vector<velox::RowVectorPtr> createFeaturesTable(FeatureOptions& opts) {
    opts.rng.seed(1);
    auto vectors = makeFeatures(1, 100, opts, pool_.get());

    const auto rowType = vectors[0]->rowType();

    auto config = std::make_shared<dwrf::Config>();
    config->set(dwrf::Config::FLATTEN_MAP, true);
    config->set<const std::vector<uint32_t>>(
        dwrf::Config::MAP_FLAT_COLS, {2, 3, 4});

    createTable("features", vectors, config);

    return vectors;
  }

  std::vector<velox::RowVectorPtr> createFeaturesTable() {
    FeatureOptions opts;
    return createFeaturesTable(opts);
  }

  lp::PlanBuilder::Context makeContext() const {
    return lp::PlanBuilder::Context{kHiveConnectorId, kDefaultSchema};
  }
};

TEST_P(SubfieldTest, structs) {
  auto structType =
      ROW({"s1", "s2", "s3"},
          {BIGINT(), ROW({"s2s1"}, {BIGINT()}), ARRAY(BIGINT())});
  auto rowType = ROW({"s", "i"}, {structType, BIGINT()});
  auto vectors = makeVectors(rowType, 1, 1);
  createTable("structs", vectors);

  {
    // Dereference struct fields by name.
    auto logicalPlan = lp::PlanBuilder(makeContext(), /*enableCoercions=*/true)
                           .tableScan("structs", rowType->names())
                           .project({"s.s1 as s1", "s.s3[1]"})
                           .filter("s1 < 10")
                           .build();
    auto fragmentedPlan = planVelox(logicalPlan);

    // t2.s = HiveColumnHandle [... requiredSubfields: [ s.s1 s.s3[1] ]]
    verifyRequiredSubfields(
        extractPlanNode(fragmentedPlan), {{"s", {".s1", ".s3[1]"}}});

    auto referencePlan = PlanBuilder()
                             .tableScan("structs", rowType)
                             .filter("s.s1 < 10")
                             .project({"s.s1", "s.s3[1]"})
                             .planNode();
    checkSame(fragmentedPlan, referencePlan);
  }

  {
    // Dereference struct fields by indices.
    auto logicalPlan = lp::PlanBuilder(makeContext(), /*enableCoercions=*/true)
                           .tableScan("structs", rowType->names())
                           .project({"s[1] as s1", "s[2].s2s1", "s[3][1]"})
                           .filter("s1 < 10")
                           .build();
    auto fragmentedPlan = planVelox(logicalPlan);

    verifyRequiredSubfields(
        extractPlanNode(fragmentedPlan),
        {{"s", {".s1", ".s2.s2s1", ".s3[1]"}}});

    auto referencePlan = PlanBuilder()
                             .tableScan("structs", rowType)
                             .filter("s.s1 < 10")
                             .project({"s.s1", "(s.s2).s2s1", "s.s3[1]"})
                             .planNode();
    checkSame(fragmentedPlan, referencePlan);
  }
}

TEST_P(SubfieldTest, anonymousStructs) {
  auto rowType = ROW({"a", "b", "c"}, {BIGINT(), BIGINT(), ARRAY(BIGINT())});
  auto vectors = makeVectors(rowType, 1, 1);
  createTable("anon_structs", vectors);

  {
    // Dereference anonymous struct field in projection.
    auto logicalPlan = parseSelect(
        "SELECT row(a, b, c).field0, row(a, b, c)[3][1] FROM anon_structs",
        kHiveConnectorId);
    auto fragmentedPlan = planVelox(logicalPlan);

    verifyRequiredSubfields(
        extractPlanNode(fragmentedPlan), {{"a", {}}, {"c", {"[1]"}}});

    auto referencePlan = PlanBuilder()
                             .tableScan("anon_structs", rowType)
                             .project({"a", "c[1]"})
                             .planNode();
    checkSame(fragmentedPlan, referencePlan);
  }

  {
    // Dereference anonymous struct field in filter.
    auto logicalPlan = parseSelect(
        "SELECT row(a, b, c).field2[1] FROM anon_structs WHERE row(a, b, c).field0 > 0 ",
        kHiveConnectorId);
    auto fragmentedPlan = planVelox(logicalPlan);

    verifyRequiredSubfields(extractPlanNode(fragmentedPlan), {{"c", {"[1]"}}});

    auto referencePlan = PlanBuilder()
                             .tableScan("anon_structs", rowType)
                             .filter("a > 0")
                             .project({"c[1]"})
                             .planNode();
    checkSame(fragmentedPlan, referencePlan);
  }
}

TEST_P(SubfieldTest, anonymousStructTableScan) {
  // Simulate filtering or projection on a table scan column with unnamed struct
  // fields and verify that Axiom throws the "Index subfield not suitable for
  // pruning" error as expected.
  auto innerType = ROW({"s1", "", ""}, {BIGINT(), BIGINT(), ARRAY(BIGINT())});
  auto rowType = ROW({"s"}, {innerType});
  auto vectors = makeVectors(rowType, 1, 1);
  createTable("anon_scan", vectors);

  {
    // Anonymous struct access in project.
    auto logicalPlan = lp::PlanBuilder(makeContext())
                           .tableScan("anon_scan", rowType->names())
                           .project({"s[1] as s1", "s[3][1]"})
                           .build();

    VELOX_ASSERT_THROW(
        planVelox(logicalPlan), "Index subfield not suitable for pruning");
  }

  {
    // Anonymous struct access in filter.
    auto logicalPlan = lp::PlanBuilder(makeContext(), /*enableCoercions=*/true)
                           .tableScan("anon_scan", rowType->names())
                           .project({"s[3][1] as arr_elem"})
                           .filter("arr_elem > 0")
                           .build();

    VELOX_ASSERT_THROW(
        planVelox(logicalPlan), "Index subfield not suitable for pruning");
  }
}

TEST_P(SubfieldTest, genie) {
  createFeaturesTable();

  declareGenies();

  // Selected fields of genie are accessed. The uid and idslf args are not
  // accessed and should not be in the table scan.
  {
    auto logicalPlan =
        lp::PlanBuilder(makeContext())
            .tableScan("features")
            .project(
                {"genie(uid, float_features, id_list_features, id_score_list_features) as g"})
            // Access some fields of the genie by name, others by index.
            .project(
                {"g.ff[10200::int] as f2",
                 "g[2][10100::int] as f11",
                 "g[2][10200::int] + 22::REAL  as f2b",
                 "g.idlf[201600::int] as idl100"})
            .build();

    auto plan = extractPlanNode(planVelox(logicalPlan));
    verifyRequiredSubfields(
        plan,
        {
            {"uid", {}},
            {"float_features", {subfield("10200"), subfield("10100")}},
            {"id_list_features", {subfield("201600")}},
        });
  }

  // All of genie is returned.
  {
    auto logicalPlan =
        lp::PlanBuilder(makeContext())
            .tableScan("features")
            .project(
                {"genie(uid, float_features, id_list_features, id_score_list_features) as gtemp"})
            .project({"gtemp as g"})
            .project(
                {"g",
                 "g[2][10100::int] as f10",
                 "g[2][10200::int] as f2",
                 "g[3][200600::int] as idl100",
                 "cardinality(g[3][200600::int]) as idl100card"})
            .build();

    auto plan = extractPlanNode(planVelox(logicalPlan));
    verifyRequiredSubfields(
        plan,
        {
            {"uid", {}},
            {"float_features", {}},
            {"id_list_features", {}},
            {"id_score_list_features", {}},
        });
  }

  // We expect the genie to explode and the filters to be first.
  {
    auto logicalPlan =
        lp::PlanBuilder(makeContext())
            .tableScan("features")
            .project(
                {"exploding_genie(uid, float_features, id_list_features, id_score_list_features) as g"})
            .project({"g[2] as ff", "g as gg"})
            .project(
                {"ff[10100::int] as f10",
                 "ff[10100::int] as f11",
                 "ff[10200::int] as f2",
                 "gg[2][10200::int] + 22::REAL as f2b",
                 "gg[3][200600::int] as idl100"})
            .filter("f10 < 10::REAL and f11 < 10::REAL")
            .build();

    auto plan = extractPlanNode(planVelox(logicalPlan));
    verifyRequiredSubfields(
        plan,
        {
            {"float_features", {subfield("10100"), subfield("10200")}},
            {"id_list_features", {subfield("200600")}},
        });
  }
}

TEST_P(SubfieldTest, maps) {
  auto vectors = createFeaturesTable();

  testMakeRowFromMap();

  {
    lp::PlanBuilder::Context ctx(kHiveConnectorId, kDefaultSchema);
    auto logicalPlan =
        lp::PlanBuilder(ctx)
            .tableScan("features")
            .project({"uid", "float_features as ff"})
            .join(
                lp::PlanBuilder(ctx)
                    .tableScan("features")
                    .filter(
                        "uid % 2 = 1 and cast(float_features[10300::int] as integer) % 2::int = 0::int")
                    .project({"uid as opt_uid", "float_features as opt_ff"}),
                "uid = opt_uid",
                lp::JoinType::kLeft)
            .project(
                {"uid",
                 "opt_uid",
                 "ff[10100::int] as f10",
                 "ff[10200::int] as f20",
                 "opt_ff[10100::int] as o10",
                 "opt_ff[10200::int] as o20"})
            .build();

    auto plan = extractPlanNode(planVelox(logicalPlan));
    // TODO Add verification.
  }
  {
    auto logicalPlan =
        lp::PlanBuilder(makeContext())
            .tableScan("features")
            .project(
                {"float_features[10100::int] as f1",
                 "float_features[10200::int] as f2",
                 "id_score_list_features[200800::int][100000::BIGINT]"})
            .build();

    auto plan = extractPlanNode(planVelox(logicalPlan));
    verifyRequiredSubfields(
        plan,
        {
            {"float_features", {subfield("10100"), subfield("10200")}},
            {"id_score_list_features", {subfield("200800", "[100000]")}},
        });
  }
  {
    auto logicalPlan = lp::PlanBuilder(makeContext())
                           .tableScan("features")
                           .project(
                               {"float_features[10000::int] as ff",
                                "id_score_list_features[200800::int] as sc1",
                                "id_list_features as idlf"})
                           .project({"sc1[1::BIGINT] + 1::REAL as score"})
                           .build();

    auto plan = extractPlanNode(planVelox(logicalPlan));
    verifyRequiredSubfields(
        plan,
        {
            {"id_score_list_features", {subfield("200800", "[1]")}},
        });
  }

  {
    auto logicalPlan = lp::PlanBuilder(makeContext())
                           .tableScan("features")
                           .project(
                               {"float_features[10100::int] as ff",
                                "id_score_list_features[200800::int] as sc1",
                                "id_list_features as idlf",
                                "uid"})
                           .project(
                               {"sc1[1::BIGINT] + 1::REAL as score",
                                "idlf[cast(uid % 100 as INTEGER)] as any"})
                           .build();

    auto plan = extractPlanNode(planVelox(logicalPlan));
    verifyRequiredSubfields(
        plan,
        {
            {"uid", {}},
            {"id_score_list_features", {subfield("200800", "[1]")}},
            {"id_list_features", {"[*]"}},
        });
  }

  {
    auto builder =
        lp::PlanBuilder(makeContext())
            .tableScan("features")
            .project(
                {"transform(id_list_features[201800::int], x -> x + 1) as ids"});

    auto result = runVelox(builder.build());
    auto expected = extractAndIncrementIdList(vectors, 201800);
    assertEqualResults(expected, result.results);
  }
}

TEST_P(SubfieldTest, parallelExpr) {
  FeatureOptions opts;
  const auto vectors = createFeaturesTable(opts);
  const auto rowType = vectors[0]->rowType();

  // No randoms in test expr, different runs must come out the same.
  opts.randomPct = 0;

  core::PlanNodePtr referencePlan;
  {
    std::vector<std::string> names;
    std::vector<core::TypedExprPtr> exprs;

    opts.rng.seed(1);
    makeExprs(opts, names, exprs);

    referencePlan = PlanBuilder()
                        .tableScan("features", rowType)
                        .addNode([&](std::string id, auto node) {
                          return std::make_shared<core::ProjectNode>(
                              id, std::move(names), std::move(exprs), node);
                        })
                        .planNode();
  }

  std::vector<std::string> names;
  std::vector<lp::ExprPtr> exprs;

  opts.rng.seed(1);
  makeLogicalExprs(opts, names, exprs);

  auto ctx = makeContext();
  auto logicalPlan = std::make_shared<lp::ProjectNode>(
      ctx.planNodeIdGenerator->next(),
      lp::PlanBuilder(ctx).tableScan("features", rowType->names()).build(),
      std::move(names),
      std::move(exprs));

  optimizerOptions_.parallelProjectWidth = 8;
  auto fragmentedPlan = planVelox(logicalPlan);

  auto* parallelProject = core::PlanNode::findFirstNode(
      extractPlanNode(fragmentedPlan).get(), [](const core::PlanNode* node) {
        return dynamic_cast<const core::ParallelProjectNode*>(node) != nullptr;
      });

  ASSERT_TRUE(parallelProject != nullptr);

  checkSame(fragmentedPlan, referencePlan);
}

TEST_P(SubfieldTest, unnest) {
  createTable(
      "t_unnest",
      {makeRowVector(
          {"a"},
          {makeNestedArrayVectorFromJson<int64_t>(
              {"[[1, 2], [3, 4]]", "[]"})})});

  auto logicalPlan = lp::PlanBuilder(makeContext())
                         .tableScan("t_unnest")
                         .unnest({"a[1]"})
                         .map({"a[3]"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan()
                     .project()
                     .unnest()
                     .project()
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  verifyRequiredSubfields(plan, {{"a", {"[1]", "[3]"}}});
}

TEST_P(SubfieldTest, orderBy) {
  createTable(
      "t_orderby",
      {makeRowVector(
          {"a"}, {makeArrayVectorFromJson<int64_t>({"[1, 2]", "[1, 2, 3]"})})});

  auto logicalPlan = lp::PlanBuilder(makeContext())
                         .tableScan("t_orderby")
                         .orderBy({"a[1]"})
                         .map({"a[3]"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan()
                     .project()
                     .orderBy()
                     .project()
                     .build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  verifyRequiredSubfields(plan, {{"a", {"[1]", "[3]"}}});
}

TEST_P(SubfieldTest, subquery) {
  createTable(
      "t_subquery",
      {
          makeRowVector(
              {"a"},
              {makeRowVector(
                  {"x", "y", "z"},
                  {
                      makeFlatVector<int32_t>({1, 2, 3}),
                      makeFlatVector<int32_t>({10, 20, 30}),
                      makeFlatVector<int32_t>({11, 22, 33}),
                  })}),
      });

  {
    auto logicalPlan = parseSelect(
        "SELECT a.y FROM t_subquery WHERE a.x = (SELECT 1)", kHiveConnectorId);

    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan("t_subquery")
                       .project()
                       .hashJoin(matchValues().project().build())
                       .project()
                       .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
    verifyRequiredSubfields(plan, {{"a", {".x", ".y"}}});
  }

  {
    auto logicalPlan = parseSelect(
        "SELECT 1 FROM t_subquery WHERE a.x = (SELECT count(*) FROM (VALUES 1, 2, 3) as t(n) WHERE n = a.z)",
        kHiveConnectorId);

    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher = core::PlanMatcherBuilder()
                       .tableScan()
                       .project()
                       .hashJoin(matchValues().aggregation().project().build())
                       .filter()
                       .project()
                       .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
    verifyRequiredSubfields(plan, {{"a", {".x", ".z"}}});
  }
}

TEST_P(SubfieldTest, overAggregation) {
  createTable(
      "t",
      {makeRowVector(
          {"a", "b"},
          {
              makeArrayVectorFromJson<int64_t>({"[1, 2]", "[1, 2, 3]"}),
              makeArrayVectorFromJson<int64_t>({"[10, 20]", "[10, 20, 30]"}),
          })});

  auto logicalPlan = lp::PlanBuilder(makeContext())
                         .tableScan("t")
                         .aggregate({"a"}, {"array_agg(b) as c"})
                         .map({"a[2]", "c[1]"})
                         .build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher =
      core::PlanMatcherBuilder().tableScan().aggregation().project().build();
  AXIOM_ASSERT_PLAN(plan, matcher);

  verifyRequiredSubfields(plan, {{"a", {}}, {"b", {}}});
}

TEST_P(SubfieldTest, blackbox) {
  auto data = makeRowVector(
      {"id", "m"},
      {
          makeFlatVector<int64_t>({1, 2}),
          makeMapVectorFromJson<int32_t, float>(
              {"{1: 0.1, 2: 0.2}", "{3: 0.3, 4: 0.4}"}),
      });

  createTable("t", {data});

  lp::PlanBuilder::Context ctx(
      kHiveConnectorId, kDefaultSchema, getQueryCtx(), resolveDfFunction);

  auto logicalPlan =
      lp::PlanBuilder(ctx)
          .tableScan("t")
          .project(
              {"make_row_from_map(m, array[1, 2, 3], array['f1', 'f2', 'f3']) as m"})
          .project({"make_named_row('x', m.f1, 'y', m.f2) as m"})
          .project({"m.x", "m.y"})
          .build();

  ASSERT_NO_THROW(toSingleNodePlan(logicalPlan));
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    SubfieldTests,
    SubfieldTest,
    testing::ValuesIn(std::vector<int32_t>{1, 2, 3}));

} // namespace
} // namespace facebook::axiom::optimizer
