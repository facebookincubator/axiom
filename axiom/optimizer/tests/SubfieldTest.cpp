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

#include <gmock/gmock.h>
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

    optimizerOptions_ = OptimizerOptions{};
    optimizerOptions_.traceFlags = FLAGS_optimizer_trace;

    switch (GetParam()) {
      case 1:
        optimizerOptions_.pushdownSubfields = false;
        break;
      case 2:
        optimizerOptions_.pushdownSubfields = true;
        break;
      case 3:
        optimizerOptions_.pushdownSubfields = true;
        optimizerOptions_.mapAsStruct["features"] = {
            "float_features", "id_list_features", "id_score_list_features"};
        break;
      case 4:
        // `pushdownSubfields` and `mapAsStruct` are v1-only options.
        useV2_ = true;
        break;
      default:
        FAIL();
    }
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

  void testDtSubfields() {
    declareGenies();

    // Query 1: select idl[1] from (select id_list_features[200200] as idl from
    // features limit 2);
    {
      auto logicalPlan = parseSelect(
          "select idl[1] from (select id_list_features[200200] as idl from features limit 2)",
          kHiveConnectorId);
      ASSERT_NO_THROW(runVelox(logicalPlan));
    }

    // Query 2: select g[3][200200] from (select genie(uid, ts, float_features,
    // id_list_features, id_score_list_features) g from features limit 2);
    {
      auto logicalPlan = parseSelect(
          "select g[3][200200] from (select genie(uid, ts, float_features, id_list_features, id_score_list_features) g from features limit 2)",
          kHiveConnectorId);
      ASSERT_NO_THROW(runVelox(logicalPlan));
    }

    // Query 3: select g[3][200200] from (select exploding_genie(uid, ts,
    // float_features, id_list_features, id_score_list_features) g from features
    // limit 2);
    {
      auto logicalPlan = parseSelect(
          "select g[3][200200] from (select exploding_genie(uid, ts, float_features, id_list_features, id_score_list_features) g from features limit 2)",
          kHiveConnectorId);
      ASSERT_NO_THROW(runVelox(logicalPlan));
    }
  }

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
                {"make_named_row('f1b', r2.f1 + 1::REAL, 'f2b', r2.f2 + 2::REAL) as named"})
            .project({"named as named1"})
            .project(
                {"make_named_row('f1b', named1.f1b, 'f2b', named1.f2b + 3::REAL) as named3"})
            .project({"named3 as named2"})
            .filter("named2.f1b < 10000::REAL")
            .project({"make_named_row('rf2', named2.f2b * 2::REAL) as fin"})
            .build();

    const auto plan = toSingleNodePlan(logicalPlan);

    auto scan = [this] {
      return matchHiveScanWithFilter(
          "features",
          {{"float_features", {subfield("10010"), subfield("10020")}}},
          "float_features[10010] + 1 < 10000");
    };

    auto matcher = scan().localPartition(scan().project()).project().build();

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

  using HiveQueriesTestBase::matchHiveScan;

  // Matches a scan of 'table' that reads exactly 'subfields' and carries
  // exactly 'remainingFilter' as its non-pushed filter.
  static core::PlanMatcherBuilder matchHiveScanWithFilter(
      const std::string& table,
      folly::F14FastMap<std::string, std::vector<std::string>> subfields,
      const std::string& remainingFilter) {
    return core::PlanMatcherBuilder().hiveScan(
        table,
        /*subfieldFilters=*/{},
        remainingFilter,
        /*sampleRate=*/std::nullopt,
        [subfields = std::move(subfields)](const core::PlanNodePtr& scan) {
          verifyRequiredSubfields(*scan, subfields);
        });
  }

  // Matches a scan of 'table' that reads exactly 'subfields' of each of its
  // columns. Use when a plan has more than one scan, so that each is checked
  // where the matcher names it.
  static core::PlanMatcherBuilder matchHiveScan(
      const std::string& table,
      folly::F14FastMap<std::string, std::vector<std::string>> subfields) {
    return core::PlanMatcherBuilder().tableScan(
        table,
        [subfields = std::move(subfields)](const core::PlanNodePtr& scan) {
          verifyRequiredSubfields(*scan, subfields);
        });
  }

  // Asserts the subfields each column of 'scanNode' is read with.
  static void verifyRequiredSubfields(
      const core::PlanNode& scanNode,
      const folly::F14FastMap<std::string, std::vector<std::string>>&
          expectedSubfields) {
    const auto& assignments = scanNode.as<core::TableScanNode>()->assignments();
    ASSERT_EQ(assignments.size(), expectedSubfields.size());

    for (const auto& [_, columnHandle] : assignments) {
      const auto* handle =
          columnHandle->as<velox::connector::hive::HiveColumnHandle>();
      ASSERT_TRUE(handle != nullptr);

      const auto& name = handle->name();
      const auto it = expectedSubfields.find(name);
      ASSERT_TRUE(it != expectedSubfields.end())
          << "Unexpected column: " << name;

      // Required subfields are a set: the order a column handle lists them in
      // is not part of the contract.
      std::vector<std::string> actualNames;
      for (const auto& subfield : handle->requiredSubfields()) {
        actualNames.push_back(subfield.toString());
      }
      std::vector<std::string> expectedNames;
      for (const auto& suffix : it->second) {
        expectedNames.push_back(fmt::format("{}{}", name, suffix));
      }
      EXPECT_THAT(
          actualNames, testing::UnorderedElementsAreArray(expectedNames))
          << handle->toString();
    }
  }

  // Creates table 't' with one array column, long enough for the paths these
  // tests name.
  void createArrayTable() {
    createTable(
        "t",
        {makeRowVector(
            {"a"},
            {makeArrayVectorFromJson<int64_t>({"[1, 2, 3]", "[4, 5, 6]"})})});
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
  auto rowType = ROW({
      {"s",
       ROW({
           {"s1", BIGINT()},
           {"s2", ROW({{"s2s1", BIGINT()}})},
           {"s3", ARRAY(BIGINT())},
       })},
      {"i", BIGINT()},
  });
  auto vectors = makeVectors(rowType, 1, 1);
  createTable("structs", vectors);

  {
    // Dereference struct fields by name.
    auto logicalPlan =
        parseSelect("SELECT s.s1, s.s3[1] FROM structs WHERE s.s1 < 10");
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchHiveScan("structs", {{"s", {".s1", ".s3[1]"}}})
            .projectIf(optimizerOptions_.pushdownSubfields)
            .project()
            .build());

    auto referencePlan = PlanBuilder()
                             .tableScan("structs", rowType)
                             .filter("s.s1 < 10")
                             .project({"s.s1", "s.s3[1]"})
                             .planNode();
    auto fragmentedPlan = planVelox(logicalPlan);
    checkSame(fragmentedPlan, referencePlan);
  }

  {
    // Dereference struct fields by indices.
    auto logicalPlan = lp::PlanBuilder(makeContext(), /*enableCoercions=*/true)
                           .tableScan("structs", rowType->names())
                           .project({"s[1] as s1", "s[2].s2s1", "s[3][1]"})
                           .filter("s1 < 10")
                           .build();
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchHiveScan("structs", {{"s", {".s1", ".s2.s2s1", ".s3[1]"}}})
            .projectIf(optimizerOptions_.pushdownSubfields)
            .project()
            .build());

    auto referencePlan = PlanBuilder()
                             .tableScan("structs", rowType)
                             .filter("s.s1 < 10")
                             .project({"s.s1", "(s.s2).s2s1", "s.s3[1]"})
                             .planNode();
    auto fragmentedPlan = planVelox(logicalPlan);
    checkSame(fragmentedPlan, referencePlan);
  }
}

TEST_P(SubfieldTest, anonymousStructs) {
  if (useV2_) {
    GTEST_SKIP() << "v2 does not rewrite a field read of a constructed row "
                    "into the argument that supplies it";
  }

  auto rowType = ROW({"a", "b", "c"}, {BIGINT(), BIGINT(), ARRAY(BIGINT())});
  auto vectors = makeVectors(rowType, 1, 1);
  createTable("anon_structs", vectors);

  {
    // Dereference anonymous struct field in projection.
    auto logicalPlan = parseSelect(
        "SELECT row(a, b, c).field0, row(a, b, c)[3][1] FROM anon_structs",
        kHiveConnectorId);
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchHiveScan("anon_structs", {{"a", {}}, {"c", {"[1]"}}})
            .project()
            .build());

    auto referencePlan = PlanBuilder()
                             .tableScan("anon_structs", rowType)
                             .project({"a", "c[1]"})
                             .planNode();
    auto fragmentedPlan = planVelox(logicalPlan);
    checkSame(fragmentedPlan, referencePlan);
  }

  {
    // Dereference anonymous struct field in filter.
    auto logicalPlan = parseSelect(
        "SELECT row(a, b, c).field2[1] FROM anon_structs WHERE row(a, b, c).field0 > 0 ",
        kHiveConnectorId);
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchHiveScan("anon_structs", {{"c", {"[1]"}}}).project().build());

    auto referencePlan = PlanBuilder()
                             .tableScan("anon_structs", rowType)
                             .filter("a > 0")
                             .project({"c[1]"})
                             .planNode();
    auto fragmentedPlan = planVelox(logicalPlan);
    checkSame(fragmentedPlan, referencePlan);
  }
}

TEST_P(SubfieldTest, anonymousStructTableScan) {
  if (useV2_) {
    GTEST_SKIP() << "v2 reads an unnamed struct field whole";
  }

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
  if (useV2_) {
    GTEST_SKIP() << "v2 does not explode a function result into subfields";
  }

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

    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchHiveScan(
            "features",
            {
                {"uid", {}},
                {"float_features", {subfield("10200"), subfield("10100")}},
                {"id_list_features", {subfield("201600")}},
            })
            .project()
            .build());
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

    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchHiveScan(
            "features",
            {
                {"uid", {}},
                {"float_features", {}},
                {"id_list_features", {}},
                {"id_score_list_features", {}},
            })
            .project()
            .build());
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

    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchHiveScan(
            "features",
            {
                {"float_features", {subfield("10100"), subfield("10200")}},
                {"id_list_features", {subfield("200600")}},
            })
            .projectIf(optimizerOptions_.pushdownSubfields)
            .project()
            .build());
  }
}

TEST_P(SubfieldTest, maps) {
  auto vectors = createFeaturesTable();

  if (!useV2_) {
    // make_row_from_map has no Velox implementation; only v1 removes it
    // before execution.
    testMakeRowFromMap();
  }

  testDtSubfields();

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

    // Each side of the join narrows its own scan: the build side also reads
    // the key its filter takes.
    auto matcher =
        matchHiveScan(
            "features",
            {
                {"uid", {}},
                {"float_features", {subfield("10100"), subfield("10200")}},
            })
            .projectIf(optimizerOptions_.pushdownSubfields)
            .hashJoinLeft(matchHiveScan(
                "features",
                {
                    {"uid", {}},
                    {"float_features",
                     {subfield("10100"), subfield("10200"), subfield("10300")}},
                }))
            .project()
            .build();

    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
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

    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchHiveScan(
            "features",
            {
                {"float_features", {subfield("10100"), subfield("10200")}},
                {"id_score_list_features", {subfield("200800", "[100000]")}},
            })
            .projectIf(optimizerOptions_.pushdownSubfields)
            .project()
            .build());
  }
  {
    auto logicalPlan = parseSelect(
        "SELECT sc1[1] + 1e0 AS score FROM ("
        "  SELECT float_features[10000] AS ff,"
        "         id_score_list_features[200800] AS sc1,"
        "         id_list_features AS idlf"
        "  FROM features"
        ")");

    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchHiveScan(
            "features",
            {
                {"id_score_list_features", {subfield("200800", "[1]")}},
            })
            .projectIf(optimizerOptions_.pushdownSubfields)
            .project()
            .build());
  }

  {
    auto logicalPlan = parseSelect(
        "SELECT sc1[1] + 1e0 AS score, idlf[CAST(uid % 100 AS INTEGER)] AS any "
        "FROM ("
        "  SELECT float_features[10100] AS ff,"
        "         id_score_list_features[200800] AS sc1,"
        "         id_list_features AS idlf,"
        "         uid"
        "  FROM features"
        ")");

    // A wildcard with nothing below it reads the whole map. v1 says so with
    // an explicit `[*]`, v2 by asking for no subfield at all.
    const std::vector<std::string> everyKey =
        useV2_ ? std::vector<std::string>{} : std::vector<std::string>{"[*]"};
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchHiveScan(
            "features",
            {
                {"uid", {}},
                {"id_score_list_features", {subfield("200800", "[1]")}},
                {"id_list_features", everyKey},
            })
            .projectIf(optimizerOptions_.pushdownSubfields)
            .project()
            .build());
  }

  {
    auto logicalPlan = parseSelect(
        "SELECT transform(id_list_features[201800], x -> x + 1) AS ids "
        "FROM features");

    auto result = runVelox(logicalPlan);
    auto expected = extractAndIncrementIdList(vectors, 201800);
    assertEqualResults(expected, result.results);
  }
}

TEST_P(SubfieldTest, cardinality) {
  createFeaturesTable();

  // cardinality(m) requires the whole map.
  {
    auto logicalPlan =
        parseSelect("SELECT cardinality(float_features) AS n FROM features");
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchHiveScan("features", {{"float_features", {}}}).project().build());
  }

  // cardinality(m) and m[k] on the same column: the whole map is required.
  {
    auto logicalPlan = parseSelect(
        "SELECT cardinality(float_features) AS n, float_features[10100] AS v "
        "FROM features");
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchHiveScan("features", {{"float_features", {}}}).project().build());
  }
}

TEST_P(SubfieldTest, parallelExpr) {
  if (useV2_) {
    GTEST_SKIP() << "v2 does not split an expression across parallel "
                    "projections";
  }

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
      fragmentedPlan.plan->fragments().at(0).fragment.planNode.get(),
      [](const core::PlanNode* node) {
        return node->is<core::ParallelProjectNode>();
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

  auto logicalPlan = parseSelect(
      "SELECT u, a[3] FROM t_unnest CROSS JOIN UNNEST(a[1]) AS t(u)");

  auto matcher = matchHiveScan("t_unnest", {{"a", {"[1]", "[3]"}}})
                     .project()
                     .unnest()
                     .project()
                     .build();

  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
}

TEST_P(SubfieldTest, aggregateMaskAndOrderKeys) {
  createTable(
      "t_agg",
      {makeRowVector(
          {"a", "b", "c"},
          {
              makeArrayVectorFromJson<int64_t>({"[1, 2]", "[1, 2, 3]"}),
              makeArrayVectorFromJson<int64_t>({"[10, 20]", "[10, 20, 30]"}),
              makeArrayVectorFromJson<int64_t>({"[5, 6]", "[5, 6, 7]"}),
          })});

  // A FILTER mask and an ORDER BY key narrow the scan to the same subfields an
  // argument does.
  auto logicalPlan = parseSelect(
      "SELECT array_agg(a[1] ORDER BY c[1]) FILTER (WHERE b[1] > 0) "
      "FROM t_agg");

  auto matcher =
      matchHiveScan("t_agg", {{"a", {"[1]"}}, {"b", {"[1]"}}, {"c", {"[1]"}}})
          .project()
          .aggregation()
          .build();

  AXIOM_ASSERT_PLAN_V2(toSingleNodePlan(logicalPlan), matcher);
}

TEST_P(SubfieldTest, recursiveCte) {
  if (!useV2_) {
    GTEST_SKIP() << "v1 does not implement recursive plans";
  }

  createArrayTable();

  auto matchAnchor = [](std::vector<std::string> subfields) {
    return core::PlanMatcherBuilder()
        .fixedPoint(
            core::FixedPointMatch("r").outputState(
                /*append=*/true,
                matchHiveScan("t", {{"a", std::move(subfields)}})))
        .project()
        .build();
  };

  {
    // The anchor passes the column through, so the state holds it whole and
    // the step's path has to reach the anchor's scan.
    auto logicalPlan = parseSelect(
        "WITH RECURSIVE r(a) AS ("
        "  SELECT a FROM t"
        "  UNION ALL"
        "  SELECT a FROM r WHERE a[2] > 0)"
        " SELECT a[1] FROM r");

    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan), matchAnchor({"[1]", "[2]"}));
  }

  {
    // Several paths in the step all reach the anchor's scan.
    auto logicalPlan = parseSelect(
        "WITH RECURSIVE r(a) AS ("
        "  SELECT a FROM t"
        "  UNION ALL"
        "  SELECT a FROM r WHERE a[2] > 0 AND a[3] > 0)"
        " SELECT a[1] FROM r");

    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan), matchAnchor({"[1]", "[2]", "[3]"}));
  }

  {
    // The anchor projects the subfield itself, so the state holds a scalar and
    // nothing the step does can widen the scan.
    auto logicalPlan = parseSelect(
        "WITH RECURSIVE r(x) AS ("
        "  SELECT a[1] FROM t"
        "  UNION ALL"
        "  SELECT x FROM r WHERE x > 0)"
        " SELECT x FROM r");

    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        core::PlanMatcherBuilder()
            .fixedPoint(
                core::FixedPointMatch("r").outputState(
                    /*append=*/true,
                    matchHiveScan("t", {{"a", {"[1]"}}}).project()))
            .project()
            .build());
  }
}

TEST_P(SubfieldTest, pushedFilter) {
  createArrayTable();

  // The predicate moves below the projection that produced it, so the scan
  // reads what the predicate takes as well as what the query returns.
  auto logicalPlan = parseSelect(
      "SELECT k FROM (SELECT a[1] AS v, a[2] AS k FROM t) WHERE v > 0");

  AXIOM_ASSERT_PLAN_V2(
      toSingleNodePlan(logicalPlan),
      matchHiveScan("t", {{"a", {"[1]", "[2]"}}}).project().build());
}

TEST_P(SubfieldTest, mapOfRow) {
  auto rowType = ROW({
      {"m", MAP(VARCHAR(), ROW({{"f1", BIGINT()}, {"f2", BIGINT()}}))},
  });
  createTable("t_map_of_row", makeVectors(rowType, 1, 1));

  // A constant map key followed by a field reads one key and one field.
  auto logicalPlan = parseSelect("SELECT m['x'].f1 FROM t_map_of_row");

  AXIOM_ASSERT_PLAN_V2(
      toSingleNodePlan(logicalPlan),
      matchHiveScan("t_map_of_row", {{"m", {"[\"x\"].f1"}}}).project().build());
}

TEST_P(SubfieldTest, negativeArrayIndex) {
  if (!useV2_) {
    GTEST_SKIP() << "v1 incorrectly pushes down `a[-1]`";
  }

  createArrayTable();

  // An index counted from the end of the array names no prefix, so the whole
  // array is read.
  {
    auto logicalPlan = parseSelect("SELECT element_at(a, -1) FROM t");
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchHiveScan("t", {{"a", {}}}).project().build());
  }

  {
    auto logicalPlan = parseSelect("SELECT element_at(a, 2) FROM t");
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(logicalPlan),
        matchHiveScan("t", {{"a", {"[2]"}}}).project().build());
  }
}

TEST_P(SubfieldTest, constructedRow) {
  createFeaturesTable();

  // A map read through a field of a row the query builds is narrowed to the
  // key, and the row's other arguments are unaffected.
  auto logicalPlan = parseSelect(
      "SELECT r.y[10100] FROM ("
      "  SELECT ROW(uid AS x, float_features AS y) AS r FROM features"
      ")");

  // TODO Rewrite a field read of a constructed row into the argument that
  // supplies it, so v2 also drops the arguments no field reads.
  folly::F14FastMap<std::string, std::vector<std::string>> expected{
      {"float_features", {subfield("10100")}}};
  if (useV2_) {
    expected.emplace("uid", std::vector<std::string>{});
  }

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(logicalPlan),
      matchHiveScan("features", std::move(expected)).project().build());
}

TEST_P(SubfieldTest, wholeColumnAndPath) {
  createArrayTable();

  // A column the query returns is read whole, however narrowly a predicate
  // reads it.
  auto logicalPlan = parseSelect("SELECT a FROM t WHERE a[1] > 0");

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(logicalPlan), matchHiveScan("t", {{"a", {}}}).build());
}

TEST_P(SubfieldTest, unionAll) {
  createArrayTable();

  // Reading one element of a union's output narrows the scan under each leg.
  auto logicalPlan = parseSelect(
      "SELECT a[1] FROM (SELECT a FROM t UNION ALL SELECT a FROM t)");

  auto matchLeg = [] { return matchHiveScan("t", {{"a", {"[1]"}}}); };
  auto matcher =
      matchLeg().localPartition({matchLeg().project()}).project().build();

  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
}

TEST_P(SubfieldTest, orderBy) {
  createArrayTable();

  auto logicalPlan = parseSelect("SELECT a[3] FROM t ORDER BY a[1]");

  auto matcher = matchHiveScan("t", {{"a", {"[1]", "[3]"}}})
                     .project()
                     .orderBy()
                     .project()
                     .build();

  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
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

  // This test verifies subfield pushdown, not join ordering. The decorrelated
  // subquery join has two cost-equivalent commutations, so use syntactic join
  // order to keep the plan shape stable across build environments.
  optimizerOptions_.syntacticJoinOrder = true;

  {
    auto logicalPlan = parseSelect(
        "SELECT a.y FROM t_subquery WHERE a.x = (SELECT 1)", kHiveConnectorId);

    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher = matchHiveScan("t_subquery", {{"a", {".x", ".y"}}})
                       .project()
                       .hashJoin(matchValues().project())
                       .project()
                       .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
  }

  {
    auto logicalPlan = parseSelect(
        "SELECT 1 FROM t_subquery "
        "WHERE a.x = (SELECT count(*) FROM (VALUES 1, 2, 3) as t(n) WHERE n = a.z)",
        kHiveConnectorId);

    auto plan = toSingleNodePlan(logicalPlan);

    auto matcher = matchHiveScan("t_subquery", {{"a", {".x", ".z"}}})
                       .project()
                       .hashJoin(matchValues().aggregation().projectIf(!useV2_))
                       .filter()
                       .project()
                       .build();
    AXIOM_ASSERT_PLAN(plan, matcher);
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

  auto logicalPlan = parseSelect(
      "SELECT a[2], c[1] FROM ("
      "  SELECT a, array_agg(b) AS c FROM t GROUP BY a"
      ")");

  // A grouping key and an aggregate's argument are both read in full, so
  // neither element read above narrows the scan.
  auto matcher = matchHiveScan("t", {{"a", {}}, {"b", {}}})
                     .aggregation()
                     .project()
                     .build();

  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
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

// A struct column crosses a DT boundary (the inner aggregation triggers
// finalizeDt). The outer query accesses only subfield .x but the inner
// DT outputs the full struct — subfield pruning does not propagate
// across the boundary.
TEST_P(SubfieldTest, subfieldAcrossDtBoundary) {
  testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));

  auto logicalPlan = parseSelect(
      "WITH s AS ("
      "  SELECT ROW(a AS x, b AS y) AS a, count(*) AS b"
      "  FROM t"
      "  GROUP BY 1"
      ") "
      "SELECT a.x, sum(b) FROM s GROUP BY 1",
      kTestConnectorId);

  // A grouping key is compared in full, so the whole struct crosses the
  // aggregation even though only one of its fields is read above.
  auto matcher = core::PlanMatcherBuilder()
                     .tableScan()
                     .project({"row_constructor(a, b) as r"})
                     .singleAggregation({"r"}, {"count(*) as cnt"})
                     .project()
                     .singleAggregation()
                     .build();
  AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
}

TEST_P(SubfieldTest, leftJoinWithUnmaterializedSubfield) {
  createFeaturesTable();

  // Reading one level deeper than a projected subfield narrows the scan to
  // the whole path.
  auto logicalPlan = parseSelect(
      "SELECT ff, nested[1] AS score "
      "FROM (SELECT uid, float_features[10100] AS ff FROM features) l "
      "LEFT JOIN "
      "(SELECT uid, id_score_list_features[200800] AS nested FROM features) r "
      "ON l.uid = r.uid");

  if (useV2_) {
    auto matcher =
        matchHiveScan(
            "features",
            {
                {"uid", {}},
                {"float_features", {subfield("10100")}},
            })
            .project()
            .hashJoinLeft(
                matchHiveScan(
                    "features",
                    {
                        {"uid", {}},
                        {"id_score_list_features", {subfield("200800", "[1]")}},
                    })
                    .project())
            .project()
            .build();

    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  } else {
    optimizerOptions_.pushdownSubfields = true;
    VELOX_ASSERT_THROW(
        toSingleNodePlan(logicalPlan),
        "Null expression for join column: nested");
  }
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    SubfieldTests,
    SubfieldTest,
    testing::ValuesIn(std::vector<int32_t>{1, 2, 3, 4}));

} // namespace
} // namespace facebook::axiom::optimizer
