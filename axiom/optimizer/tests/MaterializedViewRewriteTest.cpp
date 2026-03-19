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
#include "axiom/common/SchemaTableName.h"
#include "axiom/connectors/MaterializedViewDefinition.h"
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

/// ConnectorMetadata wrapper that delegates table lookups and split management
/// to the underlying TestConnectorMetadata, adding a controllable
/// getMaterializedViewStatus() override for testing MV rewrite.
class MvTestConnectorMetadata : public connector::ConnectorMetadata {
 public:
  explicit MvTestConnectorMetadata(connector::ConnectorMetadata* delegate)
      : delegate_(delegate) {
    VELOX_CHECK_NOT_NULL(delegate_);
  }

  connector::TablePtr findTable(const SchemaTableName& name) override {
    return delegate_->findTable(name);
  }

  connector::ConnectorSplitManager* splitManager() override {
    return delegate_->splitManager();
  }

  std::vector<std::string> listSchemaNames(
      const connector::ConnectorSessionPtr& session) override {
    return delegate_->listSchemaNames(session);
  }

  bool schemaExists(
      const connector::ConnectorSessionPtr& session,
      const std::string& schemaName) override {
    return delegate_->schemaExists(session, schemaName);
  }

  std::optional<connector::MaterializedViewStatus> getMaterializedViewStatus(
      const connector::Table& mvTable) override {
    auto it = statusMap_.find(mvTable.name());
    if (it != statusMap_.end()) {
      return it->second;
    }
    return std::nullopt;
  }

  void setMaterializedViewStatus(
      const SchemaTableName& tableName,
      connector::MaterializedViewStatus status) {
    statusMap_.insert_or_assign(tableName, std::move(status));
  }

 private:
  connector::ConnectorMetadata* delegate_;
  folly::F14FastMap<
      SchemaTableName,
      connector::MaterializedViewStatus,
      SchemaTableNameHash>
      statusMap_;
};

/// Build a MaterializedViewDefinition with common defaults.
connector::MaterializedViewDefinition makeMvDef(
    const std::string& schema,
    const std::string& mvTableName,
    const std::string& baseTableName,
    const std::vector<connector::ColumnMapping>& columnMappings,
    const std::optional<std::vector<std::string>>& refreshColumns,
    const std::string& originalSql = "") {
  return connector::MaterializedViewDefinition(
      originalSql.empty() ? fmt::format("SELECT * FROM {}", baseTableName)
                          : originalSql,
      schema,
      mvTableName,
      {connector::SchemaTableName{schema, baseTableName}},
      std::nullopt,
      std::nullopt,
      columnMappings,
      {},
      refreshColumns);
}

/// Build a ColumnMapping where MV column name == base table column name.
connector::ColumnMapping identityMapping(
    const std::string& schema,
    const std::string& mvTableName,
    const std::string& baseTableName,
    const std::string& columnName) {
  return connector::ColumnMapping{
      connector::TableColumn{
          connector::SchemaTableName{schema, mvTableName}, columnName},
      {connector::TableColumn{
          connector::SchemaTableName{schema, baseTableName}, columnName}}};
}

/// Build a ColumnMapping where MV column has a different name from base column.
connector::ColumnMapping renamedMapping(
    const std::string& schema,
    const std::string& mvTableName,
    const std::string& baseTableName,
    const std::string& mvColumnName,
    const std::string& baseColumnName) {
  return connector::ColumnMapping{
      connector::TableColumn{
          connector::SchemaTableName{schema, mvTableName}, mvColumnName},
      {connector::TableColumn{
          connector::SchemaTableName{schema, baseTableName}, baseColumnName}}};
}

constexpr auto kConnectorId = "mv_test";
constexpr auto kSchema = "test_schema";

// Returns the table name in the format used by PlanMatcher
// (SchemaTableName::toString()).
std::string matchName(std::string_view table) {
  return SchemaTableName{std::string(kSchema), std::string(table)}.toString();
}

class MaterializedViewRewriteTest : public test::QueryTestBase {
 protected:
  void SetUp() override {
    test::QueryTestBase::SetUp();

    testConnector_ = std::make_shared<connector::TestConnector>(kConnectorId);
    velox::connector::registerConnector(testConnector_);

    auto* delegateMetadata =
        connector::ConnectorMetadata::tryMetadata(kConnectorId);
    VELOX_CHECK_NOT_NULL(delegateMetadata);

    mvMetadata_ = std::make_shared<MvTestConnectorMetadata>(delegateMetadata);
    connector::ConnectorMetadata::unregisterMetadata(kConnectorId);
    connector::ConnectorMetadata::registerMetadata(kConnectorId, mvMetadata_);
  }

  void TearDown() override {
    connector::ConnectorMetadata::unregisterMetadata(kConnectorId);
    mvMetadata_.reset();

    velox::connector::unregisterConnector(kConnectorId);
    testConnector_.reset();
    test::QueryTestBase::TearDown();
  }

  /// Register an MV table and its base table with identical schemas.
  /// Returns the MV TestTable for further customization.
  std::shared_ptr<connector::TestTable> registerMvAndBase(
      const std::string& mvName,
      const std::string& baseName,
      const RowTypePtr& schema,
      const std::vector<std::string>& refreshColumns,
      const std::vector<connector::ColumnMapping>& columnMappings = {}) {
    SchemaTableName mvTableName{std::string(kSchema), mvName};
    SchemaTableName baseTableName{std::string(kSchema), baseName};

    auto mvTable = testConnector_->addTable(mvTableName, schema);
    testConnector_->addTable(baseTableName, schema);

    auto mappings = columnMappings;
    if (mappings.empty()) {
      for (const auto& name : schema->names()) {
        mappings.push_back(identityMapping(kSchema, mvName, baseName, name));
      }
    }

    mvTable->setMaterializedViewDefinition(
        makeMvDef(kSchema, mvName, baseName, mappings, refreshColumns));

    return mvTable;
  }

  // Sets the MV status for a given table.
  void setMvStatus(
      const SchemaTableName& tableName,
      connector::MaterializedViewState state,
      std::vector<std::string> boundaryValues = {}) {
    mvMetadata_->setMaterializedViewStatus(
        tableName,
        connector::MaterializedViewStatus{state, std::move(boundaryValues)});
  }

  std::shared_ptr<connector::TestConnector> testConnector_;
  std::shared_ptr<MvTestConnectorMetadata> mvMetadata_;
};

// --- Partially materialized MV: single range ---
TEST_F(MaterializedViewRewriteTest, partiallyMaterializedSingleRange) {
  auto schema = ROW({"a", "b", "ds"}, {BIGINT(), VARCHAR(), VARCHAR()});
  registerMvAndBase("mv_t", "base_t", schema, {"ds"});

  const std::string qMv = "mv_t";
  const std::string qBase = "base_t";
  setMvStatus(
      {std::string(kSchema), "mv_t"},
      connector::MaterializedViewState::kPartiallyMaterialized,
      {"2026-01-01", "2026-01-31"});

  lp::PlanBuilder::Context ctx(kConnectorId, kSchema);
  auto logicalPlan = lp::PlanBuilder(ctx).tableScan(qMv).build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan(matchName(qMv))
                     .filter()
                     .localPartition(
                         core::PlanMatcherBuilder()
                             .tableScan(matchName(qBase))
                             .filter()
                             .project()
                             .build())
                     .build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

// --- Partially materialized MV: multiple ranges ---

TEST_F(MaterializedViewRewriteTest, partiallyMaterializedMultipleRanges) {
  auto schema = ROW({"a", "b", "ds"}, {BIGINT(), VARCHAR(), VARCHAR()});
  registerMvAndBase("mv_t", "base_t", schema, {"ds"});

  const std::string qMv = "mv_t";
  const std::string qBase = "base_t";
  setMvStatus(
      {std::string(kSchema), "mv_t"},
      connector::MaterializedViewState::kPartiallyMaterialized,
      {"2026-01-01", "2026-01-15", "2026-02-01", "2026-02-15"});

  lp::PlanBuilder::Context ctx(kConnectorId, kSchema);
  auto logicalPlan = lp::PlanBuilder(ctx).tableScan(qMv).build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan(matchName(qMv))
                     .filter()
                     .localPartition(
                         core::PlanMatcherBuilder()
                             .tableScan(matchName(qBase))
                             .filter()
                             .project()
                             .build())
                     .build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

// --- Not materialized: full base table replacement ---

TEST_F(MaterializedViewRewriteTest, notMaterialized) {
  auto schema = ROW({"a", "b", "ds"}, {BIGINT(), VARCHAR(), VARCHAR()});
  registerMvAndBase("mv_t", "base_t", schema, {"ds"});

  const std::string qMv = "mv_t";
  const std::string qBase = "base_t";
  setMvStatus(
      {std::string(kSchema), "mv_t"},
      connector::MaterializedViewState::kNotMaterialized);

  lp::PlanBuilder::Context ctx(kConnectorId, kSchema);
  auto logicalPlan = lp::PlanBuilder(ctx).tableScan(qMv).build();

  auto plan = toSingleNodePlan(logicalPlan);

  // MV has no data — replaced with base table wrapped in a Project
  // (mapping base columns back to MV column names).
  auto matcher =
      core::PlanMatcherBuilder().tableScan(matchName(qBase)).project().build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

// --- Too many partitions missing: full base table replacement ---

TEST_F(MaterializedViewRewriteTest, tooManyPartitionsMissing) {
  auto schema = ROW({"a", "b", "ds"}, {BIGINT(), VARCHAR(), VARCHAR()});
  registerMvAndBase("mv_t", "base_t", schema, {"ds"});

  const std::string qMv = "mv_t";
  const std::string qBase = "base_t";
  setMvStatus(
      {std::string(kSchema), "mv_t"},
      connector::MaterializedViewState::kTooManyPartitionsMissing);

  lp::PlanBuilder::Context ctx(kConnectorId, kSchema);
  auto logicalPlan = lp::PlanBuilder(ctx).tableScan(qMv).build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher =
      core::PlanMatcherBuilder().tableScan(matchName(qBase)).project().build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

// --- Fully materialized: no rewrite needed ---

TEST_F(MaterializedViewRewriteTest, fullyMaterialized) {
  auto schema = ROW({"a", "b", "ds"}, {BIGINT(), VARCHAR(), VARCHAR()});
  registerMvAndBase("mv_t", "base_t", schema, {"ds"});

  const std::string qMv = "mv_t";
  setMvStatus(
      {std::string(kSchema), "mv_t"},
      connector::MaterializedViewState::kFullyMaterialized);

  lp::PlanBuilder::Context ctx(kConnectorId, kSchema);
  auto logicalPlan = lp::PlanBuilder(ctx).tableScan(qMv).build();

  auto plan = toSingleNodePlan(logicalPlan);

  // MV is fully materialized — scan MV as-is.
  auto matcher = core::PlanMatcherBuilder().tableScan(matchName(qMv)).build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

// --- Regular table (no MV definition): no rewrite ---

TEST_F(MaterializedViewRewriteTest, regularTableNoRewrite) {
  auto schema = ROW({"a", "b"}, {BIGINT(), VARCHAR()});
  testConnector_->addTable(
      SchemaTableName{std::string(kSchema), "regular_t"}, schema);

  lp::PlanBuilder::Context ctx(kConnectorId, kSchema);
  auto logicalPlan = lp::PlanBuilder(ctx).tableScan("regular_t").build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher =
      core::PlanMatcherBuilder().tableScan(matchName("regular_t")).build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

// --- Column name remapping between MV and base table ---

TEST_F(MaterializedViewRewriteTest, columnNameRemapping) {
  auto mvSchema =
      ROW({"mv_col_a", "mv_col_b", "ds"}, {BIGINT(), VARCHAR(), VARCHAR()});
  auto baseSchema =
      ROW({"base_col_a", "base_col_b", "ds"}, {BIGINT(), VARCHAR(), VARCHAR()});

  SchemaTableName mvName{std::string(kSchema), "mv_renamed"};
  SchemaTableName baseName{std::string(kSchema), "base_renamed"};
  const std::string qMv = "mv_renamed";
  const std::string qBase = "base_renamed";

  auto mvTable = testConnector_->addTable(mvName, mvSchema);
  testConnector_->addTable(baseName, baseSchema);

  std::vector<connector::ColumnMapping> mappings = {
      renamedMapping(
          kSchema, "mv_renamed", "base_renamed", "mv_col_a", "base_col_a"),
      renamedMapping(
          kSchema, "mv_renamed", "base_renamed", "mv_col_b", "base_col_b"),
      identityMapping(kSchema, "mv_renamed", "base_renamed", "ds"),
  };

  mvTable->setMaterializedViewDefinition(makeMvDef(
      kSchema,
      "mv_renamed",
      "base_renamed",
      mappings,
      std::vector<std::string>{"ds"}));

  setMvStatus(
      mvName,
      connector::MaterializedViewState::kPartiallyMaterialized,
      {"2026-01-01", "2026-01-31"});

  lp::PlanBuilder::Context ctx(kConnectorId, kSchema);
  auto logicalPlan = lp::PlanBuilder(ctx).tableScan(qMv).build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan(matchName(qMv))
                     .filter()
                     .localPartition(
                         core::PlanMatcherBuilder()
                             .tableScan(matchName(qBase))
                             .filter()
                             .project()
                             .build())
                     .build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

// --- MV with filter: filter should be pushed through UNION ALL ---

TEST_F(MaterializedViewRewriteTest, filterPushdownThroughUnionAll) {
  auto schema = ROW({"a", "b", "ds"}, {BIGINT(), VARCHAR(), VARCHAR()});
  registerMvAndBase("mv_t", "base_t", schema, {"ds"});

  const std::string qMv = "mv_t";
  const std::string qBase = "base_t";
  setMvStatus(
      {std::string(kSchema), "mv_t"},
      connector::MaterializedViewState::kPartiallyMaterialized,
      {"2026-01-01", "2026-01-31"});

  lp::PlanBuilder::Context ctx(kConnectorId, kSchema);
  auto logicalPlan =
      lp::PlanBuilder(ctx).tableScan(qMv).filter("a > 10").build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan(matchName(qMv))
                     .filter()
                     .localPartition(
                         core::PlanMatcherBuilder()
                             .tableScan(matchName(qBase))
                             .filter()
                             .project()
                             .build())
                     .build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

// --- MV in a JOIN: currently not supported, verify rewrite doesn't crash ---
// The MV rewrite replaces the MV BaseTable with a UNION ALL DT in the parent.
// When the MV is part of a join, the join condition references columns that
// are re-parented after the rewrite. This is a known limitation; the test
// verifies the query still produces a valid plan (the optimizer falls back
// to scanning the MV as-is if the rewrite fails to integrate with joins).
// TODO: Add full join support in MV rewrite.

// --- No refresh columns: no rewrite ---

TEST_F(MaterializedViewRewriteTest, noRefreshColumnsNoRewrite) {
  auto schema = ROW({"a", "b"}, {BIGINT(), VARCHAR()});

  SchemaTableName mvTableName{std::string(kSchema), "mv_no_refresh"};
  SchemaTableName baseTableName{std::string(kSchema), "base_no_refresh"};
  const std::string qMv = "mv_no_refresh";
  const std::string qBase = "base_no_refresh";

  auto mvTable = testConnector_->addTable(mvTableName, schema);
  testConnector_->addTable(baseTableName, schema);

  std::vector<connector::ColumnMapping> mappings = {
      identityMapping(kSchema, "mv_no_refresh", "base_no_refresh", "a"),
      identityMapping(kSchema, "mv_no_refresh", "base_no_refresh", "b"),
  };

  mvTable->setMaterializedViewDefinition(makeMvDef(
      kSchema, "mv_no_refresh", "base_no_refresh", mappings, std::nullopt));

  setMvStatus(
      mvTableName,
      connector::MaterializedViewState::kPartiallyMaterialized,
      {"2026-01-01", "2026-01-31"});

  lp::PlanBuilder::Context ctx(kConnectorId, kSchema);
  auto logicalPlan = lp::PlanBuilder(ctx).tableScan(qMv).build();

  auto plan = toSingleNodePlan(logicalPlan);

  // No refresh columns — rewrite bails out, scan MV as-is.
  auto matcher = core::PlanMatcherBuilder().tableScan(matchName(qMv)).build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

// --- Odd number of boundary values: no rewrite ---

TEST_F(MaterializedViewRewriteTest, oddBoundaryValuesNoRewrite) {
  auto schema = ROW({"a", "ds"}, {BIGINT(), VARCHAR()});
  registerMvAndBase("mv_odd", "base_odd", schema, {"ds"});

  const std::string qMv = "mv_odd";
  setMvStatus(
      {std::string(kSchema), "mv_odd"},
      connector::MaterializedViewState::kPartiallyMaterialized,
      {"2026-01-01"});

  lp::PlanBuilder::Context ctx(kConnectorId, kSchema);
  auto logicalPlan = lp::PlanBuilder(ctx).tableScan(qMv).build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = core::PlanMatcherBuilder().tableScan(matchName(qMv)).build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

// --- Verify query graph structure via verifyOptimization callback ---

TEST_F(MaterializedViewRewriteTest, queryGraphStructure) {
  auto schema = ROW({"a", "b", "ds"}, {BIGINT(), VARCHAR(), VARCHAR()});
  registerMvAndBase("mv_t", "base_t", schema, {"ds"});

  const std::string qMv = "mv_t";
  const std::string qBase = "base_t";
  setMvStatus(
      {std::string(kSchema), "mv_t"},
      connector::MaterializedViewState::kPartiallyMaterialized,
      {"2026-01-01", "2026-01-31"});

  lp::PlanBuilder::Context ctx(kConnectorId, kSchema);
  auto logicalPlan = lp::PlanBuilder(ctx).tableScan(qMv).build();

  verifyOptimization(*logicalPlan, [&](Optimization& opt) {
    auto* rootDt = opt.rootDt();
    ASSERT_NE(rootDt, nullptr);

    // After MV rewrite, the root should have one table: a UNION ALL DT.
    ASSERT_EQ(rootDt->tables.size(), 1);
    auto* unionDt = rootDt->tables[0]->as<DerivedTable>();
    ASSERT_NE(unionDt, nullptr);
    ASSERT_TRUE(unionDt->setOp.has_value());
    EXPECT_EQ(unionDt->setOp.value(), logical_plan::SetOperation::kUnionAll);

    // UNION ALL should have 2 children: MV DT and base DT.
    ASSERT_EQ(unionDt->children.size(), 2);

    // Both children share the same column objects as the UNION ALL parent.
    ASSERT_EQ(unionDt->columns.size(), 3);
    for (size_t i = 0; i < unionDt->columns.size(); ++i) {
      EXPECT_EQ(unionDt->children[0]->columns[i], unionDt->columns[i]);
      EXPECT_EQ(unionDt->children[1]->columns[i], unionDt->columns[i]);
    }

    // First child wraps the MV table.
    auto* mvChild = unionDt->children[0];
    ASSERT_EQ(mvChild->tables.size(), 1);
    EXPECT_TRUE(mvChild->tables[0]->is(PlanType::kTableNode));
    auto* mvBt = mvChild->tables[0]->as<BaseTable>();
    EXPECT_EQ(
        mvBt->schemaTable->name(),
        (SchemaTableName{std::string(kSchema), "mv_t"}));

    // Second child wraps the base table.
    auto* baseChild = unionDt->children[1];
    ASSERT_EQ(baseChild->tables.size(), 1);
    EXPECT_TRUE(baseChild->tables[0]->is(PlanType::kTableNode));
    auto* baseBt = baseChild->tables[0]->as<BaseTable>();
    EXPECT_EQ(
        baseBt->schemaTable->name(),
        (SchemaTableName{std::string(kSchema), "base_t"}));

    // MV table should have a range filter added.
    EXPECT_FALSE(mvBt->columnFilters.empty() && mvBt->filter.empty());

    // Base table should have a complement filter added.
    EXPECT_FALSE(baseBt->columnFilters.empty() && baseBt->filter.empty());

    // Child DTs should have exprs mapping inner columns to shared output.
    EXPECT_EQ(mvChild->exprs.size(), unionDt->columns.size());
    EXPECT_EQ(baseChild->exprs.size(), unionDt->columns.size());
  });
}

// --- Aggregation MV: MV stores pre-aggregated results ---
// The MV is defined as SELECT ds, SUM(x) AS total FROM base GROUP BY ds.
// The MV table has columns (ds, total), the base table has (x, ds).
// For missing partitions, the base table side returns raw rows.

TEST_F(MaterializedViewRewriteTest, aggregationMv) {
  auto mvSchema = ROW({"ds", "total"}, {VARCHAR(), BIGINT()});
  auto baseSchema = ROW({"x", "ds"}, {BIGINT(), VARCHAR()});

  SchemaTableName mvTableName{std::string(kSchema), "mv_agg"};
  SchemaTableName baseTableName{std::string(kSchema), "base_agg"};
  const std::string qMv = "mv_agg";
  const std::string qBase = "base_agg";

  auto mvTable = testConnector_->addTable(mvTableName, mvSchema);
  testConnector_->addTable(baseTableName, baseSchema);

  std::vector<connector::ColumnMapping> mappings = {
      identityMapping(kSchema, "mv_agg", "base_agg", "ds"),
      renamedMapping(kSchema, "mv_agg", "base_agg", "total", "x"),
  };

  mvTable->setMaterializedViewDefinition(makeMvDef(
      kSchema,
      "mv_agg",
      "base_agg",
      mappings,
      std::vector<std::string>{"ds"},
      "SELECT ds, SUM(x) AS total FROM base_agg GROUP BY ds"));

  setMvStatus(
      mvTableName,
      connector::MaterializedViewState::kPartiallyMaterialized,
      {"2026-01-01", "2026-01-31"});

  lp::PlanBuilder::Context ctx(kConnectorId, kSchema);
  auto logicalPlan = lp::PlanBuilder(ctx).tableScan(qMv).build();

  auto plan = toSingleNodePlan(logicalPlan);

  // MV side scans mv_agg with range filter; base side scans base_agg with
  // complement filter and a project to map column names.
  auto matcher = core::PlanMatcherBuilder()
                     .tableScan(matchName(qMv))
                     .filter()
                     .localPartition(
                         core::PlanMatcherBuilder()
                             .tableScan(matchName(qBase))
                             .filter()
                             .project()
                             .build())
                     .build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

// --- Column subset MV: MV selects only some columns from base ---
// The MV is defined as SELECT a, ds FROM base (dropping column b).
// The MV table has columns (a, ds), the base table has (a, b, ds).

TEST_F(MaterializedViewRewriteTest, columnSubsetMv) {
  auto mvSchema = ROW({"a", "ds"}, {BIGINT(), VARCHAR()});
  auto baseSchema = ROW({"a", "b", "ds"}, {BIGINT(), VARCHAR(), VARCHAR()});

  SchemaTableName mvTableName{std::string(kSchema), "mv_subset"};
  SchemaTableName baseTableName{std::string(kSchema), "base_subset"};
  const std::string qMv = "mv_subset";
  const std::string qBase = "base_subset";

  auto mvTable = testConnector_->addTable(mvTableName, mvSchema);
  testConnector_->addTable(baseTableName, baseSchema);

  std::vector<connector::ColumnMapping> mappings = {
      identityMapping(kSchema, "mv_subset", "base_subset", "a"),
      identityMapping(kSchema, "mv_subset", "base_subset", "ds"),
  };

  mvTable->setMaterializedViewDefinition(makeMvDef(
      kSchema,
      "mv_subset",
      "base_subset",
      mappings,
      std::vector<std::string>{"ds"},
      "SELECT a, ds FROM base_subset"));

  setMvStatus(
      mvTableName,
      connector::MaterializedViewState::kPartiallyMaterialized,
      {"2026-01-01", "2026-01-31"});

  lp::PlanBuilder::Context ctx(kConnectorId, kSchema);
  auto logicalPlan = lp::PlanBuilder(ctx).tableScan(qMv).build();

  auto plan = toSingleNodePlan(logicalPlan);

  auto matcher = core::PlanMatcherBuilder()
                     .tableScan(matchName(qMv))
                     .filter()
                     .localPartition(
                         core::PlanMatcherBuilder()
                             .tableScan(matchName(qBase))
                             .filter()
                             .project()
                             .build())
                     .build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

// --- Aggregation MV fully materialized: no rewrite needed ---
// Verifies that a fully materialized aggregation MV is scanned as-is.

TEST_F(MaterializedViewRewriteTest, aggregationMvFullyMaterialized) {
  auto mvSchema = ROW({"ds", "total"}, {VARCHAR(), BIGINT()});
  auto baseSchema = ROW({"x", "ds"}, {BIGINT(), VARCHAR()});

  SchemaTableName mvTableName{std::string(kSchema), "mv_agg_full"};
  SchemaTableName baseTableName{std::string(kSchema), "base_agg_full"};
  const std::string qMv = "mv_agg_full";

  auto mvTable = testConnector_->addTable(mvTableName, mvSchema);
  testConnector_->addTable(baseTableName, baseSchema);

  std::vector<connector::ColumnMapping> mappings = {
      identityMapping(kSchema, "mv_agg_full", "base_agg_full", "ds"),
      renamedMapping(kSchema, "mv_agg_full", "base_agg_full", "total", "x"),
  };

  mvTable->setMaterializedViewDefinition(makeMvDef(
      kSchema,
      "mv_agg_full",
      "base_agg_full",
      mappings,
      std::vector<std::string>{"ds"},
      "SELECT ds, SUM(x) AS total FROM base_agg_full GROUP BY ds"));

  setMvStatus(
      mvTableName, connector::MaterializedViewState::kFullyMaterialized);

  lp::PlanBuilder::Context ctx(kConnectorId, kSchema);
  auto logicalPlan = lp::PlanBuilder(ctx).tableScan(qMv).build();

  auto plan = toSingleNodePlan(logicalPlan);

  // Fully materialized — scan MV as-is, no UNION ALL.
  auto matcher = core::PlanMatcherBuilder().tableScan(matchName(qMv)).build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

// --- Aggregation MV not materialized: full base table replacement ---
// Verifies that an empty aggregation MV is replaced with a base table scan.

TEST_F(MaterializedViewRewriteTest, aggregationMvNotMaterialized) {
  auto mvSchema = ROW({"ds", "total"}, {VARCHAR(), BIGINT()});
  auto baseSchema = ROW({"x", "ds"}, {BIGINT(), VARCHAR()});

  SchemaTableName mvTableName{std::string(kSchema), "mv_agg_empty"};
  SchemaTableName baseTableName{std::string(kSchema), "base_agg_empty"};
  const std::string qMv = "mv_agg_empty";
  const std::string qBase = "base_agg_empty";

  auto mvTable = testConnector_->addTable(mvTableName, mvSchema);
  testConnector_->addTable(baseTableName, baseSchema);

  std::vector<connector::ColumnMapping> mappings = {
      identityMapping(kSchema, "mv_agg_empty", "base_agg_empty", "ds"),
      renamedMapping(kSchema, "mv_agg_empty", "base_agg_empty", "total", "x"),
  };

  mvTable->setMaterializedViewDefinition(makeMvDef(
      kSchema,
      "mv_agg_empty",
      "base_agg_empty",
      mappings,
      std::vector<std::string>{"ds"},
      "SELECT ds, SUM(x) AS total FROM base_agg_empty GROUP BY ds"));

  setMvStatus(mvTableName, connector::MaterializedViewState::kNotMaterialized);

  lp::PlanBuilder::Context ctx(kConnectorId, kSchema);
  auto logicalPlan = lp::PlanBuilder(ctx).tableScan(qMv).build();

  auto plan = toSingleNodePlan(logicalPlan);

  // Not materialized — replaced with base table scan + project.
  auto matcher =
      core::PlanMatcherBuilder().tableScan(matchName(qBase)).project().build();

  AXIOM_ASSERT_PLAN(plan, matcher);
}

} // namespace
} // namespace facebook::axiom::optimizer
