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
#include <gtest/gtest.h>

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/SchemaResolver.h"
#include "axiom/connectors/tests/TestConnector.h"
#include "axiom/optimizer/QueryGraph.h"
#include "axiom/optimizer/QueryGraphContext.h"
#include "axiom/optimizer/Schema.h"
#include "axiom/optimizer/tests/QueryTestBase.h"
#include "axiom/optimizer/v2/Builder.h"
#include "axiom/optimizer/v2/Node.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/common/memory/Memory.h"

namespace facebook::axiom::optimizer::v2::test {
namespace {

struct ReplacementCase {
  NodeCP original;
  NodeVector sameInputs;
  NodeVector replacementInputs;
};

class NodeTransformTest : public optimizer::test::QueryTestBase {
 protected:
  void SetUp() override {
    QueryTestBase::SetUp();
    pool_ = velox::memory::memoryManager()->addLeafPool("node_transform");
    allocator_ = std::make_unique<velox::HashStringAllocator>(pool_.get());
    context_ = std::make_unique<optimizer::QueryGraphContext>(*allocator_);
    optimizer::queryCtx() = context_.get();
    builder_ = std::make_unique<Builder>();
    resolver_ = std::make_unique<connector::SchemaResolver>(
        connector::ConnectorMetadataRegistry::global());
    schema_ = std::make_unique<optimizer::Schema>(*resolver_);
  }

  void TearDown() override {
    schema_.reset();
    resolver_.reset();
    builder_.reset();
    optimizer::queryCtx() = nullptr;
    context_.reset();
    allocator_.reset();
    pool_.reset();
    QueryTestBase::TearDown();
  }

  // Registers a single scannable table 't(a BIGINT)' through the test
  // connector, used by the scan and tableWrite cases (default is TPC-H).
  void configureTestConnector() override {
    writeTargetTable_ =
        testConnector_->addTable("t", velox::ROW({"a"}, {velox::BIGINT()}));
  }

  optimizer::ColumnCP makeColumn(
      std::string_view name,
      const velox::TypePtr& type) {
    return optimizer::Column::create(
        name, optimizer::Value(optimizer::queryCtx()->toType(type)));
  }

  NodeCP emptyLeaf(std::string_view name) {
    return builder_->makeEmptyValues(makeColumns(name));
  }

  // Builds a Values leaf with the same output columns as 'input' but a single
  // opaque row, so it interns to a different node than the empty-rows leaves
  // used as originals. Only node identity matters to withInputs, not contents.
  NodeCP replacementLeafWithSameColumns(NodeCP input) {
    std::vector<velox::Variant> row(
        input->outputColumns().size(), velox::Variant{static_cast<int64_t>(1)});
    const auto* rows = optimizer::queryCtx()->registerVariant(
        std::make_unique<velox::Variant>(velox::Variant::array(
            std::vector<velox::Variant>{velox::Variant::row(std::move(row))})));
    return builder_->makeValues(
        /*source=*/nullptr,
        rows,
        optimizer::ColumnVector{input->outputColumns()});
  }

  void assertLeafContract(NodeCP leaf, std::string_view error) {
    EXPECT_THAT(leaf->inputs(), testing::IsEmpty());
    EXPECT_EQ(leaf->withInputs({}, *builder_), leaf);
    VELOX_ASSERT_THROW(
        leaf->withInputs({emptyLeaf("unexpected")}, *builder_), error);
  }

  void assertNonLeafContract(const ReplacementCase& testCase) {
    ASSERT_FALSE(testCase.sameInputs.empty());
    ASSERT_EQ(testCase.sameInputs.size(), testCase.original->inputs().size());
    ASSERT_EQ(testCase.replacementInputs.size(), testCase.sameInputs.size());

    EXPECT_EQ(
        testCase.original->withInputs(testCase.sameInputs, *builder_),
        testCase.original);

    NodeCP rebuilt =
        testCase.original->withInputs(testCase.replacementInputs, *builder_);
    EXPECT_NE(rebuilt, testCase.original);
    EXPECT_EQ(rebuilt->nodeType(), testCase.original->nodeType());
    EXPECT_THAT(
        rebuilt->inputs(),
        testing::ElementsAreArray(testCase.replacementInputs));

    NodeVector wrongArity;
    if (testCase.sameInputs.size() != 1) {
      wrongArity.push_back(testCase.sameInputs[0]);
    }
    const std::string_view message =
        testCase.sameInputs.size() == 1 ? "(0 vs. 1)" : "(1 vs. 2)";
    VELOX_ASSERT_THROW(
        testCase.original->withInputs(std::move(wrongArity), *builder_),
        message);
  }

  ReplacementCase unaryCase(NodeCP node, NodeCP input) {
    return ReplacementCase{
        .original = node,
        .sameInputs = NodeVector{input},
        .replacementInputs = NodeVector{replacementLeafWithSameColumns(input)},
    };
  }

  optimizer::ColumnVector makeColumns(std::string_view name) {
    return optimizer::ColumnVector{makeColumn(name, velox::BIGINT())};
  }

  ExprCP truePredicate() {
    return builder_->makeBoolean(true);
  }

  // BaseTable backed by the connector-registered 't', with a real layout so
  // Scan's partitioning inference (which reads BaseTable::layout()) succeeds.
  optimizer::BaseTable* makeScannableBaseTable() {
    const auto* schemaTable = schema_->findTable(
        kTestConnectorId, SchemaTableName{kDefaultSchema, "t"});
    VELOX_CHECK_NOT_NULL(schemaTable);
    auto* baseTable = optimizer::make<optimizer::BaseTable>();
    baseTable->cname = toName("scan_alias");
    baseTable->schemaTable = schemaTable;
    baseTable->filteredCardinality = schemaTable->cardinality;
    return baseTable;
  }

  const connector::Table* makeConnectorTable() {
    return writeTargetTable_.get();
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<velox::HashStringAllocator> allocator_;
  std::unique_ptr<optimizer::QueryGraphContext> context_;
  std::unique_ptr<Builder> builder_;
  std::unique_ptr<connector::SchemaResolver> resolver_;
  std::unique_ptr<optimizer::Schema> schema_;
  std::shared_ptr<connector::Table> writeTargetTable_;
};

TEST_F(NodeTransformTest, scan) {
  auto* scan = builder_->make<Scan>(
      {makeScannableBaseTable(),
       makeColumns("scan_col"),
       ExprVector{truePredicate()}});
  assertLeafContract(scan, "Leaf node cannot have inputs: Scan");
}

TEST_F(NodeTransformTest, values) {
  assertLeafContract(
      emptyLeaf("values_col"), "Leaf node cannot have inputs: Values");
}

TEST_F(NodeTransformTest, filter) {
  NodeCP input = emptyLeaf("a");

  assertNonLeafContract(unaryCase(
      builder_->make<Filter>({input, ExprVector{truePredicate()}}), input));
}

TEST_F(NodeTransformTest, project) {
  NodeCP input = emptyLeaf("a");
  ColumnCP inputColumn = input->outputColumns()[0];
  ColumnCP boolColumn = makeColumn("flag", velox::BOOLEAN());

  assertNonLeafContract(unaryCase(
      builder_->make<Project>(
          {input,
           ExprVector{inputColumn, truePredicate()},
           optimizer::ColumnVector{inputColumn, boolColumn}}),
      input));
}

TEST_F(NodeTransformTest, limit) {
  NodeCP input = emptyLeaf("a");

  assertNonLeafContract(unaryCase(
      builder_->make<Limit>({input, /*offset=*/17, /*count=*/23}), input));
}

TEST_F(NodeTransformTest, sort) {
  NodeCP input = emptyLeaf("a");
  ColumnCP inputColumn = input->outputColumns()[0];

  assertNonLeafContract(unaryCase(
      builder_->make<Sort>(
          {input,
           ExprVector{inputColumn},
           optimizer::OrderTypeVector{optimizer::OrderType::kDescNullsLast}}),
      input));
}

TEST_F(NodeTransformTest, topN) {
  NodeCP input = emptyLeaf("a");
  ColumnCP inputColumn = input->outputColumns()[0];

  assertNonLeafContract(unaryCase(
      builder_->make<TopN>(
          {input,
           ExprVector{inputColumn},
           optimizer::OrderTypeVector{optimizer::OrderType::kDescNullsLast},
           /*offset=*/11,
           /*count=*/29}),
      input));
}

TEST_F(NodeTransformTest, aggregate) {
  NodeCP input = emptyLeaf("a");
  ColumnCP inputColumn = input->outputColumns()[0];
  ColumnCP aggregateColumn = makeColumn("sum_a", velox::BIGINT());

  assertNonLeafContract(unaryCase(
      builder_->make<Aggregate>(Aggregate::Key{
          .input = input,
          .groupingKeys = ExprVector{inputColumn},
          .aggregates = AggregateCallVector{builder_->makeAggregate(
              toName("sum"),
              optimizer::Value(inputColumn->value().type),
              ExprVector{inputColumn},
              optimizer::FunctionSet{},
              /*isDistinct=*/false,
              /*condition=*/nullptr,
              inputColumn->value().type,
              /*orderKeys=*/{},
              /*orderTypes=*/{})},
          .outputColumns =
              optimizer::ColumnVector{inputColumn, aggregateColumn},
          .step = AggregateStep::kSingle,
      }),
      input));
}

TEST_F(NodeTransformTest, groupId) {
  NodeCP input = emptyLeaf("a");
  ColumnCP inputColumn = input->outputColumns()[0];
  ColumnCP groupColumn = makeColumn("group_a", velox::BIGINT());
  ColumnCP groupIdColumn = makeColumn("group_id", velox::BIGINT());
  QGVector<QGVector<int32_t>> groupingSets;
  groupingSets.push_back(QGVector<int32_t>{0});

  assertNonLeafContract(unaryCase(
      builder_->make<GroupId>(
          {input,
           ExprVector{inputColumn},
           ExprVector{inputColumn},
           std::move(groupingSets),
           optimizer::ColumnVector{groupColumn},
           groupIdColumn,
           optimizer::ColumnVector{groupColumn, inputColumn, groupIdColumn}}),
      input));
}

TEST_F(NodeTransformTest, markDistinct) {
  NodeCP input = emptyLeaf("a");
  ColumnCP inputColumn = input->outputColumns()[0];
  ColumnCP markerColumn = makeColumn("marker", velox::BOOLEAN());

  assertNonLeafContract(unaryCase(
      builder_->make<MarkDistinct>(
          {input,
           optimizer::ColumnVector{markerColumn},
           ExprVector{inputColumn},
           optimizer::ColumnVector{},
           optimizer::ColumnVector{inputColumn, markerColumn}}),
      input));
}

TEST_F(NodeTransformTest, unnest) {
  ColumnCP inputColumn = makeColumn("a", velox::BIGINT());
  ColumnCP boolColumn = makeColumn("flag", velox::BOOLEAN());
  ColumnCP arrayColumn = makeColumn("items", velox::ARRAY(velox::BIGINT()));
  NodeCP input = builder_->makeEmptyValues(
      optimizer::ColumnVector{inputColumn, boolColumn, arrayColumn});
  ColumnCP unnestColumn = makeColumn("item", velox::BIGINT());
  ColumnCP ordinalityColumn = makeColumn("ord", velox::BIGINT());
  QGVector<optimizer::ColumnVector> unnestColumns;
  unnestColumns.push_back(optimizer::ColumnVector{unnestColumn});

  assertNonLeafContract(unaryCase(
      builder_->make<Unnest>(
          {input,
           ExprVector{arrayColumn},
           optimizer::ColumnVector{inputColumn},
           std::move(unnestColumns),
           ordinalityColumn,
           optimizer::ColumnVector{
               inputColumn, unnestColumn, ordinalityColumn}}),
      input));
}

TEST_F(NodeTransformTest, window) {
  NodeCP input = emptyLeaf("a");
  ColumnCP inputColumn = input->outputColumns()[0];
  WindowFunctions windowFunctions;
  windowFunctions.push_back(
      WindowFunction{
          .call = builder_->makeCall(
              toName("sum"),
              optimizer::Value(optimizer::queryCtx()->toType(velox::BIGINT())),
              ExprVector{inputColumn},
              optimizer::FunctionSet{}),
          .frame = optimizer::Frame::wholePartition(),
          .ignoreNulls = true,
      });
  ColumnCP windowColumn = makeColumn("window_sum", velox::BIGINT());

  assertNonLeafContract(unaryCase(
      builder_->make<Window>(
          {input,
           std::move(windowFunctions),
           ExprVector{inputColumn},
           ExprVector{inputColumn},
           optimizer::OrderTypeVector{optimizer::OrderType::kAscNullsFirst},
           optimizer::ColumnVector{inputColumn, windowColumn}}),
      input));
}

TEST_F(NodeTransformTest, topNRowNumber) {
  NodeCP input = emptyLeaf("a");
  ColumnCP inputColumn = input->outputColumns()[0];
  ColumnCP rankColumn = makeColumn("rank", velox::BIGINT());

  assertNonLeafContract(unaryCase(
      builder_->make<TopNRowNumber>(
          {input,
           TopNRowNumber::RankFunction::kRank,
           ExprVector{inputColumn},
           ExprVector{inputColumn},
           optimizer::OrderTypeVector{optimizer::OrderType::kAscNullsFirst},
           /*limit=*/7,
           rankColumn,
           optimizer::ColumnVector{inputColumn, rankColumn}}),
      input));
}

TEST_F(NodeTransformTest, enforceSingleRow) {
  NodeCP input = emptyLeaf("a");

  assertNonLeafContract(
      unaryCase(builder_->make<EnforceSingleRow>({input}), input));
}

TEST_F(NodeTransformTest, assignUniqueId) {
  NodeCP input = emptyLeaf("a");

  assertNonLeafContract(unaryCase(
      builder_->make<AssignUniqueId>(
          {input, makeColumn("uid", velox::BIGINT())}),
      input));
}

TEST_F(NodeTransformTest, enforceDistinct) {
  NodeCP input = emptyLeaf("a");
  ColumnCP inputColumn = input->outputColumns()[0];

  assertNonLeafContract(unaryCase(
      builder_->make<EnforceDistinct>(
          {input, ExprVector{inputColumn}, toName("duplicate scalar row")}),
      input));
}

TEST_F(NodeTransformTest, exchange) {
  NodeCP input = emptyLeaf("a");

  assertNonLeafContract(unaryCase(
      builder_->make<Exchange>(
          {input,
           Partitioning{
               .kind = PartitionKind::kGather,
               .scope = PropertyScope::kGlobal,
           }}),
      input));
}

TEST_F(NodeTransformTest, tableWrite) {
  NodeCP input = emptyLeaf("a");
  ColumnCP inputColumn = input->outputColumns()[0];

  assertNonLeafContract(unaryCase(
      builder_->make<TableWrite>(
          {input,
           makeConnectorTable(),
           connector::WriteKind::kInsert,
           ExprVector{inputColumn}}),
      input));
}

TEST_F(NodeTransformTest, multiInput) {
  NodeCP left = emptyLeaf("left");
  NodeCP right = emptyLeaf("right");
  NodeCP replacementLeft = replacementLeafWithSameColumns(left);
  NodeCP replacementRight = replacementLeafWithSameColumns(right);

  auto* unionAll = builder_->make<UnionAll>(
      {NodeVector{left, right},
       QGVector<optimizer::ColumnVector>{
           optimizer::ColumnVector{left->outputColumns()[0]},
           optimizer::ColumnVector{right->outputColumns()[0]}},
       optimizer::ColumnVector{makeColumn("union_out", velox::BIGINT())}});
  assertNonLeafContract({
      .original = unionAll,
      .sameInputs = NodeVector{left, right},
      .replacementInputs = NodeVector{replacementLeft, replacementRight},
  });

  auto* join = builder_->make<Join>(
      {left,
       right,
       velox::core::JoinType::kLeft,
       ExprVector{left->outputColumns()[0]},
       ExprVector{right->outputColumns()[0]},
       ExprVector{truePredicate()},
       /*nullAware=*/false,
       /*nullAsValue=*/false,
       optimizer::ColumnVector{
           left->outputColumns()[0], right->outputColumns()[0]}});
  assertNonLeafContract({
      .original = join,
      .sameInputs = NodeVector{left, right},
      .replacementInputs = NodeVector{replacementLeft, replacementRight},
  });

  ColumnCP includeMarker = makeColumn("include", velox::BOOLEAN());
  auto* apply = builder_->make<Apply>(
      {left,
       right,
       optimizer::ColumnVector{left->outputColumns()[0]},
       velox::core::JoinType::kLeft,
       ExprVector{truePredicate()},
       /*enforceSingleRow=*/true,
       /*markColumn=*/nullptr,
       /*inLhs=*/nullptr,
       /*inBodyKey=*/nullptr,
       includeMarker,
       optimizer::ColumnVector{
           left->outputColumns()[0],
           right->outputColumns()[0],
           includeMarker}});
  assertNonLeafContract({
      .original = apply,
      .sameInputs = NodeVector{left, right},
      .replacementInputs = NodeVector{replacementLeft, replacementRight},
  });
}

TEST_F(NodeTransformTest, unionAllWrongArity) {
  NodeCP left = emptyLeaf("left");
  NodeCP right = emptyLeaf("right");
  auto* unionAll = builder_->make<UnionAll>(
      {NodeVector{left, right},
       QGVector<optimizer::ColumnVector>{
           optimizer::ColumnVector{left->outputColumns()[0]},
           optimizer::ColumnVector{right->outputColumns()[0]}},
       optimizer::ColumnVector{makeColumn("union_out", velox::BIGINT())}});

  VELOX_ASSERT_THROW(
      unionAll->withInputs(NodeVector{left}, *builder_),
      "(1 vs. 2) UnionAll replacement input count must match existing input count");
}

} // namespace
} // namespace facebook::axiom::optimizer::v2::test
