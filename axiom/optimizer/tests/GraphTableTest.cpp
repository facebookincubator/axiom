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

#include "axiom/optimizer/tests/QueryTestBase.h"

namespace facebook::axiom::optimizer {
namespace {

// Nations and regions, with an edge from each nation to its region.
constexpr std::string_view kNationGraph =
    "GRAPH g AS ("
    "  VERTEX TABLES ("
    "    nation KEY (n_nationkey) LABEL Nation PROPERTIES (n_name),"
    "    region KEY (r_regionkey) LABEL Region PROPERTIES (r_name))"
    "  EDGE TABLES ("
    "    nation AS in_region KEY (n_nationkey)"
    "      SOURCE KEY (n_nationkey) REFERENCES nation (n_nationkey)"
    "      DESTINATION KEY (n_regionkey) REFERENCES region (r_regionkey)"
    "      LABEL InRegion))";

// People and who knows whom.
constexpr std::string_view kPeopleGraph =
    "GRAPH g AS ("
    "  VERTEX TABLES (person KEY (id) LABEL Person PROPERTIES (id, name))"
    "  EDGE TABLES (knows KEY (src, dst)"
    "    SOURCE KEY (src) REFERENCES person (id)"
    "    DESTINATION KEY (dst) REFERENCES person (id) LABEL Knows))";

// Employees, with an edge from each employee to their manager that is the
// employee's own row.
constexpr std::string_view kManagerGraph =
    "GRAPH g AS ("
    "  VERTEX TABLES (employee KEY (id) LABEL Employee PROPERTIES (name))"
    "  EDGE TABLES ("
    "    employee AS reports_to KEY (id)"
    "      SOURCE KEY (id) REFERENCES employee (id)"
    "      DESTINATION KEY (manager) REFERENCES employee (id)"
    "      LABEL ReportsTo))";

class GraphTableTest : public test::QueryTestBase {
 protected:
  velox::core::PlanNodePtr toSingleNodePlan(
      std::string_view graph,
      std::string_view match) {
    return QueryTestBase::toSingleNodePlan(parseSelect(
        fmt::format("WITH {} SELECT * FROM GRAPH_TABLE (g {})", graph, match)));
  }

  // Matches a scan of kPeopleGraph's person table with 'aliases' for its
  // columns.
  static velox::core::PlanMatcherBuilder matchPerson(
      const std::vector<std::optional<std::string>>& aliases = {}) {
    return matchScan("person").aliases(aliases);
  }

  // Adds the tables of kPeopleGraph next to TPC-H: 1'000 people, each knowing
  // ten others.
  void configureTestConnector() override {
    testConnector_->addTpchTables();
    testConnector_
        ->addTable(
            "person",
            velox::ROW({{"id", velox::BIGINT()}, {"name", velox::VARCHAR()}}))
        ->setStats(1'000, {{"name", {.numDistinct = 1'000}}});
    testConnector_
        ->addTable("knows", velox::ROW({"src", "dst"}, velox::BIGINT()))
        ->setStats(10'000, {});
    testConnector_
        ->addTable(
            "employee",
            velox::ROW(
                {{"id", velox::BIGINT()},
                 {"manager", velox::BIGINT()},
                 {"name", velox::VARCHAR()}}))
        ->setStats(1'000, {});
  }
};

// ANY keeps one match per pair of endpoints: a RowNumber partitioned by both
// endpoints' keys with a limit of one. Each nation row is both a Nation vertex
// and its InRegion edge, so nation is read once.
TEST_F(GraphTableTest, selector) {
  auto plan = toSingleNodePlan(
      kNationGraph,
      "MATCH ANY (n IS Nation) -[IS InRegion]-> (r IS Region) "
      "COLUMNS (n.n_name AS nation, r.r_name AS region)");
  auto matcher =
      matchScan("nation")
          .aliases({"n_nationkey", "n_name", "n_regionkey"})
          .hashJoinInner(
              matchScan("region"), {.keys = {{"n_regionkey = r_regionkey"}}})
          .rowNumber({"n_nationkey", "n_regionkey"}, 1)
          .project({"n_name", "r_name"})
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A repeated edge walks from the start: the anchor reads the start's rows, and
// each step probes a hash table of the edges, built once for the loop. The
// walks with at least one hop then join the end.
TEST_F(GraphTableTest, repetition) {
  auto plan = toSingleNodePlan(
      kPeopleGraph,
      "MATCH (a IS Person) -[IS Knows]->{1,2} (b IS Person) "
      "COLUMNS (a.name AS a, b.name AS b)");
  auto matcher =
      matchFixedPoint(
          startFixedPoint("walk")
              .outputState(
                  /*append=*/true, matchPerson().project({"name", "id", "0"}))
              .hashTableState(matchScan("knows"))
              .plan(matchDelta("walk", {"name", "vertex", "hops"})
                        .filter("hops < 2")
                        .stateHashJoin({"vertex"})
                        .project({"name", "dst", "axiom_bigint_add(hops, 1)"}))
              .convergeOnEmpty({.maxIterations = 3}))
          .aliases({"name", "vertex", "hops"})
          .filter("hops >= 1")
          .hashJoinInner(
              matchPerson({"id", "b_name"}), {.keys = {{"vertex = id"}}})
          .project({"name", "b_name"})
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// With several workers and edges too large to broadcast, the walks run as one
// loop spread over the workers by the vertex each walk has reached: the edges
// are repartitioned on their source, and each step repartitions the walks on
// the vertex they reach. The end joins the walks where they are.
TEST_F(GraphTableTest, partitionedRepetition) {
  optimizerOptions_.broadcastSizeLimit = 0;
  auto logicalPlan = parseSelect(
      fmt::format(
          "WITH {} SELECT * FROM GRAPH_TABLE (g "
          "MATCH (a IS Person) -[IS Knows]->{{1,2}} (b IS Person) "
          "COLUMNS (a.name AS a, b.name AS b))",
          kPeopleGraph));
  auto matcher =
      matchFixedPoint(startFixedPoint("walk")
                          .outputState(
                              /*append=*/true,
                              matchPerson()
                                  .project({"name", "id AS vertex", "0"})
                                  .shuffle({"vertex"}))
                          .hashTableState(matchScan("knows").shuffle({"src"}))
                          .plan(matchDelta("walk", {"name", "vertex", "hops"})
                                    .filter("hops < 2")
                                    .stateHashJoin({"vertex"})
                                    .project(
                                        {"name",
                                         "dst AS reached",
                                         "axiom_bigint_add(hops, 1)"})
                                    .shuffle({"reached"}))
                          .convergeOnEmptyEverywhere({.maxIterations = 3}))
          .aliases({"name", "vertex", "hops"})
          .filter("hops >= 1")
          .hashJoinInner(
              matchPerson({"id", "b_name"}).shuffle({"id"}),
              {.keys = {{"vertex = id"}}})
          .project({"name", "b_name"})
          .gather()
          .build();
  AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
}

// A repeated subpath's hops are one repetition of its pattern, built once as
// the loop's hash table: the edges joined with the persons at both ends and
// filtered with the subpath's condition.
TEST_F(GraphTableTest, repeatedSubpath) {
  auto plan = toSingleNodePlan(
      kPeopleGraph,
      "MATCH "
      "     (a IS Person) "
      "     ((x IS Person) -[IS Knows]-> (y IS Person) WHERE x.id < y.id){1,2} "
      "     (b IS Person) "
      "COLUMNS (a.name AS a, b.name AS b)");
  auto matcher =
      matchFixedPoint(
          startFixedPoint("walk")
              .outputState(
                  /*append=*/true, matchPerson().project({"name", "id", "0"}))
              .hashTableState(
                  matchScan("knows")
                      .filter("src < dst")
                      .hashJoinInner(
                          matchPerson({"x_id"}), {.keys = {{"src = x_id"}}})
                      .hashJoinInner(
                          matchPerson({"y_id"}), {.keys = {{"dst = y_id"}}}))
              .plan(matchDelta("walk", {"name", "vertex", "hops"})
                        .filter("hops < 2")
                        .stateHashJoin({"vertex"})
                        .project({"name", "dst", "axiom_bigint_add(hops, 1)"}))
              .convergeOnEmpty({.maxIterations = 3}))
          .aliases({"name", "vertex", "hops"})
          .filter("hops >= 1")
          .hashJoinInner(
              matchPerson({"id", "b_name"}), {.keys = {{"vertex = id"}}})
          .project({"name", "b_name"})
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// Walks of at most one hop need no loop: the start's rows for zero hops, and
// the start joined with the edges for one.
TEST_F(GraphTableTest, upToOneHop) {
  auto plan = toSingleNodePlan(
      kPeopleGraph,
      "MATCH (a IS Person) -[IS Knows]->{0,1} (b IS Person) "
      "COLUMNS (a.name AS a, b.name AS b)");
  auto matcher =
      matchPerson()
          .project({"name", "id"})
          .localPartition(
              {matchScan("knows")
                   .hashJoinInner(matchPerson(), {.keys = {{"src = id"}}})
                   .project({"name", "dst"})})
          .aliases({"name", "vertex"})
          .hashJoinInner(
              matchPerson({"id", "b_name"}), {.keys = {{"vertex = id"}}})
          .project({"name", "b_name"})
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// An optional subpath walks zero or one hop, as {0,1} does, and the walks
// also carry its names: NULL for zero hops.
TEST_F(GraphTableTest, optionalSubpath) {
  auto plan = toSingleNodePlan(
      kPeopleGraph,
      "MATCH (a IS Person) ((x IS Person) -[IS Knows]-> (m IS Person))? (b IS Person) "
      "COLUMNS (b.name AS b, m.name AS via)");
  auto matcher =
      matchPerson()
          .project({"id", "null"})
          .localPartition({
              matchPerson({"a_id"})
                  .hashJoinInner(
                      matchScan("knows")
                          .hashJoinInner(
                              matchPerson({"x_id"}), {.keys = {{"src = x_id"}}})
                          .hashJoinInner(
                              matchPerson({"m_id", "m_name"}),
                              {.keys = {{"dst = m_id"}}}),
                      {.keys = {{"a_id = src"}}})
                  .project({"dst", "m_name"}),
          })
          .aliases({"vertex", "via"})
          .hashJoinInner(
              matchPerson({"b_id", "b_name"}), {.keys = {{"vertex = b_id"}}})
          .project({"b_name", "via"})
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// The end has fewer rows than the start, so the walk leaves from the end and
// reads each hop from its destination to its source.
TEST_F(GraphTableTest, repetitionFromEnd) {
  auto plan = toSingleNodePlan(
      kPeopleGraph,
      "MATCH (a IS Person) -[k IS Knows]->{1,2} (b IS Person WHERE b.name = 'Dave') "
      "COLUMNS (a.name AS a, k.src AS sources)");
  auto matcher =
      matchFixedPoint(
          startFixedPoint("walk")
              .outputState(
                  /*append=*/true,
                  matchPerson({"id", "name"})
                      .filter("name = 'Dave'")
                      .project({"id", "[]", "0"}))
              .hashTableState(matchScan("knows").project({"dst", "src"}))
              .plan(matchDelta("walk", {"vertex", "sources", "hops"})
                        .filter("hops < 2")
                        .stateHashJoin({"vertex"})
                        .project(
                            {"src",
                             "axiom_array_prepend(src, sources)",
                             "axiom_bigint_add(hops, 1)"}))
              .convergeOnEmpty({.maxIterations = 3}))
          .aliases({"vertex", "sources", "hops"})
          .filter("hops >= 1")
          .hashJoinInner(
              matchPerson({"id", "a_name"}), {.keys = {{"vertex = id"}}})
          .project({"a_name", "sources"})
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A vertex keeps only the tables that can connect to its edges. InRegion
// reaches only regions, so the vertex between the two edges reads region alone,
// with no union over nation and region and no table ID column.
TEST_F(GraphTableTest, narrowing) {
  auto plan = toSingleNodePlan(
      kNationGraph,
      "MATCH (a IS Nation) -[IS InRegion]-> (y IS Nation | Region) <-[IS InRegion]- (b IS Nation) "
      "COLUMNS (a.n_name AS a, y.r_name AS y, b.n_name AS b)");
  auto matcher =
      matchScan("nation")
          .aliases({"a_name", "a_regionkey"})
          .hashJoinInner(
              matchScan("region"), {.keys = {{"a_regionkey = r_regionkey"}}})
          .hashJoinInner(
              matchScan("nation").aliases({"b_name", "b_regionkey"}),
              {.keys = {{"a_regionkey = b_regionkey"}}})
          .project({"a_name", "r_name", "b_name"})
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

// A vertex between two edges reads its row from an edge whose rows are its
// rows. Each ReportsTo edge is its employee's row, so the manager between two
// hops is the second hop's row: employee is read once per hop and once for the
// top manager, and m's name comes from the second read.
TEST_F(GraphTableTest, edgeRows) {
  auto plan = toSingleNodePlan(
      kManagerGraph,
      "MATCH (a IS Employee) -[IS ReportsTo]-> (m IS Employee) "
      "-[IS ReportsTo]-> (b IS Employee) "
      "COLUMNS (a.name AS a, m.name AS m, b.name AS b)");
  auto matcher =
      matchScan("employee")
          .aliases({"a_manager", "a_name"})
          .hashJoinInner(
              matchScan("employee")
                  .aliases({"m_id", "m_manager", "m_name"})
                  .hashJoinInner(
                      matchScan("employee").aliases({"b_id", "b_name"}),
                      {.keys = {{"m_manager = b_id"}}}),
              {.keys = {{"a_manager = m_id"}}})
          .project({"a_name", "m_name", "b_name"})
          .build();
  AXIOM_ASSERT_PLAN(plan, matcher);
}

} // namespace
} // namespace facebook::axiom::optimizer
