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

#include "axiom/optimizer/tests/PlanMatcher.h"
#include "axiom/optimizer/tests/QueryTestBase.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;

// Tests for UNION ALL fragment planning. Each test covers one
// leg-combination case and verifies both single-node and distributed
// plans, run under both optimizers. See
// axiom/optimizer/docs/UnionAllPlanning.md for the design. Distributed
// matchers carry the producer fragment type on the boundary that closes each
// fragment (gather / arbitrary / shuffle / shuffleMerge).
//
// TODO: Extend PlanMatcher to allow specifying symbol aliases in
// tableScan like other methods (singleAggregation, project, etc.).
// Then we can use the same column names in SQL across tables and
// reference them via aliases in matchers, instead of giving each table
// a distinct column name to avoid internal disambiguation renames.
class UnionAllTest : public test::QueryTestBase,
                     public ::testing::WithParamInterface<bool> {
 protected:
  void SetUp() override {
    QueryTestBase::SetUp();
    useV2_ = GetParam();

    // t and u have different column names ('a' and 'b') to avoid the
    // optimizer's internal column-disambiguation rename ('u.a' → 'a_0')
    // that would appear when both tables share the same column name.
    testConnector_->addTable("t", ROW("a", BIGINT()))
        ->setStats(100'000, {{"a", {.numDistinct = 1'000}}});
    testConnector_->addTable("u", ROW("b", BIGINT()))
        ->setStats(10'000, {{"b", {.numDistinct = 1'000}}});
  }
};

// Two scans (kSource + kSource) co-locate in one fragment with a
// LocalPartition. No remote exchanges.
TEST_P(UnionAllTest, twoScans) {
  auto logicalPlan = parseSelect("FROM t UNION ALL FROM u", kTestConnectorId);

  {
    auto matcher =
        matchScan("t").localPartition(matchScan("u").project()).build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    auto matcher = matchScan("t")
                       .localPartition(matchScan("u").project())
                       .gather(FragmentType::kSource)
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// Two DISTINCTs (kFixed N + kFixed N) co-locate in one kFixed N fragment
// with both incoming hash exchanges.
TEST_P(UnionAllTest, twoDistincts) {
  auto logicalPlan = parseSelect(
      "SELECT DISTINCT a FROM t UNION ALL SELECT DISTINCT b FROM u",
      kTestConnectorId);

  {
    auto matcher =
        matchScan("t")
            .singleAggregation({"a"}, {})
            .localPartition(
                matchScan("u").singleAggregation({"b"}, {}).project())
            .build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    auto matcher =
        matchScan("t")
            .distributedAggregation({"a"}, {})
            .localPartition(
                matchScan("u").distributedAggregation({"b"}, {}).project())
            .gather(FragmentType::kFixed)
            .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// All single-task legs (kSingle + kSingle) co-locate in one kSingle
// fragment.
TEST_P(UnionAllTest, twoValues) {
  auto logicalPlan =
      parseSelect("VALUES 1, 2 UNION ALL VALUES 3", kTestConnectorId);

  {
    auto matcher =
        matchValues().localPartition(matchValues().project()).build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    // All-gather UnionAll output stays gather; no extra gather Repartition
    // needed.
    auto matcher = matchValues()
                       .localPartition(matchValues().project())
                       .output(FragmentType::kSingle)
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// Scan + Values (kSource + kSingle) — Values wrapped in arbitrary, scan
// hosts the union fragment as kSource.
TEST_P(UnionAllTest, scanAndValues) {
  auto logicalPlan = parseSelect("FROM t UNION ALL VALUES 1", kTestConnectorId);

  {
    auto matcher =
        matchScan("t").localPartition(matchValues().project()).build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    // v2 renames the arbitrary-isolated Values leg on the exchange's consumer
    // side (an extra project); v1 fuses the rename into the leg before the
    // exchange. Which side to place it is future work.
    auto matcher = matchScan("t")
                       .localPartition(
                           matchValues()
                               .project()
                               .arbitrary(FragmentType::kSingle)
                               .projectIf(useV2_))
                       .gather(FragmentType::kSource)
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// Scan + two Values (kSource + kSingle + kSingle) — both Values legs are
// grouped into one kSingle sub-union and wrapped in a single arbitrary
// exchange (one wrap regardless of how many kSingle inputs there are).
TEST_P(UnionAllTest, scanAndTwoValues) {
  auto logicalPlan = parseSelect(
      "FROM t UNION ALL VALUES 1 UNION ALL VALUES 2", kTestConnectorId);

  {
    auto matcher =
        matchScan("t")
            .localPartition({matchValues().project(), matchValues().project()})
            .build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    auto matcher = matchScan("t")
                       .localPartition(
                           matchValues()
                               .project()
                               .localPartition(matchValues().project())
                               .arbitrary(FragmentType::kSingle))
                       .gather(FragmentType::kSource)
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// DISTINCT + Values (kFixed N + kSingle) — DISTINCT hosts the union
// fragment as kFixed N; Values wrapped in arbitrary.
TEST_P(UnionAllTest, distinctAndValues) {
  auto logicalPlan = parseSelect(
      "SELECT DISTINCT a FROM t UNION ALL VALUES 1", kTestConnectorId);

  {
    auto matcher = matchScan("t")
                       .singleAggregation({"a"}, {})
                       .localPartition(matchValues().project())
                       .build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    auto matcher = matchScan("t")
                       .distributedAggregation({"a"}, {})
                       .localPartition(
                           matchValues()
                               .project()
                               .arbitrary(FragmentType::kSingle)
                               .projectIf(useV2_))
                       .gather(FragmentType::kFixed)
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// Scan + DISTINCT (kSource + kFixed N) — scan adapts to kFixed N; both
// legs co-locate in one kFixed N fragment with the DISTINCT's hash exchange
// feeding the same N tasks. No arbitrary wrap.
TEST_P(UnionAllTest, scanAndDistinct) {
  auto logicalPlan = parseSelect(
      "SELECT DISTINCT a FROM t UNION ALL FROM u", kTestConnectorId);

  {
    auto matcher = matchScan("t")
                       .singleAggregation({"a"}, {})
                       .localPartition(matchScan("u").project())
                       .build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    auto matcher = matchScan("t")
                       .distributedAggregation({"a"}, {})
                       .localPartition(matchScan("u").project())
                       .gather(FragmentType::kFixed)
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// Scan + DISTINCT + Values (kSource + kFixed N + kSingle) — scan + DISTINCT
// co-locate in kFixed N fragment, kSingle Values wrapped in arbitrary into
// the same fragment.
TEST_P(UnionAllTest, scanAndDistinctAndValues) {
  auto logicalPlan = parseSelect(
      "FROM t "
      "UNION ALL SELECT DISTINCT b FROM u "
      "UNION ALL VALUES 1",
      kTestConnectorId);

  {
    auto matcher =
        matchScan("t")
            .localPartition(
                {matchScan("u").singleAggregation({"b"}, {}).project(),
                 matchValues().project()})
            .build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    auto matcher =
        matchScan("t")
            .localPartition(
                {matchScan("u").distributedAggregation({"b"}, {}).project(),
                 matchValues()
                     .project()
                     .arbitrary(FragmentType::kSingle)
                     .projectIf(useV2_)})
            .gather(FragmentType::kFixed)
            .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// ---------------------------------------------------------------------------
// B-class: GROUP BY over UNION ALL. Parent requires hash(K) on union output.
// Per the design (axiom/optimizer/docs/UnionAllPlanning.md), the principle is
// "Apply A; substitute hash(K) for arbitrary in any wrapping; add hash(K)
// shuffle above the union if its output isn't already hash(K)."
// ---------------------------------------------------------------------------

// GROUP BY over two scans. Both scans co-locate in one kSource fragment per
// twoScans; the GROUP BY's hash(a) shuffle goes above the union, with the
// FINAL agg in a separate kFixed N fragment.
TEST_P(UnionAllTest, groupByOverTwoScans) {
  auto logicalPlan = parseSelect(
      "SELECT a, COUNT(*) FROM (FROM t UNION ALL FROM u) GROUP BY a",
      kTestConnectorId);

  {
    auto matcher = matchScan("t")
                       .localPartition(matchScan("u").project())
                       .singleAggregation({"a"}, {"count(*)"})
                       .build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    auto matcher = matchScan("t")
                       .localPartition(matchScan("u").project())
                       .distributedAggregation({"a"}, {"count(*)"})
                       .gather(FragmentType::kFixed)
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// GROUP BY over two DISTINCTs (kFixed N + kFixed N). Both legs already
// produce hash output on their respective keys.
//
// TODO: The design's optimal plan would have the GROUP BY's FINAL agg
// run in the same kFixed N fragment as the union, with no extra hash
// shuffle (worker `i` already has all rows with hash(K) % N == i from
// both legs). Today the optimizer doesn't propagate column renames
// through UnionAll's distribution, so it can't see that the union output
// is hash(K). It adds an extra local hash partition above the union
// before the FINAL agg.
TEST_P(UnionAllTest, groupByOverTwoDistincts) {
  auto logicalPlan = parseSelect(
      "SELECT a, COUNT(*) FROM ("
      "  SELECT DISTINCT a FROM t UNION ALL SELECT DISTINCT b FROM u"
      ") GROUP BY a",
      kTestConnectorId);

  {
    auto matcher =
        matchScan("t")
            .singleAggregation({"a"}, {})
            .localPartition(
                matchScan("u").singleAggregation({"b"}, {}).project())
            .singleAggregation({"a"}, {"count(*)"})
            .build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    // The two distinct legs co-partition on the union output key, so the GROUP
    // BY runs in the union fragment with no remote shuffle. v2 does a local
    // two-stage aggregation (partial → local HASH → final); v1 a single
    // aggregation after the hash.
    auto builder =
        matchScan("t").distributedAggregation({"a"}, {}).localPartition(
            matchScan("u").distributedAggregation({"b"}, {}).project());
    if (useV2_) {
      builder.partialAggregation({"a"}, {"count(*)"})
          .localPartition({"a"})
          .finalAggregation();
    } else {
      builder.localPartition({"a"}).singleAggregation({"a"}, {"count(*)"});
    }
    auto matcher = builder.gather(FragmentType::kFixed).build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// GROUP BY over two single-task legs. Union stays kSingle; the GROUP BY runs
// in the same kSingle fragment. The query alias 'AS x(k)' doesn't propagate
// to the agg's grouping key (which uses internal name 'c0'); a Project on
// top renames 'c0' to 'k'.
TEST_P(UnionAllTest, groupByOverTwoValues) {
  auto logicalPlan = parseSelect(
      "SELECT k, COUNT(*) FROM ("
      "  VALUES 1, 2 UNION ALL VALUES 3"
      ") AS x(k) GROUP BY k",
      kTestConnectorId);

  {
    auto matcher = matchValues()
                       .localPartition(matchValues().project())
                       .singleAggregation({"c0"}, {"count(*)"})
                       .project()
                       .build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    // The union is kSingle (all-gather inputs), so the GROUP BY runs entirely
    // in that fragment with no remote shuffle. v2 does a local two-stage
    // aggregation (partial → local HASH → final) to pre-aggregate before the
    // intra-fragment exchange; v1 does a single aggregation after the hash.
    auto builder = matchValues().localPartition(matchValues().project());
    if (useV2_) {
      builder.partialAggregation({"c0"}, {"count(*)"})
          .localPartition({"c0"})
          .finalAggregation();
    } else {
      builder.localPartition({"c0"}).singleAggregation({"c0"}, {"count(*)"});
    }
    auto matcher = builder.project().output(FragmentType::kSingle).build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// GROUP BY over scan + Values (kSource + kSingle). Same A4 layout (Values
// wrapped in arbitrary, scan in kSource union); GROUP BY's hash shuffle
// goes above the union.
TEST_P(UnionAllTest, groupByOverScanAndValues) {
  auto logicalPlan = parseSelect(
      "SELECT a, COUNT(*) FROM ("
      "  FROM t UNION ALL VALUES 1"
      ") GROUP BY a",
      kTestConnectorId);

  {
    auto matcher = matchScan("t")
                       .localPartition(matchValues().project())
                       .singleAggregation({"a"}, {"count(*)"})
                       .build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    auto matcher = matchScan("t")
                       .localPartition(
                           matchValues()
                               .project()
                               .arbitrary(FragmentType::kSingle)
                               .projectIf(useV2_))
                       .distributedAggregation({"a"}, {"count(*)"})
                       .gather(FragmentType::kFixed)
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// GROUP BY over DISTINCT + Values (kFixed N + kSingle). DISTINCT hosts the
// kFixed N union, Values wrapped in arbitrary. The union's output is then
// hash-shuffled to a separate kFixed N fragment for the FINAL agg.
//
// TODO: The design's optimal plan would wrap kSingle Values in hash(K)
// directly (not arbitrary), making the union's output hash(K), and the
// FINAL agg would run in the same kFixed N fragment as the union. Today
// the kSingle wrap uses arbitrary and the agg adds its own hash shuffle
// above.
TEST_P(UnionAllTest, groupByOverDistinctAndValues) {
  auto logicalPlan = parseSelect(
      "SELECT a, COUNT(*) FROM ("
      "  SELECT DISTINCT a FROM t UNION ALL VALUES 1"
      ") GROUP BY a",
      kTestConnectorId);

  {
    auto matcher = matchScan("t")
                       .singleAggregation({"a"}, {})
                       .localPartition(matchValues().project())
                       .singleAggregation({"a"}, {"count(*)"})
                       .build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    // The arbitrary-isolated Values leg renames on the exchange's consumer side
    // in v2 (extra project). The union output is not hash(a) (the Values leg is
    // arbitrary), so the GROUP BY shuffles: v2 does partial → shuffle → final,
    // v1 a single aggregation after the shuffle.
    auto builder =
        matchScan("t").distributedAggregation({"a"}, {}).localPartition(
            matchValues()
                .project()
                .arbitrary(FragmentType::kSingle)
                .projectIf(useV2_));
    if (useV2_) {
      builder.distributedAggregation({"a"}, {"count(*)"});
    } else {
      builder.distributedSingleAggregation({"a"}, {"count(*)"});
    }
    auto matcher = builder.gather(FragmentType::kFixed).build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// GROUP BY over scan + DISTINCT (kSource + kFixed N). Both legs co-locate
// per A6 in kFixed N union (scan adapts to fixed width). GROUP BY's hash
// shuffle goes above the union (scan rows aren't hash(K), so output is
// mixed and a shuffle is needed).
TEST_P(UnionAllTest, groupByOverScanAndDistinct) {
  auto logicalPlan = parseSelect(
      "SELECT a, COUNT(*) FROM ("
      "  FROM t UNION ALL SELECT DISTINCT b FROM u"
      ") GROUP BY a",
      kTestConnectorId);

  {
    auto matcher =
        matchScan("t")
            .localPartition(
                matchScan("u").singleAggregation({"b"}, {}).project())
            .singleAggregation({"a"}, {"count(*)"})
            .build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    auto matcher =
        matchScan("t")
            .localPartition(
                matchScan("u").distributedAggregation({"b"}, {}).project())
            .distributedAggregation({"a"}, {"count(*)"})
            .gather(FragmentType::kFixed)
            .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// GROUP BY over scan + DISTINCT + Values (kSource + kFixed N + kSingle).
// scan + DISTINCT co-locate in kFixed N, Values wrapped in arbitrary. GROUP
// BY's hash shuffle goes above the union.
TEST_P(UnionAllTest, groupByOverScanAndDistinctAndValues) {
  auto logicalPlan = parseSelect(
      "SELECT a, COUNT(*) FROM ("
      "  FROM t "
      "  UNION ALL SELECT DISTINCT b FROM u "
      "  UNION ALL VALUES 1"
      ") GROUP BY a",
      kTestConnectorId);

  {
    auto matcher =
        matchScan("t")
            .localPartition(
                {matchScan("u").singleAggregation({"b"}, {}).project(),
                 matchValues().project()})
            .singleAggregation({"a"}, {"count(*)"})
            .build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    auto matcher =
        matchScan("t")
            .localPartition(
                {matchScan("u").distributedAggregation({"b"}, {}).project(),
                 matchValues()
                     .project()
                     .arbitrary(FragmentType::kSingle)
                     .projectIf(useV2_)})
            .distributedAggregation({"a"}, {"count(*)"})
            .gather(FragmentType::kFixed)
            .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// ---------------------------------------------------------------------------
// C-class: UNION ALL feeding ORDER BY (no LIMIT). Parent requires single-row
// output. Per the design (UnionAllPlanning.md C principle), parallel legs
// co-locate per A and a gather feeds the kSingle final fragment that hosts
// ORDER BY. kSingle legs would ideally co-locate in the final fragment
// directly.
//
// In all distributed plans, OrderBy splits into PARTIAL (in the parallel
// fragment) + LocalMerge + PartitionedOutput[SINGLE] + MergeExchange (in the
// final kSingle fragment). The matcher uses orderBy().localMerge().
// shuffleMerge().
// ---------------------------------------------------------------------------

// ORDER BY over two scans. Both scans co-locate in one kSource fragment
// per A1; that fragment's OrderBy is split into PARTIAL+LocalMerge with a
// merge exchange to the final kSingle fragment.
TEST_P(UnionAllTest, orderByOverTwoScans) {
  auto logicalPlan = parseSelect(
      "SELECT * FROM (FROM t UNION ALL FROM u) ORDER BY a", kTestConnectorId);

  {
    auto matcher = matchScan("t")
                       .localPartition(matchScan("u").project())
                       .orderBy({"a ASC NULLS LAST"})
                       .build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    auto matcher = matchScan("t")
                       .localPartition(matchScan("u").project())
                       .orderBy({"a ASC NULLS LAST"})
                       .localMerge()
                       .shuffleMerge(FragmentType::kSource)
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// ORDER BY over two single-task legs. The union is kSingle; per the design
// the ORDER BY should run in the same kSingle fragment with no remote
// exchange.
//
TEST_P(UnionAllTest, orderByOverTwoValues) {
  auto logicalPlan = parseSelect(
      "SELECT * FROM (VALUES 1 UNION ALL VALUES 2) ORDER BY 1",
      kTestConnectorId);

  {
    auto matcher = matchValues()
                       .localPartition(matchValues().project())
                       .orderBy({"c0 ASC NULLS LAST"})
                       .build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    // The union is kSingle, so the sort runs in that fragment: no gather.
    auto matcher = matchValues()
                       .localPartition(matchValues().project())
                       .orderBy({"c0 ASC NULLS LAST"})
                       .localMerge()
                       .output(FragmentType::kSingle)
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// ORDER BY over two DISTINCTs. Both DISTINCTs co-locate in kFixed N per A2;
// the ORDER BY's PARTIAL runs there with a gather (merge exchange) to the
// kSingle final fragment.
TEST_P(UnionAllTest, orderByOverTwoDistincts) {
  auto logicalPlan = parseSelect(
      "SELECT * FROM ("
      "  SELECT DISTINCT a FROM t UNION ALL SELECT DISTINCT b FROM u"
      ") ORDER BY a",
      kTestConnectorId);

  {
    auto matcher =
        matchScan("t")
            .singleAggregation({"a"}, {})
            .localPartition(
                matchScan("u").singleAggregation({"b"}, {}).project())
            .orderBy({"a ASC NULLS LAST"})
            .build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    auto matcher =
        matchScan("t")
            .distributedAggregation({"a"}, {})
            .localPartition(
                matchScan("u").distributedAggregation({"b"}, {}).project())
            .orderBy({"a ASC NULLS LAST"})
            .localMerge()
            .shuffleMerge(FragmentType::kFixed)
            .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// ORDER BY over scan + Values. Per the design, the scan is in its own kSource
// fragment with a gather above; Values co-locate in the kSingle final
// fragment.
//
// TODO: The optimizer follows A's rule (wrap kSingle in arbitrary into the
// parallel fragment) instead of C's rule (co-locate kSingle in final
// kSingle fragment). Today: Values wrapped in arbitrary into the kSource
// union fragment with the scan; gather above. Same gather count, but extra
// arbitrary exchange that the design avoids.
TEST_P(UnionAllTest, orderByOverScanAndValues) {
  auto logicalPlan = parseSelect(
      "SELECT * FROM (FROM t UNION ALL VALUES 1) ORDER BY a", kTestConnectorId);

  {
    auto matcher = matchScan("t")
                       .localPartition(matchValues().project())
                       .orderBy({"a ASC NULLS LAST"})
                       .build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    auto matcher = matchScan("t")
                       .localPartition(
                           matchValues()
                               .project()
                               .arbitrary(FragmentType::kSingle)
                               .projectIf(useV2_))
                       .orderBy({"a ASC NULLS LAST"})
                       .localMerge()
                       .shuffleMerge(FragmentType::kSource)
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// ORDER BY over DISTINCT + Values. Same TODO as orderByOverScanAndValues:
// Values wrapped in arbitrary into the kFixed N union fragment with the
// DISTINCT, instead of co-located in the kSingle final.
TEST_P(UnionAllTest, orderByOverDistinctAndValues) {
  auto logicalPlan = parseSelect(
      "SELECT * FROM (SELECT DISTINCT a FROM t UNION ALL VALUES 1) ORDER BY a",
      kTestConnectorId);

  {
    auto matcher = matchScan("t")
                       .singleAggregation({"a"}, {})
                       .localPartition(matchValues().project())
                       .orderBy({"a ASC NULLS LAST"})
                       .build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    auto matcher = matchScan("t")
                       .distributedAggregation({"a"}, {})
                       .localPartition(
                           matchValues()
                               .project()
                               .arbitrary(FragmentType::kSingle)
                               .projectIf(useV2_))
                       .orderBy({"a ASC NULLS LAST"})
                       .localMerge()
                       .shuffleMerge(FragmentType::kFixed)
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// ORDER BY over scan + DISTINCT. Both legs co-locate in kFixed N per A6;
// gather above to kSingle final.
TEST_P(UnionAllTest, orderByOverScanAndDistinct) {
  auto logicalPlan = parseSelect(
      "SELECT * FROM (FROM t UNION ALL SELECT DISTINCT b FROM u) ORDER BY a",
      kTestConnectorId);

  {
    auto matcher =
        matchScan("t")
            .localPartition(
                matchScan("u").singleAggregation({"b"}, {}).project())
            .orderBy({"a ASC NULLS LAST"})
            .build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    auto matcher =
        matchScan("t")
            .localPartition(
                matchScan("u").distributedAggregation({"b"}, {}).project())
            .orderBy({"a ASC NULLS LAST"})
            .localMerge()
            .shuffleMerge(FragmentType::kFixed)
            .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// ORDER BY over scan + DISTINCT + Values. Scan + DISTINCT co-locate in
// kFixed N per A6; Values wrapped in arbitrary; gather above.
//
// TODO: The optimizer follows A's rule (wrap kSingle in arbitrary into the
// parallel fragment) instead of C's rule (Values co-located in the kSingle
// final fragment). Same gather count, but extra arbitrary exchange that the
// design avoids.
TEST_P(UnionAllTest, orderByOverScanAndDistinctAndValues) {
  auto logicalPlan = parseSelect(
      "SELECT * FROM ("
      "  FROM t "
      "  UNION ALL SELECT DISTINCT b FROM u "
      "  UNION ALL VALUES 1) ORDER BY a",
      kTestConnectorId);

  {
    auto matcher =
        matchScan("t")
            .localPartition(
                {matchScan("u").singleAggregation({"b"}, {}).project(),
                 matchValues().project()})
            .orderBy({"a ASC NULLS LAST"})
            .build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    auto matcher =
        matchScan("t")
            .localPartition(
                {matchScan("u").distributedAggregation({"b"}, {}).project(),
                 matchValues()
                     .project()
                     .arbitrary(FragmentType::kSingle)
                     .projectIf(useV2_)})
            .orderBy({"a ASC NULLS LAST"})
            .localMerge()
            .shuffleMerge(FragmentType::kFixed)
            .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// ---------------------------------------------------------------------------
// D-class: UNION ALL as a hash join build. D1 is broadcast (A-class union),
// D2 is shuffled (B-class union).
// ---------------------------------------------------------------------------

// UNION ALL as build of a broadcast hash join. Build is small (small input
// table v plus filtered u) so the optimizer chooses broadcast over shuffled
// join. Distinct column names ('a', 'c', 'b') avoid the optimizer's
// disambiguation rename so matchers reference SQL-level names directly.
//
// Per D1's design: the build's union legs co-locate per A1 in one kSource
// fragment with a BROADCAST output above; the join's desired hash(c) is
// dropped because no leg produces it natively (treated as a hint, not a
// requirement, when no leg matches).
TEST_P(UnionAllTest, broadcastJoinBuildOverUnion) {
  testConnector_->addTable("v", ROW("c", BIGINT()))
      ->setStats(10, {{"c", {.numDistinct = 10}}});

  auto logicalPlan = parseSelect(
      "FROM t JOIN ("
      "  FROM v UNION ALL FROM u WHERE b < 0"
      ") s ON t.a = s.c",
      kTestConnectorId);

  {
    // v2 merges the equi-join keys (a = c) into one equivalence column, so the
    // join outputs only `a` and a project reconstructs `c` (= a) for the
    // output; v1 keeps both keys in the join output.
    auto matcher = matchScan("t")
                       .hashJoin(matchScan("v").localPartition(
                           matchScan("u").filter("b < 0").project()))
                       .projectIf(useV2_)
                       .build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    auto matcher =
        matchScan("t")
            .hashJoin(
                matchScan("v")
                    .localPartition(matchScan("u").filter("b < 0").project())
                    .broadcast(FragmentType::kSource))
            .projectIf(useV2_)
            .gather(FragmentType::kSource)
            .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// UNION ALL as build of a shuffled hash join. Both build legs (the union)
// and probe get hash-partitioned on the join key. The union legs co-locate
// in one kSource fragment with a single hash output (B1 pattern). Distinct
// column names ('a', 'c', 'b') avoid the optimizer's disambiguation rename.
TEST_P(UnionAllTest, shuffledJoinBuildOverUnion) {
  testConnector_->addTable("v", ROW("c", BIGINT()))
      ->setStats(100'000, {{"c", {.numDistinct = 1'000}}});

  auto logicalPlan = parseSelect(
      "SELECT t.a FROM t JOIN (FROM v UNION ALL FROM u) s ON t.a = s.c",
      kTestConnectorId);

  {
    auto matcher = matchScan("v")
                       .localPartition(matchScan("u").project())
                       .hashJoin(matchScan("t"))
                       .build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    auto matcher =
        matchScan("v")
            .localPartition(matchScan("u").project())
            .shuffle({"c"}, FragmentType::kSource)
            .hashJoin(matchScan("t").shuffle({"a"}, FragmentType::kSource))
            .gather(FragmentType::kFixed)
            .build();
    // Cap the broadcast size limit low so the union build stays hash
    // partitioned (the scenario under test) rather than the small probe being
    // broadcast.
    optimizerOptions_.broadcastSizeLimit = 1024;
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

// ---------------------------------------------------------------------------
// Distribution-metadata propagation through UNION ALL. UNION ALL preserves
// only how rows map to partitions (kind, partition type, partition keys);
// per-input ordering, uniqueness, and clustering do not survive because
// rows from different legs interleave arbitrarily.
// ---------------------------------------------------------------------------

// GROUP BY on a key that each leg of the UNION ALL is independently sorted
// on. The aggregation must be hash-based, not streaming: even though each
// leg is sorted on the grouping key, the UNION ALL output is not, so
// streaming aggregation would emit one group per leg per key. The per-leg
// ORDER BYs are dropped by the dead-sort optimization (their order is
// unobservable past the order-destroying UNION ALL).
TEST_P(UnionAllTest, groupByOverUnionAllWithOrderedLegs) {
  auto logicalPlan = parseSelect(
      "SELECT a, count(*) FROM ("
      "  (SELECT a FROM t ORDER BY a)"
      "  UNION ALL"
      "  (SELECT b FROM u ORDER BY b)"
      ") GROUP BY a",
      kTestConnectorId);

  {
    auto matcher = matchScan("t")
                       .localPartition(matchScan("u").project())
                       .singleAggregation({"a"}, {"count(*)"})
                       .build();
    AXIOM_ASSERT_PLAN(toSingleNodePlan(logicalPlan), matcher);
  }

  {
    auto matcher = matchScan("t")
                       .localPartition(matchScan("u").project())
                       .distributedAggregation({"a"}, {"count(*)"})
                       .gather(FragmentType::kFixed)
                       .build();
    AXIOM_ASSERT_DISTRIBUTED_PLAN(planVelox(logicalPlan).plan, matcher);
  }
}

AXIOM_INSTANTIATE_V1_V2(UnionAllTest);

} // namespace
} // namespace facebook::axiom::optimizer
