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

#include <gtest/gtest.h>

#include "axiom/optimizer/tests/QueryTestBase.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::optimizer::test {
namespace {

using namespace facebook::velox;

// A table that can only be read by key is reachable only through an index
// lookup. These tests cover the planning and lowering of that shape.
class IndexLookupJoinTest : public QueryTestBase,
                            public testing::WithParamInterface<bool> {
 protected:
  void SetUp() override {
    useV2_ = GetParam();
    QueryTestBase::SetUp();
  }

  void configureTestConnector() override {
    testConnector_->addTable("t", ROW({"a", "b"}, BIGINT()));
    testConnector_->addTable("u", ROW("x", BIGINT()));
    testConnector_->addLookupTable(
        "lookup", ROW({"k1", "k2", "v"}, BIGINT()), {"k1", "k2"});
    testConnector_->addLookupTable(
        "partitioned_lookup",
        ROW({"k", "v"}, BIGINT()),
        {"k"},
        connector::TestBucketSpec{{"k"}, 4});
  }
};

// A lookup-only table is joined by key rather than scanned and hash-joined.
TEST_P(IndexLookupJoinTest, indexLookup) {
  const auto sql = "SELECT * FROM t JOIN lookup ON t.a = lookup.k1";
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(parseSelect(sql)),
      matchScan("t")
          .indexLookupJoin(
              matchScan("lookup"), core::JoinType::kInner, {"a = k1"})
          .projectIf(useV2_, {"a", "b", "a as k1", "k2", "v"})
          .build());
}

TEST_P(IndexLookupJoinTest, lookupOnLeft) {
  if (!useV2_) {
    return;
  }

  const auto sql = "SELECT * FROM lookup JOIN t ON lookup.k1 = t.a";
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(parseSelect(sql)),
      matchScan("t")
          .indexLookupJoin(
              matchScan("lookup"), core::JoinType::kInner, {"a = k1"})
          .project({"k1", "k2", "v", "k1 as a", "b"})
          .build());
}

TEST_P(IndexLookupJoinTest, leftJoin) {
  if (!useV2_) {
    return;
  }

  const auto sql = "SELECT * FROM t LEFT JOIN lookup ON t.a = lookup.k1";
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(parseSelect(sql)),
      matchScan("t")
          .indexLookupJoin(
              matchScan("lookup"), core::JoinType::kLeft, {"a = k1"})
          .build());
}

TEST_P(IndexLookupJoinTest, multiKey) {
  if (!useV2_) {
    return;
  }

  const auto sql =
      "SELECT * FROM t JOIN lookup ON t.a = lookup.k2 AND t.b = lookup.k1";
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(parseSelect(sql)),
      matchScan("t")
          .indexLookupJoin(
              matchScan("lookup"), core::JoinType::kInner, {"b = k1", "a = k2"})
          .project({"a", "b", "b as k1", "a as k2", "v"})
          .build());
}

TEST_P(IndexLookupJoinTest, residualFilter) {
  if (!useV2_) {
    return;
  }

  {
    SCOPED_TRACE("Filter on a selected lookup column");
    const auto sql =
        "SELECT * FROM t JOIN lookup ON t.a = lookup.k1 AND t.b < lookup.v";
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(parseSelect(sql)),
        matchScan("t")
            .indexLookupJoin(
                matchScan("lookup"),
                core::JoinType::kInner,
                {"a = k1"},
                "b < v")
            .project({"a", "b", "a as k1", "k2", "v"})
            .build());
  }

  {
    SCOPED_TRACE("Filter on a lookup column the query does not select");
    const auto sql =
        "SELECT t.a FROM t JOIN lookup ON t.a = lookup.k1 AND t.b < lookup.v";
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(parseSelect(sql)),
        matchScan("t")
            .indexLookupJoin(
                matchScan("lookup"),
                core::JoinType::kInner,
                {"a = k1"},
                "b < v")
            .build());
  }
}

TEST_P(IndexLookupJoinTest, prefixCoverage) {
  if (!useV2_) {
    return;
  }

  const auto sql = "SELECT * FROM t JOIN lookup ON t.a = lookup.k2";
  VELOX_ASSERT_THROW(
      toSingleNodePlan(parseSelect(sql)),
      "Lookup equality keys must cover an index prefix");
}

TEST_P(IndexLookupJoinTest, duplicateLookupKey) {
  if (!useV2_) {
    return;
  }

  {
    const auto sql =
        "SELECT * FROM t JOIN lookup ON t.a = lookup.k1 AND t.b = lookup.k1";
    SCOPED_TRACE(sql);
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(parseSelect(sql)),
        matchScan("t")
            .filter("a = b")
            .indexLookupJoin(
                matchScan("lookup"), core::JoinType::kInner, {"a = k1"})
            .project({"a", "b", "a as k1", "k2", "v"})
            .build());
  }

  {
    const auto sql =
        "SELECT * FROM t LEFT JOIN lookup "
        "ON t.a = lookup.k1 AND t.b = lookup.k1";
    SCOPED_TRACE(sql);
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(parseSelect(sql)),
        matchScan("t")
            .indexLookupJoin(
                matchScan("lookup"), core::JoinType::kLeft, {"a = k1"}, "a = b")
            .build());
  }
}

TEST_P(IndexLookupJoinTest, partitionedLookupUnsupported) {
  if (!useV2_) {
    return;
  }

  const auto sql =
      "SELECT * FROM t JOIN partitioned_lookup ON t.a = "
      "partitioned_lookup.k";
  VELOX_ASSERT_THROW(
      toSingleNodePlan(parseSelect(sql)),
      "Partitioned index lookup is unsupported");
}

// The join's filter applies a lookup conjunct the connector rejects, so a left
// join keeps a probe row whose matches all fail the conjunct.
TEST_P(IndexLookupJoinTest, connectorRejectedFilter) {
  if (!useV2_) {
    return;
  }

  {
    SCOPED_TRACE("Inner join");
    const auto sql =
        "SELECT * FROM t JOIN lookup ON t.a = lookup.k1 WHERE lookup.v > 0";
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(parseSelect(sql)),
        matchScan("t")
            .indexLookupJoin(
                matchScan("lookup"),
                core::JoinType::kInner,
                {"a = k1"},
                "v > 0")
            .project({"a", "b", "a as k1", "k2", "v"})
            .build());
  }

  {
    SCOPED_TRACE("Left join");
    const auto sql =
        "SELECT * FROM t LEFT JOIN lookup ON t.a = lookup.k1 AND lookup.v > 0";
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(parseSelect(sql)),
        matchScan("t")
            .indexLookupJoin(
                matchScan("lookup"), core::JoinType::kLeft, {"a = k1"}, "v > 0")
            .build());
  }

  {
    SCOPED_TRACE("Rejected filter on a column the query does not select");
    const auto sql =
        "SELECT t.a, lookup.k2 "
        "FROM t JOIN lookup ON t.a = lookup.k1 "
        "WHERE lookup.v > 0";
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(parseSelect(sql)),
        matchScan("t")
            .indexLookupJoin(
                matchScan("lookup"),
                core::JoinType::kInner,
                {"a = k1"},
                "v > 0")
            .build());
  }
}

// A filter the probe enforces is not applied to the lookup table: every row the
// lookup reads has keys equal to a probe row's.
TEST_P(IndexLookupJoinTest, probeFilters) {
  if (!useV2_) {
    return;
  }

  {
    SCOPED_TRACE("Filter on the probe");
    const auto sql =
        "SELECT * FROM t JOIN lookup ON t.a = lookup.k1 WHERE t.a = 5";
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(parseSelect(sql)),
        matchScan("t")
            .filter("a = 5")
            .indexLookupJoin(
                matchScan("lookup"), core::JoinType::kInner, {"a = k1"}, "")
            .project({"5 as a", "b", "5 as k1", "k2", "v"})
            .build());
  }

  {
    SCOPED_TRACE("Filter on the lookup keys");
    const auto sql =
        "SELECT * "
        "FROM lookup JOIN t ON lookup.k1 = t.a "
        "WHERE lookup.k1 > 5";
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(parseSelect(sql)),
        matchScan("t")
            .filter("a > 5")
            .indexLookupJoin(
                matchScan("lookup"), core::JoinType::kInner, {"a = k1"}, "")
            .project({"k1", "k2", "v", "k1 as a", "b"})
            .build());
  }

  {
    SCOPED_TRACE("Filter the probe input guarantees");
    const auto sql =
        "SELECT * "
        "FROM (SELECT * FROM t WHERE a > 5) t "
        "LEFT JOIN lookup ON t.a = lookup.k1 AND t.a = lookup.v";
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(parseSelect(sql)),
        matchScan("t")
            .filter("a > 5")
            .indexLookupJoin(
                matchScan("lookup"),
                core::JoinType::kLeft,
                {"a = k1"},
                "k1 = v")
            .build());
  }

  {
    SCOPED_TRACE("Filter the probe input guarantees, lookup on the left");
    const auto sql =
        "SELECT * "
        "FROM lookup JOIN (SELECT * FROM t WHERE a > 5) t ON lookup.k1 = t.a";
    AXIOM_ASSERT_PLAN(
        toSingleNodePlan(parseSelect(sql)),
        matchScan("t")
            .filter("a > 5")
            .indexLookupJoin(
                matchScan("lookup"), core::JoinType::kInner, {"a = k1"}, "")
            .project({"k1", "k2", "v", "k1 as a", "b"})
            .build());
  }
}

// A filter copied across a WHERE equality onto a lookup-only table below
// another join is applied by that index lookup join when the connector rejects
// it.
TEST_P(IndexLookupJoinTest, impliedFilters) {
  if (!useV2_) {
    return;
  }

  const auto sql =
      "SELECT * FROM u, (SELECT * FROM t JOIN lookup ON t.a = lookup.k1) q "
      "WHERE u.x = q.v AND u.x = 5";
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(parseSelect(sql)),
      matchScan("u")
          .filter("x = 5")
          .hashJoinInner(
              matchScan("t").indexLookupJoin(
                  matchScan("lookup"),
                  core::JoinType::kInner,
                  {"a = k1"},
                  "v = 5"),
              {.keys = {{"x = v"}}, .filter = ""})
          .project({"5", "a", "b", "a", "k2", "x"})
          .build());
}

TEST_P(IndexLookupJoinTest, downstreamJoin) {
  if (!useV2_) {
    return;
  }

  const auto sql =
      "SELECT * FROM (t JOIN lookup ON t.a = lookup.k1) JOIN u ON t.b = u.x";
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(parseSelect(sql)),
      matchScan("t")
          .indexLookupJoin(
              matchScan("lookup"), core::JoinType::kInner, {"a = k1"})
          .hashJoinInner(matchScan("u"), {.keys = {{"b = x"}}})
          .project({"a", "b", "a as k1", "k2", "v", "b as x"})
          .build());
}

AXIOM_INSTANTIATE_V1_V2(IndexLookupJoinTest);

} // namespace
} // namespace facebook::axiom::optimizer::test
