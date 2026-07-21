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

#include <folly/ScopeGuard.h>
#include <folly/json.h>
#include <gtest/gtest.h>

#include "axiom/cli/tests/SqlQueryRunnerTestBase.h"

using namespace facebook::velox;

namespace axiom::sql {
namespace {

// Runs the EXPLAIN (TYPE IO) cases under both optimizer v1 and v2 (the bool
// parameter), which must produce identical IO JSON.
class ExplainIoTest : public SqlQueryRunnerTestBase,
                      public ::testing::WithParamInterface<bool> {
 protected:
  void SetUp() override {
    useV2_ = GetParam();
    SqlQueryRunnerTestBase::SetUp();
  }
};

TEST_P(ExplainIoTest, inputAndOutputTables) {
  testConnector_->addTpchTables();

  // Parses and re-serializes JSON with sorted keys for deterministic
  // comparison.
  auto normalizeJson = [](const std::string& jsonStr) {
    folly::json::serialization_opts opts;
    opts.pretty_formatting = true;
    opts.sort_keys = true;
    return folly::json::serialize(folly::parseJson(jsonStr), opts);
  };

  auto getJson = [&](const std::string& query) {
    auto result = run(query);
    VELOX_CHECK(result.message.has_value());
    return normalizeJson(result.message.value());
  };

  // SELECT with no table scans.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT 1"),
      normalizeJson(R"({"inputTableColumnInfos": []})"));

  // SELECT with table scan.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM nation"),
      normalizeJson(R"({
        "inputTableColumnInfos": [{
          "table": {
            "catalog": "test",
            "schemaTable": {"schema": "default", "table": "nation"}
          },
          "columnConstraints": []
        }]
      })"));

  // CTAS with output table.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "CREATE TABLE t AS SELECT * FROM nation"),
      normalizeJson(R"({
        "inputTableColumnInfos": [{
          "table": {
            "catalog": "test",
            "schemaTable": {"schema": "default", "table": "nation"}
          },
          "columnConstraints": []
        }],
        "outputTable": {
          "catalog": "test",
          "schemaTable": {"schema": "default", "table": "t"}
        }
      })"));

  // INSERT with output table.
  testConnector_->addTable("t", ROW({"key", "name"}, {BIGINT(), VARCHAR()}));
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "INSERT INTO t(key, name) "
          "SELECT r_regionkey, r_name FROM region"),
      normalizeJson(R"({
        "inputTableColumnInfos": [{
          "table": {
            "catalog": "test",
            "schemaTable": {"schema": "default", "table": "region"}
          },
          "columnConstraints": []
        }],
        "outputTable": {
          "catalog": "test",
          "schemaTable": {"schema": "default", "table": "t"}
        }
      })"));

  // JOIN: multiple input tables sorted by name.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM nation, region "
          "   WHERE n_regionkey = r_regionkey"),
      normalizeJson(R"({
        "inputTableColumnInfos": [
          {
            "table": {
              "catalog": "test",
              "schemaTable": {"schema": "default", "table": "nation"}
            },
            "columnConstraints": []
          },
          {
            "table": {
              "catalog": "test",
              "schemaTable": {"schema": "default", "table": "region"}
            },
            "columnConstraints": []
          }
        ]
      })"));
}

TEST_P(ExplainIoTest, columnConstraints) {
  run("CREATE TABLE t (x BIGINT, n BIGINT, ds VARCHAR, region VARCHAR) "
      "WITH (explain_io = ARRAY['n', 'ds', 'region'])");
  SCOPE_EXIT {
    run("DROP TABLE t");
  };

  auto normalizeJson = [](const std::string& jsonStr) {
    folly::json::serialization_opts opts;
    opts.pretty_formatting = true;
    opts.sort_keys = true;
    return folly::json::serialize(folly::parseJson(jsonStr), opts);
  };

  auto getJson = [&](const std::string& query) {
    auto result = run(query);
    VELOX_CHECK(result.message.has_value());
    return normalizeJson(result.message.value());
  };

  auto makeConstraint = [&](const std::string& columnName,
                            const std::string& typeSignature,
                            const std::string& domainJson) {
    return normalizeJson(
        fmt::format(
            R"({{"columnName": "{}", "typeSignature": "{}", "domain": {}}})",
            columnName,
            typeSignature,
            domainJson));
  };

  auto makeTable = [&](const std::string& constraintsJson) {
    return normalizeJson(
        fmt::format(
            R"({{
          "inputTableColumnInfos": [{{
            "table": {{
              "catalog": "test",
              "schemaTable": {{"schema": "default", "table": "t"}}
            }},
            "columnConstraints": [{}]
          }}]
        }})",
            constraintsJson));
  };

  // Equality constraint.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM t "
          "   WHERE ds = '2026-03-17'"),
      makeTable(makeConstraint(
          "ds",
          "VARCHAR",
          R"({
            "nullsAllowed": false,
            "ranges": [
              {
                "low": {"value": "2026-03-17", "bound": "EXACTLY"},
                "high": {"value": "2026-03-17", "bound": "EXACTLY"}
              }
            ]
          })")));

  // IN constraint.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM t "
          "   WHERE ds IN ('2026-03-17', '2026-03-18')"),
      makeTable(makeConstraint(
          "ds",
          "VARCHAR",
          R"({
            "nullsAllowed": false,
            "ranges": [
              {
                "low": {"value": "2026-03-17", "bound": "EXACTLY"},
                "high": {"value": "2026-03-17", "bound": "EXACTLY"}
              },
              {
                "low": {"value": "2026-03-18", "bound": "EXACTLY"},
                "high": {"value": "2026-03-18", "bound": "EXACTLY"}
              }
            ]
          })")));

  // Range constraint.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM t "
          "   WHERE ds >= '2026-03-01' AND ds <= '2026-03-31'"),
      makeTable(makeConstraint(
          "ds",
          "VARCHAR",
          R"({
            "nullsAllowed": false,
            "ranges": [
              {
                "low": {"value": "2026-03-01", "bound": "EXACTLY"},
                "high": {"value": "2026-03-31", "bound": "EXACTLY"}
              }
            ]
          })")));

  // BETWEEN maps to a closed range, inclusive on both ends.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM t "
          "   WHERE ds BETWEEN '2026-03-01' AND '2026-03-31'"),
      makeTable(makeConstraint(
          "ds",
          "VARCHAR",
          R"({
            "nullsAllowed": false,
            "ranges": [
              {
                "low": {"value": "2026-03-01", "bound": "EXACTLY"},
                "high": {"value": "2026-03-31", "bound": "EXACTLY"}
              }
            ]
          })")));

  // BETWEEN on an integer column produces the same closed range.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM t "
          "   WHERE n BETWEEN 1 AND 10"),
      makeTable(makeConstraint(
          "n",
          "BIGINT",
          R"({
            "nullsAllowed": false,
            "ranges": [
              {
                "low": {"value": 1, "bound": "EXACTLY"},
                "high": {"value": 10, "bound": "EXACTLY"}
              }
            ]
          })")));

  // LIKE with a literal prefix maps to a half-open range [prefix, prefix++),
  // where the upper bound increments the last byte of the prefix.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM t "
          "   WHERE ds LIKE '2026-05%'"),
      makeTable(makeConstraint(
          "ds",
          "VARCHAR",
          R"({
            "nullsAllowed": false,
            "ranges": [
              {
                "low": {"value": "2026-05", "bound": "EXACTLY"},
                "high": {"value": "2026-06", "bound": "BELOW"}
              }
            ]
          })")));

  // The '_' single-character wildcard also terminates the prefix.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM t "
          "   WHERE ds LIKE '2026-05_'"),
      makeTable(makeConstraint(
          "ds",
          "VARCHAR",
          R"({
            "nullsAllowed": false,
            "ranges": [
              {
                "low": {"value": "2026-05", "bound": "EXACTLY"},
                "high": {"value": "2026-06", "bound": "BELOW"}
              }
            ]
          })")));

  // LIKE with no wildcards degenerates to equality.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM t "
          "   WHERE ds LIKE '2026-05-01'"),
      makeTable(makeConstraint(
          "ds",
          "VARCHAR",
          R"({
            "nullsAllowed": false,
            "ranges": [
              {
                "low": {"value": "2026-05-01", "bound": "EXACTLY"},
                "high": {"value": "2026-05-01", "bound": "EXACTLY"}
              }
            ]
          })")));

  auto noConstraints = normalizeJson(R"({
    "inputTableColumnInfos": [{
      "table": {
        "catalog": "test",
        "schemaTable": {"schema": "default", "table": "t"}
      },
      "columnConstraints": []
    }]
  })");

  // No constraint on explain_io column (filter on non-explain_io column only).
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM t "
          "   WHERE x = 1"),
      noConstraints);

  // Unconvertible filter on explain_io column drops the column entirely.
  // NOT is not supported, so ds <> 'foo' (which becomes NOT(eq(ds, 'foo')))
  // causes the column to be dropped.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM t "
          "   WHERE ds <> 'foo'"),
      noConstraints);

  // NOT BETWEEN (parsed as not(between(...))) is unconvertible and drops the
  // column, same as other negations.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM t "
          "   WHERE ds NOT BETWEEN '2026-03-01' AND '2026-03-31'"),
      noConstraints);

  // BETWEEN with low > high is an empty range, so the whole table is excluded.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM t "
          "   WHERE ds BETWEEN '2026-03-31' AND '2026-03-01'"),
      normalizeJson(R"({"inputTableColumnInfos": []})"));

  // LIKE with a leading wildcard has no usable prefix, so it is dropped.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM t "
          "   WHERE ds LIKE '%05'"),
      noConstraints);

  // LIKE with an ESCAPE clause is not converted (escape semantics are not
  // interpreted), so the column is dropped.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM t "
          "   WHERE ds LIKE '2026-05%' ESCAPE '#'"),
      noConstraints);

  // Mix of convertible and unconvertible filters: unconvertible conjunct is
  // skipped (broader is safe), convertible conjunct is shown.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM t "
          "   WHERE ds >= '2026-03-01' AND ds <> 'foo'"),
      makeTable(makeConstraint(
          "ds",
          "VARCHAR",
          R"({
            "nullsAllowed": false,
            "ranges": [
              {
                "low": {"value": "2026-03-01", "bound": "EXACTLY"}
              }
            ]
          })")));

  // IS NULL OR equality: nullsAllowed is true.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM t "
          "   WHERE ds IS NULL OR ds = '2026-03-17'"),
      makeTable(makeConstraint(
          "ds",
          "VARCHAR",
          R"({
            "nullsAllowed": true,
            "ranges": [
              {
                "low": {"value": "2026-03-17", "bound": "EXACTLY"},
                "high": {"value": "2026-03-17", "bound": "EXACTLY"}
              }
            ]
          })")));

  // OR with unconvertible disjunct: the entire OR expression cannot be
  // converted (dropping a disjunct would narrow the result, which is unsafe).
  // The unconvertible OR is skipped as a conjunct (broader is safe).
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM t "
          "   WHERE ds = '2026-03-17' OR length(ds) > 3"),
      noConstraints);

  // Constraints on both explain_io columns.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM t "
          "   WHERE ds = '2026-03-17' AND region = 'us'"),
      normalizeJson(R"({
        "inputTableColumnInfos": [{
          "table": {
            "catalog": "test",
            "schemaTable": {"schema": "default", "table": "t"}
          },
          "columnConstraints": [
            {
              "columnName": "ds",
              "typeSignature": "VARCHAR",
              "domain": {
                "nullsAllowed": false,
                "ranges": [{
                  "low": {"value": "2026-03-17", "bound": "EXACTLY"},
                  "high": {"value": "2026-03-17", "bound": "EXACTLY"}
                }]
              }
            },
            {
              "columnName": "region",
              "typeSignature": "VARCHAR",
              "domain": {
                "nullsAllowed": false,
                "ranges": [{
                  "low": {"value": "us", "bound": "EXACTLY"},
                  "high": {"value": "us", "bound": "EXACTLY"}
                }]
              }
            }
          ]
        }]
      })"));

  // Greater-than (exclusive low bound).
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM t "
          "   WHERE ds > '2026-03-01'"),
      makeTable(makeConstraint(
          "ds",
          "VARCHAR",
          R"({
            "nullsAllowed": false,
            "ranges": [
              {
                "low": {"value": "2026-03-01", "bound": "ABOVE"}
              }
            ]
          })")));

  // Subquery: constraints are extracted from the inner table scan.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM (SELECT * FROM t WHERE ds = '2026-03-17') sub"),
      makeTable(makeConstraint(
          "ds",
          "VARCHAR",
          R"({
            "nullsAllowed": false,
            "ranges": [
              {
                "low": {"value": "2026-03-17", "bound": "EXACTLY"},
                "high": {"value": "2026-03-17", "bound": "EXACTLY"}
              }
            ]
          })")));

  // UNION ALL: same table scanned twice, merged into one entry with
  // united constraints.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM t WHERE ds = '2026-03-17' "
          "UNION ALL "
          "SELECT * FROM t WHERE ds = '2026-03-18'"),
      makeTable(makeConstraint(
          "ds",
          "VARCHAR",
          R"({
            "nullsAllowed": false,
            "ranges": [
              {
                "low": {"value": "2026-03-17", "bound": "EXACTLY"},
                "high": {"value": "2026-03-17", "bound": "EXACTLY"}
              },
              {
                "low": {"value": "2026-03-18", "bound": "EXACTLY"},
                "high": {"value": "2026-03-18", "bound": "EXACTLY"}
              }
            ]
          })")));

  // UNION (distinct): domains from both branches are united.
  // (2026-03-17, +inf) ∪ (2026-03-18, +inf) = (2026-03-17, +inf).
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM t WHERE ds > '2026-03-17' "
          "UNION "
          "SELECT * FROM t WHERE ds > '2026-03-18'"),
      makeTable(makeConstraint(
          "ds",
          "VARCHAR",
          R"({
            "nullsAllowed": false,
            "ranges": [
              {
                "low": {"value": "2026-03-17", "bound": "ABOVE"}
              }
            ]
          })")));

  // ds <= x OR ds >= x covers all values — constraint is omitted.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT * FROM t "
          "   WHERE ds <= '2026-03-17' OR ds >= '2026-03-17'"),
      noConstraints);

  // Same table scanned twice with filters on different columns: neither column
  // is constrained in both scans, so the whole table may be read.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT a.x FROM t a, t b "
          "   WHERE a.ds = '2026-03-17' AND b.region = 'us'"),
      noConstraints);

  // Same table scanned twice: ds is constrained in both (values unioned), but
  // region only in one, so region is dropped (the other scan reads all
  // regions).
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT a.x FROM t a, t b "
          "   WHERE a.ds = '2026-03-17' "
          "     AND b.ds = '2026-03-18' AND b.region = 'us'"),
      makeTable(makeConstraint(
          "ds",
          "VARCHAR",
          R"({
            "nullsAllowed": false,
            "ranges": [
              {
                "low": {"value": "2026-03-17", "bound": "EXACTLY"},
                "high": {"value": "2026-03-17", "bound": "EXACTLY"}
              },
              {
                "low": {"value": "2026-03-18", "bound": "EXACTLY"},
                "high": {"value": "2026-03-18", "bound": "EXACTLY"}
              }
            ]
          })")));

  // Same table scanned twice with overlapping ds ranges: the union coalesces
  // them into a single range spanning both.
  ASSERT_EQ(
      getJson(
          "EXPLAIN (TYPE IO) "
          "SELECT a.x FROM t a, t b "
          "   WHERE a.ds BETWEEN '2026-03-01' AND '2026-03-20' "
          "     AND b.ds BETWEEN '2026-03-10' AND '2026-03-31'"),
      makeTable(makeConstraint(
          "ds",
          "VARCHAR",
          R"({
            "nullsAllowed": false,
            "ranges": [
              {
                "low": {"value": "2026-03-01", "bound": "EXACTLY"},
                "high": {"value": "2026-03-31", "bound": "EXACTLY"}
              }
            ]
          })")));
}

INSTANTIATE_TEST_SUITE_P(
    SqlQueryRunner,
    ExplainIoTest,
    ::testing::Values(false, true),
    [](const ::testing::TestParamInfo<bool>& info) {
      return info.param ? "v2" : "v1";
    });

} // namespace
} // namespace axiom::sql
