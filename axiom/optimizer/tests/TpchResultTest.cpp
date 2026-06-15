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

#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include "axiom/optimizer/tests/HiveQueriesTestBase.h"
#include "axiom/optimizer/tests/TpchQueries.h"
#include "velox/exec/tests/utils/TpchQueryBuilder.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace facebook::velox;

// Runs each TPC-H query end-to-end on generated data and checks the results
// against a reference plan from `TpchQueryBuilder`. Plan shape is asserted
// separately in `TpchPlanTest`, which uses injected statistics and needs no
// data. CI generates scale factor 0.1; other scales can be supplied via
// `--tpch_data_path` (see `HiveQueriesTestBase`).
class TpchResultTest : public test::HiveQueriesTestBase {
 protected:
  static void SetUpTestCase() {
    test::HiveQueriesTestBase::SetUpTestCase();
    createTpchTables(velox::tpch::tables);
  }

  void SetUp() override {
    HiveQueriesTestBase::SetUp();
    referenceBuilder_ =
        std::make_unique<exec::test::TpchQueryBuilder>(localFileFormat_);
    referenceBuilder_->initialize(localDataPath_);
  }

  void checkTpchQuery(int32_t query) {
    checkResults(
        test::readTpchSql(query), referenceBuilder_->getQueryPlan(query).plan);
  }

  std::unique_ptr<exec::test::TpchQueryBuilder> referenceBuilder_;
};

TEST_F(TpchResultTest, q01) {
  checkTpchQuery(1);
}

TEST_F(TpchResultTest, q02) {
  checkTpchQuery(2);
}

TEST_F(TpchResultTest, q03) {
  checkTpchQuery(3);
}

TEST_F(TpchResultTest, q04) {
  checkTpchQuery(4);
}

TEST_F(TpchResultTest, q05) {
  checkTpchQuery(5);
}

TEST_F(TpchResultTest, q06) {
  checkTpchQuery(6);
}

TEST_F(TpchResultTest, q07) {
  checkTpchQuery(7);
}

TEST_F(TpchResultTest, q08) {
  checkTpchQuery(8);
}

TEST_F(TpchResultTest, q09) {
  checkTpchQuery(9);
}

// q9 with `p_name like '%green%'` replaced by the estimable `p_size <= 3`. The
// reference comes from `getAltPlan(9)`.
TEST_F(TpchResultTest, q09Alt) {
  checkResults(
      test::readTpchSql("q9_alt"), referenceBuilder_->getAltPlan(9).plan);
}

TEST_F(TpchResultTest, q10) {
  checkTpchQuery(10);
}

TEST_F(TpchResultTest, q11) {
  checkTpchQuery(11);
}

TEST_F(TpchResultTest, q12) {
  checkTpchQuery(12);
}

TEST_F(TpchResultTest, q13) {
  checkTpchQuery(13);
}

TEST_F(TpchResultTest, q14) {
  checkTpchQuery(14);
}

TEST_F(TpchResultTest, q15) {
  checkTpchQuery(15);
}

TEST_F(TpchResultTest, q16) {
  checkTpchQuery(16);
}

TEST_F(TpchResultTest, q17) {
  checkTpchQuery(17);
}

TEST_F(TpchResultTest, q18) {
  checkTpchQuery(18);
}

TEST_F(TpchResultTest, q19) {
  checkTpchQuery(19);
}

TEST_F(TpchResultTest, q20) {
  checkTpchQuery(20);
}

TEST_F(TpchResultTest, q21) {
  checkTpchQuery(21);
}

TEST_F(TpchResultTest, q22) {
  checkTpchQuery(22);
}

} // namespace
} // namespace facebook::axiom::optimizer

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  // TPC-H data generation instantiates a folly Singleton (DBGenBackend), which
  // requires folly::Init to have run registrationComplete().
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
