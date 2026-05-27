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

#include <chrono>
#include <future>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "axiom/optimizer/Optimization.h"
#include "axiom/optimizer/tests/QueryTestBase.h"

using namespace facebook::velox;

namespace facebook::axiom::optimizer {
namespace {

constexpr std::chrono::seconds kOptimizationTimeout{30};

constexpr std::string_view kHangingSql = R"sql(
select
  ref_17.a as c0,
  (select b from main.t limit 1) as c1,
  case when ref_0.y <= subq_0.c11 then subq_0.c3 else subq_0.c3 end as c2,
  ref_5.b as c3,
  ref_13.a as c4
from
  main.u as ref_0
      inner join (select
              (select x from main.u limit 1) as c0,
              ref_1.x as c1,
              ref_1.y as c2,
              ref_1.y as c3,
              ref_1.x as c4,
              ref_1.x as c5,
              ref_1.y as c6,
              ref_1.y as c7,
              ref_1.x as c8,
              ref_1.y as c9,
              ref_1.y as c10,
              ref_1.y as c11,
              ref_1.x as c12
            from
              main.u as ref_1
            where ref_1.x is not NULL
            limit 109) as subq_0
        inner join main.u as ref_2
        on (EXISTS (
            select
                subq_0.c0 as c0
              from
                main.u as ref_3
              where true
              limit 140))
      on (EXISTS (
          select
              ref_0.x as c0,
              37 as c1
            from
              main.u as ref_4
            where (false)
              or (cast(null as integer) = cast(null as integer))
            limit 85))
    inner join main.t as ref_5
          left join main.u as ref_6
          on (15 is NULL)
        inner join main.u as ref_7
            inner join main.u as ref_8
              inner join main.t as ref_9
                right join main.u as ref_10
                on (ref_9.b = ref_9.b)
              on (ref_8.y LIKE ref_8.y)
            on (ref_9.c is not NULL)
          inner join main.u as ref_11
            inner join main.t as ref_12
            on (ref_11.x = ref_12.a )
          on ((ref_10.y >= ref_11.y)
              or (ref_9.d >= ref_9.d))
        on (ref_11.x is NULL)
      inner join main.t as ref_13
        left join main.u as ref_14
              right join main.t as ref_15
              on (ref_15.b = 26)
            inner join main.t as ref_16
            on (ref_16.a is NULL)
          inner join main.t as ref_17
            left join main.t as ref_18
            on (ref_18.a <= ref_18.b)
          on (false)
        on (ref_18.b < (select a from main.t limit 1))
      on (ref_17.c < ref_17.c)
    on (cast(nullif(ref_14.y, subq_0.c11) as varchar) > subq_0.c9)
where power(
    cast(subq_0.c8 as bigint),
    cast((select a from main.t limit 1) as bigint)) = ref_18.c
limit 59
)sql";

class JoinEnumerationCutoffTest : public test::QueryTestBase {
 protected:
  void SetUp() override {
    test::QueryTestBase::SetUp();

    testConnector_
        ->addTable(
            SchemaTableName{"main", "t"},
            ROW({"a", "b", "c", "d"}, {BIGINT(), BIGINT(), DOUBLE(), DATE()}))
        ->setStats(1'000, {});

    testConnector_
        ->addTable(
            SchemaTableName{"main", "u"},
            ROW({"x", "y"}, {BIGINT(), VARCHAR()}))
        ->setStats(1'000, {});
  }
};

TEST_F(JoinEnumerationCutoffTest, completesWithinTimeout) {
  auto logicalPlan =
      parseSelect(std::string(kHangingSql), kTestConnectorId, "main");

  std::packaged_task<void()> task([&]() {
    verifyOptimization(*logicalPlan, [](Optimization& opt) { opt.bestPlan(); });
  });
  auto future = task.get_future();
  std::thread worker(std::move(task));

  const auto status = future.wait_for(kOptimizationTimeout);
  if (status != std::future_status::ready) {
    worker.detach();
    FAIL() << "Optimization did not finish within "
           << kOptimizationTimeout.count()
           << "s — join enumeration hit combinatorial blowup in makeJoins()";
    return;
  }
  worker.join();
  future.get();
}

} // namespace
} // namespace facebook::axiom::optimizer
