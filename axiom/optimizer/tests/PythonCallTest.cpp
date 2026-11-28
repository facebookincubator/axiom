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
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::axiom::optimizer {
namespace {

using namespace velox;
namespace lp = facebook::axiom::logical_plan;

class PythonCallTest : public test::QueryTestBase {
 protected:
  static constexpr std::string_view kEnvironment{"test://python"};

  // A Python function and the type its calls return.
  struct Function {
    lp::PythonFunctionPtr function;
    TypePtr resultType;
  };

  static Function makeFunction(
      std::string name,
      TypePtr resultType,
      bool deterministic = true,
      std::string_view environmentUri = kEnvironment,
      std::string_view pythonVersion = "3.12") {
    return {
        std::make_shared<lp::PythonFunction>(lp::PythonFunction{
            .name = name,
            .pickledFunction = "pickle of " + name,
            .environmentUri = std::string{environmentUri},
            .pythonVersion = std::string{pythonVersion},
            .deterministic = deterministic,
        }),
        std::move(resultType)};
  }

  // Returns the own name and pickled code of the functions that expressions
  // call by 'names'.
  std::vector<std::pair<std::string, std::string>> pickles(
      const std::vector<std::string>& names) const {
    std::vector<std::pair<std::string, std::string>> result;
    for (const auto& name : names) {
      const auto& function = *functions_.at(name).function;
      result.emplace_back(function.name, function.pickledFunction);
    }
    return result;
  }

  // Returns a PlanBuilder that scans 'table', with expressions that call the
  // functions in 'functions_' by name.
  lp::PlanBuilder makeBuilder(std::string_view table) {
    auto context = makeContext();
    context.hook = [this](
                       const std::string& name,
                       const std::vector<lp::ExprPtr>& args) -> lp::ExprPtr {
      const auto it = functions_.find(name);
      if (it == functions_.end()) {
        return nullptr;
      }
      return std::make_shared<lp::PythonCallExpr>(
          it->second.resultType, it->second.function, args);
    };
    lp::PlanBuilder builder(context);
    builder.tableScan(std::string{table});
    return builder;
  }

  folly::F14FastMap<std::string, Function> functions_{
      {"score", makeFunction("score", DOUBLE())},
      {"label", makeFunction("label", VARCHAR())},
      {"sample", makeFunction("sample", DOUBLE(), /*deterministic=*/false)},
      {"other",
       makeFunction(
           "other",
           DOUBLE(),
           /*deterministic=*/true,
           "test://other-python")},
      {"older",
       makeFunction(
           "older",
           DOUBLE(),
           /*deterministic=*/true,
           kEnvironment,
           "3.11")},
      // An equal function in a separate object.
      {"score_copy", makeFunction("score", DOUBLE())},
      // The same function, called for a different result type.
      {"score_text", makeFunction("score", VARCHAR())},
      // A function named like the TRY that wraps calls in a Velox plan.
      {"named_try", makeFunction("try", DOUBLE())},
  };

  void SetUp() override {
    test::QueryTestBase::SetUp();

    // A different function with the same name, as with two lambdas.
    auto otherScore = *functions_.at("score").function;
    otherScore.pickledFunction = "pickle of another score";
    functions_.emplace(
        "other_score",
        Function{
            std::make_shared<lp::PythonFunction>(std::move(otherScore)),
            DOUBLE()});
  }
};

TEST_F(PythonCallTest, singleCall) {
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(
          makeBuilder("nation").project({"score(n_name)"}).build()),
      matchScan("nation")
          .pythonCall(
              {"score(n_name) as s"},
              {.environmentUri = std::string{kEnvironment},
               .pythonVersion = "3.12",
               .functions = pickles({"score"})})
          .build());
}

// A call reading another call's result shares its node. With a SQL
// expression in between, the outer call runs in a node above.
TEST_F(PythonCallTest, nestedCalls) {
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(
          makeBuilder("nation").project({"label(score(n_name))"}).build()),
      matchScan("nation").pythonCall({"label(score(n_name)) as l"}).build());

  // The calls are in separate projections.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(makeBuilder("nation")
                           .project({"score(n_name) as s"})
                           .project({"label(s)"})
                           .build()),
      matchScan("nation").pythonCall({"label(score(n_name)) as l"}).build());

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(makeBuilder("nation")
                           .project({"label(score(n_name) + 1.0)"})
                           .build()),
      matchScan("nation")
          .pythonCall({"score(n_name) as s"})
          .project({"s + 1.0 as p"})
          .pythonCall({"label(p) as l"})
          .build());

  // A call reading a nondeterministic call's result runs in a node above it.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(
          makeBuilder("nation").project({"label(sample(n_name))"}).build()),
      matchScan("nation")
          .pythonCall({"sample(n_name) as s"})
          .pythonCall({"label(s) as l"})
          .build());
}

// Calls of one projection that read the same input share a node. Arguments
// that are expressions are computed below it; constants go to the node as
// they are.
TEST_F(PythonCallTest, callsShareNode) {
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(makeBuilder("nation")
                           .project(
                               {"score(n_name)",
                                "label(cast(n_nationkey as double) + 1.0)",
                                "score('x')"})
                           .build()),
      matchScan("nation")
          .project({"n_name", "cast(n_nationkey as double) + 1.0 as p"})
          .pythonCall(
              {"score(n_name) as a", "label(p) as b", "score('x') as c"})
          .build());

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(makeBuilder("nation").project({"score('x')"}).build()),
      matchScan("nation").pythonCall({"score('x') as s"}).build());

  // A filter fixing a column's value makes the call's argument a constant.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(makeBuilder("nation")
                           .filter("n_name = 'x'")
                           .project({"score(n_name)"})
                           .build()),
      matchScan("nation")
          .filter("n_name = 'x'")
          .projectNone()
          .pythonCall({"score('x') as s"})
          .build());
}

// A deterministic call appearing more than once runs once, also when its
// function comes as separate equal objects; a nondeterministic one runs once
// per occurrence.
TEST_F(PythonCallTest, repeatedCall) {
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(makeBuilder("nation")
                           .project({"score(n_name)", "score(n_name)"})
                           .build()),
      matchScan("nation")
          .pythonCall({"score(n_name) as s"})
          .project({"s", "s"})
          .build());

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(makeBuilder("nation")
                           .project({"score(n_name)", "score_copy(n_name)"})
                           .build()),
      matchScan("nation")
          .pythonCall({"score(n_name) as s"})
          .project({"s", "s"})
          .build());

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(makeBuilder("nation")
                           .project({"sample(n_name)", "sample(n_name)"})
                           .build()),
      matchScan("nation")
          .pythonCall(
              {"sample(n_name) as a", "sample(n_name) as b"},
              {.functions = pickles({"sample"})})
          .build());
}

// Each function and result type is a function of its own in the node, also
// when functions share a name, as two lambdas do, or a function is called for
// two result types. A function named try works too. The node keeps each
// function's own name for error messages.
TEST_F(PythonCallTest, functionNames) {
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(makeBuilder("nation")
                           .project({"score(n_name)", "score_text(n_name)"})
                           .build()),
      matchScan("nation")
          .pythonCall(
              {"score(n_name) as a", "score(n_name) as b"},
              {.functions = pickles({"score", "score_text"})})
          .build());

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(makeBuilder("nation")
                           .project({"score(n_name)", "other_score(n_name)"})
                           .build()),
      matchScan("nation")
          .pythonCall(
              {"score(n_name) as a", "score(n_name) as b"},
              {.functions = pickles({"score", "other_score"})})
          .build());

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(
          makeBuilder("nation").project({"named_try(n_name)"}).build()),
      matchScan("nation")
          .pythonCall(
              {"try(n_name) as t"}, {.functions = pickles({"named_try"})})
          .build());
}

// TRY over a call makes rows the call fails on NULL, including failures of
// the Python calls nested in it. Other arguments are computed outside the TRY,
// so they must be columns or constants; computed in a projection below, the
// TRY covers only the call. A call under TRY and a call reading its result run
// in separate nodes, each keeping its own TRY.
TEST_F(PythonCallTest, tryCall) {
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(
          makeBuilder("nation").project({"try(score(n_name))"}).build()),
      matchScan("nation").pythonCall({"try(score(n_name)) as s"}).build());

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(
          makeBuilder("nation").project({"try(label(score(n_name)))"}).build()),
      matchScan("nation")
          .pythonCall({"try(label(score(n_name))) as l"})
          .build());

  // A nondeterministic call nested under TRY has one reader.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(makeBuilder("nation")
                           .project({"try(label(sample(n_name)))"})
                           .build()),
      matchScan("nation")
          .pythonCall({"try(label(sample(n_name))) as l"})
          .build());

  // A call under TRY reading the result of a separate call, under TRY or
  // not, runs in a node above it, so each call keeps its own TRY.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(makeBuilder("nation")
                           .project({"try(score(n_name)) as s"})
                           .project({"try(label(s))"})
                           .build()),
      matchScan("nation")
          .pythonCall({"try(score(n_name)) as s"})
          .pythonCall({"try(label(s)) as l"})
          .build());

  // Calls with and without TRY share a node.
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(
          makeBuilder("nation")
              .project(
                  {"score(n_name)", "try(score(n_name))", "try(label(n_name))"})
              .build()),
      matchScan("nation")
          .pythonCall(
              {"score(n_name) as a",
               "try(score(n_name)) as b",
               "try(label(n_name)) as c"})
          .build());

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(
          makeBuilder("nation").project({"label(try(score(n_name)))"}).build()),
      matchScan("nation")
          .pythonCall({"try(score(n_name)) as s"})
          .pythonCall({"label(s) as l"})
          .build());

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(makeBuilder("nation")
                           .project({"score(n_name) as s"})
                           .project({"try(label(s))"})
                           .build()),
      matchScan("nation")
          .pythonCall({"score(n_name) as s"})
          .pythonCall({"try(label(s)) as l"})
          .build());

  VELOX_ASSERT_USER_THROW(
      toSingleNodePlan(
          makeBuilder("nation").project({"try(score(lower(n_name)))"}).build()),
      "Argument of a Python call under TRY must be a column, a constant or a "
      "Python call");
  VELOX_ASSERT_USER_THROW(
      toSingleNodePlan(makeBuilder("nation")
                           .project({"try(label(score(lower(n_name))))"})
                           .build()),
      "Argument of a Python call under TRY must be a column, a constant or a "
      "Python call");
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(makeBuilder("nation")
                           .project({"lower(n_name) as x"})
                           .project({"try(score(x))"})
                           .build()),
      matchScan("nation")
          .project({"lower(n_name) as x"})
          .pythonCall({"try(score(x)) as s"})
          .build());

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(makeBuilder("nation")
                           .project({"score(n_name) as s"})
                           .project({"try(label(score(s)))"})
                           .build()),
      matchScan("nation")
          .pythonCall({"score(n_name) as s"})
          .pythonCall({"try(label(score(s))) as l"})
          .build());
}

// A condition that does not read a call's result runs before the calls, so
// they see only the rows it keeps, also when it is nondeterministic.
TEST_F(PythonCallTest, filterPlacement) {
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(makeBuilder("nation")
                           .filter("n_nationkey > 10 AND score(n_name) > 0.5")
                           .project({"n_name"})
                           .build()),
      matchScan("nation")
          .filter("n_nationkey > 10")
          .pythonCall({"score(n_name) as s"})
          .filter("s > 0.5")
          .project({"n_name"})
          .build());

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(makeBuilder("nation")
                           .filter("rand() < 0.5 AND score(n_name) > 0.5")
                           .project({"n_name"})
                           .build()),
      matchScan("nation")
          .filter("rand() < 0.5")
          .pythonCall({"score(n_name) as s"})
          .filter("s > 0.5")
          .project({"n_name"})
          .build());
}

// A call runs only while something reads its result: a consumer above the
// node, or another call of the node.
TEST_F(PythonCallTest, unusedCall) {
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(makeBuilder("nation")
                           .project({"n_name", "score(n_name) as s"})
                           .project({"n_name"})
                           .build()),
      matchScan("nation").build());

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(
          makeBuilder("nation")
              .project({"score(n_name) as s", "label(n_name) as l"})
              .project({"s"})
              .build()),
      matchScan("nation").pythonCall({"score(n_name) as s"}).build());

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(
          makeBuilder("nation")
              .project({"score(n_name) as s", "label(score(n_name)) as l"})
              .project({"l"})
              .build()),
      matchScan("nation").pythonCall({"label(score(n_name)) as l"}).build());

  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(
          makeBuilder("nation")
              .project({"n_name", "try(label(score(n_name))) as l"})
              .project({"n_name"})
              .build()),
      matchScan("nation").build());
}

// A call over no rows plans to an empty Values.
TEST_F(PythonCallTest, emptyInput) {
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(makeBuilder("nation")
                           .filter("n_nationkey < 0 AND n_nationkey > 0")
                           .project({"score(n_name)"})
                           .build()),
      matchValues(makeRowVector(ROW("s", DOUBLE()), 0)).build());
}

// A call can feed an aggregate. A call inside a lambda body runs per element,
// so it cannot become a node.
TEST_F(PythonCallTest, positions) {
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(
          makeBuilder("nation").aggregate({}, {"sum(score(n_name))"}).build()),
      matchScan("nation")
          .pythonCall({"score(n_name) as s"})
          .singleAggregation({}, {"sum(s)"})
          .build());

  VELOX_ASSERT_USER_THROW(
      toSingleNodePlan(makeBuilder("nation")
                           .project({"transform(ARRAY[n_name], x -> score(x))"})
                           .build()),
      "Python call is not supported inside a lambda");
}

// A worker runs one environment with one Python version, so calls that
// differ in either run in separate nodes.
TEST_F(PythonCallTest, environments) {
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(
          makeBuilder("nation")
              .project({"score(n_name)", "other(n_name)", "older(n_name)"})
              .build()),
      matchScan("nation")
          .pythonCall(
              {"score(n_name) as a"},
              {.environmentUri = std::string{kEnvironment},
               .pythonVersion = "3.12"})
          .pythonCall(
              {"other(n_name) as b"},
              {.environmentUri = "test://other-python",
               .pythonVersion = "3.12"})
          .pythonCall(
              {"older(n_name) as c"},
              {.environmentUri = std::string{kEnvironment},
               .pythonVersion = "3.11"})
          .build());

  // A call nested under TRY runs in the node of the call it is nested in.
  VELOX_ASSERT_USER_THROW(
      toSingleNodePlan(
          makeBuilder("nation").project({"try(label(older(n_name)))"}).build()),
      "Python call under TRY must run in the Python version of the call it is "
      "nested in");
  VELOX_ASSERT_USER_THROW(
      toSingleNodePlan(
          makeBuilder("nation").project({"try(label(other(n_name)))"}).build()),
      "Python call under TRY must run in the environment of the call it is "
      "nested in");
}

// A call runs for every row of its node's input and fails the query on the
// first error, so it cannot sit where the query may skip it or turn its
// errors into NULLs. Computed in a projection below, it runs for every row,
// and only the expression above is guarded.
TEST_F(PythonCallTest, guardedPositions) {
  VELOX_ASSERT_USER_THROW(
      toSingleNodePlan(makeBuilder("nation")
                           .project({"coalesce(1.0, score(n_name))"})
                           .build()),
      "Python call is not supported under COALESCE");
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(makeBuilder("nation")
                           .project({"score(n_name) as s"})
                           .project({"coalesce(1.0, s)"})
                           .build()),
      matchScan("nation")
          .pythonCall({"score(n_name) as s"})
          .project({"coalesce(1.0, s)"})
          .build());

  VELOX_ASSERT_USER_THROW(
      toSingleNodePlan(
          makeBuilder("nation").project({"try(score(n_name) + 1.0)"}).build()),
      "Python call is not supported under TRY");
  AXIOM_ASSERT_PLAN(
      toSingleNodePlan(makeBuilder("nation")
                           .project({"score(n_name) as s"})
                           .project({"try(s + 1.0)"})
                           .build()),
      matchScan("nation")
          .pythonCall({"score(n_name) as s"})
          .project({"try(s + 1.0)"})
          .build());
}

} // namespace
} // namespace facebook::axiom::optimizer
