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

#include "axiom/optimizer/v2/Node.h"
#include "axiom/optimizer/v2/tests/UnitTestBase.h"

namespace facebook::axiom::optimizer::v2::test {
namespace {

using ::testing::HasSubstr;

class NodePrinterTest : public UnitTestBase {};

// WorkingTable prints its `name` and outputs but has no inputs.
TEST_F(NodePrinterTest, workingTable) {
  optimizer::ColumnVector cols{makeColumn("n", velox::BIGINT())};
  NodeCP wt = builder_->make<WorkingTable>({toName("counter"), cols});

  const std::string out = wt->toString();
  EXPECT_THAT(out, HasSubstr("- WorkingTable[name=counter]"));
  EXPECT_THAT(out, HasSubstr(":BIGINT"));
}

// FixedPoint prints its `name`, anchor sub-plan, and step sub-plan.
TEST_F(NodePrinterTest, fixedPoint) {
  optimizer::ColumnVector cols{makeColumn("n", velox::BIGINT())};
  NodeCP anchor = builder_->makeEmptyValues(cols);
  NodeCP step = builder_->make<WorkingTable>({toName("counter"), cols});
  NodeCP fp = builder_->make<FixedPoint>(
      {anchor, step, toName("counter"), std::move(cols)});

  const std::string out = fp->toString();
  EXPECT_THAT(out, HasSubstr("- FixedPoint[name=counter]"));
  EXPECT_THAT(out, HasSubstr("anchor:"));
  EXPECT_THAT(out, HasSubstr("- Values"));
  EXPECT_THAT(out, HasSubstr("step:"));
  EXPECT_THAT(out, HasSubstr("- WorkingTable[name=counter]"));
}

} // namespace
} // namespace facebook::axiom::optimizer::v2::test
