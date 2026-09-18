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

#include "axiom/cli/Pager.h"
#include <gtest/gtest.h>
#include <string>

namespace axiom::cli {
namespace {

// Text larger than a pipe buffer, so that a pager which stops reading makes the
// write fail part way through instead of all of it fitting in the pipe.
std::string largeText() {
  return std::string(256 << 10, 'x');
}

TEST(PagerTest, showsText) {
  EXPECT_TRUE(Pager::print(largeText(), "cat > /dev/null"));
}

// Quitting the pager before it has read everything leaves the results shown:
// reprinting them would defeat the quit.
TEST(PagerTest, pagerStopsReading) {
  EXPECT_TRUE(Pager::print(largeText(), "head -c 4096 > /dev/null"));
}

// Quitting also leaves a nonzero exit status behind, which likewise does not
// mean the results went unseen.
TEST(PagerTest, pagerExitsNonZeroAfterReading) {
  EXPECT_TRUE(Pager::print(largeText(), "cat > /dev/null; exit 3"));
}

// A command the shell cannot run shows nothing, so the caller has to print the
// results itself. Text this small reaches the pipe in full before the shell
// reports the failure, so the byte count alone cannot detect this.
TEST(PagerTest, commandCannotBeRun) {
  EXPECT_FALSE(Pager::print("results\n", "axiom_no_such_pager_command"));
}

TEST(PagerTest, commandIsNotExecutable) {
  EXPECT_FALSE(Pager::print("results\n", "/dev/null"));
}

} // namespace
} // namespace axiom::cli
