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

#include "axiom/cli/StdinReader.h"
#include <gtest/gtest.h>
#include <unistd.h>
#include <cstdio>
#include <fstream>
#include "axiom/cli/linenoise/linenoise.h"

namespace axiom::cli {
namespace {

class StdinReaderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    savedStdin_ = dup(STDIN_FILENO);
    ASSERT_NE(savedStdin_, -1);
  }

  void TearDown() override {
    dup2(savedStdin_, STDIN_FILENO);
    close(savedStdin_);
  }

  void setStdInput(const std::string& input) {
    int pipefd[2];
    ASSERT_EQ(pipe(pipefd), 0);

    ssize_t written = write(pipefd[1], input.data(), input.size());
    ASSERT_EQ(written, static_cast<ssize_t>(input.size()));
    close(pipefd[1]);

    dup2(pipefd[0], STDIN_FILENO);
    close(pipefd[0]);
  }

  void testSingleCommand(
      const std::string& input,
      const std::string& expected) {
    setStdInput(input);
    bool atEnd = false;
    EXPECT_EQ(readCommand("SQL> ", atEnd), expected);
    EXPECT_FALSE(atEnd);
  }

  void testMultiCommand(
      const std::string& input,
      const std::vector<std::string>& expected) {
    setStdInput(input);
    bool atEnd = false;
    for (const auto& cmd : expected) {
      EXPECT_EQ(readCommand("SQL> ", atEnd), cmd);
      EXPECT_FALSE(atEnd);
    }
  }

  std::vector<std::string> getHistory() {
    char tmpFile[] = "/tmp/linenoise_test_XXXXXX";
    int fd = mkstemp(tmpFile);
    EXPECT_NE(fd, -1);
    close(fd);
    linenoiseHistorySave(tmpFile);

    std::vector<std::string> lines;
    std::ifstream file(tmpFile);
    std::string line;
    while (std::getline(file, line)) {
      lines.push_back(line);
    }
    return lines;
  }

 private:
  int savedStdin_;
};

TEST_F(StdinReaderTest, singleCommand) {
  testSingleCommand("SELECT 1;\n", "SELECT 1");
  testSingleCommand("DROP TABLE bar;\n", "DROP TABLE bar");
}

TEST_F(StdinReaderTest, multiCommand) {
  testMultiCommand(
      "SELECT 1;\nSELECT 2;\nSELECT 3;\n",
      {"SELECT 1", "SELECT 2", "SELECT 3"});
}

TEST_F(StdinReaderTest, whitespace) {
  testSingleCommand("   SELECT 1;\n", "SELECT 1");
  testSingleCommand("SELECT 2;   \n", "SELECT 2");
  testSingleCommand("\t\tSELECT 3;\n", "SELECT 3");
  testSingleCommand("SELECT 4;\t\t\n", "SELECT 4");
  testSingleCommand("  \t  SELECT 5;  \t  \n", "SELECT 5");
  testSingleCommand("\r\nSELECT 6;\r\n", "SELECT 6");
}

TEST_F(StdinReaderTest, history) {
  auto initialSize = getHistory().size();
  testMultiCommand("SELECT\n1;\nSELECT 2;\n", {"SELECT\n1", "SELECT 2"});

  auto history = getHistory();
  ASSERT_EQ(history.size(), initialSize + 2);
  EXPECT_EQ(history[history.size() - 2], "SELECT 1;");
  EXPECT_EQ(history[history.size() - 1], "SELECT 2;");
}

} // namespace
} // namespace axiom::cli
