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

#include <folly/json.h>
#include <gtest/gtest.h>
#include <sstream>
#include "axiom/cli/AlignedTablePrinter.h"
#include "axiom/cli/JsonPrinter.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace axiom::sql {
namespace {

using namespace facebook::velox;

class OutputPrinterTest : public ::testing::Test,
                          public facebook::velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  std::string captureOutput(const std::function<void()>& func) {
    std::stringstream buffer;
    std::streambuf* oldCout = std::cout.rdbuf(buffer.rdbuf());
    func();
    std::cout.rdbuf(oldCout);
    return buffer.str();
  }
  RowVectorPtr createTestData() {
    return makeRowVector(
        {"id", "name", "age", "score"},
        {
            makeFlatVector<int32_t>({1, 2, 3}),
            makeFlatVector<std::string>({"Alice", "Bob", "Charlie"}),
            makeFlatVector<int64_t>({30, 25, 35}),
            makeFlatVector<double>({95.5, 87.3, 92.1}),
        });
  }

  RowVectorPtr createTestDataWithNulls() {
    return makeRowVector(
        {"id", "name", "value"},
        {
            makeNullableFlatVector<int32_t>({1, std::nullopt, 3}),
            makeNullableFlatVector<std::string>(
                {"Alice", std::nullopt, "Charlie"}),
            makeNullableFlatVector<double>({10.5, 20.0, std::nullopt}),
        });
  }
};

TEST_F(OutputPrinterTest, JsonPrinterBasic) {
  auto data = createTestData();
  JsonPrinter printer;

  auto output = captureOutput([&]() { printer.printResults({data}, 100); });

  std::istringstream stream(output);
  std::string line;
  std::vector<std::string> lines;
  while (std::getline(stream, line)) {
    if (!line.empty()) {
      lines.push_back(line);
    }
  }

  EXPECT_EQ(lines.size(), 3);

  auto json1 = folly::parseJson(lines[0]);
  EXPECT_EQ(json1["id"].asInt(), 1);
  EXPECT_EQ(json1["name"].asString(), "Alice");
  EXPECT_EQ(json1["age"].asInt(), 30);
  EXPECT_DOUBLE_EQ(json1["score"].asDouble(), 95.5);

  auto json2 = folly::parseJson(lines[1]);
  EXPECT_EQ(json2["id"].asInt(), 2);
  EXPECT_EQ(json2["name"].asString(), "Bob");
  EXPECT_EQ(json2["age"].asInt(), 25);
  EXPECT_DOUBLE_EQ(json2["score"].asDouble(), 87.3);

  auto json3 = folly::parseJson(lines[2]);
  EXPECT_EQ(json3["id"].asInt(), 3);
  EXPECT_EQ(json3["name"].asString(), "Charlie");
  EXPECT_EQ(json3["age"].asInt(), 35);
  EXPECT_DOUBLE_EQ(json3["score"].asDouble(), 92.1);
}

TEST_F(OutputPrinterTest, JsonPrinterWithNulls) {
  auto data = createTestDataWithNulls();
  JsonPrinter printer;

  auto output = captureOutput([&]() { printer.printResults({data}, 100); });

  std::istringstream stream(output);
  std::string line;
  std::vector<std::string> lines;
  while (std::getline(stream, line)) {
    if (!line.empty()) {
      lines.push_back(line);
    }
  }

  EXPECT_EQ(lines.size(), 3);

  auto json1 = folly::parseJson(lines[0]);
  EXPECT_EQ(json1["id"].asInt(), 1);
  EXPECT_EQ(json1["name"].asString(), "Alice");
  EXPECT_DOUBLE_EQ(json1["value"].asDouble(), 10.5);

  auto json2 = folly::parseJson(lines[1]);
  EXPECT_TRUE(json2["id"].isNull());
  EXPECT_TRUE(json2["name"].isNull());
  EXPECT_DOUBLE_EQ(json2["value"].asDouble(), 20.0);

  auto json3 = folly::parseJson(lines[2]);
  EXPECT_EQ(json3["id"].asInt(), 3);
  EXPECT_EQ(json3["name"].asString(), "Charlie");
  EXPECT_TRUE(json3["value"].isNull());
}

TEST_F(OutputPrinterTest, JsonPrinterMaxRows) {
  auto data = createTestData();
  JsonPrinter printer;

  auto output = captureOutput([&]() {
    auto rowsPrinted = printer.printResults({data}, 2);
    EXPECT_EQ(rowsPrinted, 3);
  });

  std::istringstream stream(output);
  std::string line;
  std::vector<std::string> lines;
  while (std::getline(stream, line)) {
    if (!line.empty()) {
      lines.push_back(line);
    }
  }

  EXPECT_EQ(lines.size(), 2);

  auto json1 = folly::parseJson(lines[0]);
  EXPECT_EQ(json1["id"].asInt(), 1);

  auto json2 = folly::parseJson(lines[1]);
  EXPECT_EQ(json2["id"].asInt(), 2);
}

TEST_F(OutputPrinterTest, JsonPrinterEmptyResults) {
  JsonPrinter printer;

  auto output = captureOutput([&]() {
    std::vector<RowVectorPtr> emptyResults;
    auto rowsPrinted = printer.printResults(emptyResults, 100);
    EXPECT_EQ(rowsPrinted, 0);
  });

  EXPECT_TRUE(output.empty());
}

TEST_F(OutputPrinterTest, JsonPrinterMultipleBatches) {
  auto data1 = makeRowVector(
      {"id", "name"},
      {
          makeFlatVector<int32_t>({1, 2}),
          makeFlatVector<std::string>({"Alice", "Bob"}),
      });

  auto data2 = makeRowVector(
      {"id", "name"},
      {
          makeFlatVector<int32_t>({3, 4}),
          makeFlatVector<std::string>({"Charlie", "Diana"}),
      });

  JsonPrinter printer;

  auto output =
      captureOutput([&]() { printer.printResults({data1, data2}, 100); });

  std::istringstream stream(output);
  std::string line;
  std::vector<std::string> lines;
  while (std::getline(stream, line)) {
    if (!line.empty()) {
      lines.push_back(line);
    }
  }

  EXPECT_EQ(lines.size(), 4);

  auto json1 = folly::parseJson(lines[0]);
  EXPECT_EQ(json1["id"].asInt(), 1);
  EXPECT_EQ(json1["name"].asString(), "Alice");

  auto json4 = folly::parseJson(lines[3]);
  EXPECT_EQ(json4["id"].asInt(), 4);
  EXPECT_EQ(json4["name"].asString(), "Diana");
}

TEST_F(OutputPrinterTest, AlignedTablePrinterBasic) {
  auto data = createTestData();
  AlignedTablePrinter printer;

  auto output = captureOutput([&]() { printer.printResults({data}, 100); });

  EXPECT_TRUE(output.find("id") != std::string::npos);
  EXPECT_TRUE(output.find("name") != std::string::npos);
  EXPECT_TRUE(output.find("age") != std::string::npos);
  EXPECT_TRUE(output.find("score") != std::string::npos);
  EXPECT_TRUE(output.find("Alice") != std::string::npos);
  EXPECT_TRUE(output.find("Bob") != std::string::npos);
  EXPECT_TRUE(output.find("Charlie") != std::string::npos);
  EXPECT_TRUE(output.find("(3 rows in 1 batches)") != std::string::npos);
  EXPECT_TRUE(output.find("-+-") != std::string::npos);
  EXPECT_TRUE(output.find(" | ") != std::string::npos);
}

TEST_F(OutputPrinterTest, AlignedTablePrinterMaxRows) {
  auto data = createTestData();
  AlignedTablePrinter printer;

  auto output = captureOutput([&]() {
    auto rowsPrinted = printer.printResults({data}, 2);
    EXPECT_EQ(rowsPrinted, 3);
  });

  EXPECT_TRUE(output.find("...1 more rows.") != std::string::npos);
  EXPECT_TRUE(output.find("Alice") != std::string::npos);
  EXPECT_TRUE(output.find("Bob") != std::string::npos);
  EXPECT_TRUE(output.find("Charlie") == std::string::npos);
}

TEST_F(OutputPrinterTest, AlignedTablePrinterEmptyResults) {
  AlignedTablePrinter printer;

  auto output = captureOutput([&]() {
    std::vector<RowVectorPtr> emptyResults;
    auto rowsPrinted = printer.printResults(emptyResults, 100);
    EXPECT_EQ(rowsPrinted, 0);
  });

  EXPECT_TRUE(output.find("(0 rows in 0 batches)") != std::string::npos);
}

TEST_F(OutputPrinterTest, TimingToString) {
  Timing timing;
  timing.micros = 1000000;
  timing.userNanos = 800000000;
  timing.systemNanos = 150000000;

  auto str = timing.toString();

  EXPECT_TRUE(str.find("user") != std::string::npos);
  EXPECT_TRUE(str.find("system") != std::string::npos);
  EXPECT_TRUE(str.find('%') != std::string::npos);
}

TEST_F(OutputPrinterTest, JsonPrinterBooleanTypes) {
  auto data = makeRowVector(
      {"id", "active"},
      {
          makeFlatVector<int32_t>({1, 2}),
          makeFlatVector<bool>({true, false}),
      });

  JsonPrinter printer;

  auto output = captureOutput([&]() { printer.printResults({data}, 100); });

  std::istringstream stream(output);
  std::string line;
  std::vector<std::string> lines;
  while (std::getline(stream, line)) {
    if (!line.empty()) {
      lines.push_back(line);
    }
  }

  auto json1 = folly::parseJson(lines[0]);
  EXPECT_TRUE(json1["active"].asBool());

  auto json2 = folly::parseJson(lines[1]);
  EXPECT_FALSE(json2["active"].asBool());
}

TEST_F(OutputPrinterTest, JsonPrinterArrayType) {
  auto data = makeRowVector(
      {
          "id",
          "tags",
      },
      {
          makeFlatVector<int32_t>({1, 2}),
          makeArrayVector<int32_t>({
              {1, 2, 3},
              {4, 5},
          }),
      });

  JsonPrinter printer;
  auto output = captureOutput([&]() { printer.printResults({data}, 100); });

  std::istringstream stream(output);
  std::string line;
  std::vector<std::string> lines;
  while (std::getline(stream, line)) {
    if (!line.empty()) {
      lines.push_back(line);
    }
  }

  EXPECT_EQ(lines.size(), 2);

  auto json1 = folly::parseJson(lines[0]);
  EXPECT_EQ(json1["id"].asInt(), 1);
  EXPECT_TRUE(json1["tags"].isArray());
  EXPECT_EQ(json1["tags"].size(), 3);
  EXPECT_EQ(json1["tags"][0].asInt(), 1);
  EXPECT_EQ(json1["tags"][1].asInt(), 2);
  EXPECT_EQ(json1["tags"][2].asInt(), 3);

  auto json2 = folly::parseJson(lines[1]);
  EXPECT_EQ(json2["id"].asInt(), 2);
  EXPECT_TRUE(json2["tags"].isArray());
  EXPECT_EQ(json2["tags"].size(), 2);
  EXPECT_EQ(json2["tags"][0].asInt(), 4);
  EXPECT_EQ(json2["tags"][1].asInt(), 5);
}

TEST_F(OutputPrinterTest, JsonPrinterArrayWithNulls) {
  auto elements =
      makeNullableFlatVector<int32_t>({1, std::nullopt, 3, 4, std::nullopt});
  auto data = makeRowVector(
      {
          "id",
          "values",
      },
      {
          makeFlatVector<int32_t>({1, 2}),
          makeArrayVector({0, 3}, elements),
      });

  JsonPrinter printer;
  auto output = captureOutput([&]() { printer.printResults({data}, 100); });

  std::istringstream stream(output);
  std::string line;
  std::vector<std::string> lines;
  while (std::getline(stream, line)) {
    if (!line.empty()) {
      lines.push_back(line);
    }
  }

  auto json1 = folly::parseJson(lines[0]);
  EXPECT_EQ(json1["values"].size(), 3);
  EXPECT_EQ(json1["values"][0].asInt(), 1);
  EXPECT_TRUE(json1["values"][1].isNull());
  EXPECT_EQ(json1["values"][2].asInt(), 3);

  auto json2 = folly::parseJson(lines[1]);
  EXPECT_EQ(json2["values"].size(), 2);
  EXPECT_EQ(json2["values"][0].asInt(), 4);
  EXPECT_TRUE(json2["values"][1].isNull());
}

TEST_F(OutputPrinterTest, JsonPrinterMapType) {
  auto data = makeRowVector(
      {
          "id",
          "attributes",
      },
      {
          makeFlatVector<int32_t>({1, 2}),
          makeMapVector<int32_t, std::string>({
              {{1, "one"}, {2, "two"}},
              {{3, "three"}},
          }),
      });

  JsonPrinter printer;
  auto output = captureOutput([&]() { printer.printResults({data}, 100); });

  std::istringstream stream(output);
  std::string line;
  std::vector<std::string> lines;
  while (std::getline(stream, line)) {
    if (!line.empty()) {
      lines.push_back(line);
    }
  }

  EXPECT_EQ(lines.size(), 2);

  auto json1 = folly::parseJson(lines[0]);
  EXPECT_EQ(json1["id"].asInt(), 1);
  EXPECT_TRUE(json1["attributes"].isObject());
  EXPECT_EQ(json1["attributes"]["1"].asString(), "one");
  EXPECT_EQ(json1["attributes"]["2"].asString(), "two");

  auto json2 = folly::parseJson(lines[1]);
  EXPECT_EQ(json2["id"].asInt(), 2);
  EXPECT_TRUE(json2["attributes"].isObject());
  EXPECT_EQ(json2["attributes"]["3"].asString(), "three");
}

TEST_F(OutputPrinterTest, JsonPrinterNestedStructType) {
  auto innerData = makeRowVector(
      {
          "street",
          "zip",
      },
      {
          makeFlatVector<std::string>({"123 Main St", "456 Oak Ave"}),
          makeFlatVector<int32_t>({12345, 67890}),
      });

  auto data = makeRowVector(
      {
          "id",
          "name",
          "address",
      },
      {
          makeFlatVector<int32_t>({1, 2}),
          makeFlatVector<std::string>({"Alice", "Bob"}),
          innerData,
      });

  JsonPrinter printer;
  auto output = captureOutput([&]() { printer.printResults({data}, 100); });

  std::istringstream stream(output);
  std::string line;
  std::vector<std::string> lines;
  while (std::getline(stream, line)) {
    if (!line.empty()) {
      lines.push_back(line);
    }
  }

  EXPECT_EQ(lines.size(), 2);

  auto json1 = folly::parseJson(lines[0]);
  EXPECT_EQ(json1["id"].asInt(), 1);
  EXPECT_EQ(json1["name"].asString(), "Alice");
  EXPECT_TRUE(json1["address"].isObject());
  EXPECT_EQ(json1["address"]["street"].asString(), "123 Main St");
  EXPECT_EQ(json1["address"]["zip"].asInt(), 12345);

  auto json2 = folly::parseJson(lines[1]);
  EXPECT_EQ(json2["id"].asInt(), 2);
  EXPECT_EQ(json2["name"].asString(), "Bob");
  EXPECT_TRUE(json2["address"].isObject());
  EXPECT_EQ(json2["address"]["street"].asString(), "456 Oak Ave");
  EXPECT_EQ(json2["address"]["zip"].asInt(), 67890);
}

TEST_F(OutputPrinterTest, JsonPrinterComplexNestedTypes) {
  auto arrayElements = makeArrayVector<int32_t>({
      {1, 2, 3},
      {4, 5},
      {6},
  });

  auto innerStruct = makeRowVector(
      {
          "count",
          "items",
      },
      {
          makeFlatVector<int32_t>({10, 20, 30}),
          arrayElements,
      });

  auto data = makeRowVector(
      {
          "id",
          "data",
      },
      {
          makeFlatVector<int32_t>({1, 2, 3}),
          innerStruct,
      });

  JsonPrinter printer;
  auto output = captureOutput([&]() { printer.printResults({data}, 100); });

  std::istringstream stream(output);
  std::string line;
  std::vector<std::string> lines;
  while (std::getline(stream, line)) {
    if (!line.empty()) {
      lines.push_back(line);
    }
  }

  EXPECT_EQ(lines.size(), 3);

  auto json1 = folly::parseJson(lines[0]);
  EXPECT_EQ(json1["id"].asInt(), 1);
  EXPECT_TRUE(json1["data"].isObject());
  EXPECT_EQ(json1["data"]["count"].asInt(), 10);
  EXPECT_TRUE(json1["data"]["items"].isArray());
  EXPECT_EQ(json1["data"]["items"].size(), 3);
  EXPECT_EQ(json1["data"]["items"][0].asInt(), 1);
  EXPECT_EQ(json1["data"]["items"][1].asInt(), 2);
  EXPECT_EQ(json1["data"]["items"][2].asInt(), 3);

  auto json2 = folly::parseJson(lines[1]);
  EXPECT_EQ(json2["id"].asInt(), 2);
  EXPECT_EQ(json2["data"]["count"].asInt(), 20);
  EXPECT_EQ(json2["data"]["items"].size(), 2);
  EXPECT_EQ(json2["data"]["items"][0].asInt(), 4);
  EXPECT_EQ(json2["data"]["items"][1].asInt(), 5);
}

TEST_F(OutputPrinterTest, JsonPrinterAllNumericTypes) {
  auto data = makeRowVector(
      {
          "tiny",
          "small",
          "normal",
          "big",
          "real_val",
          "double_val",
      },
      {
          makeFlatVector<int8_t>({1, -2}),
          makeFlatVector<int16_t>({100, -200}),
          makeFlatVector<int32_t>({10000, -20000}),
          makeFlatVector<int64_t>({1000000L, -2000000L}),
          makeFlatVector<float>({1.5f, -2.5f}),
          makeFlatVector<double>({99.99, -88.88}),
      });

  JsonPrinter printer;
  auto output = captureOutput([&]() { printer.printResults({data}, 100); });

  std::istringstream stream(output);
  std::string line;
  std::vector<std::string> lines;
  while (std::getline(stream, line)) {
    if (!line.empty()) {
      lines.push_back(line);
    }
  }

  auto json1 = folly::parseJson(lines[0]);
  EXPECT_EQ(json1["tiny"].asInt(), 1);
  EXPECT_EQ(json1["small"].asInt(), 100);
  EXPECT_EQ(json1["normal"].asInt(), 10000);
  EXPECT_EQ(json1["big"].asInt(), 1000000);
  EXPECT_FLOAT_EQ(json1["real_val"].asDouble(), 1.5f);
  EXPECT_DOUBLE_EQ(json1["double_val"].asDouble(), 99.99);

  auto json2 = folly::parseJson(lines[1]);
  EXPECT_EQ(json2["tiny"].asInt(), -2);
  EXPECT_EQ(json2["small"].asInt(), -200);
  EXPECT_EQ(json2["normal"].asInt(), -20000);
  EXPECT_EQ(json2["big"].asInt(), -2000000);
  EXPECT_FLOAT_EQ(json2["real_val"].asDouble(), -2.5f);
  EXPECT_DOUBLE_EQ(json2["double_val"].asDouble(), -88.88);
}

TEST_F(OutputPrinterTest, JsonPrinterDateType) {
  auto data = makeRowVector(
      {
          "id",
          "birth_date",
      },
      {
          makeFlatVector<int32_t>({1, 2}),
          makeFlatVector<int32_t>({19054, 19419}, DATE()),
      });

  JsonPrinter printer;
  auto output = captureOutput([&]() { printer.printResults({data}, 100); });

  std::istringstream stream(output);
  std::string line;
  std::vector<std::string> lines;
  while (std::getline(stream, line)) {
    if (!line.empty()) {
      lines.push_back(line);
    }
  }

  EXPECT_EQ(lines.size(), 2);

  auto json1 = folly::parseJson(lines[0]);
  EXPECT_EQ(json1["id"].asInt(), 1);
  EXPECT_TRUE(json1["birth_date"].isString());
  EXPECT_EQ(json1["birth_date"].asString(), "2022-03-03");

  auto json2 = folly::parseJson(lines[1]);
  EXPECT_EQ(json2["id"].asInt(), 2);
  EXPECT_TRUE(json2["birth_date"].isString());
  EXPECT_EQ(json2["birth_date"].asString(), "2023-03-03");
}

TEST_F(OutputPrinterTest, JsonPrinterTimestampType) {
  auto data = makeRowVector(
      {
          "id",
          "created_at",
      },
      {
          makeFlatVector<int32_t>({1, 2}),
          makeFlatVector<Timestamp>(
              {Timestamp(1672531200, 0), Timestamp(1704067200, 0)}),
      });

  JsonPrinter printer;
  auto output = captureOutput([&]() { printer.printResults({data}, 100); });

  std::istringstream stream(output);
  std::string line;
  std::vector<std::string> lines;
  while (std::getline(stream, line)) {
    if (!line.empty()) {
      lines.push_back(line);
    }
  }

  EXPECT_EQ(lines.size(), 2);

  auto json1 = folly::parseJson(lines[0]);
  EXPECT_EQ(json1["id"].asInt(), 1);
  EXPECT_TRUE(json1["created_at"].isString());

  auto json2 = folly::parseJson(lines[1]);
  EXPECT_EQ(json2["id"].asInt(), 2);
  EXPECT_TRUE(json2["created_at"].isString());
}

} // namespace
} // namespace axiom::sql
