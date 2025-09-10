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

#include "axiom/optimizer/Model.h"
#include "velox/common/base/Exceptions.h"

#include <gtest/gtest.h>
#include <iostream>
#include <sstream>

using namespace facebook::velox;
using namespace facebook::axiom::optimizer;

class ModelTest : public testing::Test {
 protected:
  static float absv(float x) {
    return x > 0 ? x : -x;
  }

  static float weighted(float x1, float x2, float m1, float m2, float x) {
    float w1 = 1.0 / absv(x1 - x);
    float w2 = 1.0 / absv(x2 - x);
    return m1 * w1 + m2 * w2 / (w1 + w2);
  }

  struct Record {
    float expected;
    float estimate;
    float error;
    std::vector<float> point;
  };

  bool closeEnough(float x, float y, float expectRatio = 1.2) {
    float ratio = x / y;
    return ratio > 1 / expectRatio && ratio < expectRatio;
  }

  void expectClose(
      float expected,
      float estimate,
      float margin,
      const std::vector<float>& point) {
    EXPECT_TRUE(closeEnough(expected, estimate, margin))
        << "Expect " << expected << " got " << estimate << " at "
        << pointString(point);
  }

  void expectClose(
      float expected,
      Model& m,
      float margin,
      const std::vector<float>& point) {
    expectClose(expected, m.query(point), margin, point);
  }

  std::string pointString(const std::vector<float> point) {
    std::stringstream out;
    out << "{}";
    for (auto& c : point) {
      out << c << " ";
    }
    out << "}";
    return out.str();
  }

  void record(float expected, float estimate, std::vector<float> point) {
    float error =
        expected > estimate ? expected / estimate : estimate / expected;
    records_.push_back({expected, estimate, error, point});
  }

  void report() {
    auto copy = records_;
    std::sort(copy.begin(), copy.end(), [&](const Record& a, const Record& b) {
      return a.error > b.error;
    });
    for (auto i = 0; i < copy.size(); ++i) {
      if (copy[i].error < 1.01) {
        break;
      }
      std::cout << fmt::format(
          "{} {} {}: {}\n",
          copy[i].error,
          copy[i].estimate,
          copy[i].expected,
          pointString(copy[i].point));
    }
  }

  std::vector<Record> records_;
};

// Print a value in 'm' at coordinates in 'chars'. 'chars' is apce delimited
// floats.
FOLLY_NOINLINE float qs(Model* m, const char* chars) {
  std::vector<float> point;
  std::string str(chars);
  std::istringstream in(str);
  float f;
  while (in >> f) {
    point.push_back(f);
  }
  if (point.size() != m->rank()) {
    std::cout << "Expect " << m->rank() << " dims.\n";
    return -1;
  }
  f = m->query(point);
  std::cout << f << " at {";
  for (auto c : point) {
    std::cout << c << " ";
  }
  std::cout << "}\n";
  return f;
}

TEST_F(ModelTest, dim1) {
  Model m1(1);
  m1.insert({10}, 10);
  m1.insert({20}, 20);
  m1.insert({30}, 40);
  m1.precompute();
  EXPECT_THROW(m1.query({5}), VeloxException);
  EXPECT_EQ(15, m1.query({15}));
  EXPECT_EQ(20, m1.query({20}));
  EXPECT_EQ(30, m1.query({25}));
  EXPECT_EQ(50, m1.query({35}));
}

TEST_F(ModelTest, dim2) {
  Model m(2);
  m.insert({10, 1}, 10);
  m.insert({20, 1}, 20);
  m.insert({30, 1}, 40);
  m.insert({40, 1}, 100);

  m.insert({10, 2}, 12);
  m.insert({10, 3}, 14);
  m.insert({10, 5}, 20);
  m.precompute();

  // The values at x, 1 follow the x series.
  EXPECT_EQ(15, m.query({15, 1}));
  EXPECT_EQ(20, m.query({20, 1}));
  expectClose(30, m, 1.01, {25, 1});
  expectClose(70, m, 1.05, {35, 1});
  expectClose(74, m, 1.05, {35, 3});
  expectClose(108, m, 1.05, {35, 5});
}

float dim3Point(float i, float j, float k) {
  return (i + 1) + (j + 1) * 2 + (k + 1) * 3;
}

TEST_F(ModelTest, dim3) {
  Model m(3);
  for (float i = 0; i < 4; ++i) {
    for (float j = 0; j < 4; ++j) {
      for (float k = 0; k < 4; ++k) {
        m.insert({i, j, k}, dim3Point(i, j, k));
      }
    }
  }
  m.precompute();
  for (float i = 0; i < 6; ++i += 0.5) {
    for (float j = 0; j < 6; j += 0.5) {
      for (float k = 0; k < 6; k += 0.5) {
        auto expected = dim3Point(i, j, k);
        float estimate = m.query({i, j, k});
        record(expected, estimate, {i, j, k});
        expectClose(expected, estimate, 1.18, {i, j, k});
      }
    }
  }
  report();
}
