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

#include "axiom/pyspark/SparkVeloxConverter.h"
#include "velox/type/Type.h"

using namespace facebook;

namespace axiom::collagen::test {
namespace {

class SparkVeloxConverterTest : public ::testing::Test {};

TEST_F(SparkVeloxConverterTest, toVeloxTypeFromDataType) {
  // Create and test primitive types
  spark::connect::DataType dataType;

  dataType.mutable_boolean();
  EXPECT_EQ(velox::BOOLEAN(), toVeloxType(dataType));

  dataType.mutable_byte();
  EXPECT_EQ(velox::TINYINT(), toVeloxType(dataType));

  dataType.mutable_short_();
  EXPECT_EQ(velox::SMALLINT(), toVeloxType(dataType));

  dataType.mutable_integer();
  EXPECT_EQ(velox::INTEGER(), toVeloxType(dataType));

  dataType.mutable_long_();
  EXPECT_EQ(velox::BIGINT(), toVeloxType(dataType));

  dataType.mutable_double_();
  EXPECT_EQ(velox::DOUBLE(), toVeloxType(dataType));

  dataType.mutable_float_();
  EXPECT_EQ(velox::REAL(), toVeloxType(dataType));

  dataType.mutable_string();
  EXPECT_EQ(velox::VARCHAR(), toVeloxType(dataType));

  dataType.mutable_date();
  EXPECT_EQ(velox::DATE(), toVeloxType(dataType));

  dataType.mutable_timestamp();
  EXPECT_EQ(velox::TIMESTAMP(), toVeloxType(dataType));

  dataType.mutable_binary();
  EXPECT_EQ(velox::VARBINARY(), toVeloxType(dataType));

  dataType.mutable_null();
  EXPECT_EQ(velox::UNKNOWN(), toVeloxType(dataType));
}

TEST_F(SparkVeloxConverterTest, toVeloxTypeFromLiteral) {
  // Test boolean literal
  spark::connect::Expression::Literal boolLiteral;
  boolLiteral.set_boolean(true);
  EXPECT_EQ(velox::BOOLEAN(), toVeloxType(boolLiteral));

  // Test integer literal
  spark::connect::Expression::Literal intLiteral;
  intLiteral.set_integer(42);
  EXPECT_EQ(velox::INTEGER(), toVeloxType(intLiteral));

  // Test long literal
  spark::connect::Expression::Literal longLiteral;
  longLiteral.set_long_(42L);
  EXPECT_EQ(velox::BIGINT(), toVeloxType(longLiteral));

  // Test double literal
  spark::connect::Expression::Literal doubleLiteral;
  doubleLiteral.set_double_(3.14);
  EXPECT_EQ(velox::DOUBLE(), toVeloxType(doubleLiteral));

  // Test string literal
  spark::connect::Expression::Literal stringLiteral;
  stringLiteral.set_string("hello");
  EXPECT_EQ(velox::VARCHAR(), toVeloxType(stringLiteral));

  // Test binary literal
  spark::connect::Expression::Literal binaryLiteral;
  binaryLiteral.set_binary("binary data");
  EXPECT_EQ(velox::VARBINARY(), toVeloxType(binaryLiteral));

  // Test null literal
  spark::connect::Expression::Literal nullLiteral;
  nullLiteral.mutable_null();
  EXPECT_EQ(velox::UNKNOWN(), toVeloxType(nullLiteral));
}

TEST_F(SparkVeloxConverterTest, toVeloxVariant) {
  spark::connect::Expression::Literal literal;

  literal.set_boolean(true);
  EXPECT_EQ(toVeloxVariant(literal)->kind(), velox::TypeKind::BOOLEAN);

  literal.set_integer(42);
  EXPECT_EQ(toVeloxVariant(literal)->kind(), velox::TypeKind::INTEGER);

  literal.set_float_(0.99);
  EXPECT_EQ(toVeloxVariant(literal)->kind(), velox::TypeKind::REAL);

  literal.set_string("hello world");
  EXPECT_EQ(toVeloxVariant(literal)->kind(), velox::TypeKind::VARCHAR);

  // Variants operate on the pysical type layer, so they don't understand
  // logical types.
  literal.set_date(0);
  EXPECT_EQ(toVeloxVariant(literal)->kind(), velox::TypeKind::INTEGER);
}

TEST_F(SparkVeloxConverterTest, jsonToVeloxType) {
  // Test simple primitive type.
  EXPECT_EQ(velox::VARCHAR(), jsonToVeloxType("\"string\""));
  EXPECT_EQ(velox::INTEGER(), jsonToVeloxType("\"integer\""));
  EXPECT_EQ(velox::BIGINT(), jsonToVeloxType("\"long\""));

  // Test struct type.
  std::string structJson = R"({
    "type": "struct",
    "fields": [
      {"name": "name", "type": "string"},
      {"name": "age", "type": "integer"},
      {"name": "active", "type": "boolean"}
    ]
  })";

  auto structType = jsonToVeloxType(structJson);
  EXPECT_TRUE(structType->isRow());
  const auto& rowType = structType->asRow();
  EXPECT_EQ(3, rowType.size());
  EXPECT_EQ("name", rowType.nameOf(0));
  EXPECT_EQ("age", rowType.nameOf(1));
  EXPECT_EQ("active", rowType.nameOf(2));
  EXPECT_EQ(velox::VARCHAR(), rowType.childAt(0));
  EXPECT_EQ(velox::INTEGER(), rowType.childAt(1));
  EXPECT_EQ(velox::BOOLEAN(), rowType.childAt(2));

  // Test array type.
  std::string arrayJson = R"({
    "type": "array",
    "elementType": "string",
    "containsNull": true
  })";

  auto arrayType = jsonToVeloxType(arrayJson);
  EXPECT_TRUE(arrayType->isArray());
  EXPECT_EQ(velox::VARCHAR(), arrayType->asArray().elementType());

  // Test map type.
  std::string mapJson = R"({
    "type": "map",
    "keyType": "string",
    "valueType": "integer"
  })";

  auto mapType = jsonToVeloxType(mapJson);
  EXPECT_TRUE(mapType->isMap());
  EXPECT_EQ(velox::VARCHAR(), mapType->asMap().keyType());
  EXPECT_EQ(velox::INTEGER(), mapType->asMap().valueType());
}

TEST_F(SparkVeloxConverterTest, aggregateMapsToReduce) {
  EXPECT_EQ(toVeloxFunctionName("aggregate"), "reduce");
}

} // namespace
} // namespace axiom::collagen::test
