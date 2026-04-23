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

#include "axiom/pyspark/SparkVeloxConverter.h"
#include <folly/String.h>
#include <folly/json.h>
#include <string>
#include "axiom/pyspark/Exception.h"

using namespace facebook;

namespace axiom::collagen {

std::string toVeloxFunctionName(const std::string& sparkFunctionName) {
  static std::unordered_map<std::string, std::string> functionMap = {
      // Arithmetic operators
      {"+", "plus"},
      {"-", "minus"},
      {"*", "multiply"},
      {"/", "divide"},
      // Comparison operators
      {"==", "eq"},
      // Arithmetic operators (additional)
      {"%", "mod"},
      {">", "gt"},
      {"<", "lt"},
      {">=", "gte"},
      {"<=", "lte"},
      // Higher-order array/map functions (Spark SQL -> Velox/Presto names)
      {"exists", "any_match"},
      {"forall", "all_match"},
      {"aggregate", "reduce"},
      // Array/collection functions
      {"size", "cardinality"},
      {"array_repeat", "repeat"},
      // Null checking functions
      {"isnull", "is_null"},
  };

  auto it = functionMap.find(sparkFunctionName);
  if (it != functionMap.end()) {
    return it->second;
  }
  return sparkFunctionName;
}

facebook::axiom::logical_plan::JoinType toAxiomJoinType(
    const spark::connect::Join::JoinType& joinType) {
  switch (joinType) {
    case spark::connect::Join::JOIN_TYPE_INNER:
      return facebook::axiom::logical_plan::JoinType::kInner;

    case spark::connect::Join::JOIN_TYPE_LEFT_OUTER:
      return facebook::axiom::logical_plan::JoinType::kLeft;

    case spark::connect::Join::JOIN_TYPE_RIGHT_OUTER:
      return facebook::axiom::logical_plan::JoinType::kRight;

    case spark::connect::Join::JOIN_TYPE_FULL_OUTER:
      return facebook::axiom::logical_plan::JoinType::kFull;

    default:
      COLLAGEN_NYI("Unable to convert join type: {}", joinType);
  }
}

facebook::axiom::logical_plan::SetOperation toAxiomSetOperation(
    const spark::connect::SetOperation::SetOpType& setOpType,
    bool isAll) {
  switch (setOpType) {
    case spark::connect::SetOperation::SET_OP_TYPE_UNION:
      return isAll ? facebook::axiom::logical_plan::SetOperation::kUnionAll
                   : facebook::axiom::logical_plan::SetOperation::kUnion;

    case spark::connect::SetOperation::SET_OP_TYPE_INTERSECT:
      return facebook::axiom::logical_plan::SetOperation::kIntersect;

    case spark::connect::SetOperation::SET_OP_TYPE_EXCEPT:
      return facebook::axiom::logical_plan::SetOperation::kExcept;

    default:
      COLLAGEN_NYI("Unable to convert set operation type: {}", setOpType);
  }
}

velox::TypePtr toVeloxType(const spark::connect::DataType& sparkType) {
  static std::unordered_map<int32_t, velox::TypePtr> sparkToVeloxType = {
      {spark::connect::DataType::kNull, velox::UNKNOWN()},
      {spark::connect::DataType::kBoolean, velox::BOOLEAN()},
      {spark::connect::DataType::kByte, velox::TINYINT()},
      {spark::connect::DataType::kShort, velox::SMALLINT()},
      {spark::connect::DataType::kInteger, velox::INTEGER()},
      {spark::connect::DataType::kLong, velox::BIGINT()},
      {spark::connect::DataType::kFloat, velox::REAL()},
      {spark::connect::DataType::kDouble, velox::DOUBLE()},
      {spark::connect::DataType::kBinary, velox::VARBINARY()},
      {spark::connect::DataType::kString, velox::VARCHAR()},
      {spark::connect::DataType::kDate, velox::DATE()},
      {spark::connect::DataType::kTimestamp, velox::TIMESTAMP()},
  };
  auto it = sparkToVeloxType.find(sparkType.kind_case());
  if (it != sparkToVeloxType.end()) {
    return it->second;
  }

  switch (sparkType.kind_case()) {
    default:
      COLLAGEN_NYI(
          "Data type conversion not implemented yet for '{}'",
          sparkType.kind_case());
  }
}

// Returns the Velox type for a particular Spark literal.
velox::TypePtr toVeloxType(const spark::connect::Expression::Literal& literal) {
  static std::unordered_map<int32_t, velox::TypePtr> literalToType = {
      {spark::connect::Expression::Literal::kNull, velox::UNKNOWN()},
      {spark::connect::Expression::Literal::kBoolean, velox::BOOLEAN()},
      {spark::connect::Expression::Literal::kByte, velox::TINYINT()},
      {spark::connect::Expression::Literal::kShort, velox::SMALLINT()},
      {spark::connect::Expression::Literal::kInteger, velox::INTEGER()},
      {spark::connect::Expression::Literal::kLong, velox::BIGINT()},
      {spark::connect::Expression::Literal::kFloat, velox::REAL()},
      {spark::connect::Expression::Literal::kDouble, velox::DOUBLE()},
      {spark::connect::Expression::Literal::kBinary, velox::VARBINARY()},
      {spark::connect::Expression::Literal::kString, velox::VARCHAR()},
      {spark::connect::Expression::Literal::kDate, velox::DATE()},
      {spark::connect::Expression::Literal::kTimestamp, velox::TIMESTAMP()},
  };
  auto it = literalToType.find(literal.literal_type_case());
  if (it != literalToType.end()) {
    return it->second;
  }

  // Special cases:
  switch (literal.literal_type_case()) {
    // There is no map literal, as it seems.
    case spark::connect::Expression::Literal::kArray:
      return velox::ARRAY(toVeloxType(literal.array().element_type()));

    // Returns the type of the null.
    case spark::connect::Expression::Literal::kNull:
      return toVeloxType(literal.null());

    default:
      COLLAGEN_NYI(
          "Literal type conversion not implemented yet for '{}'",
          literal.literal_type_case());
  }
}

std::shared_ptr<velox::Variant> toVeloxVariant(
    const spark::connect::Expression::Literal& literal) {
  switch (literal.literal_type_case()) {
    case spark::connect::Expression::Literal::kNull:
      return std::make_shared<velox::Variant>(
          velox::Variant::null(toVeloxType(literal.null())->kind()));

    case spark::connect::Expression::Literal::kBinary:
      return std::make_shared<velox::Variant>(
          velox::Variant::binary(literal.binary()));

    case spark::connect::Expression::Literal::kBoolean:
      return std::make_shared<velox::Variant>(literal.boolean());

    case spark::connect::Expression::Literal::kByte:
      return std::make_shared<velox::Variant>(literal.byte());

    case spark::connect::Expression::Literal::kShort:
      return std::make_shared<velox::Variant>(literal.short_());

    case spark::connect::Expression::Literal::kInteger:
      return std::make_shared<velox::Variant>(literal.integer());

    case spark::connect::Expression::Literal::kLong:
      return std::make_shared<velox::Variant>(literal.long_());

    case spark::connect::Expression::Literal::kFloat:
      return std::make_shared<velox::Variant>(literal.float_());

    case spark::connect::Expression::Literal::kDouble:
      return std::make_shared<velox::Variant>(literal.double_());

    case spark::connect::Expression::Literal::kString:
      return std::make_shared<velox::Variant>(literal.string());

    case spark::connect::Expression::Literal::kDate:
      return std::make_shared<velox::Variant>(literal.date());

    case spark::connect::Expression::Literal::kTimestamp:
      return std::make_shared<velox::Variant>(literal.timestamp());

    default:
      COLLAGEN_NYI(
          "Literal type to variant not implemented yet for '{}'",
          literal.literal_type_case());
  }
}

namespace {

// This is a simple example of a json type created by Spark Connect:
// {
//  "fields":
//   [
//     {"metadata":{},"name":"Name","nullable":true,"type":"string"},
//     {"metadata":{},"name":"Age","nullable":true,"type":"long"},
//     {"metadata":{},"name":"List","nullable":true,"type":
//        {"containsNull":true,"elementType":"string","type":"array"}
//     }
//   ],
//  "type":"struct"
// }
//
// The format is a little wonky so it requires a bit more care in the recursion
// for complex types.
velox::TypePtr jsonToVeloxType(const folly::dynamic& obj) {
  // "leaves" are primitive type.
  if (obj.isString()) {
    const auto& sparkType = obj.asString();

    if (sparkType == "string") {
      return velox::VARCHAR();
    } else if (sparkType == "binary") {
      return velox::VARBINARY();
    } else if (sparkType == "byte") {
      return velox::TINYINT();
    } else if (sparkType == "short") {
      return velox::SMALLINT();
    } else if (sparkType == "integer") {
      return velox::INTEGER();
    } else if (sparkType == "long") {
      return velox::BIGINT();
    } else if (sparkType == "float") {
      return velox::REAL();
    } else if (sparkType == "double") {
      return velox::DOUBLE();
    } else if (sparkType == "boolean") {
      return velox::BOOLEAN();
    } else if (sparkType == "timestamp") {
      return velox::TIMESTAMP();
    } else if (sparkType == "date") {
      return velox::DATE();
    }
  }
  // Complex types are handled differently. Rows have a string description as
  // the "type" attributes, while arrays and maps have objects.
  else if (obj.isObject()) {
    if (obj["type"].isString()) {
      const auto& sparkType = obj["type"].asString();

      if (sparkType == "struct") {
        std::vector<velox::TypePtr> types;
        std::vector<std::string> names;

        for (const auto& child : obj["fields"]) {
          types.push_back(jsonToVeloxType(child));
          names.push_back(child["name"].asString());
        }
        return velox::ROW(std::move(names), std::move(types));
      } else if (sparkType == "array") {
        return velox::ARRAY(jsonToVeloxType(obj["elementType"]));
      } else if (sparkType == "map") {
        return velox::MAP(
            jsonToVeloxType(obj["keyType"]), jsonToVeloxType(obj["valueType"]));
      } else {
        return jsonToVeloxType(obj["type"]);
      }
    } else if (obj["type"].isObject()) {
      return jsonToVeloxType(obj["type"]);
    }
  }
  COLLAGEN_FAIL("Unexpected Spark json type: '{}'", folly::toJson(obj));
}

} // namespace

velox::TypePtr jsonToVeloxType(std::string_view jsonInput) {
  return jsonToVeloxType(folly::parseJson(jsonInput));
}

} // namespace axiom::collagen
