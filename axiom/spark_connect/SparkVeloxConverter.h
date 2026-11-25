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

#pragma once

#include <memory>
#include <string_view>

#include "axiom/spark_connect/third-party/protos/expressions.pb.h" // @manual=fbcode//axiom/spark_connect/third-party/protos:spark_connect_proto-cpp
#include "axiom/spark_connect/third-party/protos/types.pb.h" // @manual=fbcode//axiom/spark_connect/third-party/protos:spark_connect_proto-cpp
#include "velox/type/Type.h"
#include "velox/type/Variant.h"

namespace axiom::spark_connect {

/// Converts types and values between Spark Connect and Velox.
class SparkVeloxConverter {
 public:
  /// Returns the Velox type for a Spark data type.
  static facebook::velox::TypePtr toVeloxType(
      const spark::connect::DataType& type);

  /// Returns the Velox type of a Spark literal.
  static facebook::velox::TypePtr toVeloxType(
      const spark::connect::Expression::Literal& literal);

  /// Returns the value of a Spark literal.
  static std::shared_ptr<facebook::velox::Variant> toVariant(
      const spark::connect::Expression::Literal& literal);

  /// Returns the Velox type for the JSON form of a Spark data type, the form a
  /// local relation's schema takes.
  static facebook::velox::TypePtr parseJsonType(std::string_view json);

  /// Returns the Spark type for a Velox type. Velox types carry no
  /// nullability, so every field, element and map value is nullable.
  static spark::connect::DataType toSparkType(
      const facebook::velox::TypePtr& type);
};

} // namespace axiom::spark_connect
