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

#include <string>
#include <unordered_map>
#include <vector>

#include "axiom/logical_plan/LogicalPlanNode.h"
#include "axiom/spark_connect/third-party/protos/commands.grpc.pb.h" // @manual=fbcode//axiom/spark_connect/third-party/protos:spark_connect_proto-cpp
#include "axiom/spark_connect/third-party/protos/expressions.grpc.pb.h" // @manual=fbcode//axiom/spark_connect/third-party/protos:spark_connect_proto-cpp
#include "axiom/spark_connect/third-party/protos/relations.grpc.pb.h" // @manual=fbcode//axiom/spark_connect/third-party/protos:spark_connect_proto-cpp
#include "velox/type/Type.h"
#include "velox/type/Variant.h"

namespace axiom::spark_connect {

/// A series of conversion utilities between Spark Connect protobuf structures
/// and Axiom/Velox.

/// Convert Spark join types to Axiom join types
facebook::axiom::logical_plan::JoinType toAxiomJoinType(
    const spark::connect::Join::JoinType& joinType);

/// Convert Spark set operation types to Axiom set operation types
facebook::axiom::logical_plan::SetOperation toAxiomSetOperation(
    const spark::connect::SetOperation::SetOpType& setOpType,
    bool isAll = false);

/// Returns the Velox type for a particular Spark data type.
facebook::velox::TypePtr toVeloxType(const spark::connect::DataType& sparkType);

/// Returns the Velox type for a particular Spark literal.
facebook::velox::TypePtr toVeloxType(
    const spark::connect::Expression::Literal& literal);

/// Convert Spark literals to Velox variants
std::shared_ptr<facebook::velox::Variant> toVeloxVariant(
    const spark::connect::Expression::Literal& literal);

/// Convert JSON string type representation to Velox type
facebook::velox::TypePtr jsonToVeloxType(std::string_view jsonInput);

} // namespace axiom::spark_connect
