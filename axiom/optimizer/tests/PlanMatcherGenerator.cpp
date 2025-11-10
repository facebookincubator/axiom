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

#include "axiom/optimizer/tests/PlanMatcherGenerator.h"
#include <sstream>
#include <vector>
#include "velox/core/PlanNode.h"

namespace facebook::velox::core {

namespace {

/// Escapes a string for use in C++ string literals.
std::string escapeString(const std::string& str) {
  std::ostringstream oss;
  for (char c : str) {
    switch (c) {
      case '"':
        oss << "\\\"";
        break;
      case '\\':
        oss << "\\\\";
        break;
      case '\n':
        oss << "\\n";
        break;
      case '\t':
        oss << "\\t";
        break;
      default:
        oss << c;
    }
  }
  return oss.str();
}

/// Generates a vector literal from a vector of strings.
std::string generateVectorLiteral(const std::vector<std::string>& items) {
  if (items.empty()) {
    return "{}";
  }

  std::ostringstream oss;
  oss << "{";
  for (size_t i = 0; i < items.size(); ++i) {
    if (i > 0) {
      oss << ", ";
    }
    oss << "\"" << escapeString(items[i]) << "\"";
  }
  oss << "}";
  return oss.str();
}

/// Generates code for a TableScanNode.
std::string generateTableScanCode(const TableScanNode& node) {
  const auto& tableName = node.tableHandle()->name();

  // Get the column names and types

  std::ostringstream oss;
  oss << ".tableScan(\"" << escapeString(tableName) << "\")";

  return oss.str();
}

/// Generates code for a ValuesNode.
std::string generateValuesCode(const ValuesNode& node) {
  return ".values()";
}

/// Generates code for a FilterNode.
std::string generateFilterCode(const FilterNode& node) {
  const auto& predicate = node.filter();
  std::ostringstream oss;
  oss << ".filter(\"" << escapeString(predicate->toString()) << "\")";
  return oss.str();
}

/// Generates code for a ProjectNode.
std::string generateProjectCode(const ProjectNode& node) {
  const auto& projections = node.projections();
  const auto& names = node.names();

  std::vector<std::string> expressions;
  for (size_t i = 0; i < projections.size(); ++i) {
    std::string exprStr = projections[i]->toString();
    // Add alias if the name differs from the expression
    if (names[i] != exprStr) {
      exprStr += " AS " + names[i];
    }
    expressions.push_back(exprStr);
  }

  std::ostringstream oss;
  oss << ".project(" << generateVectorLiteral(expressions) << ")";
  return oss.str();
}

/// Generates code for a ParallelProjectNode.
std::string generateParallelProjectCode(const ParallelProjectNode& node) {
  const auto& projections = node.projections();
  const auto& names = node.names();

  std::vector<std::string> expressions;
  for (size_t i = 0; i < projections.size(); ++i) {
    std::string exprStr = projections[i]->toString();
    if (names[i] != exprStr) {
      exprStr += " AS " + names[i];
    }
    expressions.push_back(exprStr);
  }

  std::ostringstream oss;
  oss << ".parallelProject(" << generateVectorLiteral(expressions) << ")";
  return oss.str();
}

/// Generates code for an AggregationNode.
std::string generateAggregationCode(const AggregationNode& node) {
  const auto& groupingKeys = node.groupingKeys();
  const auto& aggregates = node.aggregates();
  const auto& aggregateNames = node.aggregateNames();
  const auto step = node.step();

  std::vector<std::string> groupingKeyStrs;
  for (const auto& key : groupingKeys) {
    groupingKeyStrs.push_back(key->toString());
  }

  std::vector<std::string> aggregateStrs;
  for (size_t i = 0; i < aggregates.size(); ++i) {
    std::string aggStr = aggregates[i].call->toString();
    if (aggregateNames[i] != aggStr) {
      aggStr += " AS " + aggregateNames[i];
    }
    aggregateStrs.push_back(aggStr);
  }

  std::ostringstream oss;

  // Determine the aggregation type
  std::string stepMethod;
  switch (step) {
    case AggregationNode::Step::kSingle:
      stepMethod = "singleAggregation";
      break;
    case AggregationNode::Step::kPartial:
      stepMethod = "partialAggregation";
      break;
    case AggregationNode::Step::kFinal:
      stepMethod = "finalAggregation";
      break;
    case AggregationNode::Step::kIntermediate:
      stepMethod = "aggregation"; // Use generic for intermediate
      break;
  }

  oss << "." << stepMethod << "(";
  oss << generateVectorLiteral(groupingKeyStrs);
  oss << ", ";
  oss << generateVectorLiteral(aggregateStrs);
  oss << ")";

  return oss.str();
}

/// Generates code for an UnnestNode.
std::string generateUnnestCode(const UnnestNode& node) {
  const auto& replicateVars = node.replicateVariables();
  const auto& unnestVars = node.unnestVariables();

  std::vector<std::string> replicateStrs;
  for (const auto& var : replicateVars) {
    replicateStrs.push_back(var->toString());
  }

  std::vector<std::string> unnestStrs;
  for (const auto& var : unnestVars) {
    unnestStrs.push_back(var->toString());
  }

  std::ostringstream oss;
  oss << ".unnest(";
  oss << generateVectorLiteral(replicateStrs);
  oss << ", ";
  oss << generateVectorLiteral(unnestStrs);
  oss << ")";

  return oss.str();
}

/// Generates code for a LimitNode.
std::string generateLimitCode(const LimitNode& node) {
  const auto offset = node.offset();
  const auto count = node.count();
  const auto isPartial = node.isPartial();

  std::ostringstream oss;
  if (isPartial) {
    oss << ".partialLimit(" << offset << ", " << count << ")";
  } else {
    oss << ".finalLimit(" << offset << ", " << count << ")";
  }

  return oss.str();
}

/// Generates code for a TopNNode.
std::string generateTopNCode(const TopNNode& node) {
  const auto count = node.count();

  std::ostringstream oss;
  oss << ".topN(" << count << ")";

  return oss.str();
}

/// Generates code for an OrderByNode.
std::string generateOrderByCode(const OrderByNode& node) {
  const auto& sortingKeys = node.sortingKeys();
  const auto& sortingOrders = node.sortingOrders();

  std::vector<std::string> orderingStrs;
  for (size_t i = 0; i < sortingKeys.size(); ++i) {
    std::ostringstream oss;
    oss << sortingKeys[i]->toString();
    oss << " " << (sortingOrders[i].isAscending() ? "ASC" : "DESC");
    oss << " NULLS " << (sortingOrders[i].isNullsFirst() ? "FIRST" : "LAST");
    orderingStrs.push_back(oss.str());
  }

  std::ostringstream oss;
  oss << ".orderBy(" << generateVectorLiteral(orderingStrs) << ")";

  return oss.str();
}

/// Generates code for a LocalPartitionNode.
std::string generateLocalPartitionCode(const LocalPartitionNode& node) {
  return ".localPartition()";
}

/// Generates code for a LocalMergeNode.
std::string generateLocalMergeCode(const LocalMergeNode& node) {
  return ".localMerge()";
}

/// Generates code for a PartitionedOutputNode.
std::string generatePartitionedOutputCode(const PartitionedOutputNode& node) {
  return ".partitionedOutput()";
}

/// Generates code for an ExchangeNode.
std::string generateExchangeCode(const ExchangeNode& node) {
  return ".exchange()";
}

/// Generates code for a MergeExchangeNode.
std::string generateMergeExchangeCode(const MergeExchangeNode& node) {
  return ".mergeExchange()";
}

/// Generates code for a TableWriteNode.
std::string generateTableWriteCode(const TableWriteNode& node) {
  return ".tableWrite()";
}

/// Forward declarations
std::string generatePlanMatcherCodeImpl(const PlanNodePtr& planNode);
void generateJoinMatchers(
    const PlanNodePtr& planNode,
    std::vector<std::string>& matchers,
    int& matcherCounter);

/// Generates the matcher variable name for a join side.
std::string getJoinMatcherVarName(int counter) {
  if (counter == 0) {
    return "rightMatcher";
  }
  return "rightMatcher" + std::to_string(counter);
}

/// Collects all join right-side matchers in the tree.
void generateJoinMatchers(
    const PlanNodePtr& planNode,
    std::vector<std::string>& matchers,
    int& matcherCounter) {
  if (auto* joinNode = dynamic_cast<const HashJoinNode*>(planNode.get())) {
    VELOX_CHECK_EQ(
        joinNode->sources().size(), 2, "HashJoinNode must have 2 sources");

    // Recursively process left side for nested joins
    generateJoinMatchers(joinNode->sources()[0], matchers, matcherCounter);

    // Generate matcher for the right side
    const auto& rightSource = joinNode->sources()[1];

    // First, collect any nested joins on the right side
    generateJoinMatchers(rightSource, matchers, matcherCounter);

    // Then generate the matcher for this right side
    std::string rightMatcherCode = generatePlanMatcherCodeImpl(rightSource);
    std::string matcherVar = getJoinMatcherVarName(matcherCounter++);

    std::ostringstream oss;
    oss << "auto " << matcherVar << " = core::PlanMatcherBuilder()";
    oss << rightMatcherCode;
    oss << ".build();";

    matchers.push_back(oss.str());
  } else {
    // Recursively process sources
    for (const auto& source : planNode->sources()) {
      generateJoinMatchers(source, matchers, matcherCounter);
    }
  }
}

/// Generates code for a HashJoinNode (inline call only, not the right matcher).
std::string generateHashJoinCode(const HashJoinNode& node, int matcherIndex) {
  std::string matcherVar = getJoinMatcherVarName(matcherIndex);

  const auto joinType = node.joinType();
  std::ostringstream oss;
  oss << ".hashJoin(" << matcherVar << ", velox::core::JoinType::";

  // Map JoinType to its enum name
  switch (joinType) {
    case JoinType::kInner:
      oss << "kInner";
      break;
    case JoinType::kLeft:
      oss << "kLeft";
      break;
    case JoinType::kRight:
      oss << "kRight";
      break;
    case JoinType::kFull:
      oss << "kFull";
      break;
    case JoinType::kLeftSemiProject:
      oss << "kLeftSemiProject";
      break;
    case JoinType::kRightSemiProject:
      oss << "kRightSemiProject";
      break;
    case JoinType::kLeftSemiFilter:
      oss << "kLeftSemiFilter";
      break;
    case JoinType::kRightSemiFilter:
      oss << "kRightSemiFilter";
      break;
    case JoinType::kAnti:
      oss << "kAnti";
      break;
    default:
      oss << "kInner"; // Default fallback
  }

  oss << ")";

  return oss.str();
}

/// Helper to count joins in a tree (to track matcher indices).
int countJoins(const PlanNodePtr& planNode, int currentIndex = 0) {
  int index = currentIndex;
  if (dynamic_cast<const HashJoinNode*>(planNode.get())) {
    index++;
  }
  for (const auto& source : planNode->sources()) {
    index = countJoins(source, index);
  }
  return index;
}

/// Helper to find the matcher index for a specific join node.
int findJoinMatcherIndex(
    const PlanNodePtr& root,
    const PlanNodePtr& targetJoin,
    int& currentIndex) {
  if (auto* joinNode = dynamic_cast<const HashJoinNode*>(root.get())) {
    if (root == targetJoin) {
      return currentIndex++;
    }
    // Process left side first
    int leftIndex =
        findJoinMatcherIndex(joinNode->sources()[0], targetJoin, currentIndex);
    if (leftIndex >= 0) {
      return leftIndex;
    }
    // Then increment for this join
    currentIndex++;
    // Then process right side
    return findJoinMatcherIndex(
        joinNode->sources()[1], targetJoin, currentIndex);
  } else {
    for (const auto& source : root->sources()) {
      int result = findJoinMatcherIndex(source, targetJoin, currentIndex);
      if (result >= 0) {
        return result;
      }
    }
  }
  return -1;
}

// Thread-local counter for tracking join matcher indices
thread_local int g_joinMatcherIndex = 0;

/// Resets the join matcher index counter
void resetJoinMatcherIndex() {
  g_joinMatcherIndex = 0;
}

/// Recursive implementation of generatePlanMatcherCode
std::string generatePlanMatcherCodeImpl(const PlanNodePtr& planNode) {
  std::ostringstream result;

  // Process sources first (post-order traversal for non-join nodes)
  const auto& sources = planNode->sources();

  // Special handling for joins which have two sources
  if (auto* joinNode = dynamic_cast<const HashJoinNode*>(planNode.get())) {
    // For joins, we generate the left source inline
    // The right source matcher is generated separately
    if (!sources.empty()) {
      result << generatePlanMatcherCodeImpl(sources[0]);
    }
    result << generateHashJoinCode(*joinNode, g_joinMatcherIndex++);
  } else {
    // For non-join nodes, process the first source recursively
    if (!sources.empty()) {
      result << generatePlanMatcherCodeImpl(sources[0]);
    }

    // Generate code for the current node
    if (auto* tableScan = dynamic_cast<const TableScanNode*>(planNode.get())) {
      result << generateTableScanCode(*tableScan);
    } else if (auto* values = dynamic_cast<const ValuesNode*>(planNode.get())) {
      result << generateValuesCode(*values);
    } else if (auto* filter = dynamic_cast<const FilterNode*>(planNode.get())) {
      result << generateFilterCode(*filter);
    } else if (
        auto* parallelProject =
            dynamic_cast<const ParallelProjectNode*>(planNode.get())) {
      result << generateParallelProjectCode(*parallelProject);
    } else if (
        auto* project = dynamic_cast<const ProjectNode*>(planNode.get())) {
      result << generateProjectCode(*project);
    } else if (
        auto* agg = dynamic_cast<const AggregationNode*>(planNode.get())) {
      result << generateAggregationCode(*agg);
    } else if (auto* unnest = dynamic_cast<const UnnestNode*>(planNode.get())) {
      result << generateUnnestCode(*unnest);
    } else if (auto* limit = dynamic_cast<const LimitNode*>(planNode.get())) {
      result << generateLimitCode(*limit);
    } else if (auto* topN = dynamic_cast<const TopNNode*>(planNode.get())) {
      result << generateTopNCode(*topN);
    } else if (
        auto* orderBy = dynamic_cast<const OrderByNode*>(planNode.get())) {
      result << generateOrderByCode(*orderBy);
    } else if (
        auto* localPartition =
            dynamic_cast<const LocalPartitionNode*>(planNode.get())) {
      result << generateLocalPartitionCode(*localPartition);
    } else if (
        auto* localMerge =
            dynamic_cast<const LocalMergeNode*>(planNode.get())) {
      result << generateLocalMergeCode(*localMerge);
    } else if (
        auto* partitionedOutput =
            dynamic_cast<const PartitionedOutputNode*>(planNode.get())) {
      result << generatePartitionedOutputCode(*partitionedOutput);
    } else if (
        auto* mergeExchange =
            dynamic_cast<const MergeExchangeNode*>(planNode.get())) {
      result << generateMergeExchangeCode(*mergeExchange);
    } else if (
        auto* exchange = dynamic_cast<const ExchangeNode*>(planNode.get())) {
      result << generateExchangeCode(*exchange);
    } else if (
        auto* tableWrite =
            dynamic_cast<const TableWriteNode*>(planNode.get())) {
      result << generateTableWriteCode(*tableWrite);
    } else {
      // For unknown node types, add a comment
      result << "  // Unknown node type: "
             << folly::demangle(typeid(*planNode).name());
    }
  }

  return result.str();
}

} // namespace

std::string generatePlanMatcherCode(
    const PlanNodePtr& planNode,
    const std::string& builderVarName) {
  std::ostringstream oss;

  // Reset the join matcher index counter
  resetJoinMatcherIndex();

  // First, collect all join right-side matchers
  std::vector<std::string> joinMatchers;
  int matcherCounter = 0;
  generateJoinMatchers(planNode, joinMatchers, matcherCounter);

  // Reset again for the main matcher generation
  resetJoinMatcherIndex();

  // Generate the join matchers first
  for (const auto& matcher : joinMatchers) {
    oss << matcher << "\n\n";
  }

  // Then generate the main matcher
  oss << "auto " << builderVarName << " = core::PlanMatcherBuilder()";
  oss << generatePlanMatcherCodeImpl(planNode);
  oss << ".build();\n";

  return oss.str();
}

} // namespace facebook::velox::core
