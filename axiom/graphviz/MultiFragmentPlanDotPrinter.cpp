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

#include "axiom/graphviz/MultiFragmentPlanDotPrinter.h"
#include <fmt/core.h>
#include <fmt/format.h>
#include <folly/String.h>
#include <folly/container/F14Map.h>
#include <locale>
#include "axiom/graphviz/Html.h"
#include "axiom/graphviz/Palette.h"
#include "velox/core/PlanNode.h"

namespace facebook::axiom::graphviz {

using optimizer::ExecutableFragment;
using optimizer::FragmentTypeName;
using optimizer::MultiFragmentPlan;
using optimizer::NodePredictionMap;

namespace {

// Rendering primitives. Each takes content as a string plus the ostream and
// emits one piece of HTML — no domain knowledge.

void printTableStart(std::ostream& out, int32_t fragmentIndex) {
  out << "  fragment_" << fragmentIndex << " [shape=none, margin=0, label=<\n";
  out << "    <TABLE BORDER=\"1\" CELLBORDER=\"0\" CELLSPACING=\"0\" "
         "CELLPADDING=\"8\" COLOR=\""
      << kPalette.text << "\">\n";
}

void printTableEnd(std::ostream& out) {
  out << "    </TABLE>\n";
  out << "  >];\n";
}

// Emits one HTML table row. 'color' may be empty (no BGCOLOR). 'bold'
// wraps the content in <B>..</B>.
void printRow(
    std::ostream& out,
    std::string_view content,
    std::string_view color,
    bool bold) {
  out << "      <TR><TD ALIGN=\"LEFT\"";
  if (!color.empty()) {
    out << " BGCOLOR=\"" << color << "\"";
  }
  out << ">";
  if (bold) {
    out << "<B>";
  }
  out << content;
  if (bold) {
    out << "</B>";
  }
  out << "</TD></TR>\n";
}

// Logic helpers — pure: read fragment / plan node fields, return strings.

// Returns a human-readable name for a partition key expression. Field
// references are rendered by their column name; other expressions fall back
// to toString().
std::string formatPartitionKey(const velox::core::TypedExprPtr& key) {
  if (auto field =
          std::dynamic_pointer_cast<const velox::core::FieldAccessTypedExpr>(
              key)) {
    return field->name();
  }
  return key->toString();
}

// Output distribution rendering: a kind ("broadcast" / "hash" / "gather" /
// "arbitrary") and a full HTML-ready label that may include keys
// (e.g. "hash[k1, k2]").
struct OutputDistribution {
  std::string kind;
  std::string label;
};

// Locale that adds comma thousands separators independent of the host
// system's installed locales. Constructed once on first use.
const std::locale& commaLocale() {
  struct CommaGrouping : std::numpunct<char> {
    char do_thousands_sep() const override {
      return ',';
    }
    std::string do_grouping() const override {
      return "\3";
    }
  };
  static const std::locale kLocale(std::locale::classic(), new CommaGrouping);
  return kLocale;
}

// Trailing invisible padding for the "out:" cell. Graphviz under-measures
// HTML-table text in proportional fonts, so without padding long
// "hash[...]" lists overflow the cell border. The non-breaking spaces force
// a wider width budget without changing the visible content.
constexpr std::string_view kCellPadding = "&nbsp;&nbsp;&nbsp;&nbsp;";

// Formats the output distribution of a fragment from its root plan node.
// Returns empty fields when the root is not a PartitionedOutputNode (i.e.
// the final fragment delivers results in-process).
OutputDistribution outputDistribution(const velox::core::PlanNodePtr& root) {
  const auto* partitionedOutput =
      dynamic_cast<const velox::core::PartitionedOutputNode*>(root.get());
  if (partitionedOutput == nullptr) {
    return {};
  }

  switch (partitionedOutput->kind()) {
    case velox::core::PartitionedOutputNode::Kind::kBroadcast:
      return {.kind = "broadcast", .label = "broadcast"};
    case velox::core::PartitionedOutputNode::Kind::kArbitrary:
      return {.kind = "arbitrary", .label = "arbitrary"};
    case velox::core::PartitionedOutputNode::Kind::kPartitioned: {
      const auto& keys = partitionedOutput->keys();
      if (keys.empty()) {
        return {.kind = "gather", .label = "gather"};
      }
      std::vector<std::string> escapedKeys;
      escapedKeys.reserve(keys.size());
      for (const auto& key : keys) {
        escapedKeys.push_back(escapeHtml(formatPartitionKey(key)));
      }
      return {
          .kind = "hash",
          .label = fmt::format("hash[{}]", folly::join(", ", escapedKeys)),
      };
    }
  }
  VELOX_UNREACHABLE();
}

std::string formatFragmentHeader(const ExecutableFragment& fragment) {
  if (fragment.numRemotePartitions.has_value()) {
    return fmt::format(
        "{} — {} × {}",
        fragment.taskPrefix,
        FragmentTypeName::toName(fragment.type),
        fragment.numRemotePartitions.value());
  }
  return fmt::format(
      "{} — {}", fragment.taskPrefix, FragmentTypeName::toName(fragment.type));
}

// Returns true if 'node' should be omitted from the body. The root
// PartitionedOutputNode is implied by the output distribution shown in the
// header; ProjectNode adds noise without revealing structure.
bool shouldSkip(const velox::core::PlanNode& node, bool isRoot) {
  if (isRoot &&
      dynamic_cast<const velox::core::PartitionedOutputNode*>(&node)) {
    return true;
  }
  return dynamic_cast<const velox::core::ProjectNode*>(&node) != nullptr;
}

// Formats one body row's content as HTML (indentation + node name + optional
// ": detail" suffix). Returned as raw HTML — caller passes to printRow.
std::string formatBodyContent(
    int32_t indent,
    std::string_view nodeName,
    const std::optional<std::string>& detail) {
  std::string content;
  content.reserve(static_cast<size_t>(indent) * 12 + nodeName.size() + 32);
  for (int32_t level = 0; level < indent; ++level) {
    content += "&nbsp;&nbsp;";
  }
  content += escapeHtml(nodeName);
  if (detail.has_value()) {
    content += ": ";
    content += escapeHtml(*detail);
  }
  return content;
}

// Per-Exchange producer info: task prefix and the producer fragment's
// output distribution (already formatted, e.g. "hash[k]" or "broadcast").
struct ExchangeProducer {
  std::string taskPrefix;
  std::string distribution;
};

using ExchangeProducerMap =
    folly::F14FastMap<velox::core::PlanNodeId, ExchangeProducer>;

// Returns the per-row detail suffix for a body node, or std::nullopt when
// nothing extra is interesting:
//   - Exchange / MergeExchange → "stageN, hash" (producer + distribution).
//   - TableScan → table name from the connector handle.
//   - Limit → "N" or "N (offset M)".
//   - TopN → "N".
std::optional<std::string> bodyDetail(
    const velox::core::PlanNode& node,
    const ExchangeProducerMap& exchangeProducers) {
  if (dynamic_cast<const velox::core::ExchangeNode*>(&node) != nullptr) {
    const auto it = exchangeProducers.find(node.id());
    if (it == exchangeProducers.end()) {
      return std::nullopt;
    }
    if (it->second.distribution.empty()) {
      return it->second.taskPrefix;
    }
    return fmt::format(
        "{}, {}", it->second.taskPrefix, it->second.distribution);
  }
  if (const auto* scan =
          dynamic_cast<const velox::core::TableScanNode*>(&node)) {
    if (scan->tableHandle() != nullptr) {
      return scan->tableHandle()->name();
    }
    return std::nullopt;
  }
  if (const auto* limit = dynamic_cast<const velox::core::LimitNode*>(&node)) {
    if (limit->offset() > 0) {
      return fmt::format("{} (offset {})", limit->count(), limit->offset());
    }
    return std::to_string(limit->count());
  }
  if (const auto* topN = dynamic_cast<const velox::core::TopNNode*>(&node)) {
    return std::to_string(topN->count());
  }
  return std::nullopt;
}

// Walks the Velox plan tree. For each non-skipped node, computes the row
// content and emits it via printRow. Indentation reflects tree depth among
// emitted (non-skipped) nodes.
void walkBody(
    std::ostream& out,
    const velox::core::PlanNodePtr& node,
    int32_t indent,
    bool isRoot,
    const ExchangeProducerMap& exchangeProducers) {
  const bool skip = shouldSkip(*node, isRoot);
  const int32_t childIndent = skip ? indent : indent + 1;

  if (!skip) {
    const auto detail = bodyDetail(*node, exchangeProducers);
    const auto content = formatBodyContent(indent, node->name(), detail);
    printRow(out, content, /*color=*/"", /*bold=*/false);
  }

  for (const auto& source : node->sources()) {
    walkBody(out, source, childIndent, /*isRoot=*/false, exchangeProducers);
  }
}

void printFragment(
    std::ostream& out,
    int32_t fragmentIndex,
    const ExecutableFragment& fragment,
    const folly::F14FastMap<std::string, std::string>& producerDistribution,
    const NodePredictionMap& prediction) {
  ExchangeProducerMap exchangeProducers;
  for (const auto& input : fragment.inputStages) {
    auto it = producerDistribution.find(input.producerTaskPrefix);
    std::string distribution =
        it != producerDistribution.end() ? it->second : std::string{};
    exchangeProducers.emplace(
        input.consumerNodeId,
        ExchangeProducer{
            .taskPrefix = input.producerTaskPrefix,
            .distribution = std::move(distribution),
        });
  }

  const auto distribution = outputDistribution(fragment.fragment.planNode);
  const auto header = formatFragmentHeader(fragment);

  printTableStart(out, fragmentIndex);
  if (!distribution.label.empty()) {
    auto label = "out: " + distribution.label;
    const auto it = prediction.find(fragment.fragment.planNode->id());
    if (it != prediction.end()) {
      label += fmt::format(
          commaLocale(), "&nbsp;&nbsp;~{:.0Lf} rows", it->second.cardinality);
    }
    // Trailing padding gives Graphviz extra width budget so long
    // "hash[...]" labels do not overflow the cell border.
    label += kCellPadding;
    printRow(out, label, kPalette.highlight, /*bold=*/false);
  }
  printRow(out, escapeHtml(header), kPalette.header, /*bold=*/true);
  walkBody(
      out,
      fragment.fragment.planNode,
      /*indent=*/0,
      /*isRoot=*/true,
      exchangeProducers);
  printTableEnd(out);
}

} // namespace

void MultiFragmentPlanDotPrinter::print(
    const MultiFragmentPlan& plan,
    const NodePredictionMap& prediction,
    std::ostream& out) {
  out << "digraph multi_fragment_plan {\n";
  out << "  rankdir=BT;\n";
  out << "  node [fontname=\"Helvetica\"];\n";
  out << "  edge [fontname=\"Helvetica\", color=\"" << kPalette.lines
      << "\"];\n";

  folly::F14FastMap<std::string, int32_t> taskPrefixToIndex;
  folly::F14FastMap<std::string, std::string> producerDistribution;
  const auto& fragments = plan.fragments();
  taskPrefixToIndex.reserve(fragments.size());
  producerDistribution.reserve(fragments.size());
  for (int32_t index = 0; index < static_cast<int32_t>(fragments.size());
       ++index) {
    const auto& fragment = fragments[index];
    taskPrefixToIndex[fragment.taskPrefix] = index;
    producerDistribution[fragment.taskPrefix] =
        outputDistribution(fragment.fragment.planNode).kind;
  }

  for (int32_t index = 0; index < static_cast<int32_t>(fragments.size());
       ++index) {
    printFragment(
        out, index, fragments[index], producerDistribution, prediction);
  }

  for (int32_t index = 0; index < static_cast<int32_t>(fragments.size());
       ++index) {
    const auto& fragment = fragments[index];
    for (const auto& input : fragment.inputStages) {
      const auto producerIt = taskPrefixToIndex.find(input.producerTaskPrefix);
      VELOX_CHECK(
          producerIt != taskPrefixToIndex.end(),
          "Unknown producer task prefix: {}",
          input.producerTaskPrefix);
      out << "  fragment_" << producerIt->second << " -> fragment_" << index
          << ";\n";
    }
  }

  out << "}\n";
}

} // namespace facebook::axiom::graphviz
