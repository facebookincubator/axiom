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

#include <functional>

#include <antlr4-runtime.h>

namespace axiom::sql {

/// Bounds the nesting of grammar rules while an ANTLR parser runs, so that
/// deeply nested input fails instead of overflowing the stack.
///
///   DepthLimitListener depthLimit(1024, [](const antlr4::Token& start) {
///     VELOX_USER_FAIL("Expression exceeds maximum nesting depth");
///   });
///   parser.addParseListener(&depthLimit);
class DepthLimitListener : public antlr4::tree::ParseTreeListener {
 public:
  /// @param fail Called with the first token of the rule that exceeds
  /// 'maxDepth'. Must throw.
  DepthLimitListener(
      uint32_t maxDepth,
      std::function<void(const antlr4::Token& start)> fail)
      : maxDepth_{maxDepth}, fail_{std::move(fail)} {}

  void enterEveryRule(antlr4::ParserRuleContext* context) override;

  void exitEveryRule(antlr4::ParserRuleContext* /*context*/) override {
    if (depth_ > 0) {
      --depth_;
    }
  }

  void visitTerminal(antlr4::tree::TerminalNode* /*node*/) override {}

  void visitErrorNode(antlr4::tree::ErrorNode* /*node*/) override {}

  /// Resets the depth, to parse again.
  void reset() {
    depth_ = 0;
  }

 private:
  const uint32_t maxDepth_;
  const std::function<void(const antlr4::Token& start)> fail_;
  uint32_t depth_{0};
};

} // namespace axiom::sql
