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
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "axiom/connectors/ConnectorContext.h"

namespace facebook::axiom {

/// Holds what one component is given for one query: the query's context, this
/// component's property slice, and the writer it records into. The application
/// makes one when it decides to run that component, and gives every component
/// of a query the same context.
///
/// A component reaches a connector through its context:
///   metadata->beginWrite(session->context()->sessionFor(*metadata), ...);
///
/// Invariants:
///   - `context` and `statsWriter` are non-null.
class BaseSession {
 public:
  BaseSession(
      connector::ConnectorContextPtr context,
      std::shared_ptr<velox::BaseRuntimeStatWriter> statsWriter,
      connector::Properties properties)
      : context_{std::move(context)},
        statsWriter_{std::move(statsWriter)},
        properties_{std::move(properties)} {
    VELOX_CHECK_NOT_NULL(context_, "BaseSession requires a context");
    VELOX_CHECK_NOT_NULL(statsWriter_, "BaseSession requires a writer");
  }

  virtual ~BaseSession() = default;

  /// Returns the query's context, shared with every other component session of
  /// the query and with nothing outside it.
  const connector::ConnectorContextPtr& context() const {
    return context_;
  }

  /// Returns the query identifier.
  const std::string& queryId() const {
    return context_->queryId();
  }

  /// Returns the identity of the user who submitted the query.
  const std::string& user() const {
    return context_->user();
  }

  /// Returns the value of session property 'name' if set for this component,
  /// or std::nullopt otherwise. The returned view is valid for the lifetime of
  /// this session.
  std::optional<std::string_view> property(std::string_view name) const {
    auto it = properties_.find(name);
    if (it == properties_.end()) {
      return std::nullopt;
    }
    return it->second;
  }

  /// Returns this component's write handle into the query's stats.
  velox::BaseRuntimeStatWriter& statsWriter() const {
    return *statsWriter_;
  }

 private:
  const connector::ConnectorContextPtr context_;
  const std::shared_ptr<velox::BaseRuntimeStatWriter> statsWriter_;
  const connector::Properties properties_;
};

} // namespace facebook::axiom
