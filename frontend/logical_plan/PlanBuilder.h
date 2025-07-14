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

#include "logical_plan/LogicalPlanNode.h" //@manual
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/PlanNodeIdGenerator.h"

namespace facebook::velox::logical_plan {

class NameAllocator;
class NameMappings;

class PlanBuilder {
 public:
  struct Context {
    std::shared_ptr<core::PlanNodeIdGenerator> planNodeIdGenerator;
    std::shared_ptr<NameAllocator> nameAllocator;

    Context()
        : planNodeIdGenerator{std::make_shared<core::PlanNodeIdGenerator>()},
          nameAllocator{std::make_shared<NameAllocator>()} {}
  };

  PlanBuilder()
      : planNodeIdGenerator_(std::make_shared<core::PlanNodeIdGenerator>()),
        nameAllocator_(std::make_shared<NameAllocator>()) {}

  explicit PlanBuilder(const Context& context)
      : planNodeIdGenerator_{context.planNodeIdGenerator},
        nameAllocator_{context.nameAllocator} {
    VELOX_CHECK_NOT_NULL(planNodeIdGenerator_);
    VELOX_CHECK_NOT_NULL(nameAllocator_);
  }

  PlanBuilder& values(const RowTypePtr& rowType, std::vector<Variant> rows);

  PlanBuilder& tableScan(
      const std::string& connectorId,
      const std::string& tableName,
      const std::vector<std::string>& columnNames);

  PlanBuilder& filter(const std::string& predicate);

  PlanBuilder& project(const std::vector<std::string>& projections);

  /// An alias for 'project'.
  PlanBuilder& map(const std::vector<std::string>& projections) {
    return project(projections);
  }

  /// Similar to 'project', but appends 'projections' to the existing columns.
  PlanBuilder& with(const std::vector<std::string>& projections);

  PlanBuilder& aggregate(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates);

  PlanBuilder& join(
      const PlanBuilder& right,
      const std::string& condition,
      JoinType joinType);

  PlanBuilder& sort(const std::vector<std::string>& sortingKeys);

  /// An alias for 'sort'.
  PlanBuilder& orderBy(const std::vector<std::string>& sortingKeys) {
    return sort(sortingKeys);
  }

  PlanBuilder& limit(int32_t count) {
    return limit(0, count);
  }

  PlanBuilder& limit(int32_t offset, int32_t count);

  PlanBuilder& as(const std::string& alias);

  LogicalPlanNodePtr build();

 private:
  std::string nextId() {
    return planNodeIdGenerator_->next();
  }

  std::string newName(const std::string& hint);

  ExprPtr resolveInputName(
      const std::optional<std::string>& alias,
      const std::string& name) const;

  ExprPtr resolveScalarTypes(const core::ExprPtr& expr) const;

  AggregateExprPtr resolveAggregateTypes(const core::ExprPtr& expr) const;

  void resolveProjections(
      const std::vector<std::string>& projections,
      std::vector<std::string>& outputNames,
      std::vector<ExprPtr>& exprs,
      NameMappings& mappings);

  const std::shared_ptr<core::PlanNodeIdGenerator> planNodeIdGenerator_;
  const std::shared_ptr<NameAllocator> nameAllocator_;
  const parse::ParseOptions parseOptions_;

  LogicalPlanNodePtr node_;

  // Mapping from user-provided to auto-generated output column names.
  std::shared_ptr<NameMappings> outputMapping_;
};

/// Generate unique names based on user-provided hints.
class NameAllocator {
 public:
  /// Returns 'hint' as is it is unique. Otherwise, return 'hint_N' where N is a
  /// numeric suffix appended to ensure uniqueness. If 'hint' already has a
  /// suffix and is not unique, the suffix is replaced with a new one.
  ///
  /// Example:
  ///
  ///   newName("a") -> "a"
  ///   newName("a") -> "a_0"
  ///   newName("a") -> "a_1"
  ///   newName("a_0") -> "a_2"
  std::string newName(const std::string& hint);

  void reset() {
    names_.clear();
    nextId_ = 0;
  }

 private:
  std::unordered_set<std::string> names_;
  int32_t nextId_{0};
};

/// Maintains a mapping from user-visible names to auto-generated IDs.
/// Unique names may be accessed by name alone. Non-unique names must be
/// disambiguated using an alias.
class NameMappings {
 public:
  struct QualifiedName {
    std::optional<std::string> alias;
    std::string name;

    bool operator==(const QualifiedName& other) const {
      return alias == other.alias && name == other.name;
    }

    std::string toString() const {
      if (alias.has_value()) {
        return fmt::format("{}.{}", alias.value(), name);
      }

      return name;
    }
  };

  /// Adds a mapping from 'name' to 'id'. Throws if 'name' already exists.
  void add(const QualifiedName& name, const std::string& id) {
    bool ok = mappings_.emplace(name, id).second;
    VELOX_CHECK(ok, "Duplicate name: {}", name.toString());
  }

  /// Adds a mapping from 'name' to 'id'. Throws if 'name' already exists.
  void add(const std::string& name, const std::string& id) {
    bool ok =
        mappings_.emplace(QualifiedName{.alias = {}, .name = name}, id).second;
    VELOX_CHECK(ok, "Duplicate name: {}", name);
  }

  /// Returns ID for the specified 'name' if exists.
  std::optional<std::string> lookup(const std::string& name) const;

  /// Returns ID for the specified 'name' if exists.
  std::optional<std::string> lookup(
      const std::string& alias,
      const std::string& name) const;

  /// Returns all names for the specified ID. There can be up to 2 names: w/ and
  /// w/o alias.
  std::vector<QualifiedName> reverseLookup(const std::string& id) const;

  /// Sets new alias for the names. Unique names will be accessible both with
  /// the new alias and without. Ambiguous names will no longer be accessible.
  ///
  /// Used in PlanBuilder::as() API.
  void setAlias(const std::string& alias);

  /// Merges mappings from 'other' into this. Removes unqualified access to
  /// non-unique names.
  ///
  /// @pre IDs are unique across 'this' and 'other'. This expectation is not
  /// verified explicitly. Violations would lead to undefined behavior.
  ///
  /// Used in PlanBuilder::join() API.
  void merge(const NameMappings& other);

  /// Returns a mapping from IDs to unaliased names for a subset of columns with
  /// unique names.
  ///
  /// Used to produce final output.
  std::unordered_map<std::string, std::string> uniqueNames() const;
  std::string toString() const;

  void reset() {
    mappings_.clear();
  }

 private:
  struct QualifiedNameHasher {
    size_t operator()(const QualifiedName& value) const;
  };

  // Mapping from names to IDs. Unique names may appear twice: w/ and w/o an
  // alias.
  std::unordered_map<QualifiedName, std::string, QualifiedNameHasher> mappings_;
};

} // namespace facebook::velox::logical_plan
