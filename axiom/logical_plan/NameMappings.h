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

#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>
#include <optional>
#include <string>
#include <vector>

namespace facebook::axiom::logical_plan {

/// Maintains a mapping from user-visible names to auto-generated column IDs.
/// Unique names may be accessed by name alone. Non-unique names must be
/// disambiguated using an alias. Also tracks optional user-specified output
/// names per column ID to support duplicate and empty names in query output.
class NameMappings {
 public:
  struct QualifiedName {
    std::optional<std::string> alias;
    std::string name;

    bool operator==(const QualifiedName& other) const = default;

    std::string toString() const;
  };

  /// Adds a mapping from 'name' to 'id'. Throws if 'name' already exists.
  void add(const QualifiedName& name, const std::string& id);

  /// Adds a mapping from 'name' to 'id'. Throws if 'name' already exists.
  void add(const std::string& name, const std::string& id);

  /// Marks the specified 'id' as hidden. The 'id' must have been added earlier
  /// via 'add' API.
  void markHidden(const std::string& id);

  /// Returns ID for the specified 'name' if exists.
  std::optional<std::string> lookup(const std::string& name) const;

  /// Returns ID for the specified 'alias.name' if exists.
  std::optional<std::string> lookup(
      const std::string& alias,
      const std::string& name) const;

  /// Returns true if the specified 'id' was marked as hidden via 'markHidden'
  /// API.
  bool isHidden(const std::string& id) const;

  /// Returns all names for the specified ID. There can be up to 2 names: w/ and
  /// w/o alias.
  std::vector<QualifiedName> reverseLookup(const std::string& id) const;

  /// Sets new alias for the names. Unique names will be accessible both with
  /// the new alias and without. Ambiguous names will no longer be accessible.
  ///
  /// Used in PlanBuilder::as() API.
  void setAlias(const std::string& alias);

  /// Merges mappings and user names from 'other' into this. Removes
  /// unqualified access to non-unique names.
  ///
  /// @pre IDs are unique across 'this' and 'other'. This expectation is not
  /// verified explicitly. Violations would lead to undefined behavior.
  ///
  /// Used in PlanBuilder::join() and PlanBuilder::unnest() APIs.
  void merge(const NameMappings& other);

  /// Enables unqualified access to unique names.
  void enableUnqualifiedAccess();

  /// Returns a mapping from IDs to unaliased names for a subset of columns with
  /// unique names.
  ///
  /// Used to produce final output.
  folly::F14FastMap<std::string, std::string> uniqueNames() const;

  /// Returns a set of IDs for all columns that are accessible using specified
  /// 'alias'.
  folly::F14FastSet<std::string> idsWithAlias(const std::string& alias) const;

  /// Stores a user-specified output name for the given column ID. May be empty
  /// or duplicate across columns. Each ID may only be set once.
  void addUserName(const std::string& id, const std::string& name);

  /// Copies the user-specified output name for the given column ID from
  /// 'source', if one exists. Does nothing if 'source' has no user name for
  /// the given ID.
  void copyUserName(const std::string& id, const NameMappings& source);

  /// Returns the user-specified output name for the given column ID, or
  /// nullptr.
  const std::string* userName(const std::string& id) const;

  std::string toString() const;

  void reset() {
    mappings_.clear();
    userNames_.clear();
  }

 private:
  struct QualifiedNameHasher {
    size_t operator()(const QualifiedName& value) const;
  };

  // Mapping from names to IDs. Unique names may appear twice: w/ and w/o an
  // alias.
  folly::F14FastMap<QualifiedName, std::string, QualifiedNameHasher> mappings_;

  // IDs of hidden columns.
  folly::F14FastSet<std::string> hiddenIds_;

  // Maps column ID to user-specified output name. May contain empty strings
  // and values that are duplicated across different IDs.
  folly::F14FastMap<std::string, std::string> userNames_;
};

} // namespace facebook::axiom::logical_plan
