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
///
/// A name that names more than one column is marked ambiguous. 'lookup'
/// returns std::nullopt for it, as for a name that names no column, and
/// 'isAmbiguous' tells the two apart. For example, after merging relations 't'
/// and 'u' that both have a column 'x':
///
///   lookup("t", "x")                                   // t's column
///   lookup("x")                                        // std::nullopt
///   isAmbiguous({.alias = std::nullopt, .name = "x"})  // true
///
/// Invariants:
/// - A name maps to one column or is marked ambiguous, never both.
/// - A marked name names two or more columns of the scope.
class NameMappings {
 public:
  struct QualifiedName {
    std::optional<std::string> alias;
    std::string name;

    bool operator==(const QualifiedName& other) const = default;

    std::string toString() const;
  };

  /// Adds a mapping from 'name' to 'id'. Throws if 'name' already exists.
  /// Does nothing if 'name' was marked ambiguous, so a caller may not assume
  /// that 'name' resolves once this returns.
  void add(const QualifiedName& name, const std::string& id);

  /// @overload
  void add(const std::string& name, const std::string& id);

  /// Drops the mapping for 'name' and marks it ambiguous: it names more than
  /// one column, so it names none. The columns stay in the output, reachable
  /// by their other names. The mark lasts until 'reset'.
  void markAmbiguous(const QualifiedName& name);

  /// Marks the specified 'id' as hidden. The 'id' must have been added earlier
  /// via 'add' API.
  void markHidden(const std::string& id);

  /// Returns ID for the specified 'name' if exists.
  std::optional<std::string> lookup(const std::string& name) const;

  /// Returns ID for the specified 'alias.name' if exists.
  std::optional<std::string> lookup(
      const std::string& alias,
      const std::string& name) const;

  /// Returns true if 'name' names more than one column.
  bool isAmbiguous(const QualifiedName& name) const;

  /// Returns true if the specified 'id' was marked as hidden via 'markHidden'
  /// API.
  bool isHidden(const std::string& id) const;

  /// Returns all names for the specified ID. There can be up to 2 names: w/ and
  /// w/o alias.
  std::vector<QualifiedName> reverseLookup(const std::string& id) const;

  /// Sets new alias for the names. Unique names will be accessible both with
  /// the new alias and without. Ambiguous names will no longer be accessible.
  /// 'ids' are the IDs of all columns of the relation; 'idsWithAlias' returns
  /// them all for the new alias. An ambiguous unqualified name is also
  /// ambiguous under the new alias.
  ///
  /// Used in PlanBuilder::as() API.
  void setAlias(const std::string& alias, const std::vector<std::string>& ids);

  /// Drops all aliases and qualified names, ambiguous ones included, leaving
  /// only unqualified access. Columns that were reachable only through an alias
  /// are no longer accessible by name.
  ///
  /// Used in PlanBuilder::clearAliases() API.
  void clearAliases();

  /// Merges mappings and user names from 'other' into this. Removes
  /// unqualified access to names that both sides use in any form: qualified,
  /// unqualified or ambiguous. Keeps both sides' ambiguous names and the
  /// columns each alias names.
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

  /// Returns the IDs of the columns that 'alias' names: the columns of the
  /// relation it was set on with 'setAlias', and the columns reachable as
  /// 'alias.name'.
  folly::F14FastSet<std::string> idsWithAlias(const std::string& alias) const;

  /// Stores a user-specified output name for the given column ID. May be empty
  /// or duplicate across columns. Each ID may only be set once.
  void addUserName(const std::string& id, const std::string& name);

  /// Copies the user-specified output name for the given column ID from
  /// 'source', if one exists. Does nothing if 'source' has no user name for
  /// the given ID.
  void copyUserName(const std::string& id, const NameMappings& source);

  /// Adds the column 'id' to every alias that names it in 'source'. Used when
  /// a scope rebuilt from 'source' keeps that column.
  void copyAliases(const std::string& id, const NameMappings& source);

  /// Marks every name that 'source' marks ambiguous. Used when a scope rebuilt
  /// from 'source' keeps all of its columns.
  void copyAmbiguousNames(const NameMappings& source);

  /// Returns the user-specified output name for the given column ID, or
  /// nullptr.
  const std::string* userName(const std::string& id) const;

  std::string toString() const;

  void reset() {
    mappings_.clear();
    reverseIndex_.clear();
    userNames_.clear();
    ambiguousNames_.clear();
    aliasIds_.clear();
  }

 private:
  struct QualifiedNameHasher {
    size_t operator()(const QualifiedName& value) const;
  };

  // Re-derives reverseIndex_ from mappings_.
  void rebuildReverseIndex();

  // Adds a mapping unless 'name' is marked ambiguous. Every insertion into
  // mappings_ goes through here, so a marked name cannot be reinstated.
  // Returns true if the mapping was added.
  bool insert(const QualifiedName& name, const std::string& id);

  // Mapping from names to IDs. Unique names may appear twice: w/ and w/o an
  // alias.
  folly::F14FastMap<QualifiedName, std::string, QualifiedNameHasher> mappings_;

  // Inverse of mappings_: each ID maps to the QualifiedName(s) that resolve
  // to it (at most 2: with and without alias). Kept in sync with mappings_.
  folly::F14FastMap<std::string, std::vector<QualifiedName>> reverseIndex_;

  // Names that resolved to more than one column.
  folly::F14FastSet<QualifiedName, QualifiedNameHasher> ambiguousNames_;

  // Maps each alias set with 'setAlias' to the IDs of its relation's columns,
  // including columns that no name in mappings_ reaches.
  folly::F14FastMap<std::string, folly::F14FastSet<std::string>> aliasIds_;

  // IDs of hidden columns.
  folly::F14FastSet<std::string> hiddenIds_;

  // Maps column ID to user-specified output name. May contain empty strings
  // and values that are duplicated across different IDs.
  folly::F14FastMap<std::string, std::string> userNames_;
};

} // namespace facebook::axiom::logical_plan
